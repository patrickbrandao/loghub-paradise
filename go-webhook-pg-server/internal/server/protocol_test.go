package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

// countingBody is an endless request body that counts what the handler read.
type countingBody struct{ n atomic.Int64 }

func (b *countingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	b.n.Add(int64(len(p)))
	return len(p), nil
}

// SPEC §4.3, the reject drain: only HTTP/1.x without a pending
// Expect: 100-continue drains, and only HTTP/1.x ever gets Connection: close.
// On h2 that header becomes a GOAWAY for the whole connection, which a proxy
// shares among many clients; with Expect pending, reading the body is what
// makes net/http send the 100 Continue that invites it.
func TestRejectDrainFollowsTheProtocol(t *testing.T) {
	cases := []struct {
		name      string
		major     int
		expect    string
		wantRead  bool
		wantClose bool
	}{
		{name: "http/1.1 drains and closes past the limit", major: 1, wantRead: true, wantClose: true},
		{name: "http/2 neither drains nor closes", major: 2},
		{name: "expect 100-continue is not drained", major: 1, expect: "100-continue"},
		{name: "expect is case-insensitive", major: 1, expect: "100-Continue"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testServer(t, &stubRing{}, Config{RejectDrainBytes: 64 << 10})
			s.health.MarkFatal(errors.New("destination gone")) // the 503 of step 8

			body := &countingBody{}
			r := httptest.NewRequest("POST", "/hook", body)
			r.ContentLength = 128 << 10
			if tc.major == 2 {
				r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/2.0", 2, 0
			}
			if tc.expect != "" {
				r.Header.Set("Expect", tc.expect)
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)

			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", w.Code)
			}
			if read := body.n.Load(); (read > 0) != tc.wantRead {
				t.Errorf("%d body bytes read; want a drain: %v", read, tc.wantRead)
			}
			if got := w.Header().Get("Connection") == "close"; got != tc.wantClose {
				t.Errorf("Connection: %q; want close: %v", w.Header().Get("Connection"), tc.wantClose)
			}
		})
	}
}

// startOnSocket serves s on a loopback port and returns the address.
func startOnSocket(t *testing.T, s *Server) string {
	t.Helper()
	if err := s.Listen(context.Background()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })
	return s.Addr()
}

// On a real socket: a refused request that announced Expect: 100-continue
// gets its refusal as the FIRST line, never a 100 Continue. The client sends
// no body at all, so a drain would sit on it until read_timeout after
// inviting it.
func TestRejectWithExpectSendsNo100Continue(t *testing.T) {
	s, _ := testServer(t, &stubRing{}, Config{Bind: "127.0.0.1:0", Listeners: 1,
		RejectDrainBytes: 64 << 10, ReadTimeout: 3 * time.Second})
	s.health.MarkFatal(errors.New("destination gone"))
	addr := startOnSocket(t, s)

	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, "POST /hook HTTP/1.1\r\nHost: example.com\r\n"+
		"Content-Length: 100000\r\nExpect: 100-continue\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading the first line: %v", err)
	}
	if !strings.HasPrefix(line, "HTTP/1.1 503") {
		t.Fatalf("first line %q, want the 503 and no 100 Continue", strings.TrimSpace(line))
	}
	// Not draining is the right answer only because net/http then closes the
	// connection itself: the unsent body cannot be left in the stream.
	resp, err := http.ReadResponse(bufio.NewReader(io.MultiReader(strings.NewReader(line), br)), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if !resp.Close {
		t.Error("the connection was left open with an unsent body announced")
	}
}

// SPEC §13.5, server.write_timeout: the deadline covers writing the answer,
// counted from when the answer starts, not from the end of the headers. A
// body that takes longer than write_timeout to arrive -- well inside
// read_timeout -- still gets its 200; counted from the headers, the event went
// into the ring and the answer was lost, so the client sent it again under
// another uuid.
func TestSlowUploadStillGetsItsAnswer(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServer(t, stub, Config{Bind: "127.0.0.1:0", Listeners: 1,
		ReadTimeout: 5 * time.Second, WriteTimeout: 200 * time.Millisecond})
	addr := startOnSocket(t, s)

	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, "POST /hook HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond) // three times write_timeout
	if _, err := io.WriteString(c, "hello"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("no answer to an upload slower than write_timeout: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
}

// The same over h2c, where net/http arms write_timeout per stream when the
// stream opens and resets it with INTERNAL_ERROR when it fires -- the body
// still arriving.
func TestSlowUploadStillGetsItsAnswerOverH2C(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServer(t, stub, Config{Bind: "127.0.0.1:0", Listeners: 1, HTTP2: true, HTTP2MaxStreams: 16,
		ReadTimeout: 5 * time.Second, WriteTimeout: 200 * time.Millisecond})
	addr := startOnSocket(t, s)

	tr := &http.Transport{}
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = &p
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	pr, pw := io.Pipe()
	go func() {
		time.Sleep(600 * time.Millisecond) // three times write_timeout
		_, _ = io.WriteString(pw, "hello")
		_ = pw.Close()
	}()
	req, err := http.NewRequest("POST", "http://"+addr+"/hook", pr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("no answer to an h2c upload slower than write_timeout: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 {
		t.Fatalf("status %d over HTTP/%d, want 200 over HTTP/2", resp.StatusCode, resp.ProtoMajor)
	}
}

// The deadline is re-armed, not dropped: every answer -- the 200, a refusal
// before the uuid, one after it -- is written with write_timeout armed from
// the moment it starts.
func TestWriteTimeoutIsArmedForEveryAnswer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stub  *stubRing
		setup func(s *Server)
		code  int
	}{
		{name: "200", stub: &stubRing{}, code: http.StatusOK},
		{name: "503 before the uuid", stub: &stubRing{},
			setup: func(s *Server) { s.health.MarkFatal(errors.New("gone")) }, code: http.StatusServiceUnavailable},
		{name: "429 after the uuid", stub: &stubRing{err: ring.ErrFull}, code: http.StatusTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testServer(t, tc.stub, Config{WriteTimeout: 200 * time.Millisecond})
			if tc.setup != nil {
				tc.setup(s)
			}
			w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
			s.ServeHTTP(w, httptest.NewRequest("POST", "/hook", strings.NewReader("x")))
			if w.Code != tc.code {
				t.Fatalf("status %d, want %d", w.Code, tc.code)
			}
			if w.atHeader.IsZero() {
				t.Fatal("the answer was written with no write deadline armed")
			}
			if left := w.atHeader.Sub(w.headerAt); left <= 0 || left > 200*time.Millisecond {
				t.Errorf("deadline %v after the answer started, want within write_timeout", left)
			}
		})
	}
}

// SPEC §13.5 (§19.1 item 32): the deadline is cleared on entry only when
// read_timeout bounds the body. With read_timeout = 0 nothing else would: an
// h2c stream reading a slow body would hold a goroutine, up to 64 KiB and a
// max_concurrent_streams slot for ever. There net/http's write_timeout keeps
// limiting the upload, as before the change of item 4.
func TestWriteDeadlineIsKeptWithoutReadTimeout(t *testing.T) {
	for _, tc := range []struct {
		read    time.Duration
		cleared bool
	}{{read: 5 * time.Second, cleared: true}, {read: 0, cleared: false}} {
		s, _ := testServer(t, &stubRing{}, Config{ReadTimeout: tc.read, WriteTimeout: 200 * time.Millisecond})
		w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
		s.ServeHTTP(w, httptest.NewRequest("POST", "/hook", strings.NewReader("x")))
		if w.Code != http.StatusOK {
			t.Fatalf("read_timeout %v: status %d, want 200", tc.read, w.Code)
		}
		if w.clearedOnce != tc.cleared {
			t.Errorf("read_timeout %v: deadline cleared on entry = %v, want %v", tc.read, w.clearedOnce, tc.cleared)
		}
	}
}

// The same on a real h2c stream: with read_timeout = 0, an upload slower
// than write_timeout is reset by net/http, never held without a deadline.
func TestSlowH2CUploadIsBoundedWithoutReadTimeout(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServer(t, stub, Config{Bind: "127.0.0.1:0", Listeners: 1, HTTP2: true, HTTP2MaxStreams: 16,
		ReadTimeout: 0, WriteTimeout: 200 * time.Millisecond})
	addr := startOnSocket(t, s)

	tr := &http.Transport{}
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = &p
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	pr, pw := io.Pipe()
	go func() {
		time.Sleep(800 * time.Millisecond) // four times write_timeout
		_, _ = io.WriteString(pw, "hello")
		_ = pw.Close()
	}()
	req, err := http.NewRequest("POST", "http://"+addr+"/hook", pr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		t.Fatalf("status %d: an h2c upload with no read_timeout outlived write_timeout", resp.StatusCode)
	}
}

// faultyListener fails its first Accepts with a non-temporary error, the way
// ENOBUFS or a firewall's EPERM arrive, then behaves.
type faultyListener struct {
	net.Listener
	fails atomic.Int32
}

func (l *faultyListener) Accept() (net.Conn, error) {
	if l.fails.Add(-1) >= 0 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", syscall.ENOBUFS)}
	}
	return l.Listener.Accept()
}

// SPEC §10.3: an accept error that is not the listener's own close is
// retried, with a pause and a sampled WARN; the listener never gives up.
// net/http retries only Temporary() errors, so ENOBUFS used to end Serve and
// the process exited without draining.
func TestAcceptRetriesNonTemporaryErrors(t *testing.T) {
	s, _ := testServer(t, &stubRing{}, Config{Bind: "127.0.0.1:0", Listeners: 1})
	s.listenFn = func(ctx context.Context, addr string, reusePort bool) (net.Listener, error) {
		ln, err := listen(ctx, addr, reusePort)
		if err != nil {
			return nil, err
		}
		fl := &faultyListener{Listener: ln}
		fl.fails.Store(3)
		return fl, nil
	}
	if err := s.Listen(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })

	resp, _, _ := rawExchange(t, s.Addr(),
		"POST /hook HTTP/1.1\r\nHost: example.com\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d after the accept errors, want 200", resp.StatusCode)
	}
	select {
	case err := <-served:
		t.Fatalf("Serve returned before Close: %v", err)
	default:
	}
	_ = s.Close()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve after Close: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Close")
	}
}

// SPEC §10.3: with server.listeners > 1, one listener that stops with an
// error ends Serve at once, so the process goes through the shutdown. Waiting
// for every listener left a dead one invisible until all of them died.
func TestServeReturnsWhenOneListenerStops(t *testing.T) {
	s, _ := testServer(t, &stubRing{}, Config{Bind: "127.0.0.1:0", Listeners: 2})
	if err := s.Listen(context.Background()); err != nil {
		t.Skipf("two listeners on one port: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })

	// Close one listener from below, as a dead socket would: its http.Server
	// is not shutting down, so its Serve ends with an error.
	_ = s.listeners[0].Close()

	select {
	case err := <-served:
		if err == nil {
			t.Fatal("Serve returned nil for a listener that died")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve is still waiting on the live listener; the dead one is invisible")
	}
}

// deadlineRecorder records the write deadline in force when the header is
// written.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	last, atHeader, headerAt time.Time
	clearedOnce              bool
}

func (w *deadlineRecorder) SetWriteDeadline(d time.Time) error {
	if d.IsZero() {
		w.clearedOnce = true
	}
	w.last = d
	return nil
}
func (w *deadlineRecorder) WriteHeader(code int) {
	w.atHeader, w.headerAt = w.last, time.Now()
	w.ResponseRecorder.WriteHeader(code)
}
