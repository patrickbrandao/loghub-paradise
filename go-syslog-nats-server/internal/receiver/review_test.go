package receiver

// Regression tests for the independent review of 2026-10-05. Each names its
// finding. F1 (filter and rate limiter charged on retry) and the sink
// reviewer's ring=none logic refusal went with the features they were about:
// this server has neither (SPEC §1.2).

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
)

// ------------------------------------------------------------------------ F2

// recConn records the deadlines set on it.
type recConn struct {
	net.Conn
	mu            sync.Mutex
	read, written time.Time
	readSet       int
	writeSet      int
}

func (c *recConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read, c.written = t, t
	c.readSet++
	c.writeSet++
	return nil
}
func (c *recConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = t
	c.readSet++
	return nil
}
func (c *recConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = t
	c.writeSet++
	return nil
}

// F2: the frame loop arms the read and write deadlines together, and a
// shutdown expires both. This server never writes, so the write deadline is
// inert today; the test pins that armDeadline keeps the two in step anyway, so
// a future write on the connection cannot become the one call no shutdown
// deadline reaches.
func TestArmDeadlineArmsBothDirections(t *testing.T) {
	var closing atomic.Bool
	c := &recConn{}
	armDeadline(c, time.Minute, &closing)
	if c.writeSet == 0 || c.written.IsZero() || !c.written.Equal(c.read) {
		t.Fatalf("write deadline %v, read %v: the write direction is not armed", c.written, c.read)
	}
	armDeadline(c, 0, &closing)
	if !c.written.IsZero() || !c.read.IsZero() {
		t.Fatal("idle_timeout=0 must clear both deadlines")
	}
	closing.Store(true)
	armDeadline(c, time.Hour, &closing)
	if time.Until(c.written) > time.Second || time.Until(c.read) > time.Second {
		t.Fatal("a reader re-arming during shutdown undid the shutdown's deadline")
	}
}

// blockConn blocks its reader until Close, ignoring every deadline: a reader no
// deadline can reach, the worst case Close's grace exists for.
type blockConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (b *blockConn) Close() error                     { b.once.Do(func() { close(b.closed) }); return nil }
func (b *blockConn) SetDeadline(time.Time) error      { return nil }
func (b *blockConn) SetReadDeadline(time.Time) error  { return nil }
func (b *blockConn) SetWriteDeadline(time.Time) error { return nil }

// F2: if a reader still does not return after closeGrace, Close closes its
// socket instead of waiting on the peer forever.
func TestStreamCloseForceClosesAfterGrace(t *testing.T) {
	h := newHarness(t, 10)
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m, log: logger(),
		conns: map[net.Conn]struct{}{}}
	c := &blockConn{closed: make(chan struct{})}
	if !s.track(c) {
		t.Fatal("track")
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.untrack(c)
		<-c.closed // a reader parked where no deadline reaches
	}()
	start := time.Now()
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeGrace + 5*time.Second):
		t.Fatal("Close waited on a reader its deadlines could not reach")
	}
	if el := time.Since(start); el < closeGrace/2 {
		t.Fatalf("Close returned after %v: it did not give the readers their grace", el)
	}
}

// ------------------------------------------------------------------------ F3

// errAfterReader yields its data, then fails with err (an idle timeout, a
// reset) instead of io.EOF.
type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// F3: an error that is not EOF, inside the run of digits auto-detection is
// reading, dropped those bytes with no counter. They are a non-transparent
// frame, delivered like any frame the end of the connection completed.
func TestFramerErrorInsideDigitsDeliversThem(t *testing.T) {
	for _, e := range []error{timeoutErr{}, syscall.ECONNRESET, io.EOF} {
		f := NewFramer(bufio.NewReader(&errAfterReader{data: []byte("123"), err: e}), FramingAuto, 1<<10)
		msg, size, fr, err := f.Next()
		if err != nil || string(msg) != "123" || size != 3 || fr != item.FramingNonTransparent {
			t.Fatalf("%v: %q size %d %v err %v", e, msg, size, fr, err)
		}
		if _, _, _, err := f.Next(); err == nil {
			t.Fatalf("%v: the end of the connection was not reported after the frame", e)
		}
	}
}

// F3 over a socket: the idle timeout inside "123" delivers the digits, exactly
// as it delivers "abc".
func TestTCPIdleTimeoutInsideDigitsDeliversThem(t *testing.T) {
	for _, payload := range []string{"abc", "123"} {
		h := newHarness(t, 10)
		tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 200 * time.Millisecond}})
		c := dial(t, tr.Addr())
		fmt.Fprint(c, payload)
		expectClosed(t, c)
		h.waitItems(t, 1)
		if got := h.drainRing(); len(got) != 1 || string(got[0].Raw) != payload {
			t.Fatalf("payload %q: %v", payload, got)
		}
	}
}

// ------------------------------------------------------------------------ F4

// F4: whatever sits at listen.unix is removed ONLY when it is a socket and a
// connect to it is refused. A regular file, a directory and a live stream
// socket are left alone, with a clear error.
func TestUnixRemovesOnlyAStaleDatagramSocket(t *testing.T) {
	h := newHarness(t, 10)
	dir := shortDir(t)

	file := filepath.Join(dir, "precious")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewUnix(context.Background(), UnixConfig{Path: file}, h.in, h.m, logger()); err == nil ||
		!strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("regular file: err = %v", err)
	}
	if b, err := os.ReadFile(file); err != nil || string(b) != "data" {
		t.Fatalf("the regular file was touched: %q %v", b, err)
	}

	sub := filepath.Join(dir, "adir")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewUnix(context.Background(), UnixConfig{Path: sub}, h.in, h.m, logger()); err == nil {
		t.Fatal("a directory at listen.unix was accepted")
	}
	if fi, err := os.Stat(sub); err != nil || !fi.IsDir() {
		t.Fatal("the directory was removed")
	}

	stream := filepath.Join(dir, "stream")
	ln, err := net.Listen("unix", stream)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if _, err := NewUnix(context.Background(), UnixConfig{Path: stream}, h.in, h.m, logger()); err == nil ||
		!strings.Contains(err.Error(), "refusing to remove") {
		t.Fatalf("live stream socket: err = %v", err)
	}
	if c, err := net.Dial("unix", stream); err != nil {
		t.Fatalf("the live stream socket was removed: %v", err)
	} else {
		c.Close()
	}
}

// ------------------------------------------------------------------------ F7

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type emfileListener struct {
	mu     sync.Mutex
	closed bool
	calls  int
}

func (l *emfileListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, net.ErrClosed
	}
	l.calls++
	return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
}
func (l *emfileListener) Close() error   { l.mu.Lock(); l.closed = true; l.mu.Unlock(); return nil }
func (l *emfileListener) Addr() net.Addr { return &net.TCPAddr{} }

// F7: a persistent accept error (EMFILE under a flood) logged an unsampled
// WARN every 10 ms. It is sampled now, and the loop still pauses and goes on.
func TestAcceptErrorLogIsSampled(t *testing.T) {
	h := newHarness(t, 10)
	var buf syncBuf
	l := &emfileListener{}
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m,
		log: obs.NewLogger(&buf, "text", "warn"), conns: map[net.Conn]struct{}{},
		lns: []net.Listener{l}, acceptLog: obs.NewSampler(30 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	l.Close()
	s.wg.Wait()
	if n := strings.Count(buf.String(), "accept failed"); n != 1 {
		t.Fatalf("%d accept-failed lines in 0.5 s, want 1 (sampled)", n)
	}
	l.mu.Lock()
	calls := l.calls
	l.mu.Unlock()
	if calls < 5 || calls > 100 {
		t.Fatalf("%d accept attempts in 0.5 s: the loop must keep trying, with its 10 ms pause", calls)
	}
}

// --------------------------------------------------- PROXY: a stalled header

// A trusted peer whose v1 header starts and then stalls past header_timeout is
// an INVALID header (the signature matched), closed as reason="proxy" -- never
// "absent", and never fed to the framer.
func TestProxyStalledHeaderIsInvalid(t *testing.T) {
	h := newHarness(t, 10)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{
		Proxy: ProxyConfig{Mode: ProxyOptional, Trusted: cidrs, HeaderTimeout: 150 * time.Millisecond}}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "PROXY TCP4 1.2.3.4") // and nothing more
	expectClosed(t, c)
	waitFor(t, "the invalid header", func() bool {
		return h.m.ProxyHeadersTotal[obs.ProxyInvalid].Load() == 1 &&
			h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1
	})
	if h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 0 || h.m.MessagesTotal[obs.TransportTCP].Load() != 0 {
		t.Fatal("a stalled header was treated as absent and framed")
	}
}
