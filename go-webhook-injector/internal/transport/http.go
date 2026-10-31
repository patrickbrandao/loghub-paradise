// Package transport sends the requests of a run to a webhook server
// (SPEC §4): HTTP/1.1 over a keep-alive connection per worker, with optional
// pipelining, or HTTP/2 (h2c by prior knowledge on http://, ALPN on https://)
// with many streams per connection.
package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/source"
)

// Outcomes that are not a status code (SPEC §8.2). A response is reported
// by its status code ("200", "429", ...); only 2xx is a success.
const (
	Timeout     = "timeout"     // no response within --response-timeout
	ConnError   = "conn_error"  // the connection broke before the response
	NoResponse  = "no_response" // the server closed with this request unanswered
	BadResponse = "bad_response"
	NotSent     = "not_sent" // given up at the end before it was written
)

// Req is one request, ready to go on the wire.
type Req struct {
	Method string
	URI    string // origin-form: path and query
	Host   string
	Header [][2]string // in order; never Host, Content-Length or hop-by-hop
	Body   []byte
}

// BuildFunc turns a message of the source into a request.
type BuildFunc func(seq uint64, m *source.Message) Req

// Options of a dialer.
type Options struct {
	URL             *url.URL // scheme and host:port dialed
	Timeout         time.Duration
	ResponseTimeout time.Duration
	TLS             *tls.Config // https only
	HTTP2           bool
	Pipeline        int    // HTTP/1.1 requests in flight per connection
	Streams         int    // HTTP/2 requests in flight per connection
	PerConn         uint64 // HTTP/1.1 requests per connection; 0 = no limit
	Retries         int    // resends of a 429 or 503
	RetryMaxWait    time.Duration
	Build           BuildFunc
	// OnDial is told of every connection attempt (the report counts them).
	OnDial func(error)
	// OnResponse, when set, sees every final response (--uuid-out).
	OnResponse func(seq uint64, status int, uuid string)
}

// Dialer returns the dialer of o.
func Dialer(o Options) (sched.Dialer, error) {
	if o.Build == nil || o.URL == nil {
		return nil, errors.New("transport: Build and URL are required")
	}
	if o.URL.Scheme != "http" && o.URL.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %q", o.URL.Scheme)
	}
	if o.OnDial == nil {
		o.OnDial = func(error) {}
	}
	o.Pipeline = max(o.Pipeline, 1)
	o.Streams = max(o.Streams, 1)
	if o.HTTP2 {
		return func(context.Context) (sched.Conn, error) { return newH2(o), nil }, nil
	}
	return func(ctx context.Context) (sched.Conn, error) {
		c, err := dial(ctx, o)
		o.OnDial(err)
		if err != nil {
			return nil, err
		}
		return newH1(o, c), nil
	}, nil
}

func addr(u *url.URL) string {
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "https" {
		return net.JoinHostPort(u.Hostname(), "443")
	}
	return net.JoinHostPort(u.Hostname(), "80")
}

func dial(ctx context.Context, o Options) (net.Conn, error) {
	d := net.Dialer{Timeout: o.Timeout, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", addr(o.URL))
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if o.URL.Scheme != "https" {
		return c, nil
	}
	cfg := o.TLS.Clone()
	cfg.NextProtos = []string{"http/1.1"}
	t := tls.Client(c, cfg)
	hctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	if err := t.HandshakeContext(hctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return t, nil
}

// AppendRequest writes r in HTTP/1.1 wire form. closeConn adds
// "Connection: close" (the last request of a spent connection).
func AppendRequest(dst []byte, r *Req, closeConn bool) []byte {
	dst = append(dst, r.Method...)
	dst = append(dst, ' ')
	dst = append(dst, r.URI...)
	dst = append(dst, " HTTP/1.1\r\nHost: "...)
	dst = append(dst, r.Host...)
	dst = append(dst, "\r\n"...)
	for _, h := range r.Header {
		dst = append(dst, h[0]...)
		dst = append(dst, ": "...)
		dst = append(dst, h[1]...)
		dst = append(dst, "\r\n"...)
	}
	// A body always carries its length; a bodiless GET or HEAD carries none,
	// as a browser sends it.
	if len(r.Body) > 0 || !bodiless(r.Method) {
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, int64(len(r.Body)), 10)
		dst = append(dst, "\r\n"...)
	}
	if closeConn {
		dst = append(dst, "Connection: close\r\n"...)
	}
	dst = append(dst, "\r\n"...)
	return append(dst, r.Body...)
}

func bodiless(m string) bool { return m == "GET" || m == "HEAD" || m == "OPTIONS" || m == "DELETE" }

// retryable says whether a status is worth resending: the server said "not
// now" (429 ring full, 503 not ready), never "no".
func retryable(status int) bool { return status == 429 || status == 503 }

// retryWait reads Retry-After (seconds or an HTTP date), bounded by max;
// without one, a short pause.
func retryWait(v string, maxWait time.Duration) time.Duration {
	w := 100 * time.Millisecond
	if v != "" {
		if s, err := strconv.Atoi(v); err == nil && s >= 0 {
			w = time.Duration(s) * time.Second
		} else if t, err := http.ParseTime(v); err == nil {
			w = time.Until(t)
		}
	}
	return min(max(w, 0), maxWait)
}

// pend is a request written (or to be resent) and not yet answered.
type pend struct {
	seq   uint64
	msg   *source.Message
	req   Req
	done  sched.DoneFunc
	tries int
	due   time.Time // resend time of a retry
	last  string    // outcome of the last answer, for a retry given up
}

// h1Conn is one HTTP/1.1 keep-alive connection. The worker writes; a reader
// goroutine matches responses to requests in order (pipelining keeps up to
// Pipeline requests unanswered).
//
// Every request given to Send gets done exactly once: from the reader on a
// response, from fail when the connection dies, or from Close for a retry
// that never went out again.
type h1Conn struct {
	o   Options
	c   net.Conn
	bw  *bufio.Writer
	buf []byte

	slots chan struct{} // one token per request in flight

	mu      sync.Mutex
	queue   []*pend // written, unanswered, in wire order
	retries []*pend // answered 429/503, waiting to be resent
	dead    error   // set once by the reader when it stops
	written uint64
	spent   bool // the Connection: close request is written
	closing bool // the server answered "Connection: close"
	unsent  []sched.Pending
	answers uint64

	readerDone chan struct{}
}

func newH1(o Options, c net.Conn) *h1Conn {
	h := &h1Conn{o: o, c: c, bw: bufio.NewWriterSize(c, 64<<10),
		slots: make(chan struct{}, o.Pipeline), readerDone: make(chan struct{})}
	go h.read()
	return h
}

var (
	errDead         = errors.New("connection closed")
	errServerClosed = errors.New("server closed the connection (Connection: close)")
)

// Send implements sched.Conn.
func (h *h1Conn) Send(seq uint64, m *source.Message, done sched.DoneFunc) error {
	p := &pend{seq: seq, msg: m, req: h.o.Build(seq, m), done: done}
	if err := h.resendDue(false); err != nil {
		// p never reached the wire: the next connection takes it.
		h.giveBack(p)
		return err
	}
	return h.write(p)
}

// write puts p on the wire, waiting for a pipeline slot first.
func (h *h1Conn) write(p *pend) error {
	select {
	case h.slots <- struct{}{}:
	default:
		// The pipeline is full: what is buffered must reach the server, or
		// the responses that free a slot never come.
		if err := h.bw.Flush(); err != nil {
			h.giveBack(p)
			return err
		}
		select {
		case h.slots <- struct{}{}:
		case <-h.readerDone:
			h.giveBack(p)
			return h.deadErr()
		}
	}
	h.mu.Lock()
	if h.closing || h.spent || h.dead != nil {
		// Not written: back to the worker, for the next connection. A spent
		// connection already sent its "Connection: close" request; nothing
		// may follow it (a retry may have been that request).
		closing, dead := h.closing || h.spent, h.dead
		h.unsent = append(h.unsent, sched.Pending{Seq: p.seq, Msg: p.msg, Done: p.done, Stuck: h.answers == 0})
		h.mu.Unlock()
		<-h.slots
		if closing {
			return sched.ErrSpent
		}
		return dead
	}
	h.written++
	last := h.o.PerConn > 0 && h.written >= h.o.PerConn
	h.spent = last
	h.queue = append(h.queue, p)
	if len(h.queue) == 1 {
		// The reader may be idle in Peek with no deadline; under the lock,
		// so it cannot clear this one after we set it.
		_ = h.c.SetReadDeadline(time.Now().Add(h.o.ResponseTimeout))
	}
	h.mu.Unlock()

	h.buf = AppendRequest(h.buf[:0], &p.req, last)
	_ = h.c.SetWriteDeadline(time.Now().Add(h.o.Timeout))
	// From here p is the reader's (or fail's). A write error does not close
	// the socket: the server may have said "Connection: close" in a response
	// still unread, and only the reader can tell what it did not process.
	if _, err := h.bw.Write(h.buf); err != nil {
		return err
	}
	// With one request in flight there is nothing to batch: push it now, or
	// the server waits for bytes still in our buffer.
	if h.o.Pipeline == 1 || last {
		if err := h.bw.Flush(); err != nil {
			return err
		}
	}
	if last {
		return sched.ErrSpent
	}
	return nil
}

func (h *h1Conn) deadErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dead != nil {
		return h.dead
	}
	return errDead
}

// resendDue writes the retries whose time has come. all also takes those
// not yet due (Close waits for them first).
func (h *h1Conn) resendDue(all bool) error {
	h.mu.Lock()
	if h.spent {
		h.mu.Unlock()
		return sched.ErrSpent
	}
	if len(h.retries) == 0 {
		h.mu.Unlock()
		return nil
	}
	now := time.Now()
	var due []*pend
	keep := h.retries[:0]
	for _, p := range h.retries {
		if all || !p.due.After(now) {
			due = append(due, p)
		} else {
			keep = append(keep, p)
		}
	}
	h.retries = keep
	h.mu.Unlock()
	for i, p := range due {
		if err := h.write(p); err != nil {
			if !errors.Is(err, sched.ErrSpent) {
				// Never written: the next connection takes them.
				for _, q := range due[i+1:] {
					h.giveBack(q)
				}
				return err
			}
			// Spent: the rest waits for Close, which hands them on.
			h.mu.Lock()
			h.retries = append(h.retries, due[i+1:]...)
			h.mu.Unlock()
			return sched.ErrSpent
		}
	}
	return nil
}

// Flush implements sched.Conn. ErrSpent says a resend used the connection's
// last request: the worker closes it without counting it as broken.
func (h *h1Conn) Flush() error {
	err := h.resendDue(false)
	if err != nil && !errors.Is(err, sched.ErrSpent) {
		return err
	}
	if ferr := h.bw.Flush(); ferr != nil {
		return ferr
	}
	return err
}

// Close implements sched.Conn: waits for the answers and the retries until
// ctx ends, then closes. What the server announced it would not process is
// given back (Unsent); anything else left fails.
func (h *h1Conn) Close(ctx context.Context) error {
	flushed := true
	for ctx.Err() == nil {
		h.mu.Lock()
		var next time.Time
		for _, p := range h.retries {
			if next.IsZero() || p.due.Before(next) {
				next = p.due
			}
		}
		nq, nr := len(h.queue), len(h.retries)
		dead := h.dead != nil
		spent := h.spent
		h.mu.Unlock()
		if nq+nr == 0 || dead {
			break
		}
		if spent && nq == 0 {
			break // only retries left, and no room for them on this connection
		}
		if flushed && !next.IsZero() && !time.Now().Before(next) {
			if h.resendDue(false) != nil {
				flushed = false
			}
		}
		if flushed && h.bw.Flush() != nil {
			// The write side is gone; the reader still decides what the
			// server answered, up to its response timeout.
			flushed = false
		}
		wait := 5 * time.Millisecond
		if !next.IsZero() {
			wait = min(max(time.Until(next), time.Millisecond), 50*time.Millisecond)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-h.readerDone:
		case <-t.C:
		}
		t.Stop()
	}
	h.c.Close()
	<-h.readerDone
	h.mu.Lock()
	rs := h.retries
	h.retries = nil
	// A retry stranded by a spent or server-closed connection goes on the
	// next one; one stranded by the end of the run keeps its last answer.
	if (h.spent || h.closing) && ctx.Err() == nil {
		for _, p := range rs {
			h.unsent = append(h.unsent, sched.Pending{Seq: p.seq, Msg: p.msg, Done: p.done, Stuck: h.answers == 0})
		}
		rs = nil
	}
	h.mu.Unlock()
	for _, p := range rs {
		p.done(p.last, false)
	}
	return nil
}

// giveBack hands a request that never reached the wire back to the worker.
func (h *h1Conn) giveBack(p *pend) {
	h.mu.Lock()
	h.unsent = append(h.unsent, sched.Pending{Seq: p.seq, Msg: p.msg, Done: p.done, Stuck: h.answers == 0})
	h.mu.Unlock()
}

// Unsent implements sched.Resender.
func (h *h1Conn) Unsent() []sched.Pending {
	h.mu.Lock()
	defer h.mu.Unlock()
	u := h.unsent
	h.unsent = nil
	return u
}

// read matches responses to the queue, in order.
func (h *h1Conn) read() {
	defer close(h.readerDone)
	br := bufio.NewReaderSize(h.c, 64<<10)
	for {
		h.mu.Lock()
		for len(h.queue) == 0 {
			// Idle: no deadline, the server may keep the connection open.
			// Set under the lock: write sets the next one under it too.
			_ = h.c.SetReadDeadline(time.Time{})
			h.mu.Unlock()
			// Peek blocks until a byte or an error: an unsolicited close
			// (idle timeout of the server) is seen here.
			if _, err := br.Peek(1); err != nil {
				h.fail(err, true)
				return
			}
			h.mu.Lock()
			if len(h.queue) == 0 {
				h.mu.Unlock()
				h.fail(errors.New("unsolicited bytes from the server"), true)
				return
			}
		}
		p := h.queue[0]
		h.mu.Unlock()
		_ = h.c.SetReadDeadline(time.Now().Add(h.o.ResponseTimeout))
		resp, err := http.ReadResponse(br, &http.Request{Method: p.req.Method})
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		if err != nil {
			h.fail(err, false)
			return
		}
		// Popped and, if it is a retry, queued again in one critical section:
		// Close must never see it in neither list (it would close early and
		// fail a retry the drain had room for).
		retry := retryable(resp.StatusCode) && p.tries < h.o.Retries
		h.mu.Lock()
		h.queue[0] = nil
		h.queue = h.queue[1:]
		h.answers++
		if retry {
			p.tries++
			p.last = strconv.Itoa(resp.StatusCode)
			p.due = time.Now().Add(retryWait(resp.Header.Get("Retry-After"), h.o.RetryMaxWait))
			h.retries = append(h.retries, p)
		}
		h.mu.Unlock()
		<-h.slots
		if !retry {
			h.answer(p, resp)
		}
		if resp.Close {
			// RFC 9112 §9.6: the server reads nothing after this response,
			// so what is behind it was not processed and can go again.
			h.mu.Lock()
			h.closing = true
			for _, q := range h.queue {
				h.unsent = append(h.unsent, sched.Pending{Seq: q.seq, Msg: q.msg, Done: q.done, Stuck: h.answers == 0})
				<-h.slots
			}
			h.queue = nil
			h.mu.Unlock()
			h.fail(errServerClosed, true)
			return
		}
	}
}

// answer records a final response.
func (h *h1Conn) answer(p *pend, resp *http.Response) {
	code := strconv.Itoa(resp.StatusCode)
	if h.o.OnResponse != nil {
		h.o.OnResponse(p.seq, resp.StatusCode, resp.Header.Get("X-LogHub-UUID"))
	}
	p.done(code, resp.StatusCode >= 200 && resp.StatusCode < 300)
}

// fail ends the connection and answers what is still in the queue. clean
// says the connection ended between responses (a close by the server), not
// in the middle of one.
func (h *h1Conn) fail(err error, clean bool) {
	h.c.Close()
	h.mu.Lock()
	if h.dead == nil {
		h.dead = err
	}
	q := h.queue
	h.queue = nil
	h.mu.Unlock()
	outcome := ConnError
	var ne net.Error
	switch {
	case errors.As(err, &ne) && ne.Timeout():
		outcome = Timeout
	case clean:
		outcome = NoResponse
	}
	for i, p := range q {
		<-h.slots
		o := outcome
		if i > 0 && !clean {
			// Only the first was being read; the ones behind it were never
			// answered.
			o = NoResponse
			if outcome == Timeout {
				o = Timeout
			}
		}
		p.done(o, false)
	}
}

// h2Conn is one HTTP/2 connection (net/http with a single connection per
// worker), with up to Streams requests in flight.
type h2Conn struct {
	o      Options
	client *http.Client
	tr     *http.Transport
	sem    chan struct{}
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	base   string
}

func newH2(o Options) *h2Conn {
	tr := &http.Transport{
		MaxConnsPerHost:     1,
		MaxIdleConnsPerHost: 1,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: o.Timeout,
		// Each transport gets its own copy: net/http writes NextProtos into
		// the one it is given, and the workers' transports run at once.
		TLSClientConfig:    o.TLS.Clone(),
		DisableCompression: true,
	}
	d := net.Dialer{Timeout: o.Timeout, KeepAlive: 30 * time.Second}
	tr.DialContext = func(ctx context.Context, network, a string) (net.Conn, error) {
		c, err := d.DialContext(ctx, network, a)
		o.OnDial(err)
		return c, err
	}
	p := new(http.Protocols)
	if o.URL.Scheme == "https" {
		p.SetHTTP2(true)
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	tr.Protocols = p
	ctx, cancel := context.WithCancel(context.Background())
	return &h2Conn{o: o, tr: tr, client: &http.Client{Transport: tr, Timeout: o.ResponseTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		sem: make(chan struct{}, o.Streams), ctx: ctx, cancel: cancel,
		base: o.URL.Scheme + "://" + addr(o.URL)}
}

// Send implements sched.Conn: the request runs in its own goroutine, at most
// Streams at a time.
func (h *h2Conn) Send(seq uint64, m *source.Message, done sched.DoneFunc) error {
	r := h.o.Build(seq, m)
	select {
	case h.sem <- struct{}{}:
	case <-h.ctx.Done():
		done(NotSent, false)
		return nil
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer func() { <-h.sem }()
		h.do(seq, &r, done)
	}()
	return nil
}

func (h *h2Conn) do(seq uint64, r *Req, done sched.DoneFunc) {
	for tries := 0; ; tries++ {
		req, err := http.NewRequestWithContext(h.ctx, r.Method, h.base+r.URI, bytes.NewReader(r.Body))
		if err != nil {
			done(BadResponse, false)
			return
		}
		req.Host = r.Host
		for _, kv := range r.Header {
			req.Header.Add(kv[0], kv[1])
		}
		resp, err := h.client.Do(req)
		if err != nil {
			var ne net.Error
			switch {
			case errors.As(err, &ne) && ne.Timeout():
				done(Timeout, false)
			case h.ctx.Err() != nil:
				done(NoResponse, false)
			default:
				done(ConnError, false)
			}
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if retryable(resp.StatusCode) && tries < h.o.Retries {
			t := time.NewTimer(retryWait(resp.Header.Get("Retry-After"), h.o.RetryMaxWait))
			select {
			case <-t.C:
				continue
			case <-h.ctx.Done():
				t.Stop()
				done(strconv.Itoa(resp.StatusCode), false)
				return
			}
		}
		if h.o.OnResponse != nil {
			h.o.OnResponse(seq, resp.StatusCode, resp.Header.Get("X-LogHub-UUID"))
		}
		done(strconv.Itoa(resp.StatusCode), resp.StatusCode >= 200 && resp.StatusCode < 300)
		return
	}
}

// Flush implements sched.Conn (each request goes at once).
func (h *h2Conn) Flush() error { return nil }

// Close implements sched.Conn: waits for the requests in flight until ctx
// ends, then cancels them.
func (h *h2Conn) Close(ctx context.Context) error {
	ch := make(chan struct{})
	go func() { h.wg.Wait(); close(ch) }()
	select {
	case <-ch:
	case <-ctx.Done():
		h.cancel()
		<-ch
	}
	h.cancel()
	h.tr.CloseIdleConnections()
	return nil
}
