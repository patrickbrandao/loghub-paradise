package receiver

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
)

// GELF over HTTP (SPEC §4.5): POST /gelf, one document per request, or one
// per line with http.bulk. There is no HTTPS (SPEC §1.2).
//
// The answer is the confirmation, and 4xx is FINAL for the client: only what
// will never pass gets one. Everything that may pass on a new attempt -- a full
// ring, a DOWN sink, a shutdown -- is 503 with Retry-After.
//
// HTTP/1.1 with keep-alive (and the stdlib's sequential pipelining), never
// HTTP/2: a connection carries many requests, bounded by http.idle_timeout
// between them and, optionally, by http.max_requests_per_conn. Opening a TCP
// connection per document cost a handshake and a TIME_WAIT per event
// (2026-10-31: 20 000 requests opened 20 000 connections).

// Defaults of [http] (SPEC §16.5).
const (
	DefaultHTTPReadHeaderTimeout = 10 * time.Second
	DefaultHTTPReadTimeout       = 30 * time.Second
	DefaultHTTPIdleTimeout       = 60 * time.Second
	DefaultHTTPBulkMaxBytes      = 4 << 20
	httpWriteTimeout             = 10 * time.Second
	httpMaxHeaderBytes           = 16 << 10
)

// GELFPath is the only path served.
const GELFPath = "/gelf"

// HTTPConfig is [listen] http and the [http] section, plus [proxy].
type HTTPConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	// IdleTimeout is http.idle_timeout: how long a keep-alive connection
	// may wait for its next request. Zero selects the default.
	IdleTimeout time.Duration
	// MaxRequestsPerConn is http.max_requests_per_conn: the answer to the
	// Nth request of a connection carries Connection: close. 0 = no cap.
	MaxRequestsPerConn int
	MaxConns           int
	MaxConnsPerIP      int
	// Bulk is http.bulk: the body is split on \n, one document per line
	// (SPEC §4.5.1). BulkMaxBytes is http.bulk_max_bytes, the ceiling of the
	// whole decompressed body in that mode; zero selects the default.
	Bulk         bool
	BulkMaxBytes int
	Proxy        ProxyConfig
}

// HTTP is one GELF HTTP listener.
type HTTP struct {
	transport item.Transport
	obsT      obs.Transport
	in        *Ingest
	m         *obs.Metrics
	log       *obs.Logger
	cfg       HTTPConfig

	ln  net.Listener
	srv *http.Server

	// ctx is the receive context Start was given: cancelled before Close,
	// it turns every waiting Accept into a Shutdown (503).
	ctx     context.Context
	closing atomic.Bool
	done    chan struct{}

	proxyLog *obs.Sampler
	errLog   *obs.Sampler

	bufs sync.Pool // *[]byte for bodies
	docs sync.Pool // *[]byte for decompressed documents
}

// NewHTTP opens the HTTP listener.
func NewHTTP(ctx context.Context, cfg HTTPConfig, in *Ingest, m *obs.Metrics, lg *obs.Logger) (*HTTP, error) {
	return newHTTP(ctx, item.TransportHTTP, cfg, in, m, lg)
}

func newHTTP(ctx context.Context, t item.Transport, cfg HTTPConfig,
	in *Ingest, m *obs.Metrics, lg *obs.Logger) (*HTTP, error) {
	if err := cfg.Proxy.Validate(); err != nil {
		return nil, err
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = DefaultHTTPReadHeaderTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = DefaultHTTPReadTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultHTTPIdleTimeout
	}
	if cfg.BulkMaxBytes <= 0 {
		cfg.BulkMaxBytes = DefaultHTTPBulkMaxBytes
	}
	h := &HTTP{transport: t, obsT: obsTransport(t), in: in, m: m, log: lg, cfg: cfg,
		done: make(chan struct{}), proxyLog: obs.NewSampler(30 * time.Second),
		errLog: obs.NewSampler(30 * time.Second)}
	l, err := listenStream(ctx, cfg.Addr, false)
	if err != nil {
		return nil, fmt.Errorf("receiver %s: %s: %w", t, cfg.Addr, err)
	}
	limiter := newConnLimiter(cfg.MaxConns, cfg.MaxConnsPerIP, func(lim obs.ConnLimit) {
		m.ConnsRefusedTotal[h.obsT][lim].Inc()
	})
	h.ln = &proxyListener{Listener: limiter.wrap(l), h: h}
	h.srv = &http.Server{
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		// net/http arms the write deadline when the headers end; ServeHTTP
		// clears it and armWrite puts it back before the answer, so it
		// counts from the answer (SPEC §4.5).
		WriteTimeout:   httpWriteTimeout,
		IdleTimeout:    cfg.IdleTimeout,
		MaxHeaderBytes: httpMaxHeaderBytes,
		// No TLSNextProto: there is no TLS, so no "h2" can be negotiated, and
		// cleartext HTTP/2 (h2c) is off unless Protocols asks for it.
		ConnState: h.connState,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, connKey{}, c)
		},
		// The stdlib logs a malformed request per connection; a scanner on
		// the port would flood the log. Discarded.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	return h, nil
}

type connKey struct{}

// Addr reports the bound address, for the boot log.
func (h *HTTP) Addr() string { return h.ln.Addr().String() }

// Transport is http.
func (h *HTTP) Transport() item.Transport { return h.transport }

// Start serves. ctx is the receive context (SPEC §13.4).
func (h *HTTP) Start(ctx context.Context) {
	h.ctx = ctx
	go func() {
		defer close(h.done)
		err := h.srv.Serve(h.ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !h.closing.Load() {
			h.log.Error("http serve ended", "transport", h.transport.String(), "error", err)
		}
	}()
}

// Close stops accepting, lets the requests in flight finish for up to
// closeGrace (their Accept already sees the cancelled receive context and
// answers 503), then closes what is left.
func (h *HTTP) Close() error {
	h.closing.Store(true)
	// Every answer from here on carries Connection: close, so a keep-alive
	// connection ends after its request in flight instead of being severed
	// in the middle of one (Shutdown closes only the idle ones).
	h.srv.SetKeepAlivesEnabled(false)
	ctx, cancel := context.WithTimeout(context.Background(), closeGrace)
	defer cancel()
	err := h.srv.Shutdown(ctx)
	if err != nil {
		err = h.srv.Close()
	}
	if h.ctx != nil {
		<-h.done
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}

func (h *HTTP) connState(c net.Conn, st http.ConnState) {
	switch st {
	case http.StateNew:
		h.m.ConnsTotal[h.obsT].Inc()
		h.m.ConnsActive[h.obsT].Inc()
	case http.StateClosed, http.StateHijacked:
		h.m.ConnsActive[h.obsT].Dec()
	}
}

// ServeHTTP is the GELF HTTP input.
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The write deadline counts from the answer, not from the end of the
	// headers where net/http armed it (SPEC §4.5). Otherwise a body slower
	// than the write timeout -- legitimate within read_timeout -- or a ring
	// that takes its time (ring=none, sqlite fsync) entered the ring and then
	// lost its 202, and the client sent the event again under another uuid.
	// The body stays bounded by read_timeout; armWrite re-arms before every
	// answer (the fix of go-webhook-server, 2026-10-14).
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	pc := h.conn(r)
	if n := h.cfg.MaxRequestsPerConn; n > 0 && pc != nil && pc.reqs.Add(1) >= int64(n) {
		// The cap of http.max_requests_per_conn: this answer ends the
		// connection, whatever it says.
		w.Header().Set("Connection", "close")
	}

	if r.URL.Path != GELFPath {
		h.fail(w, rc, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.fail(w, rc, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	comp, declared, ok := contentEncoding(r.Header.Get("Content-Encoding"))
	if !ok {
		h.in.Reject(h.transport, DecompressFailed)
		h.fail(w, rc, http.StatusUnsupportedMediaType, "unsupported Content-Encoding")
		return
	}
	maxDoc := h.in.MaxMessageBytes()
	bodyMax := maxDoc // the decompressed body
	if h.cfg.Bulk {
		bodyMax = max(maxDoc, h.cfg.BulkMaxBytes)
	}
	limit := WireLimit(bodyMax)
	if r.ContentLength > int64(limit) {
		// The ONLY use of the declared length: an early refusal. The body is
		// never sized from it (SPEC §4.5).
		h.in.Reject(h.transport, TooLarge)
		h.fail(w, rc, http.StatusRequestEntityTooLarge, "document too large")
		return
	}

	bp, _ := h.bufs.Get().(*[]byte)
	if bp == nil {
		bp = new([]byte)
	}
	defer putBuf(&h.bufs, bp)
	// The buffer grows with what ARRIVES, never with Content-Length, by
	// classes and never past limit+1 (SPEC §4.5).
	body, err := readLimited((*bp)[:0], r.Body, limit+1)
	*bp = body[:0]
	if err != nil {
		// A client that stalled past read_timeout or went away: there is no
		// document, and nobody may be listening for the answer.
		h.fail(w, rc, http.StatusBadRequest, "could not read the body")
		return
	}
	now := time.Now() // the one clock sample: the body is whole (SPEC §3.1)
	if len(body) > limit {
		h.in.Reject(h.transport, TooLarge)
		h.fail(w, rc, http.StatusRequestEntityTooLarge, "document too large")
		return
	}
	if !declared {
		comp = Detect(body)
	} else if comp == item.CompressionDeflate && isZlibHeader(body) {
		// "deflate" in HTTP is the zlib format (RFC 9110 §8.4.1.2); a raw
		// deflate stream is the common mistake, and both are read.
		comp = item.CompressionZlib
	}

	dp, _ := h.docs.Get().(*[]byte)
	if dp == nil {
		dp = new([]byte)
	}
	defer putBuf(&h.docs, dp)
	doc, err := Decompress((*dp)[:0], body, comp, bodyMax)
	*dp = doc[:0]
	if err != nil {
		h.in.Reject(h.transport, decompressOutcome(err))
		if errors.Is(err, errTooLarge) {
			h.fail(w, rc, http.StatusRequestEntityTooLarge, "document too large")
		} else {
			h.fail(w, rc, http.StatusBadRequest, "the body does not decompress")
		}
		return
	}

	a := h.addrs(r)
	wire := Wire{
		Transport:   h.transport,
		Compression: comp,
		SrcIP:       a.src.Addr(),
		SrcPort:     a.src.Port(),
		DstIP:       a.dst.Addr(),
		DstPort:     a.dst.Port(),
		Time:        now,
	}
	if a.proxied {
		wire.PeerIP, wire.PeerPort = a.peer.Addr(), a.peer.Port()
	}

	ctx := h.ctx
	if ctx == nil {
		ctx = r.Context()
	}
	var out Outcome
	if h.cfg.Bulk {
		out = h.acceptBulk(ctx, doc, &wire, maxDoc)
	} else {
		out = h.in.Accept(ctx, doc, &wire)
		h.in.RecordDrop(out)
	}
	h.answer(w, rc, out, now)
}

// acceptBulk is http.bulk (SPEC §4.5.1): one document per line of the body,
// split on \n, a line that is empty or only JSON whitespace skipped. Every
// line is its own document with its own uuid, all from the request's ONE clock
// sample (the uuid generator orders the ties). It is all or nothing for size
// only: a line above convert.max_message_bytes refuses the whole body (413)
// before anything is queued. Past that check the lines go in one by one, and
// the first one refused for a reason that may pass later stops the request:
// the lines before it STAY in the ring and the answer is 503, so a client that
// resends the body duplicates them (documented; the alternative would be to
// take back items the ring already guaranteed).
func (h *HTTP) acceptBulk(ctx context.Context, body []byte, base *Wire, maxDoc int) Outcome {
	lines := 0
	for rest := body; len(rest) > 0; {
		line, tail := cutLine(rest)
		rest = tail
		if blank(line) {
			continue
		}
		if len(line) > maxDoc {
			// One arrival and one drop for the request: none of its lines
			// was looked at as a document.
			h.in.Reject(h.transport, TooLarge)
			return TooLarge
		}
		lines++
	}
	if lines == 0 {
		out := h.in.Accept(ctx, nil, base) // counts the arrival as empty
		h.in.RecordDrop(out)
		return out
	}
	for rest := body; len(rest) > 0; {
		line, tail := cutLine(rest)
		rest = tail
		if blank(line) {
			continue
		}
		w := *base // a fresh uuid and a fresh admission per line
		out := h.in.Accept(ctx, line, &w)
		h.in.RecordDrop(out)
		if out != Accepted {
			return out
		}
	}
	return Accepted
}

// cutLine splits b at the first \n. A \r before it is JSON whitespace and is
// cut too, so a CRLF body gives the same documents as an LF one.
func cutLine(b []byte) (line, rest []byte) {
	i := bytes.IndexByte(b, '\n')
	if i < 0 {
		line = b
	} else {
		line, rest = b[:i], b[i+1:]
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, rest
}

// answer turns the pipeline's outcome into the HTTP status (SPEC §4.5).
func (h *HTTP) answer(w http.ResponseWriter, rc *http.ResponseController, out Outcome, now time.Time) {
	switch out {
	case Accepted:
		h.m.ConfirmDuration.ObserveDuration(time.Since(now))
		armWrite(rc)
		w.WriteHeader(http.StatusAccepted)
	case Empty, Unaddressable:
		h.fail(w, rc, http.StatusBadRequest, "empty document")
	case TooLarge:
		h.fail(w, rc, http.StatusRequestEntityTooLarge, "document too large")
	default:
		// ring_full, sink_down, shutdown: all may pass later, so none is
		// final (SPEC §4.5). Only the shutdown
		// ends the connection: a full ring is a reason to wait one second,
		// not to pay a new handshake.
		w.Header().Set("Retry-After", "1")
		if out == Shutdown || h.closing.Load() {
			w.Header().Set("Connection", "close")
		}
		h.fail(w, rc, http.StatusServiceUnavailable, "unavailable: "+out.String())
	}
}

// fail writes an error answer with its write deadline armed.
func (h *HTTP) fail(w http.ResponseWriter, rc *http.ResponseController, code int, msg string) {
	armWrite(rc)
	http.Error(w, msg, code)
}

// armWrite starts the write timeout for the answer about to be written: a
// client that stops reading still cannot hold the connection past it.
func armWrite(rc *http.ResponseController) {
	_ = rc.SetWriteDeadline(time.Now().Add(httpWriteTimeout))
}

// readLimited reads r into dst until EOF or until dst holds limit bytes,
// growing it by the frame classes (then doubling) and never past limit. A
// bytes.Buffer doubles, and its last step took a body near the ceiling to
// twice the ceiling (2026-10-31 review).
func readLimited(dst []byte, r io.Reader, limit int) ([]byte, error) {
	for len(dst) < limit {
		dst = growOne(dst, limit)
		n, err := r.Read(dst[len(dst):min(cap(dst), limit)])
		dst = dst[:len(dst)+n]
		if err == io.EOF {
			return dst, nil
		}
		if err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// putBuf returns a body or document buffer to its pool. One near the ceiling
// is not kept for the next request: the pool would hold max_message_bytes per
// idle P for nothing.
func putBuf(p *sync.Pool, b *[]byte) {
	if cap(*b) <= 64<<10 {
		p.Put(b)
	}
}

// contentEncoding reads Content-Encoding: comp is the container, declared
// whether one was named (absent and identity mean "detect by the bytes").
func contentEncoding(v string) (comp item.Compression, declared, ok bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "identity":
		return item.CompressionNone, false, true
	case "gzip", "x-gzip":
		return item.CompressionGzip, true, true
	case "deflate":
		return item.CompressionDeflate, true, true
	}
	return 0, false, false
}

// addrs are the payload's two ends for one request: the socket's, or what a
// trusted PROXY header declared. X-Forwarded-For is never read (SPEC §4.5).
func (h *HTTP) addrs(r *http.Request) connAddrs {
	if pc := h.conn(r); pc != nil {
		return pc.a
	}
	c, _ := r.Context().Value(connKey{}).(net.Conn)
	var a connAddrs
	if c != nil {
		pip, pport := addrPortOf(c.RemoteAddr())
		lip, lport := addrPortOf(c.LocalAddr())
		a.peer = netip.AddrPortFrom(pip, pport)
		a.src = a.peer
		a.dst = netip.AddrPortFrom(lip, lport)
	}
	return a
}

// conn is the request's connection as this listener wrapped it; nil only for a handler called outside a server (tests).
func (h *HTTP) conn(r *http.Request) *proxyConn {
	c, _ := r.Context().Value(connKey{}).(net.Conn)
	pc, _ := c.(*proxyConn)
	return pc
}

func (h *HTTP) warnProxy(msg string, a connAddrs, err error) {
	if ok, suppressed := h.proxyLog.Allow(); ok {
		args := []any{"transport", h.transport.String(), "peer", a.peer.String(), "suppressed", suppressed}
		if err != nil {
			args = append(args, "error", err)
		}
		h.log.Warn(msg, args...)
	}
}

// proxyListener wraps every accepted connection in a proxyConn. Nothing is
// read here: Accept runs on the server's single accept goroutine, and a peer
// that never sends its header would stall every other client behind it.
type proxyListener struct {
	net.Listener
	h *HTTP
}

func (l *proxyListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// SO_KEEPALIVE as on the stream transports (SPEC §4.4).
	if tc, ok := tcpConnOf(c); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(keepAlivePeriod)
	}
	pc := &proxyConn{Conn: c, h: l.h}
	pip, pport := addrPortOf(c.RemoteAddr())
	lip, lport := addrPortOf(c.LocalAddr())
	pc.a.peer = netip.AddrPortFrom(pip, pport)
	pc.a.src = pc.a.peer
	pc.a.dst = netip.AddrPortFrom(lip, lport)
	return pc, nil
}

// proxyConn reads the PROXY header (SPEC §4.6) on its first Read, inside the
// connection's own goroutine, before HTTP. It keeps the
// deadline the HTTP server asked for: the header has its own
// (proxy.header_timeout), and the server's is restored after it.
type proxyConn struct {
	net.Conn
	h *HTTP

	once sync.Once
	br   *bufio.Reader
	err  error
	a    connAddrs

	mu       sync.Mutex
	deadline time.Time

	// reqs counts the requests served, for http.max_requests_per_conn.
	reqs atomic.Int64
}

func (c *proxyConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

func (c *proxyConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

func (c *proxyConn) Read(p []byte) (int, error) {
	c.once.Do(c.init)
	if c.err != nil {
		return 0, c.err
	}
	if c.br != nil {
		return c.br.Read(p)
	}
	return c.Conn.Read(p)
}

var errProxyRefused = errors.New("receiver: connection refused by proxy.protocol")

func (c *proxyConn) init() {
	px := c.h.cfg.Proxy
	if px.Mode == ProxyOff {
		return
	}
	c.br = bufio.NewReaderSize(c.Conn, proxyReadBuffer)
	if !applyProxy(c.Conn, c.br, &c.a, px, &c.h.closing, c.h.m, c.h.warnProxy) {
		// A read error net/http counts as a network failure (an *OpError on
		// "read"), so the server closes WITHOUT an answer, as SPEC §4.6
		// requires. Any other error made it write "400 Bad Request" to a
		// connection the PROXY protocol refused.
		c.err = &net.OpError{Op: "read", Net: "tcp", Source: c.Conn.LocalAddr(),
			Addr: c.Conn.RemoteAddr(), Err: errProxyRefused}
		return
	}
	// applyProxy armed the header deadline; give the server its own back.
	c.mu.Lock()
	d := c.deadline
	c.mu.Unlock()
	_ = c.Conn.SetDeadline(d)
}
