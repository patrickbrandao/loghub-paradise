package receiver

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// backpressurePoll is how often a stalled reader re-checks the ring.
//
// It is a poll and not a notification on purpose: a notification would need the
// ring to know about its readers, and 50ms of extra latency at the exact moment
// the system is saturated is not a cost anyone can measure (SPEC §5.7).
const backpressurePoll = 50 * time.Millisecond

// defaultBackpressureTimeout is tcp.backpressure_timeout (SPEC §5.7, §16.5).
const defaultBackpressureTimeout = 30 * time.Second

// streamReadBuffer is the bufio.Reader size of one TCP or TLS connection. It is
// not a configuration key: a frame is read through it one refill at a time, so
// a bigger buffer buys nothing and a smaller one only costs syscalls.
const streamReadBuffer = 64 << 10

// proxyReadBuffer is the reader in front of a TLS handshake when the PROXY
// protocol is on. It only has to hold what a Peek of the header needs (at most
// the 107 bytes of v1); the TLS records behind it flow through in reads of
// their own size.
const proxyReadBuffer = 4 << 10

// keepAlivePeriod is the SO_KEEPALIVE period of SPEC §4.4.
const keepAlivePeriod = 30 * time.Second

// StreamConfig is what the two connection-oriented transports share. tcp and
// tls each have their own value, with their own limits and their own limiter
// (SPEC §4.4, §4.5).
type StreamConfig struct {
	// Addr is listen.tcp or listen.tls ("host:port"; ":1514" is every
	// address).
	Addr string
	// Listeners is listen.tcp_listeners / listen.tls_listeners; above 1 the
	// sockets share Addr through SO_REUSEPORT.
	Listeners int
	// IdleTimeout closes a connection with no traffic (120 s); zero never
	// does.
	IdleTimeout time.Duration
	// MaxConns and MaxConnsPerIP are per TRANSPORT, across its listeners;
	// zero is unlimited. The global cap blocks in accept, the per-IP cap
	// closes at once (SPEC §4.4).
	MaxConns      int
	MaxConnsPerIP int
	// BackpressureTimeout is how long a reader stays stopped on a full ring
	// or a DOWN sink before closing the connection (30 s, SPEC §5.7). Zero or
	// negative selects 30 s; the configuration refuses zero (SPEC §13.2).
	BackpressureTimeout time.Duration
	// Proxy is the [proxy] section, the same for tcp and tls.
	Proxy ProxyConfig
}

// Stream receives one connection-oriented transport: TCP (RFC 6587, SPEC §4.4)
// or TLS (RFC 5425, SPEC §4.5). Build it with NewTCP or NewTLS.
//
// It is the transport with real backpressure, and the one that should be the
// default for any emitter that cares about its data. A full ring does not drop
// anything here: the reader simply STOPS READING, the socket buffer fills, the
// TCP window closes, and the emitter stops transmitting. Its own queue is
// exactly where the data should sit while we cannot take it (SPEC §5.7).
//
// Per connection, in order (SPEC §2.1): the PROXY header when configured and
// the peer is trusted, the TLS handshake for tls, then frames until the
// connection ends.
type Stream struct {
	transport item.Transport
	obsT      obs.Transport
	cfg       StreamConfig
	framing   FramingMode

	tlsConf          *tls.Config // nil for tcp
	handshakeTimeout time.Duration

	in  *Ingest
	m   *obs.Metrics
	log *obs.Logger

	lns []net.Listener
	wg  sync.WaitGroup

	// conns tracks live connections so shutdown can close them and unblock
	// readers parked in Read.
	mu    sync.Mutex
	conns map[net.Conn]struct{}

	// closing is atomic because the read loop checks it on every frame without
	// taking mu. See armDeadline for why that check is needed at all.
	closing atomic.Bool

	// Sampled anomaly lines, one per 30 s per kind (SPEC §14.2): a broken
	// emitter or balancer must not be able to flood the collector's log.
	frameLog  *obs.Sampler
	proxyLog  *obs.Sampler
	tlsLog    *obs.Sampler
	bpLog     *obs.Sampler
	acceptLog *obs.Sampler
}

func newStream(ctx context.Context, t item.Transport, cfg StreamConfig, framing FramingMode,
	tlsConf *tls.Config, handshakeTimeout time.Duration,
	in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	if err := cfg.Proxy.Validate(); err != nil {
		return nil, err
	}
	if cfg.BackpressureTimeout <= 0 {
		cfg.BackpressureTimeout = defaultBackpressureTimeout
	}
	s := &Stream{
		transport: t, obsT: obsTransport(t), cfg: cfg, framing: framing,
		tlsConf: tlsConf, handshakeTimeout: handshakeTimeout,
		in: in, m: m, log: log,
		conns:    map[net.Conn]struct{}{},
		frameLog: obs.NewSampler(30 * time.Second),
		proxyLog: obs.NewSampler(30 * time.Second),
		tlsLog:   obs.NewSampler(30 * time.Second),
		bpLog:    obs.NewSampler(30 * time.Second),

		acceptLog: obs.NewSampler(30 * time.Second),
	}
	n := cfg.Listeners
	if n < 1 {
		n = 1
	}
	// One limiter for every listener of this transport: the ceilings are per
	// transport, not per listener (SPEC §4.4).
	limiter := newConnLimiter(cfg.MaxConns, cfg.MaxConnsPerIP, func(l obs.ConnLimit) {
		m.ConnsRefusedTotal[s.obsT][l].Inc()
	})
	for i := 0; i < n; i++ {
		l, err := listenStream(ctx, cfg.Addr, n > 1)
		if err != nil {
			s.closeListeners()
			return nil, fmt.Errorf("receiver %s: %s: %w", t, cfg.Addr, err)
		}
		s.lns = append(s.lns, limiter.wrap(l))
	}
	return s, nil
}

// Addr reports the bound address, for the boot log (SPEC §4.1).
func (s *Stream) Addr() string {
	if len(s.lns) == 0 {
		return s.cfg.Addr
	}
	return s.lns[0].Addr().String()
}

// Transport is tcp or tls.
func (s *Stream) Transport() item.Transport { return s.transport }

// Start launches one accept loop per listener. ctx is the RECEIVE context:
// cancelling it is what releases a reader held in the backpressure loop
// (SPEC §13.4), so it must be cancelled before Close is called.
func (s *Stream) Start(ctx context.Context) {
	for i := range s.lns {
		s.wg.Add(1)
		go func(l net.Listener) {
			defer s.wg.Done()
			s.acceptLoop(ctx, l)
		}(s.lns[i])
	}
}

func (s *Stream) acceptLoop(ctx context.Context, l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// A transient accept error (descriptor exhaustion, for instance)
			// must not end the loop: the listener is still good, and giving up
			// would take the transport down for the life of the process. The
			// pause keeps the loop from spinning; the line is SAMPLED, because
			// EMFILE under a connection flood persists, and one unsampled WARN
			// every 10 ms would flood the log for as long as it lasts (SPEC
			// §14.2).
			if ok, suppressed := s.acceptLog.Allow(); ok {
				s.log.Warn("accept failed", "transport", s.transport.String(), "error", err,
					"suppressed", suppressed)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !s.track(c) {
			c.Close()
			continue
		}
		s.m.ConnsTotal[s.obsT].Inc()
		s.m.ConnsActive[s.obsT].Inc()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.m.ConnsActive[s.obsT].Dec()
			defer s.untrack(c)
			s.serve(ctx, c)
		}()
	}
}

func (s *Stream) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing.Load() {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Stream) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	// The raw socket is closed, never the tls.Conn: this server never writes
	// application data, and a close_notify is one more write that a stalled
	// peer could make wait.
	c.Close()
}

// connAddrs is what serve knows about one connection's two ends.
type connAddrs struct {
	src, dst, peer netip.AddrPort
	proxied        bool
}

// serve reads one connection until it ends.
func (s *Stream) serve(ctx context.Context, c net.Conn) {
	// SO_KEEPALIVE, because a connection that died without a FIN -- an expired
	// NAT entry, a machine that vanished -- would otherwise hang around until the
	// idle timeout, holding a descriptor and a max_conns slot. An appliance that
	// logs once an hour is a normal case, so that timeout is generous.
	if tc, ok := tcpConnOf(c); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(keepAlivePeriod)
	}

	// The payload's two ends are the socket's (SPEC §4.4), unless a trusted
	// PROXY header says otherwise below.
	var a connAddrs
	pip, pport := addrPortOf(c.RemoteAddr())
	lip, lport := addrPortOf(c.LocalAddr())
	a.peer = netip.AddrPortFrom(pip, pport)
	a.src = a.peer
	a.dst = netip.AddrPortFrom(lip, lport)

	// --- PROXY: before anything else, the TLS handshake included -------------
	var br *bufio.Reader
	if s.cfg.Proxy.Mode != ProxyOff {
		size := streamReadBuffer
		if s.tlsConf != nil {
			size = proxyReadBuffer
		}
		br = bufio.NewReaderSize(c, size)
		if !s.readProxy(c, br, &a) {
			return
		}
	}

	// --- TLS ------------------------------------------------------------------
	var session *item.TLSInfo
	if s.tlsConf != nil {
		var raw net.Conn = c
		if br != nil {
			// The PROXY reader may hold the first bytes of the ClientHello.
			raw = &bufferedConn{Conn: c, r: br}
		}
		tc := tls.Server(raw, s.tlsConf)
		if !s.handshake(ctx, c, tc, a) {
			return
		}
		session = sessionInfo(tc.ConnectionState())
		br = bufio.NewReaderSize(tc, streamReadBuffer)
	} else if br == nil {
		br = bufio.NewReaderSize(c, streamReadBuffer)
	}

	fr := NewFramer(br, s.framing, s.in.MaxMessageBytes())
	// One Wire per connection, refilled per frame: the pipeline keeps a
	// pointer to it across retries, and a fresh one per frame would be a heap
	// allocation per message.
	var w Wire
	for {
		armDeadline(c, s.cfg.IdleTimeout, &s.closing)
		raw, size, framing, err := fr.Next()
		if err != nil {
			s.frameError(err, a)
			return
		}
		w = Wire{
			Transport: s.transport,
			Framing:   framing,
			SrcIP:     a.src.Addr(),
			SrcPort:   a.src.Port(),
			DstIP:     a.dst.Addr(),
			DstPort:   a.dst.Port(),
			TLS:       session,
			Size:      size,
			// The one clock sample: the last octet of the frame was just
			// read, and nothing has parsed it yet (SPEC §3.1).
			Time: time.Now(),
		}
		if a.proxied {
			w.PeerIP, w.PeerPort = a.peer.Addr(), a.peer.Port()
		}

		// This is the 429 of this protocol, and it is better than one: a 429
		// depends on the client honouring Retry-After, and most do not. A
		// closed TCP window is not optional.
		if !s.holdFor(ctx, raw, &w) {
			return
		}
	}
}

// readProxy applies proxy.protocol to one connection (SPEC §4.6). It returns
// false when the connection must be closed.
func (s *Stream) readProxy(c net.Conn, br *bufio.Reader, a *connAddrs) bool {
	px := s.cfg.Proxy
	if !px.Trusts(a.peer.Addr()) {
		if px.Mode == ProxyRequired {
			s.m.MessagesDroppedTotal[obs.DropProxy].Inc()
			s.warnProxy("refusing a connection from an untrusted peer: proxy.protocol=required", *a, nil)
			return false
		}
		// optional: nothing is read from an untrusted peer. A header it
		// sends anyway is the start of a malformed message, which is the
		// honest record of a forgery attempt.
		return true
	}

	armDeadline(c, px.headerTimeout(), &s.closing)
	hdr, found, err := ReadProxyHeader(br)
	switch {
	case errors.Is(err, ErrProxyHeader):
		s.m.ProxyHeadersTotal[obs.ProxyInvalid].Inc()
		s.m.MessagesDroppedTotal[obs.DropProxy].Inc()
		s.warnProxy("closing a connection with an invalid PROXY header", *a, err)
		return false
	case err != nil && !isTimeout(err):
		// The connection ended before its first byte: a balancer's TCP
		// health check, or a client that gave up. Nothing to count.
		return false
	case !found:
		// No header, or none within header_timeout.
		s.m.ProxyHeadersTotal[obs.ProxyAbsent].Inc()
		if px.Mode == ProxyRequired {
			s.m.MessagesDroppedTotal[obs.DropProxy].Inc()
			s.warnProxy("closing a connection without a PROXY header: proxy.protocol=required", *a, nil)
			return false
		}
		return true
	case hdr.Local:
		// LOCAL, UNSPEC, UNIX, DGRAM, UNKNOWN: the real peer stands, as if
		// there were no header.
		s.m.ProxyHeadersTotal[obs.ProxyLocal].Inc()
		return true
	}
	if hdr.Version == 1 {
		s.m.ProxyHeadersTotal[obs.ProxyV1].Inc()
	} else {
		s.m.ProxyHeadersTotal[obs.ProxyV2].Inc()
	}
	// The payload takes the client and the address it dialed; the real TCP
	// peer -- the balancer -- goes to x-proxy, so the actual end of the
	// transport stays on record (invariant §20.8). The connection limiter
	// already counted the balancer, which is what SPEC §4.4 says it sees.
	a.src, a.dst, a.proxied = hdr.Src, hdr.Dst, true
	return true
}

func (s *Stream) warnProxy(msg string, a connAddrs, err error) {
	if ok, suppressed := s.proxyLog.Allow(); ok {
		args := []any{"transport", s.transport.String(), "peer", a.peer.String(), "suppressed", suppressed}
		if err != nil {
			args = append(args, "error", err)
		}
		s.log.Warn(msg, args...)
	}
}

// handshake runs the TLS handshake under tls.handshake_timeout, which protects
// against a slowloris that opens connections and never finishes the
// handshake (SPEC §4.5).
func (s *Stream) handshake(ctx context.Context, c net.Conn, tc *tls.Conn, a connAddrs) bool {
	d := s.handshakeTimeout
	if d <= 0 {
		d = defaultHandshakeTimeout
	}
	// Read AND write deadline: the handshake writes too, and a peer that
	// stops reading would otherwise hold the ServerHello forever.
	_ = c.SetDeadline(time.Now().Add(d))
	if s.closing.Load() {
		_ = c.SetDeadline(time.Now())
	}
	err := tc.HandshakeContext(ctx)
	// The deadlines stay as they are: the frame loop re-arms BOTH at once
	// (armDeadline) before its first read.
	if err == nil {
		return true
	}
	if s.closing.Load() || ctx.Err() != nil {
		return false // shutdown, not the peer's fault
	}
	s.m.TLSHandshakeErrorsTotal.Inc()
	if ok, suppressed := s.tlsLog.Allow(); ok {
		s.log.Warn("TLS handshake failed", "peer", a.peer.String(),
			"error", err, "suppressed", suppressed)
	}
	return false
}

// holdFor runs Accept, and on a retryable refusal stops reading until the ring
// drains or the deadline passes (SPEC §5.7).
//
// It returns false when the connection must be closed.
//
// The deadline exists because an emitter blocked forever is worse than one that
// reconnects: it holds a descriptor, a kernel buffer and a max_conns slot
// indefinitely, and with a thousand emitters in that state the server cannot
// even accept the one that WOULD drain.
func (s *Stream) holdFor(ctx context.Context, raw []byte, w *Wire) bool {
	// From the frame being whole to the ring's guarantee being met -- the
	// confirmation this transport gives by reading on (SPEC §14.1).
	held := w.Time
	out := s.in.Accept(ctx, raw, w)
	if !out.Retryable() {
		// Accepted, filtered, rate-limited, empty, consumed (a final refusal
		// by the sink, already counted there): nothing to hold back. A
		// rate-limited emitter will not drain on its own, so stalling its
		// window would only move the queue without resolving anything.
		if out == Accepted {
			s.m.ConfirmDuration.ObserveDuration(time.Since(held))
		}
		s.in.RecordDrop(out)
		return out != Shutdown
	}

	start := time.Now()
	deadline := start.Add(s.cfg.BackpressureTimeout)
	timer := time.NewTimer(backpressurePoll)
	defer timer.Stop()

	for {
		// While this loop runs, nothing is read from the socket. The receive
		// buffer fills, the advertised window goes to zero, and the emitter
		// stops sending. Nothing is lost.
		select {
		case <-ctx.Done():
			s.m.AddBackpressure(s.obsT, time.Since(start))
			s.in.RecordDrop(Shutdown)
			return false
		case <-timer.C:
			timer.Reset(backpressurePoll)
		}

		// Retry does not count the arrival again: Accept already did.
		out = s.in.Retry(ctx, raw, w)
		if !out.Retryable() {
			s.m.AddBackpressure(s.obsT, time.Since(start))
			if out == Accepted {
				s.m.ConfirmDuration.ObserveDuration(time.Since(held))
			}
			s.in.RecordDrop(out)
			return out != Shutdown
		}
		if time.Now().After(deadline) {
			// The message in hand is counted as dropped; what the emitter
			// has not sent yet stays with it (SPEC §5.7).
			s.m.AddBackpressure(s.obsT, time.Since(start))
			s.in.RecordDrop(out)
			if ok, suppressed := s.bpLog.Allow(); ok {
				s.log.Warn("closing a connection after holding it at the backpressure "+
					"timeout; the emitter should reconnect and retry",
					"transport", s.transport.String(), "peer", w.SrcIP.String(),
					"held", time.Since(start).String(), "reason", out.String(),
					"suppressed", suppressed)
			}
			return false
		}
	}
}

func (s *Stream) frameError(err error, a connAddrs) {
	switch {
	case errors.Is(err, io.EOF):
		return // the emitter closed cleanly
	case errors.Is(err, ErrMalformedFrame):
		s.m.MessagesDroppedTotal[obs.DropMalformedFrame].Inc()
		if ok, suppressed := s.frameLog.Allow(); ok {
			s.log.Warn("closing a connection with a desynchronised frame; there is no "+
				"way to find the next message boundary",
				"transport", s.transport.String(), "peer", a.peer.String(),
				"error", err, "suppressed", suppressed)
		}
	case isTimeout(err):
		return // the idle timeout, or the shutdown's deadline: not an error
	default:
		s.log.Debug("connection ended", "transport", s.transport.String(),
			"peer", a.peer.String(), "error", err)
	}
}

// closeGrace is how long Close waits, after expiring every deadline, before
// it closes the sockets of readers that still have not returned.
const closeGrace = time.Second

// Close stops accepting and stops reading, and waits for every connection's
// reader to return.
//
// An expired deadline breaks a reader out of Read (or out of a PROXY read or a
// handshake); the message it was already handing to the ring still gets its
// Accept. A reader held in the backpressure loop is released by the cancelled
// receive context, which the app cancels BEFORE calling Close (SPEC §13.4).
// Neither transport has an answer to send, so the socket is closed by serve on
// its way out -- after the last message it read has either entered the ring or
// been counted.
//
// BOTH deadlines expire, not only the read one. A syslog receiver never writes
// application data, but crypto/tls does write on its own inside Read: a TLS
// 1.3 peer that sends KeyUpdate(update_requested) and never reads parks the
// reader in the write of the answer, where a read deadline cannot reach it.
// Should a reader STILL not return within closeGrace, its socket is closed
// under it: a shutdown must not wait on a peer.
func (s *Stream) Close() error {
	s.mu.Lock()
	s.closing.Store(true)
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	err := s.closeListeners()
	for _, c := range conns {
		_ = c.SetDeadline(time.Now())
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeGrace):
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		<-done
	}
	return err
}

func (s *Stream) closeListeners() error {
	var errs []error
	for _, l := range s.lns {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// armDeadline sets the read AND write deadlines for the next frame, without
// undoing a shutdown.
//
// Both, because a read is not the only thing a reader can block in: crypto/tls
// answers a TLS 1.3 KeyUpdate(update_requested) with a write from inside Read,
// and a peer that requests updates and never reads would park the reader in
// that write with no deadline at all, beyond the reach of idle_timeout and of
// a shutdown. On plain TCP the write deadline is inert: nothing is written.
//
// Close stops a reader by setting its deadlines to "now". A reader that is
// between two frames at that instant would otherwise renew them for another
// idle_timeout before its next read, and shutdown would sit waiting for up to
// two minutes. So the reader sets its deadline FIRST and checks closing AFTER;
// Close sets closing FIRST and the deadline AFTER. In every interleaving the
// last deadline written is "now", which is the one property that matters.
//
// A zero d clears the deadlines: idle_timeout = 0 means never (SPEC §16.5).
func armDeadline(c net.Conn, d time.Duration, closing *atomic.Bool) {
	if d > 0 {
		_ = c.SetDeadline(time.Now().Add(d))
	} else {
		_ = c.SetDeadline(time.Time{})
	}
	if closing.Load() {
		_ = c.SetDeadline(time.Now())
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// bufferedConn is a connection whose first bytes were already pulled into a
// bufio.Reader -- by the PROXY reader -- and must be read from there before
// the socket. Deadlines, addresses and Close go to the socket.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.r.Read(p) }
