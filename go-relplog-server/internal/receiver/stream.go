package receiver

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// backpressurePoll is how often a stalled reader re-checks the ring.
//
// It is a poll and not a notification on purpose: a notification would need the
// ring to know about its readers, and 50ms of extra latency at the exact moment
// the system is saturated is not a cost anyone can measure (SPEC §5.7).
const backpressurePoll = 50 * time.Millisecond

// proxyReadBuffer is the reader in front of a TLS handshake when the PROXY
// protocol is on. It only has to hold what a Peek of the header needs (at most
// the 107 bytes of v1); the TLS records behind it flow through in reads of
// their own size. Without TLS the same reader carries the RELP frames.
const proxyReadBuffer = relpReadBuffer

// keepAlivePeriod is the SO_KEEPALIVE period of SPEC §4.4.
const keepAlivePeriod = 30 * time.Second

// StreamConfig is what the two transports share: RELP in clear (tcp) and RELP
// over TLS (tls). Each has its own value, with its own limits and its own
// limiter (SPEC §4.4, §4.5); the RELP half is the same for both.
type StreamConfig struct {
	// Addr is listen.tcp or listen.tls ("host:port"; ":20514" is every
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
	// Proxy is the [proxy] section, the same for tcp and tls.
	Proxy ProxyConfig
	// RELP is the [relp] section, the same for tcp and tls.
	RELP RELPConfig
}

// Stream receives one transport: RELP over TCP (SPEC §4.4) or RELP over TLS
// (SPEC §4.5). Build it with NewTCP or NewTLS.
//
// Per connection, in order (SPEC §2.1): the PROXY header when configured and
// the peer is trusted, the TLS handshake for tls, then one RELP session
// (relp.go) until the connection ends. The session is where the
// acknowledgement lives: "200 OK" only after the ring's guarantee, and a full
// ring held -- never refused -- until the emitter can be handed its message
// back by a closed session (SPEC §5.4, §5.7).
type Stream struct {
	transport item.Transport
	obsT      obs.Transport
	cfg       StreamConfig
	relp      RELPConfig
	budget    *Budget
	openRsp   string

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

func newStream(ctx context.Context, t item.Transport, cfg StreamConfig,
	tlsConf *tls.Config, handshakeTimeout time.Duration,
	in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	if err := cfg.Proxy.Validate(); err != nil {
		return nil, err
	}
	rc := cfg.RELP
	if rc.Window <= 0 {
		rc.Window = defaultWindow
	}
	if rc.MaxFrameBytes <= 0 {
		rc.MaxFrameBytes = defaultMaxFrameBytes
	}
	if rc.BackpressureTimeout <= 0 {
		rc.BackpressureTimeout = defaultBackpressureTimeout
	}
	if int64(in.MaxMessageBytes()) > rc.MaxFrameBytes {
		// A message the truncation would keep must fit the frame fuse, or
		// every long line would close the connection instead of being cut.
		return nil, fmt.Errorf("receiver %s: relp.max_frame_bytes %d is below syslog.max_message_bytes %d",
			t, rc.MaxFrameBytes, in.MaxMessageBytes())
	}
	budget := rc.Budget
	if budget == nil {
		budget = NewBudget(0, m)
	}
	if budget.Max() < int64(in.MaxMessageBytes()) {
		return nil, fmt.Errorf("receiver %s: relp.inflight_max_bytes %d is below syslog.max_message_bytes %d",
			t, budget.Max(), in.MaxMessageBytes())
	}
	s := &Stream{
		transport: t, obsT: obsTransport(t), cfg: cfg, relp: rc, budget: budget,
		openRsp: openResponse(rc.Software),
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
// cancelling it is what releases the transactions held in backpressure (SPEC
// §13.4), so it must be cancelled before Close is called.
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
	// The raw socket is closed, never the tls.Conn: every answer was already
	// flushed by the session's writer, and a close_notify is one more write
	// that a stalled peer could make wait.
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
		br = bufio.NewReaderSize(c, proxyReadBuffer)
		if !s.readProxy(c, br, &a) {
			return
		}
	}

	// --- TLS ------------------------------------------------------------------
	var session *item.TLSInfo
	var rw net.Conn = c
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
		rw = &tlsConn{Conn: tc, raw: c}
		br = bufio.NewReaderSize(tc, relpReadBuffer)
	} else if br == nil {
		br = bufio.NewReaderSize(c, relpReadBuffer)
	}

	// The handshake left both deadlines armed; the session owns them now:
	// the reader the read side, the writer the write side. Until the first
	// answer is written the write side is the idle timeout: crypto/tls may
	// write from inside Read (a KeyUpdate answer) before the writer ever has,
	// and that write must be bounded (SPEC §4.5).
	if d := s.cfg.IdleTimeout; d > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(d))
	} else {
		_ = c.SetWriteDeadline(time.Time{})
	}
	s.runRELP(ctx, rw, br, a, session)
}

// tlsConn writes through the TLS connection and sets deadlines on the raw
// socket, which is where crypto/tls takes them from anyway; Close closes the
// raw socket (see untrack).
type tlsConn struct {
	*tls.Conn
	raw net.Conn
}

func (t *tlsConn) Close() error                       { return t.raw.Close() }
func (t *tlsConn) SetDeadline(d time.Time) error      { return t.raw.SetDeadline(d) }
func (t *tlsConn) SetReadDeadline(d time.Time) error  { return t.raw.SetReadDeadline(d) }
func (t *tlsConn) SetWriteDeadline(d time.Time) error { return t.raw.SetWriteDeadline(d) }

// readProxy applies proxy.protocol to one connection (SPEC §4.6). It returns
// false when the connection must be closed.
func (s *Stream) readProxy(c net.Conn, br *bufio.Reader, a *connAddrs) bool {
	px := s.cfg.Proxy
	if !px.Trusts(a.peer.Addr()) {
		if px.Mode == ProxyRequired {
			s.m.ConnsRefusedTotal[s.obsT][obs.ConnRefusedProxy].Inc()
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
	if (err != nil || !found) && s.closing.Load() {
		// The shutdown expired the deadline of a connection still waiting for
		// its header (or halfway through one). That is a deploy, not the
		// balancer's fault: nothing is counted and nothing is logged, as the
		// TLS handshake already does (S §22.1 item 3).
		return false
	}
	switch {
	case errors.Is(err, ErrProxyHeader):
		s.m.ProxyHeadersTotal[obs.ProxyInvalid].Inc()
		s.m.ConnsRefusedTotal[s.obsT][obs.ConnRefusedProxy].Inc()
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
			s.m.ConnsRefusedTotal[s.obsT][obs.ConnRefusedProxy].Inc()
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
	// The deadlines stay as they are: the session re-arms the read side
	// before its first frame and serve resets the write side.
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

// closeGrace is how long Close waits, after expiring every read deadline,
// before it closes the sockets of sessions that still have not returned. It
// covers one write of the session's answers (relpWriteTimeout) with room to
// spare: the decided answers are what the wait is for.
const closeGrace = relpWriteTimeout + time.Second

// Close stops accepting and stops READING, and waits for every session to
// finish what it already decided (SPEC §5.8, §13.4).
//
// It does not close the sockets itself. An expired read deadline breaks each
// reader out of ReadFrame (or out of a PROXY read or a handshake); the session
// then waits for its in-flight transactions, lets the writer flush their
// answers and the serverclose hint, and only then closes the connection.
// Closing the socket here instead would discard "200 OK" frames for messages
// already in the ring, and each of those becomes a duplicate event when
// rsyslog retransmits (the bug of the reference, SPEC §5.9).
//
// The transactions held in backpressure are released by the cancelled receive
// context, which the app cancels BEFORE calling Close: they get no answer, and
// librelp retransmits them on its next session.
//
// Only the read deadline expires: the writer must still be able to write. A
// session that STILL has not returned within closeGrace -- a peer that stopped
// reading, a reader parked in the write of a TLS KeyUpdate answer -- has its
// socket closed under it: a shutdown must not wait on a peer.
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
		_ = c.SetReadDeadline(time.Now())
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

// armDeadline sets the read AND write deadlines of the PROXY header wait,
// without undoing a shutdown. Before the RELP session starts nothing else owns
// the write side, so both are set; the session then splits them between its
// reader and its writer (armReadDeadline, relpSession.idleWriteDeadline).
//
// Close stops a reader by setting its read deadline to "now". A reader that
// arms its own deadline at that instant would otherwise renew it, and shutdown
// would sit waiting. So the reader sets its deadline FIRST and checks closing
// AFTER; Close sets closing FIRST and the deadline AFTER. In every
// interleaving the last deadline written is "now".
//
// A zero d clears the deadlines.
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
