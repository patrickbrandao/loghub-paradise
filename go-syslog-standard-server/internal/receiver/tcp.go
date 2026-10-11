package receiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// backpressurePoll is how often a stalled reader re-checks the ring.
//
// It is a poll and not a notification on purpose: a notification would need the
// ring to know about its readers, and 50ms of extra latency at the exact moment
// the system is saturated is not a cost anyone can measure.
const backpressurePoll = 50 * time.Millisecond

// TCPConfig mirrors [listen] and [tcp].
type TCPConfig struct {
	Addr                string
	Listeners           int
	Framing             envelope.Framing
	FramingAuto         bool
	MaxFrameBytes       int64
	IdleTimeout         time.Duration
	MaxConns            int
	MaxConnsPerIP       int
	BackpressureTimeout time.Duration
}

// tcpReadBuffer is the bufio.Reader size of one TCP or RELP connection. It is
// not a configuration key: a frame is read through it in chunks of the same
// size (octetChunk), so a bigger buffer buys nothing and a smaller one only
// costs syscalls.
const tcpReadBuffer = 64 << 10

// TCP receives RFC 6587 streams (SPEC §4.4).
//
// It is the transport with real backpressure, and the one that should be the
// default for any emitter that cares about its data. A full ring does not drop
// anything here: the reader simply STOPS READING, the socket buffer fills, the
// TCP window closes, and the emitter stops transmitting. Its own queue is
// exactly where the data should sit while we cannot take it.
type TCP struct {
	cfg TCPConfig
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
}

// NewTCP opens the listeners.
func NewTCP(ctx context.Context, cfg TCPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*TCP, error) {
	t := &TCP{cfg: cfg, in: in, m: m, log: log, conns: map[net.Conn]struct{}{}}
	n := cfg.Listeners
	if n < 1 {
		n = 1
	}
	// One limiter for every listener of this transport: the ceilings are per
	// transport, not per listener (SPEC §4.4).
	limiter := newConnLimiter(cfg.MaxConns, cfg.MaxConnsPerIP)
	for i := 0; i < n; i++ {
		l, err := listenStream(ctx, cfg.Addr, n > 1)
		if err != nil {
			t.closeListeners()
			return nil, fmt.Errorf("receiver tcp: %s: %w", cfg.Addr, err)
		}
		t.lns = append(t.lns, limiter.wrap(l))
	}
	return t, nil
}

// Addr reports the bound address.
func (t *TCP) Addr() string {
	if len(t.lns) == 0 {
		return t.cfg.Addr
	}
	return t.lns[0].Addr().String()
}

// Start launches one accept loop per listener.
func (t *TCP) Start(ctx context.Context) {
	listener := t.Addr()
	for i := range t.lns {
		t.wg.Add(1)
		go func(l net.Listener) {
			defer t.wg.Done()
			t.acceptLoop(ctx, l, listener)
		}(t.lns[i])
	}
}

func (t *TCP) acceptLoop(ctx context.Context, l net.Listener, listener string) {
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// A transient accept error (descriptor exhaustion, for instance)
			// must not end the loop: the listener is still good, and giving up
			// would take the transport down for the life of the process.
			t.log.Warn("accept failed", "transport", "tcp", "error", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !t.track(c) {
			c.Close()
			continue
		}
		t.m.ConnsTotal[obs.TransportTCP].Inc()
		t.m.ConnsActive[obs.TransportTCP].Inc()

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.m.ConnsActive[obs.TransportTCP].Dec()
			defer t.untrack(c)
			t.serve(ctx, c, listener)
		}()
	}
}

func (t *TCP) track(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing.Load() {
		return false
	}
	t.conns[c] = struct{}{}
	return true
}

func (t *TCP) untrack(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
	c.Close()
}

// serve reads one connection until it ends.
func (t *TCP) serve(ctx context.Context, c net.Conn, listener string) {
	// SO_KEEPALIVE, because a connection that died without a FIN -- an expired
	// NAT entry, a machine that vanished -- would otherwise hang around until the
	// idle timeout, holding a descriptor and a max_conns slot. An appliance that
	// logs once an hour is a normal case, so that timeout is generous.
	if tc, ok := tcpConnOf(c); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	br := bufio.NewReaderSize(c, tcpReadBuffer)
	fr := NewFramer(br, t.cfg.Framing, t.cfg.FramingAuto, int(t.cfg.MaxFrameBytes))

	addr, port := peerOf(c)

	for {
		armDeadline(c, t.cfg.IdleTimeout, &t.closing)
		raw, err := fr.Next()
		if err != nil {
			t.logFrameError(err, addr)
			return
		}

		w := Wire{
			Transport: envelope.TransportTCP,
			Framing:   fr.Mode(),
			Listener:  listener,
			PeerAddr:  addr,
			PeerPort:  port,
		}

		// This is the 429 of this protocol, and it is better than one: a 429
		// depends on the client honouring Retry-After, and most do not. A closed
		// TCP window is not optional.
		if !t.holdFor(ctx, c, raw, w) {
			return
		}
	}
}

// holdFor runs Accept, and on a retryable refusal stops reading until the ring
// drains or the deadline passes (SPEC §5.4).
//
// It returns false when the connection must be closed.
//
// The deadline exists because an emitter blocked forever is worse than one that
// reconnects: it holds a descriptor, a kernel buffer and a max_conns slot
// indefinitely, and with a thousand emitters in that state the server cannot
// even accept the one that WOULD drain.
func (t *TCP) holdFor(ctx context.Context, c net.Conn, raw []byte, w Wire) bool {
	// From the frame being whole to the ring's guarantee being met -- the
	// confirmation this transport gives by reading on (SPEC §13.1).
	held := time.Now()
	out := t.in.Accept(ctx, raw, w)
	if !out.Retryable() {
		// Accepted, filtered or rate-limited: nothing to hold back. A
		// rate-limited emitter will not drain on its own, so stalling its window
		// would only move the queue without resolving anything.
		if out == Accepted {
			t.m.ConfirmDuration.ObserveDuration(time.Since(held))
		}
		t.in.RecordDrop(out)
		return out != Shutdown
	}

	start := time.Now()
	deadline := start.Add(t.cfg.BackpressureTimeout)

	for {
		// While this loop runs, nothing is read from the socket. The receive
		// buffer fills, the advertised window goes to zero, and the emitter
		// stops sending. Nothing is lost.
		select {
		case <-ctx.Done():
			t.in.RecordDrop(Shutdown)
			return false
		case <-time.After(backpressurePoll):
		}

		out = t.in.Retry(ctx, raw, w)
		if !out.Retryable() {
			t.m.AddBackpressure(obs.TransportTCP, time.Since(start))
			if out == Accepted {
				t.m.ConfirmDuration.ObserveDuration(time.Since(held))
			}
			t.in.RecordDrop(out)
			return out != Shutdown
		}
		if time.Now().After(deadline) {
			t.m.AddBackpressure(obs.TransportTCP, time.Since(start))
			t.in.RecordDrop(out)
			t.log.Warn("closing a connection after holding it at the backpressure "+
				"timeout; the emitter should reconnect and retry",
				"transport", "tcp", "peer", w.PeerAddr.String(),
				"held", time.Since(start).String(), "reason", out.String())
			return false
		}
	}
}

func (t *TCP) logFrameError(err error, addr netip.Addr) {
	switch {
	case errors.Is(err, io.EOF):
		return // the emitter closed cleanly
	case errors.Is(err, ErrFrameTooLarge):
		t.m.MessagesDroppedTotal[obs.DropFrameTooLarge].Inc()
		t.log.Warn("closing a connection whose frame passed tcp.max_frame_bytes",
			"transport", "tcp", "peer", addr.String())
	case errors.Is(err, ErrMalformedFrame), errors.Is(err, io.ErrUnexpectedEOF):
		t.m.MessagesDroppedTotal[obs.DropMalformedFrame].Inc()
		t.log.Warn("closing a connection with a desynchronised frame; there is no "+
			"way to find the next message boundary",
			"transport", "tcp", "peer", addr.String(), "error", err)
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return // the idle timeout, which is not an error
		}
		t.log.Debug("connection ended", "transport", "tcp",
			"peer", addr.String(), "error", err)
	}
}

// Close stops accepting and stops reading, and waits for every connection's
// reader to return.
//
// An expired read deadline breaks a reader out of Read; the message it was
// already handing to the ring still gets its Accept. A reader held in the
// backpressure loop is released by the cancelled receive context. TCP has no
// answer to send, so the socket is closed by serve on its way out -- after the
// last message it read has either entered the ring or been counted.
func (t *TCP) Close() error {
	t.mu.Lock()
	t.closing.Store(true)
	conns := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	err := t.closeListeners()
	for _, c := range conns {
		_ = c.SetReadDeadline(time.Now())
	}
	t.wg.Wait()
	return err
}

func (t *TCP) closeListeners() error {
	var errs []error
	for _, l := range t.lns {
		errs = append(errs, l.Close())
	}
	return errors.Join(errs...)
}

// armDeadline sets the idle deadline for the next read, without undoing a
// shutdown.
//
// Close stops a reader by setting its read deadline to "now". A reader that is
// between two frames at that instant would otherwise renew the deadline for
// another idle_timeout before its next read, and shutdown would sit waiting for
// up to two minutes. So the reader sets its deadline FIRST and checks closing
// AFTER; Close sets closing FIRST and the deadline AFTER. In every interleaving
// the last deadline written is "now", which is the one property that matters.
func armDeadline(c net.Conn, idle time.Duration, closing *atomic.Bool) {
	if idle > 0 {
		_ = c.SetReadDeadline(time.Now().Add(idle))
	}
	if closing.Load() {
		_ = c.SetReadDeadline(time.Now())
	}
}
