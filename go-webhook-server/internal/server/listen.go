package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/clientip"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// listen opens a TCP listener, optionally with SO_REUSEPORT so several
// listeners can share one address (SPEC §12.4, server.listeners > 1).
//
// The default is a single listener: Go's accept loop is rarely the bottleneck,
// and the single shared ring is the real point of contention anyway.
func listen(ctx context.Context, addr string, reusePort bool) (net.Listener, error) {
	lc := net.ListenConfig{}
	if reusePort {
		lc.Control = func(network, address string, c syscall.RawConn) error {
			var serr error
			err := c.Control(func(fd uintptr) {
				serr = setReusePort(fd)
			})
			if err != nil {
				return err
			}
			return serr
		}
	}
	return lc.Listen(ctx, "tcp", addr)
}

// The pause between two failed accepts doubles from acceptRetryMin to
// acceptRetryMax; a success starts the next failure over at the minimum.
const (
	acceptRetryMin = 5 * time.Millisecond
	acceptRetryMax = time.Second
)

// retryListener retries every accept error except the listener's own close
// (SPEC §10.3), under the connection limiter, around every public socket.
//
// net/http's Serve retries only Temporary() errors and returns on anything
// else -- ENOBUFS, ENOMEM, a firewall's EPERM, EPROTO -- and a returned Serve
// took the process down without draining: with ring=memory the events already
// answered 200 vanished uncounted. The listener itself is still good after
// any of those, so giving up is never the right answer; the syslog/RELP
// family's accept loop never gives up either. The pause keeps a persistent
// failure from spinning, and the WARN is sampled because EMFILE under a
// connection flood lasts, and one line per retry would flood the log.
type retryListener struct {
	net.Listener
	log     *obs.Logger
	sampler *obs.Sampler

	closeOnce sync.Once
	done      chan struct{}
}

func newRetryListener(l net.Listener, log *obs.Logger) *retryListener {
	return &retryListener{Listener: l, log: log, sampler: obs.NewSampler(10 * time.Second),
		done: make(chan struct{})}
}

func (l *retryListener) Accept() (net.Conn, error) {
	delay := acceptRetryMin
	for {
		c, err := l.Listener.Accept()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return c, err
		}
		if ok, suppressed := l.sampler.Allow(); ok {
			l.log.Warn("accept failed; retrying", "addr", l.Addr().String(), "error", err,
				"suppressed_since_last", suppressed)
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-l.done:
			t.Stop()
			return nil, net.ErrClosed
		}
		delay = min(delay*2, acceptRetryMax)
	}
}

func (l *retryListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.Listener.Close()
}

// connLimiter enforces server.max_conns and server.max_conns_per_ip for the
// whole server, across every listener.
//
// It is created ONCE, in Listen, and shared by all server.listeners: with
// SO_REUSEPORT the kernel spreads connections over the listeners, so a limiter
// per listener would multiply both ceilings by server.listeners and let one
// address fill its per-IP quota on each of them (SPEC §13.5).
//
// The two limits behave differently on purpose:
//
//   - The global cap BLOCKS in Accept rather than accepting and closing. An
//     accepted-then-closed connection looks to the client like a successful
//     connect followed by a reset, indistinguishable from a crash. Leaving the
//     connection in the kernel backlog lets the client's own connect timeout do
//     the right thing.
//
//   - The per-IP cap CLOSES immediately, because blocking would stall every
//     other address behind one abusive peer. There is nothing better available:
//     the peer's identity is all that is known at accept time, so no HTTP status
//     can be sent.
//
// Note the per-IP cap is keyed on the TCP peer, not on the resolved client
// address: no request has been read yet, so no forwarding header exists. Behind
// a single reverse proxy every connection shares one peer, which makes this
// limit useless there -- hence the default of 0. The key is the EXACT address,
// a /128 in IPv6, IPv4-mapped folded into IPv4 (SPEC §13.5, a contestable
// decision): counting by IPv6 prefix was declined, because a /64 can be a
// whole LAN of emitters and webhook senders leave from pools of neighbouring
// addresses; against a hostile /64 there are max_conns and the firewall.
type connLimiter struct {
	sem     chan struct{}
	perIP   int
	metrics perIPRejector

	mu         sync.Mutex
	perIPCount map[netip.Addr]int
}

// perIPRejector is the callback used to count per-IP refusals, kept as an
// interface so the listener does not depend on the metric registry.
type perIPRejector func()

func newConnLimiter(maxConns, maxPerIP int, onReject perIPRejector) *connLimiter {
	cl := &connLimiter{
		perIP:      maxPerIP,
		metrics:    onReject,
		perIPCount: make(map[netip.Addr]int),
	}
	if maxConns > 0 {
		cl.sem = make(chan struct{}, maxConns)
	}
	return cl
}

// wrap puts one listener under the limiter. Every listener of the server is
// wrapped by the same limiter, which is what makes the ceilings server-wide.
func (cl *connLimiter) wrap(l net.Listener) net.Listener {
	return &limitListener{Listener: l, lim: cl, done: make(chan struct{})}
}

// newLimitListener builds a limiter for a single listener. Listen does not use
// it -- it shares one limiter across all listeners -- but it is the natural
// unit for the tests of the two ceilings.
func newLimitListener(l net.Listener, maxConns, maxPerIP int, onReject perIPRejector) net.Listener {
	return newConnLimiter(maxConns, maxPerIP, onReject).wrap(l)
}

// limitListener is one listener under a shared connLimiter.
type limitListener struct {
	net.Listener
	lim *connLimiter

	closeOnce sync.Once
	done      chan struct{}
}

func (l *limitListener) Accept() (net.Conn, error) {
	cl := l.lim
	for {
		if cl.sem != nil {
			select {
			case cl.sem <- struct{}{}:
			case <-l.done:
				return nil, net.ErrClosed
			}
		}

		c, err := l.Listener.Accept()
		if err != nil {
			cl.releaseGlobal()
			return nil, err
		}

		addr, ok := cl.claimIP(c)
		if !ok {
			// Over the per-IP cap: refuse this one and keep accepting, so a
			// single noisy address cannot stall everyone else.
			_ = c.Close()
			cl.releaseGlobal()
			if cl.metrics != nil {
				cl.metrics()
			}
			continue
		}

		return &limitConn{Conn: c, release: func() { cl.release(addr) }}, nil
	}
}

// claimIP reserves a per-IP slot. The returned address is the key to release.
func (cl *connLimiter) claimIP(c net.Conn) (netip.Addr, bool) {
	if cl.perIP <= 0 {
		return netip.Addr{}, true
	}
	addr, _ := clientip.PeerAddr(c.RemoteAddr().String())

	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.perIPCount[addr] >= cl.perIP {
		return addr, false
	}
	cl.perIPCount[addr]++
	return addr, true
}

func (cl *connLimiter) release(addr netip.Addr) {
	cl.releaseGlobal()
	if cl.perIP <= 0 {
		return
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if n := cl.perIPCount[addr]; n <= 1 {
		// Delete rather than leave a zero: the map must not grow by one entry
		// per address ever seen.
		delete(cl.perIPCount, addr)
	} else {
		cl.perIPCount[addr] = n - 1
	}
}

func (cl *connLimiter) releaseGlobal() {
	if cl.sem == nil {
		return
	}
	select {
	case <-cl.sem:
	default:
	}
}

func (l *limitListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.Listener.Close()
}

type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// CloseWrite forwards the half-close that net/http looks for by type assertion.
//
// Without it the assertion in the stdlib's closeWriteAndWait fails, no FIN is
// sent, and the connection goes straight to a full Close while the client is
// still writing -- which most TCP stacks answer with an RST that also discards
// whatever was already in the client's receive buffer. The client then loses
// the very response it was being sent: the 431 and 400 that net/http writes
// before any handler runs.
//
// That is the identical failure the reject drain of SPEC §4.3 exists to
// prevent, arriving through a path this server does not control, and it appears
// only once server.max_conns or server.max_conns_per_ip is configured -- which
// is exactly when a client is most likely to be hitting a limit.
func (c *limitConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}
