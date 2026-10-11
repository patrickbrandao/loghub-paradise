package receiver

import (
	"context"
	"net"
	"net/netip"
	"sync"
)

// listenStream opens a TCP listener, optionally with SO_REUSEPORT so several
// listeners can share one address.
func listenStream(ctx context.Context, addr string, reusePort bool) (net.Listener, error) {
	lc := net.ListenConfig{}
	if reusePort {
		lc.Control = reuseControl
	}
	return lc.Listen(ctx, "tcp", addr)
}

// connLimiter enforces max_conns and max_conns_per_ip for ONE transport, across
// all of its listeners.
//
// It is created once per transport, never per listener: with N listeners on
// one address (SO_REUSEPORT) the kernel spreads the connections, so a limiter
// per listener would multiply both ceilings by N and let one address occupy
// its per-IP quota on each of them (SPEC §4.4).
//
// The two limits behave differently on purpose:
//
//   - The global cap BLOCKS in Accept rather than accepting and closing. An
//     accepted-then-closed connection looks to the emitter like a successful
//     connect followed by a reset, indistinguishable from a crash, and rsyslog
//     would treat it as a delivery failure worth retrying immediately. Leaving
//     the connection in the kernel backlog lets the emitter's own connect
//     timeout do the right thing.
//
//   - The per-IP cap CLOSES immediately, because blocking would stall every
//     other address behind one abusive peer. There is nothing better available:
//     the peer's identity is all that is known at accept time.
//
// The key is the TCP peer. There is no other address in syslog -- the hostname in
// the message is the emitter's claim and can be anything (§19.8).
type connLimiter struct {
	max   int
	perIP int

	sem chan struct{} // nil when max is 0

	mu   sync.Mutex
	byIP map[netip.Addr]int
}

// newConnLimiter builds the limiter for one transport, or returns nil when both
// ceilings are off; a nil limiter wraps nothing.
func newConnLimiter(max, perIP int) *connLimiter {
	if max <= 0 && perIP <= 0 {
		return nil
	}
	cl := &connLimiter{max: max, perIP: perIP}
	if max > 0 {
		cl.sem = make(chan struct{}, max)
	}
	if perIP > 0 {
		cl.byIP = make(map[netip.Addr]int)
	}
	return cl
}

// wrap puts one listener under the limiter. Every listener of the transport is
// wrapped by the same limiter, which is what makes the ceilings per transport.
func (cl *connLimiter) wrap(l net.Listener) net.Listener {
	if cl == nil {
		return l
	}
	return &limitListener{Listener: l, cl: cl, done: make(chan struct{})}
}

// limitListener is one listener under a shared connLimiter.
type limitListener struct {
	net.Listener
	cl *connLimiter

	// done unblocks an Accept parked on the global cap when the listener is
	// closed. Without it a shutdown at max_conns would wait forever for a
	// slot that only a client could free.
	closeOnce sync.Once
	done      chan struct{}
}

func (l *limitListener) Accept() (net.Conn, error) {
	cl := l.cl
	// A loop, not a recursive call, on the per-IP refusal. Recursion grew the
	// stack by one frame per refused connection, and a single peer reconnecting
	// past its cap in a tight loop -- the exact behaviour the cap exists for --
	// would push the goroutine past the runtime's stack limit and kill the
	// process. The runtime does not recover from that; it throws.
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

		key := peerAddrOf(c)
		if cl.byIP != nil {
			cl.mu.Lock()
			if cl.byIP[key] >= cl.perIP {
				cl.mu.Unlock()
				c.Close()
				cl.releaseGlobal()
				// Not counted as a dropped message: no message existed yet.
				continue
			}
			cl.byIP[key]++
			cl.mu.Unlock()
		}
		return &limitConn{Conn: c, cl: cl, key: key}, nil
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
	cl   *connLimiter
	key  netip.Addr
	once sync.Once
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		if c.cl.byIP != nil {
			c.cl.mu.Lock()
			if n := c.cl.byIP[c.key] - 1; n <= 0 {
				delete(c.cl.byIP, c.key)
			} else {
				c.cl.byIP[c.key] = n
			}
			c.cl.mu.Unlock()
		}
		c.cl.releaseGlobal()
	})
	return err
}

// tcpConnOf reaches the *net.TCPConn behind a connection -- through the
// limiter's wrapper when a ceiling is configured -- so the keep-alive settings
// of the transports apply whether or not the socket is wrapped.
func tcpConnOf(c net.Conn) (*net.TCPConn, bool) {
	if lc, ok := c.(*limitConn); ok {
		c = lc.Conn
	}
	tc, ok := c.(*net.TCPConn)
	return tc, ok
}

// peerAddrOf extracts the peer address, unmapped so an IPv4-mapped v6 address
// and the plain v4 form are the same key.
func peerAddrOf(c net.Conn) netip.Addr {
	ta, ok := c.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return netip.Addr{}
	}
	a, ok := netip.AddrFromSlice(ta.IP)
	if !ok {
		return netip.Addr{}
	}
	return a.Unmap()
}

// peerOf returns the address and port of a stream connection.
func peerOf(c net.Conn) (netip.Addr, uint16) {
	ta, ok := c.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return netip.Addr{}, 0
	}
	a, ok := netip.AddrFromSlice(ta.IP)
	if !ok {
		return netip.Addr{}, 0
	}
	return a.Unmap(), uint16(ta.Port)
}
