package sink

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

// stallDialer dials the NATS connection itself so that a write that misses its
// deadline can close the socket (SPEC §10.8.1).
//
// nats.go bounds every socket write by FlusherTimeout (one minute by default;
// the sink sets sink.op_timeout), but a publish takes no context, and past
// 32 KiB of buffer the client flushes inline, holding the connection's lock.
// Against a peer that stops reading without a reset -- a frozen host, a paused
// container, a partition that drops packets -- the first writer waits out its
// deadline and gets an i/o timeout, the connection stays CONNECTED with half a
// frame written, and the next writer that takes the lock waits its own
// deadline on the same full socket: eight workers were eight deadlines in a
// row, with Health and Close queued behind the same lock, and the shutdown ran
// past its stop_grace_period before the dump (review of 2026-11-21, item 001).
// Closing the socket at the first missed deadline makes every later write fail
// at once (network: there is no reconnect buffer), and the client's read loop
// sees the close and reconnects; the half-written frame is never continued.
//
// The cost: nats.go splits its dial timeout across the addresses a host name
// resolves to, and a custom dialer cannot know how many there are, so each
// address gets the whole timeout.
type stallDialer struct {
	d       net.Dialer
	onStall func(remote string)
}

func newStallDialer(connectTimeout time.Duration, onStall func(remote string)) *stallDialer {
	return &stallDialer{d: net.Dialer{Timeout: connectTimeout}, onStall: onStall}
}

// Dial implements nats.CustomDialer.
func (s *stallDialer) Dial(network, address string) (net.Conn, error) {
	c, err := s.d.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return &stallConn{Conn: c, dialer: s}, nil
}

// stallConn is the socket under the client (and under its TLS, where there is
// one): every method is the socket's, except that a write ending in a missed
// deadline closes it.
type stallConn struct {
	net.Conn
	dialer *stallDialer
	once   sync.Once
}

func (c *stallConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
		c.once.Do(func() {
			_ = c.Conn.Close()
			if c.dialer.onStall != nil {
				c.dialer.onStall(c.RemoteAddr().String())
			}
		})
	}
	return n, err
}
