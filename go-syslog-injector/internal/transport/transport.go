// Package transport opens the connections a run sends on (SPEC §4): UDP
// (RFC 5426), unix datagram, TCP (RFC 6587, octet-counting or LF) and TLS
// (RFC 5425), with an optional PROXY protocol header on TCP and TLS.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/sched"
)

// Outcomes of a message (SPEC §8.2).
const (
	Sent       = "sent"        // written to the socket (no ack exists in syslog)
	WriteError = "write_error" // the connection broke before it was written
	TooLarge   = "too_large"   // does not fit a datagram
	NoBufs     = "nobufs"      // the local kernel dropped the datagram
	Refused    = "refused"     // ICMP port unreachable (from an earlier datagram)
)

// Framing of TCP and TLS.
type Framing uint8

const (
	Octet Framing = iota // RFC 6587 §3.4.1, the only one in TLS
	LF                   // RFC 6587 §3.4.2
)

// Options of a dialer.
type Options struct {
	Network string // udp | tcp | tls | unix
	Target  string // host:port, or a path for unix
	Framing Framing
	Timeout time.Duration
	TLS     *tls.Config
	Proxy   *ProxyHeader // nil: no PROXY header
}

// MaxDatagram is the largest UDP payload over IPv4 (65535 - 20 - 8).
const MaxDatagram = 65507

// Dialer returns the dialer of o.
func Dialer(o Options) (sched.Dialer, error) {
	switch o.Network {
	case "udp":
		return func(ctx context.Context) (sched.Conn, error) {
			d := net.Dialer{Timeout: o.Timeout}
			c, err := d.DialContext(ctx, "udp", o.Target)
			if err != nil {
				return nil, err
			}
			return &dgramConn{c: c, timeout: o.Timeout}, nil
		}, nil
	case "unix":
		return func(ctx context.Context) (sched.Conn, error) {
			d := net.Dialer{Timeout: o.Timeout}
			c, err := d.DialContext(ctx, "unixgram", o.Target)
			if err != nil {
				return nil, err
			}
			return &dgramConn{c: c, timeout: o.Timeout, unix: true}, nil
		}, nil
	case "tcp", "tls":
		if o.Network == "tls" && o.Framing != Octet {
			return nil, errors.New("TLS framing is octet-counting only (RFC 5425 §4.3)")
		}
		return func(ctx context.Context) (sched.Conn, error) {
			c, err := dialStream(ctx, o)
			if err != nil {
				return nil, err
			}
			sc := &streamConn{c: c, framing: o.Framing, timeout: o.Timeout}
			go sc.watch()
			return sc, nil
		}, nil
	}
	return nil, fmt.Errorf("unknown transport %q", o.Network)
}

func dialStream(ctx context.Context, o Options) (net.Conn, error) {
	d := net.Dialer{Timeout: o.Timeout, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", o.Target)
	if err != nil {
		return nil, err
	}
	if o.Proxy != nil {
		// The header goes before the TLS handshake, as a balancer sends it.
		h, err := o.Proxy.Encode(c.LocalAddr(), c.RemoteAddr())
		if err == nil {
			_ = c.SetWriteDeadline(time.Now().Add(o.Timeout))
			_, err = c.Write(h)
		}
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("proxy header: %w", err)
		}
	}
	if o.Network != "tls" {
		return c, nil
	}
	tc := tls.Client(c, o.TLS)
	hctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return tc, nil
}

// dgramConn is a connected UDP or unix datagram socket: one message, one
// datagram, outcome at once.
type dgramConn struct {
	c       net.Conn
	timeout time.Duration
	unix    bool
}

func (d *dgramConn) Send(msg []byte, done sched.DoneFunc) error {
	if !d.unix && len(msg) > MaxDatagram {
		done(TooLarge, false)
		return nil
	}
	deadline := time.Now().Add(d.timeout)
	for {
		_ = d.c.SetWriteDeadline(deadline)
		_, err := d.c.Write(msg)
		switch {
		case err == nil:
			done(Sent, true)
			return nil
		case errors.Is(err, syscall.EMSGSIZE):
			done(TooLarge, false)
			return nil
		case errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.EAGAIN):
			// A unix socket is the backpressure of the server (its SPEC
			// §4.3): wait like syslog(3) does. A UDP drop is reported.
			if d.unix && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				continue
			}
			done(NoBufs, false)
			return nil
		case !d.unix && errors.Is(err, syscall.ECONNREFUSED):
			done(Refused, false)
			return nil
		default:
			done(WriteError, false)
			return err
		}
	}
}

func (d *dgramConn) Flush() error                { return nil }
func (d *dgramConn) Close(context.Context) error { return d.c.Close() }

// streamConn frames messages into a buffer written at Flush (or when it
// passes flushAt). A syslog stream has no acknowledgement: "sent" is "the
// kernel took it", and a server that closed the connection is noticed by the
// reader goroutine, which only waits for EOF.
type streamConn struct {
	c       net.Conn
	framing Framing
	timeout time.Duration
	buf     []byte
	pending []sched.DoneFunc
	closed  atomic.Bool // the server closed or reset its side
}

const flushAt = 64 << 10

// ErrServerClosed is the reason of a connection the server ended.
var ErrServerClosed = errors.New("server closed the connection")

func (s *streamConn) watch() {
	var b [512]byte
	for {
		if _, err := s.c.Read(b[:]); err != nil {
			s.closed.Store(true)
			return
		}
		// A syslog server sends nothing; whatever comes is ignored.
	}
}

func (s *streamConn) Send(msg []byte, done sched.DoneFunc) error {
	if s.closed.Load() {
		done(WriteError, false)
		s.failPending()
		return ErrServerClosed
	}
	if s.framing == Octet {
		s.buf = strconv.AppendInt(s.buf, int64(len(msg)), 10)
		s.buf = append(s.buf, ' ')
		s.buf = append(s.buf, msg...)
	} else {
		s.buf = append(s.buf, msg...)
		s.buf = append(s.buf, '\n')
	}
	s.pending = append(s.pending, done)
	if len(s.buf) >= flushAt {
		return s.Flush()
	}
	return nil
}

func (s *streamConn) Flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	_ = s.c.SetWriteDeadline(time.Now().Add(s.timeout))
	_, err := s.c.Write(s.buf)
	s.buf = s.buf[:0]
	if err != nil {
		s.failPending()
		return err
	}
	for _, d := range s.pending {
		d(Sent, true)
	}
	s.pending = s.pending[:0]
	return nil
}

func (s *streamConn) failPending() {
	for _, d := range s.pending {
		d(WriteError, false)
	}
	s.pending = s.pending[:0]
	s.buf = s.buf[:0]
}

func (s *streamConn) Close(context.Context) error {
	err := s.Flush()
	s.failPending()
	if cerr := s.c.Close(); err == nil {
		err = cerr
	}
	return err
}
