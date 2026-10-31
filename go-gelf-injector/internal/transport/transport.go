// Package transport opens the connections a run sends on (SPEC §4): GELF
// over UDP (compressed, chunked), TCP and TLS (null-byte framing), and HTTP
// or HTTPS (POST, keep-alive, pipelining), with an optional PROXY protocol
// header on every stream transport.
package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// Outcomes of a message that are not an HTTP status code (SPEC §8.2). An HTTP
// response is reported by its status code ("202", "503", ...); only 2xx is a
// success.
const (
	Sent        = "sent"        // UDP: the kernel took every datagram; TCP/TLS: the buffer was written
	WriteError  = "write_error" // the connection broke before it was written
	TooLarge    = "too_large"   // UDP: more than 128 chunks of --chunk-size
	NoBufs      = "nobufs"      // the local kernel dropped a datagram
	Refused     = "refused"     // ICMP port unreachable (from an earlier datagram)
	NulByte     = "nul_byte"    // TCP/TLS: a NUL inside the document would split it
	Timeout     = "timeout"     // HTTP: no response within --response-timeout
	ConnError   = "conn_error"  // HTTP: the connection broke before the response
	NoResponse  = "no_response" // HTTP: the server closed with this request unanswered
	BadResponse = "bad_response"
	NotSent     = "not_sent"
)

// Options of a dialer.
type Options struct {
	Network string // udp | tcp | tls | http | https
	Target  string // host:port
	Timeout time.Duration
	TLS     *tls.Config  // tls and https
	Proxy   *ProxyHeader // nil: no PROXY header (stream transports only)

	// UDP and HTTP
	Compress  gelf.Compression
	ChunkSize int // UDP: the largest datagram, chunk header included

	// HTTP
	Path            string // request target, e.g. /gelf
	Host            string // Host header
	ResponseTimeout time.Duration
	Pipeline        int    // requests in flight per connection
	PerConn         uint64 // requests per connection; 0 = no limit
	Retries         int    // resends of a 429 or 503
	RetryMaxWait    time.Duration

	// OnDial is told of every connection attempt (the report counts them).
	OnDial func(error)
}

// MaxDatagram is the largest UDP payload over IPv4 (65535 - 20 - 8).
const MaxDatagram = 65507

// Dialer returns the dialer of o.
func Dialer(o Options) (sched.Dialer, error) {
	if o.OnDial == nil {
		o.OnDial = func(error) {}
	}
	switch o.Network {
	case "udp":
		if o.ChunkSize < gelf.MinChunkSize || o.ChunkSize > MaxDatagram {
			return nil, fmt.Errorf("chunk size must be %d..%d", gelf.MinChunkSize, MaxDatagram)
		}
		return func(ctx context.Context) (sched.Conn, error) {
			d := net.Dialer{Timeout: o.Timeout}
			c, err := d.DialContext(ctx, "udp", o.Target)
			o.OnDial(err)
			if err != nil {
				return nil, err
			}
			return &udpConn{c: c, o: o, rnd: mrand.New(mrand.NewPCG(mrand.Uint64(), mrand.Uint64()))}, nil
		}, nil
	case "tcp", "tls":
		if o.Compress != gelf.None {
			return nil, errors.New("GELF over TCP is not compressed")
		}
		return func(ctx context.Context) (sched.Conn, error) {
			c, err := dialStream(ctx, o)
			o.OnDial(err)
			if err != nil {
				return nil, err
			}
			sc := &streamConn{c: c, timeout: o.Timeout}
			go sc.watch()
			return sc, nil
		}, nil
	case "http", "https":
		o.Pipeline = max(o.Pipeline, 1)
		return func(ctx context.Context) (sched.Conn, error) { return dialHTTP(ctx, o) }, nil
	}
	return nil, fmt.Errorf("unknown transport %q", o.Network)
}

// dialStream dials TCP, writes the PROXY header if any, then does the TLS
// handshake for tls and https.
func dialStream(ctx context.Context, o Options) (net.Conn, error) {
	d := net.Dialer{Timeout: o.Timeout, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", o.Target)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok && (o.Network == "http" || o.Network == "https") {
		// A request waits for nothing after it: push it at once.
		_ = tc.SetNoDelay(true)
	}
	if o.Proxy != nil {
		// The header goes before the TLS handshake, as a balancer sends it.
		h, err := o.Proxy.Encode(c.LocalAddr(), c.RemoteAddr())
		if err == nil {
			_ = c.SetWriteDeadline(time.Now().Add(o.Timeout))
			_, err = c.Write(h)
			_ = c.SetWriteDeadline(time.Time{})
		}
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("proxy header: %w", err)
		}
	}
	if o.Network != "tls" && o.Network != "https" {
		return c, nil
	}
	cfg := o.TLS.Clone()
	if o.Network == "https" {
		cfg.NextProtos = []string{"http/1.1"}
	}
	tc := tls.Client(c, cfg)
	hctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	if err := tc.HandshakeContext(hctx); err != nil {
		c.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return tc, nil
}

// udpConn is a connected UDP socket: one message is one datagram, or up to
// 128 chunks of it; the outcome is known at once.
type udpConn struct {
	c   net.Conn
	o   Options
	z   gelf.Compressor
	rnd *mrand.Rand
	buf []byte
}

// Send implements sched.Conn.
func (u *udpConn) Send(_ uint64, m *source.Message, done sched.DoneFunc) error {
	p := u.z.Compress(u.o.Compress, m.Data)
	n := gelf.NumChunks(len(p), u.o.ChunkSize)
	if n == 0 {
		return u.send(p, done)
	}
	if n > gelf.MaxChunks {
		// Refused before anything is sent: a receiver drops a message
		// announcing more than 128 chunks.
		done(TooLarge, false)
		return nil
	}
	// The id only has to be unique among the messages a receiver is
	// reassembling from this source port; random is what the GELF libraries use.
	var id [8]byte
	v := u.rnd.Uint64()
	for i := range id {
		id[i] = byte(v >> (8 * i))
	}
	per := u.o.ChunkSize - gelf.HeaderLen
	for seq := range n {
		part := p[seq*per : min((seq+1)*per, len(p))]
		u.buf = gelf.AppendChunk(u.buf[:0], id, seq, n, part)
		if outcome, err := u.write(u.buf); outcome != "" {
			// The rest of the message is not sent: the receiver could
			// never complete it.
			done(outcome, false)
			return err
		}
	}
	done(Sent, true)
	return nil
}

func (u *udpConn) send(b []byte, done sched.DoneFunc) error {
	if outcome, err := u.write(b); outcome != "" {
		done(outcome, false)
		return err
	}
	done(Sent, true)
	return nil
}

// write sends one datagram: "" when the kernel took it, or the failure (and
// an error when the socket is unusable).
func (u *udpConn) write(b []byte) (string, error) {
	_ = u.c.SetWriteDeadline(time.Now().Add(u.o.Timeout))
	_, err := u.c.Write(b)
	switch {
	case err == nil:
		return "", nil
	case errors.Is(err, syscall.EMSGSIZE):
		return TooLarge, nil
	case errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.EAGAIN):
		return NoBufs, nil
	case errors.Is(err, syscall.ECONNREFUSED):
		return Refused, nil
	}
	return WriteError, err
}

func (u *udpConn) Flush() error                { return nil }
func (u *udpConn) Close(context.Context) error { return u.c.Close() }

// streamConn frames documents with a NUL into a buffer written at Flush (or
// when it passes flushAt). GELF over TCP has no acknowledgement: "sent" is
// "the kernel took it", and a server that closed the connection is noticed
// by the reader goroutine, which only waits for EOF.
type streamConn struct {
	c       net.Conn
	timeout time.Duration
	buf     []byte
	pending []sched.DoneFunc
	ends    []int       // end of each pending frame in buf, its NUL included
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
		// A GELF server sends nothing; whatever comes is ignored.
	}
}

// Send implements sched.Conn.
func (s *streamConn) Send(_ uint64, m *source.Message, done sched.DoneFunc) error {
	if bytes.IndexByte(m.Data, 0) >= 0 {
		// The receiver would split it in two (or more) documents.
		done(NulByte, false)
		return nil
	}
	if s.closed.Load() {
		done(WriteError, false)
		s.failPending()
		return ErrServerClosed
	}
	s.buf = append(s.buf, m.Data...)
	s.buf = append(s.buf, 0)
	s.pending = append(s.pending, done)
	s.ends = append(s.ends, len(s.buf))
	if len(s.buf) >= flushAt {
		return s.Flush()
	}
	return nil
}

// Flush implements sched.Conn.
func (s *streamConn) Flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	_ = s.c.SetWriteDeadline(time.Now().Add(s.timeout))
	n, err := s.c.Write(s.buf)
	s.buf = s.buf[:0]
	if err != nil {
		// A partial write (a --timeout under backpressure, typically): the
		// frames whose bytes, NUL included, all went to the kernel are sent
		// by the same rule as a full write; the rest failed. The frame cut
		// in the middle reaches the server truncated, and it reads it as a
		// malformed document at EOF (SPEC §4.2).
		for i, d := range s.pending {
			if s.ends[i] <= n {
				d(Sent, true)
			} else {
				d(WriteError, false)
			}
		}
		s.pending = s.pending[:0]
		s.ends = s.ends[:0]
		return err
	}
	for _, d := range s.pending {
		d(Sent, true)
	}
	s.pending = s.pending[:0]
	s.ends = s.ends[:0]
	return nil
}

func (s *streamConn) failPending() {
	for _, d := range s.pending {
		d(WriteError, false)
	}
	s.pending = s.pending[:0]
	s.ends = s.ends[:0]
	s.buf = s.buf[:0]
}

// Close implements sched.Conn.
func (s *streamConn) Close(context.Context) error {
	err := s.Flush()
	s.failPending()
	if cerr := s.c.Close(); err == nil {
		err = cerr
	}
	return err
}
