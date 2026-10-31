// Package transport is the RELP client of the injector (SPEC §4): a session
// over TCP or TLS (with an optional PROXY header), a window of unacknowledged
// transactions, and the reaction to each answer that librelp has, so the
// server sees what it would see from rsyslog's omrelp.
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
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/sched"
)

// Outcomes of a message (SPEC §8.2).
const (
	Acked      = "acked"      // 200 OK
	Rejected   = "rejected"   // any other code: librelp drops the message
	Unanswered = "unanswered" // the session ended without an answer, retries spent
)

// Options of a dialer.
type Options struct {
	Network    string // tcp | tls
	Target     string
	Timeout    time.Duration // dial, handshake, write and open answer
	AckTimeout time.Duration // the oldest unacknowledged waits at most this
	Window     int
	TLS        *tls.Config
	Proxy      *ProxyHeader
	Software   string // relp_software offer
	// BreakOnReject ends the session after a non-200 answer, as librelp does
	// (its scrsp.c returns an error that suspends the action).
	BreakOnReject bool
}

// MaxTxnr is where librelp wraps the transaction number back to 1.
const MaxTxnr = 999_999_999

// MaxWindow bounds --window (librelp's default is 128).
const MaxWindow = 65536

// Errors that end a session.
var (
	ErrServerClose = errors.New("server sent serverclose")
	ErrAckTimeout  = errors.New("no answer within the ack timeout")
	ErrRejected    = errors.New("session ended after a rejected message")
	ErrClosed      = errors.New("session closed")
)

// Dialer returns the dialer of o.
func Dialer(o Options) (sched.Dialer, error) {
	if o.Network != "tcp" && o.Network != "tls" {
		return nil, fmt.Errorf("unknown transport %q", o.Network)
	}
	if o.Window < 1 || o.Window > MaxWindow {
		return nil, fmt.Errorf("window must be 1..%d", MaxWindow)
	}
	return func(ctx context.Context) (sched.Conn, error) {
		c, err := dialStream(ctx, o)
		if err != nil {
			return nil, err
		}
		rc := &relpConn{
			c: c, o: o, br: bufio.NewReaderSize(c, 16<<10),
			window: make(chan struct{}, o.Window),
			pend:   make(map[int]*pend),
			dead:   make(chan struct{}),
			closeR: make(chan struct{}),
		}
		if err := rc.open(); err != nil {
			c.Close()
			return nil, err
		}
		go rc.read()
		return rc, nil
	}, nil
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

type pend struct {
	txnr int
	m    *sched.Msg
}

type relpConn struct {
	c  net.Conn
	o  Options
	br *bufio.Reader

	buf    []byte // frames not yet written; only the worker touches it
	window chan struct{}

	mu      sync.Mutex
	pend    map[int]*pend
	next    int
	closeTx int // txnr of our close, 0 until sent
	holds   int // messages held without a frame (keys -1, -2, ...)

	dead    chan struct{}
	deadErr error
	once    sync.Once
	closeR  chan struct{} // the answer to our close arrived
}

const flushAt = 64 << 10

func (r *relpConn) offers() string {
	sw := r.o.Software
	if sw == "" {
		sw = "go-relplog-injector"
	}
	return "relp_version=0\nrelp_software=" + sw + "\ncommands=syslog"
}

// open sends the open command and reads its answer synchronously: a session
// whose open is refused is a dial error.
func (r *relpConn) open() error {
	off := r.offers()
	frame := "1 open " + strconv.Itoa(len(off)) + " " + off + "\n"
	_ = r.c.SetWriteDeadline(time.Now().Add(r.o.Timeout))
	if _, err := io.WriteString(r.c, frame); err != nil {
		return fmt.Errorf("relp open: %w", err)
	}
	_ = r.c.SetReadDeadline(time.Now().Add(r.o.Timeout))
	f, err := ReadFrame(r.br)
	_ = r.c.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("relp open: %w", err)
	}
	if f.Txnr != 1 || f.Cmd != "rsp" {
		return fmt.Errorf("relp open: unexpected %d %s", f.Txnr, f.Cmd)
	}
	if code, text := RspCode(f.Data); code != 200 {
		return fmt.Errorf("relp open refused: %s", text)
	}
	if !bytes.Contains(f.Data, []byte("commands=syslog")) {
		return errors.New("relp open: server does not offer commands=syslog")
	}
	r.next = 2
	return nil
}

func (r *relpConn) kill(err error) {
	r.once.Do(func() {
		r.deadErr = err
		close(r.dead)
		r.c.Close()
	})
}

func (r *relpConn) isDead() bool {
	select {
	case <-r.dead:
		return true
	default:
		return false
	}
}

// Send queues one syslog transaction. With the window full it flushes and
// waits: that wait is the RELP backpressure of the server.
func (r *relpConn) Send(m *sched.Msg) error {
	select {
	case r.window <- struct{}{}:
	default:
		if err := r.Flush(); err != nil {
			r.hold(m)
			return err
		}
		select {
		case r.window <- struct{}{}:
		case <-r.dead:
			r.hold(m)
			return r.deadErr
		}
	}
	if r.isDead() {
		r.hold(m)
		return r.deadErr
	}
	r.mu.Lock()
	tx := r.next
	r.next++
	if r.next > MaxTxnr {
		r.next = 1
	}
	r.pend[tx] = &pend{txnr: tx, m: m}
	r.mu.Unlock()
	r.buf = appendSyslog(r.buf, tx, m.Data)
	if len(r.buf) >= flushAt {
		return r.Flush()
	}
	return nil
}

// hold keeps a message that never made it into a frame, for Unacked.
func (r *relpConn) hold(m *sched.Msg) {
	// It never reached the wire: this session is not a try.
	m.Tries--
	r.mu.Lock()
	// A negative key never collides with a txnr and sorts first: it was the
	// next message in order.
	r.holds++
	r.pend[-r.holds] = &pend{txnr: -1, m: m}
	r.mu.Unlock()
}

func appendSyslog(b []byte, tx int, msg []byte) []byte {
	b = strconv.AppendInt(b, int64(tx), 10)
	b = append(b, " syslog "...)
	b = strconv.AppendInt(b, int64(len(msg)), 10)
	if len(msg) > 0 {
		b = append(b, ' ')
		b = append(b, msg...)
	}
	return append(b, '\n')
}

// Flush writes the queued frames.
func (r *relpConn) Flush() error {
	if len(r.buf) == 0 {
		return nil
	}
	if r.isDead() {
		r.buf = r.buf[:0]
		return r.deadErr
	}
	_ = r.c.SetWriteDeadline(time.Now().Add(r.o.Timeout))
	_, err := r.c.Write(r.buf)
	r.buf = r.buf[:0]
	if err != nil {
		r.kill(err)
		return err
	}
	return nil
}

// read handles the server's frames until the session ends.
func (r *relpConn) read() {
	for {
		r.mu.Lock()
		waiting := len(r.pend) > 0 || r.closeTx != 0
		r.mu.Unlock()
		if waiting {
			_ = r.c.SetReadDeadline(time.Now().Add(r.o.AckTimeout))
		} else {
			_ = r.c.SetReadDeadline(time.Time{})
		}
		f, err := ReadFrame(r.br)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				r.mu.Lock()
				waiting = len(r.pend) > 0 || r.closeTx != 0
				r.mu.Unlock()
				if !waiting {
					continue
				}
				err = ErrAckTimeout
			}
			r.kill(err)
			return
		}
		switch f.Cmd {
		case "serverclose":
			r.kill(ErrServerClose)
			return
		case "rsp":
		default:
			r.kill(fmt.Errorf("unexpected command %q from the server", f.Cmd))
			return
		}
		r.mu.Lock()
		if r.closeTx != 0 && f.Txnr == r.closeTx {
			r.mu.Unlock()
			close(r.closeR)
			return
		}
		p := r.pend[f.Txnr]
		delete(r.pend, f.Txnr)
		r.mu.Unlock()
		if p == nil {
			r.kill(fmt.Errorf("answer to unknown transaction %d", f.Txnr))
			return
		}
		<-r.window
		code, _ := RspCode(f.Data)
		if code == 200 {
			p.m.Done(Acked, true)
			continue
		}
		p.m.Done(Rejected, false)
		if r.o.BreakOnReject {
			r.kill(ErrRejected)
			return
		}
	}
}

// Close waits (until ctx ends) for every answer, sends close, waits for its
// answer, and ends the connection. Messages still unanswered stay for
// Unacked: the worker decides whether they are retransmitted.
func (r *relpConn) Close(ctx context.Context) error {
	if err := r.Flush(); err != nil {
		return err
	}
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		r.mu.Lock()
		n := len(r.pend)
		r.mu.Unlock()
		if n == 0 || r.isDead() {
			break
		}
		select {
		case <-ctx.Done():
			r.kill(ctx.Err())
			return ctx.Err()
		case <-r.dead:
		case <-t.C:
		}
	}
	if r.isDead() {
		return r.deadErr
	}
	r.mu.Lock()
	r.closeTx = r.next
	tx := r.next
	r.mu.Unlock()
	_ = r.c.SetWriteDeadline(time.Now().Add(r.o.Timeout))
	if _, err := io.WriteString(r.c, strconv.Itoa(tx)+" close 0\n"); err != nil {
		r.kill(err)
		return err
	}
	select {
	case <-r.closeR:
	case <-r.dead:
	case <-ctx.Done():
	}
	r.kill(ErrClosed)
	return nil
}

// Unacked returns, in sending order, the messages of a closed session that
// never got an answer; their done has not been called.
func (r *relpConn) Unacked() []*sched.Msg {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]int, 0, len(r.pend))
	for k := range r.pend {
		keys = append(keys, k)
	}
	// Held messages (negative keys) come after the framed ones: they were
	// handed over later. Among framed ones, txnr order (no wrap inside a
	// window: the gap would need 10^9 transactions in flight).
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if (a < 0) != (b < 0) {
			return a > 0
		}
		if a < 0 {
			return a > b
		}
		return a < b
	})
	out := make([]*sched.Msg, 0, len(keys))
	for _, k := range keys {
		out = append(out, r.pend[k].m)
	}
	r.pend = map[int]*pend{}
	return out
}

// Frame is one RELP frame.
type Frame struct {
	Txnr int
	Cmd  string
	Data []byte
}

// MaxFrameData bounds what the server may send in one frame (an open answer
// is a few hundred bytes; a rsp text, a line).
const MaxFrameData = 1 << 20

// ReadFrame reads "TXNR SP CMD SP DATALEN [SP DATA] LF". DATALEN 0 is
// accepted with and without the space before the LF, as the server's reader
// accepts both from the field.
func ReadFrame(br *bufio.Reader) (Frame, error) {
	var f Frame
	tx, err := readNum(br, ' ', 9)
	if err != nil {
		return f, err
	}
	cmd, err := br.ReadString(' ')
	if err != nil {
		return f, err
	}
	cmd = cmd[:len(cmd)-1]
	if len(cmd) == 0 || len(cmd) > 32 {
		return f, fmt.Errorf("relp: bad command %q", cmd)
	}
	for i := 0; i < len(cmd); i++ {
		if cmd[i] < 'a' || cmd[i] > 'z' {
			return f, fmt.Errorf("relp: bad command %q", cmd)
		}
	}
	n, sep, err := readLen(br)
	if err != nil {
		return f, err
	}
	f.Txnr, f.Cmd = tx, cmd
	if n == 0 {
		if sep == ' ' {
			if b, err := br.ReadByte(); err != nil || b != '\n' {
				return f, errors.New("relp: DATALEN 0 without trailer")
			}
		}
		return f, nil
	}
	if sep != ' ' {
		return f, errors.New("relp: data without space")
	}
	if n > MaxFrameData {
		return f, fmt.Errorf("relp: frame of %d bytes", n)
	}
	f.Data = make([]byte, n)
	if _, err := io.ReadFull(br, f.Data); err != nil {
		return f, err
	}
	if b, err := br.ReadByte(); err != nil || b != '\n' {
		return f, errors.New("relp: missing trailer")
	}
	return f, nil
}

func readNum(br *bufio.Reader, end byte, max int) (int, error) {
	v, n := 0, 0
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == end && n > 0 {
			return v, nil
		}
		if b < '0' || b > '9' || n >= max {
			return 0, fmt.Errorf("relp: bad number")
		}
		v = v*10 + int(b-'0')
		n++
	}
}

func readLen(br *bufio.Reader) (int, byte, error) {
	v, n := 0, 0
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		if (b == ' ' || b == '\n') && n > 0 {
			return v, b, nil
		}
		if b < '0' || b > '9' || n >= 9 {
			return 0, 0, fmt.Errorf("relp: bad DATALEN")
		}
		v = v*10 + int(b-'0')
		n++
	}
}

// RspCode parses the "CODE text" at the start of a rsp's data.
func RspCode(d []byte) (int, string) {
	line := d
	if i := bytes.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	if len(line) < 3 {
		return 0, string(line)
	}
	c, err := strconv.Atoi(string(line[:3]))
	if err != nil {
		return 0, string(line)
	}
	return c, string(line)
}
