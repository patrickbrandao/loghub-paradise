package receiver

// Regression tests for the independent review of 2026-10-05 (go-syslog-server,
// carried by go-relplog-server). Each names its finding; of F1 (filter and
// rate limiter) only the half that needs neither stays, and the ring=none
// logic refusal went with the feature it tested (SPEC §1.2).

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

// ------------------------------------------------------------------------ F1

// F1 (go-relplog-server: a held message was re-charged to the rate limiter on
// every 50 ms retry). Here there is no limiter, and what stays of it is the
// other half: a held message is counted ONCE as an arrival, keeps its uuid
// through the retries, and is accepted the moment the ring has room.
func TestRELPHeldMessageIsChargedOnce(t *testing.T) {
	h := newHarness(t, 1)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: 10 * time.Second}}})

	// Fill the one-slot ring from elsewhere, so the RELP message is held.
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("192.0.2.1")); out != Accepted {
		t.Fatalf("filler: %v", out)
	}
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>held")

	// ~12 retries; then drain.
	time.Sleep(600 * time.Millisecond)
	h.ring.Drain()
	rc.expect(ids[0], "200 OK")
	if got := h.ring.Drain(); len(got) != 1 || string(got[0].Raw) != "<13>held" {
		t.Fatalf("held message lost: %v", got)
	}
	if v := h.m.MessagesTotal[obs.TransportTCP].Load(); v != 2 {
		t.Fatalf("messages_total = %d, want 2: a retry counted the arrival again", v)
	}
	for r := range h.m.MessagesDroppedTotal {
		if v := h.m.MessagesDroppedTotal[r].Load(); v != 0 {
			t.Fatalf("messages_dropped{%s} = %d: a held message is not a drop", obs.DropReason(r), v)
		}
	}
}

// ------------------------------------------------------------------------ F2

// recConn records the deadlines set on it.
type recConn struct {
	net.Conn
	mu            sync.Mutex
	read, written time.Time
	readSet       int
	writeSet      int
}

func (c *recConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read, c.written = t, t
	c.readSet++
	c.writeSet++
	return nil
}
func (c *recConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = t
	c.readSet++
	return nil
}
func (c *recConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = t
	c.writeSet++
	return nil
}

// F2: before the RELP session, the PROXY header wait arms the WRITE deadline
// with the read one, and a shutdown expires both.
func TestArmDeadlineArmsBothDirections(t *testing.T) {
	var closing atomic.Bool
	c := &recConn{}
	armDeadline(c, time.Minute, &closing)
	if c.writeSet == 0 || c.written.IsZero() || !c.written.Equal(c.read) {
		t.Fatalf("write deadline %v, read %v: the write direction is not armed", c.written, c.read)
	}
	armDeadline(c, 0, &closing)
	if !c.written.IsZero() || !c.read.IsZero() {
		t.Fatal("idle_timeout=0 must clear both deadlines")
	}
	closing.Store(true)
	armDeadline(c, time.Hour, &closing)
	if time.Until(c.written) > time.Second || time.Until(c.read) > time.Second {
		t.Fatal("a reader re-arming during shutdown undid the shutdown's deadline")
	}
}

// Inside the session the reader arms only the READ side: the writer owns the
// write deadline, and a reader that touched it would cut a write of answers
// short. A shutdown's "now" is never undone.
func TestArmReadDeadlineLeavesTheWriteSide(t *testing.T) {
	var closing atomic.Bool
	c := &recConn{}
	armReadDeadline(c, time.Minute, &closing)
	if c.writeSet != 0 || c.read.IsZero() {
		t.Fatalf("read %v, %d write sets: the reader must arm the read side only", c.read, c.writeSet)
	}
	armReadDeadline(c, 0, &closing)
	if !c.read.IsZero() {
		t.Fatal("idle_timeout=0 must clear the read deadline")
	}
	closing.Store(true)
	armReadDeadline(c, time.Hour, &closing)
	if time.Until(c.read) > time.Second {
		t.Fatal("a reader re-arming during shutdown undid the shutdown's deadline")
	}
}

// blockConn blocks its reader until Close, ignoring every deadline: a reader no
// deadline can reach, the worst case Close's grace exists for.
type blockConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (b *blockConn) Close() error                     { b.once.Do(func() { close(b.closed) }); return nil }
func (b *blockConn) SetDeadline(time.Time) error      { return nil }
func (b *blockConn) SetReadDeadline(time.Time) error  { return nil }
func (b *blockConn) SetWriteDeadline(time.Time) error { return nil }

// F2: if a reader still does not return after closeGrace, Close closes its
// socket instead of waiting on the peer forever.
func TestStreamCloseForceClosesAfterGrace(t *testing.T) {
	h := newHarness(t, 10)
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m, log: logger(),
		conns: map[net.Conn]struct{}{}}
	c := &blockConn{closed: make(chan struct{})}
	if !s.track(c) {
		t.Fatal("track")
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.untrack(c)
		<-c.closed // a reader parked where no deadline reaches
	}()
	start := time.Now()
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeGrace + 5*time.Second):
		t.Fatal("Close waited on a reader its deadlines could not reach")
	}
	if el := time.Since(start); el < closeGrace/2 {
		t.Fatalf("Close returned after %v: it did not give the readers their grace", el)
	}
}

// ------------------------------------------------------------------------ F7

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type emfileListener struct {
	mu     sync.Mutex
	closed bool
	calls  int
}

func (l *emfileListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, net.ErrClosed
	}
	l.calls++
	return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
}
func (l *emfileListener) Close() error   { l.mu.Lock(); l.closed = true; l.mu.Unlock(); return nil }
func (l *emfileListener) Addr() net.Addr { return &net.TCPAddr{} }

// F7: a persistent accept error (EMFILE under a flood) logged an unsampled
// WARN every 10 ms. It is sampled now, and the loop still pauses and goes on.
func TestAcceptErrorLogIsSampled(t *testing.T) {
	h := newHarness(t, 10)
	var buf syncBuf
	l := &emfileListener{}
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m,
		log: obs.NewLogger(&buf, "text", "warn"), conns: map[net.Conn]struct{}{},
		lns: []net.Listener{l}, acceptLog: obs.NewSampler(30 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	l.Close()
	s.wg.Wait()
	if n := strings.Count(buf.String(), "accept failed"); n != 1 {
		t.Fatalf("%d accept-failed lines in 0.5 s, want 1 (sampled)", n)
	}
	l.mu.Lock()
	calls := l.calls
	l.mu.Unlock()
	if calls < 5 || calls > 100 {
		t.Fatalf("%d accept attempts in 0.5 s: the loop must keep trying, with its 10 ms pause", calls)
	}
}

// --------------------------------------------------- PROXY: a stalled header

// A trusted peer whose v1 header starts and then stalls past header_timeout is
// an INVALID header (the signature matched), refused as reason="proxy" --
// never "absent", and never fed to the RELP reader.
func TestProxyStalledHeaderIsInvalid(t *testing.T) {
	h := newHarness(t, 10)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{
		Proxy: ProxyConfig{Mode: ProxyOptional, Trusted: cidrs, HeaderTimeout: 150 * time.Millisecond}}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "PROXY TCP4 1.2.3.4") // and nothing more
	expectClosed(t, c)
	waitFor(t, "the invalid header", func() bool {
		return h.m.ProxyHeadersTotal[obs.ProxyInvalid].Load() == 1 && proxyRefused(h) == 1
	})
	if h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 0 || h.m.RELPFrameErrorsTotal[obs.FrameMalformed].Load() != 0 {
		t.Fatal("a stalled header was treated as absent and read as RELP")
	}
}
