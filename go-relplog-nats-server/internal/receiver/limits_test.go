package receiver

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime/debug"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

func TestRELPPerIPLimit(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{MaxConnsPerIP: 2}})
	for i := 0; i < 2; i++ {
		// The open answer proves the server registered the connection.
		dialRELP(t, tr.Addr())
	}
	// The third is accepted by the kernel and closed immediately: blocking would
	// stall every other address behind one abusive peer.
	c := dial(t, tr.Addr())
	expectClosed(t, c)
	waitFor(t, "the per_ip refusal", func() bool {
		return h.m.ConnsRefusedTotal[obs.TransportTCP][obs.ConnLimitPerIP].Load() == 1
	})
}

// The global ceiling parks the accept loop, which is counted; the waiting
// connection is served as soon as a slot frees.
func TestRELPGlobalLimitParksAndCounts(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{MaxConns: 1}})
	first := dialRELP(t, tr.Addr())

	second := newRELPClient(t, dial(t, tr.Addr())) // completes in the kernel backlog
	n := second.send("open", relpTestOffers)
	waitFor(t, "the global park", func() bool {
		return h.m.ConnsRefusedTotal[obs.TransportTCP][obs.ConnLimitGlobal].Load() >= 1
	})
	second.expectNothing(100 * time.Millisecond)
	first.c.Close()
	second.expect(n, "200 OK")
}

// ------------------------------------------------------------------ limiter

// stubListener hands out pre-made connections and then reports closed. Every
// connection shares the zero peer key, so a per-IP cap of 1 refuses all but
// the first -- which is exactly what a reconnecting abuser looks like.
type stubListener struct {
	conns []net.Conn
	next  int
}

func (s *stubListener) Accept() (net.Conn, error) {
	if s.next >= len(s.conns) {
		return nil, net.ErrClosed
	}
	c := s.conns[s.next]
	s.next++
	return c, nil
}
func (s *stubListener) Close() error   { return nil }
func (s *stubListener) Addr() net.Addr { return &net.TCPAddr{} }

func pipeConn() net.Conn {
	a, b := net.Pipe()
	go func() { _ = b.Close() }()
	return a
}

// The per-IP refusal used to call Accept recursively, one stack frame per
// refused connection. A peer reconnecting past its cap in a tight loop -- the
// case the cap exists for -- would grow the stack until the runtime threw. With
// a tight stack limit, twenty thousand refusals prove the loop.
func TestLimitListenerRefusesWithoutRecursing(t *testing.T) {
	old := debug.SetMaxStack(256 << 10)
	defer debug.SetMaxStack(old)

	const refusals = 20000
	st := &stubListener{}
	st.conns = append(st.conns, pipeConn()) // the one that gets the slot
	for i := 0; i < refusals; i++ {
		st.conns = append(st.conns, pipeConn())
	}
	var counted int
	ln := newConnLimiter(0, 1, func(l obs.ConnLimit) {
		if l == obs.ConnLimitPerIP {
			counted++
		}
	}).wrap(st)

	first, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after %d refusals", err, refusals)
	}
	if st.next != refusals+1 {
		t.Fatalf("listener served %d connections, want %d", st.next, refusals+1)
	}
	if counted != refusals {
		t.Fatalf("%d refusals counted, want %d", counted, refusals)
	}
}

// SPEC §4.4: the ceilings are per transport. Two listeners of one transport
// share one limiter, or max_conns is silently multiplied by the listener count.
func TestConnLimitsAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(1, 0, nil)
	st1 := &stubListener{conns: []net.Conn{pipeConn()}}
	st2 := &stubListener{conns: []net.Conn{pipeConn()}}
	ln1, ln2 := lim.wrap(st1), lim.wrap(st2)

	a1, err := ln1.Accept()
	if err != nil {
		t.Fatal(err)
	}
	second := make(chan struct{})
	go func() {
		_, _ = ln2.Accept()
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("listener 2 accepted past the global cap; the limiter is not shared")
	case <-time.After(150 * time.Millisecond):
	}
	_ = a1.Close()
	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("listener 2 did not resume after listener 1 freed the slot")
	}
}

// A shutdown at max_conns must not wait for a client to free a slot: closing
// the listener unblocks the parked Accept.
func TestLimitListenerCloseUnblocksAccept(t *testing.T) {
	lim := newConnLimiter(1, 0, nil)
	st := &stubListener{conns: []net.Conn{pipeConn()}}
	ln := lim.wrap(st)
	if _, err := ln.Accept(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept stayed parked on the global cap after Close")
	}
}

// SPEC §4.4: the per-IP ceiling is per transport too. net.Pipe peers share the
// zero address, so to the limiter every stub connection is one IP: the second
// listener must refuse its connection and come back with its own exhaustion
// instead of a second accept.
func TestConnLimitsPerIPAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(0, 1, nil)
	st1 := &stubListener{conns: []net.Conn{pipeConn()}}
	st2 := &stubListener{conns: []net.Conn{pipeConn()}}
	ln1, ln2 := lim.wrap(st1), lim.wrap(st2)

	a1, err := ln1.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ln2.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener 2: err = %v, want the per-IP refusal followed by ErrClosed", err)
	}
	if st2.next != 1 {
		t.Fatalf("listener 2 served %d connections, want exactly the one refused", st2.next)
	}
	lim.mu.Lock()
	n := lim.byIP[netip.Addr{}]
	lim.mu.Unlock()
	if n != 1 {
		t.Fatalf("per-IP count = %d after one accept and one refusal, want 1", n)
	}

	_ = a1.Close()
	lim.mu.Lock()
	_, still := lim.byIP[netip.Addr{}]
	lim.mu.Unlock()
	if still {
		t.Fatal("the per-IP entry survived the last connection's Close")
	}
}

// With both ceilings on, a connection refused by the per-IP limit has ALREADY
// taken a global slot on its way in; that slot must come back, or a burst from
// one address would consume the whole transport.
func TestBothCeilingsAreReleasedExactlyOnce(t *testing.T) {
	lim := newConnLimiter(2, 1, nil)
	st := &stubListener{conns: []net.Conn{pipeConn(), pipeConn(), pipeConn()}}
	ln := lim.wrap(st)

	a1, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after the refusals", err)
	}
	if held := len(lim.sem); held != 1 {
		t.Fatalf("%d global slots held after the refusals, want 1 (the live connection)", held)
	}
	_ = a1.Close()
	_ = a1.Close() // a second Close must not release a second time
	if held := len(lim.sem); held != 0 {
		t.Fatalf("%d global slots held after the last Close, want 0", held)
	}
	lim.mu.Lock()
	left := len(lim.byIP)
	lim.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d per-IP entries left after the last Close, want 0", left)
	}
}

// The wiring, not the mechanism: every listener a transport opens is wrapped by
// the SAME limiter. A limiter created inside the listener loop would pass the
// unit tests above and still multiply the ceilings by listen.tcp_listeners.
func TestTCPListenersShareOneLimiter(t *testing.T) {
	// SO_REUSEPORT on port 0 would give each socket its own port; take one.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	h := newHarness(t, 64)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{Addr: addr, Listeners: 2, MaxConns: 1}})
	if len(tr.lns) != 2 {
		t.Fatalf("%d listeners, want 2", len(tr.lns))
	}
	var shared *connLimiter
	for i, l := range tr.lns {
		ll, ok := l.(*limitListener)
		if !ok {
			t.Fatalf("listener %d is not under the limiter", i)
		}
		if shared == nil {
			shared = ll.cl
		} else if ll.cl != shared {
			t.Fatal("the listeners hold different limiters; max_conns would be per listener again")
		}
	}
}

// The keep-alive settings of serve() are applied through the limiter's
// wrapper: a *limitConn is not a *net.TCPConn, and a plain type assertion on
// the wrapper silently skipped them whenever a ceiling was configured.
func TestKeepAliveReachesTheSocketUnderTheLimiter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, ok := tcpConnOf(&limitConn{Conn: c, cl: newConnLimiter(1, 0, nil)}); !ok {
		t.Fatal("tcpConnOf did not reach the TCP socket behind the limiter's wrapper")
	}
	if _, ok := tcpConnOf(pipeConn()); ok {
		t.Fatal("tcpConnOf reported a TCP socket for a pipe")
	}
}

func TestNewTCPRefusesAProxyWithoutTrustedNetworks(t *testing.T) {
	h := newHarness(t, 1)
	_, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{
		Addr: "127.0.0.1:0", Proxy: ProxyConfig{Mode: ProxyOptional}}}, h.in, h.m, logger())
	if err == nil {
		t.Fatal("proxy.protocol=optional without proxy.trusted was accepted (SPEC §13.2: exit 1)")
	}
}
