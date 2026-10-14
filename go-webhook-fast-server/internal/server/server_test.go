package server

import (
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// The request pipeline is covered end to end by internal/app's integration
// suite, which drives the same wiring the binary uses. What is tested here is
// the connection-level machinery that sits BELOW the handler, where an
// integration test cannot reach: the accept-time limits.

// fakeListener hands out connections on demand so the limits can be exercised
// without real sockets and without timing luck.
type fakeListener struct {
	mu     sync.Mutex
	conns  chan net.Conn
	closed bool
}

func newFakeListener(buf int) *fakeListener {
	return &fakeListener{conns: make(chan net.Conn, buf)}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	c, ok := <-l.conns
	if !ok {
		return nil, net.ErrClosed
	}
	return c, nil
}

func (l *fakeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.conns)
	}
	return nil
}

func (l *fakeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }

func (l *fakeListener) offer(c net.Conn) { l.conns <- c }

// fakeConn reports a fixed remote address and records whether it was closed.
type fakeConn struct {
	net.Conn
	remote net.Addr
	closed chan struct{}
	once   sync.Once
}

func newFakeConn(remote string) *fakeConn {
	addr, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		panic(err)
	}
	return &fakeConn{remote: addr, closed: make(chan struct{})}
}

func (c *fakeConn) RemoteAddr() net.Addr { return c.remote }
func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *fakeConn) wasClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// ------------------------------------------------------------ per-IP ceiling

// SPEC §4.3 / server.max_conns_per_ip. The per-IP cap CLOSES rather than
// blocking, because blocking would let one abusive peer stall every other
// address behind it.
func TestPerIPLimitRefusesExcessAndKeepsServingOthers(t *testing.T) {
	fl := newFakeListener(32)
	var rejected int
	ln := newLimitListener(fl, 0, 2, func() { rejected++ })

	// Three connections from one address; the third must be refused.
	a1, a2, a3 := newFakeConn("10.0.0.1:1001"), newFakeConn("10.0.0.1:1002"), newFakeConn("10.0.0.1:1003")
	// One from a different address, offered last: it must still get through,
	// which is the whole point of refusing rather than blocking.
	b1 := newFakeConn("10.0.0.2:2001")

	for _, c := range []net.Conn{a1, a2, a3, b1} {
		fl.offer(c)
	}

	got := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		c, err := ln.Accept()
		if err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
		got = append(got, c)
	}

	if !a3.wasClosed() {
		t.Error("the third connection from 10.0.0.1 was not refused")
	}
	if rejected != 1 {
		t.Errorf("%d refusals counted, want 1", rejected)
	}
	if a1.wasClosed() || a2.wasClosed() {
		t.Error("a connection within the limit was closed")
	}
	if b1.wasClosed() {
		t.Error("a different address was refused because of another peer's usage")
	}
	if len(got) != 3 {
		t.Fatalf("accepted %d connections, want 3", len(got))
	}

	// Closing one frees a slot for that address.
	if err := got[0].Close(); err != nil {
		t.Fatal(err)
	}
	a4 := newFakeConn("10.0.0.1:1004")
	fl.offer(a4)
	if _, err := ln.Accept(); err != nil {
		t.Fatalf("Accept after a slot was freed: %v", err)
	}
	if a4.wasClosed() {
		t.Error("a connection was refused although a slot had been freed")
	}
}

// textAddr is a remote address that prints exactly as given, so a peer can
// arrive IPv4-mapped or zoned the way a dual-stack socket reports it
// (net.TCPAddr would print a mapped address in its IPv4 form).
type textAddr string

func (a textAddr) Network() string { return "tcp" }
func (a textAddr) String() string  { return string(a) }

// SPEC §13.5 (decided, contestable: DECISOES of go-webhook-fast-server, 4c):
// max_conns_per_ip counts the EXACT peer address, a /128 in IPv6. Two
// addresses of one /64 are two quotas; counting by prefix was declined,
// because a /64 can be a whole LAN of emitters and webhook senders leave from
// pools of neighbouring addresses. An IPv4-mapped peer is the same key as its
// IPv4 form, so the mapping is no way around the cap. This test pins the rule,
// so a change to it is a deliberate one.
func TestPerIPLimitCountsTheExactAddress(t *testing.T) {
	fl := newFakeListener(8)
	var refused int
	ln := newLimitListener(fl, 0, 1, func() { refused++ })

	offered := []string{
		"[2001:db8::1]:1001",
		"[2001:db8::2]:1002",      // same /64, another address: its own quota
		"[2001:db8::1]:1003",      // the first again: refused
		"192.0.2.7:1004",          // IPv4
		"[::ffff:192.0.2.7]:1005", // the same peer, IPv4-mapped: refused
	}
	conns := make([]*fakeConn, len(offered))
	for i, a := range offered {
		conns[i] = &fakeConn{remote: textAddr(a), closed: make(chan struct{})}
		fl.offer(conns[i])
	}
	_ = fl.Close() // Accept ends with ErrClosed once the five are handled

	var got []string
	for {
		c, err := ln.Accept()
		if err != nil {
			break
		}
		got = append(got, c.RemoteAddr().String())
	}
	want := []string{"[2001:db8::1]:1001", "[2001:db8::2]:1002", "192.0.2.7:1004"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("accepted %v, want %v", got, want)
	}
	if refused != 2 || !conns[2].wasClosed() || !conns[4].wasClosed() {
		t.Fatalf("%d refusals (third closed %v, fifth closed %v), want the repeated /128 and the mapped IPv4",
			refused, conns[2].wasClosed(), conns[4].wasClosed())
	}
}

// The per-IP table must not grow by one entry for every address ever seen; a
// long-running server facing the internet would otherwise leak unboundedly.
func TestPerIPTableDoesNotLeak(t *testing.T) {
	fl := newFakeListener(64)
	ln := newLimitListener(fl, 0, 4, nil)
	ll := ln.(*limitListener)

	for i := 0; i < 50; i++ {
		c := newFakeConn(fmt.Sprintf("10.1.%d.%d:5000", i/256, i%256))
		fl.offer(c)
		accepted, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		if err := accepted.Close(); err != nil {
			t.Fatal(err)
		}
	}

	ll.lim.mu.Lock()
	n := len(ll.lim.perIPCount)
	ll.lim.mu.Unlock()
	if n != 0 {
		t.Errorf("the per-IP table holds %d entries after every connection closed, want 0", n)
	}
}

// A double Close must not release the slot twice, which would let the address
// exceed its ceiling.
func TestPerIPReleaseIsIdempotent(t *testing.T) {
	fl := newFakeListener(8)
	ln := newLimitListener(fl, 0, 1, nil)
	ll := ln.(*limitListener)

	fl.offer(newFakeConn("10.0.0.9:1"))
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	_ = c.Close()
	_ = c.Close()

	ll.lim.mu.Lock()
	n := ll.lim.perIPCount[netip.MustParseAddr("10.0.0.9")]
	ll.lim.mu.Unlock()
	if n != 0 {
		t.Errorf("count = %d after repeated Close, want 0", n)
	}
}

func TestPerIPDisabledAcceptsEverything(t *testing.T) {
	fl := newFakeListener(32)
	ln := newLimitListener(fl, 0, 0, func() { t.Error("nothing should be refused with the limit off") })

	for i := 0; i < 10; i++ {
		c := newFakeConn("10.0.0.1:1000")
		fl.offer(c)
		if _, err := ln.Accept(); err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
		if c.wasClosed() {
			t.Fatalf("connection %d was refused with the per-IP limit disabled", i)
		}
	}
}

// ------------------------------------------------------------ global ceiling

// SPEC: the global cap BLOCKS rather than accepting and closing. An
// accepted-then-closed connection is indistinguishable from a crash to the
// client; leaving it in the kernel backlog lets the client's own connect
// timeout do the right thing.
func TestGlobalLimitBlocksRatherThanRefusing(t *testing.T) {
	fl := newFakeListener(8)
	ln := newLimitListener(fl, 2, 0, func() { t.Error("the global cap must not refuse; it must block") })

	c1, c2, c3 := newFakeConn("10.0.0.1:1"), newFakeConn("10.0.0.2:2"), newFakeConn("10.0.0.3:3")
	for _, c := range []net.Conn{c1, c2, c3} {
		fl.offer(c)
	}

	a1, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); err != nil {
		t.Fatal(err)
	}

	// The third Accept must not return until a slot is freed.
	third := make(chan struct{})
	go func() {
		_, _ = ln.Accept()
		close(third)
	}()

	select {
	case <-third:
		t.Fatal("Accept returned past the global limit instead of blocking")
	case <-time.After(150 * time.Millisecond):
	}
	if c3.wasClosed() {
		t.Error("the third connection was closed; the global cap must leave it queued")
	}

	_ = a1.Close()
	select {
	case <-third:
	case <-time.After(2 * time.Second):
		t.Error("Accept did not resume after a slot was freed")
	}
}

// SPEC §13.5: max_conns and max_conns_per_ip are ceilings of the SERVER. With
// server.listeners > 1 every listener must share one limiter, or the ceilings
// are multiplied by the number of listeners.
func TestConnLimitsAreSharedAcrossListeners(t *testing.T) {
	t.Run("global cap", func(t *testing.T) {
		fl1, fl2 := newFakeListener(8), newFakeListener(8)
		lim := newConnLimiter(1, 0, nil)
		ln1, ln2 := lim.wrap(fl1), lim.wrap(fl2)

		fl1.offer(newFakeConn("10.0.0.1:1"))
		a1, err := ln1.Accept()
		if err != nil {
			t.Fatal(err)
		}

		// The only slot is taken through listener 1: an Accept on listener 2
		// must block, not hand out a second one.
		fl2.offer(newFakeConn("10.0.0.2:2"))
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
	})

	t.Run("per-IP cap", func(t *testing.T) {
		fl1, fl2 := newFakeListener(8), newFakeListener(8)
		var rejected int
		lim := newConnLimiter(0, 1, func() { rejected++ })
		ln1, ln2 := lim.wrap(fl1), lim.wrap(fl2)

		fl2.offer(newFakeConn("10.0.0.2:2"))
		if _, err := ln2.Accept(); err != nil {
			t.Fatal(err)
		}

		// The address is at its cap through listener 2, so listener 1 must
		// refuse it and go on to the next connection.
		c3 := newFakeConn("10.0.0.2:3")
		fl1.offer(c3)
		fl1.offer(newFakeConn("10.0.0.3:4"))
		if _, err := ln1.Accept(); err != nil {
			t.Fatal(err)
		}
		if !c3.wasClosed() || rejected != 1 {
			t.Errorf("the per-IP cap was not applied across listeners (closed=%v, rejected=%d)",
				c3.wasClosed(), rejected)
		}
	})
}

func TestLimitListenerCloseUnblocksAccept(t *testing.T) {
	fl := newFakeListener(1)
	ln := newLimitListener(fl, 1, 0, nil)

	fl.offer(newFakeConn("10.0.0.1:1"))
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
		if err == nil {
			t.Error("Accept returned a connection after Close")
		}
	case <-time.After(2 * time.Second):
		t.Error("Close did not unblock a waiting Accept; shutdown would hang")
	}

	// Close must be idempotent: the shutdown path can reach it more than once.
	if err := ln.Close(); err != nil && err != net.ErrClosed {
		t.Errorf("second Close: %v", err)
	}
}
