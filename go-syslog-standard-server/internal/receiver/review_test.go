package receiver

// Tests for the connection ceilings of SPEC §4.4: the ceilings are shared
// across listeners for BOTH limits, both limits are released exactly once, and
// the keep-alive settings reach the socket through the limiter's wrapper.

import (
	"errors"
	"net"
	"net/netip"
	"testing"
)

// SPEC §4.4: the per-IP ceiling is per transport too. net.Pipe peers share the
// zero address, so to the limiter every stub connection is one IP: the second
// listener must refuse its connection and come back with its own exhaustion
// instead of a second accept.
func TestConnLimitsPerIPAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(0, 1)
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
	lim := newConnLimiter(2, 1)
	st := &stubListener{conns: []net.Conn{pipeConn(), pipeConn(), pipeConn()}}
	ln := lim.wrap(st)

	a1, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// The two remaining connections are over the per-IP cap: refused, and the
	// listener runs out.
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after the refusals", err)
	}
	if held := len(lim.sem); held != 1 {
		t.Fatalf("%d global slots held after the refusals, want 1 (the live connection)", held)
	}

	_ = a1.Close()
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
	h := newHarness(t, 64, "debug", nil)
	tr := startTCP(t, h, TCPConfig{Listeners: 2, MaxConns: 1})
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

	if _, ok := tcpConnOf(&limitConn{Conn: c, cl: newConnLimiter(1, 0)}); !ok {
		t.Fatal("tcpConnOf did not reach the TCP socket behind the limiter's wrapper")
	}
	if _, ok := tcpConnOf(pipeConn()); ok {
		t.Fatal("tcpConnOf reported a TCP socket for a pipe")
	}
}
