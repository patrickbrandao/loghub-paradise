package receiver

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// Regression tests from the port of the go-relplog-pg-server/go-relplog-nats-server known-issues
// round (2026-10-14, SPEC §22.1).

// tcp.idle_timeout shorter than relp.backpressure_timeout: a session with a
// HELD transaction and no new frame closes on the idle, and the transaction is
// what was waiting -- relp_unanswered_total{reason="backpressure"}, not
// server_closed (SPEC §5.4). The effect is unchanged: the idle still closes
// the session, and librelp retransmits.
func TestIdleCloseWithAHeldTransactionIsBackpressure(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 300 * time.Millisecond,
		RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("192.0.2.1")); out != Accepted {
		t.Fatal(out)
	}
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>held")
	waitFor(t, "the held transaction", func() bool { return h.m.RELPInflightTransactions.Load() == 1 })
	waitFor(t, "the unanswered count", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load()+
			h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load() == 1
	})
	if bp, sc := h.m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load(),
		h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load(); bp != 1 || sc != 0 {
		t.Fatalf("idle close with a held transaction: backpressure = %d, server_closed = %d; want 1 and 0", bp, sc)
	}
	rc.expectServerClose()
}

// Control: the idle with nothing held leaves nothing unanswered, and counts
// nothing.
func TestIdleCloseWithNothingHeldCountsNothing(t *testing.T) {
	h := newHarness(t, 4, "debug", nil)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 300 * time.Millisecond}})
	rc := dialRELP(t, tr.Addr())
	n := rc.syslog("<13>one")
	rc.expect(n[0], "200 OK")
	rc.expectServerClose()
	for r := range h.m.RELPUnansweredTotal {
		if v := h.m.RELPUnansweredTotal[r].Load(); v != 0 {
			t.Fatalf("relp_unanswered_total[%d] = %d after an idle close with nothing held", r, v)
		}
	}
}

// Control (ported from go-relplog-nats-server): a second open ends the session (librelp's copen.c) with a transaction held:
// the server ended it with its peer still there, so what it leaves unanswered
// is server_closed, not conn_lost (SPEC §5.4, §14.1).
func TestSecondOpenLeavesItsHeldTransactionServerClosed(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("192.0.2.1")); out != Accepted {
		t.Fatal(out)
	}
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>held")
	waitFor(t, "the held transaction", func() bool { return h.m.RELPInflightTransactions.Load() == 1 })
	rc.send("open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	waitFor(t, "the server_closed count", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load() == 1
	})
	if v := h.m.RELPUnansweredTotal[obs.UnansweredConnLost].Load(); v != 0 {
		t.Fatalf("conn_lost = %d for a session the server ended with its peer on", v)
	}
}

type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c addrConn) RemoteAddr() net.Addr { return c.remote }

func connFrom(ip string) net.Conn {
	return addrConn{Conn: pipeConn(), remote: &net.TCPAddr{IP: net.ParseIP(ip), Port: 40000}}
}

// Lock test ported from go-syslog-pg-server on 2026-10-14 (decision 16, SPEC
// §4.4): max_conns_per_ip counts the EXACT peer address, a /128 in IPv6. Two
// addresses of one /64 are two quotas; counting by prefix was declined,
// because a /64 can be a whole network of RELP emitters. An IPv4-mapped peer
// is the same key as its IPv4 form (SPEC §4.2), so the mapping is no way
// around the cap. This test pins the rule, so a change to it is a deliberate
// one.
func TestPerIPLimitCountsTheExactAddress(t *testing.T) {
	var refused []obs.ConnLimit
	lim := newConnLimiter(0, 1, func(l obs.ConnLimit) { refused = append(refused, l) })
	st := &stubListener{conns: []net.Conn{
		connFrom("2001:db8::1"),
		connFrom("2001:db8::2"),      // same /64, another address: its own quota
		connFrom("2001:db8::1"),      // the first again: refused
		connFrom("192.0.2.7"),        // IPv4
		connFrom("::ffff:192.0.2.7"), // the same peer, IPv4-mapped: refused
	}}
	ln := lim.wrap(st)
	var got []string
	for {
		c, err := ln.Accept()
		if err != nil {
			break
		}
		got = append(got, peerAddrOf(c).String())
	}
	if want := "2001:db8::1 2001:db8::2 192.0.2.7"; strings.Join(got, " ") != want {
		t.Fatalf("accepted %v, want %s", got, want)
	}
	if len(refused) != 2 || refused[0] != obs.ConnLimitPerIP || refused[1] != obs.ConnLimitPerIP {
		t.Fatalf("refusals %v, want two per_ip", refused)
	}
}

// slowEnqueueRing is a memory ring whose Enqueue waits for the test and then
// reports a full ring: a first Accept that takes as long as a group COMMIT, a
// Redis round trip or a synchronous sink write in go-relplog-server.
type slowEnqueueRing struct {
	*ring.Memory
	entered chan struct{}
	gate    chan struct{}
}

func (r *slowEnqueueRing) Enqueue(ctx context.Context, it *item.Item) error {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.gate
	return ring.ErrFull
}

// The transaction counts as held from before its FIRST Accept (independent
// review of 2026-10-14, A2; SPEC §5.4). An idle timeout that fired while that
// Accept was still inside a slow ring recorded server_closed, and the
// transaction, held afterwards and then abandoned, was counted under it.
func TestIdleDuringASlowFirstAcceptIsBackpressure(t *testing.T) {
	h := newHarness(t, 4, "debug", nil)
	sr := &slowEnqueueRing{Memory: h.ring, entered: make(chan struct{}, 1), gate: make(chan struct{})}
	h.in = NewIngest(sr, h.sink, h.filt, nil, h.m, logger(), h.cfg)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 200 * time.Millisecond,
		RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>slow")
	select {
	case <-sr.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the transaction never reached the ring")
	}
	// The idle (200ms) fires while the first Accept is still in the ring.
	time.Sleep(700 * time.Millisecond)
	close(sr.gate)
	waitFor(t, "the unanswered count", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load()+
			h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load() == 1
	})
	if bp, sc := h.m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load(),
		h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load(); bp != 1 || sc != 0 {
		t.Fatalf("idle during a slow first Accept: backpressure = %d, server_closed = %d; want 1 and 0", bp, sc)
	}
}
