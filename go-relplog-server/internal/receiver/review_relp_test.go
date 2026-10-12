package receiver

// Regression tests from the independent review of 2026-10-11 (SPEC §5.4,
// §5.6, §5.7): each failed before its fix.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// slowRing: Enqueue #1 refuses at once (held), Enqueue #2 takes `slow` and
// refuses (a sink write that times out under ring=none), every later Enqueue
// accepts. Like ring.Memory and ring.None it ignores ctx cancellation.
type slowRing struct {
	stubRing
	mu       sync.Mutex
	n        int
	slow     time.Duration
	inSlow   chan struct{}
	accepted atomic.Int64
}

func (r *slowRing) Enqueue(ctx context.Context, it *item.Item) error {
	r.mu.Lock()
	r.n++
	n := r.n
	r.mu.Unlock()
	switch n {
	case 1:
		return ring.ErrFull
	case 2:
		close(r.inSlow)
		time.Sleep(r.slow)
		return ring.ErrFull
	}
	r.accepted.Add(1)
	return nil
}

// A peer that vanishes while a held transaction is inside a slow Retry: the
// next select in decide sees ctx.Done AND an expired timer, and picks the
// timer half of the time -> Retry -> the message is enqueued after its
// session ended (SPEC 5.7 says "never").
func TestReviewRetryAfterPeerLeft(t *testing.T) {
	const runs = 10
	late := 0
	for i := 0; i < runs; i++ {
		h := newHarness(t, 10, "debug", nil)
		r := &slowRing{slow: 300 * time.Millisecond, inSlow: make(chan struct{})}
		h.in = NewIngest(r, h.sink, h.filt, nil, h.m, logger(), h.cfg)
		tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
		rc := dialRELP(t, tr.Addr())
		rc.syslog("<13>held")
		<-r.inSlow
		rc.c.Close() // the peer is gone: librelp will retransmit "held"
		time.Sleep(500 * time.Millisecond)
		if r.accepted.Load() > 0 {
			late++
		}
		tr.Close()
	}
	t.Logf("%d of %d runs enqueued the message after its peer left", late, runs)
	if late > 0 {
		t.Fatalf("held transaction enqueued after the session ended in %d/%d runs", late, runs)
	}
}

// The budget charges what a transaction really holds: two pooled copies of
// its line (the transaction's and the item's inside Enqueue) and the fixed
// overhead of the goroutine, its stack, the Wire and the timer, measured at
// about 9.6 KiB for a small line by the review of 2026-10-11 (SPEC §5.6).
func TestTxnChargeCoversTheTransaction(t *testing.T) {
	if got := txnCharge(len("<13>small message")); got != 2*2048+8192 {
		t.Fatalf("txnCharge(small) = %d, want two 2 KiB copies and 8 KiB of overhead", got)
	}
	if got := txnCharge(60 << 10); got != 2*(64<<10)+8192 {
		t.Fatalf("txnCharge(60 KiB) = %d", got)
	}
}

// SPEC 5.4 reconciliation: messages_total = accepted + dropped + consumed +
// relp_unanswered_total. A frame the reader holds while waiting for a window
// slot is counted unanswered but never as an arrival.
func TestReviewReconciliationAfterAbort(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Window: 1, BackpressureTimeout: 300 * time.Millisecond}}})
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("192.0.2.1")); out != Accepted {
		t.Fatal(out)
	}
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>held", "<13>waits for the window")
	rc.expectServerClose()
	m := h.m
	total := m.MessagesTotal[obs.TransportTCP].Load()
	acc := m.MessagesAcceptedTotal[obs.TransportTCP].Load()
	var un uint64
	for i := range m.RELPUnansweredTotal {
		un += m.RELPUnansweredTotal[i].Load()
	}
	t.Logf("messages_total=%d accepted=%d unanswered=%d", total, acc, un)
	if total != acc+un {
		t.Fatalf("reconciliation broken: %d != %d + %d", total, acc, un)
	}
}

// A session the SERVER ends (malformed frame) with a transaction held: the
// peer is still there, yet the transaction is counted conn_lost.
func TestReviewUnansweredReasonOnServerEnd(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("192.0.2.1")); out != Accepted {
		t.Fatal(out)
	}
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>held")
	time.Sleep(100 * time.Millisecond)
	rc.write("x garbage\n")
	rc.expectServerClose()
	waitFor(t, "the server_closed count", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load() == 1
	})
	if v := h.m.RELPUnansweredTotal[obs.UnansweredConnLost].Load(); v != 0 {
		t.Fatalf("conn_lost = %d for a session the server ended with its peer on", v)
	}
}
