package receiver

// Regression tests from the independent review of go-relplog-pg-server on
// 2026-10-13 (SPEC §5.4, §5.6, §5.7; TESTING §9.3).

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// The full-ring pre-check sees the BYTE ceiling too (ring.FullFor). With
// ring.max_bytes the binding one, it saw only the item count, and a held
// transaction minted a uuid, took a pooled buffer, copied the line and built
// an item on every 50 ms retry, for Enqueue to refuse it (SPEC §5.7).
func TestIngestByteCeilingIsRefusedBeforeTheCopy(t *testing.T) {
	m := newMetrics()
	r := ring.NewMemory(100, 2048+ring.ItemOverhead, time.Millisecond) // one 2 KiB item fits
	in := NewIngest(r, newUp(), m, logger(), defaultIngestConfig())
	if out := in.Accept(context.Background(), []byte("<13>first"), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
	w := tcpWire("10.0.0.3")
	allocs := testing.AllocsPerRun(200, func() {
		w.UUID = uuidv7.UUID{}
		if out := in.Retry(context.Background(), []byte("<13>second"), w); out != RingFull {
			t.Fatalf("byte-full ring: %v", out)
		}
	})
	if allocs != 0 {
		t.Fatalf("a retry held on the byte ceiling allocated %.1f times, want 0", allocs)
	}
	if !w.UUID.IsZero() {
		t.Fatal("a uuid was drawn for a message the byte ceiling refused")
	}
}

// The byte pre-check charges the TRUNCATED line, the one the item would copy:
// a 5000-byte line cut to max_message_bytes = 100 takes a 2 KiB buffer, and
// fits a ring with room for exactly that.
func TestIngestByteCeilingChargesTheTruncatedLine(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 100
	r := ring.NewMemory(100, 2048+ring.ItemOverhead, time.Millisecond)
	in := NewIngest(r, newUp(), newMetrics(), logger(), cfg)
	if out := in.Accept(context.Background(), []byte("<13>"+strings.Repeat("x", 5000)), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("a line truncated to 100 bytes into a ring with room for one 2 KiB item: %v", out)
	}
}

// A closed ring is not "full": the pre-check lets Enqueue say ErrClosed, which
// is the shutdown -- answered with nothing, like a held transaction past its
// session, never with the backpressure of a full ring.
func TestIngestClosedFullRingIsTheShutdown(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte("<13>filler"), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatal(out)
	}
	h.ring.Close()
	if out := h.in.Accept(context.Background(), []byte("<13>late"), tcpWire("10.0.0.3")); out != Shutdown {
		t.Fatalf("closed and full ring: %v, want %v", out, Shutdown)
	}
}

// SPEC §5.7, "o fim da sessão vence": a transaction whose session ended while
// it was being built is not enqueued -- librelp retransmits what it never got
// an answer for. The memory ring ignores the context, so the pipeline checks
// it right before the hand-off.
func TestIngestEndedSessionIsNotEnqueued(t *testing.T) {
	h := newHarness(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := h.in.Accept(ctx, []byte("<13>late"), tcpWire("10.0.0.3")); out != Shutdown {
		t.Fatalf("ended session: %v, want %v", out, Shutdown)
	}
	if n := h.ring.Stats().Items; n != 0 {
		t.Fatalf("%d messages enqueued after their session ended", n)
	}
}

// A frame dispatched by a session that already ended must never start a
// transaction (SPEC §5.7). The window select picks at random when both cases
// are ready, so about half the iterations reach Acquire with the session over:
// the context check after Acquire is what stops them (relp.go dispatch).
func TestDispatchAfterSessionEndStartsNothing(t *testing.T) {
	h := newHarness(t, 1000)
	tr := startTCP(t, h, TCPConfig{})
	src := netip.MustParseAddrPort("192.0.2.1:514")
	for i := 0; i < 64; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ss := &relpSession{s: tr, a: connAddrs{src: src, peer: src}, parent: context.Background(),
			ctx: ctx, cancel: cancel, acks: make(chan ack, 8), sem: make(chan struct{}, 4),
			writerDone: make(chan struct{})}
		f := RELPFrame{Txnr: 1, Command: obs.RELPSyslog, Data: []byte("<13>late"), Size: 8}
		started := ss.dispatch(&f, time.Now())
		ss.inFlight.Wait()
		if started {
			t.Fatalf("iteration %d: a transaction started after its session ended", i)
		}
	}
	if n := h.ring.Stats().Items; n != 0 {
		t.Fatalf("%d messages enqueued behind librelp's retransmission", n)
	}
}

// Invariant 24 (SPEC §5.6): the transaction's copy is made only AFTER its slot
// in the window and its bytes in the budget. The frame buffer is the reader's,
// reused by the next read; here it is overwritten while the dispatch waits for
// the budget, and the ring must receive what the buffer holds when the copy is
// finally made -- a copy taken before the wait would carry the old bytes, and
// would be memory the budget never bounded.
func TestCopyComesAfterTheWindowAndTheBudget(t *testing.T) {
	h := newHarness(t, 10)
	budget := NewBudget(64<<10, h.m)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Budget: budget}}})
	held, _ := budget.Acquire(context.Background(), 64<<10) // the whole budget
	src := netip.MustParseAddrPort("192.0.2.1:514")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ss := &relpSession{s: tr, a: connAddrs{src: src, peer: src}, parent: context.Background(),
		ctx: ctx, cancel: cancel, acks: make(chan ack, 8), sem: make(chan struct{}, 4),
		writerDone: make(chan struct{})}
	data := []byte("<13>before")
	f := RELPFrame{Txnr: 1, Command: obs.RELPSyslog, Data: data, Size: len(data)}
	done := make(chan bool, 1)
	go func() { done <- ss.dispatch(&f, time.Now()) }()
	waitFor(t, "the dispatch holding its window slot", func() bool { return len(ss.sem) == 1 })
	time.Sleep(50 * time.Millisecond) // and waiting on the budget
	select {
	case <-done:
		t.Fatal("the dispatch did not wait for the budget")
	default:
	}
	copy(data, "<13>after!") // the reader reused its buffer
	budget.Release(held)
	if !<-done {
		t.Fatal("the transaction never started")
	}
	ss.inFlight.Wait()
	items := h.drainRing()
	if len(items) != 1 || string(items[0].Raw) != "<13>after!" {
		var got []string
		for _, it := range items {
			got = append(got, string(it.Raw))
		}
		t.Fatalf("the ring holds %q: the copy was made before the window and the budget", got)
	}
}

// A second open ends the session (librelp's copen.c) with a transaction held:
// the server ended it with its peer still there, so what it leaves unanswered
// is server_closed, not conn_lost (SPEC §5.4, §14.1).
func TestSecondOpenLeavesItsHeldTransactionServerClosed(t *testing.T) {
	h := newHarness(t, 1)
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
