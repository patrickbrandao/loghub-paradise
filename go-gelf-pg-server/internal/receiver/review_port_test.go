package receiver

// Regression tests of go-relplog-pg-server's independent review of 2026-10-13,
// ported (SPEC §5.7; TESTING §9).

import (
	"context"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/ring"
)

// The full-ring pre-check sees the BYTE ceiling too (ring.FullFor). With
// ring.max_bytes the binding one, it saw only the item count, and a TCP reader
// held in the backpressure loop minted a uuid, took a pooled buffer, copied the
// line and built an item on every 50 ms retry, for Enqueue to refuse it (SPEC
// §5.7) -- and a UDP flood did the same per datagram.
func TestIngestByteCeilingIsRefusedBeforeTheCopy(t *testing.T) {
	m := newMetrics()
	r := ring.NewMemory(100, 2048+ring.ItemOverhead, time.Millisecond) // one 2 KiB item fits
	in := NewIngest(r, newUp(), m, logger(), defaultIngestConfig())
	if out := in.Accept(context.Background(), []byte(doc("first")), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
	w := tcpWire("10.0.0.3")
	second := []byte(doc("second"))
	allocs := testing.AllocsPerRun(200, func() {
		w.UUID = uuidv7.UUID{}
		if out := in.Retry(context.Background(), second, w); out != RingFull {
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

// GELF never truncates (SPEC §5.5): a document above max_message_bytes is
// refused as too_large before the ring is even asked, so it neither takes a
// buffer nor counts against the byte ceiling, and the next document that fits
// still gets in.
func TestIngestTooLargeNeverReachesTheRing(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 1024
	m := newMetrics()
	r := ring.NewMemory(100, 2048+ring.ItemOverhead, time.Millisecond)
	in := NewIngest(r, newUp(), m, logger(), cfg)
	if out := in.Accept(context.Background(), []byte(doc(strings.Repeat("x", 5000))), tcpWire("10.0.0.3")); out != TooLarge {
		t.Fatalf("a 5000-byte document with max_message_bytes = 1024: %v, want %v", out, TooLarge)
	}
	if r.Stats().Items != 0 {
		t.Fatal("a refused document reached the ring")
	}
	if out := in.Accept(context.Background(), []byte(doc("fits")), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
}

// A closed ring is not "full": the pre-check lets Enqueue say ErrClosed, which
// is the shutdown -- counted as such, never held in the backpressure loop of a
// full ring.
func TestIngestClosedFullRingIsTheShutdown(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte(doc("filler")), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatal(out)
	}
	h.ring.Close()
	if out := h.in.Accept(context.Background(), []byte(doc("late")), tcpWire("10.0.0.3")); out != Shutdown {
		t.Fatalf("closed and full ring: %v, want %v", out, Shutdown)
	}
}

// What was NOT ported, on purpose: go-relplog-pg-server refuses to enqueue a
// transaction whose session context ended while it was built, because librelp
// retransmits what it never got an answer for. GELF has no answer to
// withhold: a document this server READ is gone from the emitter, and dropping it
// at the hand-off would be a loss, not a deduplication. So a cancelled
// context still enqueues (SPEC §5.7, §13.4 phase 3: what was read finishes
// entering the ring).
func TestIngestCancelledContextStillEnqueues(t *testing.T) {
	h := newHarness(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := h.in.Accept(ctx, []byte(doc("read before the shutdown")), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("a line read before the shutdown: %v, want %v", out, Accepted)
	}
}
