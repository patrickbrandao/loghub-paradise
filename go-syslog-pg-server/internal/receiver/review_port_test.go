package receiver

// Regression tests of go-relplog-pg-server's independent review of 2026-10-13,
// ported (SPEC §5.7; TESTING §9).

import (
	"context"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
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
// is the shutdown -- counted as such, never held in the backpressure loop of a
// full ring.
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

// What was NOT ported, on purpose: go-relplog-pg-server refuses to enqueue a
// transaction whose session context ended while it was built, because librelp
// retransmits what it never got an answer for. Syslog has no answer to
// withhold: a line this server READ is gone from the emitter, and dropping it
// at the hand-off would be a loss, not a deduplication. So a cancelled
// context still enqueues (SPEC §5.7, §13.4 phase 3: what was read finishes
// entering the ring).
func TestIngestCancelledContextStillEnqueues(t *testing.T) {
	h := newHarness(t, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := h.in.Accept(ctx, []byte("<13>read before the shutdown"), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("a line read before the shutdown: %v, want %v", out, Accepted)
	}
}
