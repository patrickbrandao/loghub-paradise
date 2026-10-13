package receiver

// Regression tests ported from the independent reviews of go-syslog-pg-server
// and go-relplog-nats-server (2026-10-13): the full-ring pre-check on both
// ceilings and the end of the session checked right before the hand-off
// (SPEC §5.7). Each failed here before its fix.

import (
	"context"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// A full ring is refused before the uuid, the copy and the item. A held RELP
// transaction retries every 50 ms (SPEC §5.7), and the refusal used to cost a
// pooled buffer, a copy and an item, as much as an acceptance. The control is
// the accepted message that fills the ring.
func TestIngestFullRingIsRefusedBeforeTheCopy(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	if out := h.in.Accept(context.Background(), []byte("<13>1 - - a - - - first"), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
	w := tcpWire("10.0.0.3")
	allocs := testing.AllocsPerRun(200, func() {
		w.UUID = uuidv7.UUID{}
		if out := h.in.Accept(context.Background(), []byte("<13>1 - - a - - - second"), w); out != RingFull {
			t.Fatalf("full ring: %v", out)
		}
	})
	if allocs != 0 {
		t.Fatalf("refusing on a full ring allocated %.1f times per message, want 0", allocs)
	}
	if !w.UUID.IsZero() {
		t.Fatal("a uuid was drawn for a message the full ring refused")
	}
}

// The pre-check sees the BYTE ceiling too (ring.FullFor). With ring.max_bytes
// the binding one, a held transaction took a pooled buffer, copied the line
// and built an item on every 50 ms retry, for Enqueue to refuse it.
func TestIngestByteCeilingIsRefusedBeforeTheCopy(t *testing.T) {
	r := ring.NewMemory(100, 2048+ring.ItemOverhead, time.Millisecond) // one 2 KiB item fits
	in := NewIngest(r, newUp(), nil, nil, newMetrics(), logger(), defaultIngestConfig())
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
	in := NewIngest(r, newUp(), nil, nil, newMetrics(), logger(), cfg)
	if out := in.Accept(context.Background(), []byte("<13>"+strings.Repeat("x", 5000)), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("a line truncated to 100 bytes into a ring with room for one 2 KiB item: %v", out)
	}
}

// A closed ring is not "full": the pre-check lets Enqueue say ErrClosed, which
// is the shutdown -- answered with nothing, never with the backpressure of a
// full ring.
func TestIngestClosedFullRingIsTheShutdown(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
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
// an answer for, and the copy in the ring would be a duplicate. The memory
// ring ignores the context, so the pipeline checks it right before the
// hand-off.
func TestIngestEndedSessionIsNotEnqueued(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := h.in.Accept(ctx, []byte("<13>late"), tcpWire("10.0.0.3")); out != Shutdown {
		t.Fatalf("ended session: %v, want %v", out, Shutdown)
	}
	if n := h.ring.Stats().Items; n != 0 {
		t.Fatalf("%d messages enqueued after their session ended", n)
	}
}
