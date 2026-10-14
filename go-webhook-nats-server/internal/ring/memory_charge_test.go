package ring

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
)

// retained is an item whose JLH is 100 bytes inside a 16 KiB buffer: what a
// small webhook whose header estimate landed in the medium pool class holds
// (SPEC §12.2).
func retained() *envelope.Item {
	return envelope.NewItem("01962a3f-07bc-70b4-ae05-c57e7541d8a9", make([]byte, 100, 16<<10), -1)
}

// SPEC §7.2 (user's decision, 2026-10-14): the memory ring
// charges what the item RETAINS -- cap(Env) plus a fixed per-item cost -- not
// len(Env). Counting the 100 bytes let max_bytes worth of such items hold 160
// times max_bytes of RAM.
func TestMemoryChargesTheRetainedBuffer(t *testing.T) {
	const want = 16<<10 + ItemOverhead
	r := NewMemory(10, 20<<10, 0)
	ctx := context.Background()

	if err := r.Enqueue(ctx, retained()); err != nil {
		t.Fatalf("the first 16 KiB holder should fit in 20 KiB: %v", err)
	}
	if s := r.Stats(); s.Bytes != want {
		t.Fatalf("bytes = %d, want %d (cap of the buffer plus ItemOverhead)", s.Bytes, want)
	}
	// len would say 200 bytes and admit it; the RAM says no.
	if err := r.Enqueue(ctx, retained()); !errors.Is(err, ErrFull) {
		t.Fatalf("a second 16 KiB holder: err = %v, want ErrFull", err)
	}
	if s := r.Stats(); s.Bytes != want || s.Items != 1 {
		t.Fatalf("a refused enqueue left accounting behind: %+v", s)
	}
}

// The value charged is recorded in the item and is what Commit and Drain give
// back, whatever happened to the buffer in between: a Release that ran first
// (it empties Env) must not leave the ring counting bytes nobody holds.
func TestMemoryGivesBackWhatItCharged(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(ctx, retained()); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.Dequeue(ctx, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	got[0].Env = nil // what Release does to the buffer
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 2 || s.Bytes != 2*(16<<10+ItemOverhead) {
		t.Fatalf("after commit = %+v, want 2 items / %d bytes", s, 2*(16<<10+ItemOverhead))
	}
	if out := r.Drain(); len(out) != 2 {
		t.Fatalf("drained %d, want 2", len(out))
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("after drain = %+v, want empty: Enqueue and Drain disagree", s)
	}
}

// The overhead constant must cover at least the item struct and its channel
// slot; the rest of the breakdown is in memory.go.
func TestItemOverheadCoversTheStruct(t *testing.T) {
	need := int64(unsafe.Sizeof(envelope.Item{})) + int64(unsafe.Sizeof((*envelope.Item)(nil)))
	if need > ItemOverhead {
		t.Fatalf("envelope.Item plus its channel slot is %d bytes but ItemOverhead is %d; raise it", need, ItemOverhead)
	}
}

// SPEC §7.2 (triage finding of 2026-10-14): both ceilings
// count the same population -- accepted and not yet committed -- which
// includes the batch a worker took out of the channel and holds while it
// retries. The channel alone admitted a fourth item into a capacity-3 ring
// with one batch held.
func TestMemoryHeldItemsCountAgainstTheCeiling(t *testing.T) {
	r := NewMemory(3, 0, 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(ctx, mkItem(10)); err != nil {
			t.Fatal(err)
		}
	}
	held, err := r.Dequeue(ctx, 1)
	if err != nil || len(held) != 1 {
		t.Fatalf("dequeue = %d %v", len(held), err)
	}
	if err := r.Enqueue(ctx, mkItem(10)); !errors.Is(err, ErrFull) {
		t.Fatalf("with one held: err = %v, want ErrFull", err)
	}
	if s := r.Stats(); s.Items != 3 {
		t.Fatalf("items = %d, want 3", s.Items)
	}
	if err := r.Commit(held); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(ctx, mkItem(10)); err != nil {
		t.Fatalf("after the commit: %v", err)
	}
}
