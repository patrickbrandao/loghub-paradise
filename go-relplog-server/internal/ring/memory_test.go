package ring

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// held is what mkItem(i, size) costs the memory ring: mkItem's raw buffer has
// cap == len, plus the per-item overhead.
func held(size int) int64 { return int64(size) + ItemOverhead }

// ---------------------------------------------------------------- memory ring

func TestMemoryItemCeiling(t *testing.T) {
	r := NewMemory(3, 0, 0)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if err := r.Enqueue(context.Background(), mkItem(3, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if s := r.Stats(); s.Items != 3 || s.CapItems != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

// With 200-byte syslog lines the item ceiling binds long before the byte one.
// The byte ceiling earns its place the moment an emitter sends 64 KiB lines.
func TestMemoryByteCeiling(t *testing.T) {
	r := NewMemory(1000, 2*held(60)-1, 0)
	if err := r.Enqueue(context.Background(), mkItem(0, 60)); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(1, 60)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	// The refused item must not have left its bytes behind in the accounting.
	if s := r.Stats(); s.Bytes != held(60) {
		t.Fatalf("bytes = %d, want %d: a refused enqueue leaked accounting", s.Bytes, held(60))
	}
}

// SPEC §9.2, review finding F4: the memory ring counts the memory an item
// HOLDS -- cap(raw) plus the struct -- not len(raw). The receiver copies a line
// into a pooled buffer of its size class, so an 8193-byte line holds a 64 KiB
// buffer; counting 8193 let max_bytes worth of such lines hold eight times
// max_bytes of RAM. Enqueue, Commit and Drain must agree, or the accounting
// drifts with every batch.
func TestMemoryAccountsTheBufferItHolds(t *testing.T) {
	line := bytes.Repeat([]byte{'x'}, 8193)
	mk := func(i int) *item.Item {
		it := mkItem(i, 0)
		it.Raw, _ = item.CopyRaw(line) // what every receiver does
		return it
	}
	big := mk(0)
	if cap(big.Raw) != 64<<10 {
		t.Fatalf("an 8193-byte line got a %d-byte buffer; the pool classes changed", cap(big.Raw))
	}
	const want = 64<<10 + ItemOverhead

	r := NewMemory(100, 3*want, 0)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), mk(i)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if s := r.Stats(); s.Bytes != 3*want {
		t.Fatalf("bytes = %d, want %d (cap of the 64 KiB buffer plus the overhead, x3)", s.Bytes, 3*want)
	}
	// len would say 3 x 8193 = 24 KiB and admit a fourth; the RAM says no.
	if err := r.Enqueue(context.Background(), mk(3)); !errors.Is(err, ErrFull) {
		t.Fatalf("fourth 64 KiB holder: err = %v, want ErrFull", err)
	}

	got, err := r.Dequeue(context.Background(), 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Bytes != 2*want || s.Items != 2 {
		t.Fatalf("after commit = %+v, want 2 items / %d bytes", s, 2*want)
	}
	if out := r.Drain(); len(out) != 2 {
		t.Fatalf("drained %d", len(out))
	}
	if s := r.Stats(); s.Bytes != 0 || s.Items != 0 {
		t.Fatalf("after drain = %+v, want empty: Enqueue and Drain disagree", s)
	}
}

// The overhead constant must cover at least the struct and the channel slot it
// stands for; the breakdown is in memory.go.
func TestItemOverheadCoversTheStruct(t *testing.T) {
	need := int64(unsafe.Sizeof(item.Item{})) + int64(unsafe.Sizeof((*item.Item)(nil)))
	if need > ItemOverhead {
		t.Fatalf("item.Item plus its channel slot is %d bytes but ItemOverhead is %d; raise it", need, ItemOverhead)
	}
}

func TestMemoryCommitReleasesAccounting(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	for i := 0; i < 4; i++ {
		r.Enqueue(context.Background(), mkItem(i, 25))
	}
	got, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(got) != 4 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	r.Commit(got)
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after commit = %+v", s)
	}
}

func TestMemoryDequeueBatchesWithoutWaiting(t *testing.T) {
	r := NewMemory(100, 0, time.Second)
	for i := 0; i < 20; i++ {
		r.Enqueue(context.Background(), mkItem(i, 10))
	}
	start := time.Now()
	got, err := r.Dequeue(context.Background(), 20)
	if err != nil || len(got) != 20 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	// A ring with a backlog fills the batch immediately; the delay must cost
	// nothing when the server is busy.
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("full batch waited %v for the timer", d)
	}
}

func TestMemoryDrain(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	for i := 0; i < 5; i++ {
		r.Enqueue(context.Background(), mkItem(i, 20))
	}
	out := r.Drain()
	if len(out) != 5 {
		t.Fatalf("drained %d", len(out))
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after drain = %+v", s)
	}
	if len(r.Drain()) != 0 {
		t.Fatal("second drain returned items")
	}
}

func TestMemoryOrderIsPreserved(t *testing.T) {
	r := NewMemory(100, 0, 0)
	for i := 0; i < 50; i++ {
		r.Enqueue(context.Background(), mkItem(i, 8))
	}
	got, _ := r.Dequeue(context.Background(), 50)
	for i, it := range got {
		if idx(it) != i {
			t.Fatalf("position %d holds item %d", i, idx(it))
		}
	}
}

func TestMemoryClosedRefuses(t *testing.T) {
	r := NewMemory(10, 0, 0)
	r.Close()
	if err := r.Enqueue(context.Background(), mkItem(0, 10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestMemoryConcurrentEnqueue(t *testing.T) {
	const goroutines, each = 8, 500
	r := NewMemory(goroutines*each, goroutines*each*held(16), 0)
	var wg sync.WaitGroup
	var accepted atomic.Int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if r.Enqueue(context.Background(), mkItem(g*each+i, 16)) == nil {
					accepted.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	if accepted.Load() != goroutines*each {
		t.Fatalf("accepted %d of %d", accepted.Load(), goroutines*each)
	}
	if s := r.Stats(); s.Items != int64(goroutines*each) || s.Bytes != goroutines*each*held(16) {
		t.Fatalf("stats = %+v", s)
	}
}

// Touch is a no-op for memory: nothing else can claim an item held here.
func TestMemoryTouchIsANoOp(t *testing.T) {
	r := NewMemory(10, 0, 0)
	if err := r.Touch([]*item.Item{mkItem(0, 1)}); err != nil {
		t.Fatal(err)
	}
}

// SPEC §9.2: both ceilings count accepted-and-not-committed, so a batch a
// worker holds still takes its slots. The channel alone bounded only what was
// queued, and a capacity-3 ring with one item held accepted a fourth.
func TestMemoryHeldItemsCountAgainstTheCeiling(t *testing.T) {
	r := NewMemory(3, 0, 0)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	held, err := r.Dequeue(context.Background(), 1)
	if err != nil || len(held) != 1 {
		t.Fatalf("dequeue = %d %v", len(held), err)
	}
	if err := r.Enqueue(context.Background(), mkItem(3, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("with one held: err = %v, want ErrFull", err)
	}
	if s := r.Stats(); s.Items != 3 {
		t.Fatalf("items = %d, want 3", s.Items)
	}
	if err := r.Commit(held); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(3, 10)); err != nil {
		t.Fatalf("after the commit: %v", err)
	}
}
