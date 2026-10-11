package ring

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
)

// testFrom is a fixed AgentUUID. The ring never reads it, but ParseHead and the
// contract oracle refuse an envelope without a valid one, and the items here
// must be the real thing the handler hands over.
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// at is one fixed instant for every test envelope. The generator still makes
// each uuid unique -- its free bits are random --, and the envelope's length
// then depends on the body alone, which is what lets the byte-ceiling tests be
// exact instead of approximate.
var at = time.Date(2026, 10, 6, 17, 3, 26, 57932925, time.UTC)

// mkItemBody builds an item around a whole webhook JLH carrying body.
func mkItemBody(body []byte) *envelope.Item {
	h := envelope.Header{UUID: envelope.NewUUID(at), From: testFrom, Space: "default",
		Type: envelope.Type, Level: envelope.Level, SID: 1, Event: envelope.EventAt(at)}
	r := &envelope.Request{Mode: envelope.ModeFast, Src: netip.MustParseAddr("203.0.113.9"),
		Dst: netip.MustParseAddr("127.0.0.1"), SPort: 5555, DPort: 8080,
		PeerAddr: netip.MustParseAddr("203.0.113.9"), PeerPort: 5555, SrcSource: "tcp",
		Proto: "HTTP/1.1", Method: "POST", Host: "example.com", Path: "/x", RawQuery: "a=1",
		Header: http.Header{"Content-Type": {"application/octet-stream"}}, HostHeader: "example.com",
		ContentType: "application/octet-stream", Body: body}
	env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(body))
	return envelope.NewItem(h.UUID.String(), env, -1)
}

// mkItem is mkItemBody with a text body of bodyLen bytes.
func mkItem(bodyLen int) *envelope.Item {
	return mkItemBody(bytes.Repeat([]byte("x"), bodyLen))
}

// ---------------------------------------------------------------- memory ring

func TestMemoryEnqueueDequeue(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := r.Enqueue(ctx, mkItem(10)); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if st := r.Stats(); st.Items != 5 {
		t.Errorf("Items = %d, want 5", st.Items)
	}

	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Errorf("Dequeue returned %d, want 5", len(got))
	}
	// Occupancy only drops at Commit: an item handed to a worker is still the
	// ring's responsibility until the sink confirms it.
	if st := r.Stats(); st.Items != 5 {
		t.Errorf("Items = %d after Dequeue, want 5 until Commit", st.Items)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if st := r.Stats(); st.Items != 0 || st.Bytes != 0 {
		t.Errorf("after Commit: items=%d bytes=%d, want 0 and 0", st.Items, st.Bytes)
	}
}

// SPEC §10.3: Seal is a barrier. Once it returns, every Enqueue that
// succeeded put its item where the Drain after it finds it, and none
// succeeds afterwards. Without the barrier, an Enqueue that passed the closed
// check before Seal sent its item after the Drain: accepted, answered 200,
// and in a ring nobody reads again. The window is a few instructions wide:
// 300 rounds caught the lock-less Enqueue about half the time without -race
// (the review of the port back into go-webhook-nats-server); 3000 caught it
// 20 runs in 20.
func TestMemorySealIsABarrierForEnqueue(t *testing.T) {
	for round := 0; round < 3000; round++ {
		r := NewMemory(1<<16, 0, 0)
		var accepted atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for {
					if err := r.Enqueue(context.Background(), mkItem(8)); err != nil {
						return
					}
					accepted.Add(1)
				}
			}()
		}
		close(start)
		time.Sleep(50 * time.Microsecond)
		r.Seal()
		drained := len(r.Drain())
		wg.Wait()
		if late := len(r.Drain()); late != 0 || int64(drained) != accepted.Load() {
			t.Fatalf("round %d: %d accepted, %d drained at the seal, %d landed after it",
				round, accepted.Load(), drained, late)
		}
	}
}

// SPEC §7.2: two ceilings, whichever is reached first.
func TestMemoryItemCeiling(t *testing.T) {
	r := NewMemory(3, 1<<30, 0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := r.Enqueue(ctx, mkItem(1)); err != nil {
			t.Fatalf("item %d should fit: %v", i, err)
		}
	}
	if err := r.Enqueue(ctx, mkItem(1)); !errors.Is(err, ErrFull) {
		t.Errorf("err = %v, want ErrFull", err)
	}
}

func TestMemoryByteCeiling(t *testing.T) {
	// The bytes counted are the whole JLH, body included (Size = len(Env)).
	// Room for two 1000-byte bodies and one small one, not three large.
	big, small := mkItem(1000).Size(), mkItem(10).Size()
	if small >= big {
		t.Fatalf("test setup: small envelope %d bytes, large %d", small, big)
	}
	r := NewMemory(1000, 2*big+small, 0)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := r.Enqueue(ctx, mkItem(1000)); err != nil {
			t.Fatalf("item %d should fit: %v", i, err)
		}
	}
	if err := r.Enqueue(ctx, mkItem(1000)); !errors.Is(err, ErrFull) {
		t.Errorf("err = %v, want ErrFull on the byte ceiling", err)
	}
	// A rejected item must not leave its bytes behind in the accounting, or the
	// ring would slowly refuse everything.
	if st := r.Stats(); st.Bytes != 2*big {
		t.Errorf("Bytes = %d after a rejection, want %d with the reservation released", st.Bytes, 2*big)
	}
	// A smaller item still fits in what remains.
	if err := r.Enqueue(ctx, mkItem(10)); err != nil {
		t.Errorf("a small item should still fit: %v", err)
	}
}

func TestMemoryDequeueBlocksUntilItem(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := r.Dequeue(ctx, 5)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the context deadline", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Error("Dequeue returned immediately; it should block for an item")
	}
}

// The batch delay must not be paid when there is already a backlog: it should
// cost latency only when latency is cheap.
func TestMemoryBatchDelayNotPaidWhenBacklogged(t *testing.T) {
	r := NewMemory(100, 1<<20, 500*time.Millisecond)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := r.Enqueue(ctx, mkItem(10)); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	got, err := r.Dequeue(ctx, 10)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Errorf("got %d items, want 10", len(got))
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("a full batch took %s; the delay should not apply when items are queued", elapsed)
	}
}

func TestMemoryDrain(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := r.Enqueue(ctx, mkItem(10)); err != nil {
			t.Fatal(err)
		}
	}
	got := r.Drain()
	if len(got) != 4 {
		t.Errorf("Drain returned %d, want 4", len(got))
	}
	if st := r.Stats(); st.Items != 0 || st.Bytes != 0 {
		t.Errorf("Drain must clear the accounting: items=%d bytes=%d", st.Items, st.Bytes)
	}
	if got := r.Drain(); len(got) != 0 {
		t.Errorf("a second Drain returned %d items", len(got))
	}
}

func TestMemoryClosedRejects(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	r.Close()
	if err := r.Enqueue(context.Background(), mkItem(10)); !errors.Is(err, ErrClosed) {
		t.Errorf("err = %v, want ErrClosed", err)
	}
}

func TestMemorySaturated(t *testing.T) {
	// The byte ceiling is far away: this is the item side of high water. Every
	// item is a whole JLH of several hundred bytes, so a tight byte ceiling
	// would refuse before the item count is ever reached.
	r := NewMemory(10, 1<<20, 0)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		if err := r.Enqueue(ctx, mkItem(1)); err != nil {
			t.Fatal(err)
		}
	}
	if r.Stats().Saturated(0.8) {
		t.Error("7 of 10 items should be below the 80% high water mark")
	}
	for i := 0; i < 2; i++ {
		if err := r.Enqueue(ctx, mkItem(1)); err != nil {
			t.Fatal(err)
		}
	}
	if !r.Stats().Saturated(0.8) {
		t.Error("9 of 10 items should be above the 80% high water mark")
	}
}

// The accounting must survive concurrent producers and consumers exactly; a
// drift here would eventually make the ring refuse everything or accept past
// its ceiling. Run with -race.
func TestMemoryConcurrentAccounting(t *testing.T) {
	r := NewMemory(5000, 1<<30, 0)
	ctx := context.Background()

	const producers, each = 8, 500
	var accepted, rejected atomic.Int64

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := r.Enqueue(ctx, mkItem(100)); err != nil {
					rejected.Add(1)
				} else {
					accepted.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if st := r.Stats(); st.Items != accepted.Load() {
		t.Errorf("Items = %d, want the %d accepted", st.Items, accepted.Load())
	}

	var drained int64
	for {
		got, err := r.Dequeue(ctx, 100)
		if err != nil || len(got) == 0 {
			break
		}
		drained += int64(len(got))
		if err := r.Commit(got); err != nil {
			t.Fatal(err)
		}
		if r.Stats().Items == 0 {
			break
		}
	}
	if drained != accepted.Load() {
		t.Errorf("drained %d, want the %d accepted", drained, accepted.Load())
	}
	if st := r.Stats(); st.Items != 0 || st.Bytes != 0 {
		t.Errorf("after a full drain: items=%d bytes=%d, want 0 and 0", st.Items, st.Bytes)
	}
}
