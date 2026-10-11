package ring

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/jlhcheck"
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

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
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

// ------------------------------------------------------------------ none ring

type recordingWriter struct {
	mu    sync.Mutex
	items []*envelope.Item
	err   error
	delay time.Duration
}

func (w *recordingWriter) Write(ctx context.Context, items []*envelope.Item) error {
	if w.delay > 0 {
		select {
		case <-time.After(w.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.items = append(w.items, items...)
	return nil
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.items)
}

func TestNoneWritesThrough(t *testing.T) {
	wr := &recordingWriter{}
	r := NewNone(wr, 10, time.Second)

	if err := r.Enqueue(context.Background(), mkItem(10)); err != nil {
		t.Fatal(err)
	}
	if wr.count() != 1 {
		t.Errorf("the writer saw %d items, want 1", wr.count())
	}
}

func TestNonePropagatesWriteError(t *testing.T) {
	sentinel := errors.New("sink is down")
	wr := &recordingWriter{err: sentinel}
	r := NewNone(wr, 10, time.Second)

	if err := r.Enqueue(context.Background(), mkItem(10)); !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want the sink's own error surfaced to the handler", err)
	}
}

// SPEC §7.5: the semaphore is what stops a slow destination from letting
// blocked goroutines grow without limit, each holding a body.
func TestNoneInflightCeiling(t *testing.T) {
	wr := &recordingWriter{delay: 300 * time.Millisecond}
	r := NewNone(wr, 2, time.Second)
	ctx := context.Background()

	var wg sync.WaitGroup
	var full atomic.Int64
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Enqueue(ctx, mkItem(10)); errors.Is(err, ErrFull) {
				full.Add(1)
			}
		}()
	}
	wg.Wait()

	if full.Load() == 0 {
		t.Error("no request was refused; the in-flight ceiling did nothing")
	}
	if st := r.Stats(); st.Items != 0 {
		t.Errorf("in-flight = %d after everything finished, want 0", st.Items)
	}
	if st := r.Stats(); st.CapItems != 2 {
		t.Errorf("CapItems = %d, want the configured ceiling of 2", st.CapItems)
	}
}

// Blocking here would make the drain workers park forever AND make shutdown
// wait out its whole drain_timeout discovering there was never a backlog.
func TestNoneDequeueReturnsImmediately(t *testing.T) {
	r := NewNone(&recordingWriter{}, 10, time.Second)
	start := time.Now()
	_, err := r.Dequeue(context.Background(), 10)
	if !errors.Is(err, ErrNoBacklog) {
		t.Errorf("err = %v, want ErrNoBacklog", err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Error("Dequeue blocked; it must report at once that there is no backlog")
	}
}

// SPEC §7.5: the write is bounded by sink.op_timeout, not by the client. A
// request context that is already cancelled must still let the write run.
func TestNoneWriteIsDetachedFromTheCallerContext(t *testing.T) {
	var sawLive atomic.Bool
	wr := &probingWriter{probe: func(ctx context.Context) error {
		sawLive.Store(ctx.Err() == nil)
		return nil
	}}
	r := NewNone(wr, 10, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Enqueue(ctx, mkItem(10)); err != nil {
		t.Fatalf("Enqueue with a cancelled caller context: %v", err)
	}
	if !sawLive.Load() {
		t.Fatal("the sink saw a cancelled context; the write must be detached from the client")
	}
}

// ... and the timeout is the sink's own clock: a write that never answers ends
// in DeadlineExceeded after op_timeout, which is the one error the handler must
// treat as "buffers still referenced" (SPEC §12.3).
func TestNoneAppliesOpTimeout(t *testing.T) {
	wr := &probingWriter{probe: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	r := NewNone(wr, 10, 30*time.Millisecond)

	start := time.Now()
	err := r.Enqueue(context.Background(), mkItem(10))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded from op_timeout", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("op_timeout was not applied")
	}
}

type probingWriter struct {
	probe func(ctx context.Context) error
}

func (w *probingWriter) Write(ctx context.Context, items []*envelope.Item) error {
	return w.probe(ctx)
}

// ---------------------------------------------------------------- sqlite ring

func newSQLiteRing(t *testing.T, capItems int) *SQLiteRing {
	t.Helper()
	r, err := NewSQLite(SQLiteConfig{
		Path:           filepath.Join(t.TempDir(), "ring.db"),
		Synchronous:    "OFF", // tests care about correctness, not durability
		CommitBatch:    16,
		CommitMaxDelay: 5 * time.Millisecond,
		CapacityItems:  capItems,
		CapacityBytes:  1 << 30,
	}, quietLog())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestSQLiteRoundTrip(t *testing.T) {
	r := newSQLiteRing(t, 1000)
	ctx := context.Background()

	in := mkItem(32)
	id, env := in.ID, bytes.Clone(in.Env)
	if err := r.Enqueue(ctx, in); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("Dequeue returned %d items, want 1", len(got))
	}

	// The regression that mattered: an item read back must be the item that
	// went in. Losing the id here made every event collide on the destination's
	// primary key, and 149 of 150 were silently dropped.
	it := got[0]
	if it.ID != id {
		t.Errorf("id = %q, want %q -- the identity must survive the ring", it.ID, id)
	}
	// The JLH is the whole event, body included: nothing is promoted to a
	// column and rebuilt, so the bytes must be the ones the client was answered
	// for (SPEC §7.4).
	if !bytes.Equal(it.Env, env) {
		t.Errorf("the JLH changed in the ring:\n got %s\nwant %s", it.Env, env)
	}
	h, _, err := envelope.ParseHead(it.Env)
	if err != nil || h.UUID.String() != it.ID {
		t.Errorf("ParseHead = %v, %v; want the uuid column to be the envelope's uuid %s", h.UUID, err, it.ID)
	}
	if it.Size() != int64(len(env)) {
		t.Errorf("Size = %d, want len(JLH) = %d", it.Size(), len(env))
	}
	if it.RowID == 0 {
		t.Error("RowID was not set; Commit could not delete the row")
	}
}

func TestSQLiteEnqueueIsDurableBeforeReturn(t *testing.T) {
	r := newSQLiteRing(t, 1000)
	ctx := context.Background()

	// The whole point of this mode: when Enqueue returns, the row is committed.
	// And the JLH is back in its pool: the row is the event now (SPEC §12.3).
	for i := 0; i < 50; i++ {
		it := mkItem(16)
		if err := r.Enqueue(ctx, it); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
		if !it.Released() {
			t.Fatalf("Enqueue %d: committed item not released", i)
		}
	}
	var n int
	if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM jlh_ring`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Errorf("%d rows committed, want 50 -- Enqueue returned before the COMMIT", n)
	}
}

// SPEC §7.4: once the item is with the writer, Enqueue reports the COMMIT's
// result even if the request context was cancelled meanwhile. Returning early
// would let the handler pool buffers the writer is about to INSERT -- with
// -race, this test is where that would show.
func TestSQLiteEnqueueWaitsOutACancelledContext(t *testing.T) {
	r := newSQLiteRing(t, 1000)

	// Ten at once stays under the pending channel (CommitBatch*2): a full
	// channel is a legitimate ErrFull, not the behaviour under test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	items := make([]*envelope.Item, 0, 10)
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < 10; i++ {
		it := mkItem(64)
		items = append(items, it)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Enqueue(ctx, it); err != nil {
				errs.Add(1)
			}
		}()
	}
	wg.Wait()

	if errs.Load() != 0 {
		t.Fatalf("%d of 10 enqueues failed on a cancelled context; the COMMIT result must be awaited", errs.Load())
	}
	for i, it := range items {
		if it.RowID == 0 {
			t.Fatalf("item %d has no RowID; Enqueue returned before its group commit", i)
		}
	}
	var n int
	if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM jlh_ring`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Errorf("%d rows committed, want 10", n)
	}
}

// Closing the ring with a hand-off in flight must not strand the handler: the
// item is either acknowledged by the writer's final flush or refused outright.
func TestSQLiteCloseDoesNotStrandAWaitingHandler(t *testing.T) {
	r := newSQLiteRing(t, 1000)

	done := make(chan error, 1)
	go func() { done <- r.Enqueue(context.Background(), mkItem(16)) }()

	// Let the hand-off happen, then close underneath it.
	time.Sleep(2 * time.Millisecond)
	r.Close()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want nil (flushed) or ErrClosed (never picked up)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Enqueue blocked forever after Close")
	}
}

func TestSQLiteCommitDeletesRows(t *testing.T) {
	r := newSQLiteRing(t, 1000)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if err := r.Enqueue(ctx, mkItem(16)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.Dequeue(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM jlh_ring`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			if st := r.Stats(); st.Items != 0 {
				t.Errorf("Items = %d after Commit, want 0", st.Items)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("rows were not deleted within the deadline")
}

// A row handed to one worker must never be handed to another, or the same event
// is delivered twice by two workers at once.
func TestSQLiteNoDoubleDelivery(t *testing.T) {
	r := newSQLiteRing(t, 1000)
	ctx := context.Background()

	for i := 0; i < 60; i++ {
		if err := r.Enqueue(ctx, mkItem(8)); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[int64]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var dup, total atomic.Int64

	// A bounded context is required, not incidental: once every row is with a
	// worker, Dequeue correctly blocks waiting for new ones, because a row
	// already handed out must never be handed out again.
	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				items, err := r.Dequeue(dctx, 8)
				if err != nil {
					return
				}
				mu.Lock()
				for _, it := range items {
					if seen[it.RowID] {
						dup.Add(1)
					}
					seen[it.RowID] = true
				}
				mu.Unlock()
				total.Add(int64(len(items)))
			}
		}()
	}
	wg.Wait()

	if dup.Load() > 0 {
		t.Errorf("%d rows were handed out more than once", dup.Load())
	}
	if total.Load() != 60 {
		t.Errorf("workers took %d rows in total, want each of the 60 exactly once", total.Load())
	}
}

func TestSQLiteBacklogSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ring.db")
	cfg := SQLiteConfig{
		Path: path, Synchronous: "OFF", CommitBatch: 8,
		CommitMaxDelay: 5 * time.Millisecond,
		CapacityItems:  1000, CapacityBytes: 1 << 30,
	}

	r1, err := NewSQLite(cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 25; i++ {
		it := mkItem(16)
		ids = append(ids, it.ID)
		if err := r1.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening is what a restart after a crash does; the rows are the backlog.
	r2, err := NewSQLite(cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	if got := r2.Backlog(); got != 25 {
		t.Errorf("Backlog = %d, want 25", got)
	}
	items, err := r2.Dequeue(context.Background(), 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 25 {
		t.Errorf("recovered %d items, want 25", len(items))
	}
	for i, it := range items {
		if i < len(ids) && it.ID != ids[i] {
			t.Errorf("recovered item %d has id %q, want %q -- in order, identity intact", i, it.ID, ids[i])
			break
		}
	}
}

func TestSQLiteItemCeiling(t *testing.T) {
	r := newSQLiteRing(t, 5)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := r.Enqueue(ctx, mkItem(8)); err != nil {
			t.Fatalf("item %d should fit: %v", i, err)
		}
	}
	if err := r.Enqueue(ctx, mkItem(8)); !errors.Is(err, ErrFull) {
		t.Errorf("err = %v, want ErrFull", err)
	}
}

func TestSQLiteClosedRejects(t *testing.T) {
	r := newSQLiteRing(t, 100)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(8)); !errors.Is(err, ErrClosed) {
		t.Errorf("err = %v, want ErrClosed", err)
	}
	// Close must be idempotent: shutdown paths call it more than once.
	if err := r.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// SPEC §7.4: a row committed before the 200 is the event, whole, after a
// crash. The next boot hands the drain the same uuid and the same JLH bytes the
// client was answered for, so a destination that deduplicates by uuid sees one
// event, never a variant of it.
//
// The crash is a copy of ring.db and its WAL taken while the ring is still open
// -- no Close, no checkpoint --, which is the file state a SIGKILL leaves behind.
// Opening the copy runs SQLite's own WAL recovery, the path a real restart takes.
// TestSQLiteBacklogSurvivesReopen covers the graceful close.
func TestSQLiteJLHSurvivesACrash(t *testing.T) {
	cfg := SQLiteConfig{
		Path: filepath.Join(t.TempDir(), "ring.db"), Synchronous: "OFF", CommitBatch: 8,
		CommitMaxDelay: 5 * time.Millisecond, CapacityItems: 1000, CapacityBytes: 1 << 30,
	}
	r1, err := NewSQLite(cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r1.Close() })

	// Text and binary bodies: the second kind travels in base64, and both must
	// come back byte for byte.
	type sent struct {
		id  string
		env []byte
	}
	var want []sent
	var wantBytes int64
	for i := 0; i < 25; i++ {
		body := bytes.Repeat([]byte("y"), i*37)
		if i%2 == 1 {
			body = append(body, 0x00, '\n', 0xff, '\r')
		}
		it := mkItemBody(body)
		want = append(want, sent{it.ID, bytes.Clone(it.Env)})
		wantBytes += it.Size()
		if err := r1.Enqueue(context.Background(), it); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	crashed := filepath.Join(t.TempDir(), "ring.db")
	for _, suffix := range []string{"", "-wal"} {
		b, err := os.ReadFile(cfg.Path + suffix)
		if err != nil {
			t.Fatalf("snapshot of ring.db%s: %v", suffix, err)
		}
		if err := os.WriteFile(crashed+suffix, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	cfg.Path = crashed
	r2, err := NewSQLite(cfg, quietLog())
	if err != nil {
		t.Fatalf("reopening after the crash: %v", err)
	}
	defer r2.Close()

	if st := r2.Stats(); st.Items != int64(len(want)) || st.Bytes != wantBytes {
		t.Errorf("recovered accounting: items=%d bytes=%d, want %d and %d",
			st.Items, st.Bytes, len(want), wantBytes)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := r2.Dequeue(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(want) {
		t.Fatalf("recovered %d items, want %d", len(items), len(want))
	}
	for i, it := range items {
		if it.ID != want[i].id {
			t.Errorf("item %d: uuid %q, want %q", i, it.ID, want[i].id)
		}
		if !bytes.Equal(it.Env, want[i].env) {
			t.Errorf("item %d: the JLH changed across the crash", i)
		}
		e, err := jlhcheck.Validate(it.Env)
		if err != nil {
			t.Errorf("item %d: the recovered JLH breaks the contract: %v", i, err)
			continue
		}
		if e.UUID != it.ID {
			t.Errorf("item %d: envelope uuid %s, uuid column %s", i, e.UUID, it.ID)
		}
	}
}

// ------------------------------------------------------- the v1 ring table

// v1RingSchema is the ring table of the server before the LogHub envelope (as
// still kept by go-webhook-fast-server): the old envelope and the body apart,
// plus the promoted columns. legacyRing looks at the name alone, but a faithful
// copy keeps these tests about the file an upgrade actually finds on disk.
const v1RingSchema = `
CREATE TABLE ring (
  rowid_    INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id  TEXT    NOT NULL,
  ts        INTEGER NOT NULL,
  nts       INTEGER NOT NULL,
  method    TEXT    NOT NULL,
  host      TEXT    NOT NULL,
  path      TEXT    NOT NULL,
  query     TEXT    NOT NULL,
  proto     TEXT    NOT NULL,
  src_ip    TEXT    NOT NULL,
  src_ver   INTEGER NOT NULL,
  env       BLOB    NOT NULL,
  body      BLOB    NOT NULL,
  nbytes    INTEGER NOT NULL
) STRICT;
`

// makeV1Ring writes a ring.db as the previous version leaves it, with rows
// events still waiting in the old format.
func makeV1Ring(t *testing.T, path string, rows int) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(v1RingSchema); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e%02d", i)
		env := []byte(`{"id":"` + id + `","ts":1757116800}`)
		body := []byte{byte(i), 0x00, 0xff}
		if _, err := db.Exec(`INSERT INTO ring (event_id, ts, nts, method, host, path, query, proto,
			src_ip, src_ver, env, body, nbytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, 1757116800, 1757116800000000000, "POST", "h", "/p", "",
			"HTTP/1.1", "203.0.113.9", 4, env, body, len(env)+len(body)); err != nil {
			t.Fatal(err)
		}
	}
}

// v1Rows reads the v1 table back as one string, for an exact before/after
// comparison; "" when the table does not exist.
func v1Rows(t *testing.T, path string) (string, bool) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !tableExists(t, db, "ring") {
		return "", false
	}
	rows, err := db.Query(`SELECT rowid_, event_id, env, body, nbytes FROM ring ORDER BY rowid_`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var (
			id, n     int64
			event     string
			env, body []byte
		)
		if err := rows.Scan(&id, &event, &env, &body, &n); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%d|%s|%q|%x|%d\n", id, event, env, body, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sb.String(), true
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`,
		name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// logBuffer collects a ring's log lines; the writer goroutine and the test read
// it concurrently.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func capturingLog() (*slog.Logger, *logBuffer) {
	buf := &logBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func ringConfig(path string) SQLiteConfig {
	return SQLiteConfig{Path: path, Synchronous: "OFF", CommitBatch: 8,
		CommitMaxDelay: 5 * time.Millisecond, CapacityItems: 1000, CapacityBytes: 1 << 30}
}

// roundTrip proves the jlh_ring works: one item in, the same item out, and the
// row gone after Commit.
func roundTrip(t *testing.T, r *SQLiteRing) {
	t.Helper()
	in := mkItem(16)
	id, env := in.ID, bytes.Clone(in.Env)
	if err := r.Enqueue(context.Background(), in); err != nil {
		t.Fatalf("Enqueue on the new ring: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != id || !bytes.Equal(got[0].Env, env) {
		t.Fatalf("Dequeue returned %d items; want exactly the one enqueued (%s)", len(got), id)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var n int
		if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM jlh_ring`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d rows left in jlh_ring after Commit", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// SPEC §7.4: an empty v1 table holds nothing anyone could lose, so the boot
// drops it and the ring carries on in jlh_ring.
func TestSQLiteDropsAnEmptyV1Table(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	makeV1Ring(t, path, 0)
	if _, ok := v1Rows(t, path); !ok {
		t.Fatal("test setup: the v1 table was not created")
	}

	log, buf := capturingLog()
	r, err := NewSQLite(ringConfig(path), log)
	if err != nil {
		t.Fatalf("NewSQLite over an empty v1 table: %v", err)
	}
	t.Cleanup(func() { r.Close() })

	if tableExists(t, r.readDB, "ring") {
		t.Error("the empty v1 table is still there; it should have been dropped")
	}
	if !tableExists(t, r.readDB, "jlh_ring") {
		t.Error("jlh_ring was not created")
	}
	out := buf.String()
	if !strings.Contains(out, "dropped the empty pre-LogHub table") {
		t.Errorf("no log line for the drop:\n%s", out)
	}
	if strings.Contains(out, "level=ERROR") {
		t.Errorf("an empty v1 table is not an error:\n%s", out)
	}
	roundTrip(t, r)
}

// SPEC §7.4: a v1 table with rows is left exactly as it is -- nothing lost,
// nothing mixed into the LogHub ring, nothing delivered in a format no
// destination understands -- and the boot says so with an ERROR and the count.
// The new ring works beside it, and a second boot still does not touch it: the
// rows wait for the previous version to drain them.
func TestSQLiteLeavesAPopulatedV1TableUntouched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	makeV1Ring(t, path, 3)
	before, _ := v1Rows(t, path)
	if strings.Count(before, "\n") != 3 {
		t.Fatalf("test setup: v1 rows = %q", before)
	}

	for boot := 1; boot <= 2; boot++ {
		log, buf := capturingLog()
		r, err := NewSQLite(ringConfig(path), log)
		if err != nil {
			t.Fatalf("boot %d: NewSQLite refused a populated v1 table: %v", boot, err)
		}
		out := buf.String()
		if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "events=3") ||
			!strings.Contains(out, "pre-LogHub") {
			t.Errorf("boot %d: want an ERROR with the 3 waiting events:\n%s", boot, out)
		}
		if strings.Contains(out, "dropped") {
			t.Errorf("boot %d: a table with rows must never be dropped:\n%s", boot, out)
		}
		// None of it is backlog: counting it would make the drain wait for rows
		// it can never read.
		if st := r.Stats(); st.Items != 0 || st.Bytes != 0 || r.Backlog() != 0 {
			t.Errorf("boot %d: the v1 rows leaked into the accounting: %+v", boot, st)
		}
		roundTrip(t, r)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}

		after, ok := v1Rows(t, path)
		if !ok {
			t.Fatalf("boot %d: the v1 table with rows was dropped", boot)
		}
		if after != before {
			t.Fatalf("boot %d: the v1 rows changed:\n got %q\nwant %q", boot, after, before)
		}
	}
}
