package ring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func item(i int, size int) *envelope.Item {
	raw := make([]byte, size)
	for j := range raw {
		raw[j] = 'a'
	}
	h := envelope.Head{
		ID:        fmt.Sprintf("id-%06d", i),
		TS:        1757116800,
		NTS:       int64(i),
		Transport: envelope.TransportTCP,
		Framing:   envelope.FramingLF,
		Listener:  ":1514",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  uint16(40000 + i%1000),
		PRI:       165,
		FromWire:  true,
	}
	return envelope.NewItem(h, raw, -1)
}

// ---------------------------------------------------------------- memory ring

func TestMemoryItemCeiling(t *testing.T) {
	r := NewMemory(3, 0, 0)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), item(i, 10)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if err := r.Enqueue(context.Background(), item(3, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if s := r.Stats(); s.Items != 3 || s.CapItems != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

// With 200-byte syslog lines the item ceiling binds long before the byte one.
// The byte ceiling earns its place the moment an emitter sends 64 KiB lines.
func TestMemoryByteCeiling(t *testing.T) {
	r := NewMemory(1000, 100, 0)
	if err := r.Enqueue(context.Background(), item(0, 60)); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), item(1, 60)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	// The refused item must not have left its bytes behind in the accounting.
	if s := r.Stats(); s.Bytes != 60 {
		t.Fatalf("bytes = %d, want 60: a refused enqueue leaked accounting", s.Bytes)
	}
}

func TestMemoryCommitReleasesAccounting(t *testing.T) {
	r := NewMemory(10, 1000, 0)
	for i := 0; i < 4; i++ {
		r.Enqueue(context.Background(), item(i, 25))
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
		r.Enqueue(context.Background(), item(i, 10))
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
	r := NewMemory(10, 1000, 0)
	for i := 0; i < 5; i++ {
		r.Enqueue(context.Background(), item(i, 20))
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
		r.Enqueue(context.Background(), item(i, 8))
	}
	got, _ := r.Dequeue(context.Background(), 50)
	for i, it := range got {
		if it.Head.ID != fmt.Sprintf("id-%06d", i) {
			t.Fatalf("position %d holds %s", i, it.Head.ID)
		}
	}
}

func TestMemoryClosedRefuses(t *testing.T) {
	r := NewMemory(10, 0, 0)
	r.Close()
	if err := r.Enqueue(context.Background(), item(0, 10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestMemoryConcurrentEnqueue(t *testing.T) {
	const goroutines, each = 8, 500
	r := NewMemory(goroutines*each, int64(goroutines*each*16), 0)
	var wg sync.WaitGroup
	var accepted atomic.Int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if r.Enqueue(context.Background(), item(g*each+i, 16)) == nil {
					accepted.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	if accepted.Load() != goroutines*each {
		t.Fatalf("accepted %d of %d", accepted.Load(), goroutines*each)
	}
	if s := r.Stats(); s.Items != int64(goroutines*each) || s.Bytes != int64(goroutines*each*16) {
		t.Fatalf("stats = %+v", s)
	}
}

// ------------------------------------------------------------------ none ring

type fakeWriter struct {
	mu      sync.Mutex
	written int
	err     error
	delay   time.Duration
	release chan struct{}
}

func (w *fakeWriter) Write(ctx context.Context, items []*envelope.Item) error {
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.written += len(items)
	return nil
}

func TestNoneWritesThrough(t *testing.T) {
	w := &fakeWriter{}
	r := NewNone(w, 4, time.Second)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), item(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	if w.written != 3 {
		t.Fatalf("written = %d", w.written)
	}
	if s := r.Stats(); s.Items != 0 || s.CapItems != 4 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestNoneSurfacesSinkError(t *testing.T) {
	want := errors.New("destination refused")
	r := NewNone(&fakeWriter{err: want}, 2, time.Second)
	if err := r.Enqueue(context.Background(), item(0, 10)); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

// The semaphore is the only backpressure this mode has. Without it a slow
// destination lets blocked goroutines grow without limit, each holding a line.
func TestNoneSemaphoreIsFull(t *testing.T) {
	w := &fakeWriter{release: make(chan struct{})}
	r := NewNone(w, 2, time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); r.Enqueue(context.Background(), item(i, 10)) }(i)
	}
	// Wait for both slots to be taken.
	deadline := time.Now().Add(2 * time.Second)
	for r.Stats().Items < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := r.Stats().Items; got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}
	if err := r.Enqueue(context.Background(), item(9, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	close(w.release)
	wg.Wait()
}

// Blocking here would make every drain worker sit forever and would make the
// shutdown drain wait out its whole timeout before discovering there is nothing
// to drain.
func TestNoneDequeueReturnsAtOnce(t *testing.T) {
	r := NewNone(&fakeWriter{}, 2, time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := r.Dequeue(context.Background(), 10); !errors.Is(err, ErrNoBacklog) {
			t.Errorf("err = %v, want ErrNoBacklog", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dequeue blocked")
	}
	if len(r.Drain()) != 0 {
		t.Fatal("Drain returned items")
	}
}

// ---------------------------------------------------------------- sqlite ring

// SPEC §9.5: the write is bounded by sink.op_timeout, not by the receiver. A
// receive context that is already cancelled must still let the write run.
func TestNoneWriteIsDetachedFromTheCallerContext(t *testing.T) {
	var sawLive atomic.Bool
	w := &probingWriter{probe: func(ctx context.Context) error {
		sawLive.Store(ctx.Err() == nil)
		return nil
	}}
	r := NewNone(w, 4, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Enqueue(ctx, item(0, 10)); err != nil {
		t.Fatalf("Enqueue with a cancelled receive context: %v", err)
	}
	if !sawLive.Load() {
		t.Fatal("the sink saw a cancelled context; the write must be detached from the receiver")
	}
}

// ... and the timeout is the sink's own clock: a write that never answers ends
// in DeadlineExceeded after op_timeout, which is the one error the receiver
// must treat as "buffer still referenced" (SPEC §14.2).
func TestNoneAppliesOpTimeout(t *testing.T) {
	w := &probingWriter{probe: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	r := NewNone(w, 4, 30*time.Millisecond)

	start := time.Now()
	err := r.Enqueue(context.Background(), item(0, 10))
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

func newSQLiteRing(t *testing.T, capItems int, capBytes int64) (*SQLiteRing, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ring.db")
	r, err := NewSQLite(SQLiteConfig{
		Path:           path,
		Synchronous:    "OFF", // tests must not pay an fsync per commit
		CommitBatch:    8,
		CommitMaxDelay: 2 * time.Millisecond,
		CapacityItems:  capItems,
		CapacityBytes:  capBytes,
	}, discard())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r, path
}

func TestSQLiteEnqueueWaitsForCommit(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	it := item(0, 40)
	if err := r.Enqueue(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	// Enqueue returned, so the COMMIT is done and the row has an identity.
	if it.RowID == 0 {
		t.Fatal("Enqueue returned before the row existed")
	}
	if s := r.Stats(); s.Items != 1 || s.Bytes != 40 {
		t.Fatalf("stats = %+v", s)
	}
}

// Losing an item's identity on the way back out is how the sibling service once
// discarded 149 of 150 events to a primary-key collision at the destination.
func TestSQLiteRoundTripPreservesTheWholeItem(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	orig := item(42, 64)
	orig.Head.Truncated = true
	orig.Head.PRI = 190
	if err := r.Enqueue(context.Background(), orig); err != nil {
		t.Fatal(err)
	}

	got, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	back := got[0]
	if back.Head != orig.Head {
		t.Fatalf("head changed\n got %+v\nwant %+v", back.Head, orig.Head)
	}
	if string(back.Raw) != string(orig.Raw) {
		t.Fatal("raw line changed")
	}
}

// SPEC §9.4: once the item is with the writer, Enqueue reports the COMMIT's
// result even if the receive context was cancelled meanwhile -- the shape of
// phase 3 of the shutdown. Returning early would let the receiver pool a buffer
// the writer is about to INSERT; with -race, this test is where that shows.
func TestSQLiteEnqueueWaitsOutACancelledContext(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)

	// Ten at once stays under the pending channel (CommitBatch*2): a full
	// channel is a legitimate ErrFull, not the behaviour under test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	items := make([]*envelope.Item, 0, 10)
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < 10; i++ {
		it := item(i, 64)
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
	if s := r.Stats(); s.Items != 10 {
		t.Fatalf("stats = %+v, want 10 items committed", s)
	}
}

// Closing the ring with a hand-off in flight must not strand the receiver: the
// item is either acknowledged by the writer's final flush or refused outright.
func TestSQLiteCloseDoesNotStrandAWaitingReceiver(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)

	done := make(chan error, 1)
	go func() { done <- r.Enqueue(context.Background(), item(0, 16)) }()

	time.Sleep(time.Millisecond)
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

// SPEC §9.4: a row whose head does not decode leaves the accounting at once and
// is moved to the quarantine table by the writer. Left in place and still
// counted, it made the boot drain wait forever for a counter that never reached
// zero, every transport closed.
func TestSQLiteUndecodableHeadIsQuarantined(t *testing.T) {
	var quarantined atomic.Int64
	r, err := NewSQLite(SQLiteConfig{
		Path:           filepath.Join(t.TempDir(), "ring.db"),
		Synchronous:    "OFF",
		CommitBatch:    8,
		CommitMaxDelay: 2 * time.Millisecond,
		CapacityItems:  1000,
		Quarantined:    func(n int) { quarantined.Add(int64(n)) },
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })

	// A good row, a poisoned row, a good row -- as a previous run could have
	// left them.
	if err := r.Enqueue(context.Background(), item(0, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.writeDB.Exec(`INSERT INTO ring (id, head, raw, nbytes) VALUES (?, ?, ?, ?)`,
		"poison", []byte("{not json"), []byte("raw line"), 8); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), item(2, 10)); err != nil {
		t.Fatal(err)
	}
	if err := r.Recount(); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 3 || s.Bytes != 28 {
		t.Fatalf("stats before = %+v", s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("dequeued %d items, want the 2 good ones", len(got))
	}
	if s := r.Stats(); s.Items != 2 || s.Bytes != 20 {
		t.Fatalf("stats after the skip = %+v; the poisoned row must leave the accounting at once", s)
	}
	if quarantined.Load() != 1 {
		t.Fatalf("quarantined callback = %d, want 1", quarantined.Load())
	}

	// The writer moves the row: gone from ring, present in quarantine, bytes
	// intact, reason recorded.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var inRing, inQuarantine int
		_ = r.readDB.QueryRow(`SELECT COUNT(*) FROM ring WHERE id = 'poison'`).Scan(&inRing)
		_ = r.readDB.QueryRow(`SELECT COUNT(*) FROM quarantine WHERE id = 'poison'`).Scan(&inQuarantine)
		if inRing == 0 && inQuarantine == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("poisoned row: ring=%d quarantine=%d, want 0 and 1", inRing, inQuarantine)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var reason string
	var raw []byte
	if err := r.readDB.QueryRow(`SELECT reason, raw FROM quarantine WHERE id = 'poison'`).Scan(&reason, &raw); err != nil {
		t.Fatal(err)
	}
	if reason == "" || string(raw) != "raw line" {
		t.Fatalf("quarantine row: reason=%q raw=%q", reason, raw)
	}

	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after commit = %+v, want empty", s)
	}
}

func TestSQLiteCursorNeverRedelivers(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	const n = 60
	for i := 0; i < n; i++ {
		if err := r.Enqueue(context.Background(), item(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]int{}
	for got := 0; got < n; {
		items, err := r.Dequeue(context.Background(), 7)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			seen[it.Head.ID]++
		}
		got += len(items)
	}
	if len(seen) != n {
		t.Fatalf("saw %d distinct items, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s delivered %d times", id, c)
		}
	}
}

// The drain must not stall when workers x batch exceeds any fixed read window:
// that is the deadlock a cursor exists to avoid, and it happened in the DEFAULT
// configuration of the sibling service.
func TestSQLiteConcurrentDrainDoesNotStall(t *testing.T) {
	r, _ := newSQLiteRing(t, 10000, 0)
	const n = 400
	for i := 0; i < n; i++ {
		if err := r.Enqueue(context.Background(), item(i, 16)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		mu    sync.Mutex
		seen  = map[string]int{}
		total atomic.Int64
		wg    sync.WaitGroup
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for total.Load() < n && ctx.Err() == nil {
				items, err := r.Dequeue(ctx, 256)
				if err != nil {
					return
				}
				mu.Lock()
				for _, it := range items {
					seen[it.Head.ID]++
				}
				mu.Unlock()
				total.Add(int64(len(items)))
				r.Commit(items)
			}
		}()
	}
	wg.Wait()

	if int(total.Load()) != n {
		t.Fatalf("drained %d of %d", total.Load(), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("%s delivered %d times", id, c)
		}
	}
}

func TestSQLiteBacklogSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	cfg := SQLiteConfig{Path: path, Synchronous: "OFF", CommitBatch: 4,
		CommitMaxDelay: 2 * time.Millisecond, CapacityItems: 1000}

	r, err := NewSQLite(cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := r.Enqueue(context.Background(), item(i, 32)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	r2, err := NewSQLite(cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Backlog() != 10 {
		t.Fatalf("backlog = %d, want 10", r2.Backlog())
	}
	// The recount must agree with what the live accounting would have said, or
	// the byte ceiling behaves differently after a crash than before it.
	if s := r2.Stats(); s.Bytes != 320 {
		t.Fatalf("recounted bytes = %d, want 320", s.Bytes)
	}
	got, err := r2.Dequeue(context.Background(), 20)
	if err != nil || len(got) != 10 {
		t.Fatalf("dequeue after reopen = %d %v", len(got), err)
	}
	if got[0].Head.ID != "id-000000" {
		t.Fatalf("order lost across reopen: first is %s", got[0].Head.ID)
	}
}

func TestSQLiteCommitDeletesRows(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	for i := 0; i < 10; i++ {
		r.Enqueue(context.Background(), item(i, 16))
	}
	got, _ := r.Dequeue(context.Background(), 10)
	r.Commit(got)

	// Commit drops the accounting at once but hands the DELETE to the writer
	// goroutine, because only one goroutine may write. So the rows outlive the
	// accounting for a moment, and a crash in that window redelivers them --
	// the correct at-least-once bias, since every sink writes idempotently on
	// the id.
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after commit = %+v", s)
	}
	deadline := time.Now().Add(5 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%d rows left in the queue table after 5s", n)
}

func TestSQLiteCeilings(t *testing.T) {
	r, _ := newSQLiteRing(t, 2, 0)
	for i := 0; i < 2; i++ {
		if err := r.Enqueue(context.Background(), item(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Enqueue(context.Background(), item(2, 16)); !errors.Is(err, ErrFull) {
		t.Fatalf("item ceiling: err = %v", err)
	}

	r2, _ := newSQLiteRing(t, 1000, 40)
	if err := r2.Enqueue(context.Background(), item(0, 30)); err != nil {
		t.Fatal(err)
	}
	if err := r2.Enqueue(context.Background(), item(1, 30)); !errors.Is(err, ErrFull) {
		t.Fatalf("byte ceiling: err = %v", err)
	}
}

// Every mode must satisfy the same interface, so app/ never branches on mode.
func TestAllModesImplementRing(t *testing.T) {
	var _ Ring = NewMemory(1, 0, 0)
	var _ Ring = NewNone(&fakeWriter{}, 1, time.Second)
	r, _ := newSQLiteRing(t, 1, 0)
	var _ Ring = r
}

func TestSaturated(t *testing.T) {
	s := Stats{Items: 80, CapItems: 100}
	if !s.Saturated(0.80) {
		t.Error("80/100 not saturated at 0.80")
	}
	if s.Saturated(0.81) {
		t.Error("80/100 saturated at 0.81")
	}
	b := Stats{Bytes: 90, CapBytes: 100}
	if !b.Saturated(0.80) {
		t.Error("byte saturation not detected")
	}
	// ring=none reports no byte ceiling; that must not read as saturated.
	if (Stats{Items: 0, CapItems: 2048}).Saturated(0.80) {
		t.Error("empty ring reported saturated")
	}
}
