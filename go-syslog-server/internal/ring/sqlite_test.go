package ring

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

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
	it := mkItem(0, 40)
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
// The item that leaves must be the item that entered: same uuid, same clock
// sample, same addresses, same TLS session, same original size (SPEC §3.5).
func TestSQLiteRoundTripPreservesTheWholeItem(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	orig := []*item.Item{
		mkTLSItem(42, "<190>1 2026-10-03T12:00:00Z h a - - - linha \x00\xff crua"),
		mkItem(43, 64),
	}
	for _, it := range orig {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	got, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	for i, back := range got {
		if !reflect.DeepEqual(back.Head, orig[i].Head) {
			t.Fatalf("head changed\n got %+v\nwant %+v", back.Head, orig[i].Head)
		}
		if string(back.Raw) != string(orig[i].Raw) {
			t.Fatal("raw line changed")
		}
		if back.RowID == 0 {
			t.Fatal("dequeued item without its row id; Commit could not delete it")
		}
	}
	// The uuid column is the head's uuid, verifiable without decoding.
	var col string
	if err := r.readDB.QueryRow(`SELECT uuid FROM ring WHERE seq = ?`, got[0].RowID).Scan(&col); err != nil {
		t.Fatal(err)
	}
	if col != orig[0].Head.UUID.String() {
		t.Fatalf("uuid column = %q, want %q", col, orig[0].Head.UUID.String())
	}
}

// The raw line of a dequeued item is the item's own buffer: releasing it must
// not disturb another item, and the bytes must survive the next read of the
// driver's row buffer.
func TestSQLiteDequeuedItemsOwnTheirRaw(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	a, b := mkItem(0, 30), mkItem(1, 30)
	b.Raw = []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	for _, it := range []*item.Item{a, b} {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	if string(got[0].Raw) != string(a.Raw) || string(got[1].Raw) != string(b.Raw) {
		t.Fatalf("raw lines mixed up: %q %q", got[0].Raw, got[1].Raw)
	}
	got[0].Release()
	if string(got[1].Raw) != string(b.Raw) {
		t.Fatal("releasing one item changed another's raw line")
	}
}

// SPEC §9.4: once the item is with the writer, Enqueue reports the COMMIT's
// result even if the receive context was cancelled meanwhile -- the shape of
// phase 3 of the shutdown (SPEC §13.4). Returning early would let the receiver
// pool a buffer the writer is about to INSERT; with -race, this test is where
// that shows.
func TestSQLiteEnqueueWaitsOutACancelledContext(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)

	// Ten at once stays under the pending channel (CommitBatch*2): a full
	// channel is a legitimate ErrFull, not the behaviour under test.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	items := make([]*item.Item, 0, 10)
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < 10; i++ {
		it := mkItem(i, 64)
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
	go func() { done <- r.Enqueue(context.Background(), mkItem(0, 16)) }()

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
	if _, err := r.Dequeue(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dequeue after Close: %v, want ErrClosed", err)
	}
}

// SPEC §9.4: a row whose head does not decode leaves the accounting at once and
// is moved to the quarantine table by the writer. Left in place and still
// counted, it made the boot drain wait forever for a counter that never reached
// zero, every transport closed.
func TestSQLiteUndecodableHeadIsQuarantined(t *testing.T) {
	m := obs.NewMetrics(obs.Info{})
	r, err := NewSQLite(SQLiteConfig{
		Path:           filepath.Join(t.TempDir(), "ring.db"),
		Synchronous:    "OFF",
		CommitBatch:    8,
		CommitMaxDelay: 2 * time.Millisecond,
		CapacityItems:  1000,
		Metrics:        m,
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })

	// A good row, a poisoned row, a row from a codec of another version, a
	// good row -- as a previous run (or another binary) could have left them.
	if err := r.Enqueue(context.Background(), mkItem(0, 10)); err != nil {
		t.Fatal(err)
	}
	for _, poison := range []struct{ uuid, head string }{
		{"poison", "{not json"},
		{"future", `{"v":2}`},
	} {
		if _, err := r.writeDB.Exec(`INSERT INTO ring (uuid, head, raw, nbytes) VALUES (?, ?, ?, ?)`,
			poison.uuid, []byte(poison.head), []byte("raw line"), 8); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Enqueue(context.Background(), mkItem(2, 10)); err != nil {
		t.Fatal(err)
	}
	if err := r.Recount(); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 4 || s.Bytes != 36 {
		t.Fatalf("stats before = %+v", s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || idx(got[0]) != 0 || idx(got[1]) != 2 {
		t.Fatalf("dequeued %d items, want the 2 good ones", len(got))
	}
	if s := r.Stats(); s.Items != 2 || s.Bytes != 20 {
		t.Fatalf("stats after the skip = %+v; the poisoned rows must leave the accounting at once", s)
	}
	if n := m.RingQuarantinedTotal.Load(); n != 2 {
		t.Fatalf("syslog_ring_quarantined_total = %d, want 2", n)
	}

	// The writer moves the rows: gone from ring, present in quarantine, bytes
	// intact, reason recorded.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var inRing, inQuarantine int
		_ = r.readDB.QueryRow(`SELECT COUNT(*) FROM ring WHERE uuid IN ('poison','future')`).Scan(&inRing)
		_ = r.readDB.QueryRow(`SELECT COUNT(*) FROM quarantine WHERE uuid IN ('poison','future')`).Scan(&inQuarantine)
		if inRing == 0 && inQuarantine == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("poisoned rows: ring=%d quarantine=%d, want 0 and 2", inRing, inQuarantine)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var reason string
	var raw []byte
	if err := r.readDB.QueryRow(`SELECT reason, raw FROM quarantine WHERE uuid = 'poison'`).Scan(&reason, &raw); err != nil {
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
	// And a recount -- the boot's view -- agrees once the deletes landed.
	waitRows(t, r, 0)
	if err := r.Recount(); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("recount = %+v, want empty", s)
	}
}

// A uuid column that disagrees with the head is corruption nobody can resolve;
// the column wins when it is itself a valid UUIDv7.
func TestSQLiteUUIDColumnIsTheCrossCheck(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	it := mkItem(0, 10)
	other := mkItem(1, 10).Head.UUID
	if err := r.Enqueue(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	if _, err := r.writeDB.Exec(`UPDATE ring SET uuid = ? WHERE seq = ?`, other.String(), it.RowID); err != nil {
		t.Fatal(err)
	}
	got, err := r.Dequeue(context.Background(), 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("dequeue = %d %v", len(got), err)
	}
	if got[0].Head.UUID != other {
		t.Fatalf("uuid = %s, want the column's %s", got[0].Head.UUID, other)
	}
}

func TestSQLiteCursorNeverRedelivers(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	const n = 60
	for i := 0; i < n; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[int]int{}
	for got := 0; got < n; {
		items, err := r.Dequeue(context.Background(), 7)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			seen[idx(it)]++
		}
		got += len(items)
	}
	if len(seen) != n {
		t.Fatalf("saw %d distinct items, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("item %d delivered %d times", id, c)
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
		if err := r.Enqueue(context.Background(), mkItem(i, 16)); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		mu    sync.Mutex
		seen  = map[int]int{}
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
					seen[idx(it)]++
				}
				mu.Unlock()
				_ = r.Commit(items)
				if total.Add(int64(len(items))) >= n {
					// The others are blocked waiting for rows that will
					// never come; release them instead of waiting out ctx.
					cancel()
				}
			}
		}()
	}
	wg.Wait()

	if int(total.Load()) != n {
		t.Fatalf("drained %d of %d", total.Load(), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("item %d delivered %d times", id, c)
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
		if err := r.Enqueue(context.Background(), mkItem(i, 32)); err != nil {
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
	for i, it := range got {
		if idx(it) != i {
			t.Fatalf("order lost across reopen: position %d holds item %d", i, idx(it))
		}
	}
}

func waitRows(t *testing.T, r *SQLiteRing, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%d rows left in the ring table after 5s, want %d", n, want)
}

func TestSQLiteCommitDeletesRows(t *testing.T) {
	r, _ := newSQLiteRing(t, 1000, 0)
	for i := 0; i < 10; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := r.Dequeue(context.Background(), 10)
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}

	// Commit drops the accounting at once but hands the DELETE to the writer
	// goroutine, because only one goroutine may write. So the rows outlive the
	// accounting for a moment, and a crash in that window redelivers them --
	// the correct at-least-once bias, since every destination deduplicates on
	// (uuid, step).
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after commit = %+v", s)
	}
	waitRows(t, r, 0)
}

// Deletes already handed to the writer must land even when Close follows at
// once: a row left behind would be delivered a second time by the next boot.
func TestSQLiteCloseFlushesPendingDeletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	cfg := SQLiteConfig{Path: path, Synchronous: "OFF", CommitBatch: 4,
		CommitMaxDelay: 2 * time.Millisecond}
	r, err := NewSQLite(cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 8)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := r.Dequeue(context.Background(), 10)
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r2, err := NewSQLite(cfg, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Backlog() != 0 {
		t.Fatalf("backlog after a committed batch and Close = %d, want 0", r2.Backlog())
	}
}

func TestSQLiteCeilings(t *testing.T) {
	r, _ := newSQLiteRing(t, 2, 0)
	for i := 0; i < 2; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Enqueue(context.Background(), mkItem(2, 16)); !errors.Is(err, ErrFull) {
		t.Fatalf("item ceiling: err = %v", err)
	}

	r2, _ := newSQLiteRing(t, 1000, 40)
	if err := r2.Enqueue(context.Background(), mkItem(0, 30)); err != nil {
		t.Fatal(err)
	}
	if err := r2.Enqueue(context.Background(), mkItem(1, 30)); !errors.Is(err, ErrFull) {
		t.Fatalf("byte ceiling: err = %v", err)
	}
}

func TestSQLiteTouchAndDrainAreNoOps(t *testing.T) {
	r, _ := newSQLiteRing(t, 10, 0)
	if err := r.Enqueue(context.Background(), mkItem(0, 8)); err != nil {
		t.Fatal(err)
	}
	if r.Touch([]*item.Item{mkItem(0, 8)}) != nil {
		t.Fatal("Touch failed")
	}
	// Drain must not dump what is already on disk (SPEC §12).
	if len(r.Drain()) != 0 {
		t.Fatal("Drain returned items")
	}
	if r.Mode() != "sqlite" {
		t.Fatal(r.Mode())
	}
}

// Review finding F5: the ceilings are reserved BEFORE the hand-off to the
// writer. Checked against counters that only move after the group commit, 200
// receivers arriving inside one commit_max_delay all saw an empty ring and all
// got in past a ceiling of 10.
func TestSQLiteConcurrentEnqueueRespectsTheCeilings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capItems int
		capBytes int64
	}{
		{"items", 10, 0},
		{"bytes", 0, 100}, // ten 10-byte lines
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewSQLite(SQLiteConfig{
				Path:           filepath.Join(t.TempDir(), "ring.db"),
				Synchronous:    "OFF",
				CommitBatch:    256, // one group commit for the whole burst
				CommitMaxDelay: 20 * time.Millisecond,
				CapacityItems:  tc.capItems,
				CapacityBytes:  tc.capBytes,
			}, discard())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { r.Close() })

			const n = 200
			var wg sync.WaitGroup
			var accepted, full atomic.Int64
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					switch err := r.Enqueue(context.Background(), mkItem(i, 10)); {
					case err == nil:
						accepted.Add(1)
					case errors.Is(err, ErrFull):
						full.Add(1)
					default:
						t.Errorf("enqueue: %v", err)
					}
				}(i)
			}
			close(start)
			wg.Wait()

			if accepted.Load() != 10 || full.Load() != n-10 {
				t.Fatalf("accepted %d, refused %d; want exactly 10 past the ceiling", accepted.Load(), full.Load())
			}
			var rows int64
			if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if s := r.Stats(); rows != 10 || s.Items != 10 || s.Bytes != 100 {
				t.Fatalf("rows = %d, stats = %+v; want 10 / 10 / 100", rows, s)
			}
			// No reservation is left behind: the recount -- the boot's view --
			// gives the same numbers.
			if err := r.Recount(); err != nil {
				t.Fatal(err)
			}
			if s := r.Stats(); s.Items != 10 || s.Bytes != 100 {
				t.Fatalf("after recount = %+v", s)
			}
		})
	}
}

// Review finding F3b: a group commit that fails (disk full, an I/O error, the
// lock past busy_timeout) is the ring's storage refusing: the receiver gets an
// error wrapping ErrUnavailable -- ring_unavailable, not sink_down -- the
// reservation is undone, and Available is false until a commit succeeds.
func TestSQLiteFailedInsertIsUnavailable(t *testing.T) {
	r, _ := newSQLiteRing(t, 100, 0)
	if !r.Available() {
		t.Fatal("a fresh ring is unavailable")
	}
	if err := r.Enqueue(context.Background(), mkItem(0, 10)); err != nil {
		t.Fatal(err)
	}
	// The INSERT fails, as with SQLITE_FULL.
	if _, err := r.writeDB.Exec(`CREATE TRIGGER full BEFORE INSERT ON ring
		BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END`); err != nil {
		t.Fatal(err)
	}
	err := r.Enqueue(context.Background(), mkItem(1, 10))
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if r.Available() {
		t.Fatal("Available still true after a failed commit")
	}
	if s := r.Stats(); s.Items != 1 || s.Bytes != 10 {
		t.Fatalf("stats = %+v: the failed item's reservation leaked", s)
	}

	if _, err := r.writeDB.Exec(`DROP TRIGGER full`); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(2, 10)); err != nil {
		t.Fatalf("enqueue after the disk recovered: %v", err)
	}
	if !r.Available() {
		t.Fatal("Available still false after a successful commit")
	}
	if s := r.Stats(); s.Items != 2 || s.Bytes != 20 {
		t.Fatalf("stats = %+v", s)
	}
}

// ... and the same when the COMMIT itself is what fails. A deferred foreign-key
// violation refuses the COMMIT and, by SQLite's rules, leaves the transaction
// OPEN: without the explicit ROLLBACK every later BEGIN on the writer's one
// connection would fail, and the ring would stay unavailable after the cause
// was gone.
func TestSQLiteFailedCommitIsUnavailableAndRecovers(t *testing.T) {
	r, _ := newSQLiteRing(t, 100, 0)
	for _, q := range []string{
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE child (pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER failcommit AFTER INSERT ON ring BEGIN INSERT INTO child VALUES (42); END`,
	} {
		if _, err := r.writeDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	err := r.Enqueue(context.Background(), mkItem(0, 10))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable from the COMMIT", err)
	}
	t.Logf("commit error: %v", err)
	if r.Available() {
		t.Fatal("Available still true after a failed COMMIT")
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v: the reservation leaked", s)
	}

	if _, err := r.writeDB.Exec(`DROP TRIGGER failcommit`); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(1, 10)); err != nil {
		t.Fatalf("enqueue after the cause was removed: %v", err)
	}
	if !r.Available() {
		t.Fatal("Available still false after a successful commit")
	}
	if err := r.Recount(); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 1 || s.Bytes != 10 {
		t.Fatalf("recount = %+v: the failed batch left rows behind", s)
	}
}
