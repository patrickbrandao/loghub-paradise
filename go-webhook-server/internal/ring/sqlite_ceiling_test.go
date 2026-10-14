package ring

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// SPEC §7.4 (port of go-syslog-server's test, 2026-10-14): both ceilings
// hold under a burst that lands inside ONE group commit. The check compared
// against counters that only grow after the COMMIT, so the whole burst got
// through. CommitMaxDelay is long enough that no COMMIT happens before the
// 200 goroutines have handed their items over (pending holds 2 x 256), which
// is what makes the count exact rather than racy.
func TestSQLiteConcurrentEnqueueRespectsTheCeilings(t *testing.T) {
	one := mkItem(10).Size()
	for _, tc := range []struct {
		name     string
		capItems int
		capBytes int64
	}{
		{"items", 10, 0},
		{"bytes", 0, 10 * one}, // ten envelopes of a 10-byte body
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := SQLiteConfig{
				Path:           filepath.Join(t.TempDir(), "ring.db"),
				Synchronous:    "OFF",
				CommitBatch:    256, // one group commit for the whole burst
				CommitMaxDelay: 500 * time.Millisecond,
				CapacityItems:  tc.capItems,
				CapacityBytes:  tc.capBytes,
			}
			r, err := NewSQLite(cfg, quietLog())
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					r.Close()
				}
			})

			const n = 200
			var wg sync.WaitGroup
			var accepted, full atomic.Int64
			start := make(chan struct{})
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					switch err := r.Enqueue(context.Background(), mkItem(10)); {
					case err == nil:
						accepted.Add(1)
					case errors.Is(err, ErrFull):
						full.Add(1)
					default:
						t.Errorf("enqueue: %v", err)
					}
				}()
			}
			close(start)
			wg.Wait()

			if accepted.Load() != 10 || full.Load() != n-10 {
				t.Fatalf("accepted %d, refused %d; want exactly 10 past the ceiling", accepted.Load(), full.Load())
			}
			var rows int64
			if err := r.readDB.QueryRow(`SELECT COUNT(*) FROM jlh_ring`).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if s := r.Stats(); rows != 10 || s.Items != 10 || s.Bytes != 10*one {
				t.Fatalf("rows = %d, stats = %+v; want 10 / 10 / %d", rows, s, 10*one)
			}
			// No reservation is left behind: the boot's view -- the recount
			// of a reopened ring -- gives the same numbers.
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			again, err := NewSQLite(cfg, quietLog())
			if err != nil {
				t.Fatal(err)
			}
			defer again.Close()
			if s := again.Stats(); s.Items != 10 || s.Bytes != 10*one {
				t.Fatalf("after reopening = %+v, want 10 / %d", s, 10*one)
			}
		})
	}
}

// A reservation that never becomes a row is given back: the refusals above,
// and an Enqueue refused because the ring is closed. Stats shows
// reservations, and after the burst settles only rows remain. This one stops
// at the closed flag, before any reservation; the branch that gives a
// reservation back when the writer leaves is the next test's.
func TestSQLiteReservationIsReleasedWhenTheRingCloses(t *testing.T) {
	r, err := NewSQLite(SQLiteConfig{
		Path: filepath.Join(t.TempDir(), "ring.db"), Synchronous: "OFF",
		CommitBatch: 8, CommitMaxDelay: 5 * time.Millisecond, CapacityItems: 100,
	}, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(10)); err != nil {
		t.Fatal(err)
	}
	if s := r.Stats(); s.Items != 1 {
		t.Fatalf("items = %d, want 1", s.Items)
	}
	r.Close()
	if err := r.Enqueue(context.Background(), mkItem(10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v, want ErrClosed", err)
	}
	if s := r.Stats(); s.Items != 1 {
		t.Fatalf("items = %d after a refused enqueue, want 1: a reservation leaked", s.Items)
	}
}

// The branch the test above never reaches (independent review of
// 2026-10-14, T1): the writer has returned while closed is still false -- the
// window inside Close between close(stop) and the flag being read -- so the
// item is reserved and sent, and only writerDone answers. The writer will
// never commit it, and its reservation must go back; with the unreserve
// removed, Stats kept counting an item that was never a row.
func TestSQLiteReservationIsReleasedWhenTheWriterIsGone(t *testing.T) {
	r, err := NewSQLite(SQLiteConfig{
		Path: filepath.Join(t.TempDir(), "ring.db"), Synchronous: "OFF",
		CommitBatch: 8, CommitMaxDelay: 5 * time.Millisecond, CapacityItems: 100,
	}, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	close(r.stop)
	r.wg.Wait() // the writer is gone; closed is still false
	t.Cleanup(func() {
		r.closed.Store(true)
		if r.insert != nil {
			r.insert.Close()
		}
		_ = r.writeDB.Close()
		_ = r.readDB.Close()
	})
	if err := r.Enqueue(context.Background(), mkItem(10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("with the writer gone: %v, want ErrClosed", err)
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v after an item the writer never took, want 0 / 0: a reservation leaked", s)
	}
}

// SPEC §7.4 (2026-10-14): a read that fails part way serves what it read
// before the failure. Those rows are already behind the cursor; dropping them
// stranded them until the next boot.
//
// The failure is made real, not mocked: jlh_ring is created first without
// STRICT and NOT NULL (the ring's CREATE TABLE IF NOT EXISTS keeps it), and
// the second row's uuid is NULL, which Scan cannot put in a string.
func TestSQLiteDequeueServesWhatItReadBeforeAFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE jlh_ring (
		rowid_ INTEGER PRIMARY KEY AUTOINCREMENT, uuid TEXT, jlh BLOB, nbytes INTEGER)`); err != nil {
		t.Fatal(err)
	}
	first, third := mkItem(10), mkItem(10)
	for _, row := range []struct {
		uuid any
		env  []byte
	}{{first.ID, first.Env}, {nil, first.Env}, {third.ID, third.Env}} {
		if _, err := db.Exec(`INSERT INTO jlh_ring (uuid, jlh, nbytes) VALUES (?,?,?)`,
			row.uuid, row.env, len(row.env)); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	r, err := NewSQLite(ringConfig(path), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.Dequeue(context.Background(), 3)
	if err != nil {
		t.Fatalf("Dequeue = %v; the row read before the failure must be served without it", err)
	}
	if len(got) != 1 || got[0].ID != first.ID {
		t.Fatalf("Dequeue returned %d items, want the first row only", len(got))
	}
	// The failing row is met again by the next read, not skipped.
	if _, err := r.Dequeue(context.Background(), 3); err == nil {
		t.Fatal("the NULL row was skipped instead of failing again")
	}
}
