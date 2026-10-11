package ring

// A ring.db a previous life left behind (TESTING.md §8 item 42, SPEC §7.4):
// the rows ring=sqlite keeps aside with sink=none, and the probe a boot with
// another ring mode runs before it touches the file.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// plantRows enqueues n items into a fresh ring at path and closes it, which is
// what a previous life leaves; it returns their ids in order.
func plantRows(t *testing.T, path string, n int) []string {
	t.Helper()
	r, err := NewSQLite(ringConfig(path), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for range n {
		it := mkItem(16)
		ids = append(ids, it.ID)
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// SPEC §7.4: with KeepBacklog the rows found at boot are a previous life's
// that this one must not deliver. The cursor starts after the last of them --
// not after as many rows as there are: the first two were delivered before,
// so the kept rowids run 3..5 -- and none is counted, so a backlog larger than
// the ceiling does not fill the ring. A ring opened later without it reads
// every one, in order.
func TestSQLiteKeepBacklogSetsThePreviousRunAside(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	old := plantRows(t, path, 5)
	first, err := NewSQLite(ringConfig(path), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done, err := first.Dequeue(ctx, 2)
	if err != nil || len(done) != 2 {
		t.Fatalf("Dequeue = %d items, %v", len(done), err)
	}
	if err := first.Commit(done); err != nil {
		t.Fatal(err)
	}
	waitRows(t, path, 3)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	old = old[2:]

	cfg := ringConfig(path)
	cfg.KeepBacklog = true
	cfg.CapacityItems = 2 // below the 3 kept: counting them would refuse everything
	r, err := NewSQLite(cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if k, b, st := r.Kept(), r.Backlog(), r.Stats(); k != 3 || b != 0 || st.Items != 0 || st.Bytes != 0 {
		t.Fatalf("Kept=%d Backlog=%d Stats=%+v; want the 3 rows kept and nothing counted", k, b, st)
	}

	var fresh []string
	for range 2 {
		it := mkItem(16)
		fresh = append(fresh, it.ID)
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatalf("Enqueue with 3 kept rows and capacity 2: %v", err)
		}
	}
	got, err := r.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != fresh[0] || got[1].ID != fresh[1] {
		var ids []string
		for _, it := range got {
			ids = append(ids, it.ID)
		}
		t.Fatalf("Dequeue returned %v, want only this run's %v", ids, fresh)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	waitRows(t, path, 3) // the DELETEs reached the disk before the Close
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	r2, err := NewSQLite(ringConfig(path), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Kept() != 0 || r2.Backlog() != 3 {
		t.Fatalf("reopened without KeepBacklog: Kept=%d Backlog=%d, want 0 and 3", r2.Kept(), r2.Backlog())
	}
	all, err := r2.Dequeue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != len(old) {
		t.Fatalf("a ring without KeepBacklog read %d rows, want the %d kept", len(all), len(old))
	}
	for i, it := range all {
		if it.ID != old[i] {
			t.Fatalf("row %d is %s, want %s: the kept rows, in order", i, it.ID, old[i])
		}
	}
}

// SPEC §7.4: Close applies the DELETEs already queued. The writer used to
// leave on stop with them in the channel -- the select picks at random when
// both are ready, about half the time on one CPU -- and the delivered rows
// came back at the next boot (TESTING.md §8 item 40): duplicates with
// sink=redis, and rows left in a ring.db the boot drained but must keep. One
// CPU is what a container with one core runs, and what makes the race show.
func TestSQLiteCloseAppliesTheQueuedDeletes(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	dir := t.TempDir()
	for i := range 30 {
		path := filepath.Join(dir, fmt.Sprintf("ring-%d.db", i))
		plantRows(t, path, 8)
		r, err := NewSQLite(ringConfig(path), quietLog())
		if err != nil {
			t.Fatal(err)
		}
		items, err := r.Dequeue(context.Background(), 8)
		if err != nil || len(items) != 8 {
			t.Fatalf("Dequeue = %d items, %v", len(items), err)
		}
		if err := r.Commit(items); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		left, err := ProbeSQLite(path)
		if err != nil {
			t.Fatal(err)
		}
		if left.Rows != 0 {
			t.Fatalf("round %d: %d delivered rows still in jlh_ring after Close; the next boot "+
				"delivers them again", i, left.Rows)
		}
	}
}

// waitRows waits until jlh_ring at path holds want rows.
func waitRows(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		left, err := ProbeSQLite(path)
		if err != nil {
			t.Fatal(err)
		}
		if left.Rows == int64(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("jlh_ring holds %d rows, want %d", left.Rows, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// SPEC §7.4: the probe a boot with another ring mode runs creates no file and
// no table, says a database without either ring table is no ring, counts both
// tables, and refuses what is not a regular file, saying so -- without
// blocking on a FIFO, which SQLite itself only answers with "disk I/O error".
func TestProbeSQLiteCreatesNothing(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing", func(t *testing.T) {
		path := filepath.Join(dir, "missing.db")
		left, err := ProbeSQLite(path)
		if err != nil || left != (Left{}) {
			t.Fatalf("ProbeSQLite = %+v, %v; want nothing and no error", left, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the probe created %s: %v", path, err)
		}
	})

	t.Run("not a ring", func(t *testing.T) {
		// The events.db of sink=sqlite, when ring.sqlite.path points at it.
		path := filepath.Join(dir, "events.db")
		db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE inbox (uuid TEXT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		left, err := ProbeSQLite(path)
		if err != nil || left.Ring {
			t.Fatalf("ProbeSQLite = %+v, %v; want no ring", left, err)
		}
		if tableExists(t, db, "jlh_ring") || tableExists(t, db, "ring") {
			t.Fatal("the probe created a ring table in a database that is not a ring")
		}
		db.Close()

		// The same file once a ring=sqlite life pointed ring.sqlite.path at
		// it: a ring, but not only a ring.
		plantRows(t, path, 2)
		left, err = ProbeSQLite(path)
		if err != nil || left != (Left{Ring: true, Shared: true, Rows: 2}) {
			t.Fatalf("ProbeSQLite = %+v, %v; want a shared ring with 2 rows", left, err)
		}
	})

	t.Run("ring and v1", func(t *testing.T) {
		path := filepath.Join(dir, "ring.db")
		makeV1Ring(t, path, 2)
		plantRows(t, path, 3)
		left, err := ProbeSQLite(path)
		if err != nil || left != (Left{Ring: true, Rows: 3, Legacy: 2}) {
			t.Fatalf("ProbeSQLite = %+v, %v; want a ring with 3 rows and 2 legacy", left, err)
		}
	})

	t.Run("v1 only", func(t *testing.T) {
		path := filepath.Join(dir, "v1.db")
		makeV1Ring(t, path, 1)
		left, err := ProbeSQLite(path)
		if err != nil || left != (Left{Ring: true, Legacy: 1}) {
			t.Fatalf("ProbeSQLite = %+v, %v; want a ring with 1 legacy row", left, err)
		}
	})

	t.Run("not sqlite", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.db")
		if err := os.WriteFile(path, []byte("this is not an SQLite database, not even close"), 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := ProbeSQLite(path); err == nil {
			t.Fatal("a file that is not SQLite probed clean")
		}
	})

	t.Run("fifo", func(t *testing.T) {
		path := filepath.Join(dir, "fifo.db")
		if err := syscall.Mkfifo(path, 0o640); err != nil {
			t.Skip("mkfifo:", err)
		}
		done := make(chan error, 1)
		go func() { _, err := ProbeSQLite(path); done <- err }()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "not a regular file") {
				t.Fatalf("a FIFO probed with %v; want it refused as not a regular file", err)
			}
		case <-time.After(3 * time.Second):
			// Unblock the open the probe is stuck in, so the goroutine ends.
			if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
				f.Close()
			}
			t.Fatal("the probe blocked on a FIFO; the boot would too")
		}
	})
}
