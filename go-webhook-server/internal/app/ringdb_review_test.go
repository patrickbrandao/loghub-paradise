package app

// The ring.db a previous life left behind (TESTING.md §8 item 42, SPEC §7.4):
// read by every boot, as the dumps are (SPEC §9), delivered and removed by a
// life with another ring mode, and left alone -- outside the ring's count --
// by a life whose sink discards.

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// plantRing leaves n rows of testID in a ring.db at path, as a ring=sqlite
// life whose destination was down leaves them, and returns their uuids.
func plantRing(t *testing.T, path string, n int) []string {
	t.Helper()
	r, err := ring.NewSQLite(ring.SQLiteConfig{Path: path, Synchronous: "OFF"}, obs.NewLogger(
		&bytes.Buffer{}, "text", "error").Logger)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := range n {
		id, env := testJLH(t, testID, fmt.Sprintf("/r/%d", i), fmt.Sprintf(`{"r":%d}`, i))
		if err := r.Enqueue(t.Context(), envelope.NewItem(id, env, -1)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// plantV1Ring adds the pre-LogHub table `ring` with n rows to the database at
// path (SPEC §7.4); only its name and its row count matter to the boot.
func plantV1Ring(t *testing.T, path string, n int) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE ring (rowid_ INTEGER PRIMARY KEY AUTOINCREMENT,
		id TEXT NOT NULL, env BLOB NOT NULL, body BLOB NOT NULL, nbytes INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, err := db.Exec(`INSERT INTO ring (id, env, body, nbytes) VALUES (?,?,?,?)`,
			fmt.Sprintf("v1-%d", i), []byte(`{"v":1}`), []byte("abc"), 10); err != nil {
			t.Fatal(err)
		}
	}
}

// ringFilesLeft reports which of the ring.db's three files still exist.
func ringFilesLeft(path string) []string {
	var left []string
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			left = append(left, filepath.Base(p))
		}
	}
	return left
}

var sqliteSink = []string{"--sink.type=sqlite", "--sink.workers=1", "--sink.sqlite.synchronous=OFF"}

// SPEC §7.4: a life with another ring mode delivers what a ring=sqlite life
// left in ring.db before its public listener opens, then removes the file --
// WAL and shared memory too. An empty one, from a clean shutdown, goes as
// well. Until 2026-10-11 such a boot never even looked, and the rows the
// shutdown's WARN promised to "the next boot" stayed there, answered 200 and
// never delivered.
func TestLeftRingDBIsDeliveredAndRemoved(t *testing.T) {
	for _, mode := range []string{"memory", "none"} {
		for _, rows := range []int{4, 0} {
			t.Run(fmt.Sprintf("ring=%s/%d rows", mode, rows), func(t *testing.T) {
				dir := t.TempDir()
				ringDB := filepath.Join(dir, "ring.db")
				ids := plantRing(t, ringDB, rows)

				var log syncBuffer
				startLog = &log
				defer func() { startLog = nil }()
				in := start(t, dir, append([]string{"--ring.mode=" + mode}, sqliteSink...)...)
				// Ready means past step 5: the file is already gone.
				if left := ringFilesLeft(ringDB); len(left) > 0 {
					t.Errorf("still on disk once the public listener is open: %v", left)
				}
				if code := in.stop(); code != ExitOK {
					t.Fatalf("exit %d, want 0", code)
				}

				got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
				if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
					t.Errorf("the inbox holds %v, want exactly the ring.db's %v", got, ids)
				}
				out := log.String()
				if rows > 0 && !strings.Contains(out, "delivering them before opening the public listener") {
					t.Errorf("the boot did not say whose rows it delivered:\n%s", out)
				}
				if !strings.Contains(out, "ring.db left by a ring.mode=sqlite life removed") {
					t.Errorf("the boot did not report the removal:\n%s", out)
				}
			})
		}
	}
}

// SPEC §7.4, §9.2: sink=none discards by configuration, so a previous life's
// rows are not delivered to it, whatever this life's ring mode. With
// ring=sqlite they stay out of the ring's count -- a ceiling below them still
// accepts, and /readyz is not saturated by them -- and the shutdown's WARN
// counts them for the next boot. A life with a destination then delivers
// exactly them. Until 2026-10-11 ring=sqlite drained them into the discard.
func TestSinkNoneLeavesTheRingDBRows(t *testing.T) {
	for _, mode := range []string{"memory", "none", "sqlite"} {
		t.Run("ring="+mode, func(t *testing.T) {
			dir := t.TempDir()
			ringDB := filepath.Join(dir, "ring.db")
			ids := plantRing(t, ringDB, 10)

			args := []string{"--ring.mode=" + mode, "--sink.type=none"}
			if mode == "sqlite" {
				// Half the 10 kept rows: counted, they would refuse everything;
				// not counted, the ceiling leaves room for a worker a commit
				// behind.
				args = append(args, "--ring.sqlite.synchronous=OFF", "--ring.capacity_items=5")
			}
			var log syncBuffer
			startLog = &log
			in := start(t, dir, args...)
			if n := len(in.postN(4)); n != 4 {
				t.Errorf("%d of 4 accepted beside the 10 rows found", n)
			}
			code := in.stop()
			startLog = nil
			if code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}
			if n := countRows(t, ringDB, "jlh_ring"); n != len(ids) {
				t.Errorf("ring.db holds %d rows after a life with sink=none, want the %d it found", n, len(ids))
			}
			out := log.String()
			if !strings.Contains(out, "sink.type=none discards what it is given; they are NOT delivered") {
				t.Errorf("the boot did not say why the rows stay:\n%s", out)
			}
			if mode == "sqlite" && !strings.Contains(out, "count=10 of_a_previous_life=10") {
				t.Errorf("the shutdown's WARN does not count the rows kept for the next boot:\n%s", out)
			}

			next := start(t, dir, append([]string{"--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF"},
				sqliteSink...)...)
			if code := next.stop(); code != ExitOK {
				t.Fatalf("the life with a destination exited %d, want 0", code)
			}
			got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
			if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
				t.Errorf("the life with a destination delivered %v, want exactly the kept %v", got, ids)
			}
		})
	}
}

// SPEC §7.4: the pre-LogHub table with rows keeps the whole file. The rows of
// jlh_ring beside it are still delivered, and the ERROR is said once, by
// whoever looks at the table.
func TestLeftRingDBWithLegacyRowsStays(t *testing.T) {
	for _, rows := range []int{2, 0} {
		t.Run(fmt.Sprintf("%d jlh_ring rows", rows), func(t *testing.T) {
			dir := t.TempDir()
			ringDB := filepath.Join(dir, "ring.db")
			var ids []string
			if rows > 0 {
				ids = plantRing(t, ringDB, rows)
			}
			plantV1Ring(t, ringDB, 2)

			var log syncBuffer
			startLog = &log
			defer func() { startLog = nil }()
			in := start(t, dir, append([]string{"--ring.mode=memory"}, sqliteSink...)...)
			if code := in.stop(); code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}

			got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
			if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
				t.Errorf("the inbox holds %v, want exactly the jlh_ring's %v", got, ids)
			}
			if n := countRows(t, ringDB, "ring"); n != 2 {
				t.Errorf("the v1 table holds %d rows, want its 2 untouched", n)
			}
			if rows > 0 {
				if n := countRows(t, ringDB, "jlh_ring"); n != 0 {
					t.Errorf("%d jlh_ring rows left after the drain", n)
				}
			}
			if n := strings.Count(log.String(), "the pre-LogHub table still holds events"); n != 1 {
				t.Errorf("the pre-LogHub ERROR was said %d times, want once:\n%s", n, log.String())
			}
		})
	}
}

// SPEC §7.4: a file at ring.sqlite.path that is not a ring is never touched --
// the events.db, when the two paths coincide under another ring -- and one
// that cannot be read is an ERROR the boot goes past: this life never needs it.
func TestNotARingDBIsLeftAlone(t *testing.T) {
	t.Run("the events.db", func(t *testing.T) {
		dir := t.TempDir()
		events := filepath.Join(dir, "events.db")
		in := start(t, dir, append([]string{"--ring.mode=memory", "--ring.sqlite.path=" + events},
			sqliteSink...)...)
		ids := in.postN(3)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
		got := eventsIn(t, events, envelope.ModeFast)
		if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
			t.Errorf("the events.db holds %v, want the %v answered 200", got, ids)
		}
		db, err := sql.Open("sqlite", "file:"+events+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
			WHERE type = 'table' AND name IN ('jlh_ring', 'ring')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Error("the boot added a ring table to the events.db")
		}
	})

	t.Run("empty file", func(t *testing.T) {
		// No table at all: SQLite reads it as an empty database, which is no
		// ring and nothing to remove.
		dir := t.TempDir()
		ringDB := filepath.Join(dir, "ring.db")
		if err := os.WriteFile(ringDB, nil, 0o640); err != nil {
			t.Fatal(err)
		}
		in := start(t, dir, append([]string{"--ring.mode=memory"}, sqliteSink...)...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
		if _, err := os.Stat(ringDB); err != nil {
			t.Errorf("an empty file at ring.sqlite.path was removed: %v", err)
		}
	})

	t.Run("not sqlite", func(t *testing.T) {
		dir := t.TempDir()
		ringDB := filepath.Join(dir, "ring.db")
		junk := []byte("not an SQLite database; a boot without ring=sqlite must leave it be")
		if err := os.WriteFile(ringDB, junk, 0o640); err != nil {
			t.Fatal(err)
		}
		var log syncBuffer
		startLog = &log
		defer func() { startLog = nil }()
		in := start(t, dir, append([]string{"--ring.mode=memory"}, sqliteSink...)...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
		if after, err := os.ReadFile(ringDB); err != nil || !bytes.Equal(after, junk) {
			t.Errorf("the unreadable file was removed or altered: %v", err)
		}
		if !strings.Contains(log.String(), "cannot read the ring.db a ring.mode=sqlite life may have left") {
			t.Errorf("the boot did not name the file it could not read:\n%s", log.String())
		}
	})
}

// SPEC §7.4: only a file that is the ring's alone is removed. The events.db a
// life with both paths on it writes to, which an earlier ring=sqlite life
// pointed ring.sqlite.path at -- leaving a jlh_ring in it, with rows or
// without -- is drained and kept. The first version of this round removed it,
// the destination with every event in it, from under the sink that had it
// open, and exited 0 (independent review of this round).
func TestSharedRingFileIsDrainedNeverRemoved(t *testing.T) {
	for _, rows := range []int{2, 0} {
		t.Run(fmt.Sprintf("%d jlh_ring rows", rows), func(t *testing.T) {
			dir := t.TempDir()
			events := filepath.Join(dir, "events.db")
			args := append([]string{"--ring.mode=memory", "--ring.sqlite.path=" + events}, sqliteSink...)
			before := start(t, dir, args...)
			ids := before.postN(3)
			if code := before.stop(); code != ExitOK || len(ids) != 3 {
				t.Fatalf("the first life exited %d with %d of 3 accepted", code, len(ids))
			}
			ids = append(ids, plantRing(t, events, rows)...)

			var log syncBuffer
			startLog = &log
			in := start(t, dir, args...)
			code := in.stop()
			startLog = nil
			if code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}

			got := eventsIn(t, events, envelope.ModeFast)
			if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
				t.Errorf("the events.db holds %v, want the first life's events and the ring's: %v", got, ids)
			}
			if n := countRows(t, events, "jlh_ring"); n != 0 {
				t.Errorf("%d jlh_ring rows left after the drain", n)
			}
			if rows > 0 && !strings.Contains(log.String(), "the file is NOT removed") {
				t.Errorf("the boot did not say why the file stays:\n%s", log.String())
			}
		})
	}
}

// SPEC §7.4: a ring.db the probe reads but the ring cannot open -- here a
// write-protected file in the rollback journal, which NewSQLite cannot even
// declare its schema in -- is an ERROR, and the boot goes on with the file as
// it was.
func TestLeftRingDBThatCannotBeOpenedIsReported(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes a write-protected file")
	}
	dir := t.TempDir()
	ringDB := filepath.Join(dir, "ring.db")
	db, err := sql.Open("sqlite", "file:"+ringDB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE jlh_ring (rowid_ INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid TEXT NOT NULL, jlh BLOB NOT NULL, nbytes INTEGER NOT NULL) STRICT`); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		id, env := testJLH(t, testID, fmt.Sprintf("/ro/%d", i), `{}`)
		if _, err := db.Exec(`INSERT INTO jlh_ring (uuid, jlh, nbytes) VALUES (?,?,?)`,
			id, env, len(env)); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	if err := os.Chmod(ringDB, 0o444); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(ringDB)
	if err != nil {
		t.Fatal(err)
	}

	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	in := start(t, dir, append([]string{"--ring.mode=memory"}, sqliteSink...)...)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	if after, err := os.ReadFile(ringDB); err != nil || !bytes.Equal(after, before) {
		t.Errorf("the ring.db that could not be opened was removed or altered: %v", err)
	}
	if !strings.Contains(log.String(), "cannot open the ring.db a ring.mode=sqlite life left; its events are NOT delivered") {
		t.Errorf("the boot did not name the ring.db it could not open:\n%s", log.String())
	}
}

// SPEC §7.4: the database goes last, so that no crash leaves an old WAL beside
// a name with no database. A -wal that cannot be removed -- a directory with a
// file in it -- stops the removal before the ring.db.
func TestLeftRingDBIsRemovedLast(t *testing.T) {
	ringDB := filepath.Join(t.TempDir(), "ring.db")
	plantRing(t, ringDB, 0)
	if err := os.MkdirAll(filepath.Join(ringDB+"-wal", "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	var log syncBuffer
	a := &App{log: obs.NewLogger(&log, "text", "info")}
	a.removeLeftRing(ringDB)
	if _, err := os.Stat(ringDB); err != nil {
		t.Errorf("the ring.db went before its WAL: %v", err)
	}
	if !strings.Contains(log.String(), "cannot remove the ring.db") {
		t.Errorf("the failed removal was not reported:\n%s", log.String())
	}
}

// SPEC §7.4, §9.2: the drain of a left ring.db ends the boot like the backlog's
// own -- 130 on a SIGTERM, 3 when the destination goes away -- and in both the
// file stays with every row for the next boot.
func TestLeftRingDBDrainThatDoesNotFinishKeepsTheRows(t *testing.T) {
	c := redisClient(t)
	for _, tc := range []struct {
		name   string
		signal bool
		want   int
	}{
		{"SIGTERM", true, ExitAborted},
		{"destination gone", false, ExitSink},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := redisStream(t, c)
			dir := t.TempDir()
			ringDB := filepath.Join(dir, "ring.db")
			plantRing(t, ringDB, 5)

			proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
			proxy.Hold(holdWrites(stream))
			in := startExpectingExit(t, dir, redisArgs(proxy.Addr(), stream,
				"--ring.mode=memory", "--sink.op_timeout=60s", "--sink.boot_timeout=30s")...)
			in.waitBootState("draining backlog", 20*time.Second)
			time.Sleep(300 * time.Millisecond) // the write is in flight, held
			if tc.signal {
				in.cancel()
				time.Sleep(200 * time.Millisecond)
			}
			proxy.Close()

			select {
			case code := <-in.done:
				if code != tc.want {
					t.Errorf("exit code %d, want %d", code, tc.want)
				}
			case <-time.After(30 * time.Second):
				in.cancel()
				t.Fatal("the unfinished drain of the left ring.db did not end the boot")
			}
			if n := countRows(t, ringDB, "jlh_ring"); n != 5 {
				t.Errorf("ring.db holds %d rows after the unfinished drain, want the 5 still owed", n)
			}
		})
	}
}
