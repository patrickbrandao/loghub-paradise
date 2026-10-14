package app

// The boot's two batch writers -- the dump's reingestion and the ring.db
// drain -- against a destination that refuses one record for good (SPEC
// §9.2, §10.1 step 5) and against sink.batch_max_bytes (SPEC §8.1 step 2),
// both of 2026-10-14.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/sink"
)

// refusingSink refuses for good (logic) every batch that holds the item
// whose uuid is bad, and records the size of every write it passes on.
type refusingSink struct {
	sink.Sink
	bad string

	mu     sync.Mutex
	writes [][2]int // items, JLH bytes
}

func (s *refusingSink) Write(ctx context.Context, items []*envelope.Item) error {
	b := 0
	for _, it := range items {
		if it.ID == s.bad {
			return &sink.Error{Class: sink.ClassLogic, Op: "test", Err: errors.New("refused for good")}
		}
		b += len(it.Env)
	}
	s.mu.Lock()
	s.writes = append(s.writes, [2]int{len(items), b})
	s.mu.Unlock()
	return s.Sink.Write(ctx, items)
}

func (s *refusingSink) got() [][2]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]int(nil), s.writes...)
}

// dumpIDs reads back the uuids of a dump, in order.
func dumpIDs(t *testing.T, dir, path string) []string {
	t.Helper()
	res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(path)
	if err != nil || res.Truncated {
		t.Fatalf("reading the planted dump: %v, %+v", err, res)
	}
	ids := make([]string, len(res.Items))
	for i, it := range res.Items {
		ids[i] = it.ID
	}
	return ids
}

// SPEC §9.2: a record the destination refuses as logic during the
// reingestion no longer stops every boot with 3. The batch is redone item by
// item, the others are delivered, the refused one stays in the file, and the
// file goes to .corrupt. Until 2026-10-14 one such record looped the boot
// with the public port closed.
func TestBootLogicRefusalInADumpIsQuarantinedNotFatal(t *testing.T) {
	dir := t.TempDir()
	path := plantDump(t, dir, 5)
	ids := dumpIDs(t, dir, path)
	rs := &refusingSink{bad: ids[1]}

	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	in := startHooked(t, dir, testID, func(a *App) {
		a.wrapSink = func(s sink.Sink) sink.Sink { rs.Sink = s; return rs }
	}, append([]string{"--ring.mode=memory"}, sqliteSink...)...)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d, want 0", code)
	}

	got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
	want := append(append([]string{}, ids[0]), ids[2:]...)
	if len(got) != 4 || len(missing(want, got)) > 0 {
		t.Errorf("the inbox holds %v, want the 4 records the destination took %v", got, want)
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("the dump with a refused record is still in place under its own name")
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		t.Errorf("the dump with a refused record was not quarantined: %v", err)
	}
	if out := log.String(); !strings.Contains(out, ids[1]) {
		t.Errorf("no log names the refused record %s:\n%s", ids[1], out)
	}
}

// SPEC §10.1 step 5: a ring.db row the destination refuses as logic is
// dropped and counted as sink_logic, as the workers do (SPEC §8.1), and the
// boot goes on -- with ring=sqlite, and with another ring mode draining what a
// ring=sqlite life left.
func TestBootLogicRefusalInRingDBIsDroppedAndCounted(t *testing.T) {
	for _, mode := range []string{"sqlite", "memory"} {
		t.Run("ring="+mode, func(t *testing.T) {
			dir := t.TempDir()
			ringDB := filepath.Join(dir, "ring.db")
			ids := plantRing(t, ringDB, 5)
			rs := &refusingSink{bad: ids[2]}

			in := startHooked(t, dir, testID, func(a *App) {
				a.wrapSink = func(s sink.Sink) sink.Sink { rs.Sink = s; return rs }
			}, append([]string{"--ring.mode=" + mode}, sqliteSink...)...)
			if m := in.metrics(); !strings.Contains(m, `whs_events_dropped_total{reason="sink_logic"} 1`) {
				t.Errorf("the refused row is not counted as sink_logic:\n%s", grepLines(m, "whs_events_"))
			}
			if code := in.stop(); code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}

			got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
			want := append(append([]string{}, ids[:2]...), ids[3:]...)
			if len(got) != 4 || len(missing(want, got)) > 0 {
				t.Errorf("the inbox holds %v, want the 4 rows the destination took %v", got, want)
			}
			if mode == "memory" {
				if left := ringFilesLeft(ringDB); len(left) > 0 {
					t.Errorf("the drained ring.db is still on disk: %v", left)
				}
			} else if n := countRows(t, ringDB, "jlh_ring"); n != 0 {
				t.Errorf("ring.db holds %d rows, want 0: the refused row was dropped, not kept", n)
			}
		})
	}
}

// SPEC §8.1 step 2: the boot's writers cut by sink.batch_max_bytes as the
// workers do. With a ceiling of two envelopes, five records go out as writes
// of at most two -- from a dump and from a ring.db.
func TestBootWritesInPiecesUnderBatchMaxBytes(t *testing.T) {
	_, env := testJLH(t, testID, "/d/0", `{"i":0}`)
	ceiling := fmt.Sprint(2*len(env) + 8) // two records of this size, not three

	check := func(t *testing.T, rs *refusingSink) {
		t.Helper()
		total := 0
		for _, w := range rs.got() {
			if w[0] > 2 {
				t.Errorf("a write of %d items (%d bytes) under a ceiling of %s", w[0], w[1], ceiling)
			}
			total += w[0]
		}
		if total != 5 {
			t.Errorf("wrote %d items in %v, want 5", total, rs.got())
		}
	}

	t.Run("dump", func(t *testing.T) {
		dir := t.TempDir()
		plantDump(t, dir, 5)
		rs := &refusingSink{}
		in := startHooked(t, dir, testID, func(a *App) {
			a.wrapSink = func(s sink.Sink) sink.Sink { rs.Sink = s; return rs }
		}, append([]string{"--ring.mode=memory", "--sink.batch_max_bytes=" + ceiling}, sqliteSink...)...)
		check(t, rs)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
	})
	// And the workers, which get the key from the assembly: five posts inside
	// one sink.batch_max_delay are one Dequeue, written in pieces.
	t.Run("workers", func(t *testing.T) {
		dir := t.TempDir()
		rs := &refusingSink{}
		in := startHooked(t, dir, testID, func(a *App) {
			a.wrapSink = func(s sink.Sink) sink.Sink { rs.Sink = s; return rs }
		}, append([]string{"--ring.mode=memory", "--sink.batch_max_delay=500ms",
			"--sink.batch_max_bytes=" + ceiling}, sqliteSink...)...)
		if ids := in.postN(5); len(ids) != 5 {
			t.Fatalf("accepted %d of 5", len(ids))
		}
		if !waitFor(5*time.Second, func() bool {
			n := 0
			for _, w := range rs.got() {
				n += w[0]
			}
			return n == 5
		}) {
			t.Fatalf("the workers wrote %v, want 5 items", rs.got())
		}
		check(t, rs)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
	})
	t.Run("ring.db", func(t *testing.T) {
		dir := t.TempDir()
		plantRing(t, filepath.Join(dir, "ring.db"), 5)
		rs := &refusingSink{}
		in := startHooked(t, dir, testID, func(a *App) {
			a.wrapSink = func(s sink.Sink) sink.Sink { rs.Sink = s; return rs }
		}, append([]string{"--ring.mode=sqlite", "--sink.batch_max_bytes=" + ceiling}, sqliteSink...)...)
		check(t, rs)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
	})
}

// partialSink stores every item of a batch but the bad one and answers logic
// with Delivered, the way sink=nats reports a stream that refused one message
// (SPEC §8.1 step 5); it counts the writes of every uuid.
type partialSink struct {
	sink.Sink
	bad string

	mu     sync.Mutex
	writes map[string]int
}

func (s *partialSink) Write(_ context.Context, items []*envelope.Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stored []int
	for i, it := range items {
		if it.ID == s.bad {
			continue
		}
		s.writes[it.ID]++
		stored = append(stored, i)
	}
	if len(stored) == len(items) {
		return nil
	}
	return &sink.Error{Class: sink.ClassLogic, Op: "test", Err: errors.New("one refused"), Delivered: stored}
}

// SPEC §8.1 step 5 and §9.2 (W §19.1 item 33): a logic refusal that says what
// the destination stored is settled by that list -- the stored items count as
// written, the refused ones go to onRefused -- and nothing is sent again. The
// item-by-item retry republished every item the stream had already kept,
// duplicates that only Nats-Msg-Id inside sink.nats.duplicates absorbs.
func TestBootWriteHonoursPartialDelivery(t *testing.T) {
	a, _ := abortApp(t)
	var items []*envelope.Item
	for i := range 4 {
		id, env := testJLH(t, testID, fmt.Sprintf("/partial/%d", i), `{}`)
		items = append(items, envelope.NewItem(id, env, -1))
	}
	ps := &partialSink{Sink: sink.NewNone(), bad: items[2].ID, writes: map[string]int{}}
	a.sink = ps
	var refusedIDs []string
	refused, err := a.writeOrSingle(context.Background(), items, func(it *envelope.Item, _ error) {
		refusedIDs = append(refusedIDs, it.ID)
	})
	if err != nil || refused != 1 || len(refusedIDs) != 1 || refusedIDs[0] != items[2].ID {
		t.Fatalf("refused %d (%v), err %v; want the one bad item", refused, refusedIDs, err)
	}
	for _, it := range items {
		if it.ID != items[2].ID && ps.writes[it.ID] != 1 {
			t.Errorf("item %s written %d times, want once", it.ID, ps.writes[it.ID])
		}
	}
	if got := a.metrics.EventsWrittenTotal.Load(); got != 3 {
		t.Errorf("events written %d, want 3", got)
	}
}
