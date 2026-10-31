package app

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/sink"
)

// Recovery from what a previous life left behind, and the abort that leaves
// nothing behind.

// SPEC §13.4: the second signal exits at once with 130 "aceitando a perda
// (contada em reason="abort" com ring=memory)": every accepted message that
// was not delivered -- held by a retrying worker or still in the ring -- is
// counted, and nothing is dumped.
func TestSecondSignalCountsTheAbortedLoss(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, testID, append(baseArgs(dir),
		"--ring.mode=memory",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		// Long enough that the graceful half of the shutdown never wakes
		// during the test process: in the binary, the exit ends it.
		"--shutdown.pre_stop_delay=1h"), fs.wrap)
	in.waitReady()
	const n = 9
	lines := make([]string, n)
	for i := range lines {
		lines[i] = doc(fmt.Sprintf("abort-%d", i))
	}
	sendNUL(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted and a worker retrying", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && fs.attempts.Load() > 1
	})
	in.cancel()
	waitFor(t, 5*time.Second, "/readyz to report the shutdown", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "shutting down")
	})
	in.app.abort <- os.Interrupt
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if got := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != n {
		t.Errorf(`gelf_events_dropped_total{reason="abort"} = %d, want %d`, got, n)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*")); len(files) != 0 {
		t.Errorf("the abort wrote a dump: %v", files)
	}
}

// SPEC §12.2: a dump damaged at the end contributes its intact records -- the
// same envelopes the converter makes from them -- and is then renamed to
// .corrupt instead of being deleted.
func TestDamagedDumpIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	now := time.Now()
	var items []*item.Item
	for i := 0; i < 4; i++ {
		raw := []byte(doc(fmt.Sprintf("from-a-crash-%d", i)))
		at := now.Add(time.Duration(i) * time.Millisecond)
		h := item.Head{UUID: jlh.NewUUID(at), From: testID.From, Space: testID.Space,
			Transport: item.TransportUDP, SrcIP: netip.MustParseAddr("192.0.2.7"), SrcPort: uint16(40000 + i),
			DstPort: 12201}
		h.SetTime(at)
		items = append(items, item.New(h, raw, -1))
	}
	path, err := emergency.NewStore(emgDir, testID.From).Write(items)
	if err != nil {
		t.Fatal(err)
	}
	// A crash in the middle of the next record: a header and half a head.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("120 40\n{\"v\":1,\"uu")
	f.Close()

	in := start(t, dir, "--listen.udp=")
	rows := sqliteRows(t, in.eventsDB())
	if len(rows) != len(items) {
		t.Fatalf("%d rows from a dump with %d intact records", len(rows), len(items))
	}
	conv := convert.New(in.app.convertConfig())
	byUUID := map[string]eventRow{}
	for _, r := range rows {
		checkRow(t, r)
		byUUID[r.UUID] = r
	}
	for _, it := range items {
		want, _ := conv.Append(nil, &it.Head, it.Raw)
		if r, ok := byUUID[it.Head.UUID.String()]; !ok || !bytes.Equal(r.Env, want) {
			t.Errorf("record %s was not delivered as its own envelope", it.Head.UUID)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(emgDir, "emg-*.bin")); len(left) != 0 {
		t.Errorf("the damaged dump is still pending: %v", left)
	}
	if q, _ := filepath.Glob(filepath.Join(emgDir, "*.corrupt")); len(q) != 1 {
		all, _ := os.ReadDir(emgDir)
		t.Errorf("quarantined files = %v (dir %v), want the dump renamed to .corrupt", q, all)
	}
}

// SPEC §9.4: an undecodable row in ring.db does not keep the boot waiting
// forever. It is moved to the quarantine table, the accounting is corrected,
// gelf_ring_quarantined_total counts it, and the transports open.
func TestRingSQLitePoisonedRowIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	args := []string{"--ring.mode=sqlite", "--listen.udp="}
	first := start(t, dir, args...)
	ringDB := first.app.cfg.RingSQL.Path
	if code := first.stop(); code != ExitOK {
		t.Fatalf("first life exited %d", code)
	}
	db := openSQLite(t, ringDB)
	if _, err := db.Exec(`INSERT INTO ring (uuid, head, raw, nbytes) VALUES (?, ?, ?, ?)`,
		"poison", []byte("{"), []byte("raw"), 3); err != nil {
		t.Fatal(err)
	}
	db.Close()

	in := start(t, dir, args...) // fails the test if the transports never open
	if got := in.metric("gelf_ring_quarantined_total"); got != "1" {
		t.Errorf("gelf_ring_quarantined_total = %s, want 1", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if got := sqliteCount(t, ringDB, "ring"); got != 0 {
		t.Errorf("%d rows left in ring", got)
	}
	db = openSQLite(t, ringDB)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM quarantine WHERE uuid = 'poison'`).Scan(&n); err != nil || n != 1 {
		t.Errorf("quarantine holds %d poison rows (%v), want 1", n, err)
	}
}

// crashItems builds n items as a previous life would have accepted them, for
// the boot paths to deliver.
func crashItems(n int, prefix string) []*item.Item {
	now := time.Now()
	var items []*item.Item
	for i := 0; i < n; i++ {
		raw := []byte(doc(fmt.Sprintf("%s-%d", prefix, i)))
		at := now.Add(time.Duration(i) * time.Millisecond)
		h := item.Head{UUID: jlh.NewUUID(at), From: testID.From, Space: testID.Space,
			Transport: item.TransportUDP, SrcIP: netip.MustParseAddr("192.0.2.7"), SrcPort: uint16(40000 + i),
			DstPort: 12201}
		h.SetTime(at)
		items = append(items, item.New(h, raw, -1))
	}
	return items
}

// refuseOne fails, as ClassLogic, every chunk carrying uuid.
func refuseOne(uuid string) func(*sink.Batch) error {
	return func(b *sink.Batch) error {
		for i := range b.Envelopes {
			if b.Envelopes[i].UUID() == uuid {
				return &sink.Error{Class: sink.ClassLogic, Op: "write", Err: errors.New("value too long (injected)")}
			}
		}
		return nil
	}
}

// SPEC §10.2, §12.2, §13.1 steps 9-10: the boot deliveries -- dump
// reingestion and the ring=sqlite backlog -- follow the drain workers' rules
// chunk by chunk. A logic refusal loses that chunk, counted, and the chunks
// after it are still written (and the dump or the rows are settled, or the
// next boot would refuse them again forever); a transient failure is retried
// with the same bytes; a fatal one is exit 3 with the dump left on disk.
func TestBootDeliveryFollowsTheWorkerRules(t *testing.T) {
	// One envelope per chunk, so a refusal is exactly one message.
	oneChunk := "--sink.batch_max_bytes=1"
	dumpOf := func(t *testing.T, dir string, items []*item.Item) string {
		t.Helper()
		path, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Write(items)
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	messages := func(t *testing.T, path string) map[string]bool {
		out := map[string]bool{}
		for _, r := range sqliteRows(t, path) {
			out[checkRow(t, r).Payload.message()] = true
		}
		return out
	}

	t.Run("dump: logic refusal in the middle", func(t *testing.T) {
		dir := t.TempDir()
		items := crashItems(5, "dump-logic")
		path := dumpOf(t, dir, items)
		fs := newFaultySink(false)
		fs.decide = refuseOne(items[2].Head.UUID.String())
		in := launch(t, testID, append(baseArgs(dir), oneChunk), fs.wrap)
		in.waitReady()
		got := messages(t, in.eventsDB())
		for i := range items {
			if want := i != 2; got[fmt.Sprintf("dump-logic-%d", i)] != want {
				t.Errorf("dump-logic-%d delivered = %v, want %v", i, !want, want)
			}
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); d != 1 {
			t.Errorf(`gelf_events_dropped_total{reason="sink_logic"} = %d, want 1`, d)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("the settled dump is still on disk: %v", err)
		}
	})

	t.Run("dump: transient failures are retried", func(t *testing.T) {
		dir := t.TempDir()
		items := crashItems(3, "dump-retry")
		path := dumpOf(t, dir, items)
		fs := newFaultySink(false)
		var failures atomic.Int64
		fs.decide = func(*sink.Batch) error {
			if failures.Add(1) <= 3 {
				return errors.New("connection reset (injected)")
			}
			return nil
		}
		in := launch(t, testID, append(baseArgs(dir), oneChunk,
			"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms"), fs.wrap)
		in.waitReady()
		if got := messages(t, in.eventsDB()); len(got) != 3 {
			t.Errorf("delivered %v, want all three", got)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("the reingested dump is still on disk: %v", err)
		}
	})

	t.Run("dump: fatal leaves the dump and exits 3", func(t *testing.T) {
		dir := t.TempDir()
		path := dumpOf(t, dir, crashItems(3, "dump-fatal"))
		fs := newFaultySink(false)
		fs.failFatal.Store(true)
		in := launch(t, testID, append(baseArgs(dir), oneChunk), fs.wrap)
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the undelivered dump was removed: %v", err)
		}
	})

	t.Run("ring=sqlite backlog: logic refusal in the middle", func(t *testing.T) {
		dir := t.TempDir()
		args := append(baseArgs(dir), "--ring.mode=sqlite", oneChunk)
		first := start(t, dir, "--ring.mode=sqlite")
		ringDB := first.app.cfg.RingSQL.Path
		if code := first.stop(); code != ExitOK {
			t.Fatalf("first life exited %d", code)
		}
		items := crashItems(5, "backlog-logic")
		db := openSQLite(t, ringDB)
		for _, it := range items {
			if _, err := db.Exec(`INSERT INTO ring (uuid, head, raw, nbytes) VALUES (?, ?, ?, ?)`,
				it.Head.UUID.String(), item.EncodeHead(nil, &it.Head), it.Raw, len(it.Raw)); err != nil {
				t.Fatal(err)
			}
		}
		db.Close()

		fs := newFaultySink(false)
		fs.decide = refuseOne(items[1].Head.UUID.String())
		in := launch(t, testID, args, fs.wrap)
		in.waitReady()
		got := messages(t, in.eventsDB())
		for i := range items {
			if want := i != 1; got[fmt.Sprintf("backlog-logic-%d", i)] != want {
				t.Errorf("backlog-logic-%d delivered = %v, want %v", i, !want, want)
			}
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); d != 1 {
			t.Errorf(`gelf_events_dropped_total{reason="sink_logic"} = %d, want 1`, d)
		}
		// The ring's writer goroutine runs the DELETEs of a commit
		// asynchronously; the accounting is already zero at Ready.
		if !eventually(10*time.Second, func() bool { return sqliteCount(t, ringDB, "ring") == 0 }) {
			t.Errorf("%d rows left in ring.db: a refused chunk must be committed, not redelivered forever",
				sqliteCount(t, ringDB, "ring"))
		}
	})
}
