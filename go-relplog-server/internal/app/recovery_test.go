package app

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
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
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - abort-%d", i)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
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
		t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d, want %d`, got, n)
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
		raw := []byte(fmt.Sprintf("<13>Oct 11 22:14:15 localhost app[%d]: from-a-crash-%d", i, i))
		at := now.Add(time.Duration(i) * time.Millisecond)
		items = append(items, item.New(crashHead(at, raw), raw, -1))
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

	in := start(t, dir)
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
// relplog_ring_quarantined_total counts it, and the transports open.
func TestRingSQLitePoisonedRowIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	args := []string{"--ring.mode=sqlite"}
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
	if got := in.metric("relplog_ring_quarantined_total"); got != "1" {
		t.Errorf("relplog_ring_quarantined_total = %s, want 1", got)
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

// crashHead is the head a previous life of THIS server wrote for raw: RELP in
// clear, octet-counting, received at at from an emitter on the loopback.
func crashHead(at time.Time, raw []byte) item.Head {
	h := item.Head{UUID: jlh.NewUUID(at), From: testID.From, Space: testID.Space,
		Transport: item.TransportTCP, Framing: item.FramingOctetCounting, Size: len(raw),
		SrcIP: netip.MustParseAddr("127.0.0.1"), SrcPort: 40514,
		DstIP: netip.MustParseAddr("127.0.0.1"), DstPort: 20514}
	h.SetTime(at)
	return h
}

// crashItems builds n items as a previous life would have accepted them, for
// the boot paths to deliver.
func crashItems(n int, prefix string) []*item.Item {
	now := time.Now()
	var items []*item.Item
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf("<13>Oct 11 22:14:15 localhost app: %s-%d", prefix, i))
		at := now.Add(time.Duration(i) * time.Millisecond)
		items = append(items, item.New(crashHead(at, raw), raw, -1))
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
			t.Errorf(`relplog_events_dropped_total{reason="sink_logic"} = %d, want 1`, d)
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
			t.Errorf(`relplog_events_dropped_total{reason="sink_logic"} = %d, want 1`, d)
		}
		// The ring's writer goroutine runs the DELETEs of a commit
		// asynchronously; the accounting is already zero at Ready.
		if !eventually(10*time.Second, func() bool { return sqliteCount(t, ringDB, "ring") == 0 }) {
			t.Errorf("%d rows left in ring.db: a refused chunk must be committed, not redelivered forever",
				sqliteCount(t, ringDB, "ring"))
		}
	})
}

// SPEC §12.2, §22.1 item 25: at the end of the reingestion the emergency
// gauges are recounted from the directory, never zeroed. A dump that cannot be
// read stays and counts as a file (its messages are unknowable); a dump that
// was delivered but could not be removed stays and counts with its messages.
// Zeroed, the alert on these gauges went quiet with both still on disk.
func TestEmergencyGaugesRecountTheDiskAfterReingestion(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads and removes through any permission")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	store := emergency.NewStore(emgDir, testID.From)
	unreadable, err := store.Write(crashItems(2, "unreadable"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	const n = 3
	if _, err := store.Write(crashItems(n, "kept")); err != nil {
		t.Fatal(err)
	}
	// A directory that can be read but not written: the delivered dump
	// cannot be removed. ring=sqlite, because ring=memory proves at boot that
	// the directory is writable (SPEC §12.3).
	if err := os.Chmod(emgDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(emgDir, 0o700); os.Chmod(unreadable, 0o600) })

	in := start(t, dir, "--ring.mode=sqlite")
	if got := len(sqliteRows(t, in.eventsDB())); got != n {
		t.Fatalf("%d rows reingested, want the %d of the readable dump", got, n)
	}
	if left, _ := store.List(); len(left) != 2 {
		t.Fatalf("dumps on disk = %v, want both still there", left)
	}
	if got := in.metric("relplog_emergency_files"); got != "2" {
		t.Errorf("relplog_emergency_files = %s with two dumps on disk, want 2", got)
	}
	if got := in.metric("relplog_emergency_events_pending"); got != strconv.Itoa(n) {
		t.Errorf("relplog_emergency_events_pending = %s, want the %d of the dump left on disk", got, n)
	}
}

// SPEC §12.1, §12.2, §22.1 items 23 and 26. With sink=none the dumps of a
// previous life are NOT reingested: sink=none discards what it is given, and
// "delivering" them there destroyed messages a real destination's life had
// answered 200. They stay, named in a WARN and counted in the gauges. An
// interrupted dump -- its hidden temporary -- is reported too, never read nor
// removed, whatever the sink.
func TestSinkNoneKeepsTheDumpsAndReportsTheInterruptedOnes(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	const n = 4
	path, err := emergency.NewStore(emgDir, testID.From).Write(crashItems(n, "kept-by-none"))
	if err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(emgDir, "."+emergency.FileName(testID.From, time.Now().Add(time.Second))+".tmp")
	if err := os.WriteFile(tmp, []byte("12 3\npartial"), 0o640); err != nil {
		t.Fatal(err)
	}

	var buf logBuf
	in := launch(t, testID, append(baseArgs(dir), "--sink.type=none"), nil, captureLog(&buf))
	in.waitReady()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("sink=none destroyed the dump: %v", err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("the interrupted dump was touched: %v", err)
	}
	if got := in.app.metrics.EventsWrittenTotal.Load(); got != 0 {
		t.Errorf("%d dumped messages were handed to sink=none", got)
	}
	if got := in.metric("relplog_emergency_files"); got != "1" {
		t.Errorf("relplog_emergency_files = %s, want 1", got)
	}
	if got := in.metric("relplog_emergency_events_pending"); got != strconv.Itoa(n) {
		t.Errorf("relplog_emergency_events_pending = %s, want %d", got, n)
	}
	log := buf.String()
	for _, want := range []string{"sink.type=none discards what it is given", path,
		"interrupted emergency dumps found", tmp} {
		if !strings.Contains(log, want) {
			t.Errorf("the boot log lacks %q:\n%s", want, log)
		}
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	// The next life with a destination reingests the kept dump.
	second := start(t, dir)
	if got := len(sqliteRows(t, second.eventsDB())); got != n {
		t.Errorf("the life with a destination reingested %d of %d", got, n)
	}
}

// SPEC §12.2, §22.1 item 24, and the same rule for Remove: an ErrNotDurable
// means the rename (or the unlink) HAPPENED and only the directory fsync after
// it failed. The quarantine is then reported as done -- under the new name,
// with the byte counts of what was lost -- plus a WARN about the fsync; the
// removal is a WARN about a dump that may reappear. Only an empty name (or
// another error) is "cannot". Neither fsync failure can be provoked on a real
// filesystem, so the reporting is driven directly.
func TestQuarantineAndRemovalReportTheirOutcome(t *testing.T) {
	var buf logBuf
	a := &App{log: obs.NewLogger(&buf, "json", "info")}
	notDurable := fmt.Errorf("%w: fsync of directory /x: input/output error", emergency.ErrNotDurable)
	res := &emergency.ReadResult{Truncated: true, TruncatedBytes: 1234, Undecodable: 2, UndecodableBytes: 77}

	a.reportQuarantine("/d/emg-a.bin", "/d/emg-a.bin.corrupt", notDurable, res)
	log := buf.String()
	for _, want := range []string{"directory fsync failed", "file was quarantined",
		`"path":"/d/emg-a.bin.corrupt"`, `"discarded_bytes":1234`, `"undecodable_bytes":77`} {
		if !strings.Contains(log, want) {
			t.Errorf("quarantined without a durable name: the log lacks %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "cannot quarantine") {
		t.Errorf("a quarantine that happened is reported as not done:\n%s", log)
	}

	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportQuarantine("/d/emg-b.bin", "", errors.New("permission denied"), res)
	if log := buf.String(); !strings.Contains(log, "cannot quarantine") || strings.Contains(log, "file was quarantined") {
		t.Errorf("a failed rename must be reported as not quarantined:\n%s", log)
	}

	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-c.bin", notDurable)
	if log := buf.String(); !strings.Contains(log, "may reappear") || strings.Contains(log, "cannot remove") ||
		!strings.Contains(log, `"level":"WARN"`) {
		t.Errorf("a removal that happened is not reported as a WARN about the fsync:\n%s", log)
	}
	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-d.bin", errors.New("permission denied"))
	if log := buf.String(); !strings.Contains(log, "cannot remove a reingested dump") {
		t.Errorf("a failed removal is not an ERROR:\n%s", log)
	}
	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-e.bin", nil)
	if log := buf.String(); log != "" {
		t.Errorf("a clean removal logged %q", log)
	}
}
