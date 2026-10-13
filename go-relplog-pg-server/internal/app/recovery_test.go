package app

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/sink"
)

// Recovery from what a previous life left behind, and the abort that leaves
// nothing behind.

// SPEC §13.4: the second signal exits at once with 130 "aceitando a perda
// (contada em reason="abort")": every accepted message that was not delivered
// -- held by a retrying worker or still in the ring -- is counted, and nothing
// is dumped.
func TestSecondSignalCountsTheAbortedLoss(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, testID, append(baseArgs(t, dir),
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
	if files, _ := filepath.Glob(filepath.Join(in.emgDir(), "emg-*")); len(files) != 0 {
		t.Errorf("the abort wrote a dump: %v", files)
	}
}

// crashItems builds n items as a previous life would have accepted them, for
// the boot paths to deliver.
func crashItems(n int, prefix string) []*item.Item {
	now := time.Now()
	var items []*item.Item
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf("<13>Oct 11 22:14:15 localhost app[%d]: %s-%d", i, prefix, i))
		at := now.Add(time.Duration(i) * time.Millisecond)
		items = append(items, item.New(crashHead(at, raw), raw, -1))
	}
	return items
}

// dumpOf writes items as the emergency dump of a previous life would hold
// them, in the emergency.dir a server booted on dataDir uses by default.
func dumpOf(t *testing.T, dataDir string, items []*item.Item) string {
	t.Helper()
	res, err := emergency.NewStore(filepath.Join(dataDir, "emergency"), testID.From).
		Write(items, testConverter(t), true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != len(items) {
		t.Fatalf("the dump holds %d of %d items", res.Written, len(items))
	}
	return res.Path
}

// SPEC §12.1: a crash between the link and the removal of the temporary
// leaves a twin -- the same file under the hidden name -- of a dump the boot
// reingests and removes; the twin goes too. An interrupted dump's temporary,
// which is no twin, is left for the operator, and the WARN that names it is
// the only thing telling them it is there.
func TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne(t *testing.T) {
	dir := t.TempDir()
	path := dumpOf(t, dir, crashItems(3, "twin"))
	intact := dumpLines(t, path)
	twin := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.Link(path, twin); err != nil {
		t.Fatal(err)
	}
	cut := filepath.Join(filepath.Dir(path), "."+emergency.FileName(testID.From, time.Now().Add(-time.Hour))+".tmp")
	if err := os.WriteFile(cut, []byte("half a dump"), 0o640); err != nil {
		t.Fatal(err)
	}
	var log syncBuffer
	in := launch(t, testID, baseArgs(t, dir), nil, func(a *App) {
		a.log = obs.NewLogger(&log, "text", "info")
	})
	in.waitReady()
	checkReingested(t, intact, in.rows())
	for _, p := range []string{path, twin} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there: %v", filepath.Base(p), err)
		}
	}
	if _, err := os.Stat(cut); err != nil {
		t.Errorf("the interrupted dump was touched: %v", err)
	}
	if out := log.String(); !strings.Contains(out, "interrupted emergency dumps") || !strings.Contains(out, cut) ||
		strings.Contains(out, filepath.Base(twin)) {
		t.Errorf("the boot did not report the interrupted dump, and only it:\n%s", out)
	}
}

// SPEC §12.2: a damaged dump -- one invalid line in the middle, a last line cut
// mid-write -- contributes its intact lines, exactly the envelopes it holds,
// and is then renamed to .corrupt instead of being deleted.
func TestDamagedDumpIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	items := crashItems(4, "from-a-crash")
	path := dumpOf(t, dir, items)
	intact := dumpLines(t, path)

	// Rebuild the file around the intact lines: two of them, a line that is
	// JSON but no envelope of this server, the other two, and a crash in the
	// middle of a fifth.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(b, []byte("\n"))
	var damaged []byte
	damaged = append(damaged, lines[0]...)
	damaged = append(damaged, lines[1]...)
	damaged = append(damaged, `{"uuid":"not-an-envelope"}`+"\n"...)
	damaged = append(damaged, lines[2]...)
	damaged = append(damaged, lines[3]...)
	damaged = append(damaged, lines[0][:len(lines[0])/2]...)
	if err := os.WriteFile(path, damaged, 0o640); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir)
	checkReingested(t, intact, in.rows())
	if left := dumpFiles(t, in.emgDir(), testID.From); len(left) != 0 {
		t.Errorf("the damaged dump is still pending: %v", left)
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		all, _ := os.ReadDir(in.emgDir())
		t.Errorf("the dump was not renamed to .corrupt (%v); dir holds %v", err, all)
	}
	if got := in.metric("relplog_emergency_files"); got != "0" {
		t.Errorf("relplog_emergency_files = %s after the reingestion", got)
	}
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

// SPEC §10.2, §12.2, §13.1 step 9: the dump reingestion follows the drain
// workers' rules chunk by chunk. A logic refusal loses that chunk, counted, and
// the chunks after it are still written (and the dump is settled, or the next
// boot would refuse it again forever); a transient failure is retried with the
// same bytes; a fatal one is exit 3 with the dump left on disk.
func TestBootDeliveryFollowsTheWorkerRules(t *testing.T) {
	// One envelope per chunk, so a refusal is exactly one message.
	oneChunk := "--sink.batch_max_bytes=1"
	messages := func(t *testing.T, in *instance) map[string]bool {
		out := map[string]bool{}
		for _, r := range in.rows() {
			out[checkRow(t, r).Payload.message()] = true
		}
		return out
	}

	t.Run("logic refusal in the middle", func(t *testing.T) {
		dir := t.TempDir()
		items := crashItems(5, "dump-logic")
		path := dumpOf(t, dir, items)
		fs := newFaultySink(false)
		fs.decide = refuseOne(items[2].Head.UUID.String())
		in := launch(t, testID, append(baseArgs(t, dir), oneChunk), fs.wrap)
		in.waitReady()
		got := messages(t, in)
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

	t.Run("transient failures are retried", func(t *testing.T) {
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
		in := launch(t, testID, append(baseArgs(t, dir), oneChunk,
			"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms"), fs.wrap)
		in.waitReady()
		if got := messages(t, in); len(got) != 3 {
			t.Errorf("delivered %v, want all three", got)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("the reingested dump is still on disk: %v", err)
		}
	})

	t.Run("fatal leaves the dump and exits 3", func(t *testing.T) {
		dir := t.TempDir()
		path := dumpOf(t, dir, crashItems(3, "dump-fatal"))
		fs := newFaultySink(false)
		fs.failFatal.Store(true)
		in := launch(t, testID, append(baseArgs(t, dir), oneChunk), fs.wrap)
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the undelivered dump was removed: %v", err)
		}
		if n := in.count(); n != 0 {
			t.Errorf("%d rows written by a fatal sink", n)
		}
	})
}

// syncBuffer is a bytes.Buffer a logger and the test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// SPEC §12.2, S §22.1 item 25 (the parent's fix, ported): at the end of the
// reingestion the emergency gauges are recounted from the directory, never
// zeroed. A dump that cannot be read stays and counts as a file (its messages
// are unknowable); a dump that was delivered but could not be removed stays
// and counts with its messages. Zeroed -- what go-syslog-pg-server did -- the
// alert on these gauges went quiet with both still on disk.
func TestEmergencyGaugesRecountTheDiskAfterReingestion(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads and removes through any permission")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	unreadable := dumpOf(t, dir, crashItems(2, "unreadable"))
	if err := os.Chmod(unreadable, 0); err != nil {
		t.Fatal(err)
	}
	const n = 3
	kept := dumpOf(t, dir, crashItems(n, "kept"))
	keptLines := dumpLines(t, kept)
	// A directory that can be read but not written: the delivered dump
	// cannot be removed. emergency.enabled=false, because an enabled
	// emergency proves at boot that the directory is writable (SPEC §12.3);
	// the reingestion reads it whatever emergency.enabled says (§12.2).
	if err := os.Chmod(emgDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(emgDir, 0o700); os.Chmod(unreadable, 0o600) })

	in := start(t, dir, "--emergency.enabled=false")
	checkReingested(t, keptLines, in.rows())
	if left, _ := emergency.NewStore(emgDir, testID.From).List(); len(left) != 2 {
		t.Fatalf("dumps on disk = %v, want both still there", left)
	}
	if got := in.metric("relplog_emergency_files"); got != "2" {
		t.Errorf("relplog_emergency_files = %s with two dumps on disk, want 2", got)
	}
	if got := in.metric("relplog_emergency_events_pending"); got != strconv.Itoa(n) {
		t.Errorf("relplog_emergency_events_pending = %s, want the %d of the dump left on disk", got, n)
	}
}

// SPEC §12.2, S §22.1 item 24, and the same rule for Remove (the parent's
// fixes, ported): an ErrNotDurable means the rename (or the unlink) HAPPENED
// and only the directory fsync after it failed. The quarantine is then
// reported as done -- under the new name, with the byte counts of what was
// lost -- plus a WARN about the fsync; the removal is a WARN about a dump that
// may reappear. Only an empty name (or another error) is "cannot". Neither
// fsync failure can be provoked on a real filesystem, so the reporting is
// driven directly.
func TestQuarantineAndRemovalReportTheirOutcome(t *testing.T) {
	var buf logBuf
	a := &App{log: obs.NewLogger(&buf, "json", "info")}
	notDurable := fmt.Errorf("%w: fsync of directory /x: input/output error", emergency.ErrNotDurable)
	res := &emergency.ReadResult{Truncated: true, TruncatedBytes: 1234, Invalid: 2, InvalidBytes: 77}

	a.reportQuarantine("/d/emg-a.jsonl", "/d/emg-a.jsonl.corrupt", notDurable, res)
	log := buf.String()
	for _, want := range []string{"directory fsync failed", "file was quarantined",
		`"path":"/d/emg-a.jsonl.corrupt"`, `"truncated_bytes":1234`, `"invalid_bytes":77`} {
		if !strings.Contains(log, want) {
			t.Errorf("quarantined without a durable name: the log lacks %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "cannot quarantine") {
		t.Errorf("a quarantine that happened is reported as not done:\n%s", log)
	}

	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportQuarantine("/d/emg-b.jsonl", "", errors.New("permission denied"), res)
	if log := buf.String(); !strings.Contains(log, "cannot quarantine") || strings.Contains(log, "file was quarantined") {
		t.Errorf("a failed rename must be reported as not quarantined:\n%s", log)
	}

	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-c.jsonl", notDurable)
	if log := buf.String(); !strings.Contains(log, "may reappear") || strings.Contains(log, "cannot remove") ||
		!strings.Contains(log, `"level":"WARN"`) {
		t.Errorf("a removal that happened is not reported as a WARN about the fsync:\n%s", log)
	}
	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-d.jsonl", errors.New("permission denied"))
	if log := buf.String(); !strings.Contains(log, "cannot remove a reingested dump") {
		t.Errorf("a failed removal is not an ERROR:\n%s", log)
	}
	buf = logBuf{}
	a.log = obs.NewLogger(&buf, "json", "info")
	a.reportRemoval("/d/emg-e.jsonl", nil)
	if log := buf.String(); log != "" {
		t.Errorf("a clean removal logged %q", log)
	}
}
