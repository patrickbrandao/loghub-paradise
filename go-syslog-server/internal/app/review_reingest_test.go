package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// The reingestion of SPEC §12.2 (§22 pendências 23-26), ported from
// go-relplog-server (recovery_test.go there) and go-relplog-nats-server
// (review_cut_test.go).

// SPEC §12.2, §22 pendência 25: the gauges are recounted from the directory at
// the end of the reingestion, never zeroed. Two dumps stay on disk -- one that
// cannot be read, one delivered whose removal fails -- and the gauges say so.
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
	if got := in.metric("syslog_emergency_files"); got != "2" {
		t.Errorf("syslog_emergency_files = %s with two dumps on disk, want 2", got)
	}
	if got := in.metric("syslog_emergency_events_pending"); got != strconv.Itoa(n) {
		t.Errorf("syslog_emergency_events_pending = %s, want the %d of the dump left on disk", got, n)
	}
}

// SPEC §12.1, §12.2, §22 pendências 23 and 26. With sink=none the dumps of a
// previous life are NOT reingested: sink=none discards what it is given, and
// "delivering" them there destroyed messages a real destination's life had
// accepted. They stay, named in a WARN and counted in the gauges. An
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
	if got := in.metric("syslog_emergency_files"); got != "1" {
		t.Errorf("syslog_emergency_files = %s, want 1", got)
	}
	if got := in.metric("syslog_emergency_events_pending"); got != strconv.Itoa(n) {
		t.Errorf("syslog_emergency_events_pending = %s, want %d", got, n)
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

// SPEC §12.2, §22 pendência 24, and the same rule for Remove: an ErrNotDurable
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

// SPEC §12.2: the dump written at the shutdown ADDS to the gauges. A dump the
// boot could not reingest (here, unreadable) is still pending, and setting the
// gauges to the new dump alone forgot it (ported from go-relplog-nats-server,
// mutant M18 of the review of go-relplog-pg-server).
func TestShutdownDumpAddsToTheGauges(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads through any permission")
	}
	dir := t.TempDir()
	old, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Write(crashItems(2, "old"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(old, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(old, 0o600) })
	fs := newFaultySink(true)
	fs.failFatal.Store(true)
	in := launch(t, testID, append(baseArgs(dir), "--listen.udp=", "--ring.mode=memory"), fs.wrap)
	in.waitReady()
	if got := in.metric("syslog_emergency_files"); got != "1" {
		t.Fatalf("syslog_emergency_files = %s with the unreadable dump on disk, want 1", got)
	}
	const n = 3
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - gauge-%d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	fs.open()
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if fl, e := m.EmergencyFiles.Load(), m.EmergencyEventsPending.Load(); fl != 2 || e != n {
		t.Fatalf("after the dump: files %d, events %d; want 2 files (the old one too) and %d events", fl, e, n)
	}
}

// scriptSink answers each Write with script(n), n counting from 1.
type scriptSink struct {
	calls  atomic.Int32
	script func(n int) error
}

func (s *scriptSink) Write(ctx context.Context, b *sink.Batch) error {
	return s.script(int(s.calls.Add(1)))
}
func (s *scriptSink) Health(context.Context) error { return nil }
func (s *scriptSink) Close() error                 { return nil }
func (s *scriptSink) Type() string                 { return "script" }

// reingestApp is the part of an App that reingest reads, with sink.batch=1
// and the given sink.boot_timeout; the dumps go in the returned directory.
func reingestApp(t *testing.T, s sink.Sink, bootTimeout time.Duration) (*App, *emergency.Store) {
	t.Helper()
	cfg := config.Defaults()
	cfg.Emergency.Dir = filepath.Join(t.TempDir(), "emergency")
	cfg.Sink.Type = config.SinkTypeSQLite
	cfg.Sink.Batch = 1
	cfg.Sink.OpTimeout = time.Second
	cfg.Sink.RetryBackoffMin = 10 * time.Millisecond
	cfg.Sink.RetryBackoffMax = 20 * time.Millisecond
	cfg.Sink.BootTimeout = bootTimeout
	m := obs.NewMetrics(obs.Info{})
	a := &App{cfg: cfg, id: testID, metrics: m, log: obs.NewLogger(io.Discard, "text", "error"),
		admin: NewAdmin(m, 0.8)}
	a.direct = sink.NewInstrumented(s, nil, a.workerConfig(), a.log.Logger, m)
	return a, emergency.NewStore(cfg.Emergency.Dir, testID.From)
}

// The rule go-relplog-nats-server fixed for its streamed reingestion (one
// sink.boot_timeout WITHOUT PROGRESS, independent review of 2026-10-13) holds
// here by construction -- every chunk gets its own deadline in deliverDirect
// -- and this guards it: a healthy reingestion that runs past the budget
// still retries a transient failure, and a sink that never recovers still
// ends the boot with 3.
func TestLongReingestionStillRetriesATransientFailure(t *testing.T) {
	s := &scriptSink{script: func(n int) error {
		time.Sleep(150 * time.Millisecond)
		if n == 3 {
			return &sink.Error{Class: sink.ClassNetwork, Op: "write", Err: errors.New("blip")}
		}
		return nil
	}}
	a, store := reingestApp(t, s, 250*time.Millisecond)
	if _, err := store.Write(crashItems(3, "long")); err != nil {
		t.Fatal(err)
	}
	if code := a.reingest(context.Background()); code != ExitOK {
		t.Fatalf("reingest = %d, want %d", code, ExitOK)
	}
	if got := s.calls.Load(); got != 4 {
		t.Fatalf("%d writes, want 4 (3 chunks and one retry)", got)
	}
	dead := &scriptSink{script: func(int) error {
		return &sink.Error{Class: sink.ClassNetwork, Op: "write", Err: errors.New("down")}
	}}
	a, store = reingestApp(t, dead, 100*time.Millisecond)
	if _, err := store.Write(crashItems(2, "dead")); err != nil {
		t.Fatal(err)
	}
	if code := a.reingest(context.Background()); code != ExitSink {
		t.Fatalf("reingest against a dead sink = %d, want %d", code, ExitSink)
	}
}

// SPEC §12.2 (§22 pendência 25): syslog_emergency_events_pending says what is on
// disk. During the reingestion it used to read the size of the file being
// delivered -- neither what is on disk nor what is left -- and it is only
// known again at the recount of the end; syslog_emergency_files, exact and
// free, is set before the first write (ported from go-relplog-nats-server).
func TestReingestionDoesNotShowTheFileAsPending(t *testing.T) {
	var a *App
	var pending, files atomic.Int64
	pending.Store(-1)
	s := &scriptSink{script: func(n int) error {
		if n == 1 {
			pending.Store(a.metrics.EmergencyEventsPending.Load())
			files.Store(a.metrics.EmergencyFiles.Load())
		}
		return nil
	}}
	a, store := reingestApp(t, s, time.Second)
	for _, p := range []string{"g", "h"} {
		if _, err := store.Write(crashItems(2, p)); err != nil {
			t.Fatal(err)
		}
	}
	if code := a.reingest(context.Background()); code != ExitOK {
		t.Fatal(code)
	}
	if v := pending.Load(); v != 0 {
		t.Errorf("pending read %d during the reingestion, want 0 (unknown until the recount)", v)
	}
	if v := files.Load(); v != 2 {
		t.Errorf("files read %d during the reingestion, want the 2 dumps", v)
	}
	if fl, ev := a.metrics.EmergencyFiles.Load(), a.metrics.EmergencyEventsPending.Load(); fl != 0 || ev != 0 {
		t.Fatalf("after a clean reingestion: files %d, pending %d", fl, ev)
	}
}
