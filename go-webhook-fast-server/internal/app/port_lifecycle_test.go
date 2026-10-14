package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/sink"
)

// The tests of this file come from the port of the syslog/RELP family's
// corrections (2026-10-13): the loss ledger of an abort in its final form
// (lossMu, enterDumpPhase, oweDump, settleDump, emergency.Store.SetAbandon,
// nothing counted after the owner), the signal rules of the shutdown, and a
// bind cancelled by a signal. The unit tests are go-syslog-server's
// review_abort_test.go, adapted to the envelope items and to this server's
// reasons (a disabled dump is the abort's loss, SPEC §9.3).

// abortApp is the part of an App that abortLoss and dumpEmergency read: a
// memory ring holding five events, two of them in a batch a worker holds.
func abortApp(t *testing.T) (*App, []*envelope.Item) {
	t.Helper()
	r := ring.NewMemory(100, 1<<20, time.Millisecond)
	for i := range 5 {
		id, env := testJLH(t, testID, fmt.Sprintf("/abort/%d", i), `{}`)
		if err := r.Enqueue(context.Background(), envelope.NewItem(id, env, -1)); err != nil {
			t.Fatal(err)
		}
	}
	held, _ := r.Dequeue(context.Background(), 2)
	loaded, err := config.Load(config.Options{Args: []string{"--ring.mode=memory", "--sink.type=none"}})
	if err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: loaded.Config, ring: r, log: obs.NewLogger(io.Discard, "text", "error"),
		metrics: obs.NewMetrics("test", "test", "go", config.RingMemory, config.SinkNoneType)}
	return a, held
}

// withStore gives a an emergency store in dir that gives up once a counted
// the abort, as build wires it.
func withStore(a *App, dir string) {
	a.emg = emergency.NewStore(dir, testID.From)
	a.emg.SetAbandon(a.aborted.Load)
	a.dumps = true
}

func abortedCount(a *App) uint64 { return a.metrics.EventsDroppedTotal[obs.DropAbort].Load() }

func failedDumps(a *App) uint64 {
	return a.metrics.EventsDroppedTotal[obs.DropEmergencyFailed].Load()
}

// An abort between the drain and the dump counts every leftover: the counter
// is set from the ring's count BEFORE the drain (enterDumpPhase), when held
// and queued items are still one population there. With atRisk the drained
// items were out of the ring's count and not yet in atRisk: 2 counted for 5.
func TestAbortBetweenTheDrainAndTheDumpCountsEverything(t *testing.T) {
	a, held := abortApp(t)
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet, the dump phase must start")
	}
	drained := a.ring.Drain()
	a.abortLoss()
	if got := abortedCount(a); got != uint64(len(held)+len(drained)) {
		t.Fatalf("abort counted %d of %d lost", got, len(held)+len(drained))
	}
}

// After a dump that SUCCEEDED nothing is lost, although the held batch is still
// in the ring's count: an abort then counts nothing.
func TestAbortAfterASavedDumpCountsNothing(t *testing.T) {
	a, _ := abortApp(t)
	a.dumpPhase.Store(true)
	a.dumping.Store(0) // dumpEmergency took it back after a successful Write
	a.abortLoss()
	if got := abortedCount(a); got != 0 {
		t.Fatalf("abort after a saved dump counted %d saved events as lost", got)
	}
}

// With emergency.enabled=false, an abort and the shutdown's own count of the
// loss never count the same events twice: the counter is taken by exactly one
// of them.
func TestAbortAndDisabledDumpCountOnce(t *testing.T) {
	a, held := abortApp(t)
	items := append(held, a.ring.Drain()...)
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.abortLoss() // the abort wins the race
	a.dumpEmergency(items)
	if lost := abortedCount(a) + failedDumps(a); lost != uint64(len(items)) {
		t.Fatalf("%d events counted lost for %d (abort %d, emergency_failed %d)", lost, len(items),
			abortedCount(a), failedDumps(a))
	}
}

// After a dump that FAILED the loss is emergency_failed's, counted once, and an
// abort racing it counts nothing more.
func TestAbortAndFailedDumpCountOnce(t *testing.T) {
	a, held := abortApp(t)
	items := append(held, a.ring.Drain()...)
	dir := t.TempDir()
	withStore(a, dir)
	// A file where the directory was fails the write.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.dumpEmergency(items)
	a.abortLoss() // an abort right after the failure
	if failed := failedDumps(a); failed != uint64(len(items)) || abortedCount(a) != 0 {
		t.Fatalf("emergency_failed %d, abort %d; want %d and 0", failed, abortedCount(a), len(items))
	}
}

// An abort that lands while the shutdown collects the leftovers (Pending,
// Drain) took the counter; putting it back afterwards let a disabled dump count
// the same events again: 10 counted lost for 5.
func TestAbortDuringTheDrainIsCountedOnce(t *testing.T) {
	a, held := abortApp(t)
	total := a.ring.Stats().Items
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet, the dump phase must start")
	}
	items := append(held, a.ring.Drain()...)
	a.abortLoss() // the second signal, during Pending/Drain
	if a.oweDump(len(items)) {
		a.dumpEmergency(items) // dumps == false: a disabled dump
	}
	if lost := abortedCount(a) + failedDumps(a); lost != uint64(total) {
		t.Fatalf("%d counted lost for %d (abort %d, emergency_failed %d)", lost, total,
			abortedCount(a), failedDumps(a))
	}
}

// The abort that lands before the dump phase counts the ring, and the phase
// never starts: there is no instant between the two stores any more.
func TestAbortBeforeTheDumpPhaseKeepsItClosed(t *testing.T) {
	a, _ := abortApp(t)
	total := a.ring.Stats().Items
	a.abortLoss()
	if a.enterDumpPhase() || a.oweDump(3) {
		t.Fatal("the dump phase started after the abort")
	}
	a.abortLoss() // a third signal counts nothing more
	if got := abortedCount(a); got != uint64(total) {
		t.Fatalf("abort counted %d for %d", got, total)
	}
}

// A dump counted lost by an abort must not reach the disk and come back at the
// next boot: the store gives up and leaves nothing behind (TESTING §8 item 40:
// the dump could still be linked to its final name before the os.Exit).
func TestDumpAfterTheAbortIsAbandoned(t *testing.T) {
	a, held := abortApp(t)
	dir := t.TempDir()
	withStore(a, dir)
	items := append(held, a.ring.Drain()...)
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.abortLoss()
	a.dumpEmergency(items)
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("the abandoned dump left %d entries: %v", len(ents), ents[0].Name())
	}
	if got := abortedCount(a) + failedDumps(a); got != uint64(len(items)) {
		t.Fatalf("%d counted lost for %d", got, len(items))
	}
}

// An abort that lands after the dump's last abandon check -- the dump then
// publishes -- counted every event: the dump counts nothing more, and the file
// is reingested at the next boot (an overcount, never a loss).
func TestAbortAfterTheLastAbandonCheckCountsNothingMore(t *testing.T) {
	a, held := abortApp(t)
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet")
	}
	items := append(held, a.ring.Drain()...)
	a.oweDump(len(items))
	dir := t.TempDir()
	withStore(a, dir)
	var checks atomic.Int32
	a.emg.SetAbandon(func() bool {
		if checks.Add(1) == 2 { // the check before the publish
			a.abortLoss() // the abort lands right after it
		}
		return false
	})
	a.dumpEmergency(items)
	if abortedCount(a) != uint64(len(items)) || failedDumps(a) != 0 {
		t.Fatalf("abort %d, emergency_failed %d; want %d, 0", abortedCount(a), failedDumps(a), len(items))
	}
	if d, _ := emergency.NewStore(dir, testID.From).List(); len(d) != 1 {
		t.Fatalf("dumps on disk = %v, want the one that completed", d)
	}
	if v := a.metrics.EmergencyFiles.Load(); v != 0 {
		t.Fatalf("whs_emergency_files = %d after a dump the abort owned, want 0", v)
	}
}

// abortLoss publishes aborted before it takes the counter. A dump that reads
// the flag in between gives up (ErrAbandoned) and used to take the whole count
// as emergency_failed, with the abort's line then saying nothing was lost: one
// count, the wrong reason (X2 of the independent review of go-syslog-server's
// port, 2026-10-13). The dump runs exactly there, with lossMu held; it may not
// take the counter before abortLoss does. Both branches: a store that gives
// up, and no dump at all (emergency.enabled=false) -- there the reason is the
// abort's either way, and what the test holds is that the dump waited.
func TestDumpThatSawTheAbortLeavesTheCountToIt(t *testing.T) {
	for _, withDump := range []bool{true, false} {
		name := map[bool]string{true: "abandoned dump", false: "disabled dump"}[withDump]
		t.Run(name, func(t *testing.T) {
			a, held := abortApp(t)
			items := append(held, a.ring.Drain()...)
			dir := t.TempDir()
			if withDump {
				withStore(a, dir)
			}
			a.dumpPhase.Store(true)
			a.dumping.Store(int64(len(items)))
			done := make(chan struct{})
			a.inAbort = func() {
				go func() { defer close(done); a.dumpEmergency(items) }()
				select {
				case <-done:
					t.Error("the dump took the counter while the abort held it")
				case <-time.After(300 * time.Millisecond):
				}
			}
			a.abortLoss()
			<-done
			if abortedCount(a) != uint64(len(items)) || failedDumps(a) != 0 {
				t.Fatalf("abort %d, emergency_failed %d; want %d, 0", abortedCount(a), failedDumps(a), len(items))
			}
			if ents, _ := os.ReadDir(dir); len(ents) != 0 {
				t.Fatalf("the abandoned dump left %d entries", len(ents))
			}
		})
	}
}

// SPEC §10.3: in a FATAL shutdown the first SIGTERM/SIGINT is tolerated
// (logged), the second aborts, and SIGQUIT always aborts -- the first one
// included.
func TestFatalShutdownToleratesTheFirstSignal(t *testing.T) {
	run := func(fatal bool, sigs ...os.Signal) int {
		a, _ := abortApp(t)
		abort := make(chan os.Signal, len(sigs))
		block := make(chan struct{})
		defer close(block)
		done := make(chan int, 1)
		go func() {
			done <- a.shutdownWith(func(context.Context) int { <-block; return ExitSink }, fatal, abort)
		}()
		for _, s := range sigs {
			abort <- s
		}
		select {
		case code := <-done:
			return code
		case <-time.After(300 * time.Millisecond):
			return -1
		}
	}
	if code := run(true, syscall.SIGTERM); code != -1 {
		t.Errorf("FATAL shutdown, one SIGTERM: exit %d, want it to go on", code)
	}
	if code := run(true, syscall.SIGTERM, os.Interrupt); code != ExitAborted {
		t.Errorf("FATAL shutdown, two signals: exit %d, want %d", code, ExitAborted)
	}
	if code := run(true, syscall.SIGQUIT); code != ExitAborted {
		t.Errorf("FATAL shutdown, SIGQUIT: exit %d, want %d", code, ExitAborted)
	}
	if code := run(false, os.Interrupt); code != ExitAborted {
		t.Errorf("signalled shutdown, second signal: exit %d, want %d", code, ExitAborted)
	}
}

// ---------------------------------------------------------------- harness

// startHooked is startAs with a hook of the test's own on the assembled App,
// run after the usual ones (abort channel, log).
func startHooked(t *testing.T, dir string, id ident.Identity, hook func(*App), extra ...string) *instance {
	t.Helper()
	in := &instance{t: t, id: id, dir: dir, pub: freePort(t), adm: freePort(t), done: make(chan int, 1)}
	in.args = append([]string{
		"--server.bind=" + in.pub, "--admin.bind=" + in.adm, "--ring.data_dir=" + dir,
		"--log.level=error", "--log.format=text",
		"--sink.health_interval=100ms", "--sink.health_timeout=100ms", "--sink.boot_timeout=5s",
	}, extra...)
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	in.abort = make(chan os.Signal, 2)
	hooked := make(chan struct{})
	go func() {
		in.done <- runContext(ctx, in.args, id, func(a *App) {
			a.abort = in.abort
			if startLog != nil {
				a.log = obs.NewLogger(startLog, "text", "info")
			}
			in.app = a
			if hook != nil {
				hook(a)
			}
			close(hooked)
		})
	}()
	if !in.waitReady(10 * time.Second) {
		in.stop()
		t.Fatalf("instance never became ready; args=%v", in.args)
	}
	<-hooked
	t.Cleanup(func() { in.stop() })
	return in
}

// heldSink is a destination that takes nothing while held: every write fails
// as the network does, while its health check -- the real sink's -- passes.
type heldSink struct {
	sink.Sink
	held atomic.Bool
}

var errHeld = &sink.Error{Class: sink.ClassNetwork, Op: "held", Err: errors.New("connection refused")}

func (s *heldSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.held.Load() {
		time.Sleep(5 * time.Millisecond)
		return errHeld
	}
	return s.Sink.Write(ctx, items)
}

// ---------------------------------------------------------------- abort, end to end

// A4 of the plan: with ring=sqlite the abort loses nothing -- the rows stay in
// ring.db for the next boot -- so a second signal in phase 4, with the sink
// taking nothing, counts no abort, and the next boot delivers every row.
// abortLoss's ring=sqlite branch is what keeps the count at zero.
func TestAbortWithTheSQLiteRingLosesNothing(t *testing.T) {
	dir := t.TempDir()
	gate := &heldSink{}
	gate.held.Store(true)
	in := startHooked(t, dir, testID, func(a *App) {
		a.wrapSink = func(s sink.Sink) sink.Sink { gate.Sink = s; return gate }
	}, "--ring.mode=sqlite", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF", "--sink.batch=4",
		"--shutdown.drain_timeout=10m", "--shutdown.pre_stop_delay=0s")
	ids := in.postN(9)
	if len(ids) != 9 {
		t.Fatal("not every request was accepted")
	}
	done := make(chan int, 1)
	go func() { done <- in.stop() }()
	time.Sleep(300 * time.Millisecond) // into phase 4: the ring cannot drain
	in.interrupt()
	if code := <-done; code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if got := abortedCount(in.app); got != 0 {
		t.Fatalf("dropped{abort} = %d with ring=sqlite, want 0: the rows stay in ring.db", got)
	}
	ringDB := filepath.Join(dir, "ring.db")
	if n := countRows(t, ringDB, "jlh_ring"); n != 9 {
		t.Fatalf("%d rows left in ring.db, want 9", n)
	}

	next := start(t, dir, "--ring.mode=sqlite", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF")
	if code := next.stop(); code != ExitOK {
		t.Fatalf("next boot: exit %d", code)
	}
	got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Fatalf("%d of %d events left in ring.db never reached the sink", len(lost), len(ids))
	}
}

// TESTING §8 item 36, closed: an abort that lands while the dump is being
// written counts the dump's events once, as abort, and the dump gives up
// instead of being published for the next boot to reingest (counted lost AND
// delivered). beforeDump holds the dump until the abort has counted.
func TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump(t *testing.T) {
	dir := t.TempDir()
	gate := &heldSink{}
	gate.held.Store(true)
	var in *instance
	in = startHooked(t, dir, testID, func(a *App) {
		a.wrapSink = func(s sink.Sink) sink.Sink { gate.Sink = s; return gate }
		a.beforeDump = func() {
			in.interrupt()
			for !a.aborted.Load() {
				time.Sleep(time.Millisecond)
			}
		}
	}, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF", "--sink.batch=4",
		"--shutdown.drain_timeout=500ms", "--shutdown.pre_stop_delay=0s")
	if len(in.postN(7)) != 7 {
		t.Fatal("not every request was accepted")
	}
	if code := in.stop(); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	// The graceful path goes on in the background until the store gives up.
	time.Sleep(200 * time.Millisecond)
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 7 || f != 0 {
		t.Fatalf("dropped{abort}=%d dropped{emergency_failed}=%d, want 7 and 0", a, f)
	}
	ents, _ := os.ReadDir(filepath.Join(dir, "emergency"))
	if len(ents) != 0 {
		t.Fatalf("the dump the abort counted left %d entries: %s", len(ents), ents[0].Name())
	}
}

// ---------------------------------------------------------------- bind

// cancelAfterWrite cancels the boot's context once a write of the reingestion
// returns: the signal lands between the reingestion and the bind.
type cancelAfterWrite struct {
	sink.Sink
	cancel context.CancelFunc
}

func (c cancelAfterWrite) Write(ctx context.Context, items []*envelope.Item) error {
	err := c.Sink.Write(ctx, items)
	c.cancel()
	return err
}

// SPEC §10.5: a signal that lands after the reingestion and before the public
// bind is an interrupted boot, 130. With a host name the lookup fails with
// "operation was canceled", and the boot exited 1 as for a bad address; a
// literal IP never looks at the context, and the boot opened, answered /readyz
// 200 and left with 0 through the graceful path (X1 of the plan; port of the
// syslog/RELP family, 2026-10-13).
func TestSignalBeforeTheBindExits130(t *testing.T) {
	for _, bind := range []string{"127.0.0.1:0", "localhost:0"} {
		t.Run(bind, func(t *testing.T) {
			dir := t.TempDir()
			plantDump(t, dir, 3)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var a *App
			code := runContext(ctx, []string{
				"--server.bind=" + bind, "--admin.enabled=false", "--ring.data_dir=" + dir,
				"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
				"--log.level=error", "--log.format=text",
			}, testID, func(app *App) {
				a = app
				app.abort = make(chan os.Signal, 1)
				app.wrapSink = func(s sink.Sink) sink.Sink { return cancelAfterWrite{Sink: s, cancel: cancel} }
			})
			if code != ExitAborted {
				t.Fatalf("a signal before the bind exited %d, want %d", code, ExitAborted)
			}
			if n := countRows(t, filepath.Join(dir, "events.db"), "inbox"); n != 3 {
				t.Fatalf("%d events reingested, want the dump's 3 (the signal must land after it)", n)
			}
			// The bound listener was closed: its port is free again.
			if addr := a.srv.Addr(); addr != "" {
				if l, err := net.Listen("tcp", addr); err != nil {
					t.Fatalf("the interrupted boot left %s bound: %v", addr, err)
				} else {
					l.Close()
				}
			}
		})
	}
}

// The same on admin.bind, before the sink: the lookup of "localhost" fails
// with "operation was canceled", which is a signal, not a bad address.
func TestCancelledAdminBindIsAnAbortNotAConfigError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := []string{"--admin.bind=localhost:0", "--server.bind=127.0.0.1:0",
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.type=none",
		"--log.level=error"}
	if code := RunContext(ctx, args, testID); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
}

// ---------------------------------------------------------------- reingestion logs

// SPEC §9.2: a reingested dump whose unlink worked and whose directory fsync
// did not IS removed; the boot says so at WARN, with the risk of a second
// delivery, instead of the "cannot remove" ERROR, which was false.
func TestReingestedDumpRemovedWithoutDurabilityIsAWarning(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens any directory")
	}
	dir := t.TempDir()
	dump := plantDump(t, dir, 2)
	emgDir := filepath.Dir(dump)
	// Write and search, no read: the reingestion opens the dump by name, the
	// unlink works, and the directory cannot be opened for its fsync. The
	// listing needs read permission, so it is taken away once the dump is
	// read: wrapSink's first Write is after the Read.
	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	in := startHooked(t, dir, testID, func(a *App) {
		a.wrapSink = func(s sink.Sink) sink.Sink {
			return chmodOnWrite{Sink: s, dir: emgDir, mode: 0o300}
		}
	}, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1")
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Lstat(dump); !os.IsNotExist(err) {
		t.Fatalf("the dump is still there: %v", err)
	}
	out := log.String()
	if !strings.Contains(out, "removed but the directory fsync failed") || strings.Contains(out, "cannot remove") {
		t.Fatalf("the boot did not report the removal as removed:\n%s", grepLines(out, "dump"))
	}
}

// chmodOnWrite sets dir's mode on the first write: the reingestion's.
type chmodOnWrite struct {
	sink.Sink
	dir  string
	mode os.FileMode
}

func (c chmodOnWrite) Write(ctx context.Context, items []*envelope.Item) error {
	_ = os.Chmod(c.dir, c.mode)
	return c.Sink.Write(ctx, items)
}

// The same for a truncated dump: renamed to .corrupt with only the directory
// fsync failing, it IS quarantined -- WARN for the fsync, and the line with
// the discarded bytes under the new name, not "cannot quarantine".
func TestQuarantineWithoutDurabilityIsStillReported(t *testing.T) {
	var log syncBuffer
	a := &App{log: obs.NewLogger(&log, "text", "info")}
	notDurable := fmt.Errorf("%w: fsync of directory d: input/output error", emergency.ErrNotDurable)
	a.reportQuarantine("d/emg-x.bin", "d/emg-x.bin.corrupt", notDurable,
		&emergency.ReadResult{Truncated: true, TruncatedBytes: 7})
	out := log.String()
	if strings.Contains(out, "cannot quarantine") || !strings.Contains(out, "directory fsync failed") ||
		!strings.Contains(out, "unreadable tail was quarantined") || !strings.Contains(out, "emg-x.bin.corrupt") {
		t.Fatalf("a quarantine whose directory fsync failed was not reported as one:\n%s", out)
	}
}

// fatalSink is a destination that takes nothing while held, as heldSink, and
// answers "never" (ClassFatal) to every write once made fatal.
type fatalSink struct {
	heldSink
	fatal atomic.Bool
}

var errNever = &sink.Error{Class: sink.ClassFatal, Op: "fatal", Err: errors.New("permission denied")}

func (s *fatalSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.fatal.Load() {
		return errNever
	}
	return s.heldSink.Write(ctx, items)
}

// SPEC §10.2, §10.3, §10.5 (a §19.1 pendência, closed 2026-10-13): a sink that goes
// FATAL during the drain of a SIGTERM ends the wait at once -- nothing will
// drain any more -- and the exit is 3 whatever the ring. The wait used to run
// to the end of drain_timeout (here ten minutes), and with ring=sqlite the
// process left with 0 with the destination FATAL.
func TestFatalDuringTheDrainEndsTheWaitAndExits3(t *testing.T) {
	for _, mode := range []string{config.RingMemory, config.RingSQLiteMode} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			gate := &fatalSink{}
			gate.held.Store(true)
			in := startHooked(t, dir, testID, func(a *App) {
				a.wrapSink = func(s sink.Sink) sink.Sink { gate.Sink = s; return gate }
			}, "--ring.mode="+mode, "--sink.type=sqlite", "--sink.workers=1",
				"--sink.sqlite.synchronous=OFF", "--sink.batch=4",
				"--shutdown.drain_timeout=10m", "--shutdown.pre_stop_delay=0s")
			ids := in.postN(6)
			if len(ids) != 6 {
				t.Fatal("not every request was accepted")
			}
			done := make(chan int, 1)
			go func() { done <- in.stop() }()
			time.Sleep(300 * time.Millisecond) // into phase 4
			gate.fatal.Store(true)
			select {
			case code := <-done:
				if code != ExitSink {
					t.Fatalf("exit %d, want %d: the sink ended FATAL", code, ExitSink)
				}
			case <-time.After(20 * time.Second):
				in.interrupt()
				t.Fatal("the drain waited for drain_timeout with the sink FATAL")
			}
			if mode == config.RingMemory {
				checkDump(t, dir, testID, envelope.ModeFast, ids)
			} else if n := countRows(t, filepath.Join(dir, "ring.db"), "jlh_ring"); n != 6 {
				t.Fatalf("%d rows in ring.db, want 6", n)
			}
		})
	}
}
