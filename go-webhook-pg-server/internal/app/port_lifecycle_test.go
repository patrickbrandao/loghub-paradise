package app

import (
	"context"
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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
)

// The tests of this file come from the port of the syslog/RELP family's
// corrections (2026-10-13), the same as go-webhook-fast-server's
// port_lifecycle_test.go: the loss ledger of an abort in its final form
// (lossMu, enterDumpPhase, oweDump, settleDump, emergency.Store.SetAbandon,
// nothing counted after the owner), the signal rules of the shutdown, a bind
// cancelled by a signal, a sink FATAL during the drain, and the reader of the
// dumps that runs at every boot. The unit tests are go-syslog-server's
// review_abort_test.go, adapted to the envelope items and to this server's
// reasons (a disabled dump is the abort's loss, SPEC §9.3). There is no
// ring=sqlite here: the A4 test of the fast has no counterpart.

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
	loaded, err := config.Load(config.Options{Args: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: loaded.Config, ring: r, log: obs.NewLogger(io.Discard, "text", "error"),
		metrics: obs.NewMetrics("test", "test", "go", "memory", "test")}
	return a, held
}

// withStore gives a an emergency store in dir that gives up once a counted
// the abort, as build wires it.
func withStore(a *App, dir string) {
	a.emg = emergency.NewStore(dir, testID.From, 0)
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
// items were out of the ring's count and not yet in atRisk: 2 counted for 5
// (TESTING §8 item 16).
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
// next boot: the store gives up and leaves nothing behind (TESTING §8 item 16:
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
	if d, _ := emergency.NewStore(dir, testID.From, 0).List(); len(d) != 1 {
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

// startHooked is start with a hook of the test's own on the assembled App,
// run after the usual ones (abort channel, the fake destination, log).
func startHooked(t *testing.T, dir string, ms *memSink, hook func(*App), extra ...string) *instance {
	t.Helper()
	in := &instance{t: t, id: testID, dir: dir, pub: freePort(t), adm: freePort(t),
		done: make(chan int, 1), mem: ms}
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
		in.done <- runContext(ctx, in.args, testID, func(a *App) {
			a.abort = in.abort
			a.sinkOverride = ms
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

// ---------------------------------------------------------------- abort, end to end

// TESTING §8 item 16, closed: an abort that lands while the dump is being
// written counts the dump's events once, as abort, and the dump gives up
// instead of being published for the next boot to reingest (counted lost AND
// delivered). beforeDump holds the dump until the abort has counted.
func TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump(t *testing.T) {
	dir := t.TempDir()
	ms := &memSink{}
	var in *instance
	in = startHooked(t, dir, ms, func(a *App) {
		a.beforeDump = func() {
			in.interrupt()
			for !a.aborted.Load() {
				time.Sleep(time.Millisecond)
			}
		}
	}, "--sink.workers=1", "--sink.batch=4", "--sink.op_timeout=50ms",
		"--shutdown.drain_timeout=500ms", "--shutdown.pre_stop_delay=0s")
	ms.hang.Store(true) // takes nothing, with the health check UP
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

// SPEC §10.2, §10.3, §10.5 (§19.1 item 10, closed 2026-10-13): a sink that goes
// FATAL during the drain of a SIGTERM ends the wait at once -- nothing will
// drain any more -- and the dump is written then, not at the end of
// drain_timeout (here ten minutes, past any stop_grace_period).
func TestFatalDuringTheDrainEndsTheWaitAndExits3(t *testing.T) {
	dir := t.TempDir()
	ms := &memSink{}
	in := startHooked(t, dir, ms, nil, "--sink.workers=1", "--sink.batch=4", "--sink.op_timeout=50ms",
		"--shutdown.drain_timeout=10m", "--shutdown.pre_stop_delay=0s")
	ms.hang.Store(true)
	ids := in.postN(6)
	if len(ids) != 6 {
		t.Fatal("not every request was accepted")
	}
	done := make(chan int, 1)
	go func() { done <- in.stop() }()
	time.Sleep(300 * time.Millisecond) // into phase 4
	ms.fatal.Store(true)
	select {
	case code := <-done:
		if code != ExitSink {
			t.Fatalf("exit %d, want %d: the sink ended FATAL", code, ExitSink)
		}
	case <-time.After(20 * time.Second):
		in.interrupt()
		t.Fatal("the drain waited for drain_timeout with the sink FATAL")
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)
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
			ms := &memSink{}
			var a *App
			code := runContext(ctx, []string{
				"--server.bind=" + bind, "--admin.enabled=false", "--ring.data_dir=" + dir,
				"--log.level=error", "--log.format=text",
			}, testID, func(app *App) {
				a = app
				app.abort = make(chan os.Signal, 1)
				app.sinkOverride = cancelAfterWrite{Sink: ms, cancel: cancel}
			})
			if code != ExitAborted {
				t.Fatalf("a signal before the bind exited %d, want %d", code, ExitAborted)
			}
			if n := ms.count(); n != 3 {
				t.Fatalf("%d events reingested, want the dump's 3 (the signal must land after it)", n)
			}
			// The bound listener was closed: its port is free again.
			if addr := a.srv.Addr(); addr != "" {
				l, err := net.Listen("tcp", addr)
				if err != nil {
					t.Fatalf("the interrupted boot left %s bound: %v", addr, err)
				}
				l.Close()
			}
		})
	}
}

// The same on admin.bind, before the sink: the lookup of "localhost" fails
// with "operation was canceled", which is a signal, not a bad address.
func TestCancelledAdminBindIsAnAbortNotAConfigError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := []string{"--admin.bind=localhost:9101", "--server.bind=127.0.0.1:0",
		"--ring.data_dir=" + t.TempDir(), "--log.level=error"}
	code := runContext(ctx, args, testID, func(a *App) { a.sinkOverride = &memSink{} })
	if code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
}

// ---------------------------------------------------------------- reingestion

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
	// Write and search, no read, from the reingestion's first write on: the
	// dump was already read, the unlink works, and the directory cannot be
	// opened for its fsync.
	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	ms := &memSink{}
	in := startHooked(t, dir, ms, func(a *App) {
		a.sinkOverride = chmodOnWrite{Sink: ms, dir: emgDir, mode: 0o300}
	})
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Lstat(dump); !os.IsNotExist(err) {
		t.Fatalf("the dump is still there: %v", err)
	}
	out := log.String()
	if !strings.Contains(out, "removed but the directory fsync failed") || strings.Contains(out, "cannot remove") {
		t.Fatalf("the boot did not report the removal as removed:\n%s", out)
	}
}

// chmodOnWrite sets dir's mode on every write: the first is the reingestion's.
type chmodOnWrite struct {
	sink.Sink
	dir  string
	mode os.FileMode
}

func (c chmodOnWrite) Write(ctx context.Context, items []*envelope.Item) error {
	_ = os.Chmod(c.dir, c.mode)
	return c.Sink.Write(ctx, items)
}

// The same for a damaged dump: renamed to .corrupt with only the directory
// fsync failing, it IS quarantined -- WARN for the fsync, and the ERROR with
// the skipped lines under the new name, not "cannot quarantine".
func TestQuarantineWithoutDurabilityIsStillReported(t *testing.T) {
	var log syncBuffer
	a := &App{log: obs.NewLogger(&log, "text", "info")}
	notDurable := fmt.Errorf("%w: fsync of directory d: input/output error", emergency.ErrNotDurable)
	a.reportQuarantine("d/emg-x.jsonl", "d/emg-x.jsonl.corrupt", notDurable,
		&emergency.ReadResult{DamagedLines: 1, DamagedBytes: 7}, 0)
	out := log.String()
	if strings.Contains(out, "cannot quarantine") || !strings.Contains(out, "directory fsync failed") ||
		!strings.Contains(out, "file was quarantined") || !strings.Contains(out, "emg-x.jsonl.corrupt") {
		t.Fatalf("a quarantine whose directory fsync failed was not reported as one:\n%s", out)
	}
}

// SPEC §9.2 (§19.1 item 8, closed 2026-10-13): the dumps of a previous life
// are reingested at every boot, emergency.enabled or not -- only the writing
// is switched off. The store used to exist only with the key on, and a life
// with it off left the dump on disk with no log, whs_emergency_files at 0 and
// /readyz 200.
func TestDumpsAreReingestedWithEmergencyDisabled(t *testing.T) {
	dir := t.TempDir()
	dump := plantDump(t, dir, 3)
	ms := &memSink{}
	in := startHooked(t, dir, ms, nil, "--emergency.enabled=false")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if n := ms.count(); n != 3 {
		t.Fatalf("%d events reingested, want the dump's 3", n)
	}
	if _, err := os.Stat(dump); !os.IsNotExist(err) {
		t.Fatalf("the reingested dump is still there: %v", err)
	}
}

// A directory that cannot be listed stops only a life that writes dumps; with
// emergency.enabled=false it is an ERROR and the boot goes on (SPEC §9.2, the
// rule of go-webhook-fast-server).
func TestUnlistableEmergencyDirStopsOnlyALifeThatDumps(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root lists any directory")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })
	in := startHooked(t, dir, &memSink{}, nil, "--emergency.enabled=false")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("emergency.enabled=false: exit %d, want 0", code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code := runContext(ctx, []string{"--server.bind=127.0.0.1:0", "--admin.enabled=false",
		"--ring.data_dir=" + dir, "--log.level=error"}, testID, func(a *App) { a.sinkOverride = &memSink{} })
	if code != ExitConfig {
		t.Fatalf("a life that dumps: exit %d, want %d", code, ExitConfig)
	}
}

// SPEC §11.1: whs_emergency_files counts every listed dump during the
// reingestion and after it, an unreadable one included (§19.1 item 12).
func TestEmergencyGaugesCountAnUnreadableDump(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads any file")
	}
	dir := t.TempDir()
	locked := plantDump(t, dir, 2)
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o640) })
	in := startHooked(t, dir, &memSink{}, nil)
	if got := in.app.metrics.EmergencyFiles.Load(); got != 1 {
		t.Fatalf("whs_emergency_files = %d with an unreadable dump on disk, want 1", got)
	}
}

// SPEC §11.1: while the boot reingests, whs_emergency_files is the number of
// dumps it listed -- every one of them awaits reingestion -- so an alert on it
// stays up through a long reingestion, read on the admin port that is open the
// whole boot. It stayed at 0 until the end (the rule of go-webhook-fast-server,
// ported 2026-10-13).
func TestEmergencyFilesGaugeDuringReingestion(t *testing.T) {
	dir := t.TempDir()
	plantDump(t, dir, 2)
	plantDump(t, dir, 1)
	ms := &memSink{}
	ms.hang.Store(true)
	in := startExpectingExitWith(t, dir, ms, "--sink.op_timeout=10s", "--sink.boot_timeout=30s")
	if !waitFor(20*time.Second, func() bool {
		_, body := in.readyz()
		return strings.Contains(body, "reingesting")
	}) {
		t.Fatal("the boot never reported the reingestion")
	}
	if m := in.metrics(); !strings.Contains(m, "\nwhs_emergency_files 2\n") {
		t.Errorf("whs_emergency_files during the reingestion of 2 dumps is not 2:\n%s", m)
	}
	in.cancel()
	select {
	case <-in.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the interrupted reingestion did not end")
	}
}

// The exit is 3 whenever the sink ends FATAL, an empty ring included: here it
// goes FATAL during the pre-stop delay, with nothing queued, and the shutdown
// left with 0 (SPEC §10.2, §10.5).
func TestFatalDuringThePreStopDelayExits3(t *testing.T) {
	ms := &memSink{}
	in := startHooked(t, t.TempDir(), ms, nil, "--shutdown.pre_stop_delay=1s")
	done := make(chan int, 1)
	go func() { done <- in.stop() }()
	time.Sleep(100 * time.Millisecond) // inside the pre-stop delay
	ms.fatal.Store(true)
	if code := <-done; code != ExitSink {
		t.Fatalf("exit %d, want %d: the sink ended FATAL", code, ExitSink)
	}
}

// SPEC §9.1, §9.3: an envelope with a raw newline is left out of the dump and
// lost, counted once as emergency_failed, and the rest is saved. With the
// counter now taken by dumpEmergency, the left-out ones are counted there too.
func TestNewlineEnvelopeLeftOutOfTheDumpIsCountedOnce(t *testing.T) {
	a, held := abortApp(t)
	items := append(held, a.ring.Drain()...)
	items = append(items, envelope.NewItem("bad", []byte("{\"x\":\n1}"), -1))
	dir := t.TempDir()
	withStore(a, dir)
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.dumpEmergency(items)
	if failedDumps(a) != 1 || abortedCount(a) != 0 {
		t.Fatalf("emergency_failed %d, abort %d; want 1 and 0", failedDumps(a), abortedCount(a))
	}
	if files, events, _ := emergency.NewStore(dir, testID.From, 0).Count(); files != 1 || events != int64(len(items)-1) {
		t.Fatalf("the dump holds %d files, %d events; want 1 and %d", files, events, len(items)-1)
	}
	a.abortLoss() // an abort after the dump counts nothing
	if abortedCount(a) != 0 {
		t.Fatalf("abort after the dump counted %d", abortedCount(a))
	}
}
