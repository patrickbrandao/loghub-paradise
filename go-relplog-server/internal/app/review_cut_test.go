package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// Regression tests from the independent review of go-relplog-pg-server on
// 2026-10-13, ported here because the shutdown, the dump and the boot are
// common to the two servers (TESTING §9). The shutdown and the boot run for
// real, against the NATS server, where review_abort_test.go and
// review_rn_test.go drive the loss ledger by hand.

// fatalShutdownWithOneWorker boots with one drain worker and a destination
// that answers every write with ClassFatal, accepts n messages -- the first
// held by the worker, the rest queued in the ring -- and opens the gate, so
// the FATAL shutdown runs. hooks run on the App before it starts.
func fatalShutdownWithOneWorker(t *testing.T, n int, hooks ...func(*App)) (*instance, *fixture, string) {
	t.Helper()
	f := newFixture(t)
	dir := t.TempDir()
	fs := newFaultySink(true)
	fs.failFatal.Store(true)
	in := launch(t, f.id, f.args(dir, "--sink.workers=1"), fs.wrap, hooks...)
	in.waitReady()
	tcp := in.app.TransportAddr("tcp")
	sendRELP(t, tcp, "<13>1 2026-09-12T14:33:01Z host app - - - first")
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	lines := make([]string, n-1)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - rest-%d", i)
	}
	sendRELP(t, tcp, lines...)
	fs.open()
	return in, f, dir
}

// With ONE worker the rest of the messages wait in the ring and Drain takes
// them out of its count, so during the dump the ring says 1 of n: only the
// dump phase makes the abort count all of them. With the default four workers
// (fatalShutdownInDump, TestAbortDuringTheDumpCountsTheLeftovers) every
// message can be held by some worker, Drain returns nothing, and the test
// passes without the dump phase (mutant M07 of the review of
// go-relplog-pg-server).
func TestAbortDuringTheDumpWithItemsDrainedFromTheRing(t *testing.T) {
	const n = 9
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	t.Cleanup(rel)
	in, _, dir := fatalShutdownWithOneWorker(t, n, func(a *App) {
		a.beforeDump = func() { close(entered); <-release }
	})
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("never reached the dump")
	}
	if st := in.app.ring.Stats(); st.Items == n {
		t.Fatalf("the ring still counts %d during the dump: nothing was drained, the test proves nothing", st.Items)
	}
	defer settle(t, dir, rel)
	in.app.abort <- syscall.SIGTERM
	in.app.abort <- syscall.SIGINT
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
		t.Errorf(`abort = %d, want %d`, d, n)
	}
}

// The real gracefulShutdown with an abort landing while it collects the
// leftovers (between the dump phase and the drain) and a dump that would fail:
// the messages are counted once, as abort, and no dump is attempted after it.
// The old shutdown re-armed the counter after the collection and counted the
// failed dump again (2N); a hand-copied shutdown in the tests hid it (M06, M28).
func TestAbortDuringTheCollectionCountsOnceAndDumpsNothing(t *testing.T) {
	const n = 9
	var emgDir string
	in, _, _ := fatalShutdownWithOneWorker(t, n, func(a *App) {
		emgDir = a.cfg.Emergency.Dir
		a.beforeCollect = func() {
			// A dump into this would fail and count emergency_failed.
			if err := os.RemoveAll(emgDir); err == nil {
				_ = os.WriteFile(emgDir, nil, 0o600)
			}
			a.abortLoss() // the second signal lands here
		}
	})
	if code := in.waitExit(30 * time.Second); code != ExitAborted && code != ExitSink {
		t.Fatalf("exit %d", code)
	}
	m := in.app.metrics
	ab, failed := m.EventsDroppedTotal[obs.EventDropAbort].Load(), m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
	if ab != n || failed != 0 {
		t.Fatalf("abort %d, emergency_failed %d for %d messages; want %d and 0", ab, failed, n, n)
	}
}

// The same abort with a working emergency directory: no dump is published
// after it. A file written there would be reingested at the next boot although
// every message in it was counted lost.
func TestAbortDuringTheCollectionPublishesNoDump(t *testing.T) {
	const n = 5
	in, f, dir := fatalShutdownWithOneWorker(t, n, func(a *App) {
		a.beforeCollect = func() { a.abortLoss() }
	})
	in.waitExit(30 * time.Second)
	if d := dumpFiles(t, filepath.Join(dir, "emergency"), f.id.From); len(d) != 0 {
		t.Fatalf("dumps published after the abort counted them lost: %v", d)
	}
	if ab := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); ab != n {
		t.Fatalf("abort = %d, want %d", ab, n)
	}
}

// SIGQUIT while serving aborts at once and counts what the ring holds
// (SPEC §13.4). In-process runs have no SIGQUIT channel unless a test gives
// one; without it the abortLoss of that branch was never run by a test (M16).
func TestSIGQUITWhileServingCountsTheRing(t *testing.T) {
	const n = 4
	f := newFixture(t)
	quit := make(chan os.Signal, 1)
	fs := newFaultySink(true) // writes wait: the messages stay in the ring
	in := launch(t, f.id, f.args(t.TempDir()), fs.wrap, func(a *App) { a.quit = quit })
	in.waitReady()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - quit-%d", i)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
	quit <- syscall.SIGQUIT
	if code := in.waitExit(10 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	fs.open()
	if ab := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); ab != n {
		t.Fatalf("abort = %d, want the %d in the ring", ab, n)
	}
}

// cancelAfterWrite cancels the boot's context once a write of the reingestion
// returns, and records what relplog_emergency_events_pending and
// relplog_emergency_files said while it ran.
type cancelAfterWrite struct {
	sink.Sink
	cancel       context.CancelFunc
	pending      *atomic.Int64
	files        *atomic.Int64
	m            func() *obs.Metrics
	sawFirstOnce *sync.Once
}

func (c cancelAfterWrite) Write(ctx context.Context, b *sink.Batch) error {
	if c.m != nil && c.sawFirstOnce != nil {
		c.sawFirstOnce.Do(func() {
			c.pending.Store(c.m().EmergencyEventsPending.Load())
			c.files.Store(c.m().EmergencyFiles.Load())
		})
	}
	err := c.Sink.Write(ctx, b)
	if c.cancel != nil {
		c.cancel()
	}
	return err
}

// SPEC §12.2: while dumps are reingested, relplog_emergency_files says how many
// wait, and relplog_emergency_events_pending is not known until the recount at
// the end. It said the size of the chunk in flight (4 here, with 20 on disk),
// a number that is neither, and that made
// TestEmergencyGaugesRecountTheDiskAfterReingestion pass with the recount
// removed (M31). TestReingestionDoesNotShowTheChunkAsPending proves the same
// on a hand-built App; this one on the real boot.
func TestPendingGaugeIsNotTheChunkDuringReingestion(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	dumpOf(t, f.id, dir, crashItems(10, "a"))
	dumpOf(t, f.id, dir, crashItems(10, "b"))
	var pending, files atomic.Int64
	pending.Store(-1)
	var once sync.Once
	var app atomic.Pointer[App]
	in := launch(t, f.id, f.args(dir, "--sink.batch=4"), func(s sink.Sink) sink.Sink {
		return cancelAfterWrite{Sink: s, pending: &pending, files: &files, sawFirstOnce: &once,
			m: func() *obs.Metrics { return app.Load().metrics }}
	}, func(a *App) { app.Store(a) })
	in.waitReady()
	if got := pending.Load(); got != 0 {
		t.Errorf("relplog_emergency_events_pending during the reingestion = %d, want 0 (unknown until the recount)", got)
	}
	if got := files.Load(); got != 2 {
		t.Errorf("relplog_emergency_files during the reingestion = %d, want the 2 dumps", got)
	}
	if got := in.metric("relplog_emergency_events_pending"); got != "0" {
		t.Errorf("relplog_emergency_events_pending after the reingestion = %s, want 0", got)
	}
}

// SPEC §12.2: the dump written at the shutdown ADDS to the gauges. A dump the
// boot could not reingest (here, unreadable) is still pending, and setting the
// gauges to the new dump alone forgot it (M18).
func TestShutdownDumpAddsToTheGauges(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads through any permission")
	}
	f := newFixture(t)
	dir := t.TempDir()
	old := dumpOf(t, f.id, dir, crashItems(2, "old"))
	if err := os.Chmod(old, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(old, 0o600) })
	fs := newFaultySink(true)
	fs.failFatal.Store(true)
	in := launch(t, f.id, f.args(dir), fs.wrap)
	in.waitReady()
	if got := in.metric("relplog_emergency_files"); got != "1" {
		t.Fatalf("relplog_emergency_files = %s with the unreadable dump on disk, want 1", got)
	}
	const n = 3
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - gauge-%d", i)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
	fs.open()
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if fl, e := m.EmergencyFiles.Load(), m.EmergencyEventsPending.Load(); fl != 2 || e != n {
		t.Fatalf("after the dump: files %d, events %d; want 2 files (the old one too) and %d events", fl, e, n)
	}
}

// SPEC §13.5: a signal that lands after the reingestion and before the bind
// cancels the bind context; with a host name in listen.tcp the lookup fails.
// That is an interrupted boot, 130, not the exit 1 of a bad address.
// TestCancelledBindIsAnAbortNotAConfigError proves it on admin.bind, before
// the sink; this one on the transport, after the reingestion.
func TestSignalBeforeTheBindExits130(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	dumpOf(t, f.id, dir, crashItems(3, "pre-bind"))
	args := append(withoutFlag(f.args(dir), "--listen.tcp="), "--listen.tcp=localhost:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := runContext(ctx, args, f.id, func(a *App) {
		a.abort = make(chan os.Signal, 1)
		a.wrapSink = func(s sink.Sink) sink.Sink { return cancelAfterWrite{Sink: s, cancel: cancel} }
	})
	if code != ExitAborted {
		t.Errorf("a signal before the bind exited %d, want %d", code, ExitAborted)
	}
}

// An abort that lands after the dump's last abandon check -- the dump then
// publishes -- counted every message, the malformed ones the dump leaves out
// included: the dump counts nothing more, no reason="malformed" for them
// (independent review of go-relplog-pg-server, 2026-10-13, A7).
func TestAbortAfterTheLastAbandonCheckCountsNothingMore(t *testing.T) {
	a, held := abortApp(t)
	a.cfg.Syslog.AcceptMalformed = false
	raw := []byte("GET / HTTP/1.1") // malformed: refused by the dump
	bad := item.New(crashHead(time.Now(), raw), raw, -1)
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet")
	}
	items := append(append(held, a.ring.Drain()...), bad)
	a.oweDump(len(items))
	a.emg = emergency.NewStore(t.TempDir(), testID.From)
	var checks atomic.Int32
	a.emg.SetAbandon(func() bool {
		if checks.Add(1) == 2 { // the check before the publish
			a.abortLoss() // the abort lands right after it
		}
		return false
	})
	if !a.dumpEmergency(items) {
		t.Fatal("the dump did not complete")
	}
	m := a.metrics
	if aborted(a) != uint64(len(items)) || m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load() != 0 ||
		m.EventsDroppedTotal[obs.EventDropMalformed].Load() != 0 {
		t.Fatalf("abort %d, emergency_failed %d, malformed %d; want %d, 0, 0", aborted(a),
			m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(),
			m.EventsDroppedTotal[obs.EventDropMalformed].Load(), len(items))
	}
}

// SPEC §5.8, §13.4 phase 3: the receive context is cancelled BEFORE the
// transports close, and that is what releases a transaction held in the
// backpressure loop: unanswered, reason="shutdown", for librelp to
// retransmit. The shutdown does not wait for it (M30).
func TestShutdownReleasesAHeldTransactionAsShutdown(t *testing.T) {
	f := newFixture(t)
	fs := newFaultySink(false)
	in := launch(t, f.id, f.args(t.TempDir(), "--relp.backpressure_timeout=30s",
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	fs.failHealth.Store(true)
	waitFor(t, 10*time.Second, "the sink DOWN", func() bool { return m.SinkUp.Load() == 0 })
	rc := dialRELP(t, in.app.TransportAddr("tcp"))
	rc.syslog("<13>1 2026-09-12T14:33:02Z host app - - - held-at-shutdown")
	waitFor(t, 10*time.Second, "the transaction held", func() bool { return m.RELPInflightTransactions.Load() == 1 })
	began := time.Now()
	in.stop()
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("the shutdown took %v with a held transaction", took)
	}
	if n := m.RELPUnansweredTotal[obs.UnansweredShutdown].Load(); n != 1 {
		t.Fatalf(`relp_unanswered_total{reason="shutdown"} = %d, want 1 (server_closed %d, conn_lost %d)`, n,
			m.RELPUnansweredTotal[obs.UnansweredServerClosed].Load(), m.RELPUnansweredTotal[obs.UnansweredConnLost].Load())
	}
}
