package app

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// The loss ledger of an abort (SPEC §13.4, §22 pendência 29), driven by hand:
// ported from go-relplog-server (review_abort_test.go, independent review of
// 2026-10-11) and from go-relplog-nats-server (review_rn_test.go and
// review_cut_test.go, independent reviews of 2026-10-13), where the
// ownership of the count ends in its final form -- lossMu, enterDumpPhase,
// oweDump, emergency.Store.SetAbandon, and nothing counted after the owner.

// abortApp is the part of an App that abortLoss and dumpEmergency read: a
// memory ring holding five messages, two of them in a batch a worker holds.
func abortApp(t *testing.T) (*App, []*item.Item) {
	t.Helper()
	r := ring.NewMemory(100, 1<<20, time.Millisecond)
	for _, it := range crashItems(5, "abort") {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	held, _ := r.Dequeue(context.Background(), 2)
	cfg := config.Defaults()
	cfg.Ring.Mode = config.RingModeMemory
	a := &App{cfg: cfg, ring: r, metrics: obs.NewMetrics(obs.Info{}), log: obs.NewLogger(io.Discard, "text", "error")}
	return a, held
}

func aborted(a *App) uint64 { return a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load() }

func failedDumps(a *App) uint64 {
	return a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
}

// An abort between the drain and the dump counts every leftover: the counter
// is set from the ring's count BEFORE the drain (enterDumpPhase), when held
// and queued items are still one population there.
func TestAbortBetweenTheDrainAndTheDumpCountsEverything(t *testing.T) {
	a, held := abortApp(t)
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet, the dump phase must start")
	}
	drained := a.ring.Drain()
	a.abortLoss()
	if got := aborted(a); got != uint64(len(held)+len(drained)) {
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
	if got := aborted(a); got != 0 {
		t.Fatalf("abort after a saved dump counted %d saved messages as lost", got)
	}
}

// With emergency.enabled=false, an abort and the shutdown's own
// emergency_failed count never count the same messages twice: the counter is
// taken with Swap by exactly one of them.
func TestAbortAndDisabledDumpCountOnce(t *testing.T) {
	a, held := abortApp(t)
	items := append(held, a.ring.Drain()...)
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.abortLoss() // the abort wins the race
	a.dumpEmergency(items)
	if lost := aborted(a) + failedDumps(a); lost != uint64(len(items)) {
		t.Fatalf("%d messages counted lost for %d (abort %d, emergency_failed %d)", lost, len(items),
			aborted(a), failedDumps(a))
	}
}

// After a dump that FAILED the loss is emergency_failed's, counted once, and an
// abort racing it counts nothing more (Swap: one side only).
func TestAbortAndFailedDumpCountOnce(t *testing.T) {
	a, held := abortApp(t)
	items := append(held, a.ring.Drain()...)
	dir := t.TempDir()
	a.emg = emergency.NewStore(dir, testID.From)
	// A file where the directory was fails the write.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	if a.dumpEmergency(items) {
		t.Fatal("the dump into a file instead of a directory succeeded")
	}
	a.abortLoss() // an abort right after the failure
	if failed := failedDumps(a); failed != uint64(len(items)) || aborted(a) != 0 {
		t.Fatalf("emergency_failed %d, abort %d; want %d and 0", failed, aborted(a), len(items))
	}
}

// An abort that lands while the shutdown collects the leftovers (Pending,
// Drain) took the counter; putting it back afterwards let a disabled dump count
// the same messages again: 10 counted lost for 5.
func TestAbortDuringTheDrainIsCountedOnce(t *testing.T) {
	a, held := abortApp(t)
	total := a.ring.Stats().Items
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet, the dump phase must start")
	}
	items := append(held, a.ring.Drain()...)
	a.abortLoss() // the second signal, during Pending/Drain
	if a.oweDump(len(items)) {
		a.dumpEmergency(items) // emg == nil: a disabled dump
	}
	if lost := aborted(a) + failedDumps(a); lost != uint64(total) {
		t.Fatalf("%d counted lost for %d (abort %d, emergency_failed %d)", lost, total,
			aborted(a), failedDumps(a))
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
	if got := aborted(a); got != uint64(total) {
		t.Fatalf("abort counted %d for %d", got, total)
	}
}

// A dump counted lost by an abort must not reach the disk and come back at the
// next boot: the store gives up and leaves nothing behind.
func TestDumpAfterTheAbortIsAbandoned(t *testing.T) {
	a, held := abortApp(t)
	dir := t.TempDir()
	a.emg = emergency.NewStore(dir, testID.From)
	a.emg.SetAbandon(a.aborted.Load)
	items := append(held, a.ring.Drain()...)
	a.dumpPhase.Store(true)
	a.dumping.Store(int64(len(items)))
	a.abortLoss()
	a.dumpEmergency(items)
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("the abandoned dump left %d entries: %v", len(ents), ents[0].Name())
	}
	if got := aborted(a) + failedDumps(a); got != uint64(len(items)) {
		t.Fatalf("%d counted lost for %d", got, len(items))
	}
}

// An abort that lands after the dump's last abandon check -- the dump then
// publishes -- counted every message: the dump counts nothing more, and the
// file is reingested at the next boot (an overcount, never a loss).
func TestAbortAfterTheLastAbandonCheckCountsNothingMore(t *testing.T) {
	a, held := abortApp(t)
	if !a.enterDumpPhase() {
		t.Fatal("no abort yet")
	}
	items := append(held, a.ring.Drain()...)
	a.oweDump(len(items))
	dir := t.TempDir()
	a.emg = emergency.NewStore(dir, testID.From)
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
	if aborted(a) != uint64(len(items)) || failedDumps(a) != 0 {
		t.Fatalf("abort %d, emergency_failed %d; want %d, 0", aborted(a), failedDumps(a), len(items))
	}
	if d, _ := emergency.NewStore(dir, testID.From).List(); len(d) != 1 {
		t.Fatalf("dumps on disk = %v, want the one that completed", d)
	}
}

// A SIGQUIT that reached only run's watcher -- after the shutdown branch was
// taken, before the abort channel existed -- still aborts at once.
func TestLateSIGQUITStillAborts(t *testing.T) {
	a, _ := abortApp(t)
	a.quitting.Store(true)
	block := make(chan struct{})
	defer close(block)
	done := make(chan int, 1)
	go func() {
		done <- a.shutdownWith(func() int { <-block; return ExitOK }, false, make(chan os.Signal))
	}()
	select {
	case code := <-done:
		if code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the graceful shutdown ran although SIGQUIT had arrived")
	}
}

// A SIGQUIT whose watcher stores the flag just after shutdownWith read it
// still aborts at once: it arrives through quitNow, not through a flag read
// once at the entry.
func TestSIGQUITAfterTheEntryCheckStillAborts(t *testing.T) {
	a, _ := abortApp(t)
	a.quitNow = make(chan struct{})
	block := make(chan struct{})
	defer close(block)
	done := make(chan int, 1)
	go func() {
		done <- a.shutdownWith(func() int { <-block; return ExitOK }, false, make(chan os.Signal))
	}()
	time.Sleep(50 * time.Millisecond) // past the entry check
	a.quitting.Store(true)
	close(a.quitNow)
	select {
	case code := <-done:
		if code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the graceful shutdown went on although SIGQUIT had arrived")
	}
}

// SPEC §13.4, §22 pendência 28: in a FATAL shutdown the first SIGTERM/SIGINT is
// tolerated (logged), the second aborts, and SIGQUIT always aborts.
func TestFatalShutdownToleratesTheFirstSignal(t *testing.T) {
	run := func(fatal bool, sigs ...os.Signal) int {
		a, _ := abortApp(t)
		abort := make(chan os.Signal, len(sigs))
		block := make(chan struct{})
		defer close(block)
		done := make(chan int, 1)
		go func() {
			done <- a.shutdownWith(func() int { <-block; return ExitSink }, fatal, abort)
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
	if code := run(true, os.Interrupt); code != -1 {
		t.Errorf("FATAL shutdown, one SIGINT: exit %d, want it to go on", code)
	}
	if code := run(true, os.Interrupt, os.Interrupt); code != ExitAborted {
		t.Errorf("FATAL shutdown, two SIGINT: exit %d, want %d", code, ExitAborted)
	}
	if code := run(true, syscall.SIGQUIT); code != ExitAborted {
		t.Errorf("FATAL shutdown, SIGQUIT: exit %d, want %d", code, ExitAborted)
	}
	if code := run(false, os.Interrupt); code != ExitAborted {
		t.Errorf("signalled shutdown, second signal: exit %d, want %d", code, ExitAborted)
	}
}
