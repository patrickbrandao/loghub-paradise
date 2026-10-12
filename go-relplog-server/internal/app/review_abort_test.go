package app

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// Regression tests from the independent review of 2026-10-11 (SPEC §13.4).

// abortApp is the part of an App that abortLoss and dumpEmergency read: a
// memory ring holding five messages, two of them in a batch a worker holds.
func abortApp(t *testing.T) (*App, []*item.Item) {
	t.Helper()
	r := ring.NewMemory(100, 1<<20, time.Millisecond)
	now := time.Now()
	for i := 0; i < 5; i++ {
		raw := []byte("<13>1 - h a - - - x")
		if err := r.Enqueue(context.Background(), item.New(crashHead(now, raw), raw, -1)); err != nil {
			t.Fatal(err)
		}
	}
	held, _ := r.Dequeue(context.Background(), 2)
	cfg := &config.Config{}
	cfg.Ring.Mode = config.RingModeMemory
	a := &App{cfg: cfg, ring: r, metrics: obs.NewMetrics(obs.Info{}), log: obs.NewLogger(io.Discard, "text", "error")}
	return a, held
}

func aborted(a *App) uint64 { return a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load() }

// An abort between the drain and the dump counts every leftover: the counter
// is set from the ring's count BEFORE the drain (gracefulShutdown), when held
// and queued items are still one population there.
func TestAbortBetweenTheDrainAndTheDumpCountsEverything(t *testing.T) {
	a, held := abortApp(t)
	a.dumping.Store(a.ring.Stats().Items) // what gracefulShutdown does before Drain
	a.dumpPhase.Store(true)
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
	lost := aborted(a) + a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
	if lost != uint64(len(items)) {
		t.Fatalf("%d messages counted lost for %d (abort %d, emergency_failed %d)", lost, len(items),
			aborted(a), a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load())
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
