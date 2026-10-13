package app

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/sink"
)

// Findings of go-relplog-nats-server's independent review of 2026-10-13,
// ported (docs/TESTING.md).

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
	lost := aborted(a) + a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
	if lost != uint64(total) {
		t.Fatalf("%d counted lost for %d (abort %d, emergency_failed %d)", lost, total,
			aborted(a), a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load())
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
	ents, _ := os.ReadDir(dir)
	if len(ents) != 0 {
		t.Fatalf("the abandoned dump left %d entries: %v", len(ents), ents[0].Name())
	}
	if got := aborted(a) + a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); got != uint64(len(items)) {
		t.Fatalf("%d counted lost for %d", got, len(items))
	}
	// The store on its own, mid-way: no temporary is left either.
	s := emergency.NewStore(dir, testID.From)
	var calls atomic.Int32
	s.SetAbandon(func() bool { return calls.Add(1) > 1 })
	if _, err := s.Write(crashItems(testID, 3, "x"), testConverter(t), true); !errors.Is(err, emergency.ErrAbandoned) {
		t.Fatalf("Write = %v, want ErrAbandoned", err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("a temporary survived: %v", ents[0].Name())
	}
}

// A SIGQUIT whose watcher stores the flag just after shutdownWith read it
// still aborts at once.
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

func reingestApp(t *testing.T, s sink.Sink, bootTimeout time.Duration) (*App, string) {
	t.Helper()
	dataDir := t.TempDir()
	cfg := &config.Config{}
	cfg.Emergency.Dir = filepath.Join(dataDir, "emergency")
	cfg.Sink.Batch = 1
	cfg.Sink.BatchMaxBytes = 1 << 20
	cfg.Sink.OpTimeout = time.Second
	cfg.Sink.RetryBackoffMin = 10 * time.Millisecond
	cfg.Sink.RetryBackoffMax = 20 * time.Millisecond
	cfg.Sink.BootTimeout = bootTimeout
	m := obs.NewMetrics(obs.Info{})
	a := &App{cfg: cfg, id: testID, metrics: m, log: obs.NewLogger(io.Discard, "text", "error"),
		admin: NewAdmin(m, 0.8)}
	a.direct = sink.NewInstrumented(s, nil, a.workerConfig(), a.log.Logger, m)
	return a, dataDir
}

// One boot_timeout WITHOUT PROGRESS: a healthy reingestion that runs past the
// budget still retries a transient failure. With one deadline from the start,
// the third chunk's first failure was exit 3, and every boot after it
// republished the same dump.
func TestLongReingestionStillRetriesATransientFailure(t *testing.T) {
	s := &scriptSink{script: func(n int) error {
		time.Sleep(150 * time.Millisecond)
		if n == 3 {
			return &sink.Error{Class: sink.ClassNetwork, Op: "write", Err: errors.New("blip")}
		}
		return nil
	}}
	a, dataDir := reingestApp(t, s, 250*time.Millisecond)
	dumpOf(t, testID, dataDir, crashItems(testID, 3, "long"))
	if code := a.reingest(context.Background()); code != ExitOK {
		t.Fatalf("reingest = %d, want %d", code, ExitOK)
	}
	if got := s.calls.Load(); got != 4 {
		t.Fatalf("%d writes, want 4 (3 chunks and one retry)", got)
	}
	// A sink that never recovers still ends the boot: no progress for a whole
	// boot_timeout is exit 3.
	dead := &scriptSink{script: func(int) error {
		return &sink.Error{Class: sink.ClassNetwork, Op: "write", Err: errors.New("down")}
	}}
	a, dataDir = reingestApp(t, dead, 100*time.Millisecond)
	dumpOf(t, testID, dataDir, crashItems(testID, 2, "dead"))
	if code := a.reingest(context.Background()); code != ExitSink {
		t.Fatalf("reingest against a dead sink = %d, want %d", code, ExitSink)
	}
}

// syslog_emergency_events_pending says what is on disk; during the
// reingestion it used to read the size of the chunk being delivered.
func TestReingestionDoesNotShowTheChunkAsPending(t *testing.T) {
	var a *App
	var seen atomic.Int64
	seen.Store(-1)
	s := &scriptSink{script: func(int) error {
		if v := a.metrics.EmergencyEventsPending.Load(); v != 0 {
			seen.Store(v)
		}
		return nil
	}}
	a, dataDir := reingestApp(t, s, time.Second)
	dumpOf(t, testID, dataDir, crashItems(testID, 2, "g"))
	if code := a.reingest(context.Background()); code != ExitOK {
		t.Fatal(code)
	}
	if v := seen.Load(); v != -1 {
		t.Fatalf("pending read %d during the reingestion, the size of a chunk", v)
	}
	if v := a.metrics.EmergencyEventsPending.Load(); v != 0 {
		t.Fatalf("pending = %d after a clean reingestion", v)
	}
}

// A signal that cancels the boot while the admin port binds a host name is an
// interrupted boot (130), not a bad address (1): the lookup of "localhost"
// fails with "operation was canceled".
func TestCancelledBindIsAnAbortNotAConfigError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := append(localArgs(t.TempDir()), "--admin.bind=localhost:0")
	if code := RunContext(ctx, args, testID); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
}
