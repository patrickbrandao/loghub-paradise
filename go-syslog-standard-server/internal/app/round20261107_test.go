package app

// Tests of the §21.1 items fixed on 2026-11-07 in this package: the emergency
// gauges recounted from disk (14), the abort counting the held batches (15) and
// the single boot deadline of the sink (17).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"
)

// Item 14: a dump that cannot be read stays on disk, and the gauges say so
// after the reingestion instead of going to zero (SPEC §11.2).
func TestEmergencyGaugesCountWhatIsLeftOnDisk(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a file of mode 000")
	}
	dir := t.TempDir()
	emg := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emg, 0o750); err != nil {
		t.Fatal(err)
	}
	stuck := filepath.Join(emg, "slstd-emg-old-1.bin")
	if err := os.WriteFile(stuck, []byte("0 3\nabc"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stuck, 0o600) })
	// go-syslog-server's dump in the same directory is not this server's, and
	// is neither read, counted nor renamed (pendência 20).
	foreign := filepath.Join(emg, "emg-other-2.bin")
	if err := os.WriteFile(foreign, []byte("not ours"), 0o600); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir, "--listen.udp=", "--listen.relp=")
	defer in.stop()
	if got := in.app.metrics.EmergencyFiles.Load(); got != 1 {
		t.Fatalf("syslog_emergency_files = %d with an unreadable dump left behind, want 1", got)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("go-syslog-server's dump was touched: %v", err)
	}
	if _, err := os.Stat(foreign + ".corrupt"); err == nil {
		t.Fatal("go-syslog-server's dump was quarantined")
	}
}

// Item 15: a second signal that lands while the shutdown writes the emergency
// dump counts the whole dump as lost -- the batch the worker held AND the queue
// Drain took out of the ring's accounting -- not the (nearly) empty ring
// (SPEC §12.3).
func TestSecondSignalDuringTheDumpCountsEverything(t *testing.T) {
	dir := t.TempDir()
	var wrapped *failingWrites
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	appCh := make(chan *App, 1)
	tcp := freeAddr(t)
	inDump := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	args := []string{
		"--ring.data_dir=" + dir,
		"--admin.bind=" + freeAddr(t),
		"--listen.udp=",
		"--listen.tcp=" + tcp,
		"--sink.type=sqlite",
		"--sink.sqlite.synchronous=OFF",
		"--sink.batch=1",
		"--sink.health_interval=50ms",
		"--sink.retry_backoff_min=5ms",
		"--sink.retry_backoff_max=10ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=100ms",
		"--log.level=error",
		"--node.id=abort",
	}
	go func() {
		done <- runContext(ctx, args, func(a *App) {
			a.abort = make(chan os.Signal, 1)
			a.ready = make(chan struct{})
			a.wrapSink = func(s sink.Sink) sink.Sink {
				wrapped = &failingWrites{Sink: s}
				wrapped.fail.Store(true)
				return wrapped
			}
			a.beforeDump = func() {
				close(inDump)
				<-release // the dump never finishes: the abort's os.Exit cuts it
			}
			appCh <- a
		})
	}()
	a := <-appCh
	select {
	case <-a.Ready():
	case code := <-done:
		t.Fatalf("exited during boot: %d", code)
	case <-time.After(15 * time.Second):
		t.Fatal("never opened its transports")
	}

	c, err := net.Dial("tcp", tcp)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		fmt.Fprintf(c, "<13>held %d\n", i)
	}
	c.Close()
	// One worker (sink=sqlite), batch 1: it holds one message and the other
	// two wait in the ring's queue.
	if !waitFor(10*time.Second, func() bool {
		return a.workers != nil && a.workers.Held() == 1 && a.ring.Stats().Items == 3
	}) {
		t.Fatal("never reached one held message and two queued")
	}

	cancel() // first signal: drain, collect, dump
	select {
	case <-inDump:
	case <-time.After(10 * time.Second):
		t.Fatal("the shutdown never reached the dump")
	}
	a.abort <- os.Interrupt
	select {
	case code := <-done:
		if code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second signal did not abort")
	}
	if got := a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 3 {
		t.Fatalf("events_dropped_total{reason=abort} = %d, want 3: the dump being written is the loss", got)
	}
}

// downSink fails every health check with a network error.
type downSink struct{ sink.Sink }

func (downSink) Health(context.Context) error {
	return &sink.Error{Class: sink.ClassNetwork, Op: "health", Err: errors.New("down (injected)")}
}

// Item 17: the boot of the sink has ONE sink.boot_timeout. Time spent building
// the sink (the redis dial loop) is taken out of what WaitReady gets; before,
// each step had the whole budget and the wait could double.
func TestSinkBootHasOneDeadline(t *testing.T) {
	const boot = time.Second
	const building = 700 * time.Millisecond
	dir := t.TempDir()
	start := time.Now()
	code := runContext(context.Background(), []string{
		"--ring.data_dir=" + dir,
		"--admin.bind=" + freeAddr(t),
		"--listen.udp=",
		"--listen.tcp=" + freeAddr(t),
		"--sink.type=sqlite",
		"--sink.sqlite.synchronous=OFF",
		"--sink.boot_timeout=" + boot.String(),
		"--log.level=error",
		"--node.id=deadline",
	}, func(a *App) {
		a.abort = make(chan os.Signal, 1)
		a.wrapSink = func(s sink.Sink) sink.Sink {
			time.Sleep(building) // the dial loop's share of the budget
			return downSink{s}
		}
	})
	elapsed := time.Since(start)
	if code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if elapsed > boot+400*time.Millisecond {
		t.Fatalf("the boot gave up after %v with sink.boot_timeout=%v: the wait got its own budget", elapsed, boot)
	}
}

// Revisão independente de 2026-11-07, STD item 1: the registry of what the
// shutdown collected is cleared as soon as the dump file is durable, BEFORE
// the gauges are recounted from the directory -- a second signal during the
// recount does not count as lost what is already on disk (SPEC §12.3).
func TestCollectedIsClearedBeforeTheRecount(t *testing.T) {
	dir := t.TempDir()
	a := &App{
		log:     obs.NewLogger(io.Discard, "text", "error"),
		metrics: obs.NewMetrics("memory", "none", "tcp", obs.BuildInfo{}),
		emg:     emergency.NewStore(dir, "recount"),
	}
	items := []*envelope.Item{envelope.NewItem(envelope.Head{ID: "019823f1-7a2c-7b3d-8e4f-000000000001"}, []byte("x"), -1)}
	a.collected = len(items)
	seen := -1
	a.beforeRecount = func() {
		a.lossMu.Lock()
		seen = a.collected
		a.lossMu.Unlock()
	}
	if !a.dumpEmergency(items) {
		t.Fatal("the dump failed")
	}
	if seen != 0 {
		t.Fatalf("collected = %d while the gauges were recounted: an abort there counts the dump on disk as lost", seen)
	}
	if got := a.metrics.EmergencyEventsPending.Load(); got != 1 {
		t.Fatalf("syslog_emergency_events_pending = %d, want 1", got)
	}
}
