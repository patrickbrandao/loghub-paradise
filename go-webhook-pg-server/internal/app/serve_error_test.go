package app

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
)

// failServeOn makes the public Serve return an error once fail is closed,
// while the real one keeps answering until the shutdown stops it: the error
// that a listener which could not be retried any more would bring.
func failServeOn(fail <-chan struct{}) func(func() error) func() error {
	return func(serve func() error) func() error {
		return func() error {
			go func() { _ = serve() }()
			<-fail
			return errors.New("accept: no buffer space available")
		}
	}
}

// SPEC §10.3 and §10.5 (port of go-webhook-server, W 5): a public Serve that
// still ends with an error goes through the graceful shutdown -- drain, and
// the dump when the destination cannot take it -- and exits 1 only if that
// shutdown was complete. It used to return 1 at once: the ring, answered 200,
// vanished with no dump and no count.
func TestServeErrorGoesThroughTheShutdown(t *testing.T) {
	t.Run("held sink: the accepted events are dumped", func(t *testing.T) {
		dir := t.TempDir()
		ms := &memSink{}
		fail := make(chan struct{})
		in := startHooked(t, dir, ms, func(a *App) {
			a.wrapServe = failServeOn(fail)
		}, "--sink.workers=1", "--sink.batch=4", "--sink.op_timeout=50ms",
			"--shutdown.drain_timeout=300ms", "--shutdown.pre_stop_delay=0s")
		ms.hang.Store(true) // takes nothing, with the health check UP
		ids := in.postN(5)
		if len(ids) != 5 {
			t.Fatal("not every request was accepted")
		}
		close(fail)
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d: the shutdown dumps what the sink did not take", code, ExitSink)
		}
		checkDump(t, dir, testID, envelope.ModeFast, ids)
	})

	// No signal started this shutdown, so the orchestrator's first SIGTERM
	// -- sent because /readyz failed -- is tolerated like in a FATAL one: it
	// used to count as the second signal and abort with 130, the ring
	// answered 200 going to reason="abort" instead of the dump (W 31).
	t.Run("held sink, then one signal: still dumped, exit 3", func(t *testing.T) {
		dir := t.TempDir()
		ms := &memSink{}
		fail := make(chan struct{})
		in := startHooked(t, dir, ms, func(a *App) {
			a.wrapServe = failServeOn(fail)
		}, "--sink.workers=1", "--sink.batch=4", "--sink.op_timeout=50ms",
			"--shutdown.drain_timeout=500ms", "--shutdown.pre_stop_delay=0s")
		ms.hang.Store(true)
		ids := in.postN(5)
		if len(ids) != 5 {
			t.Fatal("not every request was accepted")
		}
		close(fail)
		in.abort <- syscall.SIGTERM
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d: the first signal of this shutdown is tolerated", code, ExitSink)
		}
		checkDump(t, dir, testID, envelope.ModeFast, ids)
	})

	t.Run("healthy sink: drained, then exit 1", func(t *testing.T) {
		dir := t.TempDir()
		ms := &memSink{}
		fail := make(chan struct{})
		in := startHooked(t, dir, ms, func(a *App) {
			a.wrapServe = failServeOn(fail)
		}, "--sink.workers=1", "--sink.batch_max_delay=2s", "--sink.batch=512",
			"--shutdown.drain_timeout=10s", "--shutdown.pre_stop_delay=0s")
		ids := in.postN(5)
		if len(ids) != 5 {
			t.Fatal("not every request was accepted")
		}
		close(fail)
		if code := in.waitExit(30 * time.Second); code != ExitConfig {
			t.Fatalf("exit %d, want %d: the listener stopped, the shutdown was complete", code, ExitConfig)
		}
		if lost := missing(ids, eventsIn(t, in)); len(lost) > 0 {
			t.Fatalf("%d of %d accepted events never reached the sink", len(lost), len(ids))
		}
		if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 0 {
			t.Fatalf("%d dumps after a complete drain", len(dumps))
		}
	})
}
