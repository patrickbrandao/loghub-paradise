package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/sink"
)

// stuckWriteSink wraps the real NATS sink: from the second write on, Write
// never returns until release is closed, whatever its context says -- the
// shape of a publish flushed inline on a socket nobody reads, before the
// socket had a deadline (SPEC §10.7, review of 2026-11-21, item 001), or of
// any later write stuck below its context. With failHealth, Health fails as
// the real probe does when the connection's lock is held by such a write, so
// the fuses take the sink to FATAL.
type stuckWriteSink struct {
	sink.Sink
	failHealth bool
	writes     atomic.Int64
	stuck      atomic.Bool
	release    chan struct{}
}

func (s *stuckWriteSink) Write(ctx context.Context, b *sink.Batch) error {
	if s.writes.Add(1) >= 2 {
		s.stuck.Store(true)
		<-s.release // the context is ignored on purpose
		return nil
	}
	return s.Sink.Write(ctx, b)
}

func (s *stuckWriteSink) Health(ctx context.Context) error {
	if s.failHealth && s.stuck.Load() {
		return &sink.Error{Class: sink.ClassNetwork, Op: "health",
			Err: errors.New("the connection's lock is held by a write past the probe's deadline (injected)")}
	}
	return s.Sink.Health(ctx)
}

// SPEC §13.4: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the chunks the stuck workers hold go to the dump with the rest of the ring,
// and the process exits 3 -- on the FATAL path and on a SIGTERM's. What the
// first write published stays in the stream and out of the dump. Before
// 2026-11-23 the shutdown waited in Workers.Wait for good: no dump, no exit,
// until the SIGKILL took the ring with nothing counting it (review of
// 2026-11-21, item 016).
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fatal bool // the FATAL shutdown; otherwise a SIGTERM's
	}{{"fatal", true}, {"sigterm", false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			dir := t.TempDir()
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := launch(t, f.id, f.args(dir,
				"--listen.udp=", "--sink.max_retries=2", "--sink.fail_timeout=1s",
				"--sink.workers=2", "--sink.batch=4", "--sink.op_timeout=1s", "--shutdown.drain_timeout=1s"),
				func(s sink.Sink) sink.Sink { ss.Sink = s; return ss })
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(ss.release) }) }
			// Also from a Cleanup registered after launch's, so it runs first: a test
			// that fails early still lets the stuck write go, the instance exits, and its
			// own cleanup removes what it made at the destination.
			t.Cleanup(release)
			in.waitReady()

			sendNUL(t, in.app.TransportAddr("tcp"), docs(40, "stuck-%04d")...)
			// Not necessarily all 40: once the sink is DOWN the transport may
			// stop reading. What was accepted is what must not be lost.
			waitFor(t, 10*time.Second, "a write stuck", ss.stuck.Load)

			start := time.Now()
			var code int
			if tc.fatal {
				code = in.waitExit(20 * time.Second)
			} else {
				code = in.stop()
			}
			took := time.Since(start)
			if code != ExitSink {
				t.Fatalf("exit %d after %s, want %d", code, took.Round(time.Millisecond), ExitSink)
			}
			if took > 15*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}

			accepted := int(in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load())
			written := int(in.app.metrics.EventsWrittenTotal.Load())
			if written == 0 {
				t.Fatal("the first write delivered nothing: the test needs a delivered chunk next to the stuck ones")
			}
			if got := f.count(); got != written {
				t.Errorf("the stream holds %d messages and the sink counted %d written", got, written)
			}
			dumps := dumpFiles(t, in.emgDir(), f.id.From)
			if len(dumps) != 1 {
				t.Fatalf("dump files = %v, want 1", dumps)
			}
			dumped := dumpLines(t, dumps[0])
			if len(dumped)+written != accepted || accepted < 8 {
				t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted",
					len(dumped), written, accepted)
			}
			inStream := map[string]bool{}
			for _, m := range f.msgs() {
				inStream[m.env.UUID] = true
			}
			for _, e := range dumped {
				if inStream[e.UUID] {
					t.Errorf("envelope %s is in the stream and in the dump: the dump took a delivered chunk", e.UUID)
				}
			}

			// The stuck writes return at last, successfully. Their chunks are
			// the shutdown's now: the workers settle nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writers next to the dump's reads.
			release()
			time.Sleep(200 * time.Millisecond)
		})
	}
}
