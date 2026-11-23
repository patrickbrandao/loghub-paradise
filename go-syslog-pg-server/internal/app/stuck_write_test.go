package app

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/sink"
)

// stuckWriteSink wraps the real PostgreSQL sink: from the second write on,
// Write never returns until release is closed, whatever its context says.
// The pgx write honours its context (it arms the socket's deadline from it),
// so this is the shape of a write that does not -- what a COMMIT stuck in
// fsync is in the parent's sink=sqlite -- and the shutdown may not depend on
// every write being well-behaved. With failHealth, Health fails once a write
// is stuck, as a database whose connections hang would, so the fuses of
// §13.3 take the sink to FATAL.
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
			Err: errors.New("the database stopped answering (injected)")}
	}
	return s.Sink.Health(ctx)
}

// SPEC §13.4: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the chunks the stuck workers hold go to the dump with the rest of the ring,
// and the process exits 3 -- on the FATAL path (the fuses of §13.3) and on a
// SIGTERM's. Before 2026-11-23 the shutdown waited in Workers.Wait for good:
// no dump, no exit, until the SIGKILL took the ring with nothing counting it
// (review of 2026-11-21, item 016).
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fatal bool // the FATAL shutdown; otherwise a SIGTERM's
	}{
		{"fatal, ring=memory", true},
		{"sigterm, ring=memory", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := launch(t, testID, append(baseArgs(t, dir),
				"--listen.udp=", "--sink.workers=2", "--sink.max_retries=2", "--sink.fail_timeout=1s",
				"--sink.batch=4", "--sink.op_timeout=1s", "--shutdown.drain_timeout=1s"),
				func(s sink.Sink) sink.Sink { ss.Sink = s; return ss })
			// A failing run (the unbounded Wait of before) must not leave the
			// stuck writes, and the instance, behind it: released here, the
			// instance exits and the cleanups of the destination run. The
			// shutdown leaves the sink unclosed when a worker never returned
			// (closeSink); the test process outlives it, so the pool is
			// closed here too. Close is idempotent.
			released := false
			t.Cleanup(func() {
				if !released {
					close(ss.release)
				}
				if ss.Sink != nil {
					_ = ss.Sink.Close()
				}
			})
			in.waitReady()

			const n = 40
			lines := make([]string, n)
			for i := range lines {
				lines[i] = fmt.Sprintf("<13>1 - h app - - - stuck-%04d", i)
			}
			sendLF(t, in.app.TransportAddr("tcp"), lines...)
			// Not necessarily all n: once the sink is DOWN the transport may
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
			dumps := dumpFiles(t, in.emgDir(), testID.From)
			if len(dumps) != 1 {
				t.Fatalf("dump files = %v, want 1", dumps)
			}
			dumped := dumpLines(t, dumps[0])
			if len(dumped)+written != accepted || accepted < 8 {
				t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted",
					len(dumped), written, accepted)
			}
			if rows := in.count(); rows != written {
				t.Errorf("the queue holds %d rows, the sink counted %d written", rows, written)
			}

			// The stuck writes return at last, successfully. Their chunks are
			// the shutdown's now: the workers settle nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writers next to the dump's reads.
			close(ss.release)
			released = true
			time.Sleep(200 * time.Millisecond)
		})
	}
}
