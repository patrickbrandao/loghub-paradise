package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// stuckWriteSink wraps the real sink=sqlite: from the second write on, Write
// never returns until release is closed, whatever its context says -- the
// shape of a COMMIT (or a rotation, or the os.Stat of updateSizeGauge) stuck
// in fsync on a hung volume, none of which takes a context (SPEC §10.4, review
// A1). With failHealth, Health fails as the real probe does for a writer that
// holds the database past StuckAfter, so the fuses take the sink to FATAL.
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
		return &sink.Error{Class: sink.ClassNetwork, Op: "heartbeat",
			Err: errors.New("the writer held the database past the probe's deadline (injected)")}
	}
	return s.Sink.Health(ctx)
}

func (s *stuckWriteSink) NeedsColumns() bool { return true }
func (s *stuckWriteSink) MaxWorkers() int    { return 1 }

// SPEC §13.4: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the chunk the stuck worker holds goes to the dump with the rest of the
// ring, and the process exits 3 -- on the FATAL path (the A1 fuse of §10.4)
// and on a SIGTERM's. With a persistent ring nothing is dumped: the rows stay
// for the next boot. Before 2026-11-23 the shutdown waited in Workers.Wait for
// good: no dump, no exit, until the SIGKILL took the ring with nothing
// counting it (review of 2026-11-21, item 016).
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ring  string
		fatal bool
		dump  bool
		code  int
	}{
		{"fatal, ring=memory", "memory", true, true, ExitSink},
		{"sigterm, ring=memory", "memory", false, true, ExitSink},
		{"fatal, ring=sqlite", "sqlite", true, false, ExitSink},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := launch(t, testID, append(baseArgs(dir),
				"--listen.udp=", "--ring.mode="+tc.ring, "--sink.max_retries=2", "--sink.fail_timeout=1s",
				"--sink.batch=4", "--sink.op_timeout=1s", "--shutdown.drain_timeout=1s"),
				func(s sink.Sink) sink.Sink { ss.Sink = s; return ss })
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(ss.release) }) }
			// Also from a Cleanup registered after launch's, so it runs first: a test
			// that fails early still lets the stuck write go, the instance exits, and its
			// own cleanup removes what it made at the destination.
			t.Cleanup(release)
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
			if code != tc.code {
				t.Fatalf("exit %d after %s, want %d", code, took.Round(time.Millisecond), tc.code)
			}
			if took > 15*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}

			accepted := int(in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load())
			written := int(in.app.metrics.EventsWrittenTotal.Load())
			dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
			if !tc.dump {
				if len(dumps) != 0 {
					t.Errorf("dumps %v with a persistent ring, want none: the rows stay for the next boot", dumps)
				}
			} else {
				if len(dumps) != 1 {
					t.Fatalf("dump files = %v, want 1", dumps)
				}
				res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(dumps[0])
				if err != nil || len(res.Items)+written != accepted || accepted < 8 {
					t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted (err %v)",
						len(res.Items), written, accepted, err)
				}
			}

			// The stuck write returns at last, successfully. Its chunk is the
			// shutdown's now: the worker settles nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writer next to the dump's reads.
			release()
			time.Sleep(200 * time.Millisecond)
		})
	}
}
