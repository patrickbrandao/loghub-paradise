package app

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/sink"
)

// stuckWriteSink wraps the real sink=sqlite: from the second write on, Write
// never returns until release is closed, whatever its context says -- the
// shape of a COMMIT (or a rotation, or the os.Stat of the size gauge) stuck in
// fsync on a hung volume, none of which takes a context (SPEC §8.3.5; review
// of 2026-11-21, item 016). With failHealth, Health fails as the real probe
// does for a writer that holds the database past sink.op_timeout +
// sink.health_timeout, so the fuses take the sink to FATAL. A released write
// succeeds without reaching the database, and reads its envelopes on the way
// out.
type stuckWriteSink struct {
	sink.Sink
	failHealth bool
	writes     atomic.Int64
	stuck      atomic.Bool
	release    chan struct{}
	late       atomic.Int64 // envelopes the released writes read
}

func (s *stuckWriteSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.writes.Add(1) >= 2 {
		s.stuck.Store(true)
		<-s.release // the context is ignored on purpose
		for _, it := range items {
			_ = bytes.Clone(it.Env)
		}
		s.late.Add(int64(len(items)))
		return nil
	}
	return s.Sink.Write(ctx, items)
}

func (s *stuckWriteSink) Health(ctx context.Context) error {
	if s.failHealth && s.stuck.Load() {
		return &sink.Error{Class: sink.ClassNetwork, Op: "heartbeat",
			Err: errors.New("the writer held the database past the probe's deadline (injected)")}
	}
	return s.Sink.Health(ctx)
}

// SPEC §10.3: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the batch the stuck worker holds goes to the dump with the rest of the ring,
// and the process exits 3 -- on the FATAL path (the fuses of §10.2 over the
// probe of §8.3.5) and on a SIGTERM's. With ring=sqlite nothing is dumped:
// the rows stay in ring.db for the next boot. Before 2026-11-23 the shutdown
// waited in Workers.Wait for good: no dump, no exit, until the orchestrator's
// SIGKILL took everything answered 200, with nothing counting it (review of
// 2026-11-21, item 016).
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ring  string
		fatal bool // the FATAL shutdown; otherwise a SIGTERM's
	}{
		{"fatal, ring=memory", "memory", true},
		{"sigterm, ring=memory", "memory", false},
		{"fatal, ring=sqlite", "sqlite", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := startHooked(t, dir, testID, func(a *App) {
				a.wrapSink = func(s sink.Sink) sink.Sink { ss.Sink = s; return ss }
			}, append([]string{"--ring.mode=" + tc.ring, "--sink.batch=4", "--sink.op_timeout=500ms",
				"--sink.fail_timeout=1s", "--sink.max_retries=2", "--shutdown.drain_timeout=1s"},
				sqliteSink...)...)
			// Not necessarily all 20: once the sink is DOWN the public port
			// answers 503. What was accepted is what must not be lost.
			ids := in.postN(20)
			if !waitFor(5*time.Second, ss.stuck.Load) {
				t.Fatalf("no write got stuck (%d accepted)", len(ids))
			}

			start := time.Now()
			var code int
			if tc.fatal {
				code = in.waitExit(15 * time.Second)
			} else {
				code = in.stop()
			}
			took := time.Since(start)
			if code != ExitSink {
				t.Fatalf("exit %d after %s, want %d", code, took.Round(time.Millisecond), ExitSink)
			}
			// drain_timeout (1s) + sink.op_timeout (0.5s), plus the health
			// fuses on the FATAL path, and room for -race.
			if took > 10*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}

			written := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
			rest := missing(ids, written)
			if len(rest) == 0 || len(ids) < 2 {
				t.Fatalf("%d accepted, %d written: the test needs a stuck batch", len(ids), len(written))
			}
			t.Logf("exit %d after %s: %d accepted, %d written, %d held or queued",
				code, took.Round(time.Millisecond), len(ids), len(written), len(rest))
			if tc.ring == "sqlite" {
				if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 0 {
					t.Errorf("dumps %v with ring=sqlite, want none: the rows stay for the next boot", dumps)
				}
				if n := countRows(t, filepath.Join(dir, "ring.db"), "jlh_ring"); n != len(rest) {
					t.Errorf("%d rows left in ring.db, want the %d accepted and not written", n, len(rest))
				}
			} else {
				checkDump(t, dir, testID, envelope.ModeFast, rest)
			}

			// The stuck write returns at last, successfully. Its batch is the
			// shutdown's now: the worker settles nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writer next to the dump's reads.
			close(ss.release)
			if !waitFor(5*time.Second, func() bool { return ss.late.Load() > 0 }) {
				t.Fatal("the released write never returned")
			}
		})
	}
}
