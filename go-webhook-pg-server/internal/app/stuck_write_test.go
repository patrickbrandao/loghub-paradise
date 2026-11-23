package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
)

// stuckSink is a destination whose Write ignores its context until release is
// closed. pgx never does -- it arms the socket's deadline from the context --
// so this stands for a driver bug or a future destination, as the NATS
// publish without a socket deadline was in go-webhook-nats-server (review of
// 2026-11-21, item 016). With fatalOnStuck its Health turns FATAL once a
// write is stuck. A write that is finally released succeeds and keeps a copy
// of its envelopes, as memSink does.
type stuckSink struct {
	*memSink
	fatalOnStuck bool

	entered chan struct{} // closed when the first write gets stuck
	once    sync.Once
	stuck   atomic.Bool
	release chan struct{}
	late    atomic.Int64 // envelopes the released writes stored
}

func newStuckSink(fatalOnStuck bool) *stuckSink {
	return &stuckSink{memSink: &memSink{}, fatalOnStuck: fatalOnStuck,
		entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *stuckSink) Write(_ context.Context, items []*envelope.Item) error {
	s.stuck.Store(true)
	s.once.Do(func() { close(s.entered) })
	<-s.release // the context is ignored on purpose
	s.memSink.mu.Lock()
	for _, it := range items {
		s.memSink.envs = append(s.memSink.envs, bytes.Clone(it.Env))
	}
	s.memSink.mu.Unlock()
	s.late.Add(int64(len(items)))
	return nil
}

func (s *stuckSink) Health(ctx context.Context) error {
	if s.fatalOnStuck && s.stuck.Load() {
		return &sink.Error{Class: sink.ClassFatal, Op: "stuck", Err: errors.New("a write never returned")}
	}
	return s.memSink.Health(ctx)
}

// SPEC §10.3: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the batches they hold go to the dump with the rest of the ring, and the
// process exits 3. Before 2026-11-23 the shutdown waited in Workers.Wait for
// good: no dump, no exit, until the orchestrator's SIGKILL took everything
// answered 200, with nothing counting it (review of 2026-11-21, item 016).
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fatal bool // the FATAL shutdown; otherwise a SIGTERM's
	}{{"fatal", true}, {"sigterm", false}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ss := newStuckSink(tc.fatal)
			in := startHooked(t, dir, ss.memSink, func(a *App) { a.sinkOverride = ss },
				"--sink.workers=2", "--sink.batch=4", "--sink.op_timeout=500ms",
				"--sink.fail_timeout=1s", "--sink.max_retries=2", "--shutdown.drain_timeout=1s")
			ids := in.postN(20)
			if len(ids) != 20 {
				t.Fatalf("%d of 20 accepted", len(ids))
			}
			select {
			case <-ss.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("no write reached the destination")
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
				t.Fatalf("exit %d after %s, want %d with the dump", code, took.Round(time.Millisecond), ExitSink)
			}
			// drain_timeout (1s) + sink.op_timeout (0.5s), plus the health
			// fuses on the FATAL path, and room for -race.
			if took > 10*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}
			if got := ss.memSink.count(); got != 0 {
				t.Fatalf("the destination confirmed %d events before the exit; the test needs none", got)
			}
			checkDump(t, dir, testID, envelope.ModeFast, ids)

			// The stuck writes return at last, successfully. Their batches
			// are the dump's now: the workers settle nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writers next to the dump's reads.
			close(ss.release)
			if !waitFor(5*time.Second, func() bool { return ss.late.Load() > 0 }) {
				t.Fatal("the released writes never returned")
			}
		})
	}
}
