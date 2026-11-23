package sink

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

// ignoringSink's Write ignores its context until release is closed, then
// answers with answer. No write of this server does that today -- pgx arms
// the socket's deadline from the context -- but a driver bug or a future
// destination would: a COMMIT stuck in fsync, a publish on a socket without a
// deadline (the NATS one of go-webhook-nats-server).
type ignoringSink struct {
	entered chan struct{}
	once    sync.Once
	release chan struct{}
	answer  error
	done    atomic.Int64
}

func (s *ignoringSink) Write(context.Context, []*envelope.Item) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	s.done.Add(1)
	return s.answer
}
func (s *ignoringSink) Health(context.Context) error { return nil }
func (s *ignoringSink) Close() error                 { return nil }
func (s *ignoringSink) Type() string                 { return "ignoring" }

// commitCountingRing counts the Commits that reach the ring.
type commitCountingRing struct {
	ring.Ring
	commits atomic.Int64
}

func (r *commitCountingRing) Commit(items []*envelope.Item) error {
	r.commits.Add(1)
	return r.Ring.Commit(items)
}

// SPEC §10.3: once WaitFor gave up and Pending handed the batch of a stuck
// worker to the dump, that worker settles nothing when its write finally
// returns -- delivered or refused for good: no Commit, no Release (which would
// pool the buffers the dump is reading), no drop counted (the reingestion
// meets the refusal again) -- and it stops instead of taking more from the
// ring (review of 2026-11-21, item 016).
func TestWorkersSettleNothingOnceTheShutdownTookTheirBatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer error
	}{
		{"delivered", nil},
		{"refused", &Error{Class: ClassLogic, Op: "write", Err: errors.New("refused")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := ring.NewMemory(100, 1<<30, 0)
			r := &commitCountingRing{Ring: mem}
			var items []*envelope.Item
			for i := 0; i < 5; i++ {
				it := sizedItem(64)
				items = append(items, it)
				if err := mem.Enqueue(context.Background(), it); err != nil {
					t.Fatal(err)
				}
			}
			s := &ignoringSink{entered: make(chan struct{}), release: make(chan struct{}), answer: tc.answer}
			m := testMetrics()
			h := NewHealth(&scriptedSink{healthy: true}, HealthConfig{}, quietLog(), m)
			w := NewWorkers(r, s, h, WorkerConfig{Workers: 1, Batch: 3, OpTimeout: 100 * time.Millisecond,
				BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}, quietLog(), m)
			ctx, stop := context.WithCancel(context.Background())
			w.Start(ctx)
			select {
			case <-s.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the worker never wrote")
			}

			stop()
			if w.WaitFor(100 * time.Millisecond) {
				t.Fatal("WaitFor reported a worker stuck in a write that ignores its context as returned")
			}
			held := w.Pending()
			if len(held) != 3 {
				t.Fatalf("Pending gave %d items, want the 3 of the stuck batch", len(held))
			}
			queued := mem.Drain()

			close(s.release)
			if !w.WaitFor(5 * time.Second) {
				t.Fatal("the worker did not return after its write did")
			}
			if n := r.commits.Load(); n != 0 {
				t.Errorf("%d Commits after the shutdown took the batch, want 0", n)
			}
			for i, it := range held {
				if it.Released() {
					t.Errorf("held item %d was released: its buffer went back to the pool under the dump", i)
				}
			}
			if n := s.done.Load(); n != 1 {
				t.Errorf("%d writes, want 1: the worker went on after the shutdown took its batch", n)
			}
			if got := len(held) + len(queued); got != len(items) {
				t.Errorf("held %d + queued %d, want the %d items", len(held), len(queued), len(items))
			}
			if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 0 {
				t.Errorf("%d events counted dropped for a batch the dump carries", got)
			}
		})
	}
}
