package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ring"
)

// flakyRing fails Dequeue with a storage error a few times, the way ring=sqlite
// surfaces SQLITE_BUSY from its select, then behaves.
type flakyRing struct {
	ring.Ring
	failures atomic.Int64
	err      error
}

func (r *flakyRing) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	if r.failures.Add(-1) >= 0 {
		return nil, r.err
	}
	return r.Ring.Dequeue(ctx, n)
}

// SPEC §8.1 step 1 (port of go-syslog-server's test, 2026-10-14): a
// Dequeue error that is not one of the ring's ends -- a busy or failing
// ring.db -- must not end the worker. It is waited out with backoff and the
// worker keeps draining, before and after.
func TestWorkersSurviveTransientDequeueErrors(t *testing.T) {
	fr := &flakyRing{Ring: ring.NewMemory(1000, 0, time.Millisecond),
		err: fmt.Errorf("ring sqlite: select: %w", errors.New("database is locked (5) (SQLITE_BUSY)"))}
	fr.failures.Store(6)
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(fr, ss, h, WorkerConfig{Workers: 2, Batch: 8, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond}, quietLog(), m)

	enqueue := func(n int) {
		for _, it := range mkBatch(n) {
			if err := fr.Enqueue(context.Background(), it); err != nil {
				t.Fatal(err)
			}
		}
	}
	enqueue(5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	if !eventually(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 5 }) {
		t.Fatalf("delivered %d of 5: the workers died on a transient Dequeue error", m.EventsWrittenTotal.Load())
	}
	if fr.failures.Load() >= 0 {
		t.Fatal("the failures were never hit")
	}
	// Still alive: a second wave is drained too.
	enqueue(5)
	if !eventually(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 10 }) {
		t.Fatalf("delivered %d of 10 after the failures", m.EventsWrittenTotal.Load())
	}
	cancel()
	w.Wait()
}

// The three ends of a worker stay ends: shutdown, a closed ring, and a ring
// that never holds a backlog (ring=none).
func TestWorkersStopOnTheEndsOfTheRing(t *testing.T) {
	for _, end := range []error{ring.ErrClosed, ring.ErrNoBacklog, fmt.Errorf("wrapped: %w", ring.ErrClosed)} {
		fr := &flakyRing{Ring: ring.NewMemory(10, 0, 0), err: end}
		fr.failures.Store(1 << 30)
		ss := &scriptedSink{healthy: true}
		m := testMetrics()
		h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
		w := NewWorkers(fr, ss, h, WorkerConfig{Workers: 2, Batch: 4, OpTimeout: time.Second,
			BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, quietLog(), m)
		w.Start(context.Background())
		done := make(chan struct{})
		go func() { w.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%v did not end the workers", end)
		}
	}
	// And the context, with a ring that would otherwise keep failing.
	fr := &flakyRing{Ring: ring.NewMemory(10, 0, 0), err: errors.New("disk I/O error")}
	fr.failures.Store(1 << 30)
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(fr, ss, h, WorkerConfig{Workers: 2, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, quietLog(), m)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	cancel()
	done := make(chan struct{})
	go func() { w.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled context did not end a worker that was retrying Dequeue")
	}
}

// sizesSink records how many items and bytes each Write carried, and can fail
// a number of writes first.
type sizesSink struct {
	mu     sync.Mutex
	writes [][2]int // items, bytes
	failN  int
	err    error
}

func (s *sizesSink) Write(_ context.Context, items []*envelope.Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failN > 0 {
		s.failN--
		return s.err
	}
	b := 0
	for _, it := range items {
		b += len(it.Env)
	}
	s.writes = append(s.writes, [2]int{len(items), b})
	return nil
}
func (s *sizesSink) Health(context.Context) error { return nil }
func (s *sizesSink) Close() error                 { return nil }
func (s *sizesSink) Type() string                 { return "sizes" }

func (s *sizesSink) got() [][2]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]int(nil), s.writes...)
}

// SPEC §8.1 step 2 (user's decision, 2026-10-14): a dequeued
// batch is written in pieces of at most sink.batch_max_bytes of JLH. Four
// envelopes of the same size under a ceiling of two go out as two writes of
// two, in order, and each piece is committed on its own.
func TestWorkersSplitABatchByBytes(t *testing.T) {
	items := []*envelope.Item{sizedItem(1 << 20), sizedItem(1 << 20), sizedItem(1 << 20), sizedItem(1 << 20)}
	// The envelopes differ by a byte or two (the event's clock sample), so
	// the ceiling is two of the largest: never three of any.
	one := 0
	for _, it := range items {
		one = max(one, len(it.Env))
	}
	r := ring.NewMemory(100, 1<<30, 0)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	ss := &sizesSink{}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{Workers: 1, Batch: 4, BatchMaxBytes: int64(2 * one),
		OpTimeout: time.Second, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, quietLog(), m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	if !eventually(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 4 }) {
		t.Fatalf("delivered %d of 4", m.EventsWrittenTotal.Load())
	}
	got := ss.got()
	if len(got) != 2 || got[0][0] != 2 || got[1][0] != 2 || got[0][1] > 2*one || got[1][1] > 2*one {
		t.Fatalf("writes = %v, want two of 2 items / at most %d bytes", got, 2*one)
	}
	if !eventually(t, time.Second, func() bool { return r.Stats().Items == 0 && r.Stats().Bytes == 0 }) {
		t.Fatalf("ring after the pieces = %+v, want empty", r.Stats())
	}
	cancel()
	w.Wait()
}

// An envelope larger than the ceiling is never split nor refused: it goes
// alone, and the ones around it keep their own pieces.
func TestWorkersSendAnEnvelopeAboveTheCeilingAlone(t *testing.T) {
	small, big := sizedItem(10), sizedItem(64<<10)
	items := []*envelope.Item{sizedItem(10), big, small}
	bigLen := len(big.Env) // the write releases it
	r := ring.NewMemory(100, 1<<30, 0)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	ss := &sizesSink{}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{Workers: 1, Batch: 3, BatchMaxBytes: 4 << 10,
		OpTimeout: time.Second, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, quietLog(), m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	if !eventually(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 3 }) {
		t.Fatalf("delivered %d of 3", m.EventsWrittenTotal.Load())
	}
	got := ss.got()
	if len(got) != 3 || got[1] != [2]int{1, bigLen} {
		t.Fatalf("writes = %v, want three, the large envelope alone in the second", got)
	}
	cancel()
	w.Wait()
}

// SPEC §8.1 step 3, §9: while a piece is retried, the rest of its batch is
// still the worker's -- out of the ring and not yet written -- and must be in
// Pending for the emergency dump, with the delivered pieces out of it.
func TestWorkersHoldTheRestOfTheBatchWhileAPieceRetries(t *testing.T) {
	// Three pieces, and the MIDDLE one fails: what the worker holds must be
	// that piece and the one after it, never written. With the failing piece
	// the last one (the first form of this test), a hold per piece -- what
	// SPEC §8.1 step 3 forbids -- returned the same Pending and passed
	// (independent review of 2026-10-14, T2). The same with a fatal answer,
	// which abandons the batch at once.
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"network until the shutdown", errors.New("connection refused")},
		{"fatal", &Error{Class: ClassFatal, Op: "test", Err: errors.New("gone for good")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := mkBatch(6)
			one := 0
			for _, it := range items {
				one = max(one, len(it.Env))
			}
			r := ring.NewMemory(100, 1<<30, 0)
			for _, it := range items {
				if err := r.Enqueue(context.Background(), it); err != nil {
					t.Fatal(err)
				}
			}
			got, err := r.Dequeue(context.Background(), 6)
			if err != nil || len(got) != 6 {
				t.Fatalf("Dequeue = %d %v", len(got), err)
			}

			// The first piece goes through; the second fails until the
			// shutdown, or for good.
			ss := &switchSink{fail: tc.err}
			m := testMetrics()
			h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
			w := NewWorkers(r, ss, h, WorkerConfig{Workers: 1, Batch: 6, BatchMaxBytes: int64(2 * one),
				OpTimeout: time.Second, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, quietLog(), m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ss.afterFirst = func() {
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
			}
			if w.deliver(ctx, got, 0) {
				t.Fatal("deliver reported the batch settled while its second piece never was")
			}
			left := w.Pending()
			if len(left) != 4 || left[0] != got[2] || left[1] != got[3] || left[2] != got[4] || left[3] != got[5] {
				t.Fatalf("Pending = %d items, want the failing piece and the one after it (items 3 to 6)", len(left))
			}
			if s := r.Stats(); s.Items != 4 {
				t.Fatalf("ring = %+v, want the first piece committed and the other four still counted", s)
			}
			if n := ss.n.Load(); tc.name == "fatal" && n != 2 {
				t.Fatalf("%d writes after a fatal answer, want 2: the third piece is never tried", n)
			}
		})
	}
}

// switchSink accepts its first write and fails every later one with fail
// (as network when nil).
type switchSink struct {
	n          atomic.Int64
	afterFirst func()
	fail       error
}

func (s *switchSink) Write(context.Context, []*envelope.Item) error {
	if s.n.Add(1) == 1 {
		s.afterFirst()
		return nil
	}
	if s.fail != nil {
		return s.fail
	}
	return errors.New("connection refused")
}
func (s *switchSink) Health(context.Context) error { return nil }
func (s *switchSink) Close() error                 { return nil }
func (s *switchSink) Type() string                 { return "switch" }

// SplitByBytes is the one rule every batch writer shares (SPEC §8.1 step 2):
// the workers, the dump's reingestion and the ring.db drain.
func TestSplitByBytes(t *testing.T) {
	mk := func(sizes ...int) []*envelope.Item {
		out := make([]*envelope.Item, len(sizes))
		for i, n := range sizes {
			out[i] = envelope.NewItem(fmt.Sprint(i), make([]byte, n), -1)
		}
		return out
	}
	for _, tc := range []struct {
		name  string
		sizes []int
		max   int64
		want  []int // piece lengths
	}{
		{"empty", nil, 10, nil},
		{"all fit", []int{3, 3, 3}, 10, []int{3}},
		{"exactly the ceiling", []int{5, 5, 5}, 10, []int{2, 1}},
		{"one byte over", []int{5, 6}, 10, []int{1, 1}},
		{"large alone at the head", []int{20, 1, 1}, 10, []int{1, 2}},
		{"large alone in the middle", []int{1, 20, 1}, 10, []int{1, 1, 1}},
		{"no ceiling", []int{100, 100}, 0, []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []int
			rest := mk(tc.sizes...)
			for len(rest) > 0 {
				n := SplitByBytes(rest, tc.max)
				if n < 1 || n > len(rest) {
					t.Fatalf("SplitByBytes = %d of %d", n, len(rest))
				}
				got = append(got, n)
				rest = rest[n:]
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("pieces = %v, want %v", got, tc.want)
			}
		})
	}
}
