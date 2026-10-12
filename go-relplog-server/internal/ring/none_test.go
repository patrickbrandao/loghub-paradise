package ring

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

type fakeWriter struct {
	mu      sync.Mutex
	written int
	err     error
	release chan struct{}
}

func (w *fakeWriter) Write(ctx context.Context, items []*item.Item) error {
	if w.release != nil {
		select {
		case <-w.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.written += len(items)
	return nil
}

func (w *fakeWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

type probingWriter struct {
	probe func(ctx context.Context) error
}

func (w *probingWriter) Write(ctx context.Context, items []*item.Item) error {
	return w.probe(ctx)
}

func TestNoneWritesThrough(t *testing.T) {
	w := &fakeWriter{}
	r := NewNone(w, 4, time.Second)
	for i := 0; i < 3; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	if w.count() != 3 {
		t.Fatalf("written = %d", w.count())
	}
	if s := r.Stats(); s.Items != 0 || s.CapItems != 4 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestNoneSurfacesSinkError(t *testing.T) {
	want := errors.New("destination refused")
	r := NewNone(&fakeWriter{err: want}, 2, time.Second)
	if err := r.Enqueue(context.Background(), mkItem(0, 10)); !errors.Is(err, want) {
		t.Fatalf("err = %v", err)
	}
}

// The semaphore is the only backpressure this mode has. Without it a slow
// destination lets blocked goroutines grow without limit, each holding a line.
func TestNoneSemaphoreIsFull(t *testing.T) {
	w := &fakeWriter{release: make(chan struct{})}
	r := NewNone(w, 2, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _ = r.Enqueue(context.Background(), mkItem(i, 10)) }(i)
	}
	// Wait for both slots to be taken.
	deadline := time.Now().Add(2 * time.Second)
	for r.Stats().Items < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := r.Stats().Items; got != 2 {
		t.Fatalf("in-flight = %d, want 2", got)
	}
	if err := r.Enqueue(context.Background(), mkItem(9, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	close(w.release)
	wg.Wait()
}

// SPEC §9.6: with sink=none the semaphore is dispensed. The app expresses that
// as maxInflight = 0, because the ring must not know the sink's type; Stats
// then reports no ceiling, which never reads as saturated.
func TestNoneWithoutSemaphoreNeverFills(t *testing.T) {
	w := &fakeWriter{release: make(chan struct{})}
	r := NewNone(w, 0, 5*time.Second)

	const n = 64
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if r.Enqueue(context.Background(), mkItem(i, 10)) != nil {
				errs.Add(1)
			}
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for r.Stats().Items < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := r.Stats(); s.Items != n || s.CapItems != 0 || s.Saturated(0.8) {
		t.Fatalf("stats = %+v, want %d in flight and no ceiling", s, n)
	}
	close(w.release)
	wg.Wait()
	if errs.Load() != 0 || w.count() != n {
		t.Fatalf("errors = %d, written = %d", errs.Load(), w.count())
	}
}

// Blocking here would make every drain worker sit forever and would make the
// shutdown drain wait out its whole timeout before discovering there is nothing
// to drain.
func TestNoneDequeueReturnsAtOnce(t *testing.T) {
	r := NewNone(&fakeWriter{}, 2, time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := r.Dequeue(context.Background(), 10); !errors.Is(err, ErrNoBacklog) {
			t.Errorf("err = %v, want ErrNoBacklog", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dequeue blocked")
	}
	if len(r.Drain()) != 0 {
		t.Fatal("Drain returned items")
	}
	if r.Touch([]*item.Item{mkItem(0, 1)}) != nil || r.Commit(nil) != nil {
		t.Fatal("Touch and Commit are no-ops in this mode")
	}
}

// SPEC §9.6: the write is bounded by sink.op_timeout, not by the receiver. A
// receive context that is already cancelled must still let the write run.
func TestNoneWriteIsDetachedFromTheCallerContext(t *testing.T) {
	var sawLive atomic.Bool
	w := &probingWriter{probe: func(ctx context.Context) error {
		sawLive.Store(ctx.Err() == nil)
		return nil
	}}
	r := NewNone(w, 4, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Enqueue(ctx, mkItem(0, 10)); err != nil {
		t.Fatalf("Enqueue with a cancelled receive context: %v", err)
	}
	if !sawLive.Load() {
		t.Fatal("the sink saw a cancelled context; the write must be detached from the receiver")
	}
}

// ... and the timeout is the sink's own clock: a write that never answers ends
// in DeadlineExceeded after op_timeout, which is the one error the receiver
// must treat as "buffer still referenced" (SPEC §15.2).
func TestNoneAppliesOpTimeout(t *testing.T) {
	w := &probingWriter{probe: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	r := NewNone(w, 4, 30*time.Millisecond)

	start := time.Now()
	err := r.Enqueue(context.Background(), mkItem(0, 10))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded from op_timeout", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("op_timeout was not applied")
	}
	if r.Stats().Items != 0 {
		t.Fatal("a timed-out write left its semaphore slot taken")
	}
}

func TestNoneClosedRefuses(t *testing.T) {
	r := NewNone(&fakeWriter{}, 2, time.Second)
	r.Close()
	if err := r.Enqueue(context.Background(), mkItem(0, 10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}
