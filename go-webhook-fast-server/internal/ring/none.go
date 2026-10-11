package ring

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// None is the unbuffered mode (SPEC §7.5): the handler performs the sink write
// synchronously and only then answers 200.
//
//	with sink=redis   200 means the XADD was confirmed by Redis
//	with sink=sqlite  200 means the COMMIT on events.db finished
//	with sink=none    200 is immediate and nothing was written
//
// Throughput stays high with sink=redis because rueidis coalesces the
// concurrent calls of many goroutines into a single write syscall; there is no
// round trip per request.
//
// This type holds a Writer -- an interface declared in this package -- and never
// learns which destination is behind it.
type None struct {
	sink Writer

	// opTimeout bounds each synchronous write (SPEC §7.5). Zero means no bound.
	opTimeout time.Duration

	// inflight bounds concurrent writes. Without it a slow destination would
	// let the number of blocked goroutines grow without limit, each holding a
	// body of up to max_body_bytes -- the exact memory blow-up the ring exists
	// to prevent. A full semaphore is ErrFull, keeping "full means 429"
	// identical across all three modes.
	inflight chan struct{}
	capacity int64

	cur    atomic.Int64
	closed atomic.Bool
}

// NewNone builds the unbuffered ring. maxInflight must be at least 1; opTimeout
// is sink.op_timeout.
func NewNone(s Writer, maxInflight int, opTimeout time.Duration) *None {
	if maxInflight < 1 {
		maxInflight = 1
	}
	return &None{
		sink:      s,
		opTimeout: opTimeout,
		inflight:  make(chan struct{}, maxInflight),
		capacity:  int64(maxInflight),
	}
}

func (r *None) Mode() string { return "none" }

// Enqueue writes straight through to the sink and waits for the answer.
func (r *None) Enqueue(ctx context.Context, it *envelope.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}

	select {
	case r.inflight <- struct{}{}:
	default:
		// Never block: a queued handler would be a hidden buffer with none of
		// the ring's accounting.
		return ErrFull
	}
	r.cur.Add(1)
	defer func() {
		r.cur.Add(-1)
		<-r.inflight
	}()

	// The write runs on a context detached from the caller's cancellation and
	// bounded by sink.op_timeout instead (SPEC §7.5). A client that hangs up
	// mid-write must not abort the command: rueidis may have queued it already
	// and would send it anyway, while the handler, told "cancelled", would be
	// free to pool the buffers the pipeline is about to serialise. Detached,
	// the write completes or times out on its own clock, and the timeout is
	// the one outcome the caller has to treat as "still referenced"
	// (SPEC §12.3).
	wctx := context.WithoutCancel(ctx)
	if r.opTimeout > 0 {
		var cancel context.CancelFunc
		wctx, cancel = context.WithTimeout(wctx, r.opTimeout)
		defer cancel()
	}

	batch := [1]*envelope.Item{it}
	if err := r.sink.Write(wctx, batch[:]); err != nil {
		// The item was never accepted. Whether the handler may pool its
		// buffers depends on WHICH error this is, and is decided there
		// (SPEC §12.3).
		return err
	}
	// The synchronous write returned success: the destination consumed the
	// buffers in full, so ownership moves on exactly as it does after a
	// worker's confirmed batch, and they go back to the pool here
	// (SPEC §12.3). The handler reads nothing but lengths after this point.
	it.Release()
	return nil
}

// Dequeue reports immediately that there is no backlog to take.
//
// Blocking here would be wrong in two ways: the drain workers would sit on a
// channel forever, and the shutdown drain of SPEC §10.3 would wait out its
// whole drain_timeout before discovering there was never anything to drain.
// Returning at once makes the workers exit and shutdown proceed straight to the
// next phase, with no call site needing to know the mode.
func (r *None) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	return nil, ErrNoBacklog
}

// Commit is unreachable in this mode, since Dequeue never yields items.
func (r *None) Commit(items []*envelope.Item) error { return nil }

// Drain returns nothing: an item is either still owned by a live handler or
// already delivered, so there is never a backlog to dump (SPEC §9).
func (r *None) Drain() []*envelope.Item { return nil }

// Stats reports in-flight writes against the semaphore, so /readyz and the
// whs_ring_* gauges work in this mode with no special case.
func (r *None) Stats() Stats {
	return Stats{
		Items:    r.cur.Load(),
		Bytes:    0,
		CapItems: r.capacity,
		CapBytes: 0,
	}
}

func (r *None) Close() error {
	r.closed.Store(true)
	return nil
}
