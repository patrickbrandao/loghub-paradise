package ring

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// None is the unbuffered mode (SPEC §9.6): the receiver goroutine converts the
// item, builds the JLH and performs the sink write synchronously, and only then
// confirms (SPEC §2.3):
//
//	with sink=redis     the XADD was accepted by Redis
//	with sink=sqlite    the COMMIT on events.db finished
//	with sink=postgres  the server confirmed the INSERT, with whatever
//	or sink=mysql       durability IT is configured to give
//	with sink=nats      the stream returned a PubAck, with whatever
//	                    durability the STREAM is configured to give
//	with sink=none      nothing was written
//
// It is also the one mode where the conversion runs on the receive path,
// because here the receiver is the writer: there is no worker to hand the work
// to (SPEC §2.2). That is acceptable precisely because this is the mode that is
// slow by definition.
//
// Throughput stays reasonable with sink=redis because rueidis coalesces the
// concurrent calls of many goroutines into a single write syscall; there is no
// round trip per message. The same holds for sink=nats, whose client serialises
// every goroutine's publish into one outbound buffer. With sink=postgres or
// sink=mysql there is no coalescing at all: throughput is pool_size divided by
// the commit latency, and a max_inflight above the pool only builds a hidden
// queue (the boot warns, SPEC §9.6).
//
// This type holds a Writer -- an interface declared in this package -- and never
// learns which destination is behind it (invariant §20.15). The writer the app
// hands in is what converts and serialises; the ring only bounds and times it.
type None struct {
	sink Writer

	// opTimeout bounds each synchronous write (SPEC §9.6). Zero means no bound.
	opTimeout time.Duration

	// inflight bounds concurrent writes. Without it a slow destination would
	// let the number of blocked goroutines grow without limit, each holding a
	// raw line -- the exact memory blow-up the ring exists to prevent. A full
	// semaphore is ErrFull, keeping "full" identical across every mode.
	//
	// It is nil when the app asked for no semaphore, which SPEC §9.6 does with
	// sink=none: a write that costs nothing needs no bound, and the benchmark of
	// transport that ring=none + sink=none exists for would otherwise measure
	// the semaphore.
	inflight chan struct{}
	capacity int64

	cur    atomic.Int64
	closed atomic.Bool
}

// NewNone builds the unbuffered ring. maxInflight is ring.max_inflight; zero or
// less disables the semaphore, which the app does with sink=none (SPEC §9.6) --
// the decision is taken there, because this package must not know the sink's
// type. opTimeout is sink.op_timeout.
func NewNone(s Writer, maxInflight int, opTimeout time.Duration) *None {
	r := &None{sink: s, opTimeout: opTimeout}
	if maxInflight > 0 {
		r.inflight = make(chan struct{}, maxInflight)
		r.capacity = int64(maxInflight)
	}
	return r
}

func (r *None) Mode() string { return "none" }

// Enqueue writes straight through to the sink and waits for the answer.
func (r *None) Enqueue(ctx context.Context, it *item.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}

	if r.inflight != nil {
		select {
		case r.inflight <- struct{}{}:
		default:
			// Never block: a queued receiver would be a hidden buffer with none
			// of the ring's accounting.
			return ErrFull
		}
	}
	r.cur.Add(1)
	defer func() {
		r.cur.Add(-1)
		if r.inflight != nil {
			<-r.inflight
		}
	}()

	// The write runs on a context detached from the receiver's cancellation
	// and bounded by sink.op_timeout instead (SPEC §9.6). A connection that
	// drops mid-write, or the shutdown cancelling the receive context
	// (SPEC §13.4 phase 3), must not abort the command: the destination's
	// client may have queued it already and would send it anyway, while the
	// receiver, told "cancelled", would be free to pool the buffer the pipeline
	// is about to serialise. Detached, the write completes or times out on its
	// own clock, and the timeout is one of the outcomes the receiver has to
	// treat as "still referenced": it counts as a full ring for the transport
	// (retryable), but the buffer is discarded, never pooled (SPEC §15.2).
	wctx := context.WithoutCancel(ctx)
	if r.opTimeout > 0 {
		var cancel context.CancelFunc
		wctx, cancel = context.WithTimeout(wctx, r.opTimeout)
		defer cancel()
	}

	batch := [1]*item.Item{it}
	if err := r.sink.Write(wctx, batch[:]); err != nil {
		// The item was never accepted. Whether the receiver may pool its
		// buffer depends on WHICH error this is, and is decided there
		// (SPEC §15.2): the error is returned unwrapped so that decision can
		// see it.
		return err
	}
	// The synchronous write returned success: the destination consumed the
	// buffer in full, so ownership moves on exactly as it does after a
	// worker's confirmed batch, and it goes back to the pool here
	// (SPEC §15.2). The receiver touches nothing of the item afterwards.
	it.Release()
	return nil
}

// Dequeue reports immediately that there is no backlog to take.
//
// Blocking here would be wrong in two ways: the drain workers would sit on a
// channel forever, and the shutdown drain of SPEC §13.4 would wait out its
// whole drain_timeout before discovering there was never anything to drain.
// Returning at once makes the workers exit and shutdown proceed straight to the
// next phase, with no call site needing to know the mode.
func (r *None) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	return nil, ErrNoBacklog
}

// Commit is unreachable in this mode, since Dequeue never yields items.
func (r *None) Commit(items []*item.Item) error { return nil }

// Touch is a no-op: nothing is ever held for a retry in this mode.
func (r *None) Touch([]*item.Item) error { return nil }

// Drain returns nothing: an item is either still owned by a live receiver or
// already delivered, so there is never a backlog to dump (SPEC §12).
func (r *None) Drain() []*item.Item { return nil }

// Stats reports in-flight writes against the semaphore (SPEC §9.6: items = in
// flight, capItems = max_inflight), so /readyz and the relplog_ring_* gauges
// work in this mode with no special case. Without a semaphore CapItems is 0,
// which Saturated reads as "no ceiling".
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
