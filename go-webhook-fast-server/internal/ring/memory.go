package ring

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// Memory is the default ring (SPEC §7.3): a buffered channel plus byte
// accounting.
//
// Enqueue returns as soon as the item is in the channel, which is what makes
// this the highest-throughput mode. The guarantee it buys is correspondingly
// narrow: the event is in THIS node's RAM. A SIGKILL, an OOM or a kernel panic
// loses it. SIGTERM does not (the shutdown drain), and neither does a fatal
// sink failure (the emergency dump).
type Memory struct {
	ch chan *envelope.Item

	items    atomic.Int64
	bytes    atomic.Int64
	capItems int64
	capBytes int64

	// batchDelay is how long Dequeue waits to fill a batch once it has one
	// item. Larger batches mean fewer round trips to the destination.
	batchDelay time.Duration

	closed atomic.Bool
	// sealMu makes Close a barrier: Enqueue holds it shared from the closed
	// check through the channel send, and Close takes it exclusively, so once
	// Close returns no item can still land in the channel. Without it, an
	// upload whose body finished after the shutdown's Drain passed the check,
	// was sent into a ring nobody would read again and was answered 200 --
	// lost, uncounted (SPEC §10.3). The send never blocks, so neither does
	// Close for longer than one send.
	sealMu sync.RWMutex
}

// ItemOverhead is what one queued item holds besides its JLH buffer, on
// 64-bit, rounded to malloc size classes:
//
//	 80  the envelope.Item struct (unsafe.Sizeof is 80 with Charged, measured
//	     2026-10-14): ID and Env headers, the pool class, the two atomic
//	     flags, the done pointer, RowID and Charged.
//	 48  the ID string's bytes: a canonical uuid is 36.
//	112  the done channel (hchan plus one buffered error) when the handler
//	     asked for one; counted always, because the ceiling must hold in the
//	     worst case, not the average.
//	  8  the item's slot in this ring's channel buffer.
//	---
//	248  rounded up to 256.
//
// TestItemOverheadCoversTheStruct fails if the struct outgrows the number.
const ItemOverhead = 256

// charge is what an item costs THIS ring in memory (SPEC §7.2): the whole
// buffer of the pool class the JLH was written into, not len(Env). The pool
// classes are 4x apart (SPEC §12.2), so a 2.4 KiB JLH in a 16 KiB buffer
// holds 16 KiB, and a ceiling that counted 2.4 let max_bytes worth of such
// events hold four to seven times max_bytes of RAM -- past GOMEMLIMIT and
// the container's limit (SPEC §12.4).
//
// Only the memory ring counts this way. ring=sqlite counts len(Env), which is
// what it STORES, so that the boot recount (SUM(nbytes)) agrees with the live
// accounting.
func charge(it *envelope.Item) int64 { return int64(cap(it.Env)) + ItemOverhead }

// NewMemory builds the ring. Both ceilings are enforced; whichever is reached
// first produces ErrFull.
func NewMemory(capItems int, capBytes int64, batchDelay time.Duration) *Memory {
	if capItems < 1 {
		capItems = 1
	}
	return &Memory{
		ch:         make(chan *envelope.Item, capItems),
		capItems:   int64(capItems),
		capBytes:   capBytes,
		batchDelay: batchDelay,
	}
}

func (r *Memory) Mode() string { return "memory" }

// Enqueue accepts the item or returns ErrFull at once.
//
// Both ceilings are claimed BEFORE the channel send and released if the send
// fails, so the accounting can never drift above what is actually held. Both
// matter: with small bodies the item count -- or, now that the retained
// buffer is what is charged, the byte count -- binds, and with 5 MiB bodies the
// byte count does. Enforcing only items would allow 65536 x 5 MiB = 320 GiB
// of theoretical exposure.
func (r *Memory) Enqueue(ctx context.Context, it *envelope.Item) error {
	r.sealMu.RLock()
	defer r.sealMu.RUnlock()
	if r.closed.Load() {
		return ErrClosed
	}

	// Both ceilings count the same population: accepted and not yet
	// committed, which includes the batches the workers hold while they retry
	// (SPEC §7.2). The channel alone only bounds what is still QUEUED: with a
	// batch held, a capacity-3 ring accepted a fourth event and reported 4/3,
	// up to workers x batch past capacity_items, while the byte ceiling,
	// counted until Commit, said otherwise. The channel buffer is capItems
	// long, so a send under this count never finds it full; the default
	// branch below is a guard, not a path.
	if r.items.Add(1) > r.capItems {
		r.items.Add(-1)
		return ErrFull
	}
	// Recorded in the item: Commit and Drain give back this number, never a
	// recomputation from a buffer that Release may already have emptied.
	size := charge(it)
	it.Charged = size
	if r.capBytes > 0 {
		if r.bytes.Add(size) > r.capBytes {
			r.bytes.Add(-size)
			r.items.Add(-1)
			return ErrFull
		}
	}

	select {
	case r.ch <- it:
		return nil
	default:
		// Never block: a queued handler would be a hidden buffer with none of
		// the ring's accounting and none of its backpressure.
		if r.capBytes > 0 {
			r.bytes.Add(-size)
		}
		r.items.Add(-1)
		return ErrFull
	}
}

// Dequeue blocks for the first item, then spends up to batchDelay collecting
// more.
//
// The wait only happens when there is nothing else queued: if the ring already
// holds a backlog, the batch fills immediately and the timer never fires. So
// the delay costs latency exactly when latency is cheap, and costs nothing when
// the server is busy.
func (r *Memory) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	if n < 1 {
		n = 1
	}

	var first *envelope.Item
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case it, ok := <-r.ch:
		if !ok {
			return nil, ErrClosed
		}
		first = it
	}

	out := make([]*envelope.Item, 0, n)
	out = append(out, first)

	// Take whatever is already queued without waiting.
	for len(out) < n {
		select {
		case it, ok := <-r.ch:
			if !ok {
				return out, nil
			}
			out = append(out, it)
			continue
		default:
		}
		break
	}
	if len(out) >= n || r.batchDelay <= 0 {
		return out, nil
	}

	// Still short: wait a little for more to arrive.
	t := time.NewTimer(r.batchDelay)
	defer t.Stop()
	for len(out) < n {
		select {
		case it, ok := <-r.ch:
			if !ok {
				return out, nil
			}
			out = append(out, it)
		case <-t.C:
			return out, nil
		case <-ctx.Done():
			return out, nil
		}
	}
	return out, nil
}

// Commit releases the accounting for a delivered batch: one item each, and the
// bytes Enqueue recorded in it (SPEC §7.2). The buffers themselves are
// released by the caller, which is the one place that owns them (SPEC §12.3).
func (r *Memory) Commit(items []*envelope.Item) error {
	var n, b int64
	for _, it := range items {
		n++
		b += it.Charged
	}
	r.items.Add(-n)
	if r.capBytes > 0 {
		r.bytes.Add(-b)
	}
	return nil
}

// Drain empties the ring and returns everything still pending, for the
// emergency dump. Accounting is cleared with it.
func (r *Memory) Drain() []*envelope.Item {
	var out []*envelope.Item
	for {
		select {
		case it, ok := <-r.ch:
			if !ok {
				return out
			}
			out = append(out, it)
		default:
			if len(out) > 0 {
				_ = r.Commit(out)
			}
			return out
		}
	}
}

func (r *Memory) Stats() Stats {
	return Stats{
		Items:    r.items.Load(),
		Bytes:    r.bytes.Load(),
		CapItems: r.capItems,
		CapBytes: r.capBytes,
	}
}

// Seal stops accepting, as a barrier: once it returns, no Enqueue is in
// flight and none will succeed. The shutdown seals the ring right before its
// last Drain (SPEC §10.3), so whatever is accepted is either drained or
// refused with 503 -- never answered 200 into a ring nobody reads again.
//
// The channel is deliberately NOT closed: a drain worker blocked in Dequeue
// would then receive a zero value, and a handler racing on Enqueue would
// panic.
func (r *Memory) Seal() {
	r.sealMu.Lock()
	r.closed.Store(true)
	r.sealMu.Unlock()
}

// Close stops accepting, as Seal does. Items still in the ring are the
// caller's: Drain them, before or after.
func (r *Memory) Close() error {
	r.Seal()
	return nil
}
