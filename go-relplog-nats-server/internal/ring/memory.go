package ring

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
)

// Memory is the only ring (SPEC §9.3): a buffered channel plus byte
// accounting.
//
// Enqueue returns as soon as the item is in the channel, which is what makes
// this the highest-throughput mode. The guarantee it buys is correspondingly
// narrow: the message is in THIS node's RAM. A SIGKILL, an OOM or a kernel
// panic loses it. SIGTERM does not (the shutdown drain), and neither does a
// fatal sink failure (the emergency dump).
type Memory struct {
	ch chan *item.Item

	items    atomic.Int64
	bytes    atomic.Int64
	capItems int64
	capBytes int64

	// batchDelay is how long Dequeue waits to fill a batch once it has one
	// item. Larger batches mean fewer round trips to the destination, and here
	// they also amortise the conversion, which runs per item inside the worker.
	batchDelay time.Duration

	closed atomic.Bool
}

// afterReserve, nil outside tests, runs in Enqueue right after the item slot
// is reserved: the point where a Close racing the Enqueue must still be seen
// (TestEnqueueSeesACloseThatRacedTheReservation; the pattern of
// emergency's var link = os.Link).
var afterReserve func()

// ItemOverhead is what one queued item holds besides its raw buffer, on
// 64-bit:
//
//	224  the item.Item struct, 216 bytes (unsafe.Sizeof, measured 2026-10-07),
//	     in its 224-byte malloc size class: the head with its uuid, clock
//	     sample, three netip.Addr and ports, the Raw slice header and the
//	     atomic flags.
//	   8  the item's slot in this ring's channel buffer.
//	 ---
//	 232  rounded up to 256.
//
// The parents go-relplog-server and go-syslog-server count 384: their item
// also carries the ring=sqlite row id, the ring=redis entry id and a done
// channel, all gone with those rings (the cut of go-syslog-nats-server, kept).
// From and Space are the instance's identity strings, shared
// by every item, so neither is per item. TestItemOverheadCoversTheStruct fails
// if the struct outgrows the number.
const ItemOverhead = 256

// heldBytes is what an item costs THIS ring in memory (SPEC §9.2: "o item
// ocupa cap(cru) mais a struct"). It is cap, not len: the receiver copies a
// line into a pooled buffer of its size class, so an 8193-byte line holds a
// 64 KiB buffer, and a ceiling that counted 8193 would let max_bytes worth of
// such lines hold eight times max_bytes of RAM.
//
// It must give the same number in Enqueue, Commit and Drain: nothing touches
// Raw between them -- the caller releases the buffer only after Commit.
func heldBytes(it *item.Item) int64 { return int64(cap(it.Raw)) + ItemOverhead }

// NewMemory builds the ring. Both ceilings are enforced; whichever is reached
// first produces ErrFull.
func NewMemory(capItems int, capBytes int64, batchDelay time.Duration) *Memory {
	if capItems < 1 {
		capItems = 1
	}
	return &Memory{
		ch:         make(chan *item.Item, capItems),
		capItems:   int64(capItems),
		capBytes:   capBytes,
		batchDelay: batchDelay,
	}
}

func (r *Memory) Mode() string { return "memory" }

// Enqueue accepts the item or returns ErrFull at once.
//
// The byte ceiling is claimed BEFORE the channel send and released if the send
// fails, so the accounting can never drift above what is actually held.
//
// Both ceilings matter. With 200-byte syslog lines the item count is what binds long
// before the byte count: each holds a 2 KiB pool buffer plus ItemOverhead,
// and 65536 x 2.375 KiB is 152 MiB against a 512 MiB ceiling. The byte ceiling
// earns its place the moment one emitter starts sending lines above 8 KiB on
// the same instance: each holds a 64 KiB buffer, and 65536 items would be
// 4 GiB.
func (r *Memory) Enqueue(ctx context.Context, it *item.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}

	// Both ceilings count the same population: accepted and not yet
	// committed, which includes the batches the workers hold while they retry
	// (SPEC §9.2). The channel alone would only bound what is still QUEUED: with
	// a batch held, a capacity-4 ring accepted a fifth message and reported
	// 5/4 items, while the byte ceiling, counted until Commit, said otherwise.
	// The channel buffer is capItems long, so a send under this count never
	// finds it full; the default branch below is a guard, not a path.
	if r.items.Add(1) > r.capItems {
		r.items.Add(-1)
		return ErrFull
	}
	if afterReserve != nil {
		afterReserve()
	}
	// The closed check above can be stale by now: an abort closes the ring
	// and then reads Stats (SPEC §13.4). Re-checked after the reservation,
	// either this Add is visible to that read (counted) or the Close is
	// visible here and the item is refused as the shutdown it is -- never a
	// 200 the abort's os.Exit loses uncounted. Sequentially consistent atomics
	// guarantee at least one of the two; both can happen in one race (the
	// abort counts the reservation and the Enqueue still refuses it), a
	// transient overcount SPEC §13.4 records, never a loss. The cost on the
	// hot path is one Load.
	if r.closed.Load() {
		r.items.Add(-1)
		return ErrClosed
	}
	size := heldBytes(it)
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
		// Never block: a queued receiver goroutine would be a hidden buffer
		// with none of the ring's accounting and none of its backpressure. The
		// RELP transaction holds instead, retrying every 50 ms (SPEC §5.7).
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
func (r *Memory) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	if n < 1 {
		n = 1
	}

	var first *item.Item
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case it, ok := <-r.ch:
		if !ok {
			return nil, ErrClosed
		}
		first = it
	}

	out := make([]*item.Item, 0, n)
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

// Commit releases the accounting for a delivered batch. The buffers themselves
// are released by the caller, which is the one place that owns them
// (SPEC §15.2).
func (r *Memory) Commit(items []*item.Item) error {
	var n, b int64
	for _, it := range items {
		n++
		b += heldBytes(it)
	}
	r.items.Add(-n)
	if r.capBytes > 0 {
		r.bytes.Add(-b)
	}
	return nil
}

// Drain empties the ring and returns everything still pending, for the
// emergency dump. Accounting is cleared with it.
func (r *Memory) Drain() []*item.Item {
	var out []*item.Item
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

// FullFor is a cheap, racy pre-check for the receiver: true when an Enqueue of
// an n-byte line right now would fail on EITHER ceiling. Enqueue stays the
// authority.
//
// n is the length the receiver is about to copy (after truncation), and the
// byte test is heldBytes of the item that copy would make: the pooled class of
// n plus ItemOverhead. Checking the item ceiling alone let a transaction held
// on ring.max_bytes mint a uuid, take a pooled buffer, copy the line and build
// an item on every 50 ms retry, only for Enqueue to refuse it (independent
// review of go-relplog-pg-server, 2026-10-13; SPEC §5.7).
//
// A closed ring is never "full": Enqueue then reports ErrClosed, which the
// receiver answers as the shutdown it is, not as backpressure.
func (r *Memory) FullFor(n int) bool {
	if r.closed.Load() {
		return false
	}
	if r.items.Load() >= r.capItems {
		return true
	}
	return r.capBytes > 0 && r.bytes.Load()+int64(item.PooledCap(n))+ItemOverhead > r.capBytes
}

func (r *Memory) Stats() Stats {
	return Stats{
		Items:    r.items.Load(),
		Bytes:    r.bytes.Load(),
		CapItems: r.capItems,
		CapBytes: r.capBytes,
	}
}

// Close stops accepting. The channel is deliberately NOT closed: a drain worker
// blocked in Dequeue would then receive a zero value, and a receiver racing on
// Enqueue would panic. Callers Drain first, then Close.
func (r *Memory) Close() error {
	r.closed.Store(true)
	return nil
}
