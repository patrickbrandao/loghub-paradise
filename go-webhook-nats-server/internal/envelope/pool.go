package envelope

import (
	"sync"
	"sync/atomic"
)

// Buffer pooling, per SPEC §12.2 and §12.3.
//
// Size classes stop at 256 KiB. Above that a buffer is allocated fresh and NEVER
// returned to a pool: pooling worst-case buffers makes the pool hold the worst
// case forever, and Go's large object heap already handles those well.
//
// The ownership rule is absolute and is what the whole scheme rests on:
//
//	The Item owns its buffers. They return to the pool exactly once, and only
//	after the item has been confirmed by the sink (or committed to the ring's
//	own storage). A retried batch does NOT return them -- the item still owns
//	them.
//
// Returning a buffer that something still references would put one client's
// webhook body inside another client's event. It is silent, it does not show up
// under load, and it is the worst bug this program could have.

const (
	classSmall  = 4 << 10
	classMedium = 16 << 10
	classLarge  = 64 << 10
	classHuge   = 256 << 10

	// PoolMaxBytes is the largest body that is pooled. Anything above is
	// allocated per request.
	PoolMaxBytes = classHuge
)

var bodyPools = [4]sync.Pool{
	{New: func() any { b := make([]byte, 0, classSmall); return &b }},
	{New: func() any { b := make([]byte, 0, classMedium); return &b }},
	{New: func() any { b := make([]byte, 0, classLarge); return &b }},
	{New: func() any { b := make([]byte, 0, classHuge); return &b }},
}

// classFor returns the pool index for a required capacity, or -1 when the size
// is beyond the largest class.
func classFor(n int64) int {
	switch {
	case n <= classSmall:
		return 0
	case n <= classMedium:
		return 1
	case n <= classLarge:
		return 2
	case n <= classHuge:
		return 3
	}
	return -1
}

// GetBody returns a zero-length slice with capacity for at least n bytes, and
// the class it came from. A class of -1 means the buffer is not pooled and must
// not be returned.
func GetBody(n int64) (buf []byte, class int) {
	class = classFor(n)
	if class < 0 {
		return make([]byte, 0, n), -1
	}
	p := bodyPools[class].Get().(*[]byte)
	return (*p)[:0], class
}

// PutBody returns a pooled buffer. class must be the value GetBody returned.
// Passing -1 is a no-op, so callers need no special case for large bodies.
func PutBody(buf []byte, class int) {
	if class < 0 || class >= len(bodyPools) || buf == nil {
		return
	}
	b := buf[:0]
	bodyPools[class].Put(&b)
}

// GetEnv returns a zero-length buffer for a JLH of about n bytes, and the
// class it came from (-1: not pooled). The JLH carries the body (SPEC §12.2),
// so it is sized by the same classes; the handler asks with SizeHint: the
// body's exact representation, from the pre-pass, plus an allowance for the
// head.
func GetEnv(n int) (buf []byte, class int) { return GetBody(int64(n)) }

// PutEnv returns a JLH buffer. class must be the value GetEnv returned, and is
// -1 for a buffer that outgrew it (an append that reallocated): that one is
// not the pool's any more.
func PutEnv(buf []byte, class int) { PutBody(buf, class) }

// Item is one event travelling from the handler through the ring to the sink.
//
// Env is the JLH envelope (SPEC §3.1), body included, and is owned by the Item
// (see the ownership rule above). Release must be called exactly once, by
// whoever confirms the item.
type Item struct {
	ID  string // the envelope's uuid, canonical: logs, X-LogHub-UUID, Commit
	Env []byte // the JLH, byte for byte what every destination receives

	// envClass is the pool class Env came from; -1 when it was not pooled.
	envClass int

	// released guards against a double return, which is the ownership bug this
	// whole scheme exists to prevent.
	//
	// It is atomic, not a plain bool, because a non-atomic flag is not a guard:
	// if two goroutines ever both believe they own the item, both would read
	// false and both would file the same buffer back into the pool. The flag
	// only means something if the test-and-set is indivisible.
	released atomic.Bool

	// tainted records that a lower layer may still be reading Env even though
	// the call that used it has returned. It is set on the error paths of SPEC
	// §12.3 -- a sink write that timed out or was cancelled -- and it turns the
	// eventual Release into a hand-over to the GC instead of to the pool.
	// rueidis is why it exists in go-webhook-fast-server: a DoMulti that
	// returns early on a cancelled context leaves its commands queued. Here the
	// NATS sink sets it on every jetstream failure after the first publish: a
	// pending acknowledgement future still holds the message whose Data is
	// this very buffer (SPEC §8.7.2).
	tainted atomic.Bool

	// done, when non-nil, is closed by the ring once the item's durability
	// guarantee is met, releasing the handler to answer 200.
	done chan error

	// RowID is the ring=sqlite row this item came from, or 0. It lets Commit
	// delete exactly the delivered rows without a side table keyed by item.
	RowID int64
}

// Size is the item's contribution to the ring's byte accounting.
func (it *Item) Size() int64 { return int64(len(it.Env)) }

// Release returns the item's buffers to their pools. It is idempotent so a
// double release is a no-op rather than corruption, but a double release is
// still a bug: the second caller believed it owned the item.
//
// A tainted item (see Taint) is finished but NOT pooled: its buffers are left
// to the garbage collector, because something below may still be reading them.
func (it *Item) Release() {
	if it == nil || it.released.Swap(true) {
		return
	}
	if it.tainted.Load() {
		return
	}
	PutEnv(it.Env, it.envClass)
	it.Env = nil
}

// Taint marks the item as one whose buffers may still be read by a lower layer
// after the call that used them returned: a sink write that timed out or was
// cancelled. From then on Release hands the buffers to the GC instead of the
// pool, even if a later attempt delivers the item -- the earlier attempt's
// command may still be on its way through another pipeline connection, and
// pooling the buffer is how another client's body ends up inside it. The cost is
// one allocation on a path that only runs when something already went wrong.
func (it *Item) Taint() {
	if it != nil {
		it.tainted.Store(true)
	}
}

// Discard gives the item up WITHOUT returning its buffers to the pool.
//
// It is Taint followed by Release, for the error paths where the caller no
// longer wants the item but cannot know whether a lower layer still reads it:
// a ring=none write that timed out, or any failure that is not the ring refusing
// the item outright (SPEC §12.3). It deliberately does not nil Env -- that
// write would race with the reader. The buffer goes to the GC instead.
func (it *Item) Discard() {
	if it == nil {
		return
	}
	it.tainted.Store(true)
	it.released.Store(true)
}

// Released reports whether Release has already run. Tests use it to assert the
// ownership rule.
func (it *Item) Released() bool { return it.released.Load() }

// NewItem builds an item that owns env, a JLH from pool class envClass (-1:
// not pooled, as for an item read back from ring.db or a dump).
func NewItem(id string, env []byte, envClass int) *Item {
	return &Item{ID: id, Env: env, envClass: envClass}
}

// Done returns the channel a handler waits on for the ring's durability
// acknowledgement, creating it on first use.
func (it *Item) Done() chan error {
	if it.done == nil {
		it.done = make(chan error, 1)
	}
	return it.done
}

// Ack delivers the durability result to a waiting handler. It is safe to call
// when nobody is waiting.
func (it *Item) Ack(err error) {
	if it.done == nil {
		return
	}
	select {
	case it.done <- err:
	default:
	}
}
