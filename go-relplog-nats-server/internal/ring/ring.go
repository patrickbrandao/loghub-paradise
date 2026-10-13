// Package ring holds an accepted message between a receiver and the sink
// (SPEC §9).
//
// The ring is where this server's central promise lives:
//
//	Enqueue returns nil only once the item is held by the ring. Whatever
//	confirmation the transport has happens after that return and never
//	before.
//
// On this server that confirmation is the "200 OK" of the RELP transaction
// (SPEC §2.3, §5.4); a full ring means the transaction is HELD -- no answer
// until the ring takes it, or, past relp.backpressure_timeout, a session closed
// without one, which librelp retransmits (SPEC §5.7).
//
// An item in the ring is head + raw line, never a JLH envelope: the conversion
// runs in the drain worker (SPEC §2.2). There is one mode, memory (SPEC §9);
// the interface stays because it is the seam the receiver and the workers are
// tested through, and nothing here consults the sink (invariant §20.15).
package ring

import (
	"context"
	"errors"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
)

// ErrFull means the ring is at one of its two ceilings. It is never a reason to
// accept and then discard (invariant §20.2); what a receiver does with it is
// the transport's business.
var ErrFull = errors.New("ring: full")

// ErrClosed means the ring is shutting down and will accept nothing further.
var ErrClosed = errors.New("ring: closed")

// Stats is the ring's occupancy, exported as the relplog_ring_* gauges and used
// by /readyz to decide readiness.
type Stats struct {
	Items    int64
	Bytes    int64
	CapItems int64
	CapBytes int64
}

// Saturated reports whether occupancy has passed the high-water fraction. This
// is what makes /readyz fail BEFORE the ring is actually full, giving a load
// balancer a chance to route elsewhere instead of watching messages get dropped.
func (s Stats) Saturated(highWater float64) bool {
	if s.CapItems > 0 && float64(s.Items) >= float64(s.CapItems)*highWater {
		return true
	}
	if s.CapBytes > 0 && float64(s.Bytes) >= float64(s.CapBytes)*highWater {
		return true
	}
	return false
}

// Ring buffers accepted messages.
//
// Implementations must be safe for concurrent use: Enqueue runs on every
// receiver goroutine while Dequeue runs on every drain worker.
//
// An item in the ring carries the head of SPEC §3.4 and the raw line, and NO
// envelope: the conversion happens in the drain worker (SPEC §2.2). That is why
// Stats().Bytes counts each item's raw buffer (its capacity) plus the
// ItemOverhead of the struct, and no JLH -- the JLH is not memory anyone has
// allocated yet (SPEC §9.2).
type Ring interface {
	// Enqueue accepts the item, or returns ErrFull immediately. It never blocks
	// waiting for capacity: backpressure belongs to the transport, not to a
	// stalled goroutine.
	Enqueue(ctx context.Context, it *item.Item) error

	// Dequeue returns up to n items for delivery, blocking until at least one
	// is available or ctx is done. A caller treats an EMPTY batch with a nil
	// error as "ask again".
	Dequeue(ctx context.Context, n int) ([]*item.Item, error)

	// Commit is called after the sink confirmed the batch -- or after a
	// deliberate, counted drop (a permanent rejection, a refused malformed
	// message). It is where the ring drops its own record of the items; their
	// buffers are released by the caller, exactly once, per the ownership rule
	// of SPEC §15.2.
	Commit(items []*item.Item) error

	// Drain returns everything still pending and empties the ring. It is used
	// by the emergency dump, so it must not block on the sink.
	Drain() []*item.Item

	// Stats reports occupancy.
	Stats() Stats

	// Mode is the configured name, for logs and relplog_config_info.
	Mode() string

	// Close releases resources. Items still pending are the caller's problem;
	// Drain first if they matter.
	Close() error
}
