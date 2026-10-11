// Package ring holds an accepted event between the handler and the sink
// (SPEC §7).
//
// The ring is where the server's central promise lives:
//
//	Enqueue returns nil only once the configured mode's durability guarantee
//	has been met. The handler answers 200 after that return and never before.
//
// Nothing here consults the sink's type; the ring and the sink are orthogonal
// axes (SPEC §2.1, invariant §17.12).
package ring

import (
	"context"
	"errors"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
)

// ErrFull means the ring is at one of its two ceilings. The handler turns it
// into 429 (SPEC §4.4). It is never a reason to accept and then discard.
var ErrFull = errors.New("ring: full")

// ErrClosed means the ring is shutting down and will accept nothing further.
var ErrClosed = errors.New("ring: closed")

// ErrNoBacklog is returned by Dequeue on a ring that never holds a backlog.
// Both the drain workers and the shutdown drain treat any Dequeue error as
// "nothing more to take", so this needs no special case at either call site.
var ErrNoBacklog = errors.New("ring: no backlog in this mode")

// Writer is the slice of a destination that ring=none needs in order to write
// through synchronously.
//
// It is declared HERE, at the point of use, rather than imported from the sink
// package. That breaks what would otherwise be an import cycle, and it enforces
// invariant SPEC §17.12 structurally: the ring cannot see the sink package at
// all, so it cannot inspect which destination it is feeding even by accident.
type Writer interface {
	Write(ctx context.Context, items []*envelope.Item) error
}

// Stats is the ring's occupancy, exported as the whs_ring_* gauges and used by
// /readyz to decide readiness.
type Stats struct {
	Items    int64
	Bytes    int64
	CapItems int64
	CapBytes int64
}

// Saturated reports whether occupancy has passed the high-water fraction. This
// is what makes /readyz fail BEFORE the ring is actually full, giving the load
// balancer a chance to route elsewhere instead of collecting 429s.
func (s Stats) Saturated(highWater float64) bool {
	if s.CapItems > 0 && float64(s.Items) >= float64(s.CapItems)*highWater {
		return true
	}
	if s.CapBytes > 0 && float64(s.Bytes) >= float64(s.CapBytes)*highWater {
		return true
	}
	return false
}

// Ring buffers accepted events.
//
// Implementations must be safe for concurrent use: Enqueue runs on every
// handler goroutine while Dequeue runs on every drain worker.
type Ring interface {
	// Enqueue accepts the item, or returns ErrFull immediately. It never blocks
	// waiting for capacity -- backpressure is a 429, not a stalled handler --
	// but it does block for as long as the mode's durability guarantee takes.
	Enqueue(ctx context.Context, it *envelope.Item) error

	// Dequeue returns up to n items for delivery, blocking until at least one
	// is available or ctx is done.
	Dequeue(ctx context.Context, n int) ([]*envelope.Item, error)

	// Commit is called after the sink confirmed the batch, or refused it for
	// good (logic). It is where the ring drops its own record of the items.
	// Their buffers are released by the caller, AFTER Commit: the ring
	// subtracts it.Size(), which is len(Env), and Release empties Env
	// (SPEC §12.3).
	Commit(items []*envelope.Item) error

	// Drain returns everything still pending and empties the ring. It is used
	// by the emergency dump, so it must not block on the sink.
	Drain() []*envelope.Item

	// Stats reports occupancy.
	Stats() Stats

	// Mode is the configured name, for logs and whs_config_info.
	Mode() string

	// Close releases resources. Items still pending are the caller's problem;
	// Drain first if they matter.
	Close() error
}
