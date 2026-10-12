// Package ring holds an accepted message between a receiver and the sink
// (SPEC §9).
//
// The ring is where this server's central promise lives:
//
//	Enqueue returns nil only once the configured mode's durability guarantee
//	has been met. Whatever confirmation the transport has happens after that
//	return and never before.
//
// What that confirmation IS depends on the transport (SPEC §2.3):
//
//	tcp, tls  resuming reads, so the TCP window reopens and the emitter keeps
//	          going
//	unix      nothing, but the kernel already blocked the sender's send()
//	udp       nothing at all; a full ring means a counted drop, and nobody was
//	          promised otherwise
//
// An item in the ring is head + raw line, never a JLH envelope: the conversion
// runs in the drain worker (SPEC §2.2). Four modes: memory, sqlite, redis and
// none (SPEC §9). Nothing here consults the sink's type; the ring and the sink
// are orthogonal axes (SPEC §2.1, invariant §20.15).
package ring

import (
	"context"
	"errors"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// ErrFull means the ring is at one of its two ceilings. It is never a reason to
// accept and then discard (invariant §20.2); what a receiver does with it is
// the transport's business.
var ErrFull = errors.New("ring: full")

// ErrClosed means the ring is shutting down and will accept nothing further.
var ErrClosed = errors.New("ring: closed")

// ErrUnavailable means the ring's own storage cannot be reached: the Redis of
// ring=redis is down. It is retryable like ErrFull -- UDP drops with
// reason="ring_unavailable", TCP stops reading -- but unlike ErrFull the item
// may already sit in a pipeline that still sends it, so the receiver discards
// rather than pools its buffer (SPEC §15.2).
var ErrUnavailable = errors.New("ring: unavailable")

// ErrNoBacklog is returned by Dequeue on a ring that never holds a backlog.
// Both the drain workers and the shutdown drain treat any Dequeue error as
// "nothing more to take", so this needs no special case at either call site.
var ErrNoBacklog = errors.New("ring: no backlog in this mode")

// Writer is the slice of a destination that ring=none needs in order to write
// through synchronously.
//
// It is declared HERE, at the point of use, rather than imported from the sink
// package. That breaks what would otherwise be an import cycle, and it enforces
// invariant §20.15 structurally: the ring cannot see the sink package at all,
// so it cannot inspect which destination it is feeding even by accident.
type Writer interface {
	Write(ctx context.Context, items []*item.Item) error
}

// Stats is the ring's occupancy, exported as the relplog_ring_* gauges and used
// by /readyz to decide readiness. With ring=redis it is the GLOBAL occupancy of
// the shared stream, read from a cache the drain refreshes.
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
// Stats().Bytes counts only raw lines -- the JLH is not memory anyone has
// allocated yet.
type Ring interface {
	// Enqueue accepts the item, or returns ErrFull immediately. It never blocks
	// waiting for capacity -- backpressure belongs to the transport, not to a
	// stalled goroutine -- but it does block for as long as the mode's
	// durability guarantee takes.
	Enqueue(ctx context.Context, it *item.Item) error

	// Dequeue returns up to n items for delivery, blocking until at least one
	// is available or ctx is done. It may also return an EMPTY batch with a
	// nil error: the SQLite ring does so when every row it selected went to
	// quarantine (SPEC §9.4), the Redis ring when a read timed out, and a
	// caller treats that as "ask again".
	Dequeue(ctx context.Context, n int) ([]*item.Item, error)

	// Commit is called after the sink confirmed the batch -- or after a
	// deliberate, counted drop (a permanent rejection, a refused malformed
	// message). It is where the ring drops its own record of the items; their
	// buffers are released by the caller, exactly once, per the ownership rule
	// of SPEC §15.2.
	Commit(items []*item.Item) error

	// Touch renews the ring's claim on items a worker is still holding in a
	// retry loop. Only ring=redis does anything: it resets the idle time of the
	// pending entries so another replica's XAUTOCLAIM does not take a batch
	// that is alive (SPEC §9.5). The other modes return nil.
	Touch(items []*item.Item) error

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
