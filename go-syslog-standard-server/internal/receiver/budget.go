package receiver

import (
	"context"
	"sync"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// DefaultInflightMaxBytes is relp.inflight_max_bytes (SPEC §4.5, §15.5).
const DefaultInflightMaxBytes = 64 << 20

// Budget is the process-wide ceiling on the bytes held by the copies of
// in-flight RELP transactions (SPEC §4.5; pendência 2 de §21.1, ported from
// go-relplog-server's receiver/budget.go).
//
// Those copies are NOT in the ring: each transaction holds its own copy of the
// line from the moment it is read until it is decided, and during a
// backpressure hold -- exactly when the ring is full -- they pile up. Without
// a ceiling, relp.window + 1 copies of up to relp.max_frame_bytes per
// connection, times relp.max_conns = 0 connections, is unbounded memory that
// neither ring.max_bytes nor §14.4 sees, precisely at the moment the system is
// saturated. The budget is shared by every connection of every RELP listener,
// so it bounds the whole process.
//
// A reader that cannot get its bytes STOPS READING, like a full window: the
// socket buffer fills, the TCP window closes and the emitter waits. Nothing is
// refused for it.
type Budget struct {
	max int64
	m   *obs.Metrics

	mu     sync.Mutex
	used   int64
	notify chan struct{} // closed and replaced on every release
}

// txnOverhead is what one in-flight transaction holds beyond the copies of its
// line: the goroutine and its stack, the Wire, the timer and the closure. The
// figure is go-relplog-server's, measured at about 9.6 KiB in all for a small
// line (independent review there, 2026-10-11).
const txnOverhead = 8 << 10

// TxnCharge is what one transaction of an n-byte line costs the budget: the
// transaction's own copy, the item's copy inside Ingest.Accept (which with
// ring=none or a slow group commit lives as long as the transaction), and
// txnOverhead. Charging only the copy let small lines put several times the
// ceiling in memory.
func TxnCharge(n int) int64 {
	return 2*int64(n) + txnOverhead
}

// NewBudget builds the ceiling. A non-positive max selects
// DefaultInflightMaxBytes. Config validates that max is at least
// relp.max_frame_bytes, so one transaction always fits; Acquire clamps anyway,
// so a single copy can never wait forever.
func NewBudget(max int64, m *obs.Metrics) *Budget {
	if max <= 0 {
		max = DefaultInflightMaxBytes
	}
	return &Budget{max: max, m: m, notify: make(chan struct{})}
}

// Max is the ceiling.
func (b *Budget) Max() int64 { return b.max }

// Used is what is held now.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Acquire takes n bytes, waiting while the ceiling is exhausted. It returns
// the amount actually charged -- n clamped to the ceiling -- which the caller
// hands back to Release, or false when ctx ended first (nothing is charged
// then).
func (b *Budget) Acquire(ctx context.Context, n int64) (int64, bool) {
	if n > b.max {
		n = b.max
	}
	waited := false
	for {
		b.mu.Lock()
		if b.used+n <= b.max {
			b.used += n
			b.mu.Unlock()
			if b.m != nil {
				b.m.RELPInflightBytes.Add(n)
			}
			return n, true
		}
		ch := b.notify
		b.mu.Unlock()
		if !waited {
			waited = true
			if b.m != nil {
				b.m.RELPInflightWaitsTotal.Inc()
			}
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, false
		}
	}
}

// Release returns what Acquire charged.
func (b *Budget) Release(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
	if b.m != nil {
		b.m.RELPInflightBytes.Add(-n)
	}
}
