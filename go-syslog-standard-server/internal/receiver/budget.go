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

	mu   sync.Mutex
	used int64
	// waiters is the FIFO of readers parked in Acquire. Bytes are granted
	// strictly in arrival order: a newcomer never jumps the queue, even
	// when its charge would fit. Without the order a charge clamped to the
	// ceiling (it needs used == 0) starved behind a stream of small ones,
	// each fitting in the gap the last release left (revisão independente de
	// 2026-11-07; SPEC §4.5).
	waiters []*budgetWaiter
}

type budgetWaiter struct {
	n       int64
	ready   chan struct{} // closed once granted
	granted bool          // under mu
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
	return &Budget{max: max, m: m}
}

// waiting is how many readers are parked in Acquire (for tests).
func (b *Budget) waiting() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.waiters)
}

// Max is the ceiling.
func (b *Budget) Max() int64 { return b.max }

// Used is what is held now.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Acquire takes n bytes, waiting while the ceiling is exhausted or another
// reader waits ahead of this one (FIFO). It returns the amount actually
// charged -- n clamped to the ceiling -- which the caller hands back to
// Release, or false when ctx ended first (nothing is charged then).
func (b *Budget) Acquire(ctx context.Context, n int64) (int64, bool) {
	return b.AcquireWait(ctx, n, nil)
}

// AcquireWait is Acquire with onWait, run once (outside the lock) when the
// caller is about to park; the RELP reader drops its frame buffer there.
func (b *Budget) AcquireWait(ctx context.Context, n int64, onWait func()) (int64, bool) {
	if n > b.max {
		n = b.max
	}
	b.mu.Lock()
	if len(b.waiters) == 0 && b.used+n <= b.max {
		b.used += n
		b.mu.Unlock()
		if b.m != nil {
			b.m.RELPInflightBytes.Add(n)
		}
		return n, true
	}
	w := &budgetWaiter{n: n, ready: make(chan struct{})}
	b.waiters = append(b.waiters, w)
	b.mu.Unlock()
	if b.m != nil {
		b.m.RELPInflightWaitsTotal.Inc()
	}
	if onWait != nil {
		onWait()
	}

	select {
	case <-w.ready:
		return n, true
	case <-ctx.Done():
	}
	b.mu.Lock()
	if w.granted {
		// Granted while the context ended: hand the bytes back, which may
		// grant the next in line.
		b.used -= n
		if b.m != nil {
			b.m.RELPInflightBytes.Add(-n)
		}
	} else {
		for i, x := range b.waiters {
			if x == w {
				b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
				break
			}
		}
	}
	// Leaving the head of the queue may let the next one in.
	b.grantLocked()
	b.mu.Unlock()
	return 0, false
}

// grantLocked hands bytes to the waiters at the head of the queue, in order,
// while they fit. It stops at the first that does not: the order is the
// fairness.
func (b *Budget) grantLocked() {
	for len(b.waiters) > 0 {
		w := b.waiters[0]
		if b.used+w.n > b.max {
			return
		}
		b.used += w.n
		if b.m != nil {
			b.m.RELPInflightBytes.Add(w.n)
		}
		w.granted = true
		close(w.ready)
		b.waiters[0] = nil
		b.waiters = b.waiters[1:]
	}
}

// Release returns what Acquire charged.
func (b *Budget) Release(n int64) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	if b.m != nil {
		b.m.RELPInflightBytes.Add(-n)
	}
	b.grantLocked()
	b.mu.Unlock()
}
