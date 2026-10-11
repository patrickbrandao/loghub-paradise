// Package ratelimit implements the per-network-prefix token bucket of SPEC §8.
//
// It is the sibling service's limiter unchanged, because the algorithm never
// knew anything about HTTP: Allow takes a netip.Addr. What differs is the key.
// There, the address is RESOLVED from forwarding headers before it gets here.
// Here it is the resolved src-ip of SPEC §8: the transport peer, or the client
// declared by a PROXY header from a TRUSTED balancer (SPEC §4.6). The hostname
// inside the message, which is the only other address in sight, is an
// emitter's claim and must never become a bucket key: it is trivially forged
// and would poison every other emitter's bucket (invariant §20.8). The unix
// transport has no address at all and is never limited (SPEC §4.3).
//
// Two properties drive the design:
//
//   - Eviction is exact, not heuristic. A bucket refilled to burst is
//     indistinguishable from a bucket that never existed, so a periodic sweep
//     can delete every full bucket with no loss of precision. There is no LRU
//     and no age guessing.
//
//   - The clock is monotonic. A wall-clock jump backwards would hand every
//     client a free refill; a jump forwards would freeze them. Elapsed time is
//     measured with time.Since, which uses the monotonic reading.
//
// Every parameter is reloadable by SIGHUP, including Enabled (SPEC §16.3).
package ratelimit

import (
	"hash/maphash"
	"math"
	"math/bits"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Params are the tunables. They are swapped as a unit so a reload can never be
// observed half-applied.
type Params struct {
	Enabled bool
	Rate    float64 // tokens per second per prefix
	Burst   float64 // bucket capacity
	V4Mask  int     // 0..32
	V6Mask  int     // 0..128
	Exempt  []netip.Prefix
	// MaxBuckets caps tracked prefixes. Beyond it new keys pass unlimited and
	// the saturation is visible through Len().
	MaxBuckets int
	// SweepInterval is how often RunSweeper evicts full buckets. It lives here
	// so a SIGHUP change reaches the sweeper through the same atomic snapshot
	// as everything else.
	SweepInterval time.Duration
}

type bucket struct {
	tokens float64
	lastNS int64
}

type shard struct {
	mu sync.Mutex
	m  map[netip.Prefix]*bucket
	// Deliberate over-padding: it pushes each shard past one cache line so two
	// cores taking different shard locks do not ping-pong the same line. The
	// exact size of sync.Mutex is an implementation detail, so this pads past
	// the worst case rather than computing a tight fit.
	_ [64]byte
}

// Limiter is safe for concurrent use.
type Limiter struct {
	shards []shard
	mask   uint64 // len(shards)-1; len is always a power of two
	seed   maphash.Seed

	// params is read on every message. An RWMutex here would put a shared
	// cache line in the hot path for a value that changes only on SIGHUP; an
	// atomic pointer to an immutable snapshot costs one load.
	params atomic.Pointer[Params]

	// setMu serialises SetParams so a concurrent reload cannot lose the
	// mask-change comparison.
	setMu sync.Mutex

	// now returns monotonic nanoseconds since the limiter was created.
	now func() int64

	count atomic.Int64
}

// New builds a Limiter. shardHint of 0 selects a size from GOMAXPROCS.
func New(p Params, shardHint int) *Limiter {
	if shardHint <= 0 {
		shardHint = runtime.GOMAXPROCS(0) * 4
	}
	n := nextPow2(shardHint)

	start := time.Now()
	l := &Limiter{
		shards: make([]shard, n),
		mask:   uint64(n - 1),
		seed:   maphash.MakeSeed(),
		now:    func() int64 { return int64(time.Since(start)) },
	}
	l.params.Store(&p)
	for i := range l.shards {
		l.shards[i].m = make(map[netip.Prefix]*bucket)
	}
	return l
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// SetParams applies a reload. When either mask changes, every bucket is dropped:
// the keys would otherwise describe prefixes of a different width and the
// accounting would be meaningless. Rate, burst, exemptions and the ceiling can
// all change without losing state.
func (l *Limiter) SetParams(p Params) {
	l.setMu.Lock()
	old := l.params.Load()
	maskChanged := p.V4Mask != old.V4Mask || p.V6Mask != old.V6Mask
	l.params.Store(&p)
	l.setMu.Unlock()

	if maskChanged {
		l.Reset()
	}
}

// Params returns a copy of the current parameters.
func (l *Limiter) Params() Params { return *l.params.Load() }

// Reset drops every bucket.
//
// The count is decreased shard by shard, by what each shard held, under that
// shard's lock -- the same lock Allow takes to insert and count a bucket. A
// single count.Store(0) at the end would race with a concurrent Allow: a
// bucket inserted into an already-cleared shard would be counted and then
// zeroed, the count would sit below the buckets really held, and the next
// Sweep would drive it negative.
func (l *Limiter) Reset() {
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n := len(s.m)
		clear(s.m)
		l.count.Add(int64(-n))
		s.mu.Unlock()
	}
}

// Len is the number of tracked prefixes, exported as syslog_ratelimit_buckets.
func (l *Limiter) Len() int { return int(l.count.Load()) }

// Key reduces an address to the prefix used as the bucket key.
func Key(addr netip.Addr, v4Mask, v6Mask int) netip.Prefix {
	addr = addr.WithZone("").Unmap()
	bitsWanted := v6Mask
	if addr.Is4() {
		bitsWanted = v4Mask
	}
	if bitsWanted < 0 {
		bitsWanted = 0
	}
	if bitsWanted > addr.BitLen() {
		bitsWanted = addr.BitLen()
	}
	p, err := addr.Prefix(bitsWanted)
	if err != nil {
		// Prefix only fails when bits is out of range, which the clamping above
		// has already excluded; fall back to the host route rather than panic.
		return netip.PrefixFrom(addr, addr.BitLen())
	}
	return p
}

// Allow charges one token to addr's prefix. It returns whether the message may
// proceed and, when it may not, how long until a token is available. Syslog has
// nobody to advertise that to -- the receiver drops with reason="ratelimit",
// even on TCP, because an emitter above its rate does not drain on its own
// (SPEC §8) -- but the value is kept for the log line and for parity with the
// sibling service.
//
// A disabled limiter, an exempt address and an unusable address all allow.
func (l *Limiter) Allow(addr netip.Addr) (bool, time.Duration) {
	p := l.params.Load()

	if !p.Enabled || !addr.IsValid() {
		return true, 0
	}
	for _, ex := range p.Exempt {
		if ex.Contains(addr) {
			return true, 0
		}
	}

	key := Key(addr, p.V4Mask, p.V6Mask)
	s := &l.shards[maphash.Comparable(l.seed, key)&l.mask]
	now := l.now()

	s.mu.Lock()
	b, ok := s.m[key]
	if !ok {
		// The ceiling exists so hostile key churn cannot exhaust memory. Past
		// it new prefixes pass unlimited, which is visible as Len() pinned at
		// the maximum.
		if l.count.Load() >= int64(p.MaxBuckets) {
			s.mu.Unlock()
			return true, 0
		}
		b = &bucket{tokens: p.Burst, lastNS: now}
		s.m[key] = b
		l.count.Add(1)
	} else {
		refill(b, now, p.Rate, p.Burst)
	}

	if b.tokens >= 1 {
		b.tokens--
		s.mu.Unlock()
		return true, 0
	}
	deficit := 1 - b.tokens
	s.mu.Unlock()

	return false, retryAfter(deficit, p.Rate)
}

// refill advances a bucket to now. Splitting this out keeps Allow and Sweep
// using exactly the same accounting, which is what makes "full" mean the same
// thing in both.
func refill(b *bucket, now int64, rate, burst float64) {
	elapsed := now - b.lastNS
	if elapsed <= 0 {
		// A repeated timestamp is normal at high rates; never let it subtract.
		b.lastNS = now
		return
	}
	b.tokens += float64(elapsed) / 1e9 * rate
	if b.tokens > burst {
		b.tokens = burst
	}
	b.lastNS = now
}

// retryAfter converts a token deficit into a whole number of seconds, rounded
// up and floored at 1: Retry-After is expressed in seconds, and advertising 0
// would invite an immediate retry that is guaranteed to fail again.
func retryAfter(deficit, rate float64) time.Duration {
	if rate <= 0 {
		return time.Second
	}
	secs := math.Ceil(deficit / rate)
	if secs < 1 {
		secs = 1
	}
	if secs > 3600 {
		secs = 3600
	}
	return time.Duration(secs) * time.Second
}

// Sweep deletes every bucket that has refilled to capacity. Such a bucket is
// byte-for-byte equivalent to one that does not exist, so this is exact
// eviction rather than a cache policy: nothing is forgotten that would have
// changed a later decision.
//
// Returns how many buckets were removed.
func (l *Limiter) Sweep() int {
	p := l.params.Load()
	now := l.now()
	removed := 0

	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		here := 0
		for k, b := range s.m {
			refill(b, now, p.Rate, p.Burst)
			if b.tokens >= p.Burst {
				delete(s.m, k)
				here++
			}
		}
		// Under the shard's lock, like Reset and Allow: the count never
		// lags the maps by more than the shard being worked on.
		if here > 0 {
			l.count.Add(int64(-here))
		}
		s.mu.Unlock()
		removed += here
	}
	return removed
}

// RunSweeper sweeps until stop is closed. Interval changes take effect at the
// next tick.
func (l *Limiter) RunSweeper(stop <-chan struct{}, interval func() time.Duration) {
	for {
		d := interval()
		if d <= 0 {
			d = 30 * time.Second
		}
		t := time.NewTimer(d)
		select {
		case <-stop:
			t.Stop()
			return
		case <-t.C:
			l.Sweep()
		}
	}
}
