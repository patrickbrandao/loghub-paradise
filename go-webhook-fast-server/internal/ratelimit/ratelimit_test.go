package ratelimit

import (
	"hash/maphash"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock lets the tests advance time exactly, so refill assertions are not
// racing the wall clock.
type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ns.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func newTest(p Params, shards int) (*Limiter, *fakeClock) {
	l := New(p, shards)
	c := &fakeClock{}
	l.now = c.now
	return l, c
}

func defaults() Params {
	return Params{
		Enabled: true, Rate: 10, Burst: 20,
		V4Mask: 24, V6Mask: 48, MaxBuckets: 1000,
	}
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("bad addr %q: %v", s, err)
	}
	return a
}

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("bad prefix %q: %v", s, err)
	}
	return p.Masked()
}

// --------------------------------------------------------------------- basics

func TestDisabledAllowsEverything(t *testing.T) {
	p := defaults()
	p.Enabled = false
	l, _ := newTest(p, 4)

	for i := 0; i < 1000; i++ {
		if ok, _ := l.Allow(mustAddr(t, "203.0.113.9")); !ok {
			t.Fatalf("request %d denied by a disabled limiter", i)
		}
	}
	if l.Len() != 0 {
		t.Errorf("a disabled limiter tracked %d buckets; it should track none", l.Len())
	}
}

func TestBurstThenDeny(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")

	// Burst is 20: the first 20 pass with no time passing at all.
	for i := 0; i < 20; i++ {
		if ok, _ := l.Allow(a); !ok {
			t.Fatalf("request %d should be within the burst", i+1)
		}
	}
	ok, retry := l.Allow(a)
	if ok {
		t.Fatal("request 21 should exceed the burst")
	}
	if retry < time.Second {
		t.Errorf("retryAfter = %s, want at least 1s", retry)
	}
}

func TestRefillIsProportionalToTime(t *testing.T) {
	l, clk := newTest(defaults(), 4) // rate 10/s, burst 20
	a := mustAddr(t, "203.0.113.9")

	for i := 0; i < 20; i++ {
		l.Allow(a)
	}
	if ok, _ := l.Allow(a); ok {
		t.Fatal("bucket should be empty")
	}

	// 500ms at 10 tokens/s is 5 tokens.
	clk.advance(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow(a); !ok {
			t.Fatalf("token %d of the refill should be available", i+1)
		}
	}
	if ok, _ := l.Allow(a); ok {
		t.Fatal("only 5 tokens were earned; the 6th must be denied")
	}
}

func TestRefillCapsAtBurst(t *testing.T) {
	l, clk := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")

	l.Allow(a)
	// An hour of idling must not bank an hour of tokens.
	clk.advance(time.Hour)

	n := 0
	for {
		ok, _ := l.Allow(a)
		if !ok {
			break
		}
		n++
		if n > 100 {
			t.Fatal("bucket exceeded burst after a long idle")
		}
	}
	if n != 20 {
		t.Errorf("allowed %d after idling, want exactly the burst of 20", n)
	}
}

// ------------------------------------------------------------------------ keys

func TestKeyMasking(t *testing.T) {
	cases := []struct {
		addr           string
		v4Mask, v6Mask int
		want           string
	}{
		{"203.0.113.9", 24, 48, "203.0.113.0/24"},
		{"203.0.113.9", 32, 48, "203.0.113.9/32"},
		{"203.0.113.9", 16, 48, "203.0.0.0/16"},
		{"203.0.113.9", 0, 48, "0.0.0.0/0"},
		{"2001:db8:1:2::1", 24, 48, "2001:db8:1::/48"},
		{"2001:db8:1:2::1", 24, 64, "2001:db8:1:2::/64"},
		{"2001:db8:1:2::1", 24, 128, "2001:db8:1:2::1/128"},
		// An IPv4-mapped address must key as IPv4, or the same client would
		// get two independent budgets.
		{"::ffff:203.0.113.9", 24, 48, "203.0.113.0/24"},
		// A mask wider than the address family is clamped, not an error.
		{"203.0.113.9", 100, 48, "203.0.113.9/32"},
	}
	for _, tc := range cases {
		got := Key(mustAddr(t, tc.addr), tc.v4Mask, tc.v6Mask)
		if got.String() != tc.want {
			t.Errorf("Key(%s, %d, %d) = %s, want %s", tc.addr, tc.v4Mask, tc.v6Mask, got, tc.want)
		}
	}
}

func TestAddressesInOnePrefixShareABudget(t *testing.T) {
	l, _ := newTest(defaults(), 4)

	// Twenty different hosts inside one /24 spend one shared burst.
	for i := 0; i < 20; i++ {
		a := netip.AddrFrom4([4]byte{203, 0, 113, byte(i)})
		if ok, _ := l.Allow(a); !ok {
			t.Fatalf("host %d should be within the shared burst", i)
		}
	}
	if ok, _ := l.Allow(mustAddr(t, "203.0.113.200")); ok {
		t.Fatal("a 21st host in the same /24 should be denied")
	}
	// A neighbouring /24 is unaffected.
	if ok, _ := l.Allow(mustAddr(t, "203.0.114.1")); !ok {
		t.Fatal("a different /24 must have its own budget")
	}
	if l.Len() != 2 {
		t.Errorf("tracked %d buckets, want 2", l.Len())
	}
}

func TestIPv6UsesItsOwnMask(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	// Same /48, different /64s: one budget.
	for i := 0; i < 20; i++ {
		a := mustAddr(t, "2001:db8:1::1")
		if ok, _ := l.Allow(a); !ok {
			t.Fatalf("request %d should be within the burst", i)
		}
	}
	if ok, _ := l.Allow(mustAddr(t, "2001:db8:1:ffff::9")); ok {
		t.Fatal("a different /64 inside the same /48 shares the budget and must be denied")
	}
	if ok, _ := l.Allow(mustAddr(t, "2001:db8:2::1")); !ok {
		t.Fatal("a different /48 must have its own budget")
	}
}

// -------------------------------------------------------------------- exempt

func TestExemptBypassesEntirely(t *testing.T) {
	p := defaults()
	p.Exempt = []netip.Prefix{mustPrefix(t, "10.0.0.0/8"), mustPrefix(t, "2001:db8::/32")}
	l, _ := newTest(p, 4)

	for i := 0; i < 500; i++ {
		if ok, _ := l.Allow(mustAddr(t, "10.1.2.3")); !ok {
			t.Fatalf("exempt v4 address denied at request %d", i)
		}
		if ok, _ := l.Allow(mustAddr(t, "2001:db8::5")); !ok {
			t.Fatalf("exempt v6 address denied at request %d", i)
		}
	}
	if l.Len() != 0 {
		t.Errorf("exempt addresses allocated %d buckets; they should allocate none", l.Len())
	}
}

// --------------------------------------------------------------------- sweep

// SPEC §6.3: a bucket refilled to capacity is indistinguishable from one that
// never existed, so deleting it is exact eviction rather than a cache policy.
func TestSweepRemovesOnlyFullBuckets(t *testing.T) {
	l, clk := newTest(defaults(), 8) // rate 10/s, burst 20

	full := mustAddr(t, "203.0.113.1")     // will be left untouched and refill
	drained := mustAddr(t, "198.51.100.1") // will be emptied

	l.Allow(full)
	for i := 0; i < 20; i++ {
		l.Allow(drained)
	}
	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2", l.Len())
	}

	// Nothing is full yet: `full` is one token short.
	if n := l.Sweep(); n != 0 {
		t.Errorf("swept %d buckets immediately, want 0", n)
	}
	if l.Len() != 2 {
		t.Errorf("Len = %d after a no-op sweep, want 2", l.Len())
	}

	// 100ms earns 1 token: enough to top `full` up, not enough for `drained`.
	clk.advance(100 * time.Millisecond)
	if n := l.Sweep(); n != 1 {
		t.Errorf("swept %d, want exactly the refilled bucket", n)
	}
	if l.Len() != 1 {
		t.Errorf("Len = %d, want 1", l.Len())
	}

	// The surviving bucket must have kept its state. The sweep's own refill
	// credited it the single token those 100ms earned -- so exactly one request
	// passes and the next does not. Anything else (0 or 20) would mean the
	// sweep either double-charged it or reset it to a full burst.
	if ok, _ := l.Allow(drained); !ok {
		t.Error("the surviving bucket should have the one token those 100ms earned")
	}
	if ok, _ := l.Allow(drained); ok {
		t.Error("the surviving bucket was reset to a full burst by the sweep")
	}

	// Once it too refills, it is swept.
	clk.advance(10 * time.Second)
	if n := l.Sweep(); n != 1 {
		t.Errorf("swept %d, want 1", n)
	}
	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0", l.Len())
	}
}

// A swept bucket must behave exactly like one that was never created; if it did
// not, eviction would be silently handing out free budget.
func TestSweptBucketIsIndistinguishable(t *testing.T) {
	l, clk := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")

	l.Allow(a)
	clk.advance(10 * time.Second) // fully refilled
	l.Sweep()
	if l.Len() != 0 {
		t.Fatal("bucket should have been swept")
	}

	// A fresh key gets a full burst; the swept one must too.
	n := 0
	for {
		ok, _ := l.Allow(a)
		if !ok {
			break
		}
		n++
		if n > 100 {
			t.Fatal("runaway")
		}
	}
	if n != 20 {
		t.Errorf("a swept bucket allowed %d, want the full burst of 20", n)
	}
}

// ------------------------------------------------------------------- ceiling

func TestMaxBucketsCeiling(t *testing.T) {
	p := defaults()
	p.MaxBuckets = 5
	p.V4Mask = 32 // every address is its own bucket
	l, _ := newTest(p, 4)

	for i := 0; i < 5; i++ {
		a := netip.AddrFrom4([4]byte{203, 0, 113, byte(i)})
		for j := 0; j < 20; j++ {
			l.Allow(a)
		}
		if ok, _ := l.Allow(a); ok {
			t.Fatalf("bucket %d should be exhausted", i)
		}
	}
	if l.Len() != 5 {
		t.Fatalf("Len = %d, want the ceiling of 5", l.Len())
	}

	// Past the ceiling, new keys pass unlimited rather than evicting a
	// tracked one; the saturation is visible through Len().
	fresh := mustAddr(t, "198.51.100.77")
	for i := 0; i < 100; i++ {
		if ok, _ := l.Allow(fresh); !ok {
			t.Fatalf("beyond the ceiling new keys must pass; denied at %d", i)
		}
	}
	if l.Len() != 5 {
		t.Errorf("Len = %d, want it pinned at the ceiling", l.Len())
	}

	// Existing buckets keep enforcing.
	if ok, _ := l.Allow(netip.AddrFrom4([4]byte{203, 0, 113, 0})); ok {
		t.Error("a tracked bucket must keep enforcing past the ceiling")
	}
}

// ------------------------------------------------------------------- reload

func TestSetParamsKeepsStateWhenMasksAreUnchanged(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")
	for i := 0; i < 20; i++ {
		l.Allow(a)
	}
	if ok, _ := l.Allow(a); ok {
		t.Fatal("bucket should be empty")
	}

	p := defaults()
	p.Rate = 50 // rate change only
	l.SetParams(p)

	if l.Len() != 1 {
		t.Errorf("Len = %d; a rate change must not drop buckets", l.Len())
	}
	if ok, _ := l.Allow(a); ok {
		t.Error("a rate change must not refund the spent budget")
	}
}

// Changing a mask changes what a key means, so the old buckets describe
// prefixes of a different width and must be dropped rather than reinterpreted.
func TestSetParamsClearsStateWhenMaskChanges(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")
	for i := 0; i < 20; i++ {
		l.Allow(a)
	}

	p := defaults()
	p.V4Mask = 16
	l.SetParams(p)

	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0 after a mask change", l.Len())
	}
	if ok, _ := l.Allow(a); !ok {
		t.Error("after a mask change the new key should start with a full burst")
	}
}

func TestSetParamsCanDisableAtRuntime(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")
	for i := 0; i < 21; i++ {
		l.Allow(a)
	}
	if ok, _ := l.Allow(a); ok {
		t.Fatal("bucket should be empty")
	}

	p := defaults()
	p.Enabled = false
	l.SetParams(p)

	if ok, _ := l.Allow(a); !ok {
		t.Error("disabling the limiter must take effect immediately")
	}
}

// ---------------------------------------------------------------- retryAfter

func TestRetryAfter(t *testing.T) {
	cases := []struct {
		deficit, rate float64
		want          time.Duration
	}{
		{1, 10, time.Second},    // 0.1s rounds up to the 1s floor
		{1, 1, time.Second},     //
		{5, 1, 5 * time.Second}, //
		{1, 0.5, 2 * time.Second},
		{1, 0, time.Second},  // a zero rate must not divide by zero
		{1, -5, time.Second}, // nor a negative one
		{1e9, 1, time.Hour},  // clamped, so Retry-After stays plausible
	}
	for _, tc := range cases {
		if got := retryAfter(tc.deficit, tc.rate); got != tc.want {
			t.Errorf("retryAfter(%g, %g) = %s, want %s", tc.deficit, tc.rate, got, tc.want)
		}
	}
}

func TestRetryAfterIsNeverZero(t *testing.T) {
	l, _ := newTest(defaults(), 4)
	a := mustAddr(t, "203.0.113.9")
	for i := 0; i < 20; i++ {
		l.Allow(a)
	}
	// Advertising 0 would invite an immediate retry guaranteed to fail again.
	for i := 0; i < 50; i++ {
		ok, retry := l.Allow(a)
		if ok {
			t.Fatal("bucket should stay empty")
		}
		if retry < time.Second {
			t.Fatalf("retryAfter = %s, want at least 1s", retry)
		}
	}
}

// ------------------------------------------------------------------- clocks

// A backwards clock step must never hand out a free refill. The limiter uses a
// monotonic source, but refill() also guards the arithmetic directly.
func TestNonPositiveElapsedNeverRefunds(t *testing.T) {
	b := &bucket{tokens: 3, lastNS: 1000}
	refill(b, 500, 10, 20) // time went backwards
	if b.tokens != 3 {
		t.Errorf("tokens = %g, want them unchanged", b.tokens)
	}
	if b.lastNS != 500 {
		t.Errorf("lastNS = %d, want it advanced to the observed time", b.lastNS)
	}
	refill(b, 500, 10, 20) // identical timestamp
	if b.tokens != 3 {
		t.Errorf("tokens = %g after a repeated timestamp, want them unchanged", b.tokens)
	}
}

// ------------------------------------------------------------------ helpers

func TestNextPow2(t *testing.T) {
	cases := map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 31: 32, 32: 32, 33: 64}
	for in, want := range cases {
		if got := nextPow2(in); got != want {
			t.Errorf("nextPow2(%d) = %d, want %d", in, got, want)
		}
	}
}

// ---------------------------------------------------------------- concurrency

// The total number of allowed requests must equal the burst exactly, no matter
// how many goroutines race for it. Run with -race.
func TestConcurrentAllowGrantsExactlyTheBurst(t *testing.T) {
	p := defaults()
	p.Burst = 100
	p.Rate = 0.0001 // effectively no refill during the test
	l, _ := newTest(p, 16)
	a := mustAddr(t, "203.0.113.9")

	const goroutines, each = 20, 50
	var allowed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if ok, _ := l.Allow(a); ok {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != 100 {
		t.Errorf("allowed %d of %d requests, want exactly the burst of 100", got, goroutines*each)
	}
}

func TestConcurrentAllowSweepAndReload(t *testing.T) {
	l, _ := newTest(defaults(), 16)

	stop := make(chan struct{})

	// The two background goroutines run until stop is closed, so they are
	// tracked separately from the finite workers: waiting on one group for
	// both would deadlock.
	var bg sync.WaitGroup
	bg.Add(2)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.Sweep()
			}
		}
	}()
	go func() {
		defer bg.Done()
		masks := []int{24, 16, 24, 32}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				p := defaults()
				p.V4Mask = masks[i%len(masks)]
				l.SetParams(p)
			}
		}
	}()

	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func(seed int) {
			defer workers.Done()
			for j := 0; j < 2000; j++ {
				a := netip.AddrFrom4([4]byte{203, 0, byte(seed), byte(j)})
				l.Allow(a)
			}
		}(i)
	}

	workers.Wait()
	close(stop)
	bg.Wait()

	// The counter is maintained across concurrent inserts, sweeps and resets;
	// a negative value would mean a double decrement.
	if l.Len() < 0 {
		t.Errorf("Len went negative: %d", l.Len())
	}
	// At rest the counter is exact (SPEC §6.3).
	if got, want := l.Len(), mapLen(l); got != want {
		t.Errorf("Len = %d at rest, but %d buckets exist", got, want)
	}
}

// shardOf is the index of the shard addr's bucket lives in, computed the way
// Allow computes it.
func shardOf(l *Limiter, addr netip.Addr) int {
	p := l.params.Load()
	return int(maphash.Comparable(l.seed, Key(addr, p.V4Mask, p.V6Mask)) & l.mask)
}

// addrsInShard returns n addresses with distinct /24 keys whose buckets all
// land in shard want.
func addrsInShard(t *testing.T, l *Limiter, want, n int) []netip.Addr {
	t.Helper()
	var out []netip.Addr
	for i := 0; i < 1<<16 && len(out) < n; i++ {
		a := netip.AddrFrom4([4]byte{10, byte(i >> 8), byte(i), 1})
		if shardOf(l, a) == want {
			out = append(out, a)
		}
	}
	if len(out) < n {
		t.Fatalf("found only %d addresses in shard %d", len(out), want)
	}
	return out
}

// mapLen counts the buckets that really exist, under each shard's lock.
func mapLen(l *Limiter) int {
	n := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return n
}

// TestResetDoesNotLoseConcurrentInserts pins SPEC §6.3: the bucket counter
// moves with the map, under the lock of the shard that changes. A Reset that
// cleared shard by shard and zeroed the counter at the end, outside the locks,
// forgot a bucket inserted into an already-cleared shard while it was still
// walking the others: the counter read 0 with a bucket alive, the next sweep
// drove it to -1, and max_buckets let one prefix too many in.
//
// The interleaving is forced, not hoped for: the test holds shard 1's lock so
// the Reset parks there after clearing shard 0, inserts into shard 0, and
// only then lets the Reset finish.
func TestResetDoesNotLoseConcurrentInserts(t *testing.T) {
	p := defaults()
	p.MaxBuckets = 3
	l, clock := newTest(p, 2)

	in0 := addrsInShard(t, l, 0, 2)
	in1 := addrsInShard(t, l, 1, 3)
	b, a := in0[0], in0[1]

	l.Allow(b) // shard 0 holds one bucket, which the Reset will clear
	if l.Len() != 1 {
		t.Fatalf("Len = %d after one insert, want 1", l.Len())
	}

	s0, s1 := &l.shards[0], &l.shards[1]
	s1.mu.Lock()
	done := make(chan struct{})
	go func() { l.Reset(); close(done) }()

	// Wait for an observable state: shard 0 empty means the Reset went
	// through it and is now parked on shard 1.
	for {
		s0.mu.Lock()
		n := len(s0.m)
		s0.mu.Unlock()
		if n == 0 {
			break
		}
		runtime.Gosched()
	}

	l.Allow(a) // a fresh bucket in the already-cleared shard 0
	s1.mu.Unlock()
	<-done

	if got, want := l.Len(), mapLen(l); got != want || got != 1 {
		t.Fatalf("after Reset with a concurrent insert: Len = %d, buckets = %d; want both 1", got, want)
	}

	// max_buckets = 3: two more prefixes fit, the third passes unlimited.
	for _, x := range in1 {
		l.Allow(x)
	}
	if got, want := l.Len(), mapLen(l); got != want || got != 3 {
		t.Fatalf("after filling the ceiling: Len = %d, buckets = %d; want both 3 (max_buckets)", got, want)
	}

	// Every bucket refilled: the sweep removes them all and the counter
	// lands on exactly zero, never below.
	clock.advance(time.Hour)
	l.Sweep()
	if got, want := l.Len(), mapLen(l); got != 0 || want != 0 {
		t.Fatalf("after a full sweep: Len = %d, buckets = %d; want 0 and 0", got, want)
	}
}

// TestMaxBucketsHoldsUnderConcurrentInserts: the ceiling is reserved before
// the bucket is created (SPEC §6.3), so inserts racing on different shards
// cannot overshoot it. Without the reservation the check and the increment
// were atomic only per shard. This one catches that by load, so it guards
// against a regression rather than proving the rule; the deterministic proof
// of the counter is the test above.
func TestMaxBucketsHoldsUnderConcurrentInserts(t *testing.T) {
	p := defaults()
	p.MaxBuckets = 8
	l, _ := newTest(p, 64)

	for round := 0; round < 200; round++ {
		l.Reset()
		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup
		for g := 0; g < 32; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				start.Wait()
				l.Allow(netip.AddrFrom4([4]byte{10, byte(round), byte(g), 1}))
			}(g)
		}
		start.Done()
		wg.Wait()
		if got, want := l.Len(), mapLen(l); got != want || want > p.MaxBuckets {
			t.Fatalf("round %d: Len = %d, buckets = %d, max_buckets = %d", round, got, want, p.MaxBuckets)
		}
	}
}

func BenchmarkAllowSameKey(b *testing.B) {
	p := defaults()
	p.Rate = 1e9
	p.Burst = 1e9
	l := New(p, 0)
	a := netip.MustParseAddr("203.0.113.9")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(a)
		}
	})
}

func BenchmarkAllowSpreadKeys(b *testing.B) {
	p := defaults()
	p.Rate = 1e9
	p.Burst = 1e9
	p.MaxBuckets = 1 << 20
	l := New(p, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint32
		for pb.Next() {
			i++
			l.Allow(netip.AddrFrom4([4]byte{byte(i >> 16), byte(i >> 8), byte(i), 1}))
		}
	})
}
