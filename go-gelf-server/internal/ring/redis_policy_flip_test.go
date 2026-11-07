package ring

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli/clustertest"
	"github.com/redis/rueidis"
)

// fakeRing is a RedisRing on the server-free cluster stand-in, without its
// loops. policy is the maxmemory_policy that INFO memory answers; down makes
// every command fail like an unreachable server.
func fakeRing(t *testing.T, policy *atomic.Value, down *atomic.Bool) (*RedisRing, *clustertest.Client) {
	t.Helper()
	c := clustertest.New()
	c.Reply = func(args []string) rueidis.RedisResult {
		if down != nil && down.Load() {
			return rueidis.NewErrorResult(errors.New("dial tcp: connection refused"))
		}
		switch args[0] {
		case "XLEN":
			return clustertest.Int(0)
		case "INFO":
			return clustertest.String("# Memory\r\nmaxmemory_policy:" + policy.Load().(string) + "\r\n")
		case "EVALSHA":
			return clustertest.Int(1) // the enqueue script: accepted
		}
		return clustertest.Nil() // GET of the counter, the XADD probe
	}
	stream := "{syslog:ring}"
	r := &RedisRing{
		cfg:       RedisConfig{Stream: stream, OpTimeout: time.Second},
		log:       discard(),
		client:    c,
		policy:    new(rediscli.PolicyWatch),
		policyLog: obs.NewSampler(30 * time.Second),
		keys:      []string{stream, stream + ":bytes"},
		probeKey:  stream + ":probe",
		capItems:  "0",
		capBytes:  "0",
		idleMS:    "1000",
	}
	return r, c
}

// SPEC §9.5 (independent review of 2026-11-07, HIGH): the policy turning
// allkeys-* with the service up makes the ring UNAVAILABLE -- Enqueue refuses
// with ErrUnavailable (and ErrFatal), Available stays false -- and only a good
// reading of the policy brings it back. Before the fix the flip only made
// /readyz blink: the refresh set down, the next Enqueue succeeded and set it
// back, and the receivers kept writing into a stream the server may evict
// whole. Server-free, so it runs in every `go test`; the live version is
// TestRedisRingPolicyIsReadAtEveryRefresh.
func TestRedisRingPolicyFlipMakesItUnavailable(t *testing.T) {
	ctx := context.Background()
	var policy atomic.Value
	policy.Store("noeviction")
	var down atomic.Bool
	r, c := fakeRing(t, &policy, &down)

	// Control: a good policy, the ring works.
	if err := r.RefreshStats(ctx); err != nil || !r.Available() {
		t.Fatalf("noeviction: refresh %v, available %v", err, r.Available())
	}
	if err := r.Enqueue(ctx, mkItem(0, 16)); err != nil {
		t.Fatalf("noeviction: enqueue %v", err)
	}

	policy.Store("allkeys-lru")
	if err := r.RefreshStats(ctx); !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrFatal) {
		t.Fatalf("refresh after the flip: %v", err)
	}
	for i := 0; i < 3; i++ {
		c.Reset()
		err := r.Enqueue(ctx, mkItem(i, 16))
		if !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrFatal) ||
			!errors.Is(err, rediscli.ErrEvictionPolicy) {
			t.Fatalf("enqueue %d after the flip: %v; want ErrUnavailable+ErrFatal wrapping ErrEvictionPolicy",
				i+1, err)
		}
		if len(c.Calls()) != 0 {
			t.Fatalf("enqueue %d after the flip ran the script: %v", i+1, c.Calls())
		}
		if r.Available() {
			t.Fatalf("available after enqueue %d under allkeys-lru", i+1)
		}
	}

	// A failed reading is not a good one: still unavailable once the
	// server answers again with the same policy, and while it does not.
	down.Store(true)
	_ = r.RefreshStats(ctx)
	down.Store(false)
	if r.Available() {
		t.Fatal("a failed refresh cleared the allkeys-* state")
	}
	if err := r.Enqueue(ctx, mkItem(9, 16)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("enqueue after a failed refresh: %v", err)
	}

	// Back to noeviction: one good reading and the ring takes writes again.
	policy.Store("noeviction")
	if err := r.RefreshStats(ctx); err != nil || !r.Available() {
		t.Fatalf("back to noeviction: refresh %v, available %v", err, r.Available())
	}
	if err := r.Enqueue(ctx, mkItem(10, 16)); err != nil {
		t.Fatalf("back to noeviction: enqueue %v", err)
	}

	// allkeys-* again, and then the ACL loses INFO and CONFIG: the reading
	// becomes unreadable, the probe passes (down is false), and the ring
	// stays unavailable -- the last thing known is that the policy evicts.
	policy.Store("allkeys-lfu")
	_ = r.RefreshStats(ctx)
	acl := c.Reply
	c.Reply = func(args []string) rueidis.RedisResult {
		if args[0] == "INFO" || args[0] == "CONFIG" {
			return clustertest.Err("NOPERM this user has no permissions to run the '" +
				args[0] + "' command")
		}
		return acl(args)
	}
	for i := 0; i < 2; i++ {
		if err := r.RefreshStats(ctx); !errors.Is(err, ErrUnavailable) || r.Available() {
			t.Fatalf("refresh %d with the policy unreadable after allkeys-lfu: %v (available=%v)",
				i+1, err, r.Available())
		}
	}
	if r.down.Load() {
		t.Fatal("control: the probe itself passed, down must be false here")
	}
	if err := r.Enqueue(ctx, mkItem(11, 16)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("enqueue with the policy unreadable after allkeys-lfu: %v", err)
	}
}

// SPEC §9.5, §10.3.1: on a cluster the ring's refresh is ONE pipeline whose
// keyed commands share the stream's hash-tag slot, with the policy reading
// in it -- so the reading reaches the primary that owns the stream and
// rueidis has no cross-slot mix to panic on. The CONFIG GET fallback (INFO
// without the line) is pinned the same way.
func TestRedisRingClusterReadingsArePinned(t *testing.T) {
	ctx := context.Background()
	var policy atomic.Value
	policy.Store("noeviction")
	r, c := fakeRing(t, &policy, nil)
	if err := r.RefreshStats(ctx); err != nil {
		t.Fatal(err)
	}
	calls := c.Calls()
	if len(calls) != 1 || len(calls[0]) != 4 {
		t.Fatalf("refresh: %v; want one pipeline of XLEN, GET, XADD and INFO", calls)
	}
	bad, keyless := c.Violations(r.keys[0])
	if len(bad) > 0 || keyless != 1 {
		t.Fatalf("refresh: %v (keyless %d)", bad, keyless)
	}

	// INFO without the policy line: the fallback is a reading of its own.
	c.Reset()
	c.Reply = func(args []string) rueidis.RedisResult {
		switch args[0] {
		case "XLEN":
			return clustertest.Int(0)
		case "INFO":
			return clustertest.String("# Memory\r\n")
		}
		return clustertest.Nil()
	}
	_ = r.RefreshStats(ctx)
	if bad, keyless := c.Violations(r.keys[0]); len(bad) > 0 || keyless != 2 {
		t.Fatalf("refresh with the CONFIG GET fallback: %v (keyless %d): %v", bad, keyless, c.Calls())
	}
}
