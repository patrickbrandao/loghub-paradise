package ring

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
)

// SPEC §9.5 (redis-valkey-08, 2026-11-07): the policy is re-read at every
// RefreshStats. A ring that booted on a noeviction server and whose server
// then turns allkeys-lru (CONFIG SET, a managed panel) becomes unavailable
// with ErrFatal at the next refresh. The LRU server stands in for the
// CONFIG SET -- the test servers are shared by every package of the run -- on
// a copy of the booted ring without its loops, so no goroutine races the
// swapped client.
func TestRedisRingPolicyIsReadAtEveryRefresh(t *testing.T) {
	lruAddr := os.Getenv("RELPLOG_TEST_REDIS_LRU_ADDR")
	if lruAddr == "" {
		t.Skip("RELPLOG_TEST_REDIS_LRU_ADDR not set")
	}
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}},
		Stream:    stream,
		Consumer:  "0199a000-0000-7000-8000-000000000003",
		OpTimeout: time.Second,
		ClaimIdle: time.Hour,
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.RefreshStats(ctx); err != nil || !r.Available() {
		t.Fatalf("refresh on a noeviction server: %v", err)
	}

	lru, err := rediscli.New(rediscli.Config{Addrs: []string{lruAddr}})
	if err != nil {
		t.Fatal(err)
	}
	defer lru.Close()
	moved := &RedisRing{cfg: r.cfg, log: discard(), client: lru, policy: r.policy,
		policyLog: obs.NewSampler(30 * time.Second), keys: r.keys, probeKey: r.probeKey}
	for i := 0; i < 2; i++ {
		err := moved.RefreshStats(ctx)
		if !errors.Is(err, ErrFatal) || !errors.Is(err, ErrUnavailable) ||
			!errors.Is(err, rediscli.ErrEvictionPolicy) || moved.Available() {
			t.Fatalf("refresh %d after the policy changed: %v (available=%v); want unavailable, "+
				"ErrFatal wrapping ErrEvictionPolicy", i+1, err, moved.Available())
		}
	}
}

// SPEC §10.3.1: the boot logs who answered.
func TestRedisRingLogsTheServerIdentity(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var logs bytes.Buffer
	r, err := NewRedis(ctx, RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}},
		Stream:    stream,
		Consumer:  "0199a000-0000-7000-8000-000000000004",
		OpTimeout: time.Second,
		ClaimIdle: time.Hour,
	}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	info, err := admin.Do(ctx, admin.B().Info().Section("server").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	id := rediscli.ParseServerInfo(info)
	if want := "server=" + id.Server + " server_version=" + id.Version; id.Version == "" ||
		!strings.Contains(logs.String(), want) {
		t.Fatalf("want %q in the log; got:\n%s", want, logs.String())
	}
}
