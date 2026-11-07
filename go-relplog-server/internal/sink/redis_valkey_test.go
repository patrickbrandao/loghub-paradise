package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
)

// SPEC §10.3 (redis-valkey-08, 2026-11-07): the policy is re-read at every
// check, not only at the boot's. A sink that came up on a noeviction server
// whose client now reaches an allkeys-lru one -- what a CONFIG SET, or a
// managed Redis/Valkey panel, looks like to the sink -- is refused at the next
// Health, and at the one after. Swapping the client stands in for the CONFIG
// SET: the test servers are shared by every package of the run.
func TestRedisPolicyIsReadAtEveryCheck(t *testing.T) {
	lruAddr := os.Getenv("RELPLOG_TEST_REDIS_LRU_ADDR")
	if lruAddr == "" {
		t.Skip("RELPLOG_TEST_REDIS_LRU_ADDR not set")
	}
	f := redisSetup(t)
	r := f.sink(f.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Health(ctx); err != nil {
		t.Fatalf("first check on a noeviction server: %v", err)
	}
	lru, err := rediscli.New(rediscli.Config{Addrs: []string{lruAddr}})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	healthy := r.client
	r.client = lru
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.client = healthy; r.mu.Unlock(); lru.Close() }()
	for i := 0; i < 2; i++ {
		if err := r.Health(ctx); !IsFatal(err) || !errors.Is(err, rediscli.ErrEvictionPolicy) {
			t.Fatalf("check %d after the policy changed: %v; want fatal ErrEvictionPolicy", i+1, err)
		}
	}
}

// SPEC §10.3.1: the first check that reaches the server logs who it is, once.
// Valkey reports redis_version:7.2.4 in every release; the log carries
// valkey_version instead.
func TestRedisLogsTheServerIdentity(t *testing.T) {
	f := redisSetup(t)
	var logs bytes.Buffer
	r, err := NewRedis(f.cfg, slog.New(slog.NewTextHandler(&logs, nil)), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := r.Health(ctx); err != nil {
			t.Fatal(err)
		}
	}
	info, err := f.admin.Do(ctx, f.admin.B().Info().Section("server").Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	id := rediscli.ParseServerInfo(info)
	want := fmt.Sprintf("server=%s server_version=%s", id.Server, id.Version)
	if id.Server == "unknown" || id.Version == "" || strings.Count(logs.String(), want) != 1 {
		t.Fatalf("want one %q in the log; got:\n%s", want, logs.String())
	}
	t.Logf("server under test: %s %s", id.Server, id.Version)
}
