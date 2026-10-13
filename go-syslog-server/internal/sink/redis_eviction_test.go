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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
)

// SPEC §10.3: under maxmemory-policy allkeys-* a full Redis evicts whole
// streams of delivered events instead of answering OOM, and the write probe
// cannot tell (an XADD at maxmemory succeeds, by evicting). The first check
// refuses such a server as FATAL, so the boot exits 3. The server is a second
// one, started with --maxmemory-policy allkeys-lru.
func TestRedisRefusesAnEvictingServer(t *testing.T) {
	addr := os.Getenv("SYSLOG_TEST_REDIS_LRU_ADDR")
	if addr == "" {
		t.Skip("SYSLOG_TEST_REDIS_LRU_ADDR not set")
	}
	r, err := NewRedis(RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}},
		Workspace: fmt.Sprintf("t%d", time.Now().UnixNano()),
	}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = r.Health(ctx)
	if !IsFatal(err) || !errors.Is(err, rediscli.ErrEvictionPolicy) {
		t.Fatalf("Health on an allkeys-lru server: %v; want fatal ErrEvictionPolicy", err)
	}
	// Still refused on the next check: nothing marked it validated.
	if err := r.Health(ctx); !IsFatal(err) {
		t.Fatalf("second check: %v", err)
	}
}

// An ACL that cannot read the policy is a WARN, and the sink comes up.
func TestRedisWarnsWhenThePolicyIsUnreadable(t *testing.T) {
	f := redisSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	user := fmt.Sprintf("sls-sink-noinfo-%d", time.Now().UnixNano())
	if err := f.admin.Do(ctx, f.admin.B().Arbitrary("ACL", "SETUSER", user, "on", ">pw", "~*", "&*",
		"+@all", "-info", "-config").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.admin.Do(context.Background(), f.admin.B().Arbitrary("ACL", "DELUSER", user).Build()) })

	var logs bytes.Buffer
	cfg := f.cfg
	cfg.Username, cfg.Password = user, "pw"
	r, err := NewRedis(cfg, slog.New(slog.NewTextHandler(&logs, nil)), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Health(ctx); err != nil {
		t.Fatalf("an unreadable policy must not refuse the boot: %v", err)
	}
	if !strings.Contains(logs.String(), "cannot read maxmemory-policy") {
		t.Fatalf("no WARN about the unreadable policy; log:\n%s", logs.String())
	}
}
