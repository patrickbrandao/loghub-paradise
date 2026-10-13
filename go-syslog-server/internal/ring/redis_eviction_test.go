package ring

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

// SPEC §9.5: a Redis whose maxmemory-policy is allkeys-* evicts the stream
// instead of answering OOM, so ring=redis refuses it at boot with ErrFatal
// (exit 3) and creates nothing. The server is a second one, started with
// --maxmemory-policy allkeys-lru (see internal/rediscli/eviction_test.go).
func TestRedisRingRefusesAnEvictingServer(t *testing.T) {
	addr := os.Getenv("SYSLOG_TEST_REDIS_LRU_ADDR")
	if addr == "" {
		t.Skip("SYSLOG_TEST_REDIS_LRU_ADDR not set")
	}
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}},
		Stream:    stream,
		Consumer:  "0199a000-0000-7000-8000-000000000001",
		OpTimeout: time.Second,
	}, discard())
	if err == nil {
		r.Close()
		t.Fatal("NewRedis accepted a server with maxmemory-policy allkeys-lru")
	}
	if !errors.Is(err, ErrFatal) || !errors.Is(err, rediscli.ErrEvictionPolicy) {
		t.Fatalf("want ErrFatal wrapping ErrEvictionPolicy, got %v", err)
	}
	if !strings.Contains(err.Error(), "allkeys-lru") {
		t.Errorf("the error must name the policy: %v", err)
	}
	n, xerr := admin.Do(ctx, admin.B().Exists().Key(stream).Build()).AsInt64()
	if xerr != nil || n != 0 {
		t.Fatalf("the refused boot created the stream (exists=%d, %v)", n, xerr)
	}
}

// An ACL that cannot read the policy is a WARN, not a refusal: the ring
// boots, and the log says what the operator must keep.
func TestRedisRingWarnsWhenThePolicyIsUnreadable(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user := fmt.Sprintf("sls-ring-noinfo-%d", time.Now().UnixNano())
	if err := admin.Do(ctx, admin.B().Arbitrary("ACL", "SETUSER", user, "on", ">pw", "~*", "&*",
		"+@all", "-info", "-config").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Do(context.Background(), admin.B().Arbitrary("ACL", "DELUSER", user).Build()) })

	var logs bytes.Buffer
	r, err := NewRedis(ctx, RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}, Username: user, Password: "pw"},
		Stream:    stream,
		Consumer:  "0199a000-0000-7000-8000-000000000002",
		OpTimeout: time.Second,
		ClaimIdle: time.Hour,
	}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("an unreadable policy must not refuse the boot: %v", err)
	}
	r.Close()
	if !strings.Contains(logs.String(), "cannot read maxmemory-policy") {
		t.Fatalf("no WARN about the unreadable policy; log:\n%s", logs.String())
	}
}
