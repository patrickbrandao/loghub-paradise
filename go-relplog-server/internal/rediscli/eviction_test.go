package rediscli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// The eviction rule of SPEC §9.5 and §10.3, without a server.
func TestEvictsUnexpiredKeys(t *testing.T) {
	for policy, want := range map[string]bool{
		"allkeys-lru":     true,
		"allkeys-lfu":     true,
		"allkeys-random":  true,
		"ALLKEYS-LRU":     true,
		"allkeys-future":  true, // a policy Redis adds later is refused, not waved through
		"noeviction":      false,
		"volatile-lru":    false,
		"volatile-lfu":    false,
		"volatile-random": false,
		"volatile-ttl":    false,
	} {
		if got := EvictsUnexpiredKeys(policy); got != want {
			t.Errorf("%s: evicts=%v, want %v", policy, got, want)
		}
		if err := verdict(policy); (err != nil) != want || (err != nil && !errors.Is(err, ErrEvictionPolicy)) {
			t.Errorf("%s: verdict %v", policy, err)
		}
	}
}

func TestPolicyFromInfo(t *testing.T) {
	info := "# Memory\r\nused_memory:1024\r\nmaxmemory:0\r\nmaxmemory_policy:allkeys-lru\r\nallocator_frag_ratio:1.0\r\n"
	if p, ok := policyFromInfo(info); !ok || p != "allkeys-lru" {
		t.Errorf("got %q %v", p, ok)
	}
	if _, ok := policyFromInfo("# Memory\r\nused_memory:1\r\n"); ok {
		t.Error("a missing line must be reported as missing, not as an empty policy")
	}
}

// ErrEvictionPolicy must be fatal through the shared table: that is what makes
// the ring's bootErr wrap it in ErrFatal and the sink's classifyRedis make it
// ClassFatal, both exit 3. Wrapped, as CheckEviction returns it.
func TestClassifyEvictionPolicyIsFatal(t *testing.T) {
	err := fmt.Errorf("ring redis: eviction: %w", verdict("allkeys-lru"))
	if Classify(err) != Fatal {
		t.Fatalf("an allkeys-* policy must be fatal, got class %d", Classify(err))
	}
	if Classify(fmt.Errorf("x: %w", ErrPolicyUnknown)) == Fatal {
		t.Fatal("an unreadable policy is a warning, not fatal")
	}
}

// The checks below run against real servers and skip without them:
//
//	docker run --rm -d -p 16409:6379 --name rls-redis redis:7-alpine
//	docker run --rm -d -p 16410:6379 --name rls-redis-lru redis:7-alpine \
//	  redis-server --maxmemory-policy allkeys-lru
//	RELPLOG_TEST_REDIS_ADDR=127.0.0.1:16409 RELPLOG_TEST_REDIS_LRU_ADDR=127.0.0.1:16410 \
//	  go test -race ./internal/rediscli/ ./internal/ring/ ./internal/sink/
//
// The LRU server is a second one because the first is shared by every package
// of the run, in parallel: switching its policy for one test would fail the
// others.

func liveClient(t *testing.T, env string, cfg Config) Config {
	t.Helper()
	addr := os.Getenv(env)
	if addr == "" {
		t.Skipf("%s not set", env)
	}
	cfg.Addrs = []string{addr}
	return cfg
}

func TestCheckEvictionLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("noeviction passes", func(t *testing.T) {
		c, err := New(liveClient(t, "RELPLOG_TEST_REDIS_ADDR", Config{}))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		policy, err := CheckEviction(ctx, c)
		if err != nil || policy != "noeviction" {
			t.Fatalf("policy %q, err %v; the test server must run with the default noeviction", policy, err)
		}
	})

	t.Run("allkeys-lru is fatal", func(t *testing.T) {
		c, err := New(liveClient(t, "RELPLOG_TEST_REDIS_LRU_ADDR", Config{}))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		policy, err := CheckEviction(ctx, c)
		if policy != "allkeys-lru" || !errors.Is(err, ErrEvictionPolicy) || Classify(err) != Fatal {
			t.Fatalf("policy %q, err %v: want a fatal ErrEvictionPolicy", policy, err)
		}
	})

	t.Run("an ACL without INFO and CONFIG is a warning", func(t *testing.T) {
		cfg := liveClient(t, "RELPLOG_TEST_REDIS_ADDR", Config{})
		admin, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()
		user := fmt.Sprintf("rls-noinfo-%d", time.Now().UnixNano())
		if err := admin.Do(ctx, admin.B().Arbitrary("ACL", "SETUSER", user, "on", ">pw", "~*", "&*",
			"+@all", "-info", "-config").Build()).Error(); err != nil {
			t.Fatal(err)
		}
		defer admin.Do(context.Background(), admin.B().Arbitrary("ACL", "DELUSER", user).Build())

		cfg.Username, cfg.Password = user, "pw"
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_, err = CheckEviction(ctx, c)
		if !errors.Is(err, ErrPolicyUnknown) || Classify(err) == Fatal {
			t.Fatalf("err %v: want ErrPolicyUnknown, not fatal", err)
		}
	})
}
