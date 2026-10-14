package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// redisLRUAddrEnv names a second Redis, started with
// --maxmemory-policy allkeys-lru (docs/TESTING.md §5).
const redisLRUAddrEnv = "WHS_TEST_REDIS_LRU_ADDR"

// SPEC §8.2.5 (W pendência 12): the policy rule, with no server.
func TestRedisEvictionVerdict(t *testing.T) {
	for policy, refused := range map[string]bool{
		"allkeys-lru": true, "ALLKEYS-LFU": true, "allkeys-random": true, " allkeys-next ": true,
		"noeviction": false, "volatile-lru": false, "volatile-ttl": false, "": false,
	} {
		err := evictionVerdict(policy)
		if refused != errors.Is(err, ErrEvictionPolicy) {
			t.Errorf("%q: %v", policy, err)
		}
	}
	for info, want := range map[string]string{
		"# Memory\r\nused_memory:1\r\nmaxmemory_policy:allkeys-lru\r\n": "allkeys-lru",
		"maxmemory_policy:noeviction":                                   "noeviction",
	} {
		if got, ok := policyFromInfo(info); !ok || got != want {
			t.Errorf("policyFromInfo(%q) = %q, %v", info, got, ok)
		}
	}
	if _, ok := policyFromInfo("# Memory\r\nused_memory:1\r\n"); ok {
		t.Error("a policy read from an INFO without the line")
	}
	// The verdict is fatal through the sink's classification.
	if err := classifyEviction(fmt.Errorf("x: %w", ErrEvictionPolicy)); !IsFatal(err) {
		t.Errorf("an evicting server = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// SPEC §8.2.5: under allkeys-* a full Redis evicts the stream instead of
// answering OOM, and the write probe cannot tell. The boot refuses it as
// FATAL, and so does every later check: the policy changes with CONFIG SET.
func TestRedisRefusesAnEvictingServer(t *testing.T) {
	addr := os.Getenv(redisLRUAddrEnv)
	if addr == "" {
		t.Skipf("set %s to a Redis with --maxmemory-policy allkeys-lru (docs/TESTING.md §5)", redisLRUAddrEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := RedisConfig{Addrs: []string{addr}, Stream: "loghub.{space}.{type}",
		Workspace: fmt.Sprintf("whs-test-%d", time.Now().UnixNano())}
	_, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
	if !IsFatal(err) || !errors.Is(err, ErrEvictionPolicy) {
		t.Fatalf("NewRedis on an allkeys-lru server: %v (class %s); want fatal ErrEvictionPolicy, exit 3 at boot",
			err, ClassOf(err))
	}

	// Every check, not only the boot: a sink that came up on a noeviction
	// server whose client now reaches an allkeys-lru one (what a CONFIG SET
	// looks like to the sink) is refused at the next Health, and again at
	// the one after.
	f := redisSetup(t)
	r := f.sink(f.cfg)
	lru, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	healthy := r.client
	r.client = lru
	defer func() { r.client = healthy; lru.Close() }()
	for i := 0; i < 2; i++ {
		if err := r.Health(ctx); !IsFatal(err) || !errors.Is(err, ErrEvictionPolicy) {
			t.Fatalf("check %d against an allkeys-lru server: %v; want fatal ErrEvictionPolicy", i+1, err)
		}
	}
}

// An ACL that can read neither INFO nor CONFIG is a WARN, once, and the sink
// comes up: refusing it would lock out every least-privilege deployment.
func TestRedisWarnsWhenThePolicyIsUnreadable(t *testing.T) {
	f := redisSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user := fmt.Sprintf("whs-sink-noinfo-%d", time.Now().UnixNano())
	if err := f.admin.Do(ctx, f.admin.B().Arbitrary("ACL", "SETUSER", user, "on", ">pw", "~*", "&*",
		"+@all", "-info", "-config").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.admin.Do(context.Background(), f.admin.B().Arbitrary("ACL", "DELUSER", user).Build()) })

	var logs bytes.Buffer
	cfg := f.cfg
	cfg.Username, cfg.Password = user, "pw"
	r, err := NewRedis(ctx, cfg, slog.New(slog.NewTextHandler(&logs, nil)), testMetrics())
	if err != nil {
		t.Fatalf("an unreadable policy must not refuse the boot: %v", err)
	}
	defer r.Close()
	for i := 0; i < 2; i++ {
		if err := r.Health(ctx); err != nil {
			t.Fatalf("check %d with an unreadable policy: %v", i+1, err)
		}
	}
	if n := strings.Count(logs.String(), "cannot read maxmemory-policy"); n != 1 {
		t.Fatalf("%d WARNs about the unreadable policy over three checks, want 1; log:\n%s", n, logs.String())
	}
}

// The normal test server is noeviction (or volatile-*): it passes, and the
// check runs on every Health without changing that.
func TestRedisAcceptsANonEvictingServer(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	policy, err := checkRedisEviction(ctx, r.client)
	if err != nil {
		t.Fatalf("policy %q: %v (%s must not run allkeys-*)", policy, err, redisAddrEnv)
	}
	if policy == "" {
		t.Fatal("no policy read from the test server")
	}
}

// countingClient records what each Health sends: the pipelines and their
// commands, and the commands sent on their own.
type countingClient struct {
	rueidis.Client
	mu    sync.Mutex
	multi [][]string // the first word of each command of each DoMulti
	alone int
}

func (c *countingClient) DoMulti(ctx context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	names := make([]string, len(cmds))
	for i, cmd := range cmds {
		names[i] = strings.ToUpper(cmd.Commands()[0])
	}
	c.mu.Lock()
	c.multi = append(c.multi, names)
	c.mu.Unlock()
	return c.Client.DoMulti(ctx, cmds...)
}

func (c *countingClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.mu.Lock()
	c.alone++
	c.mu.Unlock()
	return c.Client.Do(ctx, cmd)
}

// SPEC §8.2.5 (W §19.1 item 42): the probe remembers which reading of the
// policy works and stays one round trip. With INFO denied and CONFIG GET
// allowed, every check made the pipeline and then a CONFIG GET of its own --
// two round trips inside one sink.health_timeout. With both denied, after
// the WARN, the later checks ask for neither.
func TestRedisPolicyProbeStaysOneRoundTrip(t *testing.T) {
	f := redisSetup(t)
	for _, tc := range []struct {
		name  string
		deny  []string
		third string // the policy command in the later pipelines, "" for none
	}{
		{"INFO denied, CONFIG allowed", []string{"-info"}, "CONFIG"},
		{"both denied", []string{"-info", "-config"}, ""},
		{"INFO allowed", nil, "INFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			user := fmt.Sprintf("whs-sink-policy-%d", time.Now().UnixNano())
			acl := append([]string{"ACL", "SETUSER", user, "on", ">pw", "~*", "&*", "+@all"}, tc.deny...)
			if err := f.admin.Do(ctx, f.admin.B().Arbitrary(acl[0]).Args(acl[1:]...).Build()).Error(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { f.admin.Do(context.Background(), f.admin.B().Arbitrary("ACL", "DELUSER", user).Build()) })
			cfg := f.cfg
			cfg.Username, cfg.Password = user, "pw"
			r, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
			if err != nil {
				t.Fatalf("boot: %v", err)
			}
			defer r.Close()
			cc := &countingClient{Client: r.client}
			r.client = cc
			for i := 0; i < 2; i++ {
				if err := r.Health(ctx); err != nil {
					t.Fatalf("check %d: %v", i+1, err)
				}
			}
			if cc.alone != 0 || len(cc.multi) != 2 {
				t.Fatalf("two checks sent %d pipelines and %d commands alone, want 2 and 0", len(cc.multi), cc.alone)
			}
			for _, names := range cc.multi {
				third := ""
				if len(names) == 3 {
					third = names[2]
				}
				if len(names) < 2 || len(names) > 3 || third != tc.third {
					t.Fatalf("pipeline %v, want XLEN, XADD and %q", names, tc.third)
				}
			}
		})
	}
}
