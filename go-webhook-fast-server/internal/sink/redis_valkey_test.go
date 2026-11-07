package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// SPEC §8.2.6, §8.2.4: the handshake's SELECT of a db the server does not
// have -- any db but 0 on Redis Cluster and Valkey < 9 in cluster mode, a db
// beyond cluster-databases on Valkey 9 -- is fatal at once. As an
// unrecognised reply it was retried until sink.boot_timeout.
func TestClassifyMissingDB(t *testing.T) {
	for _, msg := range []string{
		"ERR SELECT is not allowed in cluster mode",
		"ERR DB index is out of range",
		"err db index is out of range",
	} {
		if got := classifyRedisReply(msg); got != ClassFatal {
			t.Errorf("%q: class %s, want fatal", msg, got)
		}
	}
	// The generic ERR prefix is not enough: other ERR replies stay retried.
	for _, msg := range []string{"ERR something new", "ERR invalid DB index"} {
		if got := classifyRedisReply(msg); got != ClassNetwork {
			t.Errorf("%q: class %s, want network", msg, got)
		}
	}
}

// SPEC §8.2.6: the server and its version come from INFO server. Valkey
// reports redis_version:7.2.4 in every release; the version is valkey_version
// under server_name:valkey. The four servers of the test matrix, as they
// answer.
func TestIdentityFromInfo(t *testing.T) {
	for _, tc := range []struct {
		info, server, version string
	}{
		{"# Server\r\nredis_version:7.4.12\r\nredis_git_sha1:00000000\r\nredis_mode:standalone\r\n", "redis", "7.4.12"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:7.2.15\r\n", "valkey", "7.2.15"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:8.1.11\r\n", "valkey", "8.1.11"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.0.7\r\nvalkey_release_stage:ga\r\n", "valkey", "9.0.7"},
		{"", "unknown", ""},
		{"# Server\r\nproxy:1\r\n", "unknown", ""},
	} {
		s, v := identityFromInfo(tc.info)
		if s != tc.server || v != tc.version {
			t.Errorf("identityFromInfo(%q) = %s %s, want %s %s", tc.info, s, v, tc.server, tc.version)
		}
	}
}

// SPEC §8.2.6: against the test server the identity is read (and logged, so
// the output of a run shows which server it ran against); an ACL without
// INFO is "unknown" and the sink still comes up.
func TestRedisIdentity(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	server, version := r.Identity()
	t.Logf("server=%s server_version=%s", server, version)
	if (server != ServerRedis && server != ServerValkey) || version == "" {
		t.Fatalf("Identity() = %q %q, want redis or valkey with a version", server, version)
	}
	if server == ServerValkey && version == "7.2.4" {
		info, _ := f.admin.Do(context.Background(), f.admin.B().Info().Section("server").Build()).ToString()
		if !strings.Contains(info, "valkey_version:7.2.4") {
			t.Fatalf("a Valkey reported redis_version as its version")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user := fmt.Sprintf("whs-sink-noinfo-id-%d", time.Now().UnixNano())
	if err := f.admin.Do(ctx, f.admin.B().Arbitrary("ACL", "SETUSER", user, "on", ">pw", "~*", "&*",
		"+@all", "-info").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.admin.Do(context.Background(), f.admin.B().Arbitrary("ACL", "DELUSER", user).Build()) })
	cfg := f.cfg
	cfg.Username, cfg.Password = user, "pw"
	r2, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("an ACL without INFO must not refuse the boot: %v", err)
	}
	defer r2.Close()
	if s, v := r2.Identity(); s != ServerUnknown || v != "" {
		t.Fatalf("Identity() without INFO = %q %q, want unknown", s, v)
	}
}

// The standalone server of the suite refuses a db beyond its databases with
// the reply Valkey 9 gives in cluster mode (SPEC §8.2.6): fatal at boot, at
// once, with the message that explains it.
func TestRedisMissingDBIsFatalAtBoot(t *testing.T) {
	f := redisSetup(t)
	cfg := f.cfg
	cfg.DB = 100000
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
	if !IsFatal(err) {
		t.Fatalf("NewRedis with db=%d: %v (class %s), want fatal", cfg.DB, err, ClassOf(err))
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the fatal came after %s", took)
	}
	if !strings.Contains(err.Error(), "cluster-databases") {
		t.Fatalf("the error does not explain the missing db: %v", err)
	}
}

// redisClusterAddrEnv names one node of a Redis or Valkey cluster whose
// announced addresses this host reaches (docs/TESTING.md §5.1). Opt-in.
const redisClusterAddrEnv = "WHS_TEST_REDIS_CLUSTER_ADDR"

// SPEC §8.2.6: in cluster mode db=1 is FATAL within two seconds on Redis
// and Valkey < 9 (SELECT is not allowed), and on Valkey 9 without the db
// (DB index is out of range); on Valkey 9 with --cluster-databases it is
// written. db=0 always writes.
func TestRedisClusterSelectDB(t *testing.T) {
	addr := os.Getenv(redisClusterAddrEnv)
	if addr == "" {
		t.Skipf("set %s to one node of a cluster (docs/TESTING.md §5.1)", redisClusterAddrEnv)
	}
	ws := fmt.Sprintf("whs-test-cluster-%d", time.Now().UnixNano())
	for _, db := range []int{0, 1} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cfg := RedisConfig{Addrs: []string{addr}, DB: db, Stream: "loghub.{space}.{type}", Workspace: ws}
		start := time.Now()
		r, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
		took := time.Since(start)
		if err != nil {
			cancel()
			if db == 0 {
				t.Fatalf("db=0 on a cluster: %v", err)
			}
			if !IsFatal(err) || took > 2*time.Second || !strings.Contains(err.Error(), "cluster-databases") {
				t.Fatalf("db=1 on a cluster: %v (class %s) after %s; want fatal within 2s, explained",
					err, ClassOf(err), took)
			}
			t.Logf("db=1: fatal after %s: %v", took, err)
			continue
		}
		if r.client.Mode() != rueidis.ClientModeCluster {
			t.Errorf("%s is not a cluster node (mode %v)", addr, r.client.Mode())
		}
		err = r.Write(ctx, []*envelope.Item{itemIn(ws, testRequest("/cluster", nil))})
		server, version := r.Identity()
		r.Close()
		cancel()
		if err != nil {
			t.Fatalf("db=%d: Write: %v", db, err)
		}
		t.Logf("db=%d: written on %s %s", db, server, version)
	}
}

// redisSentinelAddrEnv and redisSentinelMasterEnv name a Sentinel (or
// valkey-sentinel) monitoring a primary with one replica, every address
// announced as one this host reaches (docs/TESTING.md §5.1). Opt-in.
const (
	redisSentinelAddrEnv   = "WHS_TEST_REDIS_SENTINEL_ADDR"
	redisSentinelMasterEnv = "WHS_TEST_REDIS_SENTINEL_MASTER"
)

// SPEC §8.2.2, §8.2.6: through Sentinel the sink boots, checks the policy on
// the primary, writes; after SENTINEL FAILOVER it writes again -- READONLY
// and MASTERDOWN are transient (§8.2.4), the retry is the worker's -- and no
// event acknowledged before or after is missing on the new primary.
func TestRedisSentinelFailover(t *testing.T) {
	addr, master := os.Getenv(redisSentinelAddrEnv), os.Getenv(redisSentinelMasterEnv)
	if addr == "" || master == "" {
		t.Skipf("set %s and %s (docs/TESTING.md §5.1)", redisSentinelAddrEnv, redisSentinelMasterEnv)
	}
	ws := fmt.Sprintf("whs-test-sentinel-%d", time.Now().UnixNano())
	stream := redisStreamOf(ws)
	cfg := RedisConfig{Addrs: []string{addr}, SentinelMaster: master, Stream: "loghub.{space}.{type}",
		Workspace: ws, ClientName: "whs/test@test-host"}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewRedis through Sentinel: %v", err)
	}
	defer r.Close()
	if r.client.Mode() != rueidis.ClientModeSentinel {
		t.Fatalf("client mode %v, want sentinel", r.client.Mode())
	}
	server, version := r.Identity()
	t.Logf("through Sentinel: server=%s server_version=%s", server, version)
	defer func() { _ = r.client.Do(context.Background(), r.client.B().Del().Key(stream).Build()).Error() }()

	sentinel, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true,
		ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sentinel.Close()
	primaryOf := func() string {
		a, err := sentinel.Do(ctx, sentinel.B().SentinelGetMasterAddrByName().Master(master).Build()).AsStrSlice()
		if err != nil || len(a) != 2 {
			t.Fatalf("SENTINEL GET-MASTER-ADDR-BY-NAME: %v %v", a, err)
		}
		return a[0] + ":" + a[1]
	}

	var written []*envelope.Item
	write := func(tag string, within time.Duration) {
		it := itemIn(ws, testRequest("/"+tag, nil))
		deadline := time.Now().Add(within)
		for {
			err := r.Write(ctx, []*envelope.Item{it})
			if err == nil {
				written = append(written, it)
				return
			}
			if IsFatal(err) || time.Now().After(deadline) {
				t.Fatalf("Write %s: %v (class %s)", tag, err, ClassOf(err))
			}
			time.Sleep(100 * time.Millisecond) // the worker's retry, in short
		}
	}
	for i := 0; i < 5; i++ {
		write(fmt.Sprintf("before-%d", i), 0)
	}
	// The replica has everything acknowledged so far: Sentinel's failover
	// is not coordinated with the primary, and an unreplicated write would
	// be a loss of the test setup, not of the sink.
	if n, err := r.client.Do(ctx, r.client.B().Wait().Numreplicas(1).Timeout(5000).Build()).AsInt64(); err != nil || n < 1 {
		t.Fatalf("WAIT 1: %d %v; the topology needs one replica", n, err)
	}

	old := primaryOf()
	if err := sentinel.Do(ctx, sentinel.B().Arbitrary("SENTINEL", "FAILOVER", master).Build()).Error(); err != nil {
		t.Fatalf("SENTINEL FAILOVER: %v", err)
	}
	for primaryOf() == old {
		if ctx.Err() != nil {
			t.Fatal("the failover did not happen")
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("failover: %s -> %s", old, primaryOf())
	// Until Sentinel turns the old primary into a replica it still accepts
	// writes, and what it acknowledges then is discarded by the resync: the
	// asynchronous-replication window of every Sentinel deployment, not
	// something a client can close (SPEC §8.2.6: min-replicas-to-write
	// narrows it on the server). The test writes once the old primary
	// refuses, which is where the sink's part begins: READONLY retried, the
	// client following the switch, nothing acknowledged lost.
	oldNode, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{old}, DisableCache: true,
		ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer oldNode.Close()
	for {
		info, _ := oldNode.Do(ctx, oldNode.B().Info().Section("replication").Build()).ToString()
		if strings.Contains(info, "role:slave") {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("the old primary was never turned into a replica")
		}
		time.Sleep(200 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		write(fmt.Sprintf("after-%d", i), 30*time.Second)
	}
	// The health check judges the new primary, policy included.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := r.Health(ctx)
		if err == nil {
			break
		}
		if IsFatal(err) || time.Now().After(deadline) {
			t.Fatalf("Health after the failover: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Read back from the new primary itself.
	primary, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{primaryOf()}, DisableCache: true,
		ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	entries, err := primary.Do(ctx, primary.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.FieldValues["e"]] = true
	}
	for _, it := range written {
		if !got[string(it.Env)] {
			t.Errorf("event %s acknowledged and missing on the new primary", it.ID)
		}
	}
	t.Logf("%d events written, %d entries on the new primary", len(written), len(entries))
}
