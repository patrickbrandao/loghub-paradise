package sink

// Opt-in integration suite of sink=redis against a real server, Redis or
// Valkey (docs/TESTING.md §6.1). Every test skips without its variable:
//
//	SLSTD_TEST_REDIS_ADDR             a noeviction server (redis:7.4, valkey 7.2/8.1/9.0)
//	SLSTD_TEST_REDIS_LRU_ADDR         a server with maxmemory-policy allkeys-lru
//	SLSTD_TEST_REDIS_SENTINEL_ADDR    a Sentinel, with
//	SLSTD_TEST_REDIS_SENTINEL_MASTER  the name of the master set it monitors
//
// The prefix is SLSTD_, never go-syslog-server's SYSLOG_TEST_*: the two suites
// may run on one machine against different servers. The tests reconfigure the
// server (CONFIG SET, REPLICAOF), so they never run in parallel and each one
// puts back what it changed.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

func redisAddr(t *testing.T, env string) string {
	t.Helper()
	addr := os.Getenv(env)
	if addr == "" {
		t.Skipf("%s not set", env)
	}
	return addr
}

// rawRedis is a plain client for the assertions and the reconfiguration.
func rawRedis(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("raw client to %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	return c
}

func uniqueStream(t *testing.T) string {
	return fmt.Sprintf("slstd:test:%s:%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())
}

func redisBatch(n int, tag string) ([]Prepared, func()) {
	p := NewPrep("slstd-test", syslog.Options{}, nil)
	items := make([]*envelope.Item, n)
	for i := range items {
		items[i] = item(i, fmt.Sprintf("<13>1 - host app - - - %s %d", tag, i))
	}
	return p.Build(items), p.Release
}

func newTestRedis(t *testing.T, cfg RedisConfig) *Redis {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, cfg, discard(), metrics())
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// Boot, identity and one batch: XLEN and the three fields of SPEC §3.3.
func TestRedisIntegrationWriteAndXLEN(t *testing.T) {
	addr := redisAddr(t, "SLSTD_TEST_REDIS_ADDR")
	stream := uniqueStream(t)
	raw := rawRedis(t, addr)
	t.Cleanup(func() { raw.Do(context.Background(), raw.B().Del().Key(stream).Build()) })

	r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, Stream: stream})
	name, version := r.Server()
	t.Logf("server = %s %s", name, version)
	if name != "redis" && name != "valkey" {
		t.Fatalf("server identity = %q %q; INFO server is readable here", name, version)
	}
	// SLSTD_TEST_REDIS_EXPECT (e.g. "valkey 8.1") pins what the run is
	// against: a Valkey must report its valkey_version, never the fixed
	// redis_version:7.2.4 (SPEC §10.3).
	if want := os.Getenv("SLSTD_TEST_REDIS_EXPECT"); want != "" && !strings.HasPrefix(name+" "+version, want) {
		t.Fatalf("server = %s %s, want %s", name, version, want)
	}

	batch, release := redisBatch(3, "xlen")
	defer release()
	if err := r.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	n, err := raw.Do(context.Background(), raw.B().Xlen().Key(stream).Build()).AsInt64()
	if err != nil || n != 3 {
		t.Fatalf("XLEN = %d, %v; want 3", n, err)
	}
	entries, err := raw.Do(context.Background(), raw.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil || len(entries) != 3 {
		t.Fatalf("XRANGE = %d entries, %v", len(entries), err)
	}
	f := entries[0].FieldValues
	if f["v"] != envelope.Version || !strings.HasPrefix(f["e"], "{") || f["b"] != "<13>1 - host app - - - xlen 0" {
		t.Fatalf("fields = %v", f)
	}
	if err := r.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if ex, _ := raw.Do(context.Background(), raw.B().Exists().Key(stream+":healthprobe").Build()).AsInt64(); ex != 0 {
		t.Fatal("the write probe created its key")
	}
}

// OOM at maxmemory is fatal at once, never retried.
func TestRedisIntegrationOOMIsFatal(t *testing.T) {
	addr := redisAddr(t, "SLSTD_TEST_REDIS_ADDR")
	raw := rawRedis(t, addr)
	ctx := context.Background()
	stream := uniqueStream(t)
	r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, Stream: stream})

	if err := raw.Do(ctx, raw.B().ConfigSet().ParameterValue().ParameterValue("maxmemory", "1").Build()).Error(); err != nil {
		t.Fatalf("CONFIG SET maxmemory: %v", err)
	}
	defer raw.Do(ctx, raw.B().ConfigSet().ParameterValue().ParameterValue("maxmemory", "0").Build())

	err := r.Health(ctx)
	if !IsFatal(err) {
		t.Fatalf("Health at maxmemory = %v (class %s), want fatal", err, ClassOf(err))
	}
	batch, release := redisBatch(1, "oom")
	defer release()
	if err := r.Write(ctx, batch); !IsFatal(err) {
		t.Fatalf("Write at maxmemory = %v, want fatal", err)
	}
}

// allkeys-lru is fatal at boot (pendência 13 de §21.1).
func TestRedisIntegrationAllkeysPolicyIsFatalAtBoot(t *testing.T) {
	addr := redisAddr(t, "SLSTD_TEST_REDIS_LRU_ADDR")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := NewRedis(ctx, RedisConfig{Addrs: []string{addr}, Stream: uniqueStream(t)}, discard(), metrics())
	if !errors.Is(err, ErrEvictionPolicy) || !IsFatal(err) {
		t.Fatalf("NewRedis against allkeys-lru = %v, want a fatal ErrEvictionPolicy", err)
	}
}

// ... and in every probe: a CONFIG SET on a live server takes the sink down
// as FATAL (SPEC §10.3), whichever reading of the policy worked.
func TestRedisIntegrationPolicyIsRecheckedByEveryProbe(t *testing.T) {
	addr := redisAddr(t, "SLSTD_TEST_REDIS_ADDR")
	raw := rawRedis(t, addr)
	ctx := context.Background()
	r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, Stream: uniqueStream(t)})
	if err := r.Health(ctx); err != nil {
		t.Fatalf("Health under noeviction: %v", err)
	}
	set := func(p string) {
		if err := raw.Do(ctx, raw.B().ConfigSet().ParameterValue().ParameterValue("maxmemory-policy", p).Build()).Error(); err != nil {
			t.Fatalf("CONFIG SET maxmemory-policy %s: %v", p, err)
		}
	}
	set("allkeys-lru")
	defer set("noeviction")
	if err := r.Health(ctx); !errors.Is(err, ErrEvictionPolicy) || !IsFatal(err) {
		t.Fatalf("Health after CONFIG SET allkeys-lru = %v, want fatal", err)
	}
	set("volatile-lru")
	if err := r.Health(ctx); err != nil {
		t.Fatalf("Health under volatile-lru: %v", err)
	}
	// The CONFIG GET path: learned, then re-read the same way.
	r.policyRead.Store(policyByConfig)
	set("allkeys-lfu")
	if err := r.Health(ctx); !errors.Is(err, ErrEvictionPolicy) {
		t.Fatalf("Health by CONFIG GET after allkeys-lfu = %v, want ErrEvictionPolicy", err)
	}
}

// A primary turned replica refuses the write probe with READONLY: network,
// not fatal, so the state machine goes DOWN and comes back (SPEC §10.3).
func TestRedisIntegrationReplicaIsNotWritable(t *testing.T) {
	addr := redisAddr(t, "SLSTD_TEST_REDIS_ADDR")
	raw := rawRedis(t, addr)
	ctx := context.Background()
	r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, Stream: uniqueStream(t)})

	// A replica of a master that does not exist: read-only, and nothing is
	// ever synced over the data.
	if err := raw.Do(ctx, raw.B().Replicaof().Host("127.0.0.1").Port(1).Build()).Error(); err != nil {
		t.Fatalf("REPLICAOF: %v", err)
	}
	defer raw.Do(ctx, raw.B().Replicaof().No().One().Build())
	err := r.Health(ctx)
	if err == nil || IsFatal(err) || ClassOf(err) != ClassNetwork {
		t.Fatalf("Health on a replica = %v (class %s), want network", err, ClassOf(err))
	}
	if err := raw.Do(ctx, raw.B().Replicaof().No().One().Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := r.Health(ctx); err != nil {
		t.Fatalf("Health after REPLICAOF NO ONE: %v", err)
	}
}

// sink.redis.db != 0 against a cluster (redis-valkey-04): Redis Cluster and
// Valkey < 9 refuse SELECT, Valkey 9 refuses a db beyond cluster-databases --
// both fatal at once, not retried until sink.boot_timeout. And Valkey 9 with
// cluster-databases 16 takes db=1.
//
//	SLSTD_TEST_REDIS_CLUSTER_ADDR     a one-node cluster with the default databases
//	SLSTD_TEST_REDIS_CLUSTER_DB_ADDR  a Valkey 9 node with --cluster-databases 16
func TestRedisIntegrationClusterDB(t *testing.T) {
	if addr := os.Getenv("SLSTD_TEST_REDIS_CLUSTER_ADDR"); addr != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		start := time.Now()
		_, err := NewRedis(ctx, RedisConfig{Addrs: []string{addr}, DB: 1, Stream: "{slstd}:s"}, discard(), metrics())
		cancel()
		if !IsFatal(err) || !errors.Is(err, errClusterDB) {
			t.Fatalf("cluster with db=1 = %v (class %s), want the fatal errClusterDB", err, ClassOf(err))
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("the refusal took %v", time.Since(start))
		}
		t.Logf("db=1: %v", err)
		r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, Stream: uniqueStream(t)})
		batch, release := redisBatch(1, "cluster")
		defer release()
		if err := r.Write(context.Background(), batch); err != nil {
			t.Fatalf("db=0 on the cluster: %v", err)
		}
	}
	if addr := os.Getenv("SLSTD_TEST_REDIS_CLUSTER_DB_ADDR"); addr != "" {
		r := newTestRedis(t, RedisConfig{Addrs: []string{addr}, DB: 1, Stream: uniqueStream(t)})
		batch, release := redisBatch(1, "cluster-db")
		defer release()
		if err := r.Write(context.Background(), batch); err != nil {
			t.Fatalf("Valkey 9 cluster-databases 16, db=1: %v", err)
		}
	}
	if os.Getenv("SLSTD_TEST_REDIS_CLUSTER_ADDR") == "" && os.Getenv("SLSTD_TEST_REDIS_CLUSTER_DB_ADDR") == "" {
		t.Skip("SLSTD_TEST_REDIS_CLUSTER_ADDR / SLSTD_TEST_REDIS_CLUSTER_DB_ADDR not set")
	}
}

// Sentinel: boot through the Sentinel, write, fail over, write again, and
// every message is on the new primary (SPEC §10.3).
func TestRedisIntegrationSentinelFailover(t *testing.T) {
	sentinel := redisAddr(t, "SLSTD_TEST_REDIS_SENTINEL_ADDR")
	master := os.Getenv("SLSTD_TEST_REDIS_SENTINEL_MASTER")
	if master == "" {
		t.Skip("SLSTD_TEST_REDIS_SENTINEL_MASTER not set")
	}
	ctx := context.Background()
	stream := uniqueStream(t)
	r := newTestRedis(t, RedisConfig{Addrs: []string{sentinel}, SentinelMaster: master, Stream: stream})
	name, version := r.Server()
	t.Logf("primary through the Sentinel: %s %s", name, version)

	b1, rel1 := redisBatch(5, "before")
	defer rel1()
	if err := r.Write(ctx, b1); err != nil {
		t.Fatalf("Write before the failover: %v", err)
	}
	// The replica must have the five before the failover, or the test would
	// measure replication lag, not this sink.
	if err := r.client.Do(ctx, r.client.B().Wait().Numreplicas(1).Timeout(5000).Build()).Error(); err != nil {
		t.Fatalf("WAIT: %v", err)
	}

	sc := rawRedis(t, sentinel)
	before, err := sc.Do(ctx, sc.B().SentinelGetMasterAddrByName().Master(master).Build()).AsStrSlice()
	if err != nil {
		t.Fatalf("SENTINEL get-master-addr-by-name: %v", err)
	}
	if err := sc.Do(ctx, sc.B().Arbitrary("SENTINEL", "FAILOVER", master).Build()).Error(); err != nil {
		t.Fatalf("SENTINEL FAILOVER: %v", err)
	}
	var after []string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		after, _ = sc.Do(ctx, sc.B().SentinelGetMasterAddrByName().Master(master).Build()).AsStrSlice()
		if len(after) == 2 && strings.Join(after, ":") != strings.Join(before, ":") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if strings.Join(after, ":") == strings.Join(before, ":") {
		t.Fatalf("the failover never happened: master still %v", before)
	}
	t.Logf("failover %v -> %v", before, after)

	// Replication is asynchronous: between the promotion and the demotion of
	// the old primary, a write it acknowledges is lost when it turns replica.
	// That is a property of Redis/Valkey, not of this sink (min-replicas-to-write
	// is the server's answer, docs/TESTING.md §6.1), so the writes after the
	// failover start only once the old primary is a replica.
	old := rawRedis(t, before[0]+":"+before[1])
	for time.Now().Before(deadline) {
		info, _ := old.Do(ctx, old.B().Info().Section("replication").Build()).ToString()
		if strings.Contains(info, "role:slave") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The sink retries a READONLY/MASTERDOWN as network; a worker would
	// retry the same batch, and so does this loop.
	b2, rel2 := redisBatch(5, "after")
	defer rel2()
	var werr error
	for time.Now().Before(deadline) {
		if werr = r.Write(ctx, b2); werr == nil {
			break
		}
		if IsFatal(werr) {
			t.Fatalf("Write after the failover is fatal: %v", werr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if werr != nil {
		t.Fatalf("Write after the failover never succeeded: %v", werr)
	}
	if err := r.Health(ctx); err != nil {
		t.Fatalf("Health on the new primary: %v", err)
	}
	primary := rawRedis(t, after[0]+":"+after[1])
	n, err := primary.Do(ctx, primary.B().Xlen().Key(stream).Build()).AsInt64()
	if err != nil || n != 10 {
		t.Fatalf("XLEN on the new primary = %d, %v; want the 10 messages", n, err)
	}
	primary.Do(ctx, primary.B().Del().Key(stream).Build())
}
