package rediscli

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// SPEC §10.3.1 (redis-valkey-04, -05): replies that waiting cannot fix.
func TestClassifyReplyValkeyRules(t *testing.T) {
	for msg, want := range map[string]Class{
		// Cluster with db != 0: Redis Cluster and Valkey < 9, then Valkey 9
		// whose cluster-databases does not reach the db.
		"ERR SELECT is not allowed in cluster mode": Fatal,
		"ERR DB index is out of range":              Fatal,
		// As the handshake error of NewClient carries them, without "ERR ".
		"SELECT is not allowed in cluster mode": Fatal,
		"DB index is out of range":              Fatal,
		"unknown command 'evalsha', with args":  Fatal,
		// No Lua: the Redis ring is made of scripts.
		"ERR unknown command 'evalsha', with args beginning with: 'abc' '2' ": Fatal,
		"ERR unknown command 'EVAL', with args beginning with: ":              Fatal,
		"ERR unknown command `evalsha`, with args beginning with: ":           Fatal,
		// Controls: other unknown commands and other ERR stay retried.
		"ERR unknown command 'xautoclaim', with args beginning with: ": Network,
		"ERR something nobody planned for":                             Network,
		"ERR SELECT something else":                                    Network,
	} {
		if got := ClassifyReply(msg); got != want {
			t.Errorf("%q: class %d, want %d", msg, got, want)
		}
	}
}

// The four INFO server outputs of redis-valkey-05 (2026-11-06).
func TestParseServerInfo(t *testing.T) {
	for _, tc := range []struct {
		info string
		want Identity
	}{
		{"# Server\r\nredis_version:7.4.12\r\nredis_mode:standalone\r\n", Identity{"redis", "7.4.12"}},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:7.2.15\r\n", Identity{"valkey", "7.2.15"}},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:8.1.11\r\n", Identity{"valkey", "8.1.11"}},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.0.7\r\nvalkey_release_stage:ga\r\n", Identity{"valkey", "9.0.7"}},
		{"# Server\r\n", Identity{Server: "unknown"}},
	} {
		if got := ParseServerInfo(tc.info); got != tc.want {
			t.Errorf("%q: %+v, want %+v", tc.info, got, tc.want)
		}
	}
}

// Live: the identity of the test server, and an ACL without INFO is unknown,
// not an error.
func TestIdentifyLive(t *testing.T) {
	cfg := liveClient(t, "GELF_TEST_REDIS_ADDR", Config{})
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := Identify(ctx, c)
	if id.Server != "redis" && id.Server != "valkey" || id.Version == "" {
		t.Fatalf("identity %+v", id)
	}
	t.Logf("server under test: %s %s", id.Server, id.Version)
}

// SPEC §10.3.1 (redis-valkey-04): a cluster with db != 0. Opt-in, a
// one-node cluster that announces 127.0.0.1 (docs/TESTING.md §6.1):
//
//	GELF_TEST_REDIS_CLUSTER_ADDR    any image: db=1 must be FATAL at once
//	GELF_TEST_REDIS_CLUSTER_DBS=16  Valkey 9 with --cluster-databases 16:
//	                                  db=1 must work
func TestClusterDBLive(t *testing.T) {
	addr := os.Getenv("GELF_TEST_REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("GELF_TEST_REDIS_CLUSTER_ADDR not set")
	}
	start := time.Now()
	c, err := New(Config{Addrs: []string{addr}, DB: 1})
	if os.Getenv("GELF_TEST_REDIS_CLUSTER_DBS") != "" {
		if err != nil {
			t.Fatalf("cluster-databases > 1, db=1: %v", err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Do(ctx, c.B().Xadd().Key("{lhp}:cluster-db").Id("*").FieldValue().
			FieldValue("p", "1").Build()).Error(); err != nil {
			t.Fatalf("XADD on db 1: %v", err)
		}
		c.Do(ctx, c.B().Del().Key("{lhp}:cluster-db").Build())
		return
	}
	if err == nil {
		c.Close()
		t.Fatal("db=1 on a cluster without that db was accepted")
	}
	if Classify(err) != Fatal || !strings.Contains(err.Error(), "cluster-databases") {
		t.Fatalf("err %v (class %d): want Fatal with the cluster-databases hint", err, Classify(err))
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v: the refusal must be at once, not after retries", d)
	}
	t.Logf("refused: %v", err)
}
