package app

// Opt-in end-to-end suite of sink=redis (docs/TESTING.md §6.1), against Redis
// or Valkey: the same variables as internal/sink (SLSTD_TEST_REDIS_ADDR,
// SLSTD_TEST_REDIS_LRU_ADDR). They skip without them, and they never run in
// parallel: one of them turns the server into a replica and back.

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

func redisEnv(t *testing.T, env string) string {
	t.Helper()
	v := os.Getenv(env)
	if v == "" {
		t.Skipf("%s not set", env)
	}
	return v
}

func redisClient(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// Boot, one TCP message, XLEN; and the boot through the valkey alias.
func TestRedisEndToEnd(t *testing.T) {
	addr := redisEnv(t, "SLSTD_TEST_REDIS_ADDR")
	for _, spelling := range [][]string{
		{"--sink.type=redis", "--sink.redis.addrs=" + addr},
		{"--sink.type=valkey", "--sink.valkey.addrs=" + addr},
	} {
		t.Run(spelling[0], func(t *testing.T) {
			stream := fmt.Sprintf("slstd:e2e:%d", time.Now().UnixNano())
			rc := redisClient(t, addr)
			defer rc.Do(context.Background(), rc.B().Del().Key(stream).Build())
			in := start(t, t.TempDir(), append(spelling, "--listen.udp=", "--listen.relp=",
				"--sink.redis.stream="+stream)...)
			defer in.stop()

			c, err := net.Dial("tcp", in.app.TransportAddr("tcp"))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(c, "<13>1 - host app - - - para o redis\n")
			c.Close()
			if !waitFor(10*time.Second, func() bool {
				n, _ := rc.Do(context.Background(), rc.B().Xlen().Key(stream).Build()).AsInt64()
				return n == 1
			}) {
				t.Fatal("the message never reached the stream")
			}
		})
	}
}

// allkeys-lru: the boot ends with exit 3 and no transport opens.
func TestRedisAllkeysPolicyRefusesTheBoot(t *testing.T) {
	addr := redisEnv(t, "SLSTD_TEST_REDIS_LRU_ADDR")
	b := startExpectingExit(t, t.TempDir(), "--sink.type=redis", "--sink.redis.addrs="+addr,
		"--sink.boot_timeout=30s")
	select {
	case code := <-b.done:
		if code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the boot did not refuse allkeys-lru at once")
	}
	b.assertShut(t)
}

// A primary turned replica: READONLY on the write probe, /readyz 503, and back
// to 200 when it is a primary again -- the check that used to be manual
// (docs/TESTING.md §6.1).
func TestRedisReplicaTurnsReadyzDown(t *testing.T) {
	addr := redisEnv(t, "SLSTD_TEST_REDIS_ADDR")
	rc := redisClient(t, addr)
	ctx := context.Background()
	in := start(t, t.TempDir(), "--sink.type=redis", "--sink.redis.addrs="+addr,
		"--listen.udp=", "--listen.relp=", "--sink.redis.stream=slstd:e2e:replica",
		"--sink.fail_timeout=1h", "--sink.max_retries=100000")
	defer in.stop()
	if code, _ := in.get("/readyz"); code != 200 {
		t.Fatalf("/readyz = %d before the demotion", code)
	}
	if err := rc.Do(ctx, rc.B().Replicaof().Host("127.0.0.1").Port(1).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	defer rc.Do(ctx, rc.B().Replicaof().No().One().Build())
	var body string
	if !waitFor(10*time.Second, func() bool {
		code, b := in.get("/readyz")
		body = b
		return code == 503
	}) {
		t.Fatal("/readyz stayed 200 with the server a replica")
	}
	t.Logf("/readyz: %s", strings.TrimSpace(body))
	if err := rc.Do(ctx, rc.B().Replicaof().No().One().Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(10*time.Second, func() bool { code, _ := in.get("/readyz"); return code == 200 }) {
		t.Fatal("/readyz did not come back after REPLICAOF NO ONE")
	}
}
