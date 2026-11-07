package ring

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli"
)

// Sentinel (redis-valkey-06, SPEC §9.5): opt-in, against a primary + replica
// + sentinel whose addresses the host can reach (docs/TESTING.md §6.1a):
//
//	GELF_TEST_REDIS_SENTINEL_ADDR=127.0.0.1:38122 GELF_TEST_REDIS_SENTINEL_MASTER=mymaster
//
// Boot through Sentinel, write, the policy check on the primary, SENTINEL
// FAILOVER, write again: nothing accepted before or after is lost.
func TestRedisRingSentinelFailover(t *testing.T) {
	sentinel, master := sentinelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := sentinelAdmin(t, sentinel, master)
	stream := fmt.Sprintf("test:ring:sentinel:%d", time.Now().UnixNano())
	t.Cleanup(func() {
		admin.Do(context.Background(), admin.B().Del().Key(stream, stream+":bytes", stream+":quarantine").Build())
	})

	r, err := NewRedis(ctx, RedisConfig{
		Config:       rediscli.Config{Addrs: []string{sentinel}, SentinelMaster: master},
		Stream:       stream,
		Consumer:     "0199a000-0000-7000-8000-000000000005",
		BlockTimeout: 100 * time.Millisecond,
		OpTimeout:    2 * time.Second,
		ClaimIdle:    time.Hour,
	}, discard())
	if err != nil {
		t.Fatalf("boot through Sentinel: %v", err)
	}
	defer r.Close()

	const before, after = 20, 20
	for i := 0; i < before; i++ {
		if err := r.Enqueue(ctx, mkItem(i, 32)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	old := sentinelFailover(t, ctx, sentinel, master, admin)
	for i := before; i < before+after; i++ {
		for {
			err := r.Enqueue(ctx, mkItem(i, 32))
			if err == nil {
				break
			}
			if errors.Is(err, ErrFatal) || ctx.Err() != nil {
				t.Fatalf("enqueue %d after the failover from %s: %v", i, old, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	seen := map[int]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < before+after && time.Now().Before(deadline) {
		items, err := r.Dequeue(ctx, 64)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			seen[idx(it)] = true
		}
		if err := r.Commit(items); err != nil {
			t.Logf("commit: %v (retried by the next round)", err)
		}
	}
	for i := 0; i < before+after; i++ {
		if !seen[i] {
			t.Fatalf("item %d lost across the failover (%d of %d seen)", i, len(seen), before+after)
		}
	}
}

func sentinelEnv(t *testing.T) (string, string) {
	t.Helper()
	addr, master := os.Getenv("GELF_TEST_REDIS_SENTINEL_ADDR"), os.Getenv("GELF_TEST_REDIS_SENTINEL_MASTER")
	if addr == "" || master == "" {
		t.Skip("GELF_TEST_REDIS_SENTINEL_ADDR and GELF_TEST_REDIS_SENTINEL_MASTER not set")
	}
	return addr, master
}

// sentinelAdmin is a client of the current primary, through Sentinel.
func sentinelAdmin(t *testing.T, sentinel, master string) rueidis.Client {
	t.Helper()
	c, err := rediscli.New(rediscli.Config{Addrs: []string{sentinel}, SentinelMaster: master})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// sentinelFailover waits for the replica to hold everything written so far,
// asks Sentinel for a failover and waits for the new primary. It returns the
// old one.
func sentinelFailover(t *testing.T, ctx context.Context, sentinel, master string, admin rueidis.Client) string {
	t.Helper()
	// SENTINEL FAILOVER does not wait for the replica: what it has not
	// replicated is lost by Redis/Valkey, not by this server. So the
	// replication offset first.
	for {
		info, err := admin.Do(ctx, admin.B().Info().Section("replication").Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		if replicaCaughtUp(info) {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the replica never caught up:\n%s", info)
		}
		time.Sleep(50 * time.Millisecond)
	}
	sc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{sentinel},
		DisableCache: true, ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	addr := func() string {
		a, err := sc.Do(ctx, sc.B().Arbitrary("SENTINEL", "GET-MASTER-ADDR-BY-NAME").Args(master).Build()).AsStrSlice()
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(a, ":")
	}
	old := addr()
	// INPROG: a failover of another run is still settling; the topology is
	// shared, which is also why the Sentinel tests run with -p 1.
	for {
		err := sc.Do(ctx, sc.B().Arbitrary("SENTINEL", "FAILOVER").Args(master).Build()).Error()
		if err == nil {
			break
		}
		if !strings.HasPrefix(err.Error(), "INPROG") || ctx.Err() != nil {
			t.Fatalf("SENTINEL FAILOVER: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
		old = addr()
	}
	for addr() == old {
		if ctx.Err() != nil {
			t.Fatal("no new primary")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("failover %s -> %s", old, addr())
	waitDemoted(t, ctx, old)
	return old
}

// replicaCaughtUp reads INFO replication of a primary: one replica online at
// the primary's offset.
func replicaCaughtUp(info string) bool {
	var offset string
	for _, l := range strings.Split(info, "\n") {
		l = strings.TrimSpace(l)
		if v, ok := strings.CutPrefix(l, "master_repl_offset:"); ok {
			offset = v
		}
	}
	for _, l := range strings.Split(info, "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "slave0:") && strings.Contains(l, "state=online") &&
			strings.Contains(l, "offset="+offset+",") {
			return offset != ""
		}
	}
	return false
}

// waitDemoted waits until the old primary reports role:slave. Between the
// promotion and that moment the old primary still accepts writes, and loses
// them when it resyncs from the new one: Redis/Valkey asynchronous
// replication, not this server. Writing "after the failover" means after it.
func waitDemoted(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr},
		DisableCache: true, ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for {
		info, err := c.Do(ctx, c.B().Info().Section("replication").Build()).ToString()
		if err == nil && strings.Contains(info, "role:slave") {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("the old primary %s was never demoted", addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
