package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
)

// Sentinel (redis-valkey-06, SPEC §10.3): opt-in, against a primary + replica
// + sentinel whose addresses the host can reach (docs/TESTING.md §6.1a):
//
//	RELPLOG_TEST_REDIS_SENTINEL_ADDR=127.0.0.1:38122 RELPLOG_TEST_REDIS_SENTINEL_MASTER=mymaster
//
// Boot (the first Health, policy included) through Sentinel, write, SENTINEL
// FAILOVER, write again retrying as the worker would (READONLY and the
// connection are network): every envelope is in the stream of the new primary.
func TestRedisSentinelFailover(t *testing.T) {
	sentinel, master := os.Getenv("RELPLOG_TEST_REDIS_SENTINEL_ADDR"), os.Getenv("RELPLOG_TEST_REDIS_SENTINEL_MASTER")
	if sentinel == "" || master == "" {
		t.Skip("RELPLOG_TEST_REDIS_SENTINEL_ADDR and RELPLOG_TEST_REDIS_SENTINEL_MASTER not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := rediscli.New(rediscli.Config{Addrs: []string{sentinel}, SentinelMaster: master})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	space := fmt.Sprintf("t%d", time.Now().UnixNano())
	stream := "loghub." + space + ".syslog"
	t.Cleanup(func() { admin.Do(context.Background(), admin.B().Del().Key(stream).Build()) })

	r, err := NewRedis(RedisConfig{
		Config:    rediscli.Config{Addrs: []string{sentinel}, SentinelMaster: master},
		Workspace: space,
	}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Health(ctx); err != nil {
		t.Fatalf("first check through Sentinel: %v", err)
	}
	mk := func(from, n int) []*item.Item {
		out := make([]*item.Item, n)
		for i := range out {
			out[i] = newItemIn(space, fmt.Sprintf("<165>1 2026-11-07T10:00:00Z h app - - - sentinel %d", from+i))
		}
		return out
	}
	if err := r.Write(ctx, batchFrom(t, mk(0, 10)...)); err != nil {
		t.Fatalf("write before the failover: %v", err)
	}

	// The replica first holds everything (SENTINEL FAILOVER does not wait).
	for {
		info, err := admin.Do(ctx, admin.B().Info().Section("replication").Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		if replicaCaughtUp(info) {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("the replica never caught up")
		}
		time.Sleep(50 * time.Millisecond)
	}
	sc, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{sentinel},
		DisableCache: true, ForceSingleClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	primary := func() string {
		a, err := sc.Do(ctx, sc.B().Arbitrary("SENTINEL", "GET-MASTER-ADDR-BY-NAME").Args(master).Build()).AsStrSlice()
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(a, ":")
	}
	old := primary()
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
		old = primary()
	}
	for primary() == old {
		if ctx.Err() != nil {
			t.Fatal("no new primary")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("failover %s -> %s", old, primary())
	waitDemoted(t, ctx, old)

	b := batchFrom(t, mk(10, 10)...)
	for {
		err := r.Write(ctx, b)
		if err == nil {
			break
		}
		if IsFatal(err) || ctx.Err() != nil {
			t.Fatalf("write after the failover: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := r.Health(ctx); err != nil {
		t.Fatalf("check on the new primary: %v", err)
	}
	res, err := admin.Do(ctx, admin.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range res {
		for i := 0; i < 20; i++ {
			if strings.Contains(e.FieldValues["e"], fmt.Sprintf("sentinel %d\"", i)) {
				seen[fmt.Sprint(i)] = true
			}
		}
	}
	if len(seen) != 20 {
		t.Fatalf("%d of 20 envelopes in the new primary's stream (%d entries)", len(seen), len(res))
	}
}

// replicaCaughtUp reads INFO replication of a primary: one replica online at
// the primary's offset.
func replicaCaughtUp(info string) bool {
	var offset string
	for _, l := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "master_repl_offset:"); ok {
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
