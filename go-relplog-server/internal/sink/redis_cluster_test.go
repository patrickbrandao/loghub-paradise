package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
)

// Redis Cluster, db 0 (SPEC §10.3.1; opt-in, a one-node cluster announcing
// 127.0.0.1, docs/TESTING.md §6.1a): the probe and the policy re-read work.
// rueidis's cluster client panics on a pipeline that mixes keyless commands
// (INFO, CONFIG GET) with keyed ones of different slots, so on a cluster the
// policy rides with the XLEN of the stream (rediscli.Pinned, the stream's
// primary) and the write probe is a Do of its own; before the split, every
// Health here panicked. One node cannot show WHICH node answered: that is
// TestRedisHealthClusterReadingsArePinned, server-free.
func TestRedisClusterHealthAndWrite(t *testing.T) {
	addr := os.Getenv("RELPLOG_TEST_REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("RELPLOG_TEST_REDIS_CLUSTER_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	space := fmt.Sprintf("t%d", time.Now().UnixNano())
	r, err := NewRedis(RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}},
		Workspace: space,
	}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 0; i < 3; i++ {
		if err := r.Health(ctx); err != nil {
			t.Fatalf("check %d on a cluster: %v", i+1, err)
		}
	}
	b := batchFrom(t, newItemIn(space, "<165>1 2026-11-07T10:00:00Z h app - - - cluster 0"),
		newItemIn(space, "<165>1 2026-11-07T10:00:00Z h app - - - cluster 1"))
	if err := r.Write(ctx, b); err != nil {
		t.Fatalf("write on a cluster: %v", err)
	}
	c, err := rediscli.New(rediscli.Config{Addrs: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stream := "loghub." + space + ".syslog"
	defer c.Do(context.Background(), c.B().Del().Key(stream).Build())
	if n, err := c.Do(ctx, c.B().Xlen().Key(stream).Build()).AsInt64(); err != nil || n != 2 {
		t.Fatalf("XLEN %d, %v; want 2", n, err)
	}
}
