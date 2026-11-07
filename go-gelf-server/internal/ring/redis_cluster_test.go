package ring

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli"
)

// Redis Cluster, db 0 (SPEC §9.5, §10.3.1; opt-in, docs/TESTING.md §6.1a):
// boot with a hash-tagged stream, the refresh with the policy re-read (a Do
// of its own on a cluster: rueidis panics on a pipeline mixing keyless and
// keyed commands), enqueue and drain.
func TestRedisRingCluster(t *testing.T) {
	addr := os.Getenv("GELF_TEST_REDIS_CLUSTER_ADDR")
	if addr == "" {
		t.Skip("GELF_TEST_REDIS_CLUSTER_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := fmt.Sprintf("{test:ring:cluster:%d}", time.Now().UnixNano())
	r, err := NewRedis(ctx, RedisConfig{
		Config:       rediscli.Config{Addrs: []string{addr}},
		Stream:       stream,
		Consumer:     "0199a000-0000-7000-8000-000000000006",
		BlockTimeout: 100 * time.Millisecond,
		OpTimeout:    2 * time.Second,
		ClaimIdle:    time.Hour,
	}, discard())
	if err != nil {
		t.Fatalf("boot on a cluster: %v", err)
	}
	defer r.Close()
	c, err := rediscli.New(rediscli.Config{Addrs: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.Do(context.Background(), c.B().Del().Key(stream, stream+":bytes", stream+":quarantine").Build())

	for i := 0; i < 2; i++ {
		if err := r.RefreshStats(ctx); err != nil || !r.Available() {
			t.Fatalf("refresh %d on a cluster: %v", i+1, err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := r.Enqueue(ctx, mkItem(i, 16)); err != nil {
			t.Fatal(err)
		}
	}
	got := dequeueN(t, r, 5, 10*time.Second)
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
}
