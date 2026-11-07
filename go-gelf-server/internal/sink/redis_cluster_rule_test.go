package sink

import (
	"context"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli/clustertest"
	"github.com/redis/rueidis"
)

// SPEC §10.3, §10.3.1 (independent review of 2026-11-07): on a Redis Cluster
// the health check reads the policy and the identity on the primary that owns
// this instance's stream -- each keyless command rides with the XLEN of the
// stream -- and the write probe, whose key is in another slot, is a Do of its
// own: no pipeline mixes a keyless command with keyed ones of two slots (the
// rueidis panic) or travels alone (an arbitrary node). Server-free; the live
// cluster test is opt-in and has one node, which cannot tell nodes apart.
func TestRedisHealthClusterReadingsArePinned(t *testing.T) {
	c := clustertest.New()
	c.Reply = func(args []string) rueidis.RedisResult {
		switch {
		case args[0] == "XLEN":
			return clustertest.Int(0)
		case args[0] == "INFO" && len(args) > 1 && args[1] == "server":
			return clustertest.String("# Server\r\nredis_version:7.4.12\r\n")
		case args[0] == "INFO":
			return clustertest.String("# Memory\r\nmaxmemory_policy:noeviction\r\n")
		}
		return clustertest.Nil()
	}
	const space = "acme"
	r := &Redis{
		cfg:    RedisConfig{Config: rediscli.Config{Addrs: []string{"fake:6379"}}, Workspace: space, Stream: "loghub.{space}.syslog"},
		log:    discard(),
		m:      metrics(),
		client: c,
	}
	stream := "loghub." + space + ".syslog"
	for i := 0; i < 2; i++ {
		if err := r.Health(context.Background()); err != nil {
			t.Fatalf("check %d: %v", i+1, err)
		}
	}
	bad, keyless := c.Violations(stream)
	for _, b := range bad {
		t.Error(b)
	}
	// INFO memory twice, INFO server once.
	if keyless != 3 {
		t.Fatalf("%d keyless commands, want 3: %v", keyless, c.Calls())
	}
	probes := 0
	for _, call := range c.Calls() {
		for _, cmd := range call {
			if cmd.Args[0] == "XADD" {
				probes++
				if len(call) != 1 {
					t.Errorf("the write probe shares a pipeline on a cluster: %v", call)
				}
			}
		}
	}
	if probes != 2 {
		t.Fatalf("%d write probes, want one per check", probes)
	}
}
