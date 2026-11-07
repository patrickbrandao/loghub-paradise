package rediscli

import (
	"context"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli/clustertest"
)

// SPEC §10.3.1, cluster and pipeline (independent review of 2026-11-07): on a
// Redis Cluster the identity and the policy -- both readings, the CONFIG GET
// fallback included -- are read on the primary that owns the stream, never on
// whichever node rueidis picks for a lone keyless command. Server-free: the
// live cluster test is opt-in and has one node (docs/TESTING.md §6.1a).
func TestKeylessReadingsArePinnedToTheStreamOnACluster(t *testing.T) {
	const stream = "loghub.acme.syslog"
	ctx := context.Background()
	c := clustertest.New()

	Identify(ctx, c, stream)
	// INFO answers "" (no policy line): the CONFIG GET fallback runs too.
	if _, err := new(PolicyWatch).Check(ctx, c, stream); err == nil {
		t.Fatal("the fake's empty CONFIG GET reply must not pass as a policy")
	}
	bad, keyless := c.Violations(stream)
	for _, b := range bad {
		t.Error(b)
	}
	if keyless != 3 {
		t.Fatalf("%d keyless commands recorded, want 3 (INFO server, INFO memory, CONFIG GET): %v",
			keyless, c.Calls())
	}

	// A second watch that learned CONFIG GET pipelines it pinned as well.
	c.Reset()
	w := new(PolicyWatch)
	w.read.Store(int32(PolicyByConfig))
	cmd, read := w.Cmd(c)
	if read != PolicyByConfig {
		t.Fatalf("Cmd returned reading %d, want PolicyByConfig", read)
	}
	w.Judge(ctx, c, stream, read, DoPinned(ctx, c, stream, cmd))
	if bad, n := c.Violations(stream); len(bad) > 0 || n != 1 {
		t.Fatalf("CONFIG GET reading: %v (keyless %d)", bad, n)
	}
}

// Cmd hands Judge the reading it built, so a reply is never judged as the
// other reading's even when the learned state moves in between.
func TestJudgeUsesTheReadingOfCmd(t *testing.T) {
	ctx := context.Background()
	c := clustertest.New()
	w := new(PolicyWatch)
	cmd, read := w.Cmd(c)
	if read != PolicyByInfo || cmd.Commands()[0] != "INFO" {
		t.Fatalf("zero watch: reading %d %v, want INFO", read, cmd.Commands())
	}
	// Another probe learns CONFIG GET meanwhile; this reply is still INFO's.
	w.read.Store(int32(PolicyByConfig))
	c.Reset()
	w.Judge(ctx, c, "s", read, DoPinned(ctx, c, "s", cmd))
	// Judged as INFO: "" has no policy line, so the CONFIG GET fallback ran.
	// Judged as CONFIG GET, nothing more would have been sent.
	if calls := c.Calls(); len(calls) != 2 {
		t.Fatalf("%d calls, want the INFO and its CONFIG GET fallback: %v", len(calls), calls)
	}
}
