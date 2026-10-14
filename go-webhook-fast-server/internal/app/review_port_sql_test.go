package app

// Port of the SQL corrections of the syslog/RELP family (2026-10-13): the app
// is the one place that knows the ring, and it tells the ClickHouse sink how
// many writes can be in flight at once (SPEC §8.8.5).

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/config"
)

func TestSinkWritersFollowTheRing(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want int
	}{
		{config.RingMemory, 4},
		{config.RingSQLiteMode, 4},
		{config.RingNone, 64}, // the write-through handlers, up to max_inflight
	} {
		a := &App{cfg: &config.Config{}}
		a.cfg.Ring.Mode, a.cfg.Ring.MaxInflight, a.cfg.Sink.Workers = tc.mode, 64, 4
		if got := a.sinkWriters(); got != tc.want {
			t.Errorf("ring.mode=%s: sinkWriters = %d, want %d", tc.mode, got, tc.want)
		}
	}
}

// SPEC §8.8.5, end to end: with sink.workers=2 the pool derives 3, and a
// per-user memory limit that holds two worst batches but not three is a
// configuration no write can break -- only the two workers ever INSERT. The
// boot refused it while the check multiplied by the pool.
func TestClickHouseBootCountsTheWorkersNotThePool(t *testing.T) {
	e := chTest(t)
	table := e.table()
	user := fmt.Sprintf("whs_appmem_%d", time.Now().UnixNano())
	ctx := context.Background()
	// 64 MiB of worst batch costs 640 MiB: two fit in 2e9, three do not.
	if err := e.conn.Exec(ctx, "CREATE USER "+user+" IDENTIFIED BY 'pw' "+
		"SETTINGS max_memory_usage_for_user = 2000000000"); err != nil {
		t.Skipf("cannot CREATE USER (needs access management): %v", err)
	}
	t.Cleanup(func() { _ = e.conn.Exec(context.Background(), "DROP USER IF EXISTS "+user) })
	if err := e.conn.Exec(ctx, "GRANT INSERT ON `"+e.dbname+"`.`"+table+"` TO "+user); err != nil {
		t.Fatal(err)
	}
	args := e.args(table, "--ring.mode=memory", "--sink.workers=2", "--ring.max_bytes=64MiB",
		"--sink.clickhouse.username="+user, "--sink.clickhouse.password=pw")
	in := start(t, t.TempDir(), args...)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want %d", code, ExitOK)
	}
}
