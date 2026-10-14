package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// The independent review of the port of the R, RP and RN fixes into this
// server (2026-10-13): the persistent rings at the abort and with sink=none.

// leaveRingRows runs a ring=sqlite life whose destination never confirms and
// stops it, leaving n rows in ring.db. It returns the arguments that life ran
// with and the ring database's path.
func leaveRingRows(t *testing.T, dir string, n int, tag string) (args []string, ringDB string) {
	t.Helper()
	tcpAddr := freePort(t)
	args = append(baseArgs(dir), "--ring.mode=sqlite", "--listen.udp=", "--listen.tcp="+tcpAddr)
	fs := newFaultySink(true)
	first := launch(t, testID, append(args, "--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - %s-%d", tag, i)
	}
	sendLF(t, tcpAddr, lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == uint64(n)
	})
	ringDB = first.app.cfg.RingSQL.Path
	if code := first.stop(); code != ExitOK {
		t.Fatalf("first life exited %d", code)
	}
	if got := sqliteCount(t, ringDB, "ring"); got != n {
		t.Fatalf("ring.db holds %d rows, want %d", got, n)
	}
	return args, ringDB
}

// SPEC §9.4, §12.2: sink=none discards what it is given by configuration, so
// the rows a life with a destination left in ring.db are not drained into it:
// they stay, outside this run's accounting -- capacity, /readyz, gauges --,
// and a later life with a destination delivers them. The boot drain used to
// deliver all seven to the discard (independent review of the port,
// 2026-10-13; the F's rule, F SPEC §8.4). This run's own messages still go
// through the ring and are discarded as usual.
func TestSinkNoneKeepsAPreviousLifesRingBacklog(t *testing.T) {
	dir := t.TempDir()
	const n = 7
	args, ringDB := leaveRingRows(t, dir, n, "kept")

	// --- second life: sink=none -----------------------------------------------
	second := launch(t, testID, append(args, "--sink.type=none"), nil)
	second.waitReady()
	sr := second.app.ring.(*ring.SQLiteRing)
	if k := sr.Kept(); k != n {
		t.Errorf("Kept() = %d, want %d", k, n)
	}
	if st := second.app.ring.Stats(); st.Items != 0 {
		t.Errorf("the kept rows are in this run's accounting: Stats().Items = %d", st.Items)
	}
	const m = 3
	lines := make([]string, m)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - discard-%d", i)
	}
	sendLF(t, second.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "this run's messages discarded and deleted from ring.db", func() bool {
		return second.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == m &&
			second.app.ring.Stats().Items == 0 && sqliteCount(t, ringDB, "ring") == n
	})
	if code := second.stop(); code != ExitOK {
		t.Fatalf("second life exited %d", code)
	}
	if got := sqliteCount(t, ringDB, "ring"); got != n {
		t.Fatalf("after sink=none, ring.db holds %d rows, want the previous life's %d", got, n)
	}

	// --- third life: a destination delivers them -----------------------------
	third := launch(t, testID, args, nil)
	third.waitReady()
	if got := sqliteCount(t, third.eventsDB(), "syslog_events"); got != n {
		t.Fatalf("the life with a destination delivered %d rows, want %d", got, n)
	}
	if code := third.stop(); code != ExitOK {
		t.Errorf("third life exited %d", code)
	}
}

// SPEC §13.4: a second signal with a persistent ring loses nothing. Phase 4
// is held by a destination that never confirms; the abort counts nothing as
// abort, writes no dump, and the next boot finds every message in ring.db
// (independent review of the port, 2026-10-13: the persistent branch of
// abortLoss had no test).
func TestSecondSignalWithRingSQLiteLeavesEverythingInTheRing(t *testing.T) {
	dir := t.TempDir()
	tcpAddr := freePort(t)
	args := append(baseArgs(dir), "--ring.mode=sqlite", "--listen.udp=", "--listen.tcp="+tcpAddr)
	fs := newFaultySink(true) // gated: no write ever returns
	in := launch(t, testID, append(args, "--shutdown.drain_timeout=1h"), fs.wrap)
	in.waitReady()
	const n = 9
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - abort-sqlite-%d", i)
	}
	sendLF(t, tcpAddr, lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	in.cancel()
	waitFor(t, 5*time.Second, "phase 4", func() bool {
		code, _ := in.get("/readyz")
		return code == 503 && in.app.workers.Held() > 0
	})
	in.app.abort <- os.Interrupt
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if got := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 0 {
		t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d with ring=sqlite, want 0`, got)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*")); len(files) != 0 {
		t.Errorf("the abort wrote a dump: %v", files)
	}
	ringDB := in.app.cfg.RingSQL.Path
	if got := sqliteCount(t, ringDB, "ring"); got != n {
		t.Fatalf("ring.db holds %d rows after the abort, want %d", got, n)
	}
	next := launch(t, testID, args, nil)
	next.waitReady()
	if got := sqliteCount(t, next.eventsDB(), "syslog_events"); got != n {
		t.Fatalf("the next boot delivered %d rows, want %d", got, n)
	}
	if code := next.stop(); code != ExitOK {
		t.Errorf("next life exited %d", code)
	}
}

// The same with ring=redis: the abort in phase 4 counts nothing as abort and
// writes no dump; every entry stays in the stream, and another life drains it
// (independent review of the port, 2026-10-13). The abort's line names what
// this instance held, never the stream's global occupancy.
func TestSecondSignalWithRingRedisLeavesEverythingInTheStream(t *testing.T) {
	e := redisTest(t)
	r := newSharedRing(t, e)
	dir := t.TempDir()
	fs := newFaultySink(true) // gated: no write ever returns
	var buf logBuf
	in := launch(t, testID, r.args(e, dir, "--shutdown.drain_timeout=1h", "--sink.op_timeout=1h",
		"--ring.redis.claim_idle=3h"), fs.wrap, captureLog(&buf))
	in.waitReady()
	const n = 9
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - abort-redis-%d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted and a batch held", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && in.app.workers.Held() > 0
	})
	in.cancel()
	waitFor(t, 5*time.Second, "phase 4", func() bool {
		code, _ := in.get("/readyz")
		return code == 503
	})
	in.app.abort <- os.Interrupt
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if got := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 0 {
		t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d with ring=redis, want 0`, got)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*")); len(files) != 0 {
		t.Errorf("the abort wrote a dump: %v", files)
	}
	if got := e.xlen(t, r.stream); got != n {
		t.Fatalf("the stream holds %d entries after the abort, want %d", got, n)
	}
	if log := buf.String(); !strings.Contains(log, "held_by_workers") {
		t.Errorf("the abort's line does not name what this instance held:\n%s", log)
	}

	// Another life (a new identity: the claim needs claim_idle) drains them.
	dir2 := t.TempDir()
	next := launch(t, otherID, r.args(e, dir2), nil)
	next.waitReady()
	waitFor(t, 20*time.Second, "the next life to deliver every entry", func() bool {
		return sqliteCount(t, next.eventsDB(), "syslog_events") == n
	})
	if code := next.stop(); code != ExitOK {
		t.Errorf("next life exited %d", code)
	}
}

// SPEC §10.10.5: the per-user memory check of sink=clickhouse multiplies the
// worst chunk by the INSERTs this instance can have in flight -- the workers
// with a ring; with ring=none the receivers, up to max_inflight, of which the
// pool lets pool_size through -- and never counts the health check's own
// connection (independent review of the port, 2026-10-13).
func TestClickHouseWritersAreTheWorkersOrTheInflight(t *testing.T) {
	for _, tc := range []struct {
		ring                    string
		workers, pool, inflight int
		want                    int
	}{
		{config.RingModeMemory, 4, 5, 2048, 4},
		{config.RingModeSQLite, 3, 8, 2048, 3},
		{config.RingModeNone, 4, 5, 2048, 5},
		{config.RingModeNone, 4, 5, 2, 2},
	} {
		cfg := config.Defaults()
		cfg.Ring.Mode, cfg.Sink.Workers, cfg.SinkCH.PoolSize, cfg.Ring.MaxInflight = tc.ring, tc.workers, tc.pool, tc.inflight
		a := &App{cfg: cfg}
		if got := a.chWriters(); got != tc.want {
			t.Errorf("ring=%s workers=%d pool=%d max_inflight=%d: writers = %d, want %d",
				tc.ring, tc.workers, tc.pool, tc.inflight, got, tc.want)
		}
	}
}
