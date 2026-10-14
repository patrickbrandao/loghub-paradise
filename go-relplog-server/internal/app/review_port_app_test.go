package app

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// Regression tests of the independent review of go-syslog-server on
// 2026-10-13 that hold for this server (X1, X2, A3, A4 of that review's
// plan). Each failed here before its fix, or kills a mutation the suite used
// to survive.

// X1 (A1 there): a signal that lands after the reingestion and before the bind
// cancels the bind context, but a literal IP looks nothing up, so no bind
// fails: the server opened, /readyz said 200 and the shutdown that followed
// exited 0. TestSignalBeforeTheBindExits130 is the host-name case.
func TestSignalBeforeALiteralBindExits130(t *testing.T) {
	dir := t.TempDir()
	dumpOf(t, testID, dir, crashItems(3, "pre-bind"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// baseArgs binds listen.tcp to the literal 127.0.0.1:0.
	code := runContext(ctx, append(baseArgs(dir), "--ring.mode=memory"), testID, func(a *App) {
		a.abort = make(chan os.Signal, 1)
		a.wrapSink = func(s sink.Sink) sink.Sink { return cancelAfterWrite{Sink: s, cancel: cancel} }
	})
	if code != ExitAborted {
		t.Errorf("a signal before a literal bind exited %d, want %d", code, ExitAborted)
	}
}

// X2 (A2 there): abortLoss publishes aborted before it takes the dump's count.
// A dump that saw the abort in between -- it gives up (Store.SetAbandon), or,
// disabled, has nothing to write -- took the count first: emergency_failed=n
// and abort=0, the loss counted once but blamed on the disk. inAbort lands the
// dump right there; it waits for it, or, once the dump waits on lossMu (the
// fix, settleDump), gives up after a moment.
func TestAbortInsideTheDumpKeepsItsReason(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "abandoned dump"
		if disabled {
			name = "disabled dump"
		}
		t.Run(name, func(t *testing.T) {
			a, held := abortApp(t)
			if !disabled {
				a.emg = emergency.NewStore(t.TempDir(), testID.From)
				a.emg.SetAbandon(a.aborted.Load)
			}
			if !a.enterDumpPhase() {
				t.Fatal("no abort yet, the dump phase must start")
			}
			items := append(held, a.ring.Drain()...)
			if !a.oweDump(len(items)) {
				t.Fatal("no abort yet, the dump is owed")
			}
			done := make(chan struct{})
			a.inAbort = func() {
				go func() {
					defer close(done)
					a.dumpEmergency(items)
				}()
				select {
				case <-done:
				case <-time.After(300 * time.Millisecond):
				}
			}
			a.abortLoss()
			<-done
			failed := a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
			if got := aborted(a); got != uint64(len(items)) || failed != 0 {
				t.Fatalf("abort %d, emergency_failed %d for %d lost to the abort", got, failed, len(items))
			}
		})
	}
}

// A3: ring=sqlite with sink=none drained a previous life's rows into the
// discard -- messages a life with a destination had answered 200, a load test
// on the same volume after an incident (the dump's rule, SPEC §12.2). They
// stay in ring.db, outside this run's accounting, named by a WARN; a boot with
// a destination delivers them (ring.SQLiteConfig.KeepBacklog, as
// go-webhook-fast-server does).
func TestSinkNoneKeepsAPreviousLifesRingRows(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/ring.db"
	const n = 7
	r, err := ring.NewSQLite(ring.SQLiteConfig{Path: path, Synchronous: "OFF", CommitBatch: 64,
		CommitMaxDelay: time.Millisecond, BusyTimeout: time.Second, CapacityItems: 1000, CapacityBytes: 1 << 24},
		obs.NewLogger(os.Stderr, "text", "error").Logger)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range crashItems(n, "kept") {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()

	buf := &logBuf{}
	in := launch(t, testID, append(baseArgs(dir), "--ring.mode=sqlite", "--sink.type=none"), nil, captureLog(buf))
	in.waitReady()
	if items := in.app.ring.Stats().Items; items != 0 {
		t.Errorf("the ring counts %d with the previous life's rows set aside, want 0", items)
	}
	// This life's own messages still flow (and are discarded by sink=none).
	sendRELP(t, in.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - this-life")
	waitFor(t, 10*time.Second, "this life's row to leave ring.db", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == 1 && in.app.ring.Stats().Items == 0
	})
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if got := sqliteCount(t, path, "ring"); got != n {
		t.Fatalf("ring.db holds %d rows after a sink=none life, want the previous life's %d", got, n)
	}
	if !strings.Contains(buf.String(), "sink.type=none discards") {
		t.Errorf("no WARN named the rows set aside:\n%s", buf.String())
	}

	// A life with a destination delivers them.
	second := start(t, dir, "--ring.mode=sqlite")
	rows := second.waitRows(n)
	if len(rows) != n {
		t.Fatalf("the next life with a destination delivered %d of %d", len(rows), n)
	}
	second.stop()
}

// A4: with a persistent ring an abort loses nothing -- the rows (sqlite) or
// the entries (redis) were never deleted -- so nothing is counted as abort and
// nothing is dumped: the next life, or another replica, delivers them. Neither
// branch of abortLoss and gracefulShutdown for a persistent ring had a test
// (go-syslog-server review, 2026-10-13, M1/M2).
func TestAbortWithAPersistentRingCountsNothing(t *testing.T) {
	const n = 5
	run := func(t *testing.T, args []string, left func(in *instance) int) {
		fs := newFaultySink(true) // writes wait: the messages stay in the ring
		in := launch(t, testID, append(args, "--shutdown.drain_timeout=20s"), fs.wrap)
		in.waitReady()
		lines := make([]string, n)
		for i := range lines {
			lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - persist-%d", i)
		}
		sendRELP(t, in.app.TransportAddr("tcp"), lines...)
		waitFor(t, 10*time.Second, "a worker holding a batch", func() bool { return fs.attempts.Load() > 0 })
		in.cancel()                        // the first signal: phase 4 waits for the gated sink
		time.Sleep(300 * time.Millisecond) // well inside drain_timeout
		in.app.abort <- os.Interrupt       // the second signal
		if code := in.waitExit(10 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
		m := in.app.metrics
		if ab, failed := m.EventsDroppedTotal[obs.EventDropAbort].Load(),
			m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); ab != 0 || failed != 0 {
			t.Fatalf("abort %d, emergency_failed %d with a persistent ring, want 0 and 0", ab, failed)
		}
		if d := dumpFiles(t, in.app.cfg.Emergency.Dir, testID.From); len(d) != 0 {
			t.Fatalf("a persistent ring was dumped: %v", d)
		}
		if got := left(in); got != n {
			t.Fatalf("%d messages left in the ring after the abort, want %d", got, n)
		}
		fs.open()
	}
	t.Run("sqlite", func(t *testing.T) {
		dir := t.TempDir()
		run(t, append(baseArgs(dir), "--ring.mode=sqlite"), func(in *instance) int {
			return sqliteCount(t, in.app.cfg.RingSQL.Path, "ring")
		})
	})
	t.Run("redis", func(t *testing.T) {
		e := redisTest(t)
		r := newSharedRing(t, e)
		dir := t.TempDir()
		run(t, r.args(e, dir), func(*instance) int {
			return int(e.xlen(t, r.stream))
		})
	})
}

// The PublishAsyncMaxPending of the NATS sink is twice what this process can
// have in flight (the futures of a tainted chunk keep counting until their
// own timeout; go-relplog-nats-server review, 2026-10-13), and it never
// overflows: an unbounded ring.max_inflight wrapped the sum negative, and
// NewNATS silently fell back to 4096 (go-syslog-server review, 2026-10-13,
// C3; config bounds every term now, and the sum is capped besides).
func TestNATSMaxPendingIsDoubledAndBounded(t *testing.T) {
	c := config.Defaults()
	c.Sink.Workers, c.Sink.Batch, c.Ring.MaxInflight = 4, 256, 2048
	if got, want := natsMaxPending(c), 2*(4*256+2048); got != want {
		t.Errorf("natsMaxPending = %d, want %d", got, want)
	}
	c.Sink.Workers, c.Sink.Batch, c.Ring.MaxInflight = 1024, 1<<20, 1<<20
	if got := natsMaxPending(c); got <= 0 || got > math.MaxInt32 {
		t.Errorf("natsMaxPending at the config ceilings = %d, want a positive int32", got)
	}
}
