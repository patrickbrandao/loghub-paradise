package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ring"
)

// Lifecycle promises beyond the happy path: the FATAL state (SPEC §13.3), the
// dump that cannot be written (§12.3), the boot that never finds its sink
// (§13.1, §13.5) and the sink that goes DOWN under traffic (§2.3, §5.7).

// SPEC §13.3, §12, §18.8 ("perda com FATAL do sink, memory: zero, via dump e
// reingestão"): a write answered with ClassFatal ends the process ON ITS OWN
// with exit 3; every accepted message -- the batch the worker held and the rest
// of the ring -- is in the dump as a valid JLH line, and the next life delivers
// them all, with the same uuids, and removes the file.
func TestFatalWriteDumpsAndExitsThree(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	args := f.args(dir)

	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	first := launch(t, f.id, args, fs.wrap)
	first.waitReady()
	const n = 12
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - fatal-%02d", i)
	}
	sendRELP(t, first.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	fs.open()
	// No cancel: the FATAL state alone must end the process.
	if code := first.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit %d after a fatal write, want %d", code, ExitSink)
	}
	if got := first.app.metrics.SinkErrorsTotal[obs.SinkErrFatal].Load(); got == 0 {
		t.Error(`relplog_sink_errors_total{kind="fatal"} did not count the fatal write`)
	}
	dumps := dumpFiles(t, first.emgDir(), f.id.From)
	if len(dumps) != 1 {
		all, _ := os.ReadDir(first.emgDir())
		t.Fatalf("dump files = %v (dir %v), want one emg-%s-*.jsonl", dumps, all, f.id.From)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}
	if f.count() != 0 {
		t.Fatalf("the fatal sink published messages")
	}

	second := launch(t, f.id, args, nil)
	second.waitReady()
	msgs := f.msgs()
	checkReingested(t, dumped, msgs)
	envs := byMessage(t, msgs)
	for i := range lines {
		if _, ok := envs[fmt.Sprintf("fatal-%02d", i)]; !ok {
			t.Errorf("fatal-%02d was lost across the FATAL", i)
		}
	}
	if left := dumpFiles(t, second.emgDir(), f.id.From); len(left) != 0 {
		t.Errorf("the reingested dump is still pending: %v", left)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// SPEC §12.3: with emergency.enabled=false a shutdown that cannot deliver
// counts the loss exactly -- relplog_events_dropped_total{reason=
// "emergency_failed"} -- writes no file and exits 3.
func TestEmergencyDisabledCountsTheLoss(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, f.id, f.args(dir,
		"--emergency.enabled=false",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	in.waitReady()
	const n = 6
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - lost-%d", i)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted and a failed write", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && fs.attempts.Load() > 0
	})
	if code := in.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if got := m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); got != n {
		t.Errorf(`relplog_events_dropped_total{reason="emergency_failed"} = %d, want %d (SPEC §12.3)`, got, n)
	}
	if got := m.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 0 {
		t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d: the loss is emergency_failed's, counted once`, got)
	}
	if files, _ := filepath.Glob(filepath.Join(in.emgDir(), "*")); len(files) != 0 {
		t.Errorf("emergency.enabled=false wrote %v", files)
	}
}

// SPEC §13.1 steps 5, 7 and 11, §13.5: the admin port opens first and /readyz
// gives the reason while the sink is unreachable; no transport ever opens; at
// sink.boot_timeout the process exits 3. Nothing listens on the NATS address:
// a refused dial is a network failure, retried until the deadline. It needs
// no NATS at all.
func TestUnreachableSinkKeepsTransportsShut(t *testing.T) {
	adminAddr, tcpAddr, natsAddr := freePort(t), freePort(t), freePort(t)
	in := launch(t, testID, append(localArgs(t.TempDir()),
		"--admin.bind="+adminAddr, "--listen.tcp="+tcpAddr,
		"--sink.nats.urls=nats://"+natsAddr,
		"--sink.boot_timeout=3s", "--sink.op_timeout=500ms"), nil)

	tcpShut := func() bool {
		c, err := net.DialTimeout("tcp", tcpAddr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			return false
		}
		return true
	}
	var reason string
	waitFor(t, 10*time.Second, "/readyz 503 with the boot reason", func() bool {
		resp, err := http.Get("http://" + adminAddr + "/readyz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		reason = string(b)
		return resp.StatusCode == 503 && strings.Contains(reason, "sink")
	})
	if !tcpShut() {
		t.Error("a transport accepted a connection while the sink was unreachable")
	}
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d (boot_timeout)", code, ExitSink)
	}
	if !tcpShut() {
		t.Error("a transport is open after the refused boot")
	}
	t.Logf("/readyz while booting: %q", strings.TrimSpace(reason))
}

// noDrops says that nothing was counted as a drop or handed back: a held
// transaction is neither.
func noDrops(t *testing.T, m *obs.Metrics) {
	t.Helper()
	for _, d := range []obs.DropReason{obs.DropEmpty, obs.DropUnaddressable} {
		if n := m.MessagesDroppedTotal[d].Load(); n != 0 {
			t.Errorf("relplog_messages_dropped_total{reason=%q} = %d, want 0", d, n)
		}
	}
	for _, r := range []obs.RELPUnanswered{obs.UnansweredBackpressure, obs.UnansweredShutdown, obs.UnansweredConnLost} {
		if n := m.RELPUnansweredTotal[r].Load(); n != 0 {
			t.Errorf("relplog_relp_unanswered_total[%d] = %d, want 0", r, n)
		}
	}
}

// SPEC §2.3, §5.4, §5.7, §13.3: while the sink is DOWN a transaction is
// HELD -- read, neither accepted nor answered, never a 500 -- and gets its
// "200 OK" when the sink recovers. Held past relp.backpressure_timeout, the
// session ends with serverclose and the transaction WITHOUT an answer, which
// is what makes librelp reconnect and retransmit it: counted in
// relplog_relp_unanswered_total{reason="backpressure"}, never as a drop, and
// delivered once when it comes back.
func TestSinkDownHoldsTheTransaction(t *testing.T) {
	f := newFixture(t)
	fs := newFaultySink(false)
	in := launch(t, f.id, f.args(t.TempDir(),
		"--relp.backpressure_timeout=2s",
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	down := func() {
		t.Helper()
		fs.failHealth.Store(true)
		waitFor(t, 10*time.Second, "the sink DOWN", func() bool { return m.SinkUp.Load() == 0 })
	}
	up := func() {
		t.Helper()
		fs.failHealth.Store(false)
		waitFor(t, 10*time.Second, "the sink UP", func() bool { return m.SinkUp.Load() == 1 })
	}

	// --- held, then answered ------------------------------------------------
	down()
	rc := dialRELP(t, tcp)
	ids := rc.syslog("<13>1 2026-09-12T14:33:02Z host app - - - held-while-down")
	waitFor(t, 10*time.Second, "the transaction read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() == 1 })
	rc.expectNothing(300 * time.Millisecond)
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 0 {
		t.Fatalf("while DOWN: %d accepted; the transaction must be held", a)
	}
	noDrops(t, m)
	up()
	if got := rc.responses(ids); got[0] != relpRSPOK {
		t.Fatalf("after the recovery the held transaction got %q, want %q", got[0], relpRSPOK)
	}
	rc.close()
	if e := checkMsg(t, f.waitMsgs(1)[0]); e.Payload.message() != "held-while-down" {
		t.Errorf("delivered %q", e.Payload.message())
	}

	// --- held past relp.backpressure_timeout: no answer, serverclose -------
	down()
	rc = dialRELP(t, tcp)
	retransmit := "<13>1 2026-09-12T14:33:03Z host app - - - unanswered-then-retransmitted"
	rc.syslog(retransmit)
	began := time.Now()
	rc.expectServerClose(10 * time.Second)
	if took := time.Since(began); took < 1500*time.Millisecond {
		t.Errorf("the session ended after %s, before relp.backpressure_timeout", took)
	}
	if n := m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load(); n != 1 {
		t.Errorf(`relplog_relp_unanswered_total{reason="backpressure"} = %d, want 1`, n)
	}
	if got := in.metric(`relplog_relp_unanswered_total{reason="backpressure"}`); got != "1" {
		t.Errorf(`/metrics: relplog_relp_unanswered_total{reason="backpressure"} = %s, want 1`, got)
	}
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 1 {
		t.Errorf("%d accepted; the unanswered transaction must not be in the ring", a)
	}
	for _, d := range []obs.DropReason{obs.DropEmpty, obs.DropUnaddressable} {
		if n := m.MessagesDroppedTotal[d].Load(); n != 0 {
			t.Errorf("relplog_messages_dropped_total{reason=%q} = %d: an unanswered transaction is not a drop", d, n)
		}
	}
	if n := m.RELPResponsesTotal[obs.RELPCode500].Load(); n != 0 {
		t.Errorf("%d answers of 500 for a DOWN sink (SPEC §5.4)", n)
	}

	// What librelp does next: a new session with the same message.
	up()
	sendRELP(t, tcp, retransmit)
	msgs := f.waitMsgs(2)
	n := 0
	for _, msg := range msgs {
		if checkMsg(t, msg).Payload.message() == "unanswered-then-retransmitted" {
			n++
		}
	}
	if n != 1 || len(msgs) != 2 {
		t.Errorf("%d messages, %d of the retransmitted one; want 2 and 1", len(msgs), n)
	}
}

// SPEC §13.1 step 7, go-relplog-server §22.1 item 27: a refusal WaitReady
// does not retry ends the boot at once, and its last line says so instead of
// blaming sink.boot_timeout and the network. The control -- a sink that is
// merely unreachable -- still gets the boot_timeout line, so the split is
// proven and not just one message replaced by another. Neither needs a NATS:
// the faulty sink answers the health check itself.
func TestBootRefusalLogNamesItsCause(t *testing.T) {
	t.Run("fatal refusal", func(t *testing.T) {
		var buf logBuf
		fs := newFaultySink(false)
		fs.fatalHealth.Store(true)
		in := launch(t, testID, append(localArgs(t.TempDir()), "--sink.boot_timeout=120s"), fs.wrap, captureLog(&buf))
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		log := buf.String()
		if !strings.Contains(log, "the sink refused this server permanently") {
			t.Errorf("no line naming the permanent refusal:\n%s", log)
		}
		if strings.Contains(log, "within sink.boot_timeout") {
			t.Errorf("a refusal that was never retried is reported as a boot_timeout:\n%s", log)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		var buf logBuf
		fs := newFaultySink(false)
		fs.failHealth.Store(true)
		in := launch(t, testID, append(localArgs(t.TempDir()), "--sink.boot_timeout=1s"), fs.wrap, captureLog(&buf))
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		log := buf.String()
		if !strings.Contains(log, "sink did not become usable within sink.boot_timeout") ||
			strings.Contains(log, "refused this server permanently") {
			t.Errorf("an unreachable sink must be reported as the boot_timeout:\n%s", log)
		}
	})
}

// fatalShutdownInDump boots with a destination that answers every write with
// ClassFatal, accepts n messages and lets the FATAL shutdown run until it is
// about to write the emergency dump, where it is held until the returned
// release is called.
func fatalShutdownInDump(t *testing.T, n int) (*instance, *fixture, string, func()) {
	t.Helper()
	f := newFixture(t)
	dir := t.TempDir()
	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	in := launch(t, f.id, f.args(dir), fs.wrap, func(a *App) {
		a.beforeDump = func() {
			close(entered)
			<-release
		}
	})
	in.waitReady()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - in-dump-%02d", i)
	}
	// The worker takes the first message alone and blocks on the gate; the
	// rest wait in the ring. That is the shape that makes the abort's count
	// wrong without App.dumping: Drain commits what it takes out of the ring,
	// so during the dump the ring's own count is only the worker's batch.
	tcp := in.app.TransportAddr("tcp")
	sendRELP(t, tcp, lines[0])
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	sendRELP(t, tcp, lines[1:]...)
	fs.open()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the FATAL shutdown never reached the emergency dump")
	}
	return in, f, dir, releaseFn
}

// settle lets the shutdown an abort left behind run on, and proves its dump is
// abandoned: the messages were counted lost by the abort, so no file may reach
// the disk to be reingested at the next boot (emergency.Store.SetAbandon,
// independent review of 2026-10-13). In the binary the abort's os.Exit
// usually ends it first.
func settle(t *testing.T, dir string, release func()) {
	t.Helper()
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "*emg-*")); len(d) > 0 {
			// A temporary may exist for an instant before the dump gives up;
			// the final name never may.
			if f, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.jsonl")); len(f) > 0 {
				t.Fatalf("an aborted dump reached the disk: %v", f)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SPEC §13.3, §13.4 (go-relplog-server §22.1 items 28 and 29). A FATAL
// shutdown starts with no signal, so the first SIGTERM/SIGINT during it (a
// docker stop while the dump is written) is tolerated: the dump completes and
// the exit is 3. A second one aborts; SIGQUIT aborts at once -- it is never
// tolerated (pendência 11 of go-syslog-nats-server). An abort that lands while
// the dump is being written loses the WHOLE dump -- the ring's own count was
// already committed by Drain -- and counts all of it as reason="abort".
func TestFatalShutdownSignals(t *testing.T) {
	const n = 9
	t.Run("first SIGTERM is tolerated", func(t *testing.T) {
		in, f, _, release := fatalShutdownInDump(t, n)
		in.app.abort <- syscall.SIGTERM
		if code := in.waitExit(500 * time.Millisecond); code != -1 {
			t.Fatalf("the first signal of a FATAL shutdown ended it with %d", code)
		}
		release()
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d: the FATAL shutdown must reach its dump", code, ExitSink)
		}
		d := dumpFiles(t, in.emgDir(), f.id.From)
		if len(d) != 1 {
			t.Fatalf("dump files = %v, want 1", d)
		}
		if got := dumpLines(t, d[0]); len(got) != n {
			t.Fatalf("the dump holds %d of %d messages", len(got), n)
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != 0 {
			t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d after a dump that completed`, d)
		}
	})
	t.Run("second SIGTERM aborts and counts the dump", func(t *testing.T) {
		in, _, dir, release := fatalShutdownInDump(t, n)
		defer settle(t, dir, release)
		in.app.abort <- syscall.SIGTERM
		in.app.abort <- syscall.SIGINT
		if code := in.waitExit(5 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
			t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d, want the %d being dumped`, d, n)
		}
	})
	t.Run("SIGQUIT aborts at once", func(t *testing.T) {
		in, _, dir, release := fatalShutdownInDump(t, n)
		defer settle(t, dir, release)
		in.app.abort <- syscall.SIGQUIT
		if code := in.waitExit(5 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d: SIGQUIT is never tolerated", code, ExitAborted)
		}
		m := in.app.metrics
		if d := m.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
			t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d, want the %d being dumped`, d, n)
		}
		if d := m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); d != 0 {
			t.Errorf(`relplog_events_dropped_total{reason="emergency_failed"} = %d`, d)
		}
	})
}

// Review A #3: Drain commits the ring's accounting before the dump, so an
// abort that lands while the dump is written read zero and counted nothing,
// while os.Exit cut the dump short. The leftovers handed to the dump are what
// such an abort loses; once the dump finished, an abort loses nothing more.
func TestAbortDuringTheDumpCountsTheLeftovers(t *testing.T) {
	a := &App{log: obs.NewLogger(io.Discard, "text", "info"), metrics: obs.NewMetrics(obs.Info{})}
	a.ring = ring.NewMemory(100, 1<<20, 0)
	for i := 0; i < 7; i++ {
		_ = a.ring.Enqueue(context.Background(), item.New(item.Head{}, make([]byte, 10), -1))
	}
	leftover := a.ring.Drain()
	// Control: without the dump's count, the ring alone says nothing is lost.
	a.abortLoss()
	if got := a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 0 {
		t.Fatalf("control: abort counted %d before the dump started", got)
	}
	// The control was another process's abort: one abort counts once.
	a.aborted.Store(false)
	a.dumping.Store(int64(len(leftover)))
	a.dumpPhase.Store(true) // the shutdown collected the leftovers (SPEC §13.4)
	a.abortLoss()
	if got := a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); got != uint64(len(leftover)) {
		t.Fatalf("abort counted %d of the %d messages whose dump was cut short", got, len(leftover))
	}
}

// Review A #3: a FATAL shutdown starts without any signal, so the first one
// that arrives during it is not the second of SPEC §13.4: the dump goes on.
// The second one aborts.
func TestFatalShutdownToleratesTheFirstSignal(t *testing.T) {
	a := &App{log: obs.NewLogger(io.Discard, "text", "info"), metrics: obs.NewMetrics(obs.Info{})}
	a.ring = ring.NewMemory(10, 1<<20, 0)
	abort := make(chan os.Signal, 2)
	release := make(chan struct{})
	done := make(chan int, 1)
	go func() { done <- a.shutdownWith(func() int { <-release; return ExitSink }, true, abort) }()

	abort <- syscall.SIGTERM
	select {
	case code := <-done:
		t.Fatalf("the first signal of a FATAL shutdown aborted it (exit %d)", code)
	case <-time.After(100 * time.Millisecond):
	}
	abort <- syscall.SIGTERM
	if code := <-done; code != ExitAborted {
		t.Fatalf("second signal: exit %d, want %d", code, ExitAborted)
	}
	close(release)

	// Control: a graceful shutdown (not FATAL) aborts on the signal it gets,
	// which is already the second one.
	release2 := make(chan struct{})
	defer close(release2)
	go func() { done <- a.shutdownWith(func() int { <-release2; return ExitOK }, false, abort) }()
	abort <- syscall.SIGTERM
	if code := <-done; code != ExitAborted {
		t.Fatalf("graceful: exit %d, want %d", code, ExitAborted)
	}
}
