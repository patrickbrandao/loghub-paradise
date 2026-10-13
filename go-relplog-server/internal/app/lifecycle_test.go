package app

import (
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

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// Lifecycle promises beyond the happy path: the FATAL state (SPEC §13.3), the
// dump that cannot be written (§12.3), the boot that never finds its sink
// (§13.1, §13.5) and the sink that goes DOWN under traffic (§2.3, §5.7).

// SPEC §13.3, §12, §18.8 ("perda com FATAL do sink, memory: zero, via dump e
// reingestão"): a write answered with ClassFatal ends the process ON ITS OWN
// with exit 3; every accepted message -- the batch the worker held and the rest
// of the ring -- is in the dump, and the next life delivers them all.
func TestFatalWriteDumpsAndExitsThree(t *testing.T) {
	dir := t.TempDir()
	args := append(baseArgs(dir), "--ring.mode=memory")

	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	first := launch(t, testID, args, fs.wrap)
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
	dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(dumps[0])
	if err != nil || len(res.Items) != n {
		t.Fatalf("the dump holds %d of %d accepted messages (err %v)", len(res.Items), n, err)
	}

	second := launch(t, testID, args, nil)
	second.waitReady()
	rows := sqliteRows(t, filepath.Join(dir, "events.db"))
	if len(rows) != n {
		t.Fatalf("the next life delivered %d of %d", len(rows), n)
	}
	envs := byMessage(t, rows)
	for i := range lines {
		if _, ok := envs[fmt.Sprintf("fatal-%02d", i)]; !ok {
			t.Errorf("fatal-%02d was lost across the FATAL", i)
		}
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// SPEC §12.3: with emergency.enabled=false a shutdown that cannot deliver
// counts the loss exactly -- relplog_events_dropped_total{reason=
// "emergency_failed"} -- writes no file and exits 3.
func TestEmergencyDisabledCountsTheLoss(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, testID, append(baseArgs(dir),
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
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "*")); len(files) != 0 {
		t.Errorf("emergency.enabled=false wrote %v", files)
	}
}

// SPEC §13.1 steps 5, 7 and 12, §13.5: the admin port opens first and /readyz
// gives the reason while the sink is unreachable; no transport ever opens; at
// sink.boot_timeout the process exits 3.
func TestUnreachableSinkKeepsTransportsShut(t *testing.T) {
	adminAddr, tcpAddr, natsAddr := freePort(t), freePort(t), freePort(t)
	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--admin.bind="+adminAddr, "--listen.tcp="+tcpAddr,
		"--sink.type=nats", "--sink.nats.urls=nats://"+natsAddr,
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

// SPEC §2.3, §5.4, §5.7, §13.3: while the sink is DOWN a transaction is
// HELD -- read, neither accepted nor answered, never a 500 -- and gets its
// "200 OK" when the sink recovers. Held past relp.backpressure_timeout, the
// session ends with serverclose and the transaction WITHOUT an answer, which
// is what makes librelp reconnect and retransmit it: counted in
// relplog_relp_unanswered_total{reason="backpressure"}, never as a drop, and
// delivered once when it comes back.
func TestSinkDownHoldsTheTransaction(t *testing.T) {
	fs := newFaultySink(false)
	in := launch(t, testID, append(baseArgs(t.TempDir()),
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
	if e := checkRow(t, in.waitRows(1)[0]); e.Payload.message() != "held-while-down" {
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
	for _, d := range []obs.DropReason{obs.DropFilter, obs.DropEmpty, obs.DropUnaddressable} {
		if n := m.MessagesDroppedTotal[d].Load(); n != 0 {
			t.Errorf("relplog_messages_dropped_total{reason=%q} = %d: an unanswered transaction is not a drop", d, n)
		}
	}

	// What librelp does next: a new session with the same message.
	up()
	sendRELP(t, tcp, retransmit)
	rows := in.waitRows(2)
	n := 0
	for _, r := range rows {
		if checkRow(t, r).Payload.message() == "unanswered-then-retransmitted" {
			n++
		}
	}
	if n != 1 || len(rows) != 2 {
		t.Errorf("%d rows, %d of the retransmitted message; want 2 and 1", len(rows), n)
	}
	if _, err := os.Stat(in.eventsDB()); err != nil {
		t.Fatal(err)
	}
}

// SPEC §13.1 step 7, §22.1 item 27: a refusal WaitReady does not retry ends
// the boot at once, and its last line says so instead of blaming
// sink.boot_timeout and the network. The control -- a sink that is merely
// unreachable -- still gets the boot_timeout line, so the split is proven and
// not just one message replaced by another.
func TestBootRefusalLogNamesItsCause(t *testing.T) {
	t.Run("fatal refusal", func(t *testing.T) {
		var buf logBuf
		fs := newFaultySink(false)
		fs.fatalHealth.Store(true)
		in := launch(t, testID, append(baseArgs(t.TempDir()), "--sink.boot_timeout=120s"), fs.wrap, captureLog(&buf))
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
		in := launch(t, testID, append(baseArgs(t.TempDir()), "--sink.boot_timeout=1s"), fs.wrap, captureLog(&buf))
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

// fatalShutdownInDump boots ring=memory with a destination that answers every
// write with ClassFatal, accepts n messages and lets the FATAL shutdown run
// until it is about to write the emergency dump, where it is held until the
// returned release is called.
func fatalShutdownInDump(t *testing.T, n int) (*instance, string, func()) {
	t.Helper()
	dir := t.TempDir()
	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	in := launch(t, testID, append(baseArgs(dir), "--ring.mode=memory"), fs.wrap, func(a *App) {
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
	return in, dir, releaseFn
}

// settle lets the shutdown an abort left behind finish, and checks that the
// dump it was writing was abandoned: the messages were counted lost by the
// abort, so no file may reach the disk to be reingested at the next boot
// (emergency.Store.SetAbandon, ported from go-relplog-nats-server, 2026-10-13).
// In the binary the abort's os.Exit usually ends it first.
func settle(t *testing.T, dir string, release func()) {
	t.Helper()
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		// A temporary may exist for an instant before the dump gives up; the
		// final name never may.
		if f, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(f) > 0 {
			t.Fatalf("an aborted dump reached the disk: %v", f)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SPEC §13.3, §13.4, §22.1 items 28 and 29. A FATAL shutdown starts with no
// signal, so the first SIGTERM/SIGINT during it (a docker stop while the dump
// is written) is tolerated: the dump completes and the exit is 3. A second
// one aborts; SIGQUIT aborts at once. An abort that lands while the dump is
// being written loses the WHOLE dump -- the ring's own count was already
// committed by Drain -- and counts all of it as reason="abort".
func TestFatalShutdownSignals(t *testing.T) {
	const n = 9
	dumps := func(dir string) []string {
		d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
		return d
	}
	t.Run("first SIGTERM is tolerated", func(t *testing.T) {
		in, dir, release := fatalShutdownInDump(t, n)
		in.app.abort <- syscall.SIGTERM
		if code := in.waitExit(500 * time.Millisecond); code != -1 {
			t.Fatalf("the first signal of a FATAL shutdown ended it with %d", code)
		}
		release()
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d: the FATAL shutdown must reach its dump", code, ExitSink)
		}
		if d := dumps(dir); len(d) != 1 {
			t.Fatalf("dump files = %v, want 1", d)
		}
		res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(dumps(dir)[0])
		if err != nil || len(res.Items) != n {
			t.Fatalf("the dump holds %d of %d messages (%v)", len(res.Items), n, err)
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != 0 {
			t.Errorf(`relplog_events_dropped_total{reason="abort"} = %d after a dump that completed`, d)
		}
	})
	t.Run("second SIGTERM aborts and counts the dump", func(t *testing.T) {
		in, dir, release := fatalShutdownInDump(t, n)
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
		in, dir, release := fatalShutdownInDump(t, n)
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
