package app

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
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
	dir := t.TempDir()
	args := append(baseArgs(t, dir), "--listen.udp=")

	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	first := launch(t, testID, args, fs.wrap)
	first.waitReady()
	const n = 12
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - fatal-%02d", i)
	}
	sendLF(t, first.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	fs.open()
	// No cancel: the FATAL state alone must end the process.
	if code := first.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit %d after a fatal write, want %d", code, ExitSink)
	}
	if got := first.app.metrics.SinkErrorsTotal[obs.SinkErrFatal].Load(); got == 0 {
		t.Error(`syslog_sink_errors_total{kind="fatal"} did not count the fatal write`)
	}
	dumps := dumpFiles(t, first.emgDir(), testID.From)
	if len(dumps) != 1 {
		all, _ := os.ReadDir(first.emgDir())
		t.Fatalf("dump files = %v (dir %v), want one emg-%s-*.jsonl", dumps, all, testID.From)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}
	if first.count() != 0 {
		t.Fatalf("the fatal sink stored rows")
	}

	second := launch(t, testID, args, nil)
	second.waitReady()
	rows := second.rows()
	checkReingested(t, dumped, rows)
	envs := byMessage(t, rows)
	for i := range lines {
		if _, ok := envs[fmt.Sprintf("fatal-%02d", i)]; !ok {
			t.Errorf("fatal-%02d was lost across the FATAL", i)
		}
	}
	if left := dumpFiles(t, second.emgDir(), testID.From); len(left) != 0 {
		t.Errorf("the reingested dump is still pending: %v", left)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// SPEC §12.3: with emergency.enabled=false a shutdown that cannot deliver
// counts the loss exactly -- syslog_events_dropped_total{reason=
// "emergency_failed"} -- writes no file and exits 3.
func TestEmergencyDisabledCountsTheLoss(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, testID, append(baseArgs(t, dir),
		"--listen.udp=", "--emergency.enabled=false",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	in.waitReady()
	const n = 6
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - lost-%d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted and a failed write", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && fs.attempts.Load() > 0
	})
	if code := in.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if got := m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); got != n {
		t.Errorf(`syslog_events_dropped_total{reason="emergency_failed"} = %d, want %d (SPEC §12.3)`, got, n)
	}
	if got := m.EventsDroppedTotal[obs.EventDropAbort].Load(); got != 0 {
		t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d: the loss is emergency_failed's, counted once`, got)
	}
	if files, _ := filepath.Glob(filepath.Join(in.emgDir(), "*")); len(files) != 0 {
		t.Errorf("emergency.enabled=false wrote %v", files)
	}
}

// SPEC §13.1 steps 5, 7 and 11, §13.5: the admin port opens first and /readyz
// gives the reason while the sink is unreachable; no transport ever opens; at
// sink.boot_timeout the process exits 3. Nothing listens on the PostgreSQL
// address: a refused dial is a network failure, retried until the deadline.
func TestUnreachableSinkKeepsTransportsShut(t *testing.T) {
	adminAddr, tcpAddr, pgAddr := freePort(t), freePort(t), freePort(t)
	in := launch(t, testID, append(localArgs(t.TempDir()),
		"--admin.bind="+adminAddr, "--listen.udp=", "--listen.tcp="+tcpAddr,
		"--sink.postgres.addr="+pgAddr,
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

// SPEC §2.3, §5.7, §13.3: while the sink is DOWN, UDP drops and counts
// (reason="sink_down"), and TCP stops reading -- the message is neither
// accepted nor dropped -- until the sink recovers, when it is delivered.
func TestSinkDownRefusesPerTransport(t *testing.T) {
	fs := newFaultySink(false)
	in := launch(t, testID, append(baseArgs(t, t.TempDir()),
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000"), fs.wrap)
	in.waitReady()
	m := in.app.metrics

	fs.failHealth.Store(true)
	waitFor(t, 10*time.Second, "the sink DOWN", func() bool { return m.SinkUp.Load() == 0 })

	sendUDP(t, in.app.TransportAddr("udp"), "<13>1 2026-09-12T14:33:01Z host app - - - udp-while-down")
	waitFor(t, 10*time.Second, "the UDP drop", func() bool {
		return m.MessagesDroppedTotal[obs.DropSinkDown].Load() == 1
	})

	c := dialTCP(t, in.app.TransportAddr("tcp"))
	defer c.Close()
	fmt.Fprint(c, "<13>1 2026-09-12T14:33:02Z host app - - - tcp-while-down\n")
	waitFor(t, 10*time.Second, "the TCP message read", func() bool {
		return m.MessagesTotal[obs.TransportTCP].Load() == 1
	})
	if a, d := m.MessagesAcceptedTotal[obs.TransportTCP].Load(), m.MessagesDroppedTotal[obs.DropSinkDown].Load(); a != 0 || d != 1 {
		t.Fatalf("while DOWN: tcp accepted %d, sink_down drops %d; TCP must hold, not accept nor drop", a, d)
	}

	fs.failHealth.Store(false)
	rows := in.waitRows(1)
	if e := checkRow(t, rows[0]); e.Payload.message() != "tcp-while-down" {
		t.Errorf("delivered %q", e.Payload.message())
	}
	if d := m.MessagesDroppedTotal[obs.DropSinkDown].Load(); d != 1 {
		t.Errorf("sink_down drops = %d, want only the UDP one", d)
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
