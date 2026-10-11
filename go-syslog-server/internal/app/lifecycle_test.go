package app

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
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
	args := append(baseArgs(dir), "--listen.udp=", "--ring.mode=memory")

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
// counts the loss exactly -- syslog_events_dropped_total{reason=
// "emergency_failed"} -- writes no file and exits 3.
func TestEmergencyDisabledCountsTheLoss(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	in := launch(t, testID, append(baseArgs(dir),
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
		"--admin.bind="+adminAddr, "--listen.udp=", "--listen.tcp="+tcpAddr,
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

// SPEC §2.3, §5.7, §13.3: while the sink is DOWN, UDP drops and counts
// (reason="sink_down"), and TCP stops reading -- the message is neither
// accepted nor dropped -- until the sink recovers, when it is delivered.
func TestSinkDownRefusesPerTransport(t *testing.T) {
	fs := newFaultySink(false)
	in := launch(t, testID, append(baseArgs(t.TempDir()),
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
	if _, err := os.Stat(in.eventsDB()); err != nil {
		t.Fatal(err)
	}
}
