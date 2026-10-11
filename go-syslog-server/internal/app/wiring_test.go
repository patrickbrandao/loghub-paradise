package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// Wiring promises of the assembled app that no single package can prove.

// SPEC §12.2, §13.1 step 9: "no boot, todo emg-*.bin é carregado". A dump left
// by a ring=memory life is reingested by the next boot whatever its ring mode:
// an operator who moves to ring=sqlite after a crash must not strand the
// messages the crash saved.
func TestDumpIsReingestedWhateverTheNextRingMode(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, testID, append(baseArgs(dir), "--listen.udp=", "--ring.mode=memory",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendLF(t, first.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - saved-by-the-dump")
	waitFor(t, 10*time.Second, "a held message", func() bool { return fs.attempts.Load() > 0 })
	if code := first.stop(); code != ExitSink {
		t.Fatalf("first life exited %d, want %d", code, ExitSink)
	}
	if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(d) != 1 {
		t.Fatalf("dumps = %v", d)
	}

	second := start(t, dir, "--listen.udp=", "--ring.mode=sqlite")
	rows := sqliteRows(t, second.eventsDB())
	if len(rows) != 1 || checkRow(t, rows[0]).Payload.message() != "saved-by-the-dump" {
		t.Errorf("the dump was not reingested by a ring=sqlite boot: %d rows", len(rows))
	}
	if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(d) != 0 {
		t.Errorf("the dump is still pending: %v", d)
	}
}

// SPEC §8, §16.3: the limiter keys on the src-ip, drops with
// reason="ratelimit" on TCP too, never applies to unix, and is switched on and
// off by SIGHUP.
func TestRateLimitAndItsReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	write := func(enabled string) {
		t.Helper()
		body := "[ratelimit]\nenabled = " + enabled + "\nrate = 0.001\nburst = 2\n"
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("true")
	sock := shortSocketPath(t)
	in := start(t, dir, "--config="+ini, "--listen.unix="+sock)
	m := in.app.metrics
	lines := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - %s-%d", prefix, i)
		}
		return out
	}

	sendLF(t, in.app.TransportAddr("tcp"), lines("limited", 5)...)
	waitFor(t, 10*time.Second, "two accepted and three dropped", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 2 && m.MessagesDroppedTotal[obs.DropRateLimit].Load() == 3
	})
	for i := 0; i < 4; i++ {
		sendUnix(t, sock, fmt.Sprintf("<13>Oct 11 22:14:15 localhost app: unix-%d", i))
	}
	waitFor(t, 10*time.Second, "unix not limited", func() bool { return m.MessagesAcceptedTotal[obs.TransportUnix].Load() == 4 })

	write("false")
	if r := in.app.reload(); strings.Join(r.applied, ",") != "ratelimit.enabled" {
		t.Fatalf("reload applied %v", r.applied)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines("free", 5)...)
	waitFor(t, 10*time.Second, "the limiter switched off", func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 7 })
	if d := m.MessagesDroppedTotal[obs.DropRateLimit].Load(); d != 3 {
		t.Errorf("ratelimit drops = %d after the limiter was switched off", d)
	}
	in.waitRows(2 + 4 + 5)
}

// SPEC §5.3, §5.6: an empty datagram or frame produces no envelope and is
// counted as reason="empty"; the frames around it are intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	in := start(t, t.TempDir())
	c, err := net.Dial("udp", in.app.TransportAddr("udp"))
	if err != nil {
		t.Fatal(err)
	}
	c.Write(nil)
	c.Close()
	tc := dialTCP(t, in.app.TransportAddr("tcp"))
	fmt.Fprint(tc, "<13>before\n\n\r\n<13>after\n")
	tc.Close()
	waitFor(t, 10*time.Second, "three empty drops and two rows", func() bool {
		return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() == 3 &&
			sqliteCount(t, in.eventsDB(), "syslog_events") == 2
	})
	envs := byMessage(t, sqliteRows(t, in.eventsDB()))
	if _, ok := envs["before"]; !ok {
		t.Errorf("before is missing: %v", keysOfEnv(envs))
	}
	if _, ok := envs["after"]; !ok {
		t.Errorf("after is missing: %v", keysOfEnv(envs))
	}
}

// SPEC §4.2, §4.4: listen.*_listeners > 1 opens N sockets with SO_REUSEPORT,
// and traffic on any of them arrives.
func TestSeveralListenersPerTransport(t *testing.T) {
	udp, tcp := freePort(t), freePort(t)
	in := start(t, t.TempDir(), "--listen.udp="+udp, "--listen.tcp="+tcp,
		"--listen.udp_listeners=2", "--listen.tcp_listeners=2")
	const n = 20
	for i := 0; i < n; i++ {
		sendUDP(t, udp, fmt.Sprintf("<13>udp-%d", i))
		sendLF(t, tcp, fmt.Sprintf("<13>tcp-%d", i))
	}
	waitFor(t, 10*time.Second, "every message", func() bool {
		m := in.app.metrics
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && m.MessagesAcceptedTotal[obs.TransportUDP].Load() == n
	})
	for _, r := range in.waitRows(2 * n) {
		checkRow(t, r)
	}
}

// SPEC §13.2: more refusals that are exit 1, before anything opens.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	p := newPKI(t)
	same := freePort(t)
	long := "/tmp/" + strings.Repeat("x", 120) + ".sock"
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"tcp and tls on the same address", []string{"--listen.tcp=" + same, "--listen.tls=" + same,
			"--tls.cert_file=" + p.serverCert, "--tls.key_file=" + p.serverKey}},
		{"tls without a certificate", []string{"--listen.tls=127.0.0.1:0"}},
		{"tls with an unreadable certificate", []string{"--listen.tls=127.0.0.1:0",
			"--tls.cert_file=/nonexistent/cert.pem", "--tls.key_file=/nonexistent/key.pem"}},
		{"client_auth=require without a CA", []string{"--listen.tls=127.0.0.1:0",
			"--tls.cert_file=" + p.serverCert, "--tls.key_file=" + p.serverKey, "--tls.client_auth=require"}},
		{"relative unix socket", []string{"--listen.unix=dev/log"}},
		{"unix socket above sun_path", []string{"--listen.unix=" + long}},
		{"max_message_bytes below 480", []string{"--syslog.max_message_bytes=479"}},
		{"unknown filter severity", []string{"--filter.min_severity=loud"}},
		{"zero drain_timeout", []string{"--shutdown.drain_timeout=0s"}},
		{"unknown flag", []string{"--listen.typo=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if code := RunContext(ctx, append(baseArgs(t.TempDir()), tc.args...), testID); code != ExitConfig {
				t.Fatalf("exit %d, want %d", code, ExitConfig)
			}
		})
	}
	// The control: the same building blocks, valid, boot and stop cleanly. A
	// refusal table without it would also pass if every boot were refused.
	t.Run("control", func(t *testing.T) {
		in := start(t, t.TempDir(), "--listen.tls=127.0.0.1:0", "--tls.cert_file="+p.serverCert,
			"--tls.key_file="+p.serverKey, "--tls.client_ca="+p.caFile, "--tls.client_auth=require",
			"--listen.unix="+shortSocketPath(t), "--syslog.max_message_bytes=480",
			"--filter.min_severity=warning", "--shutdown.drain_timeout=1s")
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}

// SPEC §9.7: ring=redis and sink=redis on the same server degrade the ring to
// memory, with a WARN -- the ring would protect nothing the destination does
// not lose together with it.
func TestRedisRingAndSinkOnTheSameServerDegrade(t *testing.T) {
	e := redisTest(t)
	const stream = "loghub.default.syslog"
	e.del(t, stream, stream+":healthprobe")
	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--ring.mode=redis", "--ring.redis.addrs="+e.addr,
		"--sink.type=redis", "--sink.redis.addrs="+e.addr), nil)
	in.waitReady()
	if mode := in.app.ring.Mode(); mode != "memory" {
		t.Errorf("effective ring %q, want memory", mode)
	}
	if got := in.metric(`syslog_config_info{ring_mode="memory",sink_type="redis",transports="udp,tcp"}`); got != "1" {
		t.Errorf("syslog_config_info = %s", got)
	}
	warned := false
	for _, w := range in.app.loaded.Warnings {
		warned = warned || strings.Contains(w, "ring")
	}
	if !warned {
		t.Errorf("no WARN about the degraded ring: %q", in.app.loaded.Warnings)
	}
	sendLF(t, in.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - degraded")
	waitFor(t, 10*time.Second, "the entry", func() bool { return e.xlen(t, stream) == 1 })
}

// compile-time guard: the decorator satisfies the optional interfaces it
// forwards, so wrapping a SQL sink keeps its columns and its worker ceiling.
var (
	_ sink.ColumnSink    = (*faultySink)(nil)
	_ sink.WorkerLimiter = (*faultySink)(nil)
)
