package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
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
	first := launch(t, testID, append(baseArgs(dir), "--ring.mode=memory",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendRELP(t, first.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - saved-by-the-dump")
	waitFor(t, 10*time.Second, "a held message", func() bool { return fs.attempts.Load() > 0 })
	if code := first.stop(); code != ExitSink {
		t.Fatalf("first life exited %d, want %d", code, ExitSink)
	}
	if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(d) != 1 {
		t.Fatalf("dumps = %v", d)
	}

	second := start(t, dir, "--ring.mode=sqlite")
	rows := sqliteRows(t, second.eventsDB())
	if len(rows) != 1 || checkRow(t, rows[0]).Payload.message() != "saved-by-the-dump" {
		t.Errorf("the dump was not reingested by a ring=sqlite boot: %d rows", len(rows))
	}
	if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(d) != 0 {
		t.Errorf("the dump is still pending: %v", d)
	}
}

// SPEC §5.7, §8, §16.3: the limiter keys on the src-ip; above the burst the
// transaction is HELD waiting for a token -- the emitter is slowed down, never
// refused (a 500 would drop it at the emitter and break its session) -- and a
// SIGHUP that switches the limiter off releases it with its 200.
func TestRateLimitAndItsReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "relplog.ini")
	write := func(enabled string) {
		t.Helper()
		body := "[ratelimit]\nenabled = " + enabled + "\nrate = 0.001\nburst = 2\n"
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("true")
	in := start(t, dir, "--config="+ini, "--relp.backpressure_timeout=1m")
	m := in.app.metrics
	lines := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - %s-%d", prefix, i)
		}
		return out
	}

	// One transaction at a time: the window decides concurrent ones in any
	// order, and which two get the tokens would not be fixed.
	rc := dialRELP(t, in.app.TransportAddr("tcp"))
	limited := lines("limited", 3)
	for _, l := range limited[:2] {
		if got := rc.transact(l); len(got) != 1 || got[0] != relpRSPOK {
			t.Fatalf("within the burst: %q", got)
		}
	}
	held := rc.syslog(limited[2])
	rc.expectNothing(300 * time.Millisecond)
	if v := m.RateLimitHeldTotal.Load(); v != 1 {
		t.Fatalf("relplog_ratelimit_held_total = %d, want 1", v)
	}

	write("false")
	if r := in.app.reload(); strings.Join(r.applied, ",") != "ratelimit.enabled" {
		t.Fatalf("reload applied %v", r.applied)
	}
	if got := rc.responses(held); got[0] != relpRSPOK {
		t.Fatalf("the held transaction was answered %q after the limiter was switched off", got[0])
	}
	rc.close()
	sendRELP(t, in.app.TransportAddr("tcp"), lines("free", 5)...)
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 8 {
		t.Errorf("accepted %d, want 8: nothing above the limit is lost", a)
	}
	for r := range m.MessagesDroppedTotal {
		if d := m.MessagesDroppedTotal[r].Load(); d != 0 {
			t.Errorf("messages_dropped{%s} = %d", obs.DropReason(r), d)
		}
	}
	if v := m.RELPResponsesTotal[obs.RELPCode500].Load(); v != 0 {
		t.Errorf("%d 500 responses for a rate limit", v)
	}
	envs := byMessage(t, in.waitRows(3+5))
	for _, k := range []string{"limited-0", "limited-2", "free-0", "free-4"} {
		if _, ok := envs[k]; !ok {
			t.Errorf("%s is missing: %v", k, keysOfEnv(envs))
		}
	}
}

// SPEC §5.4: an empty syslog frame produces no envelope, is answered
// 200 (there is nothing to keep, and a 500 would make rsyslog log an error)
// and is counted as reason="empty"; the frames around it are intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	in := start(t, t.TempDir())
	sendRELP(t, in.app.TransportAddr("tcp"), "<13>before", "", "<13>after")
	if d := in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load(); d != 1 {
		t.Errorf(`relplog_messages_dropped_total{reason="empty"} = %d, want 1`, d)
	}
	envs := byMessage(t, in.waitRows(2))
	if _, ok := envs["before"]; !ok {
		t.Errorf("before is missing: %v", keysOfEnv(envs))
	}
	if _, ok := envs["after"]; !ok {
		t.Errorf("after is missing: %v", keysOfEnv(envs))
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if n := len(sqliteRows(t, in.eventsDB())); n != 2 {
		t.Errorf("%d rows, want 2: the empty frame must not become one", n)
	}
}

// SPEC §4.2, §4.4: listen.*_listeners > 1 opens N sockets with SO_REUSEPORT,
// and sessions on any of them arrive, in clear and over TLS.
func TestSeveralListenersPerTransport(t *testing.T) {
	p := newPKI(t)
	tcp, tlsAddr := freePort(t), freePort(t)
	in := start(t, t.TempDir(), "--listen.tcp="+tcp, "--listen.tcp_listeners=2",
		"--listen.tls="+tlsAddr, "--listen.tls_listeners=2",
		"--tls.cert_file="+p.serverCert, "--tls.key_file="+p.serverKey)
	const n = 20
	for i := 0; i < n; i++ {
		sendRELP(t, tcp, fmt.Sprintf("<13>tcp-%d", i))
		rc := dialRELPTLS(t, tlsAddr, &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1"})
		rc.sendOK(fmt.Sprintf("<13>tls-%d", i))
		rc.close()
	}
	m := in.app.metrics
	if a, b := m.MessagesAcceptedTotal[obs.TransportTCP].Load(), m.MessagesAcceptedTotal[obs.TransportTLS].Load(); a != n || b != n {
		t.Errorf("accepted tcp %d, tls %d; want %d each", a, b, n)
	}
	for _, r := range in.waitRows(2 * n) {
		checkRow(t, r)
	}
}

// SPEC §13.2: more refusals that are exit 1, before anything opens.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	p := newPKI(t)
	same := freePort(t)
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
		{"zero relp.backpressure_timeout", []string{"--relp.backpressure_timeout=0s"}},
		{"relp.window of zero", []string{"--relp.window=0"}},
		{"relp.max_frame_bytes below max_message_bytes", []string{"--relp.max_frame_bytes=1KiB"}},
		{"relp.inflight_max_bytes below max_message_bytes", []string{"--relp.inflight_max_bytes=1KiB"}},
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
			"--syslog.max_message_bytes=480", "--relp.backpressure_timeout=1s", "--relp.window=1",
			"--relp.max_frame_bytes=480", "--relp.inflight_max_bytes=480",
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
	if got := in.metric(`relplog_config_info{ring_mode="memory",sink_type="redis",transports="tcp"}`); got != "1" {
		t.Errorf("relplog_config_info = %s", got)
	}
	warned := false
	for _, w := range in.app.loaded.Warnings {
		warned = warned || strings.Contains(w, "ring")
	}
	if !warned {
		t.Errorf("no WARN about the degraded ring: %q", in.app.loaded.Warnings)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - degraded")
	waitFor(t, 10*time.Second, "the entry", func() bool { return e.xlen(t, stream) == 1 })
}

// compile-time guard: the decorator satisfies the optional interfaces it
// forwards, so wrapping a SQL sink keeps its columns and its worker ceiling.
var (
	_ sink.ColumnSink    = (*faultySink)(nil)
	_ sink.WorkerLimiter = (*faultySink)(nil)
)
