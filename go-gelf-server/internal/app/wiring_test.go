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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/sink"
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
	sendNUL(t, first.app.TransportAddr("tcp"), doc("saved-by-the-dump"))
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

// SPEC §8, §16.3: the limiter keys on the src-ip -- one bucket for every
// transport of the same client --, drops with reason="ratelimit" on TCP too,
// answers 503 with Retry-After: 1 on HTTP (never a 4xx), and is switched on
// and off by SIGHUP.
func TestRateLimitAndItsReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "gelf.ini")
	write := func(enabled string) {
		t.Helper()
		body := "[ratelimit]\nenabled = " + enabled + "\nrate = 0.001\nburst = 2\n"
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("true")
	in := start(t, dir, "--config="+ini, "--listen.http=127.0.0.1:0")
	url := "http://" + in.app.TransportAddr("http") + "/gelf"
	m := in.app.metrics
	lines := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = doc(fmt.Sprintf("%s-%d", prefix, i))
		}
		return out
	}

	sendNUL(t, in.app.TransportAddr("tcp"), lines("limited", 5)...)
	waitFor(t, 10*time.Second, "two accepted and three dropped", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 2 && m.MessagesDroppedTotal[obs.DropRateLimit].Load() == 3
	})
	code, hdr := postGELF(t, nil, url, "", []byte(doc("http-limited")))
	if code != 503 || hdr.Get("Retry-After") != "1" {
		t.Fatalf("POST over the limit = %d Retry-After %q, want 503 and 1", code, hdr.Get("Retry-After"))
	}
	if d := m.MessagesDroppedTotal[obs.DropRateLimit].Load(); d != 4 {
		t.Fatalf("ratelimit drops = %d, want 4: the HTTP request shares the client's bucket", d)
	}

	write("false")
	if r := in.app.reload(); strings.Join(r.applied, ",") != "ratelimit.enabled" {
		t.Fatalf("reload applied %v", r.applied)
	}
	sendNUL(t, in.app.TransportAddr("tcp"), lines("free", 5)...)
	waitFor(t, 10*time.Second, "the limiter switched off", func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 7 })
	if code, _ := postGELF(t, nil, url, "", []byte(doc("http-free"))); code != 202 {
		t.Errorf("POST with the limiter off = %d, want 202", code)
	}
	if d := m.MessagesDroppedTotal[obs.DropRateLimit].Load(); d != 4 {
		t.Errorf("ratelimit drops = %d after the limiter was switched off", d)
	}
	envs := byMessage(t, in.waitRows(2+5+1))
	if _, ok := envs["http-limited"]; ok {
		t.Error("the request answered 503 was written")
	}
	messageWith(t, envs, "http-free")
}

// SPEC §5.3, §5.6: an empty datagram and an empty (or blank) HTTP body produce
// no envelope and are counted as reason="empty" (HTTP 400); on TCP an empty or
// blank frame is a separator, ignored WITHOUT counting (decision 12); the
// documents around them are intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	in := start(t, t.TempDir(), "--listen.http=127.0.0.1:0")
	c, err := net.Dial("udp", in.app.TransportAddr("udp"))
	if err != nil {
		t.Fatal(err)
	}
	c.Write(nil)
	c.Close()
	if code, _ := postGELF(t, nil, "http://"+in.app.TransportAddr("http")+"/gelf", "", []byte(" \r\n")); code != 400 {
		t.Errorf("a blank POST /gelf = %d, want 400", code)
	}
	tc := dialTCP(t, in.app.TransportAddr("tcp"))
	fmt.Fprint(tc, doc("before")+"\x00\n\x00\x00 \r\n\x00"+doc("after")+"\x00")
	tc.Close()
	waitFor(t, 10*time.Second, "two empty drops and two rows", func() bool {
		return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() == 2 &&
			sqliteCount(t, in.eventsDB(), "gelf_events") == 2
	})
	if eventually(200*time.Millisecond, func() bool { return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() > 2 }) {
		t.Error("a TCP separator was counted as an empty document")
	}
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
		sendUDP(t, udp, doc(fmt.Sprintf("udp-%d", i)))
		sendNUL(t, tcp, doc(fmt.Sprintf("tcp-%d", i)))
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
		{"https and admin on the same address", []string{"--listen.https=" + same, "--admin.bind=" + same,
			"--tls.cert_file=" + p.serverCert, "--tls.key_file=" + p.serverKey}},
		{"https without a certificate", []string{"--listen.https=127.0.0.1:0"}},
		{"max_message_bytes below 1KiB", []string{"--convert.max_message_bytes=1023"}},
		{"unknown filter level", []string{"--filter.min_level=loud"}},
		{"a level synonym", []string{"--filter.levels=warn"}},
		{"zero http.read_timeout", []string{"--http.read_timeout=0s"}},
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
			"--listen.https=127.0.0.1:0", "--listen.http=127.0.0.1:0", "--convert.max_message_bytes=1KiB",
			"--filter.min_level=warning", "--filter.levels=err,warning", "--http.read_timeout=1s",
			"--shutdown.drain_timeout=1s")
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
	const stream = "loghub.default.gelf"
	e.del(t, stream, stream+":healthprobe")
	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--ring.mode=redis", "--ring.redis.addrs="+e.addr,
		"--sink.type=redis", "--sink.redis.addrs="+e.addr), nil)
	in.waitReady()
	if mode := in.app.ring.Mode(); mode != "memory" {
		t.Errorf("effective ring %q, want memory", mode)
	}
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="redis",transports="udp,tcp"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	warned := false
	for _, w := range in.app.loaded.Warnings {
		warned = warned || strings.Contains(w, "ring")
	}
	if !warned {
		t.Errorf("no WARN about the degraded ring: %q", in.app.loaded.Warnings)
	}
	sendNUL(t, in.app.TransportAddr("tcp"), doc("degraded"))
	waitFor(t, 10*time.Second, "the entry", func() bool { return e.xlen(t, stream) == 1 })
}

// compile-time guard: the decorator satisfies the optional interfaces it
// forwards, so wrapping a SQL sink keeps its columns and its worker ceiling.
var (
	_ sink.ColumnSink    = (*faultySink)(nil)
	_ sink.WorkerLimiter = (*faultySink)(nil)
)
