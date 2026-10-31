package app

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
)

// Wiring promises of the assembled app that no single package can prove.

// SPEC §12.2, §13.1 step 9: "no boot, todo emg-*.jsonl é carregado". EVERY
// dump is reingested whatever emergency.enabled says now: only the WRITING of
// dumps depends on it. An operator who switches the dumps off after a crash
// must not strand the messages the crash saved.
func TestDumpIsReingestedEvenWithEmergencyDisabled(t *testing.T) {
	dir := t.TempDir()
	args := append(baseArgs(t, dir), "--listen.udp=")
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, testID, append(args,
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendNUL(t, first.app.TransportAddr("tcp"), doc("saved-by-the-dump"))
	waitFor(t, 10*time.Second, "a held message", func() bool { return fs.attempts.Load() > 0 })
	if code := first.stop(); code != ExitSink {
		t.Fatalf("first life exited %d, want %d", code, ExitSink)
	}
	d := dumpFiles(t, first.emgDir(), testID.From)
	if len(d) != 1 {
		t.Fatalf("dumps = %v", d)
	}
	dumped := dumpLines(t, d[0])

	second := launch(t, testID, append(args, "--emergency.enabled=false"), nil)
	second.waitReady()
	rows := second.rows()
	checkReingested(t, dumped, rows)
	if len(rows) != 1 || checkRow(t, rows[0]).Payload.message() != "saved-by-the-dump" {
		t.Errorf("the dump was not reingested by an emergency.enabled=false boot: %d rows", len(rows))
	}
	if left := dumpFiles(t, second.emgDir(), testID.From); len(left) != 0 {
		t.Errorf("the dump is still pending: %v", left)
	}
}

// SPEC §5.3, §5.6: an empty datagram or POST produces no envelope and is
// counted as reason="empty"; an empty TCP frame is a separator; the documents
// around them are intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	in := start(t, t.TempDir())
	c, err := net.Dial("udp", in.app.TransportAddr("udp"))
	if err != nil {
		t.Fatal(err)
	}
	c.Write(nil)
	c.Close()
	tc := dialTCP(t, in.app.TransportAddr("tcp"))
	// An empty frame between two \0 is a separator, ignored without a count
	// (SPEC §5.6); an empty datagram and an empty POST are counted.
	fmt.Fprint(tc, doc("before")+"\x00\x00 \n\x00"+doc("after")+"\x00")
	tc.Close()
	if code, _ := postGELF(t, nil, "http://"+in.app.TransportAddr("http")+"/gelf", "", []byte("  \n")); code != 400 {
		t.Errorf("an empty POST answered %d, want 400", code)
	}
	waitFor(t, 10*time.Second, "two empty drops and two rows", func() bool {
		return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() == 2 && in.count() == 2
	})
	envs := byMessage(t, in.rows())
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

// SPEC §13.2: more refusals that are exit 1, before anything opens. The keys
// the parent go-gelf-server had and this one removed are unknown here, so a
// configuration carried over fails the boot instead of being half applied.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"tcp and http on one port", []string{"--listen.tcp=127.0.0.1:20777", "--listen.http=127.0.0.1:20777"}},
		{"max_message_bytes below 1KiB", []string{"--convert.max_message_bytes=1023"}},
		{"unknown keep_raw", []string{"--convert.keep_raw=sometimes"}},
		{"zero http read_timeout", []string{"--http.read_timeout=0s"}},
		{"zero drain_timeout", []string{"--shutdown.drain_timeout=0s"}},
		{"unknown flag", []string{"--listen.typo=1"}},
		{"removed sink.type", []string{"--sink.type=postgres"}},
		{"removed ring.mode", []string{"--ring.mode=memory"}},
		{"removed listen.tls", []string{"--listen.tls=127.0.0.1:0"}},
		{"removed listen.https", []string{"--listen.https=127.0.0.1:0"}},
		{"removed tls section", []string{"--tls.cert_file=/x.pem"}},
		{"removed sink.postgres.tls", []string{"--sink.postgres.tls=true"}},
		{"removed filter", []string{"--filter.min_level=warning"}},
		{"listen.unix of the syslog family", []string{"--listen.unix=/dev/log"}},
		{"removed ratelimit", []string{"--ratelimit.enabled=true"}},
		{"removed heartbeat beacon", []string{"--loghub.beacon_interval=0"}},
		{"eb_url requiring TLS", []string{"--loghub.eb_url=postgres://u:p@127.0.0.1:1/db?sslmode=verify-full"}},
		{"eb_url beside an explicit sink key", []string{"--loghub.eb_url=postgres://u:p@127.0.0.1:1/db",
			"--sink.postgres.addr=127.0.0.1:1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if code := RunContext(ctx, append(localArgs(t.TempDir()), tc.args...), testID); code != ExitConfig {
				t.Fatalf("exit %d, want %d", code, ExitConfig)
			}
		})
	}
	// The control: the same building blocks, valid, boot and stop cleanly --
	// the connection given by LOGHUB_EB_URL without TLS, the queue by its own
	// key. A refusal table without it would also pass if every boot were
	// refused.
	t.Run("control", func(t *testing.T) {
		pg := requirePG(t)
		in := launch(t, testID, append(localArgs(t.TempDir()),
			"--loghub.eb_url="+pg.dsn, "--sink.postgres.table="+pg.queue(t, ""),
			"--convert.max_message_bytes=1KiB", "--convert.keep_raw=always",
			"--http.read_timeout=5s", "--shutdown.drain_timeout=1s"), nil)
		in.waitReady()
		sendUDP(t, in.app.TransportAddr("udp"), doc("via-eb-url"))
		if e := checkRow(t, in.waitRows(1)[0]); e.Payload.message() != "via-eb-url" {
			t.Errorf("message %q", e.Payload.message())
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}
