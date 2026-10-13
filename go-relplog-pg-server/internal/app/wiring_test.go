package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
)

// Wiring promises of the assembled app that no single package can prove.

// SPEC §12.2, §13.1 step 9: "no boot, todo emg-*.jsonl é carregado". EVERY
// dump is reingested whatever emergency.enabled says now: only the WRITING of
// dumps depends on it. An operator who switches the dumps off after a crash
// must not strand the messages the crash saved.
func TestDumpIsReingestedEvenWithEmergencyDisabled(t *testing.T) {
	dir := t.TempDir()
	args := baseArgs(t, dir)
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, testID, append(args,
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendRELP(t, first.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - saved-by-the-dump")
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

// SPEC §5.4: an empty syslog frame produces no envelope, is answered 200
// (there is nothing to keep, and a 500 would make rsyslog log an error) and is
// counted as reason="empty"; the frames around it are intact.
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
	if n := in.count(); n != 2 {
		t.Errorf("%d rows, want 2: the empty frame must not become one", n)
	}
}

// SPEC §4.4: listen.tcp_listeners > 1 opens N sockets with SO_REUSEPORT, and
// sessions on any of them arrive.
func TestSeveralListenersPerTransport(t *testing.T) {
	tcp := freePort(t)
	in := start(t, t.TempDir(), "--listen.tcp="+tcp, "--listen.tcp_listeners=2")
	const n = 20
	for i := 0; i < n; i++ {
		sendRELP(t, tcp, fmt.Sprintf("<13>tcp-%d", i))
	}
	if a := in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != n {
		t.Errorf("accepted %d, want %d", a, n)
	}
	for _, r := range in.waitRows(n) {
		checkRow(t, r)
	}
}

// SPEC §13.2: more refusals that are exit 1, before anything opens. The keys
// the parent go-relplog-server had and this one removed are unknown here, so a
// configuration carried over fails the boot instead of being half applied.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"zero relp.backpressure_timeout", []string{"--relp.backpressure_timeout=0s"}},
		{"relp.window of zero", []string{"--relp.window=0"}},
		{"relp.max_frame_bytes below max_message_bytes", []string{"--relp.max_frame_bytes=1KiB"}},
		{"relp.inflight_max_bytes below max_message_bytes", []string{"--relp.inflight_max_bytes=1KiB"}},
		{"max_message_bytes below 480", []string{"--syslog.max_message_bytes=479"}},
		{"unknown legacy charset", []string{"--syslog.legacy_charset=klingon"}},
		{"zero drain_timeout", []string{"--shutdown.drain_timeout=0s"}},
		{"unknown flag", []string{"--listen.typo=1"}},
		{"removed sink.type", []string{"--sink.type=postgres"}},
		{"removed ring.mode", []string{"--ring.mode=memory"}},
		{"removed listen.tls", []string{"--listen.tls=127.0.0.1:0"}},
		{"removed tls section", []string{"--tls.cert_file=/x.pem"}},
		{"removed sink.postgres.tls", []string{"--sink.postgres.tls=true"}},
		{"removed filter", []string{"--filter.min_severity=warning"}},
		{"removed ratelimit", []string{"--ratelimit.enabled=true"}},
		{"removed heartbeat beacon", []string{"--loghub.beacon_interval=0"}},
		{"removed listen.udp of the syslog sibling", []string{"--listen.udp=127.0.0.1:0"}},
		{"eb_url requiring TLS", []string{"--loghub.eb_url=postgres://u:p@127.0.0.1:1/db?sslmode=verify-full"}},
		{"eb_url with a fragment", []string{"--loghub.eb_url=postgres://u:39182#Zs3@127.0.0.1:1/db"}},
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
			"--syslog.max_message_bytes=480", "--relp.backpressure_timeout=1s", "--relp.window=1",
			"--relp.max_frame_bytes=480", "--relp.inflight_max_bytes=480",
			"--syslog.legacy_charset=windows-1252", "--shutdown.drain_timeout=1s"), nil)
		in.waitReady()
		sendRELP(t, in.app.TransportAddr("tcp"), "<13>Oct 11 22:14:15 host app: via-eb-url")
		if e := checkRow(t, in.waitRows(1)[0]); e.Payload.message() != "via-eb-url" {
			t.Errorf("message %q", e.Payload.message())
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}
