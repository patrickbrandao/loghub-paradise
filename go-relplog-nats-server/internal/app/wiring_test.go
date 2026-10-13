package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

// Wiring promises of the assembled app that no single package can prove.

// SPEC §12.2, §13.1 step 9: "no boot, todo emg-*.jsonl é carregado". EVERY
// dump is reingested whatever emergency.enabled says now: only the WRITING of
// dumps depends on it. An operator who switches the dumps off after a crash
// must not strand the messages the crash saved.
func TestDumpIsReingestedEvenWithEmergencyDisabled(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	args := f.args(dir)
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, f.id, append(args,
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendRELP(t, first.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - saved-by-the-dump")
	waitFor(t, 10*time.Second, "a held message", func() bool { return fs.attempts.Load() > 0 })
	if code := first.stop(); code != ExitSink {
		t.Fatalf("first life exited %d, want %d", code, ExitSink)
	}
	d := dumpFiles(t, first.emgDir(), f.id.From)
	if len(d) != 1 {
		t.Fatalf("dumps = %v", d)
	}
	dumped := dumpLines(t, d[0])
	if c := f.count(); c != 0 {
		t.Fatalf("%d messages written by a sink that refused every write", c)
	}

	second := launch(t, f.id, append(args, "--emergency.enabled=false"), nil)
	second.waitReady()
	msgs := f.msgs()
	checkReingested(t, dumped, msgs)
	if len(msgs) != 1 || checkMsg(t, msgs[0]).Payload.message() != "saved-by-the-dump" {
		t.Errorf("the dump was not reingested by an emergency.enabled=false boot: %d messages", len(msgs))
	}
	if left := dumpFiles(t, second.emgDir(), f.id.From); len(left) != 0 {
		t.Errorf("the dump is still pending: %v", left)
	}
}

// SPEC §5.1, §5.4: an empty syslog frame (DATALEN 0) produces no envelope,
// is answered 200 and counted as reason="empty"; the frames around it are
// intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	sendRELP(t, in.app.TransportAddr("tcp"), "<13>before", "", "<13>after")
	if d := in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load(); d != 1 {
		t.Errorf(`relplog_messages_dropped_total{reason="empty"} = %d, want 1`, d)
	}
	envs := byMessage(t, f.waitMsgs(2))
	if _, ok := envs["before"]; !ok {
		t.Errorf("before is missing: %v", keysOfEnv(envs))
	}
	if _, ok := envs["after"]; !ok {
		t.Errorf("after is missing: %v", keysOfEnv(envs))
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if n := len(f.msgs()); n != 2 {
		t.Errorf("%d messages, want 2: the empty frame must not become one", n)
	}
}

// SPEC §4.4: listen.tcp_listeners > 1 opens N sockets with SO_REUSEPORT, and
// sessions on any of them arrive.
func TestSeveralListenersPerTransport(t *testing.T) {
	f := newFixture(t)
	tcp := freePort(t)
	in := f.start(t.TempDir(), "--listen.tcp="+tcp, "--listen.tcp_listeners=2")
	const n = 20
	for i := 0; i < n; i++ {
		sendRELP(t, tcp, fmt.Sprintf("<13>tcp-%d", i))
	}
	if a := in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != n {
		t.Errorf("accepted %d, want %d", a, n)
	}
	for _, m := range f.waitMsgs(n) {
		checkMsg(t, m)
	}
}

// SPEC §13.2: more refusals that are exit 1, before anything opens. The keys
// the parent go-relplog-server had and this one removed are unknown here, so a
// configuration carried over fails the boot instead of being half applied; and
// there is no TLS anywhere, so tls:// is refused wherever a URL goes.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"removed listen.unix", []string{"--listen.unix=/dev/log"}},
		{"removed listen.udp", []string{"--listen.udp=:1514"}},
		{"removed tcp.framing", []string{"--tcp.framing=lf"}},
		{"relp.max_frame_bytes below max_message_bytes", []string{"--relp.max_frame_bytes=1KiB"}},
		{"relp.inflight_max_bytes below max_message_bytes", []string{"--relp.inflight_max_bytes=1KiB"}},
		{"zero relp.backpressure_timeout", []string{"--relp.backpressure_timeout=0s"}},
		{"max_message_bytes below 480", []string{"--syslog.max_message_bytes=479"}},
		{"unknown legacy charset", []string{"--syslog.legacy_charset=klingon"}},
		{"zero drain_timeout", []string{"--shutdown.drain_timeout=0s"}},
		{"unknown flag", []string{"--listen.typo=1"}},
		{"removed sink.type", []string{"--sink.type=nats"}},
		{"removed ring.mode", []string{"--ring.mode=memory"}},
		{"removed listen.tls", []string{"--listen.tls=127.0.0.1:0"}},
		{"removed tls.handshake_timeout", []string{"--tls.handshake_timeout=10s"}},
		{"removed tls section", []string{"--tls.cert_file=/x.pem"}},
		{"removed sink.nats.tls", []string{"--sink.nats.tls=true"}},
		{"removed sink.nats.subjects", []string{"--sink.nats.subjects=facility-severity"}},
		{"removed sink.postgres", []string{"--sink.postgres.table=loghub.inbox"}},
		{"removed filter", []string{"--filter.min_severity=warning"}},
		{"removed ratelimit", []string{"--ratelimit.enabled=true"}},
		{"tls:// in sink.nats.urls", []string{"--sink.nats.urls=tls://127.0.0.1:1"}},
		{"ws:// in sink.nats.urls", []string{"--sink.nats.urls=ws://127.0.0.1:1"}},
		{"stream name with a dot", []string{"--sink.nats.stream=LOGHUB.SYSLOG"}},
		{"two authentication methods", []string{"--sink.nats.token=x", "--sink.nats.username=u"}},
		{"ctl_namespace with a wildcard", []string{"--loghub.ctl_namespace=loghub.*"}},
		{"eb_url with tls://", []string{"--loghub.eb_url=tls://127.0.0.1:1"}},
		{"eb_url of another eventbus", []string{"--loghub.eb_url=postgres://u:p@127.0.0.1:1/db"}},
		{"eb_url beside an explicit sink key", []string{"--loghub.eb_url=nats://127.0.0.1:1",
			"--sink.nats.urls=nats://127.0.0.1:1"}},
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
	// the connection given by LOGHUB_EB_URL without TLS, the stream by its own
	// key, the periodic beacon off. A refusal table without it would also pass
	// if every boot were refused.
	t.Run("control", func(t *testing.T) {
		f := newFixture(t)
		in := launch(t, f.id, append(localArgs(t.TempDir()),
			"--loghub.eb_url="+f.nats.url, "--sink.nats.stream="+f.stream,
			"--syslog.max_message_bytes=480",
			"--syslog.legacy_charset=windows-1252", "--shutdown.drain_timeout=1s",
			"--loghub.beacon_interval=0"), nil)
		in.waitReady()
		sendRELP(t, in.app.TransportAddr("tcp"), "<13>Oct 11 22:14:15 host app: via-eb-url")
		if e := checkMsg(t, f.waitMsgs(1)[0]); e.Payload.message() != "via-eb-url" {
			t.Errorf("message %q", e.Payload.message())
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}
