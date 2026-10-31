package app

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
)

// Wiring promises of the assembled app that no single package can prove.

// SPEC §12.2, §13.1 step 9: "no boot, todo emg-*.jsonl é carregado". EVERY
// dump is reingested whatever emergency.enabled says now: only the WRITING of
// dumps depends on it. An operator who switches the dumps off after a crash
// must not strand the messages the crash saved.
func TestDumpIsReingestedEvenWithEmergencyDisabled(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	args := f.args(dir, "--listen.udp=")
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, f.id, append(args,
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	sendNUL(t, first.app.TransportAddr("tcp"), doc("saved-by-the-dump"))
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

// SPEC §5.3, §5.6: an empty datagram and an empty (or blank) HTTP body produce
// no envelope and are counted as reason="empty" (HTTP 400); on TCP an empty or
// blank frame is a separator, ignored WITHOUT counting (decision 12); the
// documents around them are intact.
func TestEmptyMessagesAreCountedNotWritten(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--listen.http=127.0.0.1:0")
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
	waitFor(t, 10*time.Second, "two empty drops and two messages", func() bool {
		return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() == 2 && f.count() == 2
	})
	if eventually(200*time.Millisecond, func() bool { return in.app.metrics.MessagesDroppedTotal[obs.DropEmpty].Load() > 2 }) {
		t.Error("a TCP separator was counted as an empty document")
	}
	envs := byMessage(t, f.msgs())
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
	f := newFixture(t)
	udp, tcp := freePort(t), freePort(t)
	in := f.start(t.TempDir(), "--listen.udp="+udp, "--listen.tcp="+tcp,
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
	for _, m := range f.waitMsgs(2 * n) {
		checkMsg(t, m)
	}
}

// SPEC §13.2: more refusals that are exit 1, before anything opens. The keys
// the parent go-gelf-server had and this one removed are unknown here, so a
// configuration carried over fails the boot instead of being half applied; and
// there is no TLS anywhere, so tls:// is refused wherever a URL goes.
func TestMoreBootRefusalsExitOne(t *testing.T) {
	long := freePort(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"removed listen.https", []string{"--listen.https=127.0.0.1:0"}},
		{"removed filter", []string{"--filter.min_level=warning"}},
		{"max_message_bytes below 1KiB", []string{"--convert.max_message_bytes=1023"}},
		{"unknown keep_raw", []string{"--convert.keep_raw=never"}},
		{"tcp and http on one socket", []string{"--listen.tcp=" + long, "--listen.http=" + long}},
		{"zero drain_timeout", []string{"--shutdown.drain_timeout=0s"}},
		{"unknown flag", []string{"--listen.typo=1"}},
		{"removed sink.type", []string{"--sink.type=nats"}},
		{"removed ring.mode", []string{"--ring.mode=memory"}},
		{"removed listen.tls", []string{"--listen.tls=127.0.0.1:0"}},
		{"removed tls section", []string{"--tls.cert_file=/x.pem"}},
		{"removed sink.nats.tls", []string{"--sink.nats.tls=true"}},
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
			"--listen.http=127.0.0.1:0", "--convert.max_message_bytes=1KiB",
			"--convert.keep_raw=always", "--shutdown.drain_timeout=1s",
			"--loghub.beacon_interval=0"), nil)
		in.waitReady()
		sendUDP(t, in.app.TransportAddr("udp"), doc("via-eb-url"))
		if e := checkMsg(t, f.waitMsgs(1)[0]); e.Payload.message() != "via-eb-url" {
			t.Errorf("message %q", e.Payload.message())
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}

// SPEC §10.7: the async-publish window is twice what the workers can have in
// flight. With exactly workers x batch, a quick retry of a tainted chunk --
// whose futures still count until their own timeout -- stalled on a full
// window (go-relplog-nats-server's independent review, 2026-10-13).
func TestMaxPendingHasTwiceTheWorkersBatches(t *testing.T) {
	c := &config.Config{}
	c.Sink.Workers, c.Sink.Batch = 8, 256
	if got := maxPending(c); got != 2*8*256 {
		t.Fatalf("maxPending = %d, want %d", got, 2*8*256)
	}
}
