package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// sink=nats adds three pairs to the matrix of SPEC §18.1. These run the
// assembled server against a real nats-server with JetStream, addressed by
// SYSLOG_TEST_NATS_URL, and skip without it:
//
//	nats-server -js -sd "$(mktemp -d)" -p 14222 &
//	SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 600s ./internal/app/
//
// Each test creates its own stream from docker/nats/stream.json and deletes it
// afterwards. The frame ceilings are lowered so the largest message the server
// can accept fits the 1 MiB default max_payload, which the boot check of SPEC
// §3.9 insists on.

const natsURLEnv = "SYSLOG_TEST_NATS_URL"

type natsEnv struct {
	t   *testing.T
	url string
	nc  *nats.Conn
	js  jetstream.JetStream
}

func natsTest(t *testing.T) *natsEnv {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the nats sink tests (see docs/TESTING.md §6.4)", natsURLEnv)
	}
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("%s is not reachable: %v", natsURLEnv, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return &natsEnv{t: t, url: url, nc: nc, js: js}
}

// stream creates a stream from the canonical config under a unique name and
// returns its name and the subject the server should publish to.
func (e *natsEnv) stream() (name, subject string) {
	e.t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		e.t.Fatalf("reading the canonical stream config: %v", err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		e.t.Fatalf("parsing the canonical stream config: %v", err)
	}
	name = fmt.Sprintf("SLS_APP_%d", time.Now().UnixNano())
	prefix := strings.ToLower(name)
	cfg.Name = name
	cfg.Subjects = []string{prefix + ".>"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.CreateStream(ctx, cfg); err != nil {
		e.t.Fatalf("creating stream %s: %v", name, err)
	}
	e.t.Cleanup(func() { _ = e.js.DeleteStream(context.Background(), name) })
	return name, prefix + ".in"
}

// natsMessage is one stored message, split the way SPEC §3.8 tells a consumer.
type natsMessage struct {
	id, version string
	env         map[string]any
	envBytes    []byte
	raw         []byte
}

func (e *natsEnv) messages(stream string) []natsMessage {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, stream)
	if err != nil {
		e.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []natsMessage
	if info.State.Msgs == 0 {
		return out
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		n, err := strconv.Atoi(m.Header.Get("Sls-Envelope-Size"))
		if err != nil || n < 0 || n > len(m.Data) {
			e.t.Fatalf("message %d: Sls-Envelope-Size = %q for %d bytes of Data",
				seq, m.Header.Get("Sls-Envelope-Size"), len(m.Data))
		}
		msg := natsMessage{
			id:       m.Header.Get(jetstream.MsgIDHeader),
			version:  m.Header.Get("Sls-Version"),
			envBytes: m.Data[:n],
			raw:      m.Data[n:],
		}
		if err := json.Unmarshal(msg.envBytes, &msg.env); err != nil {
			e.t.Fatalf("message %d: the envelope is not JSON: %v", seq, err)
		}
		out = append(out, msg)
	}
	return out
}

// raws lists the raw line of every stored message.
func (e *natsEnv) raws(stream string) []string {
	var out []string
	for _, m := range e.messages(stream) {
		out = append(out, string(m.raw))
	}
	return out
}

func (e *natsEnv) args(stream, subject string, extra ...string) []string {
	return append([]string{
		"--sink.type=nats",
		"--sink.nats.urls=" + e.url,
		"--sink.nats.stream=" + stream,
		"--sink.nats.subject=" + subject,
		// Small enough for a default max_payload of 1 MiB (SPEC §3.9).
		"--tcp.max_frame_bytes=64KiB",
		"--relp.max_frame_bytes=64KiB",
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §18.1: with sink=nats every ring mode accepts traffic, reports the pair
// in syslog_config_info, exits 0 and leaves every confirmed message in the
// stream.
func TestNATSAllRingModes(t *testing.T) {
	e := natsTest(t)
	for _, ringMode := range []string{"memory", "sqlite", "none"} {
		t.Run(ringMode, func(t *testing.T) {
			stream, subject := e.stream()
			in := start(t, t.TempDir(), e.args(stream, subject, "--ring.mode="+ringMode)...)

			lines := markedLines("nats-"+ringMode, 40)
			confirmed := relpSend(t, in.app.TransportAddr("relp"), lines)
			if len(confirmed) != len(lines) {
				t.Fatalf("%d of %d messages were confirmed", len(confirmed), len(lines))
			}
			_, body := in.get("/metrics")
			want := fmt.Sprintf(`syslog_config_info{ring_mode="%s",sink_type="nats"`, ringMode)
			if !strings.Contains(body, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			if lost := missing(confirmed, e.raws(stream)); len(lost) > 0 {
				t.Errorf("%d of %d confirmed messages never reached the stream: %q", len(lost), len(confirmed), lost[0])
			}
		})
	}
}

// SPEC §9.5: ring=none writes through from the receiver, and the instrumented
// wrapper must still observe every write.
func TestNATSRingNoneIsInstrumented(t *testing.T) {
	e := natsTest(t)
	stream, subject := e.stream()
	in := start(t, t.TempDir(), e.args(stream, subject, "--ring.mode=none")...)
	defer in.stop()

	if n := len(relpSend(t, in.app.TransportAddr("relp"), markedLines("instrumented", 5))); n != 5 {
		t.Fatalf("%d of 5 confirmed", n)
	}
	_, body := in.get("/metrics")
	for _, want := range []string{
		"syslog_events_written_total 5",
		"syslog_sink_duration_seconds_count 5",
		"syslog_sink_batch_size_count 5",
		"syslog_sink_nats_duplicates_total 0",
		"syslog_sink_nats_reconnects_total 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// SPEC §3.8 end to end: the raw line comes back byte for byte out of Data, NUL
// and all, framing removed; the envelope ahead of it is the one the worker
// built, and msg-offset slices the raw line to exactly the message.
func TestNATSTheStreamMessageIsFaithful(t *testing.T) {
	e := natsTest(t)
	stream, subject := e.stream()
	in := start(t, t.TempDir(), e.args(stream, subject)...)

	line := "<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] a mensagem \x00 com nul"
	c, err := net.Dial("tcp", in.app.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "%d %s", len(line), line)
	c.Close()
	if !waitFor(10*time.Second, func() bool { return len(e.messages(stream)) == 1 }) {
		t.Fatal("the message never reached the stream")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	m := e.messages(stream)[0]
	if string(m.raw) != line {
		t.Fatalf("raw line changed\n got %q\nwant %q", m.raw, line)
	}
	if m.version != "1" || m.id != m.env["id"] {
		t.Errorf("Sls-Version = %q, Nats-Msg-Id = %q, envelope id = %v", m.version, m.id, m.env["id"])
	}
	for k, want := range map[string]any{
		"transport": "tcp", "framing": "octet", "rfc": "5424", "hostname": "web-01",
		"app-name": "nginx", "node-id": "test-node", "raw-size": float64(len(line)),
	} {
		if m.env[k] != want {
			t.Errorf("envelope %s = %#v, want %#v", k, m.env[k], want)
		}
	}
	off, size := int(m.env["msg-offset"].(float64)), int(m.env["msg-size"].(float64))
	if got := string(m.raw[off : off+size]); got != "a mensagem \x00 com nul" {
		t.Errorf("sliced msg = %q", got)
	}
	sd := m.env["structured-data"].(map[string]any)["a@1"].(map[string]any)
	if sd["k"] != `v"q` {
		t.Errorf("structured data value = %q, want the unescaped form", sd["k"])
	}
}

// --------------------------------------------------------------- boot checks

// SPEC §10.8.5 and §12.1: a stream that violates the contract stops the boot AT
// ONCE with exit 3, and no transport ever opens.
func TestNATSStreamMismatchExitsThreeImmediately(t *testing.T) {
	e := natsTest(t)
	stream, subject := e.stream()
	_, otherSubject := e.stream()
	cases := []struct {
		name string
		args []string
	}{
		{"missing stream", e.args("SLS_MISSING_STREAM", subject)},
		{"subject of another stream", e.args(stream, otherSubject)},
		// The default 16 MiB frames make the largest message far bigger than
		// any max_payload (SPEC §3.9).
		{"payload ceiling", e.args(stream, subject, "--tcp.max_frame_bytes=16MiB", "--relp.max_frame_bytes=16MiB")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := startExpectingExit(t, t.TempDir(), append(tc.args, "--sink.boot_timeout=120s")...)
			began := time.Now()
			select {
			case code := <-b.done:
				if code != ExitSink {
					t.Errorf("exit code %d, want %d", code, ExitSink)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("the process kept retrying a stream that will never match")
			}
			if took := time.Since(began); took > 10*time.Second {
				t.Errorf("exit took %s; a fatal boot check must not wait for boot_timeout", took)
			}
			b.assertShut(t)
		})
	}
}

// SPEC §12.1: an unreachable NATS is retried until sink.boot_timeout and then
// exits 3, with no transport ever open.
func TestNATSUnreachableKeepsTransportsShut(t *testing.T) {
	if os.Getenv(natsURLEnv) == "" {
		t.Skipf("set %s to run the nats sink tests", natsURLEnv)
	}
	b := startExpectingExit(t, t.TempDir(),
		"--sink.type=nats", "--sink.nats.urls=nats://"+freeAddr(t),
		"--tcp.max_frame_bytes=64KiB", "--relp.max_frame_bytes=64KiB",
		"--sink.boot_timeout=2s")
	select {
	case code := <-b.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the process never gave up on an unreachable NATS")
	}
	b.assertShut(t)
}

// ------------------------------------------------------------- availability

// SPEC §12.2: the destination goes away and comes back. The client reconnects
// on its own; while it is away /readyz fails; when it returns, nothing that was
// confirmed is missing from the stream.
func TestNATSDownThenBackLosesNothing(t *testing.T) {
	e := natsTest(t)
	stream, subject := e.stream()
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))

	in := start(t, t.TempDir(), e.args(stream, subject,
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.nats.urls=nats://"+proxy.Addr())...)

	before := relpSend(t, in.app.TransportAddr("relp"), markedLines("before", 10))
	if len(before) != 10 {
		t.Fatalf("%d of 10 confirmed before the outage", len(before))
	}

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool { code, _ := in.get("/readyz"); return code == 503 }) {
		t.Fatal("/readyz never failed while the destination was away")
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool { code, _ := in.get("/readyz"); return code == 200 }) {
		t.Fatal("the server never recovered after the destination came back")
	}

	after := relpSend(t, in.app.TransportAddr("relp"), markedLines("after", 10))
	if len(after) != 10 {
		t.Errorf("%d of 10 confirmed after recovery", len(after))
	}
	if _, body := in.get("/metrics"); !strings.Contains(body, "syslog_sink_nats_reconnects_total 1") {
		t.Error("the client reconnected once and syslog_sink_nats_reconnects_total must say so")
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if lost := missing(append(before, after...), e.raws(stream)); len(lost) > 0 {
		t.Errorf("%d confirmed messages are not in the stream", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §11 end to end: the destination dies for good while the ring still
// holds confirmed messages; they go to an emergency dump, the process exits 3,
// and the next boot reingests them into the stream.
func TestNATSFatalDumpsAndTheNextBootReingests(t *testing.T) {
	e := natsTest(t)
	stream, subject := e.stream()
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	dir := t.TempDir()

	args := e.args(stream, subject, "--ring.mode=memory",
		"--sink.workers=1", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--sink.nats.urls=nats://"+proxy.Addr())

	// Batches wait ten seconds to fill, so the confirmed messages are still in
	// the ring when the destination goes away.
	in := start(t, dir, append(args, "--sink.batch_max_delay=10s")...)
	confirmed := relpSend(t, in.app.TransportAddr("relp"), markedLines("dumped", 8))
	if len(confirmed) != 8 {
		t.Fatalf("%d of 8 confirmed", len(confirmed))
	}

	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 1 {
		t.Fatalf("%d emergency dumps on disk, want 1: %v", len(dumps), dumps)
	}

	proxy.Resume()
	second := start(t, dir, args...)
	if code := second.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if lost := missing(confirmed, e.raws(stream)); len(lost) > 0 {
		t.Fatalf("%d of %d confirmed messages were lost across the crash", len(lost), len(confirmed))
	}
}
