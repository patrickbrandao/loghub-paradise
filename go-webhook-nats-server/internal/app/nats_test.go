package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
)

// The one destination of this server (SPEC §8.7), end to end, against a real
// server with JetStream addressed by WHS_TEST_NATS_URL; the tests skip
// without it:
//
//	WHS_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 900s ./internal/app/
//
// The subject is loghub.<space>.webhook (SPEC §3.8), and a subject may belong
// to ONE stream. So every test runs as an identity with a Workspace of its own
// (uniqueID) and creates, from docker/nats/stream.json, a stream under a
// unique name that covers exactly that Workspace's subject; it is deleted
// afterwards. The body and header ceilings are lowered so the default
// max_payload of a nats-server (1 MiB) passes the boot check of SPEC §8.7.5.
//
// Ported from go-webhook-fast-server's app/nats_test.go, go-syslog-nats-server's
// (beacon, core) and go-webhook-pg-server's postgres_test.go (the scenarios of
// the one destination).

const natsURLEnv = "WHS_TEST_NATS_URL"

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
		t.Skipf("set %s to run the nats sink tests (see docs/TESTING.md §5)", natsURLEnv)
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

// uniqueID is testID with a Workspace of its own, so its subject belongs to
// the stream of one test only.
func uniqueID() ident.Identity {
	id := testID
	id.Space = fmt.Sprintf("t%d", time.Now().UnixNano())
	return id
}

// startAs is start against the real NATS of the arguments, as id.
func startAs(t *testing.T, dir string, id ident.Identity, extra ...string) *instance {
	t.Helper()
	return startWith(t, dir, id, nil, extra...)
}

// startExpectingExitAs launches, as id, an instance against the NATS of the
// arguments that is expected to fail during boot.
func startExpectingExitAs(t *testing.T, dir string, id ident.Identity, extra ...string) *instance {
	t.Helper()
	in := &instance{
		t: t, id: id, dir: dir,
		pub: freePort(t), adm: freePort(t),
		done: make(chan int, 1),
	}
	in.args = append([]string{
		"--server.bind=" + in.pub,
		"--admin.bind=" + in.adm,
		"--ring.data_dir=" + dir,
		"--log.level=error", "--log.format=text",
		"--sink.health_interval=100ms", "--sink.health_timeout=100ms",
	}, extra...)
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go func() { in.done <- runContext(ctx, in.args, id, nil) }()
	return in
}

// stream creates a stream from docker/nats/stream.json under a unique name,
// covering the subjects of the given Workspaces, and returns its name. mut
// adjusts the config first.
func (e *natsEnv) stream(mut func(*jetstream.StreamConfig), spaces ...string) string {
	e.t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		e.t.Fatalf("reading the canonical stream config: %v", err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		e.t.Fatalf("parsing the canonical stream config: %v", err)
	}
	name := fmt.Sprintf("WHS_APP_%d", time.Now().UnixNano())
	cfg.Name = name
	cfg.Subjects = nil
	for _, s := range spaces {
		cfg.Subjects = append(cfg.Subjects, "loghub."+s+"."+envelope.Type)
	}
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.CreateStream(ctx, cfg); err != nil {
		e.t.Fatalf("creating stream %s: %v", name, err)
	}
	e.t.Cleanup(func() { _ = e.js.DeleteStream(context.Background(), name) })
	return name
}

// envelopes returns the Data of every message in the stream, in sequence
// order, after holding each message's subject and headers to SPEC §3.8
// against the envelope it carries. Data is the JLH byte for byte; the
// envelope travels in no header.
func (e *natsEnv) envelopes(stream string) [][]byte {
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
	var out [][]byte
	if info.State.Msgs == 0 {
		return out
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		raw, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		out = append(out, raw.Data)
		checkMessage(e.t, raw.Subject, raw.Header, raw.Data)
	}
	return out
}

// checkMessage holds one message to SPEC §3.8: the subject and the five
// headers are the envelope's own, and nothing else rides along.
func checkMessage(t *testing.T, subject string, h nats.Header, data []byte) {
	t.Helper()
	env, err := jlhcheck.Validate(data)
	if err != nil {
		return // verify reports it
	}
	if want := "loghub." + env.Space + "." + env.Type; subject != want {
		t.Errorf("message %s on subject %q, want %q", env.UUID, subject, want)
	}
	want := map[string]string{
		jetstream.MsgIDHeader: env.UUID + "." + fmt.Sprint(env.Step),
		"Content-Type":        "application/json",
		"X-LogHub-Type":       env.Type,
		"X-LogHub-UUID":       env.UUID,
		"X-LogHub-Space":      env.Space,
	}
	for name, v := range want {
		if got := h.Get(name); got != v {
			t.Errorf("message %s: header %s = %q, want %q", env.UUID, name, got, v)
		}
	}
	// Nats-Expected-Stream is the one the publish option adds in jetstream.
	for name := range h {
		if _, ok := want[name]; !ok && name != jetstream.ExpectedStreamHeader {
			t.Errorf("message %s carries an extra header %s", env.UUID, name)
		}
	}
}

// ids reads every uuid in the stream, each envelope checked by the oracle.
func (e *natsEnv) ids(stream string, id ident.Identity) []string {
	e.t.Helper()
	return uuidsOf(verify(e.t, e.envelopes(stream), id, envelope.ModeFast))
}

func (e *natsEnv) args(stream string, extra ...string) []string {
	return append([]string{
		"--sink.nats.urls=" + e.url,
		"--sink.nats.stream=" + stream,
		// The largest envelope (SPEC §3.2: 4*ceil(B/3) + 14*H + 13*U + 8 KiB)
		// must fit a default max_payload of 1 MiB (SPEC §8.7.5). The header
		// ceiling weighs fourteen-fold, so it is lowered with the body's.
		"--server.max_body_bytes=256KiB",
		"--server.max_header_bytes=16KiB",
	}, extra...)
}

// ---------------------------------------------------------------- delivery

// SPEC §2.2, §16.1: every accepted uuid lands in the stream, and nothing else;
// the pair reports itself as the fast would (whs_config_info), the instance
// exits 0, and every message is the contract byte for byte.
func TestNATSDeliversEveryAcceptedEvent(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	in := startAs(t, t.TempDir(), id, e.args(stream)...)

	const n = 40
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d requests were accepted", len(ids), n)
	}
	m := in.metrics()
	for _, want := range []string{
		`whs_config_info{ring_mode="memory",sink_type="nats"} 1`,
		"whs_sink_nats_reconnects_total 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want %d", code, ExitOK)
	}
	got := e.ids(stream, id)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d accepted events never reached the stream", len(lost), len(ids))
	}
	if extra := missing(got, ids); len(extra) > 0 {
		t.Errorf("%d messages in the stream were never answered to a client", len(extra))
	}
}

// SPEC §3.2 and §3.8 end to end through JetStream: Data is the JLH, so a
// binary body travels in base64, an empty body as an empty content and a
// query with %00 as a \u0000 escape, all byte for byte, under the uuid the
// client was given -- with the subject and the headers checked by envelopes.
func TestNATSNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	in := startAs(t, t.TempDir(), id, e.args(stream)...)

	binID, emptyID := postRoundTrip(in)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verify(t, e.envelopes(stream), id, envelope.ModeFast)), binID, emptyID)
}

// SPEC §3.2: a request line carrying bytes that are not UTF-8 is a real
// event, and it must not take the rest of its batch down. The envelope writer
// turns them into valid JSON -- the path percent-encoded, the decoded query
// with U+FFFD. Go's client would escape the bytes, so the request is written
// by hand.
func TestNATSInvalidUTF8PathIsDelivered(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	in := startAs(t, t.TempDir(), id, e.args(stream)...)

	good := in.postN(3)
	badID := postInvalidUTF8(t, in)
	more := in.postN(3)
	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 7")
	}) {
		t.Error("the drain never reported all seven events as written")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	got := byUUID(t, verify(t, e.envelopes(stream), id, envelope.ModeFast))
	if lost := missing(append(append(good, badID), more...), keysOf(got)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the stream", len(lost))
	}
	if b := got[badID]; b != nil {
		if b.Payload.Path != "/hook/%FF%FE" {
			t.Errorf("path = %q, want the escaped path /hook/%%FF%%FE", b.Payload.Path)
		}
		if x, _ := b.Payload.Query["x"].(string); x != "�" {
			t.Errorf("query.x = %q, want U+FFFD for a byte that is not UTF-8", x)
		}
	}
}

// postInvalidUTF8 writes, by hand, a request whose path and query carry bytes
// that are not UTF-8, and returns the uuid it was answered with.
func postInvalidUTF8(t *testing.T, in *instance) string {
	t.Helper()
	c, err := net.Dial("tcp", in.pub)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /hook/\xff\xfe?x=\xc0 HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	a := checkAnswer(t, resp, b)
	if a.code != http.StatusOK {
		t.Fatalf("status = %d for a path that is not UTF-8", a.code)
	}
	return a.uuid
}

func keysOf(m map[string]*jlhcheck.Envelope) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// --------------------------------------------------------------- boot checks

// SPEC §8.7.5 and §10.1: a stream that violates the contract stops the boot
// AT ONCE with exit 3, and the public port never opens. The control is a
// stream from the shipped config, covering the Workspace's subject, which
// boots.
func TestNATSStreamMismatchExitsThreeImmediately(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	otherStream := e.stream(nil, uniqueID().Space)
	ctlID := uniqueID()
	ctlStream := e.stream(func(c *jetstream.StreamConfig) {
		c.Subjects = append(c.Subjects, "acme"+strings.TrimPrefix(ctlID.Space, "t")+".>")
	}, ctlID.Space)
	noAckID := uniqueID()
	noAckStream := e.stream(func(c *jetstream.StreamConfig) { c.NoAck = true }, noAckID.Space)

	t.Run("control", func(t *testing.T) {
		in := startAs(t, t.TempDir(), id, e.args(stream)...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("the canonical stream: exit code %d", code)
		}
	})

	cases := []struct {
		name string
		id   ident.Identity
		args []string
	}{
		// No stream listens on this Workspace's subject at all.
		{"missing stream", uniqueID(), e.args("WHS_MISSING_STREAM")},
		// The subject exists, but in another stream than the one configured.
		{"subject of another stream", id, e.args(otherStream)},
		{"payload ceiling", id, e.args(stream, "--server.max_body_bytes=64MiB")},
		{"stream capturing the control plane", ctlID, e.args(ctlStream,
			"--loghub.ctl_namespace=acme"+strings.TrimPrefix(ctlID.Space, "t")+".ctl")},
		{"no_ack stream", noAckID, e.args(noAckStream)},
	}
	if nojs := os.Getenv("WHS_TEST_NATS_NOJS_URL"); nojs != "" {
		cases = append(cases, struct {
			name string
			id   ident.Identity
			args []string
		}{"server without JetStream", id, append(e.args(stream), "--sink.nats.urls="+nojs)})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := startExpectingExitAs(t, t.TempDir(), tc.id, append(tc.args, "--sink.boot_timeout=120s")...)

			start := time.Now()
			select {
			case code := <-in.done:
				if code != ExitSink {
					t.Errorf("exit code %d, want %d", code, ExitSink)
				}
			case <-time.After(20 * time.Second):
				in.cancel()
				t.Fatal("the process kept retrying a stream that will never match")
			}
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("exit took %s; a fatal boot check must not wait for boot_timeout", took)
			}
			if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
				c.Close()
				t.Error("the public port accepted a connection despite a refused stream")
			}
		})
	}
}

// ------------------------------------------------------------- availability

// SPEC §15.3: the destination goes away and comes back. The client reconnects
// on its own; while it is away the server refuses with 503 and /readyz fails;
// when it returns, nothing that was answered 200 is missing from the stream,
// and the reconnection is counted.
func TestNATSDownThenBackLosesNothing(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))

	in := startAs(t, t.TempDir(), id, e.args(stream,
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.nats.urls=nats://"+proxy.Addr())...)

	before := in.postN(10)

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool {
		code, _ := in.post("/hook", `{"during":true}`)
		return code == http.StatusServiceUnavailable
	}) {
		t.Fatal("the server never started refusing while the sink was down")
	}
	if code, _ := in.readyz(); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while the sink was down, want 503", code)
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool {
		code, _ := in.readyz()
		return code == http.StatusOK
	}) {
		t.Fatal("the server never recovered after the sink came back")
	}

	after := in.postN(10)
	if len(after) != 10 {
		t.Errorf("%d of 10 requests accepted after recovery", len(after))
	}
	if !strings.Contains(in.metrics(), "whs_sink_nats_reconnects_total 1") {
		t.Error("the client reconnected exactly once and the counter must say so")
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if lost := missing(append(before, after...), e.ids(stream, id)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the stream", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §9 end to end: the sink dies for good, the ring still holds events
// answered 200, they go to an emergency dump, the process exits 3, and the
// next boot reingests them into the stream under the same uuids -- which is
// also what lets JetStream deduplicate a redelivery by Nats-Msg-Id.
func TestNATSFatalDumpsAndTheNextBootReingests(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	dir := t.TempDir()

	args := e.args(stream,
		"--sink.workers=1",
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s",
		"--sink.nats.urls=nats://"+proxy.Addr())

	in := startAs(t, dir, id, args...)
	const n = 8
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	checkDump(t, dir, id, envelope.ModeFast, ids)

	proxy.Resume()
	in2 := startAs(t, dir, id, args...)
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if lost := missing(ids, e.ids(stream, id)); len(lost) > 0 {
		t.Fatalf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}
}

// SPEC §8.7.5, §9.2: a dump from a life with another Workspace -- or one an
// operator moved over from another instance, a go-webhook-pg-server's
// included, whose format is the same -- lands on a subject the stream may not
// cover. The boot exits 3 at once naming nothing silently, and the file stays;
// once a stream covers the subject, the next boot delivers it, each message on
// the subject of its OWN space, and removes the file.
func TestNATSForeignWorkspaceDumpIsFatalUntilCovered(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	old := uniqueID() // the Workspace of the life that wrote the dump
	stream := e.stream(nil, id.Space)
	dir := t.TempDir()

	var lines []byte
	var want []string
	for i := 0; i < 3; i++ {
		u, env := testJLH(t, old, fmt.Sprintf("/old/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		want = append(want, u)
		lines = append(append(lines, env...), '\n')
	}
	emg := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emg, 0o750); err != nil {
		t.Fatal(err)
	}
	dump := filepath.Join(emg, fmt.Sprintf("emg-%s-%d.jsonl", old.From, time.Now().UnixNano()))
	if err := os.WriteFile(dump, lines, 0o640); err != nil {
		t.Fatal(err)
	}

	in := startExpectingExitAs(t, dir, id, append(e.args(stream), "--sink.boot_timeout=120s")...)
	start := time.Now()
	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(20 * time.Second):
		in.cancel()
		t.Fatal("the boot kept retrying a subject no stream covers")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("exit took %s; an uncovered subject is fatal, not a wait for boot_timeout", took)
	}
	if _, err := os.Stat(dump); err != nil {
		t.Fatalf("the dump did not stay for the next boot: %v", err)
	}

	// The operator widens the stream to the old Workspace's subject.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.CachedInfo().Config
	cfg.Subjects = append(cfg.Subjects, "loghub."+old.Space+".webhook")
	if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	in2 := startAs(t, dir, id, e.args(stream)...)
	if left := emergencyFiles(t, emg); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}

	// The dump drained, the operator narrows the stream back, as the error
	// said they may. The running instance forgets the subject and goes on
	// serving (SPEC §8.7.5): until 2026-10-11 its next health check was
	// FATAL, the ring dumped and the process exited 3.
	cfg.Subjects = cfg.Subjects[:len(cfg.Subjects)-1]
	if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // several health checks
	after := in2.postN(3)
	if len(after) != 3 {
		t.Fatalf("%d of 3 accepted after the narrowing", len(after))
	}
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0: narrowing the stream after the drain took it down", code)
	}
	var dumped, served [][]byte // by the Workspace of the life that wrote them
	for _, raw := range e.envelopes(stream) {
		if bytes.Contains(raw, []byte(`"space":"`+old.Space+`"`)) {
			dumped = append(dumped, raw)
		} else {
			served = append(served, raw)
		}
	}
	if lost := missing(want, uuidsOf(verify(t, dumped, old, envelope.ModeFast))); len(lost) > 0 {
		t.Errorf("%d of %d dumped events never reached the stream", len(lost), len(want))
	}
	if lost := missing(after, uuidsOf(verify(t, served, id, envelope.ModeFast))); len(lost) > 0 {
		t.Errorf("%d of 3 events accepted after the narrowing never reached the stream", len(lost))
	}
}

// ---------------------------------------------------------------- the beacon

// SPEC §8.7.10: the beacon goes out on the first connection -- asked for
// during WaitReady, sent when the beacon starts -- and every
// loghub.beacon_interval after, by NATS core on <ctl_namespace>.beacon, never
// into the data stream; a valid JLH envelope of type beacon with this
// instance's identity and the meta of SPEC §8.7.10.
func TestNATSBeacon(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	ns := "acme" + strings.TrimPrefix(id.Space, "t") + ".ctl"
	sub, err := e.nc.SubscribeSync(ns + ".beacon")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.nc.Flush(); err != nil {
		t.Fatal(err)
	}

	in := startAs(t, t.TempDir(), id, e.args(stream, "--loghub.ctl_namespace="+ns,
		"--loghub.beacon_interval=1", "--server.http2=false")...)

	for i := 0; i < 2; i++ {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("beacon %d never arrived on %s.beacon: %v", i, ns, err)
		}
		if err := jlhcheck.ValidateEnvelopeSchema(msg.Data); err != nil {
			t.Fatalf("beacon %d: %v\n%s", i, err, msg.Data)
		}
		var b struct {
			From, Space, Type string
			Payload           struct {
				Name, Hostname, Machine, Address string
				Meta                             []struct {
					Key   string
					Value json.RawMessage
				}
			}
		}
		if err := json.Unmarshal(msg.Data, &b); err != nil {
			t.Fatal(err)
		}
		if b.Type != "beacon" || b.From != id.From || b.Space != id.Space ||
			b.Payload.Name != id.AgentName || b.Payload.Hostname != id.Hostname ||
			b.Payload.Machine != id.MachineID || b.Payload.Address == "" {
			t.Errorf("beacon %d: %s", i, msg.Data)
		}
		meta := map[string]string{}
		for _, m := range b.Payload.Meta {
			meta[m.Key] = string(m.Value)
		}
		if meta["ring"] != `"memory"` || meta["sink"] != `"nats"` || meta["mode"] != `"jetstream"` ||
			meta["transports"] != `["http1"]` {
			t.Errorf("beacon %d meta = %v", i, meta)
		}
	}
	if !strings.Contains(in.metrics(), `whs_beacons_total{result="ok"}`) ||
		strings.Contains(in.metrics(), `whs_beacons_total{result="ok"} 0`) {
		t.Error(`whs_beacons_total{result="ok"} did not count the beacons`)
	}
	ids := in.postN(2)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	// The data stream holds the two events and no beacon.
	if got := e.ids(stream, id); len(got) != 2 || len(missing(ids, got)) != 0 {
		t.Errorf("the stream holds %v, want exactly %v", got, ids)
	}
}

// SPEC §8.7.10: the beacon of the FIRST connection does not depend on the
// periodic one. With loghub.beacon_interval=0 there is no ticker, so exactly one
// beacon arrives -- the one asked for by the connect hook during WaitReady,
// long before the beacon starts. Without the hook registered before the first
// connection, none would (TestNATSBeacon alone cannot tell: its ticker sends
// one anyway).
func TestNATSBeaconOnTheFirstConnectionWithoutThePeriodic(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(nil, id.Space)
	ns := "acme" + strings.TrimPrefix(id.Space, "t") + ".ctl"
	sub, err := e.nc.SubscribeSync(ns + ".beacon")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	in := startAs(t, t.TempDir(), id, e.args(stream, "--loghub.ctl_namespace="+ns,
		"--loghub.beacon_interval=0")...)
	if _, err := sub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("no beacon for the first connection: %v", err)
	}
	if msg, err := sub.NextMsg(1500 * time.Millisecond); err == nil {
		t.Errorf("a second beacon with beacon_interval=0: %s", msg.Data)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
}

// SPEC §8.7.2: core mode publishes by core NATS, with no stream at all: a
// subscriber receives every accepted event, the JLH in Data with the headers
// of SPEC §3.8, and the boot warns that delivery is best effort.
func TestNATSCoreMode(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	sub, err := e.nc.SubscribeSync("loghub." + id.Space + ".webhook")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	in := startAs(t, t.TempDir(), id, e.args("UNUSED_IN_CORE", "--sink.nats.mode=core")...)
	ids := in.postN(5)
	if len(ids) != 5 {
		t.Fatalf("%d of 5 accepted", len(ids))
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	var raws [][]byte
	for i := 0; i < 5; i++ {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("subscriber got %d of 5: %v", i, err)
		}
		checkMessage(t, msg.Subject, msg.Header, msg.Data)
		if msg.Header.Get(jetstream.ExpectedStreamHeader) != "" {
			t.Error("a core publish carries Nats-Expected-Stream")
		}
		raws = append(raws, msg.Data)
	}
	if lost := missing(ids, uuidsOf(verify(t, raws, id, envelope.ModeFast))); len(lost) > 0 {
		t.Errorf("%d accepted events never reached the subscriber", len(lost))
	}
}
