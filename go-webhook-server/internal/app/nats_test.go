package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// sink=nats adds three pairs to the matrix of SPEC §16.1. These run against a
// real server with JetStream addressed by WHS_TEST_NATS_URL and skip without
// it:
//
//	WHS_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 900s ./internal/app/
//
// The server publishes to sink.nats.subject, by default loghub.{space}.{type}
// expanded per envelope (SPEC §3.8), and a subject may belong to ONE stream.
// So every test runs as an identity with a Workspace of its own (uniqueID) and
// creates, from docker/nats/stream.json, a stream under a unique name that
// covers exactly that Workspace's subject; it is deleted afterwards. The body
// and header ceilings are lowered so the default max_payload of a nats-server
// (1 MiB) passes the boot check of SPEC §8.7.5.

const natsURLEnv = "WHS_TEST_NATS_URL"

// natsNoJSURLEnv addresses a second server, WITHOUT JetStream (docs/TESTING.md
// §5.4): the tests that need one skip that case without it.
const natsNoJSURLEnv = "WHS_TEST_NATS_NOJS_URL"

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
		t.Skipf("set %s to run the nats sink tests (see docs/TESTING.md §5.4)", natsURLEnv)
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

// stream creates a stream from the canonical config under a unique name,
// covering the subject of the given Workspace, and returns its name.
func (e *natsEnv) stream(space string) string {
	e.t.Helper()
	return e.streamWith(space, nil)
}

// streamWith is stream with the config adjusted by mut first.
func (e *natsEnv) streamWith(space string, mut func(*jetstream.StreamConfig)) string {
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
	cfg.Subjects = []string{"loghub." + space + "." + envelope.Type}
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

		env, err := jlhcheck.Validate(raw.Data)
		if err != nil {
			continue // verify reports it
		}
		if want := "loghub." + env.Space + "." + env.Type; raw.Subject != want {
			e.t.Errorf("message %s on subject %q, want %q", env.UUID, raw.Subject, want)
		}
		h := raw.Header
		for name, want := range map[string]string{
			jetstream.MsgIDHeader: env.UUID + "." + fmt.Sprint(env.Step),
			"Content-Type":        "application/json",
			"X-LogHub-Type":       env.Type,
			"X-LogHub-UUID":       env.UUID,
			"X-LogHub-Space":      env.Space,
		} {
			if got := h.Get(name); got != want {
				e.t.Errorf("message %s: header %s = %q, want %q", env.UUID, name, got, want)
			}
		}
		for _, gone := range []string{"Whs-Version", "Whs-Envelope"} {
			if h.Get(gone) != "" {
				e.t.Errorf("message %s carries the v1 header %s", env.UUID, gone)
			}
		}
	}
	return out
}

// ids reads every uuid in the stream, each envelope checked by the oracle.
func (e *natsEnv) ids(stream string, id ident.Identity, mode string) []string {
	e.t.Helper()
	return uuidsOf(verify(e.t, e.envelopes(stream), id, mode))
}

// args leaves sink.nats.subject at its default, loghub.{space}.{type}: the
// subject is the identity's, which is what the streams above cover.
func (e *natsEnv) args(stream string, extra ...string) []string {
	return append([]string{
		"--sink.type=nats",
		"--sink.nats.urls=" + e.url,
		"--sink.nats.stream=" + stream,
		// The largest envelope (SPEC §3.2: 4*ceil(B/3) + 14*H + 13*U + 8 KiB)
		// must fit a default max_payload of 1 MiB (SPEC §8.7.5). The header
		// ceiling weighs fourteen-fold, so it is lowered with the body's.
		"--server.max_body_bytes=256KiB",
		"--server.max_header_bytes=16KiB",
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §16.1: with sink=nats every ring mode accepts traffic, reports the
// pair in whs_config_info, exits 0 and leaves every accepted uuid in the
// stream, each envelope with the mode its pair promises (SPEC §3.2).
func TestNATSAllRingModes(t *testing.T) {
	e := natsTest(t)
	for _, tc := range realSinkModes {
		t.Run(tc.ring, func(t *testing.T) {
			id := uniqueID()
			stream := e.stream(id.Space)
			in := startAs(t, t.TempDir(), id, e.args(stream,
				"--ring.mode="+tc.ring,
				"--ring.sqlite.synchronous=OFF")...)

			const n = 40
			ids := in.postN(n)
			if len(ids) != n {
				t.Fatalf("%d of %d requests were accepted", len(ids), n)
			}
			if in.mode != tc.mode {
				t.Errorf("the 200 answered mode %q, want %q", in.mode, tc.mode)
			}
			m := in.metrics()
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="nats"} 1`, tc.ring)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			got := e.ids(stream, id, tc.mode)
			if lost := missing(ids, got); len(lost) > 0 {
				t.Errorf("%d of %d accepted events never reached the stream", len(lost), len(ids))
			}
			if extra := missing(got, ids); len(extra) > 0 {
				t.Errorf("%d messages in the stream were never answered to a client", len(extra))
			}
		})
	}
}

// SPEC §11.1: ring=none writes through from the handler, and the wrapper of
// SPEC §10.2 must still observe every write.
func TestNATSRingNoneIsInstrumented(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	in := startAs(t, t.TempDir(), id, e.args(e.stream(id.Space), "--ring.mode=none")...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	m := in.metrics()
	for _, want := range []string{
		"whs_events_written_total 5",
		"whs_sink_duration_seconds_count 5",
		"whs_sink_batch_size_count 5",
		"whs_sink_nats_duplicates_total 0",
		"whs_sink_nats_reconnects_total 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// --------------------------------------------------------- the wire contract

// SPEC §3.2 and §3.8 end to end through JetStream: Data is the JLH, so a
// binary body travels in base64, an empty body as an empty content and a
// query with %00 as a \u0000 escape, all byte for byte, under the uuid the
// client was given -- with the subject and the headers checked by envelopes.
func TestNATSNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(id.Space)
	in := startAs(t, t.TempDir(), id, e.args(stream, "--ring.mode=memory")...)

	binID, emptyID := postRoundTrip(in)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verify(t, e.envelopes(stream), id, envelope.ModeFast)),
		binID, emptyID, false)
}

// --------------------------------------------------------------- boot checks

// SPEC §8.7.5 and §10.1: a stream that violates the contract stops the boot
// AT ONCE with exit 3, and the public port never opens. The control is a
// stream from the shipped config, covering the Workspace's subject, which
// boots.
func TestNATSStreamMismatchExitsThreeImmediately(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(id.Space)
	other := uniqueID()
	otherStream := e.stream(other.Space)

	t.Run("control", func(t *testing.T) {
		in := startAs(t, t.TempDir(), id, e.args(stream, "--ring.mode=memory")...)
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := startExpectingExitAs(t, t.TempDir(), tc.id, append(tc.args,
				"--ring.mode=memory", "--sink.boot_timeout=120s")...)

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

// SPEC §8.7.5, §10.1: a stream or a server that refuses this one for good
// ends the boot at once with exit 3, and the log says it was refused -- not
// that the sink "did not become reachable within sink.boot_timeout", which
// sent an operator to the network when the stream was wrong (found in
// go-webhook-nats-server). A stream that never acknowledges (no_ack) and a
// server without JetStream were not even refused: every publish timed out,
// network, and the boot waited out sink.boot_timeout.
func TestNATSFatalBootRefusalIsLoggedAsSuch(t *testing.T) {
	e := natsTest(t)
	type tc struct {
		name string
		id   ident.Identity
		args []string
		want string
	}
	// The URL carries credentials the test server ignores: none of them may
	// reach the log (SPEC §8.7.1), whose "sink ready" and "nats connected"
	// lines printed the URL whole.
	withCreds := strings.Replace(e.url, "nats://", "nats://bob:s3cr3tPw@", 1)
	cases := []tc{
		{"missing stream", uniqueID(), e.args("WHS_MISSING_STREAM", "--sink.nats.urls="+withCreds), "stream not found"},
	}
	noAck := uniqueID()
	cases = append(cases, tc{"no_ack stream", noAck,
		e.args(e.streamWith(noAck.Space, func(c *jetstream.StreamConfig) { c.NoAck = true })), "no_ack"})
	if nojs := os.Getenv(natsNoJSURLEnv); nojs != "" {
		cases = append(cases, tc{"server without JetStream", uniqueID(),
			e.args("LOGHUB_WEBHOOK", "--sink.nats.urls="+nojs), "JetStream"})
	} else {
		t.Logf("%s is not set: the server without JetStream is not exercised", natsNoJSURLEnv)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf syncBuffer
			args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
				"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.boot_timeout=120s"}, c.args...)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := time.Now()
			code := runContext(ctx, args, c.id, func(a *App) {
				a.log = obs.NewLogger(&buf, "text", "info")
			})
			if code != ExitSink {
				t.Fatalf("exit code %d, want %d", code, ExitSink)
			}
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("a fatal refusal waited %s", took)
			}
			out := buf.String()
			if !strings.Contains(out, "refused this server permanently") || strings.Contains(out, "did not become reachable") {
				t.Errorf("the boot log does not say the sink refused for good:\n%s", out)
			}
			if !strings.Contains(out, c.want) {
				t.Errorf("the boot log does not name the cause %q:\n%s", c.want, out)
			}
			if strings.Contains(out, "s3cr3t") {
				t.Errorf("a credential reached the log:\n%s", out)
			}
		})
	}
}

// syncBuffer is a bytes.Buffer a logger and the test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// SPEC §10.1: an unreachable NATS is retried until sink.boot_timeout and then
// exits 3 with the public port still shut.
func TestNATSUnreachableKeepsPublicPortShut(t *testing.T) {
	if os.Getenv(natsURLEnv) == "" {
		t.Skipf("set %s to run the nats sink tests", natsURLEnv)
	}
	dead := freePort(t)
	in := startExpectingExit(t, t.TempDir(),
		"--ring.mode=memory", "--sink.type=nats",
		"--sink.nats.urls=nats://"+dead, "--sink.boot_timeout=2s")

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		in.cancel()
		t.Fatal("the process never gave up on an unreachable NATS")
	}
	if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("the public port accepted a connection while the sink was unreachable")
	}
}

// ------------------------------------------------------------- availability

// SPEC §15.3: the destination goes away and comes back. The client reconnects
// on its own; while it is away the server refuses with 503 and /readyz fails;
// when it returns, nothing that was answered 200 is missing from the stream.
func TestNATSDownThenBackLosesNothing(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(id.Space)
	proxyUpstream := strings.TrimPrefix(e.url, "nats://")
	proxy := newTCPProxy(t, proxyUpstream)

	args := e.args(stream, "--ring.mode=memory",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.nats.urls=nats://"+proxy.Addr())
	in := startAs(t, t.TempDir(), id, args...)

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
	if lost := missing(append(before, after...), e.ids(stream, id, envelope.ModeFast)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the stream", len(lost))
	}
}

// drain_timeout bounds phases 3 and 4 together (SPEC §10.3). Each had its
// own: a slow upload held phase 3 for the whole timeout, and a sink that
// never answers then held phase 4 for another, so the dump started at twice
// drain_timeout -- past a stop_grace_period sized for once, where the SIGKILL
// takes the ring. The proxy holds the connection at the first publish of the
// data subject: the worker's batch waits for an acknowledgement that never
// comes (and gives up as soon as the shutdown cancels it, which is why this
// runs on NATS rather than on Redis, whose client waits out its own write
// timeout on a stalled pipe), and the ring keeps the rest.
func TestNATSShutdownDrainTimeoutBoundsConnectionsAndRingTogether(t *testing.T) {
	e := natsTest(t)
	id := uniqueID()
	stream := e.stream(id.Space)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	subject := []byte("loghub." + id.Space + "." + envelope.Type)
	proxy.Hold(func(b []byte) bool { return bytes.Contains(b, subject) })
	dir := t.TempDir()
	in := startAs(t, dir, id, e.args(stream, "--ring.mode=memory",
		"--sink.nats.urls=nats://"+proxy.Addr(), "--shutdown.drain_timeout=2s",
		"--sink.workers=1", "--sink.batch=1", "--sink.max_retries=100000", "--sink.fail_timeout=10m",
		"--sink.op_timeout=30s", "--server.read_timeout=1m")...)
	if ids := in.postN(3); len(ids) != 3 { // one held by the worker, two in the ring
		t.Fatalf("%d of 3 accepted", len(ids))
	}
	// A slow upload: the head and part of the body, never the rest.
	conn, err := net.Dial("tcp", in.pub)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /slow HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\nContent-Length: 100\r\n\r\nhalf")
	time.Sleep(200 * time.Millisecond)

	t0 := time.Now()
	code := in.stop()
	elapsed := time.Since(t0)
	if code != ExitSink {
		t.Fatalf("exit %d, want %d (the ring dumped)", code, ExitSink)
	}
	if elapsed > 3500*time.Millisecond {
		t.Errorf("the shutdown took %s with drain_timeout=2s: the two phases each waited it out", elapsed)
	}
	if files := emergencyFiles(t, filepath.Join(dir, "emergency")); len(files) != 1 {
		t.Fatalf("dumps: %v", files)
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
	stream := e.stream(id.Space)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	dir := t.TempDir()

	args := e.args(stream, "--ring.mode=memory",
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
	if lost := missing(ids, e.ids(stream, id, envelope.ModeFast)); len(lost) > 0 {
		t.Fatalf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}
}
