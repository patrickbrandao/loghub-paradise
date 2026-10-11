package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// sink=redis is the DEFAULT destination and was the one with no automated test
// at all: TestAllLocalCombinations covers the six pairs that need no external
// destination, and the three with Redis need a real one.
//
// These run against one, addressed by WHS_TEST_REDIS_ADDR, and skip when it is
// absent so `go test ./...` stays runnable on a bare checkout:
//
//	docker run --rm -d -p 16379:6379 --name whs-test-redis redis:7-alpine
//	WHS_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race ./internal/app/
//
// A real server rather than a fake is the point. The three properties that
// matter here -- rueidis' auto-pipelining, RESP carrying a raw binary body with
// no escaping, and OOM being reported as an error reply from a server that
// still answers PING -- are exactly the ones a fake would have to invent.

const redisAddrEnv = "WHS_TEST_REDIS_ADDR"

// redisClient connects to the test server, or skips the test.
func redisClient(t *testing.T) rueidis.Client {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		t.Skipf("set %s to run the redis sink tests (see docs/TESTING.md §5)", redisAddrEnv)
	}
	c, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("%s=%s is not reachable: %v", redisAddrEnv, addr, err)
	}
	t.Cleanup(c.Close)
	if err := c.Do(context.Background(), c.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("%s=%s did not answer PING: %v", redisAddrEnv, addr, err)
	}
	return c
}

// redisStream returns a stream name unique to this test and deletes it
// afterwards, so the suite never depends on an empty server and never has to
// FLUSH one that might not be its own.
func redisStream(t *testing.T, c rueidis.Client) string {
	t.Helper()
	name := fmt.Sprintf("whs-test:%s:%d", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()),
		time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Do(ctx, c.B().Del().Key(name).Build()).Error()
	})
	return name
}

type streamEntry struct {
	fields map[string]string
}

func readStream(t *testing.T, c rueidis.Client, stream string) []streamEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := c.Do(ctx, c.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", stream, err)
	}
	out := make([]streamEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, streamEntry{fields: e.FieldValues})
	}
	return out
}

func redisArgs(addr, stream string, extra ...string) []string {
	return append([]string{
		"--sink.type=redis",
		"--sink.redis.addrs=" + addr,
		"--sink.redis.stream=" + stream,
	}, extra...)
}

// -------------------------------------------------- the missing three pairs

// SPEC §16.1: with sink=redis every ring mode must accept traffic and deliver
// every accepted uuid, and the 200 must mean what the pair promises -- the
// mode in the answer and in every envelope (SPEC §3.2).
func TestRedisAllRingModes(t *testing.T) {
	c := redisClient(t)
	addr := os.Getenv(redisAddrEnv)

	for _, tc := range realSinkModes {
		t.Run(tc.ring, func(t *testing.T) {
			stream := redisStream(t, c)
			dir := t.TempDir()
			in := start(t, dir, redisArgs(addr, stream,
				"--ring.mode="+tc.ring,
				"--ring.sqlite.synchronous=OFF")...)

			const n = 25
			ids := in.postN(n)
			if len(ids) != n {
				t.Fatalf("%d of %d requests were accepted", len(ids), n)
			}
			if in.mode != tc.mode {
				t.Errorf("the 200 answered mode %q, want %q", in.mode, tc.mode)
			}

			m := in.metrics()
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="redis"} 1`, tc.ring)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}

			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}

			got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, tc.mode))
			if lost := missing(ids, got); len(lost) > 0 {
				t.Errorf("%d of %d accepted events never reached the stream",
					len(lost), len(ids))
			}
			if extra := missing(got, ids); len(extra) > 0 {
				t.Errorf("%d entries in the stream were never answered to a client", len(extra))
			}
		})
	}
}

// SPEC §11.1: ring=none writes through from the handler instead of going via
// the drain workers, and used to report every whs_sink_* series as a permanent
// zero because only the workers were instrumented.
func TestRedisRingNoneIsInstrumented(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)

	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=none")...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}

	m := in.metrics()
	for _, want := range []string{
		"whs_events_written_total 5",
		"whs_sink_duration_seconds_count 5",
		"whs_sink_batch_size_count 5",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q; ring=none is not observing its writes", want)
		}
	}
}

// ------------------------------------------------------- the wire contract

// SPEC §3.3: one XADD with exactly one field, e, holding the JLH byte for
// byte. The body travels inside it, in base64 when it is not text, and the
// envelope's uuid is the X-LogHub-UUID the client was given.
//
// The invariant content.length == the decoded body is what a consumer relies
// on to know it has the whole body; if the transport ever escaped or
// re-encoded anything, this is where it shows.
func TestRedisEntryShapeAndBinaryBody(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory")...)

	// NUL, a newline, an invalid UTF-8 byte and a CR: everything that a
	// text-oriented encoding would mangle or truncate.
	a := in.send(http.MethodPost, "/hook", "application/octet-stream", strings.NewReader(roundTripBody))
	if a.code != http.StatusOK {
		t.Fatalf("status = %d", a.code)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	entries := readStream(t, c, stream)
	if len(entries) != 1 {
		t.Fatalf("%d entries in the stream, want 1", len(entries))
	}
	if f := entries[0].fields; len(f) != 1 {
		t.Errorf("entry has %d fields (%v), want exactly e: v and b are gone with the v1 envelope",
			len(f), keysOf(f))
	}
	envs := verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast)
	if len(envs) != 1 {
		t.Fatal("the entry does not hold a valid envelope")
	}
	e := envs[0]
	if e.UUID != a.uuid {
		t.Errorf("envelope uuid %q does not match the X-LogHub-UUID %q the client was given", e.UUID, a.uuid)
	}
	if body, err := e.Payload.Body(); err != nil || string(body) != roundTripBody {
		t.Errorf("body = %q (%v), want %q -- the body must arrive byte for byte", body, err, roundTripBody)
	}
	if e.Payload.Content.Encoding != "base64" || e.Payload.Content.Length != len(roundTripBody) {
		t.Errorf("content encoding=%q length=%d, want base64 and %d",
			e.Payload.Content.Encoding, e.Payload.Content.Length, len(roundTripBody))
	}
}

// SPEC §3.2: content is ALWAYS written, an empty body included -- length 0
// and an empty body string -- so a consumer never has to test whether the
// field exists. (The v1 entry made the same promise for its b field.)
func TestRedisEmptyBodyIsAnEmptyContent(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory")...)

	if a := in.send(http.MethodGet, "/hook", "", nil); a.code != http.StatusOK {
		t.Fatalf("status = %d", a.code)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	entries := readStream(t, c, stream)
	if len(entries) != 1 {
		t.Fatalf("%d entries, want 1", len(entries))
	}
	if f := entries[0].fields; len(f) != 1 {
		t.Errorf("entry has %d fields (%v), want only e", len(f), keysOf(f))
	}
	envs := verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast)
	if len(envs) != 1 {
		t.Fatal("the entry does not hold a valid envelope")
	}
	// The raw bytes, not only the decoded struct: a missing key and an empty
	// string decode alike.
	if raw := readStream(t, c, stream)[0].fields["e"]; !strings.Contains(raw,
		`"content":{"length":0,"type":"","encoding":"utf-8","body":""}`) {
		t.Errorf("the empty body is not an explicit empty content object:\n%s", raw)
	}
}

// SPEC §8.2.3, invariant §17.10: rueidis recycles a Completed command into a
// sync.Pool once it has executed, so a retry that reuses the slice sends
// corrupted data with no error anywhere. Write must rebuild.
//
// Writing the SAME items twice is the shape of a retry: if anything about the
// commands were retained between calls, the second batch would come out wrong
// or short.
func TestRedisRebuildsCommandsOnEveryWrite(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory", "--sink.batch=8")...)

	const n = 24
	ids := in.postN(n)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	raws := redisEnvelopes(t, c, stream)
	if len(raws) != n {
		t.Fatalf("%d entries in the stream, want %d", len(raws), n)
	}
	// verify checks content.length against the decoded body of every entry;
	// byUUID reports a uuid that repeats.
	seen := byUUID(t, verify(t, raws, testID, envelope.ModeFast))
	if lost := missing(ids, keysOfEnv(seen)); len(lost) > 0 {
		t.Errorf("%d accepted uuids are not in the stream", len(lost))
	}
}

// SPEC §8.2.5: maxlen is off by default because trimming discards events
// silently. When an operator asks for it, the approximate form must actually
// bound the stream.
func TestRedisMaxLenTrims(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory", "--sink.redis.maxlen=100")...)

	if len(in.postN(400)) != 400 {
		t.Fatal("not every request was accepted")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	entries := readStream(t, c, stream)
	// MAXLEN ~ is deliberately approximate: it trims whole radix nodes, so the
	// bound is "does not grow without limit", not an exact count.
	if len(entries) > 300 {
		t.Errorf("%d entries survived a maxlen of 100; trimming is not being applied",
			len(entries))
	}
	if len(entries) == 0 {
		t.Error("trimming emptied the stream")
	}
}

// SPEC §3.3: sink.redis.stream defaults to loghub.{space}.{type}, expanded
// with the envelope's own space and type -- the subject convention of the JLH,
// so a Redis consumer finds the envelope where a NATS one would. The
// Workspace is one of this test's own, so the key cannot collide with
// another suite on a shared server.
//
// The same run checks the two other places the identity reaches Redis: the
// connection is named whs/<agent_name>@<hostname> (SPEC §3.5), and the write
// probe of Health uses NOMKSTREAM, so it never creates its :healthprobe key.
func TestRedisDefaultStreamIsTheWorkspaceTopic(t *testing.T) {
	c := redisClient(t)
	id := uniqueID()
	id.AgentName = "whs-app-redis"
	stream := "loghub." + id.Space + ".webhook.request"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Do(ctx, c.B().Del().Key(stream, stream+":healthprobe").Build()).Error()
	})

	in := startAs(t, t.TempDir(), id, "--sink.type=redis",
		"--sink.redis.addrs="+os.Getenv(redisAddrEnv), "--ring.mode=memory")
	ids := in.postN(3)
	if len(ids) != 3 {
		t.Fatalf("%d of 3 accepted", len(ids))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	list, err := c.Do(ctx, c.B().ClientList().Build()).ToString()
	if err != nil {
		t.Fatal(err)
	}
	if want := "name=whs/whs-app-redis@test-host "; !strings.Contains(list, want) {
		t.Errorf("no connection named %q in CLIENT LIST", strings.TrimSpace(want))
	}

	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), id, envelope.ModeFast))
	if len(got) != len(ids) || len(missing(ids, got)) > 0 {
		t.Errorf("stream %s holds %v, want exactly %v", stream, got, ids)
	}
	n, err := c.Do(ctx, c.B().Exists().Key(stream+":healthprobe").Build()).AsInt64()
	if err != nil || n != 0 {
		t.Errorf("the health probe created its key (exists=%d, %v); it must write with NOMKSTREAM", n, err)
	}
}

// SPEC §3.3: the stream is expanded PER ENVELOPE, from the envelope's own
// space. The one path where that differs from the instance's Workspace is a
// dump left behind by a life that ran with another one: its envelopes go to
// their own Workspace's stream, never relabelled into the current one, while
// what this life accepts goes to its own.
func TestRedisReingestedDumpKeepsItsOwnWorkspace(t *testing.T) {
	c := redisClient(t)
	before, now := uniqueID(), uniqueID()
	oldStream := "loghub." + before.Space + ".webhook.request"
	newStream := "loghub." + now.Space + ".webhook.request"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Do(ctx, c.B().Del().Key(oldStream, newStream).Build()).Error()
	})

	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var dump []byte
	var dumped []string
	for i := 0; i < 2; i++ {
		id, env := testJLH(t, before, fmt.Sprintf("/old/%d", i), `{"old":true}`)
		dump = fmt.Appendf(dump, "%d\n", len(env))
		dump = append(dump, env...)
		dumped = append(dumped, id)
	}
	if err := os.WriteFile(filepath.Join(emgDir, "emg-"+before.From+"-1757116800000000000.bin"),
		dump, 0o600); err != nil {
		t.Fatal(err)
	}

	in := startAs(t, dir, now, "--sink.type=redis",
		"--sink.redis.addrs="+os.Getenv(redisAddrEnv), "--ring.mode=memory")
	fresh := in.postN(1)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	if got := uuidsOf(verify(t, redisEnvelopes(t, c, oldStream), before, envelope.ModeFast)); len(got) != 2 ||
		len(missing(dumped, got)) > 0 {
		t.Errorf("%s holds %v, want the dump's %v", oldStream, got, dumped)
	}
	if got := uuidsOf(verify(t, redisEnvelopes(t, c, newStream), now, envelope.ModeFast)); len(got) != 1 ||
		len(missing(fresh, got)) > 0 {
		t.Errorf("%s holds %v, want only this life's %v", newStream, got, fresh)
	}
}

// SPEC §3.2 end to end through Redis: a query with %00 travels as a \u0000
// escape in query, a binary body in base64, and both come back out of the
// entry byte for byte under the uuid the client was given.
func TestRedisNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory")...)

	binID, emptyID := postRoundTrip(in)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast)),
		binID, emptyID, false)
}

// -------------------------------------------------------- error behaviour

// SPEC §8.2.4: WRONGTYPE is FATAL -- the server is alive and every batch will
// fail the same way for as long as the key holds the wrong type. The events
// answered 200 go to the emergency dump and the process exits 3, so the
// supervisor restarts it into a boot that waits for a fixable Redis instead of
// a healthy-looking process discarding a batch after every 200.
//
// The probe is a write and would refuse the boot outright on a key of the wrong
// type (that is TestRedisReadOnlyReplicaIsFatalViaTheWriteProbe's territory),
// so the key is spoiled AFTER the boot with the probe held back: the failure
// must reach the process through a batch, the runtime path of §8.2.4.
func TestRedisWrongTypeIsFatal(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)

	dir := t.TempDir()
	in := start(t, dir, redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory", "--sink.workers=1",
		"--sink.batch_max_delay=200ms", "--sink.batch=512",
		"--sink.health_interval=10s", "--sink.health_timeout=1s",
		"--shutdown.drain_timeout=3s")...)

	// Occupy the key with a plain string so XADD cannot succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Do(ctx, c.B().Set().Key(stream).Value("not a stream").Build()).Error(); err != nil {
		t.Fatal(err)
	}

	const n = 8
	if ids := in.postN(n); len(ids) != n {
		t.Fatalf("%d of %d accepted; the ring is memory and the sink looks healthy", len(ids), n)
	}
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d -- a key of the wrong type must end the process", code, ExitSink)
	}
	if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 1 {
		t.Fatalf("%d emergency dumps on disk, want 1: the events answered 200 must survive", len(dumps))
	}
}

// ... and with ring=none the client is told 503, not 500: the process is on
// its way out, and the event was never accepted (§4.4).
func TestRedisWrongTypeWithRingNoneAnswers503(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)

	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=none", "--sink.health_interval=10s", "--sink.health_timeout=1s",
		"--shutdown.drain_timeout=3s")...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Do(ctx, c.B().Set().Key(stream).Value("not a stream").Build()).Error(); err != nil {
		t.Fatal(err)
	}

	code, _ := in.post("/hook", `{"a":1}`)
	if code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 for a fatal destination", code)
	}
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
}

// SPEC §10.1: a Redis that refuses this server for good at the boot (the
// stream key holds a string) ends it at once, with the same log line every
// other sink's fatal boot has -- "Redis refused this server permanently"
// escaped an alert written for the others.
func TestRedisFatalBootRefusalIsLoggedAsSuch(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := c.Do(ctx, c.B().Set().Key(stream).Value("not a stream").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	var buf syncBuffer
	args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.boot_timeout=120s"},
		redisArgs(os.Getenv(redisAddrEnv), stream)...)
	start := time.Now()
	code := runContext(ctx, args, testID, func(a *App) { a.log = obs.NewLogger(&buf, "text", "info") })
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("a fatal refusal waited %s", took)
	}
	if out := buf.String(); !strings.Contains(out, "the sink refused this server permanently") ||
		!strings.Contains(out, "sink=redis") {
		t.Errorf("the boot log does not say the sink refused for good:\n%s", out)
	}
}

// SPEC §10.1 step 3: a Redis that is not up yet -- the normal case for a
// compose file that starts both -- is waited for within sink.boot_timeout, and
// the boot completes when it appears. Before the fix the first refused dial
// ended the process.
func TestRedisStartingLateIsWaitedFor(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	proxy.Pause() // refuses connections: what a Redis that has not started looks like

	go func() {
		time.Sleep(1500 * time.Millisecond)
		proxy.Resume()
	}()

	started := time.Now()
	in := start(t, t.TempDir(), redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory", "--sink.boot_timeout=20s")...)
	if waited := time.Since(started); waited < time.Second {
		t.Errorf("ready after %v; the boot cannot have waited for Redis", waited)
	}

	ids := in.postN(3)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast))
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events did not reach the stream", len(lost), len(ids))
	}
}

// SPEC §10.3: with ring=sqlite a drain that does not finish leaves the rows in
// ring.db for the next boot. That is not a loss: nothing is counted as dropped,
// no dump is written (it would deliver every row twice) and the exit is clean.
// The WARN gives the rows left, the worker's held batch among them; until
// 2026-10-11 it added that batch a second time.
func TestSQLiteRingLeftoversAreNotCountedAsLost(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	dir := t.TempDir()

	// The health check is spaced out so that pausing the proxy AFTER boot
	// leaves the sink UP for the whole test: the events are accepted into
	// ring.db and the worker's writes fail, which is the shape of a
	// destination that goes away with rows still queued. (A sqlite ring hands
	// rows to the worker as soon as they are committed -- batch_max_delay
	// only applies to the memory ring -- so pausing first is the only way to
	// keep them from being delivered before the shutdown.)
	args := redisArgs(proxy.Addr(), stream,
		"--ring.mode=sqlite", "--sink.workers=1",
		"--sink.health_interval=10s", "--sink.health_timeout=1s",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--shutdown.drain_timeout=2s")

	var log syncBuffer
	startLog = &log
	in := start(t, dir, args...)
	startLog = nil
	proxy.Pause()

	const n = 5
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0: rows left in ring.db are not a failure", code)
	}
	if !strings.Contains(log.String(), "workers surrendered batches") {
		t.Fatalf("control: no worker held a batch at the shutdown:\n%s", log.String())
	}
	if want := fmt.Sprintf(`events left in ring.db for the next boot" count=%d`, n); !strings.Contains(log.String(), want) {
		t.Errorf("the WARN does not count the %d rows left once each:\n%s", n, log.String())
	}
	if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 0 {
		t.Fatalf("%d emergency dumps written for rows that are already on disk", len(dumps))
	}
	if left := countRows(t, filepath.Join(dir, "ring.db"), "jlh_ring"); left != n {
		t.Fatalf("%d rows left in ring.db, want %d", left, n)
	}

	proxy.Resume()
	in2 := start(t, dir, args...)
	if code := in2.stop(); code != ExitOK {
		t.Fatalf("the second run exited %d", code)
	}
	// The rows waited in ring.db and were drained by the second boot: they
	// are the envelopes of the first run, mode and uuid included.
	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeBucket))
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 were lost across the restart", len(lost), len(ids))
	}
}

// SPEC §10.1: an unreachable Redis must never let the public port open, and the
// admin port must stay up with the reason -- the whole point of opening admin
// first is that a crash loop is still diagnosable.
func TestRedisUnreachableKeepsPublicPortShut(t *testing.T) {
	// No Redis needed: the point is a port nobody answers on.
	dead := freePort(t)
	in := startExpectingExit(t, t.TempDir(),
		"--ring.mode=memory", "--sink.type=redis",
		"--sink.redis.addrs="+dead, "--sink.boot_timeout=2s")

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		in.stop()
		t.Fatal("the process never gave up on an unreachable Redis")
	}

	if _, err := http.Get("http://" + in.pub + "/"); err == nil {
		t.Error("the public port accepted a connection while the sink was unreachable")
	}
}

// ----------------------------------------------------------------- helpers

// redisEnvelopes returns the e field of every entry, in stream order. An entry
// with any other field is reported: SPEC §3.3 is one field, e, and nothing
// else.
func redisEnvelopes(t *testing.T, c rueidis.Client, stream string) [][]byte {
	t.Helper()
	var out [][]byte
	for i, e := range readStream(t, c, stream) {
		env, ok := e.fields["e"]
		if !ok || len(e.fields) != 1 {
			t.Errorf("entry %d has fields %v, want exactly e", i, keysOf(e.fields))
		}
		if ok {
			out = append(out, []byte(env))
		}
	}
	return out
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysOfEnv(m map[string]*jlhcheck.Envelope) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
