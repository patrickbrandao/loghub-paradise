package app

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/sink"
)

// These helpers run the REAL assembled stack in-process -- the same run() the
// binary calls -- through runContext, with a fixed identity instead of
// go-loghub-ident (whose Initialize may run once per process, SPEC §13.1). A
// test that assembled its own approximation of the wiring would prove nothing
// about what ships.
//
// The only destination is NATS (SPEC §10.7), so every test that boots a server
// past step 7 needs a real nats-server with JetStream and skips without one:
//
//	RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 900s ./internal/app/
//
// The server never creates a stream (invariant §20.17): each test gets a
// fixture -- a space of its own, two identities in it, and a stream covering
// exactly loghub.<space>.syslog (and .>) -- and the stream is deleted
// afterwards. A space per test keeps one test's messages, or a damaged dump's,
// out of the next one's count, and never overlaps another stream's subjects.

// testID is the identity of the tests that never reach the sink (refused
// configurations, refused identities). Tests that do get fixture.id instead.
var testID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space:     "default",
	AgentName: "go-relplog-nats-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

// otherID is the template of a second instance: the second life of the
// emergency test. Its from differs, so a test can tell who RECEIVED a message
// from who reingested it (SPEC §3.1, §3.5).
var otherID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8b9",
	Space:     "default",
	AgentName: "go-relplog-nats-server-test-b",
	Hostname:  "test-host-b",
	MachineID: "fedcba9876543210fedcba9876543210",
}

// localArgs binds everything to 127.0.0.1:0 and names no destination: the
// sink keys keep their defaults. It is enough for a boot that must be refused
// before step 7 (exit 1), and nothing else. Every default that would touch a
// well-known port (:20514, 127.0.0.1:9103, nats://127.0.0.1:4222) is
// overridden or never reached. Later arguments win, so a test appends what it
// needs.
func localArgs(dir string) []string {
	return []string{
		"--server.data_dir=" + dir,
		"--admin.bind=127.0.0.1:0",
		"--listen.tcp=127.0.0.1:0",
		"--sink.health_interval=50ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=5s",
		"--log.level=error",
	}
}

// ---------------------------------------------------------------- NATS

const natsURLEnv = "RELPLOG_TEST_NATS_URL"

// natsEnv is the test nats-server: one connection for the whole test process,
// closed by TestMain. The tests read streams and subscribe to the control plane
// through it; the server under test has its own.
type natsEnv struct {
	url string
	nc  *nats.Conn
	js  jetstream.JetStream
}

var (
	natsOnce   sync.Once
	natsShared *natsEnv
	natsErr    error
	natsSeq    atomic.Int64
)

// requireNATS returns the test NATS, skipping the test without
// RELPLOG_TEST_NATS_URL and failing it when the variable names a server that
// does not answer: a typo must not turn the whole suite into skips.
func requireNATS(t *testing.T) *natsEnv {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the tests that need NATS (the only sink)", natsURLEnv)
	}
	natsOnce.Do(func() {
		nc, err := nats.Connect(url, nats.Timeout(5*time.Second), nats.Name("go-relplog-nats-server app tests"))
		if err != nil {
			natsErr = err
			return
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			natsErr = err
			return
		}
		natsShared = &natsEnv{url: url, nc: nc, js: js}
	})
	if natsErr != nil {
		t.Fatalf("%s is set but NATS does not answer: %v", natsURLEnv, natsErr)
	}
	return natsShared
}

// unique is a token no other test of any process will pick: the nanosecond
// clock plus a per-process sequence.
func unique() string { return fmt.Sprintf("%d%d", time.Now().UnixNano(), natsSeq.Add(1)) }

// fixture is one test's slice of the NATS server.
type fixture struct {
	t    *testing.T
	nats *natsEnv

	// space is id's, otherSpace other's. The stream covers both, so the
	// second life of the emergency test can reingest the first one's dump
	// into loghub.<space>.syslog while being an instance of otherSpace.
	space, otherSpace string
	id, other         ident.Identity

	// stream is "" in core mode.
	stream string
	core   bool
}

// newFixture creates a space, two identities and a JetStream stream covering
// the two spaces' subjects, and registers the stream's removal.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := newSpaces(t)
	f.stream = f.createStream("RLNATS_APP_"+unique(), nil, f.space, f.otherSpace)
	return f
}

// newCoreFixture is newFixture for sink.nats.mode=core: no stream at all, so a
// core subscription is the only reader.
func newCoreFixture(t *testing.T) *fixture {
	t.Helper()
	f := newSpaces(t)
	f.core = true
	return f
}

func newSpaces(t *testing.T) *fixture {
	t.Helper()
	e := requireNATS(t)
	u := unique()
	f := &fixture{t: t, nats: e, space: "t" + u, otherSpace: "o" + u}
	f.id, f.other = testID, otherID
	// A from of its own, too: the beacon of this test's instance is told
	// apart from any other on the control plane by it.
	f.id.From, f.id.Space = jlh.NewUUID(time.Now()).String(), f.space
	f.other.From, f.other.Space = jlh.NewUUID(time.Now()).String(), f.otherSpace
	return f
}

// subject is where an envelope of space goes (SPEC §3.8).
func subject(space string) string { return "loghub." + space + ".syslog" }

// canonicalStream is the stream an operator applies: docker/nats/stream.json
// when this module has one, otherwise the parent's canonical file, inlined.
// Using the operator's file is the point -- the validation of SPEC §10.7 and
// the stream the operator gets must agree.
func canonicalStream(t *testing.T) jetstream.StreamConfig {
	t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	switch {
	case err == nil:
		var cfg jetstream.StreamConfig
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatalf("parsing docker/nats/stream.json: %v", err)
		}
		return cfg
	case !errors.Is(err, os.ErrNotExist):
		t.Fatalf("reading docker/nats/stream.json: %v", err)
	}
	return jetstream.StreamConfig{
		Name:              "LOGHUB_SYSLOG",
		Subjects:          []string{"loghub.default.syslog", "loghub.default.syslog.>"},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		Replicas:          1,
		MaxConsumers:      -1,
		MaxMsgs:           -1,
		MaxMsgsPerSubject: -1,
		MaxBytes:          -1,
		MaxMsgSize:        -1,
		Duplicates:        10 * time.Minute,
		Discard:           jetstream.DiscardOld,
	}
}

// createStream creates name from the canonical stream, retargeted at spaces
// (the canonical subjects of each), then adjusted by mut, and registers its
// removal on the fixture's test.
func (f *fixture) createStream(name string, mut func(*jetstream.StreamConfig), spaces ...string) string {
	f.t.Helper()
	cfg := canonicalStream(f.t)
	cfg.Name = name
	var subjects []string
	for _, sp := range spaces {
		for _, s := range cfg.Subjects {
			subjects = append(subjects, strings.Replace(s, "loghub.default.", "loghub."+sp+".", 1))
		}
	}
	cfg.Subjects = subjects
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.nats.js.CreateStream(ctx, cfg); err != nil {
		f.t.Fatalf("creating stream %s %v: %v", name, cfg.Subjects, err)
	}
	f.t.Cleanup(func() { _ = f.nats.js.DeleteStream(context.Background(), name) })
	return name
}

// sinkArgs points the server at the fixture's NATS and stream.
func (f *fixture) sinkArgs() []string {
	if f.core {
		return []string{"--sink.nats.urls=" + f.nats.url, "--sink.nats.mode=core"}
	}
	return []string{"--sink.nats.urls=" + f.nats.url, "--sink.nats.stream=" + f.stream}
}

// args is localArgs plus the fixture's sink, then extra.
func (f *fixture) args(dir string, extra ...string) []string {
	return append(append(localArgs(dir), f.sinkArgs()...), extra...)
}

// start boots an instance of f.id with f.args(dir, extra...) and waits until
// every transport is open.
func (f *fixture) start(dir string, extra ...string) *instance {
	f.t.Helper()
	in := launch(f.t, f.id, f.args(dir, extra...), nil)
	in.waitReady()
	return in
}

// natsMsg is one message read back from the stream: the subject and headers of
// SPEC §3.8 and the JLH in Data, validated by the oracle.
type natsMsg struct {
	subject string
	header  nats.Header
	data    []byte
	env     jlhEnv
}

// count is the number of messages the fixture's stream holds; -1 when it
// cannot be read.
func (f *fixture) count() int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := f.nats.js.Stream(ctx, f.stream)
	if err != nil {
		return -1
	}
	info, err := s.Info(ctx)
	if err != nil {
		return -1
	}
	return int(info.State.Msgs)
}

// msgs reads every message of the stream through an ordered consumer, the way
// a LogHub consumer would, in stream order.
func (f *fixture) msgs() []natsMsg {
	f.t.Helper()
	n := f.count()
	if n < 0 {
		f.t.Fatalf("cannot read stream %s", f.stream)
	}
	if n == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := f.nats.js.Stream(ctx, f.stream)
	if err != nil {
		f.t.Fatal(err)
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
	if err != nil {
		f.t.Fatal(err)
	}
	out := make([]natsMsg, 0, n)
	for len(out) < n {
		batch, err := cons.Fetch(n-len(out), jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			f.t.Fatal(err)
		}
		got := 0
		for m := range batch.Messages() {
			got++
			out = append(out, natsMsg{subject: m.Subject(), header: m.Headers(),
				data: append([]byte(nil), m.Data()...), env: mustJLH(f.t, m.Data())})
		}
		if got == 0 {
			f.t.Fatalf("consumed %d of %d messages of %s", len(out), n, f.stream)
		}
	}
	return out
}

// waitMsgs waits until the stream holds at least n messages and returns them
// all.
func (f *fixture) waitMsgs(n int) []natsMsg {
	f.t.Helper()
	waitFor(f.t, 30*time.Second, fmt.Sprintf("%d messages in %s", n, f.stream), func() bool {
		return f.count() >= n
	})
	return f.msgs()
}

// checkMsg validates the wire shape of SPEC §3.8 around a type=syslog JLH:
// the exact subject of the envelope's OWN space, Nats-Msg-Id <uuid>.<step>
// (never the bare uuid), the LogHub headers, and type=syslog at step 0 with a
// severity for level. jlhcheck already proved the rest of the envelope.
func checkMsg(t *testing.T, m natsMsg) jlhEnv {
	t.Helper()
	e := m.env
	if want := subject(e.Space); m.subject != want {
		t.Errorf("%s went to %q, want the exact %q", e.UUID, m.subject, want)
	}
	for k, want := range map[string]string{
		"Nats-Msg-Id":    e.UUID + ".0",
		"Content-Type":   "application/json",
		"X-LogHub-Type":  "syslog",
		"X-LogHub-UUID":  e.UUID,
		"X-LogHub-Space": e.Space,
	} {
		if got := m.header.Get(k); got != want {
			t.Errorf("%s: header %s = %q, want %q", e.UUID, k, got, want)
		}
	}
	if e.Type != "syslog" || e.Step != 0 || e.Level > 7 {
		t.Errorf("%s: type=%q step=%d level=%d", e.UUID, e.Type, e.Step, e.Level)
	}
	// Invariant §20.25: every envelope says RELP, octet-counting, over tcp.
	if p := e.Payload; show(p.XTransport) != "relp" || p.Framing != "octet-counting" || p.Transport != "tcp" {
		t.Errorf("%s: x-transport=%s framing=%q transport=%q, want relp, octet-counting, tcp",
			e.UUID, show(p.XTransport), p.Framing, p.Transport)
	}
	return e
}

// byMessage indexes the envelopes of msgs by payload.message, checking each.
func byMessage(t *testing.T, msgs []natsMsg) map[string]jlhEnv {
	t.Helper()
	out := make(map[string]jlhEnv, len(msgs))
	for _, m := range msgs {
		e := checkMsg(t, m)
		out[e.Payload.message()] = e
	}
	return out
}

// messageWith finds the envelope whose message contains marker.
func messageWith(t *testing.T, envs map[string]jlhEnv, marker string) jlhEnv {
	t.Helper()
	for m, e := range envs {
		if strings.Contains(m, marker) {
			return e
		}
	}
	t.Fatalf("no envelope carries %q; messages: %q", marker, keysOfEnv(envs))
	return jlhEnv{}
}

// beacon is the part of a beacon envelope (SPEC §11) the tests read.
type beacon struct {
	UUID    string `json:"uuid"`
	From    string `json:"from"`
	Space   string `json:"space"`
	Type    string `json:"type"`
	Level   int    `json:"level"`
	SID     int    `json:"sid"`
	Step    int    `json:"step"`
	Payload struct {
		Name     string `json:"name"`
		Hostname string `json:"hostname"`
		Machine  string `json:"machine"`
		Address  string `json:"address"`
		Uptime   int64  `json:"uptime"`
		Meta     []struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		} `json:"meta"`
	} `json:"payload"`
}

// beacons subscribes, by core NATS, to <ns>.beacon BEFORE the instance boots,
// so the beacon of the first connection cannot be missed.
func (f *fixture) beacons(ns string) *nats.Subscription {
	f.t.Helper()
	sub, err := f.nats.nc.SubscribeSync(ns + ".beacon")
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := f.nats.nc.Flush(); err != nil {
		f.t.Fatal(err)
	}
	return sub
}

// nextBeacon waits up to timeout for the next beacon of from, validating every
// beacon it reads with the envelope schema. ok is false on timeout.
func nextBeacon(t *testing.T, sub *nats.Subscription, from string, timeout time.Duration) (b beacon, ok bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return beacon{}, false
		}
		m, err := sub.NextMsg(left)
		if err != nil {
			return beacon{}, false
		}
		var got beacon
		if err := json.Unmarshal(m.Data, &got); err != nil {
			t.Fatalf("beacon: %v\n%s", err, m.Data)
		}
		if got.From != from {
			continue
		}
		if err := jlhcheck.ValidateEnvelopeSchema(m.Data); err != nil {
			t.Errorf("the beacon is not a valid JLH envelope: %v\n%s", err, m.Data)
		}
		return got, true
	}
}

// ---------------------------------------------------------------- dumps

// dumpFiles lists the pending dumps of dir (emergency.dir) written by from.
func dumpFiles(t *testing.T, dir, from string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "emg-"+from+"-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// dumpLines reads a dump the way an operator with jq would: line by line, each
// line a whole JLH envelope validated by the oracle (SPEC §12.1), and each line
// also accepted by the server's own reader, which is what the next boot runs.
func dumpLines(t *testing.T, path string) []jlhEnv {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatalf("%s does not end with a newline: a dump is whole lines only", path)
	}
	var out []jlhEnv
	for _, line := range strings.SplitAfter(string(b[:len(b)-1]), "\n") {
		line = strings.TrimSuffix(line, "\n")
		e := mustJLH(t, []byte(line))
		if _, err := emergency.ParseLine([]byte(line)); err != nil {
			t.Errorf("the server's reader refuses a line it wrote: %v\n%s", err, line)
		}
		out = append(out, e)
	}
	return out
}

// checkReingested proves that msgs are the dumped envelopes: the same uuids,
// the JLH in Data byte for byte the line of the dump (no conversion runs at
// reingestion, SPEC §12.2), on the subject of the line's OWN space -- whoever
// reingests it -- with the headers of SPEC §3.8.
func checkReingested(t *testing.T, dumped []jlhEnv, msgs []natsMsg) {
	t.Helper()
	if len(msgs) != len(dumped) {
		t.Fatalf("%d messages for %d dumped envelopes", len(msgs), len(dumped))
	}
	byUUID := make(map[string]natsMsg, len(msgs))
	for _, m := range msgs {
		checkMsg(t, m)
		byUUID[m.env.UUID] = m
	}
	for _, d := range dumped {
		m, ok := byUUID[d.UUID]
		if !ok {
			t.Errorf("dumped envelope %s is not in the stream", d.UUID)
			continue
		}
		if string(m.data) != string(d.bytes) {
			t.Errorf("message %s is not the dumped line byte for byte\n msg %s\nline %s", d.UUID, m.data, d.bytes)
		}
		if want := subject(d.Space); m.subject != want {
			t.Errorf("message %s went to %q, want the dumped space's %q", d.UUID, m.subject, want)
		}
	}
}

// testConverter is the converter of a server booted with localArgs: what a
// previous life with the default [syslog] keys would have dumped with.
func testConverter(t *testing.T) *convert.Converter {
	t.Helper()
	loaded, err := config.Load(config.Options{Args: localArgs(t.TempDir())})
	if err != nil {
		t.Fatal(err)
	}
	return convert.New((&App{cfg: loaded.Config}).convertConfig())
}

func withoutFlag(args []string, prefix string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if !strings.HasPrefix(a, prefix) {
			out = append(out, a)
		}
	}
	return out
}

// instance is one running server.
type instance struct {
	t      *testing.T
	app    *App
	dir    string
	cancel context.CancelFunc
	done   chan int

	mu     sync.Mutex
	exited bool
	code   int
}

// launch boots an instance and returns as soon as the App is assembled (the
// hook ran), without waiting for its transports. wrap, when set, decorates the
// real sink (App.wrapSink). hooks run inside runContext's hook, on the goroutine
// that then runs the App: whatever they capture is visible to the App's own
// goroutines without a race.
func launch(t *testing.T, id ident.Identity, args []string, wrap func(sink.Sink) sink.Sink, hooks ...func(*App)) *instance {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	in := &instance{t: t, cancel: cancel, done: make(chan int, 1)}
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, "--server.data_dir="); ok {
			in.dir = v
		}
	}
	appCh := make(chan *App, 1)
	go func() {
		in.done <- runContext(ctx, args, id, func(a *App) {
			// The test owns the second signal (SPEC §13.4) and the readiness
			// channel, so it can read both without racing run().
			a.abort = make(chan os.Signal, 1)
			a.ready = make(chan struct{})
			if wrap != nil {
				a.wrapSink = wrap
			}
			for _, h := range hooks {
				h(a)
			}
			appCh <- a
		})
	}()
	// Whatever happens in the test, the instance does not outlive it.
	t.Cleanup(func() {
		cancel()
		in.waitExit(60 * time.Second)
	})
	select {
	case a := <-appCh:
		in.app = a
	case code := <-in.done:
		in.record(code)
		t.Fatalf("the instance exited before its hook ran, with code %d", code)
	case <-time.After(30 * time.Second):
		t.Fatal("the instance never reached the hook")
	}
	return in
}

func (in *instance) record(code int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.exited {
		in.exited, in.code = true, code
	}
}

// waitReady blocks until every transport is open (SPEC §13.1 step 12).
func (in *instance) waitReady() {
	in.t.Helper()
	select {
	case <-in.app.Ready():
	case code := <-in.done:
		in.record(code)
		in.t.Fatalf("the instance exited before its transports opened, with code %d", code)
	case <-time.After(60 * time.Second):
		in.t.Fatal("the transports never opened")
	}
}

// waitExit waits for the instance to end and returns its exit code; -1 on
// timeout.
func (in *instance) waitExit(timeout time.Duration) int {
	in.mu.Lock()
	if in.exited {
		defer in.mu.Unlock()
		return in.code
	}
	in.mu.Unlock()
	select {
	case code := <-in.done:
		in.record(code)
		return code
	case <-time.After(timeout):
		return -1
	}
}

// stop is the equivalent of SIGTERM: the context the binary derives from the
// signal is cancelled, and the same graceful shutdown runs.
func (in *instance) stop() int {
	in.t.Helper()
	in.cancel()
	code := in.waitExit(60 * time.Second)
	if code == -1 {
		in.t.Fatal("the instance did not shut down within 60s")
	}
	return code
}

// get queries the admin port.
func (in *instance) get(path string) (int, string) {
	in.t.Helper()
	c := &http.Client{Timeout: 10 * time.Second}
	resp, err := c.Get("http://" + in.app.admin.Addr() + path)
	if err != nil {
		in.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// scrape reads /metrics into series -> value, the series being the exposition
// text before the value: name{labels}.
func (in *instance) scrape() map[string]string {
	in.t.Helper()
	code, body := in.get("/metrics")
	if code != http.StatusOK {
		in.t.Fatalf("/metrics answered %d", code)
	}
	return parseExposition(body)
}

func parseExposition(body string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		out[line[:i]] = line[i+1:]
	}
	return out
}

// metric reads one series from /metrics, failing when it is absent: a helper
// that returned zero for a missing series would make the test pass without
// proving anything (AGENTS.md §9).
func (in *instance) metric(series string) string {
	in.t.Helper()
	v, ok := in.scrape()[series]
	if !ok {
		in.t.Fatalf("/metrics has no series %s", series)
	}
	return v
}

// emgDir is the instance's emergency.dir.
func (in *instance) emgDir() string { return in.app.cfg.Emergency.Dir }

// ---------------------------------------------------------------- polling

// waitFor polls cond until it holds, failing the test after timeout. Every
// wait in this suite is a condition, never a fixed sleep.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// eventually is waitFor without the failure, for checks that report on their own.
func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------- senders

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("%q: %v", addr, err)
	}
	return p
}

// freePort reserves a TCP port number on 127.0.0.1 and releases it, for the
// tests that must know a transport's address BEFORE the transport exists.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func dialTCP(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return c
}

// ---------------------------------------------------------------- RELP

// A minimal RELP client, the frames librelp speaks (open with its offers,
// syslog, close), modelled on the receiver's test client. It never hides what
// the server sent: every frame comes back as it was read, and every helper
// that "sends" REQUIRES the answer it expects, so a test built on it cannot
// pass by sending into a socket nobody acknowledged.
//
// The error-returning core (the *Err methods) is what the goroutines of the
// concurrency test use, since t.Fatal may only be called from the test's own
// goroutine; everything else goes through the failing wrappers.

// relpOffers is what librelp 1.11 sends in its open.
const relpOffers = "relp_version=0\nrelp_software=librelp,1.11.0,http://librelp.adiscon.com\ncommands=syslog"

// relpRSPOK is the body of a positive answer (SPEC §5.4).
const relpRSPOK = "200 OK"

// relpChunk is how many syslog frames the senders put on the wire before
// reading their answers: below relp.window (128), as an emitter's own window
// would be, so a sender never waits on the server's window to read its frames.
const relpChunk = 64

// relpReadTimeout bounds one read of a server frame. It is long because a
// held transaction (a DOWN sink, a full ring) gets its 200 only once it clears.
const relpReadTimeout = 20 * time.Second

type relpFrame struct {
	txnr int64
	cmd  string
	data string
}

func (f relpFrame) String() string { return fmt.Sprintf("%d %s %q", f.txnr, f.cmd, f.data) }

type relpConn struct {
	t    *testing.T
	c    net.Conn
	br   *bufio.Reader
	txnr int64
}

// newRELPConn wraps a connection that is ready for RELP -- after a PROXY
// header -- without opening the session.
func newRELPConn(t *testing.T, c net.Conn) *relpConn {
	t.Helper()
	t.Cleanup(func() { c.Close() })
	return &relpConn{t: t, c: c, br: bufio.NewReader(c)}
}

// dialRELP connects to addr and opens the session, requiring its 200.
func dialRELP(t *testing.T, addr string) *relpConn {
	t.Helper()
	rc := newRELPConn(t, dialTCP(t, addr))
	rc.open()
	return rc
}

func (rc *relpConn) fatal(err error) {
	rc.t.Helper()
	if err != nil {
		rc.t.Fatal(err)
	}
}

// openErr sends the open command and requires a 200 carrying the protocol
// offers back.
func (rc *relpConn) openErr() error {
	n, err := rc.sendErr("open", relpOffers)
	if err != nil {
		return err
	}
	f, err := rc.tryRead(relpReadTimeout)
	if err != nil {
		return fmt.Errorf("reading the answer to open: %w", err)
	}
	if f.txnr != n || f.cmd != "rsp" || !strings.HasPrefix(f.data, relpRSPOK+"\n") ||
		!strings.Contains(f.data, "relp_version=0") || !strings.Contains(f.data, "commands=syslog") {
		return fmt.Errorf("open answered %v", f)
	}
	return nil
}

func (rc *relpConn) open() { rc.t.Helper(); rc.fatal(rc.openErr()) }

func relpFrameOf(txnr int64, cmd, data string) string {
	if data == "" {
		return fmt.Sprintf("%d %s 0\n", txnr, cmd)
	}
	return fmt.Sprintf("%d %s %d %s\n", txnr, cmd, len(data), data)
}

func (rc *relpConn) write(s string) error {
	_ = rc.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(rc.c, s); err != nil {
		return fmt.Errorf("RELP write: %w", err)
	}
	return nil
}

func (rc *relpConn) sendErr(cmd, data string) (int64, error) {
	rc.txnr++
	return rc.txnr, rc.write(relpFrameOf(rc.txnr, cmd, data))
}

// syslogErr writes one syslog frame per message, in one write, and returns
// their txnrs. It does not read anything.
func (rc *relpConn) syslogErr(msgs ...string) ([]int64, error) {
	var b strings.Builder
	ids := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		rc.txnr++
		b.WriteString(relpFrameOf(rc.txnr, "syslog", m))
		ids = append(ids, rc.txnr)
	}
	return ids, rc.write(b.String())
}

// syslog is syslogErr that fails the test.
func (rc *relpConn) syslog(msgs ...string) []int64 {
	rc.t.Helper()
	ids, err := rc.syslogErr(msgs...)
	rc.fatal(err)
	return ids
}

// tryRead returns the next server frame, or the error that ended the read. It
// parses the way librelp's client does: after DATALEN 0 the trailer must
// follow at once (relpframe.c).
func (rc *relpConn) tryRead(d time.Duration) (relpFrame, error) {
	_ = rc.c.SetReadDeadline(time.Now().Add(d))
	var f relpFrame
	head, err := rc.br.ReadString(' ')
	if err != nil {
		return f, err
	}
	if f.txnr, err = strconv.ParseInt(strings.TrimSuffix(head, " "), 10, 64); err != nil {
		return f, fmt.Errorf("bad txnr %q", head)
	}
	cmd, err := rc.br.ReadString(' ')
	if err != nil {
		return f, err
	}
	f.cmd = strings.TrimSuffix(cmd, " ")
	n := 0
	for {
		c, err := rc.br.ReadByte()
		if err != nil {
			return f, err
		}
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
			continue
		}
		if n == 0 {
			if c != '\n' {
				return f, fmt.Errorf("DATALEN 0 followed by %q, not the trailer", c)
			}
			return f, nil
		}
		if c != ' ' {
			return f, fmt.Errorf("DATALEN followed by %q", c)
		}
		break
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(rc.br, data); err != nil {
		return f, err
	}
	if c, err := rc.br.ReadByte(); err != nil || c != '\n' {
		return f, fmt.Errorf("missing trailer (%v)", err)
	}
	f.data = string(data)
	return f, nil
}

// read returns the next server frame, failing the test after relpReadTimeout.
func (rc *relpConn) read() relpFrame {
	rc.t.Helper()
	f, err := rc.tryRead(relpReadTimeout)
	if err != nil {
		rc.t.Fatalf("reading a RELP frame: %v", err)
	}
	return f
}

// responsesErr reads the answers to ids, which may come in any order (the
// window decides transactions concurrently, and the txnr travels in the
// answer), and returns their bodies in the order of ids.
func (rc *relpConn) responsesErr(ids []int64) ([]string, error) {
	want := make(map[int64]int, len(ids))
	for i, id := range ids {
		want[id] = i
	}
	out := make([]string, len(ids))
	for got := 0; got < len(ids); got++ {
		f, err := rc.tryRead(relpReadTimeout)
		if err != nil {
			return out, fmt.Errorf("after %d of %d answers: %w", got, len(ids), err)
		}
		i, ok := want[f.txnr]
		if f.cmd != "rsp" || !ok {
			return out, fmt.Errorf("got %v, want the rsp of one of the %d transactions sent", f, len(ids))
		}
		delete(want, f.txnr)
		out[i] = f.data
	}
	return out, nil
}

// responses is responsesErr that fails the test.
func (rc *relpConn) responses(ids []int64) []string {
	rc.t.Helper()
	out, err := rc.responsesErr(ids)
	rc.fatal(err)
	return out
}

// transactErr sends msgs relpChunk at a time and returns every answer, in the
// order of msgs.
func (rc *relpConn) transactErr(msgs ...string) ([]string, error) {
	out := make([]string, 0, len(msgs))
	for i := 0; i < len(msgs); i += relpChunk {
		ids, err := rc.syslogErr(msgs[i:min(i+relpChunk, len(msgs))]...)
		if err != nil {
			return out, err
		}
		got, err := rc.responsesErr(ids)
		out = append(out, got...)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// transact is transactErr that fails the test.
func (rc *relpConn) transact(msgs ...string) []string {
	rc.t.Helper()
	out, err := rc.transactErr(msgs...)
	rc.fatal(err)
	return out
}

// sendOKErr sends msgs and requires a "200 OK" for every one of them.
func (rc *relpConn) sendOKErr(msgs ...string) error {
	got, err := rc.transactErr(msgs...)
	if err != nil {
		return err
	}
	for i, body := range got {
		if body != relpRSPOK {
			return fmt.Errorf("message %d (%.60q) answered %q, want %q", i, msgs[i], body, relpRSPOK)
		}
	}
	return nil
}

// sendOK is sendOKErr that fails the test.
func (rc *relpConn) sendOK(msgs ...string) { rc.t.Helper(); rc.fatal(rc.sendOKErr(msgs...)) }

// closeErr ends the session the way librelp does: close, its empty rsp, and
// then the server closes the connection (cclose.c).
func (rc *relpConn) closeErr() error {
	n, err := rc.sendErr("close", "")
	if err != nil {
		return err
	}
	f, err := rc.tryRead(relpReadTimeout)
	if err != nil {
		return fmt.Errorf("reading the answer to close: %w", err)
	}
	if f.txnr != n || f.cmd != "rsp" || f.data != "" {
		return fmt.Errorf("close answered %v, want an empty rsp %d", f, n)
	}
	if f, err := rc.tryRead(5 * time.Second); err == nil {
		return fmt.Errorf("got %v after the close, want the connection closed", f)
	} else if isTimeout(err) {
		return errors.New("the server did not close the connection after close")
	}
	return rc.c.Close()
}

func (rc *relpConn) close() { rc.t.Helper(); rc.fatal(rc.closeErr()) }

// expectNothing requires that no frame arrives for d: the transaction is
// held, neither answered nor refused.
func (rc *relpConn) expectNothing(d time.Duration) {
	rc.t.Helper()
	if f, err := rc.tryRead(d); err == nil {
		rc.t.Fatalf("unexpected RELP frame %v", f)
	} else if !isTimeout(err) {
		rc.t.Fatalf("the connection ended: %v", err)
	}
	// A timed-out read that consumed nothing leaves the reader intact; a frame
	// that started inside the window would have been a failure above unless it
	// was cut mid-way, which the next read reports as a parse error.
}

// expectServerClose requires the serverclose hint (0 serverclose 0) and then
// the end of the connection, within d.
func (rc *relpConn) expectServerClose(d time.Duration) {
	rc.t.Helper()
	f, err := rc.tryRead(d)
	if err != nil {
		rc.t.Fatalf("waiting for serverclose: %v", err)
	}
	if f.txnr != 0 || f.cmd != "serverclose" || f.data != "" {
		rc.t.Fatalf("got %v, want 0 serverclose 0", f)
	}
	if f, err := rc.tryRead(5 * time.Second); err == nil {
		rc.t.Fatalf("got %v after serverclose, want the connection closed", f)
	} else if isTimeout(err) {
		rc.t.Fatal("the server did not close the connection after serverclose")
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// sendRELP is what an emitter does with a batch: open a session, send every
// message, require a "200 OK" for each, close. When it returns, the server has
// confirmed every message -- the ring's guarantee is met (SPEC §5.4) -- so a
// test may look at the ring or the destination at once.
func sendRELP(t *testing.T, addr string, msgs ...string) {
	t.Helper()
	rc := dialRELP(t, addr)
	rc.sendOK(msgs...)
	rc.close()
}

// sendRELPErr is sendRELP for a goroutine: nothing fails the test from here.
func sendRELPErr(addr string, msgs ...string) error {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	rc := &relpConn{c: c, br: bufio.NewReader(c)}
	if err := rc.openErr(); err != nil {
		return err
	}
	if err := rc.sendOKErr(msgs...); err != nil {
		return err
	}
	return rc.closeErr()
}

// ---------------------------------------------------------------- the JLH

// jlhEnv is an envelope read back from the stream or a dump. Pointer fields tell
// "absent" from "empty", which is the omission rule of SPEC §3.2.
type jlhEnv struct {
	UUID  string `json:"uuid"`
	From  string `json:"from"`
	Space string `json:"space"`
	Type  string `json:"type"`
	Level int    `json:"level"`
	SID   uint64 `json:"sid"`
	Step  int    `json:"step"`
	Event struct {
		UTS int64  `json:"uts"`
		Sub int64  `json:"sub"`
		ISO string `json:"iso"`
	} `json:"event"`
	Payload jlhPayload `json:"payload"`

	bytes []byte
}

type jlhHost struct {
	Hostname *string `json:"hostname"`
	SrcIP    *string `json:"src-ip"`
	SrcPort  *int    `json:"src-port"`
	DstIP    *string `json:"dst-ip"`
	DstPort  *int    `json:"dst-port"`
}

type codeName struct {
	Code int    `json:"code"`
	Name string `json:"name"`
}

type jlhPayload struct {
	Host      *jlhHost                  `json:"host"`
	Transport string                    `json:"transport"`
	Framing   string                    `json:"framing"`
	Format    string                    `json:"format"`
	Version   *int                      `json:"version"`
	Priority  int                       `json:"priority"`
	Facility  codeName                  `json:"facility"`
	Severity  codeName                  `json:"severity"`
	Timestamp *string                   `json:"timestamp"`
	AppName   *string                   `json:"appname"`
	ProcID    *string                   `json:"procid"`
	MsgID     *string                   `json:"msgid"`
	SD        map[string]map[string]any `json:"structured-data"`
	Message   *string                   `json:"message"`
	Size      int                       `json:"size"`
	Raw       *string                   `json:"raw"`
	RawEnc    string                    `json:"raw-encoding"`
	Parse     *struct {
		Warnings []string `json:"warnings"`
		Error    string   `json:"error"`
	} `json:"parse"`
	XProxy *struct {
		PeerIP   string `json:"peer-ip"`
		PeerPort int    `json:"peer-port"`
	} `json:"x-proxy"`
	// XTransport is this server's payload extension (decision 2 of
	// go-relplog-server, SPEC §3.2): the skill's transport enum has no relp.
	XTransport *string `json:"x-transport"`
}

// show renders an optional field for an error message.
func show[T any](p *T) string {
	if p == nil {
		return "<absent>"
	}
	return fmt.Sprint(*p)
}

func (p jlhPayload) message() string {
	if p.Message == nil {
		return ""
	}
	return *p.Message
}

// rawBytes decodes payload.raw (base64 when raw-encoding says so).
func (p jlhPayload) rawBytes() []byte {
	if p.Raw == nil {
		return nil
	}
	if p.RawEnc == "base64" {
		b, err := base64.StdEncoding.DecodeString(*p.Raw)
		if err != nil {
			return nil
		}
		return b
	}
	return []byte(*p.Raw)
}

func (p jlhPayload) hasWarning(w string) bool {
	if p.Parse == nil {
		return false
	}
	for _, x := range p.Parse.Warnings {
		if x == w {
			return true
		}
	}
	return false
}

func (p jlhPayload) hostname() *string {
	if p.Host == nil {
		return nil
	}
	return p.Host.Hostname
}

// mustJLH validates one envelope with the conformance oracle -- both LogHub
// schemas and the semantic rules (SPEC §18.2) -- and decodes it. EVERY envelope
// this suite reads back from the stream or a dump goes through here.
func mustJLH(t *testing.T, b []byte) jlhEnv {
	t.Helper()
	if err := jlhcheck.Validate(b); err != nil {
		t.Errorf("the envelope is not a valid JLH: %v\n%s", err, b)
	}
	var e jlhEnv
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("the envelope does not decode: %v\n%s", err, b)
	}
	e.bytes = append([]byte(nil), b...)
	return e
}

// ---------------------------------------------------------------- sinks

// faultySink decorates the real sink through App.wrapSink. Each knob produces
// one incident shape the health machine cannot see from the outside:
//
//   - failFatal: every write fails with ClassFatal, which ends the process
//     through the FATAL state (SPEC §13.3).
//   - failWrites: every write fails with an unclassified error -- network class
//     (invariant §20.18) -- while Health keeps passing. The state never goes
//     DOWN, receivers keep accepting, and the workers HOLD their batches outside
//     the ring (SPEC §10.2). That is the incident the emergency dump exists for.
//   - gate: writes block until the gate opens (or their op_timeout ends), so a
//     test can look at the ring while the destination has not confirmed.
//   - failHealth: the health probe fails, which is what drives DOWN (SPEC §13.3).
type faultySink struct {
	sink.Sink

	failWrites atomic.Bool
	failFatal  atomic.Bool
	failHealth atomic.Bool
	// fatalHealth: the health probe fails with ClassFatal, which WaitReady
	// does not retry -- the shape of a stream that is gone or credentials
	// that are refused (SPEC §13.1 step 7).
	fatalHealth atomic.Bool

	gate     chan struct{}
	gateOnce sync.Once

	attempts atomic.Int64

	// onWrite observes every batch that reaches the real sink. Set before the
	// instance starts; it runs on the writer's goroutine.
	onWrite func(*sink.Batch)

	// decide, when set, may fail a batch on its contents (a logic refusal of
	// one message, a transient failure of the first attempts) before it
	// reaches the real sink. Set before the instance starts.
	decide func(*sink.Batch) error
}

func newFaultySink(gated bool) *faultySink {
	f := &faultySink{}
	if gated {
		f.gate = make(chan struct{})
	}
	return f
}

// wrap is what goes into App.wrapSink.
func (f *faultySink) wrap(s sink.Sink) sink.Sink {
	f.Sink = s
	return f
}

func (f *faultySink) open() {
	if f.gate != nil {
		f.gateOnce.Do(func() { close(f.gate) })
	}
}

func (f *faultySink) Write(ctx context.Context, b *sink.Batch) error {
	f.attempts.Add(1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.failFatal.Load() {
		// The shape of a deleted stream or a revoked publish permission: the
		// destination answered and will not change its mind (SPEC §13.3).
		return &sink.Error{Class: sink.ClassFatal, Op: "write", Err: errors.New("stream not found (injected)")}
	}
	if f.failWrites.Load() {
		return errors.New("destination unreachable (injected)")
	}
	if f.decide != nil {
		if err := f.decide(b); err != nil {
			return err
		}
	}
	if f.onWrite != nil {
		f.onWrite(b)
	}
	return f.Sink.Write(ctx, b)
}

func (f *faultySink) Health(ctx context.Context) error {
	if f.fatalHealth.Load() {
		return &sink.Error{Class: sink.ClassFatal, Op: "health", Err: errors.New("stream not found (injected)")}
	}
	if f.failHealth.Load() {
		return errors.New("health probe failed (injected)")
	}
	return f.Sink.Health(ctx)
}

// keysOfEnv lists the messages of envs, sorted, for an error message.
func keysOfEnv(m map[string]jlhEnv) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------- logs

// logBuf collects an instance's log lines, safe for the App's goroutines.
type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLog is a launch hook that sends the instance's log, at info, to buf.
// baseArgs keeps the level at error; the WARN lines are what these tests read.
func captureLog(buf *logBuf) func(*App) {
	return func(a *App) { a.log = obs.NewLogger(buf, "json", "info") }
}
