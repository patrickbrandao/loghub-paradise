package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ring"
)

// The integration suite drives the SAME wiring the binary uses (app.Run), so
// what it proves is what ships. A test that assembles its own approximation of
// the stack proves nothing about the stack.

// ---------------------------------------------------------------- identity

// testID is the identity every instance runs with unless a test needs a
// Workspace of its own. It is injected rather than resolved: go-loghub-ident's
// Initialize may run once per process, and these instances share one (SPEC
// §3.5). Fixed, so that from and the dump's filename have a value to be
// compared with.
var testID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space:     "default",
	AgentName: "go-webhook-nats-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

// ---------------------------------------------------------------- harness

type instance struct {
	t        *testing.T
	app      *App // the assembled app, for tests that poke at it directly
	id       ident.Identity
	dir      string
	pub      string // public address
	adm      string // admin address
	done     chan int
	args     []string
	cancel   context.CancelFunc
	stopOnce sync.Once

	// mode is the payload.mode every 200 so far answered with (SPEC §4.4).
	mode string

	// abort stands in for the second signal of SPEC §10.3. A real one cannot be
	// used: the instance runs inside the test binary.
	abort chan os.Signal

	// mem is the fake destination of an instance started with start; nil for
	// one started against NATS (startNATS).
	mem *memSink
}

// interrupt delivers the second signal that skips the rest of the drain and
// exits 130.
func (in *instance) interrupt() {
	select {
	case in.abort <- syscall.SIGTERM:
	default:
	}
}

// freePort reserves a port and releases it, so two instances in one test run
// cannot collide on a hard-coded number.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// start launches an instance in-process with testID and a fresh fake
// destination (fakesink_test.go), and waits for it to become ready.
func start(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	return startWith(t, dir, testID, &memSink{}, extra...)
}

// startNATS is start against the real NATS the arguments name (nats_test.go).
func startNATS(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	return startWith(t, dir, testID, nil, extra...)
}

// startWith is start with an identity and a destination of the test's
// choosing: ms, or the NATS sink of the arguments when ms is nil.
// startLog, when set by a test, receives the log of the instances it starts
// (at info), for the tests that assert what the operator is told. startRing,
// when set, wraps their memory ring (App.wrapRing).
var (
	startLog  io.Writer
	startRing func(*ring.Memory) ring.Ring
)

func startWith(t *testing.T, dir string, id ident.Identity, ms *memSink, extra ...string) *instance {
	t.Helper()

	in := &instance{
		t:    t,
		id:   id,
		dir:  dir,
		pub:  freePort(t),
		adm:  freePort(t),
		done: make(chan int, 1),
		mem:  ms,
	}
	in.args = append([]string{
		"--server.bind=" + in.pub,
		"--admin.bind=" + in.adm,
		"--ring.data_dir=" + dir,
		"--log.level=error",
		"--log.format=text",
		// Short health cadence so a DOWN sink is detected inside a test's
		// patience rather than the production default of seconds.
		"--sink.health_interval=100ms",
		"--sink.health_timeout=100ms",
		"--sink.boot_timeout=5s",
	}, extra...)

	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	in.abort = make(chan os.Signal, 2)
	hooked := make(chan struct{})
	go func() {
		in.done <- runContext(ctx, in.args, id, func(a *App) {
			a.abort = in.abort
			if ms != nil {
				a.sinkOverride = ms
			}
			if startLog != nil {
				a.log = obs.NewLogger(startLog, "text", "info")
			}
			a.wrapRing = startRing
			in.app = a
			close(hooked)
		})
	}()

	if !in.waitReady(10 * time.Second) {
		in.stop()
		t.Fatalf("instance never became ready; args=%v", in.args)
	}
	<-hooked // in.app is published before the tests read it
	t.Cleanup(func() { in.stop() })
	return in
}

// startExpectingExit launches an instance against the NATS of the arguments
// that is expected to fail during boot.
func startExpectingExit(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	return startExpectingExitWith(t, dir, nil, extra...)
}

// startExpectingExitWith is startExpectingExit with the fake destination ms,
// or NATS when ms is nil.
func startExpectingExitWith(t *testing.T, dir string, ms *memSink, extra ...string) *instance {
	t.Helper()
	in := &instance{
		t: t, id: testID, dir: dir,
		pub: freePort(t), adm: freePort(t),
		done: make(chan int, 1), mem: ms,
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
	go func() {
		in.done <- runContext(ctx, in.args, in.id, func(a *App) {
			if ms != nil {
				a.sinkOverride = ms
			}
		})
	}()
	return in
}

func (in *instance) waitReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + in.adm + "/readyz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		select {
		case code := <-in.done:
			in.done <- code // put it back for the caller
			return false
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// answer is what one request got back.
type answer struct {
	code   int
	uuid   string // X-LogHub-UUID; "" for a refusal decided before the uuid existed
	mode   string // from the JSON body
	status string // from the JSON body
	stage  string // from the JSON body; only on a 200
}

// checkAnswer holds a response to SPEC §4.4: a 200, and a refusal decided after
// the uuid existed, carry X-LogHub-UUID and the JSON body with the same uuid; a
// refusal from before the uuid is one line of text/plain. X-Event-Id is gone.
func checkAnswer(t *testing.T, resp *http.Response, body []byte) answer {
	t.Helper()
	a := answer{code: resp.StatusCode, uuid: resp.Header.Get("X-LogHub-UUID")}
	if v := resp.Header.Get("X-Event-Id"); v != "" {
		t.Errorf("status %d carries X-Event-Id %q; the v1 header no longer exists", a.code, v)
	}
	ct := resp.Header.Get("Content-Type")

	if a.code != http.StatusOK && a.uuid == "" {
		if !strings.HasPrefix(ct, "text/plain") {
			t.Errorf("status %d without a uuid has Content-Type %q, want text/plain", a.code, ct)
		}
		return a
	}
	if a.uuid == "" {
		t.Errorf("a 200 without X-LogHub-UUID")
		return a
	}
	if u, err := uuid.Parse(a.uuid); err != nil || u.Version() != 7 || u.String() != a.uuid {
		t.Errorf("X-LogHub-UUID %q is not a canonical UUIDv7", a.uuid)
	}
	if ct != "application/json" {
		t.Errorf("status %d with a uuid has Content-Type %q, want application/json", a.code, ct)
	}
	var b struct {
		UUID   string `json:"uuid"`
		Mode   string `json:"mode"`
		Status string `json:"status"`
		Stage  string `json:"stage"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		t.Errorf("status %d: the body is not the answer of SPEC §4.4: %v\n%s", a.code, err, body)
		return a
	}
	a.mode, a.status, a.stage = b.Mode, b.Status, b.Stage
	if b.UUID != a.uuid {
		t.Errorf("body uuid %q differs from X-LogHub-UUID %q", b.UUID, a.uuid)
	}
	switch {
	case a.code != http.StatusOK:
		if b.Status != "error" || b.Stage != "" {
			t.Errorf("status %d answered status %q stage %q, want error and no stage", a.code, b.Status, b.Stage)
		}
	case b.Mode == envelope.ModeFast && b.Status == "accepted" && b.Stage == "ring":
	default:
		t.Errorf("a 200 answered mode %q status %q stage %q; want fast, accepted, ring",
			b.Mode, b.Status, b.Stage)
	}
	return a
}

// send performs one request and checks the answer's shape.
func (in *instance) send(method, path, contentType string, body io.Reader) answer {
	in.t.Helper()
	req, err := http.NewRequest(method, "http://"+in.pub+path, body)
	if err != nil {
		in.t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		in.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	a := checkAnswer(in.t, resp, b)
	if a.code == http.StatusOK {
		if in.mode == "" {
			in.mode = a.mode
		} else if a.mode != in.mode {
			in.t.Errorf("mode changed from %q to %q within one instance", in.mode, a.mode)
		}
	}
	return a
}

// post sends one webhook and returns the status and the uuid.
func (in *instance) post(path, body string) (int, string) {
	in.t.Helper()
	a := in.send(http.MethodPost, path, "application/json", strings.NewReader(body))
	return a.code, a.uuid
}

// postN sends n webhooks and returns the uuids that were accepted with 200.
func (in *instance) postN(n int) []string {
	in.t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		code, id := in.post(fmt.Sprintf("/e/%d", i), fmt.Sprintf(`{"n":%d}`, i))
		if code == http.StatusOK {
			if id == "" {
				in.t.Fatalf("request %d returned 200 with no X-LogHub-UUID", i)
			}
			ids = append(ids, id)
		}
	}
	return ids
}

func (in *instance) metrics() string {
	in.t.Helper()
	resp, err := http.Get("http://" + in.adm + "/metrics")
	if err != nil {
		in.t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func (in *instance) readyz() (int, string) {
	in.t.Helper()
	resp, err := http.Get("http://" + in.adm + "/readyz")
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// stop requests the same graceful shutdown a SIGTERM would, and waits for the
// exit code.
//
// It cancels a context rather than signalling: these instances run inside the
// test binary, so a real SIGTERM would take the test process down with them.
func (in *instance) stop() int {
	code := -1
	in.stopOnce.Do(func() {
		in.cancel()
		select {
		case code = <-in.done:
		case <-time.After(60 * time.Second):
			in.t.Error("instance did not exit within 60s of the shutdown request")
		}
	})
	return code
}

// waitExit waits for an instance to end ON ITS OWN -- a FATAL sink, a failed
// boot -- and records that it has.
//
// Reading in.done directly would leave the cleanup registered by start waiting
// on a channel that will never deliver again, turning a passing test into a
// sixty-second timeout.
func (in *instance) waitExit(timeout time.Duration) int {
	in.t.Helper()
	code := -1
	in.stopOnce.Do(func() {
		select {
		case code = <-in.done:
		case <-time.After(timeout):
			in.t.Errorf("the instance did not exit on its own within %s", timeout)
			in.cancel()
		}
	})
	return code
}

// ------------------------------------------------------- the JLH contract

// verify is the contract oracle of SPEC §15.1 over envelopes read back from a
// destination: each must pass jlhcheck (both schemas and the semantic rules),
// carry the identity and the pid of the instance that wrote it, and the mode
// the server promises (SPEC §3.2; "" skips that check). It returns
// the envelopes that passed. A failure is reported per envelope, so one bad
// message does not hide the others, and the uuid of a failed one is left out
// -- which the callers' missing() turns into a loss as well.
func verify(t *testing.T, raws [][]byte, id ident.Identity, mode string) []*jlhcheck.Envelope {
	t.Helper()
	out := make([]*jlhcheck.Envelope, 0, len(raws))
	for i, raw := range raws {
		e, err := jlhcheck.Validate(raw)
		if err != nil {
			t.Errorf("envelope %d breaks the JLH contract: %v\n%s", i, err, raw)
			continue
		}
		if e.From != id.From || e.Space != id.Space {
			t.Errorf("envelope %s: from=%s space=%s, want the instance's %s / %s",
				e.UUID, e.From, e.Space, id.From, id.Space)
		}
		if e.SID != int64(os.Getpid()) {
			t.Errorf("envelope %s: sid=%d, want the pid %d (SPEC §3.1)", e.UUID, e.SID, os.Getpid())
		}
		if mode != "" && e.Payload.Mode != mode {
			t.Errorf("envelope %s: payload.mode=%q, want %q",
				e.UUID, e.Payload.Mode, mode)
		}
		out = append(out, e)
	}
	return out
}

func uuidsOf(envs []*jlhcheck.Envelope) []string {
	out := make([]string, 0, len(envs))
	for _, e := range envs {
		out = append(out, e.UUID)
	}
	return out
}

func byUUID(t *testing.T, envs []*jlhcheck.Envelope) map[string]*jlhcheck.Envelope {
	t.Helper()
	m := make(map[string]*jlhcheck.Envelope, len(envs))
	for _, e := range envs {
		if m[e.UUID] != nil {
			t.Errorf("uuid %s landed twice", e.UUID)
		}
		m[e.UUID] = e
	}
	return m
}

// testJLH builds one envelope the way the server does, for the dumps the
// tests plant on disk. It is checked by the oracle before use, so a
// test that plants it proves something about the server and not about this
// helper.
func testJLH(t *testing.T, id ident.Identity, path, body string) (string, []byte) {
	t.Helper()
	now := time.Now()
	h := envelope.Header{UUID: envelope.NewUUID(now), From: id.From, Space: id.Space,
		Type: envelope.Type, Level: envelope.Level, SID: uint64(os.Getpid()), Event: envelope.EventAt(now)}
	r := &envelope.Request{Mode: envelope.ModeFast, Src: netip.MustParseAddr("203.0.113.9"),
		Dst: netip.MustParseAddr("127.0.0.1"), SPort: 5555, DPort: 8080,
		PeerAddr: netip.MustParseAddr("203.0.113.9"), PeerPort: 5555, SrcSource: "tcp",
		Proto: "HTTP/1.1", Method: "POST", Host: "example.com", URLPort: 8080, Path: path,
		Header:     http.Header{"Content-Type": {"application/json"}},
		HostHeader: "example.com:8080", ContentType: "application/json", Body: []byte(body)}
	env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(r.Body))
	verify(t, [][]byte{env}, id, envelope.ModeFast)
	return h.UUID.String(), env
}

// runRefused runs a boot that is expected to be refused. The context is
// bounded so that a boot wrongly ACCEPTED ends the instance -- with exit 0,
// which the caller reports -- instead of hanging the suite.
func runRefused(t *testing.T, args []string, id ident.Identity) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return RunContext(ctx, args, id)
}

func missing(want, got []string) []string {
	have := make(map[string]bool, len(got))
	for _, g := range got {
		have[g] = true
	}
	var out []string
	for _, w := range want {
		if !have[w] {
			out = append(out, w)
		}
	}
	return out
}

// eventsIn is what the fake destination of in holds, every envelope checked by
// the oracle.
func eventsIn(t *testing.T, in *instance) []string {
	t.Helper()
	return uuidsOf(verify(t, in.mem.stored(), testID, envelope.ModeFast))
}

// ------------------------------------------------------------- delivery

// SPEC §2.2, §16.1: the one pair this server has accepts traffic, reports
// itself in whs_config_info as the fast would (decision 3, SPEC §11.1), shuts
// down cleanly, and the destination holds every accepted uuid and nothing
// else, each envelope byte for byte the contract.
func TestEveryAcceptedEventIsDelivered(t *testing.T) {
	in := start(t, t.TempDir())

	const n = 25
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d requests were accepted", len(ids), n)
	}
	if in.mode != envelope.ModeFast {
		t.Errorf("the 200 answered mode %q, want fast", in.mode)
	}
	if m := in.metrics(); !strings.Contains(m, `whs_config_info{ring_mode="memory",sink_type="nats"} 1`) {
		t.Error("whs_config_info does not report memory/nats")
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want %d", code, ExitOK)
	}
	got := eventsIn(t, in)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d accepted events never reached the sink", len(lost), len(ids))
	}
	if extra := missing(got, ids); len(extra) > 0 {
		t.Errorf("%d events in the sink were never answered to a client", len(extra))
	}
}

// ------------------------------------------------------------ backpressure

// SPEC §17.2: a full ring is a 429. It is never a 200 followed by a discard.
func TestRingFullReturns429(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.capacity_items=5",
		// Stall the drain so the ring can actually fill.
		"--sink.batch_max_delay=1h",
	)

	var ok, full int
	for i := 0; i < 40; i++ {
		// send checks each 429 against SPEC §4.4: text/plain from the fast
		// check, the JSON error with the uuid from Enqueue.
		code, _ := in.post("/x", "body")
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			full++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if full == 0 {
		t.Error("the ring never returned 429; the capacity ceiling did nothing")
	}
	if ok > 10 {
		t.Errorf("%d requests were accepted with a capacity of 5", ok)
	}
	if m := in.metrics(); !strings.Contains(m, `whs_rejected_total{reason="ring_full"}`) {
		t.Error("ring_full rejections were not counted")
	}
	in.stop()
}

// -------------------------------------------------------------- HTTP rules

func TestHTTPSurface(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--server.max_body_bytes=1KiB",
		"--server.max_uri_bytes=256",
	)
	base := "http://" + in.pub

	// raw sends a request line the standard client will not produce and
	// returns the status line.
	raw := func(t *testing.T, line string) string {
		t.Helper()
		c, err := net.Dial("tcp", in.pub)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		fmt.Fprintf(c, "%s\r\nHost: example.com\r\nContent-Length: 0\r\n\r\n", line)
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 64)
		n, _ := c.Read(buf)
		status, _, _ := strings.Cut(string(buf[:n]), "\r\n")
		return status
	}

	t.Run("every method becomes an event", func(t *testing.T) {
		for _, m := range []string{"POST", "GET", "PUT", "DELETE", "OPTIONS", "PATCH", "FROBNICATE"} {
			req, _ := http.NewRequest(m, base+"/anything", strings.NewReader("b"))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: %v", m, err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			a := checkAnswer(t, resp, b)
			if a.code != http.StatusOK {
				t.Errorf("%s returned %d, want 200", m, a.code)
			}
			if a.uuid == "" {
				t.Errorf("%s: no X-LogHub-UUID on a 200", m)
			}
			if a.mode != envelope.ModeFast || a.status != "accepted" {
				t.Errorf("%s: mode=%q status=%q, want fast/accepted for ring=memory", m, a.mode, a.status)
			}
		}
	})

	t.Run("CONNECT is refused", func(t *testing.T) {
		// Sent by hand: the standard client will not issue CONNECT as a
		// normal request.
		if got := raw(t, "CONNECT example.com:443 HTTP/1.1"); !strings.Contains(got, "405") {
			t.Errorf("CONNECT got %q, want 405 -- answering it advertises an open proxy", got)
		}
	})

	// SPEC §4.2: the webhook schema wants an uppercase method and a
	// path starting with "/". The controls are the same requests in the form
	// the schema accepts, so a rule refusing everything cannot pass.
	t.Run("a method outside [A-Z]+ is refused", func(t *testing.T) {
		if got := raw(t, "FROBNICATE /x HTTP/1.1"); !strings.Contains(got, "200") {
			t.Fatalf("control FROBNICATE got %q, want 200", got)
		}
		for _, m := range []string{"frobnicate", "Post", "M-SEARCH"} {
			if got := raw(t, m+" /x HTTP/1.1"); !strings.Contains(got, "405") {
				t.Errorf("%s got %q, want 405", m, got)
			}
		}
	})

	t.Run("a target without / is refused", func(t *testing.T) {
		if got := raw(t, "OPTIONS /x HTTP/1.1"); !strings.Contains(got, "200") {
			t.Fatalf("control OPTIONS /x got %q, want 200", got)
		}
		// net/http would answer this one itself, 200 with no body, unless the
		// server disables its general OPTIONS handler.
		if got := raw(t, "OPTIONS * HTTP/1.1"); !strings.Contains(got, "400") {
			t.Errorf("OPTIONS * got %q, want 400", got)
		}
	})

	t.Run("413 above the body ceiling", func(t *testing.T) {
		code, _ := in.post("/big", strings.Repeat("x", 4096))
		if code != http.StatusRequestEntityTooLarge {
			t.Errorf("status %d, want 413", code)
		}
	})

	t.Run("414 above the URI ceiling", func(t *testing.T) {
		code, _ := in.post("/"+strings.Repeat("a", 512), "b")
		if code != http.StatusRequestURITooLong {
			t.Errorf("status %d, want 414", code)
		}
	})

	t.Run("empty body is accepted", func(t *testing.T) {
		code, id := in.post("/empty", "")
		if code != http.StatusOK || id == "" {
			t.Errorf("status %d id %q, want 200 with a uuid", code, id)
		}
	})

	in.stop()
}

// SPEC §4.1: HTTP/1.0, HTTP/1.1 and h2c share one port, from the standard
// library alone.
func TestH2CPriorKnowledge(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir)

	tr := &http.Transport{}
	var p http.Protocols
	p.SetUnencryptedHTTP2(true)
	tr.Protocols = &p

	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := client.Post("http://"+in.pub+"/h2", "application/json", strings.NewReader(`{"a":1}`))
	if err != nil {
		t.Fatalf("h2c request: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	if a := checkAnswer(t, resp, b); a.code != http.StatusOK {
		t.Errorf("status %d, want 200", a.code)
	}
	if resp.ProtoMajor != 2 {
		t.Errorf("served over HTTP/%d.%d, want HTTP/2", resp.ProtoMajor, resp.ProtoMinor)
	}

	// The connection gauge must move the connection to the HTTP/2 bucket
	// exactly once, not once per request.
	if m := in.metrics(); !strings.Contains(m, `whs_conns_active{proto="http2"}`) {
		t.Error("the http2 connection gauge is missing")
	}
	in.stop()
}

// ------------------------------------------------------------ admin surface

func TestAdminEndpoints(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir)

	resp, err := http.Get("http://" + in.adm + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", resp.StatusCode)
	}

	if code, _ := in.readyz(); code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", code)
	}

	// The public port must stay opaque: an admin path there is a real event,
	// not a health check.
	code, id := in.post("/metrics", "not a scrape")
	if code != http.StatusOK || id == "" {
		t.Errorf("/metrics on the public port returned %d; it must be an ordinary event", code)
	}
	in.stop()
}

// SPEC §11.2: the +Inf bucket must equal _count. If that ever diverges, every
// quantile computed from the histogram is silently wrong.
func TestHistogramInvariants(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir)
	in.postN(20)
	in.post("/big", strings.Repeat("x", 100))

	m := in.metrics()
	infs := map[string]string{}
	counts := map[string]string{}

	for _, line := range strings.Split(m, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		switch {
		case strings.Contains(name, `_bucket{`) && strings.Contains(name, `le="+Inf"`):
			base, labels := splitMetric(name)
			infs[strings.TrimSuffix(base, "_bucket")+"|"+stripLE(labels)] = value
		case strings.Contains(name, "_count"):
			base, labels := splitMetric(name)
			counts[strings.TrimSuffix(base, "_count")+"|"+labels] = value
		}
	}

	if len(infs) == 0 {
		t.Fatal("no histograms were exported")
	}
	for key, inf := range infs {
		got, ok := counts[key]
		if !ok {
			t.Errorf("%s has a +Inf bucket but no _count", key)
			continue
		}
		if inf != got {
			t.Errorf("%s: +Inf = %s but _count = %s; every quantile from this histogram is wrong",
				key, inf, got)
		}
	}
	in.stop()
}

func splitMetric(s string) (name, labels string) {
	i := strings.IndexByte(s, '{')
	if i < 0 {
		return s, ""
	}
	return s[:i], strings.TrimSuffix(s[i+1:], "}")
}

func stripLE(labels string) string {
	var keep []string
	for _, p := range strings.Split(labels, ",") {
		if !strings.HasPrefix(p, "le=") {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

// ------------------------------------------------------------- validation

// SPEC §0, §10.1: the compatibility selectors boot with the one value this
// server implements and refuse any other with exit 1, before anything opens.
// The control boots the same keys with the accepted values, so the refusals
// are about the values and not about the keys.
func TestInvalidConfigRefusesToStart(t *testing.T) {
	in := start(t, t.TempDir(), "--ring.mode=memory", "--sink.type=nats",
		"--ratelimit.enabled=false", "--sink.nats.tls=false", "--sink.nats.subject=loghub.{space}.{type}",
		"--ring.high_water=0.8")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("control: exit code %d", code)
	}

	for _, arg := range []string{
		"--ring.mode=sqlite", "--ring.mode=none", "--sink.type=redis", "--sink.type=none",
		"--sink.type=postgres", "--ratelimit.enabled=true", "--ring.high_water=2",
		"--sink.nats.tls=true", "--sink.nats.subject=webhooks", "--sink.nats.urls=tls://127.0.0.1:4222",
		// Keys of retired features are unknown parameters.
		"--ring.max_inflight=10", "--sink.redis.addrs=127.0.0.1:6379", "--ratelimit.rate=5",
		"--sink.postgres.addr=127.0.0.1:5432", "--sink.nats.tls_ca=/etc/ca.pem",
	} {
		if code := runRefused(t, []string{arg}, testID); code != ExitConfig {
			t.Errorf("%s: exit code %d, want %d", arg, code, ExitConfig)
		}
	}
}

// SPEC §3.5 and §13.5: server.node_id is gone with the v1 envelope; the
// identity comes from go-loghub-ident. An INI that still sets it is refused at
// boot rather than silently ignored, so an operator who relied on it learns
// that it does nothing. The control is the same file without the key.
func TestNodeIDInINIRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "whs.ini")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("[server]\nmax_conns = 100\n")
	in := start(t, dir, "--config="+ini)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("control: exit code %d", code)
	}

	write("[server]\nmax_conns = 100\nnode_id = whs-1\n")
	if code := runRefused(t, []string{
		"--config=" + ini,
		"--ring.data_dir=" + dir,
		"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
	}, testID); code != ExitConfig {
		t.Errorf("an INI with server.node_id: exit code %d, want %d", code, ExitConfig)
	}
}

// SPEC §3.5, §10.1 step 1a: from and space go into every envelope header
// unescaped, so an identity outside the JLH grammar ends the boot with exit 1.
// The control is every other test: they all boot with testID.
func TestInvalidIdentityRefusesToStart(t *testing.T) {
	args := []string{"--ring.data_dir=" + t.TempDir(),
		"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t)}
	bad := map[string]func(*ident.Identity){
		"from not a UUID":       func(id *ident.Identity) { id.From = "whs-1" },
		"from uppercase":        func(id *ident.Identity) { id.From = strings.ToUpper(id.From) },
		"space with underscore": func(id *ident.Identity) { id.Space = "my_space" },
		"space empty segment":   func(id *ident.Identity) { id.Space = "a..b" },
	}
	for name, spoil := range bad {
		id := testID
		spoil(&id)
		if code := runRefused(t, args, id); code != ExitConfig {
			t.Errorf("%s: exit code %d, want %d", name, code, ExitConfig)
		}
	}
}

// SPEC §10.1 step 3: an unreachable NATS keeps the public port CLOSED, is
// retried until sink.boot_timeout and then exits 3, so nothing is ever
// accepted that cannot be stored. A refused connection is the network class,
// and the boot keeps waiting. No server is needed -- the point is that there
// is none.
func TestUnreachableSinkExitsWithoutOpeningPublicPort(t *testing.T) {
	dir := t.TempDir()
	dead := freePort(t) // nothing listens here

	started := time.Now()
	in := startExpectingExit(t, dir,
		"--sink.nats.urls=nats://"+dead,
		"--sink.boot_timeout=1s",
	)

	// SPEC §10.1 step 2: the admin port is up WHILE the sink is retried, with
	// the reason in /readyz. That is the whole point of opening it first: a
	// crash loop caused by an unreachable sink stays diagnosable.
	sawReason := false
	for time.Since(started) < 800*time.Millisecond && !sawReason {
		resp, err := http.Get("http://" + in.adm + "/readyz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			sawReason = resp.StatusCode == http.StatusServiceUnavailable &&
				strings.Contains(string(body), "sink")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !sawReason {
		t.Error("/readyz never reported the boot state while the sink was unreachable")
	}

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the process did not exit after sink.boot_timeout")
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Errorf("gave up after %v; sink.boot_timeout=1s must be waited out", elapsed)
	}

	// The public listener must never have opened.
	c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond)
	if err == nil {
		c.Close()
		t.Error("the public port accepted a connection despite an unreachable sink")
	}
}

// ------------------------------------------------------------------- SIGHUP

// SPEC §13.3: there is no reload, and a SIGHUP must not do what Go does with
// it by default -- end the process, with no drain and no dump. A real signal
// is sent to the test process itself: while an instance runs, it is caught.
// Were it not, the whole test binary would die here, which is the failure
// this test exists to show.
func TestSIGHUPIsIgnored(t *testing.T) {
	// Every instance that stopped before this one left SIGHUP ignored, which
	// is what the binary does after run (SPEC §13.3); and a test binary may
	// have inherited it ignored (nohup, some CI runners). Back to Go's default
	// -- end the process -- so only the instance can be what saves it.
	signal.Reset(syscall.SIGHUP)
	if signal.Ignored(syscall.SIGHUP) {
		t.Skip("SIGHUP was ignored when this process started; the test would prove nothing")
	}
	in := start(t, t.TempDir())
	before := in.postN(3)

	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	after := in.postN(3)
	if len(before) != 3 || len(after) != 3 {
		t.Fatalf("accepted %d before and %d after the SIGHUP, want 3 and 3", len(before), len(after))
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0: a SIGHUP must not change how the process ends", code)
	}
	if lost := missing(append(before, after...), eventsIn(t, in)); len(lost) > 0 {
		t.Errorf("%d events were lost around the SIGHUP", len(lost))
	}
}

// SPEC §8.7.5, §10.1: a destination that refuses the boot check permanently
// ends the boot at once with exit 3, and the log says it was refused -- not
// that it "did not become reachable within sink.boot_timeout", which sent an
// operator to look at the network when the stream or the credentials were
// wrong (the message go-webhook-pg-server printed until the 2026-10-11 port).
func TestFatalBootRefusalIsLoggedAsSuch(t *testing.T) {
	ms := &memSink{}
	ms.fatal.Store(true)
	var buf syncBuffer
	args := []string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--sink.boot_timeout=120s"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	code := runContext(ctx, args, testID, func(a *App) {
		a.sinkOverride = ms
		a.log = obs.NewLogger(&buf, "text", "info")
	})
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("a fatal refusal waited %s", time.Since(start))
	}
	out := buf.String()
	if !strings.Contains(out, "refused this server permanently") || strings.Contains(out, "did not become reachable") {
		t.Errorf("the boot log does not say the sink refused for good:\n%s", out)
	}
}

// The WARN is the operator's only clue that the edit did not take effect.
func TestIgnoreSIGHUPWarnsOnEachSignal(t *testing.T) {
	var buf syncBuffer
	log := obs.NewLogger(&buf, "text", "warn")
	stop := ignoreSIGHUP(log)
	defer stop()

	for i := 0; i < 2; i++ {
		if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		want := i + 1
		if !waitFor(5*time.Second, func() bool { return strings.Count(buf.String(), "SIGHUP ignored") >= want }) {
			t.Fatalf("signal %d: no WARN; log so far:\n%s", want, buf.String())
		}
	}
	if !strings.Contains(buf.String(), "restart it to apply changes") {
		t.Errorf("the WARN does not say how to apply a change:\n%s", buf.String())
	}
}

// syncBuffer is a bytes.Buffer the logger goroutine and the test may share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// ------------------------------------------------------ the JLH round trip

// SPEC §3.2 end to end: a query with %00 and a binary body travel inside the
// envelope -- the NUL as a \u0000 escape in query, the body in base64 -- and
// reach the destination byte for byte, under the uuid the client was given.
// An empty body is still a content object, with an empty body string. The
// same trip through a real stream is in nats_test.go.
func TestNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	in := start(t, t.TempDir())

	binID, emptyID := postRoundTrip(in)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verify(t, in.mem.stored(), testID, envelope.ModeFast)), binID, emptyID)
}

// roundTripBody has a NUL, a newline, an invalid UTF-8 byte and a CR:
// everything a text-oriented encoding would mangle or truncate.
const roundTripBody = "A\x00B\nC\xffD\r"

// postRoundTrip sends the two requests checkRoundTrip inspects: a binary body
// on a path whose query carries %00, and an empty GET.
func postRoundTrip(in *instance) (binID, emptyID string) {
	in.t.Helper()
	a := in.send(http.MethodPost, "/hook?q=a%00b&k=v&k=w", "application/octet-stream",
		strings.NewReader(roundTripBody))
	if a.code != http.StatusOK {
		in.t.Fatalf("binary body: status %d", a.code)
	}
	e := in.send(http.MethodGet, "/hook", "", nil)
	if e.code != http.StatusOK {
		in.t.Fatalf("empty body: status %d", e.code)
	}
	return a.uuid, e.uuid
}

// checkRoundTrip checks what postRoundTrip sent, read back from a destination.
// The message is the JLH byte for byte (SPEC §3.8), so the NUL of the query
// survives the trip as its \u0000 escape.
func checkRoundTrip(t *testing.T, got map[string]*jlhcheck.Envelope, binID, emptyID string) {
	t.Helper()
	bin, empty := got[binID], got[emptyID]
	if bin == nil || empty == nil {
		t.Fatalf("binary=%v empty=%v: an accepted uuid is missing from the destination", bin != nil, empty != nil)
	}

	p := &bin.Payload
	if p.Content.Encoding != "base64" {
		t.Errorf("content.encoding = %q, want base64 for a body with NUL and invalid UTF-8", p.Content.Encoding)
	}
	if body, err := p.Body(); err != nil || string(body) != roundTripBody {
		t.Errorf("body = %q (%v), want %q -- the body must arrive byte for byte", body, err, roundTripBody)
	}
	if p.Content.Type != "application/octet-stream" {
		t.Errorf("content.type = %q", p.Content.Type)
	}
	wantQ := "a\x00b"
	if q, _ := p.Query["q"].(string); q != wantQ {
		t.Errorf("query.q = %q, want %q", q, wantQ)
	}
	if k, _ := p.Query["k"].([]any); len(k) != 2 || k[0] != "v" || k[1] != "w" {
		t.Errorf("query.k = %v, want [v w] in arrival order", p.Query["k"])
	}
	if !strings.HasSuffix(p.URL, "/hook?q=a%00b&k=v&k=w") || p.Path != "/hook" || p.Method != "POST" {
		t.Errorf("url=%q path=%q method=%q, want the raw query kept in url", p.URL, p.Path, p.Method)
	}
	if peer, _ := p.Meta("peer-addr"); peer != "127.0.0.1" || p.Network.IP.Src != "127.0.0.1" {
		t.Errorf("peer-addr=%v src=%s, want 127.0.0.1", peer, p.Network.IP.Src)
	}

	e := &empty.Payload
	if e.Content.Length != 0 || e.Content.Body != "" || e.Content.Encoding != "utf-8" {
		t.Errorf("empty body as length=%d encoding=%q body=%q, want 0, utf-8, \"\"",
			e.Content.Length, e.Content.Encoding, e.Content.Body)
	}
	if e.Method != "GET" || len(e.Query) != 0 {
		t.Errorf("empty request: method=%q query=%v", e.Method, e.Query)
	}
}

// SPEC §8.7.1, §8.7.2: the sink gets sink.op_timeout for the dial and for a
// pending acknowledgement, workers × batch as the ceiling of publishes in
// flight, the largest envelope for the boot's size checks, and the client name
// and subject of the identity. The wiring is checked where it is made: NewNATS
// would default a zero to something plausible, which is exactly what makes a
// removed line go unnoticed.
func TestNATSConfigCarriesTheDeadlinesAndCeilings(t *testing.T) {
	loaded, err := config.Load(config.Options{Args: []string{"--sink.op_timeout=7s",
		"--sink.health_timeout=300ms", "--sink.workers=3", "--sink.batch=50",
		"--loghub.ctl_namespace=acme.ctl"}, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: loaded.Config, id: testID}
	nc := a.natsConfig()
	if nc.ConnectTimeout != 7*time.Second || nc.OpTimeout != 7*time.Second {
		t.Errorf("ConnectTimeout %s OpTimeout %s, want sink.op_timeout (7s) for both",
			nc.ConnectTimeout, nc.OpTimeout)
	}
	if nc.MaxPending != 150 {
		t.Errorf("MaxPending = %d, want sink.workers × sink.batch = 150", nc.MaxPending)
	}
	if nc.MaxEventBytes != config.MaxEventBytes(loaded.Config) || nc.MaxEventBytes == 0 {
		t.Errorf("MaxEventBytes = %d, want %d", nc.MaxEventBytes, config.MaxEventBytes(loaded.Config))
	}
	if nc.ClientName != "whs/"+testID.AgentName+"@"+testID.Hostname || nc.Workspace != testID.Space ||
		nc.CtlNamespace != "acme.ctl" || nc.Mode != config.NATSModeJetStream || nc.Stream != "LOGHUB_WEBHOOK" {
		t.Errorf("identity and [loghub] wiring: %+v", nc)
	}
}
