package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
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
	AgentName: "go-webhook-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

var spaceSeq atomic.Int64

// uniqueID is testID in a Workspace of its own. The destinations that derive
// their topic from the Workspace (loghub.{space}.{type}) are shared with other
// suites, and a NATS subject may belong to one stream only: with "default"
// two suites would refuse each other's streams.
func uniqueID() ident.Identity {
	id := testID
	id.Space = fmt.Sprintf("whs-app-%d-%d", time.Now().UnixNano(), spaceSeq.Add(1))
	return id
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

// start launches an instance in-process with testID and waits for it to
// become ready.
func start(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	return startAs(t, dir, testID, extra...)
}

// startLog, when set by a test, receives the log of the instances it starts
// (at info), for the tests that assert what the operator is told. startRing,
// when set, wraps their memory ring (App.wrapRing).
var (
	startLog  io.Writer
	startRing func(*ring.Memory) ring.Ring
)

// startAs is start with an identity of the test's choosing.
func startAs(t *testing.T, dir string, id ident.Identity, extra ...string) *instance {
	t.Helper()

	in := &instance{
		t:    t,
		id:   id,
		dir:  dir,
		pub:  freePort(t),
		adm:  freePort(t),
		done: make(chan int, 1),
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

// startExpectingExit launches an instance that is expected to fail during boot.
func startExpectingExit(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	return startExpectingExitAs(t, dir, testID, extra...)
}

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
	go func() { in.done <- RunContext(ctx, in.args, id) }()
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
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		t.Errorf("status %d: the body is not the answer of SPEC §4.4: %v\n%s", a.code, err, body)
		return a
	}
	a.mode, a.status = b.Mode, b.Status
	if b.UUID != a.uuid {
		t.Errorf("body uuid %q differs from X-LogHub-UUID %q", b.UUID, a.uuid)
	}
	switch {
	case a.code != http.StatusOK:
		if b.Status != "error" {
			t.Errorf("status %d answered status %q, want error", a.code, b.Status)
		}
	case b.Mode == envelope.ModeFast && b.Status == "accepted",
		b.Mode == envelope.ModeBucket && b.Status == "stored":
	default:
		t.Errorf("a 200 answered mode %q with status %q; fast is accepted, bucket is stored",
			b.Mode, b.Status)
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

// canonical is a JLH envelope with its nine fields in the order the contract
// fixes (SPEC §3.1). The SQL destinations keep the header in columns and the
// payload in a JSON column -- jsonb and MySQL's JSON reorder keys -- so an
// envelope read back from them is rebuilt through this struct: encoding/json
// writes struct fields in declaration order.
type canonical struct {
	UUID  string `json:"uuid"`
	From  string `json:"from"`
	Space string `json:"space"`
	Type  string `json:"type"`
	Level int    `json:"level"`
	SID   int64  `json:"sid"`
	Step  int    `json:"step"`
	Event struct {
		UTS int64  `json:"uts"`
		Sub int64  `json:"sub"`
		ISO string `json:"iso"`
	} `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

// setEvent fills event from the inbox columns of SPEC §3.6: event_at holds the
// microsecond instant and event_nsec the 0-999 ns it cannot.
func (c *canonical) setEvent(at time.Time, nsec int64) {
	c.Event.UTS = at.Unix()
	c.Event.Sub = int64(at.Nanosecond()) + nsec
	c.Event.ISO = time.Unix(c.Event.UTS, c.Event.Sub).UTC().Format("2006-01-02T15:04:05.000Z")
}

func (c *canonical) bytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c); err != nil {
		t.Fatalf("rebuilding the envelope %s: %v", c.UUID, err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// verify is the contract oracle of SPEC §15.1 over envelopes read back from a
// destination: each must pass jlhcheck (both schemas and the semantic rules),
// carry the identity and the pid of the instance that wrote it, and the mode
// its ring x sink pair promises (SPEC §3.2; "" skips that check). It returns
// the envelopes that passed. A failure is reported per envelope, so one bad
// row does not hide the others, and the uuid of a failed one is left out --
// which the callers' missing() turns into a loss as well.
func verify(t *testing.T, raws [][]byte, id ident.Identity, mode string) []*jlhcheck.Envelope {
	t.Helper()
	return verifyWith(t, jlhcheck.Validate, raws, id, mode)
}

// verifyWith is verify with the oracle given: jlhcheck.ValidateJSONBCopy for
// rows of loghub.inbox (verifyJSONB).
func verifyWith(t *testing.T, check func([]byte) (*jlhcheck.Envelope, error), raws [][]byte,
	id ident.Identity, mode string) []*jlhcheck.Envelope {
	t.Helper()
	out := make([]*jlhcheck.Envelope, 0, len(raws))
	for i, raw := range raws {
		e, err := check(raw)
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
			t.Errorf("envelope %s: payload.mode=%q, want %q for this ring x sink pair",
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

// testJLH builds one envelope the way the server does, for dumps and ring
// rows the tests plant on disk. It is checked by the oracle before use, so a
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

// ------------------------------------------------- sink=sqlite read-back

// sqliteEnvelopes rebuilds every envelope of a sink=sqlite database from its
// inbox rows (SPEC §3.7). The payload column is the JLH's payload byte for
// byte, so the rebuild is the envelope the server wrote.
func sqliteEnvelopes(t *testing.T, path string) [][]byte {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT uuid, from_uuid, space, type, level, sid, step,
		event_at, event_nsec, payload FROM inbox ORDER BY uuid`)
	if err != nil {
		t.Fatalf("reading the inbox of %s: %v", path, err)
	}
	defer rows.Close()

	var out [][]byte
	for rows.Next() {
		var c canonical
		var at string
		var nsec int64
		var payload string
		if err := rows.Scan(&c.UUID, &c.From, &c.Space, &c.Type, &c.Level, &c.SID, &c.Step,
			&at, &nsec, &payload); err != nil {
			t.Fatal(err)
		}
		ts, err := time.Parse("2006-01-02T15:04:05.000000Z", at)
		if err != nil {
			t.Errorf("event_at %q is not the fixed-width form of SPEC §3.7: %v", at, err)
			continue
		}
		c.setEvent(ts, nsec)
		c.Payload = json.RawMessage(payload)
		out = append(out, c.bytes(t))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// eventsIn reads the uuids stored in a sink=sqlite database by an instance
// running as testID, every envelope checked by the oracle.
func eventsIn(t *testing.T, path, mode string) []string {
	t.Helper()
	return uuidsOf(verify(t, sqliteEnvelopes(t, path), testID, mode))
}

// countRows counts a table's rows; a file that does not exist yet holds none.
// A table that cannot be read is a failure, not a zero: an assertion that
// "nothing is left" would otherwise pass on a table that is not there.
func countRows(t *testing.T, path, table string) int {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		return 0
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("counting %s in %s: %v", table, path, err)
	}
	return n
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

// realSinkModes is SPEC §3.2 for a destination that stores: the 200 of
// ring=memory leaves the envelope in memory only (fast); ring=sqlite and
// ring=none put it on disk or in the destination before answering (bucket).
var realSinkModes = []struct{ ring, mode string }{
	{"memory", envelope.ModeFast},
	{"sqlite", envelope.ModeBucket},
	{"none", envelope.ModeBucket},
}

// ---------------------------------------------- the local combinations

// SPEC §16.1: every ring x sink pair is valid and each has its own meaning for
// what a 200 promises -- which payload.mode and the answer's status now say
// (SPEC §3.2, §4.4). Every pair must accept traffic and shut down cleanly; the
// six that need no external destination are covered here, the others in
// redis_test.go, postgres_test.go, mysql_test.go and nats_test.go.
func TestAllLocalCombinations(t *testing.T) {
	cases := []struct {
		ring, sink, mode string
	}{
		{"memory", "none", envelope.ModeFast},
		{"memory", "sqlite", envelope.ModeFast},
		{"sqlite", "none", envelope.ModeBucket},
		{"sqlite", "sqlite", envelope.ModeBucket},
		// ring=none with sink=none answers before anything is stored anywhere.
		{"none", "none", envelope.ModeFast},
		{"none", "sqlite", envelope.ModeBucket},
	}

	for _, tc := range cases {
		name := tc.ring + "+" + tc.sink
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			args := []string{
				"--ring.mode=" + tc.ring,
				"--sink.type=" + tc.sink,
				// Tests care about correctness, not about surviving power
				// loss; FULL would make every case disk-bound for nothing.
				"--ring.sqlite.synchronous=OFF",
				"--sink.sqlite.synchronous=OFF",
			}
			if tc.sink == "sqlite" {
				args = append(args, "--sink.workers=1")
			}
			in := start(t, dir, args...)

			const n = 25
			ids := in.postN(n)
			if len(ids) != n {
				t.Fatalf("%d of %d requests were accepted", len(ids), n)
			}
			if in.mode != tc.mode {
				t.Errorf("the 200 answered mode %q, want %q", in.mode, tc.mode)
			}

			// The mode is reported so an operator can confirm what is
			// actually running.
			m := in.metrics()
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="%s"} 1`, tc.ring, tc.sink)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}

			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}

			// With a real destination every accepted event must be there,
			// under the uuid its client was given, and nothing else.
			if tc.sink == "sqlite" {
				got := eventsIn(t, filepath.Join(dir, "events.db"), tc.mode)
				if lost := missing(ids, got); len(lost) > 0 {
					t.Errorf("%d of %d accepted events never reached the sink", len(lost), len(ids))
				}
				if extra := missing(got, ids); len(extra) > 0 {
					t.Errorf("%d events in the sink were never answered to a client", len(extra))
				}
			}
		})
	}
}

// ------------------------------------------------------------- durability

// SPEC §15.5: zero events lost to a SIGKILL when the ring is persistent.
//
// This is the criterion that caught the identity-loss bug, where 149 of 150
// accepted events were silently dropped on a primary key collision.
func TestSQLiteRingSurvivesHardKill(t *testing.T) {
	dir := t.TempDir()
	ringDB := filepath.Join(dir, "ring.db")
	eventsDB := filepath.Join(dir, "events.db")

	args := []string{
		"--ring.mode=sqlite", "--sink.type=sqlite", "--sink.workers=1",
		"--ring.sqlite.synchronous=OFF", "--sink.sqlite.synchronous=OFF",
	}

	in := start(t, dir, args...)
	ids := in.postN(120)
	if len(ids) == 0 {
		t.Fatal("nothing was accepted")
	}

	// A graceful stop is the closest an in-process test can get to SIGKILL
	// without killing the test binary; what matters is that the ring's rows
	// are the source of truth and the boot drain consumes them.
	in.stop()

	pending := countRows(t, ringDB, "jlh_ring")
	delivered := len(eventsIn(t, eventsDB, envelope.ModeBucket))
	if pending+delivered < len(ids) {
		t.Fatalf("after the stop only %d of %d accepted events exist (ring=%d, events=%d)",
			pending+delivered, len(ids), pending, delivered)
	}

	// Restart: the backlog must be drained before the public port opens.
	in2 := start(t, dir, args...)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && countRows(t, ringDB, "jlh_ring") > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	in2.stop()

	got := eventsIn(t, eventsDB, envelope.ModeBucket)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d accepted events were lost across the restart", len(lost), len(ids))
	}
}

// The "Enqueue returns only after the COMMIT" property of SPEC §7.4 is verified
// in internal/ring (TestSQLiteEnqueueIsDurableBeforeReturn), not here.
//
// It cannot be established at this level: with the workers running, rows are
// delivered and deleted as fast as they are written, so an empty ring table
// proves delivery rather than a missing commit. What integration CAN prove is
// the consequence -- that nothing answered 200 is lost across a restart -- which
// is TestSQLiteRingSurvivesHardKill above.

// ------------------------------------------------------------ backpressure

// SPEC §17.2: a full ring is a 429. It is never a 200 followed by a discard.
func TestRingFullReturns429(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=none",
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

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=none",
		"--ratelimit.enabled=true", "--ratelimit.rate=1", "--ratelimit.burst=3",
		"--proxy.trusted=127.0.0.0/8",
	)

	var limited bool
	for i := 0; i < 10; i++ {
		resp, err := http.Post("http://"+in.pub+"/x", "text/plain", strings.NewReader("b"))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		a := checkAnswer(t, resp, b)
		if resp.StatusCode == http.StatusTooManyRequests {
			limited = true
			if resp.Header.Get("Retry-After") == "" {
				t.Error("a 429 was sent without Retry-After; the client cannot back off correctly")
			}
			if a.uuid != "" {
				t.Error("a rate-limit 429 carries a uuid; it is decided before the envelope exists")
			}
			break
		}
	}
	if !limited {
		t.Error("the rate limiter never engaged")
	}
	in.stop()
}

// -------------------------------------------------------------- HTTP rules

func TestHTTPSurface(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=none",
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

	// SPEC §4.2: the webhook.request schema wants an uppercase method and a
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
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none")

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
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none")

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
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none")
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

// SPEC §10.1: the ring is a queue and the inbox is an append-only
// destination. Pointing both at one file must refuse to start. (Two distinct
// files boot: TestAllLocalCombinations/sqlite+sqlite.)
func TestSameSQLiteFileRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	same := filepath.Join(dir, "one.db")

	code := runRefused(t, []string{
		"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=" + same, "--sink.sqlite.path=" + same,
		"--server.bind=127.0.0.1:0", "--admin.bind=127.0.0.1:0",
	}, testID)
	if code != ExitConfig {
		t.Errorf("exit code %d, want %d", code, ExitConfig)
	}
}

func TestInvalidConfigRefusesToStart(t *testing.T) {
	// Control: the same four keys with valid values boot, so the refusals
	// below are about the values and not about the keys.
	in := start(t, t.TempDir(), "--ring.mode=memory", "--sink.type=none",
		"--ring.high_water=0.8", "--ratelimit.v4_mask=24")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("control: exit code %d", code)
	}

	for _, arg := range []string{
		"--ring.mode=disk", "--sink.type=kafka",
		"--ring.high_water=2", "--ratelimit.v4_mask=99",
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
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none", "--config="+ini)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("control: exit code %d", code)
	}

	write("[server]\nmax_conns = 100\nnode_id = whs-1\n")
	if code := runRefused(t, []string{
		"--ring.mode=memory", "--sink.type=none", "--config=" + ini,
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
	args := []string{"--ring.mode=memory", "--sink.type=none",
		"--ring.data_dir=" + t.TempDir(),
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

// SPEC §10.1 step 3: an unreachable sink keeps the public port CLOSED and exits
// 3, so nothing is ever accepted that cannot be stored.
func TestUnreachableSinkExitsWithoutOpeningPublicPort(t *testing.T) {
	dir := t.TempDir()
	dead := freePort(t) // nothing listens here

	started := time.Now()
	in := startExpectingExit(t, dir,
		"--ring.mode=memory", "--sink.type=redis",
		"--sink.redis.addrs="+dead,
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
	// SPEC §10.1 step 3: Redis is retried with backoff until sink.boot_timeout,
	// not given up on at the first refused dial.
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

// ------------------------------------------------------------------ reload

// SPEC §13.3. Two references: a reloadable key is diffed against what is
// RUNNING, so reverting it to the boot value is applied; a restart-only key is
// diffed against the BOOT configuration, so every SIGHUP keeps warning about it.
func TestReloadTracksRunningAndBootValues(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "whs.ini")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[proxy]\nmax_xff_entries = 32\n[ring]\ncapacity_items = 1000\n")
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none", "--config="+ini)

	// 1. A reloadable key changes and a restart-only key changes.
	write("[proxy]\nmax_xff_entries = 8\n[ring]\ncapacity_items = 2000\n")
	r := in.app.reload()
	if !equalKeys(r.applied, "proxy.max_xff_entries") || !equalKeys(r.ignored, "ring.capacity_items") {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// 2. The same file again: nothing new to apply, but the restart-only key
	// is still not what the process runs with, so it must be warned about
	// again rather than swallowed as "unchanged".
	r = in.app.reload()
	if len(r.applied) != 0 || !equalKeys(r.ignored, "ring.capacity_items") {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// 3. The reloadable key goes back to its boot value: that is a change
	// against what is running and must be applied, not skipped because it
	// matches the boot configuration.
	write("[proxy]\nmax_xff_entries = 32\n[ring]\ncapacity_items = 2000\n")
	r = in.app.reload()
	if !equalKeys(r.applied, "proxy.max_xff_entries") || !equalKeys(r.ignored, "ring.capacity_items") {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// 4. Everything back to boot: nothing applied, nothing to warn about.
	write("[proxy]\nmax_xff_entries = 32\n[ring]\ncapacity_items = 1000\n")
	r = in.app.reload()
	if len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
}

func equalKeys(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- rotation

// SPEC §8.3.3: rotation happens between batches, never inside a transaction.
func TestSQLiteSinkRotationNaming(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.rotate=daily", "--sink.sqlite.synchronous=OFF",
	)
	ids := in.postN(10)
	in.stop()

	want := "events-" + time.Now().UTC().Format("2006-01-02") + ".db"
	path := filepath.Join(dir, want)
	if _, err := os.Stat(path); err != nil {
		entries, _ := os.ReadDir(dir)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected %s, found %v", want, names)
	}
	if got := eventsIn(t, path, envelope.ModeFast); len(missing(ids, got)) > 0 {
		t.Errorf("%d of %d events are missing from the rotated file", len(missing(ids, got)), len(ids))
	}
}

// ------------------------------------------------- sink=sqlite contract

// SPEC §3.2 and §3.7 end to end through sink=sqlite: a query with %00 and a
// binary body travel inside the envelope -- the NUL as a \u0000 escape in
// query, the body in base64 -- and come back out of the inbox byte for byte,
// under the uuid the client was given. An empty body is still a content
// object, with an empty body string.
func TestSQLiteNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF")

	binID, emptyID := postRoundTrip(in)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	got := byUUID(t, verify(t, sqliteEnvelopes(t, filepath.Join(dir, "events.db")), testID, envelope.ModeFast))
	checkRoundTrip(t, got, binID, emptyID, false)
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
// nulReplaced is sink=postgres: jsonb refuses U+0000, so the copy in its
// payload column carries U+FFFD instead (SPEC §3.6); everywhere else the NUL
// survives the trip.
func checkRoundTrip(t *testing.T, got map[string]*jlhcheck.Envelope, binID, emptyID string, nulReplaced bool) {
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
	if nulReplaced {
		wantQ = "a�b"
	}
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
