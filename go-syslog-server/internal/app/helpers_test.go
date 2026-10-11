package app

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// These helpers run the REAL assembled stack in-process -- the same run() the
// binary calls -- through runContext, with a fixed identity instead of
// go-loghub-ident (whose Initialize may run once per process, SPEC §13.1). A
// test that assembled its own approximation of the wiring would prove nothing
// about what ships.

// testID is the identity every instance gets unless a test needs a second one.
var testID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space:     "default",
	AgentName: "go-syslog-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

// otherID is a second instance: the second life of the emergency test, the
// second replica of the shared Redis ring. Its from differs from testID's, so a
// test can tell who RECEIVED a message from who DRAINED it (SPEC §3.1, §3.5).
var otherID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8b9",
	Space:     "default",
	AgentName: "go-syslog-server-test-b",
	Hostname:  "test-host-b",
	MachineID: "fedcba9876543210fedcba9876543210",
}

// baseArgs binds everything to 127.0.0.1:0 and keeps the destination local
// (sink=sqlite). Every default that would touch a well-known port (:1514,
// 127.0.0.1:9102) or an external server (sink=nats) is overridden. Later
// arguments win, so a test appends what it needs.
func baseArgs(dir string) []string {
	return []string{
		"--server.data_dir=" + dir,
		"--admin.bind=127.0.0.1:0",
		"--listen.udp=127.0.0.1:0",
		"--listen.tcp=127.0.0.1:0",
		"--sink.type=sqlite",
		"--sink.sqlite.synchronous=OFF",
		"--ring.sqlite.synchronous=OFF",
		"--sink.health_interval=50ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=5s",
		"--log.level=error",
	}
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

// start boots an instance with baseArgs plus extra and waits until every
// transport is open.
func start(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	in := launch(t, testID, append(baseArgs(dir), extra...), nil)
	in.waitReady()
	return in
}

func (in *instance) record(code int) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.exited {
		in.exited, in.code = true, code
	}
}

// waitReady blocks until every transport is open (SPEC §13.1 step 13).
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

// eventsDB is the instance's sink.sqlite.path.
func (in *instance) eventsDB() string { return in.app.cfg.SinkSQL.Path }

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

// sendLF writes lines with non-transparent (LF) framing on one connection.
func sendLF(t *testing.T, addr string, lines ...string) {
	t.Helper()
	c := dialTCP(t, addr)
	defer c.Close()
	w := bufio.NewWriter(c)
	for _, l := range lines {
		w.WriteString(l)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

// sendOctet writes messages with octet-counting framing on one connection.
func sendOctet(t *testing.T, addr string, msgs ...string) {
	t.Helper()
	c := dialTCP(t, addr)
	defer c.Close()
	w := bufio.NewWriter(c)
	for _, m := range msgs {
		fmt.Fprintf(w, "%d %s", len(m), m)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

// sendUDP sends one datagram and returns the client's local port.
func sendUDP(t *testing.T, addr, msg string) int {
	t.Helper()
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	return c.LocalAddr().(*net.UDPAddr).Port
}

func sendUnix(t *testing.T, path, msg string) {
	t.Helper()
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
}

// shortSocketPath is a unix socket path inside t.TempDir(), or under /tmp when
// that would exceed sun_path (104 bytes with the NUL on macOS; SPEC §4.3).
func shortSocketPath(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "log.sock")
	if len(p) < 100 {
		return p
	}
	d, err := os.MkdirTemp("/tmp", "sls")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return filepath.Join(d, "log.sock")
}

// ---------------------------------------------------------------- the JLH

// jlhEnv is an envelope read back from a destination. Pointer fields tell
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

type jlhTLS struct {
	Version         string  `json:"version"`
	Cipher          string  `json:"cipher"`
	PeerSubject     *string `json:"peer-subject"`
	PeerFingerprint *string `json:"peer-fingerprint"`
}

type codeName struct {
	Code int    `json:"code"`
	Name string `json:"name"`
}

type jlhPayload struct {
	Host      *jlhHost                  `json:"host"`
	Transport string                    `json:"transport"`
	Framing   string                    `json:"framing"`
	TLS       *jlhTLS                   `json:"tls"`
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

func (p jlhPayload) srcIP() *string {
	if p.Host == nil {
		return nil
	}
	return p.Host.SrcIP
}

// mustJLH validates one envelope with the conformance oracle -- both LogHub
// schemas and the semantic rules (SPEC §18.2) -- and decodes it. EVERY envelope
// this suite reads back from a destination goes through here.
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

// ---------------------------------------------------------------- sqlite

// eventRow is one row of syslog_events (SPEC §3.7).
type eventRow struct {
	UUID      string
	Step      int
	Space     string
	Level     int
	SID       int64
	EvUTS     int64
	EvSub     int64
	Facility  int
	Severity  int
	Format    string
	Transport string
	SrcIP     sql.NullString
	Hostname  sql.NullString
	AppName   sql.NullString
	ProcID    sql.NullString
	MsgID     sql.NullString
	Env       []byte
	NBytes    int
}

func openSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// sqliteCount counts the rows of a table, -1 when it cannot be read (yet).
func sqliteCount(t *testing.T, path, table string) int {
	t.Helper()
	db := openSQLite(t, path)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// sqliteRows reads every event, in reception order.
func sqliteRows(t *testing.T, path string) []eventRow {
	t.Helper()
	db := openSQLite(t, path)
	defer db.Close()
	rows, err := db.Query(`SELECT uuid, step, space, level, sid, ev_uts, ev_sub, facility, severity,
	  format, transport, src_ip, hostname, appname, procid, msgid, env, nbytes
	  FROM syslog_events ORDER BY ev_uts, ev_sub, uuid`)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	defer rows.Close()
	var out []eventRow
	for rows.Next() {
		var r eventRow
		if err := rows.Scan(&r.UUID, &r.Step, &r.Space, &r.Level, &r.SID, &r.EvUTS, &r.EvSub,
			&r.Facility, &r.Severity, &r.Format, &r.Transport, &r.SrcIP, &r.Hostname, &r.AppName,
			&r.ProcID, &r.MsgID, &r.Env, &r.NBytes); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// waitRows waits until the instance's events.db holds at least n rows and
// returns them all.
func (in *instance) waitRows(n int) []eventRow {
	in.t.Helper()
	waitFor(in.t, 30*time.Second, fmt.Sprintf("%d rows in events.db", n), func() bool {
		return sqliteCount(in.t, in.eventsDB(), "syslog_events") >= n
	})
	return sqliteRows(in.t, in.eventsDB())
}

// colText is the sanitising rule of the promoted text columns (SPEC §3.7): NUL
// becomes U+FFFD in the column, never in env.
func colText(s string) string { return strings.ReplaceAll(s, "\x00", "�") }

// checkRow validates the envelope of a row and that every promoted column is
// the copy of the field it comes from, NULL exactly when the field is absent.
func checkRow(t *testing.T, r eventRow) jlhEnv {
	t.Helper()
	e := mustJLH(t, r.Env)
	checkColumns(t, columns{
		UUID: r.UUID, Step: r.Step, Space: r.Space, Level: r.Level, SID: r.SID,
		EvUTS: r.EvUTS, EvSub: r.EvSub, Facility: r.Facility, Severity: r.Severity,
		Format: r.Format, Transport: r.Transport, SrcIP: r.SrcIP, Hostname: r.Hostname,
		AppName: r.AppName, ProcID: r.ProcID, MsgID: r.MsgID, NBytes: r.NBytes, EnvLen: len(r.Env),
	}, e)
	return e
}

// columns is a destination row reduced to what SPEC §3.7 promises, so the
// three SQL destinations share one comparison.
type columns struct {
	UUID                             string
	Step, Level                      int
	Space                            string
	SID, EvUTS, EvSub                int64
	Facility, Severity               int
	Format, Transport                string
	SrcIP, Hostname, AppName, ProcID sql.NullString
	MsgID                            sql.NullString
	NBytes, EnvLen                   int
}

func checkColumns(t *testing.T, c columns, e jlhEnv) {
	t.Helper()
	p := &e.Payload
	if c.UUID != e.UUID || c.Step != e.Step || c.Space != e.Space || c.Level != e.Level ||
		uint64(c.SID) != e.SID || c.EvUTS != e.Event.UTS || c.EvSub != e.Event.Sub {
		t.Errorf("envelope columns %+v disagree with the envelope %s", c, e.bytes)
	}
	if c.Facility != p.Facility.Code || c.Severity != p.Severity.Code ||
		c.Format != p.Format || c.Transport != p.Transport {
		t.Errorf("payload columns %d/%d %s %s disagree with the payload %s",
			c.Facility, c.Severity, c.Format, c.Transport, e.bytes)
	}
	if c.NBytes != c.EnvLen {
		t.Errorf("nbytes = %d, len(env) = %d", c.NBytes, c.EnvLen)
	}
	for _, f := range []struct {
		name string
		col  sql.NullString
		val  *string
	}{
		{"src_ip", c.SrcIP, p.srcIP()},
		{"hostname", c.Hostname, p.hostname()},
		{"appname", c.AppName, p.AppName},
		{"procid", c.ProcID, p.ProcID},
		{"msgid", c.MsgID, p.MsgID},
	} {
		switch {
		case f.val == nil && f.col.Valid:
			t.Errorf("%s = %q but the field is absent from the payload: absent must be NULL", f.name, f.col.String)
		case f.val != nil && !f.col.Valid:
			t.Errorf("%s is NULL but the payload has %q", f.name, *f.val)
		case f.val != nil && f.col.String != colText(*f.val):
			t.Errorf("%s = %q, the payload has %q", f.name, f.col.String, *f.val)
		}
	}
}

// byMessage indexes envelopes by payload.message.
func byMessage(t *testing.T, rows []eventRow) map[string]jlhEnv {
	t.Helper()
	out := make(map[string]jlhEnv, len(rows))
	for _, r := range rows {
		e := checkRow(t, r)
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
	keys := make([]string, 0, len(envs))
	for k := range envs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Fatalf("no envelope carries %q; messages: %q", marker, keys)
	return jlhEnv{}
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
		// The shape of SQLITE_FULL, Redis OOM, a dropped table: the
		// destination answered and will not change its mind (SPEC §13.3).
		return &sink.Error{Class: sink.ClassFatal, Op: "write", Err: errors.New("disk full (injected)")}
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
	if f.failHealth.Load() {
		return errors.New("health probe failed (injected)")
	}
	return f.Sink.Health(ctx)
}

// The optional interfaces of the real sink are forwarded, so the decorator
// changes nothing but the knobs above.
func (f *faultySink) NeedsColumns() bool {
	cs, ok := f.Sink.(sink.ColumnSink)
	return ok && cs.NeedsColumns()
}

func (f *faultySink) MaxWorkers() int {
	if wl, ok := f.Sink.(sink.WorkerLimiter); ok {
		return wl.MaxWorkers()
	}
	return 0
}

// ---------------------------------------------------------------- TLS

// pki is an in-memory CA with a server and a client certificate, written as PEM
// into t.TempDir() because the server reads files (SPEC §4.5).
type pki struct {
	caFile, serverCert, serverKey string
	pool                          *x509.CertPool
	clientDER                     []byte
	clientKey                     *ecdsa.PrivateKey
	clientSubject                 string
	// sans of the server certificate, for emitters connecting by name.
	ca *x509.Certificate
}

var certSerial atomic.Int64

func newPKI(t *testing.T, serverNames ...string) *pki {
	t.Helper()
	tmpl := func(cn string, isCA bool, usage ...x509.ExtKeyUsage) *x509.Certificate {
		c := &x509.Certificate{
			SerialNumber: big.NewInt(certSerial.Add(1)),
			Subject:      pkix.Name{CommonName: cn, Organization: []string{"LogHub Test"}},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  usage,
		}
		if isCA {
			c.IsCA, c.BasicConstraintsValid = true, true
			c.KeyUsage |= x509.KeyUsageCertSign
		}
		return c
	}
	issue := func(c, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		signer := parentKey
		if parent == nil {
			parent, signer = c, key
		}
		der, err := x509.CreateCertificate(rand.Reader, c, parent, &key.PublicKey, signer)
		if err != nil {
			t.Fatal(err)
		}
		return der, key
	}
	writePEM := func(path, typ string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	caDER, caKey := issue(tmpl("Test CA", true), nil, nil)
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	srvTmpl := tmpl("syslog.test", false, x509.ExtKeyUsageServerAuth)
	srvTmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	srvTmpl.DNSNames = append([]string{"localhost"}, serverNames...)
	srvDER, srvKey := issue(srvTmpl, ca, caKey)
	cliDER, cliKey := issue(tmpl("rsyslog-01.intranet", false, x509.ExtKeyUsageClientAuth), ca, caKey)

	dir := t.TempDir()
	p := &pki{
		caFile:     filepath.Join(dir, "ca.pem"),
		serverCert: filepath.Join(dir, "server.pem"),
		serverKey:  filepath.Join(dir, "server.key"),
		pool:       x509.NewCertPool(),
		clientDER:  cliDER,
		clientKey:  cliKey,
		ca:         ca,
	}
	writePEM(p.caFile, "CERTIFICATE", caDER)
	writePEM(p.serverCert, "CERTIFICATE", srvDER)
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(p.serverKey, "EC PRIVATE KEY", keyDER)
	p.pool.AddCert(ca)
	leaf, err := x509.ParseCertificate(cliDER)
	if err != nil {
		t.Fatal(err)
	}
	p.clientSubject = leaf.Subject.String()
	return p
}

// caPEM is the CA certificate as PEM text, for a container that needs it.
func (p *pki) caPEM(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(p.caFile)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// fingerprint is the RFC 5425 §4.2.2 form SPEC §4.5 requires: sha-256: and the
// 32 octets in uppercase hex separated by colons.
func fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return "sha-256:" + strings.Join(parts, ":")
}
