package app

import (
	"bufio"
	"bytes"
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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/sink"
)

// These helpers run the REAL assembled stack in-process -- the same run() the
// binary calls -- through runContext, with a fixed identity instead of
// go-loghub-ident (whose Initialize may run once per process, SPEC §13.1). A
// test that assembled its own approximation of the wiring would prove nothing
// about what ships.
//
// The only destination is the PostgreSQL queue loghub.inbox, so every test
// that boots a server past step 7 needs a real PostgreSQL and skips without
// one:
//
//	RELPLOG_TEST_PG_DSN='postgres://rlpg:rlpg@127.0.0.1:15491/rlpg_test?sslmode=disable' \
//	  go test -race -timeout 900s ./internal/app/
//
// The server never runs DDL (invariant §20.17): each test creates its own
// queue from the file the operator applies, docker/postgres/inbox.sql, in a
// schema of its own, and drops the schema afterwards. A table per test keeps
// one test's rows -- or a damaged dump's -- out of the next one's count.

// testID is the identity every instance gets unless a test needs a second one.
var testID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space:     "default",
	AgentName: "go-relplog-pg-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

// otherID is a second instance: the second life of the emergency test. Its
// from differs from testID's, so a test can tell who RECEIVED a message from
// who reingested it (SPEC §3.1, §3.5).
var otherID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8b9",
	Space:     "default",
	AgentName: "go-relplog-pg-server-test-b",
	Hostname:  "test-host-b",
	MachineID: "fedcba9876543210fedcba9876543210",
}

// localArgs binds everything to 127.0.0.1:0 and names no destination: the
// sink keys keep their defaults. It is enough for a boot that must be refused
// before step 7 (exit 1), and nothing else. Every default that would touch a
// well-known port (:20514, 127.0.0.1:9103) is overridden. Later arguments win,
// so a test appends what it needs.
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

// baseArgs is localArgs plus a fresh queue on the test PostgreSQL. It skips
// the test without RELPLOG_TEST_PG_DSN. Two lives of one test share a queue by
// sharing the slice.
func baseArgs(t *testing.T, dir string) []string {
	t.Helper()
	pg := requirePG(t)
	return append(localArgs(dir), pg.sinkArgs(pg.queue(t, ""))...)
}

// withoutFlag drops every argument with prefix. A key a flag sets shadows the
// config file (SPEC §16.1), so a reload test removes the flag it edits in the
// file.
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

// start boots an instance with baseArgs plus extra and waits until every
// transport is open.
func start(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	in := launch(t, testID, append(baseArgs(t, dir), extra...), nil)
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
// held transaction waits for a full ring or a DOWN sink (SPEC §5.7).
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

// jlhEnv is an envelope read back from the queue or a dump. Pointer fields tell
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
	// XTransport is "relp" in every envelope of this server (D9, SPEC §3.2).
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
// this suite reads back from the queue or a dump goes through here.
func mustJLH(t *testing.T, b []byte) jlhEnv {
	t.Helper()
	if err := jlhcheck.Validate(b); err != nil {
		t.Errorf("the envelope is not a valid JLH: %v\n%s", err, b)
	}
	var e jlhEnv
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("the envelope does not decode: %v\n%s", err, b)
	}
	// Invariant §20.25: every envelope of this server says RELP, over plain
	// TCP, framed by octet count.
	if p := e.Payload; p.XTransport == nil || *p.XTransport != XTransport ||
		p.Transport != "tcp" || p.Framing != "octet-counting" {
		t.Errorf("x-transport %s transport %q framing %q: want relp over tcp, octet-counting\n%s",
			show(p.XTransport), p.Transport, p.Framing, b)
	}
	e.bytes = append([]byte(nil), b...)
	return e
}

// sameJSON reports whether two JSON documents hold the same value, whatever
// the key order and spacing: jsonb keeps neither (SPEC §3.7.1).
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("%v: %s", err, a)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	return reflect.DeepEqual(x, y)
}

// ---------------------------------------------------------------- postgres

const pgDSNEnv = "RELPLOG_TEST_PG_DSN"

// pgdb is the test PostgreSQL: one pool for the whole test process, closed by
// TestMain. A pool, not a pgx.Conn, because a few tests read the queue from
// more than one goroutine.
type pgdb struct {
	cfg  *pgx.ConnConfig
	pool *pgxpool.Pool
	dsn  string
}

var (
	pgOnce   sync.Once
	pgShared *pgdb
	pgErr    error
	pgSchema atomic.Int64
)

// requirePG returns the test PostgreSQL, skipping the test without
// RELPLOG_TEST_PG_DSN and failing it when the variable names a server that does
// not answer: a typo must not turn the whole suite into skips.
func requirePG(t *testing.T) *pgdb {
	t.Helper()
	dsn := os.Getenv(pgDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the tests that need PostgreSQL (the only sink)", pgDSNEnv)
	}
	pgOnce.Do(func() {
		pc, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			pgErr = err
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pool, err := pgxpool.NewWithConfig(ctx, pc)
		if err != nil {
			pgErr = err
			return
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			pgErr = err
			return
		}
		pgShared = &pgdb{cfg: pc.ConnConfig, pool: pool, dsn: dsn}
	})
	if pgErr != nil {
		t.Fatalf("%s is set but PostgreSQL does not answer: %v", pgDSNEnv, pgErr)
	}
	return pgShared
}

// sinkArgs points the server at table on the test PostgreSQL.
func (p *pgdb) sinkArgs(table string) []string {
	return []string{
		"--sink.postgres.addr=" + p.addr(),
		"--sink.postgres.username=" + p.cfg.User,
		"--sink.postgres.password=" + p.cfg.Password,
		"--sink.postgres.database=" + p.cfg.Database,
		"--sink.postgres.table=" + table,
	}
}

func (p *pgdb) addr() string { return net.JoinHostPort(p.cfg.Host, strconv.Itoa(int(p.cfg.Port))) }

// queue creates a loghub.inbox from docker/postgres/inbox.sql in a schema of
// its own (the DDL names its constraints and index), runs alter ($T is the
// table) and registers the schema's removal. It returns schema.inbox.
func (p *pgdb) queue(t *testing.T, alter string) string {
	t.Helper()
	schema := fmt.Sprintf("rlpg_app_%d_%d", time.Now().UnixNano(), pgSchema.Add(1))
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		t.Fatal(err)
	}
	ddl := strings.ReplaceAll(string(b), "SCHEMA IF NOT EXISTS loghub;", "SCHEMA "+schema+";")
	ddl = strings.ReplaceAll(ddl, "loghub.inbox", schema+".inbox")
	ctx := context.Background()
	if _, err := p.pool.Exec(ctx, ddl, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("applying docker/postgres/inbox.sql: %v", err)
	}
	t.Cleanup(func() { p.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	table := schema + ".inbox"
	if alter != "" {
		if _, err := p.pool.Exec(ctx, strings.ReplaceAll(alter, "$T", table), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
	}
	return table
}

// count counts the rows of a queue.
func (p *pgdb) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := p.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

// pgRow is one row of loghub.inbox (SPEC §3.7.1), with the envelope a consumer
// rebuilds from it.
type pgRow struct {
	UUID, From, Space, Type string
	Level, Step             int
	SID                     int64
	EventAt                 time.Time
	Nsec                    int
	Payload                 []byte

	// Env is the envelope rebuilt from the columns, in the order this server
	// writes the keys: the header from the columns, event from event_at and
	// event_nsec, payload as jsonb gave it back.
	Env []byte
}

// event is event.uts and event.sub as the columns hold them: event_at keeps the
// microsecond, event_nsec the 0-999 ns below it.
func (r *pgRow) event() jlh.Event {
	return jlh.Event{UTS: r.EventAt.Unix(), Sub: int64(r.EventAt.Nanosecond()) + int64(r.Nsec)}
}

func (r *pgRow) rebuild() {
	ev := r.event()
	b := fmt.Appendf(nil, `{"uuid":%q,"from":%q,"space":%q,"type":%q,"level":%d,"sid":%d,"step":%d,`+
		`"event":{"uts":%d,"sub":%d,"iso":"%s"},"payload":`,
		r.UUID, r.From, r.Space, r.Type, r.Level, r.SID, r.Step, ev.UTS, ev.Sub, ev.AppendISO(nil))
	b = append(b, r.Payload...)
	r.Env = append(b, '}')
}

// rows reads every row of a queue, in uuid order: the consumer's order.
func (p *pgdb) rows(t *testing.T, table string) []pgRow {
	t.Helper()
	rows, err := p.pool.Query(context.Background(), `
	  SELECT uuid::text, from_uuid::text, space, type, level, sid, step, event_at, event_nsec, payload::text
	  FROM `+table+` ORDER BY uuid`)
	if err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}
	defer rows.Close()
	var out []pgRow
	for rows.Next() {
		var r pgRow
		var payload string
		if err := rows.Scan(&r.UUID, &r.From, &r.Space, &r.Type, &r.Level, &r.SID, &r.Step,
			&r.EventAt, &r.Nsec, &payload); err != nil {
			t.Fatal(err)
		}
		r.Payload = []byte(payload)
		r.rebuild()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// table is the instance's queue.
func (in *instance) table() string { return in.app.cfg.SinkPG.Table }

// count counts the rows of the instance's queue.
func (in *instance) count() int {
	in.t.Helper()
	return requirePG(in.t).count(in.t, in.table())
}

// rows reads the instance's queue.
func (in *instance) rows() []pgRow {
	in.t.Helper()
	return requirePG(in.t).rows(in.t, in.table())
}

// waitRows waits until the instance's queue holds at least n rows and returns
// them all.
func (in *instance) waitRows(n int) []pgRow {
	in.t.Helper()
	waitFor(in.t, 30*time.Second, fmt.Sprintf("%d rows in %s", n, in.table()), func() bool {
		return in.count() >= n
	})
	return in.rows()
}

// checkRow validates the envelope a consumer rebuilds from a row and the
// columns the queue reads without opening the payload: type=syslog at step 0,
// a severity for level (inbox_syslog_ck), the reception clock in event_at.
// jlhcheck proves the rest -- level is severity.code, sid follows the rule of
// the host, the uuid's instant is the event's.
func checkRow(t *testing.T, r pgRow) jlhEnv {
	t.Helper()
	e := mustJLH(t, r.Env)
	if r.Type != "syslog" || r.Step != 0 || r.Level > 7 || r.Nsec < 0 || r.Nsec > 999 {
		t.Errorf("row %s: type=%q step=%d level=%d event_nsec=%d", r.UUID, r.Type, r.Step, r.Level, r.Nsec)
	}
	return e
}

// byMessage indexes the envelopes of rows by payload.message. A NUL in a
// message comes back from jsonb as U+FFFD (SPEC §3.7.1).
func byMessage(t *testing.T, rows []pgRow) map[string]jlhEnv {
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

// checkReingested proves that rows are the dumped envelopes: the same uuids,
// every queue column the one the line's header says, and the same payload
// value (jsonb keeps neither key order nor spacing, so it is compared as a
// value).
func checkReingested(t *testing.T, dumped []jlhEnv, rows []pgRow) {
	t.Helper()
	if len(rows) != len(dumped) {
		t.Fatalf("%d rows for %d dumped envelopes", len(rows), len(dumped))
	}
	byUUID := make(map[string]pgRow, len(rows))
	for _, r := range rows {
		checkRow(t, r)
		byUUID[r.UUID] = r
	}
	for _, d := range dumped {
		r, ok := byUUID[d.UUID]
		if !ok {
			t.Errorf("dumped envelope %s is not in the queue", d.UUID)
			continue
		}
		ev := r.event()
		if r.From != d.From || r.Space != d.Space || r.Type != d.Type || r.Level != d.Level ||
			uint64(r.SID) != d.SID || r.Step != d.Step || ev.UTS != d.Event.UTS || ev.Sub != d.Event.Sub {
			t.Errorf("row %s columns disagree with the dumped header\n row %s\nline %s", d.UUID, r.Env, d.bytes)
		}
		p, ok := jlh.Payload(d.bytes)
		if !ok {
			t.Fatalf("no payload in the dumped line %s", d.bytes)
		}
		if !sameJSON(t, queueForm(t, p), r.Payload) {
			t.Errorf("row %s payload is not the dumped one\n row %s\nline %s", d.UUID, r.Payload, p)
		}
	}
}

// queueForm is what loghub.inbox holds for a payload (SPEC §3.7.1): the payload
// itself, unless it carries U+0000, which jsonb refuses (22P05). Then raw goes
// in base64 -- exact, still size octets -- and every other string gets U+FFFD
// for the NUL. It is written here from the SPEC, apart from the sink's own
// code, so the two can disagree.
func queueForm(t *testing.T, payload []byte) []byte {
	t.Helper()
	if !bytes.Contains(payload, []byte(`\u0000`)) {
		return payload
	}
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		t.Fatalf("%v: %s", err, payload)
	}
	var clean func(v any) any
	clean = func(v any) any {
		switch x := v.(type) {
		case string:
			return strings.ReplaceAll(x, "\x00", "\uFFFD")
		case []any:
			for i := range x {
				x[i] = clean(x[i])
			}
		case map[string]any:
			for k, e := range x {
				x[k] = clean(e)
			}
		}
		return v
	}
	raw, hasRaw := doc["raw"].(string)
	doc = clean(doc).(map[string]any)
	if hasRaw && strings.Contains(raw, "\x00") {
		doc["raw"] = base64.StdEncoding.EncodeToString([]byte(raw))
		doc["raw-encoding"] = "base64"
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
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
//   - fatalHealth: the health probe fails with ClassFatal, which WaitReady
//     does not retry (SPEC §13.1 step 7).
type faultySink struct {
	sink.Sink

	failWrites  atomic.Bool
	failFatal   atomic.Bool
	failHealth  atomic.Bool
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
		// The shape of a dropped table or a revoked INSERT: the destination
		// answered and will not change its mind (SPEC §13.3).
		return &sink.Error{Class: sink.ClassFatal, Op: "write", Err: errors.New("relation does not exist (injected)")}
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
		// A refusal that waiting will not change: a wrong password, a
		// missing table (SPEC §13.1 step 7).
		return &sink.Error{Class: sink.ClassFatal, Op: "health", Err: errors.New("permission denied (injected)")}
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

// noDrops requires that nothing was counted as dropped or handed back: a held
// transaction that got its answer is neither (SPEC §5.4, §5.7).
func noDrops(t *testing.T, m *obs.Metrics) {
	t.Helper()
	for _, d := range []obs.DropReason{obs.DropEmpty, obs.DropUnaddressable} {
		if n := m.MessagesDroppedTotal[d].Load(); n != 0 {
			t.Errorf("relplog_messages_dropped_total{reason=%q} = %d, want 0", d, n)
		}
	}
	for _, r := range []obs.RELPUnanswered{obs.UnansweredBackpressure, obs.UnansweredShutdown, obs.UnansweredConnLost} {
		if n := m.RELPUnansweredTotal[r].Load(); n != 0 {
			t.Errorf("relplog_relp_unanswered_total[%d] = %d, want 0", r, n)
		}
	}
}
