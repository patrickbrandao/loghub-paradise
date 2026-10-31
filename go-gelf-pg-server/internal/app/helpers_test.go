package app

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/sink"
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
//	GELF_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
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
	AgentName: "go-gelf-pg-server-test",
	Hostname:  "test-host",
	MachineID: "0123456789abcdef0123456789abcdef",
}

// otherID is a second instance: the second life of the emergency test. Its
// from differs from testID's, so a test can tell who RECEIVED a message from
// who reingested it (SPEC §3.1, §3.5).
var otherID = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8b9",
	Space:     "default",
	AgentName: "go-gelf-pg-server-test-b",
	Hostname:  "test-host-b",
	MachineID: "fedcba9876543210fedcba9876543210",
}

// localArgs binds everything to 127.0.0.1:0 and names no destination: the
// sink keys keep their defaults. It is enough for a boot that must be refused
// before step 7 (exit 1), and nothing else. Every default that would touch a
// well-known port (:12201, :12202, 127.0.0.1:9104) is overridden. Later arguments win,
// so a test appends what it needs.
func localArgs(dir string) []string {
	return []string{
		"--server.data_dir=" + dir,
		"--admin.bind=127.0.0.1:0",
		"--listen.udp=127.0.0.1:0",
		"--listen.tcp=127.0.0.1:0",
		"--listen.http=127.0.0.1:0",
		"--sink.health_interval=50ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=5s",
		"--log.level=error",
	}
}

// baseArgs is localArgs plus a fresh queue on the test PostgreSQL. It skips
// the test without GELF_TEST_PG_DSN. Two lives of one test share a queue by
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

// waitReady blocks until every transport is open (SPEC §13.1 step 11).
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

// gdoc is a GELF 1.1 document with the given short_message and level.
func gdoc(msg string, level int) string {
	return fmt.Sprintf(`{"version":"1.1","host":"h","short_message":%s,"level":%d}`, jsonString(msg), level)
}

// jsonString is s as a JSON string (a NUL becomes \u0000, not Go's \x00).
func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// doc is gdoc at level 6 (info), the GELF default.
func doc(msg string) string { return gdoc(msg, 6) }

// shortOf is the short_message of a document built by gdoc or docs.
func shortOf(document string) string {
	var d struct {
		Short string `json:"short_message"`
	}
	if err := json.Unmarshal([]byte(document), &d); err != nil {
		panic(err)
	}
	return d.Short
}

// docs builds n documents whose short_message is fmt.Sprintf(format, i).
func docs(n int, format string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = doc(fmt.Sprintf(format, i))
	}
	return out
}

// sendNUL writes documents on one connection, each terminated by \0 (SPEC §5.3).
func sendNUL(t *testing.T, addr string, documents ...string) {
	t.Helper()
	c := dialTCP(t, addr)
	defer c.Close()
	writeNUL(t, c, documents...)
}

// writeNUL writes \0-terminated documents on an open connection.
func writeNUL(t *testing.T, c io.Writer, documents ...string) {
	t.Helper()
	w := bufio.NewWriter(c)
	for _, d := range documents {
		w.WriteString(d)
		w.WriteByte(0)
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

// gzipOf compresses b with gzip.
func gzipOf(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// gelfChunks splits data into count GELF chunks of message id (SPEC §5.2):
// magic 1e 0f, the 8-byte id, the sequence number, the count, the data.
func gelfChunks(id uint64, data []byte, count int) [][]byte {
	var out [][]byte
	per := (len(data) + count - 1) / count
	for i := 0; i < count; i++ {
		lo, hi := min(i*per, len(data)), min((i+1)*per, len(data))
		c := []byte{0x1e, 0x0f}
		c = binary.BigEndian.AppendUint64(c, id)
		c = append(c, byte(i), byte(count))
		out = append(out, append(c, data[lo:hi]...))
	}
	return out
}

// sendDatagrams sends every datagram from ONE socket (the chunks of a message
// share the source, SPEC §5.2) and returns its local port.
func sendDatagrams(t *testing.T, addr string, datagrams ...[]byte) int {
	t.Helper()
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, d := range datagrams {
		if _, err := c.Write(d); err != nil {
			t.Fatal(err)
		}
	}
	return c.LocalAddr().(*net.UDPAddr).Port
}

// postGELF sends one POST /gelf with an optional Content-Encoding and returns
// the status and the response headers. c nil means a plain client.
func postGELF(t *testing.T, c *http.Client, url, enc string, body []byte) (int, http.Header) {
	t.Helper()
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if enc != "" {
		req.Header.Set("Content-Encoding", enc)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header
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

// jlhPayload is the type=gelf payload (SPEC §3.2).
type jlhPayload struct {
	Host         *jlhHost       `json:"host"`
	Transport    string         `json:"transport"`
	Compression  *string        `json:"compression"`
	Chunks       *int           `json:"chunks"`
	Format       string         `json:"format"`
	GELFVersion  *string        `json:"gelf-version"`
	Level        codeName       `json:"level"`
	Facility     *string        `json:"facility"`
	Timestamp    *string        `json:"timestamp"`
	ShortMessage *string        `json:"short-message"`
	FullMessage  *string        `json:"full-message"`
	File         *string        `json:"file"`
	Line         *int           `json:"line"`
	Fields       map[string]any `json:"fields"`
	Extra        map[string]any `json:"extra"`
	Size         int            `json:"size"`
	Raw          *string        `json:"raw"`
	RawEnc       string         `json:"raw-encoding"`
	Parse        *struct {
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

// message is payload.short-message.
func (p jlhPayload) message() string {
	if p.ShortMessage == nil {
		return ""
	}
	return *p.ShortMessage
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
// this suite reads back from the queue or a dump goes through here.
func mustJLH(t *testing.T, b []byte) jlhEnv {
	t.Helper()
	if err := jlhcheck.Validate(b); err != nil {
		t.Errorf("the envelope is not a valid JLH: %v\n%s", err, b)
	}
	var e jlhEnv
	// Numbers as json.Number: fields and extra carry the emitter's numbers as
	// they arrived (1e400), which a float64 cannot hold (SPEC §3.3).
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("the envelope does not decode: %v\n%s", err, b)
	}
	e.bytes = append([]byte(nil), b...)
	return e
}

// queueJLH is mustJLH for an envelope rebuilt from a queue row. The queue's
// copy breaks one rule of the oracle on purpose: a raw with a NUL byte goes in
// base64 although it is UTF-8 (SPEC §3.7.1 over §6.9), because jsonb refuses
// \u0000. Anything else the oracle says is a failure.
func queueJLH(t *testing.T, b []byte) jlhEnv {
	t.Helper()
	var e jlhEnv
	// Numbers as json.Number: fields and extra carry the emitter's numbers as
	// they arrived (1e400), which a float64 cannot hold (SPEC §3.3).
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("the envelope does not decode: %v\n%s", err, b)
	}
	e.bytes = append([]byte(nil), b...)
	if err := jlhcheck.Validate(b); err != nil {
		nulB64 := e.Payload.RawEnc == "base64" && bytes.IndexByte(e.Payload.rawBytes(), 0) >= 0
		if !nulB64 || !strings.Contains(err.Error(), "raw is UTF-8 and still went in base64") {
			t.Errorf("the envelope is not a valid JLH: %v\n%s", err, b)
		}
	}
	return e
}

// sameJSON reports whether two JSON documents hold the same value, whatever
// the key order and spacing: jsonb keeps neither (SPEC §3.7.1). Numbers are
// compared as exact rationals: jsonb keeps a numeric's value, not always its
// spelling (1e400 comes back with its 400 zeros).
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	return reflect.DeepEqual(exactJSON(t, a), exactJSON(t, b))
}

// exactJSON decodes b with every number as the string of its exact rational
// value.
func exactJSON(t *testing.T, b []byte) any {
	t.Helper()
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	var walk func(any) any
	walk = func(v any) any {
		switch x := v.(type) {
		case json.Number:
			r, ok := new(big.Rat).SetString(string(x))
			if !ok {
				t.Fatalf("number %s", x)
			}
			return r.RatString()
		case []any:
			for i := range x {
				x[i] = walk(x[i])
			}
		case map[string]any:
			for k := range x {
				x[k] = walk(x[k])
			}
		}
		return v
	}
	return walk(v)
}

// ---------------------------------------------------------------- postgres

const pgDSNEnv = "GELF_TEST_PG_DSN"

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
// GELF_TEST_PG_DSN and failing it when the variable names a server that does
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
	schema := fmt.Sprintf("sls_app_%d_%d", time.Now().UnixNano(), pgSchema.Add(1))
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
// columns the queue reads without opening the payload: type=gelf at step 0,
// a severity for level (inbox_gelf_ck), the reception clock in event_at.
// jlhcheck proves the rest -- level is severity.code, sid follows the rule of
// the host, the uuid's instant is the event's.
func checkRow(t *testing.T, r pgRow) jlhEnv {
	t.Helper()
	e := queueJLH(t, r.Env)
	if r.Type != "gelf" || r.Step != 0 || r.Level > 7 || r.Nsec < 0 || r.Nsec > 999 {
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
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
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
			// Keys too (fields and extra take any key); none of this suite's
			// documents has two keys that collide once the NUL is replaced.
			out := map[string]any{}
			for k, e := range x {
				out[strings.ReplaceAll(k, "\x00", "\uFFFD")] = clean(e)
			}
			return out
		}
		return v
	}
	raw, hasRaw := doc["raw"].(string)
	_, encoded := doc["raw-encoding"]
	doc = clean(doc).(map[string]any)
	if hasRaw && !encoded && strings.Contains(raw, "\x00") {
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
// previous life with the default [convert] keys would have dumped with.
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
