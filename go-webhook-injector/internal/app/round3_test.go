package app

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap/pcaptest"
)

// Regressions of the round of 2026-11-07 (SPEC §4.5, §4.6, §5.5, §6.8,
// §6.9, §8.6).

// rawReq is one request as a raw keep-alive server read it: the field lines
// in wire order, and the connection it came on.
type rawReq struct {
	conn   int
	line   string
	fields []string
	body   string
}

// orderServer answers 200 to every request and records the head as it was
// on the wire (httptest gives a map, which has no order).
type orderServer struct {
	mu    sync.Mutex
	reqs  []rawReq
	conns atomic.Int64
	url   string
}

func newOrderServer(t *testing.T) *orderServer {
	s := &orderServer{}
	s.url = rawServer(t, func(c net.Conn) {
		defer c.Close()
		id := int(s.conns.Add(1))
		br := bufio.NewReader(c)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			r := rawReq{conn: id, line: strings.TrimRight(line, "\r\n")}
			cl := 0
			for {
				l, err := br.ReadString('\n')
				if err != nil {
					return
				}
				l = strings.TrimRight(l, "\r\n")
				if l == "" {
					break
				}
				r.fields = append(r.fields, l)
				if k, v, _ := strings.Cut(l, ":"); strings.EqualFold(k, "Content-Length") {
					cl, _ = strconv.Atoi(strings.TrimSpace(v))
				}
			}
			b := make([]byte, cl)
			if _, err := io.ReadFull(br, b); err != nil {
				return
			}
			r.body = string(b)
			s.mu.Lock()
			s.reqs = append(s.reqs, r)
			s.mu.Unlock()
			if _, err := c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")); err != nil {
				return
			}
		}
	})
	return s
}

func (s *orderServer) all() []rawReq {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rawReq(nil), s.reqs...)
}

var cli, srv = netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("198.51.100.1")

func at(ms int) time.Time { return time.Unix(1800000000, 0).Add(time.Duration(ms) * time.Millisecond) }

// capture writes a pcap of the given connections, the frames merged in
// time order (the connections interleave as they did on the wire).
func capture(t *testing.T, conns ...[]pcaptest.Frame) string {
	t.Helper()
	var all []pcaptest.Frame
	for _, c := range conns {
		all = append(all, c...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].TS.Before(all[j].TS) })
	file := filepath.Join(t.TempDir(), "cap.pcap")
	if err := os.WriteFile(file, pcaptest.Classic(1, all), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func flowFrames(port uint16, t0 int, payload string, mss int) []pcaptest.Frame {
	c := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, port), Dst: netip.AddrPortFrom(srv, 8080), ISN: uint32(port) * 1000, T0: at(t0)}
	return c.Frames(c.Split([]byte(payload), mss), false, false)
}

// The replay sends the captured fields in capture order, names as written
// (SPEC §5.5); --pcap-xff joins the captured X-Forwarded-For where the first
// one was.
func TestReplayKeepsHeaderOrder(t *testing.T) {
	s := newOrderServer(t)
	req := "POST /o HTTP/1.1\r\nhost: hooks.example\r\nX-Zeta: 1\r\nx-forwarded-for: 10.0.0.1\r\ncontent-type: text/plain\r\n" +
		"Connection: keep-alive\r\nX-Alpha: a\r\nX-Forwarded-For: 10.0.0.2\r\nX-Zeta: 2\r\nContent-Length: 2\r\n\r\nok"
	file := capture(t, flowFrames(40001, 0, req, 30))
	if code, sum := runJSON(t, "--url", s.url, "--pcap", file, "--pcap-xff", "--header", "X-Extra: e"); code != ExitOK || sum.OK != 1 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	got := s.all()
	want := []string{"Host: hooks.example", "X-Zeta: 1", "x-forwarded-for: 10.0.0.1, 10.0.0.2, 192.0.2.10", "content-type: text/plain",
		"X-Alpha: a", "X-Zeta: 2", "X-Extra: e", "Content-Length: 2"}
	if len(got) != 1 || strings.Join(got[0].fields, "|") != strings.Join(want, "|") || got[0].body != "ok" {
		t.Fatalf("%q", got)
	}
}

// h2cCapture is the client direction of an h2c connection: two streams,
// the second one interleaved and ended first.
func h2cCapture() []byte {
	var buf, hb bytes.Buffer
	buf.WriteString("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
	fr := http2.NewFramer(&buf, nil)
	enc := hpack.NewEncoder(&hb)
	fr.WriteSettings()
	block := func(kv ...string) []byte {
		hb.Reset()
		for i := 0; i < len(kv); i += 2 {
			enc.WriteField(hpack.HeaderField{Name: kv[i], Value: kv[i+1]})
		}
		return append([]byte(nil), hb.Bytes()...)
	}
	fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, EndHeaders: true,
		BlockFragment: block(":method", "POST", ":scheme", "http", ":authority", "h2.example", ":path", "/h2/one", "x-b", "2", "content-type", "application/json", "x-a", "1")})
	fr.WriteData(1, false, []byte(`{"n":`))
	fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, EndHeaders: true, EndStream: true,
		BlockFragment: block(":method", "GET", ":scheme", "http", ":authority", "h2.example", ":path", "/h2/two", "x-b", "2")})
	fr.WriteData(1, true, []byte(`1}`))
	return buf.Bytes()
}

// A captured h2c connection is replayed (SPEC §6.8), here over HTTP/1.1:
// pseudo-headers become the request line and Host, the fields keep the
// order of the header block.
func TestReplayH2C(t *testing.T) {
	s := newOrderServer(t)
	file := capture(t, flowFrames(40001, 0, string(h2cCapture()), 50))
	code, sum := runJSON(t, "--url", s.url, "--pcap", file)
	if code != ExitOK || sum.OK != 2 || sum.Source["http2_requests"] != 2 || sum.Source["http_h2c_streams"] != 1 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	got := s.all()
	if len(got) != 2 || got[0].line != "GET /h2/two HTTP/1.1" || got[1].line != "POST /h2/one HTTP/1.1" ||
		strings.Join(got[1].fields, "|") != "Host: h2.example|x-b: 2|content-type: application/json|x-a: 1|Content-Length: 7" ||
		got[1].body != `{"n":1}` {
		t.Fatalf("%q", got)
	}
	// And over HTTP/2 to an h2c server.
	f := newFake(t, "h2c")
	if code, sum := runJSON(t, "--url", f.srv.URL, "--pcap", file, "--http2"); code != ExitOK || sum.OK != 2 {
		t.Fatalf("h2: exit %d %+v", code, sum)
	}
	// The streams of an HTTP/2 connection run at once: the order of arrival
	// is not the order sent.
	r := f.all()
	sort.Slice(r, func(i, j int) bool { return r[i].Method > r[j].Method })
	if len(r) != 2 || r[0].Method != "POST" || r[0].Proto != "HTTP/2.0" || r[0].Host != "h2.example" || r[0].Body != `{"n":1}` {
		t.Fatalf("%+v", r)
	}
}

func req(path string) string {
	return "POST " + path + " HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n\r\nx"
}

// --pcap-flows (SPEC §6.9): the requests of one captured connection go
// on one connection of their own, in order; a flow past --concurrency
// closes the one idle the longest.
func TestPcapConnFlow(t *testing.T) {
	// A and B interleave (A: SYN 0, a1 1, a2 2, FIN 3; B: SYN 1, b1+b2 2,
	// FIN 3); C starts after both ended.
	file := capture(t,
		flowFrames(40001, 0, req("/a1")+req("/a2"), len(req("/a1"))),
		flowFrames(40002, 1, req("/b1")+req("/b2"), 1000),
		flowFrames(40003, 10, req("/c1"), 1000))
	byConn := func(rs []rawReq) map[int]string {
		m := map[int]string{}
		for _, r := range rs {
			m[r.conn] += strings.Fields(r.line)[1]
		}
		return m
	}
	s := newOrderServer(t)
	code, sum := runJSON(t, "--url", s.url, "--pcap", file, "--pcap-flows", "--concurrency", "4")
	got := byConn(s.all())
	var groups []string
	for _, g := range got {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	if code != ExitOK || sum.OK != 5 || sum.Conns.Opened != 3 || strings.Join(groups, " ") != "/a1/a2 /b1/b2 /c1" || sum.Source["pcap_flows_ended"] != 3 {
		t.Fatalf("exit %d %+v %q", code, sum, groups)
	}
	// One flow at a time: every new flow closes the other's connection.
	s = newOrderServer(t)
	code, sum = runJSON(t, "--url", s.url, "--pcap", file, "--pcap-flows", "--concurrency", "1")
	if code != ExitOK || sum.OK != 5 || sum.Events["flow split (concurrency cap)"] == 0 {
		t.Fatalf("cap 1: exit %d %+v", code, sum)
	}
	// Shared (the default): one worker, one connection for everything.
	s = newOrderServer(t)
	if code, sum = runJSON(t, "--url", s.url, "--pcap", file); code != ExitOK || sum.Conns.Opened != 1 || len(byConn(s.all())) != 1 {
		t.Fatalf("shared: exit %d %+v", code, sum)
	}
}

// A 429 handed on at the --requests-per-conn boundary waits for what is
// left of its Retry-After on the next connection, and keeps its count of
// tries (SPEC §4.5). Before, it went at once and the count restarted.
func TestRetryAfterAcrossConnections(t *testing.T) {
	f := newFake(t, "h1")
	var mu sync.Mutex
	tries := map[string][]time.Time{}
	f.status = func(_ int64, r *http.Request) (int, string) {
		mu.Lock()
		defer mu.Unlock()
		k := r.URL.Path
		tries[k] = append(tries[k], time.Now())
		if len(tries[k]) == 1 {
			return 429, "1"
		}
		return 200, ""
	}
	code, sum := runJSON(t, "--url", f.srv.URL+"/r/{seq}", "--count", "3", "--requests-per-conn", "1", "--retries", "3")
	if code != ExitOK || sum.OK != 3 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	mu.Lock()
	for k, ts := range tries {
		if len(ts) != 2 || ts[1].Sub(ts[0]) < 900*time.Millisecond {
			t.Errorf("%s: %d tries, resent after %v (Retry-After: 1)", k, len(ts), ts[len(ts)-1].Sub(ts[0]))
		}
	}
	mu.Unlock()

	// 503 forever, one request per connection: exactly 1 + --retries tries.
	g := newFake(t, "h1")
	var n atomic.Int64
	g.status = func(int64, *http.Request) (int, string) { n.Add(1); return 503, "0" }
	code, sum = runJSON(t, "--url", g.srv.URL, "--count", "2", "--requests-per-conn", "1", "--retries", "2", "--drain-timeout", "3s")
	if code != ExitFailures || sum.Outcomes["503"] != 2 || n.Load() != 6 {
		t.Fatalf("exit %d, %d tries for 2 requests with --retries 2: %+v", code, n.Load(), sum)
	}
}

// A server without keep-alive (SPEC §4.6): after two connections closed at
// their first response, each connection carries one request with its own
// "Connection: close", and nothing is written behind it for nothing.
func TestNoKeepAliveAdapts(t *testing.T) {
	f := newFake(t, "h1")
	f.maxPerConn = 1
	code, sum := runJSON(t, "--url", f.srv.URL+"/k/{seq}", "--count", "2000", "--concurrency", "2", "--pipeline", "32")
	if code != ExitOK || sum.OK != 2000 || sum.Conns.Broken != 0 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	seen := map[string]int{}
	for _, r := range f.all() {
		seen[r.URI]++
	}
	if len(seen) != 2000 || len(f.all()) != 2000 {
		t.Fatalf("%d distinct of %d", len(seen), len(f.all()))
	}
	if sum.Events["adapted: server without keep-alive (one request per connection)"] != 1 {
		t.Fatalf("not adapted: %v", sum.Events)
	}
	// Only the pipelines of the first connections were written for nothing
	// (31 of 32 each); without the adaptation, every connection's.
	if w := sum.Events["resent on a new connection"] + sum.Events["requeued unwritten (connection closing)"]; w > 200 {
		t.Fatalf("%d requests written behind a close", w)
	}
}

// keepAliveLater closes every connection at its first response until
// switch is set, then keeps them alive.
type keepAliveLater struct{ alive atomic.Bool }

func (k *keepAliveLater) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	if !k.alive.Load() {
		w.Header().Set("Connection", "close")
	}
	w.WriteHeader(200)
}

// The probe every 64 adapted connections finds a server that keeps alive
// again, and the adaptation ends.
func TestNoKeepAliveAdaptsBack(t *testing.T) {
	k := &keepAliveLater{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: k}
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close() })
	time.AfterFunc(300*time.Millisecond, func() { k.alive.Store(true) })
	code, sum := runJSON(t, "--url", "http://"+ln.Addr().String()+"/", "--duration", "1500ms", "--rate", "400", "--pipeline", "4")
	if code != ExitOK || sum.Events["adapted: server without keep-alive (one request per connection)"] != 1 ||
		sum.Events["adapted back: server keeps alive"] != 1 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

// inboxServer is a webhook server with sink=sqlite in miniature: it writes
// every request into an inbox table (W SPEC §3.7) and answers 200 with the
// uuid. drop and twice make it lose or double some.
type inboxServer struct {
	db         *sql.DB
	path       string
	n          atomic.Int64
	drop       func(n int64) bool
	twice      func(n int64) bool
	failStored func(n int64) bool // store, then answer 500
}

func newInbox(t *testing.T) *inboxServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE inbox (uuid TEXT NOT NULL PRIMARY KEY, from_uuid TEXT NOT NULL, event_at TEXT NOT NULL,
		received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), sid INTEGER NOT NULL DEFAULT 0,
		step INTEGER NOT NULL DEFAULT 0, level INTEGER NOT NULL, event_nsec INTEGER NOT NULL DEFAULT 0,
		space TEXT NOT NULL DEFAULT 'default', type TEXT NOT NULL, payload TEXT NOT NULL CHECK (json_type(payload) = 'object')) STRICT`); err != nil {
		t.Fatal(err)
	}
	return &inboxServer{db: db, path: path}
}

func (s *inboxServer) uuid(n int64) string {
	ms := uint64(time.Now().UnixMilli())
	return fmt.Sprintf("%08x-%04x-7000-8000-%012x", ms>>16, ms&0xffff, n)
}

func (s *inboxServer) store(u string, body []byte) error {
	p, _ := json.Marshal(map[string]any{"mode": "fast", "content": map[string]any{"length": len(body), "type": "application/json", "encoding": "utf-8", "body": string(body)}})
	_, err := s.db.Exec(`INSERT INTO inbox (uuid, from_uuid, event_at, level, type, payload) VALUES (?, 'f', 'x', 9, 'webhook.request', ?)`, u, string(p))
	return err
}

func (s *inboxServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	n := s.n.Add(1)
	u := s.uuid(n)
	if s.drop == nil || !s.drop(n) {
		if err := s.store(u, body); err != nil {
			w.WriteHeader(500)
			return
		}
	}
	if s.twice != nil && s.twice(n) {
		s.store(s.uuid(n+1<<40), body) // processed twice: another uuid, the same body
	}
	w.Header().Set("X-LogHub-UUID", u)
	if s.failStored != nil && s.failStored(n) {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
}

func (s *inboxServer) start(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: s}
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close() })
	return "http://" + ln.Addr().String() + "/hook"
}

// The delivery check (SPEC §8.6) against a SQLite inbox: everything there
// passes; a lost write, a request processed twice, or a missing table is
// caught: exit 1 for loss and duplicate, 4 (before sending) for a check
// that cannot be made.
func TestVerifySQLite(t *testing.T) {
	s := newInbox(t)
	url := s.start(t)
	code, sum := runJSON(t, "--url", url, "--count", "500", "--concurrency", "4", "--pipeline", "4", "--verify-sqlite", s.path)
	if v := sum.Verify; code != ExitOK || v == nil || v.Expected != 500 || v.Found != 500 || v.Records != 500 || v.Failed() || !v.Complete {
		t.Fatalf("exit %d %+v", code, sum.Verify)
	}

	s = newInbox(t)
	s.drop = func(n int64) bool { return n%50 == 0 }
	s.twice = func(n int64) bool { return n == 7 }
	url = s.start(t)
	code, sum = runJSON(t, "--url", url, "--count", "500", "--verify-sqlite", s.path, "--verify-timeout", "1s")
	if v := sum.Verify; code != ExitFailures || v.Lost != 10 || v.Duplicated != 1 || v.Found != 490 || len(v.LostSeqs) != 10 {
		t.Fatalf("exit %d %+v", code, sum.Verify)
	}

	// Stored but answered 500: not a failure of the check, but counted.
	s = newInbox(t)
	s.failStored = func(n int64) bool { return n <= 3 }
	url = s.start(t)
	code, sum = runJSON(t, "--url", url, "--count", "20", "--verify-sqlite", s.path, "--verify-timeout", "1s")
	if v := sum.Verify; code != ExitFailures || v.Unexpected != 3 || v.Failed() || !v.Complete {
		t.Fatalf("exit %d %+v", code, sum.Verify)
	}

	// No inbox: refused before anything is sent.
	empty := filepath.Join(t.TempDir(), "empty.db")
	f := newFake(t, "h1")
	if code, _, errs := runMain(t, "--url", f.srv.URL, "--verify-sqlite", empty); code != ExitVerify || len(f.all()) != 0 || !strings.Contains(errs, "table inbox") {
		t.Fatalf("exit %d %q, %d sent", code, errs, len(f.all()))
	}
}

func TestVerifyUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--verify-url", "mysql://u:pw@h/db"},
		{"--verify-url", "nats://h", "--verify-sqlite", "x.db"},
		{"--verify-timeout", "5s"},
		{"--verify-stream", "S"},
		{"--verify-table", "t"},
		{"--verify-url", "postgres://h/db", "--verify-stream", "S"},
		{"--verify-url", "redis://h", "--verify-table", "t"},
		{"--verify-sqlite", "x.db", "--verify-table", "Bad;Name"},
		{"--verify-url", "nats://h", "--verify-timeout", "0s"},
		{"--pcap-flows"},
	} {
		code, _, errs := runMain(t, args...)
		if code != ExitUsage {
			t.Errorf("%v: exit %d", args, code)
		}
		if strings.Contains(errs, "pw@") {
			t.Errorf("%v: the URL was echoed: %q", args, errs)
		}
	}
}
