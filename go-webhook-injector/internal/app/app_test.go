package app

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap/pcaptest"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/report"
)

// seen is one request as the fake server got it.
type seen struct {
	Method, URI, Host, Proto string
	Header                   http.Header
	Body                     string
	Remote                   string
}

type fake struct {
	mu    sync.Mutex
	reqs  []seen
	conns atomic.Int64
	// status, when set, decides each response (n counts from 1).
	status func(n int64, r *http.Request) (code int, retryAfter string)
	n      atomic.Int64
	srv    *httptest.Server
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	n := f.n.Add(1)
	f.mu.Lock()
	f.reqs = append(f.reqs, seen{r.Method, r.RequestURI, r.Host, r.Proto, r.Header.Clone(), string(b), r.RemoteAddr})
	f.mu.Unlock()
	code := 200
	if f.status != nil {
		var ra string
		code, ra = f.status(n, r)
		if ra != "" {
			w.Header().Set("Retry-After", ra)
		}
	}
	w.Header().Set("X-LogHub-UUID", "uuid-"+strconv.FormatInt(n, 10))
	w.WriteHeader(code)
}

func (f *fake) all() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.reqs...)
}

// newFake starts a server. mode: "h1", "h2c", "tls" (h1 and h2 by ALPN).
func newFake(t *testing.T, mode string) *fake {
	t.Helper()
	f := &fake{}
	s := httptest.NewUnstartedServer(f)
	s.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			f.conns.Add(1)
		}
	}
	switch mode {
	case "h1":
		s.Start()
	case "h2c":
		p := new(http.Protocols)
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		s.Config.Protocols = p
		s.Start()
	case "tls":
		s.EnableHTTP2 = true
		s.StartTLS()
	}
	t.Cleanup(s.Close)
	f.srv = s
	return f
}

func (f *fake) caFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ca.pem")
	b := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.srv.Certificate().Raw})
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func runMain(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(append(args, "--progress", "0"), &out, &errb)
	return code, out.String(), errb.String()
}

func runJSON(t *testing.T, args ...string) (int, report.Summary) {
	t.Helper()
	code, out, errs := runMain(t, append(args, "--report", "json")...)
	var s report.Summary
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatalf("exit %d, report %q, stderr %q: %v", code, out, errs, err)
	}
	return code, s
}

// checkSeqs asserts that the generator bodies carry seq 1..n exactly once,
// as valid JSON of this run.
func checkSeqs(t *testing.T, reqs []seen, n int, run string) {
	t.Helper()
	if len(reqs) != n {
		t.Fatalf("%d requests, want %d", len(reqs), n)
	}
	got := map[int]bool{}
	for _, r := range reqs {
		var v struct {
			Seq int    `json:"seq"`
			Run string `json:"run"`
		}
		if err := json.Unmarshal([]byte(r.Body), &v); err != nil || v.Run != run {
			t.Fatalf("body %q: %v", r.Body, err)
		}
		if got[v.Seq] {
			t.Fatalf("seq %d twice", v.Seq)
		}
		got[v.Seq] = true
	}
	for i := 1; i <= n; i++ {
		if !got[i] {
			t.Fatalf("seq %d missing", i)
		}
	}
}

func TestGeneratorConcurrent(t *testing.T) {
	f := newFake(t, "h1")
	code, sum := runJSON(t, "--url", f.srv.URL+"/hook/{seq}?r={run}", "--count", "3000",
		"--concurrency", "4", "--run", "r1", "--header", "X-Test: n-{seq}", "--size", "300")
	if code != ExitOK || sum.OK != 3000 || sum.Outcomes["200"] != 3000 || sum.Conns.Opened != 4 || f.conns.Load() != 4 {
		t.Fatalf("exit %d %+v conns %d", code, sum, f.conns.Load())
	}
	reqs := f.all()
	checkSeqs(t, reqs, 3000, "r1")
	for _, r := range reqs {
		var v struct{ Seq int }
		json.Unmarshal([]byte(r.Body), &v)
		s := strconv.Itoa(v.Seq)
		if r.Method != "POST" || r.URI != "/hook/"+s+"?r=r1" || r.Header.Get("X-Test") != "n-"+s ||
			r.Header.Get("Content-Type") != "application/json" || len(r.Body) != 300 || r.Proto != "HTTP/1.1" {
			t.Fatalf("request %+v", r)
		}
	}
}

func TestPipelining(t *testing.T) {
	f := newFake(t, "h1")
	code, sum := runJSON(t, "--url", f.srv.URL, "--count", "5000", "--concurrency", "2", "--pipeline", "32", "--run", "p")
	if code != ExitOK || sum.OK != 5000 || f.conns.Load() != 2 {
		t.Fatalf("exit %d %+v conns %d", code, sum, f.conns.Load())
	}
	checkSeqs(t, f.all(), 5000, "p")
}

func TestH2C(t *testing.T) {
	f := newFake(t, "h2c")
	code, sum := runJSON(t, "--url", f.srv.URL+"/x", "--count", "2000", "--concurrency", "2", "--http2", "--streams", "50", "--run", "h")
	if code != ExitOK || sum.OK != 2000 || sum.Conns.Opened != 2 || f.conns.Load() != 2 {
		t.Fatalf("exit %d %+v conns %d", code, sum, f.conns.Load())
	}
	reqs := f.all()
	checkSeqs(t, reqs, 2000, "h")
	if reqs[0].Proto != "HTTP/2.0" {
		t.Fatalf("proto %s", reqs[0].Proto)
	}
}

func TestTLSBothProtocols(t *testing.T) {
	f := newFake(t, "tls")
	ca := f.caFile(t)
	url := strings.Replace(f.srv.URL, "127.0.0.1", "localhost", 1) + "/t"
	// httptest's certificate names 127.0.0.1 and example.com.
	for _, extra := range [][]string{nil, {"--http2"}} {
		args := append([]string{"--url", f.srv.URL + "/t", "--count", "200", "--concurrency", "2", "--tls-ca", ca}, extra...)
		if code, sum := runJSON(t, args...); code != ExitOK || sum.OK != 200 {
			t.Fatalf("%v: exit %d %+v", extra, code, sum)
		}
	}
	protos := map[string]int{}
	for _, r := range f.all() {
		protos[r.Proto]++
	}
	if protos["HTTP/1.1"] != 200 || protos["HTTP/2.0"] != 200 {
		t.Fatalf("protos %v", protos)
	}
	// An untrusted certificate is a failed dial: unreachable.
	if code, _ := runJSON(t, "--url", url, "--count", "3", "--drain-timeout", "300ms"); code != ExitUnreachable {
		t.Fatalf("untrusted: exit %d", code)
	}
}

func TestRetryAfter429(t *testing.T) {
	f := newFake(t, "h1")
	// The first 50 requests are refused once each.
	var refused sync.Map
	f.status = func(_ int64, r *http.Request) (int, string) {
		if _, again := refused.LoadOrStore(r.Header.Get("X-Seq"), true); !again {
			n, _ := strconv.Atoi(r.Header.Get("X-Seq"))
			if n <= 50 {
				return 429, "0"
			}
		}
		return 202, ""
	}
	code, sum := runJSON(t, "--url", f.srv.URL, "--count", "200", "--concurrency", "2", "--pipeline", "4",
		"--header", "X-Seq: {seq}", "--retries", "3", "--run", "rr")
	if code != ExitOK || sum.OK != 200 || sum.Outcomes["202"] != 200 || f.n.Load() != 250 {
		t.Fatalf("exit %d %+v server saw %d", code, sum, f.n.Load())
	}
	// Without retries the 429 is the outcome, and a failure.
	f.n.Store(0)
	refused = sync.Map{}
	code, sum = runJSON(t, "--url", f.srv.URL, "--count", "100", "--header", "X-Seq: {seq}")
	if code != ExitFailures || sum.Outcomes["429"] != 50 || sum.Outcomes["202"] != 50 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

func TestRetryGivesUpWithLastStatus(t *testing.T) {
	f := newFake(t, "h2c")
	f.status = func(int64, *http.Request) (int, string) { return 503, "" }
	for _, extra := range [][]string{nil, {"--http2"}} {
		f.n.Store(0)
		args := append([]string{"--url", f.srv.URL, "--count", "10", "--retries", "2", "--retry-max-wait", "10ms"}, extra...)
		code, sum := runJSON(t, args...)
		if code != ExitFailures || sum.Outcomes["503"] != 10 || f.n.Load() != 30 {
			t.Fatalf("%v: exit %d %+v server saw %d", extra, code, sum, f.n.Load())
		}
	}
}

func TestRequestsPerConn(t *testing.T) {
	f := newFake(t, "h1")
	code, sum := runJSON(t, "--url", f.srv.URL, "--count", "1000", "--requests-per-conn", "100", "--run", "pc")
	if code != ExitOK || sum.OK != 1000 || sum.Conns.Opened != 10 || sum.Conns.Broken != 0 || f.conns.Load() != 10 {
		t.Fatalf("exit %d %+v conns %d", code, sum, f.conns.Load())
	}
	checkSeqs(t, f.all(), 1000, "pc")
}

// rawServer answers with a fixed behavior per connection.
func rawServer(t *testing.T, serve func(c net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(c)
		}
	}()
	return "http://" + ln.Addr().String()
}

// A server that closes after every third response ("Connection: close"):
// the pipelined requests behind the close were not processed, so they go
// again on a new connection, and nothing is lost or counted twice.
func TestServerClosesConnection(t *testing.T) {
	var served atomic.Int64
	url := rawServer(t, func(c net.Conn) {
		defer c.Close()
		buf := make([]byte, 64<<10)
		var pending []byte
		for answered := 0; answered < 3; {
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			pending = append(pending, buf[:n]...)
			for answered < 3 {
				i := bytes.Index(pending, []byte("\r\n\r\n"))
				if i < 0 {
					break
				}
				h := string(pending[:i])
				cl := 0
				for _, l := range strings.Split(h, "\r\n") {
					if v, ok := strings.CutPrefix(l, "Content-Length: "); ok {
						cl, _ = strconv.Atoi(v)
					}
				}
				if len(pending) < i+4+cl {
					break
				}
				pending = pending[i+4+cl:]
				answered++
				served.Add(1)
				hdr := "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
				if answered == 3 {
					hdr += "Connection: close\r\n"
				}
				c.Write([]byte(hdr + "\r\n"))
			}
		}
	})
	for _, pipe := range []string{"1", "8", "64"} {
		served.Store(0)
		code, sum := runJSON(t, "--url", url, "--count", "300", "--pipeline", pipe)
		if code != ExitOK || sum.OK != 300 || served.Load() != 300 || sum.Conns.Opened != 100 {
			t.Fatalf("pipeline %s: exit %d %+v served %d", pipe, code, sum, served.Load())
		}
	}
}

func TestResponseTimeout(t *testing.T) {
	url := rawServer(t, func(c net.Conn) {
		defer c.Close()
		io.Copy(io.Discard, c) // reads, never answers
	})
	code, sum := runJSON(t, "--url", url, "--count", "4", "--pipeline", "4", "--response-timeout", "200ms")
	if code != ExitFailures || sum.Outcomes["timeout"] != 4 || sum.Messages != 4 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	code, sum = runJSON(t, "--url", url, "--count", "3", "--http2", "--response-timeout", "200ms")
	if code == ExitOK || sum.Failed != 3 {
		t.Fatalf("h2: exit %d %+v", code, sum)
	}
}

func TestUnreachable(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	for _, extra := range [][]string{nil, {"--http2"}} {
		args := append([]string{"--url", "http://" + addr, "--count", "3", "--drain-timeout", "300ms"}, extra...)
		code, sum := runJSON(t, args...)
		if code != ExitUnreachable || sum.Conns.Opened != 0 || sum.Conns.DialErrors == 0 {
			t.Fatalf("%v: exit %d %+v", extra, code, sum)
		}
	}
}

func TestUUIDOutAndDuration(t *testing.T) {
	f := newFake(t, "h1")
	out := filepath.Join(t.TempDir(), "uuids.txt")
	code, sum := runJSON(t, "--url", f.srv.URL, "--duration", "300ms", "--rate", "100", "--uuid-out", out)
	if code != ExitOK || sum.Messages < 15 || sum.Messages > 31 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if uint64(len(lines)) != sum.Messages || !strings.HasPrefix(lines[0], "1 200 uuid-") {
		t.Fatalf("uuid-out %q", lines)
	}
}

func TestTemplateFileAndDirectory(t *testing.T) {
	f := newFake(t, "h1")
	code, _ := runJSON(t, "--url", f.srv.URL+"/tpl", "--template", `{"user":"{rand:4}","n":{seq},"x":{"y":[1,2]}}`,
		"--count", "3", "--method", "PUT", "--content-type", "application/vnd.test+json")
	dir := t.TempDir()
	lines := filepath.Join(dir, "lines.txt")
	os.WriteFile(lines, []byte("{\"a\":1}\n\n{\"b\":2}\r\n"), 0o600)
	bodies := filepath.Join(dir, "bodies")
	os.Mkdir(bodies, 0o700)
	os.WriteFile(filepath.Join(bodies, "2.json"), []byte("second\nwith newline"), 0o600)
	os.WriteFile(filepath.Join(bodies, "1.json"), []byte("first"), 0o600)
	code2, _ := runJSON(t, "--url", f.srv.URL+"/file", "--file", lines)
	code3, _ := runJSON(t, "--url", f.srv.URL+"/dir", "--file", bodies, "--content-type", "", "--header", "Content-Type: text/plain")
	if code != ExitOK || code2 != ExitOK || code3 != ExitOK {
		t.Fatalf("exits %d %d %d", code, code2, code3)
	}
	var got []string
	for _, r := range f.all() {
		got = append(got, r.Method+" "+r.URI+" "+r.Header.Get("Content-Type")+" "+r.Body)
	}
	if len(got) != 7 || !strings.HasPrefix(got[0], `PUT /tpl application/vnd.test+json {"user":"`) ||
		!strings.HasSuffix(got[0], `,"n":1,"x":{"y":[1,2]}}`) ||
		got[3] != `POST /file application/json {"a":1}` || got[4] != `POST /file application/json {"b":2}` ||
		got[5] != "POST /dir text/plain first" || got[6] != "POST /dir text/plain second\nwith newline" {
		t.Fatalf("%q", got)
	}
}

func TestPcapReplay(t *testing.T) {
	f := newFake(t, "h1")
	cli, srv := netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("198.51.100.1")
	at := func(ms int) time.Time { return time.Unix(1800000000, 0).Add(time.Duration(ms) * time.Millisecond) }
	reqs := "POST /gh/push?x=1 HTTP/1.1\r\nHost: hooks.example\r\nContent-Type: application/json\r\nX-Github-Event: push\r\nContent-Length: 11\r\n\r\n{\"ref\":\"a\"}" +
		"PUT /chunk HTTP/1.1\r\nHost: hooks.example\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n" +
		"GET http://hooks.example/abs?q=2 HTTP/1.1\r\nHost: hooks.example\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n"
	c := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 40001), Dst: netip.AddrPortFrom(srv, 8080), ISN: 7, T0: at(10)}
	resp := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(srv, 8080), Dst: netip.AddrPortFrom(cli, 40001), ISN: 99, T0: at(11)}
	other := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 40002), Dst: netip.AddrPortFrom(srv, 9999), ISN: 5, T0: at(12)}
	frames := c.Frames(c.Split([]byte(reqs), 40), false, false)
	frames = append(frames, resp.Frames(resp.Split([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"), 100), false, false)...)
	frames = append(frames, other.Frames(other.Split([]byte("DELETE /x HTTP/1.1\r\nHost: o\r\n\r\n"), 100), false, false)...)
	file := filepath.Join(t.TempDir(), "cap.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)

	code, sum := runJSON(t, "--url", f.srv.URL, "--pcap", file, "--pcap-port", "8080", "--pcap-xff")
	if code != ExitOK || sum.OK != 3 || sum.Source["http_requests"] != 3 || sum.Source["tcp_directions_filtered"] != 2 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	got := f.all()
	want := []string{
		`POST /gh/push?x=1 hooks.example push application/json xff=192.0.2.10 body={"ref":"a"}`,
		`PUT /chunk hooks.example   xff=192.0.2.10 body=abcde`,
		`GET /abs?q=2 hooks.example   xff=10.0.0.1, 192.0.2.10 body=`,
	}
	for i, r := range got {
		s := r.Method + " " + r.URI + " " + r.Host + " " + r.Header.Get("X-Github-Event") + " " + r.Header.Get("Content-Type") +
			" xff=" + strings.Join(r.Header.Values("X-Forwarded-For"), ",") + " body=" + r.Body
		if s != want[i] {
			t.Fatalf("request %d:\n got %s\nwant %s", i, s, want[i])
		}
		if r.Header.Get("Transfer-Encoding") != "" || r.Header.Get("Connection") != "" {
			t.Fatalf("hop-by-hop header replayed: %v", r.Header)
		}
	}
	// --pcap-path url and --pcap-host url, and every port.
	f.mu.Lock()
	f.reqs = nil
	f.mu.Unlock()
	code, sum = runJSON(t, "--url", f.srv.URL+"/replay/{seq}", "--pcap", file, "--pcap-path", "url", "--pcap-host", "url")
	got = f.all()
	if code != ExitOK || sum.OK != 4 || sum.Source["http_response_streams"] != 1 || got[0].URI != "/replay/1" || got[0].Host != strings.TrimPrefix(f.srv.URL, "http://") {
		t.Fatalf("exit %d %+v %+v", code, sum, got)
	}
}

func TestBadPcap(t *testing.T) {
	file := filepath.Join(t.TempDir(), "x.pcap")
	os.WriteFile(file, []byte("not a capture"), 0o600)
	if code, _, errs := runMain(t, "--pcap", file); code != ExitUsage || !strings.Contains(errs, "x.pcap") {
		t.Fatalf("exit %d %q", code, errs)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--url", "ftp://x/"},
		{"--url", "http:///nohost"},
		{"--url", "http://h/a b"},
		{"--url", "http://h/{nope}"},
		{"--method", "post"},
		{"--method", "CONNECT"},
		{"--header", "NoColon"},
		{"--header", "Content-Length: 3"},
		{"--header", "Host: x"},
		{"--header", "Bad Name: x"},
		{"--host", "a b"},
		{"--template", "x", "--file", "y"},
		{"--size", "100", "--template", "x"},
		{"--http2", "--pipeline", "4"},
		{"--pipeline", "0"},
		{"--tls-ca", "x"},
		{"--pcap-xff"},
		{"--pcap-timing", "original"},
		{"--loop"},
		{"--report", "xml"},
		{"--template", "{rand:0}"},
		{"extra"},
	} {
		if code, _, _ := runMain(t, args...); code != ExitUsage {
			t.Errorf("%v: exit %d", args, code)
		}
	}
	if code, out, _ := runMain(t, "--version"); code != ExitOK || !strings.HasPrefix(out, Name+" ") {
		t.Fatalf("version: %d %q", code, out)
	}
}

// A server that accepts and hangs up without answering: a written request
// may have been processed, so it is a failure (no_response), never resent,
// and the run ends.
func TestServerHangsUp(t *testing.T) {
	url := rawServer(t, func(c net.Conn) {
		buf := make([]byte, 1)
		c.Read(buf)
		c.Close()
	})
	code, sum := runJSON(t, "--url", url, "--count", "50", "--pipeline", "8", "--drain-timeout", "2s")
	if code != ExitFailures || sum.Messages != 50 || sum.OK != 0 || sum.Outcomes["no_response"]+sum.Outcomes["conn_error"] != 50 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}
