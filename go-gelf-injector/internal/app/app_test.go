package app

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelftest"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/report"
)

func run(t *testing.T, args ...string) (int, report.Summary, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(append([]string{"--report", "json", "--progress", "0"}, args...), &out, &errb)
	var s report.Summary
	if out.Len() > 0 {
		if err := json.Unmarshal(out.Bytes(), &s); err != nil {
			t.Fatalf("report: %v\n%s\nstderr: %s", err, out.String(), errb.String())
		}
	}
	return code, s, errb.String()
}

func mustOK(t *testing.T, code int, s report.Summary, stderr string) {
	t.Helper()
	if code != ExitOK || s.Failed != 0 {
		t.Fatalf("exit %d, outcomes %v, events %v, stderr %s", code, s.Outcomes, s.Events, stderr)
	}
}

func TestTCP(t *testing.T) {
	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--target", r.Addr, "--count", "5000", "--concurrency", "4", "--run", "tcp1")
	mustOK(t, code, s, se)
	if s.Outcomes["sent"] != 5000 {
		t.Fatalf("outcomes %v", s.Outcomes)
	}
	r.C.Wait(5000, 5*time.Second)
	if err := gelftest.CheckSeqs(r.C.Docs(), "tcp1", 5000); err != nil {
		t.Fatal(err)
	}
	if r.Conns() != 4 {
		t.Fatalf("%d connections, want 4", r.Conns())
	}
}

func TestUDPChunkedGzip(t *testing.T) {
	r, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// 30 000 bytes of random padding: ~23 KB of gzip, 3 chunks of 8192.
	code, s, se := run(t, "--transport", "udp", "--target", r.Addr, "--count", "300", "--payload-size", "30000",
		"--rate", "3000", "--run", "udp1")
	mustOK(t, code, s, se)
	r.C.Wait(300, 5*time.Second)
	if err := gelftest.CheckSeqs(r.C.Docs(), "udp1", 300); err != nil {
		t.Fatal(err)
	}
	if r.C.Counter("chunked") != 300 || r.C.Counter("gzip") != 300 || r.C.Counter("invalid") != 0 {
		t.Fatalf("chunked %d gzip %d invalid %d", r.C.Counter("chunked"), r.C.Counter("gzip"), r.C.Counter("invalid"))
	}
	for _, d := range r.C.Docs()[:3] {
		if len(d) != 30000 {
			t.Fatalf("document of %d bytes, want 30000", len(d))
		}
	}
}

func TestUDPCompressions(t *testing.T) {
	r, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, c := range []string{"zlib", "none"} {
		run := "udp" + c
		code, s, se := runArgs(t, "--transport", "udp", "--target", r.Addr, "--count", "200", "--compress", c,
			"--chunk-size", "1420", "--payload-size", "4000", "--rate", "5000", "--run", run)
		mustOK(t, code, s, se)
		r.C.Wait(200*(i+1), 5*time.Second)
	}
	var z, n [][]byte
	for _, d := range r.C.Docs() {
		if bytes.Contains(d, []byte(`"_run":"udpzlib"`)) {
			z = append(z, d)
		} else {
			n = append(n, d)
		}
	}
	if err := gelftest.CheckSeqs(z, "udpzlib", 200); err != nil {
		t.Fatal(err)
	}
	if err := gelftest.CheckSeqs(n, "udpnone", 200); err != nil {
		t.Fatal(err)
	}
	if r.C.Counter("zlib") != 200 || r.C.Counter("chunks_3") != 200 {
		t.Fatalf("zlib %d, 3-chunk messages %d", r.C.Counter("zlib"), r.C.Counter("chunks_3"))
	}
}

func runArgs(t *testing.T, args ...string) (int, report.Summary, string) { return run(t, args...) }

func TestUDPTooManyChunks(t *testing.T) {
	r, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// 20 000 bytes in chunks of 100 - 12 = 88 bytes: 228 chunks > 128.
	code, s, _ := run(t, "--transport", "udp", "--target", r.Addr, "--count", "5", "--compress", "none",
		"--chunk-size", "100", "--payload-size", "20000")
	if code != ExitFailures || s.Outcomes["too_large"] != 5 || s.Messages != 5 {
		t.Fatalf("exit %d outcomes %v", code, s.Outcomes)
	}
	time.Sleep(100 * time.Millisecond)
	if r.C.Counter("chunks") != 0 {
		t.Fatal("a refused message sent chunks")
	}
}

func TestTLSWithProxyV2(t *testing.T) {
	dir := t.TempDir()
	r, err := gelftest.ListenTCP(true, dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--transport", "tls", "--target", r.Addr, "--tls-ca", r.CA, "--count", "1000",
		"--concurrency", "2", "--proxy-protocol", "v2", "--proxy-src", "203.0.113.7:5555", "--run", "tls1")
	mustOK(t, code, s, se)
	r.C.Wait(1000, 5*time.Second)
	if err := gelftest.CheckSeqs(r.C.Docs(), "tls1", 1000); err != nil {
		t.Fatal(err)
	}
	p := r.C.Proxied()
	if len(p) != 2 || p[0] != "203.0.113.7:5555" {
		t.Fatalf("proxied %v", p)
	}
}

func TestTLSUnknownCA(t *testing.T) {
	r, err := gelftest.ListenTCP(true, t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, _ := run(t, "--transport", "tls", "--target", r.Addr, "--drain-timeout", "300ms")
	if code != ExitUnreachable || s.Outcomes["not_sent"] != 1 {
		t.Fatalf("exit %d outcomes %v", code, s.Outcomes)
	}
}

func TestHTTPPipelinedGzip(t *testing.T) {
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	code, s, se := run(t, "--transport", "http", "--target", h.Addr, "--count", "3000", "--concurrency", "3",
		"--pipeline", "16", "--compress", "gzip", "--run", "http1")
	mustOK(t, code, s, se)
	if s.Outcomes["202"] != 3000 {
		t.Fatalf("outcomes %v", s.Outcomes)
	}
	if err := gelftest.CheckSeqs(h.C.Docs(), "http1", 3000); err != nil {
		t.Fatal(err)
	}
	if h.C.Counter("encoding_gzip") != 3000 {
		t.Fatalf("gzip bodies %d", h.C.Counter("encoding_gzip"))
	}
}

func TestHTTPRetry503(t *testing.T) {
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	var calls atomic.Int64
	h.Busy = func() bool { return calls.Add(1)%3 == 1 } // one in three is refused
	code, s, se := run(t, "--transport", "http", "--target", h.Addr, "--count", "300", "--pipeline", "4",
		"--retries", "5", "--compress", "zlib", "--run", "retry1")
	mustOK(t, code, s, se)
	if err := gelftest.CheckSeqs(h.C.Docs(), "retry1", 300); err != nil {
		t.Fatal(err)
	}
	if h.C.Counter("503") == 0 || h.C.Counter("encoding_deflate") != 300 {
		t.Fatalf("503s %d, deflate bodies %d", h.C.Counter("503"), h.C.Counter("encoding_deflate"))
	}
	// Without retries, a 503 is the final outcome: one per message.
	calls.Store(0)
	h.Busy = func() bool { return true }
	code, s, _ = run(t, "--transport", "http", "--target", h.Addr, "--count", "10")
	if code != ExitFailures || s.Outcomes["503"] != 10 || s.Messages != 10 {
		t.Fatalf("exit %d outcomes %v", code, s.Outcomes)
	}
}

func TestHTTPSWithProxyV1(t *testing.T) {
	h, err := gelftest.ListenHTTP(true, t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	h.Status = 200
	code, s, se := run(t, "--transport", "https", "--target", h.Addr, "--tls-ca", h.CA, "--count", "200",
		"--proxy-protocol", "v1", "--requests-per-conn", "50", "--run", "https1")
	mustOK(t, code, s, se)
	if s.Outcomes["200"] != 200 {
		t.Fatalf("outcomes %v", s.Outcomes)
	}
	if err := gelftest.CheckSeqs(h.C.Docs(), "https1", 200); err != nil {
		t.Fatal(err)
	}
	if p := h.C.Proxied(); len(p) != 4 || !strings.HasPrefix(p[0], "127.0.0.1:") {
		t.Fatalf("proxied %v", p)
	}
}

func TestHTTPPath404(t *testing.T) {
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	code, s, _ := run(t, "--transport", "http", "--target", h.Addr, "--http-path", "/other", "--count", "3")
	if code != ExitFailures || s.Outcomes["404"] != 3 {
		t.Fatalf("exit %d outcomes %v", code, s.Outcomes)
	}
}

// --duration without --count runs for the duration (before 2026-10-31 it
// sent a single message: the default count of 1 also applied).
func TestDurationWithoutCount(t *testing.T) {
	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	start := time.Now()
	code, s, se := run(t, "--target", r.Addr, "--duration", "500ms", "--rate", "1000")
	mustOK(t, code, s, se)
	if el := time.Since(start); el < 450*time.Millisecond {
		t.Fatalf("ran %v", el)
	}
	if s.Messages < 300 || s.Messages > 520 {
		t.Fatalf("%d messages in 500ms at 1000/s", s.Messages)
	}
}

func TestUnreachable(t *testing.T) {
	code, s, _ := run(t, "--target", "127.0.0.1:1", "--count", "3", "--drain-timeout", "300ms")
	if code != ExitUnreachable || s.Outcomes["not_sent"] != 3 {
		t.Fatalf("exit %d outcomes %v", code, s.Outcomes)
	}
}

func TestServerClosesMidway(t *testing.T) {
	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.CloseAfter = 100
	code, s, _ := run(t, "--target", r.Addr, "--count", "2000", "--rate", "4000")
	// No acknowledgement in GELF/TCP: what was written into a closing
	// connection may be lost, but every message has exactly one outcome and
	// the worker reconnects.
	if s.Messages != 2000 || s.OK+s.Failed != 2000 || s.Conns.Opened < 2 {
		t.Fatalf("exit %d messages %d ok %d failed %d conns %+v", code, s.Messages, s.OK, s.Failed, s.Conns)
	}
}

func TestTemplateAndFile(t *testing.T) {
	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tpl := `{"version":"1.1","host":"{host}","short_message":"t {seq}","_obj":{"a":{"b":1}},"_id":"{uuid}","_r":"{rand:4}","_ts":"{ts}","_u":{unix}}`
	code, s, se := run(t, "--target", r.Addr, "--template", tpl, "--count", "3", "--hostname", "h1")
	mustOK(t, code, s, se)
	r.C.Wait(3, 2*time.Second)
	for _, d := range r.C.Docs() {
		var m map[string]any
		if err := json.Unmarshal(d, &m); err != nil {
			t.Fatalf("%v: %s", err, d)
		}
		if m["host"] != "h1" || !strings.HasPrefix(m["short_message"].(string), "t ") || len(m["_id"].(string)) != 36 {
			t.Fatalf("%s", d)
		}
	}
	file := filepath.Join(t.TempDir(), "in.jsonl")
	os.WriteFile(file, []byte("{\"short_message\":\"a\"}\r\n\n  \n{\"short_message\":\"b\"}\nnot json\n{\"x\":\"nul\x00inside\"}"), 0o600)
	code, s, _ = run(t, "--target", r.Addr, "--file", file)
	if code != ExitFailures || s.Outcomes["sent"] != 3 || s.Outcomes["nul_byte"] != 1 || s.Source["file_empty_lines"] != 2 {
		t.Fatalf("exit %d outcomes %v source %v", code, s.Outcomes, s.Source)
	}
	r.C.Wait(6, 2*time.Second)
	docs := r.C.Docs()[3:]
	if string(docs[0]) != `{"short_message":"a"}` || string(docs[2]) != "not json" {
		t.Fatalf("%q", docs)
	}
	code, s, se = run(t, "--target", r.Addr, "--file", file, "--loop", "--count", "10")
	if s.Messages != 10 {
		t.Fatalf("loop: %d messages, exit %d %s", s.Messages, code, se)
	}
}

func TestUsage(t *testing.T) {
	cases := [][]string{
		{"--template", "{}", "--file", "x"},
		{"--transport", "tcp", "--compress", "gzip"},
		{"--transport", "tls", "--compress", "zlib"},
		{"--transport", "tcp", "--chunk-size", "1000"},
		{"--transport", "udp", "--chunk-size", "12"},
		{"--transport", "udp", "--chunk-size", "70000"},
		{"--transport", "udp", "--proxy-protocol", "v1"},
		{"--pipeline", "4"},
		{"--retries", "2"},
		{"--tls-ca", "x"},
		{"--transport", "http", "--http-path", "gelf"},
		{"--compress", "brotli", "--transport", "udp"},
		{"--transport", "quic"},
		{"--level", "8"},
		{"--template", "{}", "--payload-size", "100"},
		{"--loop"},
		{"--pcap-port", "12201"},
		{"--pcap-timing", "original"},
		{"--template", `{"a":"{nope}"}`},
		{"--target", "nohost"},
		{"--proxy-src", "1.2.3.4:5"},
		{"extra"},
		{"--progress", "5x"},
		{"--progress", "-1s"},
		{"--run", `x"y\z`},
		{"--run", strings.Repeat("a", 65)},
	}
	for _, c := range cases {
		if code, _, _ := run(t, c...); code != ExitUsage {
			t.Errorf("%v: exit %d, want 2", c, code)
		}
	}
	pc := filepath.Join(t.TempDir(), "x.pcap")
	os.WriteFile(pc, []byte("not a capture"), 0o600)
	if code, _, _ := run(t, "--pcap", pc); code != ExitUsage {
		t.Errorf("bad capture: exit %d", code)
	}
}

func genDoc(seq int, run string, pad int) []byte {
	d := fmt.Sprintf(`{"version":"1.1","host":"cap","short_message":"seq=%d run=%s injector=go-gelf-injector","level":6,"_seq":%d,"_run":"%s"`, seq, run, seq, run)
	if pad > 0 {
		d += `,"_pad":"` + strings.Repeat("p", pad) + `"`
	}
	return []byte(d + "}")
}

func gz(b []byte) []byte {
	var o bytes.Buffer
	w := gzip.NewWriter(&o)
	w.Write(b)
	w.Close()
	return o.Bytes()
}

func zl(b []byte) []byte {
	var o bytes.Buffer
	w := zlib.NewWriter(&o)
	w.Write(b)
	w.Close()
	return o.Bytes()
}

// chunks splits p into GELF chunks of at most size payload bytes.
func chunks(p []byte, size int, id byte) [][]byte {
	n := (len(p) + size - 1) / size
	var out [][]byte
	for i := range n {
		c := []byte{0x1e, 0x0f, id, 1, 2, 3, 4, 5, 6, 7, byte(i), byte(n)}
		out = append(out, append(c, p[i*size:min((i+1)*size, len(p))]...))
	}
	return out
}

// TestPcapReplay builds a capture with every way GELF travels -- plain,
// gzip, zlib and chunked UDP (chunks out of order and repeated), a TCP
// stream with NUL framing caught in the middle, an HTTP connection with
// pipelined gzip POSTs, and DNS noise -- and replays it over TCP.
func TestPcapReplay(t *testing.T) {
	cli, srv := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2")
	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	var frames []pcaptest.Frame
	udp := func(ms int, sport, dport uint16, p []byte) {
		ip := pcaptest.IP(cli, srv, 17, pcaptest.UDP(sport, dport, p), uint16(ms))
		frames = append(frames, pcaptest.Frame{TS: at(ms), Data: pcaptest.Link(1, ip)})
	}
	const runID = "cap1"
	udp(0, 53000, 53, []byte("\x12\x34\x01\x00 dns"))
	udp(1, 40000, 12201, genDoc(1, runID, 0))
	udp(2, 40000, 12201, gz(genDoc(2, runID, 0)))
	udp(3, 40000, 12201, zl(genDoc(3, runID, 0)))
	cs := chunks(gz(genDoc(4, runID, 0)), 20, 9)
	for i := len(cs) - 1; i >= 0; i-- { // reversed, with a duplicate
		udp(4+i, 40000, 12201, cs[i])
		if i == 1 {
			udp(4+i, 40000, 12201, cs[i])
		}
	}
	plain := chunks(genDoc(5, runID, 3000), 1400, 10)
	for i, c := range plain {
		udp(20+i, 40001, 12201, c)
	}
	// TCP caught midstream: the capture starts inside the document of seq 5.
	var stream []byte
	stream = append(stream, `half","_seq":5}`...)
	stream = append(stream, 0)
	for seq := 6; seq <= 8; seq++ {
		stream = append(stream, genDoc(seq, runID, 0)...)
		stream = append(stream, 0)
	}
	stream = append(stream, genDoc(9, runID, 0)...) // last one without NUL, ended by FIN
	tc := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 41000), Dst: netip.AddrPortFrom(srv, 12201), ISN: 100, T0: at(50)}
	frames = append(frames, tc.Frames(tc.Split(stream, 37), true, false)...)
	// HTTP: two pipelined POSTs, gzip and plain, and a GET.
	var hs []byte
	for seq := 10; seq <= 11; seq++ {
		body := genDoc(seq, runID, 0)
		enc := ""
		if seq == 10 {
			body, enc = gz(body), "Content-Encoding: gzip\r\n"
		}
		hs = append(hs, "POST /gelf HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"+enc+"Content-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"...)
		hs = append(hs, body...)
	}
	hs = append(hs, "GET /health HTTP/1.1\r\nHost: x\r\n\r\n"...)
	hc := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 42000), Dst: netip.AddrPortFrom(srv, 12202), ISN: 5, T0: at(80)}
	frames = append(frames, hc.Frames(hc.Split(hs, 100), false, false)...)
	rsp := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(srv, 12202), Dst: netip.AddrPortFrom(cli, 42000), ISN: 9, T0: at(81)}
	frames = append(frames, rsp.Frames(rsp.Split([]byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n"), 100), false, false)...)

	file := filepath.Join(t.TempDir(), "cap.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)

	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--target", r.Addr, "--pcap", file)
	mustOK(t, code, s, se)
	r.C.Wait(10, 3*time.Second)
	docs := r.C.Docs()
	// seq 5 appears once from UDP; its midstream TCP tail is skipped.
	want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	if err := gelftest.CheckSeqs(docs, runID, len(want)); err != nil {
		t.Fatalf("%v; source %v", err, s.Source)
	}
	src := s.Source
	for k, v := range map[string]uint64{"udp_datagrams": 3, "udp_chunked_messages": 2, "udp_chunks_duplicate": 1,
		"udp_not_gelf": 1, "compressed_gzip": 3, "compressed_zlib": 1, "tcp_streams_gelf": 1, "tcp_messages": 4,
		"http_messages": 2, "http_requests_skipped": 1, "tcp_midstream": 1, "messages_replayed": 11} {
		if src[k] != v {
			t.Errorf("%s = %d, want %d (source %v)", k, src[k], v, src)
		}
	}

	// The same capture over UDP (chunked, zlib) and HTTP, in original timing.
	u, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	code, s, se = run(t, "--transport", "udp", "--target", u.Addr, "--pcap", file, "--compress", "zlib", "--chunk-size", "200",
		"--pcap-timing", "original", "--pcap-speed", "10")
	mustOK(t, code, s, se)
	u.C.Wait(11, 3*time.Second)
	if err := gelftest.CheckSeqs(u.C.Docs(), runID, 11); err != nil {
		t.Fatal(err)
	}
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	code, s, se = run(t, "--transport", "http", "--target", h.Addr, "--pcap", file, "--pcap-port", "12201", "--loop", "--count", "20")
	mustOK(t, code, s, se)
	if h.C.N() != 20 || s.Source["pcap_passes"] < 2 {
		t.Fatalf("%d documents, passes %d", h.C.N(), s.Source["pcap_passes"])
	}
}

// A server that answers "Connection: close" (the G family without
// keep-alive, or with a per-connection cap): every message arrives exactly
// once, no connection counts as broken, and with --pipeline 1 nothing is
// written behind the close (review-gi #3: the slot was freed before the
// connection was marked closing, so the next request went to a socket the
// server no longer read, and broken pipes were counted as broken
// connections).
func TestHTTPServerClosingConnections(t *testing.T) {
	for _, c := range []struct {
		perConn, pipeline int
	}{{1, 1}, {1, 32}, {7, 1}, {7, 8}, {50, 32}} {
		h, err := gelftest.ListenHTTP(false, "", false)
		if err != nil {
			t.Fatal(err)
		}
		h.MaxPerConn = c.perConn
		run1 := fmt.Sprintf("close%d_%d", c.perConn, c.pipeline)
		code, s, se := run(t, "--transport", "http", "--target", h.Addr, "--count", "1000", "--concurrency", "4",
			"--pipeline", strconv.Itoa(c.pipeline), "--compress", "gzip", "--run", run1)
		h.Close()
		mustOK(t, code, s, se)
		if s.Outcomes["202"] != 1000 {
			t.Fatalf("%s: outcomes %v", run1, s.Outcomes)
		}
		if err := gelftest.CheckSeqs(h.C.Docs(), run1, 1000); err != nil {
			t.Fatalf("%s: %v", run1, err)
		}
		if s.Conns.Broken != 0 {
			t.Fatalf("%s: %d broken connections, events %v", run1, s.Conns.Broken, s.Events)
		}
		if c.pipeline == 1 && s.Events["resent on a new connection"] != 0 {
			t.Fatalf("%s: requests written behind Connection: close: events %v", run1, s.Events)
		}
		t.Logf("%s: connections %d, events %v", run1, s.Conns.Opened, s.Events)
	}
}
