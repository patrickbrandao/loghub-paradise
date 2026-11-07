package app

// Tests of the round of 2026-11-07 (SPEC §9 items 1-7).

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelftest"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
)

// --http-chunked sends every body with Transfer-Encoding: chunked, and the
// server takes each document (SPEC §4.3, item 4).
func TestHTTPChunked(t *testing.T) {
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	code, s, se := run(t, "--transport", "http", "--target", h.Addr, "--count", "500", "--pipeline", "4",
		"--compress", "gzip", "--http-chunked", "7", "--payload-size", "300", "--run", "chunk1")
	mustOK(t, code, s, se)
	if err := gelftest.CheckSeqs(h.C.Docs(), "chunk1", 500); err != nil {
		t.Fatal(err)
	}
	if n := h.C.Counter("chunked"); n != 500 {
		t.Fatalf("%d chunked requests, want 500", n)
	}
	code, s, se = run(t, "--transport", "http", "--target", h.Addr, "--count", "10", "--run", "chunk0")
	mustOK(t, code, s, se)
	if n := h.C.Counter("chunked"); n != 500 {
		t.Fatalf("Content-Length run sent chunked: %d", n)
	}
}

// Against a server that closes every connection at its first answer, the
// connections opened after two of them use a pipeline of 1: what is written
// behind the close (and given back) stays bounded instead of ~pipeline-1
// per message (SPEC §4.3, item 7).
func TestHTTPAdaptsToClosingServer(t *testing.T) {
	h, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	h.MaxPerConn = 1
	code, s, se := run(t, "--transport", "http", "--target", h.Addr, "--count", "300", "--pipeline", "32", "--run", "adapt1")
	mustOK(t, code, s, se)
	if err := gelftest.CheckSeqs(h.C.Docs(), "adapt1", 300); err != nil {
		t.Fatal(err)
	}
	if s.Events["adapted: server closes every connection, pipeline 1"] != 1 {
		t.Fatalf("not adapted: events %v", s.Events)
	}
	// Two connections of 32 in flight before the adaptation: at most 2*31
	// requests went behind a close.
	if r := s.Events["resent on a new connection"]; r > 62 {
		t.Fatalf("%d requests resent after the adaptation should have stopped them; events %v", r, s.Events)
	}
	// A keep-alive server is never adapted.
	h2, err := gelftest.ListenHTTP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	h2.MaxPerConn = 50
	code, s, se = run(t, "--transport", "http", "--target", h2.Addr, "--count", "300", "--pipeline", "8", "--run", "adapt2")
	mustOK(t, code, s, se)
	for k := range s.Events {
		if strings.HasPrefix(k, "adapted") {
			t.Fatalf("keep-alive server adapted: %v", s.Events)
		}
	}
}

// flowsCapture has three TCP connections from three emitters (one IPv6),
// interleaved, and the UDP datagrams of two more pairs: five flows. Each
// document's seq is unique; per flow they ascend.
func flowsCapture(t *testing.T) (string, map[string][]int) {
	t.Helper()
	t0 := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	srv4 := netip.MustParseAddr("10.9.0.1")
	srv6 := netip.MustParseAddr("2001:db8::1")
	type tcpf struct {
		src, dst netip.AddrPort
		seqs     []int
	}
	flows := []tcpf{
		{netip.MustParseAddrPort("10.1.0.1:40001"), netip.AddrPortFrom(srv4, 12201), []int{1, 4, 7}},
		{netip.MustParseAddrPort("10.1.0.2:40002"), netip.AddrPortFrom(srv4, 12201), []int{2, 5, 8}},
		{netip.MustParseAddrPort("[2001:db8::7]:40003"), netip.AddrPortFrom(srv6, 12201), []int{3, 6, 9}},
	}
	want := map[string][]int{}
	var frames []pcaptest.Frame
	for i, f := range flows {
		var stream []byte
		for _, seq := range f.seqs {
			stream = append(stream, genDoc(seq, "flows", 0)...)
			stream = append(stream, 0)
		}
		c := pcaptest.Conn{Link: 1, Src: f.src, Dst: f.dst, ISN: uint32(1000 * (i + 1)), T0: t0.Add(time.Duration(i) * time.Millisecond)}
		frames = append(frames, c.Frames(c.Split(stream, 60), false, false)...)
		want[f.src.String()] = f.seqs
	}
	udp := func(src netip.AddrPort, ms, seq int) {
		ip := pcaptest.IP(src.Addr(), srv4, 17, pcaptest.UDP(src.Port(), 12201, genDoc(seq, "flows", 0)), uint16(seq))
		frames = append(frames, pcaptest.Frame{TS: t0.Add(time.Duration(ms) * time.Millisecond), Data: pcaptest.Link(1, ip)})
		want[src.String()] = append(want[src.String()], seq)
	}
	u1, u2 := netip.MustParseAddrPort("10.2.0.1:50001"), netip.MustParseAddrPort("10.2.0.2:50002")
	udp(u1, 200, 10)
	udp(u2, 201, 11)
	udp(u1, 202, 12)
	udp(u2, 203, 13)
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].TS.Before(frames[j].TS) })
	file := filepath.Join(t.TempDir(), "flows.pcap")
	if err := os.WriteFile(file, pcaptest.Classic(1, frames), 0o600); err != nil {
		t.Fatal(err)
	}
	return file, want
}

// --pcap-flows: one connection per captured flow, each announcing the
// captured source in its PROXY header, the flow's documents in capture
// order on it (SPEC §6.8, item 3).
func TestPcapFlows(t *testing.T) {
	file, want := flowsCapture(t)
	r, err := gelftest.ListenTCP(false, "", true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--target", r.Addr, "--pcap", file, "--pcap-flows", "--proxy-protocol", "v2", "--concurrency", "16")
	mustOK(t, code, s, se)
	r.C.Wait(13, 3*time.Second)
	if r.Conns() != 5 || s.Conns.Opened != 5 {
		t.Fatalf("%d connections (report %d), want one per flow (5); events %v", r.Conns(), s.Conns.Opened, s.Events)
	}
	got := r.C.Proxied()
	sort.Strings(got)
	var srcs []string
	for k := range want {
		srcs = append(srcs, k)
	}
	sort.Strings(srcs)
	if strings.Join(got, ",") != strings.Join(srcs, ",") {
		t.Fatalf("PROXY sources %v, want the captured %v", got, srcs)
	}
	if err := gelftest.CheckSeqs(r.C.Docs(), "flows", 13); err != nil {
		t.Fatal(err)
	}
	// Without the flag the capture shares the --concurrency connections.
	r2, err := gelftest.ListenTCP(false, "", true)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	code, s, se = run(t, "--target", r2.Addr, "--pcap", file, "--proxy-protocol", "v2")
	mustOK(t, code, s, se)
	if r2.Conns() != 1 {
		t.Fatalf("%d connections without --pcap-flows", r2.Conns())
	}

	// Each flow's documents keep their capture order on its connection.
	order := map[string][]int{}
	for _, d := range r.C.Docs() {
		var g gelftest.Gen
		json.Unmarshal(d, &g)
		for src, seqs := range want {
			for _, q := range seqs {
				if uint64(q) == g.Seq {
					order[src] = append(order[src], q)
				}
			}
		}
	}
	for src, seqs := range want {
		if fmt.Sprint(order[src]) != fmt.Sprint(seqs) {
			t.Fatalf("flow %s arrived %v, want %v", src, order[src], seqs)
		}
	}
}

// The end of a captured connection closes its connection: two connections
// on the same 4-tuple (port reuse) are two connections on the target too.
func TestPcapFlowsEnd(t *testing.T) {
	t0 := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	src, dst := netip.MustParseAddrPort("10.1.0.1:40001"), netip.MustParseAddrPort("10.9.0.1:12201")
	var frames []pcaptest.Frame
	for i := range 2 {
		var stream []byte
		for seq := 1 + 3*i; seq <= 3+3*i; seq++ {
			stream = append(stream, genDoc(seq, "fend", 0)...)
			stream = append(stream, 0)
		}
		c := pcaptest.Conn{Link: 1, Src: src, Dst: dst, ISN: uint32(5000 * (i + 1)), T0: t0.Add(time.Duration(i) * time.Second)}
		frames = append(frames, c.Frames(c.Split(stream, 80), false, false)...)
	}
	file := filepath.Join(t.TempDir(), "reuse.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)
	r, err := gelftest.ListenTCP(false, "", true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--target", r.Addr, "--pcap", file, "--pcap-flows", "--proxy-protocol", "v2", "--concurrency", "4")
	mustOK(t, code, s, se)
	r.C.Wait(6, 3*time.Second)
	if r.Conns() != 2 {
		t.Fatalf("%d connections for two captured connections; events %v", r.Conns(), s.Events)
	}
	if err := gelftest.CheckSeqs(r.C.Docs(), "fend", 6); err != nil {
		t.Fatal(err)
	}
}

// Past --concurrency × 256 open flows the least recently used closes, and a
// later document of it reopens: 260 UDP pairs, each sending twice, at
// --concurrency 1.
func TestPcapFlowsEviction(t *testing.T) {
	t0 := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	dst := netip.MustParseAddr("10.9.0.1")
	var frames []pcaptest.Frame
	seq := 0
	for pass := range 2 {
		for i := range 260 {
			seq++
			src := netip.AddrFrom4([4]byte{10, 3, byte(i >> 8), byte(i)})
			ip := pcaptest.IP(src, dst, 17, pcaptest.UDP(40000, 12201, genDoc(seq, "evict", 0)), uint16(seq))
			frames = append(frames, pcaptest.Frame{TS: t0.Add(time.Duration(pass*1000+i) * time.Millisecond), Data: pcaptest.Link(1, ip)})
		}
	}
	file := filepath.Join(t.TempDir(), "evict.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)
	u, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	code, s, se := run(t, "--transport", "udp", "--target", u.Addr, "--pcap", file, "--pcap-flows", "--rate", "5000")
	mustOK(t, code, s, se)
	u.C.Wait(520, 3*time.Second)
	if s.Events["flow closed at --concurrency"] == 0 || s.Conns.Opened <= 260 {
		t.Fatalf("no eviction past 256 flows: %d sockets, events %v", s.Conns.Opened, s.Events)
	}
	if err := gelftest.CheckSeqs(u.C.Docs(), "evict", 520); err != nil {
		t.Fatal(err)
	}
}

// UDP flows: one socket per captured pair (no PROXY in UDP).
func TestPcapFlowsUDP(t *testing.T) {
	file, _ := flowsCapture(t)
	u, err := gelftest.ListenUDP("")
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	code, s, se := run(t, "--transport", "udp", "--target", u.Addr, "--pcap", file, "--pcap-flows", "--concurrency", "8")
	mustOK(t, code, s, se)
	u.C.Wait(13, 3*time.Second)
	if s.Conns.Opened != 5 {
		t.Fatalf("%d sockets, want 5", s.Conns.Opened)
	}
	if err := gelftest.CheckSeqs(u.C.Docs(), "flows", 13); err != nil {
		t.Fatal(err)
	}
}

// The usage errors of the round (SPEC §3.2).
func TestUsageRound(t *testing.T) {
	db := filepath.Join(t.TempDir(), "e.db")
	cases := [][]string{
		{"--http-chunked", "10"},
		{"--transport", "http", "--http-chunked", "-1"},
		{"--transport", "http", "--http-chunked", "16777217"},
		{"--pcap-flows"},
		{"--verify-url", "nats://h", "--verify-sqlite", db},
		{"--verify-url", "amqp://h"},
		{"--verify-url", "nats://h?nope=1"},
		{"--verify-url", "redis://"},
		{"--verify-timeout", "5s"},
		{"--verify-tls-ca", "x"},
		{"--verify-sqlite", db, "--verify-timeout", "0s"},
		{"--verify-sqlite", db, "--file", "x"},
		{"--verify-sqlite", db, "--template", `{"short_message":"{seq}"}`},
		{"--verify-sqlite", db, "--verify-space", "a b"},
		{"--verify-sqlite", db, "--verify-key", "k"},
		{"--verify-url", "postgres://h/db", "--verify-key", "k"},
		// Review of 2026-11-07: a malformed element of a NATS list, and
		// --verify-tls-ca with PostgreSQL or SQLite.
		{"--verify-url", "nats://h:1,nats://u:hunter2 @h2:1"},
		{"--verify-url", "nats://h:1,http://h2"},
		{"--verify-url", "postgres://h/db", "--verify-tls-ca", "x"},
		{"--verify-sqlite", db, "--verify-tls-ca", "x"},
		{"--verify-space", "x"},
		{"--pcap-dedup", "on"},
	}
	for _, c := range cases {
		if code, _, _ := run(t, c...); code != ExitUsage {
			t.Errorf("%v: exit %d, want 2", c, code)
		}
	}
	file, _ := flowsCapture(t)
	if code, _, _ := run(t, "--pcap", file, "--pcap-flows", "--proxy-protocol", "v1", "--proxy-src", "1.2.3.4:5"); code != ExitUsage {
		t.Errorf("--pcap-flows with --proxy-src: exit %d", code)
	}
	if code, _, _ := run(t, "--pcap", file, "--verify-sqlite", db); code != ExitUsage {
		t.Errorf("--pcap with verify: exit %d", code)
	}
	if code, _, _ := run(t, "--pcap", file, "--pcap-dedup", "maybe"); code != ExitUsage {
		t.Errorf("--pcap-dedup maybe: exit %d", code)
	}
	// The URL may hold a password: never echoed.
	if code, _, se := run(t, "--verify-url", "amqp://u:hunter2@h"); code != ExitUsage || strings.Contains(se, "hunter2") {
		t.Errorf("exit %d, stderr %q", code, se)
	}
}

// eventsDB creates the gelf_events of go-gelf-server (its SPEC §3.7).
func eventsDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE gelf_events (uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
  level INTEGER NOT NULL, sid INTEGER NOT NULL, ev_uts INTEGER NOT NULL, ev_sub INTEGER NOT NULL,
  format TEXT NOT NULL, transport TEXT NOT NULL, src_ip TEXT, hostname TEXT,
  env BLOB NOT NULL, nbytes INTEGER NOT NULL, PRIMARY KEY (uuid, step)) STRICT`); err != nil {
		t.Fatal(err)
	}
	return path, db
}

// The exit codes of the check (SPEC §3.3, §10.3), as go-syslog-injector's:
// a destination that cannot be opened stops the run before the first send
// (4); one that lacks what was sent is 1.
func TestVerifyExitCodes(t *testing.T) {
	r, err := gelftest.ListenTCP(false, "", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	code, s, se := run(t, "--target", r.Addr, "--count", "50", "--verify-sqlite", filepath.Join(t.TempDir(), "none.db"))
	if code != ExitVerifyError || r.C.N() != 0 || s.Messages != 0 {
		t.Fatalf("missing destination: exit %d, %d sent, stderr %s", code, r.C.N(), se)
	}
	path, db := eventsDB(t)
	code, s, se = run(t, "--target", r.Addr, "--count", "50", "--run", "vx1", "--verify-sqlite", path, "--verify-timeout", "500ms")
	if code != ExitFailures || s.Verify == nil || s.Verify.Lost != 50 || s.Verify.Complete {
		t.Fatalf("nothing at the destination: exit %d verify %+v stderr %s", code, s.Verify, se)
	}
	// A --run already at the destination (PostgreSQL, SQLite) is refused
	// before anything is sent (review of 2026-11-07): the count would mix
	// the two runs.
	insert := func(run string, seq, i int) {
		u := fmt.Sprintf("ffffffff-ffff-7000-8000-%012x", i) // above the window's floor
		env := fmt.Sprintf(`{"type":"gelf","payload":{"fields":{"seq":%d,"run":"%s"}}}`, seq, run)
		if _, err := db.Exec(`INSERT INTO gelf_events VALUES (?,0,'default',6,1,1,0,'gelf','tcp',NULL,NULL,?,1)`, u, []byte(env)); err != nil {
			t.Error(err)
		}
	}
	insert("vx2", 1, 1_000_000)
	before := r.C.N()
	code, s, se = run(t, "--target", r.Addr, "--count", "50", "--run", "vx2", "--verify-sqlite", path, "--verify-timeout", "3s")
	if code != ExitUsage || r.C.N() != before || !strings.Contains(se, "--run already present") {
		t.Fatalf("reused --run: exit %d, %d sent, stderr %s", code, r.C.N()-before, se)
	}
	// What the receiver gets, written the way the server writes it, after
	// the check: complete.
	go func() {
		r.C.Wait(before+50, 5*time.Second)
		for seq := 1; seq <= 50; seq++ {
			insert("vx3", seq, seq)
		}
	}()
	code, s, se = run(t, "--target", r.Addr, "--count", "50", "--run", "vx3", "--verify-sqlite", path, "--verify-timeout", "3s")
	if code != ExitOK || s.Verify == nil || !s.Verify.Complete || s.Verify.Found != 50 {
		t.Fatalf("destination complete: exit %d verify %+v stderr %s", code, s.Verify, se)
	}
}
