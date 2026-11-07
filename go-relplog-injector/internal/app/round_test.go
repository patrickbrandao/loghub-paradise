package app

import (
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
)

// TestRejectTextInEvents: every non-200 answer is counted by its text
// (SPEC §8.2, pendência 6).
func TestRejectTextInEvents(t *testing.T) {
	s := newRELPServer(t, nil, false)
	s.set(func(s *relpServer) {
		s.rejectText = "500 message refused by the destination"
		s.reject = func(m string) bool { return strings.Contains(m, "seq=3 ") || strings.Contains(m, "seq=7 ") }
	})
	code, sum := runJSON(t, "--target", s.addr, "--count", "10", "--on-reject", "continue", "--window", "1")
	if code != ExitFailures || sum.Outcomes["rejected"] != 2 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	if n := sum.Events["rejected: 500 message refused by the destination"]; n != 2 {
		t.Fatalf("events %v", sum.Events)
	}
}

// TestRejectTextCardinality: 32 distinct texts at most, cut and cleaned.
func TestRejectTextCardinality(t *testing.T) {
	r := report.New()
	for i := range 40 {
		r.Rejected(fmt.Sprintf("500 reason %d", i))
	}
	r.Rejected("500 " + strings.Repeat("é", 100) + "\x01")
	r.Rejected("500 reason 0")
	ev := r.Summarize().Events
	if ev[report.RejectOther] != 9 || ev["rejected: 500 reason 0"] != 2 || len(ev) != 33 {
		t.Fatalf("%d events: other %d", len(ev), ev[report.RejectOther])
	}
	if got := report.CleanText("500 "+strings.Repeat("é", 100)+"\x01", report.MaxRejectText); len(got) > report.MaxRejectText || strings.ContainsRune(got, 0xFFFD) {
		t.Fatalf("clean %q", got)
	}
	if got := report.CleanText("a\x01b", 10); got != "a?b" {
		t.Fatalf("clean %q", got)
	}
}

// sqliteSink is a fake destination: the table of go-relplog-server's
// sink=sqlite (its SPEC §3.7), written with the JLH of what the fake RELP
// server acknowledged.
type sqliteSink struct {
	db   *sql.DB
	path string
	mu   sync.Mutex
}

func newSQLiteSink(t *testing.T) *sqliteSink {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS syslog_events (
  uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
  level INTEGER NOT NULL, sid INTEGER NOT NULL,
  ev_uts INTEGER NOT NULL, ev_sub INTEGER NOT NULL,
  facility INTEGER NOT NULL, severity INTEGER NOT NULL,
  format TEXT NOT NULL, transport TEXT NOT NULL,
  src_ip TEXT, hostname TEXT, appname TEXT, procid TEXT, msgid TEXT,
  env BLOB NOT NULL, nbytes INTEGER NOT NULL,
  PRIMARY KEY (uuid, step)
) STRICT`)
	if err != nil {
		t.Fatal(err)
	}
	return &sqliteSink{db: db, path: path}
}

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (k *sqliteSink) store(t *testing.T, msg string) {
	env, _ := json.Marshal(map[string]any{"uuid": newUUID(), "step": 0, "type": "syslog",
		"payload": map[string]any{"message": msg, "raw": msg, "x-transport": "relp"}})
	k.mu.Lock()
	defer k.mu.Unlock()
	_, err := k.db.Exec(`INSERT INTO syslog_events VALUES (?,0,'default',5,1,1,1,1,5,'rfc5424','tcp','127.0.0.1',NULL,NULL,NULL,NULL,?,?)`,
		newUUID(), env, len(env))
	if err != nil {
		t.Error(err)
	}
}

// TestVerifySQLite: the destination check against the sink=sqlite table:
// everything there (exit 0), one acknowledged message missing (exit 4, lost
// 1, its seq in the sample), one stored twice (exit 4, or 0 with
// --verify-allow-duplicates), and a destination that cannot be read (exit 5).
func TestVerifySQLite(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(k *sqliteSink, msg string) // what the "sink" does with an acked message
		args  []string
		code  int
		check func(v *report.Verify) bool
	}{
		{"all", func(k *sqliteSink, m string) { k.store(t, m) }, nil, ExitOK,
			func(v *report.Verify) bool { return v.OK && v.Found == 200 && v.Acked == 200 && v.Lost == 0 }},
		{"lost", func(k *sqliteSink, m string) {
			if !strings.Contains(m, "seq=77 ") {
				k.store(t, m)
			}
		}, nil, ExitFailures,
			func(v *report.Verify) bool {
				return !v.OK && v.Lost == 1 && len(v.LostSeqs) == 1 && v.LostSeqs[0] == "77"
			}},
		{"dup", func(k *sqliteSink, m string) {
			k.store(t, m)
			if strings.Contains(m, "seq=5 ") {
				k.store(t, m)
			}
		}, nil, ExitFailures,
			func(v *report.Verify) bool {
				return !v.OK && v.Duplicated == 1 && v.ExtraCopies == 1 && v.ExtraSameUUID == 0
			}},
		{"dup-allowed", func(k *sqliteSink, m string) {
			k.store(t, m)
			if strings.Contains(m, "seq=5 ") {
				k.store(t, m)
			}
		}, []string{"--verify-allow-duplicates"}, ExitOK,
			func(v *report.Verify) bool { return v.OK && v.Duplicated == 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newSQLiteSink(t)
			s := newRELPServer(t, nil, false)
			s.set(func(s *relpServer) { s.onAck = func(m string) { tc.store(k, m) } })
			args := append([]string{"--target", s.addr, "--count", "200", "--concurrency", "2", "--verify-sqlite", k.path, "--verify-timeout", "5s"}, tc.args...)
			code, sum := runJSON(t, args...)
			if code != tc.code || sum.Verify == nil || !tc.check(sum.Verify) {
				t.Fatalf("exit %d (want %d), verify %+v", code, tc.code, sum.Verify)
			}
		})
	}

	// A template with the markers is checked too; one without is refused.
	k := newSQLiteSink(t)
	s := newRELPServer(t, nil, false)
	s.set(func(s *relpServer) { s.onAck = func(m string) { k.store(t, m) } })
	code, sum := runJSON(t, "--target", s.addr, "--count", "30", "--template", "id {run}/{seq} at {ts} {rand:8}", "--verify-sqlite", k.path)
	if code != ExitOK || sum.Verify.Found != 30 {
		t.Fatalf("template: exit %d %+v", code, sum.Verify)
	}
	if code, _, errs := runMain(t, "--target", s.addr, "--template", "no markers {seq}", "--verify-sqlite", k.path); code != ExitUsage || !strings.Contains(errs, "{run}") {
		t.Fatalf("template without {run}: exit %d %s", code, errs)
	}

	// Unreadable: no such file.
	s2 := newRELPServer(t, nil, false)
	code, sum = runJSON(t, "--target", s2.addr, "--count", "3", "--verify-sqlite", filepath.Join(t.TempDir(), "none.db"), "--verify-timeout", "600ms")
	if code != ExitVerify || !sum.Verify.Unreadable || sum.Verify.Error == "" {
		t.Fatalf("unreadable: exit %d %+v", code, sum.Verify)
	}
	// The destination is probed before sending: nothing went out.
	if got, _ := s2.received(); len(got) != 0 || sum.Messages != 0 {
		t.Fatalf("unreadable destination, yet %d messages sent", len(got))
	}
}

// TestVerifyUsage: the refused combinations, and errors that never echo the
// URL (it may carry a password).
func TestVerifyUsage(t *testing.T) {
	for _, args := range [][]string{
		{"--verify-url", "nats://h:4222", "--verify-sqlite", "x.db"},
		{"--verify-timeout", "3s"},
		{"--verify-allow-duplicates"},
		{"--verify-url", "amqp://user:s3cr3t@h/"},
		{"--verify-url", "redis://user:s3cr3t@h:6379/0?bogus=s3cr3t"},
		{"--verify-url", "nats://user:s3cr3t@h:4222?ca=/x"},
		{"--verify-url", "nats://h:4222", "--verify-timeout", "0s"},
		{"--verify-url", "nats://h:4222", "--file", "/dev/null"},
		{"--pcap-flows"}, {"--pcap-dedup", "maybe"}, {"--verify-space", "x"},
	} {
		code, _, errs := runMain(t, args...)
		if code != ExitUsage || strings.Contains(errs, "s3cr3t") {
			t.Errorf("%v: exit %d, stderr %q", args, code, errs)
		}
	}
}

// sll2Frame frames an IP packet in Linux cooked v2 with an interface index.
func sll2Frame(ts time.Time, ifindex uint32, ip []byte) pcaptest.Frame {
	b := pcaptest.Link(pcaptest.SLL2, ip)
	binary.BigEndian.PutUint32(b[4:8], ifindex)
	return pcaptest.Frame{TS: ts, Data: b}
}

// TestPcapDedupAnyCopies: a "tcpdump -i any" capture where every datagram
// shows on the veth and, NAT-rewritten, on eth0, and one connection shows
// twice with different 4-tuples: each message is replayed once; a message
// the sender really sent twice is replayed twice; --pcap-dedup=false replays
// the copies (SPEC §6.8, pendência 2).
func TestPcapDedupAnyCopies(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	ct := netip.MustParseAddrPort("172.17.0.5:40000")
	srvIn := netip.MustParseAddrPort("172.17.0.2:514")
	host := netip.MustParseAddrPort("10.0.0.1:40000")
	srvOut := netip.MustParseAddrPort("10.0.0.2:1514")
	var frames []pcaptest.Frame
	udp := func(ms int, p string) {
		at := t0.Add(time.Duration(ms) * time.Millisecond)
		frames = append(frames,
			sll2Frame(at, 7, pcaptest.IP(ct.Addr(), srvIn.Addr(), 17, pcaptest.UDP(ct.Port(), srvIn.Port(), []byte(p)), 0)),
			sll2Frame(at.Add(30*time.Microsecond), 2, pcaptest.IP(host.Addr(), srvOut.Addr(), 17, pcaptest.UDP(host.Port(), srvOut.Port(), []byte(p)), 0)))
	}
	udp(0, "<13>u-1")
	udp(10, "<13>u-2")
	udp(11, "<13>u-2") // sent twice by the emitter
	in := pcaptest.Conn{Link: pcaptest.SLL2, Src: ct, Dst: srvIn, ISN: 777, T0: t0.Add(20 * time.Millisecond)}
	out := pcaptest.Conn{Link: pcaptest.SLL2, Src: host, Dst: srvOut, ISN: 777, T0: t0.Add(20*time.Millisecond + 40*time.Microsecond)}
	body := []byte("<13>t-1\n<13>t-2\n")
	for _, f := range in.Frames(in.Split(body, 8), false, false) {
		binary.BigEndian.PutUint32(f.Data[4:8], 7)
		frames = append(frames, f)
	}
	var tcpCopies uint64
	for _, f := range out.Frames(out.Split(body, 8), false, false) {
		binary.BigEndian.PutUint32(f.Data[4:8], 2)
		frames = append(frames, f)
		tcpCopies++
	}
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].TS.Before(frames[j].TS) })
	file := filepath.Join(t.TempDir(), "any.pcapng")
	os.WriteFile(file, pcaptest.NG(pcaptest.SLL2, frames), 0o600)

	s := newRELPServer(t, nil, false)
	code, sum := runJSON(t, "--target", s.addr, "--pcap", file)
	if code != ExitOK {
		t.Fatalf("exit %d %+v", code, sum)
	}
	_, got := s.received()
	sort.Strings(got)
	if want := "<13>t-1|<13>t-2|<13>u-1|<13>u-2|<13>u-2"; strings.Join(got, "|") != want {
		t.Fatalf("replayed %q, want %q", got, want)
	}
	if sum.Source["pcap_duplicates"] != 3+tcpCopies {
		t.Fatalf("source %v", sum.Source)
	}

	s2 := newRELPServer(t, nil, false)
	if code, sum = runJSON(t, "--target", s2.addr, "--pcap", file, "--pcap-dedup", "off"); code != ExitOK || sum.OK != 10 {
		t.Fatalf("without dedup: exit %d, %d messages", code, sum.OK)
	}
}

// TestPcapPerFlow: each original flow is replayed on its own session, in
// order, with its captured addresses in the PROXY header -- also with fewer
// session slots than flows (SPEC §6.9, pendência 3).
func TestPcapPerFlow(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	srv := netip.MustParseAddrPort("10.9.0.1:20514")
	var frames []pcaptest.Frame
	want := map[string][]string{} // captured source -> messages
	for i := range 3 {
		src := netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(10 + i)}), uint16(41000+i))
		var body strings.Builder
		for j := range 4 {
			m := fmt.Sprintf("<13>flow-%d msg-%d", i, j)
			fmt.Fprintf(&body, "%d %s", len(m), m)
			want[src.String()] = append(want[src.String()], m)
		}
		c := pcaptest.Conn{Link: 1, Src: src, Dst: srv, ISN: uint32(1000 * (i + 1)), T0: t0.Add(time.Duration(i) * time.Millisecond)}
		frames = append(frames, c.Frames(c.Split([]byte(body.String()), 9), false, i == 2)...)
	}
	udpSrc := netip.MustParseAddrPort("198.51.100.7:5140")
	for j := range 3 {
		m := fmt.Sprintf("<13>udp msg-%d", j)
		ip := pcaptest.IP(udpSrc.Addr(), netip.MustParseAddr("10.9.0.1"), 17, pcaptest.UDP(udpSrc.Port(), 514, []byte(m)), uint16(j))
		frames = append(frames, pcaptest.Frame{TS: t0.Add(time.Duration(j)*time.Millisecond + 500*time.Microsecond), Data: pcaptest.Link(1, ip)})
		want[udpSrc.String()] = append(want[udpSrc.String()], m)
	}
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].TS.Before(frames[j].TS) })
	file := filepath.Join(t.TempDir(), "flows.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)

	for _, conc := range []string{"", "1"} {
		s := newRELPServer(t, nil, true)
		args := []string{"--target", s.addr, "--pcap", file, "--pcap-flows", "--proxy-protocol", "v1"}
		if conc != "" {
			args = append(args, "--concurrency", conc)
		}
		code, sum := runJSON(t, args...)
		if code != ExitOK || sum.OK != 15 || sum.Info["flows"] != "4" {
			t.Fatalf("concurrency %q: exit %d %+v", conc, code, sum)
		}
		s.mu.Lock()
		got := map[string][]string{}
		for n, msgs := range s.sessMsgs {
			// "PROXY TCP4 192.0.2.10 10.9.0.1 41000 20514\r\n"
			f := strings.Fields(s.sessHead[n])
			if len(f) != 6 || f[3] != "10.9.0.1" {
				t.Fatalf("session %d header %q", n, s.sessHead[n])
			}
			src := netip.MustParseAddrPort(f[2] + ":" + f[4]).String()
			got[src] = append(got[src], msgs...)
			for _, m := range msgs {
				if !strings.Contains(m, map[bool]string{true: "udp", false: "flow-"}[strings.HasPrefix(src, "198.")]) {
					t.Fatalf("session of %s carried %q", src, m)
				}
			}
		}
		s.mu.Unlock()
		for src, w := range want {
			if strings.Join(got[src], "|") != strings.Join(w, "|") {
				t.Fatalf("concurrency %q: flow %s replayed %q, want %q", conc, src, got[src], w)
			}
		}
	}
}
