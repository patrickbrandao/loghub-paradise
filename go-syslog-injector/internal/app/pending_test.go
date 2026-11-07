package app

// Tests of the 2026-11-07 round (SPEC §9: the known pending items).

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap/pcaptest"
)

// withCopies returns frames with, after each one, the copy tcpdump -i any
// records on a second interface (SLL2 interface index 2) 20 µs later, its
// destination rewritten by DNAT (another address and port) -- same IP ID,
// same source port.
func withCopies(t *testing.T, frames []pcaptest.Frame, natDst netip.AddrPort) []pcaptest.Frame {
	t.Helper()
	var out []pcaptest.Frame
	for _, f := range frames {
		out = append(out, f)
		ip := append([]byte(nil), f.Data[20:]...) // strip the SLL2 header
		copy(ip[16:20], natDst.Addr().AsSlice())
		ip[22], ip[23] = byte(natDst.Port()>>8), byte(natDst.Port())
		// Checksums are not checked by the decoder (nor kept by NAT here).
		cp := pcaptest.Link(pcaptest.SLL2, ip)
		cp[7] = 2 // another interface: a copy only counts from another vantage
		out = append(out, pcaptest.Frame{TS: f.TS.Add(20 * time.Microsecond), Data: cp})
	}
	return out
}

// TestPcapDedupAnyCopies (SPEC §6.9, pendência 2): in a -i any capture (SLL2)
// every packet appears twice, the copy with the destination rewritten by NAT.
// The replay sends each message once; a real repeat (same text, another IP
// ID) is kept, and so is one with the SAME IP ID (Linux sends 0 with DF):
// each original absorbs at most one copy per other interface (2026-11-07,
// the rule of RI and GI); --pcap-dedup off sends the copies too.
func TestPcapDedupAnyCopies(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli := netip.MustParseAddr("10.1.1.1")
	srv := netip.MustParseAddrPort("172.17.0.2:1514")
	nat := netip.MustParseAddrPort("10.1.1.2:514")
	var frames []pcaptest.Frame
	udp := func(ms int, id uint16, p string) {
		ip := pcaptest.IP(cli, srv.Addr(), 17, pcaptest.UDP(5000, srv.Port(), []byte(p)), id)
		frames = append(frames, pcaptest.Frame{TS: t0.Add(time.Duration(ms) * time.Millisecond), Data: pcaptest.Link(pcaptest.SLL2, ip)})
	}
	udp(0, 100, "<13>udp-a")
	udp(5, 101, "<13>same")
	udp(6, 102, "<13>same") // a real repeat: another IP ID
	udp(7, 0, "<13>twice")
	udp(8, 0, "<13>twice") // a real repeat with the same IP ID
	c := pcaptest.Conn{Link: pcaptest.SLL2, Src: netip.AddrPortFrom(cli, 40001), Dst: srv, ISN: 7, T0: t0.Add(10 * time.Millisecond)}
	frames = append(frames, c.Frames(c.Split([]byte("9 <13>tcp-19 <13>tcp-2"), 6), false, false)...)
	frames = withCopies(t, frames, nat)
	file := filepath.Join(t.TempDir(), "any.pcap")
	os.WriteFile(file, pcaptest.Classic(pcaptest.SLL2, frames), 0o600)
	want := "<13>udp-a|<13>same|<13>same|<13>twice|<13>twice|<13>tcp-1|<13>tcp-2"

	addr, s := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--pcap", file)
	if code != ExitOK {
		t.Fatalf("exit %d %+v", code, sum)
	}
	if got := strings.Join(waitFor(t, s, 7), "|"); got != want || sum.OK != 7 {
		t.Fatalf("replayed %q (ok %d), want %q", got, sum.OK, want)
	}
	if n := sum.Source["pcap_any_duplicates"]; n != uint64(len(frames)/2) {
		t.Fatalf("pcap_any_duplicates %d, want %d", n, len(frames)/2)
	}

	addr2, s2 := streamServer(t, false, false, nil)
	if code, sum := runJSON(t, "--target", addr2, "--pcap", file, "--pcap-dedup", "off"); code != ExitOK || sum.OK != 14 {
		t.Fatalf("dedup off: exit %d ok %d, want the 14 copies", code, sum.OK)
	}
	waitFor(t, s2, 14)
}

// TestPcapDedupAfterPortFilter (SPEC §6.9): the copy the --pcap-port filter
// refuses must not shadow the one it keeps. Here the capture sees each packet
// first before DNAT (port 38514) and then after it (514); with --pcap-port
// 514 the second copy is the one replayed. Asked before the filter, the
// refused first copy became the original and the kept one was dropped.
func TestPcapDedupAfterPortFilter(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli := netip.MustParseAddr("10.1.1.1")
	ext := netip.MustParseAddrPort("192.0.2.10:38514")
	nat := netip.MustParseAddrPort("172.17.0.2:514")
	var frames []pcaptest.Frame
	ip := pcaptest.IP(cli, ext.Addr(), 17, pcaptest.UDP(5000, ext.Port(), []byte("<13>udp")), 100)
	frames = append(frames, pcaptest.Frame{TS: t0, Data: pcaptest.Link(pcaptest.SLL2, ip)})
	c := pcaptest.Conn{Link: pcaptest.SLL2, Src: netip.AddrPortFrom(cli, 40001), Dst: ext, ISN: 7, T0: t0.Add(10 * time.Millisecond)}
	frames = append(frames, c.Frames(c.Split([]byte("7 <13>tcp"), 100), false, false)...)
	frames = withCopies(t, frames, nat)
	file := filepath.Join(t.TempDir(), "dnat.pcap")
	os.WriteFile(file, pcaptest.Classic(pcaptest.SLL2, frames), 0o600)

	addr, s := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--pcap", file, "--pcap-port", "514")
	if code != ExitOK {
		t.Fatalf("exit %d %+v", code, sum)
	}
	if got := strings.Join(waitFor(t, s, 2), "|"); got != "<13>udp|<13>tcp" || sum.OK != 2 {
		t.Fatalf("replayed %q (ok %d)", got, sum.OK)
	}
}

// flowServer records, per connection, its PROXY header and its messages.
type flowServer struct {
	mu     sync.Mutex
	conns  map[string][]string // PROXY v1 line -> messages
	n      int
	closed map[string]time.Time // PROXY v1 line -> first close
}

func newFlowServer(t *testing.T) (string, *flowServer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	fs := &flowServer{conns: map[string][]string{}, closed: map[string]time.Time{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				h := strings.TrimSpace(readProxy(r))
				fs.mu.Lock()
				fs.n++
				fs.mu.Unlock()
				var s sink
				readOctet(r, &s)
				fs.mu.Lock()
				fs.conns[h] = append(fs.conns[h], s.all()...)
				if _, ok := fs.closed[h]; !ok {
					fs.closed[h] = time.Now()
				}
				fs.mu.Unlock()
			}(c)
		}
	}()
	return ln.Addr().String(), fs
}

// TestPcapFlowsProxy (SPEC §6.8, pendência 3): with --pcap-flows each
// captured flow gets its own connection, closed when the flow ends, whose
// PROXY header announces the captured origin; without it, one connection
// carries everything.
func TestPcapFlowsProxy(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	srv := netip.MustParseAddrPort("10.1.1.2:1514")
	a := pcaptest.Conn{Link: 1, Src: netip.MustParseAddrPort("10.1.1.1:40001"), Dst: srv, ISN: 7, T0: t0}
	b := pcaptest.Conn{Link: 1, Src: netip.MustParseAddrPort("10.1.1.9:40002"), Dst: srv, ISN: 900, T0: t0.Add(time.Millisecond)}
	frames := append(a.Frames(a.Split([]byte("6 <13>a16 <13>a2"), 4), false, false), b.Frames(b.Split([]byte("6 <13>b16 <13>b2"), 4), false, false)...)
	// The same 4-tuple again after a's FIN: a new connection, a new flow.
	a2 := a
	a2.ISN, a2.T0 = 5000, t0.Add(time.Second)
	frames = append(frames, a2.Frames(a2.Split([]byte("6 <13>a3"), 100), false, false)...)
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].TS.Before(frames[j].TS) })
	file := filepath.Join(t.TempDir(), "flows.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)

	addr, fs := newFlowServer(t)
	// Original timing at 2x: a2 comes 500 ms after a ended, and the
	// connection of a must be closed by then, not at the end of the run.
	code, sum := runJSON(t, "--target", addr, "--pcap", file, "--pcap-flows", "--proxy-protocol", "v1", "--concurrency", "2",
		"--pcap-timing", "original", "--pcap-speed", "2")
	end := time.Now()
	if code != ExitOK || sum.OK != 5 || sum.Conns.Opened != 3 || sum.Source["pcap_flows"] != 3 {
		t.Fatalf("exit %d ok %d opened %d flows %d", code, sum.OK, sum.Conns.Opened, sum.Source["pcap_flows"])
	}
	deadline := time.Now().Add(5 * time.Second)
	want := map[string]string{
		"PROXY TCP4 10.1.1.1 10.1.1.2 40001 1514": "<13>a1|<13>a2|<13>a3",
		"PROXY TCP4 10.1.1.9 10.1.1.2 40002 1514": "<13>b1|<13>b2",
	}
	for {
		fs.mu.Lock()
		got := map[string]string{}
		for h, m := range fs.conns {
			got[h] = strings.Join(m, "|")
		}
		n := fs.n
		fs.mu.Unlock()
		if fmt.Sprint(got) == fmt.Sprint(want) && n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("per connection %v (%d connections), want %v over 3", got, n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	fs.mu.Lock()
	aClosed := fs.closed["PROXY TCP4 10.1.1.1 10.1.1.2 40001 1514"]
	fs.mu.Unlock()
	if end.Sub(aClosed) < 250*time.Millisecond {
		t.Fatalf("the connection of flow a closed %v before the end of the run: not at its FIN", end.Sub(aClosed))
	}

	addr2, fs2 := newFlowServer(t)
	if code, sum := runJSON(t, "--target", addr2, "--pcap", file, "--proxy-protocol", "v1"); code != ExitOK || sum.Conns.Opened != 1 {
		t.Fatalf("without --pcap-flows: exit %d opened %d", code, sum.Conns.Opened)
	}
	_ = fs2
}

// eventsDB creates a syslog_events table as sink=sqlite does (the columns
// the check reads) and returns an insert function.
func eventsDB(t *testing.T) (string, func(space, run string, seq int)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE syslog_events (uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
		ev_uts INTEGER NOT NULL, env BLOB NOT NULL, PRIMARY KEY (uuid, step)) STRICT`); err != nil {
		t.Fatal(err)
	}
	n := 0
	return path, func(space, run string, seq int) {
		n++
		body := fmt.Sprintf("seq=%d run=%s injector=go-syslog-injector", seq, run)
		env := fmt.Sprintf(`{"uuid":"u%d","space":%q,"type":"syslog","payload":{"message":%q,"raw":"<13>1 - h a 1 - - %s","size":1}}`, n, space, body, body)
		if _, err := db.Exec(`INSERT INTO syslog_events VALUES (?, 0, ?, ?, ?)`, fmt.Sprint("u", n), space, time.Now().Unix(), []byte(env)); err != nil {
			t.Fatal(err)
		}
	}
}

// TestVerifySQLite (SPEC §8.5, pendência 1): the check reads the events.db,
// counts the run's seqs, and the exit code follows: one lost and one
// duplicated is 1; everything once is 0, without waiting the timeout.
func TestVerifySQLite(t *testing.T) {
	path, insert := eventsDB(t)
	for seq := 1; seq <= 10; seq++ {
		if seq != 4 {
			insert("default", "vr1", seq)
		}
	}
	insert("default", "vr1", 7)   // a duplicate: another uuid
	insert("default", "other", 4) // another run
	insert("elsewhere", "vr1", 4) // another space
	insert("default", "vr1", 99)  // a seq that was never sent

	addr, _ := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--count", "10", "--run", "vr1", "--verify-sqlite", path, "--verify-timeout", "600ms")
	v := sum.Verify
	if code != ExitFailures || v == nil {
		t.Fatalf("exit %d verify %+v", code, v)
	}
	if v.Expected != 10 || v.Found != 9 || v.Lost != 1 || v.LostSeqs != "4" || v.Duplicated != 1 || v.Unexpected != 1 || v.Complete || v.Error != "" {
		t.Fatalf("verify %+v", v)
	}
	if v.WaitedSec < 0.5 {
		t.Fatalf("gave up before --verify-timeout: %v", v.WaitedSec)
	}

	path2, insert2 := eventsDB(t)
	for seq := 1; seq <= 10; seq++ {
		insert2("default", "vr2", seq)
	}
	start := time.Now()
	code, sum = runJSON(t, "--target", addr, "--count", "10", "--run", "vr2", "--verify-sqlite", path2, "--verify-timeout", "20s")
	if code != ExitOK || sum.Verify == nil || !sum.Verify.Complete || sum.Verify.Found != 10 || time.Since(start) > 10*time.Second {
		t.Fatalf("complete: exit %d %+v in %v", code, sum.Verify, time.Since(start))
	}
	if sum.Info["verify"] != "sqlite "+path2 {
		t.Fatalf("info.verify %q", sum.Info["verify"])
	}
}

// TestVerifyUnreachable: a destination that cannot be read fails at once with
// code 4, before anything is sent, and the URL (its password) is not printed.
func TestVerifyUnreachable(t *testing.T) {
	addr, s := streamServer(t, false, false, nil)
	for _, u := range []string{"redis://u:sekret@127.0.0.1:1/0", "valkey://u:sekret@127.0.0.1:1", "postgres://u:sekret@127.0.0.1:1/db", "nats://u:sekret@127.0.0.1:1"} {
		code, out, errs := runMain(t, "--target", addr, "--count", "5", "--verify-url", u, "--timeout", "1s")
		if code != ExitVerify || strings.Contains(out+errs, "sekret") {
			t.Fatalf("%s: exit %d, stdout %q stderr %q", u[:6], code, out, errs)
		}
	}
	code, _, errs := runMain(t, "--target", addr, "--count", "5", "--verify-sqlite", filepath.Join(t.TempDir(), "missing.db"))
	if code != ExitVerify {
		t.Fatalf("missing sqlite: exit %d %s", code, errs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns != 0 {
		t.Fatalf("%d connections to the target: something was sent", s.conns)
	}
}

// TestPendingUsageErrors: the refused combinations of the round (SPEC §3.2).
// A URL with a password is never echoed.
func TestPendingUsageErrors(t *testing.T) {
	f := filepath.Join(t.TempDir(), "lines.txt")
	os.WriteFile(f, []byte("<13>x\n"), 0o600)
	for _, args := range [][]string{
		{"--verify-url", "amqp://u:sekret@h/"},
		{"--verify-url", "sekret-no-scheme"},
		{"--verify-url", "redis://h", "--verify-sqlite", "x.db"},
		{"--verify-url", "redis://h", "--file", f},
		{"--verify-url", "redis://h", "--template", "seq={seq} only"},
		{"--verify-url", "redis://h", "--run", "a b"},
		{"--verify-url", "redis://h", "--verify-timeout", "0s"},
		{"--verify-url", "postgres://h/db", "--verify-key", "a.b.c"},
		{"--verify-sqlite", "x.db", "--verify-key", "no-dash"},
		{"--verify-url", "redis://h", "--verify-space", "bad space"},
		{"--pcap-flows"},
		{"--pcap-dedup", "on"},
	} {
		code, out, errs := runMain(t, args...)
		if code != ExitUsage || strings.Contains(out+errs, "sekret") {
			t.Errorf("%v: exit %d, want %d (stderr %q)", args, code, ExitUsage, errs)
		}
	}
}
