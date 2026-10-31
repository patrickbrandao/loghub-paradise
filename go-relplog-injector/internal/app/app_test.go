package app

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
)

func readProxy(r *bufio.Reader) string {
	b, _ := r.Peek(12)
	if string(b) == "\r\n\r\n\x00\r\nQUIT\n" {
		h := make([]byte, 16)
		io.ReadFull(r, h)
		rest := make([]byte, int(h[14])<<8|int(h[15]))
		io.ReadFull(r, rest)
		return fmt.Sprintf("v2 %x", append(h[12:], rest...))
	}
	l, _ := r.ReadString('\n')
	return l
}

type prefixConn struct {
	net.Conn
	r *bufio.Reader
}

func (p *prefixConn) Read(b []byte) (int, error) { return p.r.Read(b) }

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

var seqRE = regexp.MustCompile(`seq=(\d+) run=(\w+) `)

// seqs checks that msgs carry every seq 1..n exactly once.
func seqs(t *testing.T, msgs []string, n int, run string) {
	t.Helper()
	seen := map[int]int{}
	for _, m := range msgs {
		g := seqRE.FindStringSubmatch(m + " ")
		if g == nil || g[2] != run {
			t.Fatalf("message without seq/run %q: %q", run, m)
		}
		v, _ := strconv.Atoi(g[1])
		seen[v]++
	}
	for i := 1; i <= n; i++ {
		if seen[i] != 1 {
			t.Fatalf("seq %d seen %d times", i, seen[i])
		}
	}
}

func TestAckedConcurrent(t *testing.T) {
	s := newRELPServer(t, nil, false)
	code, sum := runJSON(t, "--target", s.addr, "--count", "5000", "--concurrency", "4", "--window", "16", "--run", "r1", "--size", "300")
	if code != ExitOK || sum.OK != 5000 || sum.Outcomes["acked"] != 5000 || sum.Conns.Opened != 4 || sum.Latency == nil {
		t.Fatalf("exit %d %+v", code, sum)
	}
	_, acks := s.received()
	seqs(t, acks, 5000, "r1")
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.Contains(s.offers[0], "relp_version=0\nrelp_software=go-relplog-injector,dev,") || !strings.HasSuffix(s.offers[0], "commands=syslog") {
		t.Fatalf("offers %q", s.offers[0])
	}
}

func TestRejectContinueAndBreak(t *testing.T) {
	s := newRELPServer(t, nil, false)
	s.set(func(s *relpServer) { s.reject = func(m string) bool { return strings.Contains(m, "seq=3 ") } })
	code, sum := runJSON(t, "--target", s.addr, "--count", "50", "--on-reject", "continue")
	if code != ExitFailures || sum.Outcomes["rejected"] != 1 || sum.Outcomes["acked"] != 49 || sum.Conns.Broken != 0 {
		t.Fatalf("continue: exit %d %+v", code, sum)
	}
	s2 := newRELPServer(t, nil, false)
	s2.set(func(s *relpServer) { s.reject = func(m string) bool { return strings.Contains(m, "seq=3 ") } })
	// --rate keeps transactions after seq 3 in flight when the 500 comes, as
	// a busy rsyslog would: librelp breaks the session and resends them.
	code, sum = runJSON(t, "--target", s2.addr, "--count", "300", "--window", "64")
	if code != ExitFailures || sum.Outcomes["rejected"] != 1 || sum.Outcomes["acked"] != 299 || sum.Conns.Broken != 1 || sum.Conns.Opened != 2 {
		t.Fatalf("break: exit %d %+v", code, sum)
	}
	got, _ := s2.received()
	c := countSeq(got)
	if c["3"] != 1 {
		t.Fatalf("the rejected message was resent: %d", c["3"])
	}
}

func TestDropAndServercloseRetransmit(t *testing.T) {
	for _, mode := range []string{"drop", "serverclose"} {
		s := newRELPServer(t, nil, false)
		s.set(func(s *relpServer) {
			if mode == "drop" {
				s.dropAt = 40
			} else {
				s.closeAt = 40
			}
		})
		code, sum := runJSON(t, "--target", s.addr, "--count", "500", "--window", "32", "--run", "d1")
		if code != ExitOK || sum.OK != 500 || sum.Conns.Broken != 1 || sum.Events["retransmitted"] == 0 {
			t.Fatalf("%s: exit %d %+v", mode, code, sum)
		}
		// At least once: a session that dies can lose answers already on
		// the wire (the server's close with unread data is a RST), and
		// those messages come back as duplicates -- RELP has no message id.
		got, acks := s.received()
		c := countSeq(acks)
		for i := 1; i <= 500; i++ {
			if c[strconv.Itoa(i)] < 1 {
				t.Fatalf("%s: seq %d never acked", mode, i)
			}
		}
		if len(got) <= 500 {
			t.Fatalf("%s: no retransmission reached the server (%d frames)", mode, len(got))
		}
	}
}

func TestRetriesZero(t *testing.T) {
	s := newRELPServer(t, nil, false)
	s.set(func(s *relpServer) { s.dropAt = 10 })
	code, sum := runJSON(t, "--target", s.addr, "--count", "100", "--retries", "0", "--window", "32")
	if code != ExitFailures || sum.Outcomes["unanswered"] == 0 || sum.Messages != 100 || sum.Events["retransmitted"] != 0 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

func TestAckTimeoutAndWindow(t *testing.T) {
	s := newRELPServer(t, nil, false)
	s.set(func(s *relpServer) { s.holdAll = true })
	start := time.Now()
	// Window 2, 4 messages: two sessions of two, each broken by the ack
	// timeout. Without the window, one session would carry all four.
	code, sum := runJSON(t, "--target", s.addr, "--count", "4", "--window", "2", "--retries", "0", "--ack-timeout", "300ms", "--drain-timeout", "2s")
	if code != ExitFailures || sum.Outcomes["unanswered"] != 4 || sum.Messages != 4 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	if got, _ := s.received(); len(got) != 4 || s.sessions.Load() != 2 {
		t.Fatalf("%d transactions in %d sessions, want 4 in 2", len(got), s.sessions.Load())
	}
	if sum.Events["conn_broken: "+"no answer within the ack timeout"] != 2 {
		t.Fatalf("events %v", sum.Events)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("ack timeout not honored")
	}
}

func TestTLSAndProxyV2(t *testing.T) {
	certPEM, keyPEM := selfSigned(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	s := newRELPServer(t, &tls.Config{Certificates: []tls.Certificate{cert}}, true)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	os.WriteFile(ca, certPEM, 0o600)
	code, _, errs := runMain(t, "--target", s.addr, "--transport", "tls", "--tls-ca", ca, "--tls-server-name", "injector.test",
		"--proxy-protocol", "v2", "--proxy-src", "192.0.2.7:4000", "--count", "50", "--run", "t1")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errs)
	}
	_, acks := s.received()
	seqs(t, acks, 50, "t1")
	_, port, _ := net.SplitHostPort(s.addr)
	p, _ := strconv.Atoi(port)
	s.mu.Lock()
	head := s.heads[0]
	s.mu.Unlock()
	if want := fmt.Sprintf("v2 2111000cc00002077f000001%04x%04x", 4000, p); head != want {
		t.Fatalf("proxy header %s, want %s", head, want)
	}
	code, _, _ = runMain(t, "--target", s.addr, "--transport", "tls", "--count", "1", "--drain-timeout", "300ms", "--proxy-protocol", "v2")
	if code != ExitUnreachable {
		t.Fatalf("unverified server: exit %d", code)
	}
}

func selfSigned(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "injector.test"},
		DNSNames:              []string{"injector.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(k)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})
}

func TestUnreachable(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	code, sum := runJSON(t, "--target", addr, "--count", "3", "--drain-timeout", "300ms")
	if code != ExitUnreachable || sum.Outcomes["not_sent"] != 3 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

func TestOpenRefusedIsDialError(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				r.ReadString('\n')
				r.ReadString('\n')
				r.ReadString('\n')
				fmt.Fprintf(c, "1 rsp 16 500 not allowed\n")
			}(c)
		}
	}()
	code, sum := runJSON(t, "--target", ln.Addr().String(), "--count", "1", "--drain-timeout", "300ms")
	if code != ExitUnreachable || sum.Conns.DialErrors == 0 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--template", "a", "--file", "b"},
		{"--transport", "udp"},
		{"--window", "0"},
		{"--retries", "-1"},
		{"--ack-timeout", "0s"},
		{"--on-reject", "maybe"},
		{"--pcap-timing", "original"},
		{"--format", "raw"},
		{"--loop"},
		{"--tls-ca", "x"},
		{"extra-arg"},
	} {
		if code, _, _ := runMain(t, args...); code != ExitUsage {
			t.Errorf("%v: exit %d, want %d", args, code, ExitUsage)
		}
	}
}

// TestPcapReplay: a capture with UDP syslog, a TCP octet-counting stream, a
// LF stream started before the capture, a RELP session and unrelated traffic
// replays exactly the syslog messages, in capture order.
func TestPcapReplay(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli := netip.MustParseAddr("10.1.1.1")
	srv := netip.MustParseAddr("10.1.1.2")
	var frames []pcaptest.Frame
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	udp := func(ms int, sport, dport uint16, p string) {
		ip := pcaptest.IP(cli, srv, 17, pcaptest.UDP(sport, dport, []byte(p)), uint16(ms))
		frames = append(frames, pcaptest.Frame{TS: at(ms), Data: pcaptest.Link(1, ip)})
	}
	udp(0, 5000, 514, "<13>udp-1")
	udp(1, 5000, 53, "\x12\x34 dns query") // not syslog
	octet := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 40001), Dst: netip.AddrPortFrom(srv, 1514), ISN: 7, T0: at(10)}
	frames = append(frames, octet.Frames(octet.Split([]byte("9 <13>oct-111 <13>oct-2 x"), 5), false, false)...)
	lf := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 40002), Dst: netip.AddrPortFrom(srv, 1514), ISN: 1000, T0: at(100)}
	frames = append(frames, lf.Frames(lf.Split([]byte("tail of old line\n<13>lf-1\n<13>lf-2\n"), 7), true, false)...)
	offers := "relp_version=0\nrelp_software=librelp,1.11.0,http://librelp.adiscon.com\ncommands=syslog"
	relp := fmt.Sprintf("1 open %d %s\n2 syslog 10 <13>relp-1\n3 close 0\n", len(offers), offers)
	rc := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(cli, 40003), Dst: netip.AddrPortFrom(srv, 20514), ISN: 5, T0: at(200)}
	frames = append(frames, rc.Frames(rc.Split([]byte(relp), 30), false, false)...)
	rsp := pcaptest.Conn{Link: 1, Src: netip.AddrPortFrom(srv, 20514), Dst: netip.AddrPortFrom(cli, 40003), ISN: 9, T0: at(201)}
	frames = append(frames, rsp.Frames(rsp.Split([]byte("1 rsp 6 200 OK\n2 rsp 6 200 OK\n"), 30), false, false)...)
	udp(300, 5000, 514, "<13>udp-2")
	sort.SliceStable(frames, func(i, j int) bool { return frames[i].TS.Before(frames[j].TS) })

	dir := t.TempDir()
	file := filepath.Join(dir, "cap.pcap")
	os.WriteFile(file, pcaptest.Classic(1, frames), 0o600)
	want := []string{"<13>udp-1", "<13>oct-1", "<13>oct-2 x", "<13>lf-1", "<13>lf-2", "<13>relp-1", "<13>udp-2"}

	s := newRELPServer(t, nil, false)
	code, sum := runJSON(t, "--target", s.addr, "--pcap", file)
	if code != ExitOK || sum.OK != uint64(len(want)) {
		t.Fatalf("exit %d %+v", code, sum)
	}
	waitAcks(t, s, len(want))
	_, got := s.received()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("replayed\n %q\nwant\n %q", got, want)
	}
	if sum.Source["udp_not_syslog"] != 1 || sum.Source["tcp_streams_relp"] != 1 || sum.Source["tcp_midstream"] != 1 || sum.Source["tcp_skipped_bytes"] == 0 {
		t.Fatalf("source stats %v", sum.Source)
	}

	// pcapng, port filter (only 514/udp), original timing at 10x, loop twice.
	file2 := filepath.Join(dir, "cap.pcapng")
	os.WriteFile(file2, pcaptest.NG(1, frames), 0o600)
	s2 := newRELPServer(t, nil, false)
	start := time.Now()
	code, sum = runJSON(t, "--target", s2.addr, "--pcap", file2, "--pcap-port", "514", "--pcap-timing", "original", "--pcap-speed", "10", "--loop", "--count", "4")
	el := time.Since(start)
	if code != ExitOK || sum.OK != 4 {
		t.Fatalf("filtered: exit %d %+v", code, sum)
	}
	waitAcks(t, s2, 4)
	if _, got := s2.received(); strings.Join(got, "|") != "<13>udp-1|<13>udp-2|<13>udp-1|<13>udp-2" {
		t.Fatalf("filtered replay %q", got)
	}
	// 300 ms of capture per pass at 10x: the 4th message is due ~30.1 ms + 30 ms.
	if el < 55*time.Millisecond {
		t.Fatalf("original timing ignored: %v", el)
	}

	// --format 5424 wraps the captured text as the MSG.
	s3 := newRELPServer(t, nil, false)
	if code, _, errs := runMain(t, "--target", s3.addr, "--pcap", file, "--pcap-proto", "udp", "--format", "5424", "--hostname", "re"); code != ExitOK {
		t.Fatalf("wrap: exit %d %s", code, errs)
	}
	waitAcks(t, s3, 2)
	if _, got := s3.received(); !strings.HasSuffix(got[0], " re go-relplog-injector "+strconv.Itoa(os.Getpid())+" - - <13>udp-1") {
		t.Fatalf("wrapped %q", got[0])
	}
}

func TestBadPcap(t *testing.T) {
	f := filepath.Join(t.TempDir(), "x.pcap")
	os.WriteFile(f, []byte("definitely not a capture"), 0o600)
	if code, _, errs := runMain(t, "--pcap", f); code != ExitUsage || !strings.Contains(errs, "not a pcap") {
		t.Fatalf("exit %d %q", code, errs)
	}
}

// --duration without --count runs for the whole duration (it used to stop
// after the single default message).
func TestDurationWithoutCount(t *testing.T) {
	s := newRELPServer(t, nil, false)
	code, sum := runJSON(t, "--target", s.addr, "--duration", "400ms", "--rate", "100")
	if code != ExitOK || sum.Messages < 20 || sum.Messages > 41 || sum.Outcomes["acked"] != sum.Messages {
		t.Fatalf("exit %d, %d messages: %+v", code, sum.Messages, sum)
	}
}
