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
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap/pcaptest"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/report"
)

// sink collects what a fake server received.
type sink struct {
	mu    sync.Mutex
	msgs  []string
	conns int
	heads []string // first bytes of each connection (PROXY header)
}

func (s *sink) add(m string) {
	s.mu.Lock()
	s.msgs = append(s.msgs, m)
	s.mu.Unlock()
}

func (s *sink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.msgs...)
}

// readOctet parses RFC 6587 octet-counting frames.
func readOctet(r *bufio.Reader, s *sink) {
	for {
		n, err := r.ReadString(' ')
		if err != nil {
			return
		}
		l, err := strconv.Atoi(strings.TrimSuffix(n, " "))
		if err != nil {
			s.add("BAD FRAME " + n)
			return
		}
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return
		}
		s.add(string(b))
	}
}

func readLF(r *bufio.Reader, s *sink) {
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			return
		}
		s.add(strings.TrimSuffix(l, "\n"))
	}
}

func streamServer(t *testing.T, lf bool, proxy bool, tlsConf *tls.Config) (string, *sink) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	s := &sink{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns++
			s.mu.Unlock()
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				if proxy {
					h := readProxy(r)
					s.mu.Lock()
					s.heads = append(s.heads, h)
					s.mu.Unlock()
				}
				if tlsConf != nil {
					tc := tls.Server(&prefixConn{Conn: c, r: r}, tlsConf)
					if err := tc.Handshake(); err != nil {
						s.add("HANDSHAKE " + err.Error())
						return
					}
					r = bufio.NewReader(tc)
				}
				if lf {
					readLF(r, s)
				} else {
					readOctet(r, s)
				}
			}(c)
		}
	}()
	return ln.Addr().String(), s
}

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

func udpServer(t *testing.T) (string, *sink) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	s := &sink{}
	go func() {
		b := make([]byte, 70000)
		for {
			n, _, err := pc.ReadFrom(b)
			if err != nil {
				return
			}
			s.add(string(b[:n]))
		}
	}()
	return pc.LocalAddr().String(), s
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

func waitFor(t *testing.T, s *sink, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := s.all(); len(m) >= n {
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	m := s.all()
	t.Fatalf("server got %d messages, want %d", len(m), n)
	return m
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

func TestTCPOctetConcurrent(t *testing.T) {
	addr, s := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--count", "5000", "--concurrency", "4", "--run", "r1", "--size", "300")
	if code != ExitOK || sum.OK != 5000 || sum.Outcomes["sent"] != 5000 || sum.Conns.Opened != 4 {
		t.Fatalf("exit %d summary %+v", code, sum)
	}
	msgs := waitFor(t, s, 5000)
	seqs(t, msgs, 5000, "r1")
	for _, m := range msgs[:10] {
		if len(m) != 300 || !strings.HasPrefix(m, "<13>1 ") {
			t.Fatalf("message %q (%d bytes)", m, len(m))
		}
	}
}

func TestTCPLFAnd3164(t *testing.T) {
	addr, s := streamServer(t, true, false, nil)
	code, _, errs := runMain(t, "--target", addr, "--framing", "lf", "--format", "3164", "--count", "3", "--facility", "local0", "--severity", "err", "--hostname", "h1", "--app", "tagx")
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errs)
	}
	m := waitFor(t, s, 3)
	re := regexp.MustCompile(`^<131>[A-Z][a-z]{2} [ 0-9]\d \d\d:\d\d:\d\d h1 tagx\[\d+\]: seq=1 run=\w+ injector=go-syslog-injector$`)
	if !re.MatchString(m[0]) {
		t.Fatalf("3164 message %q", m[0])
	}
}

func TestUDPAndTooLarge(t *testing.T) {
	addr, s := udpServer(t)
	code, sum := runJSON(t, "--target", addr, "--transport", "udp", "--count", "200", "--rate", "2000", "--run", "u1")
	if code != ExitOK || sum.OK != 200 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	seqs(t, waitFor(t, s, 200), 200, "u1")
	code, sum = runJSON(t, "--target", addr, "--transport", "udp", "--count", "2", "--size", "70000")
	if code != ExitFailures || sum.Outcomes["too_large"] != 2 {
		t.Fatalf("too large: exit %d %+v", code, sum)
	}
}

func TestUnixDatagram(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "si")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "log")
	pc, err := net.ListenPacket("unixgram", path)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	s := &sink{}
	go func() {
		b := make([]byte, 70000)
		for {
			n, _, err := pc.ReadFrom(b)
			if err != nil {
				return
			}
			s.add(string(b[:n]))
		}
	}()
	code, _, errs := runMain(t, "--transport", "unix", "--target", path, "--count", "300", "--run", "x1")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errs)
	}
	seqs(t, waitFor(t, s, 300), 300, "x1")
}

func TestTLSAndProxyV2(t *testing.T) {
	certPEM, keyPEM := selfSigned(t)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	addr, s := streamServer(t, false, true, &tls.Config{Certificates: []tls.Certificate{cert}})
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	os.WriteFile(ca, certPEM, 0o600)
	code, _, errs := runMain(t, "--target", addr, "--transport", "tls", "--tls-ca", ca, "--tls-server-name", "injector.test",
		"--proxy-protocol", "v2", "--proxy-src", "192.0.2.7:4000", "--count", "50", "--run", "t1")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errs)
	}
	seqs(t, waitFor(t, s, 50), 50, "t1")
	s.mu.Lock()
	head := s.heads[0]
	s.mu.Unlock()
	// v4 STREAM, 12 bytes: 192.0.2.7 -> 127.0.0.1, 4000 -> port
	_, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	want := fmt.Sprintf("v2 2111000cc00002077f000001%04x%04x", 4000, p)
	if head != want {
		t.Fatalf("proxy header %s, want %s", head, want)
	}
	// Without the CA the handshake fails: unreachable.
	code, _, _ = runMain(t, "--target", addr, "--transport", "tls", "--count", "1", "--drain-timeout", "300ms", "--proxy-protocol", "v2")
	if code != ExitUnreachable {
		t.Fatalf("unverified server: exit %d", code)
	}
}

func TestProxyV1(t *testing.T) {
	addr, s := streamServer(t, false, true, nil)
	code, _, errs := runMain(t, "--target", addr, "--proxy-protocol", "v1", "--count", "1")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errs)
	}
	waitFor(t, s, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !regexp.MustCompile(`^PROXY TCP4 127\.0\.0\.1 127\.0\.0\.1 \d+ \d+\r\n$`).MatchString(s.heads[0]) {
		t.Fatalf("v1 header %q", s.heads[0])
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
	start := time.Now()
	code, sum := runJSON(t, "--target", addr, "--count", "3", "--drain-timeout", "300ms")
	if code != ExitUnreachable || sum.Outcomes["not_sent"] != 3 || sum.Conns.DialErrors == 0 {
		t.Fatalf("exit %d %+v", code, sum)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("did not give up within the drain timeout")
	}
}

func TestServerGoneMidRun(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var got int
	var mu sync.Mutex
	go func() {
		// First connection: read 10 frames then close. Then accept normally.
		first := true
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn, first bool) {
				defer c.Close()
				r := bufio.NewReader(c)
				s := &sink{}
				if first {
					for range 10 {
						n, _ := r.ReadString(' ')
						l, _ := strconv.Atoi(strings.TrimSpace(n))
						io.ReadFull(r, make([]byte, l))
					}
					return
				}
				readOctet(r, s)
				mu.Lock()
				got += len(s.all())
				mu.Unlock()
			}(c, first)
			first = false
		}
	}()
	code, sum := runJSON(t, "--target", ln.Addr().String(), "--count", "2000", "--rate", "4000")
	// A syslog stream has no ack: some messages written after the close
	// are lost without an error. What matters: the injector notices,
	// reconnects, and finishes.
	if sum.Conns.Opened < 2 || sum.Conns.Broken < 1 || sum.Messages != 2000 {
		t.Fatalf("exit %d %+v", code, sum)
	}
}

func TestRateAndDuration(t *testing.T) {
	addr, _ := streamServer(t, false, false, nil)
	start := time.Now()
	code, sum := runJSON(t, "--target", addr, "--count", "100", "--rate", "200")
	el := time.Since(start)
	if code != ExitOK || sum.OK != 100 || el < 450*time.Millisecond || el > 2*time.Second {
		t.Fatalf("rate: exit %d ok %d in %v", code, sum.OK, el)
	}
	start = time.Now()
	code, sum = runJSON(t, "--target", addr, "--count", "0", "--duration", "300ms", "--rate", "1000")
	el = time.Since(start)
	if code != ExitOK || sum.OK < 200 || sum.OK > 400 || el > 2*time.Second {
		t.Fatalf("duration: exit %d ok %d in %v", code, sum.OK, el)
	}
}

func TestTemplateAndMarkers(t *testing.T) {
	addr, s := streamServer(t, false, false, nil)
	code, _, errs := runMain(t, "--target", addr, "--count", "2", "--format", "raw", "--run", "zz",
		"--template", "<14>1 {ts} {host} app {pid} - - n={seq} r={run} u={uuid} k={rand:4} {{lit}}", "--hostname", "hh")
	if code != ExitOK {
		t.Fatalf("exit %d %s", code, errs)
	}
	m := waitFor(t, s, 2)
	sort.Strings(m)
	re := regexp.MustCompile(`^<14>1 \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}(Z|[+-]\d\d:\d\d) hh app \d+ - - n=[12] r=zz u=[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12} k=[a-z0-9]{4} \{lit\}$`)
	for _, x := range m {
		if !re.MatchString(x) {
			t.Fatalf("template message %q", x)
		}
	}
	if code, _, _ := runMain(t, "--target", addr, "--template", "x {nope}"); code != ExitUsage {
		t.Fatalf("unknown marker: exit %d", code)
	}
}

func TestFileLoopAndStdinRules(t *testing.T) {
	addr, s := streamServer(t, false, false, nil)
	f := filepath.Join(t.TempDir(), "lines.txt")
	os.WriteFile(f, []byte("<13>one\r\n\n<13>two\n<13>three"), 0o600)
	code, sum := runJSON(t, "--target", addr, "--file", f)
	if code != ExitOK || sum.OK != 3 {
		t.Fatalf("file: exit %d %+v", code, sum)
	}
	if got := waitFor(t, s, 3); strings.Join(got, "|") != "<13>one|<13>two|<13>three" {
		t.Fatalf("file messages %q", got)
	}
	code, sum = runJSON(t, "--target", addr, "--file", f, "--loop", "--count", "7")
	if code != ExitOK || sum.OK != 7 || sum.Source["file_passes"] != 3 {
		t.Fatalf("loop: exit %d %+v", code, sum)
	}
	if code, _, _ := runMain(t, "--target", addr, "--file", "-", "--loop"); code != ExitUsage {
		t.Fatalf("stdin loop: exit %d", code)
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--template", "a", "--file", "b"},
		{"--transport", "carrier-pigeon"},
		{"--framing", "xml"},
		{"--transport", "tls", "--framing", "lf"},
		{"--transport", "udp", "--proxy-protocol", "v1"},
		{"--pcap-timing", "original"},
		{"--format", "raw"},
		{"--transport", "unix"},
		{"--facility", "local9"},
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

	addr, s := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--pcap", file)
	if code != ExitOK || sum.OK != uint64(len(want)) {
		t.Fatalf("exit %d %+v", code, sum)
	}
	got := waitFor(t, s, len(want))
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("replayed\n %q\nwant\n %q", got, want)
	}
	if sum.Source["udp_not_syslog"] != 1 || sum.Source["tcp_streams_relp"] != 1 || sum.Source["tcp_midstream"] != 1 || sum.Source["tcp_skipped_bytes"] == 0 {
		t.Fatalf("source stats %v", sum.Source)
	}

	// pcapng, port filter (only 514/udp), original timing at 10x, loop twice.
	file2 := filepath.Join(dir, "cap.pcapng")
	os.WriteFile(file2, pcaptest.NG(1, frames), 0o600)
	addr2, s2 := streamServer(t, false, false, nil)
	start := time.Now()
	code, sum = runJSON(t, "--target", addr2, "--pcap", file2, "--pcap-port", "514", "--pcap-timing", "original", "--pcap-speed", "10", "--loop", "--count", "4")
	el := time.Since(start)
	if code != ExitOK || sum.OK != 4 {
		t.Fatalf("filtered: exit %d %+v", code, sum)
	}
	if got := waitFor(t, s2, 4); strings.Join(got, "|") != "<13>udp-1|<13>udp-2|<13>udp-1|<13>udp-2" {
		t.Fatalf("filtered replay %q", got)
	}
	// 300 ms of capture per pass at 10x: the 4th message is due ~30.1 ms + 30 ms.
	if el < 55*time.Millisecond {
		t.Fatalf("original timing ignored: %v", el)
	}

	// --format 5424 wraps the captured text as the MSG.
	addr3, s3 := streamServer(t, false, false, nil)
	if code, _, errs := runMain(t, "--target", addr3, "--pcap", file, "--pcap-proto", "udp", "--format", "5424", "--hostname", "re"); code != ExitOK {
		t.Fatalf("wrap: exit %d %s", code, errs)
	}
	if got := waitFor(t, s3, 2); !strings.HasSuffix(got[0], " re go-syslog-injector "+strconv.Itoa(os.Getpid())+" - - <13>udp-1") {
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
	addr, s := streamServer(t, false, false, nil)
	code, sum := runJSON(t, "--target", addr, "--duration", "400ms", "--rate", "100")
	if code != ExitOK || sum.Messages < 20 || sum.Messages > 41 {
		t.Fatalf("exit %d, %d messages: %+v", code, sum.Messages, sum)
	}
	waitFor(t, s, int(sum.Messages))
}
