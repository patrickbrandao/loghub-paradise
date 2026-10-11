package receiver

import (
	"context"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

func startUDP(t *testing.T, h *harness, cfg UDPConfig) *Datagram {
	t.Helper()
	d, err := NewUDP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); d.Close() })
	d.Start(ctx)
	return d
}

func sendUDP(t *testing.T, to string, msgs ...string) net.Conn {
	t.Helper()
	c, err := net.Dial("udp", to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	for _, m := range msgs {
		if _, err := c.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestUDPEndToEnd(t *testing.T) {
	h := newHarness(t, 100)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0", ReadBuffer: 1 << 20})

	lines := []string{
		`<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - um`,
		`<34>Oct 11 22:14:15 mymachine su[230]: dois`,
		"sem pri\nnunca dividido por LF",
	}
	c := sendUDP(t, d.Addr(), lines...)
	h.waitItems(t, len(lines))

	local := c.LocalAddr().(*net.UDPAddr).AddrPort()
	seen := map[string]bool{}
	for _, it := range h.drainRing() {
		hd := it.Head
		if hd.Transport != item.TransportUDP || hd.Framing != item.FramingDatagram {
			t.Errorf("wire = %v/%v", hd.Transport, hd.Framing)
		}
		if netip.AddrPortFrom(hd.SrcIP, hd.SrcPort) != local {
			t.Errorf("src %v:%d, want %v", hd.SrcIP, hd.SrcPort, local)
		}
		// Bound to a concrete address: dst-ip is that address with or
		// without PKTINFO, and dst-port is always the socket's port.
		if hd.DstIP.String() != "127.0.0.1" || portOf(t, d.Addr()) != itoa(hd.DstPort) {
			t.Errorf("dst %v:%d, listener %s", hd.DstIP, hd.DstPort, d.Addr())
		}
		checkConverts(t, it)
		seen[string(it.Raw)] = true
	}
	for _, l := range lines {
		if !seen[l] {
			t.Errorf("line not received whole: %q", l)
		}
	}
}

func itoa(p uint16) string {
	return netip.AddrPortFrom(netip.IPv4Unspecified(), p).String()[len("0.0.0.0:"):]
}

// SPEC §4.2, decision 15: on a WILDCARD listener, dst-ip comes from PKTINFO. A
// datagram sent to 127.0.0.1 reports 127.0.0.1, not 0.0.0.0 and not nothing.
// Go opens a dual-stack socket here, so this also proves the IPv4-mapped
// destination is canonicalised back to IPv4.
func TestUDPDstIPFromPktinfo(t *testing.T) {
	h := newHarness(t, 100)
	d := startUDP(t, h, UDPConfig{Addr: "0.0.0.0:0"})
	if !d.PktinfoEnabled() {
		t.Skip("PKTINFO is not available on this platform")
	}
	sendUDP(t, "127.0.0.1:"+portOf(t, d.Addr()), "<13>to loopback")
	h.waitItems(t, 1)
	hd := h.drainRing()[0].Head
	if hd.DstIP != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("dst-ip = %v, want 127.0.0.1", hd.DstIP)
	}
	if hd.SrcIP != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("src-ip = %v, want 127.0.0.1 (canonical, not ::ffff:)", hd.SrcIP)
	}
}

func TestUDPDstIPFromPktinfoIPv6(t *testing.T) {
	h := newHarness(t, 100)
	d, err := NewUDP(context.Background(), UDPConfig{Addr: "[::]:0"}, h.in, h.m, logger())
	if err != nil {
		t.Skipf("no IPv6: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); d.Close() })
	d.Start(ctx)
	if !d.PktinfoEnabled() {
		t.Skip("PKTINFO is not available on this platform")
	}
	c, err := net.Dial("udp", "[::1]:"+portOf(t, d.Addr()))
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	defer c.Close()
	c.Write([]byte("<13>to ::1"))
	h.waitItems(t, 1)
	if hd := h.drainRing()[0].Head; hd.DstIP != netip.MustParseAddr("::1") || hd.SrcIP != netip.MustParseAddr("::1") {
		t.Fatalf("dst-ip = %v src-ip = %v, want ::1", hd.DstIP, hd.SrcIP)
	}
}

// Without PKTINFO, a wildcard listener has no destination to report, and the
// field is omitted rather than written as 0.0.0.0 (SPEC §4.2).
func TestUDPWildcardWithoutPktinfoOmitsDst(t *testing.T) {
	h := newHarness(t, 100)
	d, err := NewUDP(context.Background(), UDPConfig{Addr: "0.0.0.0:0"}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	d.udp[0].pktinfo = false // what a failed setsockopt leaves
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); d.Close() })
	d.Start(ctx)
	sendUDP(t, "127.0.0.1:"+portOf(t, d.Addr()), "<13>x")
	h.waitItems(t, 1)
	hd := h.drainRing()[0].Head
	if hd.DstIP.IsValid() || hd.DstPort == 0 {
		t.Fatalf("dst = %v:%d, want no address and the socket's port", hd.DstIP, hd.DstPort)
	}
}

// The IPv4-only path: an AF_INET socket (what Go opens when the kernel has
// IPv6 disabled, common in containers) uses IP_PKTINFO on Linux and
// IP_RECVDSTADDR on the BSDs, each with its own control message layout.
func TestPktinfoOnAnIPv4OnlySocket(t *testing.T) {
	pc, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	uc := pc.(*net.UDPConn)
	if err := enablePktinfo(uc); err != nil {
		t.Skipf("PKTINFO is not available on this platform: %v", err)
	}
	sendUDP(t, "127.0.0.1:"+portOf(t, uc.LocalAddr().String()), "<13>v4")
	_ = uc.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf, oob := make([]byte, 64), make([]byte, oobBuffer)
	_, oobn, _, _, err := uc.ReadMsgUDPAddrPort(buf, oob)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsePktinfo(oob[:oobn]); got != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("destination = %v, want 127.0.0.1", got)
	}
}

func TestParsePktinfoWithoutControlMessages(t *testing.T) {
	if a := parsePktinfo(nil); a.IsValid() {
		t.Fatal(a)
	}
	if a := parsePktinfo([]byte{1, 2, 3}); a.IsValid() {
		t.Fatal(a)
	}
}

// A full ring on UDP is a counted drop and nothing else: there is nobody to tell.
func TestUDPFullRingCountsTheDrop(t *testing.T) {
	h := newHarness(t, 1)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	for i := 0; i < 20; i++ {
		sendUDP(t, d.Addr(), "<13>x")
	}
	waitFor(t, "a ring_full drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropRingFull].Load() > 0 })
}

// SPEC §5.5: the whole datagram is read and cut at max_message_bytes, with
// size = the datagram's length.
func TestUDPTruncation(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 10, cfg)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	long := "<13>" + strings.Repeat("u", 1996)
	sendUDP(t, d.Addr(), long)
	h.waitItems(t, 1)
	it := h.drainRing()[0]
	if string(it.Raw) != long[:480] || it.Head.Size != 2000 {
		t.Fatalf("raw %d bytes, size %d", len(it.Raw), it.Head.Size)
	}
	if h.m.MessagesTruncatedTotal[obs.TransportUDP].Load() != 1 {
		t.Fatal("truncation not counted")
	}
}

// SPEC §5.6: a datagram of zero octets is reason="empty".
func TestUDPEmptyDatagramIsCounted(t *testing.T) {
	h := newHarness(t, 10)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	sendUDP(t, d.Addr(), "", "<13>after")
	h.waitItems(t, 1)
	waitFor(t, "the empty drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropEmpty].Load() == 1 })
}

// SPEC §4.2: SO_RCVBUF is read back and exposed.
func TestUDPReadBufferIsReadBack(t *testing.T) {
	h := newHarness(t, 10)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0", ReadBuffer: 256 << 10})
	if d.ReadBufferBytes() <= 0 {
		t.Skip("the platform cannot read SO_RCVBUF back")
	}
	if got := h.m.UDPReadBufferBytes.Load(); got != int64(d.ReadBufferBytes()) {
		t.Fatalf("gauge = %d, read back = %d", got, d.ReadBufferBytes())
	}
}

// listen.udp_listeners > 1: N sockets on ONE address through SO_REUSEPORT.
func TestUDPReusePort(t *testing.T) {
	probe, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.LocalAddr().String()
	probe.Close()

	h := newHarness(t, 100)
	d := startUDP(t, h, UDPConfig{Addr: addr, Listeners: 2})
	if len(d.udp) != 2 || d.udp[0].localPort != d.udp[1].localPort {
		t.Fatalf("%d sockets", len(d.udp))
	}
	sendUDP(t, addr, "<13>a", "<13>b")
	h.waitItems(t, 2)
}

// ------------------------------------------------------------------ unix dgram

func startUnix(t *testing.T, h *harness, path string) *Datagram {
	t.Helper()
	d, err := NewUnix(context.Background(), UnixConfig{Path: path, Mode: 0o666}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); d.Close() })
	d.Start(ctx)
	return d
}

func TestUnixEndToEnd(t *testing.T) {
	h := newHarness(t, 100)
	path := filepath.Join(shortDir(t), "log")
	d := startUnix(t, h, path)

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 0666 is what /dev/log has everywhere, and it has to be: any process on the
	// host needs to be able to log.
	if fi.Mode().Perm() != 0o666 {
		t.Fatalf("socket mode = %o, want 666", fi.Mode().Perm())
	}

	c, err := net.Dial("unixgram", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("<13>Sep 12 10:00:00 host app: local")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	h.waitItems(t, 1)

	it := h.drainRing()[0]
	hd := it.Head
	if hd.Transport != item.TransportUnix || hd.Framing != item.FramingDatagram {
		t.Fatalf("wire = %v/%v", hd.Transport, hd.Framing)
	}
	// No network field exists on this transport (SPEC §4.3).
	if hd.SrcIP.IsValid() || hd.SrcPort != 0 || hd.DstIP.IsValid() || hd.DstPort != 0 || hd.Proxied() {
		t.Fatalf("unix message carries addresses: %+v", hd)
	}
	checkConverts(t, it)

	// Close removes the socket file, so the next boot has nothing to decide about.
	d.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("the socket file survived Close")
	}
}

// SPEC §4.3: max(65536, max_message_bytes+1) is read, and a longer message is
// cut with size = the bytes read, so the converter sees size > len(raw).
func TestUnixTruncation(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 10, cfg)
	path := filepath.Join(shortDir(t), "log")
	startUnix(t, h, path)
	c, err := net.Dial("unixgram", path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	long := "<13>" + strings.Repeat("v", 996) // within macOS's 2 KiB default
	if _, err := c.Write([]byte(long)); err != nil {
		t.Fatal(err)
	}
	h.waitItems(t, 1)
	it := h.drainRing()[0]
	if string(it.Raw) != long[:480] || it.Head.Size != 1000 {
		t.Fatalf("raw %d size %d", len(it.Raw), it.Head.Size)
	}
	if h.m.MessagesTruncatedTotal[obs.TransportUnix].Load() != 1 {
		t.Fatal("truncation not counted")
	}
}

// Stealing /dev/log from a running syslogd is not a mistake to make quietly.
func TestUnixRefusesALiveSocket(t *testing.T) {
	h := newHarness(t, 10)
	path := filepath.Join(shortDir(t), "log")
	startUnix(t, h, path)
	_, err := NewUnix(context.Background(), UnixConfig{Path: path}, h.in, h.m, logger())
	if err == nil {
		t.Fatal("the second receiver stole a served socket")
	}
	if !strings.Contains(err.Error(), "already served") {
		t.Fatalf("err = %v", err)
	}
}

// A socket left by a previous run must be cleared, or bind fails forever.
func TestUnixRemovesAStaleSocket(t *testing.T) {
	h := newHarness(t, 10)
	path := filepath.Join(shortDir(t), "log")

	// A socket file nobody is listening on: exactly what a SIGKILL leaves.
	c, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	c.Close() // a datagram socket's file stays, unserved
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the stale socket was not left behind: %v", err)
	}

	d, err := NewUnix(context.Background(), UnixConfig{Path: path}, h.in, h.m, logger())
	if err != nil {
		t.Fatalf("a stale socket was not cleared: %v", err)
	}
	d.Close()
}

// SPEC §13.2: a relative path, or one longer than sun_path, is exit 1 with the
// right message.
func TestUnixPathChecks(t *testing.T) {
	h := newHarness(t, 10)
	if _, err := NewUnix(context.Background(), UnixConfig{Path: "dev/log"}, h.in, h.m, logger()); err == nil ||
		!strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative path: %v", err)
	}
	long := "/" + strings.Repeat("d", maxSunPath)
	if _, err := NewUnix(context.Background(), UnixConfig{Path: long}, h.in, h.m, logger()); err == nil ||
		!strings.Contains(err.Error(), "sun_path") {
		t.Fatalf("long path: %v", err)
	}
}

// Close is idempotent: the app's shutdown and a deferred cleanup may both call
// it.
func TestDatagramCloseIsIdempotent(t *testing.T) {
	h := newHarness(t, 10)
	d, err := NewUDP(context.Background(), UDPConfig{Addr: "127.0.0.1:0"}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	d.Start(context.Background())
	done := make(chan struct{})
	go func() { d.Close(); d.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung")
	}
}
