package receiver

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
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
	h := newHarness(t, 100, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0", ReadBuffer: 1 << 20})

	lines := []string{
		doc("um"),
		gdoc("dois", 3),
		`{"short_message":"a LF\nnever splits a datagram"}` + "\n",
	}
	c := sendUDP(t, d.Addr(), lines...)
	h.waitItems(t, len(lines))

	local := c.LocalAddr().(*net.UDPAddr).AddrPort()
	seen := map[string]bool{}
	for _, it := range h.drainRing() {
		hd := it.Head
		if hd.Transport != item.TransportUDP || hd.Compression != item.CompressionNone || hd.Chunks != 0 {
			t.Errorf("wire = %v/%v/%d", hd.Transport, hd.Compression, hd.Chunks)
		}
		if netip.AddrPortFrom(hd.SrcIP, hd.SrcPort) != local {
			t.Errorf("src %v:%d, want %v", hd.SrcIP, hd.SrcPort, local)
		}
		// Bound to a concrete address: dst-ip is that address with or
		// without PKTINFO, and dst-port is always the socket's port.
		if hd.DstIP.String() != "127.0.0.1" || portOf(t, d.Addr()) != itoa(hd.DstPort) {
			t.Errorf("dst %v:%d, listener %s", hd.DstIP, hd.DstPort, d.Addr())
		}
		if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
			t.Error(err)
		}
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
	h := newHarness(t, 100, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "0.0.0.0:0"})
	if !d.PktinfoEnabled() {
		t.Skip("PKTINFO is not available on this platform")
	}
	sendUDP(t, "127.0.0.1:"+portOf(t, d.Addr()), doc("to loopback"))
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
	h := newHarness(t, 100, "debug", nil)
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
	c.Write([]byte(doc("to ::1")))
	h.waitItems(t, 1)
	if hd := h.drainRing()[0].Head; hd.DstIP != netip.MustParseAddr("::1") || hd.SrcIP != netip.MustParseAddr("::1") {
		t.Fatalf("dst-ip = %v src-ip = %v, want ::1", hd.DstIP, hd.SrcIP)
	}
}

// Without PKTINFO, a wildcard listener has no destination to report, and the
// field is omitted rather than written as 0.0.0.0 (SPEC §4.2).
func TestUDPWildcardWithoutPktinfoOmitsDst(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	d, err := NewUDP(context.Background(), UDPConfig{Addr: "0.0.0.0:0"}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	d.udp[0].pktinfo = false // what a failed setsockopt leaves
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); d.Close() })
	d.Start(ctx)
	sendUDP(t, "127.0.0.1:"+portOf(t, d.Addr()), doc("x"))
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
	sendUDP(t, "127.0.0.1:"+portOf(t, uc.LocalAddr().String()), doc("v4"))
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
	h := newHarness(t, 1, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	for i := 0; i < 20; i++ {
		sendUDP(t, d.Addr(), doc("x"))
	}
	waitFor(t, "a ring_full drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropRingFull].Load() > 0 })
}

// SPEC §5.5: a datagram above max_message_bytes is refused whole and counted
// too_large; nothing is cut.
func TestUDPTooLarge(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 1024
	h := newHarnessCfg(t, 10, "debug", nil, cfg)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	sendUDP(t, d.Addr(), doc(strings.Repeat("u", 2000)), doc("after"))
	h.waitItems(t, 1)
	waitFor(t, "the too_large drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropTooLarge].Load() == 1 })
	if it := h.drainRing()[0]; string(it.Raw) != doc("after") {
		t.Fatalf("raw = %q", it.Raw)
	}
	// A gzip document that inflates past the limit is too_large as well.
	sendUDP(t, d.Addr(), string(gzipOf(t, []byte(doc(strings.Repeat("g", 5000))))))
	waitFor(t, "the second too_large drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropTooLarge].Load() == 2 })
}

// SPEC §5.4: a compressed datagram is detected by its bytes and inflated; the
// ring holds the DOCUMENT and the head says the container. A container that
// does not decode is reason="decompress".
func TestUDPCompressedDatagrams(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	sendUDP(t, d.Addr(), string(gzipOf(t, []byte(doc("gz")))))
	h.waitItems(t, 1)
	sendUDP(t, d.Addr(), string(zlibOf(t, []byte(doc("zl")))))
	h.waitItems(t, 2)
	want := map[string]item.Compression{doc("gz"): item.CompressionGzip, doc("zl"): item.CompressionZlib}
	for _, it := range h.drainRing() {
		if c, ok := want[string(it.Raw)]; !ok || it.Head.Compression != c {
			t.Fatalf("raw %q compression %v", it.Raw, it.Head.Compression)
		}
	}
	gz := gzipOf(t, []byte(doc("broken")))
	sendUDP(t, d.Addr(), string(gz[:len(gz)-4]))
	waitFor(t, "the decompress drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropDecompress].Load() == 1 })
}

// SPEC §5.2, end to end through a real socket: a gzip document in three
// chunks, sent out of order with a duplicate, becomes ONE item whose clock
// sample is the LAST chunk's, with chunks=3 and compression=gzip, and whose
// envelope passes the oracle.
func TestUDPChunkedGzipEndToEnd(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	body := []byte(doc("chunked " + strings.Repeat("and compressed ", 40)))
	parts := split(0xfeedface, gzipOf(t, body), 3)

	c := sendUDP(t, d.Addr())
	send := func(b []byte) {
		if _, err := c.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	first := time.Now()
	send(parts[2])
	send(parts[0])
	send(parts[0]) // a duplicate
	waitFor(t, "the duplicate", func() bool { return h.m.UDPChunkDuplicatesTotal.Load() == 1 })
	if h.m.UDPReassemblyBytes.Load() == 0 {
		t.Fatal("the reassembly gauge does not show the open message")
	}
	time.Sleep(50 * time.Millisecond)
	last := time.Now()
	send(parts[1])
	h.waitItems(t, 1)

	it := h.drainRing()[0]
	hd := it.Head
	if string(it.Raw) != string(body) {
		t.Fatalf("raw = %q", it.Raw)
	}
	if hd.Chunks != 3 || hd.Compression != item.CompressionGzip {
		t.Fatalf("chunks %d compression %v", hd.Chunks, hd.Compression)
	}
	if hd.Time().Before(last) || !hd.Time().After(first.Add(40*time.Millisecond)) {
		t.Fatalf("sample %v: not the last chunk's (first sent %v, last %v)", hd.Time(), first, last)
	}
	env := envelopeOf(t, it)
	pl := payloadOf(t, env)
	if pl["chunks"] != float64(3) || pl["compression"] != "gzip" || pl["transport"] != "udp" {
		t.Fatalf("payload %v", pl)
	}
	waitFor(t, "the gauge to return to zero", func() bool { return h.m.UDPReassemblyBytes.Load() == 0 })
	if n := h.m.MessagesTotal[obs.TransportUDP].Load(); n != 1 {
		t.Fatalf("arrivals = %d: a chunk counted as a message", n)
	}
}

// SPEC §5.2: the sweeper counts an incomplete message once, with nothing else
// arriving; invalid and oversized chunks are counted with their reasons.
func TestUDPChunkDrops(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 1024
	h := newHarnessCfg(t, 10, "debug", nil, cfg)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0", ChunkTimeout: 100 * time.Millisecond,
		ReassemblyMaxMessages: 1})
	c := sendUDP(t, d.Addr())
	c.Write(chunk(1, 0, 2, []byte("half")))
	waitFor(t, "the chunk_timeout drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropChunkTimeout].Load() == 1 })
	time.Sleep(250 * time.Millisecond)
	if n := h.m.MessagesDroppedTotal[obs.DropChunkTimeout].Load(); n != 1 {
		t.Fatalf("chunk_timeout = %d: counted more than once", n)
	}
	c.Write(chunk(2, 3, 2, []byte("x")))
	waitFor(t, "the chunk_invalid drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropChunkInvalid].Load() == 1 })
	c.Write(chunk(3, 0, 3, []byte("x")))
	c.Write(chunk(4, 0, 2, []byte("y"))) // past udp_reassembly_max_messages=1
	waitFor(t, "the chunk_overflow drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropChunkOverflow].Load() == 1 })
	// Message 3 holds the only slot until it expires.
	waitFor(t, "message 3 to expire", func() bool { return h.m.MessagesDroppedTotal[obs.DropChunkTimeout].Load() == 2 })
	// Two chunks of 9000 bytes (macOS caps a datagram at 9216) pass the wire
	// ceiling of max 1024 (17408 bytes).
	c.Write(chunk(5, 0, 2, make([]byte, 9000)))
	c.Write(chunk(5, 1, 2, make([]byte, 9000)))
	waitFor(t, "the too_large drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropTooLarge].Load() == 1 })
	if h.ring.Stats().Items != 0 {
		t.Fatal("a broken chunked message got in")
	}
}

// SPEC §5.6: a datagram of zero octets is reason="empty".
func TestUDPEmptyDatagramIsCounted(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: "127.0.0.1:0"})
	sendUDP(t, d.Addr(), "", doc("after"))
	h.waitItems(t, 1)
	waitFor(t, "the empty drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropEmpty].Load() == 1 })
}

// SPEC §4.2: SO_RCVBUF is read back and exposed.
func TestUDPReadBufferIsReadBack(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
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

	h := newHarness(t, 100, "debug", nil)
	d := startUDP(t, h, UDPConfig{Addr: addr, Listeners: 2})
	if len(d.udp) != 2 || d.udp[0].localPort != d.udp[1].localPort {
		t.Fatalf("%d sockets", len(d.udp))
	}
	sendUDP(t, addr, doc("a"), doc("b"))
	h.waitItems(t, 2)
}

// Close is idempotent: the app's shutdown and a deferred cleanup may both call
// it.
func TestDatagramCloseIsIdempotent(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
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
