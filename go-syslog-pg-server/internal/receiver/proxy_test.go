package receiver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// proxyV2 builds a v2 header: verCmd (0x21 PROXY, 0x20 LOCAL), famProto, the
// address block and any trailing TLV bytes, with the length COMPUTED.
func proxyV2(verCmd, famProto byte, block []byte) []byte {
	b := append([]byte{}, proxyV2Sig...)
	b = append(b, verCmd, famProto, 0, 0)
	binary.BigEndian.PutUint16(b[14:16], uint16(len(block)))
	return append(b, block...)
}

func v2Inet(src, dst string, sp, dp uint16, tlv []byte) []byte {
	s, d := netip.MustParseAddr(src).As4(), netip.MustParseAddr(dst).As4()
	b := append(s[:], d[:]...)
	b = binary.BigEndian.AppendUint16(b, sp)
	b = binary.BigEndian.AppendUint16(b, dp)
	return append(b, tlv...)
}

func v2Inet6(src, dst string, sp, dp uint16) []byte {
	s, d := netip.MustParseAddr(src).As16(), netip.MustParseAddr(dst).As16()
	b := append(s[:], d[:]...)
	b = binary.BigEndian.AppendUint16(b, sp)
	return binary.BigEndian.AppendUint16(b, dp)
}

// readProxy runs the parser over input and reports what was left unread.
func readProxy(input []byte) (ProxyHeader, bool, error, string) {
	br := bufio.NewReaderSize(bytes.NewReader(input), 16)
	hdr, found, err := ReadProxyHeader(br)
	rest, _ := io.ReadAll(br)
	return hdr, found, err, string(rest)
}

func TestProxyV1(t *testing.T) {
	cases := []struct {
		in       string
		src, dst string
	}{
		{"PROXY TCP4 192.0.2.10 198.51.100.1 56324 1514\r\n", "192.0.2.10:56324", "198.51.100.1:1514"},
		{"PROXY TCP6 2001:db8::1 2001:db8::2 4000 6514\r\n", "[2001:db8::1]:4000", "[2001:db8::2]:6514"},
		// An IPv4-mapped TCP6 address is canonicalised to IPv4 (SPEC §4.2).
		{"PROXY TCP6 ::ffff:192.0.2.10 2001:db8::2 4000 6514\r\n", "192.0.2.10:4000", "[2001:db8::2]:6514"},
		{"PROXY TCP4 192.0.2.10 0.0.0.0 1 65535\r\n", "192.0.2.10:1", "invalid AddrPort"},
	}
	for _, tc := range cases {
		hdr, found, err, rest := readProxy([]byte(tc.in + "<13>after"))
		if err != nil || !found || hdr.Version != 1 || hdr.Local {
			t.Errorf("%q: %+v found=%v err=%v", tc.in, hdr, found, err)
			continue
		}
		if hdr.Src.String() != tc.src {
			t.Errorf("%q: src %v, want %s", tc.in, hdr.Src, tc.src)
		}
		if tc.dst != "invalid AddrPort" && hdr.Dst.String() != tc.dst {
			t.Errorf("%q: dst %v, want %s", tc.in, hdr.Dst, tc.dst)
		}
		if tc.dst == "invalid AddrPort" && (hdr.Dst.Addr().IsValid() || hdr.Dst.Port() != 65535) {
			t.Errorf("%q: an unspecified dst must be unknown with its port: %v", tc.in, hdr.Dst)
		}
		if rest != "<13>after" {
			t.Errorf("%q: the parser consumed past the header, left %q", tc.in, rest)
		}
	}
	hdr, found, err, rest := readProxy([]byte("PROXY UNKNOWN whatever is here\r\n<13>x"))
	if err != nil || !found || !hdr.Local || rest != "<13>x" {
		t.Fatalf("UNKNOWN: %+v %v %v %q", hdr, found, err, rest)
	}
	hdr, found, err, _ = readProxy([]byte("PROXY UNKNOWN\r\n"))
	if err != nil || !found || !hdr.Local {
		t.Fatalf("bare UNKNOWN: %+v %v %v", hdr, found, err)
	}
}

func TestProxyV1Invalid(t *testing.T) {
	for _, in := range []string{
		"PROXY TCP4 192.0.2.10 198.51.100.1 56324 1514\n",                              // LF without CR
		"PROXY TCP4 192.0.2.10 198.51.100.1 56324 1514",                                // cut short
		"PROXY TCP4 192.0.2.10 198.51.100.1 56324\r\n",                                 // missing a field
		"PROXY TCP4 192.0.2.10 198.51.100.1 56324 1514 9\r\n",                          // a field too many
		"PROXY TCP4  192.0.2.10 198.51.100.1 56324 1514\r\n",                           // double space
		"PROXY TCP4 2001:db8::1 198.51.100.1 56324 1514\r\n",                           // v6 in TCP4
		"PROXY TCP6 192.0.2.10 2001:db8::2 56324 1514\r\n",                             // v4 in TCP6
		"PROXY TCP4 192.0.2.10 198.51.100.1 65536 1514\r\n",                            // port range
		"PROXY TCP4 192.0.2.10 198.51.100.1 -1 1514\r\n",                               // sign
		"PROXY TCP4 192.0.2.999 198.51.100.1 1 2\r\n",                                  // not an IP
		"PROXY TCP4 0.0.0.0 198.51.100.1 1 2\r\n",                                      // unspecified client
		"PROXY UDP4 192.0.2.10 198.51.100.1 1 2\r\n",                                   // not a v1 protocol
		"PROXY TCP6 fe80::1%eth0 2001:db8::2 1 2\r\n",                                  // a zone
		"PROXY " + strings.Repeat("A", 120) + "\r\n",                                   // longer than 107
		"PROXY TCP4 192.0.2.10 198.51.100.1 56324 1514\r\r\n" + strings.Repeat("x", 9), // CR CR
	} {
		_, found, err, _ := readProxy([]byte(in))
		if !found || !errors.Is(err, ErrProxyHeader) {
			t.Errorf("%q: found=%v err=%v, want an invalid header", in, found, err)
		}
	}
}

func TestProxyV2(t *testing.T) {
	tlv := []byte{0x04, 0x00, 0x03, 'a', 'b', 'c'} // PP2_TYPE_NOOP, skipped
	hdr, found, err, rest := readProxy(append(proxyV2(0x21, 0x11,
		v2Inet("192.0.2.10", "198.51.100.1", 56324, 1514, tlv)), "<13>after"...))
	if err != nil || !found || hdr.Version != 2 || hdr.Local {
		t.Fatalf("TCP4: %+v %v %v", hdr, found, err)
	}
	if hdr.Src.String() != "192.0.2.10:56324" || hdr.Dst.String() != "198.51.100.1:1514" {
		t.Fatalf("TCP4: %v -> %v", hdr.Src, hdr.Dst)
	}
	if rest != "<13>after" {
		t.Fatalf("TCP4: TLVs were not skipped exactly, left %q", rest)
	}

	hdr, found, err, rest = readProxy(append(proxyV2(0x21, 0x21,
		v2Inet6("2001:db8::1", "2001:db8::2", 4000, 6514)), "x"...))
	if err != nil || !found || hdr.Src.String() != "[2001:db8::1]:4000" || hdr.Dst.String() != "[2001:db8::2]:6514" || rest != "x" {
		t.Fatalf("TCP6: %+v %v %v %q", hdr, found, err, rest)
	}

	// The real peer stands: LOCAL (with any family), UNSPEC, UDP, UNIX.
	for name, in := range map[string][]byte{
		"local":       proxyV2(0x20, 0x00, nil),
		"local tcp4":  proxyV2(0x20, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, nil)),
		"local junk":  proxyV2(0x20, 0x99, []byte{1, 2, 3}),
		"unspec":      proxyV2(0x21, 0x00, nil),
		"udp4":        proxyV2(0x21, 0x12, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, nil)),
		"udp6":        proxyV2(0x21, 0x22, v2Inet6("2001:db8::1", "2001:db8::2", 1, 2)),
		"unix stream": proxyV2(0x21, 0x31, make([]byte, proxyV2UnixSz)),
		"unix dgram":  proxyV2(0x21, 0x32, make([]byte, proxyV2UnixSz)),
	} {
		hdr, found, err, rest := readProxy(append(in, "next"...))
		if err != nil || !found || !hdr.Local || hdr.Version != 2 || rest != "next" {
			t.Errorf("%s: %+v found=%v err=%v rest=%q", name, hdr, found, err, rest)
		}
	}
}

func TestProxyV2Invalid(t *testing.T) {
	big := make([]byte, proxyV2Max) // 16 + 4096 > 4 KiB
	for name, in := range map[string][]byte{
		"version 1":        proxyV2(0x11, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, nil)),
		"command 2":        proxyV2(0x22, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, nil)),
		"family 4":         proxyV2(0x21, 0x41, nil),
		"protocol 3":       proxyV2(0x21, 0x13, nil),
		"tcp4 too short":   proxyV2(0x21, 0x11, make([]byte, 11)),
		"tcp6 too short":   proxyV2(0x21, 0x21, make([]byte, 35)),
		"unix too short":   proxyV2(0x21, 0x31, make([]byte, 100)),
		"unspecified src":  proxyV2(0x21, 0x11, v2Inet("0.0.0.0", "198.51.100.1", 1, 2, nil)),
		"above 4 KiB":      proxyV2(0x21, 0x11, big),
		"cut in the fixed": proxyV2Sig,
		"cut in the block": proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, nil))[:20],
	} {
		_, found, err, _ := readProxy(in)
		if !found || !errors.Is(err, ErrProxyHeader) {
			t.Errorf("%s: found=%v err=%v, want an invalid header", name, found, err)
		}
	}
}

// A v2 length above 4 KiB is refused from the declaration alone: not one byte
// of the block is read, so the declared length never decides a read or an
// allocation.
func TestProxyV2RefusesBeforeReading(t *testing.T) {
	in := proxyV2(0x21, 0x11, nil)
	binary.BigEndian.PutUint16(in[14:16], 0xffff)
	in = append(in, "rest"...)
	_, found, err, rest := readProxy(in)
	if !found || !errors.Is(err, ErrProxyHeader) || rest != "rest" {
		t.Fatalf("found=%v err=%v rest=%q", found, err, rest)
	}
}

// Without a signature nothing is consumed: the bytes belong to the syslog
// stream or to the ClientHello. And the decision does not wait for twelve
// bytes when the second already mismatches.
func TestProxyAbsentConsumesNothing(t *testing.T) {
	for _, in := range []string{
		"<13>hello\n",
		"PRO",          // a prefix of the signature, then EOF
		"PROXY",        // still a prefix
		"PROXYxTCP4\n", // mismatch at the sixth byte
		"\r\n<13>keepalive then message\n",
		"\x16\x03\x01\x00\xa5", // a TLS ClientHello
		"P",
	} {
		hdr, found, err, rest := readProxy([]byte(in))
		if found || err != nil || rest != in || hdr != (ProxyHeader{}) {
			t.Errorf("%q: found=%v err=%v rest=%q", in, found, err, rest)
		}
	}
	if _, found, err, _ := readProxy(nil); found || !errors.Is(err, io.EOF) {
		t.Fatalf("empty stream: found=%v err=%v, want EOF", found, err)
	}

	pr, pw := io.Pipe()
	defer pw.Close()
	go func() { _, _ = pw.Write([]byte("\r\n<")) }() // and nothing more
	done := make(chan bool, 1)
	go func() {
		_, found, _ := ReadProxyHeader(bufio.NewReader(pr))
		done <- found
	}()
	select {
	case found := <-done:
		if found {
			t.Fatal("a CRLF keep-alive was taken for a v2 signature")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the reader waited for bytes it did not need")
	}
}

func TestParseProxyModeAndCIDRs(t *testing.T) {
	for in, want := range map[string]ProxyMode{"off": ProxyOff, "": ProxyOff, "Optional": ProxyOptional, "required": ProxyRequired} {
		if got, err := ParseProxyMode(in); err != nil || got != want {
			t.Errorf("%q -> %v %v", in, got, err)
		}
	}
	if _, err := ParseProxyMode("on"); err == nil {
		t.Error("an unknown mode was accepted")
	}
	got, err := ParseCIDRList(" 127.0.0.1/32, ::1/128 ,10.1.2.3, 192.168.0.9/16,")
	if err != nil || len(got) != 4 || got[2].String() != "10.1.2.3/32" || got[3].String() != "192.168.0.0/16" {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := ParseCIDRList("10.0.0.0/33"); err == nil {
		t.Error("an invalid CIDR was accepted")
	}
	pc := ProxyConfig{Mode: ProxyOptional, Trusted: got}
	if err := pc.Validate(); err != nil {
		t.Fatal(err)
	}
	if !pc.Trusts(netip.MustParseAddr("::ffff:127.0.0.1")) || pc.Trusts(netip.MustParseAddr("10.1.2.4")) {
		t.Fatal("Trusts does not unmap, or trusts too much")
	}
	if err := (ProxyConfig{Mode: ProxyRequired}).Validate(); err == nil {
		t.Fatal("required without trusted networks was accepted")
	}
}

// ------------------------------------------------------------- over the wire

func startProxyTCP(t *testing.T, h *harness, mode ProxyMode, trusted string, extra StreamConfig) *Stream {
	t.Helper()
	cidrs, err := ParseCIDRList(trusted)
	if err != nil {
		t.Fatal(err)
	}
	extra.Proxy = ProxyConfig{Mode: mode, Trusted: cidrs, HeaderTimeout: extra.Proxy.HeaderTimeout}
	return startTCP(t, h, TCPConfig{StreamConfig: extra})
}

// SPEC §4.6: from a trusted peer, the payload's two ends are the header's and
// the real TCP peer goes to x-proxy (Head.Peer*).
func TestProxyOverTCPReplacesTheAddresses(t *testing.T) {
	for name, header := range map[string][]byte{
		"v1": []byte("PROXY TCP4 192.0.2.10 198.51.100.1 56324 514\r\n"),
		"v2": proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 56324, 514, nil)),
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, 10)
			tr := startProxyTCP(t, h, ProxyOptional, "127.0.0.1/32", StreamConfig{})
			c := dial(t, tr.Addr())
			c.Write(append(header, "<13>behind a balancer\n"...))
			h.waitItems(t, 1)
			it := h.drainRing()[0]
			hd := it.Head
			if string(it.Raw) != "<13>behind a balancer" {
				t.Fatalf("raw = %q", it.Raw)
			}
			if hd.SrcIP.String() != "192.0.2.10" || hd.SrcPort != 56324 ||
				hd.DstIP.String() != "198.51.100.1" || hd.DstPort != 514 {
				t.Fatalf("addresses %v:%d -> %v:%d", hd.SrcIP, hd.SrcPort, hd.DstIP, hd.DstPort)
			}
			local := c.LocalAddr().(*net.TCPAddr).AddrPort()
			if !hd.Proxied() || netip.AddrPortFrom(hd.PeerIP, hd.PeerPort) != local {
				t.Fatalf("peer %v:%d, want the real %v", hd.PeerIP, hd.PeerPort, local)
			}
			res := obs.ProxyV1
			if name == "v2" {
				res = obs.ProxyV2
			}
			if h.m.ProxyHeadersTotal[res].Load() != 1 {
				t.Fatalf("proxy_headers_total{%s} not counted", name)
			}
		})
	}
}

// From an UNTRUSTED peer nothing is read in optional mode: a forged header is
// the beginning of a message, and the addresses stay the socket's.
func TestProxyFromAnUntrustedPeerIsData(t *testing.T) {
	h := newHarness(t, 10)
	tr := startProxyTCP(t, h, ProxyOptional, "10.0.0.0/8", StreamConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "PROXY TCP4 192.0.2.10 198.51.100.1 56324 514\r\n<13>forged\n")
	h.waitItems(t, 2)
	items := h.drainRing()
	if string(items[0].Raw) != "PROXY TCP4 192.0.2.10 198.51.100.1 56324 514" {
		t.Fatalf("first = %q", items[0].Raw)
	}
	for _, it := range items {
		if it.Head.Proxied() || it.Head.SrcIP.String() != "127.0.0.1" {
			t.Fatalf("an untrusted header changed the addresses: %+v", it.Head)
		}
	}
	for r := obs.ProxyResult(0); r <= obs.ProxyInvalid; r++ {
		if h.m.ProxyHeadersTotal[r].Load() != 0 {
			t.Fatalf("something was counted as read from an untrusted peer (%d)", r)
		}
	}
}

func TestProxyRequired(t *testing.T) {
	t.Run("untrusted peer", func(t *testing.T) {
		h := newHarness(t, 10)
		tr := startProxyTCP(t, h, ProxyRequired, "10.0.0.0/8", StreamConfig{})
		c := dial(t, tr.Addr())
		fmt.Fprint(c, "PROXY TCP4 192.0.2.10 198.51.100.1 1 2\r\n<13>x\n")
		expectClosed(t, c)
		waitFor(t, "the proxy drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1 })
		if h.ring.Stats().Items != 0 {
			t.Fatal("a message got in from an untrusted peer under required")
		}
	})
	t.Run("trusted without header", func(t *testing.T) {
		h := newHarness(t, 10)
		tr := startProxyTCP(t, h, ProxyRequired, "127.0.0.1/32", StreamConfig{})
		c := dial(t, tr.Addr())
		fmt.Fprint(c, "<13>no header\n")
		expectClosed(t, c)
		waitFor(t, "the proxy drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1 })
		if h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 1 {
			t.Fatal("absent not counted")
		}
	})
	t.Run("trusted, silent past the header timeout", func(t *testing.T) {
		h := newHarness(t, 10)
		tr := startProxyTCP(t, h, ProxyRequired, "127.0.0.1/32",
			StreamConfig{Proxy: ProxyConfig{HeaderTimeout: 100 * time.Millisecond}})
		c := dial(t, tr.Addr())
		expectClosed(t, c)
		waitFor(t, "the proxy drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1 })
	})
	t.Run("trusted with header", func(t *testing.T) {
		h := newHarness(t, 10)
		tr := startProxyTCP(t, h, ProxyRequired, "127.0.0.1/32", StreamConfig{})
		c := dial(t, tr.Addr())
		fmt.Fprint(c, "PROXY TCP6 2001:db8::1 2001:db8::2 4000 1514\r\n<13>ok\n")
		h.waitItems(t, 1)
		if hd := h.drainRing()[0].Head; hd.SrcIP.String() != "2001:db8::1" || !hd.Proxied() {
			t.Fatalf("head %+v", hd)
		}
	})
}

// optional + trusted + no header: the connection goes on, counted as absent.
func TestProxyOptionalWithoutHeader(t *testing.T) {
	h := newHarness(t, 10)
	tr := startProxyTCP(t, h, ProxyOptional, "127.0.0.1/32", StreamConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>direct\n")
	h.waitItems(t, 1)
	if hd := h.drainRing()[0].Head; hd.Proxied() || hd.SrcIP.String() != "127.0.0.1" {
		t.Fatalf("head %+v", hd)
	}
	if h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 1 {
		t.Fatal("absent not counted")
	}
}

// LOCAL keeps the real peer, as if there were no header.
func TestProxyLocalKeepsTheRealPeer(t *testing.T) {
	h := newHarness(t, 10)
	tr := startProxyTCP(t, h, ProxyRequired, "127.0.0.1/32", StreamConfig{})
	c := dial(t, tr.Addr())
	c.Write(append(proxyV2(0x20, 0x00, nil), "<13>health\n"...))
	h.waitItems(t, 1)
	if hd := h.drainRing()[0].Head; hd.Proxied() || hd.SrcIP.String() != "127.0.0.1" {
		t.Fatalf("head %+v", hd)
	}
	if h.m.ProxyHeadersTotal[obs.ProxyLocal].Load() != 1 {
		t.Fatal("local not counted")
	}
}

// An invalid header from a trusted peer closes the connection (reason=proxy).
func TestProxyInvalidHeaderCloses(t *testing.T) {
	h := newHarness(t, 10)
	tr := startProxyTCP(t, h, ProxyOptional, "127.0.0.1/32", StreamConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "PROXY TCP4 not-an-ip 198.51.100.1 1 2\r\n<13>x\n")
	expectClosed(t, c)
	waitFor(t, "the invalid header", func() bool {
		return h.m.ProxyHeadersTotal[obs.ProxyInvalid].Load() == 1 &&
			h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1
	})
}

// A connection that closes before its first byte -- a balancer's TCP health
// check -- is not a proxy failure.
func TestProxyEmptyConnectionIsNotCounted(t *testing.T) {
	h := newHarness(t, 10)
	tr := startProxyTCP(t, h, ProxyRequired, "127.0.0.1/32", StreamConfig{})
	c := dial(t, tr.Addr())
	c.Close()
	waitFor(t, "the connection to end", func() bool {
		return h.m.ConnsTotal[obs.TransportTCP].Load() == 1 && h.m.ConnsActive[obs.TransportTCP].Load() == 0
	})
	if h.m.MessagesDroppedTotal[obs.DropProxy].Load() != 0 {
		t.Fatal("an empty connection was counted as a proxy refusal")
	}
}

// FuzzProxy is mandatory (SPEC §18.3): v1 and v2, never a panic, and never a
// byte consumed past the header the input declares. When a valid header is
// found, exactly its bytes are consumed; when none is, nothing is.
func FuzzProxy(f *testing.F) {
	seeds := [][]byte{
		[]byte("PROXY TCP4 192.0.2.10 198.51.100.1 56324 514\r\n<13>x"),
		[]byte("PROXY TCP6 2001:db8::1 2001:db8::2 4000 6514\r\n"),
		[]byte("PROXY UNKNOWN\r\n"),
		[]byte("PROXY TCP4 1.2.3.4 5.6.7.8 1 2\n"),
		proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 1, 2, []byte{1, 0, 1, 9})),
		proxyV2(0x21, 0x21, v2Inet6("2001:db8::1", "2001:db8::2", 1, 2)),
		proxyV2(0x20, 0x00, nil),
		proxyV2(0x21, 0x31, make([]byte, proxyV2UnixSz)),
		append(proxyV2(0x21, 0x11, nil)[:14], 0xff, 0xff),
		[]byte("<13>plain"),
		[]byte("\r\n\r\n\x00\r\nQUIT"),
		{},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		br := bufio.NewReaderSize(bytes.NewReader(data), 16)
		hdr, found, err := ReadProxyHeader(br)
		rest, _ := io.ReadAll(br)
		consumed := len(data) - len(rest)

		if !found {
			if consumed != 0 {
				t.Fatalf("no header, yet %d bytes consumed", consumed)
			}
			return
		}
		// The declared extent of the header.
		declared := 0
		if bytes.HasPrefix(data, proxyV1Sig) {
			declared = proxyV1Max
			if i := bytes.IndexByte(data, '\n'); i >= 0 && i < proxyV1Max {
				declared = i + 1
			}
		} else {
			declared = proxyV2Fixed
			if len(data) >= proxyV2Fixed {
				declared += int(binary.BigEndian.Uint16(data[14:16]))
			}
		}
		if consumed > declared {
			t.Fatalf("consumed %d bytes past the declared header of %d", consumed, declared)
		}
		if err != nil {
			if !errors.Is(err, ErrProxyHeader) {
				t.Fatalf("an error that is not ErrProxyHeader: %v", err)
			}
			return
		}
		if consumed != declared {
			t.Fatalf("a valid header consumed %d bytes of its %d", consumed, declared)
		}
		if !hdr.Local && (!hdr.Src.Addr().IsValid() || hdr.Src.Addr().Is4In6() || hdr.Src.Addr().IsUnspecified()) {
			t.Fatalf("a valid header with an unusable source: %v", hdr.Src)
		}
		if hdr.Version != 1 && hdr.Version != 2 {
			t.Fatalf("version %d", hdr.Version)
		}
	})
}

// The PROXY reader and its timeouts are exercised over a real connection with
// the shutdown deadline too: Close must break a reader waiting for a header.
func TestProxyCloseBreaksAHeaderWait(t *testing.T) {
	h := newHarness(t, 10)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	tr, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{Addr: "127.0.0.1:0",
		Proxy: ProxyConfig{Mode: ProxyRequired, Trusted: cidrs, HeaderTimeout: time.Hour}}}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)
	c := dial(t, tr.Addr())
	waitFor(t, "the connection", func() bool { return h.m.ConnsActive[obs.TransportTCP].Load() == 1 })
	_ = c
	cancel()
	done := make(chan struct{})
	go func() { tr.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited on a reader parked in the PROXY header")
	}
}
