package pcap_test

import (
	"net/netip"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
)

// TestDedupKey: the key of a decoded packet ignores what NAT rewrites (the
// addresses, the destination port) and the interface, and keeps the rest
// (SPEC §6.8).
func TestDedupKey(t *testing.T) {
	key := func(ifc uint32, src, dst netip.AddrPort, id uint16, l4 func(s, d uint16) []byte, proto uint8) uint64 {
		t.Helper()
		ip := pcaptest.IP(src.Addr(), dst.Addr(), proto, l4(src.Port(), dst.Port()), id)
		s, ok := pcap.NewDecoder().Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkSLL2, Data: sll2(ifc, ip)})
		if !ok {
			t.Fatal("not decoded")
		}
		return pcap.Key(s)
	}
	udp := func(p string) func(s, d uint16) []byte {
		return func(s, d uint16) []byte { return pcaptest.UDP(s, d, []byte(p)) }
	}
	tcp := func(seq uint32, p string) func(s, d uint16) []byte {
		return func(s, d uint16) []byte { return pcaptest.TCP(s, d, seq, pcaptest.ACK|pcaptest.PSH, []byte(p)) }
	}
	a := netip.MustParseAddrPort("172.17.0.5:40000")
	b := netip.MustParseAddrPort("172.17.0.2:514")
	natA := netip.MustParseAddrPort("10.0.0.1:40000")
	natB := netip.MustParseAddrPort("10.0.0.2:1514")
	base := key(1, a, b, 7, udp("<13>x"), 17)
	if key(2, natA, natB, 7, udp("<13>x"), 17) != base {
		t.Fatal("NAT-rewritten copy has another key")
	}
	for name, k := range map[string]uint64{
		"ip id":    key(1, a, b, 8, udp("<13>x"), 17),
		"src port": key(1, netip.AddrPortFrom(a.Addr(), 40001), b, 7, udp("<13>x"), 17),
		"content":  key(1, a, b, 7, udp("<13>y"), 17),
		"tcp":      key(1, a, b, 7, tcp(1, "<13>x"), 6),
	} {
		if k == base {
			t.Fatalf("%s does not change the key", name)
		}
	}
	if key(1, a, b, 7, tcp(1, "<13>x"), 6) == key(1, a, b, 7, tcp(2, "<13>x"), 6) {
		t.Fatal("tcp sequence does not change the key")
	}
}
