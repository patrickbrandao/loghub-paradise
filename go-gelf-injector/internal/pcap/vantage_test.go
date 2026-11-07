package pcap_test

import (
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
)

// Cooked v1 does not record the interface: its vantage is the packet type
// and the link-layer address (alignment 8(d) of the review of 2026-11-07,
// the rule of the four injectors). The packet type alone made the copies of
// two interfaces of the same type one vantage.
func TestCookedV1Vantage(t *testing.T) {
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 17, pcaptest.UDP(1, 12201, []byte(`{"a":1}`)), 9)
	frame := func(pkttype byte, mac byte) []byte {
		b := pcaptest.Link(pcaptest.SLL, ip)
		b[1] = pkttype
		b[4], b[5] = 0, 6 // address length
		copy(b[6:12], []byte{2, 0, 0, 0, 0, mac})
		return b
	}
	d := pcap.NewDecoder()
	v := func(b []byte) uint64 {
		s, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcaptest.SLL, Data: b})
		if !ok {
			t.Fatal("not decoded")
		}
		return s.Vantage
	}
	a, b, a2, out := v(frame(0, 1)), v(frame(0, 2)), v(frame(0, 1)), v(frame(4, 1))
	if a == b || a != a2 || a == out || a == 0 {
		t.Fatalf("vantages %x %x %x %x", a, b, a2, out)
	}
}
