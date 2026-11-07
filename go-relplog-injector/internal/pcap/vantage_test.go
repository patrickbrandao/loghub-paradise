package pcap_test

import (
	"encoding/binary"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
)

// TestVantage: what tells the copies apart -- the pcapng interface, Linux
// cooked v2's interface index and packet type (the loopback shows a packet
// once outgoing, once to us, on one interface: the packet type is from
// go-gelf-injector, 2026-11-07), cooked v1's packet type and link-layer
// address (SPEC §6.8).
func TestVantage(t *testing.T) {
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 17, pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("<13>v")), 1)
	sll2 := func(ifindex uint32, pkttype byte) []byte {
		b := pcaptest.Link(pcaptest.SLL2, ip)
		binary.BigEndian.PutUint32(b[4:8], ifindex)
		b[10] = pkttype
		return b
	}
	sll := func(pkttype uint16) []byte {
		b := pcaptest.Link(pcaptest.SLL, ip)
		binary.BigEndian.PutUint16(b[0:2], pkttype)
		return b
	}
	d := pcap.NewDecoder()
	v := func(lt uint16, iface uint32, b []byte) uint64 {
		t.Helper()
		s, ok := d.Decode(pcap.Packet{TS: t0, LinkType: lt, Data: b, Iface: iface})
		if !ok {
			t.Fatal("not decoded")
		}
		return s.Iface
	}
	out, in, eth := v(pcap.LinkSLL2, 0, sll2(1, 4)), v(pcap.LinkSLL2, 0, sll2(1, 0)), v(pcap.LinkSLL2, 0, sll2(2, 0))
	if out == in || in == eth || out == eth {
		t.Fatalf("SLL2 vantages collide: %x %x %x", out, in, eth)
	}
	if v(pcap.LinkSLL, 0, sll(4)) == v(pcap.LinkSLL, 0, sll(0)) {
		t.Fatal("SLL packet types collide")
	}
	e := pcaptest.Link(1, ip)
	if v(1, 0, e) != 0 || v(1, 1, e) == v(1, 2, e) {
		t.Fatal("pcapng interfaces")
	}
}
