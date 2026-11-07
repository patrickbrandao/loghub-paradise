package pcap_test

// Tests of the fixes ported from go-relplog-injector and go-gelf-injector on
// 2026-11-07 (SPEC §6.2, §6.3, §6.4, §6.9).

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap/pcaptest"
)

// pktap frames a link-layer frame of type dlt in xnu's pktap_header.
func pktap(next, dlt uint32, ifname string, frame []byte) []byte {
	h := make([]byte, 108) // sizeof(struct pktap_header) on xnu
	binary.LittleEndian.PutUint32(h[0:], 108)
	binary.LittleEndian.PutUint32(h[4:], next)
	binary.LittleEndian.PutUint32(h[8:], dlt)
	copy(h[12:36], ifname)
	return append(h, frame...)
}

// TestPKTAPInnerLinkType: the inner link type is pth_dlt, not pth_type_next
// (which is 1 for every packet, the number of Ethernet): a macOS -i any
// capture of the loopback (DLT_NULL) decodes, and each interface name is
// another vantage (SPEC §6.2, §6.9; from go-relplog-injector).
func TestPKTAPInnerLinkType(t *testing.T) {
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 17, pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("<13>lo0")), 1)
	d := pcap.NewDecoder()
	lo, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 0, "lo0", pcaptest.Link(pcaptest.Null, ip))})
	if !ok || string(lo.Payload) != "<13>lo0" {
		t.Fatalf("loopback in PKTAP: ok=%v %+v stats %+v", ok, lo, d.Stats)
	}
	en, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 1, "en0", pcaptest.Link(pcaptest.Ethernet, ip))})
	if !ok || en.Iface == lo.Iface {
		t.Fatalf("en0: ok=%v iface %x vs lo0 %x", ok, en.Iface, lo.Iface)
	}
	if _, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(0, 0, "lo0", nil)}); ok || d.Stats.NotIP != 1 {
		t.Fatalf("a record that is not a packet: ok=%v stats %+v", ok, d.Stats)
	}
}

// TestFragmentsWaitingAtEndAreLost: Flush (the end of a pass) counts what
// still waited for its datagram (SPEC §6.3; from go-relplog-injector).
func TestFragmentsWaitingAtEndAreLost(t *testing.T) {
	l4 := pcaptest.UDP(1, 514, []byte("<13>"+strings.Repeat("y", 3000)))
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 5, 1480)
	d := pcap.NewDecoder()
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[0])})
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[1])})
	d.Flush()
	if d.Stats.FragmentsLost != 2 {
		t.Fatalf("stats %+v", d.Stats)
	}
}

// TestVantage: what tells the copies apart -- the pcapng interface, Linux
// cooked v2's interface index and packet type, cooked v1's packet type and
// link-layer address (SPEC §6.9).
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

// TestDedupRule: a copy only from another vantage, at most one per vantage
// for each original, within the window (SPEC §6.9).
func TestDedupRule(t *testing.T) {
	d := pcap.NewDedup()
	seg := func(iface uint64, dt time.Duration, p string) pcap.Segment {
		return pcap.Segment{Proto: pcap.ProtoUDP, Src: cli4, Dst: srv4, Payload: []byte(p), WireLen: len(p), TS: t0.Add(dt), Iface: iface}
	}
	// A on veth (1), its copy on eth0 (2); the sender sends the same bytes
	// again (B): B on 1 is new, B on 2 is B's copy; a third vantage (3)
	// gives each original one more copy, and no more.
	steps := []struct {
		iface uint64
		dt    time.Duration
		copy  bool
	}{{1, 0, false}, {2, time.Millisecond, true}, {1, 2 * time.Millisecond, false}, {2, 3 * time.Millisecond, true}, {3, 4 * time.Millisecond, true}, {3, 5 * time.Millisecond, true}, {3, 6 * time.Millisecond, false}}
	for i, s := range steps {
		if got := d.Copy(seg(s.iface, s.dt, "<13>same")); got != s.copy {
			t.Fatalf("step %d: copy=%v want %v", i, got, s.copy)
		}
	}
	if d.Dropped != 4 {
		t.Fatalf("dropped %d", d.Dropped)
	}
	// Past the window it is a new original, even on another vantage.
	if d.Copy(seg(9, pcap.DedupWindow+10*time.Millisecond, "<13>same")) {
		t.Fatal("copy outside the window")
	}
	// The same vantage never makes a copy.
	for i := range 3 {
		if d.Copy(seg(0, time.Second+time.Duration(i), "<13>other")) {
			t.Fatal("same vantage taken as a copy")
		}
	}
	// The addresses and the destination port stay out of the key (NAT).
	a, b := seg(1, 2*time.Second, "<13>nat"), seg(2, 2*time.Second+time.Microsecond, "<13>nat")
	b.Dst = srv6
	if d.Copy(a) || !d.Copy(b) {
		t.Fatal("a NAT-rewritten copy was not dropped")
	}
}
