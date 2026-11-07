package pcap_test

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
)

// sll2 frames an IP packet in Linux cooked v2 with an interface index.
func sll2(ifindex uint32, ip []byte) []byte {
	b := pcaptest.Link(pcaptest.SLL2, ip)
	binary.BigEndian.PutUint32(b[4:8], ifindex)
	return b
}

// pktap frames a link-layer frame of type dlt in xnu's pktap_header.
func pktap(next, dlt uint32, ifname string, frame []byte) []byte {
	h := make([]byte, 108)
	binary.LittleEndian.PutUint32(h[0:], 108)
	binary.LittleEndian.PutUint32(h[4:], next)
	binary.LittleEndian.PutUint32(h[8:], dlt)
	copy(h[12:36], ifname)
	return append(h, frame...)
}

// TestPKTAPInnerLinkType: the inner link type is pth_dlt, not pth_type_next
// (which is 1 for every packet, the number of Ethernet): a macOS -i any
// capture of the loopback (DLT_NULL) decodes, and each interface name gives
// a different interface identity (SPEC §6.2).
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

// TestSnaplenFragmentCondemnsDatagram: the first fragment cut by the snaplen
// drops at once the fragments the datagram held and the ones after it, and
// counts the datagram once (SPEC §6.3, pendência 4).
func TestSnaplenFragmentCondemnsDatagram(t *testing.T) {
	l4 := pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("<13>"+strings.Repeat("f", 4000)))
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 77, 1480)
	if len(frags) != 3 {
		t.Fatalf("%d fragments", len(frags))
	}
	d := pcap.NewDecoder()
	dec := func(f []byte, cut bool) bool {
		data := pcaptest.Link(1, f)
		if cut {
			data = data[:200]
		}
		_, ok := d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: data, Truncated: cut})
		return ok
	}
	if dec(frags[0], false) || dec(frags[1], true) || dec(frags[2], false) {
		t.Fatal("a condemned datagram was delivered")
	}
	if d.Stats.SnaplenDatagrams != 1 || d.Stats.SnaplenUDP != 1 || d.Stats.FragmentsLost != 3 {
		t.Fatalf("stats %+v (want 1 datagram, 3 fragments lost at once)", d.Stats)
	}
	// Another datagram after it is untouched.
	l4b := pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("<13>"+strings.Repeat("g", 2000)))
	var whole bool
	for _, f := range pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4b, 78, 1480) {
		whole = dec(f, false)
	}
	if !whole {
		t.Fatal("the next datagram was not rebuilt")
	}
	d.Flush()
	if d.Stats.FragmentsLost != 3 {
		t.Fatalf("flush counted the condemned datagram again: %+v", d.Stats)
	}
}

// TestFragmentsWaitingAtEndAreLost: Flush counts what still waited.
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

func TestDedupRule(t *testing.T) {
	d := pcap.NewDedup()
	k := pcap.Key(pcap.Segment{Proto: pcap.ProtoUDP, Payload: []byte("<13>same")})
	// A on veth (1), its copy on eth0 (2); the sender sends the same bytes
	// again (B): B on 1 is new, B on 2 is B's copy.
	steps := []struct {
		iface uint64
		dt    time.Duration
		copy  bool
	}{{1, 0, false}, {2, time.Millisecond, true}, {1, 2 * time.Millisecond, false}, {2, 3 * time.Millisecond, true}, {3, 4 * time.Millisecond, true}, {3, 5 * time.Millisecond, true}, {3, 6 * time.Millisecond, false}}
	for i, s := range steps {
		if got := d.Copy(k, s.iface, t0.Add(s.dt)); got != s.copy {
			t.Fatalf("step %d: copy=%v want %v", i, got, s.copy)
		}
	}
	// Past the window it is a new original, even on another interface.
	if d.Copy(k, 9, t0.Add(pcap.DedupWindow+10*time.Millisecond)) {
		t.Fatal("copy outside the window")
	}
	// The same interface never makes a copy.
	k2 := pcap.Key(pcap.Segment{Proto: pcap.ProtoUDP, Payload: []byte("<13>other")})
	for i := range 3 {
		if d.Copy(k2, 0, t0.Add(time.Second+time.Duration(i))) {
			t.Fatal("same interface taken as a copy")
		}
	}
}
