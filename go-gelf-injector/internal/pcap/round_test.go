package pcap_test

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
)

// A direction is closed only when the capture proves its end: a FIN with
// every byte before it, or a RST right after the last byte (SPEC §6.4).
func TestTCPEndClosed(t *testing.T) {
	c := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 10, T0: t0}
	segs := c.Split([]byte(strings.Repeat("x", 300)), 100)
	k := pcap.FlowKey{Src: cli4, Dst: srv4}

	got, _ := assemble(t, c.Frames(segs, false, false))
	if !got[k].closed || got[k].ended != 1 {
		t.Fatalf("FIN: closed %v", got[k].closed)
	}
	got, _ = assemble(t, c.Frames(segs, false, true))
	if got[k].closed {
		t.Fatal("end of capture taken as closed")
	}
	rst := func(seq uint32) pcaptest.Frame {
		ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 6, pcaptest.TCP(cli4.Port(), srv4.Port(), seq, pcaptest.RST, nil), 1)
		return pcaptest.Frame{TS: t0.Add(time.Second), Data: pcaptest.Link(1, ip)}
	}
	frames := append(c.Frames(segs, false, true), rst(10+1+300))
	if got, _ = assemble(t, frames); !got[k].closed {
		t.Fatal("RST after the last byte: not closed")
	}
	// The last segment missing from the capture: the RST is ahead.
	frames = append(c.Frames(segs[:2], false, true), rst(10+1+300))
	if got, _ = assemble(t, frames); got[k].closed {
		t.Fatal("RST ahead of a hole taken as closed")
	}
}

// The vantage of a packet: the pcapng interface, and on Linux cooked the
// interface index and packet type (SPEC §6.3.1).
func TestVantage(t *testing.T) {
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 17, pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("{}")), 1)
	sll2 := func(ifindex uint32, pkttype byte) []byte {
		h := make([]byte, 20)
		binary.BigEndian.PutUint16(h[0:], 0x0800)
		binary.BigEndian.PutUint32(h[4:], ifindex)
		h[10] = pkttype
		return append(h, ip...)
	}
	sll := func(pkttype uint16) []byte {
		h := make([]byte, 16)
		binary.BigEndian.PutUint16(h[0:], pkttype)
		binary.BigEndian.PutUint16(h[14:], 0x0800)
		return append(h, ip...)
	}
	d := pcap.NewDecoder()
	v := func(lt uint16, iface uint32, b []byte) uint64 {
		s, ok := d.Decode(pcap.Packet{TS: t0, LinkType: lt, Data: b, Iface: iface})
		if !ok {
			t.Fatal("not decoded")
		}
		return s.Vantage
	}
	out, in := v(pcap.LinkSLL2, 0, sll2(1, 4)), v(pcap.LinkSLL2, 0, sll2(1, 0))
	eth := v(pcap.LinkSLL2, 0, sll2(2, 0))
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

func pktap(next, dlt uint32, ifname string, inner []byte) []byte {
	h := make([]byte, 108) // sizeof(struct pktap_header) on xnu
	binary.LittleEndian.PutUint32(h[0:], uint32(len(h)))
	binary.LittleEndian.PutUint32(h[4:], next)
	binary.LittleEndian.PutUint32(h[8:], dlt)
	copy(h[12:36], ifname)
	return append(h, inner...)
}

// The inner link type of PKTAP is pth_dlt, not pth_type_next (1 for every
// packet): a macOS -i any capture of the loopback (DLT_NULL) decodes, and
// each interface name is another vantage (ported from go-relplog-injector).
func TestPKTAPInnerLinkType(t *testing.T) {
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 17, pcaptest.UDP(cli4.Port(), srv4.Port(), []byte("{}")), 1)
	d := pcap.NewDecoder()
	lo, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 0, "lo0", pcaptest.Link(pcaptest.Null, ip))})
	if !ok || string(lo.Payload) != "{}" {
		t.Fatalf("loopback in PKTAP: ok=%v %+v stats %+v", ok, lo, d.Stats)
	}
	en, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 1, "en0", pcaptest.Link(pcaptest.Ethernet, ip))})
	if !ok || en.Vantage == lo.Vantage {
		t.Fatalf("en0: ok=%v vantage %x vs lo0 %x", ok, en.Vantage, lo.Vantage)
	}
	if _, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(0, 0, "lo0", nil)}); ok || d.Stats.NotIP != 1 {
		t.Fatalf("a record that is not a packet: ok=%v stats %+v", ok, d.Stats)
	}
}

// Fragments still waiting at the end of a pass are counted lost.
func TestFragmentsWaitingAtEndAreLost(t *testing.T) {
	l4 := pcaptest.UDP(1, 12201, []byte(strings.Repeat("y", 3000)))
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 5, 1480)
	d := pcap.NewDecoder()
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[0])})
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[1])})
	d.Flush()
	if d.Stats.FragmentsLost != 2 {
		t.Fatalf("stats %+v", d.Stats)
	}
}
