package pcap_test

// IP reassembly fixes ported from go-syslog-injector, go-relplog-injector and
// go-gelf-injector on 2026-11-07 (SPEC §6.2, §6.3). The fragments carry TCP,
// what WI replays; the rules are the same for any protocol.

import (
	"encoding/binary"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap/pcaptest"
)

// bigSegment is a TCP segment of an HTTP request large enough for 3
// fragments of 1480 bytes.
func bigSegment(seq uint32) []byte {
	req := "POST /hook HTTP/1.1\r\nHost: x\r\nContent-Length: 4000\r\n\r\n" + strings.Repeat("z", 4000)
	return pcaptest.TCP(cli4.Port(), 8080, seq, pcaptest.ACK|pcaptest.PSH, []byte(req))
}

// frag6 builds one IPv6 fragment (fragment header right after the fixed
// header) of the upper-layer bytes l4[off:off+n].
func frag6(src, dst netip.Addr, nh uint8, id uint32, l4 []byte, off, n int) []byte {
	more := off+n < len(l4)
	data := l4[off : off+n]
	b := make([]byte, 48, 48+len(data))
	b[0] = 6 << 4
	binary.BigEndian.PutUint16(b[4:6], uint16(8+len(data)))
	b[6], b[7] = 44, 64
	copy(b[8:24], src.AsSlice())
	copy(b[24:40], dst.AsSlice())
	b[40] = nh
	ff := uint16(off/8) << 3
	if more {
		ff |= 1
	}
	binary.BigEndian.PutUint16(b[42:44], ff)
	binary.BigEndian.PutUint32(b[44:48], id)
	return append(b, data...)
}

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
// capture of the loopback (DLT_NULL) decodes (SPEC §6.2; from SI/RI/GI).
func TestPKTAPInnerLinkType(t *testing.T) {
	const line = "GET / HTTP/1.1\r\n"
	ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 6, pcaptest.TCP(cli4.Port(), 8080, 1, pcaptest.ACK, []byte(line)), 1)
	d := pcap.NewDecoder()
	lo, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 0, "lo0", pcaptest.Link(pcaptest.Null, ip))})
	if !ok || string(lo.Payload) != line {
		t.Fatalf("loopback in PKTAP: ok=%v %+v stats %+v", ok, lo, d.Stats)
	}
	if en, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(1, 1, "en0", pcaptest.Link(pcaptest.Ethernet, ip))}); !ok || string(en.Payload) != line {
		t.Fatalf("en0 in PKTAP: ok=%v stats %+v", ok, d.Stats)
	}
	if _, ok := d.Decode(pcap.Packet{TS: t0, LinkType: pcap.LinkPKTAP, Data: pktap(0, 0, "lo0", nil)}); ok || d.Stats.NotIP != 1 {
		t.Fatalf("a record that is not a packet: ok=%v stats %+v", ok, d.Stats)
	}
}

// TestFragmentCutBySnaplen (SPEC §6.3): a fragment cut by the snaplen drops
// its datagram at once -- the fragments already held and those that come
// later -- and the datagram is counted once.
func TestFragmentCutBySnaplen(t *testing.T) {
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 6, bigSegment(1), 11, 1480)
	if len(frags) != 3 {
		t.Fatalf("%d fragments", len(frags))
	}
	d := pcap.NewDecoder()
	dec := func(f []byte, snap int) bool {
		data := pcaptest.Link(1, f)
		trunc := false
		if snap > 0 {
			data, trunc = data[:snap], true
		}
		_, ok := d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: data, Truncated: trunc})
		return ok
	}
	if dec(frags[0], 0) || dec(frags[1], 14+20+100) || dec(frags[2], 0) {
		t.Fatal("datagram completed without the bytes the snaplen cut")
	}
	// The first one (held, then dropped), the cut one and the last one
	// (dropped on arrival, not held for FragTimeout).
	if d.Stats.FragmentsLost != 3 || d.Stats.DatagramsSnaplen != 1 {
		t.Fatalf("stats %+v", d.Stats)
	}
	// A second cut copy (tcpdump -i any) is one more fragment, not one more
	// datagram.
	if dec(frags[1], 14+20+100) || d.Stats.FragmentsLost != 4 || d.Stats.DatagramsSnaplen != 1 {
		t.Fatalf("stats %+v", d.Stats)
	}
	// Another datagram (another IP ID) still reassembles.
	ok := false
	for _, f := range pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 6, bigSegment(1), 12, 1480) {
		ok = dec(f, 0)
	}
	if !ok || d.Stats.Reassembled != 1 {
		t.Fatalf("control datagram not reassembled: %+v", d.Stats)
	}

	// IPv6: the same rule, keyed by the fragment header's id.
	l4 := bigSegment(1)
	d = pcap.NewDecoder()
	ip6 := func(id uint32, off, n int) []byte { return frag6(cli6.Addr(), srv6.Addr(), 6, id, l4, off, n) }
	if dec(ip6(7, 0, 1448), 0) || dec(ip6(7, 1448, 1448), 14+48+100) || dec(ip6(7, 2896, len(l4)-2896), 0) {
		t.Fatal("IPv6 datagram completed without the bytes the snaplen cut")
	}
	if d.Stats.FragmentsLost != 3 || d.Stats.DatagramsSnaplen != 1 {
		t.Fatalf("IPv6 stats %+v", d.Stats)
	}
	ok = dec(ip6(8, 0, 1448), 0) || dec(ip6(8, 1448, 1448), 0) || dec(ip6(8, 2896, len(l4)-2896), 0)
	if !ok || d.Stats.Reassembled != 1 {
		t.Fatalf("IPv6 control datagram not reassembled: %+v", d.Stats)
	}
}

// TestFragmentsWaitingAtEndAreLost: Flush (the end of a pass) counts what
// still waited for its datagram (SPEC §6.3).
func TestFragmentsWaitingAtEndAreLost(t *testing.T) {
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 6, bigSegment(1), 5, 1480)
	d := pcap.NewDecoder()
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[0])})
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[1])})
	d.Flush()
	if d.Stats.FragmentsLost != 2 {
		t.Fatalf("stats %+v", d.Stats)
	}
}

// TestFragmentLateCopy (SPEC §6.3): tcpdump -i any without NAT writes the
// copies of a fragment with the same reassembly key. Interleaved, the copy of
// the last fragment came after the datagram completed and opened an orphan
// that expired as lost; it is a copy.
func TestFragmentLateCopy(t *testing.T) {
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 6, bigSegment(1), 5, 1480)
	if len(frags) != 3 {
		t.Fatalf("%d fragments", len(frags))
	}
	d := pcap.NewDecoder()
	whole := 0
	for i, f := range frags {
		for c := 0; c < 2; c++ { // the veth copy, then the eth0 copy
			ts := t0.Add(time.Duration(i*100+c*20) * time.Microsecond)
			if _, ok := d.Decode(pcap.Packet{TS: ts, LinkType: 1, Data: pcaptest.Link(1, f)}); ok {
				whole++
			}
		}
	}
	d.Flush()
	if whole != 1 || d.Stats.Reassembled != 1 || d.Stats.FragmentsLost != 0 {
		t.Fatalf("%d datagrams, stats %+v", whole, d.Stats)
	}
	// Past the copies' window, the same key is a new datagram (the IP ID
	// came round): it waits for its other fragments, and they are lost.
	d.Decode(pcap.Packet{TS: t0.Add(pcap.DedupWindow + 10*time.Millisecond), LinkType: 1, Data: pcaptest.Link(1, frags[2])})
	d.Flush()
	if d.Stats.FragmentsLost != 1 {
		t.Fatalf("a fragment after the window: stats %+v", d.Stats)
	}
	// The window holds both ways: a capture whose clock steps back a second
	// is not within the copies' window either (expiry alone, which only
	// looks forward, would keep the completed datagram).
	d = pcap.NewDecoder()
	for _, f := range frags {
		d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, f)})
	}
	d.Decode(pcap.Packet{TS: t0.Add(-time.Second), LinkType: 1, Data: pcaptest.Link(1, frags[2])})
	d.Flush()
	if d.Stats.Reassembled != 1 || d.Stats.FragmentsLost != 1 {
		t.Fatalf("a fragment before the window: stats %+v", d.Stats)
	}
}
