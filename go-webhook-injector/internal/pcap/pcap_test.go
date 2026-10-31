package pcap_test

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap/pcaptest"
)

var (
	cli4 = netip.MustParseAddrPort("10.0.0.1:40000")
	srv4 = netip.MustParseAddrPort("10.0.0.2:514")
	cli6 = netip.MustParseAddrPort("[2001:db8::1]:40000")
	srv6 = netip.MustParseAddrPort("[2001:db8::2]:514")
	t0   = time.Unix(1700000000, 123456000)
)

func readAll(t *testing.T, file []byte) []pcap.Packet {
	t.Helper()
	r, err := pcap.NewReader(bytes.NewReader(file))
	if err != nil {
		t.Fatal(err)
	}
	var out []pcap.Packet
	for {
		p, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		p.Data = append([]byte(nil), p.Data...)
		out = append(out, p)
	}
}

func udpFrame(link int, src, dst netip.AddrPort, payload string, ts time.Time) pcaptest.Frame {
	ip := pcaptest.IP(src.Addr(), dst.Addr(), 17, pcaptest.UDP(src.Port(), dst.Port(), []byte(payload)), 7)
	return pcaptest.Frame{TS: ts, Data: pcaptest.Link(link, ip)}
}

func TestFormatsAndLinks(t *testing.T) {
	for _, link := range []int{pcaptest.Ethernet, pcaptest.SLL, pcaptest.SLL2, pcaptest.Null} {
		for _, ng := range []bool{false, true} {
			frames := []pcaptest.Frame{
				udpFrame(link, cli4, srv4, "<13>hello v4", t0),
				udpFrame(link, cli6, srv6, "<13>hello v6", t0.Add(1500*time.Microsecond)),
			}
			var file []byte
			if ng {
				file = pcaptest.NG(uint16(link), frames)
			} else {
				file = pcaptest.Classic(uint32(link), frames)
			}
			pk := readAll(t, file)
			if len(pk) != 2 {
				t.Fatalf("link %d ng %v: %d packets", link, ng, len(pk))
			}
			if !pk[0].TS.Equal(t0) || pk[1].TS.Sub(pk[0].TS) != 1500*time.Microsecond {
				t.Fatalf("link %d ng %v: timestamps %v %v", link, ng, pk[0].TS, pk[1].TS)
			}
			d := pcap.NewDecoder()
			for i, want := range []struct {
				src, dst netip.AddrPort
				p        string
			}{{cli4, srv4, "<13>hello v4"}, {cli6, srv6, "<13>hello v6"}} {
				s, ok := d.Decode(pk[i])
				if !ok || s.Proto != pcap.ProtoUDP || s.Src != want.src || s.Dst != want.dst || string(s.Payload) != want.p {
					t.Fatalf("link %d ng %v pkt %d: %+v ok=%v", link, ng, i, s, ok)
				}
			}
		}
	}
}

func TestNotPcap(t *testing.T) {
	_, err := pcap.NewReader(strings.NewReader("hello world, not a capture"))
	if !errors.Is(err, pcap.ErrFormat) {
		t.Fatalf("err = %v", err)
	}
}

func TestCutFile(t *testing.T) {
	file := pcaptest.Classic(1, []pcaptest.Frame{udpFrame(1, cli4, srv4, "<13>a", t0), udpFrame(1, cli4, srv4, "<13>b", t0)})
	r, err := pcap.NewReader(bytes.NewReader(file[:len(file)-3]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("cut record: err = %v", err)
	}
}

func TestSnaplenUDP(t *testing.T) {
	f := udpFrame(1, cli4, srv4, "<13>"+strings.Repeat("x", 200), t0)
	f.Snap = 14 + 20 + 8 + 50
	pk := readAll(t, pcaptest.Classic(1, []pcaptest.Frame{f}))
	if !pk[0].Truncated {
		t.Fatal("packet not marked truncated")
	}
	s, ok := pcap.NewDecoder().Decode(pk[0])
	if !ok || !s.Truncated || len(s.Payload) != 50 || s.WireLen != 204 {
		t.Fatalf("segment %+v ok=%v", s, ok)
	}
}

func TestIPv4Fragments(t *testing.T) {
	payload := []byte("<13>" + strings.Repeat("0123456789", 300))
	l4 := pcaptest.UDP(cli4.Port(), srv4.Port(), payload)
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 99, 1480)
	if len(frags) != 3 {
		t.Fatalf("%d fragments", len(frags))
	}
	// Out of order, with one duplicate (tcpdump -i any).
	order := []int{2, 0, 0, 1}
	d := pcap.NewDecoder()
	var got []pcap.Segment
	for _, i := range order {
		s, ok := d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[i])})
		if ok {
			s.Payload = append([]byte(nil), s.Payload...)
			got = append(got, s)
		}
	}
	if len(got) != 1 || !bytes.Equal(got[0].Payload, payload) {
		t.Fatalf("reassembled %d segments", len(got))
	}
	if d.Stats.Reassembled != 1 || d.Stats.FragmentsLost != 0 {
		t.Fatalf("stats %+v", d.Stats)
	}
}

func TestIPv4FragmentExpires(t *testing.T) {
	l4 := pcaptest.UDP(1, 514, []byte("<13>"+strings.Repeat("y", 3000)))
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 5, 1480)
	d := pcap.NewDecoder()
	d.Decode(pcap.Packet{TS: t0, LinkType: 1, Data: pcaptest.Link(1, frags[0])})
	// The rest arrives after the reassembly window: the first is gone.
	for _, f := range frags[1:] {
		if _, ok := d.Decode(pcap.Packet{TS: t0.Add(pcap.FragTimeout + time.Second), LinkType: 1, Data: pcaptest.Link(1, f)}); ok {
			t.Fatal("datagram completed without its first fragment")
		}
	}
	if d.Stats.FragmentsLost == 0 {
		t.Fatal("expired fragment not counted")
	}
}

// collector records what the assembler delivers.
type collector struct {
	data  bytes.Buffer
	gaps  []int
	ended int
	mid   bool
}

func (c *collector) Data(b []byte, _ time.Time) { c.data.Write(b) }
func (c *collector) Gap(n int)                  { c.gaps = append(c.gaps, n) }
func (c *collector) End()                       { c.ended++ }

func assemble(t *testing.T, frames []pcaptest.Frame) (map[pcap.FlowKey]*collector, *pcap.Assembler) {
	t.Helper()
	got := map[pcap.FlowKey]*collector{}
	a := pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		c := &collector{mid: mid}
		got[k] = c
		return c
	})
	d := pcap.NewDecoder()
	for _, f := range frames {
		data := f.Data
		if f.Snap > 0 {
			data = data[:f.Snap]
		}
		s, ok := d.Decode(pcap.Packet{TS: f.TS, LinkType: 1, Data: data, Truncated: f.Snap > 0})
		if ok {
			a.Add(s)
		}
	}
	a.Flush()
	return got, a
}

func TestTCPInOrder(t *testing.T) {
	for _, src := range []netip.AddrPort{cli4, cli6} {
		dst := srv4
		if src.Addr().Is6() {
			dst = srv6
		}
		c := pcaptest.Conn{Link: 1, Src: src, Dst: dst, ISN: 0xfffffff0, T0: t0} // wraps around 2^32
		payload := []byte(strings.Repeat("abcdefghij", 100))
		k := pcap.FlowKey{Src: src, Dst: dst}
		got, a := assemble(t, c.Frames(c.Split(payload, 64), false, false))
		if a.Stats.Unterminated != 0 {
			t.Fatalf("FIN seen, counted unterminated")
		}
		if got[k] == nil || got[k].data.String() != string(payload) || got[k].ended != 1 || got[k].mid || len(got[k].gaps) != 0 {
			t.Fatalf("%v: %+v", src, got[k])
		}
	}
}

func TestTCPReorderRetransmitOverlap(t *testing.T) {
	c := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 1000, T0: t0}
	payload := []byte(strings.Repeat("0123456789", 50))
	segs := c.Split(payload, 40)
	// Swap two, repeat one, add an overlapping one covering 2.5 segments.
	segs[3], segs[4] = segs[4], segs[3]
	segs = append(segs[:6], append([]pcaptest.Segment{segs[5]}, segs[6:]...)...)
	over := pcaptest.Segment{Seq: segs[1].Seq + 10, Payload: payload[50:150], Flags: pcaptest.ACK}
	segs = append(segs[:2], append([]pcaptest.Segment{over}, segs[2:]...)...)
	got, a := assemble(t, c.Frames(segs, false, false))
	col := got[pcap.FlowKey{Src: cli4, Dst: srv4}]
	if col.data.String() != string(payload) {
		t.Fatalf("stream differs:\n got %q\nwant %q", col.data.String(), payload)
	}
	if a.Stats.Retransmitted == 0 || a.Stats.OutOfOrder == 0 || len(col.gaps) != 0 {
		t.Fatalf("stats %+v gaps %v", a.Stats, col.gaps)
	}
}

func TestTCPHoleBecomesGapAtEnd(t *testing.T) {
	c := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 1, T0: t0}
	payload := []byte(strings.Repeat("z", 300))
	segs := c.Split(payload, 100)
	segs = append(segs[:1], segs[2:]...) // lose the middle one
	got, a := assemble(t, c.Frames(segs, false, true))
	col := got[pcap.FlowKey{Src: cli4, Dst: srv4}]
	if a.Stats.Unterminated != 1 {
		t.Fatalf("a direction without FIN not counted: %+v", a.Stats)
	}
	if col.data.Len() != 200 || len(col.gaps) != 1 || col.gaps[0] != 100 || col.ended != 1 {
		t.Fatalf("data %d gaps %v ended %d", col.data.Len(), col.gaps, col.ended)
	}
}

func TestTCPMidstreamAndSnaplen(t *testing.T) {
	c := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 500, T0: t0}
	payload := []byte(strings.Repeat("q", 300))
	frames := c.Frames(c.Split(payload, 100), true, true)
	frames[1].Snap = 14 + 20 + 20 + 30 // second segment keeps 30 of 100 bytes
	got, a := assemble(t, frames)
	col := got[pcap.FlowKey{Src: cli4, Dst: srv4}]
	if !col.mid || a.Stats.Midstream != 1 {
		t.Fatal("not midstream")
	}
	if col.data.Len() != 230 || len(col.gaps) != 1 || col.gaps[0] != 70 {
		t.Fatalf("data %d gaps %v", col.data.Len(), col.gaps)
	}
}

func TestTCPPortReuse(t *testing.T) {
	c1 := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 100, T0: t0}
	c2 := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 900000, T0: t0.Add(time.Second)}
	f := append(c1.Frames(c1.Split([]byte("first"), 10), false, true), c2.Frames(c2.Split([]byte("second"), 10), false, false)...)
	ends := 0
	var all bytes.Buffer
	a := pcap.NewAssembler(func(pcap.FlowKey, bool) pcap.StreamHandler {
		return &funcHandler{data: func(b []byte) { all.Write(b) }, end: func() { ends++; all.WriteByte('|') }}
	})
	d := pcap.NewDecoder()
	for _, fr := range f {
		if s, ok := d.Decode(pcap.Packet{TS: fr.TS, LinkType: 1, Data: fr.Data}); ok {
			a.Add(s)
		}
	}
	a.Flush()
	if all.String() != "first|second|" || ends != 2 {
		t.Fatalf("got %q ends %d", all.String(), ends)
	}
}

type funcHandler struct {
	data func([]byte)
	end  func()
}

func (h *funcHandler) Data(b []byte, _ time.Time) { h.data(b) }
func (h *funcHandler) Gap(int)                    {}
func (h *funcHandler) End()                       { h.end() }

// FuzzReader: no input may panic the reader or the decoder.
func FuzzReader(f *testing.F) {
	f.Add(pcaptest.Classic(1, []pcaptest.Frame{udpFrame(1, cli4, srv4, "<13>x", t0)}))
	f.Add(pcaptest.NG(1, []pcaptest.Frame{udpFrame(1, cli6, srv6, "<13>x", t0)}))
	f.Add(pcaptest.Classic(113, []pcaptest.Frame{udpFrame(113, cli4, srv4, "<13>x", t0)}))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := pcap.NewReader(bytes.NewReader(b))
		if err != nil {
			return
		}
		d := pcap.NewDecoder()
		a := pcap.NewAssembler(func(pcap.FlowKey, bool) pcap.StreamHandler { return &collector{} })
		for range 1000 {
			p, err := r.Next()
			if err != nil {
				break
			}
			if s, ok := d.Decode(p); ok {
				a.Add(s)
			}
		}
		a.Flush()
	})
}
