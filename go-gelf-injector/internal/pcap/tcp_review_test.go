package pcap_test

import (
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
)

// Finding 10 of the injectors' review, 2026-11-07: the tcp.go change and
// these two tests are the same in the four injectors (from
// go-syslog-injector/internal/pcap/review_test.go).

const tcpFlags = pcap.FlagSYN

func seg(seq uint32, n, captured int) pcap.Segment {
	return pcap.Segment{TS: t0, Proto: pcap.ProtoTCP, Src: cli4, Dst: srv4, Seq: seq, Payload: make([]byte, captured), WireLen: n, Truncated: captured < n}
}

// TestTCPPendingBoundCountsEverySegment (SPEC §6.4): segments the snaplen
// emptied counted nothing against MaxPendingBytes, so a hole followed by them
// held them without bound; each held segment now costs its bookkeeping too.
func TestTCPPendingBoundCountsEverySegment(t *testing.T) {
	_, a := assembler()
	a.Add(pcap.Segment{TS: t0, Proto: pcap.ProtoTCP, Src: cli4, Dst: srv4, Seq: 99, Flags: tcpFlags})
	// Byte 100 never comes; 100 000 empty segments of 10 bytes follow it.
	for i := 0; i < 100000; i++ {
		a.Add(seg(uint32(110+10*i), 10, 0))
	}
	if a.Stats.Gaps == 0 {
		t.Fatalf("the hole was held behind %d empty segments without bound: %+v", a.Stats.OutOfOrder, a.Stats)
	}
}

// TestTCPDrainManySegments (SPEC §6.4): a hole with many segments behind it
// drains in O(n log n); the queue was sorted again at every segment taken
// out (minutes for this one).
func TestTCPDrainManySegments(t *testing.T) {
	got, a := assembler()
	a.Add(pcap.Segment{TS: t0, Proto: pcap.ProtoTCP, Src: cli4, Dst: srv4, Seq: 99, Flags: tcpFlags})
	const n = 30000
	start := time.Now()
	for i := n; i >= 1; i-- { // in reverse, the worst order
		a.Add(seg(uint32(100+i), 1, 1))
	}
	a.Add(seg(100, 1, 1)) // the hole
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("drain took %v", el)
	}
	a.Flush()
	c := got[pcap.FlowKey{Src: cli4, Dst: srv4}]
	if c.data.Len() != n+1 || len(c.gaps) != 0 {
		t.Fatalf("%d bytes, gaps %v", c.data.Len(), c.gaps)
	}
}

// TestFragmentLateCopy (SPEC §6.2, finding 9, from go-syslog-injector):
// tcpdump -i any without NAT writes the copies of a fragment with the same
// reassembly key. Interleaved, the copy of the last fragment came after the
// datagram completed and opened an orphan that expired as lost; it is a copy.
func TestFragmentLateCopy(t *testing.T) {
	l4 := pcaptest.UDP(1, 12201, []byte(`{"short_message":"`+strings.Repeat("y", 3000)+`"}`))
	frags := pcaptest.Fragments4(cli4.Addr(), srv4.Addr(), 17, l4, 5, 1480)
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
	d.Decode(pcap.Packet{TS: t0.Add(time.Second), LinkType: 1, Data: pcaptest.Link(1, frags[2])})
	d.Flush()
	if d.Stats.FragmentsLost != 1 {
		t.Fatalf("a fragment after the window: stats %+v", d.Stats)
	}
}

func assembler() (map[pcap.FlowKey]*collector, *pcap.Assembler) {
	got := map[pcap.FlowKey]*collector{}
	a := pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		c := &collector{mid: mid}
		got[k] = c
		return c
	})
	return got, a
}
