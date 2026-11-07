package pcap_test

import (
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap"
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

func assembler() (map[pcap.FlowKey]*collector, *pcap.Assembler) {
	got := map[pcap.FlowKey]*collector{}
	a := pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		c := &collector{mid: mid}
		got[k] = c
		return c
	})
	return got, a
}
