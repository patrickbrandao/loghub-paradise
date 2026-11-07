package pcap

import (
	"net/netip"
	"sort"
	"time"
)

// TCP reassembly (SPEC §6.4): one byte stream per direction of a connection,
// in sequence order, without retransmitted bytes, with the holes the capture
// lost reported as gaps so the parser above can resynchronize.

// FlowKey identifies one direction of a TCP connection.
type FlowKey struct {
	Src, Dst netip.AddrPort
}

// StreamHandler consumes one direction of a connection.
type StreamHandler interface {
	// Data delivers the next in-order bytes; ts is the capture time of the
	// segment that carried them. b is only valid during the call.
	Data(b []byte, ts time.Time)
	// Gap reports n bytes (n > 0) the capture does not have, right before
	// the next Data. n is a lower bound when the size is unknown.
	Gap(n int)
	// End is called once, at FIN/RST or at the end of the capture.
	End()
}

// NewStreamFunc is called for the first segment of a direction. midstream is
// true when the capture began after the SYN: the first byte is not the start
// of the stream. Returning nil ignores the direction for its lifetime.
type NewStreamFunc func(k FlowKey, midstream bool) StreamHandler

// Limits of the assembler.
const (
	// MaxPendingBytes is the out-of-order data held per direction while a
	// hole waits to be filled; past it, the hole is declared a gap.
	MaxPendingBytes = 4 << 20
	// MaxFlows bounds the open directions; new ones beyond it are ignored.
	MaxFlows = 1 << 17
	// FlowIdle closes a direction with no segment for this long in capture
	// time, so a capture of a long-lived emitter does not hold dead flows.
	FlowIdle = 10 * time.Minute
)

// AsmStats counts the assembler's decisions.
type AsmStats struct {
	Flows         uint64 // directions opened
	Midstream     uint64 // of those, without a SYN in the capture
	Ignored       uint64 // directions the handler refused
	Overflow      uint64 // directions refused past MaxFlows
	Retransmitted uint64 // bytes received twice and dropped
	OutOfOrder    uint64 // segments that arrived ahead of a hole
	Gaps          uint64 // holes reported
	GapBytes      uint64 // bytes in those holes
	// Unterminated counts directions that ended with the capture (or idle)
	// without a FIN or RST: their tail may be missing without any visible
	// hole -- a tcpdump stopped too early, or still behind on its buffer.
	Unterminated uint64
}

type pending struct {
	seq  uint32
	data []byte
	wire int
	ts   time.Time
}

type flow struct {
	h       StreamHandler
	next    uint32
	pend    []pending
	pendLen int
	last    time.Time
	id      uint64
	finSeen bool
	finSeq  uint32
}

// Assembler reassembles TCP directions from segments in capture order.
type Assembler struct {
	New   NewStreamFunc
	Stats AsmStats
	flows map[FlowKey]*flow
	sweep time.Time
	ids   uint64
}

// NewAssembler returns an assembler that asks newStream for each direction.
func NewAssembler(newStream NewStreamFunc) *Assembler {
	return &Assembler{New: newStream, flows: make(map[FlowKey]*flow)}
}

func seqLT(a, b uint32) bool { return int32(a-b) < 0 }

// Add feeds one TCP segment.
func (a *Assembler) Add(s Segment) {
	if s.Proto != ProtoTCP {
		return
	}
	a.expire(s.TS)
	k := FlowKey{Src: s.Src, Dst: s.Dst}
	f := a.flows[k]
	syn := s.Flags&FlagSYN != 0
	if f != nil && syn && f.h != nil && s.Seq+1 != f.next {
		// A new connection on the same 4-tuple (port reuse).
		a.end(k, f)
		f = nil
	}
	if f == nil {
		if len(a.flows) >= MaxFlows {
			a.Stats.Overflow++
			return
		}
		a.Stats.Flows++
		mid := !syn
		if mid {
			a.Stats.Midstream++
			// A pure ACK or a RST without data says nothing yet: do not open
			// a midstream direction for it.
			if len(s.Payload) == 0 {
				a.Stats.Flows--
				a.Stats.Midstream--
				return
			}
		}
		a.ids++
		f = &flow{h: a.New(k, mid), id: a.ids}
		if f.h == nil {
			a.Stats.Ignored++
		}
		f.next = s.Seq
		if syn {
			f.next = s.Seq + 1
		}
		a.flows[k] = f
	}
	f.last = s.TS
	if f.h == nil {
		if s.Flags&(FlagFIN|FlagRST) != 0 {
			delete(a.flows, k)
		}
		return
	}
	seq := s.Seq
	if syn {
		seq++ // data on a SYN (TCP Fast Open) starts after it
	}
	wireEnd := seq + uint32(s.WireLen)
	if s.Flags&FlagFIN != 0 {
		f.finSeen, f.finSeq = true, wireEnd
	}
	if s.Flags&FlagRST != 0 {
		a.deliver(f, seq, s.Payload, s.WireLen, s.TS)
		a.end(k, f)
		return
	}
	a.deliver(f, seq, s.Payload, s.WireLen, s.TS)
	if f.finSeen && !seqLT(f.next, f.finSeq) {
		a.end(k, f)
	}
}

// deliver places the bytes [seq, seq+wire) of which data is the captured
// prefix.
func (a *Assembler) deliver(f *flow, seq uint32, data []byte, wire int, ts time.Time) {
	if wire == 0 {
		return
	}
	if seqLT(f.next, seq) {
		// Ahead of a hole: hold it.
		a.Stats.OutOfOrder++
		f.pend = append(f.pend, pending{seq: seq, data: append([]byte(nil), data...), wire: wire, ts: ts})
		f.pendLen += len(data)
		if f.pendLen > MaxPendingBytes {
			a.skipToPending(f)
		}
		return
	}
	a.take(f, seq, data, wire, ts)
	a.drain(f)
}

// take consumes a segment that starts at or before f.next.
func (a *Assembler) take(f *flow, seq uint32, data []byte, wire int, ts time.Time) {
	end := seq + uint32(wire)
	if !seqLT(f.next, end) {
		a.Stats.Retransmitted += uint64(wire)
		return
	}
	skip := int(f.next - seq)
	a.Stats.Retransmitted += uint64(skip)
	if skip < len(data) {
		f.h.Data(data[skip:], ts)
		skip = len(data)
	}
	// Bytes the snaplen cut off: a gap of their size.
	if missing := int(end - (seq + uint32(skip))); skip >= len(data) && missing > 0 {
		a.gap(f, missing)
	}
	f.next = end
}

func (a *Assembler) drain(f *flow) {
	for len(f.pend) > 0 {
		sort.Slice(f.pend, func(i, j int) bool { return seqLT(f.pend[i].seq, f.pend[j].seq) })
		p := f.pend[0]
		if seqLT(f.next, p.seq) {
			return
		}
		f.pend = f.pend[1:]
		f.pendLen -= len(p.data)
		a.take(f, p.seq, p.data, p.wire, p.ts)
	}
	f.pendLen = 0
}

// skipToPending declares the hole before the first held segment lost.
func (a *Assembler) skipToPending(f *flow) {
	if len(f.pend) == 0 {
		return
	}
	sort.Slice(f.pend, func(i, j int) bool { return seqLT(f.pend[i].seq, f.pend[j].seq) })
	first := f.pend[0].seq
	if seqLT(f.next, first) {
		a.gap(f, int(first-f.next))
		f.next = first
	}
	a.drain(f)
}

func (a *Assembler) gap(f *flow, n int) {
	a.Stats.Gaps++
	a.Stats.GapBytes += uint64(n)
	f.h.Gap(n)
}

func (a *Assembler) end(k FlowKey, f *flow) {
	for len(f.pend) > 0 {
		a.skipToPending(f)
	}
	if f.h != nil {
		f.h.End()
	}
	delete(a.flows, k)
}

func (a *Assembler) expire(now time.Time) {
	if now.Sub(a.sweep) < time.Minute {
		return
	}
	a.sweep = now
	a.endWhere(func(f *flow) bool { return now.Sub(f.last) > FlowIdle })
}

// Flush ends every open direction (end of the capture).
func (a *Assembler) Flush() { a.endWhere(func(*flow) bool { return true }) }

func (a *Assembler) countUnterminated(f *flow) {
	if f.h != nil {
		a.Stats.Unterminated++
	}
}

// endWhere ends the matching directions in the order they opened, so what
// their End emits comes out the same on every run.
func (a *Assembler) endWhere(match func(*flow) bool) {
	var keys []FlowKey
	for k, f := range a.flows {
		if match(f) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return a.flows[keys[i]].id < a.flows[keys[j]].id })
	for _, k := range keys {
		a.countUnterminated(a.flows[k])
		a.end(k, a.flows[k])
	}
}
