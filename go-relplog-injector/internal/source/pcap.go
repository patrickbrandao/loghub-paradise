package source

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/extract"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap"
)

// PcapOptions select what is replayed from a capture (SPEC §6.1).
type PcapOptions struct {
	// Ports, when not empty, keeps only traffic to these destination ports
	// and drops the content check (any payload there is syslog).
	Ports map[uint16]bool
	// UDP and TCP enable each protocol (both by default).
	UDP, TCP bool
	Loop     bool
	Header   Header // FormatRaw by default: the captured message as is
	// Dedup drops the copies of tcpdump -i any (SPEC §6.8).
	Dedup bool
}

// endHandler is the extractor of one TCP direction that also marks, in the
// queue, where its flow ended (SPEC §6.9).
type endHandler struct {
	*extract.Stream
	p  *Pcap
	id uint64
}

func (h *endHandler) End() {
	h.Stream.End()
	h.p.queue = append(h.p.queue, Message{endOf: h.id})
}

// Pcap replays the syslog messages of a capture, in capture order.
type Pcap struct {
	path string
	opt  PcapOptions

	f     *os.File
	rd    *pcap.Reader
	dec   *pcap.Decoder
	asm   *pcap.Assembler
	ext   extract.Stats
	queue []Message
	first time.Time // capture time of the first packet of the pass
	base  time.Duration
	last  time.Duration
	pass  uint64
	sent  uint64
	// passSent counts the messages of the current pass: a pass with none ends
	// --loop (a capture rewritten empty between passes would spin forever).
	passSent uint64
	cut      uint64 // passes that ended at a truncated record

	dedup   *pcap.Dedup
	udpDups uint64
	flowIDs uint64                       // last flow id given
	udpFlow map[[2]netip.AddrPort]uint64 // UDP flows of the pass
	ends    []uint64                     // flows ended before the next message

	// totals of the previous passes, so the report covers the whole run
	decTotal pcap.Stats
	asmTotal pcap.AsmStats
	extTotal extract.Stats
}

// NewPcap opens a capture. A file that is not pcap/pcapng fails here, before
// the run starts.
func NewPcap(path string, opt PcapOptions) (*Pcap, error) {
	p := &Pcap{path: path, opt: opt}
	if err := p.open(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Pcap) open() error {
	f, err := os.Open(p.path)
	if err != nil {
		return err
	}
	rd, err := pcap.NewReader(f)
	if err != nil {
		f.Close()
		return fmt.Errorf("%s: %w", p.path, err)
	}
	p.f, p.rd = f, rd
	p.dec = pcap.NewDecoder()
	strict := len(p.opt.Ports) == 0
	p.asm = pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		if !p.wantPort(k.Dst.Port()) {
			return nil
		}
		src, dst := k.Src, k.Dst
		p.flowIDs++
		id := p.flowIDs
		h := &endHandler{p: p, id: id}
		h.Stream = extract.NewStream(strict, mid, &p.ext, func(b []byte, ts time.Time) {
			p.push(b, ts, src, dst, id)
		})
		return h
	})
	p.dedup = nil
	if p.opt.Dedup {
		p.dedup = pcap.NewDedup()
		p.asm.Dedup = p.dedup
	}
	p.udpFlow = map[[2]netip.AddrPort]uint64{}
	p.first = time.Time{}
	p.pass++
	p.passSent = 0
	return nil
}

func (p *Pcap) wantPort(port uint16) bool {
	return len(p.opt.Ports) == 0 || p.opt.Ports[port]
}

func (p *Pcap) push(b []byte, ts time.Time, src, dst netip.AddrPort, flow uint64) {
	at := time.Duration(-1)
	if !ts.IsZero() {
		if p.first.IsZero() {
			p.first = ts
		}
		at = p.base + ts.Sub(p.first)
		if at < p.base {
			at = p.base // a capture whose clock steps back
		}
		if at > p.last {
			p.last = at
		}
	}
	p.queue = append(p.queue, Message{Data: append([]byte(nil), b...), At: at, Src: src, Dst: dst, Flow: flow})
}

// udpFlowID returns the flow of a UDP source/destination pair in this pass.
func (p *Pcap) udpFlowID(src, dst netip.AddrPort) uint64 {
	k := [2]netip.AddrPort{src, dst}
	id := p.udpFlow[k]
	if id == 0 {
		p.flowIDs++
		id = p.flowIDs
		p.udpFlow[k] = id
	}
	return id
}

// Next implements Source.
func (p *Pcap) Next(_ uint64, now time.Time) (Message, error) {
	var m Message
	for {
		for len(p.queue) == 0 {
			if err := p.step(); err != nil {
				if !errors.Is(err, io.EOF) {
					return Message{}, err
				}
				if !p.opt.Loop || p.passSent == 0 {
					return Message{}, io.EOF
				}
				if err := p.reopen(); err != nil {
					return Message{}, err
				}
			}
		}
		m = p.queue[0]
		p.queue[0] = Message{}
		p.queue = p.queue[1:]
		if m.endOf == 0 {
			break
		}
		p.ends = append(p.ends, m.endOf)
	}
	if len(p.ends) > 0 {
		m.EndFlows, p.ends = p.ends, nil
	}
	if p.opt.Header.Format != FormatRaw {
		m.Data = p.opt.Header.Wrap(make([]byte, 0, len(m.Data)+96), m.Data, now)
	}
	p.sent++
	p.passSent++
	return m, nil
}

// step reads one packet; io.EOF after flushing the open streams at the end of
// the pass.
func (p *Pcap) step() error {
	pk, err := p.rd.Next()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// tcpdump still writing, or killed: replay what is whole.
			p.cut++
			err = io.EOF
		}
		if errors.Is(err, io.EOF) && p.asm != nil {
			p.asm.Flush()
			p.addAsm(p.asm.Stats)
			p.asm = nil
			p.dec.Flush() // fragments still waiting are lost
			// The UDP flows of the pass end with it.
			for _, id := range p.udpFlow {
				p.queue = append(p.queue, Message{endOf: id})
			}
			p.udpFlow = map[[2]netip.AddrPort]uint64{}
			if len(p.queue) > 0 {
				return nil
			}
		}
		return err
	}
	if p.first.IsZero() && !pk.TS.IsZero() {
		p.first = pk.TS // offsets count from the first packet, not the first message
	}
	seg, ok := p.dec.Decode(pk)
	if !ok {
		return nil
	}
	switch seg.Proto {
	case pcap.ProtoUDP:
		if !p.opt.UDP || !p.wantPort(seg.Dst.Port()) {
			return nil
		}
		// After the port filter, so a copy it refuses does not shadow the
		// one it keeps (NAT may rewrite the port between copies).
		if p.dedup != nil && p.dedup.Copy(pcap.KeyUDP(seg.Payload), seg.Iface, seg.TS) {
			p.udpDups++
			return nil
		}
		if seg.Truncated {
			p.ext.LostFrames++ // a datagram cut by the snaplen is not replayed
			return nil
		}
		if extract.Datagram(seg.Payload, len(p.opt.Ports) == 0, &p.ext) {
			p.push(seg.Payload, seg.TS, seg.Src, seg.Dst, p.udpFlowID(seg.Src, seg.Dst))
		}
	case pcap.ProtoTCP:
		if p.opt.TCP {
			p.asm.Add(seg)
		}
	}
	return nil
}

func (p *Pcap) reopen() error {
	p.accumulate()
	p.f.Close()
	// The next pass starts just after the last message of this one, so
	// original timing repeats the capture back to back.
	p.base = p.last + time.Millisecond
	return p.open()
}

func (p *Pcap) accumulate() {
	d, e := p.dec.Stats, p.ext
	t := &p.decTotal
	t.Packets += d.Packets
	t.NotIP += d.NotIP
	t.Malformed += d.Malformed
	t.OtherProto += d.OtherProto
	t.Fragments += d.Fragments
	t.Reassembled += d.Reassembled
	t.FragmentsLost += d.FragmentsLost
	t.Truncated += d.Truncated
	t.SnaplenDatagrams += d.SnaplenDatagrams
	t.SnaplenUDP += d.SnaplenUDP
	x := &p.extTotal
	x.Datagrams += e.Datagrams
	x.DatagramsSkip += e.DatagramsSkip
	for i := range x.Streams {
		x.Streams[i] += e.Streams[i]
	}
	x.Messages += e.Messages
	x.RELPOther += e.RELPOther
	x.Resyncs += e.Resyncs
	x.SkippedBytes += e.SkippedBytes
	x.LostFrames += e.LostFrames
	x.LongLines += e.LongLines
	p.ext = extract.Stats{}
	if p.asm != nil {
		p.addAsm(p.asm.Stats)
	}
}

func (p *Pcap) addAsm(a pcap.AsmStats) {
	t := &p.asmTotal
	t.Flows += a.Flows
	t.Midstream += a.Midstream
	t.Ignored += a.Ignored
	t.Overflow += a.Overflow
	t.Retransmitted += a.Retransmitted
	t.OutOfOrder += a.OutOfOrder
	t.Gaps += a.Gaps
	t.GapBytes += a.GapBytes
	t.Unterminated += a.Unterminated
	t.Duplicates += a.Duplicates
}

// Stats implements Source.
func (p *Pcap) Stats() map[string]uint64 {
	d := p.decTotal
	cur := p.dec.Stats
	e := p.extTotal
	ce := p.ext
	a := p.asmTotal
	m := map[string]uint64{
		"pcap_passes":               p.pass,
		"pcap_cut_at_end":           p.cut,
		"pcap_packets":              d.Packets + cur.Packets,
		"pcap_not_ip":               d.NotIP + cur.NotIP,
		"pcap_malformed":            d.Malformed + cur.Malformed,
		"pcap_other_proto":          d.OtherProto + cur.OtherProto,
		"pcap_snaplen_truncated":    d.Truncated + cur.Truncated,
		"pcap_ip_fragments":         d.Fragments + cur.Fragments,
		"pcap_ip_reassembled":       d.Reassembled + cur.Reassembled,
		"pcap_ip_fragments_lost":    d.FragmentsLost + cur.FragmentsLost,
		"udp_datagrams":             e.Datagrams + ce.Datagrams,
		"udp_not_syslog":            e.DatagramsSkip + ce.DatagramsSkip,
		"tcp_messages":              e.Messages + ce.Messages,
		"tcp_streams_octet":         e.Streams[extract.ModeOctet] + ce.Streams[extract.ModeOctet],
		"tcp_streams_lf":            e.Streams[extract.ModeLF] + ce.Streams[extract.ModeLF],
		"tcp_streams_relp":          e.Streams[extract.ModeRELP] + ce.Streams[extract.ModeRELP],
		"tcp_streams_not_syslog":    e.Streams[extract.ModeIgnore] + ce.Streams[extract.ModeIgnore] + e.Streams[extract.ModeDetect] + ce.Streams[extract.ModeDetect],
		"tcp_relp_other_commands":   e.RELPOther + ce.RELPOther,
		"tcp_resyncs":               e.Resyncs + ce.Resyncs,
		"tcp_skipped_bytes":         e.SkippedBytes + ce.SkippedBytes,
		"frames_lost":               e.LostFrames + ce.LostFrames + d.SnaplenUDP + cur.SnaplenUDP,
		"pcap_ip_datagrams_snaplen": d.SnaplenDatagrams + cur.SnaplenDatagrams,
		"udp_duplicates":            p.udpDups,
		"tcp_long_lines":            e.LongLines + ce.LongLines,
		"messages_replayed":         p.sent,
	}
	if p.asm != nil {
		c := p.asm.Stats
		a.Flows += c.Flows
		a.Midstream += c.Midstream
		a.Ignored += c.Ignored
		a.Overflow += c.Overflow
		a.Retransmitted += c.Retransmitted
		a.OutOfOrder += c.OutOfOrder
		a.Gaps += c.Gaps
		a.GapBytes += c.GapBytes
		a.Unterminated += c.Unterminated
		a.Duplicates += c.Duplicates
	}
	m["tcp_duplicate_directions"] = a.Duplicates
	m["tcp_directions"] = a.Flows
	m["tcp_midstream"] = a.Midstream
	m["tcp_directions_filtered"] = a.Ignored
	m["tcp_flows_overflow"] = a.Overflow
	m["tcp_retransmitted_bytes"] = a.Retransmitted
	m["tcp_out_of_order"] = a.OutOfOrder
	m["tcp_gaps"] = a.Gaps
	m["tcp_gap_bytes"] = a.GapBytes
	m["tcp_unterminated"] = a.Unterminated
	return m
}

// Close implements Source.
func (p *Pcap) Close() error { return p.f.Close() }
