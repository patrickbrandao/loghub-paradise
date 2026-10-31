package source

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/extract"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap"
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
	cut   uint64 // passes that ended at a truncated record

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
		return extract.NewStream(strict, mid, &p.ext, func(b []byte, ts time.Time) {
			p.push(b, ts, src, dst)
		})
	})
	p.first = time.Time{}
	p.pass++
	return nil
}

func (p *Pcap) wantPort(port uint16) bool {
	return len(p.opt.Ports) == 0 || p.opt.Ports[port]
}

func (p *Pcap) push(b []byte, ts time.Time, src, dst netip.AddrPort) {
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
	p.queue = append(p.queue, Message{Data: append([]byte(nil), b...), At: at, Src: src, Dst: dst})
}

// Next implements Source.
func (p *Pcap) Next(_ uint64, now time.Time) (Message, error) {
	for len(p.queue) == 0 {
		if err := p.step(); err != nil {
			if !errors.Is(err, io.EOF) {
				return Message{}, err
			}
			if !p.opt.Loop || p.sent == 0 {
				return Message{}, io.EOF
			}
			if err := p.reopen(); err != nil {
				return Message{}, err
			}
		}
	}
	m := p.queue[0]
	p.queue[0] = Message{}
	p.queue = p.queue[1:]
	if p.opt.Header.Format != FormatRaw {
		m.Data = p.opt.Header.Wrap(make([]byte, 0, len(m.Data)+96), m.Data, now)
	}
	p.sent++
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
		if seg.Truncated {
			p.ext.LostFrames++ // a datagram cut by the snaplen is not replayed
			return nil
		}
		if extract.Datagram(seg.Payload, len(p.opt.Ports) == 0, &p.ext) {
			p.push(seg.Payload, seg.TS, seg.Src, seg.Dst)
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
}

// Stats implements Source.
func (p *Pcap) Stats() map[string]uint64 {
	d := p.decTotal
	cur := p.dec.Stats
	e := p.extTotal
	ce := p.ext
	a := p.asmTotal
	m := map[string]uint64{
		"pcap_passes":             p.pass,
		"pcap_cut_at_end":         p.cut,
		"pcap_packets":            d.Packets + cur.Packets,
		"pcap_not_ip":             d.NotIP + cur.NotIP,
		"pcap_malformed":          d.Malformed + cur.Malformed,
		"pcap_other_proto":        d.OtherProto + cur.OtherProto,
		"pcap_snaplen_truncated":  d.Truncated + cur.Truncated,
		"pcap_ip_fragments":       d.Fragments + cur.Fragments,
		"pcap_ip_reassembled":     d.Reassembled + cur.Reassembled,
		"pcap_ip_fragments_lost":  d.FragmentsLost + cur.FragmentsLost,
		"udp_datagrams":           e.Datagrams + ce.Datagrams,
		"udp_not_syslog":          e.DatagramsSkip + ce.DatagramsSkip,
		"tcp_messages":            e.Messages + ce.Messages,
		"tcp_streams_octet":       e.Streams[extract.ModeOctet] + ce.Streams[extract.ModeOctet],
		"tcp_streams_lf":          e.Streams[extract.ModeLF] + ce.Streams[extract.ModeLF],
		"tcp_streams_relp":        e.Streams[extract.ModeRELP] + ce.Streams[extract.ModeRELP],
		"tcp_streams_not_syslog":  e.Streams[extract.ModeIgnore] + ce.Streams[extract.ModeIgnore] + e.Streams[extract.ModeDetect] + ce.Streams[extract.ModeDetect],
		"tcp_relp_other_commands": e.RELPOther + ce.RELPOther,
		"tcp_resyncs":             e.Resyncs + ce.Resyncs,
		"tcp_skipped_bytes":       e.SkippedBytes + ce.SkippedBytes,
		"frames_lost":             e.LostFrames + ce.LostFrames,
		"tcp_long_lines":          e.LongLines + ce.LongLines,
		"messages_replayed":       p.sent,
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
	}
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
