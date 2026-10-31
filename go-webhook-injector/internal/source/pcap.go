package source

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/httpx"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap"
)

// PcapOptions select what is replayed from a capture (SPEC §6.1).
type PcapOptions struct {
	// Ports, when not empty, keeps only connections to these destination
	// ports. The content must be HTTP/1.x either way.
	Ports map[uint16]bool
	Loop  bool
}

// Pcap replays the HTTP/1.x requests of a capture, in capture order.
type Pcap struct {
	path string
	opt  PcapOptions

	f     *os.File
	rd    *pcap.Reader
	dec   *pcap.Decoder
	asm   *pcap.Assembler
	ext   httpx.Stats
	queue []Message
	first time.Time
	base  time.Duration
	last  time.Duration
	pass  uint64
	sent  uint64
	// passSent counts the messages of the current pass: a pass with none ends
	// --loop (a capture rewritten empty between passes would spin forever).
	passSent uint64
	cut      uint64

	decTotal pcap.Stats
	asmTotal pcap.AsmStats
}

// NewPcap opens a capture; a file that is not pcap/pcapng fails here.
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
	p.asm = pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		if len(p.opt.Ports) > 0 && !p.opt.Ports[k.Dst.Port()] {
			return nil
		}
		src, dst := k.Src, k.Dst
		return httpx.NewStream(mid, &p.ext, func(r *httpx.Request, ts time.Time) {
			p.push(r, ts, src, dst)
		})
	})
	p.first = time.Time{}
	p.pass++
	p.passSent = 0
	return nil
}

func (p *Pcap) push(r *httpx.Request, ts time.Time, src, dst netip.AddrPort) {
	at := time.Duration(-1)
	if !ts.IsZero() {
		if p.first.IsZero() {
			p.first = ts
		}
		at = p.base + ts.Sub(p.first)
		if at < p.base {
			at = p.base
		}
		if at > p.last {
			p.last = at
		}
	}
	p.queue = append(p.queue, Message{Body: r.Body, Req: r, At: at, Src: src, Dst: dst})
}

// Next implements Source.
func (p *Pcap) Next(_ uint64, _ time.Time) (Message, error) {
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
	m := p.queue[0]
	p.queue[0] = Message{}
	p.queue = p.queue[1:]
	p.sent++
	p.passSent++
	return m, nil
}

func (p *Pcap) step() error {
	pk, err := p.rd.Next()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
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
		p.first = pk.TS
	}
	if seg, ok := p.dec.Decode(pk); ok && seg.Proto == pcap.ProtoTCP {
		p.asm.Add(seg)
	}
	return nil
}

func (p *Pcap) reopen() error {
	d := p.dec.Stats
	t := &p.decTotal
	t.Packets += d.Packets
	t.NotIP += d.NotIP
	t.Malformed += d.Malformed
	t.OtherProto += d.OtherProto
	t.Fragments += d.Fragments
	t.Reassembled += d.Reassembled
	t.FragmentsLost += d.FragmentsLost
	t.Truncated += d.Truncated
	p.f.Close()
	p.base = p.last + time.Millisecond
	return p.open()
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

// Stats implements Source. The extractor's counters accumulate over passes.
func (p *Pcap) Stats() map[string]uint64 {
	d, cur, e, a := p.decTotal, p.dec.Stats, p.ext, p.asmTotal
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
	return map[string]uint64{
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
		"tcp_directions":          a.Flows,
		"tcp_midstream":           a.Midstream,
		"tcp_directions_filtered": a.Ignored,
		"tcp_flows_overflow":      a.Overflow,
		"tcp_retransmitted_bytes": a.Retransmitted,
		"tcp_out_of_order":        a.OutOfOrder,
		"tcp_gaps":                a.Gaps,
		"tcp_gap_bytes":           a.GapBytes,
		"tcp_unterminated":        a.Unterminated,
		"http_requests":           e.Requests,
		"http_streams":            e.HTTPStreams,
		"http_response_streams":   e.Responses,
		"http_h2c_streams":        e.H2C,
		"http_not_http_streams":   e.NotHTTP,
		"http_resyncs":            e.Resyncs,
		"http_skipped_bytes":      e.SkippedBytes,
		"http_requests_lost":      e.Lost,
		"http_requests_too_large": e.TooLarge,
		"messages_replayed":       p.sent,
	}
}

// Close implements Source.
func (p *Pcap) Close() error { return p.f.Close() }
