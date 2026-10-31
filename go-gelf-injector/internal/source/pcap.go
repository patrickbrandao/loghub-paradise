package source

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/extract"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap"
)

// PcapOptions select what is replayed from a capture (SPEC §6.1).
type PcapOptions struct {
	// Ports, when not empty, keeps only traffic to these destination ports
	// and drops the content check (any payload there is GELF).
	Ports map[uint16]bool
	// UDP and TCP enable each protocol (TCP covers GELF over TCP and HTTP).
	UDP, TCP bool
	Loop     bool
}

// Pcap replays the GELF documents of a capture, in capture order.
type Pcap struct {
	path string
	opt  PcapOptions

	f     *os.File
	rd    *pcap.Reader
	dec   *pcap.Decoder
	asm   *pcap.Assembler
	dg    *extract.Datagrams
	ext   extract.Stats // accumulates over passes
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

	decTotal   pcap.Stats
	asmTotal   pcap.AsmStats
	reasmTotal gelf.ReasmStats
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

func (p *Pcap) strict() bool { return len(p.opt.Ports) == 0 }

func (p *Pcap) wantPort(port uint16) bool {
	return len(p.opt.Ports) == 0 || p.opt.Ports[port]
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
	p.dg = &extract.Datagrams{St: &p.ext, Strict: p.strict(), R: gelf.NewReassembler()}
	p.asm = pcap.NewAssembler(func(k pcap.FlowKey, mid bool) pcap.StreamHandler {
		if !p.wantPort(k.Dst.Port()) {
			return nil
		}
		src, dst := k.Src, k.Dst
		return extract.NewStream(p.strict(), mid, &p.ext, func(d extract.Doc) {
			p.push(d.Data, d.TS, src, dst)
		})
	})
	p.first = time.Time{}
	p.pass++
	p.passSent = 0
	return nil
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
	p.queue = append(p.queue, Message{Data: b, At: at, Src: src, Dst: dst})
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

// step reads one packet; io.EOF after flushing the open streams and chunked
// messages at the end of the pass.
func (p *Pcap) step() error {
	pk, err := p.rd.Next()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			p.cut++ // tcpdump still writing, or killed: replay what is whole
			err = io.EOF
		}
		if errors.Is(err, io.EOF) && p.asm != nil {
			p.asm.Flush()
			p.dg.R.Flush()
			p.addTotals()
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
		if d, ok := p.dg.Add(seg.Src, seg.Payload, seg.TS); ok {
			p.push(d.Data, d.TS, seg.Src, seg.Dst)
		}
	case pcap.ProtoTCP:
		if p.opt.TCP {
			p.asm.Add(seg)
		}
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
	// The next pass starts just after the last message of this one, so
	// original timing repeats the capture back to back.
	p.base = p.last + time.Millisecond
	return p.open()
}

// addTotals folds the assembler and the reassembler of the ending pass.
func (p *Pcap) addTotals() {
	a, t := p.asm.Stats, &p.asmTotal
	t.Flows += a.Flows
	t.Midstream += a.Midstream
	t.Ignored += a.Ignored
	t.Overflow += a.Overflow
	t.Retransmitted += a.Retransmitted
	t.OutOfOrder += a.OutOfOrder
	t.Gaps += a.Gaps
	t.GapBytes += a.GapBytes
	t.Unterminated += a.Unterminated
	r, u := p.dg.R.Stats, &p.reasmTotal
	u.Chunks += r.Chunks
	u.Messages += r.Messages
	u.Invalid += r.Invalid
	u.Duplicates += r.Duplicates
	u.Incomplete += r.Incomplete
	u.ChunksDropped += r.ChunksDropped
	u.Overflow += r.Overflow
	p.dg.R.Stats = gelf.ReasmStats{}
}

// Stats implements Source (SPEC §8.4).
func (p *Pcap) Stats() map[string]uint64 {
	d, cur, e, a, r := p.decTotal, p.dec.Stats, p.ext, p.asmTotal, p.reasmTotal
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
		c2 := p.dg.R.Stats
		r.Chunks += c2.Chunks
		r.Messages += c2.Messages
		r.Invalid += c2.Invalid
		r.Duplicates += c2.Duplicates
		r.Incomplete += c2.Incomplete
		r.ChunksDropped += c2.ChunksDropped
		r.Overflow += c2.Overflow
	}
	h := e.HTTP
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
		"udp_datagrams":           e.Datagrams,
		"udp_not_gelf":            e.DatagramsSkip,
		"udp_chunked_messages":    e.Chunked,
		"udp_chunks":              r.Chunks,
		"udp_chunks_invalid":      r.Invalid,
		"udp_chunks_duplicate":    r.Duplicates,
		"udp_chunks_overflow":     r.Overflow,
		"udp_chunked_incomplete":  r.Incomplete,
		"udp_chunks_dropped":      r.ChunksDropped,
		"compressed_gzip":         e.Compressed[gelf.Gzip],
		"compressed_zlib":         e.Compressed[gelf.Zlib],
		"compressed_deflate":      e.Compressed[gelf.Deflate],
		"decompress_errors":       e.DecompressError,
		"docs_too_large":          e.TooLarge,
		"tcp_directions":          a.Flows,
		"tcp_midstream":           a.Midstream,
		"tcp_directions_filtered": a.Ignored,
		"tcp_flows_overflow":      a.Overflow,
		"tcp_retransmitted_bytes": a.Retransmitted,
		"tcp_out_of_order":        a.OutOfOrder,
		"tcp_gaps":                a.Gaps,
		"tcp_gap_bytes":           a.GapBytes,
		"tcp_unterminated":        a.Unterminated,
		"tcp_streams_gelf":        e.StreamsGELF,
		"tcp_streams_http":        e.StreamsHTTP,
		"tcp_streams_ignored":     e.StreamsIgnored,
		"tcp_messages":            e.Frames,
		"tcp_frames_not_gelf":     e.FramesNotGELF,
		"tcp_resyncs":             e.Resyncs,
		"tcp_skipped_bytes":       e.SkippedBytes,
		"frames_lost":             e.LostFrames,
		"http_messages":           e.HTTPDocs,
		"http_requests":           h.Requests,
		"http_requests_skipped":   e.HTTPSkipped,
		"http_resyncs":            h.Resyncs,
		"http_skipped_bytes":      h.SkippedBytes,
		"http_requests_lost":      h.Lost,
		"http_requests_too_large": h.TooLarge,
		"messages_replayed":       p.sent,
	}
}

// Close implements Source.
func (p *Pcap) Close() error { return p.f.Close() }
