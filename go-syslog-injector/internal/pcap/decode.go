package pcap

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"time"
)

// Proto is the transport protocol of a decoded segment.
type Proto uint8

const (
	ProtoUDP Proto = 17
	ProtoTCP Proto = 6
)

// TCP flags used by the assembler.
const (
	FlagFIN = 0x01
	FlagSYN = 0x02
	FlagRST = 0x04
	FlagACK = 0x10
)

// Segment is one UDP datagram or TCP segment with its addresses.
type Segment struct {
	TS    time.Time
	Proto Proto
	Src   netip.AddrPort
	Dst   netip.AddrPort
	Seq   uint32 // TCP only
	Ack   uint32 // TCP only
	Flags uint8  // TCP only
	// IPID is the IPv4 identification (0 for IPv6): NAT keeps it, so the
	// copies tcpdump -i any captures of one packet share it (SPEC §6.9).
	IPID    uint16
	Payload []byte
	// Truncated: the capture cut the payload short (snaplen). For TCP the
	// missing tail becomes a gap in the stream.
	Truncated bool
	// WireLen is the payload length on the wire (>= len(Payload)).
	WireLen int
	// Iface identifies where the capture saw the packet: the pcapng
	// interface, and what the link header names -- Linux cooked v2 the
	// interface index and the packet type, v1 the packet type and the
	// link-layer address, PKTAP the interface name (SPEC §6.9). The -i any
	// deduplication only needs it to differ between vantages.
	Iface uint64
}

// Skip reasons counted by the Decoder (SPEC §6.3).
var (
	errNotIP    = errors.New("not ip")
	errShort    = errors.New("short")
	errProto    = errors.New("not udp/tcp")
	errFragment = errors.New("fragment held")
)

// Stats counts what the decoder saw and skipped.
type Stats struct {
	Packets       uint64
	NotIP         uint64 // ARP, LLDP, unknown link type...
	Malformed     uint64 // short or inconsistent headers
	OtherProto    uint64 // ICMP, SCTP...
	Fragments     uint64 // fragments seen
	Reassembled   uint64 // datagrams rebuilt from fragments
	FragmentsLost uint64 // fragments dropped (incomplete, overlapping, too many)
	Truncated     uint64 // packets cut by the snaplen
	// DatagramsSnaplen counts datagrams lost because one of their fragments
	// was cut by the snaplen (SPEC §6.3): the missing bytes are not in the
	// file, so the datagram cannot be rebuilt.
	DatagramsSnaplen uint64
	// DatagramsSnaplenUDP: those of them that were UDP. They count in
	// frames_lost too, as a whole cut datagram (SPEC §6.3, alignment 8(c)).
	DatagramsSnaplenUDP uint64
}

// Decoder turns packets into segments, rebuilding IP fragments.
type Decoder struct {
	Stats   Stats
	frags   *defrag
	scratch []byte
}

// NewDecoder returns a decoder.
func NewDecoder() *Decoder { return &Decoder{frags: newDefrag()} }

// Decode returns the segment in p. ok is false when the packet carries no
// UDP or TCP data for us (counted in Stats). The segment's Payload aliases p
// or the decoder's reassembly buffer: copy before the next call.
func (d *Decoder) Decode(p Packet) (Segment, bool) {
	d.Stats.Packets++
	if p.Truncated {
		d.Stats.Truncated++
	}
	ip, link, err := linkPayload(p.LinkType, p.Data)
	if err != nil {
		d.count(err)
		return Segment{}, false
	}
	seg, err := d.ip(ip, p.TS, p.Truncated)
	if err != nil {
		d.count(err)
		return Segment{}, false
	}
	seg.TS = p.TS
	seg.Iface = uint64(p.Iface)<<40 | link
	return seg, true
}

// Flush ends the pass: fragments still waiting for their datagram are lost
// (SPEC §6.3; ported from go-relplog-injector, 2026-11-07: they vanished
// uncounted).
func (d *Decoder) Flush() {
	d.Stats.FragmentsLost += uint64(d.frags.flush())
}

func (d *Decoder) count(err error) {
	switch err {
	case errNotIP:
		d.Stats.NotIP++
	case errProto:
		d.Stats.OtherProto++
	case errFragment:
	default:
		d.Stats.Malformed++
	}
}

// linkPayload strips the link header and returns the IP packet and the
// vantage the link header names (0 when it names none).
func linkPayload(lt uint16, b []byte) ([]byte, uint64, error) {
	switch lt {
	case LinkEthernet:
		if len(b) < 14 {
			return nil, 0, errShort
		}
		et := binary.BigEndian.Uint16(b[12:14])
		b = b[14:]
		for et == 0x8100 || et == 0x88a8 || et == 0x9100 {
			if len(b) < 4 {
				return nil, 0, errShort
			}
			et = binary.BigEndian.Uint16(b[2:4])
			b = b[4:]
		}
		ip, err := etherIP(et, b)
		return ip, 0, err
	case LinkNull:
		if len(b) < 4 {
			return nil, 0, errShort
		}
		// Host byte order of the writer: try both.
		fam := binary.LittleEndian.Uint32(b[0:4])
		if fam > 0xffff {
			fam = binary.BigEndian.Uint32(b[0:4])
		}
		ip, err := bsdFamily(fam, b[4:])
		return ip, 0, err
	case LinkLoop:
		if len(b) < 4 {
			return nil, 0, errShort
		}
		ip, err := bsdFamily(binary.BigEndian.Uint32(b[0:4]), b[4:])
		return ip, 0, err
	case LinkRaw, LinkRawAlt12, LinkRawAlt14, LinkIPv4, LinkIPv6:
		return b, 0, nil
	case LinkSLL:
		if len(b) < 16 {
			return nil, 0, errShort
		}
		// Cooked v1 does not record the interface: the packet type and the
		// link-layer address are the closest thing (SPEC §6.9).
		ip, err := etherIP(binary.BigEndian.Uint16(b[14:16]), b[16:])
		return ip, 1 | uint64(hash32(b[0:2], b[6:14])), err
	case LinkSLL2:
		if len(b) < 20 {
			return nil, 0, errShort
		}
		// Cooked v2: protocol, reserved, interface index, hatype, packet
		// type (outgoing, to us, ...): the loopback shows a packet twice on
		// one interface, once per direction.
		ip, err := etherIP(binary.BigEndian.Uint16(b[0:2]), b[20:])
		return ip, uint64(binary.BigEndian.Uint32(b[4:8]))<<8 | uint64(b[10]), err
	case LinkPKTAP:
		// xnu's struct pktap_header, host order (little-endian on every
		// Mac): pth_length, pth_type_next (1 = a packet follows), pth_dlt
		// (the link type of what follows), pth_ifname[24]. Until 2026-11-07
		// the inner link type was read from pth_type_next, which is 1 for
		// every packet: a loopback capture (DLT_NULL) did not decode (ported
		// from go-relplog-injector).
		if len(b) < 12 {
			return nil, 0, errShort
		}
		hl := binary.LittleEndian.Uint32(b[0:4])
		next := binary.LittleEndian.Uint32(b[4:8])
		inner := binary.LittleEndian.Uint32(b[8:12])
		if hl < 12 || int(hl) > len(b) || inner == LinkPKTAP || inner > 0xffff {
			return nil, 0, errShort
		}
		if next != 1 {
			return nil, 0, errNotIP
		}
		var name uint64
		if hl >= 36 {
			name = 1 | uint64(hash32(b[12:36]))
		}
		ip, _, err := linkPayload(uint16(inner), b[hl:])
		return ip, name, err
	}
	return nil, 0, errNotIP
}

// hash32 is FNV-1a over the parts.
func hash32(parts ...[]byte) uint32 {
	h := uint32(2166136261)
	for _, p := range parts {
		for _, c := range p {
			h ^= uint32(c)
			h *= 16777619
		}
	}
	return h
}

func etherIP(et uint16, b []byte) ([]byte, error) {
	switch et {
	case 0x0800, 0x86DD:
		return b, nil
	}
	return nil, errNotIP
}

func bsdFamily(fam uint32, b []byte) ([]byte, error) {
	switch fam {
	case 2, 24, 28, 30: // AF_INET; AF_INET6 on NetBSD/OpenBSD, FreeBSD, macOS
		return b, nil
	}
	return nil, errNotIP
}

func (d *Decoder) ip(b []byte, ts time.Time, trunc bool) (Segment, error) {
	if len(b) < 1 {
		return Segment{}, errShort
	}
	switch b[0] >> 4 {
	case 4:
		return d.ipv4(b, ts, trunc)
	case 6:
		return d.ipv6(b, ts, trunc)
	}
	return Segment{}, errNotIP
}

func (d *Decoder) ipv4(b []byte, ts time.Time, trunc bool) (Segment, error) {
	if len(b) < 20 {
		return Segment{}, errShort
	}
	ihl := int(b[0]&0x0f) * 4
	total := int(binary.BigEndian.Uint16(b[2:4]))
	if ihl < 20 || total < ihl {
		return Segment{}, errShort
	}
	if len(b) >= total {
		b = b[:total] // Ethernet padding
	} else if !trunc {
		return Segment{}, errShort
	}
	if len(b) < ihl {
		return Segment{}, errShort
	}
	proto := b[9]
	src := netip.AddrFrom4([4]byte(b[12:16]))
	dst := netip.AddrFrom4([4]byte(b[16:20]))
	ff := binary.BigEndian.Uint16(b[6:8])
	more := ff&0x2000 != 0
	off := int(ff&0x1fff) * 8
	body := b[ihl:]
	wire := total - ihl
	if more || off != 0 {
		d.Stats.Fragments++
		key := fragKey{src: src, dst: dst, id: uint32(binary.BigEndian.Uint16(b[4:6])), proto: proto}
		if trunc {
			d.cutFragment(key, ts)
			return Segment{}, errFragment
		}
		whole, done, lost := d.frags.add(key, off, more, body, ts)
		d.Stats.FragmentsLost += uint64(lost)
		if !done {
			return Segment{}, errFragment
		}
		d.Stats.Reassembled++
		d.scratch = append(d.scratch[:0], whole...)
		body, wire = d.scratch, len(whole)
	}
	seg, err := l4(proto, src, dst, body, wire, trunc)
	seg.IPID = binary.BigEndian.Uint16(b[4:6])
	return seg, err
}

func (d *Decoder) ipv6(b []byte, ts time.Time, trunc bool) (Segment, error) {
	if len(b) < 40 {
		return Segment{}, errShort
	}
	plen := int(binary.BigEndian.Uint16(b[4:6]))
	next := b[6]
	src := netip.AddrFrom16([16]byte(b[8:24]))
	dst := netip.AddrFrom16([16]byte(b[24:40]))
	body := b[40:]
	if len(body) >= plen {
		body = body[:plen]
	} else if !trunc {
		return Segment{}, errShort
	}
	wire := plen
	for {
		switch next {
		case 0, 43, 60: // hop-by-hop, routing, destination options
			if len(body) < 8 {
				return Segment{}, errShort
			}
			hl := (int(body[1]) + 1) * 8
			if hl > len(body) {
				return Segment{}, errShort
			}
			next = body[0]
			body = body[hl:]
			wire -= hl
		case 44: // fragment
			if len(body) < 8 {
				return Segment{}, errShort
			}
			d.Stats.Fragments++
			nh := body[0]
			ff := binary.BigEndian.Uint16(body[2:4])
			off := int(ff>>3) * 8
			more := ff&1 != 0
			key := fragKey{src: src, dst: dst, id: binary.BigEndian.Uint32(body[4:8]), proto: nh, v6: true}
			if trunc {
				d.cutFragment(key, ts)
				return Segment{}, errFragment
			}
			whole, done, lost := d.frags.add(key, off, more, body[8:], ts)
			d.Stats.FragmentsLost += uint64(lost)
			if !done {
				return Segment{}, errFragment
			}
			d.Stats.Reassembled++
			d.scratch = append(d.scratch[:0], whole...)
			return l4(nh, src, dst, d.scratch, len(whole), false)
		default:
			return l4(next, src, dst, body, wire, trunc)
		}
	}
}

// cutFragment drops the datagram of a fragment the snaplen cut (SPEC §6.3).
func (d *Decoder) cutFragment(k fragKey, ts time.Time) {
	lost, first := d.frags.truncated(k, ts)
	d.Stats.FragmentsLost += uint64(lost)
	if first {
		d.Stats.DatagramsSnaplen++
		if k.proto == 17 {
			d.Stats.DatagramsSnaplenUDP++
		}
	}
}

func l4(proto uint8, src, dst netip.Addr, b []byte, wire int, trunc bool) (Segment, error) {
	switch proto {
	case 17:
		if len(b) < 8 {
			return Segment{}, errShort
		}
		ul := int(binary.BigEndian.Uint16(b[4:6]))
		if ul < 8 {
			return Segment{}, errShort
		}
		p := b[8:]
		if len(p) > ul-8 {
			p = p[:ul-8]
		}
		return Segment{
			Proto:     ProtoUDP,
			Src:       netip.AddrPortFrom(src.Unmap(), binary.BigEndian.Uint16(b[0:2])),
			Dst:       netip.AddrPortFrom(dst.Unmap(), binary.BigEndian.Uint16(b[2:4])),
			Payload:   p,
			Truncated: len(p) < ul-8,
			WireLen:   ul - 8,
		}, nil
	case 6:
		if len(b) < 20 {
			return Segment{}, errShort
		}
		doff := int(b[12]>>4) * 4
		if doff < 20 || doff > len(b) {
			return Segment{}, errShort
		}
		w := wire - doff
		if w < len(b)-doff {
			w = len(b) - doff
		}
		return Segment{
			Proto:     ProtoTCP,
			Src:       netip.AddrPortFrom(src.Unmap(), binary.BigEndian.Uint16(b[0:2])),
			Dst:       netip.AddrPortFrom(dst.Unmap(), binary.BigEndian.Uint16(b[2:4])),
			Seq:       binary.BigEndian.Uint32(b[4:8]),
			Ack:       binary.BigEndian.Uint32(b[8:12]),
			Flags:     b[13],
			Payload:   b[doff:],
			Truncated: trunc && w > len(b)-doff,
			WireLen:   w,
		}, nil
	}
	return Segment{}, errProto
}
