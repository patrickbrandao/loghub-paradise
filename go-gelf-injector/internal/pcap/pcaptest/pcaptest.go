// Package pcaptest writes small captures for the tests: classic pcap or
// pcapng, Ethernet or Linux cooked framing, IPv4 or IPv6, UDP datagrams and
// TCP segments with explicit sequence numbers (so tests can reorder, repeat
// and drop them).
package pcaptest

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"time"
)

// Link types written.
const (
	Ethernet = 1
	SLL      = 113
	SLL2     = 276
	Null     = 0
)

// Frame is one packet to write.
type Frame struct {
	TS   time.Time
	Data []byte // the link-layer frame
	// Snap, when > 0 and < len(Data), keeps only the first Snap bytes.
	Snap int
}

// Classic returns a pcap file (microseconds, little-endian).
func Classic(link uint32, frames []Frame) []byte {
	var b bytes.Buffer
	h := make([]byte, 24)
	binary.LittleEndian.PutUint32(h[0:], 0xa1b2c3d4)
	binary.LittleEndian.PutUint16(h[4:], 2)
	binary.LittleEndian.PutUint16(h[6:], 4)
	binary.LittleEndian.PutUint32(h[16:], 262144)
	binary.LittleEndian.PutUint32(h[20:], link)
	b.Write(h)
	for _, f := range frames {
		data := f.Data
		if f.Snap > 0 && f.Snap < len(data) {
			data = data[:f.Snap]
		}
		r := make([]byte, 16)
		binary.LittleEndian.PutUint32(r[0:], uint32(f.TS.Unix()))
		binary.LittleEndian.PutUint32(r[4:], uint32(f.TS.Nanosecond()/1000))
		binary.LittleEndian.PutUint32(r[8:], uint32(len(data)))
		binary.LittleEndian.PutUint32(r[12:], uint32(len(f.Data)))
		b.Write(r)
		b.Write(data)
	}
	return b.Bytes()
}

// NG returns a pcapng file (big-endian, nanosecond resolution) with one
// interface of the given link type.
func NG(link uint16, frames []Frame) []byte {
	bo := binary.BigEndian
	var b bytes.Buffer
	block := func(typ uint32, body []byte) {
		for len(body)%4 != 0 {
			body = append(body, 0)
		}
		total := uint32(12 + len(body))
		h := make([]byte, 8)
		bo.PutUint32(h[0:], typ)
		bo.PutUint32(h[4:], total)
		b.Write(h)
		b.Write(body)
		t := make([]byte, 4)
		bo.PutUint32(t, total)
		b.Write(t)
	}
	shb := make([]byte, 16)
	bo.PutUint32(shb[0:], 0x1A2B3C4D)
	bo.PutUint16(shb[4:], 1)
	binary.BigEndian.PutUint64(shb[8:], ^uint64(0)) // section length unknown
	block(0x0A0D0D0A, shb)
	idb := make([]byte, 8)
	bo.PutUint16(idb[0:], link)
	bo.PutUint32(idb[4:], 262144)
	// if_tsresol = 9 (nanoseconds), then opt_endofopt
	idb = append(idb, 0, 9, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0)
	block(1, idb)
	for _, f := range frames {
		data := f.Data
		if f.Snap > 0 && f.Snap < len(data) {
			data = data[:f.Snap]
		}
		ts := uint64(f.TS.UnixNano())
		epb := make([]byte, 20)
		bo.PutUint32(epb[4:], uint32(ts>>32))
		bo.PutUint32(epb[8:], uint32(ts))
		bo.PutUint32(epb[12:], uint32(len(data)))
		bo.PutUint32(epb[16:], uint32(len(f.Data)))
		block(6, append(epb, data...))
	}
	return b.Bytes()
}

// Link wraps an IP packet in the link header of lt.
func Link(lt int, ip []byte) []byte {
	v6 := ip[0]>>4 == 6
	et := uint16(0x0800)
	if v6 {
		et = 0x86DD
	}
	switch lt {
	case Ethernet:
		h := make([]byte, 14)
		copy(h[0:6], []byte{2, 0, 0, 0, 0, 2})
		copy(h[6:12], []byte{2, 0, 0, 0, 0, 1})
		binary.BigEndian.PutUint16(h[12:], et)
		return append(h, ip...)
	case SLL:
		h := make([]byte, 16)
		binary.BigEndian.PutUint16(h[14:], et)
		return append(h, ip...)
	case SLL2:
		h := make([]byte, 20)
		binary.BigEndian.PutUint16(h[0:], et)
		return append(h, ip...)
	case Null:
		h := make([]byte, 4)
		fam := uint32(2)
		if v6 {
			fam = 30
		}
		binary.LittleEndian.PutUint32(h, fam)
		return append(h, ip...)
	}
	panic("link type")
}

// IP builds an IPv4 or IPv6 packet (by the family of src) around a transport
// payload. id is the IPv4 identification.
func IP(src, dst netip.Addr, proto uint8, l4 []byte, id uint16) []byte {
	if src.Is4() {
		h := make([]byte, 20)
		h[0] = 0x45
		binary.BigEndian.PutUint16(h[2:], uint16(20+len(l4)))
		binary.BigEndian.PutUint16(h[4:], id)
		h[8] = 64
		h[9] = proto
		s, d := src.As4(), dst.As4()
		copy(h[12:], s[:])
		copy(h[16:], d[:])
		return append(h, l4...)
	}
	h := make([]byte, 40)
	h[0] = 0x60
	binary.BigEndian.PutUint16(h[4:], uint16(len(l4)))
	h[6] = proto
	h[7] = 64
	s, d := src.As16(), dst.As16()
	copy(h[8:], s[:])
	copy(h[24:], d[:])
	return append(h, l4...)
}

// Fragments4 splits an IPv4 datagram's transport payload into fragments of at
// most size bytes (a multiple of 8).
func Fragments4(src, dst netip.Addr, proto uint8, l4 []byte, id uint16, size int) [][]byte {
	var out [][]byte
	for off := 0; off < len(l4); off += size {
		end := min(off+size, len(l4))
		p := IP(src, dst, proto, l4[off:end], id)
		ff := uint16(off / 8)
		if end < len(l4) {
			ff |= 0x2000
		}
		binary.BigEndian.PutUint16(p[6:], ff)
		out = append(out, p)
	}
	return out
}

// UDP builds a UDP datagram (checksum zero: not verified by the reader).
func UDP(sport, dport uint16, payload []byte) []byte {
	h := make([]byte, 8)
	binary.BigEndian.PutUint16(h[0:], sport)
	binary.BigEndian.PutUint16(h[2:], dport)
	binary.BigEndian.PutUint16(h[4:], uint16(8+len(payload)))
	return append(h, payload...)
}

// TCP flags.
const (
	FIN = 0x01
	SYN = 0x02
	RST = 0x04
	PSH = 0x08
	ACK = 0x10
)

// TCP builds a TCP segment.
func TCP(sport, dport uint16, seq uint32, flags uint8, payload []byte) []byte {
	h := make([]byte, 20)
	binary.BigEndian.PutUint16(h[0:], sport)
	binary.BigEndian.PutUint16(h[2:], dport)
	binary.BigEndian.PutUint32(h[4:], seq)
	h[12] = 5 << 4
	h[13] = flags
	binary.BigEndian.PutUint16(h[14:], 65535)
	return append(h, payload...)
}

// Conn builds the frames of one client->server TCP connection: SYN, the
// payload in segments of at most mss bytes, FIN. isn is the initial sequence
// number; t0 the time of the SYN, each frame 1 ms after the previous.
type Conn struct {
	Link     int
	Src, Dst netip.AddrPort
	ISN      uint32
	T0       time.Time
}

// Segment is one data segment of a connection, before it is framed.
type Segment struct {
	Seq     uint32
	Payload []byte
	Flags   uint8
}

// Split cuts payload into data segments of at most mss bytes, numbered from
// ISN+1.
func (c Conn) Split(payload []byte, mss int) []Segment {
	var out []Segment
	seq := c.ISN + 1
	for off := 0; off < len(payload); off += mss {
		end := min(off+mss, len(payload))
		out = append(out, Segment{Seq: seq, Payload: payload[off:end], Flags: ACK | PSH})
		seq += uint32(end - off)
	}
	return out
}

// Frames returns SYN (unless noSYN), the segments in the given order, and FIN.
func (c Conn) Frames(segs []Segment, noSYN, noFIN bool) []Frame {
	var out []Frame
	t := c.T0
	add := func(seq uint32, flags uint8, p []byte) {
		ip := IP(c.Src.Addr(), c.Dst.Addr(), 6, TCP(c.Src.Port(), c.Dst.Port(), seq, flags, p), 1)
		out = append(out, Frame{TS: t, Data: Link(c.Link, ip)})
		t = t.Add(time.Millisecond)
	}
	if !noSYN {
		add(c.ISN, SYN, nil)
	}
	end := c.ISN + 1
	for _, s := range segs {
		add(s.Seq, s.Flags, s.Payload)
		if e := s.Seq + uint32(len(s.Payload)); int32(e-end) > 0 {
			end = e
		}
	}
	if !noFIN {
		add(end, FIN|ACK, nil)
	}
	return out
}
