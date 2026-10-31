// Package pcap reads tcpdump captures (classic pcap and pcapng) and turns them
// into UDP datagrams and reassembled TCP byte streams (SPEC §6).
//
// It is written on the standard library only, like the rest of the module: the
// subset needed here (the file formats, a handful of link types, IPv4/IPv6 with
// fragments, UDP and TCP) is small, and every branch is under test and fuzz.
// Live capture is out of scope: the input is always a file.
package pcap

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// Link types (LINKTYPE_* of the tcpdump.org registry) understood by Decode.
const (
	LinkNull     = 0   // BSD loopback: 4-byte address family, host order
	LinkEthernet = 1   // Ethernet II, with 802.1Q/802.1ad tags
	LinkRaw      = 101 // raw IPv4/IPv6
	LinkRawAlt12 = 12  // DLT_RAW on OpenBSD and old libpcap
	LinkRawAlt14 = 14  // DLT_RAW on some BSDs
	LinkLoop     = 108 // OpenBSD loopback: 4-byte address family, network order
	LinkSLL      = 113 // Linux "cooked" v1 (tcpdump -i any)
	LinkPKTAP    = 258 // macOS PKTAP (tcpdump -i any on macOS)
	LinkSLL2     = 276 // Linux "cooked" v2
	LinkIPv4     = 228
	LinkIPv6     = 229
)

// Packet is one captured frame.
type Packet struct {
	TS       time.Time
	LinkType uint16
	Data     []byte // only the captured bytes; valid until the next Next call
	// Truncated is true when the capture kept fewer bytes than were on the
	// wire (snaplen): the tail of the frame is missing.
	Truncated bool
}

// MaxPacket bounds a record: a larger captured length is a corrupt file, not a
// frame (the largest snaplen tcpdump writes is 262144).
const MaxPacket = 1 << 20

// Reader reads packets from a classic pcap or a pcapng stream.
type Reader struct {
	r   *bufio.Reader
	buf []byte
	ng  bool

	// classic
	bo       binary.ByteOrder
	nano     bool
	linkType uint16

	// pcapng: per interface of the current section
	ifaces []ngIface
}

type ngIface struct {
	linkType uint16
	snaplen  uint32
	// timestamp unit: ticks per second
	tps uint64
}

// ErrFormat is returned when the stream is neither pcap nor pcapng.
var ErrFormat = errors.New("pcap: not a pcap or pcapng file")

// NewReader detects the format from the first four bytes.
func NewReader(r io.Reader) (*Reader, error) {
	br := bufio.NewReaderSize(r, 256<<10)
	magic, err := br.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFormat, err)
	}
	pr := &Reader{r: br}
	switch {
	case binary.BigEndian.Uint32(magic) == 0x0A0D0D0A:
		pr.ng = true
		return pr, nil
	case binary.LittleEndian.Uint32(magic) == 0xa1b2c3d4:
		pr.bo = binary.LittleEndian
	case binary.BigEndian.Uint32(magic) == 0xa1b2c3d4:
		pr.bo = binary.BigEndian
	case binary.LittleEndian.Uint32(magic) == 0xa1b23c4d:
		pr.bo, pr.nano = binary.LittleEndian, true
	case binary.BigEndian.Uint32(magic) == 0xa1b23c4d:
		pr.bo, pr.nano = binary.BigEndian, true
	default:
		return nil, ErrFormat
	}
	var h [24]byte
	if _, err := io.ReadFull(br, h[:]); err != nil {
		return nil, fmt.Errorf("pcap: short file header: %w", err)
	}
	// The low 16 bits are the link type; the upper bits carry FCS info.
	pr.linkType = uint16(pr.bo.Uint32(h[20:24]))
	return pr, nil
}

// Next returns the next packet, io.EOF at a clean end of file, and
// io.ErrUnexpectedEOF when the file ends inside a record (a capture that was
// cut while tcpdump was writing it: every packet before it was returned).
func (pr *Reader) Next() (Packet, error) {
	if pr.ng {
		return pr.nextNG()
	}
	var h [16]byte
	if _, err := io.ReadFull(pr.r, h[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Packet{}, io.EOF
		}
		return Packet{}, io.ErrUnexpectedEOF
	}
	sec := pr.bo.Uint32(h[0:4])
	frac := pr.bo.Uint32(h[4:8])
	incl := pr.bo.Uint32(h[8:12])
	orig := pr.bo.Uint32(h[12:16])
	if incl > MaxPacket {
		return Packet{}, fmt.Errorf("pcap: record of %d bytes: corrupt file", incl)
	}
	data, err := pr.read(int(incl))
	if err != nil {
		return Packet{}, err
	}
	ns := int64(frac) * 1000
	if pr.nano {
		ns = int64(frac)
	}
	return Packet{
		TS:        time.Unix(int64(sec), ns),
		LinkType:  pr.linkType,
		Data:      data,
		Truncated: orig > incl,
	}, nil
}

func (pr *Reader) read(n int) ([]byte, error) {
	if cap(pr.buf) < n {
		pr.buf = make([]byte, n)
	}
	pr.buf = pr.buf[:n]
	if _, err := io.ReadFull(pr.r, pr.buf); err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	return pr.buf, nil
}

// pcapng block types.
const (
	ngSHB = 0x0A0D0D0A
	ngIDB = 0x00000001
	ngOPB = 0x00000002 // obsolete packet block
	ngSPB = 0x00000003
	ngEPB = 0x00000006
)

func (pr *Reader) nextNG() (Packet, error) {
	for {
		var h [8]byte
		if _, err := io.ReadFull(pr.r, h[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return Packet{}, io.EOF
			}
			return Packet{}, io.ErrUnexpectedEOF
		}
		// The SHB type is a palindrome; its byte order comes from the body.
		if binary.BigEndian.Uint32(h[0:4]) == ngSHB {
			if err := pr.readSHB(h); err != nil {
				return Packet{}, err
			}
			continue
		}
		if pr.bo == nil {
			return Packet{}, fmt.Errorf("pcapng: block before the section header")
		}
		typ := pr.bo.Uint32(h[0:4])
		total := pr.bo.Uint32(h[4:8])
		if total < 12 || total%4 != 0 || total > MaxPacket+64<<10 {
			return Packet{}, fmt.Errorf("pcapng: block of %d bytes: corrupt file", total)
		}
		body, err := pr.read(int(total) - 8)
		if err != nil {
			return Packet{}, err
		}
		body = body[:len(body)-4] // trailing total length
		switch typ {
		case ngIDB:
			if err := pr.addIface(body); err != nil {
				return Packet{}, err
			}
		case ngEPB:
			if len(body) < 20 {
				return Packet{}, fmt.Errorf("pcapng: short enhanced packet block")
			}
			id := pr.bo.Uint32(body[0:4])
			ts := uint64(pr.bo.Uint32(body[4:8]))<<32 | uint64(pr.bo.Uint32(body[8:12]))
			capLen := pr.bo.Uint32(body[12:16])
			origLen := pr.bo.Uint32(body[16:20])
			return pr.ngPacket(id, ts, capLen, origLen, body[20:])
		case ngOPB:
			if len(body) < 20 {
				return Packet{}, fmt.Errorf("pcapng: short packet block")
			}
			id := uint32(pr.bo.Uint16(body[0:2]))
			ts := uint64(pr.bo.Uint32(body[4:8]))<<32 | uint64(pr.bo.Uint32(body[8:12]))
			capLen := pr.bo.Uint32(body[12:16])
			origLen := pr.bo.Uint32(body[16:20])
			return pr.ngPacket(id, ts, capLen, origLen, body[20:])
		case ngSPB:
			if len(body) < 4 || len(pr.ifaces) == 0 {
				return Packet{}, fmt.Errorf("pcapng: simple packet block without interface")
			}
			origLen := pr.bo.Uint32(body[0:4])
			capLen := origLen
			if s := pr.ifaces[0].snaplen; s != 0 && capLen > s {
				capLen = s
			}
			if int(capLen) > len(body)-4 {
				capLen = uint32(len(body) - 4)
			}
			// SPB has no timestamp: the zero time, which the original-timing
			// scheduler treats as "no delay" (SPEC §6.5).
			return Packet{
				LinkType:  pr.ifaces[0].linkType,
				Data:      body[4 : 4+capLen],
				Truncated: origLen > capLen,
			}, nil
		default:
			// Name resolution, statistics, custom, decryption secrets...: skip.
		}
	}
}

func (pr *Reader) readSHB(h [8]byte) error {
	var bom [4]byte
	if _, err := io.ReadFull(pr.r, bom[:]); err != nil {
		return io.ErrUnexpectedEOF
	}
	switch {
	case binary.LittleEndian.Uint32(bom[:]) == 0x1A2B3C4D:
		pr.bo = binary.LittleEndian
	case binary.BigEndian.Uint32(bom[:]) == 0x1A2B3C4D:
		pr.bo = binary.BigEndian
	default:
		return fmt.Errorf("pcapng: bad byte-order magic")
	}
	total := pr.bo.Uint32(h[4:8])
	if total < 28 || total%4 != 0 || total > 1<<20 {
		return fmt.Errorf("pcapng: section header of %d bytes: corrupt file", total)
	}
	if _, err := pr.read(int(total) - 12); err != nil {
		return err
	}
	pr.ifaces = pr.ifaces[:0] // interfaces are per section
	return nil
}

func (pr *Reader) addIface(body []byte) error {
	if len(body) < 8 {
		return fmt.Errorf("pcapng: short interface description block")
	}
	ifc := ngIface{
		linkType: pr.bo.Uint16(body[0:2]),
		snaplen:  pr.bo.Uint32(body[4:8]),
		tps:      1_000_000,
	}
	opts := body[8:]
	for len(opts) >= 4 {
		code := pr.bo.Uint16(opts[0:2])
		l := int(pr.bo.Uint16(opts[2:4]))
		if code == 0 || 4+l > len(opts) {
			break
		}
		if code == 9 && l >= 1 { // if_tsresol
			v := opts[4]
			exp := uint64(v & 0x7f)
			if v&0x80 == 0 {
				if exp <= 19 {
					ifc.tps = pow(10, exp)
				}
			} else if exp <= 63 {
				ifc.tps = 1 << exp
			}
		}
		opts = opts[4+(l+3)&^3:]
	}
	pr.ifaces = append(pr.ifaces, ifc)
	return nil
}

func pow(b, e uint64) uint64 {
	r := uint64(1)
	for range e {
		r *= b
	}
	return r
}

func (pr *Reader) ngPacket(id uint32, ts uint64, capLen, origLen uint32, data []byte) (Packet, error) {
	if int(id) >= len(pr.ifaces) {
		return Packet{}, fmt.Errorf("pcapng: packet on undeclared interface %d", id)
	}
	if int(capLen) > len(data) {
		return Packet{}, fmt.Errorf("pcapng: captured length %d past the block", capLen)
	}
	ifc := pr.ifaces[id]
	sec := ts / ifc.tps
	rem := ts % ifc.tps
	ns := rem * 1_000_000_000 / ifc.tps
	if ifc.tps > 1_000_000_000 {
		ns = rem / (ifc.tps / 1_000_000_000)
	}
	return Packet{
		TS:        time.Unix(int64(sec), int64(ns)),
		LinkType:  ifc.linkType,
		Data:      data[:capLen],
		Truncated: origLen > capLen,
	}, nil
}
