package pcap

import (
	"encoding/binary"
	"slices"
	"time"
)

// Copies of one packet in a multi-interface capture (SPEC §6.9). On Linux,
// tcpdump -i any sees a packet on every interface it crosses -- the
// container's veth, the bridge, eth0 --, sometimes with the destination
// (DNAT) or the source address (SNAT) rewritten. Replayed as is, a UDP
// datagram went out twice and the TCP copy with another 4-tuple became a
// second whole flow.
//
// What stays equal between the copies is the content; what differs is the
// vantage (Segment.Iface). A packet whose key matches an original seen
// within DedupWindow on ANOTHER vantage is a copy, and each original absorbs
// at most one copy per vantage, so a sender that really sent the same bytes
// twice is replayed twice (the rule of go-relplog-injector, decision 22,
// and of go-gelf-injector, G15; until 2026-11-07 the key alone decided).

// DedupWindow is how far apart, in capture time, two packets with the same
// key are taken as copies. The copies of a packet are microseconds apart (one
// skb crossing interfaces); a real TCP retransmission carries another IP ID
// and waits at least Linux's minimum RTO (200 ms).
const DedupWindow = 50 * time.Millisecond

// dedupMax bounds the remembered originals (the oldest go first): a capture
// whose clock steps back would otherwise keep them all.
const dedupMax = 1 << 16

type dedupEntry struct {
	key    uint64
	ts     time.Time
	ifaces []uint64 // the original's vantage, then one per absorbed copy
}

// Dedup drops the -i any copies of a packet. The zero value is not usable:
// use NewDedup.
type Dedup struct {
	byKey map[uint64][]*dedupEntry
	fifo  []*dedupEntry // in capture order, for expiry
	head  int
	// Dropped counts the copies.
	Dropped uint64
}

// NewDedup returns an empty deduplicator.
func NewDedup() *Dedup { return &Dedup{byKey: make(map[uint64][]*dedupEntry)} }

// Copy reports whether s is a copy of an original seen within DedupWindow on
// another vantage, and makes s an original otherwise.
func (d *Dedup) Copy(s Segment) bool {
	key := Key(s)
	d.expire(s.TS)
	for _, e := range d.byKey[key] {
		if dt := s.TS.Sub(e.ts); dt > DedupWindow || dt < -DedupWindow {
			continue
		}
		if slices.Contains(e.ifaces, s.Iface) {
			continue // same vantage: not a copy of this one
		}
		e.ifaces = append(e.ifaces, s.Iface)
		d.Dropped++
		return true
	}
	e := &dedupEntry{key: key, ts: s.TS, ifaces: []uint64{s.Iface}}
	d.byKey[key] = append(d.byKey[key], e)
	d.fifo = append(d.fifo, e)
	return false
}

func (d *Dedup) expire(now time.Time) {
	for d.head < len(d.fifo) {
		e := d.fifo[d.head]
		if len(d.fifo)-d.head <= dedupMax && now.Sub(e.ts) <= DedupWindow {
			break
		}
		d.fifo[d.head] = nil
		d.head++
		list := d.byKey[e.key]
		if i := slices.Index(list, e); i >= 0 {
			list = slices.Delete(list, i, i+1)
		}
		if len(list) == 0 {
			delete(d.byKey, e.key)
		} else {
			d.byKey[e.key] = list
		}
	}
	if d.head > 1024 && d.head*2 > len(d.fifo) {
		d.fifo = append(d.fifo[:0], d.fifo[d.head:]...)
		d.head = 0
	}
}

func fnv64(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

const fnvOffset = 14695981039346656037

// Key is the dedup key of a decoded packet (after IP reassembly, before the
// TCP assembler). It leaves out the addresses and the destination port,
// which NAT rewrites; it keeps what NAT preserves: the IP ID, the source
// port, the TCP sequence and acknowledgement numbers and flags, the length on
// the wire and an FNV-1a 64 of the captured payload.
func Key(s Segment) uint64 {
	var n [18]byte
	n[0] = byte(s.Proto)
	binary.BigEndian.PutUint16(n[1:], s.IPID)
	binary.BigEndian.PutUint16(n[3:], s.Src.Port())
	binary.BigEndian.PutUint32(n[5:], s.Seq)
	binary.BigEndian.PutUint32(n[9:], s.Ack)
	n[13] = s.Flags
	binary.BigEndian.PutUint32(n[14:], uint32(s.WireLen))
	return fnv64(fnv64(fnvOffset, n[:]), s.Payload)
}
