package pcap

import (
	"encoding/binary"
	"hash/fnv"
	"time"
)

// Copies of one packet in a multi-interface capture (SPEC §6.9). On Linux,
// tcpdump -i any sees a packet on every interface it crosses -- the
// container's veth, the bridge, eth0 --, sometimes with the destination
// (DNAT) or the source address (SNAT) rewritten. Replayed as is, a UDP
// datagram went out twice and the TCP copy with another 4-tuple became a
// second whole flow.

// DedupWindow is how far apart, in capture time, two packets with the same
// key are taken as copies. The copies of a packet are microseconds apart (one
// skb crossing interfaces); a real TCP retransmission carries another IP ID
// and waits at least Linux's minimum RTO (200 ms).
const DedupWindow = 50 * time.Millisecond

// dedupMax bounds the remembered keys: a capture whose clock steps back would
// otherwise keep them all.
const dedupMax = 1 << 16

type dedupEntry struct {
	key uint64
	ts  time.Time
}

// Dedup drops the -i any copies of a packet.
type Dedup struct {
	seen  map[uint64]time.Time
	order []dedupEntry // FIFO of seen, for expiry in capture order
	// Dropped counts the copies.
	Dropped uint64
}

// NewDedup returns an empty deduplicator.
func NewDedup() *Dedup { return &Dedup{seen: make(map[uint64]time.Time)} }

// Copy reports whether s is a copy of a packet seen within DedupWindow, and
// remembers s otherwise. The key leaves out the addresses and the
// destination port, which NAT rewrites; it keeps what NAT preserves: the IP
// ID, the source port, the TCP sequence and acknowledgement numbers and
// flags, the length on the wire and a hash of the captured payload.
func (d *Dedup) Copy(s Segment) bool {
	h := fnv.New64a()
	var b [20]byte
	b[0] = byte(s.Proto)
	binary.BigEndian.PutUint16(b[1:3], s.IPID)
	binary.BigEndian.PutUint16(b[3:5], s.Src.Port())
	binary.BigEndian.PutUint32(b[5:9], s.Seq)
	binary.BigEndian.PutUint32(b[9:13], s.Ack)
	b[13] = s.Flags
	binary.BigEndian.PutUint32(b[14:18], uint32(s.WireLen))
	_, _ = h.Write(b[:18])
	_, _ = h.Write(s.Payload)
	k := h.Sum64()

	for len(d.order) > 0 && (len(d.order) >= dedupMax || s.TS.Sub(d.order[0].ts) > DedupWindow) {
		e := d.order[0]
		if t, ok := d.seen[e.key]; ok && t.Equal(e.ts) {
			delete(d.seen, e.key)
		}
		d.order = d.order[1:]
	}
	if t, ok := d.seen[k]; ok {
		if dt := s.TS.Sub(t); dt <= DedupWindow && dt >= -DedupWindow {
			d.Dropped++
			return true
		}
	}
	d.seen[k] = s.TS
	d.order = append(d.order, dedupEntry{key: k, ts: s.TS})
	return false
}
