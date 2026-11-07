package pcap

import (
	"encoding/binary"
	"slices"
	"time"
)

// Deduplication of the copies "tcpdump -i any" writes (SPEC §6.8): Linux
// captures a packet on every interface it crosses (a container's veth, the
// bridge, eth0), sometimes with NAT rewriting addresses and ports between
// the copies. What stays equal is the content; what differs is the
// interface. A packet whose key matches an original seen within DedupWindow
// on another interface is a copy, and each original absorbs at most one copy
// per interface, so a sender that really sent the same bytes twice is
// replayed twice.

const (
	// DedupWindow is how far apart, in capture time, two copies can be.
	DedupWindow = 50 * time.Millisecond
	// maxDedup bounds the originals remembered; the oldest go first.
	maxDedup = 1 << 16
)

type dedupEntry struct {
	key    uint64
	ts     time.Time
	ifaces []uint64
}

// Dedup remembers the recent originals. The zero value is not usable: use
// NewDedup.
type Dedup struct {
	byKey map[uint64][]*dedupEntry
	fifo  []*dedupEntry
	head  int
}

// NewDedup returns an empty deduplicator.
func NewDedup() *Dedup { return &Dedup{byKey: make(map[uint64][]*dedupEntry)} }

// Copy reports whether a packet with this key, captured on iface at ts, is a
// copy of an original seen on another interface; if not, it becomes an
// original.
func (d *Dedup) Copy(key, iface uint64, ts time.Time) bool {
	d.expire(ts)
	for _, e := range d.byKey[key] {
		if dt := ts.Sub(e.ts); dt > DedupWindow || dt < -DedupWindow {
			continue
		}
		if slices.Contains(e.ifaces, iface) {
			continue // same interface: not a copy of this one
		}
		e.ifaces = append(e.ifaces, iface)
		return true
	}
	e := &dedupEntry{key: key, ts: ts, ifaces: []uint64{iface}}
	d.byKey[key] = append(d.byKey[key], e)
	d.fifo = append(d.fifo, e)
	return false
}

func (d *Dedup) expire(now time.Time) {
	for d.head < len(d.fifo) {
		e := d.fifo[d.head]
		if len(d.fifo)-d.head <= maxDedup && now.Sub(e.ts) <= DedupWindow {
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
// TCP assembler): protocol, IPv4 ID, source port, TCP sequence,
// acknowledgement and flags, wire length and an FNV-1a of the content. The
// addresses and the destination port stay out: NAT rewrites them between
// the copies (the same key as the go-syslog-injector's).
func Key(s Segment) uint64 {
	var n [20]byte
	n[0] = byte(s.Proto)
	binary.BigEndian.PutUint16(n[1:], s.ID)
	binary.BigEndian.PutUint16(n[3:], s.Src.Port())
	binary.BigEndian.PutUint32(n[5:], s.Seq)
	binary.BigEndian.PutUint32(n[9:], s.Ack)
	n[13] = s.Flags
	binary.BigEndian.PutUint32(n[14:], uint32(s.WireLen))
	return fnv64(fnv64(fnvOffset, n[:]), s.Payload)
}
