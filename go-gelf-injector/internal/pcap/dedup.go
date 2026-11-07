package pcap

import (
	"encoding/binary"
	"slices"
	"time"
)

// Deduplication of the TCP copies "tcpdump -i any" writes (SPEC §6.2; review
// of 2026-11-07, item 7: only UDP datagrams were deduplicated, so a TCP
// connection crossing a NAT between two interfaces became two directions,
// each replayed whole). Ported from go-relplog-injector, the rule of the
// four injectors: a packet whose key matches an original seen within
// DedupWindow at another vantage is a copy, and each original absorbs at
// most one copy per vantage, so a sender that really sent the same bytes
// twice is replayed twice.

const (
	// DedupWindow is how far apart, in capture time, two copies can be.
	DedupWindow = 50 * time.Millisecond
	// maxDedup bounds the originals remembered; the oldest go first.
	maxDedup = 1 << 16
)

type dedupEntry struct {
	key      uint64
	ts       time.Time
	vantages []uint64
}

// Dedup remembers the recent originals. Use NewDedup.
type Dedup struct {
	byKey map[uint64][]*dedupEntry
	fifo  []*dedupEntry
	head  int
}

// NewDedup returns an empty deduplicator.
func NewDedup() *Dedup { return &Dedup{byKey: make(map[uint64][]*dedupEntry)} }

// Copy reports whether a packet with this key, seen at vantage at ts, is a
// copy of an original seen at another vantage; if not, it becomes an
// original. A capture with one vantage never has a copy.
func (d *Dedup) Copy(key, vantage uint64, ts time.Time) bool {
	d.expire(ts)
	for _, e := range d.byKey[key] {
		if dt := ts.Sub(e.ts); dt > DedupWindow || dt < -DedupWindow {
			continue
		}
		if slices.Contains(e.vantages, vantage) {
			continue // same vantage: not a copy of this one
		}
		e.vantages = append(e.vantages, vantage)
		return true
	}
	e := &dedupEntry{key: key, ts: ts, vantages: []uint64{vantage}}
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

// hash32 is FNV-1a 32 of the parts, as go-relplog-injector's.
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

// Key is the per-packet dedup key of a decoded segment (after IP
// reassembly, before the TCP assembler): protocol, IP identification,
// source port, TCP sequence, acknowledgement and flags, wire length and an
// FNV-1a of the content. The addresses and the destination port stay out:
// NAT rewrites them between the copies (the key of go-syslog-injector and
// go-relplog-injector).
func Key(s Segment) uint64 {
	var n [22]byte
	n[0] = byte(s.Proto)
	binary.BigEndian.PutUint32(n[1:], s.IPID)
	binary.BigEndian.PutUint16(n[5:], s.Src.Port())
	binary.BigEndian.PutUint32(n[7:], s.Seq)
	binary.BigEndian.PutUint32(n[11:], s.Ack)
	n[15] = s.Flags
	binary.BigEndian.PutUint32(n[16:], uint32(s.WireLen))
	return fnv64(fnv64(fnvOffset, n[:]), s.Payload)
}
