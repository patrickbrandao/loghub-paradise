package extract

import (
	"hash/fnv"
	"time"
)

// Copies of one datagram in a capture (SPEC §6.3.1). tcpdump -i any writes
// a routed or bridged datagram once per interface (and, on some kernels, a
// loopback one twice: outgoing and incoming); a sender that repeats a
// document on purpose writes identical datagrams too, and those must not
// vanish. Two things tell them apart: the IP identification (the copies of
// one packet share it, the sender's next packet has the next one) and the
// vantage of each copy (pcapng interface, Linux cooked interface index and
// packet type: copies are seen at different vantages, a repeat at the same
// one). The key leaves the addresses and the destination port out, as
// go-syslog-injector does: a NAT between two vantages rewrites them.

// DedupMode is --pcap-dedup.
type DedupMode uint8

const (
	// DedupAuto drops a copy only when it was seen at another vantage than
	// the copies before it (the counting rule of copyOf).
	DedupAuto DedupMode = iota
	// DedupOn drops every identical datagram (same key) within DupWindow,
	// whatever the vantage: for captures that do not record one (merged
	// Ethernet captures), at the risk of a sender's repeat with the same IP
	// identification.
	DedupOn
	// DedupOff keeps every datagram.
	DedupOff
)

const (
	// DupWindow is how long, in capture time after its last copy, a datagram
	// is remembered (go-syslog-injector's 50 ms). The copies of one packet
	// are microseconds apart.
	DupWindow = 50 * time.Millisecond
	// MaxDupKeys bounds the datagrams remembered; past it the oldest is
	// forgotten early.
	MaxDupKeys = 1 << 16
)

type dupKey struct {
	srcPort uint16
	ipid    uint32
	n       int    // length on the wire
	h       uint64 // FNV-1a 64 of the bytes
}

type dupEntry struct {
	last   time.Time
	counts map[uint64]uint32 // copies per vantage
}

// dedup decides, per datagram, whether it is a new message or a copy.
type dedup struct {
	mode  DedupMode
	keys  map[dupKey]*dupEntry
	fifo  []dupKey // insertion order, for the eviction past MaxDupKeys (lazily cleaned)
	sweep time.Time
}

func newDedup(m DedupMode) *dedup { return &dedup{mode: m, keys: map[dupKey]*dupEntry{}} }

// copyOf records one datagram and says whether it is a copy of one already
// taken. In auto, the copy that brings the count of its own vantage above
// the largest count of the other vantages is a new message, any other is a
// copy: out+in of one send is one message, and out, in, out, in of a
// document sent twice is two, even if one vantage lost a copy.
func (d *dedup) copyOf(srcPort uint16, ipid uint32, vantage uint64, b []byte, ts time.Time) bool {
	if d.mode == DedupOff {
		return false
	}
	d.expire(ts)
	h := fnv.New64a()
	h.Write(b)
	k := dupKey{srcPort: srcPort, ipid: ipid, n: len(b), h: h.Sum64()}
	e := d.keys[k]
	if e == nil {
		if len(d.keys) >= MaxDupKeys {
			d.evictOldest()
		}
		e = &dupEntry{counts: map[uint64]uint32{}}
		d.keys[k] = e
		d.fifo = append(d.fifo, k)
		e.last = ts
		e.counts[vantage]++
		return false
	}
	if ts.Sub(e.last) > DupWindow {
		// Expired but not yet swept: a new message.
		e.counts = map[uint64]uint32{}
	}
	e.last = ts
	if d.mode == DedupOn {
		return true
	}
	var others uint32
	for v, c := range e.counts {
		if v != vantage && c > others {
			others = c
		}
	}
	e.counts[vantage]++
	return e.counts[vantage] <= others
}

func (d *dedup) evictOldest() {
	for len(d.fifo) > 0 {
		k := d.fifo[0]
		d.fifo = d.fifo[1:]
		if _, ok := d.keys[k]; ok {
			delete(d.keys, k)
			return
		}
	}
}

// expire forgets the datagrams whose last copy is older than DupWindow; a
// sweep at most once per window of capture time.
func (d *dedup) expire(now time.Time) {
	if now.Sub(d.sweep) < DupWindow {
		return
	}
	d.sweep = now
	for k, e := range d.keys {
		if now.Sub(e.last) > DupWindow {
			delete(d.keys, k)
		}
	}
	if len(d.fifo) > 2*len(d.keys)+1024 {
		live := d.fifo[:0]
		for _, k := range d.fifo {
			if _, ok := d.keys[k]; ok {
				live = append(live, k)
			}
		}
		d.fifo = live
	}
}
