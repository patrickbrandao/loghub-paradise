package pcap

import (
	"net/netip"
	"sort"
	"time"
)

// IP reassembly (SPEC §6.3). A syslog datagram above the path MTU travels in
// fragments, and the capture only holds the whole message once they are
// joined. Bounded: a datagram has at most 64 KiB and 128 fragments, at most
// 1024 datagrams are open, and one not completed within FragTimeout of
// capture time is dropped.

const (
	maxFragDatagrams = 1024
	maxFragsPerDgram = 128
	maxDgramBytes    = 65535
	// FragTimeout is the reassembly window, in capture time (RFC 791's
	// suggested 15 s upper bound is far below what Linux keeps: 30 s).
	FragTimeout = 30 * time.Second
	// fragCopyWindow is how long a completed datagram's fragments are
	// remembered for its late -i any copies (the 50 ms of the copies'
	// window, extract.DupWindow and go-syslog-injector's DedupWindow).
	fragCopyWindow = 50 * time.Millisecond
)

type fragKey struct {
	src, dst netip.Addr
	id       uint32
	proto    uint8
	v6       bool
}

type frag struct {
	off  int
	data []byte
}

type fragDgram struct {
	first time.Time
	parts []frag
	total int // -1 until the last fragment arrives
	bytes int
}

type defrag struct {
	open map[fragKey]*fragDgram
	// done remembers the datagrams completed in the last fragCopyWindow,
	// with their fragments: a late -i any copy of one of them (same key, no
	// NAT) is a copy, not the first fragment of an orphan that expires as
	// lost (SPEC §6.2, review finding 9 of 2026-11-07, as in
	// go-syslog-injector).
	done map[fragKey]*doneDgram
}

type doneDgram struct {
	at    time.Time
	parts [][2]int // offset, length
}

func newDefrag() *defrag {
	return &defrag{open: make(map[fragKey]*fragDgram), done: make(map[fragKey]*doneDgram)}
}

// lateCopy reports whether a fragment of no open datagram repeats one of a
// datagram k completed within fragCopyWindow; one that does not means a new
// datagram on the key, which forgets the old one.
func (d *defrag) lateCopy(k fragKey, off, n int, ts time.Time) bool {
	g := d.done[k]
	if g == nil {
		return false
	}
	if ts.Sub(g.at) <= fragCopyWindow && g.at.Sub(ts) <= fragCopyWindow {
		for _, p := range g.parts {
			if p[0] == off && p[1] == n {
				return true
			}
		}
	}
	delete(d.done, k)
	return false
}

// add stores one fragment. done is true when the datagram is complete; lost is
// the number of fragments dropped by this call (expired datagrams, overlaps,
// over-limit).
func (d *defrag) add(k fragKey, off int, more bool, data []byte, ts time.Time) (whole []byte, done bool, lost int) {
	lost += d.expire(ts)
	g := d.open[k]
	if g == nil {
		if d.lateCopy(k, off, len(data), ts) {
			return nil, false, lost
		}
		if len(d.open) >= maxFragDatagrams {
			return nil, false, lost + 1
		}
		g = &fragDgram{first: ts, total: -1}
		d.open[k] = g
	}
	if off+len(data) > maxDgramBytes || len(g.parts) >= maxFragsPerDgram {
		lost += len(g.parts) + 1
		delete(d.open, k)
		return nil, false, lost
	}
	if !more {
		if g.total >= 0 && g.total != off+len(data) {
			lost += len(g.parts) + 1
			delete(d.open, k)
			return nil, false, lost
		}
		g.total = off + len(data)
	}
	for _, p := range g.parts {
		// The same fragment twice (tcpdump -i any sees a bridged frame on
		// two interfaces) is not an overlap: keep the first copy.
		if p.off == off && len(p.data) == len(data) {
			return nil, false, lost
		}
	}
	g.parts = append(g.parts, frag{off: off, data: append([]byte(nil), data...)})
	g.bytes += len(data)
	if g.total < 0 || g.bytes < g.total {
		return nil, false, lost
	}
	sort.Slice(g.parts, func(i, j int) bool { return g.parts[i].off < g.parts[j].off })
	buf := make([]byte, g.total)
	end := 0
	for _, p := range g.parts {
		if p.off != end {
			// A hole or an overlap (duplicate fragment, or a forged one).
			// Overlaps are refused, as modern stacks do (RFC 5722).
			if p.off < end {
				lost += len(g.parts)
				delete(d.open, k)
				return nil, false, lost
			}
			return nil, false, lost // hole: wait for the missing piece
		}
		copy(buf[p.off:], p.data)
		end = p.off + len(p.data)
	}
	delete(d.open, k)
	if end != g.total {
		return nil, false, lost + len(g.parts)
	}
	if len(d.done) < maxFragDatagrams {
		dd := &doneDgram{at: ts, parts: make([][2]int, len(g.parts))}
		for i, p := range g.parts {
			dd.parts[i] = [2]int{p.off, len(p.data)}
		}
		d.done[k] = dd
	}
	return buf, true, lost
}

// flush drops every open datagram (end of a pass) and returns the fragments
// they held.
func (d *defrag) flush() int {
	lost := 0
	for k, g := range d.open {
		lost += len(g.parts)
		delete(d.open, k)
	}
	clear(d.done)
	return lost
}

func (d *defrag) expire(now time.Time) int {
	lost := 0
	for k, g := range d.open {
		if now.Sub(g.first) > FragTimeout {
			lost += len(g.parts)
			delete(d.open, k)
		}
	}
	for k, g := range d.done {
		if now.Sub(g.at) > fragCopyWindow {
			delete(d.done, k)
		}
	}
	return lost
}
