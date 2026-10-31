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
}

func newDefrag() *defrag { return &defrag{open: make(map[fragKey]*fragDgram)} }

// add stores one fragment. done is true when the datagram is complete; lost is
// the number of fragments dropped by this call (expired datagrams, overlaps,
// over-limit).
func (d *defrag) add(k fragKey, off int, more bool, data []byte, ts time.Time) (whole []byte, done bool, lost int) {
	lost += d.expire(ts)
	g := d.open[k]
	if g == nil {
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
	return buf, true, lost
}

func (d *defrag) expire(now time.Time) int {
	lost := 0
	for k, g := range d.open {
		if now.Sub(g.first) > FragTimeout {
			lost += len(g.parts)
			delete(d.open, k)
		}
	}
	return lost
}
