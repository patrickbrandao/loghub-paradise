package gelf

import (
	"hash/maphash"
	"net/netip"
	"slices"
	"time"
)

// Reassembly limits (SPEC §6.3), the same the servers apply.
const (
	// ChunkTimeout: a message whose chunks did not all arrive within this
	// time (capture time, since its first chunk) is dropped.
	ChunkTimeout = 5 * time.Second
	// MaxOpen bounds the messages being reassembled.
	MaxOpen = 1 << 16
	// MaxOpenBytes bounds the bytes held by them.
	MaxOpenBytes = 64 << 20
	// doneMemory: how long a completed id is remembered, so a late copy of
	// one of its chunks (tcpdump -i any sees a packet twice) does not open a
	// message that never completes. Only a byte-identical copy is a
	// duplicate: a different chunk under a remembered id is a new message
	// of a sender that reused the id (SPEC §6.3).
	doneMemory = ChunkTimeout
)

// ReasmStats counts what the reassembler did.
type ReasmStats struct {
	Chunks        uint64 // chunk datagrams seen
	Messages      uint64 // messages completed
	Invalid       uint64 // bad header: count 0 or > 128, seq >= count, count changed
	Duplicates    uint64 // a chunk already held, or of a message already completed
	Incomplete    uint64 // messages dropped at their deadline or at the end
	ChunksDropped uint64 // chunks of those messages
	Overflow      uint64 // chunks refused by MaxOpen / MaxOpenBytes
}

type reasmKey struct {
	src netip.AddrPort
	id  [8]byte
}

type partial struct {
	first time.Time
	count int
	have  int
	size  int
	parts [][]byte // nil until arrived; never allocated from count alone
	sums  []uint64 // hash of each chunk datagram, for the done memory
}

// doneMsg remembers a completed message: when, and the hash of each of its
// chunk datagrams (at most 128).
type doneMsg struct {
	at   time.Time
	sums []uint64
}

// Reassembler rebuilds chunked GELF messages from captured datagrams. Time is
// capture time: expiry runs as the capture clock advances, and Flush ends
// what is open.
type Reassembler struct {
	Stats ReasmStats
	open  map[reasmKey]*partial
	done  map[reasmKey]doneMsg
	bytes int
	swept time.Time
	seed  maphash.Seed
}

// NewReassembler returns an empty reassembler.
func NewReassembler() *Reassembler {
	return &Reassembler{open: map[reasmKey]*partial{}, done: map[reasmKey]doneMsg{}, seed: maphash.MakeSeed()}
}

// Add takes one chunk datagram (IsChunk is true) from src at capture time
// ts. When it completes a message, Add returns the payload (still
// compressed, as sent) and its chunk count.
func (r *Reassembler) Add(src netip.AddrPort, b []byte, ts time.Time) (msg []byte, chunks int, ok bool) {
	r.Stats.Chunks++
	r.sweep(ts)
	if len(b) < HeaderLen {
		r.Stats.Invalid++
		return nil, 0, false
	}
	var k reasmKey
	k.src = src
	copy(k.id[:], b[2:10])
	seq, count := int(b[10]), int(b[11])
	if count == 0 || count > MaxChunks || seq >= count {
		r.Stats.Invalid++
		return nil, 0, false
	}
	sum := maphash.Bytes(r.seed, b)
	if d, seen := r.done[k]; seen {
		switch {
		case ts.Sub(d.at) > doneMemory:
			delete(r.done, k) // exact even between sweeps
		case slices.Contains(d.sums, sum):
			r.Stats.Duplicates++
			return nil, 0, false
		}
		// Otherwise a new message reusing the id: reassembled normally.
	}
	p := r.open[k]
	if p != nil && ts.Sub(p.first) > ChunkTimeout {
		// Past its deadline: dropped now, exactly as the receiver would
		// have, not at the next sweep (which runs once a second of capture
		// time). This chunk starts a new message.
		r.drop(k, p)
		p = nil
	}
	if p == nil {
		if len(r.open) >= MaxOpen {
			r.Stats.Overflow++
			return nil, 0, false
		}
		p = &partial{first: ts, count: count}
		r.open[k] = p
	}
	if p.count != count {
		r.Stats.Invalid++
		return nil, 0, false
	}
	if p.parts == nil {
		p.parts = make([][]byte, count) // count <= 128: bounded
	}
	if p.parts[seq] != nil {
		r.Stats.Duplicates++
		return nil, 0, false
	}
	part := b[HeaderLen:]
	if r.bytes+len(part) > MaxOpenBytes {
		r.Stats.Overflow++
		return nil, 0, false
	}
	p.parts[seq] = append(make([]byte, 0, len(part)+1), part...)
	if len(part) == 0 {
		p.parts[seq] = p.parts[seq][:0:1] // non-nil: marks the chunk as arrived
	}
	p.sums = append(p.sums, sum)
	p.have++
	p.size += len(part)
	r.bytes += len(part)
	if p.have < p.count {
		return nil, 0, false
	}
	out := make([]byte, 0, p.size)
	for _, q := range p.parts {
		out = append(out, q...)
	}
	r.bytes -= p.size
	delete(r.open, k)
	r.done[k] = doneMsg{at: ts, sums: p.sums}
	r.Stats.Messages++
	return out, p.count, true
}

func (r *Reassembler) sweep(now time.Time) {
	if now.IsZero() || now.Sub(r.swept) < time.Second && !r.swept.IsZero() {
		return
	}
	r.swept = now
	for k, p := range r.open {
		if now.Sub(p.first) > ChunkTimeout {
			r.drop(k, p)
		}
	}
	for k, d := range r.done {
		if now.Sub(d.at) > doneMemory {
			delete(r.done, k)
		}
	}
}

func (r *Reassembler) drop(k reasmKey, p *partial) {
	r.Stats.Incomplete++
	r.Stats.ChunksDropped += uint64(p.have)
	r.bytes -= p.size
	delete(r.open, k)
}

// Flush drops every message still open (the end of a capture pass).
func (r *Reassembler) Flush() {
	for k, p := range r.open {
		r.drop(k, p)
	}
	clear(r.done)
}
