package receiver

import (
	"hash/maphash"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
)

// GELF UDP chunks (SPEC §5.2).
//
//	 0      2                10    11      12
//	 | 1e 0f| message id (8)  | seq | count | data ...
//
// The reassembler is shared state with ceilings of its own, and every rule in
// it is one that a hostile or broken peer would otherwise turn into memory:
//
//   - nothing is allocated from count: each chunk is copied into a buffer of
//     its own size, the message keeps the chunks that ARRIVED and a 128-bit
//     map of which sequence numbers it has;
//   - a global byte ceiling and a ceiling of open messages bound the whole
//     state, whatever the peers declare;
//   - a message expires 5 s after its first chunk, swept by a clock of its own
//     (Sweep), never only when another chunk arrives: on an idle server an
//     orphan would otherwise stay forever.
//
// The open messages live in shards picked by a seeded hash of (src-ip,
// src-port, id), each under its own mutex, and the two ceilings are atomic
// counters shared by every shard (SPEC §5.2). One map under one mutex was
// measured on 2026-11-07 (BenchmarkUDPChunkFlood, SPEC §5.2): with 4 and 8
// listeners it held the flood near the rate of 1 or 2, while one reassembler
// per listener scaled. The ceilings stay GLOBAL: a per-shard ceiling would
// let one hot shard refuse while the others sit empty, or let the sum pass
// what the operator configured.

// ChunkMagic are the two first bytes of a GELF chunk.
var chunkMagic = [2]byte{0x1e, 0x0f}

// chunkHeader is magic + id + seq + count.
const chunkHeader = 12

// ChunkTimeout is the GELF reassembly deadline (SPEC §5.2).
const ChunkTimeout = 5 * time.Second

// chunkOverhead is what one chunk costs in the byte accounting beyond its
// data: the slice header and the struct around it, rounded up. Without it a
// flood of one-byte chunks would hold far more than the ceiling says.
const chunkOverhead = 64

// reassemblyShards is the number of shards: a power of two well above the
// listeners of one host, small enough that Sweep's pass over them is cheap.
// Every message lives in exactly one shard, so nothing in a message's life
// crosses two locks.
const reassemblyShards = 64

// Defaults of listen.udp_reassembly_max_bytes and _max_messages (SPEC §16.5).
const (
	DefaultReassemblyMaxBytes    = 64 << 20
	DefaultReassemblyMaxMessages = 16384
)

// IsChunk reports whether a datagram starts with the chunk magic.
func IsChunk(b []byte) bool { return len(b) >= 2 && b[0] == chunkMagic[0] && b[1] == chunkMagic[1] }

type chunkKey struct {
	src  netip.AddrPort
	id   [8]byte
	used bool
}

type chunkPart struct {
	seq  uint8
	data []byte
}

type partial struct {
	count    uint8
	have     [2]uint64 // bitmap of the sequence numbers received
	received int       // chunks received
	parts    []chunkPart
	bytes    int // data bytes, for the per-message ceiling
	cost     int // bytes charged to the global ceiling
	deadline time.Time
	// dead marks a message already counted as a drop (too large): its
	// remaining chunks are swallowed until the deadline without another
	// count (SPEC §5.2, "lápide").
	dead bool
}

func (p *partial) has(seq uint8) bool { return p.have[seq/64]&(1<<(seq%64)) != 0 }
func (p *partial) set(seq uint8)      { p.have[seq/64] |= 1 << (seq % 64) }

// ReassemblerConfig bounds a Reassembler.
type ReassemblerConfig struct {
	// MaxBytes and MaxMessages are listen.udp_reassembly_max_bytes and
	// listen.udp_reassembly_max_messages.
	MaxBytes    int
	MaxMessages int
	// MaxMessageWire is the most data one message may carry before
	// decompression: WireLimit(convert.max_message_bytes).
	MaxMessageWire int
	// Timeout is ChunkTimeout unless a test shortens it.
	Timeout time.Duration
}

// ChunkResult is what Add did with one chunk.
type ChunkResult uint8

const (
	// ChunkPending: stored, the message is not complete yet.
	ChunkPending ChunkResult = iota
	// ChunkComplete: the message is whole; Add returned its data.
	ChunkComplete
	// ChunkDuplicate: a sequence number already received; ignored.
	ChunkDuplicate
	// ChunkSwallowed: a chunk of a message already dropped; ignored.
	ChunkSwallowed
	// ChunkDropInvalid, ChunkDropOverflow, ChunkDropTooLarge: the chunk and
	// its message are dropped, and the caller counts ONE drop.
	ChunkDropInvalid
	ChunkDropOverflow
	ChunkDropTooLarge
)

// Reassembler holds the open messages of every UDP socket of the listener.
type Reassembler struct {
	cfg  ReassemblerConfig
	seed maphash.Seed
	// mask picks the shard from the hash: len(shards)-1.
	mask   uint64
	shards []reasmShard

	// bytes and open are the global ceilings' counters (SPEC §5.2), shared
	// by every shard. bytes only grows through reserveBytes and open
	// (entries, tombstones included) only through reserveSlot: CAS loops
	// that never let the sum pass MaxBytes or MaxMessages.
	bytes atomic.Int64
	open  atomic.Int64
	// expired counts messages Add found past their deadline and replaced;
	// Sweep reports them.
	expired atomic.Int64
}

// reasmShard is one lock and its map, padded so two shards' mutexes never
// share a cache line (the false sharing would bring the contention back).
type reasmShard struct {
	mu   sync.Mutex
	open map[chunkKey]*partial
	_    [64]byte
}

// NewReassembler builds one with cfg; zero fields take the defaults.
func NewReassembler(cfg ReassemblerConfig) *Reassembler {
	return newReassembler(cfg, reassemblyShards)
}

// newReassembler takes the shard count (a power of two) so the benchmark can
// measure the single-lock layout it replaced (shards = 1).
func newReassembler(cfg ReassemblerConfig, shards int) *Reassembler {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultReassemblyMaxBytes
	}
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = DefaultReassemblyMaxMessages
	}
	if cfg.MaxMessageWire <= 0 {
		cfg.MaxMessageWire = WireLimit(defaultMaxMessageBytes)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = ChunkTimeout
	}
	if shards < 1 || shards&(shards-1) != 0 {
		panic("receiver: reassembler shards must be a power of two")
	}
	r := &Reassembler{cfg: cfg, seed: maphash.MakeSeed(), mask: uint64(shards - 1),
		shards: make([]reasmShard, shards)}
	for i := range r.shards {
		r.shards[i].open = map[chunkKey]*partial{}
	}
	return r
}

// shard is the home of k. The hash is seeded per process: the id is the
// emitter's choice, and an unseeded hash would let one emitter aim all its
// messages at one shard (contention only, never correctness).
func (r *Reassembler) shard(k *chunkKey) *reasmShard {
	var b [26]byte
	a := k.src.Addr().As16()
	copy(b[:16], a[:])
	b[16], b[17] = byte(k.src.Port()>>8), byte(k.src.Port())
	copy(b[18:], k.id[:])
	return &r.shards[maphash.Bytes(r.seed, b[:])&r.mask]
}

// reserveSlot takes one of MaxMessages, or reports that none is left.
func (r *Reassembler) reserveSlot() bool {
	for {
		n := r.open.Load()
		if n >= int64(r.cfg.MaxMessages) {
			return false
		}
		if r.open.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// reserveBytes charges cost to MaxBytes, or reports that it does not fit. A
// CAS loop and not add-then-undo: two chunks that each fit alone must not
// both be refused because together they overshot for an instant.
func (r *Reassembler) reserveBytes(cost int) bool {
	for {
		n := r.bytes.Load()
		if n+int64(cost) > int64(r.cfg.MaxBytes) {
			return false
		}
		if r.bytes.CompareAndSwap(n, n+int64(cost)) {
			return true
		}
	}
}

// Add files one chunk from src received at now. With ChunkComplete it appends
// the reassembled data to dst and returns it with the chunk count; the data no
// longer belongs to the reassembler.
func (r *Reassembler) Add(dst []byte, src netip.AddrPort, b []byte, now time.Time) ([]byte, int, ChunkResult) {
	if len(b) < chunkHeader {
		return dst, 0, ChunkDropInvalid
	}
	seq, count := b[10], b[11]
	if count == 0 || int(count) > item.MaxChunks || seq >= count {
		return dst, 0, ChunkDropInvalid
	}
	data := b[chunkHeader:]
	k := chunkKey{src: src, used: true}
	copy(k.id[:], b[2:10])

	sh := r.shard(&k)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	p := sh.open[k]
	if p != nil && !now.Before(p.deadline) {
		// Expired and not swept yet: this chunk starts a new message (SPEC
		// §5.2). The old one is the chunk_timeout drop the sweeper would
		// have counted; Add has one result for the NEW chunk, so the old
		// drop is carried to the next Sweep, which reports it.
		if !p.dead {
			r.expired.Add(1)
		}
		r.remove(sh, k, p)
		p = nil
	}
	if p != nil && p.dead {
		return dst, 0, ChunkSwallowed
	}
	if p != nil && p.count != count {
		// A chunk that disagrees with the message about its size: the
		// message cannot be trusted any more.
		r.kill(p)
		return dst, 0, ChunkDropInvalid
	}
	if p != nil && p.has(seq) {
		return dst, 0, ChunkDuplicate
	}

	cost := len(data) + chunkOverhead
	if p == nil {
		if count == 1 {
			// A message in a single chunk is whole at once: no state.
			if len(data) > r.cfg.MaxMessageWire {
				return dst, 0, ChunkDropTooLarge
			}
			return append(dst, data...), 1, ChunkComplete
		}
		if !r.reserveSlot() {
			// No room to remember the message, not even as a tombstone:
			// each of its chunks is refused and counted on its own (SPEC
			// §5.2, "max_messages cheio"). The open ones are not evicted,
			// or a flood of first chunks would destroy every message in
			// reassembly.
			return dst, 0, ChunkDropOverflow
		}
		if !r.reserveBytes(cost) {
			// A slot but no bytes: the message is born a tombstone, which
			// costs no byte, so its other chunks are swallowed and the
			// message is counted ONCE.
			sh.open[k] = &partial{count: count, deadline: now.Add(r.cfg.Timeout), dead: true}
			return dst, 0, ChunkDropOverflow
		}
		p = &partial{count: count, deadline: now.Add(r.cfg.Timeout)}
		sh.open[k] = p
	} else if !r.reserveBytes(cost) {
		// kill, not remove: without the tombstone the next chunk of the
		// same message opened a new entry that never completed and was
		// counted again as chunk_timeout (2026-10-31 review).
		r.kill(p)
		return dst, 0, ChunkDropOverflow
	}
	// From here cost is charged to the global ceiling and not yet to p.
	if p.bytes+len(data) > r.cfg.MaxMessageWire {
		r.bytes.Add(-int64(cost))
		r.kill(p)
		return dst, 0, ChunkDropTooLarge
	}

	p.set(seq)
	p.received++
	p.parts = append(p.parts, chunkPart{seq: seq, data: append([]byte(nil), data...)})
	p.bytes += len(data)
	p.cost += cost
	if p.received < int(p.count) {
		return dst, 0, ChunkPending
	}

	sort.Slice(p.parts, func(i, j int) bool { return p.parts[i].seq < p.parts[j].seq })
	for _, part := range p.parts {
		dst = append(dst, part.data...)
	}
	n := int(p.count)
	r.remove(sh, k, p)
	return dst, n, ChunkComplete
}

// remove forgets a message and returns its bytes and its slot to the
// ceilings. sh.mu is held.
func (r *Reassembler) remove(sh *reasmShard, k chunkKey, p *partial) {
	r.bytes.Add(-int64(p.cost))
	r.open.Add(-1)
	delete(sh.open, k)
}

// kill turns a message into a tombstone: its data is released at once, and
// the entry (and its slot) stays until the deadline to swallow the remaining
// chunks. The shard's mu is held.
func (r *Reassembler) kill(p *partial) {
	r.bytes.Add(-int64(p.cost))
	p.cost, p.bytes, p.parts, p.dead = 0, 0, nil, true
}

// Sweep drops every message past its deadline at now and returns how many
// were incomplete (each one is a chunk_timeout drop). Tombstones expire
// without a count: they were counted when they died. It also returns the
// expired messages Add replaced since the last sweep.
//
// It takes the shards one at a time: a listener waits at most for the pass
// over one shard, never for the whole state (the single map of 16 384
// messages held its one lock ~0.16 ms per sweep, SPEC §5.2).
func (r *Reassembler) Sweep(now time.Time) int {
	n := int(r.expired.Swap(0))
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for k, p := range sh.open {
			if now.Before(p.deadline) {
				continue
			}
			if !p.dead {
				n++
			}
			r.remove(sh, k, p)
		}
		sh.mu.Unlock()
	}
	return n
}

// Bytes is the current charge against the byte ceiling, for the gauge. An
// atomic load: udpLoop reads it after every chunk, and under the old single
// lock that was a second acquisition per chunk.
func (r *Reassembler) Bytes() int { return int(r.bytes.Load()) }

// Open is the number of messages in reassembly, tombstones included.
func (r *Reassembler) Open() int { return int(r.open.Load()) }
