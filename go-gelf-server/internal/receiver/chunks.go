package receiver

import (
	"net/netip"
	"sort"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
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
	cfg ReassemblerConfig

	mu    sync.Mutex
	open  map[chunkKey]*partial
	bytes int
	// expired counts messages Add found past their deadline and replaced;
	// Sweep reports them.
	expired int
}

// NewReassembler builds one with cfg; zero fields take the defaults.
func NewReassembler(cfg ReassemblerConfig) *Reassembler {
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
	return &Reassembler{cfg: cfg, open: map[chunkKey]*partial{}}
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

	r.mu.Lock()
	defer r.mu.Unlock()

	p := r.open[k]
	if p != nil && !now.Before(p.deadline) {
		// Expired and not swept yet: this chunk starts a new message (SPEC
		// §5.2). The old one is the chunk_timeout drop the sweeper would
		// have counted; Add has one result for the NEW chunk, so the old
		// drop is carried to the next Sweep, which reports it.
		if !p.dead {
			r.expired++
		}
		r.remove(k, p)
		p = nil
	}
	if p != nil && p.dead {
		return dst, 0, ChunkSwallowed
	}
	if p != nil && p.count != count {
		// A chunk that disagrees with the message about its size: the
		// message cannot be trusted any more.
		r.kill(k, p)
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
		if len(r.open) >= r.cfg.MaxMessages {
			// No room to remember the message, not even as a tombstone:
			// each of its chunks is refused and counted on its own (SPEC
			// §5.2, "max_messages cheio"). The open ones are not evicted,
			// or a flood of first chunks would destroy every message in
			// reassembly.
			return dst, 0, ChunkDropOverflow
		}
		if r.bytes+cost > r.cfg.MaxBytes {
			// A slot but no bytes: the message is born a tombstone, which
			// costs no byte, so its other chunks are swallowed and the
			// message is counted ONCE.
			r.open[k] = &partial{count: count, deadline: now.Add(r.cfg.Timeout), dead: true}
			return dst, 0, ChunkDropOverflow
		}
		p = &partial{count: count, deadline: now.Add(r.cfg.Timeout)}
		r.open[k] = p
	} else if r.bytes+cost > r.cfg.MaxBytes {
		// kill, not remove: without the tombstone the next chunk of the
		// same message opened a new entry that never completed and was
		// counted again as chunk_timeout (2026-10-31 review).
		r.kill(k, p)
		return dst, 0, ChunkDropOverflow
	}
	if p.bytes+len(data) > r.cfg.MaxMessageWire {
		r.kill(k, p)
		return dst, 0, ChunkDropTooLarge
	}

	p.set(seq)
	p.received++
	p.parts = append(p.parts, chunkPart{seq: seq, data: append([]byte(nil), data...)})
	p.bytes += len(data)
	p.cost += cost
	r.bytes += cost
	if p.received < int(p.count) {
		return dst, 0, ChunkPending
	}

	sort.Slice(p.parts, func(i, j int) bool { return p.parts[i].seq < p.parts[j].seq })
	for _, part := range p.parts {
		dst = append(dst, part.data...)
	}
	n := int(p.count)
	r.remove(k, p)
	return dst, n, ChunkComplete
}

// remove forgets a message and returns its bytes to the ceiling.
func (r *Reassembler) remove(k chunkKey, p *partial) {
	r.bytes -= p.cost
	delete(r.open, k)
}

// kill turns a message into a tombstone: its data is released at once, and
// the entry stays until the deadline to swallow the remaining chunks.
func (r *Reassembler) kill(k chunkKey, p *partial) {
	r.bytes -= p.cost
	p.cost, p.bytes, p.parts, p.dead = 0, 0, nil, true
}

// Sweep drops every message past its deadline at now and returns how many
// were incomplete (each one is a chunk_timeout drop). Tombstones expire
// without a count: they were counted when they died. It also returns the
// expired messages Add replaced since the last sweep.
func (r *Reassembler) Sweep(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.expired
	r.expired = 0
	for k, p := range r.open {
		if now.Before(p.deadline) {
			continue
		}
		if !p.dead {
			n++
		}
		r.remove(k, p)
	}
	return n
}

// Bytes is the current charge against the byte ceiling, for the gauge.
func (r *Reassembler) Bytes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// Open is the number of messages in reassembly, tombstones included.
func (r *Reassembler) Open() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.open)
}
