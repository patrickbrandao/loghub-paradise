package receiver

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"runtime"
	"testing"
	"time"
)

// The GELF UDP reassembler of SPEC §5.2.

var (
	srcA = netip.MustParseAddrPort("192.0.2.1:4000")
	srcB = netip.MustParseAddrPort("192.0.2.2:4000")
	t0   = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
)

// chunk builds one GELF chunk datagram.
func chunk(id uint64, seq, count byte, data []byte) []byte {
	b := make([]byte, chunkHeader, chunkHeader+len(data))
	b[0], b[1] = chunkMagic[0], chunkMagic[1]
	binary.BigEndian.PutUint64(b[2:10], id)
	b[10], b[11] = seq, count
	return append(b, data...)
}

// split cuts data into count chunks of message id.
func split(id uint64, data []byte, count int) [][]byte {
	var out [][]byte
	per := (len(data) + count - 1) / count
	for i := 0; i < count; i++ {
		lo, hi := i*per, (i+1)*per
		if lo > len(data) {
			lo = len(data)
		}
		if hi > len(data) {
			hi = len(data)
		}
		out = append(out, chunk(id, byte(i), byte(count), data[lo:hi]))
	}
	return out
}

func newTestReassembler(cfg ReassemblerConfig) *Reassembler {
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	return NewReassembler(cfg)
}

func TestIsChunk(t *testing.T) {
	for b, want := range map[string]bool{"\x1e\x0f": true, "\x1e\x0fxyz": true, "\x1e": false,
		"": false, "{}": false, "\x0f\x1e": false} {
		if IsChunk([]byte(b)) != want {
			t.Errorf("IsChunk(%q) != %v", b, want)
		}
	}
}

func TestReassemblyInOrderAndOutOfOrder(t *testing.T) {
	data := []byte(doc("a message in three chunks, long enough to split"))
	for name, order := range map[string][]int{"in order": {0, 1, 2}, "out of order": {2, 0, 1}, "reversed": {2, 1, 0}} {
		t.Run(name, func(t *testing.T) {
			r := newTestReassembler(ReassemblerConfig{})
			parts := split(7, data, 3)
			for i, k := range order {
				out, n, res := r.Add(nil, srcA, parts[k], t0.Add(time.Duration(i)*time.Millisecond))
				if i < 2 {
					if res != ChunkPending || n != 0 || out != nil {
						t.Fatalf("chunk %d: %v n=%d", k, res, n)
					}
					continue
				}
				if res != ChunkComplete || n != 3 || !bytes.Equal(out, data) {
					t.Fatalf("completion: %v n=%d %q", res, n, out)
				}
			}
			if r.Open() != 0 || r.Bytes() != 0 {
				t.Fatalf("state left after completion: open %d bytes %d", r.Open(), r.Bytes())
			}
		})
	}
}

// Add APPENDS to dst: the UDP loop reuses one buffer for every message.
func TestReassemblyAppendsToDst(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	dst := make([]byte, 0, 64)
	out, _, res := r.Add(dst, srcA, chunk(1, 0, 1, []byte("solo")), t0)
	if res != ChunkComplete || string(out) != "solo" || &out[:1][0] != &dst[:1][0] {
		t.Fatalf("%v %q: the result does not reuse dst", res, out)
	}
}

// A message in ONE chunk is whole at once and keeps no state.
func TestReassemblySingleChunkKeepsNoState(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	out, n, res := r.Add(nil, srcA, chunk(1, 0, 1, []byte(doc("x"))), t0)
	if res != ChunkComplete || n != 1 || string(out) != doc("x") {
		t.Fatalf("%v n=%d %q", res, n, out)
	}
	if r.Open() != 0 || r.Bytes() != 0 {
		t.Fatal("a single-chunk message left state behind")
	}
}

func TestReassemblyDuplicatesAreIgnored(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	parts := split(9, []byte("abcdef"), 3)
	r.Add(nil, srcA, parts[0], t0)
	before := r.Bytes()
	// A duplicate with DIFFERENT data: the first arrival stands.
	dup := chunk(9, 0, 3, []byte("ZZ"))
	if _, _, res := r.Add(nil, srcA, dup, t0); res != ChunkDuplicate {
		t.Fatalf("duplicate: %v", res)
	}
	if r.Bytes() != before {
		t.Fatal("a duplicate was charged")
	}
	r.Add(nil, srcA, parts[1], t0)
	out, n, res := r.Add(nil, srcA, parts[2], t0)
	if res != ChunkComplete || n != 3 || string(out) != "abcdef" {
		t.Fatalf("%v n=%d %q", res, n, out)
	}
}

// SPEC §5.2: count 1..128, seq < count, at least the 12-byte header.
func TestReassemblyInvalidChunks(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	for name, b := range map[string][]byte{
		"short header":  chunk(1, 0, 2, nil)[:11],
		"magic only":    {0x1e, 0x0f},
		"count 0":       chunk(1, 0, 0, []byte("x")),
		"count 129":     chunk(1, 0, 129, []byte("x")),
		"count 255":     chunk(1, 0, 255, []byte("x")),
		"seq == count":  chunk(1, 2, 2, []byte("x")),
		"seq above all": chunk(1, 200, 128, []byte("x")),
	} {
		if _, n, res := r.Add(nil, srcA, b, t0); res != ChunkDropInvalid || n != 0 {
			t.Errorf("%s: %v", name, res)
		}
	}
	if r.Open() != 0 || r.Bytes() != 0 {
		t.Fatal("an invalid chunk left state")
	}
	// Control: the boundary values are valid.
	if _, _, res := r.Add(nil, srcA, chunk(2, 127, 128, []byte("x")), t0); res != ChunkPending {
		t.Fatalf("seq 127 of 128: %v", res)
	}
	if _, _, res := r.Add(nil, srcA, chunk(3, 0, 1, nil), t0); res != ChunkComplete {
		t.Fatalf("an empty single chunk: %v", res)
	}
}

// A chunk whose count disagrees with the message's: chunk_invalid, the whole
// message dropped (its data released), its remaining chunks swallowed.
func TestReassemblyCountMismatch(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	r.Add(nil, srcA, chunk(4, 0, 3, []byte("aaa")), t0)
	if _, _, res := r.Add(nil, srcA, chunk(4, 1, 4, []byte("bbb")), t0); res != ChunkDropInvalid {
		t.Fatalf("mismatch: %v", res)
	}
	if r.Bytes() != 0 {
		t.Fatalf("the dropped message still holds %d bytes", r.Bytes())
	}
	if _, _, res := r.Add(nil, srcA, chunk(4, 2, 3, []byte("ccc")), t0); res != ChunkSwallowed {
		t.Fatalf("a chunk of the dropped message: %v", res)
	}
	if n := r.Sweep(t0.Add(time.Hour)); n != 0 {
		t.Fatalf("the dropped message was counted again at its deadline: %d", n)
	}
}

// SPEC §5.2: an incomplete message expires on the sweeper's clock, with no
// further chunk, counted ONCE.
func TestReassemblyTimeoutOnAnIdleReassembler(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{Timeout: 5 * time.Second})
	r.Add(nil, srcA, chunk(5, 0, 2, []byte("half")), t0)
	if n := r.Sweep(t0.Add(5*time.Second - time.Nanosecond)); n != 0 {
		t.Fatalf("expired early: %d", n)
	}
	if n := r.Sweep(t0.Add(5 * time.Second)); n != 1 {
		t.Fatalf("Sweep at the deadline = %d, want 1", n)
	}
	if n := r.Sweep(t0.Add(time.Hour)); n != 0 {
		t.Fatalf("counted twice: %d", n)
	}
	if r.Open() != 0 || r.Bytes() != 0 {
		t.Fatal("the expired message left state")
	}
}

// A chunk for a message past its deadline and not swept yet starts a NEW
// message; the old one is counted once, by the next Sweep.
func TestReassemblyExpiredMessageReplaced(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{Timeout: time.Second})
	r.Add(nil, srcA, chunk(6, 0, 2, []byte("old-")), t0)
	late := t0.Add(time.Second)
	if _, _, res := r.Add(nil, srcA, chunk(6, 1, 2, []byte("new1")), late); res != ChunkPending {
		t.Fatalf("the late chunk did not start a new message: %v", res)
	}
	if n := r.Sweep(late); n != 1 {
		t.Fatalf("the replaced message: Sweep = %d, want 1", n)
	}
	out, n, res := r.Add(nil, srcA, chunk(6, 0, 2, []byte("new0")), late.Add(time.Millisecond))
	if res != ChunkComplete || n != 2 || string(out) != "new0new1" {
		t.Fatalf("%v %q: the new message mixed with the old", res, out)
	}
	if n := r.Sweep(late.Add(time.Hour)); n != 0 {
		t.Fatalf("a later Sweep counted again: %d", n)
	}
}

// The key is (src addr:port, id): two emitters with the same id never mix.
func TestReassemblyKeyIncludesTheSource(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{})
	srcA2 := netip.AddrPortFrom(srcA.Addr(), srcA.Port()+1)
	r.Add(nil, srcA, chunk(42, 0, 2, []byte("A0")), t0)
	r.Add(nil, srcB, chunk(42, 0, 2, []byte("B0")), t0)
	r.Add(nil, srcA2, chunk(42, 0, 2, []byte("P0")), t0)
	if r.Open() != 3 {
		t.Fatalf("open = %d, want 3 separate messages", r.Open())
	}
	for src, want := range map[netip.AddrPort]string{srcB: "B0B1", srcA: "A0A1", srcA2: "P0P1"} {
		out, _, res := r.Add(nil, src, chunk(42, 1, 2, []byte(want[2:])), t0)
		if res != ChunkComplete || string(out) != want {
			t.Fatalf("%v: %v %q, want %q", src, res, out, want)
		}
	}
}

// SPEC §5.2: the global ceilings. A chunk that would pass either is dropped
// with its message (chunk_overflow); the messages already open go on.
func TestReassemblyGlobalCeilings(t *testing.T) {
	t.Run("messages", func(t *testing.T) {
		r := newTestReassembler(ReassemblerConfig{MaxMessages: 2})
		r.Add(nil, srcA, chunk(1, 0, 2, []byte("a")), t0)
		r.Add(nil, srcA, chunk(2, 0, 2, []byte("b")), t0)
		if _, _, res := r.Add(nil, srcA, chunk(3, 0, 2, []byte("c")), t0); res != ChunkDropOverflow {
			t.Fatalf("third message: %v", res)
		}
		// A single-chunk message needs no slot.
		if _, _, res := r.Add(nil, srcA, chunk(4, 0, 1, []byte("d")), t0); res != ChunkComplete {
			t.Fatalf("a single chunk at the ceiling: %v", res)
		}
		if out, _, res := r.Add(nil, srcA, chunk(1, 1, 2, []byte("A")), t0); res != ChunkComplete || string(out) != "aA" {
			t.Fatalf("an open message after the overflow: %v %q", res, out)
		}
	})
	t.Run("bytes", func(t *testing.T) {
		data := bytes.Repeat([]byte("x"), 100)
		cost := len(data) + chunkOverhead
		r := newTestReassembler(ReassemblerConfig{MaxBytes: 2*cost + 10})
		r.Add(nil, srcA, chunk(1, 0, 3, data), t0)
		r.Add(nil, srcA, chunk(2, 0, 3, data), t0)
		if r.Bytes() != 2*cost {
			t.Fatalf("bytes = %d, want %d (data plus %d of overhead per chunk)", r.Bytes(), 2*cost, chunkOverhead)
		}
		if _, _, res := r.Add(nil, srcA, chunk(3, 0, 3, data), t0); res != ChunkDropOverflow {
			t.Fatalf("a new message past the byte ceiling: %v", res)
		}
		// A chunk of an OPEN message that would pass it drops that message.
		if _, _, res := r.Add(nil, srcA, chunk(1, 1, 3, data), t0); res != ChunkDropOverflow {
			t.Fatalf("an open message past the byte ceiling: %v", res)
		}
		// Message 3 was born a tombstone and message 1 became one: both
		// hold a slot and no byte, so their other chunks are swallowed and
		// each message is counted once (review 2026-10-31).
		if r.Bytes() != cost || r.Open() != 3 {
			t.Fatalf("after the overflow: bytes %d open %d, want %d and 3", r.Bytes(), r.Open(), cost)
		}
		if _, _, res := r.Add(nil, srcA, chunk(3, 1, 3, data), t0); res != ChunkSwallowed {
			t.Fatalf("the rest of a message refused by the ceiling: %v", res)
		}
		if n := r.Sweep(t0.Add(time.Hour)); n != 1 {
			t.Fatalf("only message 2 should expire, got %d", n)
		}
	})
}

// SPEC §5.2: a message whose data passes the wire ceiling is too_large; the
// entry stays as a tombstone that swallows the rest without another count and
// expires without one.
func TestReassemblyPerMessageCeilingTombstone(t *testing.T) {
	r := newTestReassembler(ReassemblerConfig{MaxMessageWire: 100})
	r.Add(nil, srcA, chunk(8, 0, 3, make([]byte, 60)), t0)
	if _, _, res := r.Add(nil, srcA, chunk(8, 1, 3, make([]byte, 41)), t0); res != ChunkDropTooLarge {
		t.Fatalf("past the wire ceiling: %v", res)
	}
	if r.Bytes() != 0 || r.Open() != 1 {
		t.Fatalf("tombstone: bytes %d open %d, want 0 and 1", r.Bytes(), r.Open())
	}
	if _, _, res := r.Add(nil, srcA, chunk(8, 2, 3, []byte("x")), t0); res != ChunkSwallowed {
		t.Fatalf("a chunk of the dead message: %v", res)
	}
	if _, _, res := r.Add(nil, srcA, chunk(8, 1, 3, []byte("x")), t0); res != ChunkSwallowed {
		t.Fatalf("a retransmission into the dead message: %v", res)
	}
	if n := r.Sweep(t0.Add(time.Hour)); n != 0 || r.Open() != 0 {
		t.Fatalf("the tombstone was counted at its deadline (%d) or stayed (%d)", n, r.Open())
	}
	// Exactly the ceiling is a message; a single chunk above it is too large.
	r.Add(nil, srcA, chunk(9, 0, 2, make([]byte, 50)), t0)
	if _, _, res := r.Add(nil, srcA, chunk(9, 1, 2, make([]byte, 50)), t0); res != ChunkComplete {
		t.Fatalf("exactly the wire ceiling: %v", res)
	}
	if _, _, res := r.Add(nil, srcA, chunk(10, 0, 1, make([]byte, 101)), t0); res != ChunkDropTooLarge {
		t.Fatalf("a single chunk above the ceiling: %v", res)
	}
}

// Nothing is allocated from the declared count: a thousand messages that each
// declare 128 chunks and send one byte cost what arrived, not what was
// declared.
func TestReassemblyNeverAllocatesFromCount(t *testing.T) {
	const n = 1000
	r := newTestReassembler(ReassemblerConfig{})
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = chunk(uint64(i), 0, 128, []byte{'x'})
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range chunks {
		if _, _, res := r.Add(nil, srcA, chunks[i], t0); res != ChunkPending {
			t.Fatal(res)
		}
	}
	runtime.ReadMemStats(&after)
	per := (after.TotalAlloc - before.TotalAlloc) / n
	t.Logf("%d bytes allocated per open message", per)
	if per > 1024 {
		t.Fatalf("%d bytes per one-byte chunk of a 128-chunk message: allocated from count", per)
	}
	if r.Bytes() != n*(1+chunkOverhead) {
		t.Fatalf("bytes = %d", r.Bytes())
	}
}

func TestNewReassemblerDefaults(t *testing.T) {
	r := NewReassembler(ReassemblerConfig{})
	if r.cfg.MaxBytes != DefaultReassemblyMaxBytes || r.cfg.MaxMessages != DefaultReassemblyMaxMessages ||
		r.cfg.MaxMessageWire != WireLimit(defaultMaxMessageBytes) || r.cfg.Timeout != ChunkTimeout {
		t.Fatalf("defaults %+v", r.cfg)
	}
}

// ---------------------------------------------------------------- fuzzing

// fuzzData is the deterministic data of chunk seq of message (src, id, count):
// any two arrivals for one slot carry the same bytes, so a completed message
// has exactly one right answer.
func fuzzData(src, id, seq, count byte) []byte {
	return bytes.Repeat([]byte{src, id, seq, count}, int(seq%3)+1)
}

// FuzzReassemble drives the reassembler with a stream of operations decoded
// from the input: chunks from two sources and a few ids, raw datagrams from a
// third source, and clock steps with sweeps. It never panics, never passes
// its ceilings, every completed message is the concatenation of its chunks in
// seq order, and a sweep far in the future returns every byte.
func FuzzReassemble(f *testing.F) {
	f.Add([]byte{0, 0, 0, 3, 0, 1, 0, 3, 0, 2, 0, 3})
	f.Add([]byte{0, 0, 2, 3, 1, 0, 0, 2, 0, 0, 1, 3, 1, 0, 1, 2, 0, 0, 0, 3})
	f.Add([]byte{2, 0, 0, 0, 3, 200, 0, 0, 0, 0, 1, 0})
	f.Add([]byte{0, 1, 0, 2, 3, 9, 9, 9, 0, 1, 1, 2})
	f.Add([]byte{0, 1, 0, 200, 0, 1, 1, 7, 2, 2, 5, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		cfg := ReassemblerConfig{MaxBytes: 600, MaxMessages: 4, MaxMessageWire: 60, Timeout: 10 * time.Millisecond}
		r := NewReassembler(cfg)
		srcs := []netip.AddrPort{srcA, srcB, netip.MustParseAddrPort("[2001:db8::1]:9")}
		now := t0
		for len(ops) >= 4 {
			op, a, b, c := ops[0], ops[1], ops[2], ops[3]
			ops = ops[4:]
			switch op % 4 {
			case 0, 1: // a well-formed chunk from source op%2
				src := op % 2
				id, count := a%3, c%6+1
				seq := b % (count + 1) // sometimes seq == count: invalid
				out, n, res := r.Add(nil, srcs[src], chunk(uint64(id), seq, count, fuzzData(src, id, seq, count)), now)
				if res == ChunkComplete {
					var want []byte
					for s := byte(0); s < count; s++ {
						want = append(want, fuzzData(src, id, s, count)...)
					}
					if n != int(count) || !bytes.Equal(out, want) {
						t.Fatalf("completed %d/%d: %x, want %x", n, count, out, want)
					}
				} else if n != 0 || out != nil {
					t.Fatalf("%v returned data", res)
				}
			case 2: // a raw datagram, possibly short or out of range
				raw := append([]byte{0x1e, 0x0f, 0, 0, 0, 0, 0, 0, 0, a % 2}, b, c)
				r.Add(nil, srcs[2], raw[:int(a)%len(raw)+1], now)
			case 3: // the clock moves; sometimes a sweep
				now = now.Add(time.Duration(a) * time.Millisecond / 4)
				if b%2 == 0 {
					if n := r.Sweep(now); n < 0 {
						t.Fatal(n)
					}
				}
			}
			if r.Bytes() < 0 || r.Bytes() > cfg.MaxBytes {
				t.Fatalf("bytes %d outside 0..%d", r.Bytes(), cfg.MaxBytes)
			}
			if r.Open() > cfg.MaxMessages {
				t.Fatalf("%d open messages past the ceiling %d", r.Open(), cfg.MaxMessages)
			}
		}
		r.Sweep(now.Add(time.Hour))
		if r.Bytes() != 0 || r.Open() != 0 {
			t.Fatalf("after the final sweep: bytes %d open %d", r.Bytes(), r.Open())
		}
	})
}
