package receiver

import (
	"bytes"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// SPEC §5.2 (2026-11-07): the reassembly is sharded by a hash of (src-ip,
// src-port, id), and the two ceilings stay GLOBAL. These tests pin that the
// ceilings are not per shard, and that the atomic accounting balances under
// concurrent listeners.

// idsInDistinctShards finds n message ids of src whose keys land in n
// different shards of r, so a test is about crossing shards by construction,
// not by the luck of the seed.
func idsInDistinctShards(t *testing.T, r *Reassembler, src netip.AddrPort, n int) []uint64 {
	t.Helper()
	seen := map[*reasmShard]bool{}
	var ids []uint64
	for id := uint64(1); len(ids) < n && id < 1<<16; id++ {
		k := chunkKey{src: src, used: true}
		c := chunk(id, 0, 2, nil)
		copy(k.id[:], c[2:10])
		if sh := r.shard(&k); !seen[sh] {
			seen[sh] = true
			ids = append(ids, id)
		}
	}
	if len(ids) < n {
		t.Fatalf("found %d ids in distinct shards, want %d", len(ids), n)
	}
	return ids
}

func TestReassemblyCeilingsAreGlobalAcrossShards(t *testing.T) {
	t.Run("messages", func(t *testing.T) {
		r := NewReassembler(ReassemblerConfig{MaxMessages: 3})
		ids := idsInDistinctShards(t, r, srcA, 4)
		for _, id := range ids[:3] {
			if _, _, res := r.Add(nil, srcA, chunk(id, 0, 2, []byte("a")), t0); res != ChunkPending {
				t.Fatalf("message %d under the ceiling: %v", id, res)
			}
		}
		// The fourth lives in a shard of its own, empty: still refused.
		if _, _, res := r.Add(nil, srcA, chunk(ids[3], 0, 2, []byte("a")), t0); res != ChunkDropOverflow {
			t.Fatalf("a fourth message in an empty shard: %v (the ceiling is per shard)", res)
		}
		if r.Open() != 3 {
			t.Fatalf("open = %d, want 3", r.Open())
		}
	})
	t.Run("bytes", func(t *testing.T) {
		data := bytes.Repeat([]byte("x"), 100)
		cost := len(data) + chunkOverhead
		r := NewReassembler(ReassemblerConfig{MaxBytes: 2*cost + 10})
		ids := idsInDistinctShards(t, r, srcA, 3)
		r.Add(nil, srcA, chunk(ids[0], 0, 3, data), t0)
		r.Add(nil, srcA, chunk(ids[1], 0, 3, data), t0)
		if _, _, res := r.Add(nil, srcA, chunk(ids[2], 0, 3, data), t0); res != ChunkDropOverflow {
			t.Fatalf("a message in an empty shard past the global byte ceiling: %v", res)
		}
		if r.Bytes() != 2*cost {
			t.Fatalf("bytes = %d, want %d", r.Bytes(), 2*cost)
		}
	})
}

// Many listeners at once, every outcome mixed (complete, duplicate, too
// large, overflow, expired): neither the byte charge nor the open messages
// ever pass their ceilings at any instant (the left-open messages fill
// MaxMessages, so its CAS reservation is raced too), and once everything
// completes or is swept both counters return to zero. A leak in any path (the
// undo of the too_large chunk, a tombstone's slot) or an add-then-check race
// shows here. Run with -race.
func TestReassemblyConcurrentAccountingBalances(t *testing.T) {
	const (
		listeners   = 8
		perL        = 400
		maxBytes    = 64 << 10
		maxMessages = 256
	)
	r := NewReassembler(ReassemblerConfig{MaxBytes: maxBytes, MaxMessages: maxMessages, MaxMessageWire: 3000})
	data := bytes.Repeat([]byte("y"), 700)
	big := bytes.Repeat([]byte("z"), 2000)
	var wg sync.WaitGroup
	errs := make(chan string, listeners)
	for l := 0; l < listeners; l++ {
		wg.Add(1)
		go func(l int) {
			defer wg.Done()
			src := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.9"), uint16(2000+l))
			for i := 0; i < perL; i++ {
				id := uint64(l)<<32 | uint64(i)
				var chunks [][]byte
				switch i % 4 {
				case 0, 1: // whole, with a duplicate in the middle
					chunks = [][]byte{chunk(id, 0, 3, data), chunk(id, 0, 3, data),
						chunk(id, 1, 3, data), chunk(id, 2, 3, data)}
				case 2: // too large on its second chunk, then swallowed
					chunks = [][]byte{chunk(id, 0, 3, big), chunk(id, 1, 3, big), chunk(id, 2, 3, big)}
				case 3: // left open: only the sweep ends it
					chunks = [][]byte{chunk(id, 0, 2, data)}
				}
				for _, c := range chunks {
					r.Add(nil, src, c, t0)
					if b := r.Bytes(); b > maxBytes || b < 0 {
						errs <- "bytes out of range"
						return
					}
					if o := r.Open(); o > maxMessages || o < 0 {
						errs <- "open messages out of range"
						return
					}
				}
			}
		}(l)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	r.Sweep(t0.Add(time.Hour))
	if r.Bytes() != 0 || r.Open() != 0 {
		t.Fatalf("after everything ended: bytes %d open %d, want 0 and 0", r.Bytes(), r.Open())
	}
	for i := range r.shards {
		if n := len(r.shards[i].open); n != 0 {
			t.Fatalf("shard %d still holds %d entries", i, n)
		}
	}
}
