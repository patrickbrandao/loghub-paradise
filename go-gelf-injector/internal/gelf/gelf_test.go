package gelf

import (
	"bytes"
	"compress/gzip"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"
)

func TestCompressDetectDecompress(t *testing.T) {
	doc := bytes.Repeat([]byte(`{"short_message":"hello"}`), 50)
	var z Compressor
	for _, c := range []Compression{None, Gzip, Zlib} {
		p := append([]byte(nil), z.Compress(c, doc)...)
		if Detect(p) != c {
			t.Fatalf("%v detected as %v", c, Detect(p))
		}
		out, err := Decompress(p, c, len(doc))
		if err != nil || !bytes.Equal(out, doc) {
			t.Fatalf("%v: %v", c, err)
		}
		if c != None {
			if _, err := Decompress(p, c, len(doc)-1); err != ErrTooLarge {
				t.Fatalf("%v: limit not enforced: %v", c, err)
			}
		}
	}
	if Detect([]byte("{}")) != None || Detect([]byte("x")) != None {
		t.Fatal("plain JSON detected as compressed")
	}
}

// A gzip bomb never allocates past the limit.
func TestDecompressBomb(t *testing.T) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	zero := make([]byte, 1<<20)
	for range 64 {
		w.Write(zero)
	}
	w.Close()
	if _, err := Decompress(b.Bytes(), Gzip, 256<<10); err != ErrTooLarge {
		t.Fatalf("bomb: %v", err)
	}
}

func chunkAll(p []byte, size int, id [8]byte) [][]byte {
	n := NumChunks(len(p), size)
	per := size - HeaderLen
	var out [][]byte
	for i := range n {
		out = append(out, AppendChunk(nil, id, i, n, p[i*per:min((i+1)*per, len(p))]))
	}
	return out
}

func TestNumChunks(t *testing.T) {
	for _, c := range []struct{ n, size, want int }{{100, 100, 0}, {101, 100, 2}, {176, 100, 2}, {177, 100, 3}, {8180 * 128, 8192, 128}, {8180*128 + 1, 8192, 129}} {
		if got := NumChunks(c.n, c.size); got != c.want {
			t.Errorf("NumChunks(%d, %d) = %d, want %d", c.n, c.size, got, c.want)
		}
	}
}

func TestReassembly(t *testing.T) {
	r := NewReassembler()
	src := netip.MustParseAddrPort("10.0.0.1:5000")
	t0 := time.Unix(1000, 0)
	msg := bytes.Repeat([]byte("0123456789"), 100)
	cs := chunkAll(msg, 112, [8]byte{1})
	rand.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })
	var got []byte
	for i, c := range cs {
		out, n, ok := r.Add(src, c, t0.Add(time.Duration(i)*time.Millisecond))
		if ok {
			got = out
			if n != 10 || i != len(cs)-1 {
				t.Fatalf("completed at %d with %d chunks", i, n)
			}
		}
	}
	if !bytes.Equal(got, msg) {
		t.Fatal("reassembled message differs")
	}
	// A late copy of a chunk of the completed message is a duplicate, not a
	// new message that never completes.
	if _, _, ok := r.Add(src, cs[0], t0.Add(time.Second)); ok || r.Stats.Duplicates != 1 {
		t.Fatalf("late copy: ok %v stats %+v", ok, r.Stats)
	}
	// Same id from another source is another message.
	other := netip.MustParseAddrPort("10.0.0.1:5001")
	if _, _, ok := r.Add(other, cs[0], t0.Add(time.Second)); ok {
		t.Fatal("one chunk completed a 10-chunk message")
	}
	// Past its deadline (5 s from its first chunk) the one from :5001 is
	// dropped, swept by a chunk of another message.
	r.Add(src, AppendChunk(nil, [8]byte{9}, 0, 2, []byte("x")), t0.Add(10*time.Second))
	if r.Stats.Incomplete != 1 || r.Stats.ChunksDropped != 1 {
		t.Fatalf("expiry: %+v", r.Stats)
	}
	// Invalid headers.
	for _, b := range [][]byte{
		AppendChunk(nil, [8]byte{2}, 0, 0, nil),
		AppendChunk(nil, [8]byte{2}, 0, 129, nil),
		AppendChunk(nil, [8]byte{2}, 3, 3, nil),
		{0x1e, 0x0f, 1, 2},
	} {
		r.Add(src, b, t0.Add(10*time.Second))
	}
	if r.Stats.Invalid != 4 {
		t.Fatalf("invalid: %+v", r.Stats)
	}
	r.Flush()
	if r.Stats.Incomplete != 2 {
		t.Fatalf("flush: %+v", r.Stats)
	}
}

// FuzzReassemble: no datagram sequence panics the reassembler; and a message
// chunked by AppendChunk, delivered in any order with duplicates, comes back
// whole exactly once.
func FuzzReassemble(f *testing.F) {
	f.Add([]byte("hello world, this is a gelf message"), uint8(20), uint64(1), []byte{0x1e, 0x0f, 1, 2, 3, 4, 5, 6, 7, 8, 0, 2, 'x'})
	f.Add([]byte{}, uint8(13), uint64(7), []byte{0x1e, 0x0f})
	f.Fuzz(func(t *testing.T, msg []byte, size uint8, seed uint64, junk []byte) {
		r := NewReassembler()
		src := netip.MustParseAddrPort("10.0.0.1:5000")
		t0 := time.Unix(1000, 0)
		// Arbitrary bytes as chunks, under the same source.
		for off := 0; off < len(junk); off += 16 {
			r.Add(src, append([]byte{Magic0, Magic1}, junk[off:min(off+16, len(junk))]...), t0)
		}
		sz := int(size)%200 + MinChunkSize
		if len(msg) <= sz || NumChunks(len(msg), sz) > MaxChunks {
			return
		}
		cs := chunkAll(msg, sz, [8]byte{0xfe, 0xed})
		rng := rand.New(rand.NewPCG(seed, 1))
		for i := range cs {
			if rng.IntN(3) == 0 {
				cs = append(cs, cs[i])
			}
		}
		rng.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })
		done := 0
		for _, c := range cs {
			if out, _, ok := r.Add(netip.MustParseAddrPort("10.0.0.2:1"), c, t0); ok {
				done++
				if !bytes.Equal(out, msg) {
					t.Fatal("reassembled message differs")
				}
			}
		}
		if done != 1 {
			t.Fatalf("completed %d times", done)
		}
	})
}

// FuzzDecompress: no input panics or passes the limit.
func FuzzDecompress(f *testing.F) {
	var z Compressor
	f.Add(append([]byte(nil), z.Compress(Gzip, []byte(`{"a":1}`))...))
	f.Add(append([]byte(nil), z.Compress(Zlib, []byte(`{"a":1}`))...))
	f.Add([]byte{0x1f, 0x8b, 8, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, c := range []Compression{Detect(b), Deflate} {
			out, err := Decompress(b, c, 4096)
			if err == nil && len(out) > 4096 {
				t.Fatalf("%d bytes past the limit", len(out))
			}
		}
	})
}

// The 5 s deadline holds at the moment a message would complete, not only at
// the sweep (once a second of capture time): a chunk 5.8 s after the first
// does not complete it (review-gi #7).
func TestReassemblyDeadlineExact(t *testing.T) {
	r := NewReassembler()
	src := netip.MustParseAddrPort("10.0.0.1:1")
	t0 := time.Unix(1000, 0)
	a, b := [8]byte{1}, [8]byte{2}
	r.Add(src, AppendChunk(nil, a, 0, 2, []byte("{")), t0)
	r.Add(src, AppendChunk(nil, b, 0, 2, []byte("x")), t0.Add(4900*time.Millisecond)) // sweeps here
	if _, _, ok := r.Add(src, AppendChunk(nil, a, 1, 2, []byte("}")), t0.Add(5800*time.Millisecond)); ok {
		t.Fatal("completed after its deadline")
	}
	if r.Stats.Incomplete != 1 || r.Stats.ChunksDropped != 1 {
		t.Fatalf("%+v", r.Stats)
	}
}

// A sender that reuses an id from the same source within the done memory:
// an identical copy of a chunk is a duplicate (tcpdump -i any), a different
// chunk is a new message and is reassembled (review-gi #8).
func TestReassemblyIDReuse(t *testing.T) {
	r := NewReassembler()
	src := netip.MustParseAddrPort("10.0.0.1:1")
	t0 := time.Unix(1000, 0)
	id := [8]byte{7}
	a0, a1 := AppendChunk(nil, id, 0, 2, []byte(`{"m":`)), AppendChunk(nil, id, 1, 2, []byte(`1}`))
	r.Add(src, a0, t0)
	if m, _, ok := r.Add(src, a1, t0); !ok || string(m) != `{"m":1}` {
		t.Fatalf("first: %q %v", m, ok)
	}
	if _, _, ok := r.Add(src, a1, t0.Add(time.Second)); ok || r.Stats.Duplicates != 1 {
		t.Fatalf("late copy: %v %+v", ok, r.Stats)
	}
	r.Add(src, AppendChunk(nil, id, 0, 2, []byte(`{"m":`)), t0.Add(2*time.Second)) // identical to a0: a copy
	r.Add(src, AppendChunk(nil, id, 1, 2, []byte(`2}`)), t0.Add(2*time.Second))
	m, _, ok := r.Add(src, AppendChunk(nil, id, 0, 2, []byte(`{"n":`)), t0.Add(2*time.Second))
	if !ok || string(m) != `{"n":2}` || r.Stats.Messages != 2 || r.Stats.Duplicates != 2 {
		t.Fatalf("reused id: %q %v %+v", m, ok, r.Stats)
	}
}
