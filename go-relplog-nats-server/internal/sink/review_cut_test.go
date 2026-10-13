package sink

// Regression tests from the independent review of go-relplog-pg-server on
// 2026-10-13, ported here (TESTING §9): the mutants of the chunk -- common to
// both servers -- that no test killed.

import (
	"testing"
	"unsafe"
)

func bufID(b []byte) uintptr { return uintptr(unsafe.Pointer(unsafe.SliceData(b[:1]))) }

// pooledAgain reports whether b comes back out of the pool within a few
// tries. sync.Pool drops a Put now and then on purpose (always under -race,
// at random), so one Get proves nothing either way; twenty do.
func pooledAgain(t *testing.T, bp *bufPool, put func() []byte) bool {
	t.Helper()
	for i := 0; i < 20; i++ {
		id := bufID(put())
		if got := bp.get(); cap(got) > 0 && bufID(got) == id {
			return true
		}
	}
	return false
}

// SPEC §15.2: a tainted chunk's buffer goes to the GC, never back to the pool
// -- a lower layer may still read it (M47). The control: an untainted one does
// come back.
func TestTaintedChunkBufferIsNotPooled(t *testing.T) {
	bp := newBufPool(1 << 20)
	fill := func(tainted bool) func() []byte {
		return func() []byte {
			c := &chunk{buf: append(bp.get(), "x"...)}
			c.tainted = tainted
			b := c.buf
			c.reset(bp)
			return b
		}
	}
	if pooledAgain(t, bp, fill(true)) {
		t.Fatal("a tainted chunk buffer was handed out again")
	}
	if !pooledAgain(t, bp, fill(false)) {
		t.Fatal("control: an untainted buffer never came back; the test proves nothing")
	}
}

// SPEC §15.3: a buffer that grew past sink.batch_max_bytes (one JLH larger
// than the ceiling, a chunk alone) is never pooled, or the pool holds the
// worst case forever (M52).
func TestOversizedChunkBufferIsNotPooled(t *testing.T) {
	bp := newBufPool(1024)
	if pooledAgain(t, bp, func() []byte { b := make([]byte, 1, 4096); bp.put(b); return b }) {
		t.Fatal("a buffer above the ceiling was pooled")
	}
	if !pooledAgain(t, bp, func() []byte { b := make([]byte, 1, 1024); bp.put(b); return b }) {
		t.Fatal("control: a buffer at the ceiling never came back")
	}
}

// seal cuts each JLH capacity-limited to its own length: a destination that
// appended to one would otherwise write over the next envelope (M45).
func TestSealLimitsEachEnvelopeToItsBytes(t *testing.T) {
	c := &chunk{buf: []byte(`{"a":1}{"b":2}`)}
	c.Envelopes = make([]Envelope, 2)
	c.ends = []int{7, 14}
	c.seal()
	if cap(c.Envelopes[0].JLH) != 7 {
		t.Fatalf("cap of the first JLH = %d, want its length 7", cap(c.Envelopes[0].JLH))
	}
	_ = append(c.Envelopes[0].JLH, 'X')
	if string(c.Envelopes[1].JLH) != `{"b":2}` {
		t.Fatalf("an append to one envelope changed the next: %q", c.Envelopes[1].JLH)
	}
}

// reset clears every slot, not only the length: a slot left holding an item or
// a JLH pins buffers that already went back to their pools (M46).
func TestResetClearsTheEnvelopeSlots(t *testing.T) {
	bp := newBufPool(1 << 20)
	b := batchN(t, 3)
	c := &chunk{Batch: *b}
	c.reset(bp)
	full := c.Envelopes[:cap(c.Envelopes)]
	for i := range full {
		if full[i].Item != nil || full[i].JLH != nil {
			t.Fatalf("slot %d still holds %+v after reset", i, full[i])
		}
	}
}
