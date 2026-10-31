package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// The NUL framer of SPEC §5.3: one document per frame, separators skipped
// without a count, EOF delivering the last frame, oversized frames consumed in
// sync without growing past the limit.

func framer(input string, max int) *Framer {
	return NewFramer(bufio.NewReaderSize(strings.NewReader(input), 16), max)
}

// frameResult is one Next: a document, or the marker of a frame refused as
// too large.
type frameResult struct {
	doc      string
	tooLarge bool
}

// drain calls Next until it fails with something other than ErrFrameTooLarge.
func drain(t *testing.T, f *Framer) ([]frameResult, error) {
	t.Helper()
	var out []frameResult
	for i := 0; i < 1<<16; i++ {
		d, err := f.Next()
		if errors.Is(err, ErrFrameTooLarge) {
			out = append(out, frameResult{tooLarge: true})
			continue
		}
		if err != nil {
			return out, err
		}
		out = append(out, frameResult{doc: string(d)})
	}
	t.Fatal("the framer never ended")
	return nil, nil
}

func docsOf(rs []frameResult) []string {
	var out []string
	for _, r := range rs {
		if r.tooLarge {
			out = append(out, "<too large>")
		} else {
			out = append(out, r.doc)
		}
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFramerNULFrames(t *testing.T) {
	a, b := doc("um"), doc("dois")
	for name, c := range map[string]struct {
		input string
		want  []string
	}{
		"two terminated":             {a + "\x00" + b + "\x00", []string{a, b}},
		"EOF without NUL delivered":  {a + "\x00" + b, []string{a, b}},
		"only the last unterminated": {b, []string{b}},
		"double NUL is a separator":  {a + "\x00\x00" + b + "\x00", []string{a, b}},
		"LF NUL is a separator":      {a + "\x00\n\x00" + b + "\x00", []string{a, b}},
		"leading separators":         {"\x00\x00 \x00\r\n\x00" + a + "\x00", []string{a}},
		"trailing whitespace frame":  {a + "\x00 \t\r\n", []string{a}},
		// The whitespace INSIDE a frame stays: only a frame that is nothing
		// but whitespace is a separator.
		"inner whitespace kept":   {"\n" + a + "\n\x00", []string{"\n" + a + "\n"}},
		"a LF is not a frame end": {"x\ny\x00", []string{"x\ny"}},
		"empty stream":            {"", nil},
		"only separators":         {"\x00\x00\n\x00", nil},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := drain(t, framer(c.input, 1<<10))
			if err != io.EOF {
				t.Fatalf("err = %v, want EOF", err)
			}
			if !equal(docsOf(got), c.want) {
				t.Fatalf("got %q, want %q", docsOf(got), c.want)
			}
		})
	}
}

// A frame across many small reads is reassembled, and the NUL is never in the
// document.
func TestFramerFrameAcrossReads(t *testing.T) {
	big := doc(strings.Repeat("x", 5000))
	f := NewFramer(bufio.NewReaderSize(&chunkReader{data: []byte(big + "\x00" + big + "\x00"), sizes: []byte{1, 7, 3}}, 16), 64<<10)
	for i := 0; i < 2; i++ {
		d, err := f.Next()
		if err != nil || string(d) != big {
			t.Fatalf("frame %d: %d bytes, %v", i, len(d), err)
		}
	}
	if _, err := f.Next(); err != io.EOF {
		t.Fatalf("err = %v", err)
	}
}

// SPEC §5.3: above max the frame is consumed to its NUL and refused, and the
// NEXT frame is read in sync; exactly max is a document.
func TestFramerTooLargeKeepsSync(t *testing.T) {
	const max = 100
	exact := strings.Repeat("e", max)
	input := strings.Repeat("L", 3*max) + "\x00" + exact + "\x00" + strings.Repeat("M", max+1) + "\x00after\x00" +
		strings.Repeat("T", max+1) // the last one ends at EOF, still too large
	got, err := drain(t, framer(input, max))
	if err != io.EOF {
		t.Fatalf("err = %v", err)
	}
	want := []string{"<too large>", exact, "<too large>", "after", "<too large>"}
	if !equal(docsOf(got), want) {
		t.Fatalf("got %q, want %q", docsOf(got), want)
	}
}

// An oversized frame is consumed WITHOUT keeping it: the buffer never grows
// past max, and the bytes of the frame are not allocated anywhere.
func TestFramerTooLargeDoesNotAllocate(t *testing.T) {
	const max = 64 << 10
	const huge = 32 << 20
	r := io.MultiReader(io.LimitReader(repeatReader('A'), huge), strings.NewReader("\x00"+doc("next")+"\x00"))
	f := NewFramer(bufio.NewReaderSize(r, streamReadBuffer), max)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := f.Next()
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if cap(f.buf) > max {
		t.Fatalf("the frame buffer grew to %d, past max %d", cap(f.buf), max)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4*max {
		t.Fatalf("a %d-byte frame allocated %d bytes on its way to the bin", huge, alloc)
	}
	d, err := f.Next()
	if err != nil || string(d) != doc("next") {
		t.Fatalf("next frame %q, %v: the stream lost sync", d, err)
	}
}

// The frame buffer grows with what ARRIVES, in the pool classes: a short
// frame never costs a large buffer.
func TestFramerGrowsByTheClasses(t *testing.T) {
	f := framer("abc\x00"+strings.Repeat("b", 3000)+"\x00", 128<<10)
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	if cap(f.buf) != frameClasses[0] {
		t.Fatalf("a 3-byte frame got a buffer of %d, want the first class %d", cap(f.buf), frameClasses[0])
	}
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	if cap(f.buf) != frameClasses[1] {
		t.Fatalf("a 3000-byte frame got a buffer of %d, want the second class %d", cap(f.buf), frameClasses[1])
	}
	// Above the last class it doubles, capped at the limit.
	g := appendGrow(make([]byte, 0, 64<<10), make([]byte, 70<<10), 100<<10)
	if cap(g) != 100<<10 {
		t.Fatalf("growth past the last class: cap %d, want the limit", cap(g))
	}
}

// errAfterReader yields its data, then fails with err (an idle timeout, a
// reset) instead of io.EOF.
type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// SPEC §5.3: a read error with bytes pending delivers the frame as it stands
// -- what was read is not lost -- and the error comes back on the next call.
func TestFramerErrorMidFrameDeliversThePartial(t *testing.T) {
	for _, e := range []error{timeoutErr{}, syscall.ECONNRESET, io.ErrUnexpectedEOF, io.EOF} {
		f := NewFramer(bufio.NewReader(&errAfterReader{data: []byte(doc("a") + "\x00{\"short"), err: e}), 1<<10)
		if d, err := f.Next(); err != nil || string(d) != doc("a") {
			t.Fatalf("%v: first %q %v", e, d, err)
		}
		d, err := f.Next()
		if err != nil || string(d) != `{"short` {
			t.Fatalf("%v: partial %q, %v", e, d, err)
		}
		_, err = f.Next()
		want := e
		if e == io.ErrUnexpectedEOF {
			want = io.EOF
		}
		if !errors.Is(err, want) {
			t.Fatalf("%v: then err = %v, want %v", e, err, want)
		}
		// Sticky.
		if _, err2 := f.Next(); !errors.Is(err2, want) {
			t.Fatalf("%v: the error is not sticky: %v", e, err2)
		}
	}
	// No pending byte: the error itself, at once.
	f := NewFramer(bufio.NewReader(&errAfterReader{err: syscall.ECONNRESET}), 1<<10)
	if _, err := f.Next(); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("err = %v", err)
	}
}

// After a large frame the framer does not keep the large buffer once the
// reader is empty: an idle connection must not hold 64 KiB for the rest of
// its life (SPEC §5.3, §15.1).
func TestFramerReleasesALargeBuffer(t *testing.T) {
	big := strings.Repeat("a", 60000)
	f := framer(big+"\x00b\x00", 64<<10)
	if d, err := f.Next(); err != nil || len(d) != len(big) {
		t.Fatalf("first frame: %d bytes, %v", len(d), err)
	}
	if d, err := f.Next(); err != nil || string(d) != "b" {
		t.Fatalf("second frame: %q, %v", d, err)
	}
	if _, err := f.Next(); err != io.EOF {
		t.Fatalf("third Next: %v, want EOF", err)
	}
	if cap(f.buf) > frameKeepCap {
		t.Fatalf("buffer of %d kept before a wait", cap(f.buf))
	}
}

// ... and KEEPS it while the next frame is already in the reader: released
// after every frame above frameKeepCap, a stream of 12 KiB frames reallocated
// the 64 KiB class for each one (S, review of 2026-10-13).
func TestFramerKeepsTheBufferBetweenBufferedFrames(t *testing.T) {
	line := strings.Repeat("a", 12<<10) + "\x00"
	f := NewFramer(bufio.NewReaderSize(strings.NewReader(strings.Repeat(line, 8)), 4<<10), 64<<10)
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	first := func() *byte { return &f.buf[:cap(f.buf)][0] }
	grown := first()
	kept := 0
	for i := 1; i < 8; i++ {
		buffered := f.r.Buffered() > 0
		if _, err := f.Next(); err != nil {
			t.Fatal(err)
		}
		if buffered {
			kept++
			if first() != grown {
				t.Fatalf("frame %d: buffer reallocated with the frame already buffered", i)
			}
		}
		grown = first()
	}
	if kept == 0 {
		t.Fatal("no frame was ever buffered ahead: the test proved nothing")
	}
}

func TestNewFramerDefaultLimit(t *testing.T) {
	if f := NewFramer(bufio.NewReader(strings.NewReader("")), 0); f.max != defaultMaxMessageBytes {
		t.Fatalf("max = %d", f.max)
	}
}

// ---------------------------------------------------------------- fuzzing

// repeatReader is an endless stream of one byte.
type repeatReader byte

func (r repeatReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

// chunkReader returns data in reads of the sizes given (cycled), so a fuzz
// input also explores where the reads split the frames.
type chunkReader struct {
	data  []byte
	sizes []byte
	i     int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if len(r.sizes) > 0 {
		s := int(r.sizes[r.i%len(r.sizes)])%32 + 1
		r.i++
		if s < n {
			n = s
		}
	}
	n = copy(p[:n], r.data)
	r.data = r.data[n:]
	return n, nil
}

// refFrames is the oracle of SPEC §5.3: split on NUL; the segment after the
// last NUL is a frame when it is not empty; a frame above max is "too large";
// a frame of only JSON whitespace is skipped.
func refFrames(data []byte, max int) []string {
	parts := bytes.Split(data, []byte{0})
	var out []string
	for i, p := range parts {
		if i == len(parts)-1 && len(p) == 0 {
			break
		}
		switch {
		case len(p) > max:
			out = append(out, "<too large>")
		case blank(p):
		default:
			out = append(out, string(p))
		}
	}
	return out
}

// FuzzFrame: any byte stream, any read split, any limit. Never a panic, never
// a NUL in a document, never a document above max, and the sequence of
// documents and refusals is exactly the oracle's.
func FuzzFrame(f *testing.F) {
	f.Add([]byte(nul(doc("a"), doc("b"))), []byte{3}, uint16(64))
	f.Add([]byte("\x00\x00\n\x00x"), []byte{1}, uint16(1))
	f.Add([]byte(strings.Repeat("y", 300)+"\x00z\x00"), []byte{5, 9}, uint16(100))
	f.Add([]byte(" \t\r\n\x00 a \x00"), []byte{}, uint16(2))
	f.Add([]byte{}, []byte{1}, uint16(0))
	f.Add([]byte("abc"), []byte{31}, uint16(3))
	f.Fuzz(func(t *testing.T, data, sizes []byte, max16 uint16) {
		max := int(max16)%512 + 1
		fr := NewFramer(bufio.NewReaderSize(&chunkReader{data: data, sizes: sizes}, 16), max)
		var got []string
		for i := 0; ; i++ {
			if i > len(data)+2 {
				t.Fatal("more Next calls than bytes: the framer does not advance")
			}
			d, err := fr.Next()
			if errors.Is(err, ErrFrameTooLarge) {
				got = append(got, "<too large>")
				continue
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if bytes.IndexByte(d, 0) >= 0 {
				t.Fatalf("a document with a NUL: %q", d)
			}
			if len(d) > max || len(d) == 0 || blank(d) {
				t.Fatalf("document of %d bytes (max %d): %q", len(d), max, d)
			}
			got = append(got, string(d))
		}
		if want := refFrames(data, max); !equal(got, want) {
			t.Fatalf("got %q\nwant %q", got, want)
		}
		if cap(fr.buf) > max && cap(fr.buf) > frameClasses[0] {
			t.Fatalf("buffer of %d for max %d", cap(fr.buf), max)
		}
	})
}

// BenchmarkFramer12K is 64 frames of 12 KiB through the 4 KiB reader: between
// the kept-buffer threshold (16 KiB) and the 64 KiB class.
func BenchmarkFramer12K(b *testing.B) {
	line := strings.Repeat("a", 12<<10) + "\x00"
	in := strings.Repeat(line, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	for i := 0; i < b.N; i++ {
		f := NewFramer(bufio.NewReaderSize(strings.NewReader(in), 4<<10), 64<<10)
		for {
			if _, err := f.Next(); err != nil {
				break
			}
		}
	}
}
