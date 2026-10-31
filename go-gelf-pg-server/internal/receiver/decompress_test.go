package receiver

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
)

// Decompression of SPEC §5.4.

func gzipOf(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zlibOf(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

func deflateOf(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

func compressAs(t testing.TB, c item.Compression, b []byte) []byte {
	switch c {
	case item.CompressionGzip:
		return gzipOf(t, b)
	case item.CompressionZlib:
		return zlibOf(t, b)
	case item.CompressionDeflate:
		return deflateOf(t, b)
	}
	return b
}

var allCompressions = []item.Compression{item.CompressionNone, item.CompressionGzip,
	item.CompressionZlib, item.CompressionDeflate}

// SPEC §5.4: gzip is 1f 8b; zlib is 0x78 with a header checksum that holds;
// everything else is the JSON itself.
func TestDetect(t *testing.T) {
	for _, c := range []struct {
		in   []byte
		want item.Compression
	}{
		{[]byte{0x1f, 0x8b}, item.CompressionGzip},
		{[]byte{0x1f, 0x8b, 0x08, 0x00}, item.CompressionGzip},
		{[]byte{0x78, 0x9c}, item.CompressionZlib}, // default level
		{[]byte{0x78, 0x01}, item.CompressionZlib}, // no compression
		{[]byte{0x78, 0xda}, item.CompressionZlib}, // best compression
		{[]byte{0x78, 0x5e}, item.CompressionZlib},
		{[]byte{0x78, 0x9d}, item.CompressionNone}, // checksum fails
		{[]byte{0x78, 0x00}, item.CompressionNone},
		{[]byte("x{"), item.CompressionNone},       // 'x' is 0x78: only the checksum tells
		{[]byte{0x58, 0x85}, item.CompressionNone}, // checksum holds, but CMF is not 0x78
		{[]byte{0x1f}, item.CompressionNone},
		{[]byte{0x1f, 0x8c}, item.CompressionNone},
		{[]byte{0x8b, 0x1f}, item.CompressionNone},
		{[]byte(`{"version":"1.1"}`), item.CompressionNone},
		{nil, item.CompressionNone},
	} {
		if got := Detect(c.in); got != c.want {
			t.Errorf("Detect(% x) = %v, want %v", c.in, got, c.want)
		}
	}
	// The real writers' output is recognised; raw deflate never is (it has no
	// header: HTTP declares it).
	d := []byte(doc("x"))
	if Detect(gzipOf(t, d)) != item.CompressionGzip || Detect(zlibOf(t, d)) != item.CompressionZlib {
		t.Fatal("the stdlib's own containers are not detected")
	}
}

func TestDecompressRoundTrip(t *testing.T) {
	d := []byte(doc(strings.Repeat("payload ", 500)))
	for _, c := range allCompressions {
		out, err := Decompress(nil, compressAs(t, c, d), c, 64<<10)
		if err != nil || !bytes.Equal(out, d) {
			t.Fatalf("%v: %d bytes, %v", c, len(out), err)
		}
	}
	// Reusing the readers from the pools (second round) gives the same.
	for _, c := range allCompressions {
		out, err := Decompress(make([]byte, 0, 10), compressAs(t, c, d), c, 64<<10)
		if err != nil || !bytes.Equal(out, d) {
			t.Fatalf("%v, pooled: %d bytes, %v", c, len(out), err)
		}
	}
}

// dst is reused and its old content is gone.
func TestDecompressReusesDst(t *testing.T) {
	dst := make([]byte, 0, 4096)
	dst = append(dst, "stale"...)
	out, err := Decompress(dst, gzipOf(t, []byte("new")), item.CompressionGzip, 1024)
	if err != nil || string(out) != "new" || &out[0] != &dst[:1][0] {
		t.Fatalf("%q %v: dst was not reused from its start", out, err)
	}
}

// SPEC §5.5: exactly max is accepted; max+1 is too large, in every container.
func TestDecompressBoundary(t *testing.T) {
	const max = 1000
	for _, c := range allCompressions {
		exact := bytes.Repeat([]byte("e"), max)
		if out, err := Decompress(nil, compressAs(t, c, exact), c, max); err != nil || len(out) != max {
			t.Errorf("%v: exactly max: %d %v", c, len(out), err)
		}
		over := bytes.Repeat([]byte("o"), max+1)
		out, err := Decompress(nil, compressAs(t, c, over), c, max)
		if !errors.Is(err, errTooLarge) || len(out) != 0 {
			t.Errorf("%v: max+1: %d bytes, %v", c, len(out), err)
		}
		if decompressOutcome(err) != TooLarge {
			t.Errorf("%v: outcome %v", c, decompressOutcome(err))
		}
	}
}

// SPEC §5.4: the ceiling lives INSIDE the reader. A gzip bomb is refused
// having inflated no more than about max+1 bytes, not its full size.
func TestDecompressBombIsRefusedWithoutInflating(t *testing.T) {
	const max = 128 << 10
	const inflated = 64 << 20
	for _, c := range []item.Compression{item.CompressionGzip, item.CompressionZlib, item.CompressionDeflate} {
		bomb := compressAs(t, c, make([]byte, inflated))
		t.Logf("%v bomb: %d bytes on the wire, %d inflated", c, len(bomb), inflated)
		Decompress(nil, compressAs(t, c, []byte("warm")), c, max) // the pools

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		out, err := Decompress(nil, bomb, c, max)
		runtime.ReadMemStats(&after)
		if !errors.Is(err, errTooLarge) || len(out) != 0 {
			t.Fatalf("%v: %d bytes, %v", c, len(out), err)
		}
		if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 8*max {
			t.Fatalf("%v: %d bytes allocated to refuse a bomb against max %d: it was inflated before measuring",
				c, alloc, max)
		}
	}
}

func TestDecompressCorrupt(t *testing.T) {
	d := []byte(doc(strings.Repeat("c", 200)))
	gz := gzipOf(t, d)
	badCRC := append([]byte(nil), gz...)
	badCRC[len(badCRC)-8] ^= 0xff
	zl := zlibOf(t, d)
	badAdler := append([]byte(nil), zl...)
	badAdler[len(badAdler)-1] ^= 0xff
	for name, c := range map[string]struct {
		in   []byte
		comp item.Compression
	}{
		"gzip header only":    {gz[:10], item.CompressionGzip},
		"gzip truncated":      {gz[:len(gz)/2], item.CompressionGzip},
		"gzip bad crc":        {badCRC, item.CompressionGzip},
		"gzip trailing junk":  {append(append([]byte(nil), gz...), "junk"...), item.CompressionGzip},
		"gzip magic only":     {[]byte{0x1f, 0x8b}, item.CompressionGzip},
		"zlib bad adler":      {badAdler, item.CompressionZlib},
		"zlib truncated":      {zl[:len(zl)/2], item.CompressionZlib},
		"zlib header garbage": {[]byte{0x78, 0x9c, 0xff, 0xff, 0xff}, item.CompressionZlib},
		"zlib dictionary":     {[]byte{0x78, 0xbb, 0, 0, 0, 0}, item.CompressionZlib},
		"deflate garbage":     {[]byte{0xff, 0xff, 0xff, 0xff}, item.CompressionDeflate},
		"deflate truncated":   {deflateOf(t, d)[:5], item.CompressionDeflate},
		"unknown container":   {d, item.Compression(99)},
	} {
		out, err := Decompress(nil, c.in, c.comp, 64<<10)
		if !errors.Is(err, errDecompress) || len(out) != 0 {
			t.Errorf("%s: %d bytes, %v", name, len(out), err)
		}
		if decompressOutcome(err) != DecompressFailed {
			t.Errorf("%s: outcome %v", name, decompressOutcome(err))
		}
	}
	// And a good stream after a bad one: the pooled readers were not left
	// broken.
	if out, err := Decompress(nil, gz, item.CompressionGzip, 64<<10); err != nil || !bytes.Equal(out, d) {
		t.Fatalf("after the corrupt streams: %v", err)
	}
}

// SPEC §5.4: a multi-member gzip is read whole; bytes after the end of a zlib
// or raw deflate stream are ignored.
func TestDecompressStreamEnds(t *testing.T) {
	two := append(gzipOf(t, []byte("one,")), gzipOf(t, []byte("two"))...)
	if out, err := Decompress(nil, two, item.CompressionGzip, 1024); err != nil || string(out) != "one,two" {
		t.Fatalf("multi-member gzip: %q %v", out, err)
	}
	for _, c := range []item.Compression{item.CompressionZlib, item.CompressionDeflate} {
		in := append(compressAs(t, c, []byte("doc")), "trailing"...)
		if out, err := Decompress(nil, in, c, 1024); err != nil || string(out) != "doc" {
			t.Fatalf("%v with trailing bytes: %q %v", c, out, err)
		}
	}
}

func TestWireLimit(t *testing.T) {
	if WireLimit(128<<10) != 128<<10+16<<10 {
		t.Fatal(WireLimit(128 << 10))
	}
}

// FuzzDecompress: any bytes as any container. Never a panic, never more than
// max bytes out, only the two errors, and a container the stdlib wrote from
// the input round-trips.
func FuzzDecompress(f *testing.F) {
	f.Add([]byte(doc("x")), uint8(0), uint16(100))
	f.Add(gzipOf(f, []byte(doc("gz"))), uint8(1), uint16(200))
	f.Add(zlibOf(f, []byte(doc("zl"))), uint8(2), uint16(200))
	f.Add(deflateOf(f, []byte(doc("df"))), uint8(3), uint16(200))
	f.Add([]byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff}, uint8(1), uint16(10))
	f.Add([]byte{0x78, 0x9c, 3, 0, 0, 0, 0, 1}, uint8(2), uint16(1))
	f.Fuzz(func(t *testing.T, data []byte, comp uint8, max16 uint16) {
		max := int(max16)%4096 + 1
		c := item.Compression(comp % 4)
		out, err := Decompress(nil, data, c, max)
		if len(out) > max {
			t.Fatalf("%d bytes out for max %d", len(out), max)
		}
		if err != nil && !errors.Is(err, errTooLarge) && !errors.Is(err, errDecompress) {
			t.Fatalf("unexpected error %v", err)
		}
		if err != nil && len(out) != 0 {
			t.Fatalf("an error with %d bytes", len(out))
		}
		packed := compressAs(t, c, data)
		out, err = Decompress(out, packed, c, max)
		switch {
		case len(data) > max:
			if !errors.Is(err, errTooLarge) {
				t.Fatalf("%v: %d bytes against max %d: %v", c, len(data), max, err)
			}
		case err != nil || !bytes.Equal(out, data):
			t.Fatalf("%v round trip: %v", c, err)
		}
	})
}
