// Package gelf holds the GELF wire rules the injector speaks and reads
// (SPEC §4, §6): compression (gzip, zlib) and its detection by the first
// bytes, UDP chunking (magic 0x1e 0x0f, 8-byte id, sequence, count <= 128)
// and the reassembly of captured chunks.
package gelf

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
)

// Chunk header: magic (2), message id (8), sequence (1), count (1).
const (
	Magic0    = 0x1e
	Magic1    = 0x0f
	HeaderLen = 12
	MaxChunks = 128
)

// Compression of a GELF payload.
type Compression uint8

const (
	None Compression = iota
	Gzip
	Zlib
	Deflate // raw deflate: only read, in an HTTP body sent "deflate" without the zlib wrapper
)

func (c Compression) String() string {
	return [...]string{"none", "gzip", "zlib", "deflate"}[c]
}

// ParseCompression parses --compress.
func ParseCompression(s string) (Compression, error) {
	switch s {
	case "none":
		return None, nil
	case "gzip":
		return Gzip, nil
	case "zlib", "deflate":
		// "deflate" is what HTTP calls the zlib format (Content-Encoding,
		// RFC 9110 §8.4.1.2): accepted as a synonym (SPEC §3.1).
		return Zlib, nil
	}
	return 0, fmt.Errorf("--compress: want none, gzip or zlib (deflate is a synonym of zlib), got %q", s)
}

// Detect tells the compression of b by its first bytes, as the servers do
// (go-gelf-server DESIGN decision 11): gzip 1f 8b; zlib 78 xx with the
// header checksum (b0<<8|b1) % 31 == 0; anything else is plain.
func Detect(b []byte) Compression {
	if len(b) >= 2 {
		if b[0] == 0x1f && b[1] == 0x8b {
			return Gzip
		}
		if b[0]&0x0f == 8 && (uint16(b[0])<<8|uint16(b[1]))%31 == 0 {
			return Zlib
		}
	}
	return None
}

// ErrTooLarge is returned when a decompressed payload passes the limit.
var ErrTooLarge = errors.New("decompressed payload too large")

// Decompress inflates b as c, reading at most max bytes of output: the limit
// holds inside the reader, so a compression bomb never allocates past it.
func Decompress(b []byte, c Compression, max int) ([]byte, error) {
	var r io.ReadCloser
	var err error
	switch c {
	case None:
		if len(b) > max {
			return nil, ErrTooLarge
		}
		return b, nil
	case Gzip:
		r, err = gzip.NewReader(bytes.NewReader(b))
	case Zlib:
		r, err = zlib.NewReader(bytes.NewReader(b))
	case Deflate:
		r = flate.NewReader(bytes.NewReader(b))
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(out) > max {
		return nil, ErrTooLarge
	}
	return out, nil
}

// Compressor compresses payloads, reusing its writers. Not safe for
// concurrent use: one per connection.
type Compressor struct {
	buf bytes.Buffer
	gz  *gzip.Writer
	zl  *zlib.Writer
}

// Compress appends nothing when c is None (it returns src); otherwise it
// returns the compressed bytes, valid until the next call.
func (z *Compressor) Compress(c Compression, src []byte) []byte {
	if c == None {
		return src
	}
	z.buf.Reset()
	var w io.WriteCloser
	switch c {
	case Gzip:
		if z.gz == nil {
			z.gz, _ = gzip.NewWriterLevel(&z.buf, gzip.DefaultCompression)
		} else {
			z.gz.Reset(&z.buf)
		}
		w = z.gz
	default:
		if z.zl == nil {
			z.zl, _ = zlib.NewWriterLevel(&z.buf, zlib.DefaultCompression)
		} else {
			z.zl.Reset(&z.buf)
		}
		w = z.zl
	}
	_, _ = w.Write(src) // a bytes.Buffer never fails
	_ = w.Close()
	return z.buf.Bytes()
}

// NumChunks is the number of chunks a payload of n bytes needs with
// datagrams of at most size bytes: 0 when it fits one datagram unchunked.
func NumChunks(n, size int) int {
	if n <= size {
		return 0
	}
	per := size - HeaderLen
	return (n + per - 1) / per
}

// MinChunkSize leaves at least one payload byte per chunk.
const MinChunkSize = HeaderLen + 1

// AppendChunk appends chunk seq of count, of message id, carrying part.
func AppendChunk(dst []byte, id [8]byte, seq, count int, part []byte) []byte {
	dst = append(dst, Magic0, Magic1)
	dst = append(dst, id[:]...)
	dst = append(dst, byte(seq), byte(count))
	return append(dst, part...)
}

// IsChunk reports whether a datagram carries the chunk magic.
func IsChunk(b []byte) bool { return len(b) >= 2 && b[0] == Magic0 && b[1] == Magic1 }
