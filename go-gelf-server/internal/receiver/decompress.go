package receiver

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"sync"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
)

// Decompression of GELF documents (SPEC §5.4).
//
// The ceiling lives INSIDE the reader: the decompressor is read through an
// io.LimitReader of max+1 bytes, and the extra byte is what reveals a document
// too large. Decompressing first and measuring after would let a 100 KiB gzip
// bomb inflate to gigabytes in this process before anything said no.

// wireSlack is how much more than max_message_bytes the wire may carry before
// decompression: gzip of incompressible data grows by about 30 bytes per 128
// KiB, so 16 KiB is generous and still a hard bound (SPEC §5.5).
const wireSlack = 16 << 10

// WireLimit is the most bytes a compressed body or the chunks of one UDP
// message may carry for a document of at most max bytes.
func WireLimit(max int) int { return max + wireSlack }

var (
	// errTooLarge: the document passes max_message_bytes.
	errTooLarge = errors.New("receiver: document above convert.max_message_bytes")
	// errDecompress: the container does not decode.
	errDecompress = errors.New("receiver: compressed document does not decode")
)

// Detect reads the container from the first bytes (SPEC §5.4): gzip is
// 1f 8b; zlib is a CMF byte 0x78 (deflate, 32 KiB window) whose header
// checksum holds; anything else is the JSON itself.
func Detect(b []byte) item.Compression {
	if len(b) >= 2 {
		switch {
		case b[0] == 0x1f && b[1] == 0x8b:
			return item.CompressionGzip
		case isZlibHeader(b):
			return item.CompressionZlib
		}
	}
	return item.CompressionNone
}

func isZlibHeader(b []byte) bool {
	return len(b) >= 2 && b[0] == 0x78 && (uint16(b[0])<<8|uint16(b[1]))%31 == 0
}

// readers are reused: a gzip.Reader carries a 32 KiB window, and one per
// datagram would be the dominant allocation of a compressed UDP flood.
var (
	gzipReaders sync.Pool
	flatePool   sync.Pool
)

// Decompress inflates src (a container of kind c) into dst, refusing anything
// that would pass max bytes. dst is reused; the result aliases it. With
// CompressionNone it only checks the size and copies.
func Decompress(dst, src []byte, c item.Compression, max int) ([]byte, error) {
	dst = dst[:0]
	if c == item.CompressionNone {
		if len(src) > max {
			return dst, errTooLarge
		}
		return append(dst, src...), nil
	}
	var r io.Reader
	switch c {
	case item.CompressionGzip:
		zr, _ := gzipReaders.Get().(*gzip.Reader)
		var err error
		if zr == nil {
			zr, err = gzip.NewReader(bytes.NewReader(src))
		} else {
			err = zr.Reset(bytes.NewReader(src))
		}
		if err != nil {
			return dst, errDecompress
		}
		defer gzipReaders.Put(zr)
		r = zr
	case item.CompressionZlib:
		zr, err := zlib.NewReader(bytes.NewReader(src))
		if err != nil {
			return dst, errDecompress
		}
		r = zr
	case item.CompressionDeflate:
		fr, _ := flatePool.Get().(io.ReadCloser)
		if fr == nil {
			fr = flate.NewReader(bytes.NewReader(src))
		} else {
			_ = fr.(flate.Resetter).Reset(bytes.NewReader(src), nil)
		}
		defer flatePool.Put(fr)
		r = fr
	default:
		return dst, errDecompress
	}
	lr := io.LimitedReader{R: r, N: int64(max) + 1}
	for {
		// By classes and never past max+1, the byte that reveals too_large:
		// append's doubling took a document near the ceiling to twice it.
		dst = growOne(dst, max+1)
		n, err := lr.Read(dst[len(dst):cap(dst)])
		dst = dst[:len(dst)+n]
		if len(dst) > max {
			return dst[:0], errTooLarge
		}
		if err == io.EOF {
			return dst, nil
		}
		if err != nil {
			return dst[:0], errDecompress
		}
	}
}

// growOne returns b with room for at least one more byte, growing the
// capacity by the frame classes (then doubling) but never past limit; the
// caller guarantees len(b) < limit. The growth is driven by bytes that
// ARRIVED, never by a size a peer declared (SPEC §4.5, §5.4).
func growOne(b []byte, limit int) []byte {
	if len(b) < cap(b) {
		return b
	}
	need := len(b) + 1
	c := 0
	for _, cl := range frameClasses {
		if cl >= need {
			c = cl
			break
		}
	}
	if c == 0 {
		c = max(2*cap(b), need)
	}
	c = max(min(c, limit), need)
	nb := make([]byte, len(b), c)
	copy(nb, b)
	return nb
}

// decompressOutcome maps a Decompress error to the pipeline's outcome.
func decompressOutcome(err error) Outcome {
	if errors.Is(err, errTooLarge) {
		return TooLarge
	}
	return DecompressFailed
}
