package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

// The GELF TCP framing (SPEC §5.3): one document per frame, frames separated
// by a NUL byte, the way Graylog, the Docker gelf-tcp driver and Logstash
// write it. There is no length to trust and nothing to detect.
//
// What is NEVER in a returned document: the NUL. A frame that is empty or only
// JSON whitespace (a "\n\0" some emitters send, a "\0\0") is a separator and is
// skipped without a count (decision 12). A frame above max_message_bytes is
// consumed up to its NUL WITHOUT allocating and reported as too large; the
// connection lives on.

// frameClasses are the steps a frame buffer grows by: the classes of the item
// pools (SPEC §5.3, §15.1). Above the last class the buffer doubles, up to
// max_message_bytes.
var frameClasses = [...]int{2 << 10, 8 << 10, 64 << 10}

// frameKeepCap is the largest buffer a framer keeps between frames. After one
// 64 KiB document an idle connection would otherwise hold it for as long as it
// lives, outside every ceiling (S pendência 1 de 2026-10-11, SPEC §15.1).
const frameKeepCap = 16 << 10

// ErrFrameTooLarge is returned by Next for a frame above the limit. The frame
// was consumed; the next call reads the following one.
var ErrFrameTooLarge = errors.New("receiver: GELF frame above convert.max_message_bytes")

// Framer reads NUL-terminated documents off one stream connection.
type Framer struct {
	r   *bufio.Reader
	max int

	// buf is reused across frames and only ever grows with bytes that
	// ARRIVED, up to max. The slice Next returns aliases it, valid only until
	// the following call -- Ingest copies into a pooled buffer before the
	// item exists.
	buf []byte

	// err is sticky: a frame that ended because the connection did is
	// delivered first, and the reason the connection ended comes back on the
	// next call.
	err error
}

// NewFramer wraps a connection. maxBytes is convert.max_message_bytes.
func NewFramer(r *bufio.Reader, maxBytes int) *Framer {
	if maxBytes <= 0 {
		maxBytes = defaultMaxMessageBytes
	}
	return &Framer{r: r, max: maxBytes}
}

// Next returns the next document.
//
// err is io.EOF at a clean end, ErrFrameTooLarge for a frame that was
// consumed and refused (call Next again), or the connection's own error. A
// frame the end of the connection completed -- EOF without a NUL, or an idle
// timeout, a reset, the shutdown's deadline after some bytes -- is delivered
// as it stands (SPEC §5.3): dropping bytes that arrived would be a loss with
// no counter.
func (f *Framer) Next() ([]byte, error) {
	for {
		if f.err != nil {
			return nil, f.err
		}
		// The previous document is no longer anyone's (Ingest copied it),
		// and the read below may park for the whole idle timeout: a buffer a
		// large frame grew is let go before the wait, not kept for the
		// connection's life. Only before a wait: with bytes already in the
		// reader the next frame is here, and releasing after every large
		// frame reallocated the 64 KiB class for each of them (S, review of
		// 2026-10-13).
		if cap(f.buf) > frameKeepCap && f.r.Buffered() == 0 {
			f.buf = nil
		}
		doc, tooLarge, err := f.frame()
		if err != nil && doc == nil && !tooLarge {
			return nil, err
		}
		if err != nil {
			f.err = endOfStream(err)
		}
		if tooLarge {
			return nil, ErrFrameTooLarge
		}
		if blank(doc) {
			continue // a separator, not a document (decision 12)
		}
		return doc, nil
	}
}

// frame reads up to the next NUL. doc is nil with err when the connection
// ended (or failed) before a single byte of a frame.
func (f *Framer) frame() (doc []byte, tooLarge bool, err error) {
	b := f.buf[:0]
	total := 0
	for {
		if f.r.Buffered() == 0 {
			if _, perr := f.r.Peek(1); perr != nil {
				if total == 0 {
					f.buf = b
					return nil, false, perr
				}
				err = perr
				break
			}
		}
		avail, _ := f.r.Peek(f.r.Buffered())
		end := bytes.IndexByte(avail, 0)
		seg := avail
		if end >= 0 {
			seg = avail[:end]
		}
		total += len(seg)
		if total <= f.max {
			b = appendGrow(b, seg, f.max)
		}
		if end >= 0 {
			_, _ = f.r.Discard(end + 1)
			break
		}
		_, _ = f.r.Discard(len(avail))
	}
	f.buf = b
	if total > f.max {
		// Consumed to its NUL without keeping anything past max: the
		// emitter's next frame starts in sync (SPEC §5.3).
		return nil, true, err
	}
	return b, false, err
}

// appendGrow appends p to b, growing the capacity by the pool classes (then
// doubling) but never past limit. The growth is driven by bytes that ARRIVED;
// the caller guarantees len(b)+len(p) <= limit.
func appendGrow(b, p []byte, limit int) []byte {
	need := len(b) + len(p)
	if need > cap(b) {
		c := 0
		for _, cl := range frameClasses {
			if cl >= need {
				c = cl
				break
			}
		}
		if c == 0 {
			c = 2 * cap(b)
			if c < need {
				c = need
			}
		}
		if c > limit {
			c = limit
		}
		if c < need {
			c = need
		}
		nb := make([]byte, len(b), c)
		copy(nb, b)
		b = nb
	}
	return append(b, p...)
}

// endOfStream normalises the error that ended a frame early.
func endOfStream(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return io.EOF
	}
	return err
}
