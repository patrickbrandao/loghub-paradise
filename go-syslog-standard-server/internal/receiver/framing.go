// Package receiver holds the four transports of SPEC §4 and the shared receive
// pipeline they all funnel into.
//
// Only the PRI is read here. The full parse and the envelope happen in the drain
// worker (SPEC §2.2), so this package does the minimum that lets it decide
// whether a message gets in.
package receiver

import (
	"bufio"
	"errors"
	"io"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

// Framing errors. Both END THE CONNECTION, and that is the whole point.
var (
	// ErrFrameTooLarge is the fuse of SPEC §5.5, not a policy. It fires at 16 MiB
	// by default, which no real emitter approaches.
	ErrFrameTooLarge = errors.New("receiver: frame above tcp.max_frame_bytes")

	// ErrMalformedFrame means the stream is DESYNCHRONISED and cannot be
	// recovered. A non-numeric octet count leaves no way to find where the next
	// message begins; trying to "look for the next <" would write fragments into
	// the destination as if they were messages.
	ErrMalformedFrame = errors.New("receiver: malformed frame; the stream is desynchronised")
)

// maxOctetDigits bounds the length prefix itself. RFC 6587 sets no limit, but a
// count that needs more than ten digits cannot fit any ceiling this server
// accepts, and reading digits without a bound is an unbounded read.
const maxOctetDigits = 10

// Framer reads messages off one stream connection (SPEC §5).
//
// RFC 6587 defines two framings and a real deployment may use either:
//
//	octet-counting (§3.4.1)          "38 <13>Sep 12 14:33:01 host app: oi"
//	non-transparent-framing (§3.4.2) "<13>Sep 12 14:33:01 host app: oi\n"
//
// The RFC calls the second one "not recommended" and it is the one everybody
// uses.
type Framer struct {
	r    *bufio.Reader
	mode envelope.Framing
	max  int

	// decided is false until the first byte has been seen, when mode is auto.
	decided bool

	// buf is reused across messages. The slice Next returns aliases it, so it is
	// valid only until the following call -- Accept copies into a pooled buffer
	// before the item exists.
	buf []byte
}

// NewFramer wraps a connection. mode may be FramingOctet or FramingLF to fix the
// framing, or the zero value to auto-detect.
func NewFramer(r *bufio.Reader, mode envelope.Framing, auto bool, max int) *Framer {
	f := &Framer{r: r, mode: mode, max: max, decided: !auto}
	if max <= 0 {
		f.max = 16 << 20
	}
	return f
}

// Mode reports the framing in force, which is only meaningful after the first
// successful Next on an auto-detecting framer.
func (f *Framer) Mode() envelope.Framing { return f.mode }

// Next returns the next message, without any framing bytes.
//
// What is NOT in the returned slice, ever: the octet count, the LF terminator,
// a trailing CR, a trailing NUL. Those are transport, and a framing byte that
// leaked into the raw line would break the fidelity invariant (§19.5) and show
// up in everyone's destination.
//
// The slice aliases an internal buffer and is valid only until the next call.
func (f *Framer) Next() ([]byte, error) {
	// The previous message is no longer anyone's (Ingest copied it), and the
	// read below may park for the whole idle timeout: a buffer a large frame
	// grew is let go here, before the wait, not kept for the connection's
	// life (SPEC §5.5).
	f.buf = keepSmall(f.buf, f.r)
	if !f.decided {
		if err := f.detect(); err != nil {
			return nil, err
		}
	}
	if f.mode == envelope.FramingOctet {
		return f.nextOctet()
	}
	return f.nextLF()
}

// detect decides the framing ONCE per connection, from the first useful byte
// (SPEC §5.2).
//
// Per connection and not per message, deliberately. An emitter does not switch
// framing mid-stream, and re-deciding every message would let one legitimate
// message that happens to start with a digit -- which only exists if the emitter
// omitted the PRI -- flip the framing and scramble everything after it.
func (f *Framer) detect() error {
	b, err := f.r.Peek(1)
	if err != nil {
		return err
	}
	switch {
	case b[0] >= '0' && b[0] <= '9':
		// A syslog message never begins with a digit; it begins with '<'. So a
		// digit here is the octet count.
		f.mode = envelope.FramingOctet
	default:
		// '<' is the PRI, and anything else is junk or a message with no PRI.
		// LF framing is the one that can resynchronise.
		f.mode = envelope.FramingLF
	}
	f.decided = true
	return nil
}

// nextOctet reads "LENGTH SP MESSAGE".
func (f *Framer) nextOctet() ([]byte, error) {
	n := 0
	digits := 0
	for {
		c, err := f.r.ReadByte()
		if err != nil {
			if err == io.EOF && digits == 0 {
				return nil, io.EOF // clean end, between messages
			}
			return nil, io.ErrUnexpectedEOF
		}
		if c == ' ' {
			break
		}
		if c < '0' || c > '9' {
			return nil, ErrMalformedFrame
		}
		if digits++; digits > maxOctetDigits {
			return nil, ErrFrameTooLarge
		}
		n = n*10 + int(c-'0')
		if n > f.max {
			// Refused BEFORE allocating a byte: the length was declared, so
			// there is no need to read the payload to find out it is too big.
			return nil, ErrFrameTooLarge
		}
	}
	if digits == 0 {
		return nil, ErrMalformedFrame
	}
	f.buf = f.buf[:0]
	if n == 0 {
		// A declared length of zero is a legal empty message.
		return f.buf[:0], nil
	}

	// Grown as the bytes ARRIVE, never sized from the declared count. The
	// count costs the emitter nine bytes; sizing the buffer from it would let a
	// client that declares 16 MiB and then goes quiet hold 16 MiB for the whole
	// idle timeout, and with max_conns unlimited by default a thousand such
	// connections is an OOM kill that ring.max_bytes cannot see. Reading in
	// chunks keeps this server's cost proportional to the client's, exactly as
	// the LF framing already does; the n > max fuse above still refuses an
	// oversized declaration before any read (SPEC §5.5).
	if err := readCounted(f.r, &f.buf, n); err != nil {
		// The count promised bytes the connection did not deliver. There is
		// nothing to salvage: the prefix is half a message.
		return nil, io.ErrUnexpectedEOF
	}
	return f.buf, nil
}

// minFrameCap is the first block of a counted read: a peer that declares a
// huge length and sends three bytes costs 2 KiB, not 64 (pendência 1 de
// §21.1, SPEC §5.5).
const minFrameCap = 2 << 10

// frameKeepCap is the largest buffer a reader keeps between frames. After one
// 1 MiB message an idle connection would otherwise hold 1 MiB for as long as
// it lives, outside every ceiling (SPEC §5.5).
const frameKeepCap = 16 << 10

// keepSmall runs between frames, before the read that may park: it drops a
// frame buffer that grew past frameKeepCap when the reader is EMPTY, so what
// an idle connection holds is bounded by the small frames, not by its largest
// one (SPEC §5.5; the rule of go-syslog-server's Framer.Next).
//
// Only before a wait: with bytes already in the reader the next frame is here
// and the buffer is about to be needed again. Released after every frame
// above 16 KiB, as this code did until the independent review of 2026-11-07,
// a stream of 20-64 KiB frames reallocated the buffer about five times per
// frame (readCounted doubles from minFrameCap); go-syslog-server measured the
// same rule at 9x the time and 60x the allocation for 12 KiB frames.
func keepSmall(buf []byte, r *bufio.Reader) []byte {
	if cap(buf) > frameKeepCap && r.Buffered() == 0 {
		return nil
	}
	return buf[:0]
}

// readCounted reads exactly n bytes into *buf, growing it as the bytes ARRIVE
// rather than from the declared count (SPEC §5.5). On a short read *buf holds
// what did arrive and the error is io.ErrUnexpectedEOF.
//
// It takes what the reader has buffered, one refill at a time, and the
// capacity starts at minFrameCap and doubles (capped at n) as the bytes come
// in: a legitimate large frame pays a logarithmic number of copies, while a
// client that declares n and then goes quiet costs one small block for the
// whole idle timeout (ported from go-syslog-server's Framer.readCounted).
func readCounted(r *bufio.Reader, buf *[]byte, n int) error {
	b := (*buf)[:0]
	for len(b) < n {
		if r.Buffered() == 0 {
			if _, err := r.Peek(1); err != nil {
				*buf = b
				return io.ErrUnexpectedEOF
			}
		}
		take := r.Buffered()
		if want := n - len(b); take > want {
			take = want
		}
		if need := len(b) + take; need > cap(b) {
			c := max(2*cap(b), minFrameCap, need)
			if c > n {
				c = n
			}
			grown := make([]byte, len(b), c)
			copy(grown, b)
			b = grown
		}
		p, _ := r.Peek(take)
		b = append(b, p...)
		_, _ = r.Discard(take)
	}
	*buf = b
	return nil
}

// nextLF reads up to the next newline.
func (f *Framer) nextLF() ([]byte, error) {
	f.buf = f.buf[:0]
	for {
		chunk, err := f.r.ReadSlice('\n')
		f.buf = append(f.buf, chunk...)

		if len(f.buf) > f.max {
			// No length was declared, so the ceiling is the only thing standing
			// between a connection that never sends a newline and the process
			// running out of memory. The ring's byte ceiling does not help: this
			// is not an event yet.
			return nil, ErrFrameTooLarge
		}
		if err == bufio.ErrBufferFull {
			continue // the line is longer than the reader's buffer
		}
		if err == io.EOF {
			if len(f.buf) == 0 {
				return nil, io.EOF // clean end
			}
			// A final line with no terminator. Emitters do this on close, and
			// the bytes are a whole message, so it is delivered.
			return trimLF(f.buf), nil
		}
		if err != nil {
			return nil, err
		}

		out := trimLF(f.buf)
		if len(out) == 0 {
			// A bare newline. Common as hand-rolled keep-alive, so it is skipped
			// silently -- counting it as a dropped message would make a healthy
			// connection look lossy.
			f.buf = f.buf[:0]
			continue
		}
		return out, nil
	}
}

// trimLF removes the terminator and the two bytes emitters put before it.
//
// A NUL or a CR at the END is framing: BSD emitters terminate with NUL and
// Windows ones with CRLF. A NUL in the MIDDLE is data and stays, because the raw
// line is preserved byte for byte.
func trimLF(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == 0 {
		b = b[:n-1]
	}
	return b
}
