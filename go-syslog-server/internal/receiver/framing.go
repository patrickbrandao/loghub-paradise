package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
)

// FramingMode is tcp.framing (SPEC §5.2, §16.5). TLS always uses FramingOctet
// (SPEC §4.5): RFC 5425 §4.3 has only octet-counting, and there is nothing to
// detect.
type FramingMode uint8

const (
	// FramingAuto decides ONCE per connection, from the first bytes, with the
	// confirmation of SPEC §5.2. It is the default.
	FramingAuto FramingMode = iota
	// FramingOctet fixes octet-counting (RFC 6587 §3.4.1).
	FramingOctet
	// FramingLF fixes non-transparent framing (RFC 6587 §3.4.2).
	FramingLF
)

func (m FramingMode) String() string {
	switch m {
	case FramingAuto:
		return "auto"
	case FramingOctet:
		return "octet"
	case FramingLF:
		return "lf"
	}
	return "unknown"
}

// ParseFramingMode reads the tcp.framing value: auto, octet or lf.
func ParseFramingMode(s string) (FramingMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "auto", "":
		return FramingAuto, nil
	case "octet", "octet-counting":
		return FramingOctet, nil
	case "lf", "non-transparent":
		return FramingLF, nil
	}
	return 0, fmt.Errorf("receiver: tcp.framing %q: want auto, octet or lf", s)
}

// ErrMalformedFrame means an octet-counting stream is DESYNCHRONISED and cannot
// be recovered: a MSG-LEN that does not confirm (a leading zero, no space
// within ten digits, a non-digit) leaves no way to find where the next message
// begins. Trying to "look for the next <" would write fragments into the
// destination as if they were messages (SPEC §5.2). It ENDS THE CONNECTION,
// counted as reason="malformed_frame".
//
// Size is NOT a framing error any more. The standard server closed the
// connection at a 16 MiB fuse; here an oversized message is truncated and the
// rest consumed, and the connection lives on (SPEC §5.5).
var ErrMalformedFrame = errors.New("receiver: malformed octet-counting frame; the stream is desynchronised")

// maxOctetDigits bounds MSG-LEN. RFC 6587 sets no limit, but reading digits
// without a bound is an unbounded read, and ten digits already declare more
// than any message this server keeps (SPEC §5.2: [1-9][0-9]{0,9}).
const maxOctetDigits = 10

// frameClasses are the steps a frame buffer grows by: the classes of the item
// pools (SPEC §5.4, §15.1). The first is small enough that a peer declaring a
// huge MSG-LEN and sending three bytes costs one 2 KiB block, and large enough
// that the p99 line needs no second allocation. Above the last class (only when
// max_message_bytes was raised) the buffer doubles.
var frameClasses = [...]int{2 << 10, 8 << 10, 64 << 10}

// minFrameCap is the first block.
const minFrameCap = 2 << 10

// Framer reads messages off one stream connection (SPEC §5).
//
// RFC 6587 defines two framings and a real deployment uses either:
//
//	octet-counting (§3.4.1)          "34 <13>Sep 12 14:33:01 host app: oi"
//	non-transparent-framing (§3.4.2) "<13>Sep 12 14:33:01 host app: oi\n"
//
// The RFC calls the second one "not recommended" and it is the one everybody
// uses.
//
// What is NEVER in a returned message: the MSG-LEN and its space, the LF or NUL
// terminator, the CRs before the terminator. Those are transport, and a framing
// byte that leaked into the raw line would break the fidelity invariant
// (§20.6) and show up in everyone's destination.
type Framer struct {
	r    *bufio.Reader
	mode FramingMode
	max  int

	// framing is the decision, valid once decided is true.
	framing item.Framing
	decided bool

	// buf is reused across messages and only ever grows with bytes that
	// ARRIVED, never with a declared length, up to max. The slice Next returns
	// aliases it, so it is valid only until the following call -- Ingest
	// copies into a pooled buffer before the item exists.
	buf []byte

	// err is sticky: a frame that ended because the connection did is
	// delivered first, and the reason the connection ended comes back on the
	// next call.
	err error
}

// NewFramer wraps a connection. maxBytes is syslog.max_message_bytes: the
// longest message kept; the excess is consumed without allocating (SPEC §5.5).
func NewFramer(r *bufio.Reader, mode FramingMode, maxBytes int) *Framer {
	if maxBytes <= 0 {
		maxBytes = defaultMaxMessageBytes
	}
	f := &Framer{r: r, mode: mode, max: maxBytes}
	switch mode {
	case FramingOctet:
		f.framing, f.decided = item.FramingOctetCounting, true
	case FramingLF:
		f.framing, f.decided = item.FramingNonTransparent, true
	}
	return f
}

// Framing reports the framing in force. On an auto-detecting framer it is only
// meaningful after the first successful Next.
func (f *Framer) Framing() item.Framing { return f.framing }

// Next returns the next message, its ORIGINAL size and the framing it came in.
//
// size is the length of the SYSLOG-MSG on the wire, before truncation; a
// message longer than max comes back cut to max with size > len(msg), which is
// how the converter knows it was truncated (SPEC §5.5). An EMPTY message (two
// terminators in a row, a lone CRLF) comes back with len 0 so that the
// pipeline can count it as reason="empty" (SPEC §5.6); it is never skipped in
// silence.
//
// err is io.EOF at a clean end, ErrMalformedFrame for a desynchronised
// octet-counting stream, or the connection's own error. The message slice
// aliases an internal buffer and is valid only until the next call.
func (f *Framer) Next() (msg []byte, size int, framing item.Framing, err error) {
	if f.err != nil {
		return nil, 0, f.framing, f.err
	}
	if !f.decided {
		if err := f.detect(); err != nil {
			return nil, 0, f.framing, err
		}
	}
	if f.framing == item.FramingOctetCounting {
		msg, size, err = f.nextOctet()
	} else {
		msg, size, err = f.nextLF()
	}
	return msg, size, f.framing, err
}

// detect decides the framing ONCE per connection, with confirmation (SPEC §5.2,
// deviation D1 of §1.3):
//
//	[1-9][0-9]{0,9} SP     octet-counting
//	anything else          non-transparent
//
// Per connection and not per frame, against the skill, for two real reasons.
// Re-deciding every frame lets one line without a PRI that happens to read
// "123 text" become a 123-octet frame that swallows the lines after it. And the
// digit alone is not enough even for the FIRST frame: the log4j appender of the
// corpus sends "2026-09-13 10:00:00,123 ERROR ..." with no PRI, and "2026"
// without a space would be read as a MSG-LEN and the connection closed on every
// line. The space is the confirmation.
//
// The bytes are PEEKED, one more at a time and only while they still match, so
// the decision never waits for a byte it does not need: "<13>..." decides on
// its first byte, the log4j line on its fifth. Peeking a fixed eleven bytes
// would block a short first line -- "hi\n" from an appliance that logs once an
// hour -- until ten more bytes arrived.
func (f *Framer) detect() error {
	for i := 0; ; i++ {
		b, err := f.r.Peek(i + 1)
		if len(b) <= i {
			if i == 0 {
				// Nothing at all: a clean end, or the connection's error
				// before its first byte. Report it as is.
				return err
			}
			// The stream ended -- EOF, an idle timeout, a reset, the
			// shutdown's deadline -- inside a run of digits. No space can
			// confirm a MSG-LEN any more, so the digits are a non-transparent
			// frame, and nextLF delivers it like any other frame the end of
			// the connection completed (SPEC §5.3). Returning the error here
			// would drop bytes that arrived, with no counter.
			f.decide(item.FramingNonTransparent)
			return nil
		}
		c := b[i]
		switch {
		case c == ' ' && i > 0:
			f.decide(item.FramingOctetCounting)
			return nil
		case i < maxOctetDigits && c >= '0' && c <= '9' && (i > 0 || c != '0'):
			continue
		default:
			f.decide(item.FramingNonTransparent)
			return nil
		}
	}
}

func (f *Framer) decide(fr item.Framing) {
	f.framing = fr
	f.decided = true
}

// nextOctet reads "MSG-LEN SP SYSLOG-MSG" (SPEC §5.4).
//
// The MSG-LEN octets after the space are the message, exactly: nothing is
// removed. If the emitter counted a trailing LF, the LF is data, and the
// converter takes it off the end of message (SPEC §6.7) while raw keeps it.
func (f *Framer) nextOctet() ([]byte, int, error) {
	n, err := f.readMsgLen()
	if err != nil {
		return nil, 0, err
	}
	keep := n
	if keep > int64(f.max) {
		keep = int64(f.max)
	}
	size := clampInt(n)

	if err := f.readCounted(int(keep)); err != nil {
		// The connection ended in the middle of the frame. What arrived is
		// delivered, with the DECLARED length as its size, so the converter
		// marks it truncated (SPEC §5.4); the end of the connection comes
		// back on the next call.
		f.err = endOfStream(err)
		return f.buf, size, nil
	}
	if rest := n - keep; rest > 0 {
		// The excess of a truncated message is consumed WITHOUT allocating,
		// to keep the stream in sync: bufio.Reader.Discard skips through its
		// own buffer. The connection is NOT closed for size (SPEC §5.5).
		if err := discard(f.r, rest); err != nil {
			f.err = endOfStream(err)
		}
	}
	return f.buf, size, nil
}

// readMsgLen reads MSG-LEN = NONZERO-DIGIT *DIGIT, at most ten digits, and the
// space after it (SPEC §5.2, §5.4).
func (f *Framer) readMsgLen() (int64, error) {
	var n int64
	digits := 0
	for {
		c, err := f.r.ReadByte()
		if err != nil {
			if digits == 0 {
				// Between frames: a clean end, or the connection's error.
				return 0, err
			}
			if errors.Is(err, io.EOF) {
				// Half a MSG-LEN and nothing else: a frame was cut before
				// a single octet of it arrived. Nothing to deliver, and the
				// connection is over; count it as the frame it was.
				return 0, fmt.Errorf("%w: the connection ended inside MSG-LEN", ErrMalformedFrame)
			}
			return 0, err
		}
		switch {
		case c == ' ' && digits > 0:
			return n, nil
		case c >= '0' && c <= '9':
			if digits == 0 && c == '0' {
				return 0, fmt.Errorf("%w: MSG-LEN with a leading zero", ErrMalformedFrame)
			}
			if digits++; digits > maxOctetDigits {
				return 0, fmt.Errorf("%w: MSG-LEN longer than %d digits", ErrMalformedFrame, maxOctetDigits)
			}
			n = n*10 + int64(c-'0')
		default:
			return 0, fmt.Errorf("%w: MSG-LEN is not a decimal number followed by a space", ErrMalformedFrame)
		}
	}
}

// readCounted reads exactly n bytes into f.buf, growing it as the bytes ARRIVE
// rather than from the declared count (SPEC §5.4). On a short read f.buf holds
// what did arrive.
//
// The count costs the emitter ten bytes; sizing the buffer from it would let a
// client that declares 64 KiB and then goes quiet hold 64 KiB for the whole
// idle timeout, and with max_conns unlimited by default a thousand such
// connections is memory that ring.max_bytes cannot see. Reading what is
// buffered, one refill at a time, keeps this server's cost proportional to the
// client's, exactly as the LF framing does.
func (f *Framer) readCounted(n int) error {
	b := f.buf[:0]
	for len(b) < n {
		if f.r.Buffered() == 0 {
			if _, err := f.r.Peek(1); err != nil {
				f.buf = b
				return err
			}
		}
		take := f.r.Buffered()
		if want := n - len(b); take > want {
			take = want
		}
		p, _ := f.r.Peek(take)
		b = appendGrow(b, p, f.max)
		_, _ = f.r.Discard(take)
	}
	f.buf = b
	return nil
}

// nextLF reads one non-transparent frame (SPEC §5.3): up to the first LF or
// NUL, with every CR immediately before the terminator removed.
//
// The terminator and the trailing CRs are in neither the message nor its size.
// A message longer than max keeps its first max octets and the rest is
// discarded up to the terminator, counted into size (without the trailing CRs)
// and never stored (SPEC §5.5).
//
// The end of the connection completes the last frame: it is delivered whole,
// not truncated (SPEC §5.3). The same holds for any other error that ends the
// connection after some bytes arrived -- an idle timeout, a reset, the
// shutdown's deadline -- because there is no declared length to compare with,
// and dropping bytes that did arrive would be a loss with no counter.
func (f *Framer) nextLF() ([]byte, int, error) {
	b := f.buf[:0]
	total := 0 // octets before the terminator, trailing CRs included
	crRun := 0 // CRs at the end of what has been seen so far

	for {
		if f.r.Buffered() == 0 {
			if _, err := f.r.Peek(1); err != nil {
				if total == 0 {
					f.buf = b
					return nil, 0, err
				}
				// The connection ended mid-frame: the frame is complete as
				// it stands, and the end comes back on the next call.
				f.err = endOfStream(err)
				break
			}
		}
		avail, _ := f.r.Peek(f.r.Buffered())
		end := indexTerminator(avail)
		seg := avail
		if end >= 0 {
			seg = avail[:end]
		}
		if len(seg) > 0 {
			// The CR run is tracked across segments: a CRLF can arrive
			// split over two reads, and a run of CRs can straddle the point
			// where the kept bytes stop.
			j := len(seg)
			for j > 0 && seg[j-1] == '\r' {
				j--
			}
			if j == 0 {
				crRun += len(seg)
			} else {
				crRun = len(seg) - j
			}
			if room := f.max - len(b); room > 0 {
				k := len(seg)
				if k > room {
					k = room
				}
				b = appendGrow(b, seg[:k], f.max)
			}
			total += len(seg)
		}
		if end >= 0 {
			_, _ = f.r.Discard(end + 1)
			break
		}
		_, _ = f.r.Discard(len(avail))
	}

	// The trailing CRs belong to the terminator. When the run reaches back
	// into the kept bytes, the cut removes them there too.
	size := total - crRun
	if len(b) > size {
		b = b[:size]
	}
	f.buf = b
	return b, size, nil
}

// indexTerminator finds the first LF or NUL.
func indexTerminator(b []byte) int {
	lf := bytes.IndexByte(b, '\n')
	nul := bytes.IndexByte(b, 0)
	switch {
	case lf < 0:
		return nul
	case nul < 0 || lf < nul:
		return lf
	}
	return nul
}

// appendGrow appends p to b, growing the capacity by the pool classes (then
// doubling) but never past limit. The growth is driven by bytes that ARRIVED,
// never by a declared length; the caller guarantees len(b)+len(p) <= limit.
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

// discard skips n bytes of r without storing them. bufio.Reader.Discard takes
// an int, and MSG-LEN can declare up to 9 999 999 999, which a 32-bit int does
// not hold.
func discard(r *bufio.Reader, n int64) error {
	for n > 0 {
		step := n
		if step > math.MaxInt32 {
			step = math.MaxInt32
		}
		d, err := r.Discard(int(step))
		n -= int64(d)
		if err != nil {
			return err
		}
	}
	return nil
}

// clampInt converts a declared length to the int of Head.Size. Only a 32-bit
// platform and a MSG-LEN above 2 GiB ever clamp.
func clampInt(n int64) int {
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}

// endOfStream normalises the error that ended a frame early: the half-read of
// a frame is the end of the connection, nothing more specific.
func endOfStream(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return io.EOF
	}
	return err
}
