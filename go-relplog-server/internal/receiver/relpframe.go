package receiver

import (
	"bufio"
	"errors"
	"io"
	"math"
	"strconv"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// RELP framing (SPEC §5.1).
//
//	TXNR SP COMMAND SP DATALEN [SP DATA] TRAILER
//
// TXNR is a client-chosen transaction number, COMMAND is open/syslog/close from
// the client and rsp/serverclose from us, DATALEN is decimal, and TRAILER is a
// single LF. None of the header, nor the trailer, belongs to the message: only
// DATA is the syslog line.
//
// RELP is not an RFC. It is librelp's protocol, and it is the ONLY widespread way
// to do syslog with a per-message acknowledgement -- which is why this server
// exists.
var (
	// ErrRELPFrame means the frame is unusable and the stream cannot be
	// resynchronised, so the connection ends. Looking for "the next frame"
	// would store fragments as messages.
	ErrRELPFrame = errors.New("receiver relp: malformed frame")

	// ErrFrameTooLarge is a DATALEN above relp.max_frame_bytes, refused before
	// a byte of the payload is read (SPEC §5.2).
	ErrFrameTooLarge = errors.New("receiver relp: DATALEN above relp.max_frame_bytes")
)

const (
	// relpMaxTxnrDigits: the RELP specification caps TXNR at 999 999 999, and
	// librelp wraps there (relpEngineNextTXNR). A longer number is not a
	// transaction number, it is a desynchronised stream.
	relpMaxTxnrDigits = 9
	// relpMaxCommandBytes: "1*32ALPHA" in the specification.
	relpMaxCommandBytes = 32
	// relpMaxLenDigits: DATALEN has the same nine-digit ceiling. The
	// configured relp.max_frame_bytes is the real limit; this one only stops
	// the accumulator before it overflows.
	relpMaxLenDigits = 9

	// relpFirstBlock is the first capacity of the frame buffer. A syslog line
	// has a median of 150-250 bytes, so most connections never grow it; the
	// buffer doubles as the bytes ARRIVE, never from DATALEN (SPEC §5.2).
	relpFirstBlock = 2 << 10
	// relpKeepBlock is the largest buffer a reader keeps between frames.
	// After one 64 KiB message an idle connection would otherwise hold 64 KiB
	// for as long as it lives, outside every ceiling (SPEC §15.1).
	relpKeepBlock = 16 << 10
)

// RELPFrame is one decoded frame. Data aliases an internal buffer and is valid
// only until the next ReadFrame on the same reader.
type RELPFrame struct {
	Txnr    int64
	Command obs.RELPCommand
	// Data is DATA cut at the reader's message limit (syslog.max_message_bytes):
	// what the item keeps. Size is DATALEN, the original size, which the
	// payload reports and which tells the converter the line was truncated.
	Data []byte
	Size int
}

// Truncated reports whether DATA was longer than what Data holds.
func (f *RELPFrame) Truncated() bool { return f.Size > len(f.Data) }

// RELPReader decodes frames off one connection.
type RELPReader struct {
	r        *bufio.Reader
	maxFrame int64
	maxMsg   int
	buf      []byte
	cmd      [relpMaxCommandBytes]byte
}

// NewRELPReader reads frames from r. maxFrame is relp.max_frame_bytes (the
// fuse that closes the connection); maxMsg is syslog.max_message_bytes (the
// cut that truncates). Non-positive values select 16 MiB and 64 KiB.
func NewRELPReader(r *bufio.Reader, maxFrame int64, maxMsg int) *RELPReader {
	if maxFrame <= 0 {
		maxFrame = defaultMaxFrameBytes
	}
	if maxMsg <= 0 {
		maxMsg = defaultMaxMessageBytes
	}
	return &RELPReader{r: r, maxFrame: maxFrame, maxMsg: maxMsg}
}

// ReadFrame decodes the next frame.
//
// It returns io.EOF only for a connection that ended cleanly BETWEEN frames,
// and the reader's own error (a timeout, a reset) untouched when nothing of a
// frame was read yet, so the caller can tell an idle timeout from a broken
// stream. Inside a frame, the end of the stream is io.ErrUnexpectedEOF: a half
// frame was never answered, so librelp will retransmit it, and nothing of it
// is kept.
func (rd *RELPReader) ReadFrame() (RELPFrame, error) {
	var f RELPFrame
	if cap(rd.buf) > relpKeepBlock {
		rd.buf = nil
	}

	txnr, err := rd.readUint(relpMaxTxnrDigits, true)
	if err != nil {
		return f, err
	}
	f.Txnr = txnr

	n := 0
	for {
		c, err := rd.readByte()
		if err != nil {
			return f, err
		}
		if c == ' ' {
			break
		}
		// The command is an ASCII word. Anything else means the framing is
		// off, and there is no way to find the next frame boundary.
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') || n >= relpMaxCommandBytes {
			return f, ErrRELPFrame
		}
		rd.cmd[n] = c
		n++
	}
	if n == 0 {
		return f, ErrRELPFrame
	}
	f.Command = commandOf(rd.cmd[:n])

	// DATALEN ends at a space when there is data, or at the trailer when there
	// is none. Both spellings exist in the field.
	size, sep, err := rd.readLen()
	if err != nil {
		return f, err
	}
	if size > rd.maxFrame {
		// The length was DECLARED: refused before a byte of payload is read.
		return f, ErrFrameTooLarge
	}
	f.Size = clampInt(size)

	if size == 0 {
		// "3 close 0\n" and "3 close 0 \n" are both in the field: emitters
		// disagree about whether the space before an absent DATA is written.
		// Accepting only one would refuse half the installed base over one
		// byte.
		if sep == ' ' {
			c, err := rd.readByte()
			if err != nil {
				return f, err
			}
			if c != '\n' {
				return f, ErrRELPFrame
			}
		}
		f.Data = rd.buf[:0]
		return f, nil
	}
	if sep != ' ' {
		return f, ErrRELPFrame
	}

	keep := size
	if keep > int64(rd.maxMsg) {
		keep = int64(rd.maxMsg)
	}
	if err := rd.readKept(int(keep)); err != nil {
		return f, err
	}
	// What passes syslog.max_message_bytes is consumed WITHOUT being stored
	// (SPEC §5.3), and the connection stays: truncation is not an error.
	if size > keep {
		if err := discard(rd.r, size-keep); err != nil {
			return f, midFrame(err)
		}
	}
	// The trailer is mandatory and is NOT part of the message.
	c, err := rd.readByte()
	if err != nil {
		return f, err
	}
	if c != '\n' {
		return f, ErrRELPFrame
	}
	f.Data = rd.buf
	return f, nil
}

// readKept reads exactly n bytes into rd.buf, growing it by doubling as the
// bytes arrive: a peer that declares 16 MiB and goes quiet costs one small
// block, never the declared size (the bug of 2026-09-14, SPEC §5.2).
func (rd *RELPReader) readKept(n int) error {
	if rd.buf == nil {
		rd.buf = make([]byte, 0, min(relpFirstBlock, n))
	}
	rd.buf = rd.buf[:0]
	for len(rd.buf) < n {
		if len(rd.buf) == cap(rd.buf) {
			c := 2 * cap(rd.buf)
			if c < relpFirstBlock {
				c = relpFirstBlock
			}
			if c > n {
				c = n
			}
			nb := make([]byte, len(rd.buf), c)
			copy(nb, rd.buf)
			rd.buf = nb
		}
		end := min(cap(rd.buf), n)
		got, err := io.ReadFull(rd.r, rd.buf[len(rd.buf):end])
		rd.buf = rd.buf[:len(rd.buf)+got]
		if err != nil {
			return midFrame(err)
		}
	}
	return nil
}

// readByte reads one byte INSIDE a frame: the end of the stream there is a
// half frame.
func (rd *RELPReader) readByte() (byte, error) {
	c, err := rd.r.ReadByte()
	if err != nil {
		return 0, midFrame(err)
	}
	return c, nil
}

// readUint reads the TXNR up to its space. first marks the start of a frame,
// where the end of the stream is a clean close.
func (rd *RELPReader) readUint(maxDigits int, first bool) (int64, error) {
	var v int64
	digits := 0
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			if first && digits == 0 {
				return 0, err // clean end, or the idle timeout, between frames
			}
			return 0, midFrame(err)
		}
		if c == ' ' {
			if digits == 0 {
				return 0, ErrRELPFrame
			}
			return v, nil
		}
		if c < '0' || c > '9' {
			return 0, ErrRELPFrame
		}
		if digits++; digits > maxDigits {
			return 0, ErrRELPFrame
		}
		v = v*10 + int64(c-'0')
	}
}

// readLen reads DATALEN and reports which byte terminated it.
func (rd *RELPReader) readLen() (int64, byte, error) {
	var v int64
	digits := 0
	for {
		c, err := rd.readByte()
		if err != nil {
			return 0, 0, err
		}
		if c == ' ' || c == '\n' {
			if digits == 0 {
				return 0, 0, ErrRELPFrame
			}
			return v, c, nil
		}
		if c < '0' || c > '9' {
			return 0, 0, ErrRELPFrame
		}
		if digits++; digits > relpMaxLenDigits {
			return 0, 0, ErrRELPFrame
		}
		v = v*10 + int64(c-'0')
	}
}

// commandOf maps the command word to the closed set, without allocating. The
// specification makes commands case-sensitive and lowercase.
func commandOf(b []byte) obs.RELPCommand {
	switch string(b) { // the compiler does not allocate for a switch on string(b)
	case "syslog":
		return obs.RELPSyslog
	case "open":
		return obs.RELPOpen
	case "close":
		return obs.RELPClose
	}
	return obs.RELPUnknown
}

// midFrame turns the end of the stream inside a frame into io.ErrUnexpectedEOF
// and leaves every other error (a timeout, a reset) as it is.
func midFrame(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// discard skips n bytes of r without storing them. bufio.Reader.Discard takes
// an int, which a 32-bit platform cannot fill with every DATALEN.
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

// clampInt converts a declared length to the int of Head.Size.
func clampInt(n int64) int {
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n)
}

// AppendRSP renders a response frame into dst.
//
//	<txnr> rsp <len> <code> <text>\n
//
// The code is the first token of the data, which is what librelp parses
// (scrsp.c): three digits, a space, up to 80 characters of text. 200 is
// accepted; anything else is a FINAL refusal -- librelp drops the message it
// answers and never retransmits it (SPEC §5.4).
func AppendRSP(dst []byte, txnr int64, body string) []byte {
	dst = strconv.AppendInt(dst, txnr, 10)
	dst = append(dst, " rsp "...)
	dst = strconv.AppendInt(dst, int64(len(body)), 10)
	if len(body) > 0 {
		dst = append(dst, ' ')
		dst = append(dst, body...)
	}
	return append(dst, '\n')
}

// serverClose is the "serverclose" hint (SPEC §5.8): txnr 0, no data. librelp
// marks the session broken on it (cserverclose.c) and retransmits whatever is
// still unacknowledged on the next session.
const serverClose = "0 serverclose 0\n"
