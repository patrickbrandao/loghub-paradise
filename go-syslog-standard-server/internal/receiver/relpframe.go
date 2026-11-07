package receiver

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// RELP framing (SPEC §4.5).
//
//	TXNR SP COMMAND SP DATALEN [SP DATA] TRAILER
//
// TXNR is a client-chosen transaction number, COMMAND is open/syslog/close from
// the client and rsp/serverclose from us, DATALEN is decimal, and TRAILER is a
// single LF.
//
// RELP is not an RFC. It is librelp's protocol, and it is the ONLY widespread way
// to do syslog with a per-message acknowledgement -- which makes it the only
// transport where this project's acceptance rule can be honoured to the letter.
var (
	// ErrRELPFrame means the frame is unusable and the stream cannot be
	// resynchronised, so the connection ends.
	ErrRELPFrame = errors.New("receiver relp: malformed frame")
)

const (
	relpMaxTxnrDigits   = 12
	relpMaxCommandBytes = 32
	relpMaxLenDigits    = 12
)

// RELPFrame is one decoded frame. Data aliases an internal buffer and is valid
// only until the next ReadFrame on the same reader.
type RELPFrame struct {
	Txnr    int64
	Command string
	Data    []byte
}

// RELPReader decodes frames off one connection.
type RELPReader struct {
	r   *bufio.Reader
	max int
	buf []byte
	cmd []byte
}

func NewRELPReader(r *bufio.Reader, max int) *RELPReader {
	if max <= 0 {
		max = 16 << 20
	}
	return &RELPReader{r: r, max: max}
}

// DropBuffer lets go of a frame buffer that grew past frameKeepCap, whatever
// the reader holds. The RELP reader calls it before it parks on the window or
// on relp.inflight_max_bytes (SPEC §4.5): the previous frame was already
// copied, and a connection waiting for the budget must not hold a large
// buffer outside it.
func (rd *RELPReader) DropBuffer() {
	if cap(rd.buf) > frameKeepCap {
		rd.buf = nil
	}
}

// RELPHead is a frame read up to its DATALEN: everything but the payload.
// Len is the declared DATALEN, already checked against the ceiling.
type RELPHead struct {
	Txnr    int64
	Command string
	Len     int64
	sep     byte
}

// ReadFrame decodes the next frame: ReadHead, then ReadBody.
func (rd *RELPReader) ReadFrame() (RELPFrame, error) {
	h, err := rd.ReadHead()
	if err != nil {
		return RELPFrame{Txnr: h.Txnr, Command: h.Command}, err
	}
	return rd.ReadBody(h)
}

// ReadHead reads TXNR, COMMAND and DATALEN, and stops before the payload. The
// split is what lets the RELP reader take its window slot and its bytes of
// relp.inflight_max_bytes from the DECLARED length before a byte of the
// payload is read (SPEC §4.5): reading the whole frame first put one full
// frame per waiting connection in memory outside the ceiling.
func (rd *RELPReader) ReadHead() (RELPHead, error) {
	var h RELPHead
	// Between frames, before the read that may park (SPEC §5.5).
	rd.buf = keepSmall(rd.buf, rd.r)

	txnr, err := rd.readUint(relpMaxTxnrDigits, ' ')
	if err != nil {
		return h, err
	}
	h.Txnr = txnr

	rd.cmd = rd.cmd[:0]
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			return h, io.ErrUnexpectedEOF
		}
		if c == ' ' {
			break
		}
		// The command is an ASCII word. Anything else means the framing is off,
		// and there is no way to find the next frame boundary.
		if c < 'a' || c > 'z' {
			return h, ErrRELPFrame
		}
		if len(rd.cmd) >= relpMaxCommandBytes {
			return h, ErrRELPFrame
		}
		rd.cmd = append(rd.cmd, c)
	}
	if len(rd.cmd) == 0 {
		return h, ErrRELPFrame
	}
	h.Command = string(rd.cmd)

	// DATALEN ends at a space when there is data, or at the trailer when there is
	// none. Both spellings exist in the field.
	n, sep, err := rd.readLenAndSep()
	if err != nil {
		return h, err
	}
	if n > int64(rd.max) {
		// The length was DECLARED, so this is refused before a byte of payload
		// is read -- the same fuse the octet-counting framer has.
		return h, ErrFrameTooLarge
	}
	if n > 0 && sep != ' ' {
		return h, ErrRELPFrame
	}
	h.Len, h.sep = n, sep
	return h, nil
}

// ReadBody reads the payload and the trailer of the frame h announced.
func (rd *RELPReader) ReadBody(h RELPHead) (RELPFrame, error) {
	f := RELPFrame{Txnr: h.Txnr, Command: h.Command}
	if h.Len == 0 {
		// "3 close 0\n" and "3 close 0 \n" are both in the field: emitters
		// disagree about whether the space before an absent DATA is written.
		// Accepting only one would refuse half the installed base over one byte.
		if h.sep == ' ' {
			c, err := rd.r.ReadByte()
			if err != nil {
				return f, io.ErrUnexpectedEOF
			}
			if c != '\n' {
				return f, ErrRELPFrame
			}
		} else if h.sep != '\n' {
			return f, ErrRELPFrame
		}
		f.Data = rd.buf[:0]
		return f, nil
	}

	// Grown as the bytes arrive, never sized from DATALEN: the same defence
	// the octet-counting framer has, for the same reason (SPEC §5.5).
	rd.buf = rd.buf[:0]
	if err := readCounted(rd.r, &rd.buf, int(h.Len)); err != nil {
		return f, io.ErrUnexpectedEOF
	}
	// The trailer is mandatory and is NOT part of the message.
	c, err := rd.r.ReadByte()
	if err != nil {
		return f, io.ErrUnexpectedEOF
	}
	if c != '\n' {
		return f, ErrRELPFrame
	}
	f.Data = rd.buf
	return f, nil
}

// readUint reads decimal digits up to a terminator.
func (rd *RELPReader) readUint(maxDigits int, term byte) (int64, error) {
	var v int64
	digits := 0
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			if err == io.EOF && digits == 0 {
				return 0, io.EOF // clean end, between frames
			}
			return 0, io.ErrUnexpectedEOF
		}
		if c == term {
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

// readLenAndSep reads DATALEN and reports which byte terminated it.
func (rd *RELPReader) readLenAndSep() (int64, byte, error) {
	var v int64
	digits := 0
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			return 0, 0, io.ErrUnexpectedEOF
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

// AppendRSP renders a response frame into dst.
//
//	<txnr> rsp <len> <code> <text>\n
//
// The code is the first token of the data, which is what librelp parses: 200 for
// accepted, 500 for refused.
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
