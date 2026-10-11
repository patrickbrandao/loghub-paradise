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

// ReadFrame decodes the next frame.
func (rd *RELPReader) ReadFrame() (RELPFrame, error) {
	var f RELPFrame

	txnr, err := rd.readUint(relpMaxTxnrDigits, ' ')
	if err != nil {
		return f, err
	}
	f.Txnr = txnr

	rd.cmd = rd.cmd[:0]
	for {
		c, err := rd.r.ReadByte()
		if err != nil {
			return f, io.ErrUnexpectedEOF
		}
		if c == ' ' {
			break
		}
		// The command is an ASCII word. Anything else means the framing is off,
		// and there is no way to find the next frame boundary.
		if c < 'a' || c > 'z' {
			return f, ErrRELPFrame
		}
		if len(rd.cmd) >= relpMaxCommandBytes {
			return f, ErrRELPFrame
		}
		rd.cmd = append(rd.cmd, c)
	}
	if len(rd.cmd) == 0 {
		return f, ErrRELPFrame
	}
	f.Command = string(rd.cmd)

	// DATALEN ends at a space when there is data, or at the trailer when there is
	// none. Both spellings exist in the field.
	n, sep, err := rd.readLenAndSep()
	if err != nil {
		return f, err
	}
	if n > int64(rd.max) {
		// The length was DECLARED, so this is refused before a byte of payload
		// is read -- the same fuse the octet-counting framer has.
		return f, ErrFrameTooLarge
	}

	if n == 0 {
		// "3 close 0\n" and "3 close 0 \n" are both in the field: emitters
		// disagree about whether the space before an absent DATA is written.
		// Accepting only one would refuse half the installed base over one byte.
		if sep == ' ' {
			c, err := rd.r.ReadByte()
			if err != nil {
				return f, io.ErrUnexpectedEOF
			}
			if c != '\n' {
				return f, ErrRELPFrame
			}
		} else if sep != '\n' {
			return f, ErrRELPFrame
		}
		f.Data = rd.buf[:0]
		return f, nil
	}

	if sep != ' ' {
		return f, ErrRELPFrame
	}
	// Grown as the bytes arrive, never sized from DATALEN: the same defence
	// the octet-counting framer has, for the same reason (SPEC §5.5).
	if err := readCounted(rd.r, &rd.buf, int(n)); err != nil {
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
