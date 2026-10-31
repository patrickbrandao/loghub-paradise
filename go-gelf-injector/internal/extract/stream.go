package extract

import (
	"bytes"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/httpx"
)

type mode uint8

const (
	mDetect mode = iota
	mScan        // midstream (or after a gap while detecting): looking for a proven start
	mGELF
	mHTTP
	mIgnore
)

// scanKeep bounds what a scanning direction keeps for the HTTP extractor's
// own resynchronization. Cleared at every multiple of it, by byte position:
// what is kept never depends on how the capture split the stream.
const scanKeep = 1 << 20

// httpMark is the end of a request line: " HTTP/1." then a digit
// (httpMarkDigit) then CR or LF (httpMarkEOL). The line end is required
// because the text " HTTP/1.1" alone appears inside GELF documents of access
// logs ("GET /x HTTP/1.1" 200) and would flip a midstream GELF direction to
// HTTP, losing every document after it; a raw CR or LF never appears inside
// a JSON string, so the full mark cannot come from a GELF document.
var httpMark = []byte(" HTTP/1.")

const (
	httpMarkDigit = 8 // hmatch after " HTTP/1."
	httpMarkEOL   = 9 // hmatch after the version digit
)

// Stream extracts the documents of one TCP direction (SPEC §6.5). It
// implements pcap.StreamHandler.
//
// The protocol is decided once per direction, from its first bytes: "{"
// (after whitespace or NULs) is null-framed GELF; an HTTP/1.x request line
// goes to package httpx; anything else is ignored when strict (no
// --pcap-port), or read as GELF otherwise.
//
// A direction the capture caught in the middle (or that lost bytes before
// the decision) is scanned byte by byte for the first proven start, and the
// first one found wins: a "{" whose preceding bytes, back to a NUL, are only
// whitespace (GELF), or the text " HTTP/1.x" followed by CR or LF (the end
// of a request line, which httpx then finds itself). Both are decided at a single byte with no look-ahead,
// so the outcome does not depend on how the stream was split (FuzzStream).
type Stream struct {
	st     *Stats
	strict bool
	emit   func(Doc)
	mode   mode
	ts     time.Time

	// detect and scan
	dbuf     []byte
	afterNul bool
	hmatch   int

	// GELF
	frame []byte
	skip  bool // dropping up to the next NUL (after a gap, or a frame too large)

	http *httpx.Stream
}

// NewStream returns the extractor of one direction.
func NewStream(strict, midstream bool, st *Stats, emit func(Doc)) *Stream {
	s := &Stream{st: st, strict: strict, emit: emit}
	if midstream {
		s.mode = mScan
		st.Resyncs++
	}
	return s
}

// Data implements pcap.StreamHandler.
func (s *Stream) Data(b []byte, ts time.Time) {
	s.ts = ts
	switch s.mode {
	case mDetect:
		s.detect(b)
	case mScan:
		s.scan(b)
	case mGELF:
		s.gelf(b)
	case mHTTP:
		s.http.Data(b, ts)
	}
}

func (s *Stream) detect(b []byte) {
	s.dbuf = append(s.dbuf, b...)
	i := 0
	for i < len(s.dbuf) && (isSpace(s.dbuf[i]) || s.dbuf[i] == 0) {
		i++
	}
	if i == len(s.dbuf) {
		if len(s.dbuf) > scanKeep {
			s.dbuf = s.dbuf[:0] // filler only: nothing to keep
		}
		return
	}
	rest := s.dbuf[i:]
	switch c := rest[0]; {
	case c == '{':
		s.toGELF(rest)
	case c >= 'A' && c <= 'Z':
		switch httpx.Classify(rest) {
		case httpx.KindNeed:
			return
		case httpx.KindRequest:
			s.toHTTP(false, rest)
		case httpx.KindOther:
			if s.strict {
				s.ignore()
			} else {
				s.toGELF(rest)
			}
		default: // a response or h2c: a server direction, or not replayable
			s.ignore()
		}
	default:
		if s.strict {
			s.ignore()
		} else {
			s.toGELF(rest)
		}
	}
}

func (s *Stream) ignore() {
	s.mode = mIgnore
	s.st.StreamsIgnored++
	s.dbuf = nil
}

func (s *Stream) toGELF(rest []byte) {
	s.mode = mGELF
	s.st.StreamsGELF++
	s.dbuf = nil // rest still points into it; gelf copies what it keeps
	s.gelf(rest)
}

func (s *Stream) toHTTP(mid bool, rest []byte) {
	s.mode = mHTTP
	s.st.StreamsHTTP++
	strict := s.strict
	s.http = httpx.NewStream(mid, &s.st.HTTP, func(r *httpx.Request, ts time.Time) {
		if d, ok := HTTPBody(r, strict, s.st); ok {
			s.emit(Doc{Data: d, TS: ts})
		}
	})
	s.dbuf = nil
	s.http.Data(rest, s.ts)
}

func (s *Stream) scan(b []byte) {
	for i, c := range b {
		if len(s.dbuf) >= scanKeep {
			s.st.SkippedBytes += uint64(len(s.dbuf))
			s.dbuf = s.dbuf[:0]
		}
		s.dbuf = append(s.dbuf, c)
		switch {
		case c == 0:
			s.afterNul = true
		case isSpace(c):
		case c == '{' && s.afterNul:
			s.st.SkippedBytes += uint64(len(s.dbuf) - 1)
			s.dbuf = nil
			s.mode = mGELF
			s.st.StreamsGELF++
			s.frame = append(s.frame[:0], '{')
			s.gelf(b[i+1:])
			return
		default:
			s.afterNul = false
		}
		// ' ' occurs only at the start of the mark, so a mismatch restarts
		// at 1 (on ' ') or 0: a one-byte decision with no look-ahead.
		matched := false
		switch {
		case s.hmatch < httpMarkDigit && c == httpMark[s.hmatch]:
			s.hmatch++
		case s.hmatch == httpMarkDigit && c >= '0' && c <= '9':
			s.hmatch++
		case s.hmatch == httpMarkEOL && (c == '\r' || c == '\n'):
			matched = true
		case c == httpMark[0]:
			s.hmatch = 1
		default:
			s.hmatch = 0
		}
		if matched {
			// httpx resynchronizes on the request line inside what is kept
			// and counts what it skips.
			kept := s.dbuf
			s.dbuf = nil
			s.mode = mHTTP
			s.st.StreamsHTTP++
			strict := s.strict
			s.http = httpx.NewStream(true, &s.st.HTTP, func(r *httpx.Request, ts time.Time) {
				if d, ok := HTTPBody(r, strict, s.st); ok {
					s.emit(Doc{Data: d, TS: ts})
				}
			})
			s.http.Data(kept, s.ts)
			if i+1 < len(b) {
				s.http.Data(b[i+1:], s.ts)
			}
			return
		}
	}
}

func (s *Stream) gelf(b []byte) {
	for len(b) > 0 {
		if s.skip {
			i := bytes.IndexByte(b, 0)
			if i < 0 {
				s.st.SkippedBytes += uint64(len(b))
				return
			}
			s.st.SkippedBytes += uint64(i + 1)
			b = b[i+1:]
			s.skip = false
			continue
		}
		i := bytes.IndexByte(b, 0)
		if i < 0 {
			if len(s.frame)+len(b) > MaxDoc {
				s.st.TooLarge++
				s.frame = s.frame[:0]
				s.skip = true
				continue
			}
			s.frame = append(s.frame, b...)
			return
		}
		if len(s.frame)+i > MaxDoc {
			s.st.TooLarge++
		} else {
			s.frame = append(s.frame, b[:i]...)
			s.finish(false)
		}
		s.frame = s.frame[:0]
		b = b[i+1:]
	}
}

// finish emits the frame held. At the end of the stream (last), a frame with
// no NUL is a document only if it looks whole: the servers accept one at EOF,
// but a capture that stopped early cuts the last frame anywhere.
func (s *Stream) finish(last bool) {
	f := trim(s.frame)
	if len(f) == 0 {
		return
	}
	switch {
	case last && s.strict && (!looksGELF(f) || f[len(f)-1] != '}'):
		s.st.LostFrames++
		s.st.SkippedBytes += uint64(len(f))
	case s.strict && !looksGELF(f):
		s.st.FramesNotGELF++
	default:
		s.st.Frames++
		s.emit(Doc{Data: append([]byte(nil), f...), TS: s.ts})
	}
}

// Gap implements pcap.StreamHandler: the bytes held are cut, and the stream
// resynchronizes.
func (s *Stream) Gap(n int) {
	switch s.mode {
	case mDetect:
		s.st.SkippedBytes += uint64(len(s.dbuf))
		s.dbuf = s.dbuf[:0]
		s.mode = mScan
		s.st.Resyncs++
	case mScan:
		s.st.SkippedBytes += uint64(len(s.dbuf))
		s.dbuf = s.dbuf[:0]
		s.afterNul, s.hmatch = false, 0
	case mGELF:
		if len(trim(s.frame)) > 0 {
			s.st.LostFrames++
		}
		s.st.SkippedBytes += uint64(len(s.frame))
		s.frame = s.frame[:0]
		if !s.skip {
			s.skip = true
			s.st.Resyncs++
		}
	case mHTTP:
		s.http.Gap(n)
	}
}

// End implements pcap.StreamHandler.
func (s *Stream) End() {
	switch s.mode {
	case mDetect:
		if len(trim(bytes.Trim(s.dbuf, "\x00"))) > 0 && !s.strict {
			// Never decided (an unfinished first line, say): non-strict
			// reads it as GELF, like the server would.
			s.toGELF(s.dbuf)
			s.End()
			return
		}
		s.ignore()
	case mScan:
		s.st.SkippedBytes += uint64(len(s.dbuf))
		s.ignore()
	case mGELF:
		if !s.skip {
			s.finish(true)
		}
		s.frame = nil
	case mHTTP:
		s.http.End()
	}
}
