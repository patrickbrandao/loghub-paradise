// Package httpx finds the HTTP/1.x requests inside a reassembled TCP stream
// (SPEC §6.6): pipelined requests one after the other, bodies by
// Content-Length or chunked, and resynchronization on the next request line
// after a capture gap or from a capture that began inside a connection.
//
// net/http does the parsing (http.ReadRequest), so a request is extracted
// exactly when the server's own parser would read one.
package httpx

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Request is one captured request.
type Request struct {
	Method string
	URI    string // the request-target as captured (path and query)
	Proto  string
	Header http.Header
	Body   []byte
}

// MaxRequest bounds one request (head and body); a larger one is dropped and
// the stream resynchronizes.
const MaxRequest = 64 << 20

// resyncWindow: a resynchronizing stream with no candidate keeps this tail.
const resyncWindow = 64

// Stats counts what the extractor did over all streams.
type Stats struct {
	Requests     uint64
	HTTPStreams  uint64 // client directions that carried HTTP/1.x
	Responses    uint64 // server directions (HTTP/1.x responses), ignored
	H2C          uint64 // h2c prior-knowledge directions, not replayed
	NotHTTP      uint64 // anything else
	Resyncs      uint64
	SkippedBytes uint64
	Lost         uint64 // requests cut by a gap or by the end of the stream
	TooLarge     uint64
}

type mode uint8

const (
	mDetect mode = iota
	mHTTP
	mIgnore
)

// Stream implements pcap.StreamHandler for one direction.
type Stream struct {
	st     *Stats
	emit   func(*Request, time.Time)
	mode   mode
	resync bool
	buf    []byte
	need   int // do not reparse before the buffer has this many bytes
	ts     time.Time
	ended  bool
}

// NewStream returns the parser of one direction. A midstream direction starts
// resynchronizing.
func NewStream(midstream bool, st *Stats, emit func(*Request, time.Time)) *Stream {
	return &Stream{st: st, emit: emit, resync: midstream}
}

// Data implements pcap.StreamHandler.
func (s *Stream) Data(b []byte, ts time.Time) {
	if s.mode == mIgnore {
		return
	}
	s.ts = ts
	s.buf = append(s.buf, b...)
	s.run(false)
}

// Gap implements pcap.StreamHandler.
func (s *Stream) Gap(int) {
	if s.mode == mIgnore {
		return
	}
	if len(s.buf) > 0 {
		s.st.Lost++
		s.st.SkippedBytes += uint64(len(s.buf))
		s.buf = s.buf[:0]
	}
	s.need = 0
	if !s.resync {
		s.resync = true
		s.st.Resyncs++
	}
}

// End implements pcap.StreamHandler.
func (s *Stream) End() {
	if s.ended {
		return
	}
	s.ended = true
	if s.mode != mIgnore {
		s.need = 0
		s.run(true)
		if len(s.buf) > 0 {
			s.st.Lost++
			s.st.SkippedBytes += uint64(len(s.buf))
			s.buf = nil
		}
	}
	switch s.mode {
	case mHTTP:
		s.st.HTTPStreams++
	case mDetect:
		s.st.NotHTTP++
	}
}

func (s *Stream) run(final bool) {
	for len(s.buf) > 0 && s.mode != mIgnore {
		if s.need > 0 && len(s.buf) < s.need && !final {
			return
		}
		if s.mode == mDetect && !s.resync {
			switch kind := lineKind(s.buf); kind {
			case lkNeed:
				if !final {
					return
				}
				s.mode = mIgnore
				s.st.NotHTTP++
				return
			case lkRequest:
				s.mode = mHTTP
			case lkResponse:
				s.mode = mIgnore
				s.st.Responses++
				return
			case lkH2C:
				s.mode = mIgnore
				s.st.H2C++
				return
			default:
				s.mode = mIgnore
				s.st.NotHTTP++
				return
			}
		}
		if s.resync {
			if !s.sync(final) {
				return
			}
			continue
		}
		req, n, need, err := parse(s.buf)
		switch {
		case err == nil:
			s.st.Requests++
			s.emit(req, s.ts)
			s.buf = s.buf[n:]
			s.need = 0
		case errors.Is(err, errIncomplete):
			if len(s.buf) > MaxRequest {
				s.st.TooLarge++
				s.dropAndResync()
				continue
			}
			s.need = need
			return
		case errors.Is(err, errTooLarge):
			s.st.TooLarge++
			s.dropAndResync()
		default:
			s.dropAndResync()
		}
	}
}

func (s *Stream) dropAndResync() {
	// Skip one byte so the search does not find the same broken start.
	s.st.SkippedBytes++
	s.buf = s.buf[1:]
	s.need = 0
	s.resync = true
	s.st.Resyncs++
}

// sync drops bytes up to the next request line that parses (or may still
// parse). It returns false when it needs more data.
//
// A candidate starts at any method-like word, not only after a line break: a
// pipelined request follows its predecessor's body with no separator, so a
// capture that began inside that body finds the next request mid-line.
func (s *Stream) sync(final bool) bool {
	for i := 0; i < len(s.buf); i++ {
		if !isUpper(s.buf[i]) || i > 0 && isUpper(s.buf[i-1]) {
			continue
		}
		switch lineKind(s.buf[i:]) {
		case lkRequest:
			_, _, _, err := parse(s.buf[i:])
			if err == nil || (errors.Is(err, errIncomplete) && !final) {
				s.st.SkippedBytes += uint64(i)
				s.buf = s.buf[i:]
				s.resync = false
				if s.mode == mDetect {
					s.mode = mHTTP
				}
				return true
			}
		case lkNeed:
			if !final {
				s.keepFrom(i)
				return false
			}
		}
	}
	if final {
		s.st.SkippedBytes += uint64(len(s.buf))
		s.buf = s.buf[:0]
		return false
	}
	if len(s.buf) > resyncWindow {
		// Never cut inside an uppercase word: its tail would become a
		// candidate it never was, and the result would depend on when the
		// cut happened (on how the capture split the stream).
		cut := len(s.buf) - resyncWindow
		for cut < len(s.buf) && isUpper(s.buf[cut-1]) && isUpper(s.buf[cut]) {
			cut++
		}
		s.keepFrom(cut)
	}
	return false
}

func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }

func (s *Stream) keepFrom(i int) {
	if i == 0 {
		return
	}
	s.st.SkippedBytes += uint64(i)
	s.buf = append(s.buf[:0], s.buf[i:]...)
}

type lk uint8

const (
	lkNeed lk = iota
	lkRequest
	lkResponse
	lkH2C
	lkOther
)

// lineKind classifies the first line: "METHOD SP target SP HTTP/1.x".
func lineKind(b []byte) lk {
	if bytes.HasPrefix(b, []byte("PRI * HTTP/2.0")) {
		return lkH2C
	}
	if len(b) >= 5 && string(b[:5]) == "HTTP/" {
		return lkResponse
	}
	if len(b) < 5 && bytes.HasPrefix([]byte("HTTP/"), b) {
		return lkNeed
	}
	i := 0
	for i < len(b) && i < 32 && b[i] >= 'A' && b[i] <= 'Z' {
		i++
	}
	if i == len(b) {
		return lkNeed
	}
	if i == 0 || b[i] != ' ' {
		return lkOther
	}
	end := bytes.IndexByte(b, '\n')
	if end < 0 {
		if len(b) > 16<<10 {
			return lkOther
		}
		return lkNeed
	}
	line := bytes.TrimSuffix(b[:end], []byte("\r"))
	sp := bytes.LastIndexByte(line, ' ')
	if sp <= i {
		return lkOther
	}
	proto := line[sp+1:]
	if string(proto) != "HTTP/1.1" && string(proto) != "HTTP/1.0" {
		return lkOther
	}
	return lkRequest
}

var (
	errIncomplete = errors.New("incomplete request")
	// errTooLarge: the request declares more than MaxRequest. Refused from
	// the head, or the stream would buffer up to the declared length (or
	// the rest of the capture) before the size check ever ran.
	errTooLarge = errors.New("request too large")
)

// maxHead bounds a request head (net/http's own default is 1 MiB).
const maxHead = 1 << 20

// chunkedEnd returns the length of the chunked body at the start of b (the
// last chunk and the trailer included), errIncomplete when b ends before it
// does, or an error when it is malformed.
func chunkedEnd(b []byte) (int, error) {
	line := func(at int) (string, int, error) {
		i := bytes.IndexByte(b[at:], '\n')
		if i < 0 {
			if len(b)-at > 4096 {
				return "", 0, errors.New("chunked: line too long")
			}
			return "", 0, errIncomplete
		}
		return string(bytes.TrimSuffix(b[at:at+i], []byte("\r"))), at + i + 1, nil
	}
	at := 0
	for {
		l, next, err := line(at)
		if err != nil {
			return 0, err
		}
		if i := strings.IndexByte(l, ';'); i >= 0 {
			l = l[:i]
		}
		size, err := strconv.ParseUint(strings.TrimSpace(l), 16, 63)
		if err != nil {
			return 0, errors.New("chunked: bad size line")
		}
		at = next
		if size == 0 {
			for {
				l, next, err := line(at)
				if err != nil {
					return 0, err
				}
				at = next
				if l == "" {
					return at, nil
				}
			}
		}
		if size > MaxRequest {
			return 0, errors.New("chunked: chunk too large")
		}
		end := at + int(size)
		switch {
		case len(b) < end+1:
			return 0, errIncomplete
		case b[end] == '\n':
			at = end + 1
		case b[end] == '\r' && len(b) < end+2:
			return 0, errIncomplete
		case b[end] == '\r' && b[end+1] == '\n':
			at = end + 2
		default:
			return 0, errors.New("chunked: missing CRLF after data")
		}
	}
}

// headEnd returns the offset just past the blank line that ends the head,
// or -1.
func headEnd(b []byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] != '\n' {
			continue
		}
		if i+1 < len(b) && b[i+1] == '\n' {
			return i + 2
		}
		if i+2 < len(b) && b[i+1] == '\r' && b[i+2] == '\n' {
			return i + 3
		}
	}
	return -1
}

type counter struct {
	r *bytes.Reader
	n int
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// parse reads one request at the start of b. n is its length; need, on
// errIncomplete, is a lower bound for the buffer size worth retrying at.
func parse(b []byte) (req *Request, n, need int, err error) {
	// The head must be whole before net/http sees it: on a head cut in the
	// middle of a line its parser reports a malformed header, not "more
	// data", and the stream would drop a request that was only incomplete.
	if headEnd(b) < 0 {
		if len(b) > maxHead {
			return nil, 0, 0, errors.New("request head too large")
		}
		return nil, 0, len(b) + 1, errIncomplete
	}
	c := &counter{r: bytes.NewReader(b)}
	br := bufio.NewReaderSize(c, 4096)
	hr, err := http.ReadRequest(br)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, len(b) + 1, errIncomplete
		}
		return nil, 0, 0, err
	}
	head := c.n - br.Buffered()
	if hr.ContentLength > MaxRequest {
		return nil, 0, 0, errTooLarge
	}
	// A chunked body cut inside a size line or the trailer reads as
	// malformed to net/http, so its end is found here first.
	if len(hr.TransferEncoding) > 0 {
		if _, err := chunkedEnd(b[head:]); err != nil {
			if errors.Is(err, errIncomplete) {
				return nil, 0, len(b) + 1, errIncomplete
			}
			return nil, 0, 0, err
		}
	}
	body, err := io.ReadAll(hr.Body)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			need = len(b) + 1
			if hr.ContentLength > 0 {
				need = max(need, head+int(hr.ContentLength))
			}
			need = min(need, MaxRequest+1)
			return nil, 0, need, errIncomplete
		}
		return nil, 0, 0, err
	}
	n = c.n - br.Buffered()
	h := hr.Header.Clone()
	if hr.Host != "" {
		h.Set("Host", hr.Host)
	}
	// What net/http folded into fields is put back as headers, so the replay
	// can decide about each one.
	if len(hr.TransferEncoding) > 0 {
		h.Set("Transfer-Encoding", joinComma(hr.TransferEncoding))
	}
	return &Request{Method: hr.Method, URI: hr.RequestURI, Proto: hr.Proto, Header: h, Body: body}, n, 0, nil
}

func joinComma(v []string) string {
	var b bytes.Buffer
	for i, s := range v {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(s)
	}
	return b.String()
}
