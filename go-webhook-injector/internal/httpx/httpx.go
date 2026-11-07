// Package httpx finds the HTTP requests inside a reassembled TCP stream
// (SPEC §6.6, §6.8): pipelined HTTP/1.x requests one after the other, bodies
// by Content-Length or chunked, resynchronization on the next request line
// after a capture gap or from a capture that began inside a connection, and
// the streams of a cleartext HTTP/2 (h2c) connection.
//
// net/http validates an HTTP/1.x head (http.ReadRequest), so a request is
// extracted exactly when the server's own parser would read one; the fields
// are then read again by an own parser that keeps their order (SPEC §5.5).
package httpx

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"
)

// Request is one captured request.
type Request struct {
	Method string
	URI    string // the request-target as captured (path and query)
	Proto  string
	// Header is the canonical map (Host and Transfer-Encoding put back).
	Header http.Header
	// Fields are the header fields in capture order, the names as written
	// (lowercase for HTTP/2), one entry per field line (SPEC §5.5).
	Fields [][2]string
	Body   []byte
}

// MaxRequest bounds one request (head and body); a larger one is dropped and
// the stream resynchronizes.
const MaxRequest = 64 << 20

// resyncWindow: a resynchronizing stream with no candidate keeps this tail.
const resyncWindow = 64

// Stats counts what the extractor did over all streams.
type Stats struct {
	Requests     uint64 // HTTP/1.x requests
	HTTPStreams  uint64 // client directions that carried HTTP/1.x
	Responses    uint64 // server directions (HTTP/1.x responses), ignored
	H2C          uint64 // h2c directions (prior knowledge or upgrade), replayed
	NotHTTP      uint64 // anything else
	Resyncs      uint64
	SkippedBytes uint64
	Lost         uint64 // requests cut by a gap or by the end of the stream
	TooLarge     uint64

	H2Requests uint64 // HTTP/2 streams replayed
	H2Reset    uint64 // streams the client reset (RST_STREAM)
	H2Lost     uint64 // streams open when the direction ended or broke
	H2Errors   uint64 // frames or header blocks that could not be decoded
}

type mode uint8

const (
	mDetect mode = iota
	mHTTP
	mH2
	mIgnore
)

// partial remembers, for the request at the start of the buffer, how far the
// search has gone, so the next segment continues from there instead of
// rescanning the buffer (SPEC §6.6: the quadratic cost of a large chunked
// body arriving slowly). It is reset whenever the start of the buffer moves.
type partial struct {
	lineScan int           // the request line has no LF before this offset
	headScan int           // the head has no blank line before this offset
	head     int           // > 0: the head ends here and was validated
	hr       *http.Request // the validated head
	fields   [][2]string
	chunked  bool
	cl       int // Content-Length (not chunked)
	chunkAt  int // chunked: offset, from head, of the next unproven size line
}

// Stream implements pcap.StreamHandler for one direction.
type Stream struct {
	st     *Stats
	emit   func(*Request, time.Time)
	mode   mode
	resync bool
	buf    []byte
	need   int // do not reparse before the buffer has this many bytes
	p      partial
	ts     time.Time
	ended  bool
	h2     *h2conn
	// midWord: the buffer was cut inside an uppercase word, so its first
	// byte continues that word and is no candidate (found by the fuzz of
	// go-gelf-injector, 2026-11-07: a method longer than resyncWindow split
	// in 1-byte segments was cut to its tail, which then parsed as a
	// request the whole stream never had; SPEC §6.6).
	midWord bool
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
	switch s.mode {
	case mIgnore:
		return
	case mH2:
		// The HPACK state is lost with the missing bytes: no later header
		// block can be decoded safely (SPEC §6.8).
		s.h2.abort()
		s.buf = nil
		s.mode = mIgnore
		return
	}
	if len(s.buf) > 0 {
		s.st.Lost++
		s.st.SkippedBytes += uint64(len(s.buf))
		s.buf = s.buf[:0]
	}
	s.need, s.midWord = 0, false
	s.p = partial{}
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
		if s.mode == mH2 {
			s.h2.abort()
			s.buf = nil
		} else if len(s.buf) > 0 {
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

// h2Preface is what an h2c client sends first (RFC 9113 §3.4).
const h2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

// prefaceKind says whether b starts with the h2c preface: 1 yes, 0 no, -1
// not known yet (b is a proper prefix of it).
func prefaceKind(b []byte) int {
	if len(b) >= len(h2Preface) {
		if string(b[:len(h2Preface)]) == h2Preface {
			return 1
		}
		return 0
	}
	if string(b) == h2Preface[:len(b)] {
		return -1
	}
	return 0
}

func (s *Stream) startH2() {
	s.st.H2C++
	s.mode = mH2
	s.buf = s.buf[len(h2Preface):]
	s.h2 = newH2(s.st, func(r *Request) { s.emit(r, s.ts) })
}

// shift drops n bytes from the start of the buffer and forgets the search
// state of the request that was there.
func (s *Stream) shift(n int) {
	s.buf = s.buf[n:]
	s.need = 0
	s.p = partial{}
}

func (s *Stream) run(final bool) {
	for len(s.buf) > 0 && s.mode != mIgnore {
		if s.mode == mH2 {
			if !s.h2.feed(s) {
				s.mode = mIgnore
				s.buf = nil
			}
			return
		}
		if s.need > 0 && len(s.buf) < s.need && !final {
			return
		}
		if s.mode == mDetect && !s.resync {
			switch kind := lineKind(s.buf, &s.p.lineScan); kind {
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
				switch prefaceKind(s.buf) {
				case 1:
					s.startH2()
					continue
				case -1:
					if !final {
						return
					}
				}
				s.mode = mIgnore
				s.st.NotHTTP++
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
		// An h2c upgrade: after the HTTP/1.1 request that asked for it, the
		// client sends the preface and speaks HTTP/2 (SPEC §6.8).
		switch prefaceKind(s.buf) {
		case 1:
			s.startH2()
			continue
		case -1:
			if !final {
				return
			}
		}
		req, n, need, err := parse(s.buf, &s.p)
		switch {
		case err == nil:
			s.st.Requests++
			s.emit(req, s.ts)
			s.shift(n)
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
	s.shift(1)
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
	// lf is the next LF at or after the candidate (len(buf) when there is
	// none), blank the LF that starts the next blank line (len(buf): none
	// up to the end). Every candidate between two LFs shares them, so the
	// search for each is not redone from every candidate: 1 MiB without an
	// LF, a candidate every few bytes, was quadratic (7.7 s; 3 MiB 34.6 s;
	// review of 2026-11-07).
	lf, blank := -1, -1
	for i := 0; i < len(s.buf); i++ {
		if !isUpper(s.buf[i]) || i > 0 && isUpper(s.buf[i-1]) || i == 0 && s.midWord {
			continue
		}
		// The candidate at the start keeps its search state; any other one
		// starts fresh, from the shared LF positions.
		var p partial
		if i == 0 {
			p = s.p
		}
		if lf < i {
			// The candidate at the start already searched up to its scan.
			from := i + min(p.lineScan, len(s.buf)-i)
			if j := bytes.IndexByte(s.buf[from:], '\n'); j >= 0 {
				lf = from + j
				lfScanned += j + 1
			} else {
				lf = len(s.buf)
				lfScanned += len(s.buf) - from
			}
		}
		p.lineScan = max(p.lineScan, lf-i)
		switch lineKind(s.buf[i:], &p.lineScan) {
		case lkRequest:
			if blank < i {
				if j := headEnd(s.buf, i); j >= 0 {
					blank = j - 2 // the LF of "\n\n" or of "\n\r\n"
					if s.buf[blank] != '\n' {
						blank--
					}
				} else {
					blank = len(s.buf)
				}
			}
			if p.head == 0 {
				// headEnd keeps the last three bytes in view, as parse does.
				p.headScan = max(p.headScan, min(blank, len(s.buf)-3)-i)
			}
			_, _, need, err := parse(s.buf[i:], &p)
			if err == nil || (errors.Is(err, errIncomplete) && !final) {
				s.st.SkippedBytes += uint64(i)
				s.buf = s.buf[i:]
				s.p = p
				s.need = 0
				if err != nil {
					s.need = need
				}
				s.resync, s.midWord = false, false
				if s.mode == mDetect {
					s.mode = mHTTP
				}
				return true
			}
		case lkNeed:
			if !final {
				s.keepFrom(i)
				s.p = p
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
		mid := cut == len(s.buf) && isUpper(s.buf[cut-1])
		s.keepFrom(cut)
		s.midWord = mid
	}
	return false
}

func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }

func (s *Stream) keepFrom(i int) {
	if i == 0 {
		return
	}
	s.midWord = false
	s.st.SkippedBytes += uint64(i)
	s.buf = append(s.buf[:0], s.buf[i:]...)
	s.p = partial{}
}

type lk uint8

const (
	lkNeed lk = iota
	lkRequest
	lkResponse
	lkH2C
	lkOther
)

// lfScanned counts the bytes searched for the LF of a request line, for the
// test that proves the resynchronization is linear (TestResyncLinear). A
// stream is parsed by one goroutine, the source's.
var lfScanned int

// headParses counts the heads given to net/http, for the same test: the
// candidates inside one long line must not each parse it.
var headParses int

// lineKind classifies the first line: "METHOD SP target SP HTTP/1.x". scan
// remembers how far the LF was searched for, so a long line arriving in
// pieces is not rescanned.
//
// A line without its LF holds the decision up to maxHead, the same bound the
// head has in parse: the limit must refuse a candidate the same way whether
// it arrived whole or in pieces (SPEC §6.6; 16 KiB here made a 20 KiB line
// accepted in one segment and refused in pieces).
func lineKind(b []byte, scan *int) lk {
	if bytes.HasPrefix(b, []byte("PRI * HTTP/2.0")) || len(b) < 14 && bytes.HasPrefix([]byte("PRI * HTTP/2.0"), b) {
		if len(b) < 14 {
			return lkNeed
		}
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
	from := min(*scan, len(b))
	end := bytes.IndexByte(b[from:], '\n')
	if end < 0 {
		lfScanned += len(b) - from
		*scan = len(b)
		if len(b) > maxHead {
			return lkOther
		}
		return lkNeed
	}
	lfScanned += end + 1
	end += from
	if end > maxHead {
		return lkOther
	}
	line := bytes.TrimSuffix(b[:end], []byte("\r"))
	// The version is the word after the last space: the line ends in
	// " HTTP/1.x". Looked at from the end only -- a search for the last
	// space ran over the whole line for each candidate inside it
	// (quadratic resynchronization, review of 2026-11-07).
	const v = len(" HTTP/1.1")
	if len(line) < v || len(line)-v <= i || line[len(line)-v] != ' ' {
		return lkOther
	}
	if proto := string(line[len(line)-v+1:]); proto != "HTTP/1.1" && proto != "HTTP/1.0" {
		return lkOther
	}
	// One target between the method and the version, with no space: net/http
	// refuses any other line. Searched from the method on, the search stops
	// at the next word, so the candidates of one long line ("A A A ... /x
	// HTTP/1.1") cost their own words only, not a parse of the line each.
	j := bytes.IndexByte(line[i+1:], ' ')
	if j < 0 {
		lfScanned += len(line) - i - 1
		return lkOther
	}
	lfScanned += j + 1
	if i+1+j != len(line)-v || j == 0 {
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

// maxChunkLine bounds a chunk size line or a trailer line.
const maxChunkLine = 4096

// chunkLines counts the size and trailer lines read, for the test that
// proves the search is incremental (TestChunkedIncremental). A stream is
// parsed by one goroutine, the source's.
var chunkLines int

// chunkedEnd finds the end of the chunked body at the start of b (the last
// chunk and the trailer included), resuming at offset at, the start of a
// size line already known to begin there. On errIncomplete it returns where
// to resume (the size line that could not be completed) and need, the length
// b must reach before a retry can make progress: the data of a chunk is
// never read, only jumped over.
func chunkedEnd(b []byte, at int) (end, resume, need int, err error) {
	line := func(from int) (string, int, error) {
		chunkLines++
		i := bytes.IndexByte(b[from:], '\n')
		if i < 0 {
			if len(b)-from > maxChunkLine {
				return "", 0, errors.New("chunked: line too long")
			}
			return "", 0, errIncomplete
		}
		// The same bound whether the line came whole or in pieces.
		if i > maxChunkLine {
			return "", 0, errors.New("chunked: line too long")
		}
		return string(bytes.TrimSuffix(b[from:from+i], []byte("\r"))), from + i + 1, nil
	}
	for {
		l, next, err := line(at)
		if err != nil {
			return 0, at, len(b) + 1, err
		}
		if i := strings.IndexByte(l, ';'); i >= 0 {
			l = l[:i]
		}
		size, err := strconv.ParseUint(strings.TrimSpace(l), 16, 63)
		if err != nil {
			return 0, at, 0, errors.New("chunked: bad size line")
		}
		if size == 0 {
			t := next
			for {
				l, n, err := line(t)
				if err != nil {
					return 0, at, len(b) + 1, err
				}
				t = n
				if l == "" {
					return t, at, 0, nil
				}
			}
		}
		if size > MaxRequest {
			return 0, at, 0, errors.New("chunked: chunk too large")
		}
		e := next + int(size)
		switch {
		case len(b) < e+1:
			return 0, at, e + 1, errIncomplete
		case b[e] == '\n':
			at = e + 1
		case b[e] == '\r' && len(b) < e+2:
			return 0, at, e + 2, errIncomplete
		case b[e] == '\r' && b[e+1] == '\n':
			at = e + 2
		default:
			return 0, at, 0, errors.New("chunked: missing CRLF after data")
		}
	}
}

// headEnd returns the offset just past the blank line that ends the head,
// or -1. The search starts at from (no end before it).
func headEnd(b []byte, from int) int {
	for i := max(from, 0); i < len(b); i++ {
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

// parse reads one request at the start of b, resuming the search recorded
// in p. n is its length; need, on errIncomplete, is a lower bound for the
// buffer size worth retrying at.
func parse(b []byte, p *partial) (req *Request, n, need int, err error) {
	if p.head == 0 {
		// The head must be whole before net/http sees it: on a head cut in
		// the middle of a line its parser reports a malformed header, not
		// "more data", and the stream would drop a request that was only
		// incomplete.
		h := headEnd(b, p.headScan)
		if h < 0 {
			if len(b) > maxHead {
				return nil, 0, 0, errors.New("request head too large")
			}
			// The last three bytes may start the blank line.
			p.headScan = max(len(b)-3, 0)
			return nil, 0, len(b) + 1, errIncomplete
		}
		if h > maxHead {
			// Refused whole as it would be in pieces (SPEC §6.6).
			return nil, 0, 0, errors.New("request head too large")
		}
		headParses++
		hr, err := http.ReadRequest(bufio.NewReaderSize(bytes.NewReader(b[:h]), 4096))
		if err != nil {
			return nil, 0, 0, err
		}
		if hr.ContentLength > MaxRequest {
			return nil, 0, 0, errTooLarge
		}
		p.head, p.hr = h, hr
		p.fields = headerFields(b[:h])
		p.chunked = len(hr.TransferEncoding) > 0
		p.cl = int(max(hr.ContentLength, 0))
		p.chunkAt = 0
	}
	h := p.head
	var body []byte
	if p.chunked {
		// A chunked body cut inside a size line or the trailer reads as
		// malformed to net/http, so its end is found here first.
		end, at, need, err := chunkedEnd(b[h:], p.chunkAt)
		if err != nil {
			if errors.Is(err, errIncomplete) {
				p.chunkAt = at
				return nil, 0, min(h+need, MaxRequest+1), errIncomplete
			}
			return nil, 0, 0, err
		}
		body, err = io.ReadAll(httputil.NewChunkedReader(bytes.NewReader(b[h : h+end])))
		if err != nil {
			return nil, 0, 0, err
		}
		n = h + end
	} else {
		if len(b) < h+p.cl {
			return nil, 0, min(h+p.cl, MaxRequest+1), errIncomplete
		}
		body = append([]byte(nil), b[h:h+p.cl]...)
		n = h + p.cl
	}
	hr := p.hr
	hdr := hr.Header.Clone()
	if hr.Host != "" {
		hdr.Set("Host", hr.Host)
	}
	// What net/http folded into fields is put back as headers, so the replay
	// can decide about each one.
	if len(hr.TransferEncoding) > 0 {
		hdr.Set("Transfer-Encoding", strings.Join(hr.TransferEncoding, ", "))
	}
	return &Request{Method: hr.Method, URI: hr.RequestURI, Proto: hr.Proto, Header: hdr, Fields: p.fields, Body: body}, n, 0, nil
}

// headerFields reads the field lines of a head already validated by
// net/http, in order and with the names as written. An obs-fold line is
// joined to the previous value with a space, as net/http does.
func headerFields(head []byte) [][2]string {
	var out [][2]string
	lines := bytes.Split(head, []byte("\n"))
	for _, l := range lines[1:] {
		l = bytes.TrimSuffix(l, []byte("\r"))
		if len(l) == 0 {
			break
		}
		if l[0] == ' ' || l[0] == '\t' {
			if len(out) > 0 {
				v := strings.Trim(string(l), " \t")
				if out[len(out)-1][1] == "" {
					out[len(out)-1][1] = v
				} else if v != "" {
					out[len(out)-1][1] += " " + v
				}
			}
			continue
		}
		name, value, ok := bytes.Cut(l, []byte(":"))
		if !ok {
			continue
		}
		out = append(out, [2]string{string(name), strings.Trim(string(value), " \t")})
	}
	return out
}
