package httpx

import (
	"errors"
	"net/http"
	"strings"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// The HTTP/2 side of the extractor (SPEC §6.8): golang.org/x/net/http2's
// Framer reads the frames, and its hpack Decoder, one per direction, keeps
// the dynamic table between header blocks.

// h2MaxTable bounds the dynamic table the client's encoder may announce: the
// server's SETTINGS (in the other direction, not read here) may have raised
// it above the default 4096.
const h2MaxTable = 1 << 20

// h2FrameHeader is the size of a frame header.
const h2FrameHeader = 9

// Bounds of one h2c direction (review of 2026-11-07, SPEC §6.8): without
// them a header block of 40 000 one-byte CONTINUATIONs took 30 s, and open
// streams of up to MaxRequest each had no ceiling at all.
const (
	// h2MaxHeaderBlock bounds a HEADERS and its CONTINUATIONs, frame
	// headers included: maxHead, the bound of an HTTP/1.x head.
	h2MaxHeaderBlock = maxHead
	// h2MaxStreams bounds the streams open at once; a stream opened past it
	// is not replayed (http2_streams_lost).
	h2MaxStreams = 1000
)

// h2MaxBuffered bounds the bodies held by the open streams of a direction
// together; the stream whose DATA would pass it is dropped
// (http2_streams_lost). A variable for the test only.
var h2MaxBuffered = 2 * MaxRequest

type h2stream struct {
	method, path, authority string
	fields                  [][2]string
	body                    []byte
	headers                 bool // the request head arrived
	tooLarge                bool
}

type h2conn struct {
	st      *Stats
	emit    func(*Request)
	src     frameSource
	fr      *http2.Framer
	streams map[uint32]*h2stream
	// scan is where wholeFrames stopped in the unit at the start of the
	// buffer: the next frame header to look at. Without it a block of many
	// small CONTINUATIONs arriving one by one was rescanned from its start
	// each time (quadratic).
	scan int
	// lastID is the highest stream opened (or refused): a client's stream
	// ids only grow, so a header block on a lower id that is not open is
	// the trailer of a stream already gone, not a new request.
	lastID   uint32
	buffered int // body bytes held by the open streams
}

// frameSource hands the Framer the bytes of whole frames only: ReadFrame is
// called once the frame (or the HEADERS and all its CONTINUATIONs) is in the
// buffer, so it never meets the end of the data in the middle of one.
type frameSource struct{ b []byte }

func (f *frameSource) Read(p []byte) (int, error) {
	if len(f.b) == 0 {
		return 0, errors.New("h2: read past the whole frames")
	}
	n := copy(p, f.b)
	f.b = f.b[n:]
	return n, nil
}

func newH2(st *Stats, emit func(*Request)) *h2conn {
	c := &h2conn{st: st, emit: emit, streams: map[uint32]*h2stream{}}
	c.fr = http2.NewFramer(nil, &c.src)
	c.fr.SetMaxReadFrameSize(1<<24 - 1)
	// The decoded fields too: a small block of indexes into a large dynamic
	// table expands far beyond its own size.
	c.fr.MaxHeaderListSize = maxHead
	dec := hpack.NewDecoder(4096, nil)
	dec.SetAllowedMaxDynamicTableSize(h2MaxTable)
	c.fr.ReadMetaHeaders = dec
	return c
}

// h2FramesScanned counts the frame headers wholeFrames looked at, for the
// test that proves a block in pieces is walked once
// (TestH2ContinuationChainLinear). A stream is parsed by one goroutine.
var h2FramesScanned int

// wholeFrames returns how many bytes at the start of b make a unit the
// Framer can read in one ReadFrame: one frame, or a HEADERS (PUSH_PROMISE)
// with all its CONTINUATIONs. 0 means more bytes are needed; -1 a header
// block past h2MaxHeaderBlock. *scan is the offset of the next frame header
// not yet looked at (0 for a new unit); it is kept across calls, so a unit
// arriving in pieces is walked once.
func wholeFrames(b []byte, scan *int) int {
	at := *scan
	for {
		h2FramesScanned++
		if len(b)-at < h2FrameHeader {
			*scan = at
			return 0
		}
		n := int(b[at])<<16 | int(b[at+1])<<8 | int(b[at+2])
		typ, flags := http2.FrameType(b[at+3]), http2.Flags(b[at+4])
		end := at + h2FrameHeader + n
		block := at == 0 && (typ == http2.FrameHeaders || typ == http2.FramePushPromise) && !flags.Has(http2.FlagHeadersEndHeaders)
		if (block || at > 0 && typ == http2.FrameContinuation) && end > h2MaxHeaderBlock {
			// Refused on the declared sizes, before the bytes arrive: the
			// same whether the block came whole or in pieces.
			*scan = 0
			return -1
		}
		if at > 0 && typ != http2.FrameContinuation {
			// A block not followed by its CONTINUATION: the Framer reads
			// the block and refuses the sequence.
			*scan = 0
			return at
		}
		if len(b) < end {
			*scan = at
			return 0
		}
		if at == 0 {
			if block {
				at = end
				continue
			}
			*scan = 0
			return end
		}
		if flags.Has(http2.FlagContinuationEndHeaders) {
			*scan = 0
			return end
		}
		at = end
	}
}

// feed reads what the buffer holds. It returns false when the direction can
// no longer be read (a frame or header block that does not decode).
func (c *h2conn) feed(s *Stream) bool {
	for {
		n := wholeFrames(s.buf, &c.scan)
		if n == 0 {
			return true
		}
		if n < 0 {
			// The HPACK state of the block is lost with it: the direction
			// ends, as for a block that does not decode.
			c.st.H2Errors++
			c.abort()
			return false
		}
		c.src.b = s.buf[:n]
		f, err := c.fr.ReadFrame()
		s.buf = s.buf[n:]
		if err != nil {
			var se http2.StreamError
			if errors.As(err, &se) {
				// A header block with invalid fields: that stream only.
				c.st.H2Errors++
				if _, ok := c.streams[se.StreamID]; ok {
					c.drop(se.StreamID)
					c.st.H2Lost++
				}
				continue
			}
			c.st.H2Errors++
			c.abort()
			return false
		}
		c.frame(f)
		if len(s.buf) == 0 {
			// Keep the backing array small between frames.
			s.buf = nil
		}
	}
}

func (c *h2conn) frame(f http2.Frame) {
	switch f := f.(type) {
	case *http2.MetaHeadersFrame:
		id := f.StreamID
		st := c.streams[id]
		if st == nil && id <= c.lastID {
			// The trailers of a stream already reset, refused or lost.
			return
		}
		if st == nil {
			c.lastID = id
			if f.Truncated {
				// Past MaxHeaderListSize: the fields are not all there.
				c.st.H2Errors++
				c.st.H2Lost++
				return
			}
			if len(c.streams) >= h2MaxStreams {
				c.st.H2Lost++
				return
			}
			st = &h2stream{method: f.PseudoValue("method"), path: f.PseudoValue("path"), authority: f.PseudoValue("authority"), headers: true}
			for _, hf := range f.RegularFields() {
				st.fields = append(st.fields, [2]string{hf.Name, hf.Value})
			}
			c.streams[id] = st
		}
		// A second block on a stream is its trailers: not replayed.
		if f.StreamEnded() {
			c.finish(id)
		}
	case *http2.DataFrame:
		st := c.streams[f.StreamID]
		if st == nil {
			return
		}
		if !st.tooLarge {
			switch {
			case len(st.body)+len(f.Data()) > MaxRequest:
				c.buffered -= len(st.body)
				st.tooLarge, st.body = true, nil
			case c.buffered+len(f.Data()) > h2MaxBuffered:
				// Too much held by the open streams together.
				c.drop(f.StreamID)
				c.st.H2Lost++
				return
			default:
				st.body = append(st.body, f.Data()...)
				c.buffered += len(f.Data())
			}
		}
		if f.StreamEnded() {
			c.finish(f.StreamID)
		}
	case *http2.RSTStreamFrame:
		if _, ok := c.streams[f.StreamID]; ok {
			c.drop(f.StreamID)
			c.st.H2Reset++
		}
	}
}

// drop forgets an open stream and the body it held.
func (c *h2conn) drop(id uint32) {
	if st := c.streams[id]; st != nil {
		c.buffered -= len(st.body)
		delete(c.streams, id)
	}
}

func (c *h2conn) finish(id uint32) {
	st := c.streams[id]
	c.drop(id)
	if st.tooLarge {
		c.st.TooLarge++
		return
	}
	r := &Request{Method: st.method, URI: st.path, Proto: "HTTP/2.0", Header: http.Header{}, Body: st.body}
	host := st.authority
	var cookies []string
	cookieAt := -1
	for _, kv := range st.fields {
		switch kv[0] {
		case "host":
			if host == "" {
				host = kv[1]
			}
			continue
		case "cookie":
			// RFC 9113 §8.2.3: crumbs split for compression are one field
			// again in HTTP/1.1.
			cookies = append(cookies, kv[1])
			if cookieAt >= 0 {
				continue
			}
			cookieAt = len(r.Fields)
		}
		r.Fields = append(r.Fields, kv)
	}
	if cookieAt >= 0 {
		r.Fields[cookieAt][1] = strings.Join(cookies, "; ")
	}
	for _, kv := range r.Fields {
		r.Header.Add(kv[0], kv[1])
	}
	if host != "" {
		r.Header.Set("Host", host)
	}
	c.st.H2Requests++
	c.emit(r)
}

// abort ends the direction: the streams still open are lost.
func (c *h2conn) abort() {
	c.st.H2Lost += uint64(len(c.streams))
	clear(c.streams)
	c.buffered = 0
}
