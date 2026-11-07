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
	dec := hpack.NewDecoder(4096, nil)
	dec.SetAllowedMaxDynamicTableSize(h2MaxTable)
	c.fr.ReadMetaHeaders = dec
	return c
}

// wholeFrames returns how many bytes at the start of b make a unit the
// Framer can read in one ReadFrame: one frame, or a HEADERS (PUSH_PROMISE)
// with all its CONTINUATIONs. 0 means more bytes are needed.
func wholeFrames(b []byte) int {
	at := 0
	for {
		if len(b)-at < h2FrameHeader {
			return 0
		}
		n := int(b[at])<<16 | int(b[at+1])<<8 | int(b[at+2])
		typ, flags := http2.FrameType(b[at+3]), http2.Flags(b[at+4])
		end := at + h2FrameHeader + n
		if len(b) < end {
			return 0
		}
		if at == 0 {
			if (typ == http2.FrameHeaders || typ == http2.FramePushPromise) && !flags.Has(http2.FlagHeadersEndHeaders) {
				at = end
				continue
			}
			return end
		}
		if typ != http2.FrameContinuation {
			// A block not followed by its CONTINUATION: the Framer reads
			// the block and refuses the sequence.
			return at
		}
		if flags.Has(http2.FlagContinuationEndHeaders) {
			return end
		}
		at = end
	}
}

// feed reads what the buffer holds. It returns false when the direction can
// no longer be read (a frame or header block that does not decode).
func (c *h2conn) feed(s *Stream) bool {
	for {
		n := wholeFrames(s.buf)
		if n == 0 {
			return true
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
					delete(c.streams, se.StreamID)
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
		if st == nil {
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
			if len(st.body)+len(f.Data()) > MaxRequest {
				st.tooLarge, st.body = true, nil
			} else {
				st.body = append(st.body, f.Data()...)
			}
		}
		if f.StreamEnded() {
			c.finish(f.StreamID)
		}
	case *http2.RSTStreamFrame:
		if _, ok := c.streams[f.StreamID]; ok {
			delete(c.streams, f.StreamID)
			c.st.H2Reset++
		}
	}
}

func (c *h2conn) finish(id uint32) {
	st := c.streams[id]
	delete(c.streams, id)
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
}
