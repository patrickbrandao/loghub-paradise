package httpx

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// Regressions of the review of 2026-11-07 (SPEC §6.6, §6.8).

// continuations writes the header block of one request on stream id as a
// HEADERS with the first byte, then one CONTINUATION per byte.
func continuations(c *h2, id uint32, value string) {
	c.hb.Reset()
	for _, kv := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":authority", "h"}, {":path", "/big"}, {"x-big", value}} {
		c.enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]})
	}
	blk := append([]byte(nil), c.hb.Bytes()...)
	c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: blk[:1], EndStream: true})
	for i := 1; i < len(blk); i++ {
		c.fr.WriteContinuation(id, i == len(blk)-1, blk[i:i+1])
	}
}

// A header block in 40 000 one-byte CONTINUATIONs, each in its own segment,
// is walked once: 29.9 s before (wholeFrames rescanned the block from its
// start at every segment), well under a second now.
func TestH2ContinuationChainLinear(t *testing.T) {
	c := newH2Client(true)
	continuations(c, 1, strings.Repeat("v", 40000))
	in := c.buf.Bytes()
	var g full
	s := g.stream(false)
	h2FramesScanned = 0
	feedB(s, in, 10) // one frame per segment
	// Each frame header looked at about twice (once short, once whole);
	// rescanning from the start of the block was ~40 000²/2.
	if frames := len(in) / 10; h2FramesScanned > 3*frames {
		t.Fatalf("%d frame headers scanned for %d frames", h2FramesScanned, frames)
	}
	if len(g.reqs) != 1 || g.reqs[0].URI != "/big" || len(g.reqs[0].Header.Get("X-Big")) != 40000 {
		t.Fatalf("%+v", g.st)
	}
}

// A header block past 1 MiB ends the direction (http2_errors; the streams
// open are lost), whether it comes as many CONTINUATIONs or one declared
// too large, and before its bytes arrive.
func TestH2HeaderBlockCapped(t *testing.T) {
	c := newH2Client(true)
	c.headers(1, false, false, ":method", "POST", ":scheme", "http", ":authority", "h", ":path", "/open")
	continuations(c, 3, strings.Repeat("v", 120000)) // 1.2 MB of frames
	var g full
	feedB(g.stream(false), c.buf.Bytes(), 10)
	if g.st.H2Errors != 1 || g.st.H2Lost != 1 || len(g.reqs) != 0 {
		t.Fatalf("%+v", g.st)
	}
	// One CONTINUATION declaring 2 MiB: refused at its frame header.
	c = newH2Client(true)
	c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte{0x82}})
	hdr := []byte{0x20, 0, 0, byte(http2.FrameContinuation), 0x4, 0, 0, 0, 1}
	g = full{}
	s := g.stream(false)
	s.Data(append(c.buf.Bytes(), hdr...), time.Time{})
	if g.st.H2Errors != 1 || s.mode != mIgnore {
		t.Fatalf("%+v", g.st)
	}
}

// At most 1000 streams open at once; one past it is lost, and its later
// frames (data, trailers) are not taken for a new request.
func TestH2StreamsCapped(t *testing.T) {
	c := newH2Client(true)
	n := h2MaxStreams + 1
	for i := 0; i < n; i++ {
		c.headers(uint32(2*i+1), false, false, ":method", "POST", ":scheme", "http", ":authority", "h", ":path", "/s")
	}
	for i := 0; i < n; i++ {
		c.fr.WriteData(uint32(2*i+1), false, []byte("x"))
		c.headers(uint32(2*i+1), true, false, "x-trailer", "t")
	}
	var g full
	feedB(g.stream(false), c.buf.Bytes(), 4096)
	if len(g.reqs) != h2MaxStreams || g.st.H2Lost != 1 || g.st.H2Requests != h2MaxStreams {
		t.Fatalf("%d requests, %+v", len(g.reqs), g.st)
	}
	for _, r := range g.reqs {
		if r.Method != "POST" || string(r.Body) != "x" {
			t.Fatalf("%s %q", r.Method, r.Body)
		}
	}
}

// The bodies held by the open streams together are bounded: the stream
// whose DATA would pass the bound is lost, the others are replayed.
func TestH2BufferedCapped(t *testing.T) {
	defer func(v int) { h2MaxBuffered = v }(h2MaxBuffered)
	h2MaxBuffered = 1000
	c := newH2Client(true)
	for _, id := range []uint32{1, 3, 5} {
		c.headers(id, false, false, ":method", "POST", ":scheme", "http", ":authority", "h", ":path", "/b")
	}
	c.fr.WriteData(1, false, bytes.Repeat([]byte("a"), 400))
	c.fr.WriteData(3, false, bytes.Repeat([]byte("b"), 400))
	c.fr.WriteData(5, false, bytes.Repeat([]byte("c"), 400)) // 1200 > 1000: stream 5 lost
	c.fr.WriteData(1, true, []byte("!"))
	c.fr.WriteData(3, true, nil)
	c.fr.WriteData(5, true, []byte("?"))
	var g full
	feedB(g.stream(false), c.buf.Bytes(), 100)
	if len(g.reqs) != 2 || g.st.H2Lost != 1 || len(g.reqs[0].Body) != 401 || len(g.reqs[1].Body) != 400 {
		t.Fatalf("%d requests, %+v", len(g.reqs), g.st)
	}
}

// The resynchronization is linear (review of 2026-11-07): 1 MiB without an
// LF, a candidate every two bytes, took 7.7 s (3 MiB, 34.6 s), because every
// candidate searched for the LF again to the end of the buffer. The request
// after it is still found.
func TestResyncLinear(t *testing.T) {
	for _, tail := range []string{"", "\r\n"} {
		in := strings.Repeat("A ", 1536<<10/2) + tail + "POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n\r\nZ"
		for _, chunk := range []int{4096, len(in)} {
			var g full
			lfScanned, headParses = 0, 0
			feedB(g.stream(true), []byte(in), chunk)
			if len(g.reqs) != 1 || g.reqs[0].URI != "/x" {
				t.Fatalf("tail %q chunk %d: %d requests %+v", tail, chunk, len(g.reqs), g.st)
			}
			if headParses > 4 {
				t.Fatalf("tail %q chunk %d: %d heads parsed", tail, chunk, headParses)
			}
			if lfScanned > 8*len(in) {
				t.Fatalf("tail %q chunk %d: %d bytes searched for %d", tail, chunk, lfScanned, len(in))
			}
		}
	}
}
