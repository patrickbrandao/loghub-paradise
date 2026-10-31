// Package extract finds the GELF documents inside captured traffic (SPEC
// §6): UDP datagrams (whole, or chunks reassembled) and their compression,
// the null-byte frames of a reassembled TCP stream, and the bodies of the
// HTTP POSTs of a stream (through package httpx).
//
// What every path emits is the GELF document itself: decompressed, without
// any transport framing. The sender applies the framing (and compression) of
// the chosen transport.
package extract

import (
	"net/netip"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/httpx"
)

// MaxDoc bounds one extracted document (decompressed); a larger one is
// dropped and counted. Far above the servers' 256 KiB, so the replay can
// still carry an oversized document to test their refusal.
const MaxDoc = 16 << 20

// Stats counts what the extractor did, over all datagrams and streams.
type Stats struct {
	Datagrams       uint64 // UDP datagrams taken as GELF (unchunked)
	DatagramsSkip   uint64 // UDP datagrams that do not look like GELF
	Chunked         uint64 // messages rebuilt from chunks
	Compressed      [4]uint64
	DecompressError uint64 // payloads that did not inflate
	TooLarge        uint64 // documents above MaxDoc

	StreamsGELF    uint64 // TCP directions read as null-framed GELF
	StreamsHTTP    uint64 // TCP directions handed to the HTTP extractor
	StreamsIgnored uint64 // anything else (server directions, other protocols)
	Frames         uint64 // documents from null-framed streams
	FramesNotGELF  uint64 // strict: frames that do not start with "{"
	Resyncs        uint64
	SkippedBytes   uint64
	LostFrames     uint64 // frames cut by a gap or left unterminated at the end

	HTTPDocs    uint64 // documents from HTTP POST bodies
	HTTPSkipped uint64 // requests that were not a POST with a GELF body

	HTTP httpx.Stats
}

// Doc is one extracted document.
type Doc struct {
	Data   []byte
	TS     time.Time
	Chunks int // UDP chunks it came in (0: one plain datagram or a stream)
}

// isSpace: JSON's insignificant whitespace.
func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

func trim(b []byte) []byte {
	for len(b) > 0 && isSpace(b[0]) {
		b = b[1:]
	}
	for len(b) > 0 && isSpace(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	return b
}

// looksGELF is the strict content check: a GELF document is a JSON object.
func looksGELF(b []byte) bool { return len(b) > 0 && b[0] == '{' }

// payload decompresses a UDP payload or an HTTP body (by its first bytes, or
// by hint when the HTTP header names it) and applies the content check.
func payload(b []byte, hint gelf.Compression, strict bool, st *Stats) ([]byte, bool) {
	c := hint
	if c == gelf.None {
		c = gelf.Detect(b)
	}
	if c != gelf.None {
		out, err := gelf.Decompress(b, c, MaxDoc)
		if err == gelf.ErrTooLarge {
			st.TooLarge++
			return nil, false
		}
		if err != nil && hint == gelf.Zlib {
			// "Content-Encoding: deflate" is zlib by RFC 9110, but some
			// clients send raw deflate.
			out, err = gelf.Decompress(b, gelf.Deflate, MaxDoc)
			c = gelf.Deflate
		}
		if err != nil {
			st.DecompressError++
			return nil, false
		}
		st.Compressed[c]++
		b = out
	} else if len(b) > MaxDoc {
		st.TooLarge++
		return nil, false
	}
	b = trim(b)
	if len(b) == 0 || strict && !looksGELF(b) {
		return nil, false
	}
	return b, true
}

// Datagrams extracts GELF from UDP datagrams, reassembling chunks.
type Datagrams struct {
	St     *Stats
	Strict bool
	R      *gelf.Reassembler
}

// Add takes one datagram; ok when it completes a document.
func (d *Datagrams) Add(src netip.AddrPort, b []byte, ts time.Time) (Doc, bool) {
	chunks := 0
	if gelf.IsChunk(b) {
		msg, n, ok := d.R.Add(src, b, ts)
		if !ok {
			return Doc{}, false
		}
		b, chunks = msg, n
	}
	out, ok := payload(b, gelf.None, d.Strict, d.St)
	if !ok {
		d.St.DatagramsSkip++
		return Doc{}, false
	}
	if chunks > 0 {
		d.St.Chunked++
	} else {
		d.St.Datagrams++
	}
	return Doc{Data: append([]byte(nil), out...), TS: ts, Chunks: chunks}, true
}

// HTTPBody turns a captured request into a document: a POST with a body,
// decoded by its Content-Encoding (or by its first bytes). Strict (no
// --pcap-port) also requires the path of a GELF HTTP input, "/gelf" (or
// ".../gelf" behind a prefix): a JSON webhook, an Elasticsearch _bulk or any
// API in the same capture would otherwise replay as GELF (SPEC §6.5).
func HTTPBody(r *httpx.Request, strict bool, st *Stats) ([]byte, bool) {
	if r.Method != "POST" || len(r.Body) == 0 || strict && !gelfPath(r.URI) {
		st.HTTPSkipped++
		return nil, false
	}
	hint := gelf.None
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))) {
	case "gzip", "x-gzip":
		hint = gelf.Gzip
	case "deflate":
		hint = gelf.Zlib
	}
	out, ok := payload(r.Body, hint, strict, st)
	if !ok {
		st.HTTPSkipped++
		return nil, false
	}
	st.HTTPDocs++
	return append([]byte(nil), out...), true
}

// gelfPath says whether a request-target (origin or absolute form) names a
// GELF HTTP input: its path, without the query, is "/gelf" or ends in it.
func gelfPath(uri string) bool {
	if i := strings.Index(uri, "://"); i >= 0 {
		uri = uri[i+3:]
		if j := strings.IndexByte(uri, '/'); j >= 0 {
			uri = uri[j:]
		} else {
			return false
		}
	}
	if i := strings.IndexAny(uri, "?#"); i >= 0 {
		uri = uri[:i]
	}
	return strings.HasSuffix(uri, "/gelf")
}
