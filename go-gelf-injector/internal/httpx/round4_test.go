package httpx

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The rules ported from go-webhook-injector in the review of 2026-11-07
// (alignment 8(f), SPEC §6.5): the same tests as there.

type full struct {
	reqs []*Request
	st   Stats
}

func (g *full) stream(mid bool) *Stream {
	return NewStream(mid, &g.st, func(r *Request, _ time.Time) { g.reqs = append(g.reqs, r) })
}

func feedB(s *Stream, in []byte, chunk int) {
	for off := 0; off < len(in); off += chunk {
		s.Data(in[off:min(off+chunk, len(in))], time.Time{})
	}
	s.End()
}

// A candidate is refused the same way whole or in pieces: a 20 KiB request
// line held the decision only up to 16 KiB before, so in pieces the request
// was lost; whole, it was found.
func TestLargeCandidateSplitInvariant(t *testing.T) {
	long := "POST /" + strings.Repeat("a", 20<<10) + " HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nZ"
	for _, mid := range []bool{false, true} {
		in := long
		if mid {
			in = "tail of a body " + long
		}
		var whole, pieces full
		feedB(whole.stream(mid), []byte(in), len(in))
		feedB(pieces.stream(mid), []byte(in), 1460)
		if len(whole.reqs) != 1 || len(pieces.reqs) != 1 {
			t.Fatalf("mid %v: whole %d, pieces %d requests", mid, len(whole.reqs), len(pieces.reqs))
		}
	}
	huge := "POST /h HTTP/1.1\r\nX-Big: " + strings.Repeat("b", 1<<20) + "\r\n\r\n" + reqB
	var whole, pieces full
	feedB(whole.stream(false), []byte(huge), len(huge))
	feedB(pieces.stream(false), []byte(huge), 1460)
	if len(whole.reqs) != 1 || len(pieces.reqs) != 1 || whole.reqs[0].URI != "/b" || pieces.reqs[0].URI != "/b" {
		t.Fatalf("huge head: whole %d, pieces %d", len(whole.reqs), len(pieces.reqs))
	}
}

// A large chunked body arriving in segments is scanned once.
func TestChunkedIncremental(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("POST /gelf HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n")
	const chunks = 4000
	for range chunks {
		fmt.Fprintf(&b, "400\r\n%s\r\n", strings.Repeat("d", 0x400))
	}
	b.WriteString("0\r\n\r\n")
	chunkLines = 0
	var g full
	feedB(g.stream(false), b.Bytes(), 1460)
	if len(g.reqs) != 1 || len(g.reqs[0].Body) != chunks*0x400 {
		t.Fatalf("%d requests", len(g.reqs))
	}
	if chunkLines > 3*chunks {
		t.Fatalf("%d chunk lines read for %d chunks: the body is rescanned", chunkLines, chunks)
	}
}

// The resynchronization is linear (pcap/replay item 5 of the review).
func TestResyncLinear(t *testing.T) {
	for _, tail := range []string{"", "\r\n"} {
		in := strings.Repeat("A ", 1536<<10/2) + tail + "POST /gelf HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}"
		for _, chunk := range []int{4096, len(in)} {
			var g full
			lfScanned, headParses = 0, 0
			feedB(g.stream(true), []byte(in), chunk)
			if len(g.reqs) != 1 || g.reqs[0].URI != "/gelf" {
				t.Fatalf("tail %q chunk %d: %d requests %+v", tail, chunk, len(g.reqs), g.st)
			}
			if headParses > 4 || lfScanned > 8*len(in) {
				t.Fatalf("tail %q chunk %d: %d heads parsed, %d bytes searched for %d", tail, chunk, headParses, lfScanned, len(in))
			}
		}
	}
}
