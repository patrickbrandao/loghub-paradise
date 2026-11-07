package httpx

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// Regressions of the round of 2026-11-07 (SPEC §5.5, §6.6, §6.8).

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

// The fields come in capture order, with the names as written, repeated
// fields one per line, obs-fold joined (SPEC §5.5). The map of net/http
// gave them sorted and canonicalized.
func TestFieldsKeepOrderAndCase(t *testing.T) {
	in := "POST /o HTTP/1.1\r\nhost: h\r\nX-Zeta: 1\r\ncontent-type: text/plain\r\nX-Alpha: a\r\nX-Zeta: 2\r\n" +
		"X-Fold: one\r\n  two\r\nContent-Length: 2\r\n\r\nok"
	for _, chunk := range []int{1, 5, 1000} {
		var g full
		feedB(g.stream(false), []byte(in), chunk)
		if len(g.reqs) != 1 {
			t.Fatalf("chunk %d: %d requests", chunk, len(g.reqs))
		}
		want := [][2]string{{"host", "h"}, {"X-Zeta", "1"}, {"content-type", "text/plain"}, {"X-Alpha", "a"},
			{"X-Zeta", "2"}, {"X-Fold", "one two"}, {"Content-Length", "2"}}
		if r := g.reqs[0]; !reflect.DeepEqual(r.Fields, want) || string(r.Body) != "ok" || r.Header.Get("Host") != "h" {
			t.Fatalf("chunk %d: %q body %q", chunk, r.Fields, r.Body)
		}
	}
}

// A candidate is refused the same way whole or in pieces (SPEC §6.6): a
// 20 KiB request line held the decision only up to 16 KiB before, so in
// pieces the request was lost; whole, it was found.
func TestLargeCandidateSplitInvariant(t *testing.T) {
	long := "POST /" + strings.Repeat("a", 20<<10) + " HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nZ"
	for _, mid := range []bool{false, true} {
		in := long
		if mid {
			in = "tail of a body " + long // a midstream direction resynchronizes
		}
		var whole, pieces full
		feedB(whole.stream(mid), []byte(in), len(in))
		feedB(pieces.stream(mid), []byte(in), 1460)
		if len(whole.reqs) != 1 || len(pieces.reqs) != 1 {
			t.Fatalf("mid %v: whole %d, pieces %d requests", mid, len(whole.reqs), len(pieces.reqs))
		}
	}
	// A head whose blank line is past 1 MiB is refused whole too (it was
	// refused only in pieces), and the next request is found either way.
	huge := "POST /h HTTP/1.1\r\nX-Big: " + strings.Repeat("b", 1<<20) + "\r\n\r\n" + reqB
	var whole, pieces full
	feedB(whole.stream(false), []byte(huge), len(huge))
	feedB(pieces.stream(false), []byte(huge), 1460)
	if len(whole.reqs) != 1 || len(pieces.reqs) != 1 || whole.reqs[0].URI != "/b" || pieces.reqs[0].URI != "/b" {
		t.Fatalf("huge head: whole %d, pieces %d", len(whole.reqs), len(pieces.reqs))
	}
	// A chunk-extension line over 4 KiB: malformed either way.
	ext := "PUT /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n3;e=" + strings.Repeat("x", 5000) + "\r\nabc\r\n0\r\n\r\n" + reqB
	whole, pieces = full{}, full{}
	feedB(whole.stream(false), []byte(ext), len(ext))
	feedB(pieces.stream(false), []byte(ext), 7)
	if len(whole.reqs) != len(pieces.reqs) {
		t.Fatalf("chunk line: whole %d, pieces %d", len(whole.reqs), len(pieces.reqs))
	}
}

// A large chunked body arriving in segments is scanned once (SPEC §6.6):
// every chunk line is read a bounded number of times, not once per later
// segment. Before, each segment rescanned the whole buffer.
func TestChunkedIncremental(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("PUT /up HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n")
	const chunks = 4000
	for range chunks {
		fmt.Fprintf(&b, "400\r\n%s\r\n", strings.Repeat("d", 0x400))
	}
	b.WriteString("0\r\n\r\n")
	chunkLines = 0
	var g full
	start := time.Now()
	feedB(g.stream(false), b.Bytes(), 1460)
	if len(g.reqs) != 1 || len(g.reqs[0].Body) != chunks*0x400 {
		t.Fatalf("%d requests", len(g.reqs))
	}
	if chunkLines > 3*chunks {
		t.Fatalf("%d chunk lines read for %d chunks (%v): the body is rescanned", chunkLines, chunks, time.Since(start))
	}
}

// h2 builds the client side of an h2c connection with x/net's own encoder.
type h2 struct {
	buf bytes.Buffer
	fr  *http2.Framer
	enc *hpack.Encoder
	hb  bytes.Buffer
}

func newH2Client(preface bool) *h2 {
	c := &h2{}
	if preface {
		c.buf.WriteString(h2Preface)
	}
	c.fr = http2.NewFramer(&c.buf, nil)
	c.enc = hpack.NewEncoder(&c.hb)
	c.fr.WriteSettings()
	return c
}

func (c *h2) headers(id uint32, end bool, split bool, kv ...string) {
	c.hb.Reset()
	for i := 0; i < len(kv); i += 2 {
		c.enc.WriteField(hpack.HeaderField{Name: kv[i], Value: kv[i+1]})
	}
	blk := append([]byte(nil), c.hb.Bytes()...)
	if !split || len(blk) < 4 {
		c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: blk, EndStream: end, EndHeaders: true})
		return
	}
	// HEADERS, then two CONTINUATIONs.
	a, b := len(blk)/3, 2*len(blk)/3
	c.fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: blk[:a], EndStream: end})
	c.fr.WriteContinuation(id, false, blk[a:b])
	c.fr.WriteContinuation(id, true, blk[b:])
}

func h2Sample() []byte {
	c := newH2Client(true)
	c.headers(1, false, true, ":method", "POST", ":scheme", "http", ":authority", "hooks.example", ":path", "/h2/a?x=1",
		"content-type", "application/json", "x-order", "first", "cookie", "a=1", "cookie", "b=2", "x-order", "second")
	c.fr.WriteDataPadded(1, false, []byte(`{"a":`), []byte{0, 0, 0})
	c.fr.WritePing(false, [8]byte{})
	// Stream 3 interleaved, headers only; it ends first.
	c.headers(3, true, false, ":method", "GET", ":scheme", "http", ":authority", "hooks.example", ":path", "/h2/b", "x-order", "first")
	c.fr.WriteData(1, true, []byte(`1}`))
	// Stream 5 reset by the client: not replayed.
	c.headers(5, false, false, ":method", "POST", ":scheme", "http", ":authority", "hooks.example", ":path", "/h2/c")
	c.fr.WriteRSTStream(5, http2.ErrCodeCancel)
	// Stream 7 with trailers.
	c.headers(7, false, false, ":method", "PUT", ":scheme", "http", ":authority", "hooks.example", ":path", "/h2/d")
	c.fr.WriteData(7, false, []byte("xyz"))
	c.headers(7, true, false, "x-trailer", "t")
	return c.buf.Bytes()
}

func describeH2(rs []*Request) []string {
	var out []string
	for _, r := range rs {
		out = append(out, fmt.Sprintf("%s %s %s host=%s %q body=%s", r.Method, r.URI, r.Proto, r.Header.Get("Host"), r.Fields, r.Body))
	}
	return out
}

// h2c by prior knowledge (SPEC §6.8): every complete stream, in the order
// they ended, whatever the split of the segments; the dynamic table works
// across blocks (x-order and the authority are indexed the second time).
func TestH2CPriorKnowledge(t *testing.T) {
	in := h2Sample()
	want := []string{
		`GET /h2/b HTTP/2.0 host=hooks.example [["x-order" "first"]] body=`,
		`POST /h2/a?x=1 HTTP/2.0 host=hooks.example [["content-type" "application/json"] ["x-order" "first"] ["cookie" "a=1; b=2"] ["x-order" "second"]] body={"a":1}`,
		`PUT /h2/d HTTP/2.0 host=hooks.example [] body=xyz`,
	}
	for _, chunk := range []int{1, 7, 100, len(in)} {
		var g full
		feedB(g.stream(false), in, chunk)
		if got := describeH2(g.reqs); !reflect.DeepEqual(got, want) {
			t.Fatalf("chunk %d:\n%s", chunk, strings.Join(got, "\n"))
		}
		if g.st.H2C != 1 || g.st.H2Requests != 3 || g.st.H2Reset != 1 || g.st.H2Lost != 0 || g.st.Requests != 0 || g.st.NotHTTP != 0 {
			t.Fatalf("chunk %d: %+v", chunk, g.st)
		}
	}
}

// An h2c upgrade: the HTTP/1.1 request asking for it, then the preface and
// HTTP/2 on the same direction.
func TestH2CUpgrade(t *testing.T) {
	up := "GET /up HTTP/1.1\r\nHost: h\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n"
	c := newH2Client(true)
	c.headers(3, true, false, ":method", "POST", ":scheme", "http", ":authority", "h", ":path", "/after")
	in := append([]byte(up), c.buf.Bytes()...)
	for _, chunk := range []int{1, 13, len(in)} {
		var g full
		feedB(g.stream(false), in, chunk)
		if len(g.reqs) != 2 || g.reqs[0].URI != "/up" || g.reqs[1].URI != "/after" || g.st.H2C != 1 || g.st.Requests != 1 {
			t.Fatalf("chunk %d: %q %+v", chunk, describeH2(g.reqs), g.st)
		}
	}
}

// A gap ends the direction: the HPACK state is gone, the streams open are
// lost, and nothing after the gap is decoded (SPEC §6.8).
func TestH2CGapEndsDirection(t *testing.T) {
	c := newH2Client(true)
	c.headers(1, false, false, ":method", "POST", ":scheme", "http", ":authority", "h", ":path", "/open")
	c.headers(3, true, false, ":method", "GET", ":scheme", "http", ":authority", "h", ":path", "/done")
	var g full
	s := g.stream(false)
	s.Data(c.buf.Bytes(), time.Time{})
	s.Gap(100)
	c2 := newH2Client(false)
	c2.headers(5, true, false, ":method", "GET", ":scheme", "http", ":authority", "h", ":path", "/after")
	s.Data(c2.buf.Bytes(), time.Time{})
	s.End()
	if len(g.reqs) != 1 || g.reqs[0].URI != "/done" || g.st.H2Lost != 1 {
		t.Fatalf("%q %+v", describeH2(g.reqs), g.st)
	}
	// Garbage instead of frames: one error, the direction ends.
	g = full{}
	s = g.stream(false)
	s.Data([]byte(h2Preface+"\x00\x00\x04\x09\x00\x00\x00\x00\x07abcd"), time.Time{}) // CONTINUATION alone
	s.End()
	if g.st.H2Errors != 1 || len(g.reqs) != 0 {
		t.Fatalf("%+v", g.st)
	}
}
