package extract

import (
	"bytes"
	"compress/gzip"
	"net/netip"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
)

func feed(strict, mid bool, chunk int, in []byte, gaps map[int]bool) ([]string, Stats) {
	var st Stats
	var out []string
	s := NewStream(strict, mid, &st, func(d Doc) { out = append(out, string(d.Data)) })
	for off := 0; off < len(in); off += chunk {
		if gaps[off] {
			s.Gap(10)
		}
		s.Data(in[off:min(off+chunk, len(in))], time.Time{})
	}
	s.End()
	return out, st
}

func TestNulFraming(t *testing.T) {
	in := []byte("{\"a\":1}\x00\x00  \n{\"b\":2}\x00 {\"c\":3} \x00not gelf\x00{\"d\":4}")
	for _, chunk := range []int{1, 3, 7, 1000} {
		out, st := feed(true, false, chunk, in, nil)
		want := []string{`{"a":1}`, `{"b":2}`, `{"c":3}`, `{"d":4}`}
		if !reflect.DeepEqual(out, want) || st.FramesNotGELF != 1 || st.StreamsGELF != 1 {
			t.Fatalf("chunk %d: %q %+v", chunk, out, st)
		}
	}
	// Non-strict takes the frame that is not an object too.
	out, _ := feed(false, false, 5, in, nil)
	if len(out) != 5 || out[3] != "not gelf" {
		t.Fatalf("%q", out)
	}
}

func TestTailAtEnd(t *testing.T) {
	out, st := feed(true, false, 4, []byte("{\"a\":1}\x00{\"cut\":"), nil)
	if len(out) != 1 || st.LostFrames != 1 {
		t.Fatalf("cut tail taken: %q %+v", out, st)
	}
	out, _ = feed(true, false, 4, []byte("{\"a\":1}\x00{\"whole\":2}\n"), nil)
	if len(out) != 2 {
		t.Fatalf("whole tail lost: %q", out)
	}
}

func TestGapResync(t *testing.T) {
	var in []byte
	for i := range 10 {
		in = append(in, `{"seq":`+strconv.Itoa(i)+`}`...)
		in = append(in, 0)
	}
	// Frames are 10 bytes: a gap at byte 25 cuts the frame of seq 2.
	out, st := feed(true, false, 5, in, map[int]bool{25: true})
	if len(out) != 9 || st.LostFrames != 1 || st.Resyncs != 1 {
		t.Fatalf("%q %+v", out, st)
	}
	for _, o := range out {
		if o == `{"seq":2}` {
			t.Fatal("cut frame emitted")
		}
	}
}

func TestMidstream(t *testing.T) {
	in := []byte("ack\":{\"x\":1}}\x00 \x00{\"seq\":1}\x00{\"seq\":2}\x00")
	out, st := feed(true, true, 3, in, nil)
	if !reflect.DeepEqual(out, []string{`{"seq":1}`, `{"seq":2}`}) || st.StreamsGELF != 1 || st.SkippedBytes != 16 {
		t.Fatalf("%q %+v", out, st)
	}
	// An HTTP connection caught in the middle of a body.
	body := []byte(`{"short_message":"h"}`)
	var z bytes.Buffer
	w := gzip.NewWriter(&z)
	w.Write(body)
	w.Close()
	h := "tail of a body}POST /gelf HTTP/1.1\r\nHost: x\r\nContent-Encoding: gzip\r\nContent-Length: " + strconv.Itoa(z.Len()) + "\r\n\r\n" + z.String()
	out, st = feed(true, true, 5, []byte(h), nil)
	if !reflect.DeepEqual(out, []string{string(body)}) || st.StreamsHTTP != 1 || st.Compressed[gelf.Gzip] != 1 {
		t.Fatalf("%q %+v", out, st)
	}
	// A bare LF ends the request line too.
	lf := "x}POST /gelf HTTP/1.0\nContent-Length: 7\n\n{\"a\":1}"
	if out, st = feed(true, true, 3, []byte(lf), nil); !reflect.DeepEqual(out, []string{`{"a":1}`}) || st.StreamsHTTP != 1 {
		t.Fatalf("LF: %q %+v", out, st)
	}
	// GELF access logs caught in the middle: " HTTP/1.1" inside a JSON
	// string is not a request line (no CR/LF after it), so the direction
	// stays GELF and only the cut first document is lost (review-gi #2).
	var al []byte
	al = append(al, `0.1 - - "GET /x HTTP/1.1" 200","_seq":0}`...)
	al = append(al, 0)
	for i := 1; i <= 5; i++ {
		al = append(al, `{"short_message":"GET /x HTTP/1.1 200","_seq":`+strconv.Itoa(i)+`}`...)
		al = append(al, 0)
	}
	for _, chunk := range []int{1, 7, len(al)} {
		out, st = feed(true, true, chunk, al, nil)
		if len(out) != 5 || st.StreamsGELF != 1 || st.StreamsHTTP != 0 {
			t.Fatalf("access log, chunk %d: %d docs %+v", chunk, len(out), st)
		}
	}
}

func TestDetect(t *testing.T) {
	for _, c := range []struct {
		in             string
		strict         bool
		gelf, http, ig uint64
	}{
		{"\x00 {\"a\":1}\x00", true, 1, 0, 0},
		{"POST /gelf HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}", true, 0, 1, 0},
		{"HTTP/1.1 202 Accepted\r\n\r\n", true, 0, 0, 1},
		{"SSH-2.0-OpenSSH\r\n", true, 0, 0, 1},
		{"SSH-2.0-OpenSSH\r\n", false, 1, 0, 0},
		{"\x16\x03\x01 tls", true, 0, 0, 1},
		{"", true, 0, 0, 1},
	} {
		_, st := feed(c.strict, false, 3, []byte(c.in), nil)
		if st.StreamsGELF != c.gelf || st.StreamsHTTP != c.http || st.StreamsIgnored != c.ig {
			t.Errorf("%q strict=%v: %+v", c.in, c.strict, st)
		}
	}
}

func TestDatagrams(t *testing.T) {
	var st Stats
	d := &Datagrams{St: &st, Strict: true, R: gelf.NewReassembler()}
	src := netip.MustParseAddrPort("10.0.0.1:1")
	var z gelf.Compressor
	for _, p := range [][]byte{
		[]byte(` {"a":1} `),
		append([]byte(nil), z.Compress(gelf.Zlib, []byte(`{"b":2}`))...),
		[]byte("dns"),
		{0x1f, 0x8b, 1, 2, 3},
	} {
		d.Add(src, p, time.Unix(1, 0))
	}
	if st.Datagrams != 2 || st.DatagramsSkip != 2 || st.DecompressError != 1 || st.Compressed[gelf.Zlib] != 1 {
		t.Fatalf("%+v", st)
	}
}

// FuzzStream: whatever the input, the split and the mode, nothing panics and
// the documents are the same whole or fed in pieces of 1..32 bytes.
func FuzzStream(f *testing.F) {
	f.Add([]byte("{\"a\":1}\x00{\"b\":2}\x00{\"c\""), uint8(3), true, false)
	f.Add([]byte("x\x00 {\"a\":1}\x00"), uint8(1), true, true)
	f.Add([]byte("body}POST /gelf HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}"), uint8(4), true, true)
	f.Add([]byte("POST /gelf HTTP/1.1\r\nContent-Length: 7\r\n\r\n{\"a\":1}"), uint8(2), false, false)
	f.Add([]byte("1.1\" 200\"}\x00{\"m\":\"GET / HTTP/1.1\"}\x00{\"a\":1}\x00"), uint8(5), true, true)
	f.Add([]byte("x HTTP/1.1 HTTP/1.1\n\nPOST /gelf HTTP/1.1\nContent-Length: 2\n\n{}"), uint8(3), true, true)
	f.Fuzz(func(t *testing.T, in []byte, chunk uint8, strict, mid bool) {
		a, _ := feed(strict, mid, len(in)+1, in, nil)
		b, _ := feed(strict, mid, int(chunk%32)+1, in, nil)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("split %d changed the documents: %q vs %q", chunk%32+1, a, b)
		}
	})
}

// FuzzDatagrams: no datagram panics the UDP path.
func FuzzDatagrams(f *testing.F) {
	f.Add([]byte(`{"a":1}`), []byte{0x1e, 0x0f, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1, '{', '}'})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		var st Stats
		d := &Datagrams{St: &st, Strict: true, R: gelf.NewReassembler()}
		src := netip.MustParseAddrPort("10.0.0.1:1")
		for _, p := range [][]byte{a, b, a} {
			if doc, ok := d.Add(src, p, time.Unix(1, 0)); ok && (len(doc.Data) == 0 || doc.Data[0] != '{') {
				t.Fatalf("strict took %q", doc.Data)
			}
		}
	})
}

// Strict HTTP replay takes only POSTs to a GELF input path; with
// --pcap-port (not strict) any path goes (review-gi #9).
func TestHTTPPathStrict(t *testing.T) {
	req := func(uri string) string {
		return "POST " + uri + " HTTP/1.1\r\nHost: h\r\nContent-Length: 7\r\n\r\n{\"a\":1}"
	}
	for uri, want := range map[string]bool{
		"/gelf": true, "/gelf?x=1": true, "/graylog/gelf": true, "http://h:12202/gelf": true,
		"/_bulk": false, "/hook": false, "/gelfx": false, "/gelf/x": false, "http://h:12202": false,
	} {
		out, st := feed(true, false, 5, []byte(req(uri)), nil)
		if got := len(out) == 1; got != want || !want && st.HTTPSkipped != 1 {
			t.Fatalf("strict %s: %q %+v", uri, out, st)
		}
		if out, _ := feed(false, false, 5, []byte(req(uri)), nil); len(out) != 1 {
			t.Fatalf("not strict %s: %q", uri, out)
		}
	}
}
