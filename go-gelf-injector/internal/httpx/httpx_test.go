package httpx

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

type got struct {
	reqs []string
	st   Stats
}

func (g *got) stream(mid bool) *Stream {
	return NewStream(mid, &g.st, func(r *Request, _ time.Time) {
		g.reqs = append(g.reqs, r.Method+" "+r.URI+" host="+r.Header.Get("Host")+" ct="+r.Header.Get("Content-Type")+" body="+string(r.Body))
	})
}

func feed(s *Stream, in string, chunk int) {
	for off := 0; off < len(in); off += chunk {
		s.Data([]byte(in[off:min(off+chunk, len(in))]), time.Time{})
	}
	s.End()
}

const (
	reqA = "POST /a?x=1 HTTP/1.1\r\nHost: h1\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{\"a\":1}"
	reqB = "GET /b HTTP/1.1\r\nHost: h2\r\n\r\n"
	reqC = "PUT /c HTTP/1.1\r\nHost: h3\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
)

func TestPipelinedKinds(t *testing.T) {
	for _, chunk := range []int{1, 3, 7, 1000} {
		var g got
		feed(g.stream(false), reqA+reqB+reqC, chunk)
		want := []string{
			`POST /a?x=1 host=h1 ct=application/json body={"a":1}`,
			"GET /b host=h2 ct= body=",
			"PUT /c host=h3 ct= body=abcde",
		}
		if !reflect.DeepEqual(g.reqs, want) {
			t.Fatalf("chunk %d: %q", chunk, g.reqs)
		}
		if g.st.Requests != 3 || g.st.HTTPStreams != 1 || g.st.Lost != 0 {
			t.Fatalf("chunk %d: %+v", chunk, g.st)
		}
	}
}

func TestResponsesAndH2CIgnored(t *testing.T) {
	var g got
	feed(g.stream(false), "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", 5)
	feed(g.stream(false), "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 4)
	feed(g.stream(false), "\x16\x03\x01\x02\x00", 2)
	if len(g.reqs) != 0 || g.st.Responses != 1 || g.st.H2C != 1 || g.st.NotHTTP != 1 {
		t.Fatalf("%q %+v", g.reqs, g.st)
	}
}

func TestMidstreamResync(t *testing.T) {
	var g got
	// The capture began inside reqA's body.
	feed(g.stream(true), reqA[40:]+reqB+reqC, 5)
	if len(g.reqs) != 2 || !strings.HasPrefix(g.reqs[0], "GET /b") || g.st.SkippedBytes == 0 {
		t.Fatalf("%q %+v", g.reqs, g.st)
	}
}

func TestGapLosesOnlyTheCutRequest(t *testing.T) {
	var g got
	s := g.stream(false)
	s.Data([]byte(reqA[:30]), time.Time{})
	s.Gap(10)
	s.Data([]byte(reqB+reqC), time.Time{})
	s.End()
	if len(g.reqs) != 2 || g.st.Lost != 1 || g.st.Resyncs != 1 {
		t.Fatalf("%q %+v", g.reqs, g.st)
	}
}

func TestIncompleteAtEndIsLost(t *testing.T) {
	var g got
	feed(g.stream(false), reqB+reqA[:len(reqA)-2], 4)
	if len(g.reqs) != 1 || g.st.Lost != 1 {
		t.Fatalf("%q %+v", g.reqs, g.st)
	}
}

func TestGarbageBetweenRequestsResyncs(t *testing.T) {
	var g got
	feed(g.stream(false), reqB+"garbage line\r\n"+reqA, 3)
	if len(g.reqs) != 2 || g.st.Resyncs == 0 {
		t.Fatalf("%q %+v", g.reqs, g.st)
	}
}

// A declared Content-Length above MaxRequest is refused from the head, not
// buffered until the declared length arrives.
func TestHugeContentLengthRefusedEarly(t *testing.T) {
	var g got
	s := g.stream(false)
	s.Data([]byte("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 1000000000\r\n\r\n"), time.Time{})
	s.Data(make([]byte, 1<<20), time.Time{})
	s.Data([]byte(reqB), time.Time{})
	s.End()
	if g.st.TooLarge != 1 || len(s.buf) != 0 || len(g.reqs) != 1 || !strings.HasPrefix(g.reqs[0], "GET /b") {
		t.Fatalf("%q %+v buf %d", g.reqs, g.st, len(s.buf))
	}
}

func FuzzStream(f *testing.F) {
	f.Add([]byte(reqA+reqB+reqC), uint8(3), false)
	f.Add([]byte(reqA[20:]+reqB), uint8(1), true)
	f.Add([]byte("GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nab"), uint8(2), false)
	f.Fuzz(func(t *testing.T, in []byte, chunk uint8, mid bool) {
		// Whatever the input and the split, the requests are the same and
		// nothing panics.
		var a, b got
		s1 := a.stream(mid)
		s1.Data(in, time.Time{})
		s1.End()
		feed(b.stream(mid), string(in), int(chunk%32)+1)
		if !reflect.DeepEqual(a.reqs, b.reqs) {
			t.Fatalf("split %d changed the requests:\n%q\n%q", int(chunk%32)+1, a.reqs, b.reqs)
		}
	})
}
