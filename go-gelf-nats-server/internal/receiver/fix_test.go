package receiver

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
)

// The fixes of the 2026-10-31 review round (tasks/gelf/fix-plan-servers.md),
// each test built from the reviewer's reproducer.

// rawPost writes one HTTP/1.1 POST /gelf on c.
func rawPost(c net.Conn, body string, extra string) {
	fmt.Fprintf(c, "POST /gelf HTTP/1.1\r\nHost: x\r\n%sContent-Length: %d\r\n\r\n%s", extra, len(body), body)
}

// SPEC §4.5: keep-alive. N pipelined requests on ONE connection get N answers
// and the server opened one connection.
func TestHTTPKeepAliveServesManyRequestsOnOneConnection(t *testing.T) {
	h := newHarness(t, 100)
	s := startHTTP(t, h, HTTPConfig{})
	c := dial(t, s.Addr())
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	const n = 20
	for i := 0; i < n; i++ { // pipelined: all written before any answer is read
		rawPost(c, doc(fmt.Sprintf("ka %d", i)), "")
	}
	br := bufio.NewReader(c)
	for i := 0; i < n; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("answer %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted || resp.Close {
			t.Fatalf("answer %d: %d close=%v", i, resp.StatusCode, resp.Close)
		}
	}
	h.waitItems(t, n)
	if got := h.m.ConnsTotal[obs.TransportHTTP].Load(); got != 1 {
		t.Fatalf("%d connections for %d requests", got, n)
	}
}

// http.max_requests_per_conn: the Nth answer carries Connection: close and the
// server closes.
func TestHTTPMaxRequestsPerConn(t *testing.T) {
	h := newHarness(t, 100)
	s := startHTTP(t, h, HTTPConfig{MaxRequestsPerConn: 3})
	c := dial(t, s.Addr())
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(c)
	for i := 1; i <= 3; i++ {
		rawPost(c, doc("cap"), "")
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		if want := i == 3; resp.Close != want {
			t.Fatalf("request %d: close=%v, want %v", i, resp.Close, want)
		}
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("the connection stayed open after the cap: %v", err)
	}
}

// The shutdown answers 503 with Connection: close (SPEC §4.5); a full ring
// keeps the connection.
func TestHTTP503ClosesOnlyOnShutdown(t *testing.T) {
	h := newHarness(t, 1)
	s := startHTTP(t, h, HTTPConfig{})
	c := dial(t, s.Addr())
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(c)
	codes := []int{}
	for i := 0; i < 2; i++ {
		rawPost(c, doc("full"), "")
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		if resp.Close {
			t.Fatalf("request %d (%d) closed the connection", i, resp.StatusCode)
		}
		codes = append(codes, resp.StatusCode)
	}
	if codes[0] != http.StatusAccepted || codes[1] != http.StatusServiceUnavailable {
		t.Fatalf("codes %v", codes)
	}

	// Shutdown: the ring refuses with ErrClosed.
	h.ring.Close()
	rawPost(c, doc("closing"), "")
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || !resp.Close || resp.Header.Get("Retry-After") != "1" {
		t.Fatalf("shutdown answer %d close=%v retry=%q", resp.StatusCode, resp.Close, resp.Header.Get("Retry-After"))
	}
}

// SPEC §4.5 (review 2026-10-31, R1): an upload slower than the write timeout
// that enters the ring still gets its 202. net/http armed the write deadline
// at the end of the headers.
func TestHTTPSlowBodyStillGetsItsAnswer(t *testing.T) {
	if testing.Short() {
		t.Skip("11 s")
	}
	h := newHarness(t, 10)
	s := startHTTP(t, h, HTTPConfig{})
	c := dial(t, s.Addr())
	body := doc("slow upload")
	fmt.Fprintf(c, "POST /gelf HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", len(body))
	io.WriteString(c, body[:10])
	time.Sleep(httpWriteTimeout + time.Second) // under read_timeout, over the write timeout
	io.WriteString(c, body[10:])
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if h.ring.Stats().Items != 1 {
		t.Fatalf("ring holds %d items", h.ring.Stats().Items)
	}
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("the document is in the ring but the client got %v, %v", resp, err)
	}
}

// http.bulk (SPEC §4.5.1).
func TestHTTPBulk(t *testing.T) {
	t.Run("lines", func(t *testing.T) {
		h := newHarness(t, 100)
		s := startHTTP(t, h, HTTPConfig{Bulk: true})
		body := doc("one") + "\n\n  \r\n" + doc("two") + "\r\n" + doc("three")
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "gzip", gzipOf(t, []byte(body)))
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
		items := h.drainRing()
		if len(items) != 3 {
			t.Fatalf("%d items", len(items))
		}
		seen := map[string]bool{}
		for i, it := range items {
			want := []string{"one", "two", "three"}[i]
			if string(it.Raw) != doc(want) {
				t.Fatalf("item %d raw %q", i, it.Raw)
			}
			if it.Head.UTS != items[0].Head.UTS || it.Head.Sub != items[0].Head.Sub {
				t.Fatal("the lines of one body carry different clock samples")
			}
			if it.Head.Compression != item.CompressionGzip {
				t.Fatalf("compression %v", it.Head.Compression)
			}
			u := it.Head.UUID.String()
			if seen[u] {
				t.Fatal("two lines share a uuid")
			}
			seen[u] = true
		}
		if got := h.m.MessagesTotal[obs.TransportHTTP].Load(); got != 3 {
			t.Fatalf("messages_total %d", got)
		}
	})
	t.Run("a line too large refuses everything", func(t *testing.T) {
		h := newHarness(t, 100)
		s := startHTTP(t, h, HTTPConfig{Bulk: true})
		big := doc(strings.Repeat("x", 70<<10)) // above the harness's 64 KiB
		body := doc("one") + "\n" + big + "\n" + doc("three")
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(body))
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if n := h.ring.Stats().Items; n != 0 {
			t.Fatalf("%d items queued from a refused body", n)
		}
		if got := h.m.MessagesDroppedTotal[obs.DropTooLarge].Load(); got != 1 {
			t.Fatalf("too_large %d", got)
		}
	})
	t.Run("ring full in the middle keeps the first ones", func(t *testing.T) {
		h := newHarness(t, 2)
		s := startHTTP(t, h, HTTPConfig{Bulk: true})
		body := doc("a") + "\n" + doc("b") + "\n" + doc("c") + "\n" + doc("d")
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(body))
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if n := h.ring.Stats().Items; n != 2 {
			t.Fatalf("%d items stayed", n)
		}
		if got := h.m.MessagesDroppedTotal[obs.DropRingFull].Load(); got != 1 {
			t.Fatalf("ring_full %d", got)
		}
	})
	t.Run("blank body", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{Bulk: true})
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte("\n \r\n\n"))
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if got := h.m.MessagesDroppedTotal[obs.DropEmpty].Load(); got != 1 {
			t.Fatalf("empty %d", got)
		}
	})
	t.Run("a body above one document but under bulk_max_bytes", func(t *testing.T) {
		h := newHarness(t, 100)
		s := startHTTP(t, h, HTTPConfig{Bulk: true, BulkMaxBytes: 1 << 20})
		var b strings.Builder
		for i := 0; i < 40; i++ { // 40 x 3 KiB > the 64 KiB document ceiling
			b.WriteString(doc(fmt.Sprintf("%d %s", i, strings.Repeat("y", 3<<10))))
			b.WriteByte('\n')
		}
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(b.String()))
		if resp.StatusCode != http.StatusAccepted || h.ring.Stats().Items != 40 {
			t.Fatalf("status %d, %d items", resp.StatusCode, h.ring.Stats().Items)
		}
	})
	t.Run("off: the lines are one document", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{})
		body := doc("one") + "\n" + doc("two")
		resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(body))
		if resp.StatusCode != http.StatusAccepted || h.ring.Stats().Items != 1 {
			t.Fatalf("status %d, %d items", resp.StatusCode, h.ring.Stats().Items)
		}
	})
}

// SPEC §5.2 (review 2026-10-31, R2): a message refused by the byte ceiling is
// counted once, not once per chunk, and a message that overflows mid-way is
// not counted a second time as chunk_timeout.
func TestChunkOverflowCountsOncePerMessage(t *testing.T) {
	src := netip.MustParseAddrPort("192.0.2.1:5000")
	chunk := func(id, seq, count byte, data string) []byte {
		b := []byte{0x1e, 0x0f, id, 0, 0, 0, 0, 0, 0, 0, seq, count}
		return append(b, data...)
	}
	big := strings.Repeat("a", 400)
	now := time.Now()

	r := NewReassembler(ReassemblerConfig{MaxBytes: 1000, MaxMessages: 100, Timeout: time.Second})
	r.Add(nil, src, chunk(1, 0, 3, big), now)
	r.Add(nil, src, chunk(1, 1, 3, big), now)
	drops := 0
	for seq := byte(0); seq < 4; seq++ {
		if _, _, res := r.Add(nil, src, chunk(2, seq, 4, big), now); res == ChunkDropOverflow {
			drops++
		}
	}
	if drops != 1 {
		t.Fatalf("one message refused by the ceiling counted %d times", drops)
	}
	if n := r.Sweep(now.Add(2 * time.Second)); n != 1 { // message 1, incomplete
		t.Fatalf("sweep counted %d, want 1 (the tombstone is not a timeout)", n)
	}

	r2 := NewReassembler(ReassemblerConfig{MaxBytes: 1000, MaxMessages: 100, Timeout: time.Second})
	r2.Add(nil, src, chunk(3, 0, 3, big), now)
	if _, _, res := r2.Add(nil, src, chunk(3, 1, 3, big+big), now); res != ChunkDropOverflow {
		t.Fatalf("want overflow, got %v", res)
	}
	if r2.Bytes() != 0 {
		t.Fatalf("the overflowing message still holds %d bytes", r2.Bytes())
	}
	if _, _, res := r2.Add(nil, src, chunk(3, 2, 3, "x"), now); res != ChunkSwallowed {
		t.Fatalf("the rest of the message: %v, want swallowed", res)
	}
	if n := r2.Sweep(now.Add(2 * time.Second)); n != 0 {
		t.Fatalf("the overflowed message counted again as %d chunk_timeout", n)
	}

	// max_messages full: nowhere to remember, each chunk counts (SPEC §5.2).
	r3 := NewReassembler(ReassemblerConfig{MaxBytes: 1 << 20, MaxMessages: 1, Timeout: time.Second})
	r3.Add(nil, src, chunk(4, 0, 2, "x"), now)
	for seq := byte(0); seq < 2; seq++ {
		if _, _, res := r3.Add(nil, src, chunk(5, seq, 2, "x"), now); res != ChunkDropOverflow {
			t.Fatalf("chunk %d under max_messages: %v", seq, res)
		}
	}
}

// SPEC §4.5, §5.4 (review 2026-10-31, R3): the body and the decompressed
// document never hold more than their ceiling + 1.
func TestHTTPBuffersStayUnderTheCeiling(t *testing.T) {
	limit := WireLimit(128<<10) + 1
	b, err := readLimited(nil, bytes.NewReader(make([]byte, 4*limit)), limit)
	if err != nil || len(b) != limit || cap(b) > limit {
		t.Fatalf("body: len %d cap %d (limit %d), %v", len(b), cap(b), limit, err)
	}
	max := 128 << 10
	doc, err := Decompress(nil, gzipOf(t, make([]byte, max)), item.CompressionGzip, max)
	if err != nil || cap(doc) > max+1 {
		t.Fatalf("document: len %d cap %d (max %d), %v", len(doc), cap(doc), max, err)
	}
	_, err = Decompress(nil, gzipOf(t, make([]byte, 4*max)), item.CompressionGzip, max)
	if err != errTooLarge {
		t.Fatalf("bomb: %v", err)
	}
}

var _ = context.Background
