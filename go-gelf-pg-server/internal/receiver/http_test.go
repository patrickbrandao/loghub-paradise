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
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
)

// GELF over HTTP and HTTPS (SPEC §4.5), through a real listener and the
// stdlib client.

func startHTTP(t *testing.T, h *harness, cfg HTTPConfig) *HTTP {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	s, err := NewHTTP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); s.Close() })
	s.Start(ctx)
	return s
}

func httpClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{}}
}

// encodeAs is a body with Content-Encoding enc: "deflate" is the zlib format
// (RFC 9110), "deflate-raw" is the common mistake, sent as "deflate".
func encodeAs(t testing.TB, enc string, b []byte) []byte {
	switch enc {
	case "gzip":
		return gzipOf(t, b)
	case "deflate":
		return zlibOf(t, b)
	case "deflate-raw":
		return deflateOf(t, b)
	}
	return b
}

// post sends one request and returns the response with its body read.
func post(t *testing.T, c *http.Client, url, enc string, body []byte) *http.Response {
	t.Helper()
	return do(t, c, http.MethodPost, url, enc, bytes.NewReader(body), int64(len(body)))
}

func do(t *testing.T, c *http.Client, method, url, enc string, body io.Reader, length int64) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	if enc == "deflate-raw" {
		enc = "deflate"
	}
	if enc != "" {
		req.Header.Set("Content-Encoding", enc)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func TestHTTPAccepts(t *testing.T) {
	h := newHarness(t, 10)
	s := startHTTP(t, h, HTTPConfig{})
	resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(doc("posted")))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// Keep-alive (SPEC §4.5): the answer does not close the connection.
	if resp.Close {
		t.Fatal("the answer closes the connection: keep-alive is off")
	}
	// On the wire, too: no Connection: close, and the connection stays up.
	rc := dial(t, s.Addr())
	_ = rc.SetDeadline(time.Now().Add(5 * time.Second))
	body := doc("raw")
	fmt.Fprintf(rc, "POST /gelf HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	rresp, err := http.ReadResponse(bufio.NewReader(rc), nil)
	if err != nil || rresp.StatusCode != http.StatusAccepted || rresp.Close {
		t.Fatalf("raw answer %v, %v", rresp, err)
	}
	h.waitItems(t, 2)
	h.drainRing()
	post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(doc("posted")))
	if resp.ProtoMajor != 1 || resp.ProtoMinor != 1 {
		t.Fatalf("proto %s", resp.Proto)
	}
	if h.ring.Stats().Items != 1 {
		t.Fatal("a 202 without the item in the ring")
	}
	it := h.drainRing()[0]
	hd := it.Head
	if string(it.Raw) != doc("posted") || hd.Transport != item.TransportHTTP || hd.Compression != item.CompressionNone {
		t.Fatalf("raw %q head %+v", it.Raw, hd)
	}
	if hd.SrcIP != netip.MustParseAddr("127.0.0.1") || hd.DstIP != netip.MustParseAddr("127.0.0.1") ||
		hd.SrcPort == 0 || hd.DstPort == 0 || hd.Proxied() || hd.TLS != nil {
		t.Fatalf("addresses %+v", hd)
	}
	pl := payloadOf(t, envelopeOf(t, it))
	if pl["transport"] != "http" {
		t.Fatalf("payload transport %v", pl["transport"])
	}
	if h.m.MessagesAcceptedTotal[obs.TransportHTTP].Load() != 3 || h.m.ConfirmDuration.Count() != 3 {
		t.Fatal("accepted or confirm_duration not counted")
	}
	// The two clients' connections stay idle (keep-alive); the raw one ends
	// when it is closed.
	rc.Close()
	waitFor(t, "the connection metrics", func() bool {
		return h.m.ConnsTotal[obs.TransportHTTP].Load() == 3 && h.m.ConnsActive[obs.TransportHTTP].Load() <= 2
	})
}

// SPEC §4.5, §5.4: Content-Encoding gzip and deflate (zlib, or raw deflate as
// the common mistake); absent or identity detects by the bytes.
func TestHTTPCompression(t *testing.T) {
	h := newHarness(t, 20)
	s := startHTTP(t, h, HTTPConfig{})
	url := "http://" + s.Addr() + GELFPath
	for _, c := range []struct {
		header string
		body   []byte
		want   item.Compression
	}{
		{"gzip", gzipOf(t, []byte(doc("gzip"))), item.CompressionGzip},
		{"x-gzip", gzipOf(t, []byte(doc("x-gzip"))), item.CompressionGzip},
		{"deflate", zlibOf(t, []byte(doc("deflate zlib"))), item.CompressionZlib},
		{"deflate-raw", deflateOf(t, []byte(doc("deflate raw"))), item.CompressionDeflate},
		{"", gzipOf(t, []byte(doc("detected gzip"))), item.CompressionGzip},
		{"identity", zlibOf(t, []byte(doc("detected zlib"))), item.CompressionZlib},
		{"", []byte(doc("plain")), item.CompressionNone},
	} {
		if resp := post(t, httpClient(), url, c.header, c.body); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("%s: %d", c.header, resp.StatusCode)
		}
		h.waitItems(t, 1)
		it := h.drainRing()[0]
		if it.Head.Compression != c.want || !strings.HasPrefix(string(it.Raw), `{"version"`) {
			t.Fatalf("%s: compression %v raw %q", c.header, it.Head.Compression, it.Raw)
		}
		want := c.want.String()
		if pl := payloadOf(t, envelopeOf(t, it)); (want == "" && pl["compression"] != nil) ||
			(want != "" && pl["compression"] != want) {
			t.Fatalf("%s: payload compression %v, want %q", c.header, pl["compression"], want)
		}
	}
}

func TestHTTPRefusals(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 1024
	h := newHarnessCfg(t, 10, cfg)
	s := startHTTP(t, h, HTTPConfig{})
	base := "http://" + s.Addr()
	limit := WireLimit(1024)
	c := httpClient()

	for _, path := range []string{"/", "/gelf/", "/GELF", "/gelf/x", "/other"} {
		if resp := post(t, c, base+path, "", []byte(doc("x"))); resp.StatusCode != http.StatusNotFound {
			t.Errorf("POST %s: %d, want 404", path, resp.StatusCode)
		}
	}
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodHead} {
		resp := do(t, c, m, base+GELFPath, "", nil, 0)
		if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "POST" {
			t.Errorf("%s: %d Allow=%q, want 405 with Allow: POST", m, resp.StatusCode, resp.Header.Get("Allow"))
		}
	}
	for _, enc := range []string{"br", "compress", "zstd", "gzip, br"} {
		if resp := post(t, c, base+GELFPath, enc, []byte(doc("x"))); resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Errorf("Content-Encoding %s: %d, want 415", enc, resp.StatusCode)
		}
	}
	for name, body := range map[string][]byte{"empty": nil, "whitespace": []byte(" \r\n\t "),
		"bad gzip": gzipOf(t, []byte(doc("x")))[:15], "declared gzip, not gzip": []byte(doc("x"))} {
		enc := ""
		if strings.Contains(name, "gzip") {
			enc = "gzip"
		}
		if resp := post(t, c, base+GELFPath, enc, body); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", name, resp.StatusCode)
		}
	}
	// 413 three ways: the declared length (an early refusal), a streamed
	// body with no Content-Length that passes the wire limit, and a document
	// that inflates past max.
	if resp := post(t, c, base+GELFPath, "", make([]byte, limit+1)); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("by Content-Length: %d", resp.StatusCode)
	}
	streamed := io.MultiReader(bytes.NewReader([]byte(`{"short_message":"`)), io.LimitReader(repeatReader('s'), int64(2*limit)))
	if resp := do(t, c, http.MethodPost, base+GELFPath, "", streamed, -1); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("by the streamed body: %d", resp.StatusCode)
	}
	if resp := post(t, c, base+GELFPath, "gzip", gzipOf(t, []byte(doc(strings.Repeat("z", 2000))))); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("by the decompressed size: %d", resp.StatusCode)
	}
	if resp := post(t, c, base+GELFPath, "", []byte(doc(strings.Repeat("r", 1100)))); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("a plain document above max: %d", resp.StatusCode)
	}
	// Control: a streamed body within the limits is accepted.
	if resp := do(t, c, http.MethodPost, base+GELFPath, "", strings.NewReader(doc("chunked transfer")), -1); resp.StatusCode != http.StatusAccepted {
		t.Errorf("a streamed body within the limit: %d", resp.StatusCode)
	}

	if h.ring.Stats().Items != 1 {
		t.Fatalf("%d items in the ring, want only the control", h.ring.Stats().Items)
	}
	for r, want := range map[obs.DropReason]uint64{obs.DropTooLarge: 4, obs.DropEmpty: 2, obs.DropDecompress: 2 + 4} {
		if got := h.m.MessagesDroppedTotal[r].Load(); got != want {
			t.Errorf("%s = %d, want %d", r, got, want)
		}
	}
}

// blockRing holds every Enqueue until its context ends: the ring a shutdown
// interrupts.
type blockRing struct {
	stubRing
	entered atomic.Int32
}

// Enqueue waits for the receive context and answers its error, as a ring
// that blocks on the context does: the ingest maps context.Canceled to the
// shutdown (SPEC §13.4).
func (r *blockRing) Enqueue(ctx context.Context, it *item.Item) error {
	r.entered.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

// SPEC §4.5: everything that may pass on a new attempt is 503 with
// Retry-After, never a final 4xx.
func TestHTTPServiceUnavailable(t *testing.T) {
	url := func(s *HTTP) string { return "http://" + s.Addr() + GELFPath }
	check := func(t *testing.T, resp *http.Response, what string) {
		t.Helper()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
			t.Fatalf("%s: %d Retry-After=%q, want 503 with Retry-After: 1", what, resp.StatusCode, resp.Header.Get("Retry-After"))
		}
	}
	t.Run("ring full", func(t *testing.T) {
		h := newHarness(t, 1)
		s := startHTTP(t, h, HTTPConfig{})
		post(t, httpClient(), url(s), "", []byte(doc("fills")))
		check(t, post(t, httpClient(), url(s), "", []byte(doc("refused"))), "ring full")
		if h.m.MessagesDroppedTotal[obs.DropRingFull].Load() != 1 {
			t.Fatal("ring_full not counted")
		}
	})
	t.Run("sink down", func(t *testing.T) {
		h := newHarness(t, 10)
		h.sink.up.Store(false)
		s := startHTTP(t, h, HTTPConfig{})
		check(t, post(t, httpClient(), url(s), "", []byte(doc("x"))), "sink down")
		if h.m.MessagesDroppedTotal[obs.DropSinkDown].Load() != 1 || h.ring.Stats().Items != 0 {
			t.Fatal("sink_down not counted, or the document got in")
		}
	})
	t.Run("shutdown", func(t *testing.T) {
		h := newHarness(t, 10)
		br := &blockRing{}
		h.in = NewIngest(br, h.sink, h.m, logger(), h.cfg)
		s, err := NewHTTP(context.Background(), HTTPConfig{Addr: "127.0.0.1:0"}, h.in, h.m, logger())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.Start(ctx)
		got := make(chan *http.Response, 1)
		go func() { got <- post(t, httpClient(), url(s), "", []byte(doc("in flight"))) }()
		waitFor(t, "the request to reach the ring", func() bool { return br.entered.Load() == 1 })
		cancel() // the receive context, before Close (SPEC §13.4)
		select {
		case resp := <-got:
			check(t, resp, "shutdown")
		case <-time.After(5 * time.Second):
			t.Fatal("the request in flight was not answered after the receive context ended")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if h.m.MessagesDroppedTotal[obs.DropShutdown].Load() != 1 {
			t.Fatal("shutdown not counted")
		}
	})
}

// rawRequest sends a hand-written request over a raw connection (after any
// prefix, a PROXY header) and reads the answer; nil when the server closed
// without one.
func rawRequest(t *testing.T, addr string, prefix []byte, hdrs string, body string) *http.Response {
	t.Helper()
	c := dial(t, addr)
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(c, "%sPOST /gelf HTTP/1.1\r\nHost: x\r\n%sContent-Length: %d\r\n\r\n%s", prefix, hdrs, len(body), body)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		return nil
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

// SPEC §4.5, §4.6: a trusted peer's PROXY header sets the addresses and
// x-proxy; X-Forwarded-For and X-Real-IP never do.
func TestHTTPProxyProtocol(t *testing.T) {
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	t.Run("v1 from a trusted peer", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{Proxy: ProxyConfig{Mode: ProxyOptional, Trusted: cidrs}})
		resp := rawRequest(t, s.Addr(), []byte("PROXY TCP4 192.0.2.10 198.51.100.1 56324 12202\r\n"),
			"X-Forwarded-For: 203.0.113.9\r\n", doc("proxied"))
		if resp == nil || resp.StatusCode != http.StatusAccepted {
			t.Fatalf("resp %v", resp)
		}
		it := h.drainRing()[0]
		hd := it.Head
		if hd.SrcIP.String() != "192.0.2.10" || hd.SrcPort != 56324 || hd.DstIP.String() != "198.51.100.1" ||
			hd.DstPort != 12202 || !hd.Proxied() || hd.PeerIP.String() != "127.0.0.1" {
			t.Fatalf("head %+v", hd)
		}
		if pl := payloadOf(t, envelopeOf(t, it)); pl["x-proxy"] == nil {
			t.Fatal("no x-proxy in the payload")
		}
		if h.m.ProxyHeadersTotal[obs.ProxyV1].Load() != 1 {
			t.Fatal("v1 not counted")
		}
	})
	t.Run("X-Forwarded-For is ignored", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{})
		resp := rawRequest(t, s.Addr(), nil, "X-Forwarded-For: 203.0.113.9\r\nX-Real-IP: 203.0.113.9\r\nForwarded: for=203.0.113.9\r\n", doc("direct"))
		if resp == nil || resp.StatusCode != http.StatusAccepted {
			t.Fatalf("resp %v", resp)
		}
		if hd := h.drainRing()[0].Head; hd.SrcIP.String() != "127.0.0.1" || hd.Proxied() {
			t.Fatalf("a header moved the source: %+v", hd)
		}
	})
	t.Run("required without a header closes", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{Proxy: ProxyConfig{Mode: ProxyRequired, Trusted: cidrs}})
		if resp := rawRequest(t, s.Addr(), nil, "", doc("no header")); resp != nil {
			t.Fatalf("an HTTP answer (%d) to a connection without its PROXY header", resp.StatusCode)
		}
		waitFor(t, "the proxy drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1 })
		if h.ring.Stats().Items != 0 || h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 1 {
			t.Fatal("got in, or absent not counted")
		}
	})
	t.Run("untrusted peer, optional, header is data", func(t *testing.T) {
		h := newHarness(t, 10)
		other, _ := ParseCIDRList("10.0.0.0/8")
		s := startHTTP(t, h, HTTPConfig{Proxy: ProxyConfig{Mode: ProxyOptional, Trusted: other}})
		// The header is not read, so it is the start of a malformed request.
		resp := rawRequest(t, s.Addr(), []byte("PROXY TCP4 192.0.2.10 198.51.100.1 1 2\r\n"), "", doc("x"))
		if resp != nil && resp.StatusCode == http.StatusAccepted {
			t.Fatal("a forged header from an untrusted peer was honoured")
		}
		if h.ring.Stats().Items != 0 {
			t.Fatal("got in")
		}
	})
	t.Run("v2 from a trusted peer", func(t *testing.T) {
		h := newHarness(t, 10)
		s := startHTTP(t, h, HTTPConfig{Proxy: ProxyConfig{Mode: ProxyRequired, Trusted: cidrs}})
		resp := rawRequest(t, s.Addr(), proxyV2(0x21, 0x11, v2Inet("192.0.2.77", "198.51.100.1", 5000, 12202, nil)),
			"", doc("v2 proxied"))
		if resp == nil || resp.StatusCode != http.StatusAccepted {
			t.Fatalf("resp %v", resp)
		}
		hd := h.drainRing()[0].Head
		if hd.SrcIP.String() != "192.0.2.77" || hd.SrcPort != 5000 || !hd.Proxied() || hd.TLS != nil {
			t.Fatalf("head %+v", hd)
		}
	})
}

// SPEC §4.5: http.max_conns_per_ip refuses past the cap, and Close returns
// with nothing in flight.
func TestHTTPPerIPLimitAndClose(t *testing.T) {
	h := newHarness(t, 10)
	s := startHTTP(t, h, HTTPConfig{MaxConnsPerIP: 1})
	held := dial(t, s.Addr()) // occupies the one slot, says nothing
	waitFor(t, "the held connection", func() bool { return h.m.ConnsActive[obs.TransportHTTP].Load() == 1 })
	c := dial(t, s.Addr())
	expectClosed(t, c)
	waitFor(t, "the per_ip refusal", func() bool {
		return h.m.ConnsRefusedTotal[obs.TransportHTTP][obs.ConnLimitPerIP].Load() >= 1
	})
	held.Close()
	waitFor(t, "the slot to free", func() bool { return h.m.ConnsActive[obs.TransportHTTP].Load() == 0 })
	if resp := post(t, httpClient(), "http://"+s.Addr()+GELFPath, "", []byte(doc("after"))); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("after the slot freed: %d", resp.StatusCode)
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung")
	}
	if _, err := net.DialTimeout("tcp", s.Addr(), time.Second); err == nil {
		t.Fatal("the listener survived Close")
	}
}

func TestContentEncoding(t *testing.T) {
	for v, want := range map[string]struct {
		c        item.Compression
		declared bool
		ok       bool
	}{
		"": {item.CompressionNone, false, true}, "identity": {item.CompressionNone, false, true},
		" GZIP ": {item.CompressionGzip, true, true}, "x-gzip": {item.CompressionGzip, true, true},
		"Deflate": {item.CompressionDeflate, true, true}, "br": {0, false, false},
		"gzip, identity": {0, false, false},
	} {
		c, d, ok := contentEncoding(v)
		if c != want.c || d != want.declared || ok != want.ok {
			t.Errorf("%q: %v %v %v", v, c, d, ok)
		}
	}
}

func TestNewHTTPRefusesAProxyWithoutTrustedNetworks(t *testing.T) {
	h := newHarness(t, 1)
	if _, err := NewHTTP(context.Background(), HTTPConfig{Addr: "127.0.0.1:0",
		Proxy: ProxyConfig{Mode: ProxyRequired}}, h.in, h.m, logger()); err == nil {
		t.Fatal("proxy.protocol=required without proxy.trusted was accepted")
	}
}
