package app

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
)

// The GELF transports through the whole app (SPEC §4, §5): what each receiver
// test proves in isolation, proved again with the receiver the app builds from
// the configuration, the converter the drain runs and the JetStream stream.

func zlibOf(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	w.Write(b)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func deflateOf(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	w.Write(b)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// SPEC §4.5, §5.4, §5.5: POST /gelf is 202 once the ring has the document,
// whatever its Content-Encoding (or none, detected by the bytes), and the
// message in the stream carries the transport and the real container; the refusals
// that will never pass are 4xx and write nothing. convert.max_message_bytes
// reaches the HTTP receiver: a document above it is 413.
func TestHTTPPostEndToEnd(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--listen.http=127.0.0.1:0", "--convert.max_message_bytes=1KiB")
	base := "http://" + in.app.TransportAddr("http")
	url := base + "/gelf"

	for _, tc := range []struct {
		msg, enc, want string
		body           func([]byte) []byte
	}{
		{"http-plain", "", "<absent>", func(b []byte) []byte { return b }},
		{"http-gzip", "gzip", "gzip", func(b []byte) []byte { return gzipOf(t, b) }},
		{"http-zlib", "deflate", "zlib", func(b []byte) []byte { return zlibOf(t, b) }},
		{"http-raw-deflate", "deflate", "deflate", func(b []byte) []byte { return deflateOf(t, b) }},
		{"http-sniffed-gzip", "", "gzip", func(b []byte) []byte { return gzipOf(t, b) }},
	} {
		if code, _ := postGELF(t, nil, url, tc.enc, tc.body([]byte(gdoc(tc.msg, 4)))); code != http.StatusAccepted {
			t.Errorf("%s: POST = %d, want 202", tc.msg, code)
		}
	}

	big := []byte(doc(strings.Repeat("x", 1100)))
	for _, tc := range []struct {
		name, method, path, enc string
		body                    []byte
		want                    int
	}{
		{"above max_message_bytes", http.MethodPost, "/gelf", "", big, http.StatusRequestEntityTooLarge},
		{"above max_message_bytes, gzipped", http.MethodPost, "/gelf", "gzip", gzipOf(t, big), http.StatusRequestEntityTooLarge},
		{"another path", http.MethodPost, "/gelf/", "", []byte(doc("x")), http.StatusNotFound},
		{"GET", http.MethodGet, "/gelf", "", nil, http.StatusMethodNotAllowed},
		{"unknown encoding", http.MethodPost, "/gelf", "br", []byte(doc("x")), http.StatusUnsupportedMediaType},
		{"broken gzip", http.MethodPost, "/gelf", "gzip", []byte("\x1f\x8bnot gzip"), http.StatusBadRequest},
	} {
		req, err := http.NewRequest(tc.method, base+tc.path, bytes.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		if tc.enc != "" {
			req.Header.Set("Content-Encoding", tc.enc)
		}
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, resp.StatusCode, tc.want)
		}
		if tc.want == http.StatusMethodNotAllowed && resp.Header.Get("Allow") != "POST" {
			t.Errorf("405 without Allow: POST (%q)", resp.Header.Get("Allow"))
		}
	}

	envs := byMessage(t, f.waitMsgs(5))
	for msg, comp := range map[string]string{"http-plain": "<absent>", "http-gzip": "gzip", "http-zlib": "zlib",
		"http-raw-deflate": "deflate", "http-sniffed-gzip": "gzip"} {
		e := messageWith(t, envs, msg)
		if e.Payload.Transport != "http" || show(e.Payload.Compression) != comp || e.Level != 4 ||
			e.Payload.Chunks != nil || e.Payload.TLS != nil || e.Payload.Size != len(gdoc(msg, 4)) {
			t.Errorf("%s: %s", msg, e.bytes)
		}
	}
	if got := in.metric(`gelf_messages_dropped_total{reason="too_large"}`); got != "2" {
		t.Errorf(`too_large = %s, want 2`, got)
	}
	// The unknown Content-Encoding (415) and the broken gzip (400).
	if got := in.metric(`gelf_messages_dropped_total{reason="decompress"}`); got != "2" {
		t.Errorf(`decompress = %s, want 2`, got)
	}
	if got := in.metric(`gelf_messages_accepted_total{transport="http"}`); got != "5" {
		t.Errorf(`accepted{http} = %s, want 5`, got)
	}
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="nats",transports="udp,tcp,http"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	if n := f.count(); n != 5 {
		t.Errorf("%d messages after the shutdown, want the 5 accepted", n)
	}
}

// SPEC §5.2, §5.4: a gzip document in four chunks, sent out of order with a
// duplicate, becomes ONE message with chunks=4 and compression=gzip; a zlib
// document in a single datagram is detected by its bytes; a single-chunk
// message has chunks=1. The duplicate is counted, not dropped.
func TestUDPChunkedGzipEndToEnd(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	udp := in.app.TransportAddr("udp")
	body := []byte(doc("udp-chunked " + strings.Repeat("and compressed ", 200)))
	parts := gelfChunks(0x0102030405060708, gzipOf(t, body), 4)
	sendDatagrams(t, udp, parts[3], parts[1], parts[1], parts[0], parts[2])
	sendDatagrams(t, udp, zlibOf(t, []byte(gdoc("udp-zlib", 2))))
	sendDatagrams(t, udp, gelfChunks(42, []byte(gdoc("udp-one-chunk", 1)), 1)...)

	envs := byMessage(t, f.waitMsgs(3))
	e := messageWith(t, envs, "udp-chunked")
	if e.Payload.Transport != "udp" || show(e.Payload.Compression) != "gzip" || show(e.Payload.Chunks) != "4" ||
		e.Payload.Size != len(body) {
		t.Errorf("chunked: %s", e.bytes)
	}
	e = messageWith(t, envs, "udp-zlib")
	if show(e.Payload.Compression) != "zlib" || e.Payload.Chunks != nil || e.Level != 2 {
		t.Errorf("zlib: %s", e.bytes)
	}
	e = messageWith(t, envs, "udp-one-chunk")
	if show(e.Payload.Chunks) != "1" || e.Payload.Compression != nil {
		t.Errorf("one chunk: %s", e.bytes)
	}
	if got := in.metric("gelf_udp_chunk_duplicates_total"); got != "1" {
		t.Errorf("gelf_udp_chunk_duplicates_total = %s, want 1", got)
	}
	if got := in.metric(`gelf_messages_total{transport="udp"}`); got != "3" {
		t.Errorf(`gelf_messages_total{transport="udp"} = %s: a chunk counted as a message`, got)
	}
	waitFor(t, 5*time.Second, "the reassembly gauge back to zero", func() bool { return in.metric("gelf_udp_reassembly_bytes") == "0" })
}

// SPEC §5.3: on one TCP connection documents end at \0, blank frames between
// them are separators (not counted), a frame that is not JSON is a malformed
// document kept in raw, and the last document may end at EOF without its \0.
func TestTCPNULFramingEndToEnd(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	c := dialTCP(t, in.app.TransportAddr("tcp"))
	var stream []byte
	stream = append(stream, doc("tcp-1")+"\x00\n\x00"...)
	stream = append(stream, doc("tcp-2")+"\x00\x00"...)
	stream = append(stream, "nao e json \xff\x00"...)
	stream = append(stream, doc("tcp-eof")...)
	if _, err := c.Write(stream); err != nil {
		t.Fatal(err)
	}
	c.Close()

	rows := f.waitMsgs(4)
	envs := byMessage(t, rows)
	for _, m := range []string{"tcp-1", "tcp-2", "tcp-eof"} {
		if e := messageWith(t, envs, m); e.Payload.Transport != "tcp" || e.Payload.Format != "gelf" || e.Payload.Size != len(doc(m)) {
			t.Errorf("%s: %s", m, e.bytes)
		}
	}
	var bad int
	for _, r := range rows {
		if e := checkMsg(t, r); e.Payload.Format == "malformed" {
			bad++
			if string(e.Payload.rawBytes()) != "nao e json \xff" || e.Payload.RawEnc != "base64" {
				t.Errorf("the malformed frame on TCP: %s", e.bytes)
			}
		}
	}
	if bad != 1 || len(rows) != 4 {
		t.Errorf("%d messages, %d malformed: want 4 and 1", len(rows), bad)
	}
	if got := in.metric(`gelf_messages_dropped_total{reason="empty"}`); got != "0" {
		t.Errorf("TCP separators counted as empty: %s", got)
	}
}

// SPEC §1.2, §7: there is no filter -- every level, on every transport,
// reaches the stream, and nothing is counted as reason="filter" (the label does
// not even exist here).
func TestEveryLevelReachesTheStream(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--listen.http=127.0.0.1:0")
	url := "http://" + in.app.TransportAddr("http") + "/gelf"
	sendNUL(t, in.app.TransportAddr("tcp"), gdoc("tcp-crit", 2), gdoc("tcp-debug", 7))
	sendUDP(t, in.app.TransportAddr("udp"), gdoc("udp-warning", 4))
	sendUDP(t, in.app.TransportAddr("udp"), `{"version":"1.1","host":"h","short_message":"udp-default"}`)
	for _, d := range []string{gdoc("http-emerg", 0), gdoc("http-debug", 7)} {
		if code, _ := postGELF(t, nil, url, "", []byte(d)); code != http.StatusAccepted {
			t.Errorf("POST = %d, want 202", code)
		}
	}
	envs := byMessage(t, f.waitMsgs(6))
	for m, level := range map[string]int{"tcp-crit": 2, "tcp-debug": 7, "udp-warning": 4, "udp-default": 6,
		"http-emerg": 0, "http-debug": 7} {
		if e := messageWith(t, envs, m); e.Level != level {
			t.Errorf("%s: level %d, want %d", m, e.Level, level)
		}
	}
	if len(envs) != 6 {
		t.Errorf("messages: %v", keysOfEnv(envs))
	}
	if body, err := in.scrapeRaw(); err != nil || strings.Contains(body, `reason="filter"`) {
		t.Error(`/metrics exposes reason="filter" on a server without a filter`)
	}
	if got := in.app.metrics.MessagesAcceptedTotal[obs.TransportHTTP].Load(); got != 2 {
		t.Errorf("accepted{http} = %d, want 2", got)
	}
}
