package app

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
)

// The GELF transports through the whole app (SPEC §4, §5): what each receiver
// test proves in isolation, proved again with the receiver the app builds from
// the configuration, the converter the drain runs and a real destination.

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
// whatever its Content-Encoding (or none, detected by the bytes), and the row
// in gelf_events carries the transport and the real container; the refusals
// that will never pass are 4xx and write nothing. convert.max_message_bytes
// reaches the HTTP receiver: a document above it is 413.
func TestHTTPPostEndToEnd(t *testing.T) {
	in := start(t, t.TempDir(), "--listen.http=127.0.0.1:0", "--convert.max_message_bytes=1KiB")
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

	envs := byMessage(t, in.waitRows(5))
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
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="sqlite",transports="udp,tcp,http"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	if n := len(sqliteRows(t, in.eventsDB())); n != 5 {
		t.Errorf("%d rows after the shutdown, want the 5 accepted", n)
	}
}

// SPEC §4.5, §4.7: HTTPS uses the certificate and the client_auth of [tls].
// With client_auth=require a client without a certificate never reaches the
// handler; with one, the POST is 202 and payload.tls carries the session and
// the peer.
func TestHTTPSEndToEnd(t *testing.T) {
	p := newPKI(t)
	in := start(t, t.TempDir(), "--listen.https=127.0.0.1:0",
		"--tls.cert_file="+p.serverCert, "--tls.key_file="+p.serverKey,
		"--tls.client_ca="+p.caFile, "--tls.client_auth=require")
	url := "https://" + in.app.TransportAddr("https") + "/gelf"

	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(doc("https-anon")))
	if resp, err := p.httpsClient(nil).Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("a client without a certificate got %d under client_auth=require", resp.StatusCode)
	}
	cert := p.clientCert()
	if code, _ := postGELF(t, p.httpsClient(&cert), url, "", []byte(doc("https-cert"))); code != http.StatusAccepted {
		t.Fatalf("POST over https = %d, want 202", code)
	}
	rows := in.waitRows(1)
	e := checkRow(t, rows[0])
	if e.Payload.message() != "https-cert" || e.Payload.Transport != "https" {
		t.Fatalf("row %s", e.bytes)
	}
	tl := e.Payload.TLS
	if tl == nil || tl.Version == "" || tl.Cipher == "" || tl.PeerFingerprint == nil ||
		*tl.PeerFingerprint != fingerprint(p.clientDER) || tl.PeerSubject == nil ||
		!strings.Contains(*tl.PeerSubject, "CN=gelf-client-01.intranet") {
		t.Errorf("payload.tls = %+v", tl)
	}
	if eventually(200*time.Millisecond, func() bool { return len(sqliteRows(t, in.eventsDB())) > 1 }) {
		t.Error("the anonymous request was written")
	}
}

// SPEC §5.2, §5.4: a gzip document in four chunks, sent out of order with a
// duplicate, becomes ONE row with chunks=4 and compression=gzip; a zlib
// document in a single datagram is detected by its bytes; a single-chunk
// message has chunks=1. The duplicate is counted, not dropped.
func TestUDPChunkedGzipEndToEnd(t *testing.T) {
	in := start(t, t.TempDir())
	udp := in.app.TransportAddr("udp")
	body := []byte(doc("udp-chunked " + strings.Repeat("and compressed ", 200)))
	parts := gelfChunks(0x0102030405060708, gzipOf(t, body), 4)
	sendDatagrams(t, udp, parts[3], parts[1], parts[1], parts[0], parts[2])
	sendDatagrams(t, udp, zlibOf(t, []byte(gdoc("udp-zlib", 2))))
	sendDatagrams(t, udp, gelfChunks(42, []byte(gdoc("udp-one-chunk", 1)), 1)...)

	envs := byMessage(t, in.waitRows(3))
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
	in := start(t, t.TempDir())
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

	rows := in.waitRows(4)
	envs := byMessage(t, rows)
	for _, m := range []string{"tcp-1", "tcp-2", "tcp-eof"} {
		if e := messageWith(t, envs, m); e.Payload.Transport != "tcp" || e.Payload.Format != "gelf" || e.Payload.Size != len(doc(m)) {
			t.Errorf("%s: %s", m, e.bytes)
		}
	}
	var bad int
	for _, r := range rows {
		if e := checkRow(t, r); e.Payload.Format == "malformed" {
			bad++
			if string(e.Payload.rawBytes()) != "nao e json \xff" || e.Payload.RawEnc != "base64" {
				t.Errorf("the malformed frame on TCP: %s", e.bytes)
			}
		}
	}
	if bad != 1 || len(rows) != 4 {
		t.Errorf("%d rows, %d malformed: want 4 and 1", len(rows), bad)
	}
	if got := in.metric(`gelf_messages_dropped_total{reason="empty"}`); got != "0" {
		t.Errorf("TCP separators counted as empty: %s", got)
	}
}

// SPEC §7: filter.levels narrows what min_level lets through, on every
// transport; a filtered HTTP request is still 202 (the server took it), and
// counts reason="filter".
func TestLevelFilterListOnEveryTransport(t *testing.T) {
	in := start(t, t.TempDir(), "--listen.http=127.0.0.1:0", "--filter.min_level=warning", "--filter.levels=crit,4")
	url := "http://" + in.app.TransportAddr("http") + "/gelf"
	sendNUL(t, in.app.TransportAddr("tcp"), gdoc("tcp-crit", 2), gdoc("tcp-err", 3), gdoc("tcp-info", 6))
	sendUDP(t, in.app.TransportAddr("udp"), gdoc("udp-warning", 4))
	sendUDP(t, in.app.TransportAddr("udp"), `{"version":"1.1","host":"h","short_message":"udp-default"}`)
	for _, d := range []string{gdoc("http-crit", 2), gdoc("http-debug", 7)} {
		if code, _ := postGELF(t, nil, url, "", []byte(d)); code != http.StatusAccepted {
			t.Errorf("POST = %d, want 202 (filtered or not)", code)
		}
	}
	waitFor(t, 10*time.Second, "three rows and four filtered", func() bool {
		return in.app.metrics.MessagesDroppedTotal[obs.DropFilter].Load() == 4 &&
			sqliteCount(t, in.eventsDB(), "gelf_events") == 3
	})
	envs := byMessage(t, sqliteRows(t, in.eventsDB()))
	for _, m := range []string{"tcp-crit", "udp-warning", "http-crit"} {
		messageWith(t, envs, m)
	}
	if len(envs) != 3 {
		t.Errorf("rows: %v", keysOfEnv(envs))
	}
}
