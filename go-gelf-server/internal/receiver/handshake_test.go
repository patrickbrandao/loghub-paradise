package receiver

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
)

// SPEC §4.7, §14.1 (§22.1 item 2, fixed 2026-11-07): a failed HTTPS handshake
// is counted in gelf_tls_handshake_errors_total{transport="https"}, never in
// the tls series, and a good handshake counts nothing. Before the fix the
// http.Server's ErrorLog was io.Discard and every one of these was invisible.
func TestHTTPSHandshakeErrorsAreCounted(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	s := startHTTPS(t, h, p, HTTPConfig{}, TLSConfig{ClientAuth: ClientAuthRequire, ClientCAFile: p.caFile})
	https := func() uint64 { return h.m.TLSHandshakeErrorsTotal[obs.TransportHTTPS].Load() }

	// Control first: a valid client handshakes, posts and counts nothing.
	resp := post(t, httpsClient(p, &p.client), "https://"+s.Addr()+GELFPath, "", []byte(doc("good")))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if n := https(); n != 0 {
		t.Fatalf("a good handshake counted %d errors", n)
	}

	// A client certificate from an unknown CA, refused by require.
	if _, err := dialTLS(t, s.Addr(), p, &p.stranger, 0); err == nil {
		t.Fatal("require accepted a certificate from an unknown CA")
	}
	waitFor(t, "the stranger's handshake error", func() bool { return https() == 1 })

	// A client that does not trust the server certificate aborts the
	// handshake with an alert: the server sees the failure too.
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", s.Addr(),
		&tls.Config{ServerName: "127.0.0.1", Certificates: []tls.Certificate{p.client}})
	if err == nil {
		c.Close()
		t.Fatal("the client trusted a server certificate from an unknown CA")
	}
	waitFor(t, "the distrusting client's handshake error", func() bool { return https() == 2 })

	// Plain HTTP to the HTTPS port: net/http answers 400 and reports it as a
	// handshake error, as the tls transport counts a plain-text client.
	if r := rawRequest(t, s.Addr(), nil, "POST /gelf HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n", "{}"); r != nil &&
		r.StatusCode != http.StatusBadRequest {
		t.Fatalf("plain HTTP to https: status %d", r.StatusCode)
	}
	waitFor(t, "the plain-text client's handshake error", func() bool { return https() == 3 })

	if n := h.m.TLSHandshakeErrorsTotal[obs.TransportTLS].Load(); n != 0 {
		t.Fatalf("https failures counted under tls: %d", n)
	}
	if n := https(); n != 3 {
		t.Fatalf("https handshake errors = %d, want 3", n)
	}
}

// The ErrorLog counts only the handshake line, only on https, and not while
// the listener is closing (a shutdown cuts handshakes in flight; not the
// peer's fault, as in Stream.handshake). Every other line is dropped silently.
func TestHTTPServerLogCountsOnlyHandshakeLines(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	s := startHTTPS(t, h, p, HTTPConfig{}, TLSConfig{})
	plain := startHTTP(t, h, HTTPConfig{})
	line := []byte("http: TLS handshake error from 192.0.2.1:5555: remote error: tls: bad certificate\n")
	https := func() uint64 { return h.m.TLSHandshakeErrorsTotal[obs.TransportHTTPS].Load() }

	serverLog{s}.Write([]byte("http: superfluous response.WriteHeader call\n"))
	serverLog{plain}.Write(line) // http has no handshake: never counted
	if n, m := https(), h.m.TLSHandshakeErrorsTotal[obs.TransportHTTP].Load(); n != 0 || m != 0 {
		t.Fatalf("a non-handshake line or the http listener counted (https %d, http %d)", n, m)
	}
	serverLog{s}.Write(line)
	if n := https(); n != 1 {
		t.Fatalf("the handshake line counted %d, want 1", n)
	}
	s.closing.Store(true)
	serverLog{s}.Write(line)
	if n := https(); n != 1 {
		t.Fatalf("a handshake cut by the shutdown was counted (%d)", n)
	}
}
