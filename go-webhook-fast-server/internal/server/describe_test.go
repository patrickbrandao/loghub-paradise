package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/jlhcheck"
)

// The payload of SPEC §3.2 is resolved by describe() from the request, the
// socket and the trust decision of §5. These tests drive the whole handler and
// read the result back from the envelope it wrote, through the contract oracle:
// what is checked is what every destination receives.

// wantNet is what one request must produce in network, host and url.
type wantNet struct {
	version            int
	src, dst           string
	sport, dport       int
	sec                string
	fp2, fp3, fp4      string
	schema, host, url  string
	peerAddr           string
	peerPort           int
	srcSource, proto   string
	headerHost         string // the host entry of headers; "" = none
	extraHeader, value string // one more header that must be listed as it arrived
}

func checkNet(t *testing.T, e *jlhcheck.Envelope, w wantNet) {
	t.Helper()
	p := &e.Payload
	n := &p.Network
	if n.IP.Version != w.version || n.IP.Src != w.src || n.IP.Dst != w.dst {
		t.Errorf("ip = v%d %s -> %s, want v%d %s -> %s", n.IP.Version, n.IP.Src, n.IP.Dst, w.version, w.src, w.dst)
	}
	if n.L4.SPort != w.sport || n.L4.DPort != w.dport {
		t.Errorf("l4 = %d -> %d, want %d -> %d", n.L4.SPort, n.L4.DPort, w.sport, w.dport)
	}
	if n.Sec.Mode != w.sec || n.Sec.FP2 != w.fp2 || n.Sec.FP3 != w.fp3 || n.Sec.FP4 != w.fp4 {
		t.Errorf("sec = %+v, want mode %s fp2 %q fp3 %q fp4 %q", n.Sec, w.sec, w.fp2, w.fp3, w.fp4)
	}
	if p.Schema != w.schema || p.Host != w.host || p.URL != w.url {
		t.Errorf("schema %s host %s url %s; want %s %s %s", p.Schema, p.Host, p.URL, w.schema, w.host, w.url)
	}
	// network.meta keeps the TCP truth whatever the headers said (invariant
	// §17.8), and says where src came from.
	for key, want := range map[string]any{"peer-addr": w.peerAddr, "peer-port": float64(w.peerPort),
		"src-source": w.srcSource, "http-proto": w.proto} {
		if got, ok := p.Meta(key); !ok || got != want {
			t.Errorf("meta %s = %v, want %v", key, got, want)
		}
	}
	var hostEntries []string
	extra := w.extraHeader == ""
	for _, h := range p.Headers {
		if h.Name == "host" {
			hostEntries = append(hostEntries, h.Value)
		}
		if h.Name == w.extraHeader && h.Value == w.value {
			extra = true
		}
	}
	if w.headerHost == "" && len(hostEntries) != 0 || w.headerHost != "" &&
		(len(hostEntries) != 1 || hostEntries[0] != w.headerHost) {
		t.Errorf("host entries in headers = %q, want %q", hostEntries, w.headerHost)
	}
	if !extra {
		t.Errorf("headers lack %s: %q as it arrived: %+v", w.extraHeader, w.value, p.Headers)
	}
}

// proxyHeaders are everything a trusted proxy may vouch for, with the values
// the nearest proxy wrote LAST: a header the proxy appends to (or a second
// line) must be read from its end.
func proxyHeaders(r *http.Request) {
	r.Header.Set("X-Forwarded-For", "203.0.113.9")
	r.Header.Add("X-Forwarded-Proto", "http")  // from further out
	r.Header.Add("X-Forwarded-Proto", "https") // the nearest proxy
	r.Header.Set("X-Forwarded-Host", "evil.example, API.Exemplo.com")
	r.Header.Set("X-Forwarded-Port", "8443")
	r.Header.Set("X-Client-Port", "40001")
	r.Header.Set("X-Server-Addr", "198.51.100.1")
	// Comma-free fingerprints (the JA3 in its MD5 form, which many proxies
	// send): the comma-bearing forms are TestFingerprintsKeepTheirCommas.
	r.Header.Set("X-Http2-Fingerprint", "52d84b11737d980aef856699f885ca86")
	r.Header.Set("X-Tls-Ja3", "e7d705a3286e19ea42f587b344ee6865")
	r.Header.Set("X-Tls-Ja4", "t13d1516h2_8daaf6152771_02713d6af862")
}

func TestDescribe(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	cases := []struct {
		name        string
		peer, local string
		build       func() *http.Request
		want        wantNet
	}{
		{
			// The client is the TCP peer: the socket says where it aimed.
			name: "tcp client", peer: "198.51.100.20:40000", local: "192.0.2.10:8443",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook/x?b=2&a=1", strings.NewReader("x"))
				r.Host = "Hooks.Example.COM:8443"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8443,
				sec: "none", schema: "http", host: "hooks.example.com",
				url: "http://hooks.example.com:8443/hook/x?b=2&a=1", peerAddr: "198.51.100.20", peerPort: 40000,
				srcSource: "tcp", proto: "HTTP/1.1", headerHost: "Hooks.Example.COM:8443"},
		},
		{
			// A trusted proxy that names no client: src is the peer, but where
			// the client aimed is still the proxy's word -- X-Server-Addr and
			// X-Forwarded-Port --, never the proxy->server socket, or the url
			// of an https client would carry the internal port.
			name: "trusted proxy, no client header", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/x", strings.NewReader("x"))
				r.Host = "pub.example"
				r.Header.Set("X-Forwarded-Proto", "https")
				r.Header.Set("X-Forwarded-Port", "443")
				r.Header.Set("X-Server-Addr", "198.51.100.1")
				return r
			},
			want: wantNet{version: 4, src: "10.0.0.7", dst: "198.51.100.1", sport: 51234, dport: 443,
				sec: "proxy", schema: "https", host: "pub.example", url: "https://pub.example/x",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "pub.example"},
		},
		{
			// Same, with TLS ended at the proxy and no port at all: the
			// socket's 8080 is the plain hop, so dport follows the schema.
			name: "trusted proxy, no client header, no port", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/x", strings.NewReader("x"))
				r.Host = "pub.example"
				r.Header.Set("X-Forwarded-Proto", "https")
				return r
			},
			want: wantNet{version: 4, src: "10.0.0.7", dst: "10.0.0.1", sport: 51234, dport: 443,
				sec: "proxy", schema: "https", host: "pub.example", url: "https://pub.example/x",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "pub.example"},
		},
		{
			// Lowercasing is ASCII only: the Kelvin sign is no 'k', so the
			// host is not a plain name and the payload falls back to dst.
			name: "kelvin sign host", peer: "198.51.100.20:40000", local: "192.0.2.10:80",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/k", strings.NewReader("x"))
				r.Host = "\u212Aexample.com"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 80,
				sec: "none", schema: "http", host: "192.0.2.10", url: "http://192.0.2.10/k",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "\u212Aexample.com"},
		},
		{
			// Behind a trusted proxy the local socket describes the
			// proxy->server hop and is never used; every value comes from the
			// last element of the last line the proxy wrote.
			name: "trusted proxy", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "internal.svc:8080"
				proxyHeaders(r)
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "198.51.100.1", sport: 40001, dport: 8443,
				sec: "proxy", fp2: "52d84b11737d980aef856699f885ca86",
				fp3: "e7d705a3286e19ea42f587b344ee6865", fp4: "t13d1516h2_8daaf6152771_02713d6af862",
				schema: "https", host: "api.exemplo.com", url: "https://api.exemplo.com:8443/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1",
				headerHost: "internal.svc:8080", extraHeader: "x-forwarded-host", value: "evil.example, API.Exemplo.com"},
		},
		{
			// The proxy sent only the client: unknown dst is the unspecified
			// address, unknown sport is 0, dport follows the schema, and the
			// default port is left out of the url.
			name: "trusted proxy, nothing but the client", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "api.exemplo.com"
				r.Header.Set("X-Forwarded-For", "203.0.113.9")
				r.Header.Set("X-Forwarded-Proto", "https")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "0.0.0.0", sport: 0, dport: 443,
				sec: "proxy", schema: "https", host: "api.exemplo.com", url: "https://api.exemplo.com/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1",
				headerHost: "api.exemplo.com"},
		},
		{
			// No X-Forwarded-Port: dport is the effective host's port.
			name: "trusted proxy, port from the forwarded host", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "internal.svc"
				r.Header.Set("X-Real-IP", "203.0.113.50")
				r.Header.Set("X-Forwarded-Host", "api.exemplo.com:9080")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.50", dst: "0.0.0.0", sport: 0, dport: 9080,
				sec: "none", schema: "http", host: "api.exemplo.com", url: "http://api.exemplo.com:9080/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xri", proto: "HTTP/1.1",
				headerHost: "internal.svc"},
		},
		{
			// https on 443, from X-Forwarded-Port and from the host alike, is
			// the default and is never written.
			name: "trusted proxy, default https port elided", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "internal.svc"
				r.Header.Set("X-Forwarded-For", "203.0.113.9")
				r.Header.Set("X-Forwarded-Proto", "https")
				r.Header.Set("X-Forwarded-Host", "api.exemplo.com:443")
				r.Header.Set("X-Forwarded-Port", "443")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "0.0.0.0", sport: 0, dport: 443,
				sec: "proxy", schema: "https", host: "api.exemplo.com", url: "https://api.exemplo.com/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1",
				headerHost: "internal.svc"},
		},
		{
			// The same headers from a peer that is NOT trusted are attacker
			// input: none of them is believed (invariant §17.5), and they stay
			// in headers as evidence.
			name: "untrusted peer", peer: "198.51.100.20:40000", local: "192.0.2.10:8443",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "hooks.example.com"
				proxyHeaders(r)
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8443,
				sec: "none", schema: "http", host: "hooks.example.com", url: "http://hooks.example.com:8443/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "hooks.example.com", extraHeader: "x-tls-ja4", value: "t13d1516h2_8daaf6152771_02713d6af862"},
		},
		{
			// src and dst share a family: an IPv4 X-Server-Addr cannot be the
			// destination of an IPv6 client, so dst is ::.
			name: "ipv6 client, ipv4 X-Server-Addr", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "example.com"
				r.Header.Set("X-Forwarded-For", "2001:db8::9")
				r.Header.Set("X-Server-Addr", "198.51.100.1")
				r.Header.Set("X-Client-Port", "40002")
				return r
			},
			want: wantNet{version: 6, src: "2001:db8::9", dst: "::", sport: 40002, dport: 80,
				sec: "none", schema: "http", host: "example.com", url: "http://example.com/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1", headerHost: "example.com"},
		},
		{
			name: "ipv4 client, ipv6 X-Server-Addr", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "example.com"
				r.Header.Set("X-Forwarded-For", "203.0.113.9")
				r.Header.Set("X-Server-Addr", "2001:db8::1")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "0.0.0.0", sport: 0, dport: 80,
				sec: "none", schema: "http", host: "example.com", url: "http://example.com/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1", headerHost: "example.com"},
		},
		{
			// An IPv6 host loses its brackets in host and gets them back in
			// the url.
			name: "ipv6 tcp client", peer: "[2001:db8::20]:40000", local: "[2001:db8::1]:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "[2001:DB8::1]:8080"
				return r
			},
			want: wantNet{version: 6, src: "2001:db8::20", dst: "2001:db8::1", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "2001:db8::1", url: "http://[2001:db8::1]:8080/hook",
				peerAddr: "2001:db8::20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "[2001:DB8::1]:8080"},
		},
		{
			// A Host without a port takes dport into the url...
			name: "host without port", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "example.com"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "example.com", url: "http://example.com:8080/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1", headerHost: "example.com"},
		},
		{
			// ...unless it is the schema's default.
			name: "host without port on 80", peer: "198.51.100.20:40000", local: "192.0.2.10:80",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "example.com"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 80,
				sec: "none", schema: "http", host: "example.com", url: "http://example.com/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1", headerHost: "example.com"},
		},
		{
			name: "host with the default port", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "example.com:80"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "example.com", url: "http://example.com/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1", headerHost: "example.com:80"},
		},
		{
			// HTTP/1.0 without Host: the host is the address the client aimed
			// at, and there is no host entry to put back in headers.
			name: "http/1.0 without host", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = ""
				r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/1.0", 1, 0
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "192.0.2.10", url: "http://192.0.2.10:8080/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.0"},
		},
		{
			name: "http/1.0 without host, ipv6", peer: "[2001:db8::20]:40000", local: "[2001:db8::1]:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = ""
				r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/1.0", 1, 0
				return r
			},
			want: wantNet{version: 6, src: "2001:db8::20", dst: "2001:db8::1", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "2001:db8::1", url: "http://[2001:db8::1]:8080/hook",
				peerAddr: "2001:db8::20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.0"},
		},
		{
			// Behind a proxy that names neither the host nor the server
			// address, the host is the unspecified address: still a URL.
			name: "http/1.0 without host, behind a proxy", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = ""
				r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/1.0", 1, 0
				r.Header.Set("X-Forwarded-For", "203.0.113.9")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "0.0.0.0", sport: 0, dport: 80,
				sec: "none", schema: "http", host: "0.0.0.0", url: "http://0.0.0.0/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.0"},
		},
		// SPEC §3.2, host: an IP or a name of [a-z0-9._-]. net/http accepts the
		// RFC 3986 sub-delims in Host, which no URL authority can carry, so
		// such a host falls back to dst -- and its port with it -- while the
		// raw value stays in headers.
		{
			name: "host with a sub-delim", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "a*b"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "192.0.2.10", url: "http://192.0.2.10:8080/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1", headerHost: "a*b"},
		},
		{
			name: "host with a sub-delim and a port", peer: "198.51.100.20:40000", local: "192.0.2.10:80",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "x$y:9999"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 80,
				sec: "none", schema: "http", host: "192.0.2.10", url: "http://192.0.2.10/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1", headerHost: "x$y:9999"},
		},
		{
			name: "forwarded host with a sub-delim", peer: "10.0.0.7:51234", local: "10.0.0.1:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "internal.svc"
				r.Header.Set("X-Forwarded-For", "203.0.113.9")
				r.Header.Set("X-Server-Addr", "198.51.100.1")
				r.Header.Set("X-Forwarded-Host", "a*b")
				return r
			},
			want: wantNet{version: 4, src: "203.0.113.9", dst: "198.51.100.1", sport: 0, dport: 80,
				sec: "none", schema: "http", host: "198.51.100.1", url: "http://198.51.100.1/hook",
				peerAddr: "10.0.0.7", peerPort: 51234, srcSource: "xff", proto: "HTTP/1.1",
				headerHost: "internal.svc", extraHeader: "x-forwarded-host", value: "a*b"},
		},
		{
			// The controls: a name with '_' and uppercase, and an IPv4
			// literal, are plain hosts and are kept.
			name: "plain host kept", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "My_Host.Example-1.com:9000"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "my_host.example-1.com", url: "http://my_host.example-1.com:9000/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "My_Host.Example-1.com:9000"},
		},
		{
			name: "ip literal host kept", peer: "198.51.100.20:40000", local: "192.0.2.10:8080",
			build: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
				r.Host = "203.0.113.200:9000"
				return r
			},
			want: wantNet{version: 4, src: "198.51.100.20", dst: "192.0.2.10", sport: 40000, dport: 8080,
				sec: "none", schema: "http", host: "203.0.113.200", url: "http://203.0.113.200:9000/hook",
				peerAddr: "198.51.100.20", peerPort: 40000, srcSource: "tcp", proto: "HTTP/1.1",
				headerHost: "203.0.113.200:9000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{}
			s, _ := testServerTrusting(t, stub, Config{}, trusted)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, onSocket(tc.build(), tc.peer, tc.local))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d (%s)", w.Code, w.Body.String())
			}
			checkNet(t, envelopeOf(t, s, stub), tc.want)
		})
	}
}

// SPEC §3.2, network.sec: fp3 is the JA3 of X-TLS-JA3 and fp2 the HTTP/2
// fingerprint of X-HTTP2-Fingerprint, as the trusted proxy wrote them. Both
// formats are comma-separated by definition (JA3 is five comma-joined fields;
// the Akamai HTTP/2 fingerprint lists pseudo-headers as m,a,s,p), so reading
// them as a comma list and keeping the last element destroys every one: the
// schema's own example fp3 is "771,4865-4866-4867-49195,0-23-65281-10-11,...".
// Only the last LINE may be taken -- the one the nearest proxy wrote.
func TestFingerprintsKeepTheirCommas(t *testing.T) {
	const (
		ja3 = "771,4865-4866-4867-49195,0-23-65281-10-11,29-23-24,0"
		h2  = "1:65536;4:6291456|15663105|0|m,a,s,p"
		ja4 = "t13d1516h2_8daaf6152771_02713d6af862"
	)
	stub := &stubRing{}
	s, _ := testServerTrusting(t, stub, Config{}, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
	r.Host = "api.exemplo.com"
	r.Header.Set("X-Forwarded-For", "203.0.113.9")
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Tls-Ja3", ja3)
	r.Header.Set("X-Http2-Fingerprint", h2)
	r.Header.Set("X-Tls-Ja4", ja4)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, onSocket(r, "10.0.0.7:51234", "10.0.0.1:8080"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	sec := envelopeOf(t, s, stub).Payload.Network.Sec
	if sec.FP4 != ja4 {
		t.Errorf("fp4 = %q, want %q", sec.FP4, ja4)
	}
	if sec.FP3 == "0" || sec.FP2 == "p" {
		t.Fatal("regressão de §3.2: describe.go reads X-TLS-JA3 and X-HTTP2-Fingerprint with lastElem, which keeps " +
			"only the text after the last comma: fp3 = " + strconv.Quote(sec.FP3) + ", fp2 = " + strconv.Quote(sec.FP2))
	}
	if sec.FP3 != ja3 || sec.FP2 != h2 {
		t.Errorf("fp3 = %q, fp2 = %q; want %q and %q", sec.FP3, sec.FP2, ja3, h2)
	}
}

// SPEC §3.2, host: the port of a host that falls back to dst is not the
// client's either. Behind a trusted proxy it would still reach the url
// through dport, which is derived from the effective host's port before the
// fallback; the fallback host has no port, so dport follows the schema.
func TestFallbackHostPortDoesNotReachTheURL(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServerTrusting(t, stub, Config{}, []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
	r.Host = "internal.svc"
	r.Header.Set("X-Forwarded-For", "203.0.113.9")
	r.Header.Set("X-Server-Addr", "198.51.100.1")
	r.Header.Set("X-Forwarded-Host", "a*b:9443")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, onSocket(r, "10.0.0.7:51234", "10.0.0.1:8080"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	e := envelopeOf(t, s, stub)
	if e.Payload.Host != "198.51.100.1" {
		t.Fatalf("host = %q, want the dst fallback", e.Payload.Host)
	}
	if e.Payload.URL == "http://198.51.100.1:9443/hook" && e.Payload.Network.L4.DPort == 9443 {
		t.Fatal("regressão de §3.2: describe.go derives dport from the port of an X-Forwarded-Host that is then " +
			"refused by plainHost, so the refused host's port reaches the url as dport")
	}
	if e.Payload.URL != "http://198.51.100.1/hook" || e.Payload.Network.L4.DPort != 80 {
		t.Errorf("url %s dport %d; want http://198.51.100.1/hook and 80", e.Payload.URL, e.Payload.Network.L4.DPort)
	}
}

// SPEC §3.2, path and url: what arrived, not a cleaned-up version the
// operator's proxy rules never saw -- percent-encoding kept, dot segments and
// double slashes kept, and the raw query in the url beside its decoded form.
func TestPathAndQueryAreNotNormalised(t *testing.T) {
	cases := []struct {
		target, path, url, query string
	}{
		{"/hook", "/hook", "http://example.com/hook", `{}`},
		{"/hook?a=1&b=2", "/hook", "http://example.com/hook?a=1&b=2", `{"a":"1","b":"2"}`},
		{"/hook?", "/hook", "http://example.com/hook", `{}`},
		{"/a/../b?x=%2F", "/a/../b", "http://example.com/a/../b?x=%2F", `{"x":"/"}`},
		{"//double//slash", "//double//slash", "http://example.com//double//slash", `{}`},
		{"/a%20b%2Fc", "/a%20b%2Fc", "http://example.com/a%20b%2Fc", `{}`},
		{"/a?b?c", "/a", "http://example.com/a?b?c", `{"b?c":""}`},
	}
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			stub := &stubRing{}
			s, _ := testServer(t, stub, Config{})
			r := httptest.NewRequest("POST", tc.target, strings.NewReader("x"))
			r.Host = "example.com"
			w := httptest.NewRecorder()
			s.ServeHTTP(w, onSocket(r, "198.51.100.20:40000", "192.0.2.10:80"))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}
			e := envelopeOf(t, s, stub)
			q, _ := json.Marshal(e.Payload.Query)
			if e.Payload.Path != tc.path || e.Payload.URL != tc.url || string(q) != tc.query {
				t.Errorf("path %q url %q query %s; want %q %q %s", e.Payload.Path, e.Payload.URL, q,
					tc.path, tc.url, tc.query)
			}
		})
	}
}

// ---------------------------------------------------------- a real socket

// rawExchange writes one request on a fresh connection and reads the answer.
func rawExchange(t *testing.T, addr, request string) (*http.Response, string, netip.AddrPort) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("reading the answer to %q: %v", request, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(body), netip.MustParseAddrPort(c.LocalAddr().String())
}

// What httptest cannot show: the local address comes from net/http's own
// context key, an HTTP/1.0 request arrives without Host, net/http lets a Host
// with sub-delims through, and nothing in front of the handler answers in its
// place.
func TestDescribeOnARealSocket(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServer(t, stub, Config{Bind: "127.0.0.1:0", Listeners: 1})
	if err := s.Listen(context.Background()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })
	addr := s.Addr()
	srvPort, _ := strconv.Atoi(addr[strings.LastIndexByte(addr, ':')+1:])

	t.Run("http/1.0 without host", func(t *testing.T) {
		resp, _, client := rawExchange(t, addr, "POST /hook?a=1 HTTP/1.0\r\nContent-Length: 2\r\n\r\nhi")
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-LogHub-UUID") == "" {
			t.Fatalf("status %d, uuid %q", resp.StatusCode, resp.Header.Get("X-LogHub-UUID"))
		}
		checkNet(t, envelopeOf(t, s, stub), wantNet{version: 4, src: "127.0.0.1", dst: "127.0.0.1",
			sport: int(client.Port()), dport: srvPort, sec: "none", schema: "http", host: "127.0.0.1",
			url: "http://127.0.0.1:" + strconv.Itoa(srvPort) + "/hook?a=1", peerAddr: "127.0.0.1",
			peerPort: int(client.Port()), srcSource: "tcp", proto: "HTTP/1.0"})
	})

	t.Run("host with a sub-delim", func(t *testing.T) {
		resp, _, client := rawExchange(t, addr,
			"POST /hook HTTP/1.1\r\nHost: a*b\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status %d; net/http must hand this Host to the handler", resp.StatusCode)
		}
		checkNet(t, envelopeOf(t, s, stub), wantNet{version: 4, src: "127.0.0.1", dst: "127.0.0.1",
			sport: int(client.Port()), dport: srvPort, sec: "none", schema: "http", host: "127.0.0.1",
			url: "http://127.0.0.1:" + strconv.Itoa(srvPort) + "/hook", peerAddr: "127.0.0.1",
			peerPort: int(client.Port()), srcSource: "tcp", proto: "HTTP/1.1", headerHost: "a*b"})
	})

	t.Run("lowercase method", func(t *testing.T) {
		resp, body, _ := rawExchange(t, addr,
			"post /hook HTTP/1.1\r\nHost: example.com\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi")
		if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("X-LogHub-UUID") != "" ||
			!strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
			t.Errorf("status %d, uuid %q, type %q, body %q; want a plain 405",
				resp.StatusCode, resp.Header.Get("X-LogHub-UUID"), resp.Header.Get("Content-Type"), body)
		}
	})

	t.Run("OPTIONS *", func(t *testing.T) {
		resp, body, _ := rawExchange(t, addr, "OPTIONS * HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
		if resp.StatusCode == http.StatusOK && body == "" && resp.Header.Get("X-LogHub-UUID") == "" {
			t.Fatal("regressão de §4.2: net/http answers OPTIONS * itself (200, empty) before the handler: " +
				"newHTTPServer does not set DisableGeneralOptionsHandler, so SPEC §4.2's 400 never happens")
		}
		if resp.StatusCode != http.StatusBadRequest || resp.Header.Get("X-LogHub-UUID") != "" ||
			!strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
			t.Errorf("status %d, uuid %q, type %q, body %q; want a plain 400",
				resp.StatusCode, resp.Header.Get("X-LogHub-UUID"), resp.Header.Get("Content-Type"), body)
		}
	})
}
