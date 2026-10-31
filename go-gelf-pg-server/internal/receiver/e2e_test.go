package receiver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/jlhcheck"
)

// envelopeOf converts one item the way the drain worker does, and checks the
// envelope against the oracle (both LogHub schemas and the semantic rules)
// and the head against the ring codec.
func envelopeOf(t *testing.T, it *item.Item) []byte {
	t.Helper()
	conv := convert.New(convert.DefaultConfig())
	env, _ := conv.Append(nil, &it.Head, it.Raw)
	if err := jlhcheck.Validate(env); err != nil {
		t.Fatalf("%s %q: %v\n%s", it.Head.Transport, it.Raw, err, env)
	}
	back, err := item.DecodeHead(item.EncodeHead(nil, &it.Head))
	if err != nil {
		t.Fatalf("head codec: %v", err)
	}
	if env2, _ := conv.Append(nil, &back, it.Raw); !bytes.Equal(env2, env) {
		t.Fatal("the JLH is not a pure function of the decoded head and raw (SPEC §3.5)")
	}
	return env
}

// payloadOf decodes the payload of an envelope.
func payloadOf(t *testing.T, env []byte) map[string]any {
	t.Helper()
	var e struct {
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(env, &e); err != nil {
		t.Fatal(err)
	}
	return e.Payload
}

// The documents every transport carries in the end-to-end test: plain, with
// fields, with a level, not JSON at all, and bytes that are not UTF-8.
var e2eDocs = []string{
	doc("hello"),
	`{"version":"1.1","host":"web-01","short_message":"with fields","full_message":"trace\nline 2",` +
		`"timestamp":1791028800.123,"level":3,"_user_id":42,"_path":"/x"}`,
	`{"short_message":"minimal"}`,
	`not json, still a document`,
	"{\"short_message\":\"caf\xe9\"}",
}

// TestEndToEndEnvelopesAreValidJLH is the receiver's half of invariant §20.5:
// every document that a real socket delivers -- datagram, chunked,
// compressed, framed, proxied, posted -- becomes an item whose head
// and raw convert into a JLH envelope that passes jlhcheck.Validate.
func TestEndToEndEnvelopesAreValidJLH(t *testing.T) {
	h := newHarness(t, 1000)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	proxy := ProxyConfig{Mode: ProxyOptional, Trusted: cidrs}

	udp := startUDP(t, h, UDPConfig{Addr: "0.0.0.0:0"})
	tcp := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{Proxy: proxy}})
	hp := startHTTP(t, h, HTTPConfig{Proxy: proxy})

	want := 0
	// UDP, to the wildcard listener: plain, gzip, zlib, and chunked.
	to := "127.0.0.1:" + portOf(t, udp.Addr())
	sendUDP(t, to, e2eDocs...)
	for _, d := range e2eDocs {
		sendUDP(t, to, string(gzipOf(t, []byte(d))), string(zlibOf(t, []byte(d))))
	}
	for i, d := range e2eDocs {
		var parts []string
		for _, c := range split(uint64(i+1), gzipOf(t, []byte(d)), 2) {
			parts = append(parts, string(c))
		}
		sendUDP(t, to, parts...)
	}
	want += 4 * len(e2eDocs)

	// TCP, NUL-framed with separators.
	c := dial(t, tcp.Addr())
	fmt.Fprint(c, "\x00"+strings.Join(e2eDocs, "\x00\n\x00")+"\x00")
	want += len(e2eDocs)

	// TCP behind a trusted balancer, PROXY v2.
	c = dial(t, tcp.Addr())
	c.Write(proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 56324, 12201, nil)))
	fmt.Fprint(c, nul(e2eDocs...))
	want += len(e2eDocs)

	// TCP behind a trusted balancer, PROXY v1 over IPv6 addresses.
	c = dial(t, tcp.Addr())
	fmt.Fprint(c, "PROXY TCP6 2001:db8::7 2001:db8::1 40000 12201\r\n")
	fmt.Fprint(c, nul(e2eDocs...))
	want += len(e2eDocs)

	// HTTP, identity, gzip and deflate.
	for _, d := range e2eDocs {
		for _, enc := range []string{"", "gzip", "deflate"} {
			if resp := post(t, httpClient(), "http://"+hp.Addr()+GELFPath, enc, encodeAs(t, enc, []byte(d))); resp.StatusCode != http.StatusAccepted {
				t.Fatalf("http %s: %d", enc, resp.StatusCode)
			}
			want++
		}
	}

	h.waitItems(t, want)
	seen := map[string]int{}
	for _, it := range h.drainRing() {
		pl := payloadOf(t, envelopeOf(t, it))
		host, _ := pl["host"].(map[string]any)
		key := fmt.Sprint(pl["transport"], "/", pl["compression"], "/", pl["chunks"])
		switch pl["transport"] {
		case "udp":
			if host["src-ip"] != "127.0.0.1" || (udp.PktinfoEnabled() && host["dst-ip"] != "127.0.0.1") {
				t.Errorf("udp host %v", host)
			}
		}
		if pl["tls"] != nil {
			t.Errorf("payload.tls in a server without TLS: %v", pl["tls"])
		}
		if pl["x-proxy"] != nil && pl["transport"] == "tcp" {
			switch host["src-ip"] {
			case "192.0.2.10":
				key += "/proxy-v2"
				if host["dst-ip"] != "198.51.100.1" {
					t.Errorf("tcp behind PROXY v2: host %v", host)
				}
			case "2001:db8::7":
				key += "/proxy-v1"
				if host["dst-ip"] != "2001:db8::1" {
					t.Errorf("tcp behind PROXY v1: host %v", host)
				}
			default:
				t.Errorf("tcp behind PROXY: host %v", host)
			}
		}
		seen[key]++
	}
	n := len(e2eDocs)
	for k, w := range map[string]int{
		"udp/<nil>/<nil>": n, "udp/gzip/<nil>": n, "udp/zlib/<nil>": n, "udp/gzip/2": n,
		"tcp/<nil>/<nil>": n, "tcp/<nil>/<nil>/proxy-v2": n, "tcp/<nil>/<nil>/proxy-v1": n,
		"http/<nil>/<nil>": n, "http/gzip/<nil>": n, "http/zlib/<nil>": n,
	} {
		if seen[k] != w {
			t.Errorf("%s: %d envelopes, want %d (all: %v)", k, seen[k], w, seen)
		}
	}
}
