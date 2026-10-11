package receiver

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
)

// The messages every transport carries in the end-to-end test: the two RFCs,
// a line with no PRI, an invalid PRI, bytes that are not UTF-8 and one long
// enough to be truncated.
var e2eMessages = []string{
	`<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 [ex@32473 a="1"] hello rfc5424`,
	`<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed for lonvick`,
	`2026-09-13 10:00:00,123 ERROR [main] com.example.App - no PRI at all`,
	`<192>an invalid PRI`,
	"<13>caf\xe9 au lait",
	"<13>" + strings.Repeat("t", 996),
}

// TestEndToEndEnvelopesAreValidJLH is the receiver's half of invariant §20.5:
// every message that a real socket delivers -- framed, truncated, addressed,
// proxied, encrypted -- becomes an item whose head and raw convert into a JLH
// envelope that passes BOTH LogHub schemas and the semantic rules
// (jlhcheck.Validate): frame -> Ingest -> ring -> convert -> Validate.
func TestEndToEndEnvelopesAreValidJLH(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 1000, "debug", nil, cfg)
	p := newPKI(t)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	proxy := ProxyConfig{Mode: ProxyOptional, Trusted: cidrs}

	udp := startUDP(t, h, UDPConfig{Addr: "0.0.0.0:0"})
	tcp := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{Proxy: proxy}})
	tlsT := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional, ClientCAFile: p.caFile,
		StreamConfig: StreamConfig{Proxy: proxy}})
	unixPath := filepath.Join(shortDir(t), "log")
	startUnix(t, h, unixPath)

	want := 0
	// UDP, to the wildcard listener, through 127.0.0.1.
	sendUDP(t, "127.0.0.1:"+portOf(t, udp.Addr()), e2eMessages...)
	want += len(e2eMessages)

	// TCP, non-transparent with CRLF and a NUL terminator.
	c := dial(t, tcp.Addr())
	for i, m := range e2eMessages {
		if strings.HasPrefix(m, "2026") || i%2 == 0 {
			fmt.Fprint(c, m+"\r\n")
		} else {
			fmt.Fprint(c, m+"\x00")
		}
	}
	want += len(e2eMessages)

	// TCP, octet-counting, with a NUL in the middle that is data.
	c = dial(t, tcp.Addr())
	fmt.Fprint(c, octet(append([]string{"<13>a\x00b"}, e2eMessages...)...))
	want += len(e2eMessages) + 1

	// TCP behind a trusted balancer, PROXY v2.
	c = dial(t, tcp.Addr())
	c.Write(proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 56324, 514, nil)))
	fmt.Fprint(c, strings.Join(e2eMessages, "\n")+"\n")
	want += len(e2eMessages)

	// TLS with a client certificate, behind a PROXY v1 header.
	raw := dial(t, tlsT.Addr())
	fmt.Fprint(raw, "PROXY TCP6 2001:db8::7 2001:db8::1 40000 6514\r\n")
	tc := tls.Client(raw, &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1", Certificates: []tls.Certificate{p.client}})
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(tc, octet(e2eMessages...))
	want += len(e2eMessages)

	// unix datagram.
	uc, err := net.Dial("unixgram", unixPath)
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	for _, m := range e2eMessages {
		if _, err := uc.Write([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	want += len(e2eMessages)

	h.waitItems(t, want)
	items := h.drainRing()

	conv := convert.New(convert.Config{
		DefaultPriority: cfg.DefaultPriority, IncludeRaw: true, MaxMessageBytes: cfg.MaxMessageBytes,
		SDMaxElements: 32, SDMaxParams: 64,
	})
	seen := map[string]int{}
	for _, it := range items {
		env, _ := conv.Append(nil, &it.Head, it.Raw)
		if err := jlhcheck.Validate(env); err != nil {
			t.Fatalf("%s/%s %q: %v\n%s", it.Head.Transport, it.Head.Framing, it.Raw, err, env)
		}
		// The head must survive the ring codec too (ring=sqlite, ring=redis,
		// the emergency dump: SPEC §3.4).
		back, err := item.DecodeHead(item.EncodeHead(nil, &it.Head))
		if err != nil {
			t.Fatalf("head codec: %v", err)
		}
		if env2, _ := conv.Append(nil, &back, it.Raw); string(env2) != string(env) {
			t.Fatal("the JLH is not a pure function of the decoded head and raw (SPEC §3.5)")
		}

		var e struct {
			Payload struct {
				Host struct {
					SrcIP string `json:"src-ip"`
					DstIP string `json:"dst-ip"`
				} `json:"host"`
				Transport string            `json:"transport"`
				Framing   string            `json:"framing"`
				TLS       map[string]string `json:"tls"`
				XProxy    map[string]any    `json:"x-proxy"`
				Size      int               `json:"size"`
				Parse     struct {
					Warnings []string `json:"warnings"`
				} `json:"parse"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(env, &e); err != nil {
			t.Fatal(err)
		}
		pl := e.Payload
		key := pl.Transport + "/" + pl.Framing
		switch {
		case pl.Transport == "udp":
			if pl.Host.SrcIP != "127.0.0.1" || (udp.PktinfoEnabled() && pl.Host.DstIP != "127.0.0.1") {
				t.Errorf("udp host %+v", pl.Host)
			}
		case pl.Transport == "unix":
			if pl.Host.SrcIP != "" || pl.Host.DstIP != "" {
				t.Errorf("unix host %+v", pl.Host)
			}
		case pl.Transport == "tls":
			if pl.TLS["version"] == "" || pl.TLS["peer-fingerprint"] != Fingerprint(p.clientLeaf.Raw) {
				t.Errorf("tls %+v", pl.TLS)
			}
			if pl.Host.SrcIP != "2001:db8::7" || pl.XProxy["peer-ip"] != "127.0.0.1" {
				t.Errorf("tls behind PROXY: host %+v x-proxy %v", pl.Host, pl.XProxy)
			}
		case pl.XProxy != nil:
			key += "/proxy"
			if pl.Host.SrcIP != "192.0.2.10" || pl.Host.DstIP != "198.51.100.1" || pl.XProxy["peer-ip"] != "127.0.0.1" {
				t.Errorf("tcp behind PROXY: host %+v x-proxy %v", pl.Host, pl.XProxy)
			}
		}
		if len(it.Raw) == 480 && pl.Size == 1000 {
			if !contains(pl.Parse.Warnings, "truncated") {
				t.Errorf("%s: a truncated message without the warning: %v", key, pl.Parse.Warnings)
			}
			seen[key+"/truncated"]++
		}
		seen[key]++
	}
	for _, k := range []string{"udp/datagram", "unix/datagram", "tcp/non-transparent", "tcp/octet-counting",
		"tcp/non-transparent/proxy", "tls/octet-counting"} {
		if seen[k] == 0 {
			t.Errorf("no envelope for %s", k)
		}
		if seen[k+"/truncated"] != 1 {
			t.Errorf("%s: %d truncated envelopes, want 1", k, seen[k+"/truncated"])
		}
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
