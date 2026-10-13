package receiver

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlhcheck"
)

// The messages every session carries in the end-to-end test: the two RFCs, a
// line with no PRI, an invalid PRI, bytes that are not UTF-8, a LF and a NUL
// inside DATA (RELP frames by length: both are data), and one long enough to
// be truncated.
var e2eMessages = []string{
	`<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 [ex@32473 a="1"] hello rfc5424`,
	`<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed for lonvick`,
	`2026-09-13 10:00:00,123 ERROR [main] com.example.App - no PRI at all`,
	`<192>an invalid PRI`,
	"<13>caf\xe9 au lait",
	"<13>two\nlines and a \x00 NUL",
	"<13>" + strings.Repeat("t", 996),
}

// TestEndToEndEnvelopesAreValidJLH is the receiver's half of invariant §20.5:
// every message a real RELP session delivers -- in clear, behind a balancer
// speaking PROXY v2 or v1 -- becomes an item whose head and raw
// convert into a JLH envelope that passes BOTH LogHub schemas and the semantic
// rules (jlhcheck.Validate), with the x-transport extension of this server:
// frame -> session -> Ingest -> ring -> convert -> Validate.
func TestEndToEndEnvelopesAreValidJLH(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 1000, cfg)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	proxy := ProxyConfig{Mode: ProxyOptional, Trusted: cidrs}

	tcp := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{Proxy: proxy}})

	want := 0
	session := func(rc *relpClient) {
		t.Helper()
		rc.open()
		ids := rc.syslog(e2eMessages...)
		got := rc.responses(len(ids))
		for _, id := range ids {
			if got[id] != "200 OK" {
				t.Fatalf("txnr %d: %q", id, got[id])
			}
		}
		want += len(ids)
	}

	// RELP in clear.
	session(newRELPClient(t, dial(t, tcp.Addr())))

	// RELP behind a trusted balancer, PROXY v2.
	c := dial(t, tcp.Addr())
	c.Write(proxyV2(0x21, 0x11, v2Inet("192.0.2.10", "198.51.100.1", 56324, 20514, nil)))
	session(newRELPClient(t, c))

	// RELP behind a balancer speaking PROXY v1, IPv6. The parent ran this
	// session over TLS with a client certificate; this server has no TLS
	// (SPEC §1.2).
	c = dial(t, tcp.Addr())
	c.Write([]byte("PROXY TCP6 2001:db8::7 2001:db8::1 40000 20514\r\n"))
	session(newRELPClient(t, c))

	h.waitItems(t, want)
	items := h.drainRing()

	conv := convert.New(convert.Config{
		DefaultPriority: 13, IncludeRaw: true, MaxMessageBytes: cfg.MaxMessageBytes,
		SDMaxElements: 32, SDMaxParams: 64, XTransport: "relp",
	})
	seen := map[string]int{}
	for _, it := range items {
		env, _ := conv.Append(nil, &it.Head, it.Raw)
		if err := jlhcheck.Validate(env); err != nil {
			t.Fatalf("%s/%s %q: %v\n%s", it.Head.Transport, it.Head.Framing, it.Raw, err, env)
		}
		// The emergency dump converts the same head and raw again (SPEC
		// §12.1): the bytes must not move (SPEC §3.5).
		if env2, _ := conv.Append(nil, &it.Head, it.Raw); string(env2) != string(env) {
			t.Fatal("the JLH is not a pure function of the head and raw (SPEC §3.5)")
		}

		var e struct {
			Payload struct {
				Host struct {
					SrcIP string `json:"src-ip"`
					DstIP string `json:"dst-ip"`
				} `json:"host"`
				Transport  string            `json:"transport"`
				Framing    string            `json:"framing"`
				TLS        map[string]string `json:"tls"`
				XProxy     map[string]any    `json:"x-proxy"`
				XTransport string            `json:"x-transport"`
				Size       int               `json:"size"`
				Message    string            `json:"message"`
				Parse      struct {
					Warnings []string `json:"warnings"`
				} `json:"parse"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(env, &e); err != nil {
			t.Fatal(err)
		}
		pl := e.Payload
		if pl.XTransport != "relp" || pl.Framing != "octet-counting" {
			t.Fatalf("x-transport %q framing %q: every envelope says RELP, octet-counting", pl.XTransport, pl.Framing)
		}
		if pl.Transport != "tcp" || pl.TLS != nil {
			t.Fatalf("transport %q tls %v: this server only speaks RELP over plain TCP", pl.Transport, pl.TLS)
		}
		key := pl.Transport
		switch {
		case pl.XProxy != nil && pl.Host.SrcIP == "2001:db8::7":
			key += "/proxy-v1"
			if pl.Host.DstIP != "2001:db8::1" || pl.XProxy["peer-ip"] != "127.0.0.1" {
				t.Errorf("tcp behind PROXY v1: host %+v x-proxy %v", pl.Host, pl.XProxy)
			}
		case pl.XProxy != nil:
			key += "/proxy"
			if pl.Host.SrcIP != "192.0.2.10" || pl.Host.DstIP != "198.51.100.1" || pl.XProxy["peer-ip"] != "127.0.0.1" {
				t.Errorf("tcp behind PROXY: host %+v x-proxy %v", pl.Host, pl.XProxy)
			}
		default:
			if pl.Host.SrcIP != "127.0.0.1" || pl.Host.DstIP != "127.0.0.1" {
				t.Errorf("tcp host %+v", pl.Host)
			}
		}
		if strings.Contains(pl.Message, "two\nlines") {
			seen[key+"/lf"]++
		}
		if len(it.Raw) == 480 && pl.Size == 1000 {
			if !contains(pl.Parse.Warnings, "truncated") {
				t.Errorf("%s: a truncated message without the warning: %v", key, pl.Parse.Warnings)
			}
			seen[key+"/truncated"]++
		}
		seen[key]++
	}
	for _, k := range []string{"tcp", "tcp/proxy", "tcp/proxy-v1"} {
		if seen[k] != len(e2eMessages) {
			t.Errorf("%d envelopes for %s, want %d", seen[k], k, len(e2eMessages))
		}
		if seen[k+"/truncated"] != 1 || seen[k+"/lf"] != 1 {
			t.Errorf("%s: %d truncated, %d with a LF in DATA; want 1 and 1", k, seen[k+"/truncated"], seen[k+"/lf"])
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
