package clientip

import (
	"net/netip"
	"strings"
	"testing"
)

func prefixes(t *testing.T, ss ...string) []netip.Prefix {
	t.Helper()
	out := make([]netip.Prefix, 0, len(ss))
	for _, s := range ss {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			t.Fatalf("bad test prefix %q: %v", s, err)
		}
		out = append(out, p.Masked())
	}
	return out
}

func addr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("bad test addr %q: %v", s, err)
	}
	return a.Unmap()
}

func TestResolve(t *testing.T) {
	// A realistic two-tier topology: an edge proxy on 10.0.0.0/8 behind a
	// loopback sidecar.
	r := New(prefixes(t, "127.0.0.0/8", "::1/128", "10.0.0.0/8"), 32)

	cases := []struct {
		name          string
		peer          string
		xff           string
		xri           string
		wantAddr      string
		wantSource    Source
		wantVer       int
		wantUntrusted bool
	}{
		{
			name: "untrusted peer, no headers",
			peer: "203.0.113.9", wantAddr: "203.0.113.9", wantSource: FromTCP, wantVer: 4,
		},
		{
			// The whole point of proxy.trusted: a direct client cannot claim to
			// be someone else just by sending the header.
			name: "untrusted peer forging XFF is ignored",
			peer: "203.0.113.9", xff: "1.1.1.1",
			wantAddr: "203.0.113.9", wantSource: FromTCP, wantVer: 4, wantUntrusted: true,
		},
		{
			name: "untrusted peer forging X-Real-IP is ignored",
			peer: "203.0.113.9", xri: "1.1.1.1",
			wantAddr: "203.0.113.9", wantSource: FromTCP, wantVer: 4, wantUntrusted: true,
		},
		{
			name: "trusted peer, single XFF entry",
			peer: "10.0.0.1", xff: "203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			// THE spoofing case. The client sent "9.9.9.9"; the proxy appended
			// the address it actually saw. Reading leftmost would return the
			// forgery.
			name: "client prepends a forged entry",
			peer: "10.0.0.1", xff: "9.9.9.9, 203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "client prepends several forged entries",
			peer: "10.0.0.1", xff: "1.1.1.1, 2.2.2.2, 9.9.9.9, 203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "two trusted hops are skipped",
			peer: "127.0.0.1", xff: "203.0.113.9, 10.0.0.7, 10.0.0.1",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "forgery before the real client, then trusted hops",
			peer: "127.0.0.1", xff: "6.6.6.6, 203.0.113.9, 10.0.0.7, 10.0.0.1",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "every entry trusted falls back to the leftmost",
			peer: "10.0.0.1", xff: "10.0.0.9, 10.0.0.7",
			wantAddr: "10.0.0.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "XFF wins over X-Real-IP",
			peer: "10.0.0.1", xff: "203.0.113.9", xri: "8.8.8.8",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "X-Real-IP used when XFF absent",
			peer: "10.0.0.1", xri: "203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXRI, wantVer: 4,
		},
		{
			name: "X-Real-IP used when XFF is empty",
			peer: "10.0.0.1", xff: "   ", xri: "203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXRI, wantVer: 4,
		},
		{
			name: "XFF entirely unparseable falls through to X-Real-IP",
			peer: "10.0.0.1", xff: "unknown, garbage", xri: "203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXRI, wantVer: 4,
		},
		{
			name: "no usable header falls back to the peer",
			peer: "10.0.0.1", xff: "unknown", xri: "also-garbage",
			wantAddr: "10.0.0.1", wantSource: FromTCP, wantVer: 4,
		},
		{
			name: "invalid entries are skipped, not fatal",
			peer: "10.0.0.1", xff: "unknown, 203.0.113.9, _obfuscated",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "IPv6 client",
			peer: "10.0.0.1", xff: "2001:db8::1",
			wantAddr: "2001:db8::1", wantSource: FromXFF, wantVer: 6,
		},
		{
			name: "IPv6 bracketed with a port",
			peer: "10.0.0.1", xff: "[2001:db8::1]:443",
			wantAddr: "2001:db8::1", wantSource: FromXFF, wantVer: 6,
		},
		{
			name: "IPv4 with a port appended by the proxy",
			peer: "10.0.0.1", xff: "203.0.113.9:51234",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			// An IPv4-mapped form must not become a second identity for the
			// same client: it would double the rate-limit budget.
			name: "IPv4-mapped IPv6 is unmapped to v4",
			peer: "10.0.0.1", xff: "::ffff:203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "quoted entry per RFC 7239",
			peer: "10.0.0.1", xff: `"203.0.113.9"`,
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "IPv6 loopback peer is trusted",
			peer: "::1", xff: "203.0.113.9",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "whitespace around entries",
			peer: "10.0.0.1", xff: "  9.9.9.9 ,   203.0.113.9  ",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
		{
			name: "empty entries are skipped",
			peer: "10.0.0.1", xff: ",,203.0.113.9,,",
			wantAddr: "203.0.113.9", wantSource: FromXFF, wantVer: 4,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := r.Resolve(addr(t, tc.peer), tc.xff, tc.xri)
			if got.Addr != addr(t, tc.wantAddr) {
				t.Errorf("addr = %s, want %s", got.Addr, tc.wantAddr)
			}
			if got.Source != tc.wantSource {
				t.Errorf("source = %s, want %s", got.Source, tc.wantSource)
			}
			if got.Version != tc.wantVer {
				t.Errorf("version = %d, want %d", got.Version, tc.wantVer)
			}
			if got.UntrustedForwarding != tc.wantUntrusted {
				t.Errorf("untrustedForwarding = %v, want %v", got.UntrustedForwarding, tc.wantUntrusted)
			}
		})
	}
}

// SPEC §5.3: the walk examines at most maxXFF entries, counted from the right,
// so a header with a huge chain cannot turn into CPU.
func TestMaxXFFEntries(t *testing.T) {
	r := New(prefixes(t, "10.0.0.0/8"), 3)

	// The untrusted client sits outside the window; the three rightmost
	// entries are all trusted, so the leftmost of the WINDOW is returned.
	got := r.Resolve(addr(t, "10.0.0.1"), "203.0.113.9, 10.0.0.2, 10.0.0.3, 10.0.0.4", "")
	if want := addr(t, "10.0.0.2"); got.Addr != want {
		t.Errorf("addr = %s, want %s (only the 3 rightmost entries are examined)", got.Addr, want)
	}

	// Within the window the normal rule applies.
	got = r.Resolve(addr(t, "10.0.0.1"), "1.1.1.1, 203.0.113.9, 10.0.0.3, 10.0.0.4", "")
	if want := addr(t, "203.0.113.9"); got.Addr != want {
		t.Errorf("addr = %s, want %s", got.Addr, want)
	}

	// A pathological header must terminate quickly and safely.
	huge := strings.Repeat("1.1.1.1,", 100_000) + "203.0.113.9"
	got = r.Resolve(addr(t, "10.0.0.1"), huge, "")
	if want := addr(t, "203.0.113.9"); got.Addr != want {
		t.Errorf("addr = %s, want %s", got.Addr, want)
	}
}

func TestTrustsUnmapsBeforeMatching(t *testing.T) {
	r := New(prefixes(t, "10.0.0.0/8"), 32)
	if !r.Trusts(addr(t, "10.0.0.1")) {
		t.Error("10.0.0.1 should be trusted")
	}
	// The same proxy arriving over a dual-stack socket must still be trusted.
	mapped := netip.MustParseAddr("::ffff:10.0.0.1")
	if !r.Trusts(mapped) {
		t.Error("IPv4-mapped form of a trusted proxy should be trusted")
	}
	if r.Trusts(addr(t, "11.0.0.1")) {
		t.Error("11.0.0.1 should not be trusted")
	}
}

func TestEmptyTrustSetIgnoresAllHeaders(t *testing.T) {
	r := New(nil, 32)
	got := r.Resolve(addr(t, "127.0.0.1"), "1.2.3.4", "5.6.7.8")
	if got.Addr != addr(t, "127.0.0.1") || got.Source != FromTCP {
		t.Errorf("got %s from %s, want the peer over TCP", got.Addr, got.Source)
	}
	if !got.UntrustedForwarding {
		t.Error("a header from an untrusted peer must be flagged")
	}
}

func TestParseAddrRejections(t *testing.T) {
	bad := []string{
		"", "   ", "unknown", "_hidden", "garbage",
		"999.999.999.999", "1.2.3", "1.2.3.4.5",
		"[2001:db8::1", "[2001:db8::1]x", "2001:db8::1%eth0",
		"fe80::1%25eth0", ":", "::ffff:999.1.1.1", `"`, `""`,
	}
	for _, s := range bad {
		if a, ok := parseAddr(s); ok {
			t.Errorf("parseAddr(%q) = %s, want rejection", s, a)
		}
	}
}

func TestParseAddrAccepts(t *testing.T) {
	cases := map[string]string{
		"203.0.113.9":        "203.0.113.9",
		" 203.0.113.9 ":      "203.0.113.9",
		"203.0.113.9:443":    "203.0.113.9",
		"2001:db8::1":        "2001:db8::1",
		"[2001:db8::1]":      "2001:db8::1",
		"[2001:db8::1]:443":  "2001:db8::1",
		"::ffff:203.0.113.9": "203.0.113.9",
		`"203.0.113.9"`:      "203.0.113.9",
		"::1":                "::1",
		"::":                 "::",
	}
	for in, want := range cases {
		a, ok := parseAddr(in)
		if !ok {
			t.Errorf("parseAddr(%q) rejected, want %s", in, want)
			continue
		}
		if a.String() != want {
			t.Errorf("parseAddr(%q) = %s, want %s", in, a, want)
		}
	}
}

func TestPeerAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantAddr string
		wantPort uint16
	}{
		{"203.0.113.9:51234", "203.0.113.9", 51234},
		{"[2001:db8::1]:443", "2001:db8::1", 443},
		{"203.0.113.9", "203.0.113.9", 0},
		{"[::1]", "::1", 0},
		{"::ffff:10.0.0.1", "10.0.0.1", 0},
	}
	for _, tc := range cases {
		a, p := PeerAddr(tc.in)
		if a.String() != tc.wantAddr || p != tc.wantPort {
			t.Errorf("PeerAddr(%q) = %s,%d; want %s,%d", tc.in, a, p, tc.wantAddr, tc.wantPort)
		}
	}
	if a, _ := PeerAddr("pipe"); a.IsValid() {
		t.Errorf("PeerAddr(%q) = %s, want the invalid zero Addr", "pipe", a)
	}
}

// FuzzResolve is mandatory per SPEC §15.2: this is the only code that parses
// hostile structured input, and a bug here becomes IP spoofing.
func FuzzResolve(f *testing.F) {
	seeds := []string{
		"", " ", ",", ",,,",
		"203.0.113.9",
		"9.9.9.9, 203.0.113.9",
		"1.1.1.1, 2.2.2.2, 10.0.0.1",
		"unknown, 203.0.113.9",
		"_secret",
		"[2001:db8::1]:443, 10.0.0.1",
		"::ffff:203.0.113.9",
		`"203.0.113.9"`,
		"fe80::1%eth0",
		strings.Repeat("1.1.1.1,", 200),
		"\x00\x01\x02",
		"999.999.999.999",
		strings.Repeat(",", 5000),
	}
	for _, s := range seeds {
		f.Add(s, "10.0.0.1", true)
		f.Add(s, "203.0.113.9", false)
	}

	trusted := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.0/8"),
		netip.MustParsePrefix("127.0.0.0/8"),
		netip.MustParsePrefix("::1/128"),
	}
	r := New(trusted, 32)

	f.Fuzz(func(t *testing.T, header, peerStr string, useXRI bool) {
		peer, err := netip.ParseAddr(peerStr)
		if err != nil {
			t.Skip()
		}
		// Compare against the canonical form: Resolve strips IPv6 zones and
		// unmaps, so the raw input is not the value it is required to return.
		peer = normalize(peer)

		var got Result
		if useXRI {
			got = r.Resolve(peer, "", header)
		} else {
			got = r.Resolve(peer, header, "")
		}

		// Invariant 1 (SPEC §17.5): a header is NEVER believed when the peer is
		// not a trusted proxy. This is the security property; everything else
		// is convenience.
		if !r.Trusts(peer) {
			if got.Addr != peer {
				t.Fatalf("untrusted peer %s: resolved to %s from header %q", peer, got.Addr, header)
			}
			if got.Source != FromTCP {
				t.Fatalf("untrusted peer %s: source = %s, want tcp", peer, got.Source)
			}
		}

		// Invariant 2: the result is always a usable, normalised address, so
		// the envelope and the rate-limit key can never see a half-formed one.
		if !got.Addr.IsValid() {
			t.Fatalf("invalid addr from peer %s header %q", peer, header)
		}
		if got.Addr != got.Addr.Unmap() {
			t.Fatalf("addr %s is not unmapped (peer %s, header %q)", got.Addr, peer, header)
		}
		if got.Addr.Zone() != "" {
			t.Fatalf("addr %s carries a zone (peer %s, header %q)", got.Addr, peer, header)
		}

		// Invariant 3: Version always agrees with the address family, since the
		// envelope publishes it as src-ip-version.
		wantVer := 4
		if got.Addr.Is6() {
			wantVer = 6
		}
		if got.Version != wantVer {
			t.Fatalf("version = %d for %s (peer %s, header %q)", got.Version, got.Addr, peer, header)
		}
	})
}

// A zoned peer must be reported with the zone stripped: netip.Prefix.Contains
// is false for ANY zoned address, so a leaked zone would make a link-local
// client match no entry in proxy.trusted and no entry in ratelimit.exempt.
// Found by FuzzResolve.
func TestZonedPeerIsNormalized(t *testing.T) {
	r := New(prefixes(t, "fe80::/10"), 32)

	zoned := netip.MustParseAddr("fe80::1%eth0")
	got := r.Resolve(zoned, "203.0.113.9", "")
	if !r.Trusts(zoned) {
		t.Fatal("a zoned address inside fe80::/10 must be recognised as trusted")
	}
	if got.Addr != addr(t, "203.0.113.9") {
		t.Errorf("addr = %s, want the header value once the peer is trusted", got.Addr)
	}

	// Untrusted zoned peer: reported, but without the interface name.
	r2 := New(prefixes(t, "10.0.0.0/8"), 32)
	got = r2.Resolve(zoned, "1.2.3.4", "")
	if got.Addr.Zone() != "" {
		t.Errorf("addr = %s, want the zone stripped", got.Addr)
	}
	if got.Addr != addr(t, "fe80::1") {
		t.Errorf("addr = %s, want fe80::1", got.Addr)
	}
}
