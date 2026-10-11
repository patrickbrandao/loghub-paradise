// Package clientip resolves the originating client address of a request from
// the TCP peer and the forwarding headers, per SPEC §5.
//
// The rule this package exists to enforce: a forwarding header is believed only
// when the TCP peer is itself a trusted proxy, and within the header the chain
// is walked from the RIGHT, skipping trusted hops. The client controls what it
// sends; a proxy APPENDS what it observed. Walking from the right, the first
// value outside the trust set is the first hop the attacker could not forge.
package clientip

import (
	"net/netip"
	"strings"
)

// Source records where the resolved address came from.
type Source uint8

const (
	// FromTCP means no header was believed: the address is the TCP peer.
	FromTCP Source = iota
	// FromXFF means the address came from X-Forwarded-For.
	FromXFF
	// FromXRI means the address came from X-Real-IP.
	FromXRI
)

func (s Source) String() string {
	switch s {
	case FromXFF:
		return "xff"
	case FromXRI:
		return "xri"
	}
	return "tcp"
}

// Result is the outcome of a resolution.
type Result struct {
	// Addr is the resolved client address, always unmapped so an IPv4-mapped
	// IPv6 form and the plain IPv4 form compare equal.
	Addr netip.Addr
	// Source says which input Addr came from.
	Source Source
	// Version is 4 or 6, for the src-ip-version envelope field.
	Version int
	// UntrustedForwarding is true when the request carried a forwarding header
	// but the peer was not trusted, so the header was ignored. It is the signal
	// that proxy.trusted is probably misconfigured (SPEC §5.4).
	UntrustedForwarding bool
}

// Resolver holds the trusted-proxy set. It is immutable: built once at boot,
// read concurrently by every handler without a lock.
type Resolver struct {
	trusted []netip.Prefix
	maxXFF  int
}

// New builds a Resolver. maxXFF caps how many X-Forwarded-For entries are
// examined, so a header with 100k entries cannot turn into CPU.
func New(trusted []netip.Prefix, maxXFF int) *Resolver {
	if maxXFF < 1 {
		maxXFF = 1
	}
	cp := make([]netip.Prefix, len(trusted))
	copy(cp, trusted)
	return &Resolver{trusted: cp, maxXFF: maxXFF}
}

// normalize puts an address into the one canonical form the rest of the server
// may ever see: no IPv6 zone, and IPv4-mapped collapsed to plain IPv4.
//
// Dropping the zone is not cosmetic. netip.Prefix.Contains returns false for ANY
// zoned address, so a link-local peer such as fe80::1%eth0 would silently match
// no entry in proxy.trusted. The zone is also a
// local interface name, meaningless to the worker that reads src-ip-addr.
//
// Two clients on different interfaces sharing one link-local address do collapse
// to a single identity. That is the right trade: link-local traffic reaching a
// public webhook server is already degenerate, and a leaked zone breaks every
// prefix comparison in the process.
func normalize(a netip.Addr) netip.Addr { return a.WithZone("").Unmap() }

// Trusts reports whether addr is in the trusted-proxy set.
func (r *Resolver) Trusts(addr netip.Addr) bool {
	addr = normalize(addr)
	for _, p := range r.trusted {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// Resolve applies SPEC §5.1. peer is the TCP peer address; xff and xri are the
// raw header values (empty when absent).
func (r *Resolver) Resolve(peer netip.Addr, xff, xri string) Result {
	peer = normalize(peer)
	res := Result{Addr: peer, Source: FromTCP}

	hasHeader := strings.TrimSpace(xff) != "" || strings.TrimSpace(xri) != ""

	if !r.Trusts(peer) {
		// Not behind a known proxy: headers are attacker-controlled input and
		// are ignored entirely. Record the mismatch so the operator can see it.
		res.UntrustedForwarding = hasHeader
		res.Version = version(peer)
		return res
	}

	if addr, ok := r.walkXFF(xff); ok {
		res.Addr, res.Source = addr, FromXFF
		res.Version = version(addr)
		return res
	}
	if addr, ok := parseAddr(xri); ok {
		res.Addr, res.Source = addr, FromXRI
		res.Version = version(addr)
		return res
	}

	res.Version = version(peer)
	return res
}

// walkXFF scans X-Forwarded-For from the right, skipping trusted hops, and
// returns the first untrusted address. If every entry is trusted it returns the
// leftmost one, which is the closest thing to a client the chain offers.
//
// Malformed entries ("unknown", the RFC 7239 obfuscated "_secret", garbage) are
// skipped as if absent: rejecting the request would let anyone deny service by
// poisoning a header that a proxy in front of us appended to.
func (r *Resolver) walkXFF(xff string) (netip.Addr, bool) {
	if strings.TrimSpace(xff) == "" {
		return netip.Addr{}, false
	}

	parts := strings.Split(xff, ",")

	// Only the rightmost maxXFF entries can matter: the walk starts at the
	// right and stops at the first untrusted hop, so anything further left is
	// unreachable unless every examined entry was trusted.
	start := 0
	if len(parts) > r.maxXFF {
		start = len(parts) - r.maxXFF
	}
	window := parts[start:]

	var leftmostValid netip.Addr
	var haveLeftmost bool

	for i := len(window) - 1; i >= 0; i-- {
		addr, ok := parseAddr(window[i])
		if !ok {
			continue
		}
		if !r.Trusts(addr) {
			return addr, true
		}
		leftmostValid, haveLeftmost = addr, true
	}
	// Every entry was a trusted proxy; the leftmost is as close to the origin
	// as this chain gets.
	return leftmostValid, haveLeftmost
}

// parseAddr accepts the shapes that appear in forwarding headers in the wild:
//
//	203.0.113.9
//	203.0.113.9:443            port appended by some proxies
//	2001:db8::1
//	[2001:db8::1]:443          bracketed with a port
//	"203.0.113.9"              quoted, as RFC 7239 allows
//
// and rejects everything else, including "unknown" and RFC 7239 obfuscated
// identifiers such as "_hidden".
func parseAddr(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	s = strings.TrimSpace(s)
	if s == "" {
		return netip.Addr{}, false
	}

	// Bracketed form, with or without a port.
	if s[0] == '[' {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return netip.Addr{}, false
		}
		if rest := s[end+1:]; rest != "" && !strings.HasPrefix(rest, ":") {
			return netip.Addr{}, false
		}
		s = s[1:end]
	} else if n := strings.Count(s, ":"); n == 1 {
		// Exactly one colon means host:port; more than one means bare IPv6.
		if host, _, ok := strings.Cut(s, ":"); ok {
			s = host
		}
	}

	addr, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	if addr.Zone() != "" {
		// A zone in a forwarding header is garbage: an interface name from
		// somewhere else's machine. Header entries can simply be skipped, so
		// reject. The TCP peer takes the other path -- normalize() strips the
		// zone there, because that address is ground truth and must be
		// reported, not discarded.
		return netip.Addr{}, false
	}
	return normalize(addr), true
}

func version(a netip.Addr) int {
	if a.Is4() {
		return 4
	}
	if a.Is6() {
		return 6
	}
	return 0
}

// PeerAddr extracts the address from a "host:port" string such as
// http.Request.RemoteAddr. It returns the zero Addr when the input is not
// parseable, which happens for non-TCP listeners in tests.
func PeerAddr(remoteAddr string) (netip.Addr, uint16) {
	ap, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		// Some listeners hand back a bare address with no port.
		if a, err2 := netip.ParseAddr(strings.Trim(remoteAddr, "[]")); err2 == nil {
			return normalize(a), 0
		}
		return netip.Addr{}, 0
	}
	return normalize(ap.Addr()), ap.Port()
}
