package server

import (
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/clientip"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// describe resolves everything the payload of SPEC §3.2 needs from the request
// and the trust decision of §5. The serialiser only writes what this returns.
//
// trusted says whether the TCP peer is in proxy.trusted. Only then are the
// proxy's own headers read -- X-Forwarded-Proto, -Host and -Port,
// X-Client-Port, X-Server-Addr and the fingerprints --, and each of them is
// taken from its LAST line and last element: the value the nearest proxy, the
// one we trust, wrote. A header a trusted proxy does not overwrite can be
// forged by the client and win (SPEC §5); the proxy must overwrite all of them.
func describe(r *http.Request, peer netip.Addr, peerPort uint16, ip clientip.Result,
	trusted bool, mode string, exclude map[string]bool) envelope.Request {

	req := envelope.Request{
		Mode:       mode,
		Src:        ip.Addr.Unmap(),
		PeerAddr:   peer,
		PeerPort:   peerPort,
		SrcSource:  ip.Source.String(),
		Proto:      r.Proto,
		Method:     r.Method,
		Path:       r.URL.EscapedPath(),
		RawQuery:   r.URL.RawQuery,
		Header:     r.Header,
		HostHeader: r.Host,
		Exclude:    exclude,
	}
	if req.Path == "" {
		req.Path = "/"
	}
	if v := r.Header["Content-Type"]; len(v) > 0 {
		req.ContentType = v[0]
	}

	if trusted {
		req.SecProxy = strings.EqualFold(lastElem(r.Header, "X-Forwarded-Proto"), "https")
		// A fingerprint is one value with commas of its own (JA3, the Akamai
		// HTTP/2 format): the last LINE, whole, never its last element.
		req.FP2 = lastLine(r.Header, "X-Http2-Fingerprint")
		req.FP3 = lastLine(r.Header, "X-Tls-Ja3")
		req.FP4 = lastLine(r.Header, "X-Tls-Ja4")
	}

	// The effective host: the trusted proxy's X-Forwarded-Host, else Host.
	rawHost := r.Host
	if trusted {
		if fh := lastElem(r.Header, "X-Forwarded-Host"); fh != "" {
			rawHost = fh
		}
	}
	host, hostPort := splitHost(rawHost)
	if !plainHost(host) {
		// HTTP/1.0 without Host, or a Host net/http accepts but no URL can
		// carry (sub-delims such as '*' or '$'): the payload's host becomes
		// the address the client aimed at, below, and the refused value's
		// port is no port of anything. The raw value is still in the headers
		// (SPEC §3.2).
		host, hostPort = "", 0
	}

	local := localAddr(r)
	proxied := ip.Source != clientip.FromTCP
	if !proxied {
		// The client is the TCP peer: the socket says where it aimed.
		req.Dst = local.Addr().Unmap().WithZone("")
		req.SPort = peerPort
		req.DPort = local.Port()
	} else {
		// The client is behind a proxy: the local socket describes the
		// proxy->server hop, never the client's.
		req.SPort = port(lastElem(r.Header, "X-Client-Port"))
	}
	if trusted {
		// A trusted proxy says where the client aimed even when it sent no
		// X-Forwarded-For (proxied is false then): schema and sec already
		// come from its X-Forwarded-Proto, and the url's port taken from the
		// socket was the proxy->server hop's -- "https://pub.example:8080"
		// for a client that used 443.
		if a, err := netip.ParseAddr(lastElem(r.Header, "X-Server-Addr")); err == nil {
			req.Dst = a.Unmap().WithZone("")
		}
		switch fp := port(lastElem(r.Header, "X-Forwarded-Port")); {
		case fp != 0:
			req.DPort = fp
		case !proxied && !req.SecProxy:
			// Plain HTTP on both hops: the socket's port is the best there is.
		case hostPort != 0:
			req.DPort = hostPort
		case req.SecProxy:
			req.DPort = 443
		default:
			req.DPort = 80
		}
	}
	// src and dst are of one family; unknown is the unspecified address.
	if !req.Dst.IsValid() || req.Dst.Is4() != req.Src.Is4() {
		if req.Src.Is4() {
			req.Dst = netip.IPv4Unspecified()
		} else {
			req.Dst = netip.IPv6Unspecified()
		}
	}

	if host == "" {
		host = req.Dst.String()
	}
	req.Host = host

	// The url's port is the effective host's, else dport; the default of the
	// schema is left out.
	urlPort := hostPort
	if urlPort == 0 {
		urlPort = req.DPort
	}
	if (req.SecProxy && urlPort == 443) || (!req.SecProxy && urlPort == 80) {
		urlPort = 0
	}
	req.URLPort = urlPort
	return req
}

// lastElem is the last comma-separated element of the last line of a header,
// trimmed. name must be in canonical form.
func lastElem(h http.Header, name string) string {
	v := h[name]
	if len(v) == 0 {
		return ""
	}
	s := v[len(v)-1]
	if i := strings.LastIndexByte(s, ','); i >= 0 {
		s = s[i+1:]
	}
	return strings.TrimSpace(s)
}

// lastLine is the last line of a header, whole and trimmed.
func lastLine(h http.Header, name string) string {
	v := h[name]
	if len(v) == 0 {
		return ""
	}
	return strings.TrimSpace(v[len(v)-1])
}

// splitHost separates a Host value into a lowercase host without brackets and
// its port (0 when absent or not a port). "[::1]:8080", "::1", "a.b:80" and
// "A.B" all come out right.
func splitHost(s string) (string, uint16) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", 0
	}
	var p uint16
	switch {
	case strings.HasPrefix(s, "["):
		if end := strings.IndexByte(s, ']'); end > 0 {
			if rest := s[end+1:]; strings.HasPrefix(rest, ":") {
				p = port(rest[1:])
			}
			s = s[1:end]
		}
	case strings.Count(s, ":") == 1:
		i := strings.IndexByte(s, ':')
		p = port(s[i+1:])
		s = s[:i]
	}
	return asciiLower(s), p
}

// asciiLower lowercases A-Z and nothing else. strings.ToLower follows Unicode,
// where the Kelvin sign U+212A becomes an ASCII 'k': "\u212Aexample.com" -- an
// absolute-form target or a trusted X-Forwarded-Host -- passed plainHost as
// "kexample.com", a host the client never named.
func asciiLower(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			b := []byte(s)
			for j := i; j < len(b); j++ {
				if b[j] >= 'A' && b[j] <= 'Z' {
					b[j] += 'a' - 'A'
				}
			}
			return string(b)
		}
	}
	return s
}

// plainHost reports whether h can be the host of the payload and of its url: an
// IP literal, or a DNS name of letters, digits, '-', '_' and dots.
func plainHost(h string) bool {
	if h == "" {
		return false
	}
	if _, err := netip.ParseAddr(h); err == nil {
		return !strings.Contains(h, "%") // a zone is not part of a URL host here
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// port parses a decimal TCP port, 1-65535; anything else is 0.
func port(s string) uint16 {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil || n == 0 {
		return 0
	}
	return uint16(n)
}

// localAddr is the address the connection was accepted on.
func localAddr(r *http.Request) netip.AddrPort {
	if a, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		if ap, err := netip.ParseAddrPort(a.String()); err == nil {
			return ap
		}
	}
	return netip.AddrPort{}
}

// validMethod is the schema's ^[A-Z]+$ (SPEC §4.2). Converting a lowercase
// method would lie about what arrived, so it is refused instead.
func validMethod(m string) bool {
	if m == "" {
		return false
	}
	for i := 0; i < len(m); i++ {
		if m[i] < 'A' || m[i] > 'Z' {
			return false
		}
	}
	return true
}
