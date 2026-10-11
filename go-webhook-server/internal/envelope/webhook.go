package envelope

import (
	"encoding/base64"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// Modes of the webhook.request payload this server can promise (SPEC §3.2):
// the guarantee of the ring x sink combination, fixed at boot. commit and full
// would mean waiting for a worker, which this server never does.
const (
	ModeFast   = "fast"
	ModeBucket = "bucket"
)

// Request is everything the payload of SPEC §3.2 is made of, already resolved
// by the HTTP layer: which address is the client, which headers a trusted proxy
// vouched for, which host and port the client aimed at. The serialiser below
// only writes; every decision is the handler's, where the request and the trust
// list are.
type Request struct {
	Mode string

	// network
	Src, Dst     netip.Addr // same family; Dst unspecified when unknown
	SPort, DPort uint16
	SecProxy     bool   // TLS ended at a trusted proxy (sec.mode=proxy)
	FP2, FP3     string // fingerprints from a trusted proxy; "" = absent
	FP4          string
	PeerAddr     netip.Addr
	PeerPort     uint16
	SrcSource    string // "tcp" | "xff" | "xri"
	Proto        string // "HTTP/1.1"...

	Method string
	Host   string // effective host: lowercase, no port, no brackets
	// URLPort is the port the url carries; 0 when it is the default of the
	// schema and is left out.
	URLPort  uint16
	Path     string // escaped path, never empty
	RawQuery string

	Header     http.Header
	HostHeader string          // r.Host, which net/http lifts out of Header
	Exclude    map[string]bool // headers.exclude, lowercase; nil = none

	ContentType string
	Body        []byte
}

// Schema is http or https, from where TLS ended (SPEC §3.2).
func (r *Request) Schema() string {
	if r.SecProxy {
		return "https"
	}
	return "http"
}

// BodyPlan is the pre-pass of SPEC §3.2 over the body: which encoding the
// content takes, and how many bytes the body string occupies between its
// quotes. The handler asks the pool for a buffer of the right size with it,
// before writing anything.
type BodyPlan struct {
	Base64 bool
	Len    int
}

// PlanBody measures both representations and picks utf-8 only for text --
// valid UTF-8 without NUL -- whose escape is not larger than the base64.
func PlanBody(body []byte) BodyPlan {
	b64 := base64.StdEncoding.EncodedLen(len(body))
	n, text := escapedLen(body)
	if !text || n > b64 {
		return BodyPlan{Base64: true, Len: b64}
	}
	return BodyPlan{Len: n}
}

// SizeHint is a capacity for the whole envelope: the body exactly, and a
// generous allowance for the rest that only a pathological head exceeds (the
// buffer then grows, and leaves the pool). headBytes is the measured size of
// the request head.
func SizeHint(plan BodyPlan, headBytes int64) int {
	return plan.Len + int(3*headBytes) + 1024
}

// AppendWebhook writes a whole webhook.request envelope: header, payload and
// the closing brace.
func AppendWebhook(dst []byte, h *Header, r *Request, plan BodyPlan) []byte {
	dst = AppendHeader(dst, h)
	dst = append(dst, `{"mode":`...)
	dst = AppendString(dst, r.Mode)
	dst = append(dst, `,"network":`...)
	dst = r.appendNetwork(dst)
	dst = append(dst, `,"method":`...)
	dst = AppendString(dst, r.Method)
	dst = append(dst, `,"schema":"`...)
	dst = append(dst, r.Schema()...)
	dst = append(dst, `","host":`...)
	dst = AppendString(dst, r.Host)
	dst = append(dst, `,"path":`...)
	dst = AppendString(dst, r.Path)
	dst = append(dst, `,"url":`...)
	dst = r.appendURL(dst)
	dst = append(dst, `,"headers":`...)
	dst = r.appendHeaders(dst)
	dst = append(dst, `,"params":{},"query":`...)
	dst = AppendQuery(dst, r.RawQuery)
	dst = append(dst, `,"content":{"length":`...)
	dst = strconv.AppendInt(dst, int64(len(r.Body)), 10)
	dst = append(dst, `,"type":`...)
	dst = AppendString(dst, r.ContentType)
	if plan.Base64 {
		dst = append(dst, `,"encoding":"base64","body":"`...)
		dst = base64.StdEncoding.AppendEncode(dst, r.Body)
		dst = append(dst, '"')
	} else {
		dst = append(dst, `,"encoding":"utf-8","body":`...)
		dst = AppendBytes(dst, r.Body)
	}
	dst = append(dst, `}}`...)
	return Close(dst)
}

func (r *Request) appendNetwork(dst []byte) []byte {
	dst = append(dst, `{"ip":{"version":`...)
	if r.Src.Is4() {
		dst = append(dst, '4')
	} else {
		dst = append(dst, '6')
	}
	dst = append(dst, `,"src":`...)
	dst = appendAddr(dst, r.Src)
	dst = append(dst, `,"dst":`...)
	dst = appendAddr(dst, r.Dst)
	dst = append(dst, `},"l4":{"sport":`...)
	dst = strconv.AppendUint(dst, uint64(r.SPort), 10)
	dst = append(dst, `,"dport":`...)
	dst = strconv.AppendUint(dst, uint64(r.DPort), 10)
	dst = append(dst, `},"sec":{"mode":"`...)
	if r.SecProxy {
		dst = append(dst, `proxy"`...)
	} else {
		dst = append(dst, `none"`...)
	}
	if r.FP2 != "" {
		dst = append(dst, `,"fp2":`...)
		dst = AppendString(dst, r.FP2)
	}
	// Without TLS there is no JA3/JA4 (the schema refuses them with
	// mode=none); HTTP/2 has a fingerprint even in h2c.
	if r.SecProxy && r.FP3 != "" {
		dst = append(dst, `,"fp3":`...)
		dst = AppendString(dst, r.FP3)
	}
	if r.SecProxy && r.FP4 != "" {
		dst = append(dst, `,"fp4":`...)
		dst = AppendString(dst, r.FP4)
	}
	dst = append(dst, `},"meta":[{"key":"peer-addr","value":`...)
	dst = appendAddr(dst, r.PeerAddr)
	dst = append(dst, `},{"key":"peer-port","value":`...)
	dst = strconv.AppendUint(dst, uint64(r.PeerPort), 10)
	dst = append(dst, `},{"key":"src-source","value":`...)
	dst = AppendString(dst, r.SrcSource)
	dst = append(dst, `},{"key":"http-proto","value":`...)
	dst = AppendString(dst, r.Proto)
	return append(dst, `}]}`...)
}

// appendURL writes schema://host[:port]path[?query] as one JSON string. The
// parts are escaped one after the other, which is the same as escaping the
// joined string: the escape works rune by rune, and every part is a whole
// string.
func (r *Request) appendURL(dst []byte) []byte {
	dst = append(dst, '"')
	dst = append(dst, r.Schema()...)
	dst = append(dst, "://"...)
	dst = appendInner(dst, r.urlHost())
	if r.URLPort != 0 {
		dst = append(dst, ':')
		dst = strconv.AppendUint(dst, uint64(r.URLPort), 10)
	}
	dst = appendInner(dst, r.Path)
	if r.RawQuery != "" {
		dst = append(dst, '?')
		dst = appendURLQuery(dst, r.RawQuery)
	}
	return append(dst, '"')
}

// appendURLQuery writes the raw query into the url (SPEC §3.2): as received,
// except the bytes RFC 3986 does not allow in a query, which are
// percent-encoded -- '#', space, '"<>\^`{|}', '[' and ']', controls, every
// byte from 0x80 up, and a '%' that does not start an escape. The skill asks
// for the query "crua, sem reescapar", and every legal byte gets exactly that;
// an illegal one made the url fail format: uri, and a raw '#' turned the rest
// of the query into a fragment, so parse_qsl(urlsplit(url).query) -- the
// skill's validate.py -- no longer matched payload.query. A percent-encoded
// byte decodes to itself, so the query object does not change. The output is
// ASCII without '"' or '\\': nothing to escape for JSON.
func appendURLQuery(dst []byte, q string) []byte {
	for i := 0; i < len(q); i++ {
		c := q[i]
		if !queryChar(c) && !(c == '%' && i+2 < len(q) && isHex(q[i+1]) && isHex(q[i+2])) {
			dst = append(dst, '%', upperHex[c>>4], upperHex[c&0xF])
			continue
		}
		dst = append(dst, c)
	}
	return dst
}

const upperHex = "0123456789ABCDEF"

// queryChar is RFC 3986's query = *( pchar / "/" / "?" ) without
// pct-encoded: unreserved, sub-delims, ':' and '@', '/' and '?'.
func queryChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '-', '.', '_', '~', '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=', ':', '@', '/', '?':
		return true
	}
	return false
}

// urlHost puts an IPv6 host back between brackets.
func (r *Request) urlHost() string {
	if strings.IndexByte(r.Host, ':') >= 0 {
		return "[" + r.Host + "]"
	}
	return r.Host
}

// appendInner is AppendString without the quotes.
func appendInner(dst []byte, s string) []byte { return appendEscaped(dst, s) }

func appendAddr(dst []byte, a netip.Addr) []byte {
	if !a.IsValid() {
		return append(dst, '"', '"')
	}
	dst = append(dst, '"')
	dst = a.AppendTo(dst) // never a character needing escaping
	return append(dst, '"')
}

// hopByHop are the headers that describe only the connection that carried the
// request, and are wrong once the body was reassembled (SPEC §3.2).
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-connection": true, "te": true,
	"trailer": true, "transfer-encoding": true, "upgrade": true,
}

// appendHeaders writes the array of SPEC §3.2: one {name, value} per value,
// names in lowercase, sorted by name with a stable sort -- net/http keeps the
// order of the values of one name, not the order between names -- and host put
// back from r.Host, which net/http lifts out of the map.
func (r *Request) appendHeaders(dst []byte) []byte {
	names := make([]string, 0, len(r.Header)+1)
	var conn map[string]bool
	for _, v := range r.Header["Connection"] {
		for _, tok := range strings.Split(v, ",") {
			if tok = strings.TrimSpace(tok); tok != "" {
				if conn == nil {
					conn = map[string]bool{}
				}
				conn[strings.ToLower(tok)] = true
			}
		}
	}
	for name := range r.Header {
		low := lower(name)
		if hopByHop[low] || conn[low] || r.Exclude[low] || low == "host" {
			continue
		}
		names = append(names, name)
	}
	hostEntry := r.HostHeader != "" && !r.Exclude["host"]
	if hostEntry {
		names = append(names, "") // "" stands for host
	}
	slices.SortStableFunc(names, func(a, b string) int {
		return compareLower(nameOf(a), nameOf(b))
	})

	dst = append(dst, '[')
	first := true
	for _, name := range names {
		var values []string
		if name == "" {
			values = []string{r.HostHeader}
		} else {
			values = r.Header[name]
		}
		for _, v := range values {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = append(dst, `{"name":"`...)
			if name == "" {
				dst = append(dst, "host"...)
			} else {
				dst = appendLowerName(dst, name)
			}
			dst = append(dst, `","value":`...)
			dst = AppendString(dst, v)
			dst = append(dst, '}')
		}
	}
	return append(dst, ']')
}

func nameOf(n string) string {
	if n == "" {
		return "host"
	}
	return n
}

// appendLowerName writes a header name in lowercase. net/http already refused
// any name that is not an RFC 9110 token, so nothing needs escaping.
func appendLowerName(dst []byte, name string) []byte {
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst = append(dst, c)
	}
	return dst
}

// compareLower orders two ASCII names as their lowercase forms, without
// building them.
func compareLower(a, b string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return int(ca) - int(cb)
		}
	}
	return len(a) - len(b)
}

// lower lowercases an ASCII header name without allocating when it already is.
func lower(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

// ExcludeSet turns the configured header names into the lookup the serialiser
// uses. Matching is case-insensitive.
func ExcludeSet(names []string) map[string]bool {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[strings.ToLower(strings.TrimSpace(n))] = true
	}
	return m
}
