package jlhcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// The url rules of the skill's validate.py, ported with Python 3's semantics:
// urlsplit, .hostname and parse_qsl(keep_blank_values=True) of the url must
// give back schema, host, path and query. Plus what format: uri claims and the
// schema library does not check (its "uri" is Go's lenient url.Parse): RFC
// 3986, with no fragment -- a fragment never reaches a server.
//
// Nothing here calls the envelope package: an oracle that decodes with the
// code under test agrees with every bug in it.

// uriRE is RFC 3986's absolute URI as SPEC §3.2 writes it: http or https, a
// reg-name or a bracketed IPv6 host, an optional port, a path-abempty and an
// optional query, no fragment. '[' and ']' are let into the path only: net/http
// keeps them in EscapedPath, and the path is written as it arrived (the
// skill's validate.py compares it with payload.path literally).
var uriRE = regexp.MustCompile(`^https?://(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._~!$&'()*+,;=-]+)(?::[0-9]+)?` +
	`(?:/(?:[A-Za-z0-9._~!$&'()*+,;=:@\[\]-]|%[0-9A-Fa-f]{2})*)*` +
	`(?:\?(?:[A-Za-z0-9._~!$&'()*+,;=:@/?-]|%[0-9A-Fa-f]{2})*)?$`)

func checkURL(p *Payload, jsonbCopy bool) error {
	if !uriRE.MatchString(p.URL) {
		return fmt.Errorf("url %q is not an RFC 3986 http(s) URI without fragment", p.URL)
	}
	scheme, netloc, path, query := urlsplit(p.URL)
	if scheme != p.Schema {
		return fmt.Errorf("url %q: scheme %q, schema %q", p.URL, scheme, p.Schema)
	}
	host, port := hostPort(netloc)
	if host != p.Host {
		return fmt.Errorf("url %q: hostname %q, host %q", p.URL, host, p.Host)
	}
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("url %q: bad port", p.URL)
		}
		if (p.Schema == "https" && n == 443) || (p.Schema == "http" && n == 80) {
			return fmt.Errorf("url %q carries the default port", p.URL)
		}
	}
	if path != p.Path {
		return fmt.Errorf("url %q: path %q, path field %q", p.URL, path, p.Path)
	}
	want := parseQSL(query)
	if jsonbCopy {
		want = nulAsReplacement(want)
	}
	if !reflect.DeepEqual(want, p.Query) {
		return fmt.Errorf("url %q: parse_qsl gives %#v, query is %#v", p.URL, want, p.Query)
	}
	return nil
}

// nulAsReplacement is the jsonb copy's query: NUL as U+FFFD in keys and
// values. Two keys that differ only there collapse into one, the last value
// winning -- what jsonb itself does with the duplicate name (SPEC §3.6).
func nulAsReplacement(q map[string]any) map[string]any {
	fix := func(s string) string { return strings.ReplaceAll(s, "\x00", string(utf8.RuneError)) }
	out := make(map[string]any, len(q))
	for k, v := range q {
		switch v := v.(type) {
		case string:
			out[fix(k)] = fix(v)
		case []any:
			vs := make([]any, len(v))
			for i, x := range v {
				vs[i] = fix(x.(string))
			}
			out[fix(k)] = vs
		}
	}
	return out
}

// urlsplit is Python's urllib.parse.urlsplit for a URL with a scheme and a
// netloc: the scheme lowercased, the netloc up to the first '/', '?' or '#',
// then the fragment cut at the first '#' and the query at the first '?'.
func urlsplit(u string) (scheme, netloc, path, query string) {
	if i := strings.IndexByte(u, ':'); i > 0 {
		scheme, u = strings.ToLower(u[:i]), u[i+1:]
	}
	if strings.HasPrefix(u, "//") {
		u = u[2:]
		end := strings.IndexAny(u, "/?#")
		if end < 0 {
			end = len(u)
		}
		netloc, u = u[:end], u[end:]
	}
	u, _, _ = strings.Cut(u, "#")
	path, query, _ = strings.Cut(u, "?")
	return scheme, netloc, path, query
}

// hostPort is Python's SplitResult.hostname (lowercased, brackets removed)
// and the raw port text.
func hostPort(netloc string) (host, port string) {
	if i := strings.LastIndexByte(netloc, '@'); i >= 0 {
		netloc = netloc[i+1:]
	}
	if strings.HasPrefix(netloc, "[") {
		end := strings.IndexByte(netloc, ']')
		if end < 0 {
			return strings.ToLower(netloc), ""
		}
		host, rest := netloc[1:end], netloc[end+1:]
		port, _ = strings.CutPrefix(rest, ":")
		return strings.ToLower(host), port
	}
	host, port, _ = strings.Cut(netloc, ":")
	return strings.ToLower(host), port
}

// parseQSL is parse_qsl(q, keep_blank_values=True) grouped the way the skill's
// validate.py groups it: one value a string, a repeated key a list in order.
// The values are shaped as encoding/json decodes the query object.
func parseQSL(q string) map[string]any {
	out := map[string]any{}
	for _, nv := range strings.Split(q, "&") {
		if nv == "" {
			continue
		}
		k, v, _ := strings.Cut(nv, "=")
		k = pyUnquote(strings.ReplaceAll(k, "+", " "))
		v = pyUnquote(strings.ReplaceAll(v, "+", " "))
		switch cur := out[k].(type) {
		case nil:
			out[k] = v
		case string:
			out[k] = []any{cur, v}
		case []any:
			out[k] = append(cur, v)
		}
	}
	return out
}

// pyUnquote is Python's unquote(s, 'utf-8', 'replace'): each run of ASCII is
// percent-decoded to bytes (%XX with two hex digits; any other '%' stays) and
// decoded as UTF-8 with errors='replace'; non-ASCII text passes as it is.
func pyUnquote(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(s[i:])
			out.WriteString(s[i : i+size])
			i += size
			continue
		}
		j := i
		for j < len(s) && s[j] < utf8.RuneSelf {
			j++
		}
		var b []byte
		for k := i; k < j; k++ {
			if s[k] == '%' && k+2 < j && isHex(s[k+1]) && isHex(s[k+2]) {
				v, _ := strconv.ParseUint(s[k+1:k+3], 16, 8)
				b = append(b, byte(v))
				k += 2
				continue
			}
			b = append(b, s[k])
		}
		out.WriteString(pyDecode(b))
		i = j
	}
	return out.String()
}

// pyDecode is bytes.decode('utf-8', 'replace'): one U+FFFD per maximal
// subpart of an invalid sequence. The maximal subpart is the longest prefix
// that utf8.FullRune still calls incomplete -- a lead byte and the
// continuation bytes that could have completed it --, or one byte.
func pyDecode(b []byte) string {
	var out strings.Builder
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		if r != utf8.RuneError || size > 1 {
			out.Write(b[:size])
			b = b[size:]
			continue
		}
		n := 1
		for k := 1; k <= 3 && k <= len(b); k++ {
			if !utf8.FullRune(b[:k]) {
				n = k
			}
		}
		out.WriteRune(utf8.RuneError)
		b = b[n:]
	}
	return out.String()
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// checkFamily: src and dst are addresses of the family version names.
func checkFamily(p *Payload) error {
	v := p.Network.IP.Version
	for _, s := range []string{p.Network.IP.Src, p.Network.IP.Dst} {
		a, err := netip.ParseAddr(s)
		if err != nil || a.Zone() != "" || a.Is4() != (v == 4) || a.Is4In6() {
			return fmt.Errorf("network.ip %q is not an IPv%d address", s, v)
		}
	}
	return nil
}

// NoDuplicateKeys refuses a JSON document with the same name twice in one
// object. encoding/json, the schema library, jsonb and MySQL JSON all keep the
// last one, so a duplicate is a value lost without a word -- and invisible to
// every check that decodes first.
func NoDuplicateKeys(raw []byte) error {
	type frame struct {
		obj, key bool
		seen     map[string]bool
	}
	var st []*frame
	done := func() {
		if n := len(st); n > 0 && st[n-1].obj {
			st[n-1].key = true
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		t, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if n := len(st); n > 0 && st[n-1].obj && st[n-1].key {
			if t == json.Delim('}') {
				st = st[:n-1]
				done()
				continue
			}
			k := t.(string)
			if st[n-1].seen[k] {
				return fmt.Errorf("duplicate JSON key %q", k)
			}
			st[n-1].seen[k] = true
			st[n-1].key = false
			continue
		}
		switch t {
		case json.Delim('{'):
			st = append(st, &frame{obj: true, key: true, seen: map[string]bool{}})
		case json.Delim('['):
			st = append(st, &frame{})
		case json.Delim(']'):
			st = st[:len(st)-1]
			done()
		default:
			done()
		}
	}
}
