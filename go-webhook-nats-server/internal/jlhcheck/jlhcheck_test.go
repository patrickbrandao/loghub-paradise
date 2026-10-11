package jlhcheck

import (
	"reflect"
	"testing"
)

// The oracle is code too: these pin its Python ports to values taken from
// CPython 3 (parse_qsl, bytes.decode('utf-8', 'replace'), urlsplit), so a bug
// here cannot quietly agree with a bug in the serialiser.

func TestParseQSLMatchesPython(t *testing.T) {
	for q, want := range map[string]map[string]any{
		"a=1&a=2&b":                        {"a": []any{"1", "2"}, "b": ""},
		"%E2%80=x":                         {"\uFFFD": "x"},
		"a+b=%2B":                          {"a b": "+"},
		"x=%zz%4":                          {"x": "%zz%4"},
		"x=%ED%A0%80":                      {"x": "\uFFFD\uFFFD\uFFFD"},
		"&&a=&=v":                          {"a": "", "": "v"},
		"x=%C3%A9&x=%c3%a9&x=%F0%9F%98%80": {"x": []any{"\u00E9", "\u00E9", "\U0001F600"}},
		"k=%FF&k=%FE":                      {"k": []any{"\uFFFD", "\uFFFD"}},
	} {
		if got := parseQSL(q); !reflect.DeepEqual(got, want) {
			t.Errorf("parseQSL(%q) = %#v, want %#v", q, got, want)
		}
	}
}

func TestPyDecodeMatchesPython(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"a\xffb", "a\uFFFDb"},
		{"\xed\xa0\x80", "\uFFFD\uFFFD\uFFFD"},
		{"abc\xe2\x82", "abc\uFFFD"},
		{"\xf0\x9f\x98", "\uFFFD"},
		{"\xf4\x90\x80\x80", "\uFFFD\uFFFD\uFFFD\uFFFD"},
		{"\xe0\x80\x80", "\uFFFD\uFFFD\uFFFD"},
		{"\xc0\xaf", "\uFFFD\uFFFD"},
		{"\xe2\x82\xe2\x82\xac", "\uFFFD\u20AC"},
		{"\xef\xbf\xbd\xff", "\uFFFD\uFFFD"},
	} {
		if got := pyDecode([]byte(tc.in)); got != tc.want {
			t.Errorf("pyDecode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestURLSplitMatchesPython(t *testing.T) {
	scheme, netloc, path, query := urlsplit("http://User@H.Example:8080/p/a%2Fb?q=1&r#frag")
	host, port := hostPort(netloc)
	if scheme != "http" || netloc != "User@H.Example:8080" || path != "/p/a%2Fb" || query != "q=1&r" || host != "h.example" || port != "8080" {
		t.Errorf("urlsplit: %q %q %q %q, hostname %q port %q", scheme, netloc, path, query, host, port)
	}
	if host, port := hostPort("[2001:DB8::1]:8443"); host != "2001:db8::1" || port != "8443" {
		t.Errorf("bracketed host: %q %q", host, port)
	}
}

func TestNoDuplicateKeys(t *testing.T) {
	for doc, dup := range map[string]bool{
		`{"a":1,"a":2}`:                     true,
		`{"x":[{"a":1,"a":2}]}`:             true,
		`{"a":"}","a":1}`:                   true,
		`{"q":{"\ufffd":"1","\ufffd":"2"}}`: true,
		`{"a":{"b":1},"b":{"b":2}}`:         false,
		`[{"a":1},{"a":1}]`:                 false,
		`{"a":[],"b":{},"c":[{}]}`:          false,
	} {
		if err := NoDuplicateKeys([]byte(doc)); (err != nil) != dup {
			t.Errorf("%s: err %v, want duplicate %v", doc, err, dup)
		}
	}
}

func TestURIRegexp(t *testing.T) {
	for u, ok := range map[string]bool{
		"http://a.b/":                        true,
		"https://[::1]:8443/x%2Fy?a=%22b%22": true,
		"http://a.b/a[b]?k=a+b&u=:@/?":       true,
		"http://a.b/p?a=1#b":                 false,
		"http://a.b/p?a=\"b\"":               false,
		"http://a.b/p?a=%zz":                 false,
		"http://a.b/p?a[]=1":                 false,
		"http://a.b/p?a=\u00e9":              false,
	} {
		if uriRE.MatchString(u) != ok {
			t.Errorf("%q: match %v, want %v", u, !ok, ok)
		}
	}
}
