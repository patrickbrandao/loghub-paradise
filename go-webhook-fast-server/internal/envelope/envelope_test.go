package envelope

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/jlhcheck"
)

const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// sample is a request behind a trusted proxy that ended TLS.
func sample() *Request {
	return &Request{
		Mode:      ModeFast,
		Src:       netip.MustParseAddr("203.0.113.9"),
		Dst:       netip.MustParseAddr("0.0.0.0"),
		SPort:     0,
		DPort:     443,
		SecProxy:  true,
		FP4:       "t13d1516h2_8daaf6152771_02713d6af862",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  51234,
		SrcSource: "xff",
		Proto:     "HTTP/1.1",
		Method:    "POST",
		Host:      "api.exemplo.com",
		Path:      "/hook/telegram",
		RawQuery:  "token=abc&tag=a&tag=b&flag",
		Header: http.Header{
			"Content-Type":      {"application/json"},
			"X-Forwarded-For":   {"203.0.113.9"},
			"X-Forwarded-Proto": {"https"},
			"Connection":        {"keep-alive, X-Hop"},
			"X-Hop":             {"per-connection"},
			"Keep-Alive":        {"timeout=5"},
			"X-Multi":           {"one", "two"},
		},
		HostHeader:  "api.exemplo.com",
		ContentType: "application/json",
		Body:        []byte(`{"update_id":1}`),
	}
}

func header(t time.Time) *Header {
	return &Header{UUID: NewUUID(t), From: testFrom, Space: "default", Type: Type,
		Level: Level, SID: 4242, Event: EventAt(t)}
}

func build(t *testing.T, r *Request) []byte {
	t.Helper()
	return AppendWebhook(nil, header(time.Now()), r, PlanBody(r.Body))
}

// validate is the contract of SPEC §15.1: both schemas and the semantic rules.
func validate(t *testing.T, env []byte) *jlhcheck.Envelope {
	t.Helper()
	e, err := jlhcheck.Validate(env)
	if err != nil {
		t.Fatalf("%v\n%s", err, env)
	}
	return e
}

func TestEnvelopeContract(t *testing.T) {
	env := build(t, sample())
	e := validate(t, env)
	p := &e.Payload
	if e.From != testFrom || e.Space != "default" || e.Type != "webhook" || e.Level != 9 ||
		e.SID != 4242 || e.Step != 0 {
		t.Errorf("header %+v", e)
	}
	if p.Mode != "fast" || p.Method != "POST" || p.Schema != "https" || p.Host != "api.exemplo.com" ||
		p.Path != "/hook/telegram" || p.URL != "https://api.exemplo.com/hook/telegram?token=abc&tag=a&tag=b&flag" {
		t.Errorf("payload %+v", p)
	}
	if p.Network.IP.Version != 4 || p.Network.IP.Src != "203.0.113.9" || p.Network.IP.Dst != "0.0.0.0" ||
		p.Network.L4.DPort != 443 || p.Network.Sec.Mode != "proxy" || p.Network.Sec.FP4 == "" {
		t.Errorf("network %+v", p.Network)
	}
	for k, want := range map[string]any{"peer-addr": "10.0.0.7", "peer-port": float64(51234),
		"src-source": "xff", "http-proto": "HTTP/1.1"} {
		if v, ok := p.Meta(k); !ok || v != want {
			t.Errorf("meta %s = %v, want %v", k, v, want)
		}
	}
	if len(p.Params) != 0 {
		t.Errorf("params %v, want {}", p.Params)
	}
	q, _ := json.Marshal(p.Query)
	if string(q) != `{"flag":"","tag":["a","b"],"token":"abc"}` {
		t.Errorf("query %s", q)
	}
	if p.Content.Encoding != "utf-8" || p.Content.Body != `{"update_id":1}` || p.Content.Length != 15 {
		t.Errorf("content %+v", p.Content)
	}
}

// SPEC §3.2: lowercase names, sorted by name, values of one name in arrival
// order, host put back from r.Host, hop-by-hop and Connection-listed names out.
func TestHeaders(t *testing.T) {
	e := validate(t, build(t, sample()))
	var got []string
	for _, h := range e.Payload.Headers {
		got = append(got, h.Name+"="+h.Value)
	}
	want := []string{"content-type=application/json", "host=api.exemplo.com",
		"x-forwarded-for=203.0.113.9", "x-forwarded-proto=https", "x-multi=one", "x-multi=two"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("headers\n got %v\nwant %v", got, want)
	}

	r := sample()
	r.Exclude = ExcludeSet([]string{"X-MULTI", " host "})
	r.HostHeader = "API.Exemplo.com:8443"
	e = validate(t, build(t, r))
	for _, h := range e.Payload.Headers {
		if h.Name == "x-multi" || h.Name == "host" {
			t.Errorf("excluded header %s is in the envelope", h.Name)
		}
	}
	// No redaction by default (SPEC §3.4).
	r = sample()
	r.Header.Set("Authorization", "Bearer s3cr3t")
	if !bytes.Contains(build(t, r), []byte(`"Bearer s3cr3t"`)) {
		t.Error("authorization was redacted; SPEC §3.4 captures it whole")
	}
}

func TestHostAndURL(t *testing.T) {
	for _, tc := range []struct {
		host     string
		secProxy bool
		port     uint16
		path     string
		query    string
		want     string
	}{
		{"a.b", false, 0, "/", "", "http://a.b/"},
		{"a.b", false, 8080, "/x", "q=1", "http://a.b:8080/x?q=1"},
		{"a.b", true, 8443, "/x%2Fy", "", "https://a.b:8443/x%2Fy"},
		{"2804:3d28:0:100::9", true, 8443, "/ingest/gz", "", "https://[2804:3d28:0:100::9]:8443/ingest/gz"},
		// The query is raw, except what RFC 3986 does not allow in one.
		{"::1", false, 0, "/", "a=\"b\"", `http://[::1]/?a=%22b%22`},
		{"a.b", false, 0, "/", "a=1#b&c=%zz&d=%41%%42", "http://a.b/?a=1%23b&c=%25zz&d=%41%25%42"},
		{"a.b", false, 0, "/", "x=%C3\xa9&y=<>[]{|}^`\\ \x7f", "http://a.b/?x=%C3%A9&y=%3C%3E%5B%5D%7B%7C%7D%5E%60%5C%20%7F"},
		{"a.b", false, 0, "/", "k=a+b&u=:@/?!$'()*,;=-._~", "http://a.b/?k=a+b&u=:@/?!$'()*,;=-._~"},
	} {
		r := sample()
		r.Host, r.SecProxy, r.URLPort, r.Path, r.RawQuery = tc.host, tc.secProxy, tc.port, tc.path, tc.query
		if !tc.secProxy {
			r.FP4 = ""
		}
		if strings.Contains(tc.host, ":") {
			r.Src, r.Dst = netip.MustParseAddr("2804:14c:65a1:8f3e::1b"), netip.IPv6Unspecified()
		}
		e := validate(t, build(t, r))
		if e.Payload.URL != tc.want {
			t.Errorf("url %q, want %q", e.Payload.URL, tc.want)
		}
	}
}

// SPEC §3.2: utf-8 only for valid UTF-8 without NUL whose escape is not larger
// than the base64; the pre-pass size is exact.
func TestPlanBody(t *testing.T) {
	for _, tc := range []struct {
		body   string
		base64 bool
	}{
		{"", false},
		{`{"a":1}`, false},
		{"héllo ☃ 🔥", false},
		{"a\x00b", true},
		{"a\xffb", true},
		{"\x01\x02\x03\x04\x05\x06", true}, // 36 escaped > 8 base64
		{strings.Repeat(`"`, 30), false},   // 60 escaped < 40? no: 60 > 40 -> base64
		{"line\nbreak\ttab\r", false},
		{"\u2028", true}, // 6 escaped > 4 base64
	} {
		plan := PlanBody([]byte(tc.body))
		n, text := escapedLen([]byte(tc.body))
		want := tc.base64
		if strings.Count(tc.body, `"`) == 30 {
			want = n > base64.StdEncoding.EncodedLen(len(tc.body))
		}
		if plan.Base64 != want || (!text && !plan.Base64) {
			t.Errorf("PlanBody(%q) = %+v, want base64=%v", tc.body, plan, want)
		}
		r := sample()
		r.Body = []byte(tc.body)
		env := AppendWebhook(nil, header(time.Now()), r, plan)
		e := validate(t, env)
		got, err := e.Payload.Body()
		if err != nil || string(got) != tc.body && utf8.ValidString(tc.body) {
			t.Errorf("body %q came back %q (%v)", tc.body, got, err)
		}
		if tc.body == "" && (e.Payload.Content.Encoding != "utf-8" || e.Payload.Content.Body != "") {
			t.Errorf("empty body: %+v", e.Payload.Content)
		}
		// The pre-pass measured exactly what was written between the quotes.
		_, payload, err := ParseHead(env)
		if err != nil {
			t.Fatal(err)
		}
		i := bytes.Index(payload, []byte(`"body":"`)) + len(`"body":"`)
		if j := bytes.LastIndexByte(payload, '"'); j-i != plan.Len {
			t.Errorf("PlanBody(%q).Len = %d, written %d", tc.body, plan.Len, j-i)
		}
	}
}

// Every byte value round-trips: in base64 exactly, in utf-8 only for text.
func TestBinaryBodyRoundTrips(t *testing.T) {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	r := sample()
	r.Body = all
	e := validate(t, build(t, r))
	got, err := e.Payload.Body()
	if err != nil || !bytes.Equal(got, all) || e.Payload.Content.Encoding != "base64" {
		t.Fatalf("binary body did not round-trip (%s, %v)", e.Payload.Content.Encoding, err)
	}
}

// SPEC §3.2: Python's parse_qsl(keep_blank_values=True), byte for byte.
func TestAppendQuery(t *testing.T) {
	for in, want := range map[string]string{
		"":                 `{}`,
		"&&":               `{}`,
		"a=1":              `{"a":"1"}`,
		"a=1&b=2&a=3":      `{"a":["1","3"],"b":"2"}`,
		"debug":            `{"debug":""}`,
		"=v":               `{"":"v"}`,
		"a=b=c":            `{"a":"b=c"}`,
		"a+b=c+d":          `{"a b":"c d"}`,
		"x=%2B%20%41":      `{"x":"+ A"}`,
		"x=%zz%4":          `{"x":"%zz%4"}`,
		"x=%C3%A9":         `{"x":"é"}`,
		"x=%FF":            `{"x":"\ufffd"}`,
		"x=%00":            `{"x":"\u0000"}`,
		"x=%22%5C":         `{"x":"\"\\"}`,
		"a;b=1":            `{"a;b":"1"}`,
		"t=a&t=b&t=c&u=":   `{"t":["a","b","c"],"u":""}`,
		"%E2%80%A8=\u2028": `{"\u2028":"\u2028"}`,
		// One U+FFFD per maximal invalid subpart, as Python's parse_qsl.
		"x=%E2%80":    `{"x":"\ufffd"}`,
		"x=%F0%9F%98": `{"x":"\ufffd"}`,
		"x=%ED%A0%80": `{"x":"\ufffd\ufffd\ufffd"}`,
		"x=%C3\xa9":   `{"x":"é"}`,
	} {
		got := string(AppendQuery(nil, in))
		var a, b any
		if err := json.Unmarshal([]byte(got), &a); err != nil {
			t.Fatalf("AppendQuery(%q) = %s: %v", in, got, err)
		}
		if err := json.Unmarshal([]byte(want), &b); err != nil {
			t.Fatal(err)
		}
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if string(ja) != string(jb) {
			t.Errorf("AppendQuery(%q) = %s, want %s", in, got, want)
		}
	}
	// Keys in the order of their first occurrence.
	if got := string(AppendQuery(nil, "z=1&a=2&z=3")); got != `{"z":["1","3"],"a":"2"}` {
		t.Errorf("order: %s", got)
	}
	// Keys that are written alike are one key: %FF and %FE are both U+FFFD,
	// and a duplicate JSON name would lose a value in every parser. Compared
	// as bytes, because json.Unmarshal itself keeps only the last duplicate.
	for in, want := range map[string]string{
		"%FF=1&%FE=2":        "{\"\uFFFD\":[\"1\",\"2\"]}",
		"\xff=1&%EF%BF%BD=2": "{\"\uFFFD\":[\"1\",\"2\"]}",
		"%E2%80=1&%E2=2":     "{\"\uFFFD\":[\"1\",\"2\"]}",
	} {
		if got := string(AppendQuery(nil, in)); got != want {
			t.Errorf("AppendQuery(%q) = %s, want %s", in, got, want)
		}
	}
}

// SPEC §12.1: what AppendWebhook writes, ParseHead reads back exactly, and the
// payload it returns is a sub-slice of the envelope.
func TestParseHeadRoundTrip(t *testing.T) {
	now := time.Now()
	h := header(now)
	r := sample()
	env := AppendWebhook(nil, h, r, PlanBody(r.Body))
	got, payload, err := ParseHead(env)
	if err != nil {
		t.Fatal(err)
	}
	if got.UUID != h.UUID || got.From != h.From || got.Space != h.Space || got.Type != h.Type ||
		got.Level != int(h.Level) || got.SID != h.SID || got.Step != int(h.Step) || got.Event != h.Event {
		t.Errorf("ParseHead = %+v, want %+v", got, h)
	}
	if &payload[0] != &env[len(env)-1-len(payload)] || payload[0] != '{' {
		t.Error("the payload is not a sub-slice of the envelope ending before its closing brace")
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(env, &doc); err != nil || !bytes.Equal(doc["payload"], payload) {
		t.Errorf("payload slice differs from the decoded payload (%v)", err)
	}
}

// Anything that is not exactly what this server writes is refused, never
// half-read -- the v1 envelope of an old ring.db or dump above all.
func TestParseHeadRefuses(t *testing.T) {
	good := string(build(t, sample()))
	mut := func(old, new string) []byte { return []byte(strings.Replace(good, old, new, 1)) }
	e := validate(t, []byte(good))
	cases := map[string][]byte{
		"empty":              nil,
		"v1 envelope":        []byte(`{"id":"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f","ts":1757116800}`),
		"truncated":          []byte(good[:len(good)/2]),
		"no closing brace":   []byte(good[:len(good)-1]),
		"whitespace":         mut(`{"uuid":`, `{ "uuid":`),
		"uuid v4":            mut(e.UUID, e.UUID[:14]+"4"+e.UUID[15:]),
		"uppercase from":     mut(testFrom, strings.ToUpper(testFrom)),
		"uppercase uuid":     mut(e.UUID, strings.ToUpper(e.UUID)),
		"from v4":            mut(testFrom, testFrom[:14]+"4"+testFrom[15:]),
		"space grammar":      mut(`"space":"default"`, `"space":"Default"`),
		"empty space seg":    mut(`"space":"default"`, `"space":"a..b"`),
		"level 16":           mut(`"level":9`, `"level":16`),
		"leading zero":       mut(`"level":9`, `"level":09`),
		"step 33":            mut(`"step":0`, `"step":33`),
		"sid above 2^53":     mut(`"sid":4242`, `"sid":9007199254740992`),
		"iso mismatch":       mut(`.`+e.Event.ISO[20:24], `.`+"999Z"),
		"payload not object": mut(`"payload":{`, `"payload":[{`),
	}
	for name, env := range cases {
		if _, _, err := ParseHead(env); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// One clock sample: the uuid's level-3 instant is the event (SPEC §3.1). The
// oracle checks it on every envelope; this pins it on a known instant.
func TestUUIDAndEventShareTheSample(t *testing.T) {
	at := time.Date(2026, 10, 6, 17, 3, 26, 57932925, time.UTC)
	h := header(at)
	got, ok := h.UUID.TimestampWithLevel(uuidv7.Level3)
	if !ok || !got.Equal(at) || h.Event.Time() != at {
		t.Errorf("uuid %v event %v, want %v", got, h.Event.Time(), at)
	}
	var b [24]byte
	if iso := string(h.Event.AppendISO(b[:0])); iso != "2026-10-06T17:03:26.057Z" {
		t.Errorf("iso %s: must truncate, never round", iso)
	}
}

// SPEC §3.2: the computed ceiling holds for adversarial requests inside the
// three limits the handler enforces.
func TestWorstCaseEnvelopeIsBounded(t *testing.T) {
	const B, H, U = 3000, 2000, 600
	bound := MaxEventBytes(B, H, U)

	worst := func(fill func(r *Request)) {
		t.Helper()
		r := sample()
		r.Header = http.Header{}
		r.HostHeader, r.ContentType, r.FP4 = "", "", ""
		fill(r)
		env := build(t, r)
		if int64(len(env)) > bound {
			t.Errorf("envelope of %d bytes exceeds the bound %d", len(env), bound)
		}
	}
	// Body: every byte a control character (escape 6x, so base64 wins).
	worst(func(r *Request) { r.Body = bytes.Repeat([]byte{1}, B) })
	// Body: quotes, the worst text that stays utf-8 (2x < 4/3? no: base64).
	worst(func(r *Request) { r.Body = bytes.Repeat([]byte(`"`), B) })
	// Head: as many one-byte headers as the budget allows (23 bytes of frame
	// per line), each with an obs-text value that escapes to U+FFFD.
	worst(func(r *Request) {
		for i := 0; i < H/6; i++ {
			r.Header["X"+strings.Repeat("a", i%3)+string(rune('a'+i%26))] = append(
				r.Header["X"+strings.Repeat("a", i%3)+string(rune('a'+i%26))], "\xff")
		}
	})
	// Head: one huge obs-text value duplicated in host, content-type, fp*.
	worst(func(r *Request) {
		v := strings.Repeat("\xff", H/8)
		r.HostHeader, r.Host, r.ContentType = v, strings.ToLower(v), v
		r.Header["Content-Type"] = []string{v}
		r.SecProxy, r.FP2, r.FP3, r.FP4 = true, v, v, v
	})
	// URI: "a&" pairs and %01 escapes in the query, escaped bytes in the path.
	worst(func(r *Request) {
		r.RawQuery = strings.Repeat("a&", U/4) + strings.Repeat("%01", U/6)
		r.Path = "/" + strings.Repeat("%FF", U/6)
	})
}

// ------------------------------------------------------------------ escaping

func allBytes() string {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return string(b)
}

func TestEscaping(t *testing.T) {
	// want is CPython 3's bytes.decode('utf-8', 'replace'): one U+FFFD per
	// maximal invalid subpart (SPEC §3.2), which is what the skill's
	// validate.py reads back from the url.
	cases := []struct{ in, want string }{
		{"he said \"hi\"", "he said \"hi\""},
		{"C:\\path\\to", "C:\\path\\to"},
		{"line1\x0aline2", "line1\u000Aline2"},
		{"a\x0db", "a\u000Db"},
		{"a\x09b", "a\u0009b"},
		{"a\x00b", "a\u0000b"},
		{"\x01\x02\x03\x1f", "\u0001\u0002\u0003\u001F"},
		{"a\x7fb", "a\u007Fb"},
		{"h\xc3\xa9llo w\xc3\xb6rld \xe2\x98\x83", "h\u00E9llo w\u00F6rld \u2603"},
		{"\xf0\x9f\x94\xa5\xf0\x9f\x9a\x80", "\U0001F525\U0001F680"},
		{"a\xffb", "a\uFFFDb"},
		{"\xed\xa0\x80", "\uFFFD\uFFFD\uFFFD"},
		{"abc\xe2\x82", "abc\uFFFD"},
		{"a\xe2\x80\xa8b", "a\u2028b"},
		{"a\xe2\x80\xa9b", "a\u2029b"},
		{"<a&b>", "<a&b>"},
		{"\xe2\x80", "\uFFFD"},
		{"\xf0\x9f\x98", "\uFFFD"},
		{"\xc3", "\uFFFD"},
		{"\xf4\x90\x80\x80", "\uFFFD\uFFFD\uFFFD\uFFFD"},
		{"\xe0\x80\x80", "\uFFFD\uFFFD\uFFFD"},
		{"\xf0\x80", "\uFFFD\uFFFD"},
		{"\xc0\xaf", "\uFFFD\uFFFD"},
		{"\xf8\x88\x80\x80\x80", "\uFFFD\uFFFD\uFFFD\uFFFD\uFFFD"},
		{"\xe2\x82\xe2\x82\xac", "\uFFFD\u20AC"},
		{"\xf0\x9f\x98\x80\x9f", "\U0001F600\uFFFD"},
		{"\xef\xbf\xbd\xff", "\uFFFD\uFFFD"},
		{"\xe2\x80x\xe2", "\uFFFDx\uFFFD"},
		{allBytes(), allBytes()[:128] + strings.Repeat("\uFFFD", 128)},
	}
	for _, tc := range cases {
		in := tc.in
		out := AppendString(nil, in)
		if !utf8.Valid(out) {
			t.Errorf("%q: output is not valid UTF-8", in)
		}
		var back string
		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatalf("%q: %s is not JSON: %v", in, out, err)
		}
		if back != tc.want {
			t.Errorf("%q decoded as %q, want %q", in, back, tc.want)
		}
		if utf8.ValidString(in) {
			// Valid text is encoding/json's, byte for byte once decoded.
			want, _ := json.Marshal(in)
			var std string
			_ = json.Unmarshal(want, &std)
			if back != std {
				t.Errorf("%q decoded as %q, encoding/json gives %q", in, back, std)
			}
		}
		if got := validText(in); got != tc.want {
			t.Errorf("validText(%q) = %q, want %q", in, got, tc.want)
		}
		if b := AppendBytes(nil, []byte(in)); !bytes.Equal(b, out) {
			t.Errorf("%q: AppendBytes and AppendString disagree", in)
		}
		if n, _ := escapedLen([]byte(in)); n != len(out)-2 {
			t.Errorf("%q: escapedLen %d, written %d", in, n, len(out)-2)
		}
	}
	if got := string(AppendString(nil, "<a&b>")); got != `"<a&b>"` {
		t.Errorf("<, > and & must stay as they are: %s", got)
	}
}

// FuzzEnvelope: whatever arrives inside the limits, the envelope is valid --
// both schemas and the semantic rules -- and its header reads back.
func FuzzEnvelope(f *testing.F) {
	f.Add("POST", "/x", "a=1&b", "application/json", "x-a", "v", []byte(`{"a":1}`))
	f.Add("GET", "/%FF", "%00=%FF&&", "", "x-b", "\xff\t", []byte{0, 1, 2})
	f.Add("PUT", "/", "", "text/plain", "x-c", "", []byte("\u2028\""))
	f.Fuzz(func(t *testing.T, method, path, query, ctype, hname, hval string, body []byte) {
		// Only what net/http can hand the handler: a request target has no
		// control character or space (a '#' it does take, into RawQuery),
		// and a header value no CR, LF or NUL.
		if !validMethodForTest(method) || path == "" || path[0] != '/' || !targetForTest(path+query) ||
			!validTokenForTest(hname) || strings.ContainsAny(hval, "\r\n\x00") {
			t.Skip()
		}
		// The path and query exactly as the handler gets them: net/http
		// refuses a target that does not parse (an invalid escape in the
		// path is a 400 before any handler runs).
		u, err := url.ParseRequestURI(path + "?" + query)
		if err != nil {
			t.Skip()
		}
		r := sample()
		r.Method, r.Path, r.RawQuery, r.ContentType, r.Body = method, u.EscapedPath(), u.RawQuery, ctype, body
		r.Header = http.Header{http.CanonicalHeaderKey(hname): {hval}}
		if ctype != "" {
			r.Header["Content-Type"] = []string{ctype}
		}
		env := build(t, r)
		e, err := jlhcheck.Validate(env)
		if err != nil {
			t.Fatalf("%v\n%s", err, env)
		}
		got, err := e.Payload.Body()
		if err != nil || (e.Payload.Content.Encoding == "base64" && !bytes.Equal(got, body)) {
			t.Fatalf("body did not round-trip")
		}
		if _, _, err := ParseHead(env); err != nil {
			t.Fatal(err)
		}
	})
}

func validMethodForTest(m string) bool {
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

func targetForTest(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= ' ' || c == 0x7f {
			return false
		}
	}
	return true
}

func validTokenForTest(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return !hopByHop[s] && s != "connection" && s != "host" && s != "content-type"
}

// ------------------------------------------------------------------- pools

func TestClassFor(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want int
	}{
		{0, 0}, {1, 0}, {4 << 10, 0}, {4<<10 + 1, 1}, {16 << 10, 1}, {16<<10 + 1, 2},
		{64 << 10, 2}, {64<<10 + 1, 3}, {256 << 10, 3},
		// SPEC §12.2: above 256 KiB nothing is pooled.
		{256<<10 + 1, -1}, {5 << 20, -1},
	} {
		if got := classFor(tc.n); got != tc.want {
			t.Errorf("classFor(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}
}

func TestGetBodyCapacity(t *testing.T) {
	for _, n := range []int64{0, 100, 4 << 10, 20 << 10, 100 << 10, 256 << 10, 1 << 20, 5 << 20} {
		buf, class := GetBody(n)
		if len(buf) != 0 || int64(cap(buf)) < n || (n > PoolMaxBytes && class != -1) {
			t.Errorf("GetBody(%d): len %d cap %d class %d", n, len(buf), cap(buf), class)
		}
		PutBody(buf, class)
	}
	PutBody(nil, 0)
	PutBody(nil, -1)
	PutBody(make([]byte, 10), 99)
}

// SPEC §12.3: the buffer returns to the pool exactly once.
func TestReleaseIsIdempotent(t *testing.T) {
	env, class := GetEnv(1024)
	it := NewItem("id", append(env, "jlh"...), class)
	if it.Released() {
		t.Fatal("a new item must not be marked released")
	}
	it.Release()
	if !it.Released() || it.Env != nil {
		t.Fatal("Release must mark the item and clear the reference")
	}
	it.Release()
	var nilItem *Item
	nilItem.Release()
}

// SPEC §12.3, error paths: Discard and a tainted Release leave the buffer to
// the GC -- a lower layer may still be reading it.
func TestDiscardAndTaintDoNotPool(t *testing.T) {
	for _, finish := range []func(*Item){
		func(it *Item) { it.Discard() },
		func(it *Item) { it.Taint(); it.Release() },
	} {
		env, class := GetEnv(1024)
		it := NewItem("id", append(env, "jlh"...), class)
		finish(it)
		if !it.Released() || string(it.Env) != "jlh" {
			t.Fatal("the buffer was touched or the item not finished")
		}
		again, aclass := GetEnv(1024)
		if aclass == class && &again[:1][0] == &it.Env[:1][0] {
			t.Fatal("the buffer was handed out again by the pool")
		}
		PutEnv(again, aclass)
		it.Release()
		if it.Env == nil {
			t.Fatal("a later Release must not pool or clear")
		}
	}
	var nilItem *Item
	nilItem.Discard()
	nilItem.Taint()
}

func TestItemSizeAndAck(t *testing.T) {
	it := NewItem("id", []byte("12345"), -1)
	if it.Size() != 5 {
		t.Errorf("Size = %d", it.Size())
	}
	it.Ack(nil) // no waiter: must not block
	ch := it.Done()
	it.Ack(nil)
	select {
	case err := <-ch:
		if err != nil {
			t.Error(err)
		}
	default:
		t.Error("Ack did not deliver")
	}
	it.Ack(nil)
	it.Ack(nil)
}

// ------------------------------------------------------------------ bench

func BenchmarkAppendWebhook(b *testing.B) {
	r := sample()
	h := header(time.Now())
	plan := PlanBody(r.Body)
	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendWebhook(buf[:0], h, r, plan)
	}
}

func BenchmarkParseHead(b *testing.B) {
	env := AppendWebhook(nil, header(time.Now()), sample(), PlanBody(sample().Body))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := ParseHead(env); err != nil {
			b.Fatal(err)
		}
	}
}

// A query of thousands of distinct keys -- all max_uri_bytes allows -- is
// grouped in linear time: a nested scan was a CPU amplifier.
func TestAppendQueryIsLinear(t *testing.T) {
	var b strings.Builder
	for i := 0; b.Len() < 64<<10; i++ {
		b.WriteString("k")
		b.WriteString(strconv.Itoa(i))
		b.WriteString("=&")
	}
	start := time.Now()
	out := AppendQuery(nil, b.String())
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("64 KiB of distinct keys took %v", d)
	}
	if !json.Valid(out) {
		t.Fatal("not JSON")
	}
}
