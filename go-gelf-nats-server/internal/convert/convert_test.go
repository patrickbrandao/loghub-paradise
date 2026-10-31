package convert

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlhcheck"
)

const (
	testFrom  = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"
	testSpace = "default"
)

var testRecv = time.Date(2026, 10, 31, 12, 0, 0, 123456000, time.UTC)

func udpHead() item.Head {
	h := item.Head{
		UUID: jlh.NewUUID(testRecv), From: testFrom, Space: testSpace,
		Transport: item.TransportUDP,
		SrcIP:     netip.MustParseAddr("10.0.0.3"), SrcPort: 59922,
		DstIP: netip.MustParseAddr("10.0.0.10"), DstPort: 12201,
	}
	h.SetTime(testRecv)
	return h
}

// conv converts doc with cfg, checks the whole oracle, and returns the
// envelope, its decoded payload and the result.
func conv(t testing.TB, cfg Config, h item.Head, doc string) ([]byte, map[string]any, Result) {
	t.Helper()
	cfg.Columns = true
	out, res := New(cfg).Append(nil, &h, []byte(doc))
	if err := jlhcheck.Validate(out); err != nil {
		t.Fatalf("oracle: %v\n%s", err, out)
	}
	var env map[string]any
	dec := json.NewDecoder(bytes.NewReader(out))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		t.Fatal(err)
	}
	return out, env["payload"].(map[string]any), res
}

func warnings(p map[string]any) []string {
	var out []string
	if pw, ok := p["parse"].(map[string]any); ok {
		if ws, ok := pw["warnings"].([]any); ok {
			for _, w := range ws {
				out = append(out, w.(string))
			}
		}
	}
	return out
}

// payloadJSON re-encodes a sub-value compactly for comparison.
func payloadJSON(t testing.TB, v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestConvertTable pins every rule of SPEC §6.5–§6.9, one row per rule. Each
// row names the payload keys it checks (as compact JSON) and the exact warning
// set; "raw" is checked for presence.
func TestConvertTable(t *testing.T) {
	type row struct {
		doc    string
		want   map[string]string // payload key -> compact JSON
		absent []string
		warn   []string
		raw    bool
	}
	base := `"version":"1.1","host":"h1","short_message":"m","level":6`
	rows := map[string]row{
		"minimal": {doc: `{` + base + `}`,
			want:   map[string]string{"gelf-version": `"1.1"`, "short-message": `"m"`, "level": `{"code":6,"name":"info"}`, "format": `"gelf"`},
			absent: []string{"raw", "parse", "fields", "extra"}},
		"hostname":       {doc: `{` + base + `}`, want: map[string]string{"host": `{"dst-ip":"10.0.0.10","dst-port":12201,"hostname":"h1","src-ip":"10.0.0.3","src-port":59922}`}},
		"no level":       {doc: `{"version":"1.1","host":"h","short_message":"m"}`, want: map[string]string{"level": `{"code":6,"name":"info"}`}, warn: []string{"level-default"}},
		"level 0":        {doc: `{"version":"1.1","host":"h","short_message":"m","level":0}`, want: map[string]string{"level": `{"code":0,"name":"emerg"}`}},
		"level 7":        {doc: `{"version":"1.1","host":"h","short_message":"m","level":7}`, want: map[string]string{"level": `{"code":7,"name":"debug"}`}},
		"level 3.0":      {doc: `{"version":"1.1","host":"h","short_message":"m","level":3.0}`, want: map[string]string{"level": `{"code":3,"name":"err"}`}, warn: []string{"level-coerced"}},
		"level 30e-1":    {doc: `{"version":"1.1","host":"h","short_message":"m","level":30e-1}`, want: map[string]string{"level": `{"code":3,"name":"err"}`}, warn: []string{"level-coerced"}},
		"level -0":       {doc: `{"version":"1.1","host":"h","short_message":"m","level":-0}`, want: map[string]string{"level": `{"code":0,"name":"emerg"}`}, warn: []string{"level-coerced"}},
		"level str":      {doc: `{"version":"1.1","host":"h","short_message":"m","level":"4"}`, want: map[string]string{"level": `{"code":4,"name":"warning"}`}, warn: []string{"level-coerced"}},
		"level 8":        {doc: `{"version":"1.1","host":"h","short_message":"m","level":8}`, want: map[string]string{"level": `{"code":6,"name":"info"}`}, warn: []string{"level-invalid"}, raw: true},
		"level 3.5":      {doc: `{"version":"1.1","host":"h","short_message":"m","level":3.5}`, warn: []string{"level-invalid"}, raw: true},
		"level name":     {doc: `{"version":"1.1","host":"h","short_message":"m","level":"info"}`, warn: []string{"level-invalid"}, raw: true},
		"level null":     {doc: `{"version":"1.1","host":"h","short_message":"m","level":null}`, warn: []string{"level-invalid"}, raw: true},
		"level huge exp": {doc: `{"version":"1.1","host":"h","short_message":"m","level":1e99999}`, warn: []string{"level-invalid"}, raw: true},
		"no version":     {doc: `{"host":"h","short_message":"m","level":6}`, absent: []string{"gelf-version"}, warn: []string{"version-missing"}},
		"version 1.0":    {doc: `{"version":"1.0","host":"h","short_message":"m","level":6}`, want: map[string]string{"gelf-version": `"1.0"`}},
		"version 2":      {doc: `{"version":"2.0","host":"h","short_message":"m","level":6}`, want: map[string]string{"gelf-version": `"2.0"`}, warn: []string{"version-unknown"}},
		"version num":    {doc: `{"version":1.1,"host":"h","short_message":"m","level":6}`, absent: []string{"gelf-version"}, warn: []string{"version-invalid"}, raw: true},
		"version empty":  {doc: `{"version":"","host":"h","short_message":"m","level":6}`, warn: []string{"version-invalid"}, raw: true},
		"no host":        {doc: `{"version":"1.1","short_message":"m","level":6}`, warn: []string{"host-missing"}},
		"host space":     {doc: `{"version":"1.1","host":"a b","short_message":"m","level":6}`, warn: []string{"host-invalid"}, raw: true},
		"host nbsp":      {doc: `{"version":"1.1","host":"a b","short_message":"m","level":6}`, warn: []string{"host-invalid"}, raw: true},
		"host num":       {doc: `{"version":"1.1","host":42,"short_message":"m","level":6}`, warn: []string{"host-invalid"}, raw: true},
		"host empty":     {doc: `{"version":"1.1","host":"","short_message":"m","level":6}`, warn: []string{"host-invalid"}, raw: true},
		"no short":       {doc: `{"version":"1.1","host":"h","level":6}`, want: map[string]string{"short-message": `""`}, warn: []string{"short-message-missing"}},
		"short null":     {doc: `{"version":"1.1","host":"h","short_message":null,"level":6}`, want: map[string]string{"short-message": `""`}, warn: []string{"short-message-missing"}},
		"short empty":    {doc: `{"version":"1.1","host":"h","short_message":"","level":6}`, want: map[string]string{"short-message": `""`}, warn: []string{"short-message-missing"}},
		"short num":      {doc: `{"version":"1.1","host":"h","short_message":1e400,"level":6}`, want: map[string]string{"short-message": `"1e400"`}, warn: []string{"short-message-coerced"}},
		"short obj":      {doc: `{"version":"1.1","host":"h","short_message":{ "a" : [1, "é"] },"level":6}`, want: map[string]string{"short-message": `"{\"a\":[1,\"é\"]}"`}, warn: []string{"short-message-coerced"}},
		"full":           {doc: `{` + base + `,"full_message":"line1\nline2"}`, want: map[string]string{"full-message": `"line1\nline2"`}},
		"full empty":     {doc: `{` + base + `,"full_message":""}`, absent: []string{"full-message"}},
		"full num":       {doc: `{` + base + `,"full_message":1}`, absent: []string{"full-message"}, warn: []string{"full-message-invalid"}, raw: true},
		"ts int":         {doc: `{` + base + `,"timestamp":1700000000}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.000Z"`}},
		"ts frac 1":      {doc: `{` + base + `,"timestamp":1700000000.5}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.500Z"`}},
		"ts frac 6":      {doc: `{` + base + `,"timestamp":1700000000.123456}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.123456Z"`}},
		"ts frac 10":     {doc: `{` + base + `,"timestamp":1700000000.1234567891}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.123456789Z"`}},
		"ts frac zeros":  {doc: `{` + base + `,"timestamp":1700000000.10}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.100Z"`}},
		"ts exp":         {doc: `{` + base + `,"timestamp":1.7e9}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.000Z"`}},
		"ts exp frac":    {doc: `{` + base + `,"timestamp":17000000005E-1}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.500Z"`}},
		"ts 0":           {doc: `{` + base + `,"timestamp":0}`, want: map[string]string{"timestamp": `"1970-01-01T00:00:00.000Z"`}},
		"ts max":         {doc: `{` + base + `,"timestamp":253402300799.999}`, want: map[string]string{"timestamp": `"9999-12-31T23:59:59.999Z"`}},
		"ts past max":    {doc: `{` + base + `,"timestamp":253402300800}`, absent: []string{"timestamp"}, warn: []string{"timestamp-invalid"}, raw: true},
		"ts neg":         {doc: `{` + base + `,"timestamp":-1}`, absent: []string{"timestamp"}, warn: []string{"timestamp-invalid"}, raw: true},
		"ts str":         {doc: `{` + base + `,"timestamp":"1700000000.25"}`, want: map[string]string{"timestamp": `"2023-11-14T22:13:20.250Z"`}, warn: []string{"timestamp-coerced"}},
		"ts iso str":     {doc: `{` + base + `,"timestamp":"2023-11-14T22:13:20Z"}`, warn: []string{"timestamp-invalid"}, raw: true},
		"ts huge":        {doc: `{` + base + `,"timestamp":1e400}`, warn: []string{"timestamp-invalid"}, raw: true},
		"ts tiny":        {doc: `{` + base + `,"timestamp":1e-400}`, want: map[string]string{"timestamp": `"1970-01-01T00:00:00.000000000Z"`}}, "ts exp 5 digits": {doc: `{` + base + `,"timestamp":1e-10000}`, warn: []string{"timestamp-invalid"}, raw: true},
		"facility":         {doc: `{` + base + `,"facility":"gelf-rb"}`, want: map[string]string{"facility": `"gelf-rb"`}},
		"facility num":     {doc: `{` + base + `,"facility":16}`, want: map[string]string{"facility": `"16"`}, warn: []string{"facility-coerced"}},
		"facility bool":    {doc: `{` + base + `,"facility":true}`, warn: []string{"facility-invalid"}, raw: true},
		"file line":        {doc: `{` + base + `,"file":"/app/x.rb","line":356}`, want: map[string]string{"file": `"/app/x.rb"`, "line": `356`}},
		"line str":         {doc: `{` + base + `,"line":"356"}`, want: map[string]string{"line": `356`}, warn: []string{"line-coerced"}},
		"line str zeros":   {doc: `{` + base + `,"line":"0042"}`, want: map[string]string{"line": `42`}, warn: []string{"line-coerced"}},
		"line str 00":      {doc: `{` + base + `,"line":"00"}`, want: map[string]string{"line": `0`}, warn: []string{"line-coerced"}},
		"line str long":    {doc: `{` + base + `,"line":"0000000000002147483647"}`, want: map[string]string{"line": `2147483647`}, warn: []string{"line-coerced"}},
		"line str big":     {doc: `{` + base + `,"line":"2147483648"}`, absent: []string{"line"}, warn: []string{"line-invalid"}, raw: true},
		"line float":       {doc: `{` + base + `,"line":356.0}`, want: map[string]string{"line": `356`}, warn: []string{"line-coerced"}},
		"line neg":         {doc: `{` + base + `,"line":-1}`, absent: []string{"line"}, warn: []string{"line-invalid"}, raw: true},
		"line big":         {doc: `{` + base + `,"line":2147483648}`, absent: []string{"line"}, warn: []string{"line-invalid"}, raw: true},
		"file num":         {doc: `{` + base + `,"file":1}`, warn: []string{"file-invalid"}, raw: true},
		"fields":           {doc: `{` + base + `,"_a":"x","_n":12345678901234567890,"_f":0.1,"_e":1e400}`, want: map[string]string{"fields": `{"a":"x","e":1e400,"f":0.1,"n":12345678901234567890}`}},
		"field coerced":    {doc: `{` + base + `,"_b":true,"_o":{"k":"v"},"_a":[1,2]}`, want: map[string]string{"fields": `{"a":"[1,2]","b":"true","o":"{\"k\":\"v\"}"}`}, warn: []string{"field-coerced"}},
		"field null":       {doc: `{` + base + `,"_a":null,"_b":"x"}`, want: map[string]string{"fields": `{"b":"x"}`}, warn: []string{"field-null"}},
		"field only null":  {doc: `{` + base + `,"_a":null}`, absent: []string{"fields"}, warn: []string{"field-null"}},
		"field id":         {doc: `{` + base + `,"_id":"abc"}`, want: map[string]string{"fields": `{"id":"abc"}`}, warn: []string{"field-id-reserved"}},
		"field name":       {doc: `{` + base + `,"_a b":"x","_":"y"}`, want: map[string]string{"fields": `{"":"y","a b":"x"}`}, warn: []string{"field-name-invalid"}},
		"field dots":       {doc: `{` + base + `,"_a.b-c_d":"x"}`, want: map[string]string{"fields": `{"a.b-c_d":"x"}`}},
		"extra":            {doc: `{` + base + `,"message":"hi","@timestamp":"2026-10-31T12:00:00Z","n":{"k" : [ 1 , null ]}}`, want: map[string]string{"extra": `{"@timestamp":"2026-10-31T12:00:00Z","message":"hi","n":{"k":[1,null]}}`}, warn: []string{"key-unknown"}},
		"extra null":       {doc: `{` + base + `,"x":null}`, absent: []string{"extra"}, warn: []string{"field-null"}},
		"extra nested dup": {doc: `{` + base + `,"x":{"a":1,"a":2}}`, want: map[string]string{"extra": `{"x":"{\"a\":1,\"a\":2}"}`}, warn: []string{"key-unknown", "duplicate-key"}, raw: true},
		"dup top":          {doc: `{` + base + `,"level":3}`, want: map[string]string{"level": `{"code":3,"name":"err"}`}, warn: []string{"duplicate-key"}, raw: true},
		"dup field":        {doc: `{` + base + `,"_a":"1","_b":"x","_a":"2"}`, want: map[string]string{"fields": `{"a":"2","b":"x"}`}, warn: []string{"duplicate-key"}, raw: true},
		"escaped key":      {doc: `{"version":"1.1","host":"h","short_message":"m","\u006cevel":3}`, want: map[string]string{"level": `{"code":3,"name":"err"}`}},
		"escaped key dup":  {doc: `{` + base + `,"\u006cevel":3}`, want: map[string]string{"level": `{"code":3,"name":"err"}`}, warn: []string{"duplicate-key"}, raw: true},
		"escaped field":    {doc: `{` + base + `,"\u005fa":"x"}`, want: map[string]string{"fields": `{"a":"x"}`}},
		"surrogate pair":   {doc: `{` + base + `,"_e":"\ud83d\ude00"}`, want: map[string]string{"fields": `{"e":"😀"}`}},
		"surrogate utf8":   {doc: `{` + base + `,"_e":"😀"}`, want: map[string]string{"fields": `{"e":"😀"}`}},
		"surrogates swap":  {doc: `{` + base + `,"_e":"\ude00\ud83d"}`, want: map[string]string{"fields": `{"e":"��"}`}, warn: []string{"encoding-replaced"}, raw: true},
		"high then pair":   {doc: `{` + base + `,"_e":"\ud83d\ud83d\ude00"}`, want: map[string]string{"fields": `{"e":"�😀"}`}, warn: []string{"encoding-replaced"}, raw: true},
		"lone surrogate":   {doc: `{` + base + `,"_e":"a\ud800b"}`, want: map[string]string{"fields": `{"e":"a�b"}`}, warn: []string{"encoding-replaced"}, raw: true},
		"low surrogate":    {doc: `{` + base + `,"_e":"\udc00"}`, warn: []string{"encoding-replaced"}, raw: true},
		"nul":              {doc: `{` + base + `,"_e":"a\u0000b"}`, want: map[string]string{"fields": `{"e":"a\u0000b"}`}},
		"escapes":          {doc: `{"version":"1.1","host":"h","short_message":"\"\\\/\b\f\n\r\t\u0001\u007f<>& ","level":6}`, want: map[string]string{"short-message": `"\"\\/\b\f\n\r\t\u0001` + "\x7f" + `<>&` + " " + `"`}},
	}
	for name, r := range rows {
		t.Run(name, func(t *testing.T) {
			_, p, res := conv(t, DefaultConfig(), udpHead(), r.doc)
			for k, want := range r.want {
				got := payloadJSON(t, p[k])
				var wantV any
				dec := json.NewDecoder(strings.NewReader(want))
				dec.UseNumber()
				if err := dec.Decode(&wantV); err != nil {
					t.Fatalf("bad want %s: %v", want, err)
				}
				if got != payloadJSON(t, wantV) {
					t.Errorf("%s: got %s want %s", k, got, payloadJSON(t, wantV))
				}
			}
			for _, k := range r.absent {
				if _, ok := p[k]; ok {
					t.Errorf("%s present: %v", k, p[k])
				}
			}
			if got := warnings(p); !reflect.DeepEqual(got, r.warn) {
				t.Errorf("warnings %v want %v", got, r.warn)
			}
			if _, ok := p["raw"]; ok != r.raw {
				t.Errorf("raw present=%v want %v", ok, r.raw)
			}
			if res.Format != FormatGELF {
				t.Errorf("format %v", res.Format)
			}
		})
	}
}

// The numbers of fields reach the envelope as the bytes that arrived, never
// through float64 (SPEC §3.3).
func TestNumbersAreText(t *testing.T) {
	doc := `{"version":"1.1","host":"h","short_message":"m","_a":0.1,"_b":1E+2,"_c":-0.0,"_d":9007199254740993}`
	out, _, _ := conv(t, DefaultConfig(), udpHead(), doc)
	for _, n := range []string{`"a":0.1`, `"b":1E+2`, `"c":-0.0`, `"d":9007199254740993`} {
		if !bytes.Contains(out, []byte(n)) {
			t.Errorf("%s not in %s", n, out)
		}
	}
}

func TestMalformed(t *testing.T) {
	cases := map[string]struct{ doc, err string }{
		"utf8":        {"{\"short_message\":\"\xff\"}", "UTF-8 inválido no documento"},
		"not json":    {`hello`, "documento não é JSON válido"},
		"truncated":   {`{"short_message":"x"`, "documento não é JSON válido"},
		"bom":         {"\xef\xbb\xbf{}", "documento não é JSON válido"},
		"bad escape":  {`{"a":"\x"}`, "documento não é JSON válido"},
		"raw control": {"{\"a\":\"\x01\"}", "documento não é JSON válido"},
		"bad number":  {`{"a":01}`, "documento não é JSON válido"},
		"trailing":    {`{} {}`, "conteúdo depois do fim do documento JSON"},
		"array":       {`[1,2]`, "o documento JSON não é um objeto"},
		"string":      {`"x"`, "o documento JSON não é um objeto"},
		"deep":        {strings.Repeat(`{"a":`, 65) + `1` + strings.Repeat(`}`, 65), "aninhamento JSON acima de 64 níveis"},
		// A top value that is not an object counts its levels as the top
		// object does: 64 arrays are "not an object", 65 too deep.
		"array 64": {strings.Repeat(`[`, 64) + strings.Repeat(`]`, 64), "o documento JSON não é um objeto"},
		"array 65": {strings.Repeat(`[`, 65) + strings.Repeat(`]`, 65), "aninhamento JSON acima de 64 níveis"},
		"gzip":     {string(gz(`{"a":1}`)), "UTF-8 inválido no documento"},
	}
	for name, c := range cases {
		_, p, res := conv(t, DefaultConfig(), udpHead(), c.doc)
		if res.Format != FormatMalformed || p["format"] != "malformed" {
			t.Errorf("%s: format %v", name, p["format"])
			continue
		}
		if got := p["parse"].(map[string]any)["error"]; got != c.err {
			t.Errorf("%s: error %q want %q", name, got, c.err)
		}
		if p["short-message"] != "" || res.Level != 6 {
			t.Errorf("%s: short-message %v level %d", name, p["short-message"], res.Level)
		}
		if _, ok := p["raw"]; !ok {
			t.Errorf("%s: no raw", name)
		}
	}
	// 64 levels, the top object included, is still fine; arrays count too.
	ok := strings.Repeat(`{"a":`, 63) + `[1]` + strings.Repeat(`}`, 63)
	if _, _, res := conv(t, DefaultConfig(), udpHead(), strings.Repeat(`{"a":`, 63)+`[[1]]`+strings.Repeat(`}`, 63)); res.Format != FormatMalformed {
		t.Error("depth 65 through an array accepted")
	}
	if _, _, res := conv(t, DefaultConfig(), udpHead(), ok); res.Format != FormatGELF {
		t.Error("depth 64 refused")
	}
}

func gz(s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(s))
	w.Close()
	return b.Bytes()
}

func TestKeepRawAlways(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepRaw = KeepRawAlways
	doc := `{"version":"1.1","host":"h","short_message":"m","level":1}`
	_, p, _ := conv(t, cfg, udpHead(), doc)
	if p["raw"] != doc {
		t.Fatalf("raw %v", p["raw"])
	}
}

// The level the receiver filters on is the level the envelope carries, for
// every document (SPEC §7). FuzzConvert holds this too.
func TestLevelOfMatchesConvert(t *testing.T) {
	c := New(DefaultConfig())
	for _, doc := range []string{`{"level":3}`, `{"level":"2"}`, `{"level":9}`, `{}`, `x`, `{"level":1,"level":5}`,
		`{"level":0}`, `{"level":3,"x":}`, `{"level":4.0}`} {
		h := udpHead()
		_, res := c.Append(nil, &h, []byte(doc))
		if got := c.LevelOf([]byte(doc)); got != res.Level {
			t.Errorf("%s: LevelOf %d, envelope %d", doc, got, res.Level)
		}
	}
}

// The payload keys come out in the order of SPEC §3.2, all of them at once.
func TestPayloadKeyOrder(t *testing.T) {
	h := udpHead()
	h.Transport, h.Compression, h.Chunks = item.TransportHTTPS, item.CompressionDeflate, 0
	h.TLS = &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256"}
	h.PeerIP, h.PeerPort = netip.MustParseAddr("127.0.0.1"), 9
	doc := `{"_f":"x","x":1,"line":3,"file":"f","full_message":"fm","short_message":"s","timestamp":1,"facility":"fa","level":2,"version":"1.1","host":"h","host":"h2"}`
	out, _, _ := conv(t, DefaultConfig(), h, doc)
	_, keys, err := jlhcheck.Keys(out)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"host", "transport", "compression", "tls", "format", "gelf-version", "level", "facility",
		"timestamp", "short-message", "full-message", "file", "line", "fields", "extra", "size", "raw", "parse", "x-proxy"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys\n got %v\nwant %v", keys, want)
	}
	h = udpHead()
	h.Compression, h.Chunks = item.CompressionGzip, 3
	out, _, _ = conv(t, DefaultConfig(), h, `{"short_message":"x"}`)
	if _, keys, _ = jlhcheck.Keys(out); !reflect.DeepEqual(keys[:6], []string{"host", "transport", "compression", "chunks", "format", "level"}) {
		t.Fatalf("udp keys %v", keys)
	}
}

// Determinism (SPEC §3.5): the same head and document give the same bytes,
// also from a converter that already converted something else.
func TestDeterministic(t *testing.T) {
	h := udpHead()
	doc := []byte(`{"version":"1.1","host":"h","short_message":"m","_a":1,"x":{"b":[1]},"_a":2}`)
	c := New(DefaultConfig())
	a, _ := c.Append(nil, &h, doc)
	c.Append(nil, &h, []byte(`{"_z":"other","_y":{"q":1}}`))
	b, _ := c.Append(nil, &h, doc)
	if !bytes.Equal(a, b) {
		t.Fatalf("\n%s\n%s", a, b)
	}
}

// TestWorstCaseJLHIsBounded proves SPEC §3.9 with the adversarial documents
// it names: the envelope never passes WorstJLH(L).
func TestWorstCaseJLHIsBounded(t *testing.T) {
	const L = 4096
	h := udpHead()
	h.Transport = item.TransportHTTPS
	h.TLS = &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256",
		PeerSubject: strings.Repeat("C", 1024), PeerFingerprint: "sha-256:" + strings.TrimSuffix(strings.Repeat("AB:", 32), ":")}
	h.PeerIP, h.PeerPort = netip.MustParseAddr("2001:db8::1"), 65535
	h.SrcIP = netip.MustParseAddr("2001:db8:1:2:3:4:5:6")
	h.DstIP = netip.MustParseAddr("2001:db8:7:8:9:a:b:c")
	fill := func(prefix, unit, suffix string) []byte {
		n := (L - len(prefix) - len(suffix)) / len(unit)
		return []byte(prefix + strings.Repeat(unit, n) + suffix)
	}
	docs := map[string][]byte{
		"controls":       bytes.Repeat([]byte{1}, L),
		"not utf8":       bytes.Repeat([]byte{0xff}, L),
		"quotes":         bytes.Repeat([]byte{'"'}, L),
		"coerced nested": fill(`{"version":1,"_a":[`, `"\"",`, `""]}`),
		"coerced quotes": fill(`{"version":1,"_a":`, `[`, ``),
		"many arrays":    fill(`{"version":1,"short_message":[`, `[],`, `[]]}`),
		"escaped short":  fill(`{"version":1,"short_message":"`, `\u0001`, `"}`),
		"many fields":    fill(`{"version":1`, `,"_a":[]`, `}`),
		"many nulls":     fill(`{"version":1`, `,"_":null`, `}`),
		"spaces":         fill(`{"version":1,"_a":{`, ` `, `}}`),
	}
	for _, keep := range []KeepRaw{KeepRawMalformed, KeepRawAlways} {
		cfg := DefaultConfig()
		cfg.KeepRaw = keep
		cfg.MaxMessageBytes = L
		for name, d := range docs {
			if len(d) > L {
				t.Fatalf("%s: %d bytes", name, len(d))
			}
			out, _ := New(cfg).Append(nil, &h, d)
			if len(out) > WorstJLH(L) {
				t.Errorf("%s keep=%d: %d > %d", name, keep, len(out), WorstJLH(L))
			}
		}
	}
	if got := WorstJLH(DefaultMaxMessageBytes); got != 776<<10 {
		t.Fatalf("worst JLH of the default is %d, SPEC §3.9 says 776 KiB", got)
	}
}
