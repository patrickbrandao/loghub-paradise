package convert

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var recv = time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)

func warningsOf(p map[string]any) []string {
	pw, ok := p["parse"].(map[string]any)
	if !ok {
		return nil
	}
	ws, _ := pw["warnings"].([]any)
	var out []string
	for _, w := range ws {
		out = append(out, w.(string))
	}
	return out
}

func TestScanPRI(t *testing.T) {
	cases := []struct {
		in    string
		pri   int
		state PRIState
		n     int
	}{
		{"<0>x", 0, PRIOk, 3},
		{"<13>x", 13, PRIOk, 4},
		{"<191>x", 191, PRIOk, 5},
		{"<192>x", 0, PRIInvalid, 0},
		{"<013>x", 0, PRIInvalid, 0},
		{"<00>x", 0, PRIInvalid, 0},
		{"<1234>x", 0, PRIInvalid, 0},
		{"<abc>x", 0, PRIInvalid, 0},
		{"<>x", 0, PRIInvalid, 0},
		{"<13", 0, PRIInvalid, 0},
		{"<", 0, PRIInvalid, 0},
		{"13>x", 0, PRIMissing, 0},
		{"", 0, PRIMissing, 0},
	}
	for _, c := range cases {
		pri, st, n := ScanPRI([]byte(c.in))
		if pri != c.pri || st != c.state || n != c.n {
			t.Errorf("%q: got (%d,%d,%d) want (%d,%d,%d)", c.in, pri, st, n, c.pri, c.state, c.n)
		}
	}
}

// Deviation D2 (SPEC §6.5.5): without a timestamp a STRICT tag is still read,
// and the first-character rule keeps sequence numbers and vendor tokens out.
func TestStrictTagWithoutTimestamp(t *testing.T) {
	cases := []struct {
		in, app, pid, msg string
	}{
		{"<14>myapp[123]: algo aconteceu", "myapp", "123", "algo aconteceu"},
		{"<14>_svc: x", "_svc", "", "x"},
		{"<14>/usr/sbin/cron[9]: job", "/usr/sbin/cron", "9", "job"},
		{"<14>error: algo falhou", "error", "", "algo falhou"},
		{"<14>app:", "app", "", ""},
		{"<189>123: *Mar  1 00:00:01.123: %SYS-5-CONFIG_I: x", "", "", "123: *Mar  1 00:00:01.123: %SYS-5-CONFIG_I: x"},
		{"<30>firewall,info input: in:ether1", "", "", "firewall,info input: in:ether1"},
		{"<14>Connection closed by 10.0.0.1", "", "", "Connection closed by 10.0.0.1"},
		{"<14>http://example.com down", "", "", "http://example.com down"},
		{"<14>%ASA-6-302013: Built", "", "", "%ASA-6-302013: Built"},
	}
	for _, c := range cases {
		_, p, _ := convertUDP(t, DefaultConfig(), []byte(c.in), recv)
		app, _ := p["appname"].(string)
		pid, _ := p["procid"].(string)
		if app != c.app || pid != c.pid || p["message"] != c.msg {
			t.Errorf("%q: app=%q pid=%q msg=%q", c.in, app, pid, p["message"])
		}
		if !reflect.DeepEqual(warningsOf(p), []string{"timestamp-missing"}) {
			t.Errorf("%q: warnings %v", c.in, warningsOf(p))
		}
	}
}

// The payload schema declares hostname, appname, procid and msgid ^\S+$. A value
// with a whitespace character the schema would reject is kept out of the field
// and, for RFC 5424, raw is forced so it is still in the envelope.
func TestWhitespaceNeverReachesAnSField(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IncludeRaw = false

	// RFC 5424 header token with a tab and one with U+00A0.
	_, p, _ := convertUDP(t, cfg, []byte("<14>1 2026-10-03T12:00:00Z ho\tst a b p m - text"), recv)
	if _, ok := lookup(p, "host.hostname"); ok {
		t.Error("hostname with a tab was emitted")
	}
	if _, ok := p["appname"]; ok {
		t.Error("appname with U+00A0 was emitted")
	}
	if p["procid"] != "p" || p["msgid"] != "m" {
		t.Errorf("neighbours lost: %v %v", p["procid"], p["msgid"])
	}
	if _, ok := p["raw"]; !ok {
		t.Error("raw was not forced")
	}
	if !strings.Contains(strings.Join(warningsOf(p), ","), "header-invalid") {
		t.Error("no header-invalid")
	}

	// RFC 3164: a hostname-looking token with a tab is not a hostname.
	_, p, _ = convertUDP(t, cfg, []byte("<14>Oct  3 08:00:00 ho\tst app: x"), recv)
	if _, ok := lookup(p, "host.hostname"); ok {
		t.Error("3164 hostname with a tab was emitted")
	}
}

func TestStructuredDataRules(t *testing.T) {
	cfg := DefaultConfig()
	// Repeated PARAM-NAME is an array; a duplicate SD-ID merges and warns.
	_, p, _ := convertUDP(t, cfg,
		[]byte(`<14>1 2026-10-03T12:00:00Z h a - - [x@1 k="1" k="2" j="a\b"][y v="z"][x@1 k="3" n="new"] m`), recv)
	sd := p["structured-data"].(map[string]any)
	x := sd["x@1"].(map[string]any)
	if !reflect.DeepEqual(x["k"], []any{"1", "2", "3"}) || x["j"] != `a\b` || x["n"] != "new" {
		t.Errorf("merge: %#v", x)
	}
	if !reflect.DeepEqual(warningsOf(p), []string{"sd-duplicate-id"}) {
		t.Errorf("warnings %v", warningsOf(p))
	}

	// Over the caps: sd-invalid, SD omitted, the text kept in message.
	cfg.SDMaxParams = 2
	_, p, _ = convertUDP(t, cfg, []byte(`<14>1 2026-10-03T12:00:00Z h a - - [x a="1" b="2" c="3"] tail`), recv)
	if _, ok := p["structured-data"]; ok {
		t.Error("SD over the cap was emitted")
	}
	if p["message"] != `[x a="1" b="2" c="3"] tail` {
		t.Errorf("message %q", p["message"])
	}
	cfg.SDMaxElements = 1
	_, p, _ = convertUDP(t, cfg, []byte(`<14>1 2026-10-03T12:00:00Z h a - - [x][y] tail`), recv)
	if _, ok := p["structured-data"]; ok || p["message"] != "[x][y] tail" {
		t.Errorf("element cap: %v %q", p["structured-data"], p["message"])
	}
}

func TestRawAndEncoding(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IncludeRaw = false
	// Valid UTF-8, no warning: raw omitted when switched off.
	_, p, _ := convertUDP(t, cfg, []byte("<14>1 2026-10-03T12:00:00Z h a - - - ok"), recv)
	if _, ok := p["raw"]; ok {
		t.Error("raw present with payload_raw=false")
	}
	// Invalid UTF-8: raw is mandatory, in base64.
	_, p, _ = convertUDP(t, cfg, []byte("<14>1 2026-10-03T12:00:00Z h a - - - caf\xe9"), recv)
	if p["raw-encoding"] != "base64" || p["message"] != "caf�" {
		t.Errorf("encoding: %v %q", p["raw-encoding"], p["message"])
	}
	// latin1: decoded, encoding-legacy.
	cfg.Charset = CharsetLatin1
	_, p, _ = convertUDP(t, cfg, []byte("<14>1 2026-10-03T12:00:00Z h a - - - caf\xe9"), recv)
	if p["message"] != "café" || !reflect.DeepEqual(warningsOf(p), []string{"encoding-legacy"}) {
		t.Errorf("latin1: %q %v", p["message"], warningsOf(p))
	}
	// cp1252: the euro sign and an undefined octet.
	cfg.Charset = CharsetCP1252
	_, p, _ = convertUDP(t, cfg, []byte("<14>1 2026-10-03T12:00:00Z h a - - - \x80\x81"), recv)
	if p["message"] != "€�" {
		t.Errorf("cp1252: %q", p["message"])
	}
}

func TestTrailingControlsAndNUL(t *testing.T) {
	_, p, _ := convertUDP(t, DefaultConfig(), []byte("<14>1 2026-10-03T12:00:00Z h a - - - a\x00b\r\n\x00"), recv)
	if p["message"] != "a\x00b" {
		t.Errorf("message %q", p["message"])
	}
	if p["raw"] != "<14>1 2026-10-03T12:00:00Z h a - - - a\x00b\r\n\x00" {
		t.Errorf("raw %q", p["raw"])
	}
}

// localToUnix reproduces fold=0: the first occurrence of an ambiguous time and
// the pre-transition offset for a non-existent one (SPEC §6.5.2).
func TestLocalToUnixFoldZero(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip(err)
	}
	// 2026-11-01 01:30 happens twice in New York; the first is EDT (-4).
	if got := time.Unix(localToUnix(2026, 11, 1, 1, 30, 0, ny), 0).UTC(); got.Hour() != 5 || got.Minute() != 30 {
		t.Errorf("ambiguous: %v", got)
	}
	// 2026-03-08 02:30 does not exist; fold=0 uses EST (-5) -> 07:30Z.
	if got := time.Unix(localToUnix(2026, 3, 8, 2, 30, 0, ny), 0).UTC(); got.Hour() != 7 || got.Minute() != 30 {
		t.Errorf("gap: %v", got)
	}
}

// The year inference reads the reference from the head, never the clock: a
// message received in December and converted in January keeps December's year
// (SPEC §3.5).
func TestYearInferenceUsesTheHead(t *testing.T) {
	dec := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
	_, p, _ := convertUDP(t, DefaultConfig(), []byte("<14>Dec 31 23:59:58 h a: x"), dec)
	if p["timestamp"] != "2025-12-31T23:59:58.000Z" {
		t.Errorf("timestamp %v", p["timestamp"])
	}
	jan := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)
	_, p, _ = convertUDP(t, DefaultConfig(), []byte("<14>Dec 31 23:59:58 h a: x"), jan)
	if p["timestamp"] != "2025-12-31T23:59:58.000Z" {
		t.Errorf("year change: %v", p["timestamp"])
	}
	_, p, _ = convertUDP(t, DefaultConfig(), []byte("<14>Jan  1 00:00:05 h a: x"),
		time.Date(2025, 12, 31, 23, 59, 50, 0, time.UTC))
	if p["timestamp"] != "2026-01-01T00:00:05.000Z" {
		t.Errorf("forward: %v", p["timestamp"])
	}
}

func TestResult(t *testing.T) {
	_, _, res := convertUDP(t, DefaultConfig(), []byte("<14>1 2026-10-03T12:00:00Z h\x00x app 1 - - m"), recv)
	if res.Format != FormatRFC5424 || res.Priority != 14 || res.Severity() != 6 || res.SID == 0 {
		t.Errorf("result %+v", res)
	}
}

// Decided on 2026-10-14 as in go-syslog-server (SPEC §6.5.1, DECISOES §9): an
// ISO offset out of range in RFC 3164 is applied as it came, without a
// warning, as the skill's reference converter does; the same offset in RFC 5424 is
// timestamp-invalid (§6.4.2). This test pins both halves, so a change to either
// is a deliberate one, made together with the skill and the RELP sibling.
func TestRFC3164ISOOffsetIsNotRangeChecked(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"<14>2026-10-03T08:00:00+25:00 host01 a: x", "2026-10-02T07:00:00.000Z"},
		{"<14>2026-10-03T08:00:00+05:75 host01 a: x", "2026-10-03T01:45:00.000Z"},
		{"<14>2026-10-03T08:00:00-25:00 host01 a: x", "2026-10-04T09:00:00.000Z"},
	} {
		_, p, _ := convertUDP(t, DefaultConfig(), []byte(c.in), recv)
		if p["timestamp"] != c.want {
			t.Errorf("%q: timestamp %v, want %s (the offset applied as it came)", c.in, p["timestamp"], c.want)
		}
		for _, w := range warningsOf(p) {
			if strings.HasPrefix(w, "timestamp-") {
				t.Errorf("%q: warning %s; RFC 3164 does not range-check the offset", c.in, w)
			}
		}
		if hn, _ := lookup(p, "host.hostname"); hn != "host01" || p["appname"] != "a" {
			t.Errorf("%q: the rest of the header was not read: %v", c.in, p)
		}
	}
	// The same offset in RFC 5424 is refused.
	_, p, _ := convertUDP(t, DefaultConfig(), []byte("<14>1 2026-10-03T08:00:00+25:00 h a - - - x"), recv)
	found := false
	for _, w := range warningsOf(p) {
		found = found || w == "timestamp-invalid"
	}
	if !found {
		t.Errorf("RFC 5424 with +25:00: warnings %v, want timestamp-invalid", warningsOf(p))
	}
}
