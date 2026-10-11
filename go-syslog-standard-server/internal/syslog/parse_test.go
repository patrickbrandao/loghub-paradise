package syslog

import (
	"testing"
	"time"
)

// now is a fixed reception instant so the RFC 3164 year rule is deterministic.
var now = time.Date(2026, time.September, 12, 14, 33, 1, 0, time.UTC)

func run(t *testing.T, line string) (*Parsed, int, bool, string) {
	t.Helper()
	raw := []byte(line)
	pri, n, ok := PRI(raw)
	if !ok {
		pri, n = 1*8+5, 0 // parse.default_facility / default_severity
	}
	var p Parsed
	Parse(&p, raw, n, now, Options{})

	// The property the fuzz target asserts, checked on every table case too.
	if p.MsgOffset < 0 || p.MsgOffset+p.MsgSize != len(raw) {
		t.Fatalf("msg bounds broken for %q: off=%d size=%d len=%d",
			line, p.MsgOffset, p.MsgSize, len(raw))
	}
	return &p, pri, ok, string(raw[p.MsgOffset:])
}

func TestParse5424Full(t *testing.T) {
	line := `<165>1 2026-09-12T14:33:01.123456-03:00 web-01 nginx 1234 ID47 ` +
		`[exampleSDID@32473 iut="3" eventSource="App"][origin ip="10.0.0.1"] an event`
	p, pri, ok, msg := run(t, line)

	if !ok || pri != 165 {
		t.Fatalf("pri = %d ok=%v, want 165 true", pri, ok)
	}
	if p.RFC != RFC5424 || p.Version != 1 {
		t.Fatalf("rfc=%v version=%d, want 5424 1", p.RFC, p.Version)
	}
	if string(p.Hostname) != "web-01" || string(p.AppName) != "nginx" ||
		string(p.ProcID) != "1234" || string(p.MsgID) != "ID47" {
		t.Fatalf("header = %q %q %q %q", p.Hostname, p.AppName, p.ProcID, p.MsgID)
	}
	if msg != "an event" {
		t.Fatalf("msg = %q", msg)
	}
	if p.MsgTSSrc != TS5424 {
		t.Fatalf("ts source = %v", p.MsgTSSrc)
	}
	want := time.Date(2026, 9, 12, 14, 33, 1, 123456000, time.FixedZone("", -3*3600))
	if !p.MsgTS.Equal(want) {
		t.Fatalf("msg-ts = %v, want %v", p.MsgTS, want)
	}
	if len(p.SD) != 2 {
		t.Fatalf("sd elements = %d, want 2", len(p.SD))
	}
	if string(p.SD[0].ID) != "exampleSDID@32473" || len(p.SD[0].Params) != 2 {
		t.Fatalf("sd[0] = %q %v", p.SD[0].ID, p.SD[0].Params)
	}
	if string(p.SD[0].Params[1].Name) != "eventSource" ||
		string(p.SD[0].Params[1].Value) != "App" {
		t.Fatalf("sd param = %q=%q", p.SD[0].Params[1].Name, p.SD[0].Params[1].Value)
	}
	if string(p.SD[1].ID) != "origin" || string(p.SD[1].Params[0].Value) != "10.0.0.1" {
		t.Fatalf("sd[1] = %q %v", p.SD[1].ID, p.SD[1].Params)
	}
}

func TestParse5424AllNil(t *testing.T) {
	p, _, _, msg := run(t, `<34>1 - - - - - - `)
	if p.RFC != RFC5424 {
		t.Fatalf("rfc = %v", p.RFC)
	}
	if p.Hostname != nil || p.AppName != nil || p.ProcID != nil || p.MsgID != nil {
		t.Fatalf("nil fields not empty: %q %q %q %q", p.Hostname, p.AppName, p.ProcID, p.MsgID)
	}
	if p.MsgTSSrc != TSNone || len(p.SD) != 0 || msg != "" {
		t.Fatalf("ts=%v sd=%d msg=%q", p.MsgTSSrc, len(p.SD), msg)
	}
}

func TestParse5424NoMsg(t *testing.T) {
	p, _, _, msg := run(t, `<34>1 2026-09-12T14:33:01Z host app - - -`)
	if p.RFC != RFC5424 || msg != "" {
		t.Fatalf("rfc=%v msg=%q", p.RFC, msg)
	}
}

func TestParse3164Classic(t *testing.T) {
	p, pri, ok, msg := run(t, `<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`)
	if !ok || pri != 34 {
		t.Fatalf("pri = %d %v", pri, ok)
	}
	if p.RFC != RFC3164 {
		t.Fatalf("rfc = %v", p.RFC)
	}
	if string(p.Hostname) != "mymachine" || string(p.AppName) != "su" || string(p.ProcID) != "230" {
		t.Fatalf("header = %q %q %q", p.Hostname, p.AppName, p.ProcID)
	}
	if msg != "'su root' failed" {
		t.Fatalf("msg = %q", msg)
	}
	// October 11 is in the past relative to the fixed now, so the year is
	// reception's, not the one before.
	want := time.Date(2025, 10, 11, 22, 14, 15, 0, time.UTC)
	if !p.MsgTS.Equal(want) {
		t.Fatalf("msg-ts = %v, want %v", p.MsgTS, want)
	}
}

func TestParse3164SpacePaddedDay(t *testing.T) {
	p, _, _, msg := run(t, `<13>Sep  1 08:00:00 host app: hi`)
	if p.RFC != RFC3164 || string(p.Hostname) != "host" || msg != "hi" {
		t.Fatalf("host=%q msg=%q rfc=%v", p.Hostname, msg, p.RFC)
	}
	if p.MsgTS.Day() != 1 || p.MsgTS.Month() != time.September {
		t.Fatalf("msg-ts = %v", p.MsgTS)
	}
}

// The shape a container emits: no timestamp, no hostname, tag and pid present.
// The rule that reads the tag without a positional anchor exists for this.
func TestParse3164Container(t *testing.T) {
	p, _, _, msg := run(t, `<14>myapp[123]: algo aconteceu`)
	if p.RFC != RFC3164 {
		t.Fatalf("rfc = %v", p.RFC)
	}
	if p.Hostname != nil {
		t.Fatalf("hostname guessed: %q", p.Hostname)
	}
	if string(p.AppName) != "myapp" || string(p.ProcID) != "123" || msg != "algo aconteceu" {
		t.Fatalf("app=%q pid=%q msg=%q", p.AppName, p.ProcID, msg)
	}
}

// Without a timestamp no hostname may be guessed: the first word of free text
// would become the name of a machine that does not exist.
func TestParse3164NoHostnameGuess(t *testing.T) {
	p, _, _, msg := run(t, `<14>Connection closed by 10.0.0.1`)
	if p.Hostname != nil || p.AppName != nil {
		t.Fatalf("guessed host=%q app=%q", p.Hostname, p.AppName)
	}
	if msg != "Connection closed by 10.0.0.1" {
		t.Fatalf("msg = %q", msg)
	}
	// A valid PRI alone still makes it syslog.
	if p.RFC != RFC3164 {
		t.Fatalf("rfc = %v, want 3164", p.RFC)
	}
}

// A timestamp with no tag after it still yields a hostname: after the
// timestamp, position is certain.
func TestParse3164HostnameWithoutTag(t *testing.T) {
	p, _, _, msg := run(t, `<14>Sep 12 10:00:00 srv-a texto livre aqui`)
	if string(p.Hostname) != "srv-a" || p.AppName != nil {
		t.Fatalf("host=%q app=%q", p.Hostname, p.AppName)
	}
	if msg != "texto livre aqui" {
		t.Fatalf("msg = %q", msg)
	}
}

func TestParseNoPRI(t *testing.T) {
	p, pri, ok, msg := run(t, `linha solta sem pri nenhum`)
	if ok {
		t.Fatal("PRI reported present")
	}
	if pri != 13 { // facility 1 (user) * 8 + severity 5 (notice)
		t.Fatalf("default pri = %d, want 13", pri)
	}
	if p.RFC != RFCUnknown {
		t.Fatalf("rfc = %v, want unknown", p.RFC)
	}
	if msg != "linha solta sem pri nenhum" || p.MsgOffset != 0 {
		t.Fatalf("msg = %q at %d", msg, p.MsgOffset)
	}
}

func TestPRIRejects(t *testing.T) {
	for _, s := range []string{"", "<", "<>", "<abc>", "<1234>", "<192>", "<013>", "no pri", "<13", "13>x"} {
		if _, _, ok := PRI([]byte(s)); ok {
			t.Errorf("PRI(%q) accepted", s)
		}
	}
	for _, tc := range []struct {
		in  string
		pri int
		n   int
	}{{"<0>x", 0, 3}, {"<13>x", 13, 4}, {"<191>x", 191, 5}} {
		pri, n, ok := PRI([]byte(tc.in))
		if !ok || pri != tc.pri || n != tc.n {
			t.Errorf("PRI(%q) = %d,%d,%v", tc.in, pri, n, ok)
		}
	}
}

func TestSDEscapes(t *testing.T) {
	p, _, _, _ := run(t, `<34>1 - - - - - [x a="say \"hi\"" b="c:\\dir" c="end\]" d="\n literal"] m`)
	if len(p.SD) != 1 || len(p.SD[0].Params) != 4 {
		t.Fatalf("sd = %v", p.SD)
	}
	want := []string{`say "hi"`, `c:\dir`, `end]`, `\n literal`}
	for i, w := range want {
		if got := string(p.SD[0].Params[i].Value); got != w {
			t.Errorf("param %d = %q, want %q", i, got, w)
		}
	}
}

func TestSDDuplicateIDLastWins(t *testing.T) {
	p, _, _, _ := run(t, `<34>1 - - - - - [a k="1"][a k="2"] m`)
	if len(p.SD) != 1 || string(p.SD[0].Params[0].Value) != "2" {
		t.Fatalf("sd = %v", p.SD)
	}
	if p.Anomalies[AnomDupSDID] != 1 {
		t.Fatalf("dup anomaly = %d", p.Anomalies[AnomDupSDID])
	}
}

func TestSDUnterminated(t *testing.T) {
	p, _, _, msg := run(t, `<34>1 - - - - - [a k="1" resto da linha`)
	if p.Anomalies[AnomSDUnterminated] != 1 {
		t.Fatalf("anomaly = %v", p.Anomalies)
	}
	if msg != `[a k="1" resto da linha` {
		t.Fatalf("msg = %q", msg)
	}
}

// Reuse across messages is the point of Parsed: a worker keeps one for its
// whole drain loop. A stale field leaking from the previous message would put
// one emitter's hostname on another's event.
func TestParsedReuse(t *testing.T) {
	var p Parsed
	raw1 := []byte(`<165>1 2026-09-12T14:33:01Z web-01 nginx 9 ID [a k="v"] first`)
	_, n1, _ := PRI(raw1)
	Parse(&p, raw1, n1, now, Options{})

	raw2 := []byte(`<14>segunda linha sem estrutura`)
	_, n2, _ := PRI(raw2)
	Parse(&p, raw2, n2, now, Options{})

	if p.Hostname != nil || p.AppName != nil || p.MsgID != nil || len(p.SD) != 0 {
		t.Fatalf("stale after reuse: host=%q app=%q msgid=%q sd=%d",
			p.Hostname, p.AppName, p.MsgID, len(p.SD))
	}
	if p.Version != 0 || p.MsgTSSrc != TSNone || !p.MsgTS.IsZero() {
		t.Fatalf("stale ts: v=%d src=%v ts=%v", p.Version, p.MsgTSSrc, p.MsgTS)
	}
	if string(raw2[p.MsgOffset:]) != "segunda linha sem estrutura" {
		t.Fatalf("msg = %q", raw2[p.MsgOffset:])
	}
}

func TestPreferOverridesAuto(t *testing.T) {
	raw := []byte(`<34>1 2026-09-12T14:33:01Z h a - - - m`)
	var p Parsed
	Parse(&p, raw, 4, now, Options{Prefer: RFC3164})
	if p.RFC == RFC5424 {
		t.Fatal("prefer=3164 still parsed as 5424")
	}
}

func TestNamesRoundTrip(t *testing.T) {
	for i := 0; i < 24; i++ {
		if n := FacilityName(i); n == "" {
			t.Fatalf("facility %d has no name", i)
		} else if v, ok := ParseFacility(n); !ok || v != i {
			t.Fatalf("facility %q round-trip = %d %v", n, v, ok)
		}
	}
	for i := 0; i < 8; i++ {
		n := SeverityName(i)
		if v, ok := ParseSeverity(n); !ok || v != i {
			t.Fatalf("severity %q round-trip = %d %v", n, v, ok)
		}
	}
	if FacilityName(24) != "" || SeverityName(-1) != "" {
		t.Fatal("out-of-range name is not empty")
	}
}
