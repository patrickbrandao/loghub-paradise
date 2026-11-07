package verify

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/tmpl"
)

func TestParseURL(t *testing.T) {
	for _, c := range []struct {
		in, kind, display, url, stream string
		tls                            bool
	}{
		{"nats://u:pw@n1:4222", KindNATS, "nats://n1:4222", "nats://u:pw@n1:4222", DefaultNATSStream, false},
		{"tls://n1:4222?stream=S&space=prod", KindNATS, "tls://n1:4222", "tls://n1:4222", "S", true},
		{"valkey://:pw@v:6379/2", KindRedis, "valkey://v:6379", "redis://:pw@v:6379/2", "loghub.default.syslog", false},
		{"valkeys://v:6380?space=x", KindRedis, "valkeys://v:6380", "rediss://v:6380", "loghub.x.syslog", true},
		{"redis://r:6379?db=1&stream=my", KindRedis, "redis://r:6379", "redis://r:6379?db=1", "my", false},
		{"postgresql://u:pw@p:5432/logs?sslmode=disable&table=t.q", KindPostgres, "postgresql://p:5432/logs", "postgresql://u:pw@p:5432/logs?sslmode=disable", "", false},
	} {
		tg, err := ParseURL(c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if tg.Kind != c.kind || tg.Display != c.display || tg.url != c.url || (tg.stream != c.stream && tg.redisStream() != c.stream) || tg.tls != c.tls {
			t.Fatalf("%s: %+v", c.in, tg)
		}
	}
	for _, bad := range []string{"amqp://u:s3cr3t@h/", "redis://u:s3cr3t@h?zz=s3cr3t", "nats://h?stream=a&stream=b", "nats://u:s3cr3t@h?ca=/x", "::::s3cr3t", "nats:///nohost"} {
		_, err := ParseURL(bad)
		if err == nil || strings.Contains(err.Error(), "s3cr3t") {
			t.Fatalf("%s: err %v", bad, err)
		}
	}
}

func TestMatcher(t *testing.T) {
	tp, _ := tmpl.Compile("seq={seq} run={run} injector=go-relplog-injector")
	pat, ok := tp.Pattern("ab12", "h")
	if !ok {
		t.Fatal("generator has no marker")
	}
	m, err := NewMatcher(pat)
	if err != nil {
		t.Fatal(err)
	}
	raw := "<13>1 2026-11-07T00:00:00Z h a 1 - - seq=42 run=ab12 injector=go-relplog-injector xxxx"
	for _, c := range []struct {
		e    Entry
		seq  uint64
		want bool
	}{
		{Entry{Message: "seq=42 run=ab12 injector=go-relplog-injector xxxx"}, 42, true},
		{Entry{Message: "seq=42 run=OTHER injector=go-relplog-injector"}, 0, false},
		{Entry{Message: "garbled", Raw: raw}, 42, true},
		{Entry{Raw: base64.StdEncoding.EncodeToString([]byte(raw + "\x00")), RawEncoding: "base64"}, 42, true},
		{Entry{}, 0, false},
	} {
		s, ok := m.Seq(c.e)
		if ok != c.want || s != c.seq {
			t.Fatalf("%+v: %d %v", c.e, s, ok)
		}
	}
	if _, ok := mustTmpl(t, "{seq} only").Pattern("r", "h"); ok {
		t.Fatal("template without {run} accepted")
	}
	p2, _ := mustTmpl(t, "x{{ {uuid} {rand:9999} {ts} {host} r={run} s={seq} s2={seq}").Pattern("r.1", "my.host")
	m2, err := NewMatcher(p2)
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := m2.Seq(Entry{Message: "x{ 0b8a3c1e-3f6e-4b7a-9c1d-2e3f4a5b6c7d abc9 2026-11-07T10:00:00.000001-03:00 my.host r=r.1 s=7 s2=7"}); !ok || s != 7 {
		t.Fatalf("template pattern %q: %d %v", p2, s, ok)
	}
}

func mustTmpl(t *testing.T, s string) *tmpl.Template {
	t.Helper()
	tp, err := tmpl.Compile(s)
	if err != nil {
		t.Fatal(err)
	}
	return tp
}

func TestTally(t *testing.T) {
	tp, _ := tmpl.Compile("seq={seq} run={run} ")
	pat, _ := tp.Pattern("R", "")
	m, _ := NewMatcher(pat)
	tl := newTally(5)
	for _, e := range []Entry{
		{Message: "seq=1 run=R ", UUID: "a"}, {Message: "seq=2 run=R ", UUID: "b"}, {Message: "seq=2 run=R ", UUID: "b"},
		{Message: "seq=3 run=R ", UUID: "c"}, {Message: "seq=3 run=R ", UUID: "d"}, {Message: "seq=9 run=R "}, {Message: "other"},
	} {
		tl.add(m, e)
	}
	acked := []uint64{1<<1 | 1<<2 | 1<<4} // 1, 2, 4 acknowledged
	v := tl.result(acked, false)
	if v.Expected != 5 || v.Acked != 3 || v.Found != 3 || v.Lost != 1 || v.LostSeqs[0] != "4" || v.MissingUnacked != 1 ||
		v.FoundUnacked != 1 || v.Duplicated != 2 || v.ExtraCopies != 2 || v.ExtraSameUUID != 1 || v.Unexpected != 1 || v.EntriesRead != 7 || v.OK {
		t.Fatalf("%+v", v)
	}
}
