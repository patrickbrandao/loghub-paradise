package verify

import (
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/report"
)

func TestMarkers(t *testing.T) {
	for _, c := range []struct {
		text string
		seq  uint64
		ok   bool
	}{
		{"seq=12 run=ab injector=go-syslog-injector", 12, true},
		{"x run=ab y seq=3", 3, true},
		{"seq=12 run=abc", 0, false},    // another run
		{"seq=12 xrun=ab", 0, false},    // not a word
		{"seq=1x run=ab", 0, false},     // not a number
		{"seq=0 run=ab", 0, false},      // seqs start at 1
		{"seq=5 run=ab seq=6", 5, true}, // the first one
		{"", 0, false},
	} {
		seq, ok := Markers(c.text, "ab")
		if seq != c.seq || ok != c.ok {
			t.Errorf("%q: %d %v, want %d %v", c.text, seq, ok, c.seq, c.ok)
		}
	}
	if s, ok := fromPayload("no markers", "<13>1 - - - - - - seq=9 run=ab", "ab"); !ok || s != 9 {
		t.Fatalf("raw fallback: %d %v", s, ok)
	}
}

func TestParse(t *testing.T) {
	for _, c := range []struct{ url, kind, out string }{
		{"nats://u:sekret@n1:4222,nats://n2:4222", "nats", "nats://n1:4222,n2:4222"},
		{"tls://n1:4222", "nats", "tls://n1:4222"},
		{"redis://u:sekret@r:6379/2", "redis", "redis://r:6379"},
		{"rediss://r:6380", "redis", "rediss://r:6380"},
		{"valkey://u:sekret@v:6379/0", "redis", "redis://v:6379"},
		{"valkeys://v:6380", "redis", "rediss://v:6380"},
		{"postgres://u:sekret@p:5432/db?sslmode=require", "postgres", "postgres://p:5432"},
		{"postgresql://u@p/db", "postgres", "postgresql://p"},
	} {
		tg, err := Parse(c.url, "", "default", "", "r1")
		if err != nil || tg.Kind != c.kind || tg.Destination() != c.out {
			t.Errorf("%s: %+v %v -> %q", c.kind, tg, err, tg.Destination())
			continue
		}
		if strings.Contains(tg.Destination(), "sekret") || strings.Contains(tg.Destination(), "u@") {
			t.Errorf("%s: destination shows credentials", c.kind)
		}
	}
	if tg, _ := Parse("valkeys://v:6380/1", "", "default", "", "r1"); tg.URL != "rediss://v:6380/1" {
		t.Fatalf("valkeys rewrite %q", tg.URL)
	}
	for _, bad := range []struct{ url, sqlite, space, key, run string }{
		{"amqp://u:sekret@h", "", "default", "", "r"},
		{"sekret", "", "default", "", "r"},
		{"redis://h", "x.db", "default", "", "r"},
		{"redis://h", "", "default", "", "a b"},
		{"redis://h", "", "default", "", `a"b`},
		{"redis://h", "", "a b", "", "r"},
		{"redis://h", "", "default", "a b", "r"},
		{"postgres://h/db", "", "default", "a.b.c", "r"},
		{"postgres://h/db", "", "default", "1t", "r"},
		{"", "x.db", "default", "a-b", "r"},
		{"", "x.db?mode=rw", "default", "", "r"},
	} {
		if _, err := Parse(bad.url, bad.sqlite, bad.space, bad.key, bad.run); err == nil {
			t.Errorf("%+v accepted", bad)
		} else if strings.Contains(err.Error(), "sekret") {
			t.Errorf("error quotes the URL: %v", err)
		}
	}
}

// TestCountJLH: only envelopes of type syslog, of the space and of the run
// count, by the markers of payload.message.
func TestCountJLH(t *testing.T) {
	tg := &Target{Space: "default", Run: "r1"}
	c := NewCounter()
	for _, e := range []string{
		`{"space":"default","type":"syslog","payload":{"message":"seq=1 run=r1"}}`,
		`{"space":"default","type":"syslog","payload":{"message":"seq=1 run=r1"}}`, // duplicate
		`{"space":"other","type":"syslog","payload":{"message":"seq=2 run=r1"}}`,
		`{"space":"default","type":"beacon","payload":{"message":"seq=3 run=r1"}}`,
		`{"space":"default","type":"syslog","payload":{"message":"seq=4 run=r2"}}`,
		`{"space":"default","type":"syslog","payload":{"message":"x","raw":"<13>1 - - - - - - seq=5 run=r1"}}`,
		`not json run=r1`,
	} {
		countJLH([]byte(e), tg, c)
	}
	if len(c.seen) != 2 || c.seen[1] != 2 || c.seen[5] != 1 {
		t.Fatalf("counted %v", c.seen)
	}
	var exp report.SeqSet
	for _, s := range []uint64{1, 2, 3, 5, 6, 7, 9} {
		exp.Add(s)
	}
	v := &report.Verify{Expected: exp.Len()}
	tally(c, &exp, v)
	if v.Found != 2 || v.Lost != 5 || v.Duplicated != 1 || v.Unexpected != 0 || v.LostSeqs != "2-3,6-7,9" {
		t.Fatalf("tally %+v", v)
	}
}
