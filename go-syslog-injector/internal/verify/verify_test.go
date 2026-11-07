package verify

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

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
		tg, err := Parse(c.url, "", "default", "", "r1", "")
		if err != nil || tg.Kind != c.kind || tg.Destination() != c.out {
			t.Errorf("%s: %+v %v -> %q", c.kind, tg, err, tg.Destination())
			continue
		}
		if strings.Contains(tg.Destination(), "sekret") || strings.Contains(tg.Destination(), "u@") {
			t.Errorf("%s: destination shows credentials", c.kind)
		}
	}
	if tg, _ := Parse("valkeys://v:6380/1", "", "default", "", "r1", ""); tg.URL != "rediss://v:6380/1" {
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
		if _, err := Parse(bad.url, bad.sqlite, bad.space, bad.key, bad.run, ""); err == nil {
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
	c := NewCounter(9)
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
	if c.records != 3 || c.n(1) != 2 || c.n(5) != 1 {
		t.Fatalf("counted %+v", c)
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

// TestTallyUnexpectedRecords (SPEC §8.5, decision 6 of 2026-11-07): unexpected
// counts the records whose seq is not a success -- a failed seq stored twice
// is 2 (and 1 duplicated), a seq past the largest success 1 each.
func TestTallyUnexpectedRecords(t *testing.T) {
	var exp report.SeqSet
	exp.Add(1)
	exp.Add(3)
	c := NewCounter(exp.Max())
	for _, s := range []uint64{1, 2, 2, 3, 3, 3, 1000, 1000} {
		c.Add(s)
	}
	v := &report.Verify{Expected: exp.Len()}
	tally(c, &exp, v)
	if v.Found != 2 || v.Lost != 0 || v.Duplicated != 3 || v.Unexpected != 4 {
		t.Fatalf("tally %+v", v)
	}
}

// TestParseLists (SPEC §8.5, review finding 3): every element of a NATS list
// is parsed before nats.go sees it, and no error quotes one; the destination
// shows every host.
func TestParseLists(t *testing.T) {
	tg, err := Parse("nats://u:sekret@n1:4222, tls://tok@n2:4222", "", "default", "", "r1", "")
	if err != nil || !tg.tls || tg.Destination() != "nats://n1:4222,n2:4222" {
		t.Fatalf("%+v %v %q", tg, err, tg.Destination())
	}
	for _, bad := range []string{
		"nats://u:sekret@n1:4222,nats://u:sekret@n2:4222/%zz",
		"nats://u:sekret@n1:4222,nats://u:se%kret@n2",
		"nats://u:sekret@n1:4222,",
		"nats://u:sekret@n1:4222,redis://u:sekret@n2",
		"nats://u:sek ret@n1:4222",
	} {
		_, err := Parse(bad, "", "default", "", "r1", "")
		if err == nil || strings.Contains(err.Error(), "sek") {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// TestClean: a *url.Error is never forwarded, and the secrets of the URL are
// scrubbed from any other error text.
func TestClean(t *testing.T) {
	tg, err := Parse("nats://alice:sekret@n1:4222,nats://t0ken@n2:4222", "", "default", "", "r1", "")
	if err != nil {
		t.Fatal(err)
	}
	_, perr := url.Parse("nats://alice:sekret@n1:4222/%zz")
	for _, e := range []error{
		perr,
		fmt.Errorf("wrapped: %w", perr),
		errors.New("dial nats://alice:sekret@n1:4222 and t0ken@n2 failed"),
	} {
		got := tg.clean("nats: connect", e).Error()
		if strings.Contains(got, "sekret") || strings.Contains(got, "t0ken") || strings.Contains(got, "alice") || !strings.HasPrefix(got, "nats: connect: ") {
			t.Errorf("%q", got)
		}
	}
	// A *url.Error quotes the URL as written: a password encoded otherwise
	// than url.QueryEscape would is not scrubbed by text, so it is never
	// forwarded at all.
	tg, err = Parse("nats://alice:se%6Bret@n1:4222", "", "default", "", "r1", "")
	if err != nil {
		t.Fatal(err)
	}
	_, perr = url.Parse("nats://alice:se%6Bret@n1:4222/%zz")
	if got := tg.clean("nats: connect", fmt.Errorf("x: %w", perr)).Error(); strings.Contains(got, "6Bret") {
		t.Errorf("%q", got)
	}
}

// fakeReader serves one batch of seqs per poll; errs[i] fails poll i (after
// counting a partial batch).
type fakeReader struct {
	batches [][]uint64
	errs    map[int]bool
	isFull  bool
	polls   int
}

func (f *fakeReader) poll(_ context.Context, c *Counter) error {
	i := f.polls
	f.polls++
	var b []uint64
	if f.isFull {
		for _, x := range f.batches[:min(i+1, len(f.batches))] {
			b = append(b, x...)
		}
	} else if i < len(f.batches) {
		b = f.batches[i]
	}
	if f.errs[i] {
		if len(b) > 0 {
			c.Add(b[0]) // a partial read
		}
		return errors.New("read failed")
	}
	for _, s := range b {
		c.Add(s)
	}
	return nil
}
func (f *fakeReader) full() bool { return f.isFull }
func (f *fakeReader) close()     {}

func seqs(n uint64) *report.SeqSet {
	var s report.SeqSet
	for i := uint64(1); i <= n; i++ {
		s.Add(i)
	}
	return &s
}

// TestWaitExtraRead (SPEC §8.5): once every expected seq is found, one more
// read a moment later sees the copy that came with the burst. Without it the
// duplicate is not counted.
func TestWaitExtraRead(t *testing.T) {
	Settle = 10 * time.Millisecond
	defer func() { Settle = time.Second }()
	f := &fakeReader{batches: [][]uint64{{1, 2, 3}, {2}}}
	v := (&Check{t: &Target{Kind: "nats", URL: "nats://h"}, r: f}).Wait(context.Background(), seqs(3), 5*time.Second)
	if !v.Complete || v.Duplicated != 1 || f.polls != 2 || v.Error != "" {
		t.Fatalf("%+v after %d polls", v, f.polls)
	}
}

// TestWaitRetriesErrors (SPEC §8.5, alignment of 2026-11-07): a read error is
// retried until the deadline; a full read cut short does not replace the
// count; when the last read failed the check says so.
func TestWaitRetriesErrors(t *testing.T) {
	Settle = 10 * time.Millisecond
	defer func() { Settle = time.Second }()
	tg := &Target{Kind: "sqlite", Path: "x"}
	// Errors first, then the whole read: complete, no error.
	f := &fakeReader{batches: [][]uint64{{1, 2}}, errs: map[int]bool{0: true, 1: true}, isFull: true}
	v := (&Check{t: tg, r: f}).Wait(context.Background(), seqs(2), 5*time.Second)
	if !v.Complete || v.Found != 2 || v.Error != "" || f.polls < 3 {
		t.Fatalf("%+v after %d polls", v, f.polls)
	}
	// A whole read of 1 and 2, then failing partial reads until the
	// deadline: the count stays the whole one, and the error is reported.
	f = &fakeReader{batches: [][]uint64{{1, 2}, {}}, errs: map[int]bool{}, isFull: true}
	for i := 1; i < 100; i++ {
		f.errs[i] = true
	}
	v = (&Check{t: tg, r: f}).Wait(context.Background(), seqs(3), 700*time.Millisecond)
	if v.Found != 2 || v.Lost != 1 || v.Error == "" || v.Complete || f.polls < 2 {
		t.Fatalf("%+v after %d polls", v, f.polls)
	}
}
