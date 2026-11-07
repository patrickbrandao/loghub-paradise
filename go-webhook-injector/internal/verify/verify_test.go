package verify

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeReader serves fixed rounds of records: round i is what read i sees.
type fakeReader struct {
	rounds [][]record
	reads  int
}

func (f *fakeReader) read(_ context.Context, fn func(record)) error {
	i := min(f.reads, len(f.rounds)-1)
	f.reads++
	for _, r := range f.rounds[i] {
		fn(r)
	}
	return nil
}

func (f *fakeReader) close() {}

func newV(r reader) *Verifier {
	return &Verifier{r: r, run: "run1", dest: "fake", accepted: map[string]uint64{}, noUUID: map[uint64]bool{}, okSeq: map[uint64]bool{},
		seen: map[string]bool{}, uuidFound: map[string]bool{}, groups: map[string]int{}, seqFound: map[uint64]bool{}}
}

func body(seq int, run string) string {
	return fmt.Sprintf(`{"seq":%d,"run":%q,"injector":"x"}`, seq, run)
}

func rec(key, uuid string, seq int) record {
	return record{key: key, uuid: uuid, encoding: "utf-8", body: body(seq, "run1")}
}

func jlh(key, uuid, b string) record {
	return record{key: key, jlh: fmt.Appendf(nil, `{"uuid":%q,"from":"f","payload":{"mode":"fast","content":{"length":1,"type":"","encoding":"utf-8","body":%q}}}`, uuid, b)}
}

// The counts of SPEC §8.6.4, on one destination that has everything wrong
// at once.
func TestAccounting(t *testing.T) {
	r := &fakeReader{rounds: [][]record{{
		rec("k1", "u1", 1),
		rec("k1", "u1", 1), // the same row read again (PostgreSQL re-reads): ignored
		rec("k2", "u2", 2),
		rec("k2b", "u2b", 2), // seq 2 processed twice (two uuids): one duplicate
		rec("k4", "u4", 4),   // seq 4 never had a 2xx: stored unacknowledged
		{key: "k5", uuid: "u5", encoding: "base64", body: base64.StdEncoding.EncodeToString([]byte(body(5, "run1")))},
		{key: "k6", uuid: "zz", encoding: "utf-8", body: body(6, "other")}, // another run
		jlh("n7", "u7", body(7, "run1")),
		jlh("n7b", "u7", body(7, "run1")), // the same envelope added twice (XADD): one duplicate
		jlh("n8", "u8x", body(8, "other")),
	}}}
	v := newV(r)
	for i, u := range map[int]string{1: "u1", 2: "u2", 3: "u3", 5: "u5", 7: "u7"} {
		v.Accept(uint64(i), 200, u)
	}
	v.Accept(9, 429, "u9") // not accepted
	res := v.Check(context.Background(), 300*time.Millisecond)
	if !reflect.DeepEqual(res.LostSeqs, []string{"3"}) {
		t.Fatalf("%q", res.LostSeqs)
	}
	res.LostSeqs = []string{"3"}
	want := Result{Destination: "fake", Expected: 5, Found: 4, Lost: 1, Duplicated: 2, Unexpected: 1, Records: 7, LostSeqs: []string{"3"}}
	res.Waited = 0
	if !reflect.DeepEqual(res, want) {
		t.Fatalf("got  %+v\nwant %+v", res, want)
	}
	if !res.Failed() || res.Impossible() || res.Complete {
		t.Fatal("verdict")
	}
	if r.reads < 2 {
		t.Fatalf("%d reads: a missing request must be waited for", r.reads)
	}
}

// Found on the second read: the wait ends there, and the check passes.
func TestWaitsForTheSink(t *testing.T) {
	r := &fakeReader{rounds: [][]record{{rec("a", "u1", 1)}, {rec("a", "u1", 1), rec("b", "u2", 2)}}}
	v := newV(r)
	v.Accept(1, 200, "u1")
	v.Accept(2, 202, "u2")
	res := v.Check(context.Background(), 5*time.Second)
	// Complete at the second read, then the one more read a moment later.
	if res.Failed() || !res.Complete || res.Found != 2 || r.reads != 3 || res.Waited > 3 {
		t.Fatalf("%+v after %d reads", res, r.reads)
	}
}

// A 2xx without X-LogHub-UUID is found by its seq marker; without any
// marker in the destination it cannot be verified, and the check fails.
func TestNoUUID(t *testing.T) {
	v := newV(&fakeReader{rounds: [][]record{{rec("a", "x1", 1)}}})
	v.Accept(1, 200, "")
	v.Accept(2, 200, "")
	if res := v.Check(context.Background(), 100*time.Millisecond); res.Found != 1 || res.Lost != 1 || res.Unverifiable != 0 {
		t.Fatalf("%+v", res)
	}
	v = newV(&fakeReader{rounds: [][]record{{{key: "a", uuid: "x", body: "plain"}}}})
	v.Accept(1, 200, "")
	if res := v.Check(context.Background(), 100*time.Millisecond); res.Unverifiable != 1 || res.Lost != 0 || !res.Impossible() {
		t.Fatalf("%+v", res)
	}
}

func TestRanges(t *testing.T) {
	if got := ranges([]uint64{9, 1, 2, 3, 7, 5, 6}, 20); !reflect.DeepEqual(got, []string{"1-3", "5-7", "9"}) {
		t.Fatalf("%q", got)
	}
	if got := ranges([]uint64{1, 3, 5, 7}, 2); !reflect.DeepEqual(got, []string{"1", "3", "..."}) {
		t.Fatalf("%q", got)
	}
}

// A copy of the run written after the last accepted one is caught by the
// read after the complete one (SPEC §8.6.4).
func TestFinalReadCatchesLateDuplicate(t *testing.T) {
	r := &fakeReader{rounds: [][]record{{rec("a", "u1", 1)}, {rec("a", "u1", 1), rec("b", "u9", 1)}}}
	v := newV(r)
	v.Accept(1, 200, "u1")
	if res := v.Check(context.Background(), 5*time.Second); res.Duplicated != 1 || !res.Failed() {
		t.Fatalf("%+v", res)
	}
}

func TestMarker(t *testing.T) {
	for _, c := range []struct {
		enc, body string
		seq       uint64
		ok        bool
	}{
		{"utf-8", `{"seq":12,"run":"run1","ts":"x"}`, 12, true},
		{"utf-8", `{"run":"run1","seq":3}`, 3, true},
		{"utf-8", `{"seq":12,"run":"run2"}`, 0, false},
		{"utf-8", `{"seq":"12","run":"run1"}`, 0, false},
		{"utf-8", `{"seq":1.5,"run":"run1"}`, 0, false},
		{"utf-8", `seq=1 run=run1`, 0, false},
		{"utf-8", `{"x":{"seq":1},"seq":2,"run":"run1"}`, 2, true},
		{"base64", base64.StdEncoding.EncodeToString([]byte(`{"seq":7,"run":"run1"}`)), 7, true},
		{"base64", "!!", 0, false},
	} {
		seq, ok := marker(c.enc, c.body, "run1")
		if seq != c.seq || ok != c.ok {
			t.Errorf("%s %q: %d %v", c.enc, c.body, seq, ok)
		}
	}
}

func TestUUIDFloor(t *testing.T) {
	at := time.UnixMilli(0x0193_1234_5678)
	if got := uuidFloor(at); got != "01931234-5678-0000-0000-000000000000" {
		t.Fatal(got)
	}
	// A UUIDv7 of that millisecond sorts at or after the floor; one of the
	// millisecond before, before it.
	if !("01931234-5678-7abc-8def-0123456789ab" >= uuidFloor(at)) || !("01931234-5677-7fff-bfff-ffffffffffff" < uuidFloor(at)) {
		t.Fatal("order")
	}
}

// The report and every error name the scheme and host, never the
// credentials (SPEC §3.2).
func TestNoSecretsEchoed(t *testing.T) {
	for _, u := range []string{"postgres://u:s3cret@db:5432/x?sslmode=disable", "nats://u:s3cret@n1:4222,nats://n2:4222", "rediss://:s3cret@r:6380/0"} {
		if d := describe(u); strings.Contains(d, "s3cret") || !strings.Contains(d, "://") {
			t.Errorf("%s -> %s", u, d)
		}
	}
	if _, err := Kind("mysql://u:s3cret@h/db"); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("%v", err)
	}
	if _, err := openRedis(context.Background(), "redis://u:s3cret@[bad", ""); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("%v", err)
	}
	for _, k := range []string{"nats://h", "tls://h", "postgres://h", "postgresql://h", "redis://h", "rediss://h", "valkey://h", "valkeys://h"} {
		if _, err := Kind(k); err != nil {
			t.Errorf("%s: %v", k, err)
		}
	}
	for _, tb := range []string{"inbox", "loghub.inbox", "a_1.b_2"} {
		if !ValidTable(tb) {
			t.Errorf("%s refused", tb)
		}
	}
	for _, tb := range []string{"", "Inbox", "a.b.c", "x;drop", `"x"`, "1x"} {
		if ValidTable(tb) {
			t.Errorf("%q accepted", tb)
		}
	}
}
