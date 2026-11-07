package extract

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestResyncLargeCandidateHolds: a midstream octet-counting stream whose
// first verifiable frame declares more than 64 KiB, with a LF frame start
// inside its content. Whole, the octet candidate is earlier and verified, so
// it wins; split into segments, it must still win -- the 64 KiB cap on
// waiting candidates let the later LF start win (SPEC §6.6, pendência 5).
func TestResyncLargeCandidateHolds(t *testing.T) {
	body := "<13>big " + strings.Repeat("y", 50_000) + "\n<13>inside\n" + strings.Repeat("z", 50_000)
	in := "garbage tail of a frame " + strconv.Itoa(len(body)) + " " + body + octet("<13>after", "<13>last")
	whole, _, _ := run(t, in, true, true, nil, -1)
	if len(whole) != 3 || whole[0] != body || whole[1] != "<13>after" {
		t.Fatalf("whole: %d messages, first %.40q", len(whole), whole)
	}
	for _, chunk := range []int{1460, 7, 65536} {
		got, st, _ := run(t, in, true, true, []int{chunk}, -1)
		if !reflect.DeepEqual(got, whole) {
			t.Fatalf("chunk %d: %d messages, first %.40q", chunk, len(got), got)
		}
		if st.SkippedBytes != uint64(len("garbage tail of a frame ")) {
			t.Fatalf("chunk %d: skipped %d", chunk, st.SkippedBytes)
		}
	}
}

// TestResyncLinearCost: holding a frame of several MiB, delivered in
// Ethernet-sized segments, must not rescan the buffer on every segment, and
// a midstream stream that is not syslog keeps only a small tail.
func TestResyncLinearCost(t *testing.T) {
	body := "<13>" + strings.Repeat("q", 6<<20)
	in := "junk " + strconv.Itoa(len(body)) + " " + body + octet("<13>next")
	var got []string
	var st0 Stats
	s0 := NewStream(true, true, &st0, func(b []byte, _ time.Time) { got = append(got, string(b)) })
	for off := 0; off < len(in); off += 1460 {
		s0.Data([]byte(in[off:min(off+1460, len(in))]), time.Time{})
	}
	s0.End(false)
	if len(got) != 2 || got[1] != "<13>next" || len(got[0]) != len(body) {
		t.Fatalf("%d messages", len(got))
	}
	// ~4300 segments; the search runs again only when the held frame can
	// resolve, not once per segment.
	if s0.scans > 10 {
		t.Fatalf("held a 6 MiB frame with %d searches: one per segment is quadratic", s0.scans)
	}

	// Binary midstream data with no candidate: the buffer stays small.
	var st Stats
	s := NewStream(true, true, &st, func([]byte, time.Time) {})
	seg := []byte(strings.Repeat("\x01\x02 abc\n\x7f", 160))
	for range 2000 {
		s.Data(seg, time.Time{})
	}
	if len(s.buf) > resyncWindow {
		t.Fatalf("no-candidate buffer holds %d bytes", len(s.buf))
	}
	s.End(false)
	if st.SkippedBytes != uint64(2000*len(seg)) {
		t.Fatalf("skipped %d of %d", st.SkippedBytes, 2000*len(seg))
	}
}

// TestResyncLargeRELPCandidate: the same hold for a RELP frame above 64 KiB.
func TestResyncLargeRELPCandidate(t *testing.T) {
	big := "<13>relp " + strings.Repeat("r", 80_000) + "\n<13>inside"
	in := "tail\n" + "7 syslog " + strconv.Itoa(len(big)) + " " + big + "\n8 syslog 9 <13>after\n"
	whole, _, _ := run(t, in, true, true, nil, -1)
	if len(whole) != 2 || whole[0] != big {
		t.Fatalf("whole: %.40q", whole)
	}
	for _, chunk := range []int{1460, 3} {
		if got, _, _ := run(t, in, true, true, []int{chunk}, -1); !reflect.DeepEqual(got, whole) {
			t.Fatalf("chunk %d: %.40q", chunk, got)
		}
	}
}
