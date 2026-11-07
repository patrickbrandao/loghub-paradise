package extract

import (
	"reflect"
	"testing"
	"time"
)

// TestTailClosedVersusCut: the last frame of a direction, without its end,
// is taken as the server takes it at EOF when the direction was closed (FIN
// or RST, SPEC §6.4), and by the old rule when the capture cut it (SPEC
// §6.6; from go-gelf-injector G12, 2026-11-07):
//
//   - LF: the line without terminator is a message either way (the server
//     delivers it whole at EOF; a line has no length to prove it cut);
//   - octet: closed, what arrived of the frame is a message (the server
//     delivers it, truncated) and counts in ClosedPartial; cut, it is lost;
//   - RELP: a frame without its trailer is never read, closed or not.
func TestTailClosedVersusCut(t *testing.T) {
	type outcome struct {
		msgs    []string
		lost    uint64
		partial uint64
	}
	for _, c := range []struct {
		name, in    string
		closed, cut outcome
	}{
		{"lf", "<13>a\n<13>b", outcome{[]string{"<13>a", "<13>b"}, 0, 0}, outcome{[]string{"<13>a", "<13>b"}, 0, 0}},
		{"octet", octet("<13>a") + "20 <13>part", outcome{[]string{"<13>a", "<13>part"}, 0, 1}, outcome{[]string{"<13>a"}, 1, 0}},
		{"octet header only", octet("<13>a") + "20 ", outcome{[]string{"<13>a"}, 1, 0}, outcome{[]string{"<13>a"}, 1, 0}},
		{"octet digits only", octet("<13>a") + "20", outcome{[]string{"<13>a"}, 1, 0}, outcome{[]string{"<13>a"}, 1, 0}},
		{"relp", "1 syslog 5 <13>a\n2 syslog 5 <13>b", outcome{[]string{"<13>a"}, 1, 0}, outcome{[]string{"<13>a"}, 1, 0}},
	} {
		for _, chunk := range []int{1, 3, 100} {
			for _, closed := range []bool{true, false} {
				var st Stats
				var got []string
				s := NewStream(true, false, &st, func(b []byte, _ time.Time) { got = append(got, string(b)) })
				for off := 0; off < len(c.in); off += chunk {
					s.Data([]byte(c.in[off:min(off+chunk, len(c.in))]), time.Time{})
				}
				s.End(closed)
				want := c.cut
				if closed {
					want = c.closed
				}
				if !reflect.DeepEqual(got, want.msgs) || st.LostFrames != want.lost || st.ClosedPartial != want.partial {
					t.Fatalf("%s chunk %d closed=%v: %q %+v, want %+v", c.name, chunk, closed, got, st, want)
				}
			}
		}
	}
	// What a resync left after a gap is no frame, closed or not.
	var st Stats
	var got []string
	s := NewStream(true, false, &st, func(b []byte, _ time.Time) { got = append(got, string(b)) })
	s.Data([]byte(octet("<13>a")), time.Time{})
	s.Gap(10)
	s.Data([]byte("<13>rest of a frame"), time.Time{})
	s.End(true)
	if !reflect.DeepEqual(got, []string{"<13>a"}) {
		t.Fatalf("after a gap: %q %+v", got, st)
	}
}
