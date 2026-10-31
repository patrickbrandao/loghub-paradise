package extract

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func octet(msgs ...string) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(strconv.Itoa(len(m)) + " " + m)
	}
	return b.String()
}

func relp(msgs ...string) string {
	var b strings.Builder
	offers := "relp_version=0\nrelp_software=librelp,1.11.0,http://librelp.adiscon.com\ncommands=syslog"
	fmt.Fprintf(&b, "1 open %d %s\n", len(offers), offers)
	for i, m := range msgs {
		fmt.Fprintf(&b, "%d syslog %d %s\n", i+2, len(m), m)
	}
	fmt.Fprintf(&b, "%d close 0\n", len(msgs)+2)
	return b.String()
}

// run feeds the stream in chunks of the given sizes (cycled) and returns the
// messages.
func run(t *testing.T, in string, strict, mid bool, chunks []int, gapAt int) ([]string, Stats, Mode) {
	t.Helper()
	var st Stats
	var got []string
	s := NewStream(strict, mid, &st, func(b []byte, _ time.Time) { got = append(got, string(b)) })
	for off, i := 0, 0; off < len(in); i++ {
		n := len(in) - off
		if len(chunks) > 0 {
			n = min(chunks[i%len(chunks)], n)
		}
		if gapAt > 0 && off <= gapAt && gapAt < off+n {
			// Drop the chunk that contains gapAt and report it.
			s.Gap(n)
			off += n
			continue
		}
		s.Data([]byte(in[off:off+n]), time.Time{})
		off += n
	}
	m := s.Mode()
	s.End()
	return got, st, m
}

func TestFramings(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		strict bool
		want   []string
		mode   Mode
	}{
		{"octet", octet("<13>1 - h a - - - one", "<13>two\nwith lf", "<14>three"), true,
			[]string{"<13>1 - h a - - - one", "<13>two\nwith lf", "<14>three"}, ModeOctet},
		{"lf", "<13>one\r\n<13>two\n\n<13>three\x00<13>last", true,
			[]string{"<13>one", "<13>two", "<13>three", "<13>last"}, ModeLF},
		{"relp", relp("<13>r1", "<13>r2 with\nlf", "<13>r3"), true,
			[]string{"<13>r1", "<13>r2 with\nlf", "<13>r3"}, ModeRELP},
		{"strict refuses http", "POST /hook HTTP/1.1\r\nHost: x\r\n\r\n", true, nil, ModeIgnore},
		{"lenient takes lines without PRI", "2026-09-13 10:00:00,123 ERROR boom\nsecond\n", false,
			[]string{"2026-09-13 10:00:00,123 ERROR boom", "second"}, ModeLF},
		{"digit without space is lf", "2026-09-13 a date\n", false, []string{"2026-09-13 a date"}, ModeLF},
		{"relp rsp direction ignored", "1 rsp 6 200 OK\n2 rsp 6 200 OK\n", true, nil, ModeIgnore},
	}
	for _, c := range cases {
		for _, chunks := range [][]int{nil, {1}, {3, 7}, {2, 1, 13}} {
			got, _, mode := run(t, c.in, c.strict, false, chunks, -1)
			if !reflect.DeepEqual(got, c.want) || mode != c.mode {
				t.Errorf("%s chunks %v: mode %v got %q, want %v %q", c.name, chunks, mode, got, c.mode, c.want)
			}
		}
	}
}

// TestChunkInvariance: how the capture split the bytes into segments never
// changes the messages.
func TestChunkInvariance(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var msgs []string
	for i := range 200 {
		msgs = append(msgs, fmt.Sprintf("<%d>1 2026-10-30T10:00:00Z h app - - - seq=%d %s", r.IntN(191), i, strings.Repeat("x", r.IntN(300))))
	}
	for name, in := range map[string]string{"octet": octet(msgs...), "relp": relp(msgs...), "lf": strings.Join(msgs, "\n") + "\n"} {
		for range 50 {
			chunks := []int{1 + r.IntN(100), 1 + r.IntN(5), 1 + r.IntN(2000)}
			got, st, _ := run(t, in, true, false, chunks, -1)
			if !reflect.DeepEqual(got, msgs) {
				t.Fatalf("%s chunks %v: %d messages, first diff at %d", name, chunks, len(got), firstDiff(got, msgs))
			}
			if st.SkippedBytes != 0 || st.Resyncs != 0 {
				t.Fatalf("%s: stats %+v", name, st)
			}
		}
	}
}

func firstDiff(a, b []string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// TestMidstream: a capture that began inside a connection resynchronizes on
// the first verified frame and loses only the frame it began in.
func TestMidstream(t *testing.T) {
	msgs := []string{"<13>alpha 12 <13>fake", "<13>beta", "<13>gamma", "<13>delta"}
	for name, in := range map[string]string{"octet": octet(msgs...), "relp": relp(msgs...), "lf": strings.Join(msgs, "\n") + "\n"} {
		for _, cut := range []int{1, 5, 9, 15} {
			got, st, _ := run(t, in[cut:], true, true, []int{4}, -1)
			want := msgs[1:]
			if name == "relp" {
				want = msgs // the cut falls in the open frame
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s cut %d: got %q want %q", name, cut, got, want)
			}
			if st.SkippedBytes == 0 {
				t.Errorf("%s cut %d: skipped bytes not counted", name, cut)
			}
		}
	}
}

// TestGap: bytes lost in the capture drop the frame they hit and nothing else.
func TestGap(t *testing.T) {
	msgs := []string{"<13>one", "<13>two", "<13>three", "<13>four", "<13>five"}
	for name, in := range map[string]string{"octet": octet(msgs...), "relp": relp(msgs...), "lf": strings.Join(msgs, "\n") + "\n"} {
		at := strings.Index(in, "three") + 1
		got, st, _ := run(t, in, true, false, []int{3}, at)
		for _, m := range got {
			if m == "<13>three" {
				t.Errorf("%s: the frame hit by the gap was replayed", name)
			}
		}
		if len(got) < 3 || got[len(got)-1] != "<13>five" || got[0] != "<13>one" {
			t.Errorf("%s: got %q", name, got)
		}
		if st.LostFrames == 0 || st.Resyncs == 0 {
			t.Errorf("%s: stats %+v", name, st)
		}
	}
}

func TestOctetBadLengthResyncs(t *testing.T) {
	in := octet("<13>a") + "0x <garbage>" + octet("<13>b", "<13>c")
	got, st, _ := run(t, in, true, false, nil, -1)
	if !reflect.DeepEqual(got, []string{"<13>a", "<13>b", "<13>c"}) || st.Resyncs != 1 {
		t.Fatalf("got %q stats %+v", got, st)
	}
}

func TestIncompleteAtEnd(t *testing.T) {
	got, st, _ := run(t, octet("<13>a")+"50 <13>cut", true, false, nil, -1)
	if !reflect.DeepEqual(got, []string{"<13>a"}) || st.LostFrames != 1 {
		t.Fatalf("got %q stats %+v", got, st)
	}
}

func TestDatagram(t *testing.T) {
	var st Stats
	if !Datagram([]byte("<13>x"), true, &st) || Datagram([]byte("\x12\x34dns"), true, &st) || !Datagram([]byte("no pri"), false, &st) || Datagram(nil, false, &st) {
		t.Fatal("datagram decisions")
	}
	if st.Datagrams != 2 || st.DatagramsSkip != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func FuzzStream(f *testing.F) {
	f.Add([]byte(octet("<13>a", "<13>b")), uint8(3), true, false)
	f.Add([]byte(relp("<13>a")), uint8(1), true, true)
	f.Add([]byte("<13>a\n<13>b"), uint8(2), false, true)
	f.Fuzz(func(t *testing.T, in []byte, chunk uint8, strict, mid bool) {
		// Whatever the input and the split, the messages are the same and
		// nothing panics.
		var st1, st2 Stats
		var a, b []string
		s1 := NewStream(strict, mid, &st1, func(m []byte, _ time.Time) { a = append(a, string(m)) })
		s1.Data(in, time.Time{})
		s1.End()
		s2 := NewStream(strict, mid, &st2, func(m []byte, _ time.Time) { b = append(b, string(m)) })
		n := int(chunk%32) + 1
		for off := 0; off < len(in); off += n {
			s2.Data(in[off:min(off+n, len(in))], time.Time{})
		}
		s2.End()
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("split %d changed the messages: %q vs %q", n, a, b)
		}
	})
}
