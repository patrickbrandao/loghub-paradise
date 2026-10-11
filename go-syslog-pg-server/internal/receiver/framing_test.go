package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
)

// octet builds an octet-counting stream with the lengths COMPUTED. Writing them
// by hand is how a framing test drifts out of agreement with itself.
func octet(msgs ...string) string {
	var b strings.Builder
	for _, m := range msgs {
		fmt.Fprintf(&b, "%d %s", len(m), m)
	}
	return b.String()
}

func framer(input string, mode FramingMode, max int) *Framer {
	return NewFramer(bufio.NewReader(strings.NewReader(input)), mode, max)
}

type frame struct {
	msg     string
	size    int
	framing item.Framing
}

func drain(t *testing.T, f *Framer) ([]frame, error) {
	t.Helper()
	var out []frame
	for i := 0; i < 10000; i++ {
		msg, size, fr, err := f.Next()
		if err != nil {
			return out, err
		}
		out = append(out, frame{string(msg), size, fr})
	}
	t.Fatal("drain did not terminate")
	return nil, nil
}

func msgs(fs []frame) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.msg
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFramerOctetCounting(t *testing.T) {
	a := `<13>Sep 12 14:33:01 host app: um`
	b := `<14>dois`
	got, err := drain(t, framer(octet(a, b), FramingAuto, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if !equal(msgs(got), []string{a, b}) {
		t.Fatalf("got %q", msgs(got))
	}
	for _, f := range got {
		if f.framing != item.FramingOctetCounting || f.size != len(f.msg) {
			t.Fatalf("frame %+v", f)
		}
	}
}

func TestFramerLF(t *testing.T) {
	got, err := drain(t, framer("<13>um\n<14>dois\n", FramingAuto, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if !equal(msgs(got), []string{"<13>um", "<14>dois"}) {
		t.Fatalf("got %q", msgs(got))
	}
	for _, f := range got {
		if f.framing != item.FramingNonTransparent || f.size != len(f.msg) {
			t.Fatalf("frame %+v", f)
		}
	}
}

// The framing is decided ONCE per connection (SPEC §5.2, D1). Re-deciding per
// message would let a message that happens to read "42 text" flip the framing
// and swallow everything after it.
func TestFramerDecidesOncePerConnection(t *testing.T) {
	in := "<13>um\n42 nao e um comprimento\n<14>tres\n"
	got, err := drain(t, framer(in, FramingAuto, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	want := []string{"<13>um", "42 nao e um comprimento", "<14>tres"}
	if !equal(msgs(got), want) {
		t.Fatalf("got %q, want %q", msgs(got), want)
	}
}

// SPEC §5.2: octet-counting only when the first bytes CONFIRM
// [1-9][0-9]{0,9} SP. Everything else -- a digit without a space, a leading
// zero, eleven digits -- is non-transparent.
func TestFramerConfirmationRule(t *testing.T) {
	cases := []struct {
		in   string
		want item.Framing
	}{
		{"5 hello", item.FramingOctetCounting},
		{"10 0123456789", item.FramingOctetCounting},
		{"1234567890 x", item.FramingOctetCounting}, // ten digits: the most allowed
		{"12345678901 x\n", item.FramingNonTransparent},
		{"0 x\n", item.FramingNonTransparent},
		{"05 hello\n", item.FramingNonTransparent},
		{"123\n", item.FramingNonTransparent},
		{"123", item.FramingNonTransparent}, // EOF inside the digits
		{" 12 x\n", item.FramingNonTransparent},
		{"<13>oi\n", item.FramingNonTransparent},
		{"texto solto\n", item.FramingNonTransparent},
		{"2026-09-13 10:00:00,123 ERROR boom\n", item.FramingNonTransparent},
	}
	for _, tc := range cases {
		f := framer(tc.in, FramingAuto, 1<<20)
		if _, _, fr, err := f.Next(); err != nil {
			t.Errorf("%q: %v", tc.in, err)
		} else if fr != tc.want {
			t.Errorf("%q: framing %v, want %v", tc.in, fr, tc.want)
		}
	}
}

// The log4j appender of the corpus sends lines with no PRI that begin with a
// year. As the FIRST frame, "2026" without a space must not be taken for a
// MSG-LEN: the per-frame rule of the skill would close the connection on
// every line (SPEC §5.2, reason 2 of D1).
func TestFramerLog4jFirstFrameStaysLF(t *testing.T) {
	lines := []string{
		"2026-09-13 10:00:00,123 ERROR [main] com.example.App - boom",
		"2026-09-13 10:00:01,456 WARN  [pool-1] com.example.Db - slow query",
		"2026-09-13 10:00:02,789 INFO  [main] com.example.App - recovered",
	}
	got, err := drain(t, framer(strings.Join(lines, "\n")+"\n", FramingAuto, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want a clean end", err)
	}
	if !equal(msgs(got), lines) {
		t.Fatalf("got %q", msgs(got))
	}
	for _, f := range got {
		if f.framing != item.FramingNonTransparent {
			t.Fatalf("framing = %v", f.framing)
		}
	}
}

// The decision never waits for a byte it does not need. A short first line --
// an appliance that logs once an hour -- must come out at once, not when ten
// more bytes arrive. A fixed eleven-byte peek would hang here.
func TestFramerShortFirstLineDoesNotWait(t *testing.T) {
	for _, line := range []string{"hi\n", "7\n", "<1>x\n"} {
		pr, pw := io.Pipe()
		f := NewFramer(bufio.NewReader(pr), FramingAuto, 1<<20)
		go func() { _, _ = pw.Write([]byte(line)) }() // and then nothing, without closing

		done := make(chan string, 1)
		go func() {
			msg, _, _, err := f.Next()
			if err != nil {
				done <- "error: " + err.Error()
				return
			}
			done <- string(msg)
		}()
		select {
		case got := <-done:
			if got != strings.TrimSuffix(line, "\n") {
				t.Errorf("%q -> %q", line, got)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("%q: the first frame waited for bytes the emitter never sent", line)
		}
		pw.Close()
	}
}

func TestFramerFixedMode(t *testing.T) {
	// A fixed LF framer must not read "7 " as a length.
	f := framer("7 <13>oi\n", FramingLF, 1<<20)
	msg, _, fr, err := f.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "7 <13>oi" || fr != item.FramingNonTransparent {
		t.Fatalf("msg = %q framing %v", msg, fr)
	}
	// A fixed octet framer reads it as one.
	f = framer("7 <13>oi\n", FramingOctet, 1<<20)
	msg, _, fr, err = f.Next()
	if err != nil || string(msg) != "<13>oi\n" || fr != item.FramingOctetCounting {
		t.Fatalf("msg = %q framing %v err %v", msg, fr, err)
	}
}

// SPEC §5.3: the frame ends at the first LF or NUL; every CR right before the
// terminator goes; neither terminator nor CRs are in the message or its size.
func TestFramerTerminators(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"<13>oi\n", []string{"<13>oi"}},
		{"<13>oi\r\n", []string{"<13>oi"}},
		{"<13>oi\r\r\r\n", []string{"<13>oi"}}, // ALL trailing CRs
		{"<13>oi\x00", []string{"<13>oi"}},     // NUL-terminated
		{"<13>oi\r\x00", []string{"<13>oi"}},
		{"<13>o\ri\n", []string{"<13>o\ri"}},             // a CR in the middle is data
		{"<13>a\x00<14>b\n", []string{"<13>a", "<14>b"}}, // a NUL ends the frame
		{"<13>sem quebra", []string{"<13>sem quebra"}},   // completed by EOF
		{"<13>a\n<14>b\x00<15>c\r\n", []string{"<13>a", "<14>b", "<15>c"}},
	}
	for _, tc := range cases {
		got, err := drain(t, framer(tc.in, FramingLF, 1<<20))
		if !errors.Is(err, io.EOF) {
			t.Errorf("%q: err = %v", tc.in, err)
			continue
		}
		if !equal(msgs(got), tc.want) {
			t.Errorf("%q -> %q, want %q", tc.in, msgs(got), tc.want)
		}
		for _, f := range got {
			if f.size != len(f.msg) {
				t.Errorf("%q: size %d for %q: a terminator or CR was counted", tc.in, f.size, f.msg)
			}
		}
	}
}

// A CRLF split across two reads is still a CRLF.
func TestFramerCRLFAcrossReads(t *testing.T) {
	r := io.MultiReader(strings.NewReader("<13>abc\r"), strings.NewReader("\r"), strings.NewReader("\n<14>d\n"))
	got, err := drain(t, NewFramer(bufio.NewReaderSize(r, 16), FramingLF, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !equal(msgs(got), []string{"<13>abc", "<14>d"}) || got[0].size != 7 {
		t.Fatalf("got %+v", got)
	}
}

// SPEC §5.3 and §5.6: empty frames are RETURNED, with size 0, so the pipeline
// counts them as reason="empty". They are not skipped in silence any more.
func TestFramerEmptyFramesAreReturned(t *testing.T) {
	got, err := drain(t, framer("\n\n<13>oi\n\r\n<14>tchau\n\x00", FramingLF, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	want := []string{"", "", "<13>oi", "", "<14>tchau", ""}
	if !equal(msgs(got), want) {
		t.Fatalf("got %q, want %q", msgs(got), want)
	}
	for _, f := range got {
		if f.msg == "" && f.size != 0 {
			t.Fatalf("empty frame with size %d", f.size)
		}
	}
}

// In octet counting the bytes are the message exactly: a newline inside the count
// is data the emitter put there (SPEC §5.4).
func TestFramerOctetKeepsEverythingInTheCount(t *testing.T) {
	f := framer(octet("<13>a\nb\r\n"), FramingOctet, 1<<20)
	msg, size, _, err := f.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "<13>a\nb\r\n" || size != len(msg) {
		t.Fatalf("msg = %q size %d", msg, size)
	}
}

// SPEC §5.5: an oversized octet frame keeps max bytes, reports the declared
// size, and the excess is consumed so the NEXT frame is read correctly. The
// connection is not closed for size.
func TestFramerOctetTruncationKeepsSync(t *testing.T) {
	long := "<13>" + strings.Repeat("x", 20)
	got, err := drain(t, framer(octet(long, "<14>next"), FramingAuto, 8))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %+v", got)
	}
	if got[0].msg != long[:8] || got[0].size != len(long) {
		t.Fatalf("first = %+v, want %q size %d", got[0], long[:8], len(long))
	}
	if got[1].msg != "<14>next" || got[1].size != 8 {
		t.Fatalf("second = %+v: the stream lost sync", got[1])
	}
}

// The non-transparent excess is discarded up to the terminator, counted into
// size without the trailing CRs (SPEC §5.5).
func TestFramerLFTruncationKeepsSync(t *testing.T) {
	long := "<13>" + strings.Repeat("y", 16)
	got, err := drain(t, framer(long+"\r\n<14>next\n", FramingLF, 8))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0].msg != long[:8] || got[0].size != len(long) {
		t.Fatalf("first = %+v", got)
	}
	if got[1].msg != "<14>next" {
		t.Fatalf("second = %+v: the stream lost sync", got[1])
	}
}

// Trailing CRs are terminator even when they straddle the cut, and a CR in the
// middle is data even when it is the last byte kept.
func TestFramerLFTrailingCRsAroundTheCut(t *testing.T) {
	cases := []struct {
		in       string
		max      int
		wantMsg  string
		wantSize int
	}{
		{"abcd\r\r\n", 4, "abcd", 4},  // the whole message fits: not truncated
		{"ab\r\r\n", 3, "ab", 2},      // the run reaches into the kept bytes
		{"abc\r\rX\n", 4, "abc\r", 6}, // mid-message CRs are data
		{"abcdef\r\n", 4, "abcd", 6},  // truncated, CR not counted
		{"\r\r\r\n", 2, "", 0},        // only CRs: an empty frame
		{"abc\r", 10, "abc", 3},       // EOF acts as the terminator
		{"abcdefgh\r", 4, "abcd", 8},  // ... also for a truncated last frame
		{"abcdefgh\r\r", 9, "abcdefgh", 8},
	}
	for _, tc := range cases {
		f := framer(tc.in, FramingLF, tc.max)
		msg, size, _, err := f.Next()
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if string(msg) != tc.wantMsg || size != tc.wantSize {
			t.Errorf("%q max %d -> %q size %d, want %q size %d", tc.in, tc.max, msg, size, tc.wantMsg, tc.wantSize)
		}
	}
}

// SPEC §5.4: the buffer grows with the bytes that ARRIVE, never with the
// declared count. A client that declares ~10 GB and sends three bytes costs
// one small block.
func TestFramerOctetGrowsWithTheBytesNotTheCount(t *testing.T) {
	f := framer("9999999999 abc", FramingOctet, 64<<10)
	msg, size, _, err := f.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "abc" {
		t.Fatalf("msg = %q", msg)
	}
	if size != clampInt(9999999999) {
		t.Fatalf("size = %d, want the declared MSG-LEN", size)
	}
	if cap(f.buf) > minFrameCap {
		t.Fatalf("buffer capacity = %d after three bytes; it was sized from the declared count", cap(f.buf))
	}
	if _, _, _, err := f.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after a cut frame: err = %v, want EOF", err)
	}
}

// A line that never ends is kept up to max and discarded after it, without the
// buffer growing past max: no length was declared, so the ceiling is the only
// thing between it and the process running out of memory.
func TestFramerLFLongLineIsDiscardedWithoutGrowing(t *testing.T) {
	long := strings.Repeat("a", 1<<20)
	f := framer(long+"\n<14>ok\n", FramingLF, 1024)
	got, err := drain(t, f)
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].msg) != 1024 || got[0].size != 1<<20 || got[1].msg != "<14>ok" {
		t.Fatalf("got %d frames, first len %d size %d", len(got), len(got[0].msg), got[0].size)
	}
	if cap(f.buf) > 1024 {
		t.Fatalf("buffer grew to %d past max_message_bytes", cap(f.buf))
	}
}

// SPEC §5.4: a connection closed in the middle of a frame delivers what
// arrived, with size = the declared MSG-LEN, so the converter marks it
// truncated. Then the stream ends.
func TestFramerOctetMidFrameClose(t *testing.T) {
	f := framer("100 curto", FramingOctet, 1<<20)
	msg, size, _, err := f.Next()
	if err != nil || string(msg) != "curto" || size != 100 {
		t.Fatalf("msg = %q size %d err %v", msg, size, err)
	}
	if _, _, _, err := f.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}

	// Cut while discarding the excess of a truncated frame.
	f = framer("10 abcdef", FramingOctet, 4)
	msg, size, _, err = f.Next()
	if err != nil || string(msg) != "abcd" || size != 10 {
		t.Fatalf("msg = %q size %d err %v", msg, size, err)
	}
	if _, _, _, err := f.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}

	// Cut right after the space: nothing arrived, an empty message.
	f = framer("50 ", FramingOctet, 1<<20)
	msg, size, _, err = f.Next()
	if err != nil || len(msg) != 0 || size != 50 {
		t.Fatalf("msg = %q size %d err %v", msg, size, err)
	}
}

// SPEC §5.2: in a stream decided as octet-counting, a MSG-LEN that does not
// confirm closes the connection. There is no way to resynchronise.
func TestFramerMalformedOctetIsUnrecoverable(t *testing.T) {
	for _, in := range []string{
		"12x <13>oi",       // not numeric
		"12345678901 x",    // eleven digits
		"0 x",              // zero
		"012 <13>abcdefgh", // leading zero
		" 5 hello",         // no digit
		"12",               // the connection ended inside MSG-LEN
		"<13>oi\n",         // tcp.framing=octet: a frame without a MSG-LEN
	} {
		f := framer(in, FramingOctet, 1<<20)
		if _, _, _, err := f.Next(); !errors.Is(err, ErrMalformedFrame) {
			t.Errorf("%q: err = %v, want ErrMalformedFrame", in, err)
		}
	}
	// Auto: the first frame confirms octet-counting, the second is garbage.
	f := framer(octet("<13>a")+"x12 b", FramingAuto, 1<<20)
	if msg, _, _, err := f.Next(); err != nil || string(msg) != "<13>a" {
		t.Fatalf("first = %q %v", msg, err)
	}
	if _, _, _, err := f.Next(); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("err = %v, want ErrMalformedFrame", err)
	}
}

// ... and a legitimate large frame still arrives whole, across many refills.
func TestFramerOctetLargeFrameRoundTrips(t *testing.T) {
	msg := "<13>" + strings.Repeat("x", 300<<10)
	f := NewFramer(bufio.NewReaderSize(strings.NewReader(octet(msg, "<14>after")), 4096), FramingOctet, 512<<10)
	got, err := drain(t, f)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0].msg != msg || got[1].msg != "<14>after" {
		t.Fatalf("large frame did not round-trip: %d messages", len(got))
	}
}

func TestFramerEmptyStream(t *testing.T) {
	for _, mode := range []FramingMode{FramingAuto, FramingOctet, FramingLF} {
		f := framer("", mode, 1<<20)
		if _, _, _, err := f.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("mode %v: err = %v, want EOF", mode, err)
		}
	}
}

func TestParseFramingMode(t *testing.T) {
	for in, want := range map[string]FramingMode{"auto": FramingAuto, "": FramingAuto,
		"octet": FramingOctet, "LF": FramingLF, "non-transparent": FramingLF} {
		got, err := ParseFramingMode(in)
		if err != nil || got != want {
			t.Errorf("%q -> %v %v", in, got, err)
		}
	}
	if _, err := ParseFramingMode("relp"); err == nil {
		t.Error("an unknown framing was accepted")
	}
}

// FuzzFrame is mandatory (SPEC §18.3). The framer reads straight off the
// network on the goroutine that serves a connection: a panic here takes the
// connection and, with it, an emitter's whole log stream. Properties, for all
// three modes: no panic, no message above max, size >= len(msg), and the
// buffer never grows past max -- so never by a declared MSG-LEN.
func FuzzFrame(f *testing.F) {
	seeds := []string{
		"34 <13>Sep 12 14:33:01 host app: um",
		"<13>um\n<14>dois\n",
		"0 ",
		"9999999999 x",
		"999999999999999999999 x",
		"12x y",
		"\n\n\n",
		"<13>oi\r\n",
		"<13>oi\x00\n",
		"2026-09-13 10:00:00,123 ERROR boom\n",
		"5 hello5 world",
		"abc\r\r\rdef\r\n\x00",
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	const max = 64

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, mode := range []FramingMode{FramingAuto, FramingOctet, FramingLF} {
			fr := NewFramer(bufio.NewReaderSize(bytes.NewReader(data), 16), mode, max)
			for i := 0; i < 1000; i++ {
				msg, size, framing, err := fr.Next()
				if err != nil {
					break
				}
				if len(msg) > max {
					t.Fatalf("mode %v: a message above max came back: %d bytes", mode, len(msg))
				}
				if size < len(msg) {
					t.Fatalf("mode %v: size %d < len %d", mode, size, len(msg))
				}
				if framing != item.FramingOctetCounting && framing != item.FramingNonTransparent {
					t.Fatalf("mode %v: framing %v", mode, framing)
				}
				if framing == item.FramingNonTransparent &&
					(bytes.IndexByte(msg, '\n') >= 0 || bytes.IndexByte(msg, 0) >= 0) {
					t.Fatalf("a terminator leaked into a non-transparent message: %q", msg)
				}
				if framing == item.FramingNonTransparent && size == len(msg) && len(msg) > 0 && msg[len(msg)-1] == '\r' {
					t.Fatalf("a trailing CR leaked into a whole non-transparent message: %q", msg)
				}
				if cap(fr.buf) > max {
					t.Fatalf("mode %v: buffer capacity %d above max", mode, cap(fr.buf))
				}
			}
		}
	})
}

// ------------------------------------------------------- the framing oracle

// The oracle below is an independent, straight-line reading of SPEC §5.2-§5.5
// over a whole byte slice (from the review of 2026-10-05). FuzzFrameOracle
// feeds the same bytes to the Framer through a reader that splits them at
// arbitrary points, and demands the same frames, sizes, framings and ending.

type refFrame struct {
	msg     string
	size    int
	framing item.Framing
}

func refNT(data []byte, max int) []refFrame {
	var out []refFrame
	pos := 0
	for pos < len(data) {
		rest := data[pos:]
		e := indexTerminator(rest)
		seg := rest
		if e >= 0 {
			seg = rest[:e]
			pos += e + 1
		} else {
			pos = len(data)
		}
		size := len(seg)
		for size > 0 && seg[size-1] == '\r' {
			size--
		}
		k := size
		if k > max {
			k = max
		}
		out = append(out, refFrame{string(seg[:k]), size, item.FramingNonTransparent})
	}
	return out
}

// refOctet returns the frames and whether the stream ended malformed.
func refOctet(data []byte, max int) ([]refFrame, bool) {
	var out []refFrame
	pos := 0
	for pos < len(data) {
		var n int64
		d := 0
		for {
			if pos >= len(data) {
				return out, true // ended inside MSG-LEN
			}
			c := data[pos]
			pos++
			if c == ' ' && d > 0 {
				break
			}
			if c < '0' || c > '9' || (d == 0 && c == '0') {
				return out, true
			}
			if d++; d > 10 {
				return out, true
			}
			n = n*10 + int64(c-'0')
		}
		take := n
		if avail := int64(len(data) - pos); take > avail {
			take = avail
		}
		k := take
		if k > int64(max) {
			k = int64(max)
		}
		out = append(out, refFrame{string(data[pos : pos+int(k)]), clampInt(n), item.FramingOctetCounting})
		pos += int(take)
		if take < n {
			return out, false
		}
	}
	return out, false
}

func refAuto(data []byte) item.Framing {
	for i := 0; i < len(data) && i < 11; i++ {
		c := data[i]
		if c == ' ' && i > 0 {
			return item.FramingOctetCounting
		}
		if i < 10 && c >= '0' && c <= '9' && (i > 0 || c != '0') {
			continue
		}
		return item.FramingNonTransparent
	}
	return item.FramingNonTransparent
}

// chunkReader hands out its data in pieces of 1 to 7 bytes chosen by chunks:
// a CRLF, a MSG-LEN or a terminator split across reads.
type chunkReader struct {
	data   []byte
	chunks []byte
	i      int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := 1
	if len(r.chunks) > 0 {
		n = int(r.chunks[r.i%len(r.chunks)])%7 + 1
		r.i++
	}
	n = min(n, len(p), len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

func FuzzFrameOracle(f *testing.F) {
	for _, s := range []string{"34 <13>Sep 12 14:33:01 host app: um", "<13>um\n<14>dois\n", "abc\r\r\rdef\r\n\x00",
		"5 hello5 world", "3 ab", "2026-09-13 x\n", "12345678901 x", "1234567890 x", "\r\r", "a\r\r\r\r\r\r\r\r\r\r\nb"} {
		f.Add([]byte(s), []byte{1, 2, 3}, uint8(5))
	}
	f.Fuzz(func(t *testing.T, data, chunks []byte, maxb uint8) {
		max := int(maxb%40) + 1
		for _, mode := range []FramingMode{FramingAuto, FramingOctet, FramingLF} {
			var want []refFrame
			malformed := false
			fixed := mode
			if mode == FramingAuto && len(data) > 0 {
				if refAuto(data) == item.FramingOctetCounting {
					fixed = FramingOctet
				} else {
					fixed = FramingLF
				}
			}
			if len(data) > 0 || mode != FramingAuto {
				if fixed == FramingOctet {
					want, malformed = refOctet(data, max)
				} else {
					want = refNT(data, max)
				}
			}
			r := &chunkReader{data: append([]byte(nil), data...), chunks: chunks}
			fr := NewFramer(bufio.NewReaderSize(r, 16), mode, max)
			var got []refFrame
			var endErr error
			for i := 0; i < 10000; i++ {
				msg, size, framing, err := fr.Next()
				if err != nil {
					endErr = err
					break
				}
				got = append(got, refFrame{string(msg), size, framing})
			}
			if len(got) != len(want) {
				t.Fatalf("mode %v max %d data %q: got %d frames %+v, want %d %+v", mode, max, data, len(got), got, len(want), want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("mode %v max %d data %q frame %d: got %+v want %+v", mode, max, data, i, got[i], want[i])
				}
			}
			if isMal := errors.Is(endErr, ErrMalformedFrame); isMal != malformed {
				t.Fatalf("mode %v data %q: end err %v, want malformed=%v", mode, data, endErr, malformed)
			} else if !isMal && !errors.Is(endErr, io.EOF) {
				t.Fatalf("mode %v data %q: end err %v", mode, data, endErr)
			}
		}
	})
}
