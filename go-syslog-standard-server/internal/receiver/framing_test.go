package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
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

func framer(input string, mode envelope.Framing, auto bool, max int) *Framer {
	return NewFramer(bufio.NewReader(strings.NewReader(input)), mode, auto, max)
}

func drain(t *testing.T, f *Framer) ([]string, error) {
	t.Helper()
	var out []string
	for i := 0; i < 1000; i++ {
		msg, err := f.Next()
		if err != nil {
			return out, err
		}
		out = append(out, string(msg))
	}
	t.Fatal("drain did not terminate")
	return nil, nil
}

func TestFramerOctetCounting(t *testing.T) {
	a := `<13>Sep 12 14:33:01 host app: um`
	b := `<14>dois`
	got, err := drain(t, framer(octet(a, b), 0, true, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("got %q", got)
	}
}

func TestFramerLF(t *testing.T) {
	in := "<13>um\n<14>dois\n"
	got, err := drain(t, framer(in, 0, true, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0] != "<13>um" || got[1] != "<14>dois" {
		t.Fatalf("got %q", got)
	}
}

// The framing is decided ONCE per connection. Re-deciding per message would let a
// message that happens to start with a digit flip the framing and scramble
// everything after it.
func TestFramerDecidesOncePerConnection(t *testing.T) {
	// First byte is '<', so LF framing. A later line starting with a digit must
	// NOT switch to octet counting.
	in := "<13>um\n42 nao e um comprimento\n<14>tres\n"
	got, err := drain(t, framer(in, 0, true, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	want := []string{"<13>um", "42 nao e um comprimento", "<14>tres"}
	if len(got) != 3 {
		t.Fatalf("got %q", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("message %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFramerAutoDetect(t *testing.T) {
	f := framer(octet("<13>oi"), 0, true, 1<<20)
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	if f.Mode() != envelope.FramingOctet {
		t.Fatalf("mode = %v, want octet", f.Mode())
	}

	f = framer("<13>oi\n", 0, true, 1<<20)
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	if f.Mode() != envelope.FramingLF {
		t.Fatalf("mode = %v, want lf", f.Mode())
	}

	// Junk, or a message with no PRI: LF framing is the one that can
	// resynchronise.
	f = framer("texto solto\n", 0, true, 1<<20)
	if _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	if f.Mode() != envelope.FramingLF {
		t.Fatalf("mode = %v, want lf", f.Mode())
	}
}

func TestFramerFixedMode(t *testing.T) {
	// A fixed LF framer must not read "7 " as a length.
	f := framer("7 <13>oi\n", envelope.FramingLF, false, 1<<20)
	msg, err := f.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "7 <13>oi" {
		t.Fatalf("msg = %q", msg)
	}
}

// No framing byte may ever reach the raw line: it would break the fidelity
// invariant and show up in everyone's destination.
func TestFramerStripsOnlyTheTerminators(t *testing.T) {
	cases := map[string]string{
		"<13>oi\n":       "<13>oi",         // LF
		"<13>oi\r\n":     "<13>oi",         // CRLF, Windows emitters
		"<13>oi\x00\n":   "<13>oi",         // NUL-terminated, BSD convention
		"<13>oi\x00b\n":  "<13>oi\x00b",    // a NUL in the MIDDLE is data
		"<13>o\ri\n":     "<13>o\ri",       // a CR in the middle is data
		"<13>sem quebra": "<13>sem quebra", // a final line with no terminator
	}
	for in, want := range cases {
		f := framer(in, envelope.FramingLF, false, 1<<20)
		msg, err := f.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if string(msg) != want {
			t.Errorf("%q -> %q, want %q", in, msg, want)
		}
	}
}

// A bare newline is common as hand-rolled keep-alive. Counting it as a dropped
// message would make a healthy connection look lossy.
func TestFramerSkipsEmptyLines(t *testing.T) {
	got, err := drain(t, framer("\n\n<13>oi\n\n<14>tchau\n", envelope.FramingLF, false, 1<<20))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0] != "<13>oi" || got[1] != "<14>tchau" {
		t.Fatalf("got %q", got)
	}
}

// In octet counting the bytes are the message exactly: a newline inside the count
// is data the emitter put there.
func TestFramerOctetKeepsEverythingInTheCount(t *testing.T) {
	f := framer(octet("<13>a\nb"), envelope.FramingOctet, false, 1<<20)
	msg, err := f.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "<13>a\nb" {
		t.Fatalf("msg = %q", msg)
	}
}

// A declared length above the ceiling is refused BEFORE a byte is allocated.
func TestFramerOctetCeilingRefusesBeforeAllocating(t *testing.T) {
	f := framer("999999999 x", envelope.FramingOctet, false, 1024)
	if _, err := f.Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// SPEC §5.5: below the ceiling the buffer grows with the bytes that ARRIVE,
// never with the declared count. A client that declares 16 MiB and sends three
// bytes must cost one chunk, not 16 MiB -- with max_conns unlimited by default,
// the difference is a thousand idle connections versus an OOM kill.
func TestFramerOctetGrowsWithTheBytesNotTheCount(t *testing.T) {
	f := framer("16777216 abc", envelope.FramingOctet, false, 16<<20)
	if _, err := f.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
	if cap(f.buf) > 2*octetChunk {
		t.Fatalf("buffer capacity = %d after three bytes; it was sized from the declared count", cap(f.buf))
	}
}

// ... and a legitimate large frame still arrives whole, across many chunks.
func TestFramerOctetLargeFrameRoundTrips(t *testing.T) {
	msg := "<13>" + strings.Repeat("x", 300<<10)
	f := framer(octet(msg, "<14>after"), envelope.FramingOctet, false, 16<<20)
	got, err := drain(t, f)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 2 || got[0] != msg || got[1] != "<14>after" {
		t.Fatalf("large frame did not round-trip: %d messages", len(got))
	}
}

// LF framing declares nothing, so the ceiling is the only thing between a
// connection that never sends a newline and the process running out of memory.
func TestFramerLFCeilingAbortsAnEndlessLine(t *testing.T) {
	f := framer(strings.Repeat("a", 5000), envelope.FramingLF, false, 1024)
	if _, err := f.Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

// A non-numeric octet count leaves no way to find the next boundary. Trying to
// resynchronise would write fragments as if they were messages.
func TestFramerMalformedOctetIsUnrecoverable(t *testing.T) {
	for _, in := range []string{"12x <13>oi", "1234567890123 x"} {
		f := framer(in, envelope.FramingOctet, false, 1<<20)
		_, err := f.Next()
		if !errors.Is(err, ErrMalformedFrame) && !errors.Is(err, ErrFrameTooLarge) {
			t.Errorf("%q: err = %v", in, err)
		}
	}
}

// A count that promises more than the connection delivers leaves half a message,
// and half a message is not a message.
func TestFramerOctetShortReadIsRefused(t *testing.T) {
	f := framer("100 curto", envelope.FramingOctet, false, 1<<20)
	if _, err := f.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v", err)
	}
}

func TestFramerOctetZeroLength(t *testing.T) {
	f := framer("0 "+octet("<13>x"), envelope.FramingOctet, false, 1<<20)
	msg, err := f.Next()
	if err != nil || len(msg) != 0 {
		t.Fatalf("empty message = %q %v", msg, err)
	}
	msg, err = f.Next()
	if err != nil || string(msg) != "<13>x" {
		t.Fatalf("next = %q %v", msg, err)
	}
}

func TestFramerEmptyStream(t *testing.T) {
	for _, mode := range []envelope.Framing{envelope.FramingOctet, envelope.FramingLF} {
		f := framer("", mode, false, 1<<20)
		if _, err := f.Next(); !errors.Is(err, io.EOF) {
			t.Errorf("mode %v: err = %v, want EOF", mode, err)
		}
	}
}

// -------------------------------------------------------------- RELP framing

func relpReader(in string) *RELPReader {
	return NewRELPReader(bufio.NewReader(strings.NewReader(in)), 1<<20)
}

func TestRELPFrames(t *testing.T) {
	in := "1 open 30 relp_version=0\ncommands=syslog\n" +
		"2 syslog 9 <13>hello\n" +
		"3 close 0 \n"
	rd := relpReader(in)

	f, err := rd.ReadFrame()
	if err != nil || f.Txnr != 1 || f.Command != "open" {
		t.Fatalf("open = %+v %v", f, err)
	}
	f, err = rd.ReadFrame()
	if err != nil || f.Txnr != 2 || f.Command != "syslog" || string(f.Data) != "<13>hello" {
		t.Fatalf("syslog = %+v %v", f, err)
	}
	f, err = rd.ReadFrame()
	if err != nil || f.Txnr != 3 || f.Command != "close" || len(f.Data) != 0 {
		t.Fatalf("close = %+v %v", f, err)
	}
	if _, err := rd.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

// Emitters disagree about whether the space before an absent DATA is written.
// Accepting only one spelling would refuse half the installed base over one byte.
func TestRELPZeroLengthBothSpellings(t *testing.T) {
	for _, in := range []string{"3 close 0 \n", "3 close 0\n"} {
		f, err := relpReader(in).ReadFrame()
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if f.Txnr != 3 || f.Command != "close" || len(f.Data) != 0 {
			t.Errorf("%q -> %+v", in, f)
		}
	}
}

func TestRELPMalformed(t *testing.T) {
	for _, in := range []string{
		"x open 0 \n",     // txnr is not a number
		"1 OPEN 0 \n",     // command is not lowercase
		"1 open x \n",     // length is not a number
		"1 open 5 abc\n",  // length promises more than is there
		"1 open 5 abcde",  // no trailer
		"1 open 5 abcdeX", // wrong trailer
		"1  0 \n",         // empty command
		" 1 open 0 \n",    // empty txnr
	} {
		if _, err := relpReader(in).ReadFrame(); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestRELPFrameCeiling(t *testing.T) {
	rd := NewRELPReader(bufio.NewReader(strings.NewReader("1 syslog 999999 x")), 1024)
	if _, err := rd.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

// SPEC §5.5 applies to DATALEN too: a declared length buys the client nothing
// the bytes themselves do not.
func TestRELPFrameGrowsWithTheBytesNotTheCount(t *testing.T) {
	rd := NewRELPReader(bufio.NewReader(strings.NewReader("1 syslog 16777216 abc")), 16<<20)
	if _, err := rd.ReadFrame(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
	if cap(rd.buf) > 2*octetChunk {
		t.Fatalf("buffer capacity = %d after three bytes; it was sized from DATALEN", cap(rd.buf))
	}
}

func TestRELPLargeFrameRoundTrips(t *testing.T) {
	data := "<13>" + strings.Repeat("y", 300<<10)
	in := fmt.Sprintf("7 syslog %d %s\n8 close 0\n", len(data), data)
	rd := NewRELPReader(bufio.NewReader(strings.NewReader(in)), 16<<20)
	f, err := rd.ReadFrame()
	if err != nil || f.Txnr != 7 || string(f.Data) != data {
		t.Fatalf("large frame did not round-trip: txnr=%d len=%d err=%v", f.Txnr, len(f.Data), err)
	}
	if f, err := rd.ReadFrame(); err != nil || f.Command != "close" {
		t.Fatalf("frame after the large one: %+v %v", f, err)
	}
}

func TestAppendRSP(t *testing.T) {
	if got := string(AppendRSP(nil, 2, "200 OK")); got != "2 rsp 6 200 OK\n" {
		t.Fatalf("got %q", got)
	}
	if got := string(AppendRSP(nil, 3, "")); got != "3 rsp 0\n" {
		t.Fatalf("empty body = %q", got)
	}
}

// FuzzFrame is mandatory (SPEC §17.2). The framers read straight off the network
// on the goroutine that serves a connection: a panic here takes the connection
// and, with it, an emitter's whole log stream.
func FuzzFrame(f *testing.F) {
	seeds := []string{
		"34 <13>Sep 12 14:33:01 host app: um",
		"<13>um\n<14>dois\n",
		"0 ",
		"999999999999999999999 x",
		"12x y",
		"\n\n\n",
		"<13>oi\r\n",
		"<13>oi\x00\n",
		"1 open 0 \n2 syslog 5 <13>x\n3 close 0\n",
		"1 syslog 999999999 x",
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Every framing, including both fixed modes, over the same bytes.
		for _, tc := range []struct {
			mode envelope.Framing
			auto bool
		}{
			{0, true},
			{envelope.FramingOctet, false},
			{envelope.FramingLF, false},
		} {
			fr := NewFramer(bufio.NewReader(bytes.NewReader(data)), tc.mode, tc.auto, 4096)
			for i := 0; i < 200; i++ {
				msg, err := fr.Next()
				if err != nil {
					break
				}
				if len(msg) > 4096 {
					t.Fatalf("a message above the ceiling came back: %d bytes", len(msg))
				}
			}
		}

		rd := NewRELPReader(bufio.NewReader(bytes.NewReader(data)), 4096)
		for i := 0; i < 200; i++ {
			fr, err := rd.ReadFrame()
			if err != nil {
				break
			}
			if len(fr.Data) > 4096 {
				t.Fatalf("a RELP frame above the ceiling came back: %d bytes", len(fr.Data))
			}
			if fr.Txnr < 0 {
				t.Fatalf("negative txnr: %d", fr.Txnr)
			}
			_ = AppendRSP(nil, fr.Txnr, "200 OK") // must not panic
		}
	})
}
