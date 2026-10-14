package receiver

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

func reader(input string, maxFrame int64, maxMsg int) *RELPReader {
	return NewRELPReader(bufio.NewReaderSize(strings.NewReader(input), 16), maxFrame, maxMsg)
}

type readFrame struct {
	txnr int64
	cmd  obs.RELPCommand
	data string
	size int
}

func readAll(rd *RELPReader) ([]readFrame, error) {
	var out []readFrame
	for {
		f, err := rd.ReadFrame()
		if err != nil {
			return out, err
		}
		out = append(out, readFrame{f.Txnr, f.Command, string(f.Data), f.Size})
	}
}

func TestRELPFrames(t *testing.T) {
	const msg = "<13>Sep 12 14:33:01 web-01 app: hello"
	in := "1 open 3 abc\n2 syslog " + strconv.Itoa(len(msg)) + " " + msg + "\n3 close 0\n"
	got, err := readAll(reader(in, 1<<20, 64<<10))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want a clean EOF", err)
	}
	want := []readFrame{
		{1, obs.RELPOpen, "abc", 3},
		{2, obs.RELPSyslog, msg, len(msg)},
		{3, obs.RELPClose, "", 0},
	}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("frame %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// Emitters disagree about the space before an absent DATA (SPEC §5.1): both
// spellings are read, and the stream stays in sync after each.
func TestRELPZeroLengthBothSpellings(t *testing.T) {
	got, err := readAll(reader("3 close 0\n4 close 0 \n5 syslog 1 x\n", 1<<20, 64<<10))
	if !errors.Is(err, io.EOF) || len(got) != 3 || got[2].data != "x" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// None of the header, nor the trailer, is the message (SPEC §5.1): DATA alone,
// a LF inside it included.
func TestRELPHeaderAndTrailerAreNotTheMessage(t *testing.T) {
	got, err := readAll(reader("7 syslog 5 a\nb\nc\n", 1<<20, 64<<10))
	if !errors.Is(err, io.EOF) || len(got) != 1 || got[0].data != "a\nb\nc" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestRELPMalformed(t *testing.T) {
	for _, in := range []string{
		"x syslog 1 a\n",          // txnr not a number
		" syslog 1 a\n",           // empty txnr
		"1234567890 syslog 1 a\n", // ten digits: no RELP txnr
		"1  1 a\n",                // empty command
		"1 sys1og 1 a\n",          // digit in the command
		"1 " + strings.Repeat("a", 33) + " 1 a\n",
		"1 syslog x a\n",          // DATALEN not a number
		"1 syslog  a\n",           // empty DATALEN
		"1 syslog 1234567890 a\n", // ten digits
		"1 syslog 1 ab\n",         // no trailer where DATALEN ends
		"1 syslog 0 x\n",          // zero length with data
		"1 syslog 3\nabc\n",       // DATA after LF instead of SP
	} {
		_, err := readAll(reader(in, 1<<30, 64<<10))
		if !errors.Is(err, ErrRELPFrame) {
			t.Errorf("%q: err = %v, want ErrRELPFrame", in, err)
		}
	}
}

// The fuse is checked on the DECLARED length, before a byte of DATA is read.
func TestRELPFrameCeiling(t *testing.T) {
	rd := reader("1 syslog 1025 ", 1024, 512) // no DATA at all behind it
	if _, err := rd.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if got, err := readAll(reader("1 syslog 1024 "+strings.Repeat("a", 1024)+"\n", 1024, 2048)); !errors.Is(err, io.EOF) ||
		len(got) != 1 || len(got[0].data) != 1024 {
		t.Fatalf("a frame AT the ceiling: %d frames, %v", len(got), err)
	}
}

// Between syslog.max_message_bytes and the fuse the message is cut, the rest is
// consumed without being stored, and the next frame is read in sync (SPEC §5.3).
func TestRELPTruncationKeepsSync(t *testing.T) {
	long := strings.Repeat("x", 3000)
	got, err := readAll(reader("1 syslog 3000 "+long+"\n2 syslog 2 ok\n", 1<<20, 480))
	if !errors.Is(err, io.EOF) || len(got) != 2 {
		t.Fatalf("got %d frames, %v", len(got), err)
	}
	if len(got[0].data) != 480 || got[0].size != 3000 || got[0].data != long[:480] {
		t.Fatalf("truncated frame: %d bytes kept, size %d", len(got[0].data), got[0].size)
	}
	if got[1].data != "ok" || got[1].txnr != 2 {
		t.Fatalf("the next frame was lost: %+v", got[1])
	}
	f := RELPFrame{Data: make([]byte, 480), Size: 3000}
	if !f.Truncated() {
		t.Fatal("Truncated() false for a cut frame")
	}
}

// Grown with the bytes, never from the count: a peer that declares a large
// DATALEN and sends little costs one small block (the bug of 2026-09-14).
func TestRELPFrameGrowsWithTheBytesNotTheCount(t *testing.T) {
	rd := NewRELPReader(bufio.NewReader(strings.NewReader("1 syslog 60000 abc")), 1<<20, 64<<10)
	_, err := rd.ReadFrame()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want ErrUnexpectedEOF", err)
	}
	if cap(rd.buf) > relpFirstBlock {
		t.Fatalf("buffer grew to %d for 3 bytes that arrived", cap(rd.buf))
	}
}

func TestRELPLargeFrameRoundTrips(t *testing.T) {
	big := strings.Repeat("0123456789", 6553) // 65530 bytes
	got, err := readAll(reader("9 syslog "+strconv.Itoa(len(big))+" "+big+"\n", 1<<20, 64<<10))
	if !errors.Is(err, io.EOF) || len(got) != 1 || got[0].data != big {
		t.Fatalf("round trip failed: %v", err)
	}
}

// A connection that ends inside a frame delivers nothing of it: the frame was
// never answered, and librelp retransmits it.
func TestRELPMidFrameCloseIsUnexpectedEOF(t *testing.T) {
	for _, in := range []string{"1", "1 sys", "1 syslog", "1 syslog 5", "1 syslog 5 ab", "1 syslog 5 abcde"} {
		_, err := readAll(reader(in, 1<<20, 64<<10))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%q: err = %v, want ErrUnexpectedEOF", in, err)
		}
	}
	if _, err := readAll(reader("", 1<<20, 64<<10)); !errors.Is(err, io.EOF) {
		t.Errorf("empty stream: err = %v, want io.EOF", err)
	}
}

// The command label is a closed set: the client's word never reaches a label.
func TestRELPUnknownAndUppercaseCommandsAreUnknown(t *testing.T) {
	got, err := readAll(reader("1 starttls 0\n2 SYSLOG 1 a\n", 1<<20, 64<<10))
	if !errors.Is(err, io.EOF) || len(got) != 2 || got[0].cmd != obs.RELPUnknown || got[1].cmd != obs.RELPUnknown {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// After a large frame the reader does not keep the large buffer once the
// sender went quiet: an idle connection must not hold 64 KiB for the rest of
// its life (SPEC §15.1). "Quiet" is nothing of the next frame buffered: each
// frame here arrives in a read of its own.
func TestRELPReaderReleasesALargeBuffer(t *testing.T) {
	big := "1 syslog 60000 " + strings.Repeat("a", 60000) + "\n"
	rd := NewRELPReader(bufio.NewReaderSize(&chunkReader{chunks: []string{big, "2 syslog 1 b\n"}}, 16), 1<<20, 64<<10)
	if _, err := rd.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if _, err := rd.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if cap(rd.buf) > relpKeepBlock {
		t.Fatalf("buffer of %d kept after a small frame", cap(rd.buf))
	}
}

// A sender still sending large frames keeps the buffer: let go before every
// frame, each 24 KiB line paid the whole doubling again (independent review
// of go-syslog-server, 2026-10-13, E2).
func TestRELPReaderKeepsTheBufferWhileFramesFollow(t *testing.T) {
	frame := func(n int) string { return strconv.Itoa(n) + " syslog 24576 " + strings.Repeat("a", 24576) + "\n" }
	rd := NewRELPReader(bufio.NewReaderSize(strings.NewReader(frame(1)+frame(2)+frame(3)), relpReadBuffer), 1<<20, 64<<10)
	if _, err := rd.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	grown := &rd.buf[:1][0]
	for i := 0; i < 2; i++ {
		if _, err := rd.ReadFrame(); err != nil {
			t.Fatal(err)
		}
		if &rd.buf[:1][0] != grown {
			t.Fatalf("frame %d: the buffer was let go with the next frame already buffered", i+2)
		}
	}
}

// chunkReader returns one chunk per Read: a sender that pauses between them.
type chunkReader struct{ chunks []string }

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[0])
	if c.chunks[0] = c.chunks[0][n:]; c.chunks[0] == "" {
		c.chunks = c.chunks[1:]
	}
	return n, nil
}

func TestAppendRSPAndServerClose(t *testing.T) {
	for _, c := range []struct {
		txnr int64
		body string
		want string
	}{
		{2, "200 OK", "2 rsp 6 200 OK\n"},
		{3, "", "3 rsp 0\n"},
		{9, "500 refused by the destination", "9 rsp 30 500 refused by the destination\n"},
	} {
		if got := string(AppendRSP(nil, c.txnr, c.body)); got != c.want {
			t.Errorf("AppendRSP(%d, %q) = %q, want %q", c.txnr, c.body, got, c.want)
		}
	}
	// librelp's client refuses anything between a zero DATALEN and the
	// trailer (relpframe.c, do_trailer): the reference's "0 serverclose 0 \n"
	// was an invalid frame to it.
	f, err := readServerFrame(bufio.NewReader(strings.NewReader(serverClose)))
	if err != nil || f.cmd != "serverclose" || f.txnr != 0 {
		t.Fatalf("serverclose %q: %v, %v", serverClose, f, err)
	}
	// Every response body fits librelp's 80-character text after the code.
	for _, b := range []string{rspUnaddressable, rspDisabled, rspAlreadyOpen, rspNotSupported} {
		if len(b) > 84 || b[3] != ' ' {
			t.Errorf("%q: librelp reads three digits, a space and up to 80 characters", b)
		}
	}
}

// The open response carries the offers librelp's client checks, in its own
// shape: no trailing LF, and no byte in relp_software that would start a new
// offer.
func TestOpenResponseOffers(t *testing.T) {
	got := openResponse("go-relplog-server,v1.2\n3 x")
	want := "200 OK\nrelp_version=0\nrelp_software=go-relplog-server,v1.2_3_x\ncommands=syslog"
	if got != want {
		t.Fatalf("openResponse = %q, want %q", got, want)
	}
}

// ------------------------------------------------------------------- fuzz

// refFrame is the oracle's view of one frame.
type refFrame struct {
	txnr int64
	cmd  string
	data []byte
}

// refParse is an independent reading of the RELP grammar: whole-buffer, no
// state machine. It returns the frames up to the first that does not parse,
// and whether the input ended cleanly between frames.
func refParse(in []byte) ([]refFrame, bool) {
	var out []refFrame
	for len(in) > 0 {
		sp := bytes.IndexByte(in, ' ')
		if sp < 1 || sp > relpMaxTxnrDigits || !allDigits(in[:sp]) {
			return out, false
		}
		txnr, _ := strconv.ParseInt(string(in[:sp]), 10, 64)
		in = in[sp+1:]
		sp = bytes.IndexByte(in, ' ')
		if sp < 1 || sp > relpMaxCommandBytes || !allAlpha(in[:sp]) {
			return out, false
		}
		cmd := string(in[:sp])
		in = in[sp+1:]
		end := 0
		for end < len(in) && in[end] >= '0' && in[end] <= '9' {
			end++
		}
		if end == 0 || end > relpMaxLenDigits || end == len(in) {
			return out, false
		}
		n, _ := strconv.Atoi(string(in[:end]))
		sep := in[end]
		in = in[end+1:]
		switch {
		case n == 0 && sep == '\n':
		case n == 0 && sep == ' ':
			if len(in) == 0 || in[0] != '\n' {
				return out, false
			}
			in = in[1:]
		case n > 0 && sep == ' ':
			if len(in) < n+1 || in[n] != '\n' {
				return out, false
			}
			out = append(out, refFrame{txnr, cmd, in[:n]})
			in = in[n+1:]
			continue
		default:
			return out, false
		}
		out = append(out, refFrame{txnr, cmd, nil})
	}
	return out, true
}

func allDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func allAlpha(b []byte) bool {
	for _, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

// FuzzFrame checks the reader against the oracle on arbitrary input: the same
// frames, the same data cut at the message limit, the same DATALEN, and an
// error exactly where the oracle stops. It never panics and never keeps more
// than the limit.
func FuzzFrame(f *testing.F) {
	for _, s := range []string{
		"1 open 3 abc\n2 syslog 5 hello\n3 close 0\n",
		"3 close 0 \n4 syslog 1 x\n",
		"1 syslog 600 " + strings.Repeat("y", 600) + "\n",
		"1 syslog 5 a\nb\nc\n",
		"9 syslog 3\nabc\n",
		"1 x 0\n",
		"12345 rsp 6 200 OK\n",
	} {
		f.Add([]byte(s))
	}
	const maxMsg = 512
	f.Fuzz(func(t *testing.T, in []byte) {
		want, clean := refParse(in)
		rd := NewRELPReader(bufio.NewReaderSize(bytes.NewReader(in), 16), 1<<20, maxMsg)
		for i := 0; ; i++ {
			fr, err := rd.ReadFrame()
			if err != nil {
				if i != len(want) {
					t.Fatalf("reader stopped at frame %d with %v, oracle has %d", i, err, len(want))
				}
				if clean != errors.Is(err, io.EOF) {
					t.Fatalf("clean end = %v but err = %v", clean, err)
				}
				return
			}
			if i >= len(want) {
				t.Fatalf("reader produced frame %d beyond the oracle's %d", i, len(want))
			}
			w := want[i]
			keep := w.data
			if len(keep) > maxMsg {
				keep = keep[:maxMsg]
			}
			if fr.Txnr != w.txnr || !bytes.Equal(fr.Data, keep) || fr.Size != len(w.data) ||
				fr.Command != commandOf([]byte(w.cmd)) {
				t.Fatalf("frame %d = {%d %v %q %d}, oracle {%d %s %q %d}", i,
					fr.Txnr, fr.Command, fr.Data, fr.Size, w.txnr, w.cmd, keep, len(w.data))
			}
			if len(fr.Data) > maxMsg {
				t.Fatalf("kept %d bytes past the limit", len(fr.Data))
			}
		}
	})
}
