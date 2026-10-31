// Package source produces the GELF documents a run sends (SPEC §5): the
// built-in generator, a --template, the lines of a --file (or stdin), or the
// documents found in a tcpdump capture (--pcap).
package source

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	mrand "math/rand/v2"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/tmpl"
)

// Message is one GELF document to send: the JSON itself, uncompressed and
// without framing. The transport compresses, chunks or frames it.
type Message struct {
	Data []byte
	// At is the capture offset (pcap only); negative without timing.
	At time.Duration
	// Src and Dst are the addresses in the capture (pcap only).
	Src, Dst netip.AddrPort
}

// Source yields messages. Next returns io.EOF when a finite source ends.
type Source interface {
	Next(seq uint64, now time.Time) (Message, error)
	// Stats returns the source's counters for the report (may be nil).
	Stats() map[string]uint64
	Close() error
}

// GenBody returns the generator's template (SPEC §5.1): a GELF 1.1 document
// whose short_message and additional fields carry the sequence number and
// the run, so a gap at the destination is a loss and a repeat a
// retransmission. host is JSON-escaped here, once.
func GenBody(host string, level int) string {
	h, _ := json.Marshal(host) // a string always marshals
	hs := strings.ReplaceAll(string(h), "{", "{{")
	return `{"version":"1.1","host":` + hs +
		`,"short_message":"seq={seq} run={run} injector=go-gelf-injector","level":` +
		strconv.Itoa(level) + `,"_seq":{seq},"_run":"{run}"}`
}

const padKey = `,"_pad":""`

const alnum = "abcdefghijklmnopqrstuvwxyz0123456789"

// PadJSON grows a JSON object to size bytes with an additional field "_pad"
// before its closing brace (SPEC §5.1). The padding is random [a-z0-9], so a
// compressed payload stays large (gzip of a run of one letter would fit any
// datagram, and chunking would never be exercised). The smallest padded
// document is len(m)+10 (an empty "_pad"): a size from len(m)+1 to
// len(m)+9 cannot be reached, and that document, like one already at or
// above size, is left alone.
func PadJSON(m []byte, size int, rnd *mrand.Rand) []byte {
	if size < len(m)+len(padKey) || len(m) < 2 || m[len(m)-1] != '}' {
		return m
	}
	n := size - len(m) - len(padKey)
	out := make([]byte, 0, size)
	out = append(out, m[:len(m)-1]...)
	out = append(out, `,"_pad":"`...)
	for n > 0 {
		v := rnd.Uint64()
		for k := 0; k < 10 && n > 0; k++ { // 36^10 < 2^64: ten digits per draw
			out = append(out, alnum[v%36])
			v /= 36
			n--
		}
	}
	out = append(out, `"}`...)
	return out
}

// Template generates documents from a compiled template (the generator is
// the template GenBody).
type Template struct {
	T    *tmpl.Template
	Run  string
	Host string
	PID  int
	// Size pads the generator's JSON (only the generator: a template is the
	// user's, and padding could break it).
	Size int
	rnd  *mrand.Rand
}

// Next implements Source. Called by the producer only: not concurrent.
func (t *Template) Next(seq uint64, now time.Time) (Message, error) {
	c := tmpl.Ctx{Seq: seq, Run: t.Run, Host: t.Host, PID: t.PID, Now: now}
	b := t.T.Expand(make([]byte, 0, 192), &c)
	if t.Size > 0 {
		if t.rnd == nil {
			t.rnd = mrand.New(mrand.NewPCG(mrand.Uint64(), mrand.Uint64()))
		}
		b = PadJSON(b, t.Size, t.rnd)
	}
	return Message{Data: b, At: -1}, nil
}

// Stats implements Source.
func (*Template) Stats() map[string]uint64 { return nil }

// Close implements Source.
func (*Template) Close() error { return nil }

// MaxLine bounds a line of --file; a longer one is cut (and counted).
const MaxLine = 16 << 20

// File sends one GELF document per line of a file, or of stdin for "-"
// (SPEC §5.3). Lines are sent as they are: a malformed one on purpose tests
// the server.
type File struct {
	path  string
	loop  bool
	f     io.ReadCloser
	r     *bufio.Reader
	lines uint64
	empty uint64
	long  uint64
	pass  uint64
	// passDocs counts the documents of the current pass. A pass that ends
	// with none ends the source even with --loop: a file of blank lines (or
	// one emptied between passes) would otherwise be reopened forever at
	// 100 % CPU (SPEC §5.3).
	passDocs uint64
}

// NewFile opens path ("-" for stdin). loop rereads the file at its end; it is
// refused for stdin, which cannot be reread.
func NewFile(path string, loop bool) (*File, error) {
	if path == "-" && loop {
		return nil, errors.New("--loop cannot reread stdin")
	}
	f := &File{path: path, loop: loop}
	if err := f.open(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) open() error {
	if f.path == "-" {
		f.f = io.NopCloser(os.Stdin)
	} else {
		fh, err := os.Open(f.path)
		if err != nil {
			return err
		}
		f.f = fh
	}
	f.r = bufio.NewReaderSize(f.f, 64<<10)
	f.pass++
	f.passDocs = 0
	return nil
}

// Next implements Source.
func (f *File) Next(_ uint64, _ time.Time) (Message, error) {
	for {
		line, err := f.readLine()
		if err == io.EOF {
			if !f.loop || f.passDocs == 0 {
				return Message{}, io.EOF
			}
			_ = f.f.Close()
			if err := f.open(); err != nil {
				return Message{}, err
			}
			continue
		}
		if err != nil {
			return Message{}, err
		}
		line = bytes.TrimRight(line, "\r")
		if len(bytes.TrimSpace(line)) == 0 {
			f.empty++
			continue
		}
		f.lines++
		f.passDocs++
		return Message{Data: line, At: -1}, nil
	}
}

func (f *File) readLine() ([]byte, error) {
	var out []byte
	for {
		chunk, err := f.r.ReadSlice('\n')
		out = append(out, chunk...)
		if err == bufio.ErrBufferFull {
			if len(out) >= MaxLine {
				f.long++
				for err == bufio.ErrBufferFull {
					_, err = f.r.ReadSlice('\n')
				}
				return out[:MaxLine], nil
			}
			continue
		}
		if err == io.EOF {
			if len(out) == 0 {
				return nil, io.EOF
			}
			return out, nil // last line without LF
		}
		if err != nil {
			return nil, err
		}
		return out[:len(out)-1], nil
	}
}

// Stats implements Source.
func (f *File) Stats() map[string]uint64 {
	return map[string]uint64{"file_lines": f.lines, "file_empty_lines": f.empty, "file_long_lines": f.long, "file_passes": f.pass}
}

// Close implements Source.
func (f *File) Close() error { return f.f.Close() }
