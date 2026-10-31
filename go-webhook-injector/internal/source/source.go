// Package source produces the requests a run sends (SPEC §5): bodies from the
// built-in generator, a --template, the lines of a --file (or stdin) or the
// files of a directory, or whole requests found in a tcpdump capture (--pcap).
package source

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/httpx"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/tmpl"
)

// Message is one request to send. Body is always set; Req is set only by the
// pcap source, and carries the captured method, target and headers.
type Message struct {
	Body []byte
	Req  *httpx.Request
	// At is the capture offset (pcap only); negative without timing.
	At time.Duration
	// Src and Dst are the addresses in the capture (pcap only).
	Src, Dst netip.AddrPort
}

// Source yields messages. Next returns io.EOF when a finite source ends.
type Source interface {
	Next(seq uint64, now time.Time) (Message, error)
	Stats() map[string]uint64
	Close() error
}

// GenBody is the body of the built-in generator: a JSON object with the
// sequence number and the run, so a gap at the destination is a loss and a
// repeat is a retransmission.
const GenBody = `{"seq":{seq},"run":"{run}","injector":"go-webhook-injector","ts":"{ts}"}`

// PadJSON grows a JSON object to size bytes with a "pad" member before its
// closing brace (SPEC §5.1), keeping it valid JSON.
func PadJSON(m []byte, size int) []byte {
	const key = `,"pad":""`
	if size <= len(m)+len(key) || len(m) < 2 || m[len(m)-1] != '}' {
		return m
	}
	n := size - len(m) - len(key)
	out := make([]byte, 0, size)
	out = append(out, m[:len(m)-1]...)
	out = append(out, `,"pad":"`...)
	out = append(out, bytes.Repeat([]byte{'x'}, n)...)
	out = append(out, `"}`...)
	return out
}

// Template generates bodies from a compiled template.
type Template struct {
	T    *tmpl.Template
	Run  string
	Host string
	PID  int
	// Size pads the generator's JSON (only the generator: a template's body
	// is the user's, and padding could break it).
	Size int
}

// Next implements Source.
func (t *Template) Next(seq uint64, now time.Time) (Message, error) {
	c := tmpl.Ctx{Seq: seq, Run: t.Run, Host: t.Host, PID: t.PID, Now: now}
	b := t.T.Expand(make([]byte, 0, 128), &c)
	if t.Size > 0 {
		b = PadJSON(b, t.Size)
	}
	return Message{Body: b, At: -1}, nil
}

// Stats implements Source.
func (*Template) Stats() map[string]uint64 { return nil }

// Close implements Source.
func (*Template) Close() error { return nil }

// MaxBody bounds a body read from a file or a line.
const MaxBody = 64 << 20

// File sends one body per line of a file or stdin, or one body per regular
// file of a directory (sorted by name) (SPEC §5.3).
type File struct {
	path  string
	loop  bool
	dir   []string // files of a directory; nil for a line file
	di    int
	f     io.ReadCloser
	r     *bufio.Reader
	lines uint64
	empty uint64
	long  uint64
	pass  uint64
	// passDocs counts the documents of the current pass. A pass that ends
	// with none ends the source even with --loop: a file of blank lines (or
	// one emptied between passes) would otherwise be reopened forever at
	// 100 % CPU.
	passDocs uint64
}

// NewFile opens path ("-" for stdin).
func NewFile(path string, loop bool) (*File, error) {
	if path == "-" && loop {
		return nil, errors.New("--loop cannot reread stdin")
	}
	f := &File{path: path, loop: loop}
	if path != "-" {
		if fi, err := os.Stat(path); err != nil {
			return nil, err
		} else if fi.IsDir() {
			ents, err := os.ReadDir(path)
			if err != nil {
				return nil, err
			}
			for _, e := range ents {
				if e.Type().IsRegular() {
					f.dir = append(f.dir, filepath.Join(path, e.Name()))
				}
			}
			sort.Strings(f.dir)
			if len(f.dir) == 0 {
				return nil, errors.New("--file: no regular file in " + path)
			}
			f.pass = 1
			return f, nil
		}
	}
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
	if f.dir != nil {
		return f.nextFile()
	}
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
		if len(line) == 0 {
			f.empty++
			continue
		}
		f.lines++
		f.passDocs++
		return Message{Body: line, At: -1}, nil
	}
}

func (f *File) nextFile() (Message, error) {
	if f.di == len(f.dir) {
		if !f.loop {
			return Message{}, io.EOF
		}
		f.di = 0
		f.pass++
	}
	p := f.dir[f.di]
	f.di++
	fh, err := os.Open(p)
	if err != nil {
		return Message{}, err
	}
	defer fh.Close()
	b, err := io.ReadAll(io.LimitReader(fh, MaxBody+1))
	if err != nil {
		return Message{}, err
	}
	if len(b) > MaxBody {
		f.long++
		b = b[:MaxBody]
	}
	f.lines++
	return Message{Body: b, At: -1}, nil
}

func (f *File) readLine() ([]byte, error) {
	var out []byte
	for {
		chunk, err := f.r.ReadSlice('\n')
		out = append(out, chunk...)
		if err == bufio.ErrBufferFull {
			if len(out) >= MaxBody {
				f.long++
				for err == bufio.ErrBufferFull {
					_, err = f.r.ReadSlice('\n')
				}
				return out[:MaxBody], nil
			}
			continue
		}
		if err == io.EOF {
			if len(out) == 0 {
				return nil, io.EOF
			}
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		return out[:len(out)-1], nil
	}
}

// Stats implements Source.
func (f *File) Stats() map[string]uint64 {
	return map[string]uint64{"file_bodies": f.lines, "file_empty_lines": f.empty, "file_long_bodies": f.long, "file_passes": f.pass}
}

// Close implements Source.
func (f *File) Close() error {
	if f.f != nil {
		return f.f.Close()
	}
	return nil
}
