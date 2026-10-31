// Package source produces the messages a run sends (SPEC §5): the built-in
// generator, a --template, the lines of a --file (or stdin), or the syslog
// messages found in a tcpdump capture (--pcap).
package source

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strconv"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/extract"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/tmpl"
)

// Message is one message to send. Data is the syslog message without
// transport framing; the sender frames it.
type Message struct {
	Data []byte
	// At is the capture offset of the message (pcap only), used by
	// --pcap-timing original; negative when the source has no timing.
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

// Format is how the syslog header is built around a message body (SPEC §5.1).
type Format uint8

const (
	FormatRaw Format = iota // the body is the whole message
	Format5424
	Format3164
)

// ParseFormat parses --format.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "raw":
		return FormatRaw, nil
	case "5424", "rfc5424":
		return Format5424, nil
	case "3164", "rfc3164", "bsd":
		return Format3164, nil
	}
	return 0, fmt.Errorf("--format: want 5424, 3164 or raw, got %q", s)
}

// Header holds the fields of the generated header.
type Header struct {
	Format   Format
	PRI      int
	Hostname string
	App      string
	PID      int
	MsgID    string
}

// RFC3164Layout is the BSD timestamp: no year, no zone, day space-padded.
const RFC3164Layout = "Jan _2 15:04:05"

// Wrap appends the header and the body to dst.
func (h *Header) Wrap(dst, body []byte, now time.Time) []byte {
	switch h.Format {
	case Format5424:
		dst = append(dst, '<')
		dst = strconv.AppendInt(dst, int64(h.PRI), 10)
		dst = append(dst, ">1 "...)
		dst = now.AppendFormat(dst, tmpl.TSLayout)
		dst = append(dst, ' ')
		dst = appendNil(dst, h.Hostname)
		dst = append(dst, ' ')
		dst = appendNil(dst, h.App)
		dst = append(dst, ' ')
		dst = strconv.AppendInt(dst, int64(h.PID), 10)
		dst = append(dst, ' ')
		dst = appendNil(dst, h.MsgID)
		dst = append(dst, " - "...)
	case Format3164:
		dst = append(dst, '<')
		dst = strconv.AppendInt(dst, int64(h.PRI), 10)
		dst = append(dst, '>')
		dst = now.AppendFormat(dst, RFC3164Layout)
		dst = append(dst, ' ')
		dst = append(dst, h.Hostname...)
		dst = append(dst, ' ')
		dst = append(dst, h.App...)
		dst = append(dst, '[')
		dst = strconv.AppendInt(dst, int64(h.PID), 10)
		dst = append(dst, "]: "...)
	}
	return append(dst, body...)
}

func appendNil(dst []byte, s string) []byte {
	if s == "" {
		return append(dst, '-')
	}
	return append(dst, s...)
}

// Pad grows m to size bytes with 'x' after a space (SPEC §5.1). A message
// already at or above size is left alone.
func Pad(m []byte, size int) []byte {
	if size <= len(m) {
		return m
	}
	if len(m) > 0 && size-len(m) > 1 {
		m = append(m, ' ')
	}
	for len(m) < size {
		m = append(m, 'x')
	}
	return m
}

// GenBody is the body of the built-in generator: every message carries its
// sequence number and the run, so a gap at the destination is a loss and a
// repeat is a retransmission.
const GenBody = "seq={seq} run={run} injector=go-syslog-injector"

// Template generates messages from a compiled template (the generator is the
// template GenBody).
type Template struct {
	T    *tmpl.Template
	H    Header
	Run  string
	Size int
}

// Next implements Source.
func (t *Template) Next(seq uint64, now time.Time) (Message, error) {
	c := tmpl.Ctx{Seq: seq, Run: t.Run, Host: t.H.Hostname, PID: t.H.PID, Now: now}
	body := t.T.Expand(make([]byte, 0, 128), &c)
	m := t.H.Wrap(make([]byte, 0, len(body)+96), body, now)
	return Message{Data: Pad(m, t.Size), At: -1}, nil
}

// Stats implements Source.
func (*Template) Stats() map[string]uint64 { return nil }

// Close implements Source.
func (*Template) Close() error { return nil }

// File sends one message per line of a file, or of stdin for "-" (SPEC §5.3).
type File struct {
	path  string
	loop  bool
	h     Header
	f     io.ReadCloser
	r     *bufio.Reader
	lines uint64
	empty uint64
	long  uint64
	pass  uint64
}

// MaxLine bounds a line of --file; a longer one is cut (and counted).
const MaxLine = extract.MaxFrame

// NewFile opens path ("-" for stdin). loop rereads the file at its end; it is
// refused for stdin, which cannot be reread.
func NewFile(path string, loop bool, h Header) (*File, error) {
	if path == "-" && loop {
		return nil, errors.New("--loop cannot reread stdin")
	}
	f := &File{path: path, loop: loop, h: h}
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
	return nil
}

// Next implements Source.
func (f *File) Next(_ uint64, now time.Time) (Message, error) {
	emptyPass := true
	for {
		line, err := f.readLine()
		if err == io.EOF {
			if !f.loop || emptyPass && f.lines == 0 {
				return Message{}, io.EOF
			}
			_ = f.f.Close()
			if err := f.open(); err != nil {
				return Message{}, err
			}
			emptyPass = true
			continue
		}
		if err != nil {
			return Message{}, err
		}
		emptyPass = false
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			f.empty++
			continue
		}
		f.lines++
		m := f.h.Wrap(make([]byte, 0, len(line)+96), line, now)
		return Message{Data: m, At: -1}, nil
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
				// Discard the rest of the line.
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
