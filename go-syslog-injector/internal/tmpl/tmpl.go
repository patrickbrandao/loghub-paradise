// Package tmpl expands the message templates of --template (SPEC §5.2).
//
// A template is compiled once at startup, so an unknown marker is a usage
// error before anything is sent, and the hot path only appends bytes.
//
// Markers:
//
//	{seq}     the message sequence number, starting at 1
//	{run}     the run id (--run)
//	{uuid}    a random UUID v4, new for every message
//	{ts}      local time, RFC 3339 with microseconds
//	{unix}    seconds since the epoch
//	{unixms}  milliseconds since the epoch
//	{host}    the hostname (--hostname)
//	{pid}     the injector's process id
//	{rand:N}  N random characters from [a-z0-9], 1 <= N <= 65536
//
// "{{" and "}}" are a literal brace. A lone "}" is a literal too; a "{" that
// does not open a known marker is an error, so a typo never ships as text.
package tmpl

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Ctx carries the per-message and per-run values the markers read.
type Ctx struct {
	Seq  uint64
	Run  string
	Host string
	PID  int
	Now  time.Time
}

type kind uint8

const (
	kLit kind = iota
	kSeq
	kRun
	kUUID
	kTS
	kUnix
	kUnixMS
	kHost
	kPID
	kRand
)

type part struct {
	k   kind
	lit string
	n   int
}

// Template is a compiled template. Expand is safe for concurrent use.
type Template struct {
	parts []part
	src   string
}

// MaxRand is the largest N accepted by {rand:N}.
const MaxRand = 65536

// Compile parses s. The error names the offending marker and its offset.
func Compile(s string) (*Template, error) {
	t := &Template{src: s}
	var lit strings.Builder
	flush := func() {
		if lit.Len() > 0 {
			t.parts = append(t.parts, part{k: kLit, lit: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '{' && i+1 < len(s) && s[i+1] == '{':
			lit.WriteByte('{')
			i += 2
		case c == '}' && i+1 < len(s) && s[i+1] == '}':
			lit.WriteByte('}')
			i += 2
		case c == '{':
			end := strings.IndexByte(s[i:], '}')
			if end < 0 {
				return nil, fmt.Errorf("template: unclosed '{' at offset %d", i)
			}
			name := s[i+1 : i+end]
			p, err := marker(name)
			if err != nil {
				return nil, fmt.Errorf("template: %w at offset %d", err, i)
			}
			flush()
			t.parts = append(t.parts, p)
			i += end + 1
		default:
			lit.WriteByte(c)
			i++
		}
	}
	flush()
	return t, nil
}

func marker(name string) (part, error) {
	switch name {
	case "seq":
		return part{k: kSeq}, nil
	case "run":
		return part{k: kRun}, nil
	case "uuid":
		return part{k: kUUID}, nil
	case "ts":
		return part{k: kTS}, nil
	case "unix":
		return part{k: kUnix}, nil
	case "unixms":
		return part{k: kUnixMS}, nil
	case "host":
		return part{k: kHost}, nil
	case "pid":
		return part{k: kPID}, nil
	}
	if n, ok := strings.CutPrefix(name, "rand:"); ok {
		v, err := strconv.Atoi(n)
		if err != nil || v < 1 || v > MaxRand {
			return part{}, fmt.Errorf("{rand:N} needs 1 <= N <= %d, got %q", MaxRand, n)
		}
		return part{k: kRand, n: v}, nil
	}
	return part{}, fmt.Errorf("unknown marker {%s}", name)
}

// Static reports whether the template has no marker at all.
func (t *Template) Static() bool {
	return len(t.parts) == 0 || (len(t.parts) == 1 && t.parts[0].k == kLit)
}

// String returns the source text.
func (t *Template) String() string { return t.src }

// TSLayout is the layout of {ts}.
const TSLayout = "2006-01-02T15:04:05.000000Z07:00"

const alnum = "abcdefghijklmnopqrstuvwxyz0123456789"

// Expand appends the expansion of t to dst.
func (t *Template) Expand(dst []byte, c *Ctx) []byte {
	for _, p := range t.parts {
		switch p.k {
		case kLit:
			dst = append(dst, p.lit...)
		case kSeq:
			dst = strconv.AppendUint(dst, c.Seq, 10)
		case kRun:
			dst = append(dst, c.Run...)
		case kUUID:
			dst = AppendUUID(dst)
		case kTS:
			dst = c.Now.AppendFormat(dst, TSLayout)
		case kUnix:
			dst = strconv.AppendInt(dst, c.Now.Unix(), 10)
		case kUnixMS:
			dst = strconv.AppendInt(dst, c.Now.UnixMilli(), 10)
		case kHost:
			dst = append(dst, c.Host...)
		case kPID:
			dst = strconv.AppendInt(dst, int64(c.PID), 10)
		case kRand:
			for range p.n {
				dst = append(dst, alnum[mrand.IntN(len(alnum))])
			}
		}
	}
	return dst
}

// AppendUUID appends a random RFC 9562 version 4 UUID.
func AppendUUID(dst []byte) []byte {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (Go 1.24+)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return append(dst, s[:]...)
}

// NewRunID returns 8 random hex characters.
func NewRunID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ErrEmpty is returned by callers that refuse an empty template.
var ErrEmpty = errors.New("template is empty")
