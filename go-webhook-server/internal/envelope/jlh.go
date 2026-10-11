// Package envelope builds the LogHub JLH envelope of SPEC §3: one
// type=webhook.request envelope per accepted request, written ONCE and then
// carried byte for byte by the ring, the emergency dump and every sink.
//
// The writer is hand-written rather than encoding/json, on purpose:
//
//   - It runs once per request on the hottest path of the process, and a
//     reflective encoder would allocate per field and sort the headers map.
//   - The nine fields go in the canonical order the JLH contract fixes, and the
//     bytes are reproducible: a retry, a reingested dump and a ring row resend
//     the SAME bytes for the same uuid.
//   - The string escape is the one the v1 envelope always had (SPEC §12.1):
//     invalid UTF-8 becomes U+FFFD, U+2028/U+2029 are escaped, <, > and & are
//     not.
package envelope

import (
	crand "crypto/rand"
	"strconv"
	"time"
	"unicode/utf8"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

// Type and Level are this server's: an HTTP request carried whole, by the
// bridge-in that received it (SPEC §3.1).
const (
	Type  = "webhook.request"
	Level = 9
)

// MaxSID is the largest sid the JLH schema accepts: 2^53-1, the ceiling that
// keeps the value exact in every JSON parser, JavaScript's included.
const MaxSID = 1<<53 - 1

// MaxStep is the JLH hop limit, which is also the schema ceiling.
const MaxStep = 32

// isoLayout truncates to milliseconds, which is what the JLH contract asks of
// event.iso: Go's .000 never rounds.
const isoLayout = "2006-01-02T15:04:05.000Z"

// gen is the one generator of the process (go-loghub-uuidv7 rule 13). Its
// entropy is the runtime's per-thread ChaCha8, seeded by the OS: no shared lock
// and no syscall per id, which is what SPEC §14.3 measured crypto/rand against.
// The uuid is not a secret -- it exposes its own creation instant -- and nothing
// in this server derives a capability from it.
var gen = uuidv7.NewGenerator()

// init fails loudly when the platform has no entropy at all. The generator
// itself never returns an error, so this is the only place a broken entropy
// source can surface before ids are minted.
func init() {
	var b [1]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("envelope: system entropy source unavailable: " + err.Error())
	}
}

// NewUUID returns a UUIDv7 at precision level 3 for the instant t.
//
// t must be the SAME clock sample that fills the envelope's event (SPEC §3.1):
// one sample, two views of it. This is the debt of the v1 envelope paid off --
// its id came from a second clock read inside the old library and disagreed
// with ts/uts/nts on ~12% of events.
func NewUUID(t time.Time) uuidv7.UUID {
	return gen.GenerateAt(uuidv7.Level3, t)
}

// Event is the instant of the fact: Unix seconds plus nanoseconds within the
// second. iso is derived, never stored, so it cannot disagree with the two
// numbers that are its source of truth.
type Event struct {
	UTS int64
	Sub int64
}

// EventAt splits one instant into the event fields.
func EventAt(t time.Time) Event {
	return Event{UTS: t.Unix(), Sub: int64(t.Nanosecond())}
}

// Time is the instant back as a time.Time, in UTC.
func (e Event) Time() time.Time { return time.Unix(e.UTS, e.Sub).UTC() }

// AppendISO appends yyyy-MM-ddTHH:mm:ss.SSSZ, truncated to the millisecond.
func (e Event) AppendISO(dst []byte) []byte {
	return e.Time().AppendFormat(dst, isoLayout)
}

// Header holds the eight rigid fields of an envelope.
type Header struct {
	UUID  uuidv7.UUID
	From  string
	Space string
	Type  string
	Level uint8
	SID   uint64
	Step  uint8
	Event Event
}

// AppendHeader writes `{"uuid":...,"event":{...},"payload":` and leaves the
// envelope open for the payload object. The caller appends the payload and then
// calls Close.
//
// From, Space and Type are written without escaping: they come from the
// identity library and from constants, and from and space are validated at
// boot (SPEC §3.5).
func AppendHeader(dst []byte, h *Header) []byte {
	dst = append(dst, `{"uuid":"`...)
	dst = h.UUID.AppendTo(dst)
	dst = append(dst, `","from":"`...)
	dst = append(dst, h.From...)
	dst = append(dst, `","space":"`...)
	dst = append(dst, h.Space...)
	dst = append(dst, `","type":"`...)
	dst = append(dst, h.Type...)
	dst = append(dst, `","level":`...)
	dst = strconv.AppendUint(dst, uint64(h.Level), 10)
	dst = append(dst, `,"sid":`...)
	dst = strconv.AppendUint(dst, h.SID, 10)
	dst = append(dst, `,"step":`...)
	dst = strconv.AppendUint(dst, uint64(h.Step), 10)
	dst = append(dst, `,"event":{"uts":`...)
	dst = strconv.AppendInt(dst, h.Event.UTS, 10)
	dst = append(dst, `,"sub":`...)
	dst = strconv.AppendInt(dst, h.Event.Sub, 10)
	dst = append(dst, `,"iso":"`...)
	dst = h.Event.AppendISO(dst)
	dst = append(dst, `"},"payload":`...)
	return dst
}

// Close ends an envelope opened by AppendHeader.
func Close(dst []byte) []byte { return append(dst, '}') }

const hexDigits = "0123456789abcdef"

// AppendString appends s as a JSON string literal.
//
// Header values, paths and query values are arbitrary bytes from the network,
// so this must handle every one of them. Invalid UTF-8 becomes U+FFFD, because
// a JSON string must be valid UTF-8 and a raw byte would make the whole
// envelope unparseable for every consumer -- one U+FFFD per maximal invalid
// subpart (invalidRun), not per byte as encoding/json does. U+2028 and U+2029 are
// valid JSON but terminate a line in JavaScript source, and admin tooling is
// JavaScript, so they are escaped. escapedLen must agree with this function
// byte for byte.
func AppendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	dst = appendEscaped(dst, s)
	return append(dst, '"')
}

// appendEscaped is AppendString without the quotes.
func appendEscaped(dst []byte, s string) []byte {
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			dst = appendEscape(dst, c)
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, string(utf8.RuneError)...)
			i += invalidRun(s[i:])
			start = i
			continue
		}
		if r == 0x2028 || r == 0x2029 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	return append(dst, s[start:]...)
}

// AppendBytes is AppendString for a byte slice, without converting it: the
// request body goes through here, and string(body) would copy it.
func AppendBytes(dst []byte, s []byte) []byte {
	dst = append(dst, '"')
	dst = appendEscapedBytes(dst, s)
	return append(dst, '"')
}

func appendEscapedBytes(dst []byte, s []byte) []byte {
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			dst = appendEscape(dst, c)
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRune(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, string(utf8.RuneError)...)
			i += invalidRun(s[i:])
			start = i
			continue
		}
		if r == 0x2028 || r == 0x2029 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	return append(dst, s[start:]...)
}

func appendEscape(dst []byte, c byte) []byte {
	switch c {
	case '"', '\\':
		return append(dst, '\\', c)
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\t':
		return append(dst, '\\', 't')
	}
	return append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
}

// escapedLen is the length AppendBytes would write for s between the quotes,
// and whether s is text the content may carry as utf-8 (SPEC §3.2): valid
// UTF-8 without NUL.
func escapedLen(s []byte) (n int, text bool) {
	text = true
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == 0:
				text = false
				n += 6
			case c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t':
				n += 2
			case c < 0x20:
				n += 6
			default:
				n++
			}
			i++
			continue
		}
		r, size := utf8.DecodeRune(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			text = false
			n += 3
			size = invalidRun(s[i:])
		case r == 0x2028 || r == 0x2029:
			n += 6
		default:
			n += size
		}
		i += size
	}
	return n, text
}

// invalidRun is the length of the invalid UTF-8 sequence at the start of s,
// where utf8.DecodeRune reported RuneError of size 1: the maximal subpart of
// Unicode §3.9 -- a lead byte and the continuation bytes that could still have
// completed it -- or a single byte. Each run becomes ONE U+FFFD, which is what
// Python's bytes.decode('utf-8', 'replace') writes. The skill's validate.py
// compares payload.query with parse_qsl of the url (SPEC §3.2), so "%E2%80"
// must be one U+FFFD here too; one per byte, as encoding/json writes, made
// that envelope fail the skill.
func invalidRun[T string | []byte](s T) int {
	lo, hi := byte(0x80), byte(0xBF)
	need := 0
	switch c := s[0]; {
	case c >= 0xC2 && c <= 0xDF:
		need = 1
	case c == 0xE0:
		need, lo = 2, 0xA0
	case c >= 0xE1 && c <= 0xEC, c == 0xEE, c == 0xEF:
		need = 2
	case c == 0xED:
		need, hi = 2, 0x9F
	case c == 0xF0:
		need, lo = 3, 0x90
	case c >= 0xF1 && c <= 0xF3:
		need = 3
	case c == 0xF4:
		need, hi = 3, 0x8F
	default:
		return 1
	}
	n := 1
	for n <= need && n < len(s) && s[n] >= lo && s[n] <= hi {
		n++
		lo, hi = 0x80, 0xBF
	}
	return n
}

// validText is s as AppendString writes it between its escapes: every maximal
// invalid subpart replaced by U+FFFD. It is the identity of a query key
// (AppendQuery): two keys that differ only in invalid bytes are written as one
// JSON name, so they must be grouped as one.
func validText(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	b := make([]byte, 0, len(s)+8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, string(utf8.RuneError)...)
			i += invalidRun(s[i:])
			continue
		}
		b = append(b, s[i:i+size]...)
		i += size
	}
	return string(b)
}
