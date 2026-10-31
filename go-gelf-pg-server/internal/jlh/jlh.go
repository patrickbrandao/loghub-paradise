// Package jlh writes LogHub JLH envelopes (SPEC §3.1, §3.3).
//
// Every message on the LogHub eventbus is a JSON object with exactly nine
// fields, in a fixed order: uuid, from, space, type, level, sid, step, event,
// payload. This package writes the first eight and leaves the payload to the
// caller, which appends an object and then closes the envelope with Close.
//
// It is an append writer, not encoding/json, on purpose:
//
//   - The drain worker serialises one envelope per message, and a reflective
//     encoder would allocate per field on the hottest path of the process.
//   - The bytes must be reproducible (SPEC §3.5): a retry, a reingested dump
//     and a claimed ring entry all have to produce the SAME bytes for the same
//     uuid, which is what the JLH outbox rule asks for. A map-based encoder
//     sorts keys; this one writes them in the canonical order, always.
//   - encoding/json escapes <, > and & and silently swaps invalid UTF-8 for
//     U+FFFD. Logs full of HTML and XML would become unreadable, and the swap
//     would hide the encoding-invalid warning the payload must carry.
package jlh

import (
	"bytes"
	crand "crypto/rand"
	"strconv"
	"time"
	"unicode/utf8"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

// MaxSID is the largest sid the JLH schema accepts: 2^53-1, the ceiling that
// keeps the value exact in every JSON parser, JavaScript's included.
const MaxSID = 1<<53 - 1

// MaxUTS is the last second event.iso can represent, 9999-12-31T23:59:59Z
// (JLH schema).
const MaxUTS = 253402300799

// isoLayout truncates to milliseconds, which is what the JLH contract asks of
// event.iso: Go's .000 never rounds.
const isoLayout = "2006-01-02T15:04:05.000Z"

// gen is the one generator of the process (go-loghub-uuidv7 rule 13). Its
// entropy is the runtime's per-thread ChaCha8, seeded by the OS: no shared lock,
// millions of ids per second. The concern the standard server had with a
// clock-seeded PCG belonged to the old go-loghub-uuid library and does not apply
// to this one. The uuid is not a secret -- it exposes its own creation instant --
// and nothing derives a capability from it (SPEC §17.3).
var gen = uuidv7.NewGenerator()

// init fails loudly when the platform has no entropy at all. The generator
// itself never returns an error, so this is the only place a broken entropy
// source can surface before ids are minted.
func init() {
	var b [1]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("jlh: system entropy source unavailable: " + err.Error())
	}
}

// NewUUID returns a UUIDv7 at precision level 3 for the instant t.
//
// t must be the SAME clock sample that fills the envelope's event (SPEC §3.1):
// one sample, two views of it. Reading the clock again here would make the fact
// look later than the envelope that carries it.
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
// identity library and from constants, and all three are validated at boot to
// [a-z0-9._-] and lowercase UUID characters (SPEC §13.2).
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

// payloadKey is what AppendHeader writes between the event object and the
// payload. Nothing before it in the header can contain it: the only strings
// there are a UUID, two names of [a-z0-9._-] and a timestamp.
const payloadKey = `},"payload":`

// Payload returns the payload object of an envelope written by AppendHeader
// and Close, as a sub-slice of env (SPEC §3.7: the payload column of
// loghub.inbox). It never copies; ok is false when env does not have that
// shape.
func Payload(env []byte) (payload []byte, ok bool) {
	i := bytes.Index(env, []byte(payloadKey))
	if i < 0 || len(env) < i+len(payloadKey)+3 || env[len(env)-1] != '}' {
		return nil, false
	}
	p := env[i+len(payloadKey) : len(env)-1]
	if p[0] != '{' || p[len(p)-1] != '}' {
		return nil, false
	}
	return p, true
}

// AppendKey appends `"key":`. Keys are constants of this program and need no
// escaping.
func AppendKey(dst []byte, key string) []byte {
	dst = append(dst, '"')
	dst = append(dst, key...)
	return append(dst, '"', ':')
}

// AppendInt appends a JSON integer.
func AppendInt(dst []byte, n int64) []byte { return strconv.AppendInt(dst, n, 10) }

const hexDigits = "0123456789abcdef"

// AppendString appends s as a JSON string.
//
// The escape is the JSON minimum (SPEC §3.3): `"` and `\`, the named control
// escapes, and \u00XX for every other byte below 0x20 -- NUL included. <, > and
// & stay as they are, and so do U+2028 and U+2029.
//
// s is expected to be valid UTF-8 already: the converter decodes before it
// serialises, so it can record encoding-invalid (SPEC §6.7). As a last line of
// defence an invalid byte still becomes one U+FFFD here, never a raw invalid
// byte inside the document -- an envelope that is not valid JSON would be
// rejected by every consumer on the bus.
func AppendString(dst []byte, s []byte) []byte {
	dst = append(dst, '"')
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
			dst = append(dst, "�"...)
			i++
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// AppendStr is AppendString for a string, without converting it.
func AppendStr(dst []byte, s string) []byte {
	dst = append(dst, '"')
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
			dst = append(dst, "�"...)
			i++
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
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
	case '\b':
		return append(dst, '\\', 'b')
	case '\f':
		return append(dst, '\\', 'f')
	}
	return append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
}
