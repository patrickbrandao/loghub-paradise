package sink

import (
	"bytes"
	"fmt"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
)

// inboxRow is one envelope mapped onto the columns the three SQL destinations
// share (SPEC §3.6, §3.7): the loghub.inbox of PostgreSQL and the inbox tables
// that mirror it in SQLite and MySQL. Every value comes from the JLH header,
// read back from the item's own bytes; Payload is a sub-slice of the item's
// buffer, never a copy.
type inboxRow struct {
	UUID      [16]byte
	From      [16]byte
	FromText  string // the header's from, canonical: what the SQLite column takes
	Space     string
	Type      string
	Level     int16
	SID       int64
	Step      int32
	EventAt   time.Time // UTC, truncated to the microsecond
	EventNsec int16     // the 0-999 ns the microsecond does not hold
	Payload   []byte
}

// inboxRowOf maps one item. An envelope this server did not write -- which
// only damage could produce -- is refused as data: logic, never a destination
// failure.
func inboxRowOf(it *envelope.Item) (inboxRow, error) {
	h, payload, err := envelope.ParseHead(it.Env)
	if err != nil {
		return inboxRow{}, &Error{Class: ClassLogic, Op: "insert id=" + it.ID, Err: err}
	}
	// The inbox's from_uuid CHECK wants version 7, which go-loghub-ident
	// always makes; only foreign ring or dump data could differ, and it is
	// refused here rather than as a 23514 of the whole batch.
	from, ok := parseUUID(h.From)
	if !ok || from[6]>>4 != 7 {
		return inboxRow{}, &Error{Class: ClassLogic, Op: "insert id=" + it.ID,
			Err: fmt.Errorf("from %q is not a UUIDv7", h.From)}
	}
	at, nsec := inboxEvent(h.Event.UTS, h.Event.Sub)
	return inboxRow{
		UUID: h.UUID, From: from, FromText: h.From, Space: h.Space, Type: h.Type,
		Level: int16(h.Level), SID: int64(h.SID), Step: int32(h.Step),
		EventAt: at, EventNsec: nsec, Payload: payload,
	}, nil
}

// inboxEvent splits event.uts + event.sub into event_at and event_nsec (SPEC
// §3.6): the microsecond instant the databases can hold, and the 0-999 ns they
// cannot. Integer arithmetic only: to_timestamp(uts + sub/1e9) in float8 can
// be off by one microsecond.
func inboxEvent(uts, sub int64) (time.Time, int16) {
	return time.Unix(uts, sub).UTC().Truncate(time.Microsecond), int16(sub % 1000)
}

// nulEscape is the JSON escape jsonb refuses with 22P05.
var nulEscape = []byte(`\u0000`)

// hasNULEscape reports whether payload holds a real \u0000 escape -- not the
// text "\\u0000", whose backslash is itself escaped. The substring test runs
// first: nearly every payload has neither.
func hasNULEscape(payload []byte) bool {
	if !bytes.Contains(payload, nulEscape) {
		return false
	}
	for i := 0; i+1 < len(payload); i++ {
		if payload[i] != '\\' {
			continue
		}
		if payload[i+1] != 'u' {
			i++
			continue
		}
		if i+6 <= len(payload) && bytes.Equal(payload[i:i+6], nulEscape) {
			return true
		}
		i += 5
	}
	return false
}

// jsonbSafe returns payload with every \u0000 escape turned into \ufffd (SPEC
// §3.6). jsonb refuses U+0000, and a 22P05 is class 22: the whole batch would
// be dropped as logic for one request with a %00 in its query. The body never
// has one -- a body with NUL travels in base64 (SPEC §3.2).
//
// The common case returns payload itself. Otherwise the result is a copy, so
// the item's JLH -- the bytes every retry resends and every other destination
// receives -- is never written to. Only a real escape is replaced: in \\u0000
// the backslash is itself escaped and "u0000" is text.
func jsonbSafe(payload []byte) []byte {
	if !hasNULEscape(payload) {
		return payload
	}
	var out []byte
	for i := 0; i < len(payload); i++ {
		if payload[i] != '\\' || i+1 >= len(payload) {
			continue
		}
		if payload[i+1] != 'u' {
			i++ // a two-character escape: skip what it escapes
			continue
		}
		if i+6 <= len(payload) && bytes.Equal(payload[i:i+6], nulEscape) {
			if out == nil {
				out = append([]byte(nil), payload...)
			}
			copy(out[i:], `\ufffd`)
		}
		i += 5
	}
	if out == nil {
		return payload
	}
	return out
}
