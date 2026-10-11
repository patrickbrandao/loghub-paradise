package envelope

import (
	"strconv"
	"unicode/utf8"
)

// Hand-written JSON serialisation.
//
// encoding/json would reflect over a struct and allocate for every field; this
// path runs once per message. Writing directly into a pooled buffer keeps the
// whole envelope at zero allocations once the pools are warm.

const hexDigits = "0123456789abcdef"

func appendKey(b []byte, k string) []byte {
	b = append(b, '"')
	b = append(b, k...)
	return append(b, '"', ':')
}

func appendInt(b []byte, v int64) []byte { return strconv.AppendInt(b, v, 10) }

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, "true"...)
	}
	return append(b, "false"...)
}

// appendString writes a JSON string literal from a string.
func appendString(b []byte, s string) []byte {
	return appendJSONBytes(b, []byte(s))
}

// appendBytes writes a JSON string literal from a byte slice, which is what
// every parsed field is: a subslice of the raw line, never copied.
func appendBytes(b []byte, s []byte) []byte {
	return appendJSONBytes(b, s)
}

// appendJSONBytes escapes arbitrary bytes into a JSON string.
//
// Everything it receives came off the network: a hostname from an appliance, an
// app-name from a vendor's firmware, a structured-data value from anywhere.
// Invalid UTF-8 becomes U+FFFD, matching encoding/json, because a JSON string
// MUST be valid UTF-8 and emitting a raw byte would produce a document no
// consumer can parse -- one appliance speaking latin-1 would break the envelope
// of every event in the same batch.
//
// The raw line itself is never passed through here. It travels as the separate
// binary field b, byte for byte, precisely so that this replacement can never
// touch it (invariant §19.5).
func appendJSONBytes(b []byte, s []byte) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"':
				b = append(b, '\\', '"')
			case '\\':
				b = append(b, '\\', '\\')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRune(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, "�"...)
			i += size
			start = i
			continue
		}
		// U+2028 and U+2029 are valid JSON but terminate a line in JavaScript
		// source, and a browser-side consumer is a JavaScript program.
		if r == ' ' || r == ' ' {
			b = append(b, s[start:i]...)
			b = append(b, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

// SanitiseUTF8 returns s with invalid UTF-8 sequences replaced by U+FFFD,
// returning the input unchanged when it is already valid.
//
// It exists for sink=postgres and ONLY for the text columns it promotes
// (SPEC §3.6). PostgreSQL refuses text that is not valid UTF-8 with SQLSTATE
// 22021, and a class 22 takes the whole batch down -- one hostname from an
// appliance speaking latin-1 would carry the other 255 messages with it. This
// matters more here than in the sibling service, where the promoted columns are
// a path and a method rather than vendor firmware output.
//
// It is applied to the COPY that becomes a column. The envelope and the raw
// line are never touched (invariant §19.7).
func SanitiseUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	out := make([]byte, 0, len(s)+8)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			out = append(out, "�"...)
		} else {
			out = append(out, s[i:i+size]...)
		}
		i += size
	}
	return string(out)
}
