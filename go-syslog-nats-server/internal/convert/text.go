package convert

import (
	"unicode/utf8"
)

// Text decoding (SPEC §6.7).
//
// Every text field is strict UTF-8, and EACH octet that does not start a valid
// sequence becomes ONE U+FFFD. That is utf8.DecodeRune in a loop, and it is
// also what the skill's reference converter and Go's json.Marshal produce.
// strings.ToValidUTF8 is the trap: it folds a whole invalid run into a single
// U+FFFD, and two implementations of the same contract would then disagree on
// the same bytes.

// Charset is the optional legacy charset for message (SPEC §6.7).
type Charset uint8

const (
	CharsetNone Charset = iota
	CharsetCP1252
	CharsetLatin1
)

// ParseCharset maps the configuration value. windows-1252 is accepted because
// it is the example value of SYSLOG_LEGACY_CHARSET in the skill, which uses
// Python codec names; iso-8859-1 for symmetry. Both are aliases, nothing more.
func ParseCharset(s string) (Charset, bool) {
	switch s {
	case "":
		return CharsetNone, true
	case "cp1252", "windows-1252":
		return CharsetCP1252, true
	case "latin1", "iso-8859-1":
		return CharsetLatin1, true
	}
	return 0, false
}

// cp1252High maps 0x80..0x9F, the only range where Windows-1252 differs from
// Latin-1. The five holes (0x81, 0x8D, 0x8F, 0x90, 0x9D) are undefined in the
// codec and become U+FFFD, which is what the reference's errors="replace" does.
var cp1252High = [32]rune{
	0x20AC, 0xFFFD, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
	0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0xFFFD, 0x017D, 0xFFFD,
	0xFFFD, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
	0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0xFFFD, 0x017E, 0x0178,
}

// arena is the converter's scratch for decoded text. Slices handed out stay
// valid until reset: when an append reallocates, the slices taken before still
// point at the old array, which nobody writes again, and the GC keeps it alive.
type arena struct{ b []byte }

func (a *arena) reset() { a.b = a.b[:0] }

// mark and since bracket an append run and return it with a capacity clamp, so
// an append to the returned slice can never write into later scratch.
func (a *arena) mark() int { return len(a.b) }
func (a *arena) since(m int) []byte {
	return a.b[m:len(a.b):len(a.b)]
}

// decode returns b as UTF-8: b itself when it is already valid, otherwise a
// copy in the arena with one U+FFFD per offending octet. valid reports which.
func (a *arena) decode(b []byte) (out []byte, valid bool) {
	if utf8.Valid(b) {
		return b, true
	}
	m := a.mark()
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		if r == utf8.RuneError && n == 1 {
			a.b = append(a.b, "�"...)
		} else {
			a.b = append(a.b, b[:n]...)
		}
		b = b[n:]
	}
	return a.since(m), false
}

// decodeLegacy decodes the whole of b with a single-byte charset.
func (a *arena) decodeLegacy(b []byte, cs Charset) []byte {
	m := a.mark()
	for _, c := range b {
		switch {
		case c < 0x80:
			a.b = append(a.b, c)
		case c < 0xA0 && cs == CharsetCP1252:
			a.b = utf8.AppendRune(a.b, cp1252High[c-0x80])
		default:
			a.b = utf8.AppendRune(a.b, rune(c))
		}
	}
	return a.since(m)
}

// utf8LeadLen is the length a lead byte announces, 0 for a byte that cannot
// start a sequence (the reference converter's _utf8_lead_len).
func utf8LeadLen(c byte) int {
	switch {
	case c < 0x80:
		return 1
	case c >= 0xC2 && c <= 0xDF:
		return 2
	case c >= 0xE0 && c <= 0xEF:
		return 3
	case c >= 0xF0 && c <= 0xF4:
		return 4
	}
	return 0
}

// incompleteTail returns how many final octets are the valid start of a UTF-8
// sequence cut by truncation (SPEC §6.7). Those are dropped from message with
// no encoding warning: `truncated` already explains them. An invalid tail is
// not dropped here; it becomes U+FFFD with encoding-invalid like any other.
func incompleteTail(b []byte) int {
	for k := 1; k <= 3 && k <= len(b); k++ {
		c := b[len(b)-k]
		need := utf8LeadLen(c)
		if need != 0 {
			if need <= k {
				return 0
			}
			var pad [4]byte
			n := copy(pad[:], b[len(b)-k:])
			for ; n < need; n++ {
				pad[n] = 0x80
			}
			if utf8.Valid(pad[:need]) {
				return k
			}
			return 0
		}
		if c < 0x80 || c > 0xBF {
			return 0
		}
	}
	return 0
}

// hasJSWhitespace reports whether valid UTF-8 b contains a character that
// ECMAScript's \s matches. The payload schema declares hostname, appname,
// procid and msgid as ^\S+$, so a value with one of these would make the
// envelope invalid; the converter keeps such a value out of those fields
// (SPEC §6.4.1, §6.5.3, §6.5.4, deviation D6).
//
// U+001C..U+001F and U+0085 are added on top of ECMAScript's set: Python's \s
// matches them, and the skill's own validate_payload.py would otherwise refuse
// an envelope this server considers valid.
func hasJSWhitespace(b []byte) bool {
	for len(b) > 0 {
		c := b[0]
		if c < utf8.RuneSelf {
			if c == ' ' || c >= '\t' && c <= '\r' || c >= 0x1C && c <= 0x1F {
				return true
			}
			b = b[1:]
			continue
		}
		r, n := utf8.DecodeRune(b)
		switch {
		case r == 0x0085, r == 0x00A0, r == 0x1680, r >= 0x2000 && r <= 0x200A,
			r == 0x2028, r == 0x2029, r == 0x202F, r == 0x205F, r == 0x3000, r == 0xFEFF:
			return true
		}
		b = b[n:]
	}
	return false
}

// trimEOL removes every CR, LF and NUL at the end of message (SPEC §6.7).
func trimEOL(b []byte) []byte {
	for len(b) > 0 {
		switch b[len(b)-1] {
		case '\r', '\n', 0:
			b = b[:len(b)-1]
			continue
		}
		break
	}
	return b
}

var bom = []byte{0xEF, 0xBB, 0xBF}
