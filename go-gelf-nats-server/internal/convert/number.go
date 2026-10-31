package convert

import (
	"strconv"
	"time"
)

// Decimal arithmetic on the TEXT of a JSON number (SPEC §6.5). Nothing here
// goes through float64: 1700000000.123456789 has 19 significant digits and a
// float64 keeps 15 to 17 of them, and a level of 3.0000000000000001 is not 3.

// decimal is a validated JSON number with the exponent applied: the value is
// neg × intDigits.fracDigits. intDigits has no leading zero ("" for zero),
// fracDigits keeps every digit that was to the right of the point, trailing
// zeros included (they are the precision that arrived).
type decimal struct {
	neg        bool
	intDigits  string
	fracDigits string
}

// maxExpDigits bounds the exponent: a larger one cannot produce anything this
// converter accepts (a level, a line, a timestamp before year 10000), and
// applying it would mean building a string of that many zeros.
const maxExpDigits = 4

// parseDecimal applies the exponent of a JSON number. ok is false when b is not
// a number or its exponent has more than maxExpDigits digits.
func parseDecimal(b []byte) (d decimal, ok bool) {
	if !isNumber(b) {
		return d, false
	}
	i := 0
	if b[0] == '-' {
		d.neg = true
		i++
	}
	var mant []byte
	point := -1
	for ; i < len(b) && b[i] != 'e' && b[i] != 'E'; i++ {
		if b[i] == '.' {
			point = len(mant)
			continue
		}
		mant = append(mant, b[i])
	}
	if point < 0 {
		point = len(mant)
	}
	if i < len(b) { // exponent
		i++
		eneg := false
		if b[i] == '+' || b[i] == '-' {
			eneg = b[i] == '-'
			i++
		}
		digits := b[i:]
		for len(digits) > 1 && digits[0] == '0' {
			digits = digits[1:]
		}
		if len(digits) > maxExpDigits {
			return d, false
		}
		e, _ := strconv.Atoi(string(digits))
		if eneg {
			e = -e
		}
		point += e
	}
	switch {
	case point <= 0:
		frac := make([]byte, -point, -point+len(mant))
		for k := range frac {
			frac[k] = '0'
		}
		d.fracDigits = string(append(frac, mant...))
	case point >= len(mant):
		ip := append(mant, make([]byte, point-len(mant))...)
		for k := len(mant); k < len(ip); k++ {
			ip[k] = '0'
		}
		d.intDigits = string(ip)
	default:
		d.intDigits, d.fracDigits = string(mant[:point]), string(mant[point:])
	}
	for len(d.intDigits) > 0 && d.intDigits[0] == '0' {
		d.intDigits = d.intDigits[1:]
	}
	return d, true
}

func allZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// zero reports whether the value is 0 (-0 included).
func (d decimal) zero() bool { return d.intDigits == "" && allZero(d.fracDigits) }

// integer returns the value when it is an integer between 0 and max, for
// level and line. A negative zero is zero.
func (d decimal) integer(max int64) (int64, bool) {
	if !allZero(d.fracDigits) || len(d.intDigits) > 18 {
		return 0, false
	}
	if d.neg && d.intDigits != "" {
		return 0, false
	}
	var n int64
	for i := 0; i < len(d.intDigits); i++ {
		n = n*10 + int64(d.intDigits[i]-'0')
	}
	if n > max {
		return 0, false
	}
	return n, true
}

// isPlainInt reports whether b is written as an integer literal (^-?[0-9]+$):
// no fraction, no exponent. Any other spelling of an integer is a coercion.
func isPlainInt(b []byte) bool {
	for i, c := range b {
		if !isDigit(c) && !(i == 0 && c == '-') {
			return false
		}
	}
	return len(b) > 0
}

// maxTimestamp is 9999-12-31T23:59:59Z, the last second the payload pattern
// (four-digit year) can carry.
const maxTimestamp = 253402300799

// maxFracDigits is the deepest precision kept (SPEC §6.5).
const maxFracDigits = 9

// appendGELFTimestamp writes the payload timestamp of a GELF number of seconds
// (SPEC §6.5): UTC, a four-digit year, max(3, n) fraction digits with n the
// digits that arrived (truncated at nine, never rounded). ok is false for a
// negative value, a value past year 9999, or an exponent beyond maxExpDigits.
func appendGELFTimestamp(dst, num []byte) ([]byte, bool) {
	d, ok := parseDecimal(num)
	if !ok || d.neg && !d.zero() || len(d.intDigits) > 12 {
		return dst, false
	}
	var sec int64
	for i := 0; i < len(d.intDigits); i++ {
		sec = sec*10 + int64(d.intDigits[i]-'0')
	}
	if sec > maxTimestamp {
		return dst, false
	}
	frac := d.fracDigits
	if len(frac) > maxFracDigits {
		frac = frac[:maxFracDigits]
	}
	dst = time.Unix(sec, 0).UTC().AppendFormat(dst, "2006-01-02T15:04:05")
	dst = append(dst, '.')
	dst = append(dst, frac...)
	for k := len(frac); k < 3; k++ {
		dst = append(dst, '0')
	}
	return append(dst, 'Z'), true
}
