package convert

import (
	"time"
)

// Timestamps (SPEC §6.4.2, §6.5.1, §6.5.2).
//
// Every rule here mirrors the skill's reference converter, including its quirks,
// because the golden vectors compare payload.timestamp byte for byte. The
// regular expressions of the reference are written out by hand: they run once
// per message on the drain path, and regexp would allocate.

// tsValue is a declared timestamp converted to UTC.
type tsValue struct {
	sec    int64
	ns     int64
	digits int // fraction digits as received, 0..9
}

// yearInferenceFuture is how far in the future of the reference an inferred
// year may place a BSD timestamp: a sender whose clock runs ahead.
const yearInferenceFuture = 86400

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// num reads n digits at b[i:]; ok is false when they are not all digits.
func num(b []byte, i, n int) (v int, ok bool) {
	if i+n > len(b) {
		return 0, false
	}
	for _, c := range b[i : i+n] {
		if !isDigit(c) {
			return 0, false
		}
		v = v*10 + int(c-'0')
	}
	return v, true
}

// frac reads 1..9 digits greedily, like the reference's (\d{1,9}).
func frac(b []byte, i int) (ns int64, digits int) {
	for digits < 9 && i+digits < len(b) && isDigit(b[i+digits]) {
		digits++
	}
	var v int64
	for k := 0; k < 9; k++ {
		v *= 10
		if k < digits {
			v += int64(b[i+k] - '0')
		}
	}
	return v, digits
}

func daysIn(y int, m time.Month) int {
	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// validCivil rejects what time.Date would silently normalise: 30 February
// becoming 2 March, second 60, hour 25. Years outside 1..9999 are refused too,
// because the payload's timestamp pattern has four year digits.
func validCivil(y, mo, d, h, mi, s int) bool {
	if y < 1 || y > 9999 || mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59 {
		return false
	}
	return d <= daysIn(y, time.Month(mo))
}

func timegm(y, mo, d, h, mi, s int) int64 {
	return time.Date(y, time.Month(mo), d, h, mi, s, 0, time.UTC).Unix()
}

// localToUnix converts a wall-clock time in loc to Unix seconds with the
// reference's fold=0 semantics (SPEC §6.5.2):
//
//   - an ambiguous time (the hour repeated when daylight saving ends) takes
//     its FIRST occurrence;
//   - a non-existent time (the hour skipped when it starts) takes the offset in
//     effect BEFORE the transition.
//
// time.Date does not promise either ("the choice of time zone, and therefore
// the time, is not guaranteed"), so the offsets are looked up explicitly a day
// either side and each candidate is checked.
func localToUnix(y, mo, d, h, mi, s int, loc *time.Location) int64 {
	u := timegm(y, mo, d, h, mi, s)
	if loc == nil || loc == time.UTC {
		return u
	}
	_, before := time.Unix(u-86400, 0).In(loc).Zone()
	_, after := time.Unix(u+86400, 0).In(loc).Zone()
	x1 := u - int64(before)
	if _, off := time.Unix(x1, 0).In(loc).Zone(); off == before {
		return x1
	}
	x2 := u - int64(after)
	if _, off := time.Unix(x2, 0).In(loc).Zone(); off == after {
		return x2
	}
	return x1
}

// utcYearOK keeps the formatted timestamp inside the four-digit years the
// payload schema allows: an offset can push 9999-12-31 into year 10000.
func utcYearOK(sec int64) bool {
	y := time.Unix(sec, 0).UTC().Year()
	return y >= 1 && y <= 9999
}

// appendTimestamp writes the payload timestamp: UTC, Z, the fraction with as
// many digits as received but at least 3 and at most 9, by truncation
// (SPEC §6.4.2). Never time.RFC3339Nano, which drops trailing zeros and may
// drop the fraction entirely.
func appendTimestamp(dst []byte, t tsValue) []byte {
	digits := t.digits
	if digits < 3 {
		digits = 3
	}
	if digits > 9 {
		digits = 9
	}
	dst = time.Unix(t.sec, 0).UTC().AppendFormat(dst, "2006-01-02T15:04:05")
	dst = append(dst, '.')
	var f [9]byte
	n := t.ns
	for i := 8; i >= 0; i-- {
		f[i] = byte('0' + n%10)
		n /= 10
	}
	dst = append(dst, f[:digits]...)
	return append(dst, 'Z')
}

// parseTS5424 reads an RFC 5424 TIMESTAMP token (the whole token):
//
//	YYYY-MM-DD(T|t)hh:mm:ss[.1*9DIGIT](Z|z|+hh:mm|-hh:mm)
//
// ok=false means timestamp-invalid. lax means a tolerated deviation that is
// header-invalid: a lowercase t or z, or more than six fraction digits.
func parseTS5424(tok []byte) (t tsValue, lax, ok bool) {
	if len(tok) < 20 {
		return t, false, false
	}
	y, ok1 := num(tok, 0, 4)
	mo, ok2 := num(tok, 5, 2)
	d, ok3 := num(tok, 8, 2)
	h, ok4 := num(tok, 11, 2)
	mi, ok5 := num(tok, 14, 2)
	s, ok6 := num(tok, 17, 2)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) || tok[4] != '-' || tok[7] != '-' ||
		(tok[10] != 'T' && tok[10] != 't') || tok[13] != ':' || tok[16] != ':' {
		return t, false, false
	}
	i := 19
	var ns int64
	var digits int
	if i < len(tok) && tok[i] == '.' {
		ns, digits = frac(tok, i+1)
		if digits == 0 {
			return t, false, false
		}
		i += 1 + digits
	}
	if i >= len(tok) {
		return t, false, false
	}
	var off int64
	zLower := false
	switch tok[i] {
	case 'Z', 'z':
		zLower = tok[i] == 'z'
		i++
	case '+', '-':
		oh, okh := num(tok, i+1, 2)
		om, okm := num(tok, i+4, 2)
		if !okh || !okm || i+3 >= len(tok) || tok[i+3] != ':' {
			return t, false, false
		}
		if oh > 23 || om > 59 {
			return t, false, false
		}
		off = int64(oh*3600 + om*60)
		if tok[i] == '-' {
			off = -off
		}
		i += 6
	default:
		return t, false, false
	}
	if i != len(tok) {
		return t, false, false
	}
	if !validCivil(y, mo, d, h, mi, s) {
		return t, false, false
	}
	sec := timegm(y, mo, d, h, mi, s) - off
	if !utcYearOK(sec) {
		return t, false, false
	}
	lax = tok[10] == 't' || zLower || digits > 6
	return tsValue{sec: sec, ns: ns, digits: digits}, lax, true
}

var monthNames = [12]string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}

func lower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

// tsEnd applies the reference's _ts_end: an optional ':' after the timestamp,
// then a space or the end. It returns where the rest of the message starts, or
// -1 when what matched is not a timestamp after all.
func tsEnd(b []byte, end int) int {
	if end < len(b) && b[end] == ':' {
		end++
	}
	if end == len(b) {
		return end
	}
	if b[end] == ' ' {
		return end + 1
	}
	return -1
}

// ts3164Kind tells the RFC 3164 timestamp forms apart.
type ts3164Kind uint8

const (
	tsNone  ts3164Kind = iota // nothing recognised
	tsFound                   // recognised; t is valid
	tsBad                     // recognised form, impossible date: timestamp-invalid
)

// parseTS3164 recognises a timestamp at the start of b (SPEC §6.5.1) and
// returns where the rest starts. refSec/refSub are the reception sample, the
// reference of the year inference; they stay a (seconds, nanoseconds) pair
// because a sample late in year 9999 -- which a forged head can carry -- does
// not fit int64 nanoseconds. warn receives timestamp-invalid and the inferred
// warnings.
func parseTS3164(b []byte, loc *time.Location, refSec, refSub int64, warn *Warnings) (t tsValue, kind ts3164Kind, rest int) {
	if t, kind, rest = parseBSD(b, loc, refSec, refSub, warn); kind != tsNone {
		return
	}
	return parseISO3164(b, loc, warn)
}

// parseBSD reads `Mmm +d{1,2} +[YYYY +]hh:mm:ss[.f{1,9}]`.
func parseBSD(b []byte, loc *time.Location, refSec, refSub int64, warn *Warnings) (tsValue, ts3164Kind, int) {
	if len(b) < 3 {
		return tsValue{}, tsNone, 0
	}
	mo := 0
	for i, m := range monthNames {
		if lower(b[0]) == m[0] && lower(b[1]) == m[1] && lower(b[2]) == m[2] {
			mo = i + 1
			break
		}
	}
	if mo == 0 {
		return tsValue{}, tsNone, 0
	}
	i := 3
	sp := i
	for i < len(b) && b[i] == ' ' {
		i++
	}
	if i == sp {
		return tsValue{}, tsNone, 0
	}
	ds := i
	for i < len(b) && i-ds < 2 && isDigit(b[i]) {
		i++
	}
	if i == ds {
		return tsValue{}, tsNone, 0
	}
	d, _ := num(b, ds, i-ds)
	sp = i
	for i < len(b) && b[i] == ' ' {
		i++
	}
	if i == sp {
		return tsValue{}, tsNone, 0
	}
	year := 0
	hasYear := false
	if y, ok := num(b, i, 4); ok && i+4 < len(b) && b[i+4] == ' ' {
		j := i + 4
		for j < len(b) && b[j] == ' ' {
			j++
		}
		// The year group is optional in the reference's regex; it is taken
		// only when the clock follows it.
		if _, okc := clock(b, j); okc {
			year, hasYear, i = y, true, j
		}
	}
	c, ok := clock(b, i)
	if !ok {
		return tsValue{}, tsNone, 0
	}
	i += 8
	var ns int64
	var digits int
	if i < len(b) && b[i] == '.' {
		if n, dg := frac(b, i+1); dg > 0 {
			ns, digits = n, dg
			i += 1 + dg
		}
	}
	rest := tsEnd(b, i)
	if rest < 0 {
		return tsValue{}, tsNone, 0
	}
	h, mi, s := c[0], c[1], c[2]
	if hasYear {
		if !validCivil(year, mo, d, h, mi, s) {
			warn.Add(WarnTimestampInvalid)
			return tsValue{}, tsBad, rest
		}
		sec := localToUnix(year, mo, d, h, mi, s, loc)
		if !utcYearOK(sec) {
			warn.Add(WarnTimestampInvalid)
			return tsValue{}, tsBad, rest
		}
		warn.Add(WarnTimestampZoneInferred)
		return tsValue{sec: sec, ns: ns, digits: digits}, tsFound, rest
	}
	// The most recent of Y+1, Y and Y-1 whose instant is not more than 24 h in
	// the future of the reference: that covers the year change both ways.
	refYear := time.Unix(refSec, refSub).In(locOrUTC(loc)).Year()
	limit := refSec + yearInferenceFuture
	for _, y := range [3]int{refYear + 1, refYear, refYear - 1} {
		if !validCivil(y, mo, d, h, mi, s) {
			continue
		}
		e := localToUnix(y, mo, d, h, mi, s, loc)
		// e*1e9+ns <= ref*1e9+sub+24h, compared as a pair: no overflow.
		if (e < limit || e == limit && ns <= refSub) && utcYearOK(e) {
			warn.Add(WarnTimestampYearInferred)
			warn.Add(WarnTimestampZoneInferred)
			return tsValue{sec: e, ns: ns, digits: digits}, tsFound, rest
		}
	}
	warn.Add(WarnTimestampInvalid)
	return tsValue{}, tsBad, rest
}

func locOrUTC(loc *time.Location) *time.Location {
	if loc == nil {
		return time.UTC
	}
	return loc
}

// clock reads hh:mm:ss at b[i:].
func clock(b []byte, i int) ([3]int, bool) {
	h, ok1 := num(b, i, 2)
	mi, ok2 := num(b, i+3, 2)
	s, ok3 := num(b, i+6, 2)
	if !ok1 || !ok2 || !ok3 || b[i+2] != ':' || b[i+5] != ':' {
		return [3]int{}, false
	}
	return [3]int{h, mi, s}, true
}

// parseISO3164 reads an RFC 3339 timestamp in an RFC 3164 position:
// YYYY-MM-DD(T|t)hh:mm:ss[.f{1,9}][Z|z|(+|-)hh[:]mm]. No offset means the
// configured zone, with timestamp-zone-inferred. Lowercase t/z is not a
// deviation here: header-invalid is an RFC 5424 notion.
func parseISO3164(b []byte, loc *time.Location, warn *Warnings) (tsValue, ts3164Kind, int) {
	if len(b) < 19 {
		return tsValue{}, tsNone, 0
	}
	y, ok1 := num(b, 0, 4)
	mo, ok2 := num(b, 5, 2)
	d, ok3 := num(b, 8, 2)
	c, ok4 := clock(b, 11)
	if !(ok1 && ok2 && ok3 && ok4) || b[4] != '-' || b[7] != '-' || (b[10] != 'T' && b[10] != 't') {
		return tsValue{}, tsNone, 0
	}
	i := 19
	var ns int64
	var digits int
	if i < len(b) && b[i] == '.' {
		if n, dg := frac(b, i+1); dg > 0 {
			ns, digits = n, dg
			i += 1 + dg
		}
	}
	hasOff := false
	var off int64
	if i < len(b) {
		switch b[i] {
		case 'Z', 'z':
			hasOff = true
			i++
		case '+', '-':
			// [+-]hh:?mm, taken only when it matches completely, as the
			// reference's optional group does.
			if oh, okh := num(b, i+1, 2); okh {
				j := i + 3
				if j < len(b) && b[j] == ':' {
					j++
				}
				if om, okm := num(b, j, 2); okm {
					// No range check: +25:00 and +05:75 are applied as
					// they came, as the skill's reference converter does
					// (decided on 2026-10-14 as in go-syslog-server: SPEC
					// §6.5.1, TestRFC3164ISOOffsetIsNotRangeChecked).
					off = int64(oh*3600 + om*60)
					if b[i] == '-' {
						off = -off
					}
					hasOff = true
					i = j + 2
				}
			}
		}
	}
	rest := tsEnd(b, i)
	if rest < 0 {
		return tsValue{}, tsNone, 0
	}
	h, mi, s := c[0], c[1], c[2]
	if !validCivil(y, mo, d, h, mi, s) {
		warn.Add(WarnTimestampInvalid)
		return tsValue{}, tsBad, rest
	}
	var sec int64
	if !hasOff {
		sec = localToUnix(y, mo, d, h, mi, s, loc)
	} else {
		sec = timegm(y, mo, d, h, mi, s) - off
	}
	if !utcYearOK(sec) {
		warn.Add(WarnTimestampInvalid)
		return tsValue{}, tsBad, rest
	}
	if !hasOff {
		warn.Add(WarnTimestampZoneInferred)
	}
	return tsValue{sec: sec, ns: ns, digits: digits}, tsFound, rest
}
