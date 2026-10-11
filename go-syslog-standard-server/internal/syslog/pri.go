package syslog

// PRI parses the leading "<N>" of a syslog message (SPEC §6.1).
//
// It runs on the RECEIVE path, unlike everything else in this package, because
// the facility/severity filter of SPEC §7 needs it in order to decide before
// the ring. Dropping after the ring would be accepting in order to throw away,
// which is the opposite of invariant §19.2. It is at most five bytes of work.
//
// It returns the priority and the number of bytes it consumed. ok is false when
// the PRI is absent or malformed, in which case the caller substitutes
// parse.default_facility and parse.default_severity and records
// pri-source = "default".
//
// Rejected as malformed, all of them deliberately:
//
//   - no '<' at offset zero -- there is no PRI anywhere else
//   - more than three digits, or none
//   - a leading zero ("<013>"), which RFC 5424 §6.2.1 forbids outright. A
//     sender emitting it is not speaking the protocol, and accepting it would
//     make "<0>" and "<00>" two spellings of the same thing.
//   - a value above 191, the largest facility 23 severity 7 can produce
func PRI(raw []byte) (pri int, n int, ok bool) {
	if len(raw) < 3 || raw[0] != '<' {
		return 0, 0, false
	}
	i := 1
	v := 0
	for i < len(raw) && i <= 4 && raw[i] >= '0' && raw[i] <= '9' {
		v = v*10 + int(raw[i]-'0')
		i++
	}
	digits := i - 1
	if digits == 0 || digits > 3 || i >= len(raw) || raw[i] != '>' {
		return 0, 0, false
	}
	if digits > 1 && raw[1] == '0' {
		return 0, 0, false
	}
	if v > 191 {
		return 0, 0, false
	}
	return v, i + 1, true
}
