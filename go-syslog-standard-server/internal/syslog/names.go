// Package syslog parses RFC 3164 and RFC 5424 messages (SPEC §6).
//
// It is the only code in this project that processes hostile structured input,
// the role internal/clientip has in go-webhook-server. Fuzz coverage is
// mandatory (SPEC §17.2).
//
// One rule governs everything here:
//
//	The parser NEVER fails and NEVER panics. There is no path where a message
//	is refused for not being understood. The worst possible outcome is
//	RFCUnknown with empty structural fields and the whole line as MSG.
//
// A message nobody can parse is exactly the one an operator most needs to see.
//
// The parser also never copies. Every field is a subslice of the raw line, and
// the caller owns that line for as long as it uses the result. The single
// exception is a structured-data value carrying an escape, which cannot be a
// subslice of something it does not equal.
package syslog

import "strconv"

// Facility and severity names of RFC 5424 Table 1 and Table 2.
//
// Exposing the names alongside the numbers is redundant, and it is the same
// deliberate trade the envelope makes with uts: roughly twenty bytes per event
// so that no consumer has to carry a translation table.
var facilityNames = [24]string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "alert", "clock",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

var severityNames = [8]string{
	"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
}

// FacilityName returns the name of a facility, or "" when out of range.
func FacilityName(f int) string {
	if f < 0 || f >= len(facilityNames) {
		return ""
	}
	return facilityNames[f]
}

// SeverityName returns the name of a severity, or "" when out of range.
func SeverityName(s int) string {
	if s < 0 || s >= len(severityNames) {
		return ""
	}
	return severityNames[s]
}

// aliases accepted in configuration but not emitted. They are the spellings
// operators actually type, inherited from syslog.conf and from the C headers;
// refusing them would fail a config file for a synonym the manual page uses.
var facilityAliases = map[string]int{
	"security":     4,  // the pre-RFC name of auth, still in old syslog.conf
	"console":      14, // what facility 14 is called on BSD
	"solaris-cron": 15,
	"cron2":        15,
}

var severityAliases = map[string]int{
	"emergency":     0,
	"panic":         0, // BSD name for emerg
	"critical":      2,
	"error":         3, // BSD name for err
	"warn":          4, // BSD name for warning
	"informational": 6,
	"information":   6,
}

// ParseFacility accepts a name ("local0"), an alias ("security") or a decimal
// number ("16"). It is used by configuration validation, never on the hot path.
func ParseFacility(s string) (int, bool) {
	for i, n := range facilityNames {
		if n == s {
			return i, true
		}
	}
	if v, ok := facilityAliases[s]; ok {
		return v, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 23 {
		return 0, false
	}
	return n, true
}

// ParseSeverity accepts a name ("warning"), an alias ("warn") or a decimal
// number ("4").
func ParseSeverity(s string) (int, bool) {
	for i, n := range severityNames {
		if n == s {
			return i, true
		}
	}
	if v, ok := severityAliases[s]; ok {
		return v, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 7 {
		return 0, false
	}
	return n, true
}
