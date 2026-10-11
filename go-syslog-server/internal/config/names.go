package config

import (
	"strconv"
	"strings"
)

// Facility and severity labels of RFC 5427, the same table the converter
// emits (internal/convert). They are duplicated here rather than imported
// because config sits below every other package and imports nothing of the
// module but rediscli; TestNamesAreRFC5427 pins them to the RFC labels, which
// are the same ones the converter is tested against.
var facilityNames = [24]string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "console", "cron2",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

var severityNames = [8]string{
	"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
}

// There are no aliases. SPEC §7 accepts RFC 5427 names or numbers, and
// internal/filter compiles exactly that: a synonym accepted here and refused
// there would pass validation and then fail the boot. The standard server's
// "alert" (14) and "clock" (15) are refused on purpose -- "alert" is also a
// severity, and a filter that silently meant console would be a trap.

// ParseFacility accepts an RFC 5427 name ("local0") or a decimal number ("16"), case-insensitively. It is used by configuration
// validation and by the filter, never on the hot path.
func ParseFacility(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for i, n := range facilityNames {
		if n == s {
			return i, true
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 23 {
		return 0, false
	}
	return n, true
}

// ParseSeverity accepts an RFC 5427 name ("warning") or a decimal number ("4"), case-insensitively.
func ParseSeverity(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for i, n := range severityNames {
		if n == s {
			return i, true
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 7 {
		return 0, false
	}
	return n, true
}
