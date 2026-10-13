package convert

// Normative labels of RFC 5427 (SPEC §6.2). The code is the truth; the name is
// the label. The standard server used `alert` for 14 and `clock` for 15, which
// RFC 5427 calls `console` and `cron2`.
var FacilityNames = [24]string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "console", "cron2",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

// SeverityNames are the RFC 5427 severity labels; the envelope's level is the
// code.
var SeverityNames = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

// Format is payload.format. rfc3195 never happens here: BEEP is out of scope.
type Format uint8

const (
	FormatRFC5424 Format = iota
	FormatRFC3164
	FormatMalformed
	NumFormats
)

var formatNames = [NumFormats]string{"rfc5424", "rfc3164", "malformed"}

func (f Format) String() string {
	if f < NumFormats {
		return formatNames[f]
	}
	return "unknown"
}

// Warning is one parse.warnings code. The numeric order IS the canonical order
// of the payload schema, so writing the set out in bit order is enough to
// satisfy SPEC §6.9.
type Warning uint8

const (
	WarnPRIMissing Warning = iota
	WarnPRIInvalid
	WarnVersionUnsupported
	WarnHeaderInvalid
	WarnTimestampMissing
	WarnTimestampInvalid
	WarnTimestampYearInferred
	WarnTimestampZoneInferred
	WarnHostnameMissing
	WarnSDInvalid
	WarnSDDuplicateID
	WarnEncodingInvalid
	WarnEncodingLegacy
	WarnTruncated
	NumWarnings
)

var warningNames = [NumWarnings]string{
	"pri-missing", "pri-invalid", "version-unsupported", "header-invalid",
	"timestamp-missing", "timestamp-invalid", "timestamp-year-inferred",
	"timestamp-zone-inferred", "hostname-missing", "sd-invalid",
	"sd-duplicate-id", "encoding-invalid", "encoding-legacy", "truncated",
}

func (w Warning) String() string {
	if w < NumWarnings {
		return warningNames[w]
	}
	return "unknown"
}

// Warnings is a set of codes.
type Warnings uint16

func (s *Warnings) Add(w Warning)     { *s |= 1 << w }
func (s Warnings) Has(w Warning) bool { return s&(1<<w) != 0 }
func (s Warnings) Empty() bool        { return s == 0 }
func (s *Warnings) Merge(o Warnings)  { *s |= o }

// The two normative parse.error texts of the skill (SPEC §6.6). They are data,
// compared literally by the golden vectors, which is why they are Portuguese in
// an English code base.
const (
	errPRIInvalid = "PRI inválido: a mensagem inteira foi tratada como CONTENT (RFC 3164 §4.3.3)"
	errNoHeader   = "sem PRI e sem cabeçalho syslog reconhecível"
)
