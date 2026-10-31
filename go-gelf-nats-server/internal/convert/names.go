package convert

// LevelNames are the payload.level names (SPEC §6.5): the RFC 5424 severity
// labels, the same ones go-syslog-server writes. The envelope's level is the
// code.
var LevelNames = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

// DefaultLevel is the level of a document without one, with an invalid one,
// or malformed (deviation D1, SPEC §6.5).
const DefaultLevel = 6

// Format is payload.format.
type Format uint8

const (
	FormatGELF Format = iota
	FormatMalformed
	NumFormats
)

var formatNames = [NumFormats]string{"gelf", "malformed"}

func (f Format) String() string {
	if f < NumFormats {
		return formatNames[f]
	}
	return "unknown"
}

// Warning is one parse.warnings code. The numeric order IS the canonical order
// of SPEC §6.8, so writing the set out in bit order is the rule.
type Warning uint8

const (
	WarnVersionMissing Warning = iota
	WarnVersionUnknown
	WarnVersionInvalid
	WarnHostMissing
	WarnHostInvalid
	WarnShortMessageMissing
	WarnShortMessageCoerced
	WarnFullMessageInvalid
	WarnLevelDefault
	WarnLevelCoerced
	WarnLevelInvalid
	WarnTimestampCoerced
	WarnTimestampInvalid
	WarnFacilityCoerced
	WarnFacilityInvalid
	WarnFileInvalid
	WarnLineCoerced
	WarnLineInvalid
	WarnFieldCoerced
	WarnFieldNull
	WarnFieldIDReserved
	WarnFieldNameInvalid
	WarnKeyUnknown
	WarnDuplicateKey
	WarnEncodingReplaced
	NumWarnings
)

var warningNames = [NumWarnings]string{
	"version-missing", "version-unknown", "version-invalid",
	"host-missing", "host-invalid",
	"short-message-missing", "short-message-coerced",
	"full-message-invalid",
	"level-default", "level-coerced", "level-invalid",
	"timestamp-coerced", "timestamp-invalid",
	"facility-coerced", "facility-invalid",
	"file-invalid",
	"line-coerced", "line-invalid",
	"field-coerced", "field-null", "field-id-reserved", "field-name-invalid",
	"key-unknown", "duplicate-key", "encoding-replaced",
}

// WarningNames returns the codes in canonical order, for the schema contract
// test and the metrics.
func WarningNames() []string { return warningNames[:] }

func (w Warning) String() string {
	if w < NumWarnings {
		return warningNames[w]
	}
	return "unknown"
}

// Warnings is a set of codes.
type Warnings uint32

func (s *Warnings) Add(w Warning)     { *s |= 1 << w }
func (s Warnings) Has(w Warning) bool { return s&(1<<w) != 0 }
func (s Warnings) Empty() bool        { return s == 0 }

// lossy are the warnings that mean something that arrived is not in the
// fields: a value omitted as invalid, an earlier duplicate, a character
// replaced. Each one forces raw (SPEC §6.9).
const lossy = Warnings(1<<WarnVersionInvalid | 1<<WarnHostInvalid | 1<<WarnFullMessageInvalid |
	1<<WarnLevelInvalid | 1<<WarnTimestampInvalid | 1<<WarnFacilityInvalid | 1<<WarnFileInvalid |
	1<<WarnLineInvalid | 1<<WarnDuplicateKey | 1<<WarnEncodingReplaced)

// Lossy reports whether the set forces raw.
func (s Warnings) Lossy() bool { return s&lossy != 0 }
