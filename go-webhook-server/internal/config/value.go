package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Kind is the type of a configuration parameter. It selects the parser used for
// every source of that parameter: the compiled-in default, the INI file, the
// environment and the command line all go through the same code path, so a
// default can never be accepted in a form that user input would reject.
type Kind int

const (
	KString Kind = iota
	KBool
	KInt
	KInt64
	KFloat
	KDuration
	KBytes // accepts a plain integer or a suffix: KiB, MiB, GiB, KB, MB, GB, K, M, G
	KList  // comma separated, entries trimmed, empty entries dropped
)

func (k Kind) String() string {
	switch k {
	case KString:
		return "string"
	case KBool:
		return "bool"
	case KInt:
		return "int"
	case KInt64:
		return "int64"
	case KFloat:
		return "float"
	case KDuration:
		return "duration"
	case KBytes:
		return "bytes"
	case KList:
		return "list"
	}
	return "?"
}

var byteSuffixes = []struct {
	suffix string
	mult   int64
}{
	// Longest first: "KiB" must win over "K".
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30},
	{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30},
	{"B", 1},
}

// parseBytes accepts "5MiB", "512 MiB", "64KiB", "1000000" and friends.
//
// Its errors, like every parser's here, say what is wrong and never quote the
// value, nor the strconv error that repeats it: the caller names the key, and
// a password pasted into the wrong key went whole to stderr (SPEC §13.2).
func parseBytes(s string) (int64, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, fmt.Errorf("empty byte size")
	}
	for _, bs := range byteSuffixes {
		if len(t) > len(bs.suffix) && strings.EqualFold(t[len(t)-len(bs.suffix):], bs.suffix) {
			num := strings.TrimSpace(t[:len(t)-len(bs.suffix)])
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size: the number before the unit is not an integer")
			}
			if n < 0 {
				return 0, fmt.Errorf("invalid byte size: negative")
			}
			// Overflow guard: n*mult must stay in int64.
			if bs.mult != 0 && n > (1<<62)/bs.mult {
				return 0, fmt.Errorf("invalid byte size: overflow")
			}
			return n * bs.mult, nil
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size: want an integer or a KiB/MiB/GiB suffix")
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid byte size: negative")
	}
	return n, nil
}

// FormatBytes renders a byte count using the largest exact binary unit, so the
// effective-config dump round-trips through parseBytes unchanged.
func FormatBytes(n int64) string {
	switch {
	case n == 0:
		return "0"
	case n%(1<<30) == 0:
		return strconv.FormatInt(n/(1<<30), 10) + "GiB"
	case n%(1<<20) == 0:
		return strconv.FormatInt(n/(1<<20), 10) + "MiB"
	case n%(1<<10) == 0:
		return strconv.FormatInt(n/(1<<10), 10) + "KiB"
	}
	return strconv.FormatInt(n, 10)
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("invalid bool: want true/false, yes/no, on/off or 1/0")
}

// parseList splits on commas, trims each entry and drops empty ones, so
// "a, b ,, c" and "a,b,c" are the same list and "" is the empty list.
func parseList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// setValue parses raw according to kind and stores it through ptr. ptr must be
// the pointer type matching kind; a mismatch is a programming error in the
// registry and panics rather than failing at runtime for the user.
// noControlByte refuses a text value with a control byte: below 0x20 but the
// tab, or DEL. The INI cannot carry one -- the dump split the value over two
// lines, read back as an error or as an injected key or section -- and an
// environment variable or a flag is where one comes from (SPEC §13.2, W §19.1 item 41).
// The byte is named, never the value: the caller names the key.
func noControlByte(raw string) error {
	for i := 0; i < len(raw); i++ {
		if c := raw[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return fmt.Errorf("contains a control byte (0x%02x) at offset %d; a configuration value cannot carry one", c, i)
		}
	}
	return nil
}

func setValue(kind Kind, ptr any, raw string) error {
	switch kind {
	case KString:
		p, ok := ptr.(*string)
		if !ok {
			panic("config: KString bound to non-*string")
		}
		if err := noControlByte(raw); err != nil {
			return err
		}
		*p = raw

	case KBool:
		p, ok := ptr.(*bool)
		if !ok {
			panic("config: KBool bound to non-*bool")
		}
		v, err := parseBool(raw)
		if err != nil {
			return err
		}
		*p = v

	case KInt:
		p, ok := ptr.(*int)
		if !ok {
			panic("config: KInt bound to non-*int")
		}
		v, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("invalid integer: want a decimal integer")
		}
		*p = v

	case KInt64:
		p, ok := ptr.(*int64)
		if !ok {
			panic("config: KInt64 bound to non-*int64")
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer: want a decimal integer")
		}
		*p = v

	case KFloat:
		p, ok := ptr.(*float64)
		if !ok {
			panic("config: KFloat bound to non-*float64")
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return fmt.Errorf("invalid number: want a decimal number")
		}
		// ParseFloat accepts NaN and ±Inf, and NaN passes every range check
		// (every comparison with it is false): ratelimit.rate=NaN answered 429
		// forever, ring.high_water=NaN never reported saturation (SPEC §13.5,
		// W pendência 24; the parser of the syslog family).
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("invalid number: must be finite")
		}
		*p = v

	case KDuration:
		p, ok := ptr.(*time.Duration)
		if !ok {
			panic("config: KDuration bound to non-*time.Duration")
		}
		v, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("invalid duration: want a unit suffix such as 5s, 100ms or 1h")
		}
		if v < 0 {
			return fmt.Errorf("invalid duration: negative")
		}
		*p = v

	case KBytes:
		p, ok := ptr.(*int64)
		if !ok {
			panic("config: KBytes bound to non-*int64")
		}
		v, err := parseBytes(raw)
		if err != nil {
			return err
		}
		*p = v

	case KList:
		p, ok := ptr.(*[]string)
		if !ok {
			panic("config: KList bound to non-*[]string")
		}
		if err := noControlByte(raw); err != nil {
			return err
		}
		*p = parseList(raw)

	default:
		panic("config: unknown kind")
	}
	return nil
}

// formatValue renders the current value behind ptr in a form that parses back to
// the same value. Used by the effective-config dump and by --help.
func formatValue(kind Kind, ptr any) string {
	switch kind {
	case KString:
		return *ptr.(*string)
	case KBool:
		return strconv.FormatBool(*ptr.(*bool))
	case KInt:
		return strconv.Itoa(*ptr.(*int))
	case KInt64:
		return strconv.FormatInt(*ptr.(*int64), 10)
	case KFloat:
		return strconv.FormatFloat(*ptr.(*float64), 'g', -1, 64)
	case KDuration:
		return ptr.(*time.Duration).String()
	case KBytes:
		return FormatBytes(*ptr.(*int64))
	case KList:
		return strings.Join(*ptr.(*[]string), ",")
	}
	return ""
}
