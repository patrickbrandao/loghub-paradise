package config

import (
	"fmt"
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
	// KSeconds is a KDuration that also accepts a bare integer as seconds:
	// the form the LogHub skill gives LOGHUB_BEACON_INTERVAL (SPEC §8.7.10).
	KSeconds
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
	case KSeconds:
		return "seconds"
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
				return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
			}
			if n < 0 {
				return 0, fmt.Errorf("invalid byte size %q: negative", s)
			}
			// Overflow guard: n*mult must stay in int64.
			if bs.mult != 0 && n > (1<<62)/bs.mult {
				return 0, fmt.Errorf("invalid byte size %q: overflow", s)
			}
			return n * bs.mult, nil
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: want an integer or a KiB/MiB/GiB suffix", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid byte size %q: negative", s)
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
	return false, fmt.Errorf("invalid bool %q: want true/false, yes/no, on/off or 1/0", s)
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
func setValue(kind Kind, ptr any, raw string) error {
	switch kind {
	case KString:
		p, ok := ptr.(*string)
		if !ok {
			panic("config: KString bound to non-*string")
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
			return fmt.Errorf("invalid integer %q", raw)
		}
		*p = v

	case KInt64:
		p, ok := ptr.(*int64)
		if !ok {
			panic("config: KInt64 bound to non-*int64")
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", raw)
		}
		*p = v

	case KFloat:
		p, ok := ptr.(*float64)
		if !ok {
			panic("config: KFloat bound to non-*float64")
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return fmt.Errorf("invalid number %q", raw)
		}
		*p = v

	case KDuration:
		p, ok := ptr.(*time.Duration)
		if !ok {
			panic("config: KDuration bound to non-*time.Duration")
		}
		v, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("invalid duration %q: want a unit suffix such as 5s, 100ms or 1h", raw)
		}
		if v < 0 {
			return fmt.Errorf("invalid duration %q: negative", raw)
		}
		*p = v

	case KSeconds:
		p, ok := ptr.(*time.Duration)
		if !ok {
			panic("config: KSeconds bound to non-*time.Duration")
		}
		v, err := parseSeconds(raw)
		if err != nil {
			return err
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
	case KSeconds:
		// Whole seconds print as the bare integer the skill writes; anything
		// finer keeps the unit, so the dump parses back to the same value.
		d := *ptr.(*time.Duration)
		if d%time.Second == 0 {
			return strconv.FormatInt(int64(d/time.Second), 10)
		}
		return d.String()
	case KBytes:
		return FormatBytes(*ptr.(*int64))
	case KList:
		return strings.Join(*ptr.(*[]string), ",")
	}
	return ""
}

// parseSeconds is the KSeconds parser: a bare integer is seconds, as the LogHub
// skill defines its intervals; anything else is a Go duration.
func parseSeconds(s string) (time.Duration, error) {
	t := strings.TrimSpace(s)
	if n, err := strconv.ParseInt(t, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("invalid duration %q: negative", s)
		}
		if n > int64(1<<62)/int64(time.Second) {
			return 0, fmt.Errorf("invalid duration %q: overflow", s)
		}
		return time.Duration(n) * time.Second, nil
	}
	v, err := time.ParseDuration(t)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: want whole seconds (15) or a unit suffix (15s)", s)
	}
	if v < 0 {
		return 0, fmt.Errorf("invalid duration %q: negative", s)
	}
	return v, nil
}
