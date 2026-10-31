package config

import (
	"fmt"
	"math"
	"net/netip"
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
	KDuration // Go syntax only: 30s, 5ms, 1h. A bare number is refused
	KSeconds  // KDuration that also accepts a bare integer as seconds (SPEC §16.2)
	KBytes    // a plain integer of bytes, or a KiB/MiB/GiB suffix
	KList     // comma separated, entries trimmed, empty entries dropped

	// KEnum is a string restricted to Param.Enum, matched case-insensitively
	// and stored in its canonical spelling, so comparisons elsewhere never need
	// folding and a typo fails at the source that wrote it, with its name.
	KEnum

	// KCIDRList is a comma list of CIDRs (a bare address is a /32 or /128),
	// stored as masked netip.Prefix values so the consumer never re-parses.
	KCIDRList
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
	case KEnum:
		return "enum"
	case KCIDRList:
		return "cidrs"
	}
	return "?"
}

// byteSuffixes are the only units SPEC §16.2 names. The standard also took KB,
// MB and K; they are refused here on purpose, because "8MB" meaning 8,000,000
// next to "8MiB" meaning 8,388,608 is a distinction an operator should never
// have to get right by accident.
var byteSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30},
}

// parseBytes accepts "8MiB", "512 MiB", "64KiB" and "1000000".
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
			if n > (1<<62)/bs.mult {
				return 0, fmt.Errorf("invalid byte size %q: overflow", s)
			}
			return n * bs.mult, nil
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: want an integer of bytes or a KiB/MiB/GiB suffix", s)
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

// parseDuration is Go syntax, never negative. A bare number is refused: "30"
// could be seconds or milliseconds depending on who wrote it.
func parseDuration(s string) (time.Duration, error) {
	v, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: want a unit suffix such as 5s, 100ms or 1h", s)
	}
	if v < 0 {
		return 0, fmt.Errorf("invalid duration %q: negative", s)
	}
	return v, nil
}

// parseSeconds is parseDuration that also takes a bare integer as seconds.
// LOGHUB_BEACON_INTERVAL is defined by the skill in seconds ("15"), and the
// rest of the ecosystem sets it that way; refusing it here would break the one
// variable every LogHub worker shares.
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

// ParsePrefixes parses a list of CIDRs, accepting a bare address as a
// full-length prefix so "10.0.0.5" and "10.0.0.5/32" mean the same thing.
// Addresses are unmapped first, so "::ffff:10.0.0.5" and "10.0.0.5" agree.
func ParsePrefixes(list []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(list))
	for _, raw := range list {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if strings.Contains(s, "/") {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR %q: %w", raw, err)
			}
			if p.Addr().Is4In6() && p.Bits() >= 96 {
				p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
			}
			// Masked() drops host bits so "10.0.0.5/24" behaves as
			// "10.0.0.0/24" instead of never matching.
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid address %q: %w", raw, err)
		}
		a = a.Unmap().WithZone("")
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// matchEnum returns the canonical spelling of raw in allowed.
func matchEnum(allowed []string, raw string) (string, error) {
	t := strings.TrimSpace(raw)
	for _, a := range allowed {
		if strings.EqualFold(a, t) {
			return a, nil
		}
	}
	shown := make([]string, len(allowed))
	for i, a := range allowed {
		if a == "" {
			a = `""`
		}
		shown[i] = a
	}
	return "", fmt.Errorf("%q is not one of %s", raw, strings.Join(shown, ", "))
}

// setValue parses raw according to p.Kind and stores it through p.Ptr. The
// pointer must match the kind; a mismatch is a programming error in the
// registry and panics rather than failing at runtime for the user.
func setValue(p *Param, raw string) error {
	switch p.Kind {
	case KString:
		ptr, ok := p.Ptr.(*string)
		if !ok {
			panic("config: KString bound to non-*string: " + p.Name())
		}
		*ptr = raw

	case KBool:
		ptr, ok := p.Ptr.(*bool)
		if !ok {
			panic("config: KBool bound to non-*bool: " + p.Name())
		}
		v, err := parseBool(raw)
		if err != nil {
			return err
		}
		*ptr = v

	case KInt:
		ptr, ok := p.Ptr.(*int)
		if !ok {
			panic("config: KInt bound to non-*int: " + p.Name())
		}
		v, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("invalid integer %q", raw)
		}
		*ptr = v

	case KInt64:
		ptr, ok := p.Ptr.(*int64)
		if !ok {
			panic("config: KInt64 bound to non-*int64: " + p.Name())
		}
		v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer %q", raw)
		}
		*ptr = v

	case KFloat:
		ptr, ok := p.Ptr.(*float64)
		if !ok {
			panic("config: KFloat bound to non-*float64: " + p.Name())
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return fmt.Errorf("invalid number %q", raw)
		}
		// ParseFloat accepts NaN and ±Inf, and NaN passes every range check
		// (every comparison with it is false): ring.high_water=NaN never
		// reported saturation, and log.access_sample=NaN would pass the
		// [0, 1] check.
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("invalid number %q: must be finite", raw)
		}
		*ptr = v

	case KDuration, KSeconds:
		ptr, ok := p.Ptr.(*time.Duration)
		if !ok {
			panic("config: KDuration bound to non-*time.Duration: " + p.Name())
		}
		parse := parseDuration
		if p.Kind == KSeconds {
			parse = parseSeconds
		}
		v, err := parse(raw)
		if err != nil {
			return err
		}
		*ptr = v

	case KBytes:
		ptr, ok := p.Ptr.(*int64)
		if !ok {
			panic("config: KBytes bound to non-*int64: " + p.Name())
		}
		v, err := parseBytes(raw)
		if err != nil {
			return err
		}
		*ptr = v

	case KList:
		ptr, ok := p.Ptr.(*[]string)
		if !ok {
			panic("config: KList bound to non-*[]string: " + p.Name())
		}
		*ptr = parseList(raw)

	case KEnum:
		ptr, ok := p.Ptr.(*string)
		if !ok {
			panic("config: KEnum bound to non-*string: " + p.Name())
		}
		v, err := matchEnum(p.Enum, raw)
		if err != nil {
			return err
		}
		*ptr = v

	case KCIDRList:
		ptr, ok := p.Ptr.(*[]netip.Prefix)
		if !ok {
			panic("config: KCIDRList bound to non-*[]netip.Prefix: " + p.Name())
		}
		v, err := ParsePrefixes(parseList(raw))
		if err != nil {
			return err
		}
		*ptr = v

	default:
		panic("config: unknown kind for " + p.Name())
	}
	return nil
}

// formatValue renders the current value behind p.Ptr in a form that parses
// back to the same value. Used by the effective-config dump, by the SIGHUP
// comparison and by --help.
func formatValue(p *Param) string {
	switch p.Kind {
	case KString, KEnum:
		return *p.Ptr.(*string)
	case KBool:
		return strconv.FormatBool(*p.Ptr.(*bool))
	case KInt:
		return strconv.Itoa(*p.Ptr.(*int))
	case KInt64:
		return strconv.FormatInt(*p.Ptr.(*int64), 10)
	case KFloat:
		return strconv.FormatFloat(*p.Ptr.(*float64), 'g', -1, 64)
	case KDuration, KSeconds:
		return p.Ptr.(*time.Duration).String()
	case KBytes:
		return FormatBytes(*p.Ptr.(*int64))
	case KList:
		return strings.Join(*p.Ptr.(*[]string), ",")
	case KCIDRList:
		ps := *p.Ptr.(*[]netip.Prefix)
		out := make([]string, len(ps))
		for i, q := range ps {
			out[i] = q.String()
		}
		return strings.Join(out, ",")
	}
	return ""
}
