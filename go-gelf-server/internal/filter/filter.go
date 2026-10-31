// Package filter drops documents by GELF level before the ring (SPEC §7).
//
// Someone receiving GELF receives whatever the estate emits, and the estate is
// dominated by debug and info nobody asked for. Without this, an operator has
// two options -- pay to store everything, or walk application by application.
//
// The filter is reloadable on SIGHUP (SPEC §16.3), which is the point: it is
// the knob you reach for at three in the morning, when one emitter goes mad and
// the decision is "cut debug now, investigate later".
//
// It runs on the EFFECTIVE level (SPEC §7): a document without a level, with
// an invalid one, or malformed is filtered as 6, the same value the converter
// will write. The receiver obtains it from convert.Converter.LevelOf, the
// converter's own function, so a document is never filtered under one level
// and converted under another.
package filter

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
)

// Rule is an immutable decision table. Swapping a whole Rule under an
// atomic.Pointer is what makes reload safe without a lock on the receive path:
// a document either sees the old rule or the new one, never half of each.
type Rule struct {
	minLevel int
	// levels is a fixed 8-entry table; allLevels means filter.levels was empty.
	levels    [8]bool
	allLevels bool
}

// Allow reports whether a document of this level is accepted.
//
// The comparison is <= because a LOWER level is MORE severe: min_level=4 keeps
// emerg through warning and cuts notice, info and debug. Getting the direction
// backwards would silently keep only the noise.
func (r *Rule) Allow(level int) bool {
	if level < 0 || level > 7 || level > r.minLevel {
		return false
	}
	return r.allLevels || r.levels[level]
}

// Active reports whether the rule can refuse anything. When it cannot, the
// receiver does not read the level at all (SPEC §7).
func (r *Rule) Active() bool {
	if r.minLevel < 7 {
		return true
	}
	if r.allLevels {
		return false
	}
	for _, ok := range r.levels {
		if !ok {
			return true
		}
	}
	return false
}

// MinLevel exposes the configured ceiling, for the boot log.
func (r *Rule) MinLevel() int { return r.minLevel }

// Levels returns the allowed levels in ascending order, or nil when every
// level is accepted. For the boot and reload log lines.
func (r *Rule) Levels() []int {
	if r.allLevels {
		return nil
	}
	var out []int
	for i, ok := range r.levels {
		if ok {
			out = append(out, i)
		}
	}
	return out
}

// Compile builds a Rule from the textual configuration of SPEC §16.5:
// filter.min_level and filter.levels.
//
// Both accept the level names of SPEC §6.5 or decimal numbers, because
// "min_level = warning" is what an operator writes and "4" is what a script
// writes. Names are matched case-insensitively and surrounding blanks are
// ignored; nothing else is accepted (no "warn", no "error"). An unknown name is
// an error (exit 1 at boot; a refused reload on SIGHUP), never a silent
// default.
//
// An empty levels list accepts every level.
func Compile(minLevel string, levels []string) (*Rule, error) {
	min, ok := ParseLevel(minLevel)
	if !ok {
		return nil, fmt.Errorf("filter: unknown level %q: want one of %s or 0..7",
			minLevel, strings.Join(convert.LevelNames[:], ", "))
	}
	r := &Rule{minLevel: min}
	n := 0
	for _, l := range levels {
		if strings.TrimSpace(l) == "" {
			// "a,,b" or a trailing comma in an INI value is a typo, not a
			// request to accept everything.
			continue
		}
		v, ok := ParseLevel(l)
		if !ok {
			return nil, fmt.Errorf("filter: unknown level %q: want one of %s or 0..7",
				l, strings.Join(convert.LevelNames[:], ", "))
		}
		r.levels[v] = true
		n++
	}
	if n == 0 {
		r.allLevels = true
	}
	return r, nil
}

// ParseLevel accepts a level name ("warning") or a decimal number ("4"). It
// is used by configuration validation, never on the hot path.
func ParseLevel(s string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for i, n := range convert.LevelNames {
		if n == s {
			return i, true
		}
	}
	// strconv.Atoi rather than a hand-rolled loop: "+4" and "04" are both
	// accepted by it, and both mean 4 to anyone who wrote them.
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n > 7 {
		return 0, false
	}
	return n, true
}

// Filter holds the live rule. It is read on every received document and
// written only by SIGHUP.
type Filter struct {
	rule atomic.Pointer[Rule]
}

// New returns a Filter carrying r. r must not be nil: a filter with no rule
// would have to decide whether "no rule" means accept all or drop all, and
// either default is a silent data-loss bug waiting for the first caller that
// forgets to set it.
func New(r *Rule) *Filter {
	if r == nil {
		panic("filter: nil rule")
	}
	f := &Filter{}
	f.rule.Store(r)
	return f
}

// AllowLevel is the hot-path call, one atomic load and two comparisons.
func (f *Filter) AllowLevel(level int) bool { return f.rule.Load().Allow(level) }

// Active reports whether the live rule can refuse anything.
func (f *Filter) Active() bool { return f.rule.Load().Active() }

// Replace swaps the rule atomically, for SIGHUP.
func (f *Filter) Replace(r *Rule) {
	if r == nil {
		panic("filter: nil rule")
	}
	f.rule.Store(r)
}

// Rule returns the live rule, for the boot and reload log lines.
func (f *Filter) Rule() *Rule { return f.rule.Load() }
