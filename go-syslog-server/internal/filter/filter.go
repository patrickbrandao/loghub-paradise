// Package filter drops messages by facility and severity before the ring
// (SPEC §7).
//
// go-webhook-server has no equivalent, and the difference is not taste: it is
// position in the network. Someone receiving webhooks CHOOSES who sends them;
// someone receiving syslog receives whatever the estate emits, and the estate
// is dominated by debug and info nobody asked for. Without this, an operator
// has two options -- pay to store everything, or walk machine by machine.
//
// The filter is reloadable on SIGHUP (SPEC §16.3), which is the point: it is
// the knob you reach for at three in the morning, when one emitter goes mad and
// the decision is "cut debug now, investigate later".
//
// It runs on the EFFECTIVE priority (SPEC §7): a message whose PRI is missing
// or invalid is filtered as syslog.default_priority, the same value the
// converter will write into its payload. The receiver obtains that priority
// from convert.EffectivePRI, the one PRI reader of the code base (SPEC §6.2),
// so a message is never filtered under one priority and converted under
// another.
package filter

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
)

// Rule is an immutable decision table. Swapping a whole Rule under an
// atomic.Pointer is what makes reload safe without a lock on the receive path:
// a message either sees the old rule or the new one, never half of each.
type Rule struct {
	minSeverity int
	// facilities is a fixed 24-entry table rather than a map or a slice scan.
	// The lookup happens once per received message, and an array index is the
	// cheapest form this decision can take.
	facilities [24]bool
	allFac     bool
}

// Allow reports whether a message with this facility and severity is accepted.
//
// The comparison is <= because a LOWER severity is MORE severe: min_severity=4
// keeps emerg through warning and cuts notice, info and debug. Getting the
// direction backwards would silently keep only the noise.
func (r *Rule) Allow(facility, severity int) bool {
	if severity > r.minSeverity {
		return false
	}
	if r.allFac {
		return true
	}
	if facility < 0 || facility >= len(r.facilities) {
		return false
	}
	return r.facilities[facility]
}

// AllowPRI is Allow on a whole priority value (facility*8 + severity), which
// is what the receive path holds.
func (r *Rule) AllowPRI(pri int) bool { return r.Allow(pri/8, pri%8) }

// MinSeverity exposes the configured ceiling, for the boot log.
func (r *Rule) MinSeverity() int { return r.minSeverity }

// Facilities returns the allowed facility codes in ascending order, or nil
// when every facility is accepted. For the boot and reload log lines.
func (r *Rule) Facilities() []int {
	if r.allFac {
		return nil
	}
	var out []int
	for i, ok := range r.facilities {
		if ok {
			out = append(out, i)
		}
	}
	return out
}

// Compile builds a Rule from the textual configuration of SPEC §16.5:
// filter.min_severity and filter.facilities.
//
// Both accept the RFC 5427 names of SPEC §6.2 or decimal numbers, because
// "min_severity = warning" is what an operator writes and "4" is what a script
// writes. Names are matched case-insensitively and surrounding blanks are
// ignored; nothing else is accepted. An unknown name is an error (exit 1 at
// boot, SPEC §13.2; a refused reload on SIGHUP), never a silent default.
//
// An empty facilities list accepts every facility.
func Compile(minSeverity string, facilities []string) (*Rule, error) {
	sev, ok := ParseSeverity(minSeverity)
	if !ok {
		return nil, fmt.Errorf("filter: unknown severity %q: want one of %s or 0..7",
			minSeverity, strings.Join(convert.SeverityNames[:], ", "))
	}
	r := &Rule{minSeverity: sev}
	n := 0
	for _, f := range facilities {
		if strings.TrimSpace(f) == "" {
			// "a,,b" or a trailing comma in an INI value is a typo, not a
			// request to accept everything.
			continue
		}
		v, ok := ParseFacility(f)
		if !ok {
			return nil, fmt.Errorf("filter: unknown facility %q: %s", f, facilityHint(f))
		}
		r.facilities[v] = true
		n++
	}
	if n == 0 {
		r.allFac = true
	}
	return r, nil
}

// facilityHint explains the one refusal an operator migrating from the
// standard server will meet: it called 14 "alert" and 15 "clock", which RFC
// 5427 (and therefore the payload) calls "console" and "cron2". Accepting the
// old names would make the filter disagree with what the destination shows.
func facilityHint(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "alert":
		return `facility 14 is "console" in RFC 5427 (the standard server called it "alert")`
	case "clock":
		return `facility 15 is "cron2" in RFC 5427 (the standard server called it "clock")`
	}
	return "want one of " + strings.Join(convert.FacilityNames[:], ", ") + " or 0..23"
}

// ParseSeverity accepts an RFC 5427 severity name ("warning") or a decimal
// number ("4"). It is used by configuration validation, never on the hot path.
func ParseSeverity(s string) (int, bool) {
	return parseName(s, convert.SeverityNames[:])
}

// ParseFacility accepts an RFC 5427 facility name ("local0", "cron2") or a
// decimal number ("16").
func ParseFacility(s string) (int, bool) {
	return parseName(s, convert.FacilityNames[:])
}

func parseName(s string, names []string) (int, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for i, n := range names {
		if n == s {
			return i, true
		}
	}
	// strconv.Atoi rather than a hand-rolled loop: "+4" and "04" are both
	// accepted by it, and both mean 4 to anyone who wrote them.
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || n >= len(names) {
		return 0, false
	}
	return n, true
}

// Filter holds the live rule. It is read on every received message and written
// only by SIGHUP.
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

// Allow is the hot-path call, one atomic load and two comparisons.
func (f *Filter) Allow(facility, severity int) bool {
	return f.rule.Load().Allow(facility, severity)
}

// AllowPRI is Allow on a whole priority value.
func (f *Filter) AllowPRI(pri int) bool { return f.rule.Load().AllowPRI(pri) }

// Replace swaps the rule atomically, for SIGHUP.
func (f *Filter) Replace(r *Rule) {
	if r == nil {
		panic("filter: nil rule")
	}
	f.rule.Store(r)
}

// Rule returns the live rule, for the boot and reload log lines.
func (f *Filter) Rule() *Rule { return f.rule.Load() }
