// Package filter drops messages by facility and severity before the ring
// (SPEC §7).
//
// go-webhook-server has no equivalent, and the difference is not taste: it is
// position in the network. Someone receiving webhooks CHOOSES who sends them;
// someone receiving syslog receives whatever the estate emits, and the estate
// is dominated by debug and info nobody asked for. Without this, an operator
// has two options -- pay to store everything, or walk machine by machine.
//
// The filter is reloadable on SIGHUP, which is the point: it is the knob you
// reach for at three in the morning, when one emitter goes mad and the decision
// is "cut debug now, investigate later".
package filter

import (
	"fmt"
	"sync/atomic"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
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

// MinSeverity exposes the configured ceiling, for the boot log.
func (r *Rule) MinSeverity() int { return r.minSeverity }

// Compile builds a Rule from the textual configuration. Both inputs accept
// names, aliases and decimal numbers, because "min_severity = warning" is what
// an operator writes and "4" is what a script writes.
func Compile(minSeverity string, facilities []string) (*Rule, error) {
	sev, ok := syslog.ParseSeverity(minSeverity)
	if !ok {
		return nil, fmt.Errorf("unknown severity %q: want emerg..debug or 0..7", minSeverity)
	}
	r := &Rule{minSeverity: sev}
	if len(facilities) == 0 {
		r.allFac = true
		return r, nil
	}
	for _, f := range facilities {
		v, ok := syslog.ParseFacility(f)
		if !ok {
			return nil, fmt.Errorf("unknown facility %q: want kern..local7 or 0..23", f)
		}
		r.facilities[v] = true
	}
	return r, nil
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

// Replace swaps the rule atomically, for SIGHUP.
func (f *Filter) Replace(r *Rule) {
	if r == nil {
		panic("filter: nil rule")
	}
	f.rule.Store(r)
}

// Rule returns the live rule, for the boot and reload log lines.
func (f *Filter) Rule() *Rule { return f.rule.Load() }
