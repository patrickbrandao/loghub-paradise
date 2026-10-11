package obs

import (
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Logger wraps slog with the two things SPEC §13.2 requires and slog does not
// provide: a level that SIGHUP can change at runtime, and sampling so that
// nothing is written per message on the default path.
//
// Access logging matters more here than in the sibling service: with syslog it
// is literally logging log. At 100k messages a second, one line per message
// produces more volume than the service ingests.
type Logger struct {
	*slog.Logger
	level *slog.LevelVar

	// access logging, both reloadable
	accessOn     atomic.Bool
	accessSample atomic.Uint64 // float64 bits
}

// NewLogger builds the process logger. format is "json" or "text".
func NewLogger(w io.Writer, format, level string) *Logger {
	lv := new(slog.LevelVar)
	lv.Set(parseLevel(level))

	opts := &slog.HandlerOptions{Level: lv}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}

	l := &Logger{Logger: slog.New(h), level: lv}
	l.SetAccess(false, 0)
	return l
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetLevel applies a SIGHUP level change. slog.LevelVar is atomic, so in-flight
// log calls are unaffected.
func (l *Logger) SetLevel(s string) { l.level.Set(parseLevel(s)) }

// SetAccess applies a SIGHUP change to access logging.
func (l *Logger) SetAccess(on bool, sample float64) {
	if sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}
	l.accessOn.Store(on)
	l.accessSample.Store(math.Float64bits(sample))
}

// AccessEnabled reports whether this particular message should be logged.
//
// Sampling is decided before the caller builds any attributes, so an unsampled
// message costs one atomic load and one random number -- not the allocation of
// a log record that is then discarded.
func (l *Logger) AccessEnabled() bool {
	if !l.accessOn.Load() {
		return false
	}
	s := math.Float64frombits(l.accessSample.Load())
	if s >= 1 {
		return true
	}
	if s <= 0 {
		return false
	}
	return rand.Float64() < s
}

// Sampler rate-limits a repeating log line to at most one per interval. It
// exists for conditions that are per-message but describe a persistent problem
// at the other end -- a malformed frame, or structured data that never closes,
// which would otherwise print once per message from a broken emitter. A broken
// emitter must not be able to flood the collector's log with notices about
// being broken.
type Sampler struct {
	interval time.Duration
	lastNS   atomic.Int64
	start    time.Time
	// suppressed counts what was dropped since the last emission, so the line
	// that does get written can say how much it stands for.
	suppressed atomic.Uint64
}

func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{interval: interval, start: time.Now()}
}

// Allow reports whether a line may be written now, and how many were suppressed
// since the previous one.
func (s *Sampler) Allow() (bool, uint64) {
	// The stored stamp is offset by one so that zero means "nothing emitted
	// yet" and nothing else. time.Since can legitimately return 0 on the first
	// call -- the monotonic clock is coarse enough that it does so a few times
	// in a hundred -- and without the offset that first emission would leave
	// the sentinel untouched, letting a second line straight through.
	now := int64(time.Since(s.start)) + 1
	last := s.lastNS.Load()

	if last != 0 && now-last < int64(s.interval) {
		s.suppressed.Add(1)
		return false, 0
	}
	// A lost race here just means two lines instead of one, which is harmless;
	// paying for a mutex to prevent it would not be.
	if !s.lastNS.CompareAndSwap(last, now) {
		s.suppressed.Add(1)
		return false, 0
	}
	return true, s.suppressed.Swap(0)
}
