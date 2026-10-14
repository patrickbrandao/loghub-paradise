package sink

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
)

// State is the sink's health, per SPEC §10.2.
type State uint32

const (
	StateUp State = iota
	StateDown
	StateFatal
)

func (s State) String() string {
	switch s {
	case StateDown:
		return "down"
	case StateFatal:
		return "fatal"
	}
	return "up"
}

// HealthConfig mirrors the [sink] health tunables.
type HealthConfig struct {
	Interval   time.Duration
	Timeout    time.Duration
	FailAfter  time.Duration // how long DOWN may persist before FATAL
	MaxRetries int           // consecutive failed checks before FATAL
}

// Health drives the UP/DOWN/FATAL state machine.
//
// The state is decided EXCLUSIVELY by the periodic check (SPEC §10.2). Write
// failures classified as network do not move it: the item stays in the ring and
// is retried, so nobody was told 200 for an event that then vanished. Only a
// ClassFatal write result short-circuits straight to FATAL.
//
// The consequence to keep in mind is that detection latency is up to one
// Interval. That is the deliberate trade for a single, unambiguous source of
// truth about whether the destination is usable.
type Health struct {
	sink Sink
	cfg  HealthConfig
	log  *slog.Logger
	m    *obs.Metrics

	state atomic.Uint32

	// fatal is closed once, when the state reaches FATAL, so the process can
	// wait on it without polling.
	fatal     chan struct{}
	fatalOnce atomic.Bool
	fatalErr  atomic.Pointer[error]

	consecutiveFailures int
	downSince           time.Time
}

func NewHealth(s Sink, cfg HealthConfig, log *slog.Logger, m *obs.Metrics) *Health {
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = time.Second
	}
	h := &Health{sink: s, cfg: cfg, log: log, m: m, fatal: make(chan struct{})}
	h.state.Store(uint32(StateUp))
	m.SinkUp.Set(1)
	return h
}

// State reports the current health.
func (h *Health) State() State { return State(h.state.Load()) }

// Up reports whether the sink is usable. The handler consults this before
// accepting a request: in DOWN it answers 503 immediately, even with an empty
// ring (SPEC §10.2, fail fast).
func (h *Health) Up() bool { return h.State() == StateUp }

// Fatal returns a channel closed when the sink has become permanently
// unusable. The process drains what it can and exits 3.
func (h *Health) Fatal() <-chan struct{} { return h.fatal }

// FatalErr returns the error that caused FATAL, if any.
func (h *Health) FatalErr() error {
	if p := h.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// MarkFatal forces the terminal state. It is called by the drain workers when a
// write returns ClassFatal -- a read-only server, a full disk, a dropped table --
// where
// waiting for the next health check would be pointless.
func (h *Health) MarkFatal(err error) {
	if h.fatalOnce.Swap(true) {
		return
	}
	h.state.Store(uint32(StateFatal))
	h.m.SinkUp.Set(0)
	h.fatalErr.Store(&err)
	h.log.Error("sink is permanently unusable; the process will exit",
		"sink", h.sink.Type(), "error", err)
	close(h.fatal)
}

// Run drives the state machine until ctx is done. It returns when the context
// ends or the state becomes FATAL.
func (h *Health) Run(ctx context.Context) {
	t := time.NewTicker(h.cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-h.fatal:
			return
		case <-t.C:
			h.check(ctx)
		}
	}
}

func (h *Health) check(ctx context.Context) {
	cctx, cancel := context.WithTimeout(withLifetime(ctx), h.cfg.Timeout)
	start := time.Now()
	err := h.sink.Health(cctx)
	elapsed := time.Since(start)
	cancel()

	h.m.SinkHealthDuration.ObserveDuration(elapsed)

	if err == nil {
		h.onSuccess()
		return
	}
	h.onFailure(err)
}

func (h *Health) onSuccess() {
	h.consecutiveFailures = 0
	if h.State() == StateDown {
		down := time.Since(h.downSince)
		h.state.Store(uint32(StateUp))
		h.m.SinkUp.Set(1)
		h.downSince = time.Time{}
		h.log.Warn("sink recovered", "sink", h.sink.Type(), "down_for", down.String())
	}
}

func (h *Health) onFailure(err error) {
	h.consecutiveFailures++

	if h.State() == StateUp {
		h.downSince = time.Now()
		h.state.Store(uint32(StateDown))
		h.m.SinkUp.Set(0)
		h.log.Error("sink is DOWN; requests will be refused with 503",
			"sink", h.sink.Type(), "error", err)
	}

	if IsFatal(err) {
		// The destination answered, and the answer is permanent: a missing
		// table, a revoked grant, a password rotated away. Waiting cannot fix
		// any of it, so the fuses do not apply -- this is FATAL now (SPEC
		// §8.5.5, §8.5.7), and the emergency dump preserves what the ring
		// holds.
		h.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
		h.log.Error("sink refused the health probe permanently",
			"sink", h.sink.Type(), "error", err)
		h.MarkFatal(err)
		return
	}
	// A probe that timed out is counted apart from one the network refused,
	// like a write (SPEC §11.1).
	h.m.SinkErrorsTotal[errKind(err)].Inc()

	// Two independent fuses, whichever trips first (SPEC §10.2). The retry
	// count catches a fast-failing destination; the elapsed time catches one
	// that hangs until every check times out.
	byCount := h.cfg.MaxRetries > 0 && h.consecutiveFailures >= h.cfg.MaxRetries
	byTime := h.cfg.FailAfter > 0 && !h.downSince.IsZero() &&
		time.Since(h.downSince) >= h.cfg.FailAfter

	if byCount || byTime {
		h.log.Error("sink stayed DOWN past its limits",
			"sink", h.sink.Type(),
			"consecutive_failures", h.consecutiveFailures,
			"down_for", time.Since(h.downSince).String(),
			"tripped_by", map[bool]string{true: "retries", false: "timeout"}[byCount])
		h.MarkFatal(err)
	}
}

// WaitReady blocks until the sink answers a health check or the deadline
// passes. It is the boot gate of SPEC §10.1 step 3: the public listener never
// opens while the destination is unreachable.
func (h *Health) WaitReady(ctx context.Context, timeout time.Duration, backoffMin, backoffMax time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := backoffMin
	var lastErr error

	for attempt := 1; ; attempt++ {
		cctx, cancel := context.WithTimeout(withLifetime(ctx), h.cfg.Timeout)
		err := h.sink.Health(cctx)
		cancel()

		if err == nil {
			if attempt > 1 {
				h.log.Info("sink reachable", "sink", h.sink.Type(), "attempts", attempt)
			}
			return nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsFatal(err) {
			// A wrong password, a missing table, a corrupt file: none of these
			// change within sink.boot_timeout. Retrying would only postpone the
			// exit code the operator is waiting for (SPEC §8.5.5).
			h.log.Error("sink refused the boot check permanently; not retrying",
				"sink", h.sink.Type(), "attempt", attempt, "error", err)
			return err
		}
		if time.Now().After(deadline) {
			return lastErr
		}

		// Never past the budget: the whole backoff (up to 5s) used to run
		// and then one more check, after the deadline (W 39).
		sleep := min(backoff, time.Until(deadline))
		h.log.Warn("sink not reachable yet, retrying",
			"sink", h.sink.Type(), "attempt", attempt, "error", err,
			"retry_in", sleep.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		if backoff = backoff * 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// lifetimeKey carries, inside a health check's context, the context the check
// was derived from: the boot's, or the process's. A sink that has to outlive
// the check's own deadline -- the schema validation of SPEC §8.5.5 -- watches
// this one for cancellation instead, because once the check's deadline has
// passed, the check context can no longer report that the boot was cancelled.
type lifetimeKey struct{}

func withLifetime(ctx context.Context) context.Context {
	return context.WithValue(ctx, lifetimeKey{}, ctx)
}
