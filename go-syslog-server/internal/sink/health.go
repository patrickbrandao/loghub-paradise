package sink

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// State is the sink's health, per SPEC §13.3.
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
	Interval   time.Duration // sink.health_interval
	Timeout    time.Duration // sink.health_timeout
	FailAfter  time.Duration // sink.fail_timeout: how long DOWN may persist before FATAL
	MaxRetries int           // sink.max_retries: consecutive failed checks before FATAL
}

// Health drives the UP/DOWN/FATAL state machine.
//
// The state is decided EXCLUSIVELY by the periodic check (SPEC §13.3). Write
// failures classified as network do not move it: the chunk stays with its
// worker and is retried, so nobody was confirmed for a message that then
// vanished. Only a ClassFatal write result short-circuits straight to FATAL, and
// a fatal answer to the probe itself does the same without waiting for the
// fuses.
//
// The consequence to keep in mind is that detection latency is up to one
// Interval. That is the deliberate trade for a single, unambiguous source of
// truth about whether the destination is usable.
type Health struct {
	sink Sink
	cfg  HealthConfig
	log  *slog.Logger
	m    *obs.Metrics

	// state is read without a lock (Up is on every receiver's path) and
	// WRITTEN only under mu, together with syslog_sink_up. A read-then-store
	// outside one lock let a health check that had just seen DOWN store UP over
	// a FATAL a worker set in between, and put the gauge back to 1: a process
	// on its way to exit 3 accepting messages again.
	state atomic.Uint32

	// fatal is closed once, when the state reaches FATAL, so the process can
	// wait on it without polling.
	fatal    chan struct{}
	fatalErr atomic.Pointer[error]

	// mu serialises every state transition with its gauge, and the counters
	// of the checks. FATAL is terminal: no transition leaves it.
	mu                  sync.Mutex
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

// Up reports whether the sink is usable.
//
// Every receiver consults this before accepting: in DOWN the transports apply
// their own backpressure immediately, even with an empty ring (SPEC §13.3, fail
// fast). What that means differs per transport -- a counted drop on UDP,
// stopping reads on TCP -- but the decision is this one bit.
func (h *Health) Up() bool { return h.State() == StateUp }

// Fatal returns a channel closed when the sink has become permanently
// unusable. The process dumps what it can and exits 3 (SPEC §12, §13.3).
func (h *Health) Fatal() <-chan struct{} { return h.fatal }

// FatalErr returns the error that caused FATAL, if any.
func (h *Health) FatalErr() error {
	if p := h.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}

// MarkFatal forces the terminal state. It is called by the drain workers when a
// write returns ClassFatal -- Redis OOM, SQLITE_FULL, a dropped PostgreSQL table
// -- where waiting for the next health check would be pointless.
func (h *Health) MarkFatal(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.markFatalLocked(err)
}

// markFatalLocked is MarkFatal with mu held. The first call wins; FATAL is
// never left.
func (h *Health) markFatalLocked(err error) {
	if h.State() == StateFatal {
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
	// The check carries ctx as its lifetime: a boot validation that outlives
	// sink.health_timeout (validationContext) watches it for cancellation.
	cctx, cancel := context.WithTimeout(withLifetime(ctx), h.cfg.Timeout)
	start := time.Now()
	err := h.sink.Health(cctx)
	elapsed := time.Since(start)
	cancel()

	h.m.SinkHealthDuration.ObserveDuration(elapsed)

	if ctx.Err() != nil {
		// The process is going down; a probe cut short by that is no verdict
		// on the destination.
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.State() == StateFatal {
		return
	}
	if err == nil {
		h.onSuccess()
		return
	}
	h.onFailure(err)
}

// onSuccess and onFailure run with mu held, so the state they read is the
// state they replace.
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
		h.log.Error("sink is DOWN; incoming messages will be refused or dropped per transport",
			"sink", h.sink.Type(), "error", err)
	}

	if IsFatal(err) {
		// The destination answered, and the answer is permanent: WRONGTYPE on
		// the stream key, NOAUTH after a credential rotation, a table that is
		// gone. Waiting cannot fix any of it, so the fuses do not apply -- this
		// is FATAL now (SPEC §10.3, §13.3), and the emergency dump preserves
		// what the ring holds.
		h.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
		h.log.Error("sink refused the health probe permanently",
			"sink", h.sink.Type(), "error", err)
		h.markFatalLocked(err)
		return
	}
	// A probe that timed out is counted apart from one the network refused,
	// like a write (SPEC §14.1).
	h.m.SinkErrorsTotal[errKind(err)].Inc()

	// Two independent fuses, whichever trips first (SPEC §13.3). The retry
	// count catches a fast-failing destination; the elapsed time catches one
	// that hangs until every check times out.
	byCount := h.cfg.MaxRetries > 0 && h.consecutiveFailures >= h.cfg.MaxRetries
	byTime := h.cfg.FailAfter > 0 && !h.downSince.IsZero() &&
		time.Since(h.downSince) >= h.cfg.FailAfter

	if byCount || byTime {
		trippedBy := "timeout"
		if byCount {
			trippedBy = "retries"
		}
		h.log.Error("sink stayed DOWN past its limits",
			"sink", h.sink.Type(),
			"consecutive_failures", h.consecutiveFailures,
			"down_for", time.Since(h.downSince).String(),
			"tripped_by", trippedBy)
		h.markFatalLocked(err)
	}
}

// WaitReady blocks until the sink answers a health check or the deadline passes.
//
// It is the boot gate of SPEC §13.1 step 7: no transport opens while the
// destination is unreachable. An open UDP socket in front of a dead sink would
// swallow datagrams with no way to refuse them -- the only defence is not to be
// listening, and let the emitter see ICMP port unreachable.
//
// The first successful check is also where every destination runs its one-time
// validation (schema, stream, privilege, largest event); a refusal there is
// fatal and returned at once (exit 3 without waiting for boot_timeout).
func (h *Health) WaitReady(ctx context.Context, timeout, backoffMin, backoffMax time.Duration) error {
	deadline := time.Now().Add(timeout)
	if backoffMin <= 0 {
		backoffMin = 100 * time.Millisecond
	}
	if backoffMax < backoffMin {
		backoffMax = backoffMin
	}
	backoff := backoffMin
	var lastErr error

	for attempt := 1; ; attempt++ {
		// The boot's context is the lifetime of this check: a validation
		// running past the check's deadline still sees a SIGTERM (SPEC §10.5).
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
			// exit code the operator is waiting for.
			h.log.Error("sink refused the boot check permanently; not retrying",
				"sink", h.sink.Type(), "attempt", attempt, "error", err)
			return err
		}
		if time.Now().After(deadline) {
			return lastErr
		}

		h.log.Warn("sink not reachable yet, retrying",
			"sink", h.sink.Type(), "attempt", attempt, "error", err,
			"retry_in", backoff.String())

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// lifetimeKey carries, inside a health check's context, the context the check
// was derived from: the boot's, or the process's. A sink whose boot
// validation has to outlive the check's own deadline watches this one for
// cancellation instead (validationContext), because once the check's deadline
// has passed, the check context can no longer report that the boot was
// cancelled. PostgreSQL, MySQL and ClickHouse use it.
//
// go-relplog-server keeps this pair in postgres.go, next to
// validationContext; here it lives with its two callers, Health.check and
// Health.WaitReady, so the health checks carry their lifetime whether or not
// a sink reads it.
type lifetimeKey struct{}

// withLifetime records ctx as the lifetime of the checks derived from it:
// Health.WaitReady and Health.check wrap their context in it before adding
// sink.health_timeout. Without it, validationContext falls back to watching
// the check context itself (see there for what that misses).
func withLifetime(ctx context.Context) context.Context {
	return context.WithValue(ctx, lifetimeKey{}, ctx)
}
