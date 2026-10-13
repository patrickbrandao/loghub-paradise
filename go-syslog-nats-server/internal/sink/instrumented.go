package sink

import (
	"context"
	"log/slog"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
)

// Instrumented writes envelopes that are already built -- the lines of an
// emergency dump (SPEC §12.2) -- with the observation and the fatal escalation
// that SPEC §10.1 and §13.3 require of EVERY write, no matter which code path
// issued it.
//
// The drain workers do not go through it: they need per-class handling for
// retry, backoff and the ring's commit that a decorator cannot express, and
// instrumenting in both places would double every counter. Reingestion has no
// worker and no ring of its own: it runs at boot step 9, after the ring and the
// workers are built and before the workers start (SPEC §13.1).
type Instrumented struct {
	inner  Sink
	health *Health
	log    *slog.Logger
	m      *obs.Metrics
	cfg    WorkerConfig
}

// NewInstrumented wraps inner. Of cfg only Batch and BatchMaxBytes are used:
// they cut the envelopes into chunks the way the workers would. health may be
// nil, in which case a fatal write is still counted but escalates nowhere.
func NewInstrumented(inner Sink, health *Health, cfg WorkerConfig, log *slog.Logger, m *obs.Metrics) *Instrumented {
	cfg.normalise()
	return &Instrumented{inner: inner, health: health, log: log, m: m, cfg: cfg}
}

// Deliver writes envs in chunks of at most sink.batch envelopes and
// sink.batch_max_bytes of JLH (SPEC §10.1; an envelope larger than the ceiling
// goes alone). It applies the drain workers' per-class rules itself, chunk by
// chunk (SPEC §10.2):
//
//   - network: wait is called with the attempt number and the error. nil
//     retries the SAME chunk; an error ends the call and is returned as is.
//   - logic: the chunk's events are counted as lost, logged with their uuids,
//     and the call moves on to the next chunk. Stopping there would leave the
//     later chunks unwritten while the caller, which treats a logic refusal as
//     delivered so the boot cannot crash-loop on it, removes the whole file:
//     messages lost with no count anywhere.
//   - fatal, or the caller's context ending: returned at once.
//
// Every attempt gets its own opTimeout. The envelopes' buffers belong to the
// caller and are never pooled, so nothing here needs tainting.
func (i *Instrumented) Deliver(ctx context.Context, envs []Envelope, opTimeout time.Duration,
	wait func(attempt int, err error) error) error {
	for len(envs) > 0 {
		n, size := 0, 0
		for n < len(envs) && n < i.cfg.Batch {
			l := len(envs[n].JLH)
			if n > 0 && size+l > i.cfg.BatchMaxBytes {
				break
			}
			size += l
			n++
		}
		b := Batch{Envelopes: envs[:n], Bytes: size}
		envs = envs[n:]
		// Once per chunk, not per attempt, as in the workers: the histograms
		// are "por pedaço" (SPEC §14.1), and a retried chunk observed on every
		// attempt would skew them toward whatever the destination was refusing.
		i.m.SinkBatchSize.Observe(int64(len(b.Envelopes)))
		i.m.SinkBatchBytes.Observe(int64(b.Bytes))

		for attempt := 1; ; attempt++ {
			wctx, cancel := context.WithTimeout(ctx, opTimeout)
			err := i.write(wctx, &b)
			cancel()
			if err == nil {
				break
			}
			class := ClassOf(err)
			if class == ClassLogic {
				// Settled BEFORE the context check: write already counted the
				// refusal (events_dropped{sink_logic}), so the ERROR with the
				// uuids must come with it even when a SIGTERM lands during
				// that write. Checked after, the count stood without the log
				// that says which messages were lost (go-relplog-pg-server's
				// independent review, 2026-10-13).
				stored, lost := settleLogic(err, b.Envelopes)
				i.log.Error("the sink refused a chunk permanently; the messages are lost",
					"count", len(lost), "stored", stored, "error", err, "uuids", lost)
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if class == ClassFatal {
				return err
			}
			// Counted only here, past the context check, as in the workers: a
			// write cut short by the caller's own cancellation -- SIGTERM during
			// the boot -- is not a sink failure (SPEC §14.1).
			i.m.SinkErrorsTotal[errKind(err)].Inc()
			if werr := wait(attempt, err); werr != nil {
				return werr
			}
		}
	}
	return nil
}

// write delegates one attempt at a chunk, then records what happened. A
// network failure is counted by Deliver, which alone knows whether the attempt
// ended because the caller is going away.
func (i *Instrumented) write(ctx context.Context, b *Batch) error {
	start := time.Now()
	err := i.inner.Write(ctx, b)
	// The batch is observed even when it fails, because the duration of a
	// failing write is exactly the signal that says whether the destination is
	// slow or refusing outright.
	i.m.SinkDuration.ObserveDuration(time.Since(start))

	if err == nil {
		i.m.EventsWrittenTotal.Add(uint64(len(b.Envelopes)))
		return nil
	}
	switch ClassOf(err) {
	case ClassFatal:
		i.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
		if i.health != nil {
			i.health.MarkFatal(err)
		}
	case ClassLogic:
		// Only what the destination did not store is lost (Error.Delivered).
		stored, lost := settleLogic(err, b.Envelopes)
		i.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
		i.m.EventsWrittenTotal.Add(uint64(stored))
		i.m.EventsDroppedTotal[obs.EventDropSinkLogic].Add(uint64(len(lost)))
	}
	return err
}

func (i *Instrumented) Health(ctx context.Context) error { return i.inner.Health(ctx) }
func (i *Instrumented) Close() error                     { return i.inner.Close() }
func (i *Instrumented) Type() string                     { return i.inner.Type() }
