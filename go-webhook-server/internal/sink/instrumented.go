package sink

import (
	"context"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// Instrumented wraps a sink with the observation and the fatal escalation that
// SPEC §10.2 and §11.1 require of EVERY write, no matter which code path issued
// it.
//
// It exists because ring=none writes straight through from the handler
// (SPEC §7.5) instead of going through the drain workers, and the workers were
// the only place any of this happened. Without the wrapper that mode reported
// whs_events_written_total, whs_sink_duration_seconds, whs_sink_batch_size and
// whs_sink_errors_total as a permanent zero, and -- far worse -- a ClassFatal
// result was returned to the client as a 503 while the process kept running.
// Redis answering PING happily while refusing every XADD with OOM is exactly
// that case: the health check cannot see it, so only the write can.
//
// The wrapper is what the RING receives, never what the workers receive: the
// workers need per-class handling for retry and backoff that a decorator cannot
// express, and instrumenting in both places would double every counter.
//
// It does not break the orthogonality of SPEC §17.12. The ring accepts a
// ring.Writer and still cannot see the sink package; this type consults the
// error's CLASS, which every destination produces, never the destination's
// type.
type Instrumented struct {
	inner  Sink
	health *Health
	m      *obs.Metrics
}

// NewInstrumented wraps inner. health may be nil, in which case a fatal write
// is still counted but escalates nowhere.
func NewInstrumented(inner Sink, health *Health, m *obs.Metrics) *Instrumented {
	return &Instrumented{inner: inner, health: health, m: m}
}

// Write delegates, then records what happened.
//
// The batch is observed even when it fails, because the duration of a failing
// write is exactly the signal that says whether the destination is slow or
// refusing outright.
func (i *Instrumented) Write(ctx context.Context, items []*envelope.Item) error {
	start := time.Now()
	err := i.inner.Write(ctx, items)
	elapsed := time.Since(start)

	i.m.SinkDuration.ObserveDuration(elapsed)
	i.m.SinkBatchSize.Observe(int64(len(items)))

	if err == nil {
		i.m.EventsWrittenTotal.Add(uint64(len(items)))
		return nil
	}

	switch ClassOf(err) {
	case ClassFatal:
		i.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
		if i.health != nil {
			i.health.MarkFatal(err)
		}

	case ClassLogic:
		// The destination is alive and refused this data permanently. Retrying
		// cannot help, so the event is counted as lost right here; the caller
		// turns it into the 500 of SPEC §4.4. A partial delivery (SPEC §8.1)
		// counts what the destination stored as written, as the workers do.
		i.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
		refused := len(refusedOf(items, err))
		i.m.EventsWrittenTotal.Add(uint64(len(items) - refused))
		i.m.EventsDroppedTotal[obs.DropSinkLogic].Add(uint64(refused))

	default:
		i.m.SinkErrorsTotal[errKind(err)].Inc()
	}
	return err
}

func (i *Instrumented) Health(ctx context.Context) error { return i.inner.Health(ctx) }
func (i *Instrumented) Close() error                     { return i.inner.Close() }
func (i *Instrumented) Type() string                     { return i.inner.Type() }
