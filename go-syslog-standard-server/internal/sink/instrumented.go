package sink

import (
	"context"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Instrumented adapts a Sink to ring.Writer, and wraps it with the parse, the
// observation and the fatal escalation that SPEC §12.2 and §13.1 require of
// EVERY write, no matter which code path issued it.
//
// It exists because ring=none writes straight through from the receiver
// goroutine (SPEC §9.5) instead of going through the drain workers, and the
// workers were the only place any of this happened. Without the wrapper that
// mode reported syslog_events_written_total, syslog_sink_duration_seconds,
// syslog_sink_batch_size and syslog_sink_errors_total as a permanent zero, and
// -- far worse -- a ClassFatal result died inside one receiver while the process
// kept running. Redis answering XLEN happily while refusing every XADD with OOM
// is exactly that case: the health check cannot see it, so only the write can.
//
// It is also where the full parse happens in ring=none mode, for the same
// reason: there is no worker to do it.
//
// The wrapper is what the RING receives, never what the workers receive: the
// workers need per-class handling for retry and backoff that a decorator cannot
// express, and instrumenting in both places would double every counter.
//
// It does not break the orthogonality of invariant §19.14. The ring accepts a
// ring.Writer and still cannot see this package; this type consults the error's
// CLASS, which every destination produces, never the destination's type.
type Instrumented struct {
	inner  Sink
	health *Health
	m      *obs.Metrics

	// preps is a pool, not a single Prep: in ring=none the writer is whichever
	// receiver goroutine happens to hold the message, and up to
	// ring.max_inflight of them are in here at once.
	preps sync.Pool
}

// NewInstrumented wraps inner. health may be nil, in which case a fatal write is
// still counted but escalates nowhere.
func NewInstrumented(inner Sink, health *Health, m *obs.Metrics, nodeID string, opt syslog.Options) *Instrumented {
	i := &Instrumented{inner: inner, health: health, m: m}
	i.preps.New = func() any { return NewPrep(nodeID, opt, m) }
	return i
}

// Write parses, delegates, then records what happened.
//
// The batch is observed even when it fails, because the duration of a failing
// write is exactly the signal that says whether the destination is slow or
// refusing outright.
func (i *Instrumented) Write(ctx context.Context, items []*envelope.Item) error {
	prep := i.preps.Get().(*Prep)
	batch := prep.Build(items)

	start := time.Now()
	err := i.inner.Write(ctx, batch)
	elapsed := time.Since(start)

	if isContextErr(err) {
		// The command may still be in flight (SPEC §14.2): the envelopes must
		// not be pooled, and neither will the items -- the receiver discards
		// them on this error.
		prep.Abandon()
	} else {
		prep.Release()
	}
	i.preps.Put(prep)

	i.m.SinkDuration.ObserveDuration(elapsed)
	i.m.SinkBatchSize.Observe(int64(len(batch)))

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
		// cannot help, so the event is counted as lost right here; the receiver
		// turns it into whatever refusal its transport can express.
		i.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
		i.m.EventsDroppedTotal[obs.EventDropSinkLogic].Add(uint64(len(items)))

	default:
		i.m.SinkErrorsTotal[errKind(err)].Inc()
	}
	return err
}

func (i *Instrumented) Health(ctx context.Context) error { return i.inner.Health(ctx) }
func (i *Instrumented) Close() error                     { return i.inner.Close() }
func (i *Instrumented) Type() string                     { return i.inner.Type() }
