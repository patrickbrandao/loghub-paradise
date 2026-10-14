package sink

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// Instrumented adapts a Sink to ring.Writer, and wraps it with the conversion,
// the observation and the fatal escalation that SPEC §10.1 and §13.3 require of
// EVERY write, no matter which code path issued it.
//
// It exists because ring=none writes straight through from the receiver
// goroutine (SPEC §9.6) instead of going through the drain workers, and the
// workers were the only place any of this happened. Without the wrapper that
// mode reported relplog_events_written_total, relplog_sink_duration_seconds,
// relplog_sink_batch_size and relplog_sink_errors_total as a permanent zero, and
// -- far worse -- a ClassFatal result died inside one receiver while the process
// kept running. Redis answering XLEN happily while refusing every XADD with OOM
// is exactly that case: the health check cannot see it, so only the write can.
//
// It is also where the conversion happens in ring=none mode, for the same
// reason: there is no worker to do it (SPEC §2.2).
//
// The wrapper is what the RING receives, never what the workers receive: the
// workers need per-class handling for retry and backoff that a decorator cannot
// express, and instrumenting in both places would double every counter.
//
// It does not break the orthogonality of invariant §20.15. The ring accepts a
// ring.Writer and still cannot see this package; this type consults the error's
// CLASS, which every destination produces, never the destination's type.
type Instrumented struct {
	inner  Sink
	health *Health
	log    *slog.Logger
	m      *obs.Metrics
	pool   *bufPool

	malformedLog *obs.Sampler

	// builders is a pool, not a single builder: in ring=none the writer is
	// whichever receiver goroutine happens to hold the message, up to
	// ring.max_inflight of them at once, and a Converter is not safe for
	// concurrent use.
	builders sync.Pool
}

// NewInstrumented wraps inner. Of cfg only Convert, BatchMaxBytes and
// AcceptMalformed are used: there is no retry here, the receiver decides what a
// failure means for its transport. health may be nil, in which case a fatal
// write is still counted but escalates nowhere.
func NewInstrumented(inner Sink, health *Health, cfg WorkerConfig, log *slog.Logger, m *obs.Metrics) *Instrumented {
	cfg.normalise(inner)
	i := &Instrumented{
		inner: inner, health: health, log: log, m: m,
		pool:         newBufPool(cfg.BatchMaxBytes),
		malformedLog: obs.NewSampler(30 * time.Second),
	}
	i.builders.New = func() any {
		return newBuilder(cfg.Convert, cfg.BatchMaxBytes, cfg.AcceptMalformed, i.pool, m)
	}
	return i
}

// Write converts, delegates, then records what happened.
//
// The items are NEVER released, tainted or discarded here: they belong to the
// receiver, which decides by the returned error (SPEC §15.2). Only the chunk
// buffers are this wrapper's, and after a context error they go to the GC: the
// command may still be in flight.
//
// A ring=none call normally carries one message. If it carried more JLH than
// sink.batch_max_bytes they would go in successive chunks, and the first
// failure ends the call.
func (i *Instrumented) Write(ctx context.Context, items []*item.Item) error {
	b := i.builders.Get().(*builder)
	defer func() {
		b.release()
		i.builders.Put(b)
	}()

	rest := items
	for {
		n := b.fill(rest)
		rest = rest[n:]
		c := b.cur
		if len(c.items) == 0 {
			return nil
		}
		if err := i.writeChunk(ctx, c); err != nil {
			// ring=none writes in a context the reception's cancellation
			// does not reach (SPEC §9.6): every network error here is the
			// destination's.
			i.countNetwork(err)
			return err
		}
		b.advance()
	}
}

// Deliver is Write for the boot paths, reingestion and the ring=sqlite backlog
// drain (SPEC §13.1 steps 9 and 10). They have neither a worker nor a receiver
// to decide what a failure means, so Deliver applies the drain workers'
// per-class rules itself, chunk by chunk (SPEC §10.2):
//
//   - network: wait is called with the attempt number and the error. nil
//     retries the SAME sealed chunk -- never rebuilt, so the bytes and the CPU
//     of the conversion are not spent again; an error ends the call and is
//     returned as is.
//   - logic: the chunk's events are counted as lost (by writeChunk), logged
//     with their uuids, and the call moves on to the next chunk. Stopping there
//     would leave the later chunks unwritten while the caller, which treats a
//     logic refusal as delivered so the boot cannot crash-loop on it, commits
//     the whole batch: messages lost with no count anywhere.
//   - fatal, or the caller's context ending: returned at once.
//
// Every attempt gets its own opTimeout. As in the workers, an attempt that may
// still be read by a lower layer taints the chunk and its items: their buffers
// go to the GC even if a retry succeeds (SPEC §15.2).
func (i *Instrumented) Deliver(ctx context.Context, items []*item.Item, opTimeout time.Duration,
	wait func(attempt int, err error) error) error {
	b := i.builders.Get().(*builder)
	defer func() {
		b.release()
		i.builders.Put(b)
	}()

	rest := items
	for {
		n := b.fill(rest)
		rest = rest[n:]
		c := b.cur
		if len(c.items) == 0 {
			return nil
		}
		for attempt := 1; ; attempt++ {
			wctx, cancel := context.WithTimeout(ctx, opTimeout)
			err := i.writeChunk(wctx, c)
			cancel()
			if err == nil {
				break
			}
			if taints(err) {
				c.taint()
			}
			class := ClassOf(err)
			if class == ClassLogic {
				// Settled BEFORE the context check: writeChunk already counted
				// the refusal (events_dropped{sink_logic}), so the ERROR with
				// the uuids must come with it even when a SIGTERM lands during
				// that write. Checked after, the count stood without the log
				// that says which messages were lost (go-relplog-pg-server
				// review, 2026-10-13). Only what the destination did not store
				// is lost (SPEC §10.2); writeChunk counted it the same way.
				stored, lost := settleLogic(err, c.Envelopes)
				i.log.Error("the sink refused a chunk permanently; the messages are lost",
					"count", len(lost), "stored", stored, "error", err, "uuids", lost)
				i.countRefused(c)
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
			// the boot -- is not a sink failure (SPEC §14.1). writeChunk
			// counted it before this check (port of the RP/RN round,
			// 2026-10-14, the form of go-syslog-server).
			i.countNetwork(err)
			if werr := wait(attempt, err); werr != nil {
				return werr
			}
		}
		b.advance()
	}
}

func (i *Instrumented) writeChunk(ctx context.Context, c *chunk) error {
	c.seal()
	if len(c.Envelopes) == 0 {
		i.countRefused(c)
		return nil
	}
	i.m.SinkBatchSize.Observe(int64(len(c.Envelopes)))
	i.m.SinkBatchBytes.Observe(int64(c.Bytes))

	start := time.Now()
	err := i.inner.Write(ctx, &c.Batch)
	// The batch is observed even when it fails, because the duration of a
	// failing write is exactly the signal that says whether the destination is
	// slow or refusing outright.
	i.m.SinkDuration.ObserveDuration(time.Since(start))

	if err == nil {
		i.m.EventsWrittenTotal.Add(uint64(len(c.Envelopes)))
		i.countRefused(c)
		return nil
	}
	if taints(err) {
		// The buffer only; the items are the receiver's, and it discards them
		// on this error.
		c.tainted = true
	}

	switch ClassOf(err) {
	case ClassFatal:
		i.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
		if i.health != nil {
			i.health.MarkFatal(err)
		}

	case ClassLogic:
		// The destination is alive and refused this data permanently. Retrying
		// cannot help, so the events are counted as lost right here -- only
		// those it did not store before refusing (Error.Delivered); the
		// receiver turns it into whatever refusal its transport can express.
		stored, lost := settleLogic(err, c.Envelopes)
		i.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
		i.m.EventsWrittenTotal.Add(uint64(stored))
		i.m.EventsDroppedTotal[obs.EventDropSinkLogic].Add(uint64(len(lost)))

	}
	// Network and timeout are counted by the caller (countNetwork), which
	// alone knows whether the attempt ended because the caller is going away.
	return err
}

// countNetwork counts a network or timeout failure of one attempt. Fatal and
// logic are counted by writeChunk, always: a verdict does not come from a
// cancellation.
func (i *Instrumented) countNetwork(err error) {
	if c := ClassOf(err); c != ClassFatal && c != ClassLogic {
		i.m.SinkErrorsTotal[errKind(err)].Inc()
	}
}

// countRefused records the malformed messages refused by
// syslog.accept_malformed=false. They are a deliberate, counted drop, which is
// why the call still succeeds for them (SPEC §6.6).
func (i *Instrumented) countRefused(c *chunk) {
	n := len(c.refused)
	if n == 0 {
		return
	}
	i.m.EventsDroppedTotal[obs.EventDropMalformed].Add(uint64(n))
	if ok, suppressed := i.malformedLog.Allow(); ok {
		i.log.Warn("malformed messages refused (syslog.accept_malformed=false)",
			"count", n, "first_uuid", c.refused[0].Head.UUID.String(), "suppressed", suppressed)
	}
}

func (i *Instrumented) Health(ctx context.Context) error { return i.inner.Health(ctx) }
func (i *Instrumented) Close() error                     { return i.inner.Close() }
func (i *Instrumented) Type() string                     { return i.inner.Type() }
