package sink

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ring"
)

// WorkerConfig mirrors the [sink] batching tunables.
type WorkerConfig struct {
	Workers    int
	Batch      int
	OpTimeout  time.Duration
	BackoffMin time.Duration
	BackoffMax time.Duration
}

// Workers move items from the ring to the sink. The loop is the one every
// destination of go-webhook-fast-server shares -- the drain path has no idea
// which destination it is feeding.
type Workers struct {
	ring   ring.Ring
	sink   Sink
	health *Health
	cfg    WorkerConfig
	log    *slog.Logger
	m      *obs.Metrics

	wg sync.WaitGroup

	// held records the batch each worker is currently retrying.
	//
	// SPEC §10.4 has a failing worker HOLD its batch and stop pulling from the
	// ring. That means those items are no longer in the ring, so a shutdown
	// drain would not find them and they would vanish -- events that were
	// already answered 200. Registering them here is what lets Pending() hand
	// them to the emergency dump of SPEC §9.
	holdMu sync.Mutex
	held   map[int][]*envelope.Item
}

func NewWorkers(r ring.Ring, s Sink, h *Health, cfg WorkerConfig, log *slog.Logger, m *obs.Metrics) *Workers {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Batch < 1 {
		cfg.Batch = 1
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = 100 * time.Millisecond
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = cfg.BackoffMin
	}
	return &Workers{ring: r, sink: s, health: h, cfg: cfg, log: log, m: m,
		held: make(map[int][]*envelope.Item)}
}

// Start launches the drain goroutines.
func (w *Workers) Start(ctx context.Context) {
	for i := 0; i < w.cfg.Workers; i++ {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.loop(ctx, id)
		}(i)
	}
}

// Wait blocks until every worker has returned.
func (w *Workers) Wait() { w.wg.Wait() }

func (w *Workers) loop(ctx context.Context, id int) {
	for {
		items, err := w.ring.Dequeue(ctx, w.cfg.Batch)
		if err != nil {
			// The memory ring only returns an error when the context ends.
			return
		}
		if len(items) == 0 {
			continue
		}
		if !w.deliver(ctx, items, id) {
			// deliver ABANDONED the batch and left it registered for the
			// emergency dump. This worker must stop here.
			//
			// Going round again would call Dequeue with a cancelled context,
			// and Dequeue's first select races that cancellation against a ring
			// that still holds a backlog. Go picks a ready case at random, so
			// the channel wins about half the time: the worker returns with a
			// second batch and hands it to deliver, which registers it over the
			// batch it was still holding. Those events were answered 200 and
			// end up in neither the ring nor the dump.
			return
		}
	}
}

// deliver writes one batch, retrying until it succeeds, the failure turns out
// to be permanent, or the process is going down.
//
// It reports whether ownership of the batch MOVED ON -- delivered, or rejected
// for good. False means the batch was abandoned and is still registered, and
// the caller must not fetch another one.
//
// SPEC §10.4: on a retryable failure the worker HOLDS the batch in its own
// buffer and pulls nothing more from the ring. The ring is never modified -- it
// simply stops draining and fills, until 429 appears on its own. Consequences:
// no item is lost, nothing is reordered, and the ring never needs to support
// re-insertion at the head.
func (w *Workers) deliver(ctx context.Context, items []*envelope.Item, id int) bool {
	backoff := w.cfg.BackoffMin

	// Registered for the whole attempt. The registration is cleared ONLY on the
	// paths where ownership actually moves on -- a successful write, or a
	// permanent rejection. It is deliberately not deferred: a deferred clear
	// would also fire when this worker ABANDONS the batch on shutdown or on a
	// fatal sink, which is precisely the case the registration exists for.
	w.hold(id, items)

	for attempt := 1; ; attempt++ {
		wctx, cancel := context.WithTimeout(ctx, w.cfg.OpTimeout)
		start := time.Now()
		err := w.sink.Write(wctx, items)
		elapsed := time.Since(start)
		cancel()

		w.m.SinkDuration.ObserveDuration(elapsed)
		w.m.SinkBatchSize.Observe(int64(len(items)))

		if err == nil {
			w.m.EventsWrittenTotal.Add(uint64(len(items)))
			if err := w.ring.Commit(items); err != nil {
				w.log.Error("ring commit failed after a successful write; "+
					"the events are delivered but the ring may redeliver them",
					"worker", id, "error", err)
			}
			// Ownership transfers here and nowhere else: the batch is
			// confirmed, so its buffers go back to the pool exactly once
			// (SPEC §12.3).
			for _, it := range items {
				it.Release()
			}
			w.unhold(id)
			return true
		}

		switch ClassOf(err) {
		case ClassFatal:
			w.m.SinkErrorsTotal[obs.SinkErrFatal].Inc()
			// The items stay held; the emergency dump collects them together
			// with the rest of the ring.
			w.health.MarkFatal(err)
			return false

		case ClassLogic:
			// The destination is alive and refused this data permanently.
			// Retrying cannot help, so drop it -- loudly, and counted. Only
			// what it refused: a partial delivery (SPEC §8.1) names the items
			// it stored, and those are written, not lost.
			w.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
			refused := refusedOf(items, err)
			w.m.EventsWrittenTotal.Add(uint64(len(items) - len(refused)))
			w.m.EventsDroppedTotal[obs.DropSinkLogic].Add(uint64(len(refused)))
			ids := make([]string, 0, len(refused))
			for _, it := range refused {
				ids = append(ids, it.ID)
			}
			w.log.Error("sink rejected events permanently; they are lost",
				"worker", id, "count", len(refused), "batch", len(items), "error", err, "ids", ids)
			// Commit BEFORE Release, as on success: the ring subtracts
			// it.Size(), which is len(Env), and Release empties Env. The
			// other order left the ring counting bytes nobody held, until it
			// refused every request as full with nothing in it.
			_ = w.ring.Commit(items)
			for _, it := range items {
				it.Release()
			}
			w.unhold(id)
			return true
		}

		// Network or timeout: retry, holding the batch. A write cut short by
		// the worker's own context -- the shutdown -- is not a sink failure
		// and is not counted as one (SPEC §11.1).
		if ctx.Err() == nil {
			w.m.SinkErrorsTotal[errKind(err)].Inc()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// The attempt ended by the clock, not by an answer, and nothing
			// proves the layer below has let go of the buffers. From here on
			// the items are never pooled again (SPEC §12.3): a successful
			// retry hands them to the GC. The NATS sink also taints, itself,
			// every batch that failed after its first publish (SPEC §8.7.2):
			// a pending acknowledgement future still points at Env. This
			// clause stays for any error that reaches here as a bare context
			// error, so the rule does not depend on one sink's diligence.
			for _, it := range items {
				it.Taint()
			}
		}
		if ctx.Err() != nil {
			// Shutdown while retrying. Do NOT release: these items are still
			// pending and belong in the emergency dump.
			w.log.Warn("shutdown while retrying a batch; items remain pending",
				"worker", id, "count", len(items))
			return false
		}
		if attempt == 1 || attempt%10 == 0 {
			w.log.Warn("sink write failed, retrying",
				"worker", id, "attempt", attempt, "count", len(items),
				"error", err, "retry_in", backoff.String())
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > w.cfg.BackoffMax {
			backoff = w.cfg.BackoffMax
		}
	}
}

// refusedOf returns the items of a logic failure the destination did NOT
// store: all of them, unless the error reports a partial delivery (SPEC §8.1).
// An index outside the batch is ignored rather than trusted: a miscount of
// the written series is better than a panic in the drain.
func refusedOf(items []*envelope.Item, err error) []*envelope.Item {
	delivered, ok := DeliveredOf(err)
	if !ok {
		return items
	}
	stored := make([]bool, len(items))
	kept := 0
	for _, i := range delivered {
		if i >= 0 && i < len(items) && !stored[i] {
			stored[i] = true
			kept++
		}
	}
	out := make([]*envelope.Item, 0, len(items)-kept)
	for i, it := range items {
		if !stored[i] {
			out = append(out, it)
		}
	}
	return out
}

// hold registers the batch a worker is working on.
//
// It APPENDS rather than assigns. The loop already guarantees a worker never
// arrives here still holding something, so in practice this only ever writes
// into an empty slot -- but an assignment would make a future mistake in that
// guarantee cost silently discarded events, while an append costs a slice that
// is one batch too long and loses nothing.
func (w *Workers) hold(id int, items []*envelope.Item) {
	w.holdMu.Lock()
	w.held[id] = append(w.held[id], items...)
	w.holdMu.Unlock()
}

func (w *Workers) unhold(id int) {
	w.holdMu.Lock()
	delete(w.held, id)
	w.holdMu.Unlock()
}

// Pending returns the batches the workers were still holding when they stopped.
//
// Call it only after Wait: a running worker may still deliver its batch, and
// taking the items from under it would either duplicate them or release their
// buffers twice.
func (w *Workers) Pending() []*envelope.Item {
	w.holdMu.Lock()
	defer w.holdMu.Unlock()

	var out []*envelope.Item
	for id, items := range w.held {
		out = append(out, items...)
		delete(w.held, id)
	}
	return out
}
