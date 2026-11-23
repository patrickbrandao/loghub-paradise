package sink

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// WorkerConfig mirrors the [sink] batching tunables.
type WorkerConfig struct {
	Workers    int
	Batch      int
	OpTimeout  time.Duration
	BackoffMin time.Duration
	BackoffMax time.Duration

	// NodeID and Parse are what the full parse needs. They live here because
	// the workers are where the parse runs (SPEC §2.2), which is the one
	// structural difference from the sibling service's drain path.
	NodeID string
	Parse  syslog.Options
}

// Workers move items from the ring to the sink. The same loop serves all four
// sinks -- the drain path has no idea which destination it is feeding.
//
// Each worker owns a Prep, so the parser's structured-data slices and unescape
// scratch stay allocated for the life of the process rather than per batch.
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
	// SPEC §10.2 has a failing worker HOLD its batch and stop pulling from the
	// ring. That means those items are no longer in the ring, so a shutdown
	// drain would not find them and they would vanish -- and on RELP they are
	// messages an emitter was already told "200 OK" for. Registering them here
	// is what lets Pending() hand them to the emergency dump of SPEC §11.
	holdMu sync.Mutex
	held   map[int][]*envelope.Item

	// surrendered is set, under holdMu, by Pending: from then on the
	// registration belongs to the shutdown, and a worker whose write returns
	// late settles nothing (settle, SPEC §12.3).
	surrendered bool
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

// WaitFor waits up to d for every worker to return, and reports whether they
// all did.
//
// The shutdown waits with this, not Wait (SPEC §12.3). Cancelling the
// workers' context ends a write only if the write honours it, and not every
// one does: a COMMIT stuck in fsync does not, and a NATS publish did not
// until its socket had a deadline. An unbounded Wait held the shutdown there
// -- no dump, no exit 3 -- until the orchestrator's SIGKILL took the ring
// with nothing counting it (review of 2026-11-21, item 016). A false return
// leaves those workers running; Pending then takes what they hold away from
// them.
func (w *Workers) WaitFor(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

func (w *Workers) loop(ctx context.Context, id int) {
	prep := NewPrep(w.cfg.NodeID, w.cfg.Parse, w.m)
	defer prep.Release()

	for {
		if ctx.Err() != nil {
			// The shutdown. Checked before every Dequeue, not left to it:
			// Dequeue races the cancellation against a ring that still holds
			// a backlog, and a worker that came back late from a stuck write
			// (WaitFor gave up on it) would take a batch out of the ring
			// after the shutdown collected what the workers hold -- in
			// neither the ring nor the dump.
			return
		}
		items, err := w.ring.Dequeue(ctx, w.cfg.Batch)
		if err != nil {
			// ring=none parks here until shutdown; every other mode only gets
			// an error when the context ends.
			return
		}
		if len(items) == 0 {
			continue
		}
		if !w.deliver(ctx, prep, items, id) {
			// deliver ABANDONED the batch and left it registered for the
			// emergency dump. This worker must stop here.
			//
			// Going round again would call Dequeue with a cancelled context,
			// and Dequeue's first select races that cancellation against a ring
			// that still holds a backlog. Go picks a ready case at random, so
			// the channel wins about half the time: the worker returns with a
			// second batch and hands it to deliver, which registers it over the
			// batch it was still holding. Those messages were confirmed and end
			// up in neither the ring nor the dump.
			return
		}
	}
}

// deliver parses one batch, writes it, and retries until it succeeds, the
// failure turns out to be permanent, or the process is going down.
//
// It reports whether ownership of the batch MOVED ON -- delivered, or rejected
// for good. False means the batch was abandoned and is still registered, and the
// caller must not fetch another one.
//
// SPEC §10.2: on a retryable failure the worker HOLDS the batch in its own
// buffer and pulls nothing more from the ring. The ring is never modified -- it
// simply stops draining and fills, until the transport's own backpressure
// appears on its own. Consequences: no item is lost, nothing is reordered, and
// the ring never needs to support re-insertion at the head.
func (w *Workers) deliver(ctx context.Context, prep *Prep, items []*envelope.Item, id int) bool {
	// Parsed ONCE, before the retry loop. The parse is deterministic from head
	// plus raw line, so repeating it per attempt would burn CPU proportional to
	// the batch at the exact moment the destination is already struggling.
	batch := prep.Build(items)
	// The envelope buffers go back on every path, including abandonment: the
	// emergency dump stores head and raw line and has no use for them. The one
	// exception is a batch tainted by a timed-out attempt (below), whose
	// envelopes may still be on their way through the pipeline and are left to
	// the GC instead (SPEC §14.2).
	tainted := false
	defer func() {
		if tainted {
			prep.Abandon()
		} else {
			prep.Release()
		}
	}()

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
		err := w.sink.Write(wctx, batch)
		elapsed := time.Since(start)
		cancel()

		w.m.SinkDuration.ObserveDuration(elapsed)
		w.m.SinkBatchSize.Observe(int64(len(batch)))

		if err == nil {
			w.m.EventsWrittenTotal.Add(uint64(len(items)))
			if !w.settle(id) {
				w.log.Warn("a write the shutdown gave up on was delivered after all; "+
					"its events were collected by the shutdown too",
					"worker", id, "count", len(items))
				return false
			}
			if err := w.ring.Commit(items); err != nil {
				w.log.Error("ring commit failed after a successful write; "+
					"the events are delivered but the ring may redeliver them",
					"worker", id, "error", err)
			}
			// Ownership transfers here and nowhere else: the batch is confirmed,
			// so the raw-line buffers go back to the pool exactly once
			// (SPEC §14.2).
			for _, it := range items {
				it.Release()
			}
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
			// Retrying cannot help, so drop it -- loudly, and counted.
			w.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
			if !w.settle(id) {
				// The shutdown already took the batch, refused messages
				// included: counted here, they would count again when the
				// reingestion meets the same refusal.
				return false
			}
			w.m.EventsDroppedTotal[obs.EventDropSinkLogic].Add(uint64(len(items)))
			ids := make([]string, 0, len(items))
			for _, it := range items {
				ids = append(ids, it.Head.ID)
			}
			w.log.Error("sink rejected a batch permanently; the events are lost",
				"worker", id, "count", len(items), "error", err, "ids", ids)
			for _, it := range items {
				it.Release()
			}
			_ = w.ring.Commit(items)
			return true
		}

		// Network or timeout: retry, holding the batch. A write cut short by
		// the worker's own context -- the shutdown -- is not a sink failure
		// and is not counted as one (SPEC §13.1).
		if ctx.Err() == nil {
			w.m.SinkErrorsTotal[errKind(err)].Inc()
		}
		if isContextErr(err) {
			// The attempt ended by the clock, not by an answer. rueidis returns
			// early on a cancelled context but still sends the queued commands
			// -- BinaryString views of these lines and envelopes -- and a retry
			// may then succeed on another pipeline connection while the first
			// is still serialising. From here on nothing in this batch is
			// pooled again (SPEC §14.2): a successful retry hands it to the GC.
			// A connection error that is NOT a context error needs no taint:
			// rueidis fails what was queued on the broken pipe and does not
			// re-send writes on its own (its retry covers read-only
			// commands), so once Write has returned nothing below still
			// references the buffers.
			tainted = true
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

// settle takes a batch whose ownership is moving on out of the registration,
// and reports whether it may move on. False means Pending has already taken
// the registration while this worker was still inside its write (WaitFor gave
// up on it): the batch is the shutdown's, and the caller must neither Commit
// nor Release it -- a Release would hand buffers the dump reads back to the
// pool, a Commit would delete rows ring=sqlite keeps for the next boot -- and
// must stop (SPEC §12.3).
//
// It runs BEFORE the Commit and the Release, under the same lock as Pending,
// so the two cannot both take a batch.
func (w *Workers) settle(id int) bool {
	w.holdMu.Lock()
	defer w.holdMu.Unlock()
	if w.surrendered {
		return false
	}
	delete(w.held, id)
	return true
}

// Held counts the items the workers hold right now, without taking them: what
// a second signal loses on top of the ring (SPEC §12.3). Safe while the
// workers run.
func (w *Workers) Held() int {
	w.holdMu.Lock()
	defer w.holdMu.Unlock()
	n := 0
	for _, items := range w.held {
		n += len(items)
	}
	return n
}

// Pending hands over the batches the workers hold and ends the workers' claim
// on them.
//
// Call it after Wait, or after a WaitFor that gave up. In the second case a
// worker may still be inside a write that ignores its context; settle makes
// sure that when the write returns the worker neither commits nor releases
// what is now the shutdown's, and every item is tainted, so that no Release
// pools a buffer the stuck write may still be reading (SPEC §12.3, §14.2). A
// write that still delivers puts its messages in the destination and in the
// dump (or, with ring=sqlite, delivers them again at the next boot):
// at-least-once.
func (w *Workers) Pending() []*envelope.Item {
	w.holdMu.Lock()
	defer w.holdMu.Unlock()
	w.surrendered = true

	var out []*envelope.Item
	for id, items := range w.held {
		out = append(out, items...)
		delete(w.held, id)
	}
	for _, it := range out {
		it.Taint()
	}
	return out
}
