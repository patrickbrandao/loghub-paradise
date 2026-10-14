package sink

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

// WorkerConfig mirrors the [sink] batching tunables.
type WorkerConfig struct {
	Workers    int
	Batch      int
	OpTimeout  time.Duration
	BackoffMin time.Duration
	BackoffMax time.Duration

	// BatchMaxBytes is sink.batch_max_bytes: the JLH bytes one write may
	// carry. A dequeued batch is written in pieces under it (SPEC §8.1 step
	// 2); zero means no ceiling, which only tests use.
	BatchMaxBytes int64
}

// Workers move items from the ring to the sink. The same loop serves all three
// sinks -- the drain path has no idea which destination it is feeding.
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

	// dequeueLog samples the WARN of a failing Dequeue, which repeats every
	// backoff for as long as the ring's storage keeps failing.
	dequeueLog *obs.Sampler
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
		held: make(map[int][]*envelope.Item), dequeueLog: obs.NewSampler(30 * time.Second)}
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
	backoff := w.cfg.BackoffMin
	for {
		items, err := w.ring.Dequeue(ctx, w.cfg.Batch)
		if err != nil {
			if dequeueEnds(ctx, err) {
				// Shutdown or a closed ring: this worker is done.
				return
			}
			// The memory ring returns no other error; this keeps the rule of
			// the fast, whose ring=sqlite surfaces SQLITE_BUSY and I/O errors
			// from its select, and where returning here ended the worker for
			// good, in silence: the drain stopped, the ring filled and every
			// request got 429 with /healthz at 200 (SPEC §8.1 step 1; until
			// 2026-10-14). So it is logged (sampled: it repeats every
			// backoff), waited out with backoff, and retried.
			if ok, suppressed := w.dequeueLog.Allow(); ok {
				w.log.Warn("ring dequeue failed; retrying",
					"worker", id, "error", err, "retry_in", backoff.String(), "suppressed", suppressed)
			}
			if !sleepCtx(ctx, backoff) {
				return
			}
			if backoff *= 2; backoff > w.cfg.BackoffMax {
				backoff = w.cfg.BackoffMax
			}
			continue
		}
		backoff = w.cfg.BackoffMin
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

// dequeueEnds reports whether a Dequeue error ends the worker: the context
// ended or the ring is closed (the fast's ring=none, whose ErrNoBacklog is
// its third end, is not here).
func dequeueEnds(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, ring.ErrClosed)
}

// sleepCtx waits d, or less if ctx ends first; it reports whether the full
// wait happened.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// SplitByBytes returns how many items from the head of items form the next
// piece under maxBytes of JLH (SPEC §8.1 step 2): the piece closes when the
// next envelope would take the sum past the ceiling, and an envelope larger
// than the ceiling forms a piece alone -- it is never split nor refused for
// its size. It is always at least 1 for a non-empty slice; maxBytes <= 0 means
// no ceiling.
//
// The workers, the dump's reingestion and the ring.db drain all cut by this
// one rule, so a batch is the same size whichever path writes it.
func SplitByBytes(items []*envelope.Item, maxBytes int64) int {
	if len(items) == 0 {
		return 0
	}
	if maxBytes <= 0 {
		return len(items)
	}
	sum := int64(len(items[0].Env))
	n := 1
	for n < len(items) {
		size := int64(len(items[n].Env))
		if sum+size > maxBytes {
			break
		}
		sum += size
		n++
	}
	return n
}

// deliver writes one dequeued batch, piece by piece under sink.batch_max_bytes
// (SPEC §8.1 step 2), each piece retried until it succeeds, the failure turns
// out to be permanent, or the process is going down.
//
// It reports whether ownership of the whole batch MOVED ON -- delivered, or
// rejected for good. False means what is left -- the piece in retry and the
// pieces after it -- was abandoned and is still registered, and the caller
// must not fetch another batch.
//
// SPEC §10.4: on a retryable failure the worker HOLDS the batch in its own
// buffer and pulls nothing more from the ring. The ring is never modified -- it
// simply stops draining and fills, until 429 appears on its own. Consequences:
// no item is lost, nothing is reordered, and the ring never needs to support
// re-insertion at the head.
//
// Why the pieces: a batch was bounded by count alone, and 256 large bodies are
// hundreds of MiB that do not fit in sink.op_timeout -- the write timed out,
// was network, and the same batch was retried forever with the sink UP (SPEC
// §8.1, until 2026-10-14).
func (w *Workers) deliver(ctx context.Context, items []*envelope.Item, id int) bool {
	// Registered for the whole batch. Each piece leaves the registration ONLY
	// on the paths where its ownership actually moves on -- a successful
	// write, or a permanent rejection. It is deliberately not deferred: a
	// deferred clear would also fire when this worker ABANDONS the batch on
	// shutdown or on a fatal sink, which is precisely the case the
	// registration exists for -- and then the pieces not yet written are the
	// worker's as much as the one in retry: out of the ring, answered 200.
	w.hold(id, items)

	for rest := items; len(rest) > 0; {
		n := SplitByBytes(rest, w.cfg.BatchMaxBytes)
		if !w.writePiece(ctx, rest[:n], id) {
			return false
		}
		rest = rest[n:]
	}
	return true
}

// writePiece writes one piece of a batch with the retry of SPEC §10.4, and
// reports whether its ownership moved on.
func (w *Workers) writePiece(ctx context.Context, items []*envelope.Item, id int) bool {
	backoff := w.cfg.BackoffMin

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
			w.unhold(id, items)
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
			w.m.EventsDroppedTotal[obs.DropSinkLogic].Add(uint64(len(items)))
			ids := make([]string, 0, len(items))
			for _, it := range items {
				ids = append(ids, it.ID)
			}
			w.log.Error("sink rejected a batch permanently; the events are lost",
				"worker", id, "count", len(items), "error", err, "ids", ids)
			// Commit BEFORE Release, as on success: the ring subtracts
			// it.Size(), which is len(Env), and Release empties Env. The
			// other order left the ring counting bytes nobody held, until it
			// refused every request as full with nothing in it.
			_ = w.ring.Commit(items)
			for _, it := range items {
				it.Release()
			}
			w.unhold(id, items)
			return true
		}

		// Network or timeout: retry, holding the batch. A write cut short by
		// the worker's own context -- the shutdown -- is not a sink failure
		// and is not counted as one (SPEC §11.1).
		if ctx.Err() == nil {
			w.m.SinkErrorsTotal[errKind(err)].Inc()
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// The attempt ended by the clock, not by an answer. rueidis returns
			// early on a cancelled context but still sends the queued commands,
			// which are BinaryString views of these buffers, and a retry may
			// then succeed on another pipeline connection while the first is
			// still serialising. From here on the items are never pooled again
			// (SPEC §12.3): a successful retry hands them to the GC. A
			// connection error that is NOT a context error needs no taint:
			// rueidis fails what was queued on the broken pipe and does not
			// re-send writes on its own (its retry covers read-only
			// commands), so once Write has returned nothing below still
			// references the buffers.
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

// unhold removes a settled piece's items from the registration. They are a run
// of the held slice in dequeue order -- in practice its head -- so the common
// case is one splice; anything else falls back to a filter by identity, which
// can never drop an item that was not settled (port of go-syslog-pg-server).
func (w *Workers) unhold(id int, done []*envelope.Item) {
	if len(done) == 0 {
		return
	}
	w.holdMu.Lock()
	defer w.holdMu.Unlock()

	h := w.held[id]
	if i := indexRun(h, done); i >= 0 {
		n := copy(h[i:], h[i+len(done):])
		clear(h[i+n:])
		h = h[:i+n]
	} else {
		settled := make(map[*envelope.Item]struct{}, len(done))
		for _, it := range done {
			settled[it] = struct{}{}
		}
		out := h[:0]
		for _, it := range h {
			if _, ok := settled[it]; !ok {
				out = append(out, it)
			}
		}
		clear(h[len(out):])
		h = out
	}
	if len(h) == 0 {
		delete(w.held, id)
		return
	}
	w.held[id] = h
}

// indexRun returns where run appears contiguously in h, or -1.
func indexRun(h, run []*envelope.Item) int {
	for i := 0; i+len(run) <= len(h); i++ {
		if h[i] != run[0] {
			continue
		}
		match := true
		for j := 1; j < len(run); j++ {
			if h[i+j] != run[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
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
