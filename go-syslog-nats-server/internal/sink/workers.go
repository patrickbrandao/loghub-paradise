package sink

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ring"
)

// WorkerConfig mirrors the [sink] batching tunables plus what the conversion
// needs, because the workers are where the conversion runs (SPEC §2.2).
type WorkerConfig struct {
	Workers int
	// Batch is sink.batch: items per Dequeue.
	Batch int
	// BatchMaxBytes is sink.batch_max_bytes: the JLH ceiling of one chunk
	// (SPEC §10.1). The JLH reaches 12 times the raw line, so without it
	// workers × batch × worst JLH would pass 1 GiB (SPEC §15.3).
	BatchMaxBytes int
	OpTimeout     time.Duration
	BackoffMin    time.Duration
	BackoffMax    time.Duration

	// Convert is the [syslog] conversion config, frozen at boot (SPEC §3.5).
	Convert convert.Config
	// AcceptMalformed is syslog.accept_malformed. false refuses format
	// "malformed": counted, committed with its chunk, never written (SPEC §6.6).
	AcceptMalformed bool
}

func (cfg *WorkerConfig) normalise() {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Batch < 1 {
		cfg.Batch = 1
	}
	if cfg.BatchMaxBytes < 1 {
		cfg.BatchMaxBytes = 16 << 20
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = 100 * time.Millisecond
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = cfg.BackoffMin
	}
}

// Workers move items from the ring to the sink.
//
// Each worker owns a builder, so its Converter's scratch stays allocated for the
// life of the process rather than per batch.
type Workers struct {
	ring   ring.Ring
	sink   Sink
	health *Health
	cfg    WorkerConfig
	log    *slog.Logger
	m      *obs.Metrics
	pool   *bufPool

	malformedLog *obs.Sampler
	dequeueLog   *obs.Sampler

	wg sync.WaitGroup

	// held records what each worker took out of the ring and has not finished.
	//
	// SPEC §10.2 has a failing worker HOLD its chunk and stop pulling from the
	// ring. Those items are no longer in the ring, so a shutdown drain would not
	// find them and they would vanish -- and on TCP they are messages whose
	// reading was already resumed. Registering them here is what lets Pending()
	// hand them to the emergency dump of SPEC §12. The registration covers the
	// whole dequeued batch, chunks not yet converted included, and shrinks as
	// each chunk is confirmed.
	holdMu sync.Mutex
	held   map[int][]*item.Item
}

// NewWorkers builds the drain workers. health may not be nil.
func NewWorkers(r ring.Ring, s Sink, h *Health, cfg WorkerConfig, log *slog.Logger, m *obs.Metrics) *Workers {
	cfg.normalise()
	return &Workers{
		ring: r, sink: s, health: h, cfg: cfg, log: log, m: m,
		pool:         newBufPool(cfg.BatchMaxBytes),
		malformedLog: obs.NewSampler(30 * time.Second),
		dequeueLog:   obs.NewSampler(30 * time.Second),
		held:         make(map[int][]*item.Item),
	}
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
	b := newBuilder(w.cfg.Convert, w.cfg.BatchMaxBytes, w.cfg.AcceptMalformed, w.pool, w.m)
	defer b.release()

	backoff := w.cfg.BackoffMin
	for {
		items, err := w.ring.Dequeue(ctx, w.cfg.Batch)
		if err != nil {
			if dequeueEnds(ctx, err) {
				// Shutdown or a closed ring: this worker is done.
				return
			}
			// The memory ring has no other error. Should one appear anyway,
			// returning here would end this worker for good, silently: with
			// all of them gone the ring fills, the transports stop, and
			// nothing in the process says why. So it is logged (sampled: it
			// repeats every backoff), waited out with backoff, and retried.
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
		if !w.deliver(ctx, b, items, id) {
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

// dequeueEnds reports whether a Dequeue error ends the worker: the context
// ended, or the ring is closed.
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

// deliver converts one dequeued batch chunk by chunk, writes each chunk, and
// retries until it succeeds, the failure turns out to be permanent, or the
// process is going down (SPEC §10.1, §10.2).
//
// It reports whether ownership of the whole batch MOVED ON -- delivered, or
// rejected for good. False means what is left was abandoned and is still
// registered, and the caller must not fetch another batch.
//
// On a retryable failure the worker HOLDS the chunk and pulls nothing more from
// the ring. The ring is never modified -- it simply stops draining and fills,
// until the transport's own backpressure appears. Consequences: no item is
// lost, nothing is reordered, and the ring never needs re-insertion at the head.
func (w *Workers) deliver(ctx context.Context, b *builder, items []*item.Item, id int) bool {
	// Registered for the whole batch, before any conversion. The registration
	// is cleared ONLY on the paths where ownership actually moves on -- a
	// confirmed chunk, or a permanent rejection. It is deliberately not
	// deferred: a deferred clear would also fire when this worker ABANDONS the
	// batch on shutdown or on a fatal sink, which is precisely the case the
	// registration exists for.
	w.hold(id, items)

	rest := items
	for {
		n := b.fill(rest)
		rest = rest[n:]
		c := b.cur
		if len(c.items) == 0 {
			// Nothing left: fill only stops early when an item crossed the
			// ceiling, and that item is then waiting in the next chunk.
			return true
		}
		if !w.writeChunk(ctx, c, id) {
			return false
		}
		b.advance()
	}
}

// writeChunk delivers one sealed chunk, retrying with backoff. The JLH is
// serialised ONCE, before the retry loop: a retry resends the same bytes with
// the same uuid, which is the JLH outbox rule, and redoing the conversion would
// burn CPU at the exact moment the destination is already struggling
// (SPEC §10.2).
func (w *Workers) writeChunk(ctx context.Context, c *chunk, id int) bool {
	c.seal()
	if len(c.Envelopes) == 0 {
		// Only refused malformed messages: nothing to write, everything to
		// confirm (SPEC §10.1).
		w.confirm(c, id)
		return true
	}
	w.m.SinkBatchSize.Observe(int64(len(c.Envelopes)))
	w.m.SinkBatchBytes.Observe(int64(c.Bytes))

	backoff := w.cfg.BackoffMin
	for attempt := 1; ; attempt++ {
		wctx, cancel := context.WithTimeout(ctx, w.cfg.OpTimeout)
		start := time.Now()
		err := w.sink.Write(wctx, &c.Batch)
		elapsed := time.Since(start)
		cancel()

		// Observed even when it fails: the duration of a failing write is
		// exactly the signal that says whether the destination is slow or
		// refusing outright.
		w.m.SinkDuration.ObserveDuration(elapsed)

		if err == nil {
			w.m.EventsWrittenTotal.Add(uint64(len(c.Envelopes)))
			w.confirm(c, id)
			return true
		}
		if taints(err) {
			// The attempt ended by the clock, not by an answer, or the
			// destination says a lower layer may still read the buffer: a
			// JetStream future may still be pending. From here on nothing in this chunk is pooled
			// again (SPEC §15.2), not even when a retry succeeds: the GC takes
			// it instead.
			c.taint()
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
			// Retrying cannot help, so drop it -- loudly, and counted. What the
			// destination stored before refusing (Error.Delivered) is written,
			// not lost: counting it lost would report a loss that did not
			// happen.
			stored, lost := settleLogic(err, c.Envelopes)
			w.m.SinkErrorsTotal[obs.SinkErrLogic].Inc()
			w.m.EventsWrittenTotal.Add(uint64(stored))
			w.m.EventsDroppedTotal[obs.EventDropSinkLogic].Add(uint64(len(lost)))
			w.log.Error("sink rejected a chunk permanently; the events are lost",
				"worker", id, "count", len(lost), "stored", stored, "error", err, "uuids", lost)
			w.confirm(c, id)
			return true
		}

		// Network or timeout: retry, holding the chunk. A write cut short by
		// the worker's own context -- the shutdown -- is not a sink failure
		// and is not counted as one (SPEC §14.1).
		if ctx.Err() != nil {
			// Shutdown while retrying. Do NOT release: these items are still
			// pending and belong in the emergency dump.
			w.log.Warn("shutdown while retrying a chunk; items remain pending",
				"worker", id, "count", len(c.items))
			return false
		}
		w.m.SinkErrorsTotal[errKind(err)].Inc()
		if attempt == 1 || attempt%10 == 0 {
			w.log.Warn("sink write failed, retrying",
				"worker", id, "attempt", attempt, "count", len(c.Envelopes),
				"error", err, "retry_in", backoff.String())
		}
		if !sleepCtx(ctx, backoff) {
			return false
		}
		if backoff *= 2; backoff > w.cfg.BackoffMax {
			backoff = w.cfg.BackoffMax
		}
	}
}

// confirm is the end of a chunk whose ownership moved on: delivered, refused
// for good, or refused as malformed. Commit comes BEFORE Release: the memory
// ring's byte accounting reads cap(Raw), and Release nils it -- the other order
// would leave the ring believing it still holds bytes nobody holds, until it
// refused everything as full.
func (w *Workers) confirm(c *chunk, id int) {
	if err := w.ring.Commit(c.items); err != nil {
		w.log.Error("ring commit failed after the chunk was settled; "+
			"the ring may deliver these events again",
			"worker", id, "count", len(c.items), "error", err)
	}
	if n := len(c.refused); n > 0 {
		w.m.EventsDroppedTotal[obs.EventDropMalformed].Add(uint64(n))
		if ok, suppressed := w.malformedLog.Allow(); ok {
			w.log.Warn("malformed messages refused (syslog.accept_malformed=false)",
				"worker", id, "count", n, "first_uuid", c.refused[0].Head.UUID.String(),
				"suppressed", suppressed)
		}
	}
	// Ownership transfers here and nowhere else: the chunk is settled, so the
	// raw-line buffers go back to the pool exactly once (SPEC §15.2) -- or to
	// the GC when an earlier attempt tainted them.
	for _, it := range c.items {
		it.Release()
	}
	w.unhold(id, c.items)
}

// hold registers the batch a worker is working on.
//
// It APPENDS rather than assigns. The loop already guarantees a worker never
// arrives here still holding something, so in practice this only ever writes
// into an empty slot -- but an assignment would make a future mistake in that
// guarantee cost silently discarded events, while an append costs a slice that
// is one batch too long and loses nothing (SPEC §10.2).
func (w *Workers) hold(id int, items []*item.Item) {
	w.holdMu.Lock()
	w.held[id] = append(w.held[id], items...)
	w.holdMu.Unlock()
}

// unhold removes a settled chunk's items from the registration. They are a run
// of the held slice in dequeue order -- in practice its head -- so the common
// case is one splice; anything else falls back to a filter by identity, which
// can never drop an item that was not settled.
func (w *Workers) unhold(id int, done []*item.Item) {
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
		settled := make(map[*item.Item]struct{}, len(done))
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
func indexRun(h, run []*item.Item) int {
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

// Pending returns what the workers were still holding when they stopped: the
// chunk in retry and the rest of its dequeued batch.
//
// Call it only after Wait: a running worker may still deliver its chunk, and
// taking the items from under it would either duplicate them or release their
// buffers twice.
func (w *Workers) Pending() []*item.Item {
	w.holdMu.Lock()
	defer w.holdMu.Unlock()

	var out []*item.Item
	for id, items := range w.held {
		out = append(out, items...)
		delete(w.held, id)
	}
	return out
}
