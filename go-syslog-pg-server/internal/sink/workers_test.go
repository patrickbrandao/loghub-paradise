package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
)

func TestWorkersDeliverAndRelease(t *testing.T) {
	r := ring.NewMemory(1000, 1<<30, time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(16), discard(), m)

	const n = 100
	items := itemsOf(lines(n)...)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 10*time.Second, func() bool { return fs.count() == n })
	cancel()
	w.Wait()

	written, jlhs, _, _, _ := fs.snapshot()
	if len(written) != n {
		t.Fatalf("written %d of %d", len(written), n)
	}
	for i, b := range jlhs {
		if err := jlhcheck.Validate(b); err != nil {
			t.Fatalf("envelope %d is not a valid JLH: %v\n%s", i, err, b)
		}
	}
	if got := m.EventsWrittenTotal.Load(); got != n {
		t.Fatalf("metric says %d", got)
	}
	// Ownership transfers on a successful write and nowhere else.
	for _, it := range items {
		if !it.Released() {
			t.Fatalf("%s not released after delivery", it.Head.UUID)
		}
	}
	if len(w.Pending()) != 0 {
		t.Fatal("workers left something held after delivering everything")
	}
	// Commit before Release: the memory ring counts len(Raw), and Release nils
	// it. The other order would leave bytes the ring believes it still holds.
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("ring not drained: %+v", s)
	}
	if m.ConvertTotal[obs.FormatRFC5424].Load() != n || m.ConvertDuration.Count() == 0 {
		t.Fatal("conversion not observed")
	}
	if m.SinkBatchBytes.Count() == 0 || m.SinkBatchSize.Count() == 0 {
		t.Fatal("chunk size/bytes not observed")
	}
}

// A worker that retries HOLDS its chunk outside the ring. If shutdown did not
// collect it from Pending(), those messages -- whose TCP reading was already
// resumed -- would be in neither the ring nor the dump.
func TestWorkersPendingHoldsARetryingBatch(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newRecSink()
	fs.failWith = networkErr("write", errors.New("destination unreachable"))
	fs.failures = -1
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(4), discard(), m)

	items := itemsOf(line3164, line3164, line3164, line3164)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return m.SinkErrorsTotal[obs.SinkErrNetwork].Load() > 0 })
	cancel()
	w.Wait()

	held := w.Pending()
	if len(held) == 0 {
		t.Fatal("a retrying chunk was not registered for the emergency dump")
	}
	for _, it := range held {
		if it.Released() {
			t.Fatalf("%s was released while still pending", it.Head.UUID)
		}
	}
	if len(w.Pending()) != 0 {
		t.Fatal("Pending did not drain")
	}
}

// SPEC §10.2: the retry resends the SAME bytes with the same uuid -- the JLH is
// serialised once and reused, never rebuilt.
func TestWorkersRetrySendsTheSameBytes(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newRecSink()
	fs.failWith = networkErr("write", errors.New("temporary"))
	fs.failures = 3
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	it := newItem(line5424)
	enqueueAll(t, r, []*item.Item{it})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 10*time.Second, func() bool { return fs.count() == 1 })
	cancel()
	w.Wait()

	_, _, _, _, attempts := fs.snapshot()
	if len(attempts) != 4 {
		t.Fatalf("%d attempts, want 4", len(attempts))
	}
	for i := 1; i < len(attempts); i++ {
		if !bytes.Equal(attempts[i][0], attempts[0][0]) {
			t.Fatalf("attempt %d sent different bytes:\n%s\n%s", i, attempts[i][0], attempts[0][0])
		}
	}
	if !it.Released() {
		t.Fatal("item not released after the retry succeeded")
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 3 {
		t.Fatalf("network errors counted = %d, want 3", got)
	}
}

// A fatal write escalates immediately, and the chunk stays held for the dump.
func TestWorkersFatalHoldsAndEscalates(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newRecSink()
	fs.failWith = fatalErr("write", errors.New("OOM"))
	fs.failures = -1
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	it := newItem(line5424)
	enqueueAll(t, r, []*item.Item{it})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(5 * time.Second):
		t.Fatal("fatal write did not escalate")
	}
	w.Wait()

	if h.State() != StateFatal || h.Up() {
		t.Fatalf("state = %v", h.State())
	}
	if it.Released() {
		t.Fatal("a fatally-failed item was released instead of held for the dump")
	}
	if len(w.Pending()) != 1 {
		t.Fatal("the item is not registered for the dump")
	}
	if m.SinkErrorsTotal[obs.SinkErrFatal].Load() == 0 {
		t.Fatal("fatal not counted")
	}
}

// A logic failure is a permanent refusal of THIS data. It is dropped, loudly and
// counted, committed in the ring, and the worker carries on.
func TestWorkersLogicDropsAndContinues(t *testing.T) {
	r := ring.NewMemory(1000, 1<<30, time.Millisecond)
	fs := newRecSink()
	fs.failWith = logicErr("write", errors.New("constraint"))
	fs.failures = 1
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(1)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	bad, good := newItem(line5424), newItem(line3164)
	enqueueAll(t, r, []*item.Item{bad, good})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 10*time.Second, func() bool { return fs.count() == 1 })
	cancel()
	w.Wait()

	if got := m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if !bad.Released() {
		t.Fatal("a permanently refused item was not released")
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("the dropped item was not committed in the ring: %+v", s)
	}
	if len(w.Pending()) != 0 {
		t.Fatal("a dropped chunk is still registered")
	}
}

// SPEC §15.2: an attempt that ends by the clock taints the chunk. Even when the
// retry succeeds, neither the raw lines nor the chunk buffer go back to a pool.
func TestWorkersTaintAChunkAfterATimedOutAttempt(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	var attempts atomic.Int64
	var seenMu sync.Mutex
	var seen *byte
	hs := &hookSink{write: func(ctx context.Context, b *Batch) error {
		if attempts.Add(1) == 1 {
			seenMu.Lock()
			seen = &b.Envelopes[0].JLH[:1][0]
			seenMu.Unlock()
			<-ctx.Done()
			return networkErr("xadd", ctx.Err())
		}
		return nil
	}}
	m := metrics()
	h := NewHealth(hs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	cfg.OpTimeout = 20 * time.Millisecond
	w := NewWorkers(r, hs, h, cfg, discard(), m)

	items := itemsOf(line5424, line5424, line5424, line5424)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 4 })
	cancel()
	w.Wait()

	if attempts.Load() < 2 {
		t.Fatalf("only %d attempts; the stall never happened", attempts.Load())
	}
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() == 0 {
		t.Error(`the timed-out attempt was not counted in kind="timeout"`)
	}
	for i, it := range items {
		if !it.Released() {
			t.Fatalf("item %d not released after the retry succeeded", i)
		}
		if it.Raw == nil || !it.Tainted() {
			t.Errorf("item %d was pooled after a timed-out attempt; the first command may still be in flight", i)
		}
	}
	// The chunk buffer must never come back out of the pool.
	seenMu.Lock()
	defer seenMu.Unlock()
	for i := 0; i < 64; i++ {
		b := w.pool.get()
		if cap(b) > 0 && &b[:1][0] == seen {
			t.Fatal("the chunk buffer of a timed-out attempt was pooled")
		}
	}
}

// SPEC §10.1: a chunk closes when the sum of its JLH would pass
// sink.batch_max_bytes; the item that would pass it opens the next chunk.
func TestWorkersChunkByBatchMaxBytes(t *testing.T) {
	probe := batchOf(t, lines(1)...)
	size := probe.Bytes

	r := ring.NewMemory(1000, 0, 50*time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(64)
	cfg.Workers = 1
	cfg.BatchMaxBytes = 3*size + size/2 // room for three, not four
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	const n = 10
	items := itemsOf(lines(n)...)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == n })
	cancel()
	w.Wait()

	written, _, chunks, sizes, _ := fs.snapshot()
	if got := len(chunks); got != 4 {
		t.Fatalf("%d chunks %v, want 4 (3+3+3+1)", got, chunks)
	}
	for i, c := range chunks {
		if want := min(3, n-3*i); c != want {
			t.Errorf("chunk %d has %d envelopes, want %d", i, c, want)
		}
		if sizes[i] > cfg.BatchMaxBytes {
			t.Errorf("chunk %d has %d bytes, above the ceiling %d", i, sizes[i], cfg.BatchMaxBytes)
		}
	}
	for i, it := range items {
		if written[i] != it.Head.UUID.String() {
			t.Fatalf("order broken at %d", i)
		}
	}
	if got := m.SinkBatchSize.Count(); got != 4 {
		t.Errorf("syslog_sink_batch_size observed %d times, want once per chunk (4)", got)
	}
}

// A JLH larger than the ceiling forms a chunk alone, and nothing is skipped.
func TestWorkersOversizedJLHTravelsAlone(t *testing.T) {
	small := "<13>small"
	big := "<13>" + strings.Repeat("x", 3000)
	one := batchOf(t, small).Bytes

	r := ring.NewMemory(1000, 0, 50*time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(64)
	cfg.Workers = 1
	cfg.BatchMaxBytes = 2*one + one/2 // two small ones fit, the big one never does
	w := NewWorkers(r, fs, h, cfg, discard(), m)
	items := itemsOf(small, small, big, small)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 4 })
	cancel()
	w.Wait()

	_, jlhs, chunks, _, _ := fs.snapshot()
	if len(chunks) != 3 || chunks[0] != 2 || chunks[1] != 1 || chunks[2] != 1 {
		t.Fatalf("chunks = %v, want [2 1 1]", chunks)
	}
	if len(jlhs[2]) <= cfg.BatchMaxBytes {
		t.Fatalf("the big envelope is only %d bytes", len(jlhs[2]))
	}
	for i, b := range jlhs {
		if err := jlhcheck.Validate(b); err != nil {
			t.Fatalf("envelope %d: %v", i, err)
		}
	}
}

// SPEC §6.6 and §10.1: with accept_malformed=false a malformed message is
// counted, committed with its chunk and NEVER written.
func TestWorkersRefuseMalformed(t *testing.T) {
	r := ring.NewMemory(1000, 1<<30, time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(16)
	cfg.Workers = 1
	cfg.AcceptMalformed = false
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	good1, bad, good2 := newItem(line5424), newItem(lineMalformed), newItem(line3164)
	enqueueAll(t, r, []*item.Item{good1, bad, good2})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 2 && bad.Released() })
	cancel()
	w.Wait()

	written, _, _, _, _ := fs.snapshot()
	for _, u := range written {
		if u == bad.Head.UUID.String() {
			t.Fatal("a refused malformed message was written")
		}
	}
	if got := m.EventsDroppedTotal[obs.EventDropMalformed].Load(); got != 1 {
		t.Fatalf(`events_dropped_total{reason="malformed"} = %d, want 1`, got)
	}
	if got := m.ConvertTotal[obs.FormatMalformed].Load(); got != 1 {
		t.Fatalf(`convert_total{format="malformed"} = %d, want 1`, got)
	}
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("the refused message was not committed: %+v", s)
	}
}

// A dequeued batch of nothing but refused malformed messages is confirmed
// without a write.
func TestWorkersOnlyMalformedConfirmsWithoutWriting(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	var writes atomic.Int64
	hs := &hookSink{write: func(context.Context, *Batch) error { writes.Add(1); return nil }}
	m := metrics()
	h := NewHealth(hs, HealthConfig{}, discard(), m)
	cfg := workerCfg(16)
	cfg.Workers = 1
	cfg.AcceptMalformed = false
	w := NewWorkers(r, hs, h, cfg, discard(), m)

	items := itemsOf(lineMalformed, lineMalformed)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return items[0].Released() && items[1].Released() })
	cancel()
	w.Wait()

	if writes.Load() != 0 {
		t.Fatalf("%d writes for a chunk with no envelope", writes.Load())
	}
	if m.EventsDroppedTotal[obs.EventDropMalformed].Load() != 2 {
		t.Fatal("refused messages not counted")
	}
}

// The default accepts malformed: it is written, valid, with the whole line in
// message and raw.
func TestWorkersAcceptMalformed(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(16), discard(), m)

	enqueueAll(t, r, itemsOf(lineMalformed))
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 1 })
	cancel()
	w.Wait()

	_, jlhs, _, _, _ := fs.snapshot()
	if err := jlhcheck.Validate(jlhs[0]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(jlhs[0], []byte(`"format":"malformed"`)) {
		t.Fatalf("not malformed: %s", jlhs[0])
	}
}

// Every warning bit is counted under its own code (SPEC §14.1).
func TestWorkersCountConversionWarnings(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(16), discard(), m)

	// pri-missing and a message with no header at all.
	enqueueAll(t, r, itemsOf("<14>myapp[1]: no timestamp", "no pri at all"))
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 2 })
	cancel()
	w.Wait()

	if m.ConvertWarningsTotal[convert.WarnTimestampMissing].Load() == 0 {
		t.Error("timestamp-missing not counted")
	}
	if m.ConvertWarningsTotal[convert.WarnPRIMissing].Load() == 0 {
		t.Error("pri-missing not counted")
	}
}

// A worker that abandons a batch in the middle keeps exactly the undelivered
// rest registered: the chunk that was confirmed is released and gone, the chunk
// in failure and the items never converted stay pending and unreleased.
func TestWorkersAbandonMidBatchKeepsTheRestPending(t *testing.T) {
	probe := batchOf(t, lines(1)...)
	r := ring.NewMemory(1000, 0, 50*time.Millisecond)
	var writes atomic.Int64
	hs := &hookSink{write: func(context.Context, *Batch) error {
		if writes.Add(1) == 1 {
			return nil
		}
		return fatalErr("write", errors.New("table dropped"))
	}}
	m := metrics()
	h := NewHealth(hs, HealthConfig{}, discard(), m)
	cfg := workerCfg(6)
	cfg.Workers = 1
	cfg.BatchMaxBytes = 2*probe.Bytes + probe.Bytes/2
	w := NewWorkers(r, hs, h, cfg, discard(), m)

	items := itemsOf(lines(6)...)
	enqueueAll(t, r, items)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	<-h.Fatal()
	w.Wait()

	pending := w.Pending()
	if len(pending) != 4 {
		t.Fatalf("%d pending, want 4", len(pending))
	}
	for i, it := range items {
		if i < 2 && !it.Released() {
			t.Errorf("item %d was delivered but not released", i)
		}
		if i >= 2 && it.Released() {
			t.Errorf("item %d is pending but released", i)
		}
	}
	for i, it := range pending {
		if it != items[i+2] {
			t.Fatalf("pending[%d] is not items[%d]", i, i+2)
		}
	}
}

// SPEC §10.2: hold APPENDS, and unhold removes exactly the settled run.
func TestWorkersHoldAppendsAndUnholdRemovesOnlyTheRun(t *testing.T) {
	w := NewWorkers(ring.NewMemory(10, 0, 0), newRecSink(), nil, workerCfg(4), discard(), metrics())
	a, b := itemsOf("<13>a", "<13>b"), itemsOf("<13>c", "<13>d", "<13>e")
	w.hold(0, a)
	w.hold(0, b) // must not overwrite a
	w.unhold(0, b[:2])
	got := w.Pending()
	if len(got) != 3 || got[0] != a[0] || got[1] != a[1] || got[2] != b[2] {
		t.Fatalf("pending = %v", got)
	}

	w.hold(1, a)
	w.unhold(1, []*item.Item{b[0]}) // never held: nothing may disappear
	if len(w.Pending()) != 2 {
		t.Fatal("unhold of an unknown item removed something")
	}
}

// flakyRing fails Dequeue with an error that is not an end of the ring a few
// times, then behaves. The memory ring has no such error today; the worker's
// defence against one is what is under test.
type flakyRing struct {
	ring.Ring
	failures atomic.Int64
	err      error
}

func (r *flakyRing) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	if r.failures.Add(-1) >= 0 {
		return nil, r.err
	}
	return r.Ring.Dequeue(ctx, n)
}

// Review finding 1: a Dequeue error that is not the end of the ring must not
// end the worker. With every worker gone the ring fills and the transports
// stop, and nothing says why. It is waited out with backoff and the worker
// keeps draining, before and after.
func TestWorkersSurviveTransientDequeueErrors(t *testing.T) {
	fr := &flakyRing{Ring: ring.NewMemory(1000, 0, time.Millisecond),
		err: fmt.Errorf("ring: %w", errors.New("transient failure"))}
	fr.failures.Store(6)
	fs := newRecSink()
	m := metrics()
	cfg := workerCfg(8) // two workers, both hit the failures
	w := NewWorkers(fr, fs, NewHealth(fs, HealthConfig{}, discard(), m), cfg, discard(), m)

	enqueueAll(t, fr, itemsOf(lines(5)...))
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 5 })
	if fr.failures.Load() >= 0 {
		t.Fatal("the failures were never hit")
	}
	// Still alive: a second wave is drained too.
	enqueueAll(t, fr, itemsOf(lines(5)...))
	waitFor(t, 5*time.Second, func() bool { return fs.count() == 10 })
	cancel()
	w.Wait()
}

// The ends of a worker stay ends: a closed ring, wrapped or not, and the
// shutdown -- even while the worker is backing off from a Dequeue error that
// is not an end, which is the control: that error alone keeps it alive
// (TestWorkersSurviveTransientDequeueErrors).
func TestWorkersStopOnTheEndsOfTheRing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		shutdown bool
	}{
		{"closed", ring.ErrClosed, false},
		{"wrapped closed", fmt.Errorf("wrapped: %w", ring.ErrClosed), false},
		{"shutdown while backing off", errors.New("transient failure"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr := &flakyRing{Ring: ring.NewMemory(10, 0, 0), err: tc.err}
			fr.failures.Store(1 << 30)
			fs := newRecSink()
			m := metrics()
			cfg := workerCfg(4)
			cfg.BackoffMin, cfg.BackoffMax = time.Hour, time.Hour // only the context can cut it short
			w := NewWorkers(fr, fs, NewHealth(fs, HealthConfig{}, discard(), m), cfg, discard(), m)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w.Start(ctx)
			done := make(chan struct{})
			go func() { w.Wait(); close(done) }()
			if tc.shutdown {
				select {
				case <-done:
					t.Fatal("a Dequeue error that is not an end stopped the workers")
				case <-time.After(50 * time.Millisecond):
				}
				cancel()
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("%v did not end the workers", tc.err)
			}
		})
	}
}

// recHandler keeps the records it receives.
type recHandler struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *recHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}
func (h *recHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recHandler) WithGroup(string) slog.Handler      { return h }

// attr returns the value of key in the first record whose message contains
// msg.
func (h *recHandler) attr(msg, key string) (slog.Value, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if !strings.Contains(r.Message, msg) {
			continue
		}
		var v slog.Value
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				v, found = a.Value, true
				return false
			}
			return true
		})
		return v, found
	}
	return slog.Value{}, false
}

// Review finding 8: when the destination stored part of a chunk before
// refusing the rest (Error.Delivered), only the rest is counted and logged as
// lost; the stored part is written.
func TestWorkersCountOnlyTheUndeliveredOnLogic(t *testing.T) {
	r := ring.NewMemory(1000, 0, 50*time.Millisecond)
	var calls atomic.Int64
	hs := &hookSink{write: func(context.Context, *Batch) error {
		if calls.Add(1) == 1 {
			return &Error{Class: ClassLogic, Op: "insert", Err: errors.New("data too long"), Delivered: []int{0, 2}}
		}
		return nil
	}}
	m := metrics()
	h := &recHandler{}
	cfg := workerCfg(16)
	cfg.Workers = 1
	w := NewWorkers(r, hs, NewHealth(hs, HealthConfig{}, discard(), m), cfg, slog.New(h), m)

	items := itemsOf(lines(4)...)
	enqueueAll(t, r, items)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return items[3].Released() })
	cancel()
	w.Wait()

	if got := m.EventsWrittenTotal.Load(); got != 2 {
		t.Errorf("events_written_total = %d, want 2 (the stored ones)", got)
	}
	if got := m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); got != 2 {
		t.Errorf(`events_dropped_total{reason="sink_logic"} = %d, want 2 (only the undelivered)`, got)
	}
	v, ok := h.attr("rejected a chunk permanently", "uuids")
	if !ok {
		t.Fatal("no log line for the refusal")
	}
	lost := fmt.Sprint(v.Any())
	if !strings.Contains(lost, items[1].Head.UUID.String()) || !strings.Contains(lost, items[3].Head.UUID.String()) ||
		strings.Contains(lost, items[0].Head.UUID.String()) || strings.Contains(lost, items[2].Head.UUID.String()) {
		t.Errorf("logged as lost: %s; want exactly items 1 and 3", lost)
	}
	for _, it := range items {
		if !it.Released() {
			t.Fatal("the refused chunk was not settled as a whole")
		}
	}
}
