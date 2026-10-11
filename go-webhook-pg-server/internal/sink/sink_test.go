package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func testMetrics() *obs.Metrics {
	return obs.NewMetrics("test", "none", "go", "memory", "test")
}

// testFrom is the AgentUUID every test envelope carries (SPEC §3.5).
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// testRequest is a plausible POST as the handler resolves it before
// serialising anything (SPEC §3.2).
func testRequest(path string, body []byte) *envelope.Request {
	return &envelope.Request{Mode: envelope.ModeFast,
		Src: netip.MustParseAddr("203.0.113.9"), Dst: netip.MustParseAddr("127.0.0.1"),
		SPort: 5555, DPort: 8080, PeerAddr: netip.MustParseAddr("203.0.113.9"), PeerPort: 5555,
		SrcSource: "tcp", Proto: "HTTP/1.1", Method: "POST", Host: "example.com", Path: path,
		RawQuery: "a=1", Header: http.Header{"Content-Type": {"application/json"}},
		HostHeader: "example.com", ContentType: "application/json", Body: body}
}

// itemIn serialises r into a JLH of the given space, the way the handler
// does: one clock sample for the uuid and the event, and the item owns the
// bytes.
func itemIn(space string, r *envelope.Request) *envelope.Item {
	now := time.Now()
	h := envelope.Header{UUID: envelope.NewUUID(now), From: testFrom, Space: space,
		Type: envelope.Type, Level: envelope.Level, SID: 1, Event: envelope.EventAt(now)}
	env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(r.Body))
	return envelope.NewItem(h.UUID.String(), env, -1)
}

// mkItem is a valid envelope of the default space whose path carries tag, so
// a failing test can say which event it was looking at.
func mkItem(tag string) *envelope.Item {
	return itemIn("default", testRequest("/"+tag, []byte(`{"tag":"`+tag+`"}`)))
}

func mkBatch(n int) []*envelope.Item {
	out := make([]*envelope.Item, n)
	for i := range out {
		out[i] = mkItem(fmt.Sprintf("b%d", i))
	}
	return out
}

// sizedItem carries a text body of exactly n bytes, so its envelope grows
// with n one for one.
func sizedItem(n int) *envelope.Item {
	return itemIn("default", testRequest("/sized", bytes.Repeat([]byte{'x'}, n)))
}

// badItem is an item whose Env is not a JLH this server wrote: the envelope
// of the pre-LogHub format, as a ring.db or a dump of the previous version
// would hold it. Every sink must refuse it as data (logic) and send nothing.
func badItem() *envelope.Item {
	return envelope.NewItem("0198f1c2-0000-7000-8000-000000000001",
		[]byte(`{"id":"0198f1c2-0000-7000-8000-000000000001","v":1,"ts":1757116800,"method":"POST"}`), -1)
}

// malformedPayloadItem passes envelope.ParseHead -- the header is exact and
// the payload is one balanced object -- but its payload is not JSON. Only a
// destination that parses the payload notices, and the refusal is of this
// data alone: logic.
func malformedPayloadItem(t *testing.T) *envelope.Item {
	t.Helper()
	good := mkItem("malformed")
	i := bytes.Index(good.Env, []byte(`"payload":`)) + len(`"payload":`)
	env := append(append([]byte(nil), good.Env[:i]...), `{"mode"}}`...)
	if _, _, err := envelope.ParseHead(env); err != nil {
		t.Fatalf("the malformed-payload fixture must pass ParseHead: %v", err)
	}
	return envelope.NewItem(good.ID, env, -1)
}

// validJLH runs the contract oracle on an envelope read back from a
// destination (SPEC §15.1).
func validJLH(t *testing.T, raw []byte) *jlhcheck.Envelope {
	t.Helper()
	e, err := jlhcheck.Validate(raw)
	if err != nil {
		t.Fatalf("envelope read back is not a valid webhook JLH: %v\n%s", err, raw)
	}
	return e
}

// sameJSON reports whether a and b are the same JSON value, numbers compared
// exactly: what a destination that normalises the document (jsonb, MySQL
// JSON) is required to keep.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	decode := func(raw []byte) any {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, raw)
		}
		return v
	}
	ja, _ := json.Marshal(decode(a))
	jb, _ := json.Marshal(decode(b))
	return bytes.Equal(ja, jb)
}

// The fixtures themselves: an item built here is a valid webhook,
// and the refusal fixtures are refused for the reason they are named after.
// A helper that produced invalid envelopes would make every refusal test
// below pass for the wrong reason.
func TestFixturesAreWhatTheyClaim(t *testing.T) {
	for _, it := range append(mkBatch(3), sizedItem(10), itemIn("acme.store", testRequest("/x", nil))) {
		e := validJLH(t, it.Env)
		if e.UUID != it.ID || e.From != testFrom {
			t.Errorf("item %s carries uuid %s from %s", it.ID, e.UUID, e.From)
		}
	}
	if e := validJLH(t, mkItem("tagged").Env); e.Payload.Path != "/tagged" {
		t.Errorf("mkItem path = %q, want the tag", e.Payload.Path)
	}
	if _, _, err := envelope.ParseHead(badItem().Env); err == nil {
		t.Error("badItem passes ParseHead; the refusal tests would prove nothing")
	}
	if json.Valid(malformedPayloadItem(t).Env) {
		t.Error("malformedPayloadItem is valid JSON")
	}
	seen := map[string]bool{}
	for _, it := range mkBatch(500) {
		if seen[it.ID] {
			t.Fatalf("two items share uuid %s", it.ID)
		}
		seen[it.ID] = true
	}
}

// ------------------------------------------------------- error classification

func TestClassOfDefaultsToNetwork(t *testing.T) {
	// An unwrapped error must be retried, not dropped: guessing "permanent"
	// loses data, guessing "transient" only fills the ring, which is visible.
	if got := ClassOf(errors.New("bare")); got != ClassNetwork {
		t.Errorf("class = %s, want network", got)
	}
	if IsFatal(errors.New("bare")) {
		t.Error("a bare error must not be fatal")
	}
	if !IsFatal(fatalErr("op", errors.New("x"))) {
		t.Error("a fatal error must report as fatal")
	}
}

// ------------------------------------------------------------------- health

type scriptedSink struct {
	mu       sync.Mutex
	healthy  bool
	writeErr error
	writes   atomic.Int64
}

func (s *scriptedSink) Write(ctx context.Context, items []*envelope.Item) error {
	s.writes.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeErr
}

func (s *scriptedSink) Health(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healthy {
		return nil
	}
	return errors.New("sink is down")
}

func (s *scriptedSink) Close() error { return nil }
func (s *scriptedSink) Type() string { return "scripted" }

func (s *scriptedSink) setHealthy(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = v
}

func TestHealthTransitions(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{
		Interval: 10 * time.Millisecond, Timeout: 50 * time.Millisecond,
		FailAfter: time.Hour, MaxRetries: 1000,
	}, quietLog(), m)

	if !h.Up() {
		t.Fatal("a new Health should start UP")
	}
	if m.SinkUp.Load() != 1 {
		t.Error("whs_sink_up should be 1 while UP")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	ss.setHealthy(false)
	if !eventually(t, 2*time.Second, func() bool { return !h.Up() }) {
		t.Fatal("the sink never went DOWN")
	}
	if m.SinkUp.Load() != 0 {
		t.Error("whs_sink_up should be 0 while DOWN")
	}

	ss.setHealthy(true)
	if !eventually(t, 2*time.Second, func() bool { return h.Up() }) {
		t.Fatal("the sink never recovered")
	}
	if m.SinkUp.Load() != 1 {
		t.Error("whs_sink_up should return to 1 on recovery")
	}
}

// SPEC §10.2: DOWN persisting past either fuse becomes FATAL.
func TestHealthFatalAfterMaxRetries(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 20 * time.Millisecond,
		FailAfter:  time.Hour, // the count is what should trip
		MaxRetries: 3,
	}, quietLog(), testMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("FATAL never fired after the retry limit")
	}
	if h.State() != StateFatal {
		t.Errorf("state = %s, want fatal", h.State())
	}
	if h.FatalErr() == nil {
		t.Error("FatalErr should report the cause")
	}
}

func TestHealthFatalAfterTimeout(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 20 * time.Millisecond,
		FailAfter:  80 * time.Millisecond,
		MaxRetries: 100000, // the clock is what should trip
	}, quietLog(), testMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("FATAL never fired after fail_timeout")
	}
}

// MarkFatal is called from the drain workers; a second call must not panic on a
// closed channel.
func TestMarkFatalIsIdempotent(t *testing.T) {
	h := NewHealth(&scriptedSink{healthy: true}, HealthConfig{
		Interval: time.Hour, Timeout: time.Second,
	}, quietLog(), testMetrics())

	h.MarkFatal(errors.New("first"))
	h.MarkFatal(errors.New("second"))
	h.MarkFatal(errors.New("third"))

	if h.FatalErr() == nil || !strings.Contains(h.FatalErr().Error(), "first") {
		t.Errorf("FatalErr = %v, want the first cause retained", h.FatalErr())
	}
}

func TestWaitReadySucceedsAfterRetries(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: 100 * time.Millisecond},
		quietLog(), testMetrics())

	go func() {
		time.Sleep(150 * time.Millisecond)
		ss.setHealthy(true)
	}()

	err := h.WaitReady(context.Background(), 3*time.Second, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Errorf("WaitReady: %v", err)
	}
}

func TestWaitReadyGivesUp(t *testing.T) {
	h := NewHealth(&scriptedSink{healthy: false},
		HealthConfig{Interval: time.Hour, Timeout: 50 * time.Millisecond},
		quietLog(), testMetrics())

	start := time.Now()
	err := h.WaitReady(context.Background(), 200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Error("WaitReady should fail once the boot timeout passes")
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("WaitReady took %s, far past its 200ms budget", time.Since(start))
	}
}

// ------------------------------------------------------------------ workers

func TestWorkersDeliverAndRelease(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 2, Batch: 8, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond,
	}, quietLog(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	items := mkBatch(30)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	if !eventually(t, 3*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 30 }) {
		t.Fatalf("delivered %d of 30", m.EventsWrittenTotal.Load())
	}
	if !eventually(t, time.Second, func() bool { return r.Stats().Items == 0 }) {
		t.Errorf("ring still holds %d items", r.Stats().Items)
	}

	// SPEC §12.3: buffers return to the pool exactly once, on the confirmed
	// path and nowhere else.
	if !eventually(t, time.Second, func() bool {
		for _, it := range items {
			if !it.Released() {
				return false
			}
		}
		return true
	}) {
		t.Error("delivered items were not released")
	}
}

// SPEC §10.4: a retryable failure makes the worker HOLD the batch and stop
// pulling. The ring is untouched, so it fills and 429 appears on its own.
func TestWorkersHoldBatchOnRetryableFailure(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: errors.New("connection refused")}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: 200 * time.Millisecond,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 10 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued BEFORE the worker starts: with batch_delay = 0 a worker that
	// wakes between two Enqueues takes a partial batch, holds it, and pulls
	// nothing more -- and the assertion below would then count fewer than 4.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// The worker keeps retrying rather than giving up or dropping.
	if !eventually(t, 2*time.Second, func() bool { return ss.writes.Load() > 2 }) {
		t.Fatalf("only %d write attempts; the worker is not retrying", ss.writes.Load())
	}
	if m.EventsWrittenTotal.Load() != 0 {
		t.Error("nothing was delivered, so nothing should be counted as written")
	}

	// Stopping must surrender the held batch, or those events -- already
	// answered 200 -- would vanish with no emergency dump.
	cancel()
	w.Wait()

	pending := w.Pending()
	if len(pending) != 4 {
		t.Errorf("Pending returned %d items, want the 4 the worker was holding", len(pending))
	}
	for i, it := range pending {
		if it.Released() {
			t.Errorf("held item %d was released; its buffer is back in the pool while still referenced", i)
		}
	}
}

// A permanent rejection drops the batch, loudly and counted, rather than
// retrying against a destination that will never accept it.
func TestWorkersDropOnLogicError(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: logicErr("insert", errors.New("WRONGTYPE"))}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued before the worker starts: started first, it can dequeue the
	// first item alone and the rest as a second batch -- two logic drops, two
	// writes, and a flaky "retried" failure that is not one.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	if !eventually(t, 2*time.Second, func() bool {
		return m.EventsDroppedTotal[obs.DropSinkLogic].Load() == 4
	}) {
		t.Errorf("dropped %d, want 4 counted", m.EventsDroppedTotal[obs.DropSinkLogic].Load())
	}
	if !eventually(t, time.Second, func() bool { return r.Stats().Items == 0 }) {
		t.Error("a dropped batch must still clear the ring")
	}
	// Its bytes too: Commit subtracts len(Env), which Release empties, so a
	// Release first left the ring counting bytes nobody held -- forever, until
	// it refused every request as full (the review of go-webhook-nats-server,
	// 2026-10-10).
	if b := r.Stats().Bytes; b != 0 {
		t.Errorf("the empty ring still counts %d bytes", b)
	}
	if ss.writes.Load() != 1 {
		t.Errorf("%d attempts; a permanent rejection must not be retried", ss.writes.Load())
	}
}

// A fatal write result ends the process immediately rather than waiting for the
// next health check.
func TestWorkersMarkFatal(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: fatalErr("xadd", errors.New("OOM"))}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued before the worker starts, so its first batch is all four:
	// started first, it can take the first item alone and stop on the fatal
	// answer, holding one.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("a fatal write result did not mark the sink fatal")
	}

	// The items stay held so the emergency dump can collect them. Pending
	// hands them over and forgets them: it is read once.
	cancel()
	w.Wait()
	if pending := w.Pending(); len(pending) != 4 {
		t.Errorf("Pending returned %d, want the 4 held for the dump", len(pending))
	}
}

// SPEC §12.3: an attempt that ends by the clock taints the batch. Even when the
// retry succeeds, the buffers go to the GC rather than the pool, because the
// timed-out command may still be on its way through the pipeline.
func TestWorkersTaintABatchAfterATimedOutAttempt(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &stallOnceSink{}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: 20 * time.Millisecond,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	items := mkBatch(4)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	if !eventually(t, 3*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 4 }) {
		t.Fatalf("delivered %d of 4 after the stall", m.EventsWrittenTotal.Load())
	}
	if !eventually(t, time.Second, func() bool {
		for _, it := range items {
			if !it.Released() {
				return false
			}
		}
		return true
	}) {
		t.Fatal("delivered items were not released")
	}
	for i, it := range items {
		if it.Env == nil {
			t.Errorf("item %d was pooled after a timed-out attempt; the first command may still be in flight", i)
		}
	}
	if ss.writes.Load() < 2 {
		t.Fatalf("only %d attempts; the stall never happened", ss.writes.Load())
	}
	// SPEC §11.1: the timed-out attempt is counted apart from network errors.
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() == 0 {
		t.Error("the timed-out attempt was not counted in kind=\"timeout\"")
	}
}

// stallOnceSink blocks the first write until its context expires -- the shape
// of a destination that stopped answering -- and accepts everything after.
type stallOnceSink struct {
	writes atomic.Int64
}

func (s *stallOnceSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.writes.Add(1) == 1 {
		<-ctx.Done()
		return networkErr("xadd", ctx.Err())
	}
	return nil
}
func (s *stallOnceSink) Health(context.Context) error { return nil }
func (s *stallOnceSink) Close() error                 { return nil }
func (s *stallOnceSink) Type() string                 { return "stall" }

func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// ------------------------------------------------- worker hold, the overwrite

// A worker that ABANDONS a batch on shutdown keeps it registered in held[id],
// which is what lets Pending() hand it to the emergency dump. But the worker's
// loop then calls Dequeue again, and Dequeue's first select races a cancelled
// context against a ring that still has a backlog: Go picks a ready case at
// random, so the channel can win. The worker comes back with a second batch,
// deliver calls hold(id, ...) -- and the map assignment OVERWRITES the batch it
// was still holding.
//
// Those events were answered 200 and are then in neither the ring nor held.
// They are gone, with nothing logged, and the count in "events still pending
// after the drain" looks perfectly reasonable.
//
// This is the same family as the bug that made a retrying worker's batch vanish
// (docs/TESTING.md §9); the registration fixed that one and left this hole.
func TestWorkersNeverLoseAnAbandonedBatch(t *testing.T) {
	const (
		rounds   = 30
		backlog  = 40
		perRound = backlog + 1
	)
	for round := 0; round < rounds; round++ {
		func() {
			m := testMetrics()
			// The sink refuses every write, so nothing is ever committed and
			// every accepted item must still be accounted for at the end.
			ss := &scriptedSink{healthy: true, writeErr: networkErr("write", errors.New("destination is down"))}
			r := ring.NewMemory(perRound*2, 0, 0)
			h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)

			// One worker and a batch of one: the worker takes a single item,
			// fails, and holds it while the rest of the traffic piles up in the
			// ring behind it. That is exactly the shape the incident had.
			w := NewWorkers(r, ss, h, WorkerConfig{
				Workers: 1, Batch: 256, OpTimeout: time.Second,
				BackoffMin: 50 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
			}, quietLog(), m)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Seed one item and let the worker pick it up and start retrying.
			first := mkItem("first")
			if err := r.Enqueue(ctx, first); err != nil {
				t.Fatalf("round %d: seeding: %v", round, err)
			}
			w.Start(ctx)
			if !eventually(t, 2*time.Second, func() bool { return ss.writes.Load() > 0 }) {
				t.Fatalf("round %d: the worker never attempted a write", round)
			}

			// Now fill the ring behind the stuck worker.
			for i := 0; i < backlog; i++ {
				if err := r.Enqueue(ctx, mkItem(fmt.Sprintf("b%d", i))); err != nil {
					t.Fatalf("round %d: filling: %v", round, err)
				}
			}

			cancel()
			w.Wait()

			// Everything answered 200 must be recoverable: what the workers
			// were holding, plus whatever is still in the ring.
			got := len(w.Pending()) + len(r.Drain())
			if got != perRound {
				t.Fatalf("round %d: %d of %d accepted events survived the shutdown; "+
					"%d were answered 200 and then silently lost",
					round, got, perRound, perRound-got)
			}
		}()
	}
}
