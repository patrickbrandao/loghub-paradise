package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// The wrapper exists because ring=none writes straight through from the handler
// and never reaches the drain workers, which were the only place any of this
// happened. These tests pin the three things that were missing.

func TestInstrumentedCountsSuccess(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	i := NewInstrumented(ss, nil, m)

	if err := i.Write(context.Background(), mkBatch(7)); err != nil {
		t.Fatal(err)
	}
	if got := m.EventsWrittenTotal.Load(); got != 7 {
		t.Errorf("whs_events_written_total = %d, want 7", got)
	}
	// Duration and batch size were the two histograms that stayed empty in
	// ring=none, which made the mode impossible to diagnose.
	if got := m.SinkDuration.Count(); got != 1 {
		t.Errorf("whs_sink_duration_seconds_count = %d, want 1", got)
	}
	if got := m.SinkBatchSize.Count(); got != 1 {
		t.Errorf("whs_sink_batch_size_count = %d, want 1", got)
	}
}

// The one that mattered most: with ring=none a fatal write used to be answered
// as a 503 to the client while the process ran on forever. Redis answering PING
// happily while refusing every XADD with OOM is exactly that case -- the health
// check cannot see it, so only the write can (SPEC §10.2).
func TestInstrumentedEscalatesFatal(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	ss.setWriteErr(fatalErr("xadd", errors.New("OOM command not allowed")))

	m := testMetrics()
	h := NewHealth(ss, HealthConfig{}, quietLog(), m)
	i := NewInstrumented(ss, h, m)

	if err := i.Write(context.Background(), mkBatch(3)); err == nil {
		t.Fatal("a fatal write returned nil")
	}

	select {
	case <-h.Fatal():
	default:
		t.Fatal("a fatal write did not move the state machine to FATAL")
	}
	if m.SinkUp.Load() != 0 {
		t.Error("whs_sink_up stayed at 1 after FATAL")
	}
	if got := m.SinkErrorsTotal[obs.SinkErrFatal].Load(); got != 1 {
		t.Errorf(`whs_sink_errors_total{kind="fatal"} = %d, want 1`, got)
	}
	if got := m.EventsWrittenTotal.Load(); got != 0 {
		t.Errorf("a failed batch counted %d events as written", got)
	}
}

// A logic failure is permanent, so the events are lost right here and must be
// visible as such: this is the counter an operator reads to find events that
// were accepted and then never arrived.
func TestInstrumentedCountsLogicDrop(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	ss.setWriteErr(logicErr("xadd", errors.New("WRONGTYPE")))

	m := testMetrics()
	h := NewHealth(ss, HealthConfig{}, quietLog(), m)
	i := NewInstrumented(ss, h, m)

	if err := i.Write(context.Background(), mkBatch(4)); err == nil {
		t.Fatal("a logic write returned nil")
	}
	if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 4 {
		t.Errorf(`whs_events_dropped_total{reason="sink_logic"} = %d, want 4`, got)
	}
	if got := m.SinkErrorsTotal[obs.SinkErrLogic].Load(); got != 1 {
		t.Errorf(`whs_sink_errors_total{kind="logic"} = %d, want 1`, got)
	}
	// A logic failure is the destination refusing data, not the destination
	// being unusable. Escalating it would end the process over one bad event.
	select {
	case <-h.Fatal():
		t.Error("a logic failure escalated to FATAL")
	default:
	}
}

// A partial delivery (SPEC §8.1) through the write-through path counts what
// the destination stored as written and only the rest as lost, the rule of
// the workers.
func TestInstrumentedCountsAPartialDelivery(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	ss.setWriteErr(&Error{Class: ClassLogic, Op: "ack", Err: errors.New("message too big"), Delivered: []int{0, 2, 3}})
	m := testMetrics()
	i := NewInstrumented(ss, NewHealth(ss, HealthConfig{}, quietLog(), m), m)

	if err := i.Write(context.Background(), mkBatch(4)); ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v, want the logic error back", err)
	}
	if got := m.EventsWrittenTotal.Load(); got != 3 {
		t.Errorf("whs_events_written_total = %d, want the 3 stored", got)
	}
	if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 1 {
		t.Errorf(`whs_events_dropped_total{reason="sink_logic"} = %d, want only the 1 refused`, got)
	}
}

// A network failure must NOT move the state machine: the item stays in the ring
// and is retried, so nobody was told 200 for an event that then vanished
// (SPEC §10.2).
func TestInstrumentedLeavesNetworkErrorsToTheHealthCheck(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	ss.setWriteErr(networkErr("xadd", errors.New("broken pipe")))

	m := testMetrics()
	h := NewHealth(ss, HealthConfig{}, quietLog(), m)
	i := NewInstrumented(ss, h, m)

	if err := i.Write(context.Background(), mkBatch(2)); err == nil {
		t.Fatal("a network write returned nil")
	}
	if !h.Up() {
		t.Error("a network write error moved the sink out of UP")
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 1 {
		t.Errorf(`whs_sink_errors_total{kind="network"} = %d, want 1`, got)
	}
	if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 0 {
		t.Errorf("a retryable failure counted %d events as dropped", got)
	}
}

// The wrapper must stay transparent: the ring asks it for nothing but Write,
// but the app hands the same value around as a Sink.
func TestInstrumentedIsTransparent(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	i := NewInstrumented(ss, nil, testMetrics())

	var _ Sink = i
	if i.Type() != "scripted" {
		t.Errorf("Type() = %q, want the inner sink's own name", i.Type())
	}
	if err := i.Health(context.Background()); err != nil {
		t.Errorf("Health() = %v, want the inner sink's answer", err)
	}
}
