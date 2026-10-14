package sink

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// downProbeSink answers every health check at once as unreachable.
type downProbeSink struct {
	switchSink
	checks atomic.Int32
}

func (s *downProbeSink) Health(context.Context) error {
	s.checks.Add(1)
	return &Error{Class: ClassNetwork, Op: "test", Err: errors.New("not yet")}
}

// SPEC §10.1 step 3 (W 39): WaitReady sleeps min(backoff, what is left of
// its budget). It slept the whole backoff -- up to 5s -- and then ran one
// more check past the deadline; with 300ms of budget and a 2s backoff it
// returned after 2s.
func TestWaitReadyDoesNotSleepPastItsDeadline(t *testing.T) {
	ds := &downProbeSink{}
	h := NewHealth(ds, HealthConfig{Interval: time.Hour, Timeout: 100 * time.Millisecond}, quietLog(), testMetrics())
	start := time.Now()
	err := h.WaitReady(context.Background(), 300*time.Millisecond, 2*time.Second, 2*time.Second)
	took := time.Since(start)
	if err == nil {
		t.Fatal("WaitReady succeeded against a sink that is never ready")
	}
	if took > time.Second {
		t.Fatalf("WaitReady took %s with a 300ms budget: the backoff slept past it", took)
	}
	if n := ds.checks.Load(); n != 2 {
		t.Errorf("%d checks, want 2: one at the start, one when the budget ends", n)
	}
}
