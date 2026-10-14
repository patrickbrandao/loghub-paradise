package sink

// Guard from the port of the syslog/RELP family's corrections (2026-10-13):
// go-relplog-pg-server's Deliver looked at the context before the class of
// the answer, so a SIGTERM that landed during a write the destination refused
// for good counted the loss without the log that names it. The drain here
// already settles the class first; this test keeps it that way.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// cancelThenRefuseSink is a destination that refuses the batch for good
// (logic) while the process is told to stop: the shutdown's cancel runs
// inside the write, before the refusal comes back.
type cancelThenRefuseSink struct{ cancel context.CancelFunc }

func (s *cancelThenRefuseSink) Write(context.Context, []*envelope.Item) error {
	s.cancel()
	return logicErr("insert", errors.New("refused for good"))
}
func (s *cancelThenRefuseSink) Health(context.Context) error { return nil }
func (s *cancelThenRefuseSink) Close() error                 { return nil }
func (s *cancelThenRefuseSink) Type() string                 { return "refuse" }

// SPEC §10.4: a logic refusal is a counted loss AND an ERROR naming the
// uuids, together, even when the shutdown begins during that write. The
// batch's ownership moves on (nothing left for the dump: it was refused, not
// abandoned), exactly as without the signal.
func TestWorkersLogicRefusalDuringShutdownIsLoggedAndCounted(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	for _, it := range mkBatch(3) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	items, err := r.Dequeue(context.Background(), 3)
	if err != nil || len(items) != 3 {
		t.Fatalf("Dequeue = %d items, %v", len(items), err)
	}
	ids := make([]string, len(items))
	for i, it := range items {
		ids[i] = it.ID
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &cancelThenRefuseSink{cancel: cancel}
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	m := testMetrics()
	h := NewHealth(s, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, s, h, WorkerConfig{Workers: 1, Batch: 3, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: time.Millisecond}, log, m)

	if !w.deliver(ctx, items, 0) {
		t.Fatal("deliver reported the refused batch as abandoned: it would be dumped and redelivered")
	}
	if ctx.Err() == nil {
		t.Fatal("the fixture did not cancel the worker's context during the write")
	}
	if n := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); n != 3 {
		t.Errorf("events_dropped{sink_logic} = %d, want 3", n)
	}
	if left := w.Pending(); len(left) != 0 {
		t.Errorf("%d items still held for the dump after a permanent refusal", len(left))
	}
	out := logs.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "permanently") {
		t.Fatalf("no ERROR for the refusal; the loss is counted but unnamed:\n%s", out)
	}
	for _, id := range ids {
		if !strings.Contains(out, id) {
			t.Errorf("the refusal's log does not name %s:\n%s", id, out)
		}
	}
}
