package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// Findings of the reviews of go-relplog-pg-server, go-relplog-nats-server and
// go-syslog-server (2026-10-13) that hold for this server's sinks, ported.

// A logic refusal returned by a write during which the caller's context ended
// (SIGTERM during the reingestion or the backlog drain) is still settled --
// counted AND logged with its uuids. The context check came first, so the
// refusal was counted by writeChunk and never logged, and nothing said which
// messages were lost (go-relplog-pg-server review, 2026-10-13).
func TestDeliverLogicRefusalWinsOverTheCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &hookSink{write: func(context.Context, *Batch) error {
		cancel() // the signal lands during the write
		return logicErr("insert", errors.New("value too long"))
	}}
	m := metrics()
	h := &recHandler{}
	d := NewInstrumented(fs, nil, workerCfg(4), slog.New(h), m)
	items := itemsOf(lines(2)...)
	if err := d.Deliver(ctx, items, time.Second, func(int, error) error { return nil }); err != nil {
		t.Fatalf("the refused chunk returned %v, want it settled", err)
	}
	if m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 {
		t.Fatalf("dropped %d, want 2", m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
	}
	v, ok := h.attr("refused a chunk permanently", "uuids")
	if lost := fmt.Sprint(v.Any()); !ok || !strings.Contains(lost, items[0].Head.UUID.String()) ||
		!strings.Contains(lost, items[1].Head.UUID.String()) {
		t.Fatalf("the counted refusal was not logged with its uuids: %q", lost)
	}
}
