package app

import (
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/config"
)

// SPEC §8.7.2 (porte da família syslog/RELP, 2026-10-13): the client's
// PublishAsyncMaxPending is twice what the drain workers can have in flight.
// With exactly that, the futures of a tainted batch, which keep counting until
// their own async timeout, a little after the worker's deadline, made a quick
// retry stall on a full window and fail with ErrTooManyStalledMsgs
// (go-relplog-nats-server, independent review, 2026-10-13).
func TestMaxPendingHasTwiceTheWorkersBatches(t *testing.T) {
	c := &config.Config{}
	c.Sink.Workers, c.Sink.Batch = 8, 256
	if got := maxPending(c); got != 2*8*256 {
		t.Fatalf("maxPending = %d, want %d", got, 2*8*256)
	}
}
