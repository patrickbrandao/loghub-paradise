package app

import (
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/config"
)

// SPEC §8.7.2 (porte da família syslog/RELP, 2026-10-13): the client's
// PublishAsyncMaxPending is twice what this process can have in flight -- the
// drain workers' batches plus the write-through handlers of ring=none. With
// exactly that, the futures of a tainted batch, which keep counting until
// their own async timeout, a little after the worker's deadline, made a quick
// retry stall on a full window and fail with ErrTooManyStalledMsgs
// (go-relplog-nats-server, independent review, 2026-10-13). At the ceiling of
// ring.max_inflight it stays positive, so NewNATS never falls back to 4096.
func TestNATSMaxPendingHasTwiceTheInFlight(t *testing.T) {
	c := &config.Config{}
	c.Sink.Workers, c.Sink.Batch, c.Ring.MaxInflight = 8, 256, 2048
	if got, want := natsMaxPending(c), 2*(8*256+2048); got != want {
		t.Fatalf("natsMaxPending = %d, want %d", got, want)
	}
	c.Ring.MaxInflight = 1 << 20
	if got := natsMaxPending(c); got <= 2*(1<<20) {
		t.Fatalf("natsMaxPending at the ceiling of ring.max_inflight = %d", got)
	}
}
