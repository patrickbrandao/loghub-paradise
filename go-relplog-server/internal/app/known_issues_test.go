package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// Regression tests from the known-issues round of 2026-10-14 (SPEC §22.1; port of go-relplog-pg-server/go-relplog-nats-server).

// An abort outside the dump phase -- a SIGQUIT with every transport open, or
// the second signal during pre_stop_delay -- closes the ring BEFORE reading
// its count (SPEC §13.4). Read with the ring open, a transaction enqueued
// between the count and the os.Exit got its 200 and was lost uncounted; with
// the ring closed, that Enqueue is refused as the shutdown it is (no answer,
// librelp retransmits).
func TestAbortClosesTheRingBeforeCounting(t *testing.T) {
	a, _ := abortApp(t)
	a.abortLoss()
	if got := aborted(a); got != 5 {
		t.Fatalf("abort counted %d, want the 5 in the ring", got)
	}
	raw := []byte("<13>1 - h a - - - late")
	err := a.ring.Enqueue(context.Background(), item.New(crashHead(time.Now(), raw), raw, -1))
	if !errors.Is(err, ring.ErrClosed) {
		t.Fatalf("Enqueue after the abort counted = %v, want ring.ErrClosed (a 200 the exit loses uncounted)", err)
	}
	if st := a.ring.Stats(); st.Items != 5 {
		t.Fatalf("ring holds %d after the abort, want the 5 counted", st.Items)
	}
}

// The abort closes only the memory and none rings (SPEC §13.4): the sqlite
// and redis Close flush their writer and wait for the disk or the network,
// which a SIGQUIT -- "now" -- must not, and those rings count no loss. Without
// this test, removing the persistentRing guard broke nothing (independent
// review of 2026-10-14, A3): the ring stays open, and an Enqueue after the
// abort still goes in.
func TestAbortLeavesAPersistentRingOpen(t *testing.T) {
	r, err := ring.NewSQLite(ring.SQLiteConfig{Path: filepath.Join(t.TempDir(), "ring.db"),
		Synchronous: "OFF", CapacityItems: 100, CapacityBytes: 1 << 20}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	raw := []byte("<13>1 - h a - - - x")
	if err := r.Enqueue(context.Background(), item.New(crashHead(time.Now(), raw), raw, -1)); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Ring.Mode = config.RingModeSQLite
	a := &App{cfg: cfg, ring: r, metrics: obs.NewMetrics(obs.Info{}), log: obs.NewLogger(io.Discard, "text", "error")}
	a.abortLoss()
	if got := aborted(a); got != 0 {
		t.Fatalf("abort counted %d with a persistent ring, want 0", got)
	}
	late := []byte("<13>1 - h a - - - late")
	if err := r.Enqueue(context.Background(), item.New(crashHead(time.Now(), late), late, -1)); err != nil {
		t.Fatalf("Enqueue after the abort on ring=sqlite = %v, want nil: the abort closed a persistent ring", err)
	}
}
