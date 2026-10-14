package app

import (
	"context"
	"errors"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// Regression tests from the port of the RP/RN known-issues round (2026-10-14,
// SPEC §22.1).

// An abort outside the dump phase -- a SIGQUIT with every transport open, or
// the second signal during pre_stop_delay -- closes the memory ring BEFORE
// reading its count (SPEC §13.4). Read with the ring open, a message enqueued
// between the count and the os.Exit was accepted and lost uncounted; with the
// ring closed, that Enqueue is refused as the shutdown it is (counted in
// messages_dropped{shutdown}).
func TestAbortClosesTheRingBeforeCounting(t *testing.T) {
	a, _ := abortApp(t)
	a.abortLoss()
	if got := aborted(a); got != 5 {
		t.Fatalf("abort counted %d, want the 5 in the ring", got)
	}
	err := a.ring.Enqueue(context.Background(), crashItems(1, "late")[0])
	if !errors.Is(err, ring.ErrClosed) {
		t.Fatalf("Enqueue after the abort counted = %v, want ring.ErrClosed (an accept the exit loses uncounted)", err)
	}
	if st := a.ring.Stats(); st.Items != 5 {
		t.Fatalf("ring holds %d after the abort, want the 5 counted", st.Items)
	}
}
