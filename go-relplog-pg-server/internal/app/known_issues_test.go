package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// Regression tests from the known-issues round of 2026-10-14 (SPEC §22.1).

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
