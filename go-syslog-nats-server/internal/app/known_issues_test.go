package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ring"
)

// Regression tests from the port of the RP/RN known-issues round (2026-10-14,
// SPEC §22.1).

// An abort outside the dump phase -- a SIGQUIT with every transport open, or
// the second signal during pre_stop_delay -- closes the ring BEFORE reading
// its count (SPEC §13.4). Read with the ring open, a message enqueued between
// the count and the os.Exit was accepted and lost uncounted; with the ring
// closed, that Enqueue is refused as the shutdown it is (counted in
// messages_dropped{shutdown}).
func TestAbortClosesTheRingBeforeCounting(t *testing.T) {
	a, _ := abortApp(t)
	a.abortLoss()
	if got := aborted(a); got != 5 {
		t.Fatalf("abort counted %d, want the 5 in the ring", got)
	}
	raw := []byte("<13>1 - h a - - - late")
	err := a.ring.Enqueue(context.Background(), item.New(crashHead(time.Now(), raw), raw, -1))
	if !errors.Is(err, ring.ErrClosed) {
		t.Fatalf("Enqueue after the abort counted = %v, want ring.ErrClosed (an accept the exit loses uncounted)", err)
	}
	if st := a.ring.Stats(); st.Items != 5 {
		t.Fatalf("ring holds %d after the abort, want the 5 counted", st.Items)
	}
}

// A reingested dump that cannot be removed is only an ERROR (SPEC §12.2, a
// rule since 2026-10-14): exiting would put the boot in a loop over a
// problem (the directory's permissions) that does not stop the delivery, and
// the gauges, recounted from the disk, keep counting the file. The removal
// that happened with only the directory fsync failing is a WARN, never
// "cannot remove". The pin of go-syslog-server's
// TestQuarantineAndRemovalReportTheirOutcome, for the removal.
func TestRemovalFailureIsOnlyAnError(t *testing.T) {
	var buf bytes.Buffer
	a := &App{log: obs.NewLogger(&buf, "json", "info")}
	a.reportRemoval("/d/emg-d.jsonl", errors.New("permission denied"))
	if log := buf.String(); !strings.Contains(log, "cannot remove a reingested dump") || !strings.Contains(log, `"level":"ERROR"`) {
		t.Errorf("a failed removal is not an ERROR:\n%s", log)
	}
	buf.Reset()
	notDurable := fmt.Errorf("%w: fsync of directory /x: input/output error", emergency.ErrNotDurable)
	a.reportRemoval("/d/emg-c.jsonl", notDurable)
	if log := buf.String(); !strings.Contains(log, "may reappear") || strings.Contains(log, "cannot remove") ||
		!strings.Contains(log, `"level":"WARN"`) {
		t.Errorf("a removal that happened is not reported as a WARN about the fsync:\n%s", log)
	}
	buf.Reset()
	a.reportRemoval("/d/emg-e.jsonl", nil)
	if log := buf.String(); log != "" {
		t.Errorf("a clean removal logged %q", log)
	}
}
