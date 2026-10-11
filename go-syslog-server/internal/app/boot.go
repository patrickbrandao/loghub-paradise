package app

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// reingest loads the emergency dumps a previous run left behind (SPEC §12.2).
//
// Files are processed oldest first, and one is removed only after ALL of its
// items have been confirmed by the sink. A damaged file contributes its intact
// records and is then quarantined rather than deleted: destroying evidence of a
// crash is not this program's call.
//
// There is no metadata to rebuild. The dump holds head plus raw line, and the
// conversion turns those back into the very JLH the original would have
// produced, same uuid included (SPEC §3.5).
//
// EVERY dump is read, whatever the ring mode and emergency.enabled say NOW: a
// dump left by a ring=memory run is reingested even after the operator switched
// to ring=sqlite as a reaction to the incident. Only the WRITING of dumps
// depends on the mode (SPEC §12.2).
func (a *App) reingest(ctx context.Context) int {
	store := emergency.NewStore(a.cfg.Emergency.Dir, a.id.From)
	files, err := store.List()
	if err != nil {
		a.log.Error("cannot list the emergency directory", "dir", store.Dir(), "error", err)
		return ExitConfig
	}
	if len(files) == 0 {
		return ExitOK
	}

	a.log.Warn("emergency dumps found from a previous run; reingesting before any transport opens",
		"files", len(files), "dir", store.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	for _, path := range files {
		res, err := store.Read(path)
		if err != nil {
			a.log.Error("cannot read an emergency dump; leaving it in place", "path", path, "error", err)
			continue
		}
		a.metrics.EmergencyFiles.Set(int64(len(files)))
		a.metrics.EmergencyEventsPending.Set(int64(len(res.Items)))

		if code := a.writeDirect(ctx, res.Items); code != ExitOK {
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "items", len(res.Items))
			return code
		}
		a.log.Info("emergency dump reingested", "path", path, "items", len(res.Items),
			"truncated", res.Truncated, "undecodable", res.Undecodable)

		if res.Damaged() {
			dst, qerr := store.Quarantine(path)
			if qerr != nil {
				a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
			} else {
				a.log.Error("the dump was damaged; the intact records were delivered and the "+
					"file was quarantined", "path", dst,
					"discarded_bytes", res.TruncatedBytes, "undecodable", res.Undecodable,
					"undecodable_bytes", res.UndecodableBytes)
			}
		} else if rerr := store.Remove(path); rerr != nil {
			a.log.Error("cannot remove a reingested dump", "path", path, "error", rerr)
		}
	}
	a.metrics.EmergencyFiles.Set(0)
	a.metrics.EmergencyEventsPending.Set(0)
	return ExitOK
}

// writeDirect converts and sends items straight to the sink in batches,
// bypassing the ring. Reingestion runs before any transport is open, so routing
// through the ring would only risk filling it with the backlog it exists to
// absorb.
func (a *App) writeDirect(ctx context.Context, items []*item.Item) int {
	n := a.cfg.Sink.Batch
	for i := 0; i < len(items); i += n {
		end := min(i+n, len(items))
		if code := a.deliverDirect(ctx, items[i:end]); code != ExitOK {
			return code
		}
		for _, it := range items[i:end] {
			it.Release()
		}
	}
	return ExitOK
}

// deliverDirect writes one batch with the drain workers' per-class rules
// (SPEC §10.2), which the boot paths need just as much as the workers do. The
// rules themselves live in sink.Instrumented.Deliver, chunk by chunk; this is
// the boot's policy around them:
//
//   - logic: the destination refused that data for good. It is counted and
//     logged there, and the batch is treated as delivered so the caller commits
//     it. Exiting instead would leave the same rows (or the same dump) for the
//     next boot, which would refuse them the same way: a crash loop with every
//     transport closed, forever.
//   - network: the same sealed chunk is retried with backoff, within
//     sink.boot_timeout. One transient failure is not a reason to give up the
//     boot.
//   - fatal: exit 3.
//
// A cancelled context (SIGTERM during the boot) is 130, not 3.
func (a *App) deliverDirect(ctx context.Context, items []*item.Item) int {
	backoff := a.cfg.Sink.RetryBackoffMin
	deadline := time.Now().Add(a.cfg.Sink.BootTimeout)
	errBootTimeout := errors.New("sink.boot_timeout exceeded")
	err := a.direct.Deliver(ctx, items, a.cfg.Sink.OpTimeout, func(attempt int, err error) error {
		if time.Now().After(deadline) {
			a.log.Error("the sink kept failing at boot past sink.boot_timeout", "error", err,
				"attempts", attempt)
			return errBootTimeout
		}
		if attempt == 1 || attempt%10 == 0 {
			a.log.Warn("boot delivery failed, retrying", "attempt", attempt,
				"count", len(items), "error", err, "retry_in", backoff.String())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > a.cfg.Sink.RetryBackoffMax {
			backoff = a.cfg.Sink.RetryBackoffMax
		}
		return nil
	})
	switch {
	case err == nil:
		return ExitOK
	case ctx.Err() != nil:
		a.log.Info("interrupted while delivering at boot")
		return ExitAborted
	case errors.Is(err, errBootTimeout):
		return ExitSink
	}
	a.log.Error("the sink became permanently unusable during the boot", "error", err)
	return ExitSink
}

// drainProgressTimeout is how long one Dequeue of the boot drain may find
// nothing before the accounting is suspected. With no transport open, rows can
// only be there or not: a wait this long is never a slow emitter.
const drainProgressTimeout = 5 * time.Second

// drainBacklog empties ring=sqlite before any transport opens (SPEC §13.1
// step 10). memory starts empty (the dump was reingested straight to the sink),
// none never queues, and ring=redis does not wait for a backlog that other
// replicas keep feeding: its workers start draining with everything else.
func (a *App) drainBacklog(ctx context.Context) int {
	sr, ok := a.ring.(*ring.SQLiteRing)
	if !ok {
		return ExitOK
	}
	total := sr.Backlog()
	if total == 0 {
		return ExitOK
	}

	a.log.Warn("draining the ring backlog before opening any transport", "items", total)
	start := time.Now()
	recounted := false
	for {
		st := a.ring.Stats()
		if st.Items == 0 {
			a.log.Info("backlog drained", "items", total, "took", time.Since(start).String())
			return ExitOK
		}
		a.admin.SetBootState("draining backlog: " + strconv.FormatInt(st.Items, 10) + " pending")

		// A Dequeue that finds nothing while the accounting says there is
		// something is a ring that cannot make progress. The accounting is
		// re-read from the table once, and if it still disagrees the boot ends
		// with 3 instead of keeping every transport closed forever (SPEC §9.4).
		dctx, cancel := context.WithTimeout(ctx, drainProgressTimeout)
		items, err := a.ring.Dequeue(dctx, a.cfg.Sink.Batch)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				a.log.Info("interrupted while draining the backlog")
				return ExitAborted
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if rerr := sr.Recount(); rerr != nil {
					a.log.Error("cannot recount the ring backlog", "error", rerr)
					return ExitSink
				}
				if a.ring.Stats().Items == 0 || !recounted {
					recounted = true
					continue
				}
				a.log.Error("the ring backlog cannot be drained: rows are counted but none can be read",
					"pending", a.ring.Stats().Items)
				return ExitSink
			}
			a.log.Error("cannot read the ring backlog", "error", err)
			return ExitSink
		}
		if len(items) == 0 {
			continue
		}

		if code := a.deliverDirect(ctx, items); code != ExitOK {
			a.log.Error("cannot deliver the backlog; every transport stays closed",
				"pending", st.Items)
			return code
		}
		// Commit before release: the ring's accounting reads the items.
		_ = a.ring.Commit(items)
		for _, it := range items {
			it.Release()
		}
	}
}

// dumpEmergency writes pending items to disk. It reports whether they were
// saved; false means they are genuinely lost.
func (a *App) dumpEmergency(items []*item.Item) bool {
	if a.emg == nil || len(items) == 0 {
		return false
	}
	path, err := a.emg.Write(items)
	if err != nil {
		a.log.Error("EMERGENCY DUMP FAILED; these messages are lost",
			"count", len(items), "dir", a.emg.Dir(), "error", err)
		a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Add(uint64(len(items)))
		return false
	}
	a.log.Warn("pending messages written to an emergency dump; they will be reingested at the next boot",
		"count", len(items), "path", path)
	a.metrics.EmergencyFiles.Set(1)
	a.metrics.EmergencyEventsPending.Set(int64(len(items)))
	return true
}
