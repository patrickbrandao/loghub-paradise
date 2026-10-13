package app

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
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

	// A dump cut short leaves its hidden temporary (SPEC §12.1). The twin of
	// a complete dump is removed by Leftovers; an interrupted one is reported,
	// because nothing else would tell the operator it is there -- whatever
	// sink this life has, sink=none included. Its messages were counted as
	// lost only when a second signal aborted the dump: after a SIGKILL (the
	// stop_grace_period running out), an OOM or a power loss nothing counted
	// them, and the file may well be complete.
	if left, err := store.Leftovers(); err == nil && len(left) > 0 {
		a.log.Warn("interrupted emergency dumps found; they are NOT reingested. Their messages "+
			"were counted as lost only if a second signal aborted the dump; after a SIGKILL or a "+
			"crash nothing counted them. Renamed to their final name (no leading dot, no .tmp), "+
			"their intact records are reingested at the next boot (SPEC §12.1)",
			"files", len(left), "dir", store.Dir(), "paths", left)
	}

	files, err := store.List()
	if err != nil {
		a.log.Error("cannot list the emergency directory", "dir", store.Dir(), "error", err)
		return ExitConfig
	}
	if len(files) == 0 {
		return ExitOK
	}

	if a.cfg.Sink.Type == config.SinkTypeNone {
		// sink=none discards what it is given by configuration. "Delivering"
		// a previous life's dump to it and removing the file destroyed
		// messages a real destination's life had answered 200 -- a load test
		// on the same volume after an incident. They wait on disk for a boot
		// with a destination, named in the log and counted in the gauges.
		a.log.Warn("emergency dumps found, but sink.type=none discards what it is given; they are "+
			"NOT reingested and stay on disk for a boot with a destination",
			"files", len(files), "dir", store.Dir(), "paths", files)
		a.refreshEmergencyGauges(store)
		return ExitOK
	}

	// Exact and free: every listed file awaits reingestion. Its messages are
	// only known once read, and are counted from the disk at the end. Set
	// before the boot state that announces the reingestion, so whoever sees
	// that state sees the count too.
	a.metrics.EmergencyFiles.Set(int64(len(files)))
	a.log.Warn("emergency dumps found from a previous run; reingesting before any transport opens",
		"files", len(files), "dir", store.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	for _, path := range files {
		res, err := store.Read(path)
		if err != nil {
			a.log.Error("cannot read an emergency dump; leaving it in place", "path", path, "error", err)
			continue
		}

		if code := a.writeDirect(ctx, res.Items); code != ExitOK {
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "items", len(res.Items))
			return code
		}
		a.log.Info("emergency dump reingested", "path", path, "items", len(res.Items),
			"truncated", res.Truncated, "undecodable", res.Undecodable)

		if res.Damaged() {
			dst, qerr := store.Quarantine(path)
			a.reportQuarantine(path, dst, qerr, res)
		} else {
			a.reportRemoval(path, store.Remove(path))
		}
	}
	// Recounted from the directory, never zeroed: a file that could not be
	// read, removed or quarantined is still there, still pending, and an
	// alert on these gauges must keep firing until somebody deals with it.
	a.refreshEmergencyGauges(store)
	return ExitOK
}

// reportQuarantine logs the outcome of Store.Quarantine for a damaged dump.
//
// A non-empty dst means the file WAS renamed, whatever qerr says: the error is
// then only the directory fsync that followed (emergency.ErrNotDurable), so
// the new name may not survive a power loss. Read as "not quarantined", that
// case logged "cannot quarantine" with a path that no longer existed and
// dropped the ERROR with the byte counts of what was lost (SPEC §12.2).
func (a *App) reportQuarantine(path, dst string, qerr error, res *emergency.ReadResult) {
	if dst == "" {
		a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
		return
	}
	if qerr != nil {
		a.log.Warn("the damaged dump was quarantined but the directory fsync failed; the new "+
			"name may not survive a power loss", "path", dst, "error", qerr)
	}
	a.log.Error("the dump was damaged; the intact records were delivered and the "+
		"file was quarantined", "path", dst,
		"discarded_bytes", res.TruncatedBytes, "undecodable", res.Undecodable,
		"undecodable_bytes", res.UndecodableBytes)
}

// reportRemoval logs the outcome of Store.Remove for a reingested dump. An
// ErrNotDurable means the unlink happened and only the directory fsync after
// it failed: the file is gone now, and "cannot remove" would be false. What
// the operator must know is the other consequence -- after a power loss the
// name may come back, and the next boot would deliver the dump again.
func (a *App) reportRemoval(path string, rerr error) {
	switch {
	case rerr == nil:
	case errors.Is(rerr, emergency.ErrNotDurable):
		a.log.Warn("the reingested dump was removed but the directory fsync failed; after a "+
			"power loss it may reappear and be delivered again", "path", path, "error", rerr)
	default:
		a.log.Error("cannot remove a reingested dump", "path", path, "error", rerr)
	}
}

// refreshEmergencyGauges sets relplog_emergency_files and
// relplog_emergency_events_pending from what is on disk, the only honest
// source. The file count comes from the listing, so a dump that cannot even be
// read still counts as one; its messages are unknowable and count as none.
func (a *App) refreshEmergencyGauges(store *emergency.Store) {
	files, err := store.List()
	if err == nil {
		var events int64
		if _, events, err = store.Count(); err == nil {
			a.metrics.EmergencyFiles.Set(int64(len(files)))
			a.metrics.EmergencyEventsPending.Set(events)
			return
		}
	}
	a.log.Warn("cannot count the emergency dumps; relplog_emergency_* keep their previous values",
		"dir", store.Dir(), "error", err)
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
	if k := sr.Kept(); k > 0 {
		// sink=none discards what it is given (SPEC §10.8). Draining a
		// previous life's rows into it destroyed events a life with a
		// destination had answered 200 -- a load test on the same volume
		// after an incident, the case of the dump's rule (SPEC §12.2;
		// go-syslog-server review, 2026-10-13, as go-webhook-fast-server
		// does).
		a.log.Warn("ring.db holds messages a previous life left, but sink.type=none discards what "+
			"it is given; they are NOT delivered and stay in ring.db for a boot with a destination",
			"messages", k, "path", a.cfg.RingSQL.Path)
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
//
// It zeroes App.dumping as soon as the outcome is known and BEFORE anything is
// counted: saved, an abort loses nothing more; failed, the loss is counted
// here as emergency_failed, and an abort racing that count must not add the
// same messages again as abort (settleDump: one owner of the count, under
// lossMu, SPEC §13.4).
//
// A dump the abort already counted gives up (emergency.Store.SetAbandon), and
// one that completed after the abort counts nothing more: the abort counted
// every message (independent reviews of go-relplog-nats-server and
// go-relplog-pg-server, 2026-10-13).
func (a *App) dumpEmergency(items []*item.Item) bool {
	if len(items) == 0 {
		a.settleDump()
		return false
	}
	if a.emg == nil {
		// No emergency store (emergency.enabled=false): SPEC §12.3 counts this
		// exactly like a dump that failed -- unless an abort already took the
		// counter and counted the same messages.
		if a.settleDump() > 0 {
			a.log.Error("emergency dumps are disabled; these messages are lost", "count", len(items))
			a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Add(uint64(len(items)))
		}
		return false
	}
	if a.beforeDump != nil {
		a.beforeDump()
	}
	path, err := a.emg.Write(items)
	owed := a.settleDump()
	if err != nil {
		if owed == 0 {
			return false // an abort counted them already
		}
		a.log.Error("EMERGENCY DUMP FAILED; these messages are lost",
			"count", len(items), "dir", a.emg.Dir(), "error", err)
		a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Add(uint64(len(items)))
		return false
	}
	if owed == 0 {
		// Published between the last abandon check and the abort: counted
		// lost AND kept, an overcount the next boot corrects by delivering
		// it, never a loss. Nothing more is counted; the file is on disk, so
		// the gauges below still say so.
		a.log.Warn("the emergency dump completed after an abort counted its messages lost; "+
			"the next boot reingests it", "path", path, "count", len(items))
	} else {
		a.log.Warn("pending messages written to an emergency dump; they will be reingested at the next boot",
			"count", len(items), "path", path)
	}
	// Added, not set: a dump the boot could not reingest may still be on disk
	// (unreadable, kept by sink=none), and the gauges say what is pending
	// there, not what this one dump holds (SPEC §12.2).
	a.metrics.EmergencyFiles.Inc()
	a.metrics.EmergencyEventsPending.Add(int64(len(items)))
	return true
}
