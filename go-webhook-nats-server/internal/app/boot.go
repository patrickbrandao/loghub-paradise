package app

import (
	"context"
	"errors"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/sink"
)

// reingest loads the emergency dumps a previous run left behind (SPEC §9.2).
//
// Files are processed oldest first, and one is removed only after ALL of its
// intact lines have been confirmed by the sink. A file with a damaged line --
// cut short by the crash that created it, or edited by hand -- contributes its
// intact lines and is then quarantined rather than deleted: the damage is
// unreadable to this program, but destroying evidence of a crash is not its
// call.
//
// It runs at every boot, emergency.enabled or not: the dumps are a previous
// life's (SPEC §9.2).
func (a *App) reingest(ctx context.Context) int {
	// The binary dumps of go-webhook-fast-server are never read here (SPEC
	// §9.2). They are only reported, so an operator who moved an instance over
	// learns there is something to reingest with the other program.
	if foreign, err := a.emg.Foreign(); err == nil && len(foreign) > 0 {
		a.log.Warn("binary emergency dumps of go-webhook-fast-server found; they are NOT "+
			"reingested by this server -- reingest them with go-webhook-fast-server (SPEC §9.2)",
			"files", len(foreign), "dir", a.emg.Dir())
	}

	// A dump cut short leaves its hidden temporary (SPEC §9.1). The twin of a
	// complete dump goes; an interrupted one is reported, because nothing
	// else would tell the operator it is there. Its events were counted lost
	// only when a second signal aborted the dump: after a SIGKILL -- the
	// stop_grace_period running out -- an OOM or a power loss, nothing
	// counted them, and the file may well be complete.
	if left, err := a.emg.Leftovers(); err == nil && len(left) > 0 {
		a.log.Warn("interrupted emergency dumps found; they are NOT reingested. Their events "+
			"were counted as lost only if a second signal aborted the dump; after a SIGKILL or a "+
			"crash nothing counted them. Renamed to their final name (no leading dot, no .tmp), "+
			"their intact lines are reingested at the next boot (SPEC §9.1)",
			"files", len(left), "dir", a.emg.Dir(), "paths", left)
	}

	files, err := a.emg.List()
	if err != nil {
		if !a.dumps {
			// This life never writes there (emergency.enabled=false): a
			// directory it cannot list must not stop it (SPEC §9.2). Whatever
			// is in it stays, named in the log.
			a.log.Error("cannot list the emergency directory; any dump in it is NOT reingested",
				"dir", a.emg.Dir(), "error", err)
			return ExitOK
		}
		a.log.Error("cannot list the emergency directory", "dir", a.emg.Dir(), "error", err)
		return ExitConfig
	}
	if len(files) == 0 {
		return ExitOK
	}

	// Exact and free: every listed file awaits reingestion. Its events are
	// only known once read, and are counted from the disk at the end (SPEC
	// §11.1). Set before the boot state that announces the reingestion, so
	// whoever sees that state sees the count too.
	a.metrics.EmergencyFiles.Set(int64(len(files)))
	a.log.Warn("emergency dumps found from a previous run; reingesting before serving",
		"files", len(files), "dir", a.emg.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	for _, path := range files {
		res, err := a.emg.Read(path)
		if err != nil {
			a.log.Error("cannot read an emergency dump; leaving it in place",
				"path", path, "error", err)
			continue
		}

		refused, err := a.writeDirect(ctx, res.Items)
		if err != nil {
			if ctx.Err() != nil {
				// SIGTERM during the boot: the file stays for the next one,
				// and the exit is the interruption's, as in WaitReady.
				a.log.Info("interrupted while reingesting an emergency dump; it stays on disk",
					"path", path)
				return ExitAborted
			}
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "items", len(res.Items), "error", err)
			return ExitSink
		}
		a.log.Info("emergency dump reingested",
			"path", path, "items", len(res.Items)-refused, "damaged_lines", res.DamagedLines,
			"refused_by_destination", refused)

		if res.Damaged() || refused > 0 {
			dst, qerr := a.emg.Quarantine(path)
			a.reportQuarantine(path, dst, qerr, res, refused)
		} else {
			a.reportRemoval(path, a.emg.Remove(path))
		}
	}

	a.refreshEmergencyGauges()
	return ExitOK
}

// reportQuarantine logs the outcome of Store.Quarantine for a damaged dump.
//
// A non-empty dst means the file WAS renamed, whatever qerr says: the error is
// then only the directory fsync that followed (emergency.ErrNotDurable), so
// the new name may not survive a power loss. Read as "not quarantined", that
// case logged "cannot quarantine" with a path that no longer existed and
// dropped the ERROR with the counts of what was skipped (SPEC §9.2; port of
// the syslog/RELP family, 2026-10-13).
func (a *App) reportQuarantine(path, dst string, qerr error, res *emergency.ReadResult, refused int) {
	if dst == "" {
		a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
		return
	}
	if qerr != nil {
		a.log.Warn("the damaged dump was quarantined but the directory fsync failed; the new "+
			"name may not survive a power loss", "path", dst, "error", qerr)
	}
	a.log.Error("the dump had damaged lines; its intact lines were reingested "+
		"and the file was quarantined (SPEC §9.2)",
		"path", dst, "damaged_lines", res.DamagedLines,
		"damaged_bytes", res.DamagedBytes, "refused_by_destination", refused)
}

// reportRemoval logs the outcome of Store.Remove for a reingested dump. An
// ErrNotDurable means the unlink happened and only the directory fsync after
// it failed: the file is gone now, and "cannot remove" would be false. What
// the operator must know is the other consequence -- after a power loss the
// name may come back, and the next boot would deliver the dump again (SPEC
// §9.2).
func (a *App) reportRemoval(path string, rerr error) {
	switch {
	case rerr == nil:
	case errors.Is(rerr, emergency.ErrNotDurable):
		a.log.Warn("the reingested dump was removed but the directory fsync failed; after a "+
			"power loss it may reappear and be delivered again", "path", path, "error", rerr)
	default:
		a.log.Error("cannot remove a reingested dump; it will be delivered again at the next boot",
			"path", path, "error", rerr)
	}
}

// writeDirect sends items straight to the sink in batches, bypassing the ring.
// Reingestion runs before the ring is serving, so routing through it would only
// risk filling it with the backlog it is supposed to absorb.
//
// A refusal the destination makes as logic -- a message NATS will not take,
// though it passed every check of the reader -- leaves out only the refused
// items: counted in refused, logged with their uuid, and kept in the file,
// which the caller quarantines. The NATS sink says which items it stored
// (partial delivery, SPEC §8.1); a logic error without that list is retried
// one item at a time, the rule go-webhook-pg-server needed for a batch
// refused whole. Returning the error instead made one such line stop every
// boot with exit 3, with every other line of the file and every later file
// undelivered (SPEC §9.2: damage never spreads past its line).
func (a *App) writeDirect(ctx context.Context, items []*envelope.Item) (refused int, err error) {
	write := func(batch []*envelope.Item) error {
		wctx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
		defer cancel()
		return a.sink.Write(wctx, batch)
	}
	delivered := func(batch []*envelope.Item) {
		a.metrics.EventsWrittenTotal.Add(uint64(len(batch)))
		for _, it := range batch {
			it.Release()
		}
	}

	size := a.cfg.Sink.Batch
	for i := 0; i < len(items); i += size {
		batch := items[i:min(i+size, len(items))]
		err := write(batch)
		if err == nil {
			delivered(batch)
			continue
		}
		if sink.ClassOf(err) != sink.ClassLogic {
			return refused, err
		}
		if stored, ok := sink.DeliveredOf(err); ok {
			took := make([]bool, len(batch))
			for _, i := range stored {
				if i >= 0 && i < len(batch) {
					took[i] = true
				}
			}
			var done []*envelope.Item
			for i, it := range batch {
				if took[i] {
					done = append(done, it)
					continue
				}
				refused++
				a.log.Error("the destination refused a line of an emergency dump; it stays in the "+
					"quarantined file", "id", it.ID, "error", err)
			}
			delivered(done)
			continue
		}
		for _, it := range batch {
			one := []*envelope.Item{it}
			err := write(one)
			switch {
			case err == nil:
				delivered(one)
			case sink.ClassOf(err) == sink.ClassLogic:
				refused++
				a.log.Error("the destination refused a line of an emergency dump; it stays in the "+
					"quarantined file", "id", it.ID, "error", err)
			default:
				return refused, err
			}
		}
	}
	return refused, nil
}

// dumpEmergency writes pending items to disk, or counts them lost (SPEC §9,
// §9.3).
//
// It takes App.dumping back as soon as the outcome is known and BEFORE
// anything is counted (settleDump): saved, an abort loses nothing more --
// but an envelope left out for a raw newline is lost, as emergency_failed;
// failed, the loss is counted here as emergency_failed; disabled
// (emergency.enabled=false), as abort, the reason of SPEC §9.3. An abort
// racing that count must not add the same events again (one owner of the
// count, SPEC §10.3).
//
// A dump the abort already counted gives up (emergency.Store.SetAbandon), and
// one that completed after the abort counts nothing more: the abort counted
// every event, the ones the dump leaves out included.
func (a *App) dumpEmergency(items []*envelope.Item) {
	if len(items) == 0 {
		a.settleDump()
		return
	}
	if !a.dumps {
		// emergency.enabled=false: the loss is the abort's (SPEC §9.3) --
		// unless an abort already took the counter and counted the same
		// events.
		if n := a.settleDump(); n > 0 {
			a.log.Error("emergency.enabled=false; these events are lost", "count", n)
			a.metrics.EventsDroppedTotal[obs.DropAbort].Add(uint64(n))
		}
		return
	}
	if a.beforeDump != nil {
		a.beforeDump()
	}
	path, skipped, err := a.emg.Write(items)
	owed := a.settleDump()
	if owed == 0 {
		// An abort counted every event already. A dump published between the
		// last abandon check and the abort is counted lost AND kept, an
		// overcount the next boot corrects by delivering it, never a loss.
		if err == nil && path != "" {
			a.log.Warn("the emergency dump completed after an abort counted its events lost; "+
				"the next boot reingests it", "path", path, "count", len(items)-skipped)
		}
		return
	}
	if skipped > 0 {
		// An envelope with a raw newline cannot be one line of the dump. The
		// serializer never writes one; if this ever fires it is a bug, and the
		// rest of the dump is still worth saving (SPEC §9.1, §9.3).
		a.log.Error("EMERGENCY DUMP: envelopes with a raw newline were left out; they are lost",
			"count", skipped)
	}
	if err != nil {
		a.log.Error("EMERGENCY DUMP FAILED; these events are lost",
			"count", len(items)-skipped, "dir", a.emg.Dir(), "error", err)
		a.metrics.EventsDroppedTotal[obs.DropEmergencyFailed].Add(uint64(len(items)))
		return
	}
	if skipped > 0 {
		a.metrics.EventsDroppedTotal[obs.DropEmergencyFailed].Add(uint64(skipped))
	}
	if path != "" {
		a.log.Warn("pending events written to an emergency dump; "+
			"they will be reingested at the next boot",
			"count", len(items)-skipped, "path", path)
		// Added, not re-read: refreshEmergencyGauges decodes every file in
		// full, which here -- the process about to exit, the leftover still
		// live -- doubled the memory of a dump the size of the ring, under a
		// GOMEMLIMIT of twice the ring.
		a.metrics.EmergencyFiles.Inc()
		a.metrics.EmergencyEventsPending.Add(int64(len(items) - skipped))
	}
}

// settleDump takes the dump's loss counter back once its outcome is known,
// under lossMu (the name and the form of the syslog/RELP family). abortLoss
// publishes aborted BEFORE it takes the counter, and a dump that read the flag
// in between (ErrAbandoned) took the whole count as emergency_failed: one
// count, the wrong reason, and the abort's line said nothing was lost
// (independent review of go-syslog-server's port, 2026-10-13, X2). Under the
// lock, a dump that saw the abort waits for abortLoss to finish and finds the
// counter taken.
func (a *App) settleDump() int64 {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	return a.dumping.Swap(0)
}

// refreshEmergencyGauges sets whs_emergency_* from what is on disk, which is
// the only honest source: a file the boot could not read stays there, and a
// dump adds to whatever was already pending. A failed count keeps the previous
// values, said at WARN: silent, a scrape showed stale gauges with nothing
// saying why (the rule of go-webhook-fast-server, ported 2026-10-13).
func (a *App) refreshEmergencyGauges() {
	files, events, err := a.emg.Count()
	if err != nil {
		a.log.Warn("cannot count the emergency dumps; whs_emergency_* keep their previous values",
			"dir", a.emg.Dir(), "error", err)
		return
	}
	a.metrics.EmergencyFiles.Set(int64(files))
	a.metrics.EmergencyEventsPending.Set(events)
}
