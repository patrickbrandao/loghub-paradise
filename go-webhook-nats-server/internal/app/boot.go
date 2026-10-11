package app

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
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
func (a *App) reingest(ctx context.Context) int {
	if a.emg == nil {
		return ExitOK
	}

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
		a.log.Error("cannot list the emergency directory", "dir", a.emg.Dir(), "error", err)
		return ExitConfig
	}
	if len(files) == 0 {
		return ExitOK
	}

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
			if qerr != nil {
				a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
			} else {
				a.log.Error("the dump had damaged lines; its intact lines were reingested "+
					"and the file was quarantined (SPEC §9.2)",
					"path", dst, "damaged_lines", res.DamagedLines,
					"damaged_bytes", res.DamagedBytes, "refused_by_destination", refused)
			}
		} else if rerr := a.emg.Remove(path); rerr != nil {
			a.log.Error("cannot remove a reingested dump", "path", path, "error", rerr)
		}
	}

	a.refreshEmergencyGauges()
	return ExitOK
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

// dumpEmergency writes pending items to disk and returns how many of them are
// lost: the ones left out for a raw newline, or all of them when the write
// failed. It logs; the caller counts, so an abort racing the dump cannot count
// the same events again (see App.atRisk).
func (a *App) dumpEmergency(items []*envelope.Item) (lost int) {
	if len(items) == 0 {
		return 0
	}
	path, skipped, err := a.emg.Write(items)
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
		return len(items)
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
	return skipped
}

// refreshEmergencyGauges sets whs_emergency_* from what is on disk, which is
// the only honest source: a file the boot could not read stays there, and a
// dump adds to whatever was already pending.
func (a *App) refreshEmergencyGauges() {
	if a.emg == nil {
		return
	}
	files, events, err := a.emg.Count()
	if err != nil {
		return
	}
	a.metrics.EmergencyFiles.Set(int64(files))
	a.metrics.EmergencyEventsPending.Set(events)
}
