package app

import (
	"context"
	"errors"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/sink"
)

// reingest loads the emergency dumps a previous run left behind (SPEC §12.2).
//
// Files are processed oldest first, and one is removed only after ALL of its
// envelopes have been confirmed by the sink. A damaged file contributes its
// intact lines and is then quarantined rather than deleted: destroying evidence
// of a crash is not this program's call.
//
// The lines are finished envelopes, delivered as they are: no conversion runs,
// so a dump keeps its bytes even when the [syslog] keys changed between the
// two runs. The subject and the headers come from each line's own header
// (SPEC §3.8).
//
// EVERY dump is read, whatever emergency.enabled says NOW: only the WRITING of
// dumps depends on it (SPEC §12.2).
func (a *App) reingest(ctx context.Context) int {
	store := emergency.NewStore(a.cfg.Emergency.Dir, a.id.From)
	// A dump cut short leaves its hidden temporary (SPEC §12.1). The twin of a
	// complete dump goes; an interrupted one is reported, because nothing
	// else would tell the operator it is there. Its messages were counted lost
	// only when a second signal aborted the dump: after a SIGKILL -- the
	// stop_grace_period running out -- an OOM or a power loss, nothing
	// counted them, and the file may well be complete.
	if left, err := store.Leftovers(); err == nil && len(left) > 0 {
		a.log.Warn("interrupted emergency dumps found; they are NOT reingested. Their messages "+
			"were counted as lost only if a second signal aborted the dump; after a SIGKILL or a "+
			"crash nothing counted them. Renamed to their final name (no leading dot, no .tmp), "+
			"their intact lines are reingested at the next boot (SPEC §12.1)",
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

	// Exact and free: every listed file awaits reingestion. Its envelopes are
	// only known once read, and are counted from the disk at the end. Set
	// before the boot state that announces the reingestion, so whoever sees
	// that state sees the count too (go-relplog-server, pendência 25 of
	// go-syslog-server).
	a.metrics.EmergencyFiles.Set(int64(len(files)))
	a.log.Warn("emergency dumps found from a previous run; reingesting before any transport opens",
		"files", len(files), "dir", store.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	// One sink.boot_timeout WITHOUT PROGRESS for the whole reingestion, not
	// one per file: with N dumps on disk and the sink failing, a deadline per
	// file would hold every transport closed for N times the configured
	// budget. Every delivered chunk moves it forward: a single deadline from
	// the start made a healthy reingestion of a large dump that ran past
	// boot_timeout exit 3 at the first transient failure, with no retry, and
	// every boot after it republish the same file from the start (independent
	// review, 2026-10-13; SPEC §12.2).
	deadline := time.Now().Add(a.cfg.Sink.BootTimeout)
	for _, path := range files {
		res, delivered, code := a.reingestFile(ctx, store, path, &deadline)
		if code != ExitOK {
			return code
		}
		if res == nil {
			continue // unreadable: left in place, already logged
		}
		a.log.Info("emergency dump reingested", "path", path, "events", delivered,
			"truncated", res.Truncated, "invalid_lines", res.Invalid)

		if res.Damaged() {
			dst, qerr := store.Quarantine(path)
			a.reportQuarantine(path, dst, qerr, res)
		} else {
			a.reportRemoval(path, store.Remove(path))
		}
	}
	// Recounted from the directory, never zeroed: a file that could not be
	// read, removed or quarantined is still there, still pending, and an
	// alert on these gauges must keep firing until somebody deals with it
	// (SPEC §12.2).
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
	a.log.Error("the dump was damaged; the intact lines were delivered and the "+
		"file was quarantined", "path", dst,
		"truncated_bytes", res.TruncatedBytes, "invalid_lines", res.Invalid,
		"invalid_bytes", res.InvalidBytes)
}

// reportRemoval logs the outcome of Store.Remove for a reingested dump. An
// ErrNotDurable means the unlink happened and only the directory fsync after
// it failed: the file is gone now, and "cannot remove" would be false. What
// the operator must know is the other consequence -- after a power loss the
// name may come back, and the next boot would publish the dump again.
func (a *App) reportRemoval(path string, rerr error) {
	switch {
	case rerr == nil:
	case errors.Is(rerr, emergency.ErrNotDurable):
		a.log.Warn("the reingested dump was removed but the directory fsync failed; after a "+
			"power loss it may reappear and be published again (the stream absorbs it only "+
			"within its duplicate_window)", "path", path, "error", rerr)
	default:
		a.log.Error("cannot remove a reingested dump; it will be delivered again at the next "+
			"boot (the stream absorbs it only within its duplicate_window)",
			"path", path, "error", rerr)
	}
}

// refreshEmergencyGauges sets relplog_emergency_files and
// relplog_emergency_events_pending from what is on disk, the only honest
// source (SPEC §12.2). Every listed file counts, one that cannot even be read
// included; its messages are unknowable and count as none.
func (a *App) refreshEmergencyGauges(store *emergency.Store) {
	files, events, err := store.Count()
	if err == nil {
		a.metrics.EmergencyFiles.Set(int64(files))
		a.metrics.EmergencyEventsPending.Set(events)
		return
	}
	a.log.Warn("cannot count the emergency dumps; relplog_emergency_* keep their previous values",
		"dir", store.Dir(), "error", err)
}

// reingestFile streams one dump into the sink, a chunk at a time (sink.batch
// envelopes, sink.batch_max_bytes of JLH): the file is never held whole,
// because it can be twelve times the ring that produced it (SPEC §12.2). It
// returns what the reading skipped and how many envelopes were delivered; a
// nil result means the file could not be read and stays in place.
func (a *App) reingestFile(ctx context.Context, store *emergency.Store, path string,
	deadline *time.Time) (*emergency.ReadResult, int, int) {
	r, err := store.Open(path)
	if err != nil {
		a.log.Error("cannot read an emergency dump; leaving it in place", "path", path, "error", err)
		return nil, 0, ExitOK
	}
	defer r.Close()
	delivered := 0
	for {
		recs, err := r.Next(a.cfg.Sink.Batch, int(a.cfg.Sink.BatchMaxBytes))
		if err != nil {
			// Everything delivered so far is in the stream; the rest comes back
			// with the file at the next boot, and Nats-Msg-Id <uuid>.0 lets the
			// stream absorb, within its duplicate_window, what this one already
			// published.
			a.log.Error("cannot read an emergency dump; leaving it in place", "path", path,
				"delivered", delivered, "error", err)
			return nil, delivered, ExitOK
		}
		if len(recs) == 0 {
			res := r.Result()
			return &res, delivered, ExitOK
		}
		// relplog_emergency_events_pending is not touched here: the gauges
		// say what is on disk, and are recounted from it at the end. Set to
		// the chunk size, the last scrape before an exit 3 showed one chunk
		// for a whole dump.
		if code := a.deliverDirect(ctx, envelopes(recs), *deadline); code != ExitOK {
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "delivered", delivered)
			return nil, delivered, code
		}
		delivered += len(recs)
		*deadline = time.Now().Add(a.cfg.Sink.BootTimeout)
	}
}

// envelopes turns dumped records into what the sink writes. The item carries
// only the head fields the publication reads (uuid, space; from and event for
// symmetry with the workers' envelopes) and no raw
// line: the envelope is already built, and nothing converts it again.
func envelopes(recs []emergency.Record) []sink.Envelope {
	out := make([]sink.Envelope, len(recs))
	for i := range recs {
		r := &recs[i]
		h := item.Head{UUID: r.UUID, From: r.From, Space: r.Space, UTS: r.Event.UTS, Sub: r.Event.Sub}
		out[i] = sink.Envelope{Item: item.New(h, nil, -1), JLH: r.JLH, Level: r.Level, SID: r.SID}
	}
	return out
}

// deliverDirect writes envelopes with the drain workers' per-class rules
// (SPEC §10.2), which the boot needs just as much as the workers do. The rules
// themselves live in sink.Instrumented.Deliver, chunk by chunk; this is the
// boot's policy around them:
//
//   - logic: the destination refused that data for good. It is counted and
//     logged there, and treated as delivered so the dump is removed. Exiting
//     instead would leave the same dump for the next boot, which would refuse
//     it the same way: a crash loop with every transport closed, forever.
//   - network: the same chunk is retried with backoff until deadline (one
//     sink.boot_timeout since the last chunk delivered). One transient
//     failure is not a reason to give up the boot.
//   - fatal: exit 3.
//
// A cancelled context (SIGTERM during the boot) is 130, not 3.
func (a *App) deliverDirect(ctx context.Context, envs []sink.Envelope, deadline time.Time) int {
	backoff := a.cfg.Sink.RetryBackoffMin
	errBootTimeout := errors.New("sink.boot_timeout exceeded")
	err := a.direct.Deliver(ctx, envs, a.cfg.Sink.OpTimeout, func(attempt int, err error) error {
		if time.Now().After(deadline) {
			a.log.Error("the sink kept failing at boot past sink.boot_timeout", "error", err,
				"attempts", attempt)
			return errBootTimeout
		}
		if attempt == 1 || attempt%10 == 0 {
			a.log.Warn("boot delivery failed, retrying", "attempt", attempt,
				"error", err, "retry_in", backoff.String())
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

// dumpEmergency converts the pending items and writes them to disk, one JLH per
// line (SPEC §12.1). It reports whether they were saved; false means they are
// genuinely lost.
//
// The converter is a fresh one with the frozen conversion config: the workers
// have stopped, and their converters went with them.
//
// It zeroes App.dumping as soon as the outcome is known and BEFORE anything is
// counted: saved, an abort loses nothing more; failed, the loss is counted
// here as emergency_failed, and an abort racing that count must not add the
// same messages again as abort (settleDump: one owner of the count, under
// lossMu, SPEC §13.4).
//
// A dump the abort already counted gives up (emergency.Store.SetAbandon), and
// one that completed after the abort counts nothing more: the abort counted
// every message, the malformed ones the dump leaves out included.
func (a *App) dumpEmergency(items []*item.Item) bool {
	if len(items) == 0 {
		a.dumping.Store(0)
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
	res, err := a.emg.Write(items, convert.New(a.convertConfig()), a.cfg.Syslog.AcceptMalformed)
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
		// lost AND kept, an overcount the next boot corrects by publishing
		// it, never a loss. Nothing more is counted (independent review of
		// go-relplog-pg-server, 2026-10-13).
		a.log.Warn("the emergency dump completed after an abort counted its messages lost; "+
			"the next boot reingests it", "path", res.Path, "count", res.Written)
		return true
	}
	if res.Refused > 0 {
		// syslog.accept_malformed=false: the worker would have refused these
		// too (SPEC §6.6), so the dump does not resurrect them.
		a.metrics.EventsDroppedTotal[obs.EventDropMalformed].Add(uint64(res.Refused))
		a.log.Warn("malformed messages left out of the dump (syslog.accept_malformed=false)",
			"count", res.Refused)
	}
	if res.Written > 0 {
		a.log.Warn("pending messages written to an emergency dump; they will be reingested at the next boot",
			"count", res.Written, "path", res.Path)
		// Added, not set: a dump the boot could not reingest may still be on
		// disk (unreadable, a failed removal), and the gauges say what is
		// pending there, not what this one dump holds (SPEC §12.2).
		a.metrics.EmergencyFiles.Inc()
		a.metrics.EmergencyEventsPending.Add(int64(res.Written))
	}
	return true
}
