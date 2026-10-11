package app

import (
	"context"
	"errors"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/sink"
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
// two runs. The queue columns come from each line's own header (SPEC §3.7.1).
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

	a.log.Warn("emergency dumps found from a previous run; reingesting before any transport opens",
		"files", len(files), "dir", store.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	// One sink.boot_timeout for the whole reingestion, not one per file: with
	// N dumps on disk and the sink failing, a deadline per file would hold
	// every transport closed for N times the configured budget.
	deadline := time.Now().Add(a.cfg.Sink.BootTimeout)
	for i, path := range files {
		a.metrics.EmergencyFiles.Set(int64(len(files) - i))
		res, delivered, code := a.reingestFile(ctx, store, path, deadline)
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
			if qerr != nil {
				a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
			} else {
				a.log.Error("the dump was damaged; the intact lines were delivered and the "+
					"file was quarantined", "path", dst,
					"truncated_bytes", res.TruncatedBytes, "invalid_lines", res.Invalid,
					"invalid_bytes", res.InvalidBytes)
			}
		} else if rerr := store.Remove(path); rerr != nil {
			a.log.Error("cannot remove a reingested dump; it will be delivered again at the next "+
				"boot (the queue absorbs a uuid it still holds, not one already consumed)",
				"path", path, "error", rerr)
		}
	}
	a.metrics.EmergencyFiles.Set(0)
	a.metrics.EmergencyEventsPending.Set(0)
	return ExitOK
}

// reingestFile streams one dump into the sink, a chunk at a time (sink.batch
// envelopes, sink.batch_max_bytes of JLH): the file is never held whole,
// because it can be twelve times the ring that produced it (SPEC §12.2). It
// returns what the reading skipped and how many envelopes were delivered; a
// nil result means the file could not be read and stays in place.
func (a *App) reingestFile(ctx context.Context, store *emergency.Store, path string,
	deadline time.Time) (*emergency.ReadResult, int, int) {
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
			// Everything delivered so far is in the queue; the rest comes back
			// with the file at the next boot, and ON CONFLICT (uuid) absorbs
			// what this one already wrote.
			a.log.Error("cannot read an emergency dump; leaving it in place", "path", path,
				"delivered", delivered, "error", err)
			return nil, delivered, ExitOK
		}
		if len(recs) == 0 {
			res := r.Result()
			return &res, delivered, ExitOK
		}
		a.metrics.EmergencyEventsPending.Set(int64(len(recs)))
		if code := a.deliverDirect(ctx, envelopes(recs), deadline); code != ExitOK {
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "delivered", delivered)
			return nil, delivered, code
		}
		delivered += len(recs)
	}
}

// envelopes turns dumped records into what the sink writes. The item carries
// only the head fields the queue reads (uuid, from, space, event) and no raw
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
//   - network: the same chunk is retried with backoff until deadline (the
//     reingestion's sink.boot_timeout). One transient failure is not a reason
//     to give up the boot.
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
func (a *App) dumpEmergency(items []*item.Item) bool {
	if a.emg == nil || len(items) == 0 {
		return false
	}
	res, err := a.emg.Write(items, convert.New(a.convertConfig()), a.cfg.Syslog.AcceptMalformed)
	if err != nil {
		a.log.Error("EMERGENCY DUMP FAILED; these messages are lost",
			"count", len(items), "dir", a.emg.Dir(), "error", err)
		a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Add(uint64(len(items)))
		return false
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
		a.metrics.EmergencyFiles.Set(1)
		a.metrics.EmergencyEventsPending.Set(int64(res.Written))
	}
	return true
}
