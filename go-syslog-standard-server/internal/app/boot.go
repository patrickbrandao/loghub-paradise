package app

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"
)

// reingest loads the emergency dumps a previous run left behind (SPEC §11.2).
//
// Files are processed oldest first, and one is removed only after ALL of its
// items have been confirmed by the sink. A file cut short by the crash that
// created it contributes its intact prefix and is then quarantined rather than
// deleted: what remains is unreadable to this program, but destroying evidence of
// a crash is not its call.
//
// There is no metadata to rebuild. The dump holds item head plus raw line, and
// the drain-path parse turns those back into the exact envelope the original
// would have produced -- the trap the sibling service documents around
// MetaFromJSON does not exist here.
func (a *App) reingest(ctx context.Context) int {
	if a.emg == nil {
		return ExitOK
	}

	files, err := a.emg.List()
	if err != nil {
		a.log.Error("cannot list the emergency directory", "dir", a.emg.Dir(), "error", err)
		return ExitConfig
	}
	if len(files) == 0 {
		return ExitOK
	}

	a.log.Warn("emergency dumps found from a previous run; reingesting before any transport opens",
		"files", len(files), "dir", a.emg.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	for _, path := range files {
		res, err := a.emg.Read(path)
		if err != nil {
			a.log.Error("cannot read an emergency dump; leaving it in place",
				"path", path, "error", err)
			continue
		}

		a.metrics.EmergencyFiles.Set(int64(len(files)))
		a.metrics.EmergencyEventsPending.Set(int64(len(res.Items)))

		if err := a.writeDirect(ctx, res.Items); err != nil {
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "items", len(res.Items), "error", err)
			return ExitSink
		}
		a.log.Info("emergency dump reingested", "path", path, "items", len(res.Items),
			"truncated", res.Truncated, "undecodable", res.Undecodable)

		if res.Truncated || res.Undecodable > 0 {
			dst, qerr := a.emg.Quarantine(path)
			if qerr != nil {
				a.log.Error("cannot quarantine a damaged dump", "path", path, "error", qerr)
			} else {
				a.log.Warn("the dump was damaged; the intact records were delivered and the "+
					"file was quarantined", "path", dst,
					"discarded_bytes", res.TruncatedBytes, "undecodable", res.Undecodable)
			}
		} else if rerr := a.emg.Remove(path); rerr != nil {
			a.log.Error("cannot remove a reingested dump", "path", path, "error", rerr)
		}
	}

	a.metrics.EmergencyFiles.Set(0)
	a.metrics.EmergencyEventsPending.Set(0)
	return ExitOK
}

// writeDirect parses and sends items straight to the sink in batches, bypassing
// the ring. Reingestion runs before any transport is open, so routing through the
// ring would only risk filling it with the backlog it exists to absorb.
func (a *App) writeDirect(ctx context.Context, items []*envelope.Item) error {
	prep := sink.NewPrep(a.cfg.Node.ID, a.parseOptions(), a.metrics)
	defer prep.Release()

	n := a.cfg.Sink.Batch
	for i := 0; i < len(items); i += n {
		end := min(i+n, len(items))
		batch := prep.Build(items[i:end])

		wctx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
		err := a.sink.Write(wctx, batch)
		cancel()
		if err != nil {
			return err
		}
		a.metrics.EventsWrittenTotal.Add(uint64(end - i))
		for _, it := range items[i:end] {
			it.Release()
		}
	}
	return nil
}

// drainProgressTimeout is how long one Dequeue of the boot drain may find
// nothing before the accounting is suspected. With no transport open, rows can
// only be there or not: a wait this long is never a slow emitter.
const drainProgressTimeout = 5 * time.Second

// drainBacklog empties a persistent ring before any transport opens
// (SPEC §12.1 step 6).
//
// Only ring=sqlite can have one: memory starts empty and none never queues.
// Blocking here costs availability proportional to the backlog, which is the
// deliberate trade for strict ordering and a clean starting state.
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

	prep := sink.NewPrep(a.cfg.Node.ID, a.parseOptions(), a.metrics)
	defer prep.Release()

	recounted := false
	for {
		st := a.ring.Stats()
		if st.Items == 0 {
			a.log.Info("backlog drained", "items", total, "took", time.Since(start).String())
			return ExitOK
		}
		a.admin.SetBootState("draining backlog: " + strconv.FormatInt(st.Items, 10) + " pending")

		// A Dequeue that finds nothing while the accounting says there is
		// something is a ring that cannot make progress. Waiting forever here
		// would keep every transport closed with no exit code for a supervisor
		// to act on, so the accounting is re-read from the table once, and if
		// it still disagrees the boot ends with 3 (SPEC §9.4).
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

		batch := prep.Build(items)
		wctx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
		err = a.sink.Write(wctx, batch)
		cancel()
		if err != nil {
			a.log.Error("cannot deliver the backlog; every transport stays closed",
				"error", err, "pending", st.Items)
			return ExitSink
		}
		a.metrics.EventsWrittenTotal.Add(uint64(len(items)))
		_ = a.ring.Commit(items)
		for _, it := range items {
			it.Release()
		}
	}
}

// dumpEmergency writes pending items to disk. It reports whether they were
// saved; false means they are genuinely lost.
func (a *App) dumpEmergency(items []*envelope.Item) bool {
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
	a.log.Warn("pending messages written to an emergency dump; "+
		"they will be reingested at the next boot",
		"count", len(items), "path", path)
	a.metrics.EmergencyFiles.Set(1)
	a.metrics.EmergencyEventsPending.Set(int64(len(items)))
	return true
}
