package app

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// reingest loads the emergency dumps a previous run left behind (SPEC §9.2).
//
// Files are processed oldest first, and one is removed only after ALL of its
// items have been confirmed by the sink. A file cut short by the crash that
// created it contributes its intact prefix and is then quarantined rather than
// deleted: what remains is unreadable to this program, but destroying evidence
// of a crash is not its call.
//
// It runs at every boot, whatever this life's ring mode and emergency.enabled:
// the dumps are a previous life's (SPEC §9).
func (a *App) reingest(ctx context.Context) int {
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
			"their intact records are reingested at the next boot (SPEC §9.1)",
			"files", len(left), "dir", a.emg.Dir(), "paths", left)
	}

	files, err := a.emg.List()
	if err != nil {
		if !a.dumps {
			// This life never writes there, and until 2026-10-11 never even
			// looked: a directory it cannot list must not stop it (SPEC
			// §9.2). Whatever is in it stays, named in the log.
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

	if a.cfg.Sink.Type == config.SinkNoneType {
		// sink=none discards by configuration (SPEC §8.4). Delivering a
		// previous life's dump to it and removing the file destroyed events a
		// real destination's life had answered 200 -- a load test on the same
		// volume after an incident (independent review of this round). They
		// wait for a life with a destination (SPEC §9.2).
		a.log.Warn("emergency dumps found, but sink.type=none discards what it is given; they are "+
			"NOT reingested and stay on disk for a boot with a destination (SPEC §9.2)",
			"files", len(files), "dir", a.emg.Dir())
		a.refreshEmergencyGauges()
		return ExitOK
	}

	// Exact and free: every listed file awaits reingestion. Its events are
	// only known once read, and are counted at the end (SPEC §11.1). Set
	// before the boot state that announces the reingestion, so whoever sees
	// that state sees the count too.
	a.metrics.EmergencyFiles.Set(int64(len(files)))
	a.log.Warn("emergency dumps found from a previous run; reingesting before serving",
		"files", len(files), "dir", a.emg.Dir())
	a.admin.SetBootState("reingesting emergency dumps")

	for _, path := range files {
		res, err := a.emg.Read(path)
		if errors.Is(err, emergency.ErrLegacy) {
			// SPEC §9.2: a dump of the pre-LogHub format is set aside, never
			// reingested -- no destination understands that envelope -- and
			// never deleted.
			dst, rerr := a.emg.SetAsideLegacy(path)
			if rerr != nil {
				a.log.Error("cannot set aside a pre-LogHub dump; leaving it in place",
					"path", path, "error", rerr)
				continue
			}
			a.log.Error("emergency dump in the pre-LogHub format; it is NOT reingested -- "+
				"reingest it with the previous version (SPEC §9.2)", "path", dst)
			continue
		}
		if err != nil {
			a.log.Error("cannot read an emergency dump; leaving it in place",
				"path", path, "error", err)
			continue
		}

		if err := a.writeDirect(ctx, res.Items); err != nil {
			if ctx.Err() != nil {
				// SIGTERM during the boot: the file stays for the next one,
				// and the exit is the interruption's, as in WaitReady
				// (SPEC §9.2).
				a.log.Info("interrupted while reingesting an emergency dump; it stays on disk",
					"path", path)
				return ExitAborted
			}
			a.log.Error("cannot deliver an emergency dump; it stays on disk for the next boot",
				"path", path, "items", len(res.Items), "error", err)
			return ExitSink
		}
		a.log.Info("emergency dump reingested",
			"path", path, "items", len(res.Items), "truncated", res.Truncated)

		if res.Truncated {
			dst, qerr := a.emg.Quarantine(path)
			if qerr != nil {
				a.log.Error("cannot quarantine a truncated dump", "path", path, "error", qerr)
			} else {
				a.log.Warn("the dump was truncated; its unreadable tail was quarantined",
					"path", dst, "discarded_bytes", res.TruncatedBytes)
			}
		} else if rerr := a.emg.Remove(path); rerr != nil {
			a.log.Error("cannot remove a reingested dump", "path", path, "error", rerr)
		}
	}

	a.refreshEmergencyGauges()
	return ExitOK
}

// refreshEmergencyGauges sets whs_emergency_* from what is on disk, the only
// honest source (SPEC §11.1): a file the boot could not read, or could not
// remove, stays there and is still pending. Setting both to zero here hid an
// unreadable dump from every alert on them (independent review of the
// 2026-10-11 port).
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

// writeDirect sends items straight to the sink in batches, bypassing the ring.
// Reingestion runs before the ring is serving, so routing through it would only
// risk filling it with the backlog it is supposed to absorb.
func (a *App) writeDirect(ctx context.Context, items []*envelope.Item) error {
	batch := a.cfg.Sink.Batch
	for i := 0; i < len(items); i += batch {
		end := min(i+batch, len(items))
		wctx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
		err := a.sink.Write(wctx, items[i:end])
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

// drainBacklog empties the ring.db backlog before the public listener opens
// (SPEC §10.1 step 5): this life's, with ring=sqlite, or the one a ring=sqlite
// life left behind for a life with another ring mode (SPEC §7.4).
//
// Only a ring.db can hold one: memory starts empty and none never queues.
// Blocking here costs availability proportional to the backlog, which is the
// deliberate trade for strict ordering and a clean starting state.
func (a *App) drainBacklog(ctx context.Context) int {
	sr, ok := a.ring.(*ring.SQLiteRing)
	if !ok {
		return a.drainLeftRing(ctx)
	}
	if k := sr.Kept(); k > 0 {
		a.warnKept(k)
	}
	return a.drainRing(ctx, sr)
}

// warnKept reports the rows of a previous life that sink=none leaves in
// ring.db (SPEC §7.4). Draining them into the discard destroyed events a life
// with a destination had answered 200 -- a load test on the same volume after
// an incident, the case of the dump's rule (SPEC §9.2).
func (a *App) warnKept(n int64) {
	a.log.Warn("ring.db holds events a previous life left, but sink.type=none discards what it "+
		"is given; they are NOT delivered and stay in ring.db for a boot with a destination "+
		"(SPEC §7.4)", "events", n, "path", a.cfg.RingSQL.Path)
}

// drainLeftRing delivers what a ring=sqlite life left in ring.db when this
// life runs another ring mode, and then removes the file (SPEC §7.4). Until
// 2026-10-11 such a boot never even looked: the rows, answered 200 and
// promised to "the next boot" by the shutdown's WARN, stayed there with no
// line of log (TESTING.md §8 item 42).
func (a *App) drainLeftRing(ctx context.Context) int {
	path := a.cfg.RingSQL.Path
	left, err := ring.ProbeSQLite(path)
	if err != nil {
		// Like an emergency.dir this life never writes to (SPEC §9.2): named,
		// and the boot goes on.
		a.log.Error("cannot read the ring.db a ring.mode=sqlite life may have left; "+
			"any events in it are NOT delivered", "path", path, "error", err)
		return ExitOK
	}
	if !left.Ring {
		return ExitOK
	}

	drained, opened := left.Rows == 0, false
	switch {
	case left.Rows == 0:
	case a.cfg.Sink.Type == config.SinkNoneType:
		a.warnKept(left.Rows)
	default:
		a.log.Warn("ring.db holds events a ring.mode=sqlite life left; delivering them before "+
			"opening the public listener, then removing the file (SPEC §7.4)",
			"events", left.Rows, "path", path, "ring_mode", a.cfg.Ring.Mode)
		r, err := ring.NewSQLite(ring.SQLiteConfig{
			Path:              path,
			Synchronous:       a.cfg.RingSQL.Synchronous,
			CommitBatch:       a.cfg.RingSQL.CommitBatch,
			CommitMaxDelay:    a.cfg.RingSQL.CommitMaxDelay,
			BusyTimeout:       a.cfg.RingSQL.BusyTimeout,
			WALAutocheckpoint: a.cfg.RingSQL.WALAutocheckpoint,
		}, a.log.Logger)
		if err != nil {
			a.log.Error("cannot open the ring.db a ring.mode=sqlite life left; its events are NOT "+
				"delivered", "path", path, "events", left.Rows, "error", err)
			return ExitOK
		}
		opened = true
		code := a.drainRing(ctx, r)
		// Closed before anything is removed: the last connection checkpoints
		// the WAL into the database.
		_ = r.Close()
		if code != ExitOK {
			return code // the rows not yet delivered stay for the next boot
		}
		drained = true
	}

	if left.Legacy > 0 {
		// The file stays with the pre-LogHub table. NewSQLite already said
		// so when the drain opened it.
		if !opened {
			a.log.Error("ring sqlite: the pre-LogHub table still holds events in the old envelope "+
				"format; they are left untouched and NOT delivered -- drain them with the previous "+
				"version (SPEC §7.4)", "table", "ring", "events", left.Legacy, "path", path)
		}
		return ExitOK
	}
	if left.Shared {
		// Other tables live in the file: the events.db, when ring.sqlite.path
		// and sink.sqlite.path coincide under another ring. Removing it took
		// the destination with it, from under the sink that had it open
		// (independent review of this round).
		if opened {
			a.log.Warn("the ring.db file holds tables that are not the ring's; its events were "+
				"delivered, and the file is NOT removed (SPEC §7.4)", "path", path)
		}
		return ExitOK
	}
	if drained {
		a.removeLeftRing(path)
	}
	return ExitOK
}

// removeLeftRing removes a ring.db with nothing left to deliver (SPEC §7.4):
// the WAL and the shared memory first, the database last, so that a crash in
// between never leaves an old WAL beside a database that is gone -- SQLite
// could replay it into the next one created there.
func (a *App) removeLeftRing(path string) {
	for _, p := range []string{path + "-wal", path + "-shm", path} {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			// Everything in it was delivered and deleted before the ring
			// closed: a next boot finds it empty and tries again.
			a.log.Error("cannot remove the ring.db a ring.mode=sqlite life left; nothing in it is pending",
				"path", p, "error", err)
			return
		}
	}
	a.log.Info("ring.db left by a ring.mode=sqlite life removed", "path", path)
}

// drainRing delivers every row of a ring.db to the sink, straight, before the
// public listener opens.
func (a *App) drainRing(ctx context.Context, r *ring.SQLiteRing) int {
	n := r.Backlog()
	if n == 0 {
		return ExitOK
	}

	a.log.Warn("draining the ring backlog before opening the public listener", "items", n)
	start := time.Now()

	for {
		st := r.Stats()
		if st.Items == 0 {
			a.log.Info("backlog drained", "items", n, "took", time.Since(start).String())
			return ExitOK
		}
		a.admin.SetBootState("draining backlog: " +
			itoa(st.Items) + " pending")

		items, err := r.Dequeue(ctx, a.cfg.Sink.Batch)
		if err != nil {
			if ctx.Err() != nil {
				a.log.Info("interrupted while draining the backlog")
				return ExitAborted
			}
			a.log.Error("cannot read the ring backlog", "error", err)
			return ExitSink
		}
		if len(items) == 0 {
			continue
		}

		wctx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
		err = a.sink.Write(wctx, items)
		cancel()
		if err != nil && ctx.Err() != nil {
			// The rows stay in ring.db for the next boot (SPEC §9.2).
			a.log.Info("interrupted while draining the backlog")
			return ExitAborted
		}
		if err != nil {
			a.log.Error("cannot deliver the backlog; the public listener stays closed",
				"error", err, "pending", st.Items)
			return ExitSink
		}
		a.metrics.EventsWrittenTotal.Add(uint64(len(items)))
		_ = r.Commit(items)
		for _, it := range items {
			it.Release()
		}
	}
}

// dumpEmergency writes pending items to disk and returns how many of them are
// lost: all of them when the write failed. It logs; the caller counts, so an
// abort racing the dump cannot count the same events again (see App.atRisk).
func (a *App) dumpEmergency(items []*envelope.Item) (lost int) {
	if len(items) == 0 {
		return 0
	}
	path, err := a.emg.Write(items)
	if err != nil {
		a.log.Error("EMERGENCY DUMP FAILED; these events are lost",
			"count", len(items), "dir", a.emg.Dir(), "error", err)
		return len(items)
	}
	a.log.Warn("pending events written to an emergency dump; "+
		"they will be reingested at the next boot",
		"count", len(items), "path", path)
	// Added, not set: a file the boot left behind is still pending beside
	// this one. And not re-read: refreshEmergencyGauges decodes every file in
	// full, which here -- the process about to exit, the leftover still live
	// -- would double the memory of a dump the size of the ring (SPEC §11.1).
	a.metrics.EmergencyFiles.Inc()
	a.metrics.EmergencyEventsPending.Add(int64(len(items)))
	return 0
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
