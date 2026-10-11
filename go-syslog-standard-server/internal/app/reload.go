package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// watchSIGHUP re-reads the configuration and applies only what is safe to swap
// at runtime (SPEC §15.3). Everything else is reported as ignored, by name, so an
// operator who edited the wrong key learns it immediately instead of believing a
// restart-only change took effect.
//
// The filter is the main reason this exists here: it is the knob an operator
// reaches for at three in the morning, when one emitter goes mad (SPEC §7.1).
func (a *App) watchSIGHUP(ctx context.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	defer signal.Stop(ch)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			a.reload()
		}
	}
}

// reloadResult is what one SIGHUP did: for the log lines, and for the tests.
type reloadResult struct {
	applied []string // reloadable keys that changed against what is RUNNING
	ignored []string // restart-only keys that differ from the BOOT configuration
}

func (a *App) reload() reloadResult {
	fresh, err := config.Load(config.Options{Args: a.args})
	if err != nil {
		a.log.Error("SIGHUP: the new configuration is invalid and was NOT applied; "+
			"the running configuration is unchanged", "error", err)
		return reloadResult{}
	}
	if a.current == nil {
		a.current = a.loaded
	}

	// Two references, on purpose. A reloadable key is compared with what is
	// RUNNING -- the last snapshot applied -- so that reverting it to its boot
	// value is a change like any other and gets applied: the three-in-the-
	// morning rollback of the filter is the one reload that matters most. A
	// restart-only key is compared with the BOOT configuration, the one it is
	// still running with, so that every SIGHUP keeps warning about it until the
	// restart happens, not only the first. Either reference alone fails one of
	// the two cases. The comparison uses unredacted values, so a changed
	// password counts; only key names reach the log (SPEC §15.3).
	bootVals, curVals, newVals := a.loaded.Values(), a.current.Values(), fresh.Values()
	var res reloadResult
	for _, p := range fresh.Params {
		name := p.Name()
		if p.Reload {
			if curVals[name] != newVals[name] {
				res.applied = append(res.applied, name)
			}
		} else if bootVals[name] != newVals[name] {
			res.ignored = append(res.ignored, name)
		}
	}
	if len(res.applied) == 0 && len(res.ignored) == 0 {
		a.log.Info("SIGHUP: configuration unchanged")
		return res
	}

	if len(res.applied) > 0 {
		c := fresh.Config

		// The filter is compiled BEFORE anything is swapped. Validation
		// already accepted it, so this cannot fail in practice -- but if it
		// did, applying the rest while keeping a stale filter would leave the
		// running process in a state no configuration file describes.
		rule, err := filter.Compile(c.Filter.MinSeverity, c.Filter.Facilities)
		if err != nil {
			a.log.Error("SIGHUP: the filter did not compile; NOTHING was applied", "error", err)
			return reloadResult{ignored: res.ignored}
		}
		a.filter.Replace(rule)

		a.log.SetLevel(c.Log.Level)
		a.log.SetAccess(c.Log.Access, c.Log.AccessSample)
		a.limiter.SetParams(a.rateLimitParamsFrom(c))

		// sink.sqlite.keep_files is the only [sink] key SIGHUP may change. The
		// type assertion keeps this honest: with any other sink there is
		// nothing to apply, and claiming "applied" would be a lie the operator
		// discovers only when the disk fills anyway.
		if sq, ok := a.sink.(*sink.SQLite); ok {
			sq.SetKeepFiles(c.SinkSQL.KeepFiles)
		}

		// The reloadable half is now what runs; the boot snapshot stays in
		// a.loaded for the restart-only comparison above.
		a.current = fresh
		a.log.Info("SIGHUP: applied", "keys", res.applied,
			"filter_min_severity", syslog.SeverityName(rule.MinSeverity()))
	}
	for _, name := range res.ignored {
		a.log.Warn("SIGHUP: this key requires a restart and was ignored", "key", name)
	}
	return res
}
