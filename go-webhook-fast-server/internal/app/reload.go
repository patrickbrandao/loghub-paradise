package app

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/sink"
)

// watchSIGHUP catches SIGHUP until the returned stop function is called, which
// leaves it ignored. While serving, each one re-reads the configuration and
// applies only what is safe to swap at runtime (SPEC §13.3); everything else is
// reported as ignored, by name, so an operator who edited the wrong key learns
// it immediately instead of believing a restart-only change took effect.
//
// Outside serving -- the boot, and the shutdown from the moment the workers
// stop -- a SIGHUP is answered with a WARN and nothing is reloaded. Catching it
// there is the point: the watcher used to live only while serving and to end
// with signal.Stop, which handed SIGHUP back to Go's default action, and one
// arriving during the emergency dump killed the process with the whole ring,
// answered 200, lost and counted nowhere (independent review of the
// 2026-10-11 port).
func (a *App) watchSIGHUP() (stop func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				if a.reloads.Load() {
					a.reload()
					continue
				}
				a.log.Warn("SIGHUP ignored outside of serving (boot or shutdown); the configuration " +
					"was NOT reloaded -- send it again once /readyz answers 200")
			}
		}
	}()
	// Ignore, not Stop: Stop would hand SIGHUP back to Go's default action,
	// and a signal between here and os.Exit would end the process by signal
	// instead of with the exit code run computed. Ignore also undoes Notify.
	return func() {
		signal.Ignore(syscall.SIGHUP)
		close(done)
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
	// value is a change like any other and gets applied. A restart-only key is
	// compared with the BOOT configuration, the one it is still running with,
	// so that every SIGHUP keeps warning about it until the restart happens,
	// not only the first. Either reference alone fails one of the two cases.
	// The comparison uses unredacted values, so a changed password counts;
	// only key names reach the log (SPEC §13.3).
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

		a.log.SetLevel(c.Log.Level)
		a.log.SetAccess(c.Log.Access, c.Log.AccessSample)

		a.limiter.SetParams(a.rateLimitParamsFrom(c))

		if trusted, err := config.ParsePrefixes(c.Proxy.Trusted); err == nil {
			a.srv.Reload(trusted, c.Proxy.MaxXFFEntries, envelope.ExcludeSet(c.Headers.Exclude))
		} else {
			a.log.Error("SIGHUP: proxy.trusted is invalid and was not applied", "error", err)
		}

		// sink.sqlite.keep_files is the only [sink] key SPEC §13.3 lets SIGHUP
		// change. The type assertion is what keeps this honest: with any other
		// sink there is nothing to apply, and saying "applied" would be a lie
		// the operator only discovers when the disk fills anyway.
		if sq, ok := a.sink.(*sink.SQLite); ok {
			sq.SetKeepFiles(c.SinkSQL.KeepFiles)
		}

		// The reloadable half is now what runs; the boot snapshot stays in
		// a.loaded for the restart-only comparison above.
		a.current = fresh
		a.log.Info("SIGHUP: applied", "keys", res.applied)
	}
	for _, name := range res.ignored {
		a.log.Warn("SIGHUP: this key requires a restart and was ignored", "key", name)
	}
	return res
}
