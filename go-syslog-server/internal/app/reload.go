package app

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// watchSIGHUP re-reads the configuration and applies only what is safe to swap
// at runtime (SPEC §16.3). Everything else is reported as ignored, by name, so
// an operator who edited the wrong key learns it at once. Every [syslog] key is
// restart-only: it changes the conversion, and the JLH of an item must not
// depend on WHEN it was drained (SPEC §3.5).
//
// The signal is watched for the whole run, not only while the server is up:
// outside the window between the end of the boot and the start of the
// shutdown it is logged and ignored. Unwatched, Go's default action would end
// the process with 129 in the middle of a 300 s boot.
func (a *App) watchSIGHUP(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.hup:
			if !a.reloadOpen.Load() {
				a.log.Warn("SIGHUP ignored: the configuration is reloaded only while the server is up, " +
					"not during the boot or the shutdown")
				continue
			}
			a.reload()
		}
	}
}

// reloadResult is what one SIGHUP did: for the log lines, and for the tests.
type reloadResult struct {
	applied  []string
	ignored  []string
	shadowed []string
}

func (a *App) reload() reloadResult {
	// Two references, kept by the config package: a reloadable key is compared
	// with what is RUNNING, a restart-only key with the BOOT snapshot. Either
	// alone fails one case (SPEC §16.3).
	res, err := config.Reload(a.opts, a.loaded, a.current)
	if err != nil {
		a.log.Error("SIGHUP: the new configuration is invalid and was NOT applied; "+
			"the running configuration is unchanged", "error", err)
		return reloadResult{}
	}
	out := reloadResult{applied: res.Applied, ignored: res.Ignored, shadowed: res.Shadowed}
	// Logged on every SIGHUP, changed or not: "configuration unchanged" alone
	// is exactly what an operator who edited a shadowed key sees, and it
	// does not say why.
	for _, name := range res.Shadowed {
		a.log.Warn("SIGHUP: this key is set in the config file, but an environment variable or "+
			"a flag decides its value; editing the file has no effect", "key", name,
			"origin", res.Fresh.Origin[name])
	}
	if !res.Changed() {
		a.log.Info("SIGHUP: configuration unchanged")
		return out
	}

	if len(res.Applied) > 0 {
		c := res.Fresh.Config
		// The filter is compiled BEFORE anything is swapped: applying the rest
		// with a stale filter would leave the process in a state no
		// configuration file describes.
		rule, err := filter.Compile(c.Filter.MinSeverity, c.Filter.Facilities)
		if err != nil {
			a.log.Error("SIGHUP: the filter did not compile; NOTHING was applied", "error", err)
			return reloadResult{ignored: res.Ignored, shadowed: res.Shadowed}
		}
		a.filter.Replace(rule)
		a.log.SetLevel(c.Log.Level)
		a.log.SetAccess(c.Log.Access, c.Log.AccessSample)
		a.limiter.SetParams(rateLimitParamsFrom(c))

		// sink.sqlite.keep_files is the only [sink] key SIGHUP may change; with
		// any other sink there is nothing to apply.
		if sq, ok := a.sink.(*sink.SQLite); ok {
			sq.SetKeepFiles(c.SinkSQL.KeepFiles)
		}
		a.current = res.Fresh
		a.log.Info("SIGHUP: applied", "keys", res.Applied,
			"filter_min_severity", convert.SeverityNames[rule.MinSeverity()])
	}
	for _, name := range res.Ignored {
		a.log.Warn("SIGHUP: this key requires a restart and was ignored", "key", name)
	}
	return out
}
