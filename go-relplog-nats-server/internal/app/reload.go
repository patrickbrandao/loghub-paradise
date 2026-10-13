package app

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/config"
)

// watchSIGHUP re-reads the configuration and applies only what is safe to swap
// at runtime (SPEC §16.3): log.level, log.access and log.access_sample.
// Everything else is reported as ignored, by name, so an operator who edited
// the wrong key learns it at once. Every [syslog] key is restart-only: it
// changes the conversion, and the JLH of an item must not depend on WHEN it
// was drained (SPEC §3.5).
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
	// alone fails one case (bugs 009 and 010 of the standard server).
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
		a.log.SetLevel(c.Log.Level)
		a.log.SetAccess(c.Log.Access, c.Log.AccessSample)
		a.current = res.Fresh
		a.log.Info("SIGHUP: applied", "keys", res.Applied)
	}
	for _, name := range res.Ignored {
		a.log.Warn("SIGHUP: this key requires a restart and was ignored", "key", name)
	}
	return out
}
