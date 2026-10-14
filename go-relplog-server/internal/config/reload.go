package config

import "strings"

// ReloadResult is what one SIGHUP found (SPEC §16.3).
type ReloadResult struct {
	// Fresh is the snapshot just loaded. When Applied is not empty, the app
	// applies Fresh's reloadable values and then makes Fresh its RUNNING
	// snapshot; the BOOT snapshot is never replaced.
	Fresh *Loaded

	// Applied are the reloadable keys whose new value differs from the RUNNING
	// snapshot, in registry order. The app swaps them in.
	Applied []string

	// Ignored are the restart-only keys whose new value differs from the BOOT
	// snapshot, in registry order. The app logs each one at WARN as "requires a
	// restart and was ignored". The same key is reported on every SIGHUP until
	// the restart, because the process keeps running with the boot value.
	Ignored []string

	// Shadowed are the keys the config file sets while an env variable or a
	// flag -- which win over the file (SPEC §16.1) and cannot change in a
	// running process -- decides the value, in registry order. Editing them in
	// the file can never take effect, not by SIGHUP and, for a restart-only
	// key, not by the restart either; the app logs each one at WARN on every
	// SIGHUP, so an operator who edited the file learns why nothing happened.
	Shadowed []string
}

// Changed reports whether the reload found anything to apply or to warn about.
func (r ReloadResult) Changed() bool { return len(r.Applied) > 0 || len(r.Ignored) > 0 }

// Reload loads a fresh snapshot with the boot Options and compares it (see
// Compare). running is the snapshot whose reloadable half is in effect; nil
// means the boot snapshot. An error means the new configuration is invalid and
// NOTHING may be applied: the running configuration stays as it is.
//
// Typical use in the app's SIGHUP handler:
//
//	res, err := config.Reload(opts, a.boot, a.running)
//	if err != nil { log ERROR, keep running as is; return }
//	for _, k := range res.Ignored { log WARN "requires a restart", k }
//	if len(res.Applied) > 0 {
//	    apply res.Fresh.Config's filter, log and ratelimit values and
//	    sink.sqlite.keep_files (prepare everything that can fail first);
//	    a.running = res.Fresh
//	}
func Reload(opts Options, boot, running *Loaded) (ReloadResult, error) {
	fresh, err := Load(opts)
	if err != nil {
		return ReloadResult{}, err
	}
	applied, ignored := Compare(boot, running, fresh)
	var shadowed []string
	for _, p := range fresh.Params {
		name := p.Name()
		if fresh.InFile[name] && (fresh.Source[name] == FromEnv || fresh.Source[name] == FromFlag) {
			shadowed = append(shadowed, name)
		}
	}
	return ReloadResult{Fresh: fresh, Applied: applied, Ignored: ignored, Shadowed: shadowed}, nil
}

// Compare is the two-reference comparison of SPEC §16.3.
//
// Two references, on purpose. A reloadable key is compared with what is
// RUNNING -- the last snapshot applied -- so that reverting it to its boot
// value is a change like any other and gets applied: the three-in-the-morning
// rollback of the filter is the one reload that matters most. A restart-only
// key is compared with the BOOT configuration, the one the process is still
// running with, so that every SIGHUP keeps warning about it until the restart
// happens, not only the first. Either reference alone fails one of the two
// cases (SPEC §16.3).
//
// The comparison uses unredacted values, so a changed password counts; only
// key names leave this function. running nil means boot.
func Compare(boot, running, fresh *Loaded) (applied, ignored []string) {
	if running == nil {
		running = boot
	}
	bootVals, curVals, newVals := boot.Values(), running.Values(), fresh.Values()
	for _, vals := range []map[string]string{bootVals, curVals, newVals} {
		// Synonyms are one value: an INI that says windows-1252 where the
		// boot said cp1252 changed nothing, and a SIGHUP that reported it as
		// "requires a restart" sent the operator after a restart that would
		// change nothing (independent review, 2026-10-13).
		vals["syslog.legacy_charset"] = canonicalCharset(vals["syslog.legacy_charset"])
	}
	for _, p := range fresh.Params {
		name := p.Name()
		if p.Reload {
			if curVals[name] != newVals[name] {
				applied = append(applied, name)
			}
		} else if bootVals[name] != newVals[name] {
			// A key derived in both snapshots (emergency.dir from
			// server.data_dir, sink.<type>.pool_size from sink.workers)
			// changed only because its source did, and the source is
			// reported: listing it too sent the operator after two restarts
			// for one edit. Explicit on one side and derived on the other is
			// a real change (SPEC §16.3).
			if derived(boot, name) && derived(fresh, name) {
				continue
			}
			ignored = append(ignored, name)
		}
	}
	return applied, ignored
}

// derived reports whether Load or Validate derived name in l rather than took
// it from a source: the "derived" notes of SPEC §16.2, the ones --dump-config
// comments. The other notes ("resolved to an absolute path", "lowered to 4")
// say how a value a source gave was normalised, and do not count.
func derived(l *Loaded, name string) bool { return strings.HasPrefix(l.Notes[name], "derived") }

// canonicalCharset folds the synonyms syslog.legacy_charset accepts into the
// one spelling convert.ParseCharset gives the same charset for.
// TestCharsetSynonymsAreOneValue pins the table to convert.ParseCharset.
func canonicalCharset(v string) string {
	switch v {
	case "windows-1252":
		return CharsetCP1252
	case "iso-8859-1":
		return CharsetLatin1
	}
	return v
}
