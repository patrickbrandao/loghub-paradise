package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Source records where a parameter's final value came from, for the
// effective-config dump.
type Source uint8

const (
	FromDefault Source = iota
	FromFile
	FromEnv
	FromFlag
)

func (s Source) String() string {
	switch s {
	case FromFile:
		return "file"
	case FromEnv:
		return "env"
	case FromFlag:
		return "flag"
	}
	return "default"
}

// Options injects the environment so tests never touch the real one.
type Options struct {
	Args      []string                    // os.Args[1:]
	LookupEnv func(string) (string, bool) // defaults to os.LookupEnv
}

// Loaded is the result of a successful Load.
type Loaded struct {
	Config   *Config
	Params   []*Param
	Source   map[string]Source
	SetIn    map[string]bool // explicitly set by file, env or flag (not default)
	File     string          // config file actually read, empty if none
	Meta     Meta
	Warnings []string

	// derived are the keys whose value Load derived from another one (the
	// empty default of derivedParams); the dump prints them commented.
	derived map[string]bool
}

// ErrExitEarly is returned when --help or --version was requested; the caller
// prints and exits 0 rather than starting the server.
var ErrExitEarly = errors.New("config: exit early")

// Load builds the configuration by applying, in order:
//
//	defaults  <  ini file  <  environment  <  command line
//
// Every source funnels through the same per-Kind parser, so a value that would
// be rejected on the command line is also rejected in the file and in a default.
func Load(opts Options) (*Loaded, error) {
	if opts.LookupEnv == nil {
		opts.LookupEnv = os.LookupEnv
	}

	cfg := &Config{}
	params := Registry(cfg)
	ix := newIndex(params)

	l := &Loaded{
		Config: cfg,
		Params: params,
		Source: make(map[string]Source, len(params)),
		SetIn:  make(map[string]bool, len(params)),
	}

	// --- defaults -----------------------------------------------------------
	for _, p := range params {
		if err := setValue(p.Kind, p.Ptr, p.Def); err != nil {
			// A bad default is a bug in the registry, not user error.
			panic(fmt.Sprintf("config: invalid default for %s: %v", p.Name(), err))
		}
		l.Source[p.Name()] = FromDefault
	}

	// Flags are parsed first only to discover --config and the meta switches;
	// their values are applied last, after the file and the environment.
	flagEntries, m, err := parseFlags(ix, opts.Args)
	if err != nil {
		return nil, err
	}
	l.Meta = m
	if m.Help || m.Version {
		return l, ErrExitEarly
	}

	// --- ini file -----------------------------------------------------------
	file := m.ConfigFile
	if file == "" {
		if v, ok := opts.LookupEnv("WHS_CONFIG"); ok {
			file = v
		}
	}
	if file != "" {
		entries, err := parseINIFile(file)
		if err != nil {
			return nil, err
		}
		// afterSecret: the line before was a secret, so an unknown key here
		// may be the rest of it wrapped onto this line (lookupINI).
		afterSecret := false
		guard := aliasGuard{}
		for _, e := range entries {
			p, err := ix.lookupINI(e, afterSecret)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: %w", file, e.Line, err)
			}
			afterSecret = isSecret(p)
			if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
				return nil, fmt.Errorf("%s line %d: %s: %w", file, e.Line, p.Name(), err)
			}
			if err := guard.check(p, "["+e.Sec+"] "+e.Key); err != nil {
				return nil, fmt.Errorf("%s line %d: %w", file, e.Line, err)
			}
			l.Source[p.Name()] = FromFile
			l.SetIn[p.Name()] = true
		}
		l.File = file
	}

	// --- environment --------------------------------------------------------
	envGuard := aliasGuard{}
	for _, p := range params {
		// The canonical name first, then the alias (SPEC §13.2): both are
		// this parameter in the same source, and the guard refuses them only
		// when they disagree.
		for _, name := range []string{p.Env(), p.AliasEnv()} {
			if name == "" {
				continue
			}
			v, ok := opts.LookupEnv(name)
			if !ok {
				continue
			}
			if err := setValue(p.Kind, p.Ptr, v); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			if err := envGuard.check(p, name); err != nil {
				return nil, err
			}
			l.Source[p.Name()] = FromEnv
			l.SetIn[p.Name()] = true
		}
	}

	// --- command line -------------------------------------------------------
	flagGuard := aliasGuard{}
	for _, e := range flagEntries {
		p := ix.byName[e.Name]
		if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
			return nil, fmt.Errorf("--%s: %w", e.Spelled, err)
		}
		if err := flagGuard.check(p, "--"+e.Spelled); err != nil {
			return nil, err
		}
		l.Source[p.Name()] = FromFlag
		l.SetIn[p.Name()] = true
	}

	// What is still empty or zero here is derived below, by derive or by
	// Validate (a pool from sink.workers).
	unset := map[string]bool{}
	for _, name := range derivedParams {
		if v := formatValue(ix.byName[name].Kind, ix.byName[name].Ptr); v == "" || v == "0" {
			unset[name] = true
		}
	}
	if err := derive(cfg, opts); err != nil {
		return nil, err
	}
	warns, err := Validate(cfg, l.SetIn)
	if err != nil {
		return nil, err
	}
	l.Warnings = warns
	l.derived = make(map[string]bool, len(unset))
	for name := range unset {
		if v := formatValue(ix.byName[name].Kind, ix.byName[name].Ptr); v != "" && v != "0" {
			l.derived[name] = true
		}
	}
	return l, nil
}

// derivedParams are the keys whose empty or zero value derives another one
// (SPEC §13.5). The dump prints a derived value commented: written as a key,
// it would be explicit when the dump is read back, and a pool_size would stop
// following sink.workers, a path stop following ring.data_dir -- out of the
// one writable mount (SPEC §13.6; go-relplog-pg-server's review, ported
// 2026-10-13).
var derivedParams = []string{
	"ring.sqlite.path", "sink.sqlite.path", "emergency.dir",
	"sink.postgres.pool_size", "sink.mysql.pool_size", "sink.clickhouse.pool_size",
}

// derive fills the values whose default depends on another value and therefore
// cannot be a literal in the registry (SPEC §13.5).
func derive(c *Config, opts Options) error {
	// Every file this service writes hangs off ring.data_dir: the ring queue,
	// the events database and the emergency dumps. One root is what lets a
	// container expose a single writable mount -- /data, the rule for every
	// image in the ecosystem (SPEC 13.6). A write path that does not derive
	// from here breaks that, and under a read-only rootfs it breaks loudly at
	// boot; the emergency dir is worse, because it is only created when a dump
	// happens, so the failure lands in the middle of an incident.
	if c.RingSQL.Path == "" {
		c.RingSQL.Path = filepath.Join(c.Ring.DataDir, "ring.db")
	}
	if c.SinkSQL.Path == "" {
		c.SinkSQL.Path = filepath.Join(c.Ring.DataDir, "events.db")
	}
	if c.Emergency.Dir == "" {
		c.Emergency.Dir = filepath.Join(c.Ring.DataDir, "emergency")
	}
	// Normalise enums so comparisons elsewhere never need folding.
	c.Ring.Mode = strings.ToLower(strings.TrimSpace(c.Ring.Mode))
	c.Sink.Type = strings.ToLower(strings.TrimSpace(c.Sink.Type))
	if c.Sink.Type == SinkValkeyAlias {
		// Valkey speaks the Redis protocol: one sink, and the canonical name
		// in the dump, the logs and the metrics (SPEC §13.2, §8.2.6).
		c.Sink.Type = SinkRedisType
	}
	c.SinkSQL.Rotate = strings.ToLower(strings.TrimSpace(c.SinkSQL.Rotate))
	c.Log.Level = strings.ToLower(strings.TrimSpace(c.Log.Level))
	c.Log.Format = strings.ToLower(strings.TrimSpace(c.Log.Format))
	c.RingSQL.Synchronous = strings.ToUpper(strings.TrimSpace(c.RingSQL.Synchronous))
	c.SinkSQL.Synchronous = strings.ToUpper(strings.TrimSpace(c.SinkSQL.Synchronous))
	return nil
}

// aliasGuard refuses, within ONE source, a parameter set through its
// canonical name and through its alias to different values (SPEC §13.2):
// "[sink.redis] db = 1" and "[sink.valkey] db = 2" in the same file, or
// WHS_SINK_REDIS_DB and WHS_SINK_VALKEY_DB in the same environment, have no
// winner the operator could predict -- the INI's later line, or the order the
// loader reads the environment in, would decide it in silence. Equal values
// pass: the two names are one parameter. Values are compared as parsed
// ("1MiB" and "1048576" agree) and never printed: one may be the password.
// Across sources nothing changes: the precedence decides, and the alias is
// the same key in its source.
type aliasGuard map[string]map[string]string // canonical name -> spelling -> value

// check runs right after p was set from the name spelled.
func (g aliasGuard) check(p *Param, spelled string) error {
	if aliasOf(p.Sec) == "" {
		return nil
	}
	v := formatValue(p.Kind, p.Ptr)
	seen := g[p.Name()]
	if seen == nil {
		seen = map[string]string{}
		g[p.Name()] = seen
	}
	for other, ov := range seen {
		if other != spelled && ov != v {
			return fmt.Errorf("%s and %s are set to different values; they are the same parameter "+
				"(valkey is an alias of redis): set only one, or both to the same value", other, spelled)
		}
	}
	seen[spelled] = v
	return nil
}
