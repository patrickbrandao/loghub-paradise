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
	File     string // config file actually read, empty if none
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
		for _, e := range entries {
			p, err := ix.lookupINI(e, afterSecret)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: %w", file, e.Line, err)
			}
			afterSecret = isSecret(p)
			if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
				return nil, fmt.Errorf("%s line %d: %s: %w", file, e.Line, p.Name(), err)
			}
			l.Source[p.Name()] = FromFile
		}
		l.File = file
	}

	// --- environment --------------------------------------------------------
	for _, p := range params {
		v, ok := opts.LookupEnv(p.Env())
		if !ok {
			continue
		}
		if err := setValue(p.Kind, p.Ptr, v); err != nil {
			return nil, fmt.Errorf("%s: %w", p.Env(), err)
		}
		l.Source[p.Name()] = FromEnv
	}

	// --- command line -------------------------------------------------------
	for _, e := range flagEntries {
		p := ix.byName[e.Name]
		if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
			return nil, fmt.Errorf("%s: %w", p.Flag(), err)
		}
		l.Source[p.Name()] = FromFlag
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
	warns, err := Validate(cfg)
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
	"emergency.dir", "sink.postgres.pool_size",
}

// derive fills the values whose default depends on another value and therefore
// cannot be a literal in the registry (SPEC §13.5).
func derive(c *Config, opts Options) error {
	// Every file this service writes hangs off ring.data_dir: the identity and
	// the emergency dumps. One root is what lets a container expose a single
	// writable mount -- /data, the rule for every image in the ecosystem (SPEC
	// §13.6). A write path that does not derive from here breaks that, and the
	// emergency dir is the worst place to break it: it is only created when a
	// dump happens, so the failure lands in the middle of an incident.
	if c.Emergency.Dir == "" {
		c.Emergency.Dir = filepath.Join(c.Ring.DataDir, "emergency")
	}
	// Normalise enums so comparisons elsewhere never need folding.
	c.Ring.Mode = strings.ToLower(strings.TrimSpace(c.Ring.Mode))
	c.Sink.Type = strings.ToLower(strings.TrimSpace(c.Sink.Type))
	c.Log.Level = strings.ToLower(strings.TrimSpace(c.Log.Level))
	c.Log.Format = strings.ToLower(strings.TrimSpace(c.Log.Format))
	return nil
}
