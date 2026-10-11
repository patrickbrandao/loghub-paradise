package config

import (
	"crypto/rand"
	"encoding/hex"
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
	Hostname  func() (string, error)      // defaults to os.Hostname
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
}

// ErrExitEarly is returned when --help or --version was requested; the caller
// prints and exits 0 rather than starting the server.
var ErrExitEarly = errors.New("config: exit early")

// Load builds the configuration by applying, in order:
//
//	defaults  <  ini file  <  environment  <  command line
//
// Every source funnels through the same per-Kind parser, so a value that would
// be rejected on the command line is also rejected in the file and in a
// default.
func Load(opts Options) (*Loaded, error) {
	if opts.LookupEnv == nil {
		opts.LookupEnv = os.LookupEnv
	}
	if opts.Hostname == nil {
		opts.Hostname = os.Hostname
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
		if v, ok := opts.LookupEnv("SYSLOG_CONFIG"); ok {
			file = v
		}
	}
	if file != "" {
		entries, err := parseINIFile(file)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			p, err := ix.lookup(e.Name)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: %w", file, e.Line, err)
			}
			if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
				return nil, fmt.Errorf("%s line %d: %s: %w", file, e.Line, p.Name(), err)
			}
			l.Source[p.Name()] = FromFile
			l.SetIn[p.Name()] = true
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
		l.SetIn[p.Name()] = true
	}

	// --- command line -------------------------------------------------------
	for _, e := range flagEntries {
		p := ix.byName[e.Name]
		if err := setValue(p.Kind, p.Ptr, e.Raw); err != nil {
			return nil, fmt.Errorf("%s: %w", p.Flag(), err)
		}
		l.Source[p.Name()] = FromFlag
		l.SetIn[p.Name()] = true
	}

	if err := derive(cfg, opts); err != nil {
		return nil, err
	}
	warns, err := Validate(cfg, l.SetIn)
	if err != nil {
		return nil, err
	}
	l.Warnings = warns
	return l, nil
}

// derive fills the values whose default depends on another value and therefore
// cannot be a literal in the registry (SPEC §3.4, §15.5).
func derive(c *Config, opts Options) error {
	if c.Node.ID == "" {
		host, err := opts.Hostname()
		if err == nil {
			host = strings.TrimSpace(host)
		}
		if err != nil || host == "" {
			// Fall back to random hex rather than a constant: two anonymous
			// nodes writing the same node-id would make the envelope lie.
			var b [4]byte
			if _, err := rand.Read(b[:]); err != nil {
				return fmt.Errorf("deriving node.id: %w", err)
			}
			host = hex.EncodeToString(b[:])
		}
		c.Node.ID = host
	}

	// Every file this service writes hangs off ring.data_dir: the ring queue,
	// the events database and the emergency dumps. One root is what lets a
	// container expose a single writable mount -- /data, the rule for every
	// image in the ecosystem (SPEC §15.6). A write path that does not derive
	// from here breaks that, and under a read-only rootfs it breaks loudly at
	// boot; the emergency dir is worse, because it is only created when a dump
	// happens, so the failure lands in the middle of an incident.
	//
	// listen.unix is the one path deliberately outside this root, and it is a
	// socket we read from, not data we write.
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
	c.Ring.Mode = lower(c.Ring.Mode)
	c.Sink.Type = lower(c.Sink.Type)
	c.SinkSQL.Rotate = lower(c.SinkSQL.Rotate)
	c.TCP.Framing = lower(c.TCP.Framing)
	c.Parse.Prefer = lower(c.Parse.Prefer)
	c.Filter.MinSeverity = lower(c.Filter.MinSeverity)
	c.Log.Level = lower(c.Log.Level)
	c.Log.Format = lower(c.Log.Format)
	c.RingSQL.Synchronous = strings.ToUpper(strings.TrimSpace(c.RingSQL.Synchronous))
	c.SinkSQL.Synchronous = strings.ToUpper(strings.TrimSpace(c.SinkSQL.Synchronous))

	// "rfc5424" and "rfc3164" are what an operator naturally writes; accept
	// them rather than failing on a spelling the documentation never forbade.
	c.Parse.Prefer = strings.TrimPrefix(c.Parse.Prefer, "rfc")
	return nil
}

func lower(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
