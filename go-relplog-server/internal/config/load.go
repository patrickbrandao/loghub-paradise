package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Source records where a parameter's final value came from. The dump prints
// it, and LOGHUB_EB_URL needs it to tell an explicit value from a default
// (SPEC §10.9).
type Source uint8

const (
	FromDefault Source = iota
	FromFile
	FromEnv
	FromFlag
	// FromEBURL marks a sink key filled by loghub.eb_url (SPEC §10.9).
	FromEBURL
)

func (s Source) String() string {
	switch s {
	case FromFile:
		return "file"
	case FromEnv:
		return "env"
	case FromFlag:
		return "flag"
	case FromEBURL:
		return "loghub.eb_url"
	}
	return "default"
}

// Options injects the process environment so tests never touch the real one.
// The same Options are passed again on SIGHUP (see Reload).
type Options struct {
	Args      []string                    // os.Args[1:]
	LookupEnv func(string) (string, bool) // defaults to os.LookupEnv
	// WorkDir resolves relative paths (server.data_dir and the explicit
	// ring.sqlite.path, sink.sqlite.path, emergency.dir). Empty uses
	// os.Getwd().
	WorkDir string
}

// Loaded is the result of a successful Load: one immutable snapshot.
type Loaded struct {
	Config *Config
	Params []*Param // the registry, bound to Config

	// Source and Origin say where each value came from, by canonical name.
	// Origin is human-readable: "default", "file /etc/x.ini line 3",
	// "env RELPLOG_SINK_TYPE", "flag --sink.type", "loghub.eb_url".
	Source map[string]Source
	Origin map[string]string

	// SetIn marks parameters written explicitly by the file, the environment
	// or the command line -- not defaults, and not values filled by
	// loghub.eb_url.
	SetIn map[string]bool

	// InFile marks parameters the config file sets, whatever wins in the
	// end. A key in the file that an env or a flag then overrides is
	// "shadowed": editing it there and sending SIGHUP changes nothing, and
	// the reload says so (ReloadResult.Shadowed).
	InFile map[string]bool

	// Notes explains values that differ from their source because Load
	// derived or adjusted them (a derived path, a lowered worker count, a
	// degraded ring). The dump prints them as comments.
	Notes map[string]string

	File     string // config file actually read, empty if none
	Meta     Meta
	Warnings []string // WARN-level findings of SPEC §13.2; the app logs them

	// ebOwned are the keys loghub.eb_url decides (see ebTarget.owned), filled
	// or left at their default. The dump comments them out.
	ebOwned map[string]bool
}

// ErrExitEarly is returned when --help or --version was requested; the caller
// prints and exits 0 rather than starting the server. Loaded.Meta says which.
var ErrExitEarly = errors.New("config: exit early")

// Load builds the configuration by applying, in order (SPEC §16.1):
//
//	defaults  <  ini file  <  environment  <  command line
//
// then expands loghub.eb_url (SPEC §10.9), derives the dependent values
// (absolute data_dir and the paths under it), and validates (SPEC §13.2). An
// error is exit code 1. Every source funnels through the same per-Kind parser,
// so a value that would be rejected on the command line is also rejected in
// the file and in a default.
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
		Origin: make(map[string]string, len(params)),
		SetIn:  make(map[string]bool, len(params)),
		InFile: map[string]bool{},
		Notes:  map[string]string{},
	}

	// --- defaults -----------------------------------------------------------
	applyDefaults(params)
	for _, p := range params {
		l.Source[p.Name()] = FromDefault
		l.Origin[p.Name()] = "default"
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
		if v, ok := opts.LookupEnv(configFileEnv); ok {
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
			if err := setValue(p, e.Raw); err != nil {
				return nil, fmt.Errorf("%s line %d: %s: %w", file, e.Line, p.Name(), err)
			}
			l.mark(p, FromFile, "file "+file+" line "+strconv.Itoa(e.Line))
			l.InFile[p.Name()] = true
		}
		l.File = file
	}

	// --- environment --------------------------------------------------------
	for _, p := range params {
		v, ok := opts.LookupEnv(p.Env())
		if !ok {
			continue
		}
		if err := setValue(p, v); err != nil {
			return nil, fmt.Errorf("%s (%s): %w", p.Env(), p.Name(), err)
		}
		l.mark(p, FromEnv, "env "+p.Env())
	}

	// --- command line -------------------------------------------------------
	for _, e := range flagEntries {
		p := ix.byName[e.Name]
		if err := setValue(p, e.Raw); err != nil {
			return nil, fmt.Errorf("%s: %w", p.Flag(), err)
		}
		l.mark(p, FromFlag, "flag "+p.Flag())
	}

	// --- LOGHUB_EB_URL --------------------------------------------------------
	// After every explicit source, so the conflict rule sees them all, and
	// before derivation and validation, so the sink it selects is validated
	// like any other.
	if err := applyEBURL(l, ix); err != nil {
		return nil, err
	}

	if err := derive(l, opts); err != nil {
		return nil, err
	}
	warns, notes, err := validate(cfg, l.SetIn)
	if err != nil {
		return nil, err
	}
	for k, v := range notes {
		l.Notes[k] = v
	}
	warns = append(warns, memLimitWarnings(cfg, opts.LookupEnv)...)
	l.Warnings = warns
	return l, nil
}

func (l *Loaded) mark(p *Param, src Source, origin string) {
	l.Source[p.Name()] = src
	l.Origin[p.Name()] = origin
	l.SetIn[p.Name()] = true
}

func applyDefaults(params []*Param) {
	for _, p := range params {
		if err := setValue(p, p.Def); err != nil {
			// A bad default is a bug in the registry, not user error.
			panic(fmt.Sprintf("config: invalid default for %s: %v", p.Name(), err))
		}
	}
}

// Defaults returns a Config holding only the registry defaults: no file, no
// environment, no derivation (paths under data_dir stay empty) and no
// validation. It exists for other packages' tests, which need a realistic
// Config without a process environment.
func Defaults() *Config {
	c := &Config{}
	applyDefaults(Registry(c))
	return c
}

// derive fills the values whose default depends on another value and therefore
// cannot be a literal in the registry (SPEC §16.5, §16.6).
func derive(l *Loaded, opts Options) error {
	c := l.Config
	wd := opts.WorkDir
	abs := func(path string) (string, error) {
		if filepath.IsAbs(path) {
			return filepath.Clean(path), nil
		}
		if wd == "" {
			w, err := os.Getwd()
			if err != nil {
				return "", fmt.Errorf("resolving %q: %w", path, err)
			}
			wd = w
		}
		return filepath.Join(wd, path), nil
	}

	// data_dir is resolved to an absolute path here, once, because the app
	// exports it as DATADIR before lhident.Initialize(), and go-loghub-ident
	// refuses a relative DATADIR (exit 100). Resolving it in the config means
	// the dump, the identity and the files all name the same directory
	// (SPEC §13.1 step 2).
	dd := strings.TrimSpace(c.Server.DataDir)
	if dd == "" {
		return fmt.Errorf("server.data_dir (%s): cannot be empty; it is the root of "+
			"everything the service writes", l.Origin["server.data_dir"])
	}
	resolved, err := abs(dd)
	if err != nil {
		return fmt.Errorf("server.data_dir: %w", err)
	}
	if resolved != c.Server.DataDir {
		l.Notes["server.data_dir"] = "resolved to an absolute path from " + strconv.Quote(c.Server.DataDir)
	}
	c.Server.DataDir = resolved

	// Every file this service writes hangs off server.data_dir: the ring
	// queue, the events database and the emergency dumps. One root is what
	// lets a container expose a single writable mount -- /data (SPEC §16.6). A
	// write path that does not derive from here breaks that, and under a
	// read-only rootfs it breaks loudly at boot; the emergency dir is worse,
	// because it is only used when a dump happens, so the failure would land
	// in the middle of an incident.
	for _, d := range []struct {
		name string
		ptr  *string
		leaf string
	}{
		{"ring.sqlite.path", &c.RingSQL.Path, "ring.db"},
		{"sink.sqlite.path", &c.SinkSQL.Path, "events.db"},
		{"emergency.dir", &c.Emergency.Dir, "emergency"},
	} {
		if strings.TrimSpace(*d.ptr) == "" {
			*d.ptr = filepath.Join(c.Server.DataDir, d.leaf)
			l.Notes[d.name] = "derived from server.data_dir"
			continue
		}
		p, err := abs(*d.ptr)
		if err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
		*d.ptr = p
	}

	// Normalise what is not an enum, so comparisons elsewhere never need
	// folding. The timezone keeps its case: IANA names are case-sensitive on
	// some systems.
	c.Filter.MinSeverity = lower(c.Filter.MinSeverity)
	for i, f := range c.Filter.Facilities {
		c.Filter.Facilities[i] = lower(f)
	}
	c.Syslog.RFC3164Timezone = strings.TrimSpace(c.Syslog.RFC3164Timezone)
	return nil
}

func lower(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// memLimitWarnings is the runtime row of SPEC §13.2 and §15.4. It reads the
// environment through the injected lookup, so it lives in Load rather than in
// the pure Validate. The Go runtime itself refuses a malformed GOMEMLIMIT at
// startup, so a value this parser does not understand is only a warning.
func memLimitWarnings(c *Config, lookup func(string) (string, bool)) []string {
	v, ok := lookup("GOMEMLIMIT")
	v = strings.TrimSpace(v)
	if !ok || v == "" || strings.EqualFold(v, "off") {
		return []string{"GOMEMLIMIT is not set: the garbage collector does not know the " +
			"container's memory limit and the process can be OOM-killed with garbage still " +
			"collectable. Set it below the container limit (SPEC §15.4)"}
	}
	n, err := parseMemLimit(v)
	if err != nil {
		return []string{fmt.Sprintf("GOMEMLIMIT=%q is not understood by this check: %v", v, err)}
	}
	if c.Ring.Mode == RingModeMemory && n < 2*c.Ring.MaxBytes {
		return []string{fmt.Sprintf("GOMEMLIMIT=%s is below 2 x ring.max_bytes (%s) with "+
			"ring.mode=memory: a full ring plus the workers' JLH buffers would push the "+
			"collector into constant cycles before the ring's own ceiling binds (SPEC §15.4)",
			v, FormatBytes(c.Ring.MaxBytes))}
	}
	return nil
}

// parseMemLimit reads GOMEMLIMIT the way the runtime does: an integer of bytes
// with an optional B, KiB, MiB, GiB or TiB suffix.
func parseMemLimit(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}
	mult := int64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("want bytes with an optional B/KiB/MiB/GiB/TiB suffix")
	}
	if n > (1<<62)/mult {
		return 1 << 62, nil
	}
	return n * mult, nil
}
