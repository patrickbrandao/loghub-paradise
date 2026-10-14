package config

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Redaction says how a value is masked in --dump-config and the boot log.
type Redaction uint8

const (
	RedactNone Redaction = iota
	// RedactAll replaces a non-empty value with ***: passwords and tokens.
	RedactAll
	// RedactURL masks only the userinfo of every URL in a comma list:
	// nats://user:***@host. A NATS URL may carry a token as its whole user
	// part, so a user without a password is masked entirely (redactURL).
	RedactURL
)

// Param is one tunable. The registry is the single source from which the INI
// parser, the flag parser, the environment reader, --help and the
// effective-config dump are all derived (SPEC §13.2), so the five can never
// disagree.
type Param struct {
	Sec  string // INI section, e.g. "sink.nats"
	Key  string // INI key, e.g. "urls"
	Kind Kind
	Def  string // default, in the same textual form a user would write
	Ptr  any    // pointer into a Config field
	Desc string

	// EnvName, when set, replaces the derived WHS_ name: the LOGHUB_* names
	// the LogHub skill fixes (SPEC §13.1). There is no WHS_ alias for them.
	EnvName string
	Redact  Redaction
}

// Name is the canonical dotted name: "sink.nats.urls".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.nats.urls".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "WHS_SINK_NATS_URLS", or the fixed name of a
// [loghub] key.
func (p *Param) Env() string {
	if p.EnvName != "" {
		return p.EnvName
	}
	return "WHS_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

// loghubEnv is the environment name the LogHub skill gives a [loghub] key.
func loghubEnv(key string) string { return "LOGHUB_" + strings.ToUpper(key) }

// Registry returns the parameter table bound to c. Every parameter in SPEC
// §13.5 appears exactly once.
func Registry(c *Config) []*Param {
	p := []*Param{
		// ---------------------------------------------------------------- server
		{Sec: "server", Key: "bind", Kind: KString, Def: ":8080", Ptr: &c.Server.Bind,
			Desc: "public listener address; every request here becomes an event"},
		{Sec: "server", Key: "listeners", Kind: KInt, Def: "1", Ptr: &c.Server.Listeners,
			Desc: "number of listeners; >1 enables SO_REUSEPORT"},
		{Sec: "server", Key: "read_header_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Server.ReadHeaderTimeout,
			Desc: "header read deadline; primary Slowloris defence"},
		{Sec: "server", Key: "read_timeout", Kind: KDuration, Def: "30s", Ptr: &c.Server.ReadTimeout,
			Desc: "headers plus body read deadline"},
		{Sec: "server", Key: "write_timeout", Kind: KDuration, Def: "10s", Ptr: &c.Server.WriteTimeout,
			Desc: "response write deadline"},
		{Sec: "server", Key: "idle_timeout", Kind: KDuration, Def: "60s", Ptr: &c.Server.IdleTimeout,
			Desc: "keep-alive idle deadline"},
		{Sec: "server", Key: "max_body_bytes", Kind: KBytes, Def: "5MiB", Ptr: &c.Server.MaxBodyBytes,
			Desc: "bodies above this are rejected with 413"},
		{Sec: "server", Key: "max_header_bytes", Kind: KBytes, Def: "64KiB", Ptr: &c.Server.MaxHeaderBytes,
			Desc: "headers above this are rejected with 431"},
		{Sec: "server", Key: "max_uri_bytes", Kind: KBytes, Def: "8KiB", Ptr: &c.Server.MaxURIBytes,
			Desc: "request URIs above this are rejected with 414"},
		{Sec: "server", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.Server.MaxConns,
			Desc: "concurrent connection ceiling for the whole server, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "server", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.Server.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the whole server, all listeners together; 0 = unlimited; closes immediately; useless behind a single proxy"},
		{Sec: "server", Key: "reject_drain_bytes", Kind: KBytes, Def: "64KiB", Ptr: &c.Server.RejectDrainBytes,
			Desc: "body bytes discarded before a pre-body rejection so the client can read the response"},
		{Sec: "server", Key: "http2", Kind: KBool, Def: "true", Ptr: &c.Server.HTTP2,
			Desc: "serve h2c by prior knowledge on the same port"},
		{Sec: "server", Key: "http2_max_concurrent_streams", Kind: KInt, Def: "250", Ptr: &c.Server.HTTP2MaxConcurrentStreams,
			Desc: "HTTP/2 concurrent stream ceiling per connection"},

		// ----------------------------------------------------------------- admin
		{Sec: "admin", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Admin.Enabled,
			Desc: "serve /healthz, /readyz and /metrics"},
		{Sec: "admin", Key: "bind", Kind: KString, Def: "127.0.0.1:9101", Ptr: &c.Admin.Bind,
			Desc: "admin listener address; loopback by default so it is never exposed"},

		// ----------------------------------------------------------------- proxy
		{Sec: "proxy", Key: "trusted", Kind: KList, Def: "127.0.0.0/8,::1", Ptr: &c.Proxy.Trusted,
			Desc: "CIDRs whose forwarding headers are believed; every deploy behind a proxy must set this"},
		{Sec: "proxy", Key: "max_xff_entries", Kind: KInt, Def: "32", Ptr: &c.Proxy.MaxXFFEntries,
			Desc: "X-Forwarded-For entries processed before the rest is ignored"},

		// ------------------------------------------------------------- ratelimit
		{Sec: "ratelimit", Key: "enabled", Kind: KBool, Def: "false", Ptr: &c.RateLimit.Enabled,
			Desc: "compatibility selector: only false; rate limiting belongs to the reverse proxy (SPEC §6)"},

		// ------------------------------------------------------------------ ring
		{Sec: "ring", Key: "mode", Kind: KString, Def: "memory", Ptr: &c.Ring.Mode,
			Desc: "compatibility selector: only memory (SPEC §0)"},
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling; exceeding it returns 429"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling; exceeding it returns 429: the buffer each item retains (pool class + 256 bytes)"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction above which /readyz fails"},
		{Sec: "ring", Key: "data_dir", Kind: KString, Def: "./data", Ptr: &c.Ring.DataDir,
			Desc: "root of every file the service writes; always /data in a container"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "type", Kind: KString, Def: SinkNATSType, Ptr: &c.Sink.Type,
			Desc: "compatibility selector: only nats (SPEC §0)"},
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines; they share the one NATS connection"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per write"},
		{Sec: "sink", Key: "batch_max_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.Sink.BatchMaxBytes,
			Desc: "ceiling of JLH bytes per write; a batch goes out in pieces under it and an envelope above it goes alone; 1 byte..256MiB"},
		{Sec: "sink", Key: "batch_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.Sink.BatchMaxDelay,
			Desc: "maximum wait to fill a batch"},
		{Sec: "sink", Key: "op_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Sink.OpTimeout,
			Desc: "deadline for one batch, for a pending publish acknowledgement and for the dial"},
		{Sec: "sink", Key: "health_interval", Kind: KDuration, Def: "2s", Ptr: &c.Sink.HealthInterval,
			Desc: "health check period; also the UP->DOWN detection latency"},
		{Sec: "sink", Key: "health_timeout", Kind: KDuration, Def: "1s", Ptr: &c.Sink.HealthTimeout,
			Desc: "deadline for one health check"},
		{Sec: "sink", Key: "fail_timeout", Kind: KDuration, Def: "30s", Ptr: &c.Sink.FailTimeout,
			Desc: "how long DOWN may persist before FATAL; 0 = no time fuse, FATAL only by max_retries"},
		{Sec: "sink", Key: "max_retries", Kind: KInt, Def: "10", Ptr: &c.Sink.MaxRetries,
			Desc: "failed health checks before FATAL"},
		{Sec: "sink", Key: "boot_timeout", Kind: KDuration, Def: "300s", Ptr: &c.Sink.BootTimeout,
			Desc: "how long boot waits for NATS (and the stream) before exiting 3"},
		{Sec: "sink", Key: "retry_backoff_min", Kind: KDuration, Def: "100ms", Ptr: &c.Sink.RetryBackoffMin,
			Desc: "initial backoff when a batch fails"},
		{Sec: "sink", Key: "retry_backoff_max", Kind: KDuration, Def: "2s", Ptr: &c.Sink.RetryBackoffMax,
			Desc: "backoff ceiling when a batch fails"},

		// ------------------------------------------------------------- sink.nats
		// There is no subject to configure: it is loghub.<space>.webhook, the
		// JLH rule (SPEC §3.8). subject and tls stay only as compatibility
		// selectors with go-webhook-fast-server (SPEC §0).
		{Sec: "sink.nats", Key: "urls", Kind: KList, Def: "nats://127.0.0.1:4222", Ptr: &c.SinkNATS.URLs, Redact: RedactURL,
			Desc: "NATS server URLs, nats:// only, comma separated for a cluster; userinfo is redacted in the dump"},
		{Sec: "sink.nats", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkNATS.Username,
			Desc: "user for password authentication"},
		{Sec: "sink.nats", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkNATS.Password, Redact: RedactAll,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "token", Kind: KString, Def: "", Ptr: &c.SinkNATS.Token, Redact: RedactAll,
			Desc: "token authentication; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "credentials", Kind: KString, Def: "", Ptr: &c.SinkNATS.Credentials,
			Desc: "path to a .creds file (JWT + NKey seed)"},
		{Sec: "sink.nats", Key: "mode", Kind: KString, Def: NATSModeJetStream, Ptr: &c.SinkNATS.Mode,
			Desc: "jetstream (every publish acknowledged by the stream) | core (best effort: the server received it)"},
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "LOGHUB_WEBHOOK", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that covers loghub.<space>.webhook; never created by the server, only validated; unused in core"},
		{Sec: "sink.nats", Key: "subject", Kind: KString, Def: NATSSubjectTemplate, Ptr: &c.SinkNATS.Subject,
			Desc: "compatibility selector: only loghub.{space}.{type}; the subject is derived (SPEC §3.8)"},
		{Sec: "sink.nats", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkNATS.TLS,
			Desc: "compatibility selector: only false; the connection to NATS is always plaintext (SPEC §8.7.9)"},

		// ---------------------------------------------------------------- loghub
		{Sec: "loghub", Key: "eb_url", Kind: KString, Def: "", Ptr: &c.LogHub.EBURL, Redact: RedactURL,
			EnvName: loghubEnv("eb_url"),
			Desc:    "LogHub eventbus URL; only nats://, a comma list for a cluster, which fills sink.nats.urls; tls:// is refused, there is no TLS. An explicit conflicting sink key is an error (SPEC §8.7.11)"},
		{Sec: "loghub", Key: "beacon_interval", Kind: KSeconds, Def: "15", Ptr: &c.LogHub.BeaconInterval,
			EnvName: loghubEnv("beacon_interval"),
			Desc:    "period of the heartbeat beacon; a bare integer is seconds, as the skill defines it; 0 disables the periodic beacon, not the one on connect"},
		{Sec: "loghub", Key: "ctl_namespace", Kind: KString, Def: "loghub._ctl", Ptr: &c.LogHub.CtlNamespace,
			EnvName: loghubEnv("ctl_namespace"),
			Desc:    "control-plane prefix; the beacon goes to <ctl_namespace>.beacon over NATS core, never JetStream"},

		// ------------------------------------------------------------- emergency
		{Sec: "emergency", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Emergency.Enabled,
			Desc: "dump a non-empty ring to a JSON Lines file on FATAL or drain timeout"},
		{Sec: "emergency", Key: "dir", Kind: KString, Def: "", Ptr: &c.Emergency.Dir,
			Desc: "emergency file directory; empty derives <ring.data_dir>/emergency"},

		// --------------------------------------------------------------- headers
		{Sec: "headers", Key: "exclude", Kind: KList, Def: "", Ptr: &c.Headers.Exclude,
			Desc: "header names omitted from the envelope; matched case-insensitively"},

		// ------------------------------------------------------------------- log
		{Sec: "log", Key: "level", Kind: KString, Def: "info", Ptr: &c.Log.Level,
			Desc: "debug | info | warn | error"},
		{Sec: "log", Key: "format", Kind: KString, Def: "json", Ptr: &c.Log.Format,
			Desc: "json | text"},
		{Sec: "log", Key: "access", Kind: KBool, Def: "false", Ptr: &c.Log.Access,
			Desc: "per-request logging; off by default because it caps throughput"},
		{Sec: "log", Key: "access_sample", Kind: KFloat, Def: "0.01", Ptr: &c.Log.AccessSample,
			Desc: "fraction of requests logged when access logging is on"},

		// -------------------------------------------------------------- shutdown
		{Sec: "shutdown", Key: "pre_stop_delay", Kind: KDuration, Def: "0s", Ptr: &c.Shutdown.PreStopDelay,
			Desc: "keep serving after SIGTERM so the load balancer can drop this endpoint"},
		{Sec: "shutdown", Key: "drain_timeout", Kind: KDuration, Def: "30s", Ptr: &c.Shutdown.DrainTimeout,
			Desc: "deadline for draining the ring during shutdown"},
	}

	// A duplicated name would make one source silently shadow another.
	seen := make(map[string]bool, len(p))
	for _, q := range p {
		if seen[q.Name()] {
			panic("config: duplicate parameter " + q.Name())
		}
		seen[q.Name()] = true
	}
	return p
}

// index maps canonical names and environment names to parameters.
type index struct {
	byName map[string]*Param
	byEnv  map[string]*Param
	all    []*Param
}

func newIndex(params []*Param) *index {
	ix := &index{
		byName: make(map[string]*Param, len(params)),
		byEnv:  make(map[string]*Param, len(params)),
		all:    params,
	}
	for _, p := range params {
		ix.byName[p.Name()] = p
		ix.byEnv[p.Env()] = p
	}
	return ix
}

func (ix *index) lookup(name string) (*Param, error) {
	p, ok := ix.byName[name]
	if !ok {
		// Named only when it has the shape of a name. What reaches here from
		// an INI key or a flag is everything before the first '=', and a
		// ':' typed for the '=' ("password: hunter2 = x",
		// "--sink.x.password:hunter2") puts the value inside it; the error
		// goes to stderr (SPEC §13.2; ported from the syslog/RELP family,
		// 2026-10-13).
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("unknown parameter (the name holds characters no parameter " +
				"name has, so it is not shown; a value goes after an '=')")
		}
		if ix.extendsAName(name) {
			// "--sink.x.passwordhunter2": the '=' was forgotten and the value
			// glued to the name, with or without a separator ("-hunter2",
			// "_hunter2", ".hunter2"). Quoted, the error would print the
			// secret (go-relplog-pg-server's review, ported 2026-10-13).
			return nil, fmt.Errorf("unknown parameter (it starts with the name of a parameter " +
				"and goes on, so it is not shown: the '=' between the name and the value may be missing)")
		}
		return nil, fmt.Errorf("unknown parameter %q", name)
	}
	return p, nil
}

// extendsAName reports whether name is a parameter name of the registry with
// something glued after it.
func (ix *index) extendsAName(name string) bool {
	for n := range ix.byName {
		if len(name) > len(n) && strings.HasPrefix(name, n) {
			return true
		}
	}
	return false
}

// knownSectionOf reports whether name, read as "<section>.<key>", names a
// section this server knows: "sink.wrkers" and "log.levl" do, "hunter2" and
// "x" do not (SPEC §13.2).
func (ix *index) knownSectionOf(name string) bool {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 {
		return false
	}
	return ix.knownSection(name[:i])
}

func (ix *index) knownSection(sec string) bool {
	if retiredSections[sec] {
		return true
	}
	for _, p := range ix.byName {
		if p.Sec == sec {
			return true
		}
	}
	return false
}

// iniKeyRE is what an INI key of this server looks like. It is narrower than
// paramNameRE on purpose: an unknown INI key is named only when it has this
// shape, because the INI is where a secret wraps onto a second line
// ("password = eyJhbGciOiJI" then "UzI1NiJ9abcDEF==") and the continuation
// reads as a key.
var iniKeyRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// lookupINI is lookup for an INI entry. An unknown key is named only when its
// section is one this server knows, its key has the shape of a key, and the
// line before it was not a secret: a key right after a password or a token is
// most likely the rest of that value on the next line ("password = abc" then
// "ffd3a9c1b2=") -- the error goes to stderr and, where there is a SIGHUP, to
// the server log (SPEC §13.2; go-relplog-nats-server's review, ported
// 2026-10-13).
func (ix *index) lookupINI(e iniEntry, afterSecret bool) (*Param, error) {
	if p, ok := ix.byName[e.Name]; ok {
		return p, nil
	}
	switch {
	case afterSecret:
		return nil, errors.New("unknown parameter right after a secret value; it is not shown, " +
			"in case it is the rest of that value: a value must fit on one line")
	case !iniKeyRE.MatchString(e.Key) || !ix.knownSection(e.Sec):
		return nil, errors.New("unknown parameter (not shown: its section or its key is not one " +
			"a parameter has; a value goes after an '=' on the same line)")
	}
	return ix.lookup(e.Name)
}

// paramNameRE is what a parameter name, known or not, can look like: the
// dotted lowercase words of the registry, with room for a typo in case or a
// dash. Anything else is not printed.
var paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// retiredSections are the sections of go-webhook-fast-server this server does
// not have (SPEC §0). They count as known for knownSectionOf and lookupINI: a
// configuration carried over from go-webhook-fast-server is exactly where such
// a key comes from, and the error must name it instead of blaming the value
// before it, as for any other unknown parameter.
var retiredSections = map[string]bool{
	"ring.sqlite":     true,
	"sink.clickhouse": true,
	"sink.mysql":      true,
	"sink.postgres":   true,
	"sink.redis":      true,
	"sink.sqlite":     true,
}

// sections returns section names in registry order, which is the order used by
// --help and the effective-config dump.
func (ix *index) sections() []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range ix.all {
		if !seen[p.Sec] {
			seen[p.Sec] = true
			out = append(out, p.Sec)
		}
	}
	return out
}

func (ix *index) inSection(sec string) []*Param {
	var out []*Param
	for _, p := range ix.all {
		if p.Sec == sec {
			out = append(out, p)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
