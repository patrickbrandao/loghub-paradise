package config

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Redaction says how a parameter is printed by --dump-config and Effective.
type Redaction uint8

const (
	// RedactNone prints the value as is.
	RedactNone Redaction = iota
	// RedactAll prints *** for any non-empty value: passwords and tokens.
	RedactAll
	// RedactURL keeps a URL (or a comma list of URLs) readable and replaces
	// only its userinfo: nats://user:***@host. A NATS URL may carry a token
	// as its only userinfo, so a bare user part is replaced whole.
	RedactURL
)

// Param is one tunable. The registry is the single source from which the INI
// parser, the flag parser, the environment reader, --help and the
// effective-config dump are all derived (SPEC §16.2), so the five can never
// disagree.
type Param struct {
	Sec  string // INI section, e.g. "sink.nats"
	Key  string // INI key, e.g. "urls"
	Kind Kind
	Def  string // default, in the same textual form a user would write
	Ptr  any    // pointer into a Config field
	Desc string

	// Reload marks a parameter that SIGHUP may change at runtime (SPEC §16.3).
	// Everything else is reported as requiring a restart when it differs from
	// the boot configuration.
	Reload bool

	// Redact marks a secret for the dump (SPEC §16.5: "redigida no dump").
	Redact Redaction

	// EnvName overrides the default RELPLOG_<SECTION>_<KEY> for the names a
	// LogHub skill fixed (SPEC §16.2): SYSLOG_<KEY> for [syslog],
	// LOGHUB_<KEY> for [loghub], DATADIR for server.data_dir.
	EnvName string

	// Enum lists the accepted values of a KEnum parameter in their canonical
	// spelling. Input is matched case-insensitively and stored canonical.
	Enum []string
}

// Name is the canonical dotted name: "sink.nats.urls".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.nats.urls".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "RELPLOG_SINK_NATS_URLS", or the fixed name of
// SPEC §16.2 when the skill set one. The prefix is the parent
// go-relplog-server's, so one .env can feed both servers (decision 15 of the
// parent; SPEC §16.2).
func (p *Param) Env() string {
	if p.EnvName != "" {
		return p.EnvName
	}
	return "RELPLOG_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

// configFileEnv names the INI file (SPEC §16.1). It is reserved: no parameter
// may take it as its environment name.
const configFileEnv = "RELPLOG_CONFIG"

// syslogEnv and loghubEnv build the fixed environment names of SPEC §16.2.
func syslogEnv(key string) string { return "SYSLOG_" + strings.ToUpper(key) }
func loghubEnv(key string) string { return "LOGHUB_" + strings.ToUpper(key) }

// Registry returns the parameter table bound to c. Every parameter in SPEC
// §16.5 appears exactly once.
func Registry(c *Config) []*Param {
	p := []*Param{
		// ---------------------------------------------------------------- server
		//
		// Every file this service writes hangs off data_dir: the identity
		// (written by go-loghub-ident) and emergency/. One root is what lets a
		// container expose a single writable mount -- /data -- with a read-only
		// rootfs (SPEC §16.6). Its env name is DATADIR, the
		// one go-loghub-ident reads, so both agree on the root without the
		// operator saying it twice.
		{Sec: "server", Key: "data_dir", Kind: KString, Def: "./data", Ptr: &c.Server.DataDir,
			EnvName: "DATADIR",
			Desc:    "root of everything the service writes, the identity included; resolved to an absolute path; /data in the container"},

		// ---------------------------------------------------------------- listen
		//
		// The default port is 20514, the custom of rsyslog's RELP examples, and
		// a high one on purpose: the container image runs as uid 65532 and
		// cannot bind a privileged port (SPEC §4.1). There is no listen.tls:
		// no TLS in this server (SPEC §1.2).
		{Sec: "listen", Key: "tcp", Kind: KString, Def: ":20514", Ptr: &c.Listen.TCP,
			Desc: "address of RELP in clear (omrelp without tls); the only transport, so it cannot be empty"},
		{Sec: "listen", Key: "tcp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.TCPListeners,
			Desc: "number of listeners of listen.tcp, 1..256; >1 enables SO_REUSEPORT"},

		// ------------------------------------------------------------------ relp
		//
		// The protocol half of the transport (SPEC §5).
		{Sec: "relp", Key: "window", Kind: KInt, Def: "128", Ptr: &c.RELP.Window,
			Desc: "syslog transactions in flight per connection before the reader stops reading; librelp's default window is 128"},
		{Sec: "relp", Key: "max_frame_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.RELP.MaxFrameBytes,
			Desc: "a DATALEN above this closes the connection before a byte of DATA is read; between syslog.max_message_bytes and this, the message is truncated (SPEC §5.2); at most 999999999, the nine digits of DATALEN"},
		{Sec: "relp", Key: "inflight_max_bytes", Kind: KBytes, Def: "64MiB", Ptr: &c.RELP.InflightMaxBytes,
			Desc: "ceiling, for the whole process, of the copies held by transactions not yet decided (outside ring.max_bytes); exhausted, readers stop reading (SPEC §5.6)"},
		{Sec: "relp", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.RELP.BackpressureTimeout,
			Desc: "how long a transaction is held on a full ring or a DOWN sink before the session closes WITHOUT an answer, so librelp retransmits; keep it below omrelp's timeout (90s)"},

		// ------------------------------------------------------------------- tcp
		{Sec: "tcp", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.TCP.IdleTimeout,
			Desc: "a session with no frame for this long is closed; omrelp reconnects on its next message; 0 = never"},
		{Sec: "tcp", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConns,
			Desc: "concurrent connection ceiling for the transport, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "tcp", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the transport, all listeners together, keyed on the exact TCP peer address before PROXY; 0 = unlimited; closes immediately"},

		// ----------------------------------------------------------------- proxy
		{Sec: "proxy", Key: "protocol", Kind: KEnum, Def: ProxyOff, Ptr: &c.Proxy.Protocol,
			Enum: []string{ProxyOff, ProxyOptional, ProxyRequired},
			Desc: "PROXY protocol v1/v2 before the RELP session. off reads nothing; optional reads a header from a trusted peer when one comes; required closes any connection without one (SPEC §4.6)"},
		{Sec: "proxy", Key: "trusted", Kind: KCIDRList, Def: "127.0.0.1/32,::1/128", Ptr: &c.Proxy.Trusted,
			Desc: "CIDRs of the load balancers whose PROXY header is believed; too wide a list lets anyone forge src-ip"},
		{Sec: "proxy", Key: "header_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Proxy.HeaderTimeout,
			Desc: "deadline for the PROXY header, the first bytes of the connection"},

		// ---------------------------------------------------------------- syslog
		//
		// Every key here changes the bytes of the JLH, and the JLH must be a
		// pure function of header + raw + this section: a retry has to produce
		// the same bytes under the same uuid. So none of them reloads (SPEC §3.5,
		// §16.3). The env names are the skill's, without the section
		// (SPEC §16.2).
		{Sec: "syslog", Key: "default_priority", Kind: KInt, Def: "13", Ptr: &c.Syslog.DefaultPriority,
			EnvName: syslogEnv("default_priority"),
			Desc:    "PRI used for pri-missing and pri-invalid, 0..191; 13 is user.notice, what RFC 3164 §4.3.3 prescribes"},
		{Sec: "syslog", Key: "rfc3164_timezone", Kind: KString, Def: "UTC", Ptr: &c.Syslog.RFC3164Timezone,
			EnvName: syslogEnv("rfc3164_timezone"),
			Desc:    "IANA zone of RFC 3164 timestamps that carry none (America/Sao_Paulo); Local is refused, it would differ between hosts"},
		{Sec: "syslog", Key: "max_message_bytes", Kind: KBytes, Def: "64KiB", Ptr: &c.Syslog.MaxMessageBytes,
			EnvName: syslogEnv("max_message_bytes"),
			Desc:    "longer messages are truncated, never refused (SPEC §5.3); 480..16MiB. Sets the worst JLH at 12x this + 8KiB, which must fit the NATS max_payload, and the largest copy of a transaction in flight"},
		{Sec: "syslog", Key: "payload_raw", Kind: KBool, Def: "true", Ptr: &c.Syslog.PayloadRaw,
			EnvName: syslogEnv("payload_raw"),
			Desc:    "include payload.raw when it is not mandatory (it always is for non-UTF-8 input)"},
		{Sec: "syslog", Key: "legacy_charset", Kind: KEnum, Def: "", Ptr: &c.Syslog.LegacyCharset,
			EnvName: syslogEnv("legacy_charset"),
			Enum:    []string{"", CharsetCP1252, CharsetLatin1, "windows-1252", "iso-8859-1"},
			Desc:    "decode non-UTF-8 input as this charset instead of U+FFFD (SPEC §6.7); empty disables; windows-1252 and iso-8859-1 are the same two charsets"},
		{Sec: "syslog", Key: "accept_malformed", Kind: KBool, Def: "true", Ptr: &c.Syslog.AcceptMalformed,
			EnvName: syslogEnv("accept_malformed"),
			Desc:    "false drops format=malformed in the worker, counted and sampled in the log"},
		{Sec: "syslog", Key: "sd_max_elements", Kind: KInt, Def: "32", Ptr: &c.Syslog.SDMaxElements,
			EnvName: syslogEnv("sd_max_elements"),
			Desc:    "structured-data elements accepted; more makes the SD invalid (sd-invalid), never silently cut (SPEC §6.11)"},
		{Sec: "syslog", Key: "sd_max_params", Kind: KInt, Def: "64", Ptr: &c.Syslog.SDMaxParams,
			EnvName: syslogEnv("sd_max_params"),
			Desc:    "parameters per structured-data element; more makes the SD invalid (SPEC §6.11)"},

		// ------------------------------------------------------------------ ring
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling of the memory ring, 1..16777216; with 200-byte messages this is the one that binds"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling, counting each item's raw buffer capacity plus 256 bytes; the JLH is the worker's memory, not the ring's. At least one message of syslog.max_message_bytes (its pool class + 256 bytes)"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction, of either ceiling, above which /readyz fails"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines, which also run the full conversion; they share the one NATS connection, so more only adds contention; 1..256"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per Dequeue; the async publishes of one chunk are in the air together; 1..65536"},
		{Sec: "sink", Key: "batch_max_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.Sink.BatchMaxBytes,
			Desc: "ceiling of JLH bytes per chunk; the JLH reaches 12x the raw line, and workers x batch x worst JLH would pass 1GiB; at most 256MiB (no NATS limit per chunk: each envelope is its own message, under max_payload)"},
		{Sec: "sink", Key: "batch_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.Sink.BatchMaxDelay,
			Desc: "maximum wait to fill a batch; 0 = no wait"},
		{Sec: "sink", Key: "op_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Sink.OpTimeout,
			Desc: "deadline for one write"},
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
			Desc: "initial backoff when a chunk fails"},
		{Sec: "sink", Key: "retry_backoff_max", Kind: KDuration, Def: "2s", Ptr: &c.Sink.RetryBackoffMax,
			Desc: "backoff ceiling when a chunk fails"},

		// ------------------------------------------------------------- sink.nats
		//
		// There is no subject key: the subject is loghub.<space>.syslog, the
		// rule of the JLH, not a choice (SPEC §3.8, §10.7). And no tls keys:
		// the connection is always plaintext (SPEC §1.2).
		{Sec: "sink.nats", Key: "urls", Kind: KList, Def: "nats://127.0.0.1:4222", Ptr: &c.SinkNATS.URLs, Redact: RedactURL,
			Desc: "NATS server URLs, nats:// only, comma separated for a cluster; userinfo is redacted in the dump"},
		{Sec: "sink.nats", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkNATS.Username,
			Desc: "user for password authentication"},
		{Sec: "sink.nats", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkNATS.Password, Redact: RedactAll,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "token", Kind: KString, Def: "", Ptr: &c.SinkNATS.Token, Redact: RedactAll,
			Desc: "token authentication; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "credentials", Kind: KString, Def: "", Ptr: &c.SinkNATS.Credentials,
			Desc: "path to a .creds file (JWT + NKey seed); one authentication method only"},
		{Sec: "sink.nats", Key: "mode", Kind: KEnum, Def: NATSModeJetStream, Ptr: &c.SinkNATS.Mode,
			Enum: []string{NATSModeJetStream, NATSModeCore},
			Desc: "jetstream waits for a PubAck per message; core is best effort: the server received it, nobody promised to keep it"},
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "LOGHUB_SYSLOG", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that must cover the subject; validated at boot, never created; ignored in core"},

		// ---------------------------------------------------------------- loghub
		{Sec: "loghub", Key: "eb_url", Kind: KString, Def: "", Ptr: &c.LogHub.EBURL, Redact: RedactURL,
			EnvName: loghubEnv("eb_url"),
			Desc:    "LogHub eventbus URL; only nats://, a comma list for a cluster, which fills sink.nats.urls; tls:// is refused, there is no TLS. An explicit conflicting sink key is an error (SPEC §10.9)"},
		{Sec: "loghub", Key: "beacon_interval", Kind: KSeconds, Def: "15", Ptr: &c.LogHub.BeaconInterval,
			EnvName: loghubEnv("beacon_interval"),
			Desc:    "period of the heartbeat beacon; a bare integer is seconds, as the skill defines it; at least 1s; 0 disables the periodic beacon, not the one on connect"},
		{Sec: "loghub", Key: "ctl_namespace", Kind: KString, Def: "loghub._ctl", Ptr: &c.LogHub.CtlNamespace,
			EnvName: loghubEnv("ctl_namespace"),
			Desc:    "control-plane prefix; the beacon goes to <ctl_namespace>.beacon over NATS core, never JetStream"},

		// ------------------------------------------------------------- emergency
		{Sec: "emergency", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Emergency.Enabled,
			Desc: "dump a non-empty ring to disk as JSON Lines, one JLH per line, on FATAL or drain timeout (SPEC §12)"},
		{Sec: "emergency", Key: "dir", Kind: KString, Def: "", Ptr: &c.Emergency.Dir,
			Desc: "emergency file directory; empty derives <server.data_dir>/emergency. Created at boot: a directory created during the incident fails during the incident"},

		// ----------------------------------------------------------------- admin
		{Sec: "admin", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Admin.Enabled,
			Desc: "serve /healthz, /readyz and /metrics"},
		{Sec: "admin", Key: "bind", Kind: KString, Def: "127.0.0.1:9103", Ptr: &c.Admin.Bind,
			Desc: "admin listener; 9103, the same port as go-relplog-server and go-relplog-pg-server, so two of them on one host need distinct values (SPEC §14)"},

		// ------------------------------------------------------------------- log
		{Sec: "log", Key: "level", Kind: KEnum, Def: "info", Ptr: &c.Log.Level, Reload: true,
			Enum: []string{"debug", "info", "warn", "error"},
			Desc: "minimum level of the server's own log"},
		{Sec: "log", Key: "format", Kind: KEnum, Def: "json", Ptr: &c.Log.Format,
			Enum: []string{"json", "text"},
			Desc: "encoding of the server's own log"},
		{Sec: "log", Key: "access", Kind: KBool, Def: "false", Ptr: &c.Log.Access, Reload: true,
			Desc: "per-message logging; off by default because here it is literally logging log"},
		{Sec: "log", Key: "access_sample", Kind: KFloat, Def: "0.01", Ptr: &c.Log.AccessSample, Reload: true,
			Desc: "fraction of messages logged when access logging is on, [0, 1]"},

		// -------------------------------------------------------------- shutdown
		{Sec: "shutdown", Key: "pre_stop_delay", Kind: KDuration, Def: "0s", Ptr: &c.Shutdown.PreStopDelay,
			Desc: "keep serving after SIGTERM so the load balancer can drop this endpoint before omrelp sees its session break and reconnects; 0 = no wait"},
		{Sec: "shutdown", Key: "drain_timeout", Kind: KDuration, Def: "30s", Ptr: &c.Shutdown.DrainTimeout,
			Desc: "deadline for draining the ring during shutdown"},
	}

	// A duplicated name or environment variable would make one source
	// silently shadow another; a registry mistake must fail the first test
	// run, not a deploy.
	seenName := make(map[string]bool, len(p))
	seenEnv := make(map[string]bool, len(p))
	for _, q := range p {
		if seenName[q.Name()] {
			panic("config: duplicate parameter " + q.Name())
		}
		seenName[q.Name()] = true
		env := q.Env()
		if seenEnv[env] || env == configFileEnv {
			panic("config: duplicate or reserved environment name " + env)
		}
		seenEnv[env] = true
		if (q.Kind == KEnum) != (len(q.Enum) > 0) {
			panic("config: Enum must be set exactly for KEnum parameters: " + q.Name())
		}
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
		// "--sink.nats.password:hunter2") puts the value inside it; the error
		// goes to stderr (SPEC §13.2).
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("unknown parameter (the name holds characters no parameter " +
				"name has, so it is not shown; a value goes after an '=')")
		}
		if removedParams[name] {
			return nil, fmt.Errorf("unknown parameter %q: go-relplog-server has it, "+
				"go-relplog-nats-server does not (SPEC §1.2)", name)
		}
		if ix.extendsAName(name) {
			// "--sink.nats.passwordhunter2": the '=' was forgotten and the
			// value glued to the name, with or without a separator
			// ("-hunter2", "_hunter2", ".hunter2"). Quoted, the error would
			// print the secret (independent review of go-relplog-pg-server,
			// 2026-10-13).
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
// section of the registry -- or one of go-relplog-server that this server
// dropped: "sink.nats.subjet" and "filter.min_severity" do, "hunter2" and "x"
// do not. A dropped section counts because a command line copied from
// go-relplog-server is exactly where such a flag comes from, and the error
// must name it instead of blaming the value before it (SPEC §13.2).
func (ix *index) knownSectionOf(name string) bool {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 {
		return false
	}
	return ix.knownSection(name[:i])
}

func (ix *index) knownSection(sec string) bool {
	if removedSections[sec] {
		return true
	}
	for _, p := range ix.byName {
		if p.Sec == sec {
			return true
		}
	}
	return false
}

// iniKeyRE is what an INI key of this server, or of go-relplog-server, looks
// like. It is narrower than paramNameRE on purpose: an unknown INI key is
// named only when it has this shape, because the INI is where a secret wraps
// onto a second line ("token = eyJhbGciOiJI" then "UzI1NiJ9abcDEF==") and the
// continuation reads as a key.
var iniKeyRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// lookupINI is lookup for an INI entry. An unknown key is named only when its
// section is one this server (or go-relplog-server) has, its key has the shape
// of a key, and the line before it was not a secret: a key right after a
// password or a token is most likely the rest of that value on the next line
// ("password = abc" then "ffd3a9c1b2=") -- the error goes to stderr and, on a
// SIGHUP, to the server log (SPEC §13.2, §16.2).
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
