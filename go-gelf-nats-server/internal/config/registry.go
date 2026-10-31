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

	// EnvName overrides the default GELF_<SECTION>_<KEY> for the names a
	// LogHub skill fixed (SPEC §16.2): LOGHUB_<KEY> for [loghub], DATADIR for
	// server.data_dir. No section of this server's own has a special prefix:
	// [convert] is GELF_CONVERT_<KEY> like any other.
	EnvName string

	// Enum lists the accepted values of a KEnum parameter in their canonical
	// spelling. Input is matched case-insensitively and stored canonical.
	Enum []string
}

// Name is the canonical dotted name: "sink.nats.urls".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.nats.urls".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "GELF_SINK_NATS_URLS", or the fixed name of
// SPEC §16.2 when the skill set one.
func (p *Param) Env() string {
	if p.EnvName != "" {
		return p.EnvName
	}
	return "GELF_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

// configFileEnv names the INI file (SPEC §16.1). It is reserved: no parameter
// may take it as its environment name.
const configFileEnv = "GELF_CONFIG"

// loghubEnv builds the fixed environment names of SPEC §16.2.
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
		// The defaults are the GELF ports every Graylog input and every
		// library assume: 12201 for UDP and TCP (different protocols, so they
		// share the number) and 12202 for HTTP. All three are unprivileged,
		// so the container image (uid 65532) binds them as they are. There
		// is no TLS (SPEC §1.2, §4.1).
		{Sec: "listen", Key: "udp", Kind: KString, Def: ":12201", Ptr: &c.Listen.UDP,
			Desc: "UDP address (GELF chunked/compressed datagrams); empty disables the transport"},
		{Sec: "listen", Key: "udp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.UDPListeners,
			Desc: "number of UDP sockets, 1..256; >1 enables SO_REUSEPORT and is the only way past one core"},
		{Sec: "listen", Key: "udp_read_buffer", Kind: KBytes, Def: "8MiB", Ptr: &c.Listen.UDPReadBuffer,
			Desc: "SO_RCVBUF; what does not fit is dropped by the kernel before this process sees it. The effective value is read back and exposed"},
		{Sec: "listen", Key: "udp_reassembly_max_bytes", Kind: KBytes, Def: "64MiB", Ptr: &c.Listen.UDPReassemblyMaxBytes,
			Desc: "ceiling of the bytes held by incomplete chunked UDP messages, all sources together; a chunk past it is dropped and counted; 1MiB..4GiB"},
		{Sec: "listen", Key: "udp_reassembly_max_messages", Kind: KInt, Def: "16384", Ptr: &c.Listen.UDPReassemblyMaxMessages,
			Desc: "ceiling of chunked UDP messages open at once; the first chunk of a new one past it is dropped and counted; 1..1048576"},
		{Sec: "listen", Key: "tcp", Kind: KString, Def: ":12201", Ptr: &c.Listen.TCP,
			Desc: "TCP address (GELF documents framed by \\0); empty disables the transport"},
		{Sec: "listen", Key: "tcp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.TCPListeners,
			Desc: "number of TCP listeners, 1..256; >1 enables SO_REUSEPORT"},
		{Sec: "listen", Key: "http", Kind: KString, Def: ":12202", Ptr: &c.Listen.HTTP,
			Desc: "HTTP address (POST /gelf, one document per request); empty disables the transport"},

		// ------------------------------------------------------------------- tcp
		//
		// There is no framing key: GELF over TCP is always \0-terminated
		// (SPEC §4.4).
		{Sec: "tcp", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.TCP.IdleTimeout,
			Desc: "a connection with no traffic is closed; generous, because an application logging hourly is normal; 0 = never"},
		{Sec: "tcp", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConns,
			Desc: "concurrent connection ceiling for the transport, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "tcp", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the transport, all listeners together, keyed on the TCP peer before PROXY; 0 = unlimited; closes immediately"},
		{Sec: "tcp", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.TCP.BackpressureTimeout,
			Desc: "how long to stop reading while the ring is full or the sink is down before closing the connection"},

		// ------------------------------------------------------------------ http
		//
		// The keys of the http transport, with its own limiters (SPEC §4.5).
		// The timeouts are the slowloris defence, so zero is refused. All of
		// them need a restart, like every receive key.
		{Sec: "http", Key: "read_header_timeout", Kind: KDuration, Def: "10s", Ptr: &c.HTTP.ReadHeaderTimeout,
			Desc: "deadline for the request line and headers; never 0"},
		{Sec: "http", Key: "read_timeout", Kind: KDuration, Def: "30s", Ptr: &c.HTTP.ReadTimeout,
			Desc: "deadline for the whole request, body included; never 0"},
		{Sec: "http", Key: "idle_timeout", Kind: KDuration, Def: "60s", Ptr: &c.HTTP.IdleTimeout,
			Desc: "how long a keep-alive connection may wait for its next request; never 0"},
		{Sec: "http", Key: "max_requests_per_conn", Kind: KInt, Def: "0", Ptr: &c.HTTP.MaxRequestsPerConn,
			Desc: "the answer to the Nth request of a connection carries Connection: close; 0 = no cap"},
		{Sec: "http", Key: "bulk", Kind: KBool, Def: "false", Ptr: &c.HTTP.Bulk,
			Desc: "split the body of POST /gelf on newlines, one document per non-blank line, as Graylog's enable_bulk_receiving (SPEC §4.5.1)"},
		{Sec: "http", Key: "bulk_max_bytes", Kind: KBytes, Def: "4MiB", Ptr: &c.HTTP.BulkMaxBytes,
			Desc: "with http.bulk, the ceiling of the whole decompressed body (each line still obeys convert.max_message_bytes); 1KiB..64MiB, raised to convert.max_message_bytes when below it"},
		{Sec: "http", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.HTTP.MaxConns,
			Desc: "concurrent connection ceiling of the http listener; 0 = unlimited; blocks in accept"},
		{Sec: "http", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.HTTP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling of the http transport, keyed on the TCP peer before PROXY; 0 = unlimited; closes immediately"},

		// ----------------------------------------------------------------- proxy
		{Sec: "proxy", Key: "protocol", Kind: KEnum, Def: ProxyOff, Ptr: &c.Proxy.Protocol,
			Enum: []string{ProxyOff, ProxyOptional, ProxyRequired},
			Desc: "PROXY protocol v1/v2 on TCP and HTTP. off reads nothing; optional reads a header from a trusted peer when one comes; required closes any connection without one (SPEC §4.6)"},
		{Sec: "proxy", Key: "trusted", Kind: KCIDRList, Def: "127.0.0.1/32,::1/128", Ptr: &c.Proxy.Trusted,
			Desc: "CIDRs of the load balancers whose PROXY header is believed; too wide a list lets anyone forge src-ip"},
		{Sec: "proxy", Key: "header_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Proxy.HeaderTimeout,
			Desc: "deadline for the PROXY header, the first bytes of the connection"},

		// --------------------------------------------------------------- convert
		//
		// Every key here changes the bytes of the JLH, and the JLH must be a
		// pure function of header + raw + this section: a retry has to produce
		// the same bytes under the same uuid. So none of them reloads (SPEC §3.5,
		// §16.3).
		{Sec: "convert", Key: "keep_raw", Kind: KEnum, Def: KeepRawMalformed, Ptr: &c.Convert.KeepRaw,
			Enum: []string{KeepRawMalformed, KeepRawAlways},
			Desc: "malformed writes payload.raw only when the document is malformed, has an encoding warning or lost something (SPEC §6.9); always writes it for every document"},
		{Sec: "convert", Key: "max_message_bytes", Kind: KBytes, Def: "128KiB", Ptr: &c.Convert.MaxMessageBytes,
			Desc: "ceiling of the decompressed document; larger ones are refused and counted, never truncated (SPEC §5.5); 1KiB..16MiB. Sets the worst JLH at 6x this + 8KiB, which must fit the NATS max_payload"},
		{Sec: "convert", Key: "accept_malformed", Kind: KBool, Def: "true", Ptr: &c.Convert.AcceptMalformed,
			Desc: "false drops format=malformed in the worker, counted and sampled in the log"},

		// ------------------------------------------------------------------ ring
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling of the memory ring, 1..16777216; with small documents this is the one that binds"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling, counting each item's document buffer capacity plus 288 bytes; the JLH is the worker's memory, not the ring's. At least one message of convert.max_message_bytes (its pool class + 288 bytes)"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction, of either ceiling, above which /readyz fails"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines, which also run the full conversion; they share the one NATS connection, so more only adds contention; 1..256"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per Dequeue; the async publishes of one chunk are in the air together; 1..65536"},
		{Sec: "sink", Key: "batch_max_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.Sink.BatchMaxBytes,
			Desc: "ceiling of JLH bytes per chunk; the JLH reaches 6x the decompressed document, and workers x batch x worst JLH would pass 1GiB; at most 256MiB (no NATS limit per chunk: each envelope is its own message, under max_payload)"},
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
		// There is no subject key: the subject is loghub.<space>.gelf, the
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
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "LOGHUB_GELF", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that must cover the subject; validated at boot, never created; ignored in core"},

		// ---------------------------------------------------------------- loghub
		{Sec: "loghub", Key: "eb_url", Kind: KString, Def: "", Ptr: &c.LogHub.EBURL, Redact: RedactURL,
			EnvName: loghubEnv("eb_url"),
			Desc:    "LogHub eventbus URL; only nats://, a comma list for a cluster, which fills sink.nats.urls; tls:// is refused, there is no TLS. An explicit conflicting sink key is an error (SPEC §10.9)"},
		{Sec: "loghub", Key: "beacon_interval", Kind: KSeconds, Def: "15", Ptr: &c.LogHub.BeaconInterval,
			EnvName: loghubEnv("beacon_interval"),
			Desc:    "period of the heartbeat beacon; a bare integer is seconds, as the skill defines it; 0 disables the periodic beacon, not the one on connect"},
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
		{Sec: "admin", Key: "bind", Kind: KString, Def: "127.0.0.1:9104", Ptr: &c.Admin.Bind,
			Desc: "admin listener; 9104, so it never collides with go-webhook-server (9101), go-syslog-server (9102) or go-relplog-server (9103) on the same host"},

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
			Desc: "keep serving after SIGTERM so the load balancer can drop this endpoint; matters more here, because a UDP emitter gets no error at all; 0 = no wait"},
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
		// "--sink.nats.password:hunter2") puts the value inside it; the
		// error goes to stderr (SPEC §13.2).
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("unknown parameter (the name holds characters no parameter " +
				"name has, so it is not shown; a value goes after an '=')")
		}
		if removedParams[name] {
			return nil, fmt.Errorf("unknown parameter %q: go-gelf-server has it, "+
				"go-gelf-nats-server does not (SPEC §1.2)", name)
		}
		if ix.extendsAName(name) {
			// "--sink.nats.passwordhunter2": the '=' was forgotten and
			// the value glued to the name, with or without a separator
			// ("-hunter2", "_hunter2", ".hunter2"). Quoted, the error would
			// print the secret (independent review, 2026-10-13).
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
// section of the registry -- or one of go-gelf-server that this server
// dropped: "sink.nats.subjet" and "filter.min_level" do, "hunter2" and
// "x" do not. A dropped section counts because a command line copied from
// go-gelf-server is exactly where such a flag comes from, and the error
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

// iniKeyRE is what an INI key of this server, or of go-gelf-server, looks
// like. It is narrower than paramNameRE on purpose: an unknown INI key is
// named only when it has this shape, because the INI is where a secret wraps
// onto a second line ("password = eyJhbGciOiJI" then "UzI1NiJ9abcDEF==") and
// the continuation reads as a key.
var iniKeyRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// lookupINI is lookup for an INI entry. An unknown key is named only when its
// section is one this server (or go-gelf-server) has, its key has the shape
// of a key, and the line before it was not a secret: a key right after a
// password is most likely the rest of that value on the next line
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
