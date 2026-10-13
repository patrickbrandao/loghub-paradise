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
	Sec  string // INI section, e.g. "sink.redis"
	Key  string // INI key, e.g. "addrs"
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

	// EnvName overrides the default SYSLOG_<SECTION>_<KEY> for the names a
	// LogHub skill fixed (SPEC §16.2): SYSLOG_<KEY> for [syslog],
	// LOGHUB_<KEY> for [loghub], DATADIR for server.data_dir.
	EnvName string

	// Enum lists the accepted values of a KEnum parameter in their canonical
	// spelling. Input is matched case-insensitively and stored canonical.
	Enum []string
}

// Name is the canonical dotted name: "sink.redis.addrs".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.redis.addrs".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "SYSLOG_SINK_REDIS_ADDRS", or the fixed name of
// SPEC §16.2 when the skill set one.
func (p *Param) Env() string {
	if p.EnvName != "" {
		return p.EnvName
	}
	return "SYSLOG_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

// configFileEnv names the INI file (SPEC §16.1). It is reserved: no parameter
// may take it as its environment name.
const configFileEnv = "SYSLOG_CONFIG"

// syslogEnv and loghubEnv build the fixed environment names of SPEC §16.2.
func syslogEnv(key string) string { return "SYSLOG_" + strings.ToUpper(key) }
func loghubEnv(key string) string { return "LOGHUB_" + strings.ToUpper(key) }

// Registry returns the parameter table bound to c. Every parameter in SPEC
// §16.5 appears exactly once.
func Registry(c *Config) []*Param {
	syncModes := []string{"FULL", "NORMAL", "OFF"}

	p := []*Param{
		// ---------------------------------------------------------------- server
		//
		// Every file this service writes hangs off data_dir: the identity
		// (written by go-loghub-ident), ring.db, events.db and emergency/. One
		// root is what lets a container expose a single writable mount -- /data
		// -- with a read-only rootfs (SPEC §16.6). Its env name is DATADIR, the
		// one go-loghub-ident reads, so both agree on the root without the
		// operator saying it twice.
		{Sec: "server", Key: "data_dir", Kind: KString, Def: "./data", Ptr: &c.Server.DataDir,
			EnvName: "DATADIR",
			Desc:    "root of everything the service writes, the identity included; resolved to an absolute path; /data in the container"},

		// ---------------------------------------------------------------- listen
		//
		// The default ports are 1514 and (by convention) 6514, not 514. The
		// container image runs as uid 65532 and cannot bind a privileged port,
		// so a default of 514 would mean the project's own compose files do not
		// start. The compose publishes "514:1514" instead, and a host install
		// that wants 514 uses setcap or ip_unprivileged_port_start (SPEC §4.1).
		{Sec: "listen", Key: "udp", Kind: KString, Def: ":1514", Ptr: &c.Listen.UDP,
			Desc: "UDP address (RFC 5426); empty disables the transport"},
		{Sec: "listen", Key: "udp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.UDPListeners,
			Desc: "number of UDP sockets; >1 enables SO_REUSEPORT and is the only way past one core"},
		{Sec: "listen", Key: "udp_read_buffer", Kind: KBytes, Def: "8MiB", Ptr: &c.Listen.UDPReadBuffer,
			Desc: "SO_RCVBUF; what does not fit is dropped by the kernel before this process sees it. The effective value is read back and exposed"},
		{Sec: "listen", Key: "tcp", Kind: KString, Def: ":1514", Ptr: &c.Listen.TCP,
			Desc: "TCP address (RFC 6587); empty disables the transport"},
		{Sec: "listen", Key: "tcp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.TCPListeners,
			Desc: "number of TCP listeners; >1 enables SO_REUSEPORT"},
		{Sec: "listen", Key: "tls", Kind: KString, Def: "", Ptr: &c.Listen.TLS,
			Desc: "TLS address (RFC 5425, octet-counting only); :6514 is the convention; requires tls.cert_file and tls.key_file"},
		{Sec: "listen", Key: "tls_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.TLSListeners,
			Desc: "number of TLS listeners; >1 enables SO_REUSEPORT"},
		{Sec: "listen", Key: "unix", Kind: KString, Def: "", Ptr: &c.Listen.Unix,
			Desc: "unix datagram socket path, absolute; /dev/log is the convention. The only write outside server.data_dir"},
		{Sec: "listen", Key: "unix_mode", Kind: KOctal, Def: "0666", Ptr: &c.Listen.UnixMode,
			Desc: "socket mode, octal; any process has to be able to log, which is why /dev/log is 0666 everywhere"},
		{Sec: "listen", Key: "unix_read_buffer", Kind: KBytes, Def: "8MiB", Ptr: &c.Listen.UnixReadBuffer,
			Desc: "SO_RCVBUF of the unix socket"},

		// ------------------------------------------------------------------- tcp
		{Sec: "tcp", Key: "framing", Kind: KEnum, Def: FramingAuto, Ptr: &c.TCP.Framing,
			Enum: []string{FramingAuto, FramingOctet, FramingLF},
			Desc: "decided once per connection on the first frame, never per message (SPEC §5.2, deviation D1)"},
		{Sec: "tcp", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.TCP.IdleTimeout,
			Desc: "a connection with no traffic is closed; generous, because an appliance logging hourly is normal; 0 = never"},
		{Sec: "tcp", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConns,
			Desc: "concurrent connection ceiling for the transport, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "tcp", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the transport, all listeners together, keyed on the TCP peer before PROXY; 0 = unlimited; closes immediately"},
		{Sec: "tcp", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.TCP.BackpressureTimeout,
			Desc: "how long to stop reading while the ring is full or the sink is down before closing the connection"},

		// ------------------------------------------------------------------- tls
		{Sec: "tls", Key: "cert_file", Kind: KString, Def: "", Ptr: &c.TLS.CertFile,
			Desc: "server certificate (PEM); required with listen.tls; read at boot only, a new certificate needs a restart"},
		{Sec: "tls", Key: "key_file", Kind: KString, Def: "", Ptr: &c.TLS.KeyFile,
			Desc: "server private key (PEM); required with listen.tls"},
		{Sec: "tls", Key: "client_auth", Kind: KEnum, Def: ClientAuthOptional, Ptr: &c.TLS.ClientAuth,
			Enum: []string{ClientAuthOptional, ClientAuthRequire, ClientAuthNone},
			Desc: "optional asks for a client certificate and verifies it against client_ca when one comes (without a CA it is only recorded as peer-*); require demands and verifies one; none does not ask"},
		{Sec: "tls", Key: "client_ca", Kind: KString, Def: "", Ptr: &c.TLS.ClientCA,
			Desc: "CA bundle for client certificates; mandatory with client_auth=require"},
		{Sec: "tls", Key: "min_version", Kind: KEnum, Def: TLSVersion12, Ptr: &c.TLS.MinVersion,
			Enum: []string{TLSVersion12, TLSVersion13},
			Desc: "lowest TLS version accepted; the cipher suites are Go's defaults, which cover RFC 9662"},
		{Sec: "tls", Key: "handshake_timeout", Kind: KDuration, Def: "10s", Ptr: &c.TLS.HandshakeTimeout,
			Desc: "deadline for the handshake; the defence against slowloris before a single message"},
		{Sec: "tls", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.TLS.IdleTimeout,
			Desc: "as tcp.idle_timeout, for the TLS transport; 0 = never"},
		{Sec: "tls", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.TLS.MaxConns,
			Desc: "as tcp.max_conns, for the TLS transport: a separate limiter"},
		{Sec: "tls", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.TLS.MaxConnsPerIP,
			Desc: "as tcp.max_conns_per_ip, for the TLS transport"},
		{Sec: "tls", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.TLS.BackpressureTimeout,
			Desc: "as tcp.backpressure_timeout, for the TLS transport"},

		// ----------------------------------------------------------------- proxy
		{Sec: "proxy", Key: "protocol", Kind: KEnum, Def: ProxyOff, Ptr: &c.Proxy.Protocol,
			Enum: []string{ProxyOff, ProxyOptional, ProxyRequired},
			Desc: "PROXY protocol v1/v2 on TCP and TLS. off reads nothing; optional reads a header from a trusted peer when one comes; required closes any connection without one (SPEC §4.6)"},
		{Sec: "proxy", Key: "trusted", Kind: KCIDRList, Def: "127.0.0.1/32,::1/128", Ptr: &c.Proxy.Trusted,
			Desc: "CIDRs of the load balancers whose PROXY header is believed; too wide a list lets anyone forge src-ip"},
		{Sec: "proxy", Key: "header_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Proxy.HeaderTimeout,
			Desc: "deadline for the PROXY header, which arrives before the TLS handshake"},

		// ---------------------------------------------------------------- syslog
		//
		// Every key here changes the bytes of the JLH, and the JLH must be a
		// pure function of header + raw + this section: a retry, a reingested
		// dump or another replica draining the Redis ring has to produce the
		// same bytes under the same uuid. So none of them reloads (SPEC §3.5,
		// §16.3). The env names are the skill's, without the section
		// (SPEC §16.2).
		{Sec: "syslog", Key: "default_priority", Kind: KInt, Def: "13", Ptr: &c.Syslog.DefaultPriority,
			EnvName: syslogEnv("default_priority"),
			Desc:    "PRI used for pri-missing and pri-invalid, 0..191; 13 is user.notice, what RFC 3164 §4.3.3 prescribes"},
		{Sec: "syslog", Key: "rfc3164_timezone", Kind: KString, Def: "UTC", Ptr: &c.Syslog.RFC3164Timezone,
			EnvName: syslogEnv("rfc3164_timezone"),
			Desc:    "IANA zone of RFC 3164 timestamps that carry none (America/Sao_Paulo); Local is refused, it would differ between replicas"},
		{Sec: "syslog", Key: "max_message_bytes", Kind: KBytes, Def: "64KiB", Ptr: &c.Syslog.MaxMessageBytes,
			EnvName: syslogEnv("max_message_bytes"),
			Desc:    "longer messages are truncated, never refused (SPEC §5.5); 480..16MiB. Sets the worst JLH at 12x this + 8KiB, which must fit the NATS max_payload"},
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

		// ---------------------------------------------------------------- filter
		{Sec: "filter", Key: "min_severity", Kind: KString, Def: "7", Ptr: &c.Filter.MinSeverity, Reload: true,
			Desc: "accept severity <= this; name or number. Lower is MORE severe, so 4 (warning) keeps emerg..warning"},
		{Sec: "filter", Key: "facilities", Kind: KList, Def: "", Ptr: &c.Filter.Facilities, Reload: true,
			Desc: "accepted facilities by RFC 5427 name or number; empty accepts all"},

		// ------------------------------------------------------------- ratelimit
		{Sec: "ratelimit", Key: "enabled", Kind: KBool, Def: "false", Ptr: &c.RateLimit.Enabled, Reload: true,
			Desc: "off by default: a limiter keyed on the wrong source is worse than none. Never applies to unix"},
		{Sec: "ratelimit", Key: "rate", Kind: KFloat, Def: "1000", Ptr: &c.RateLimit.Rate, Reload: true,
			Desc: "tokens per second per network prefix of the resolved src-ip; ten times the webhook default, because a /24 of hosts logs a lot"},
		{Sec: "ratelimit", Key: "burst", Kind: KFloat, Def: "2000", Ptr: &c.RateLimit.Burst, Reload: true,
			Desc: "bucket capacity per network prefix"},
		{Sec: "ratelimit", Key: "v4_mask", Kind: KInt, Def: "24", Ptr: &c.RateLimit.V4Mask, Reload: true,
			Desc: "IPv4 prefix length used as the bucket key"},
		{Sec: "ratelimit", Key: "v6_mask", Kind: KInt, Def: "48", Ptr: &c.RateLimit.V6Mask, Reload: true,
			Desc: "IPv6 prefix length used as the bucket key"},
		{Sec: "ratelimit", Key: "exempt", Kind: KCIDRList, Def: "", Ptr: &c.RateLimit.Exempt, Reload: true,
			Desc: "CIDRs exempt from rate limiting"},
		{Sec: "ratelimit", Key: "max_buckets", Kind: KInt, Def: "1000000", Ptr: &c.RateLimit.MaxBuckets, Reload: true,
			Desc: "hard ceiling on tracked prefixes; beyond it new keys pass unlimited"},
		{Sec: "ratelimit", Key: "sweep_interval", Kind: KDuration, Def: "30s", Ptr: &c.RateLimit.SweepInterval, Reload: true,
			Desc: "how often full buckets are evicted"},

		// ------------------------------------------------------------------ ring
		{Sec: "ring", Key: "mode", Kind: KEnum, Def: RingModeMemory, Ptr: &c.Ring.Mode,
			Enum: []string{RingModeMemory, RingModeSQLite, RingModeRedis, RingModeNone},
			Desc: "where a message waits before delivery: header + raw line, never the JLH (SPEC §2.2)"},
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling; with 200-byte messages this is the one that binds. Global across instances with mode=redis"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling, counting raw lines only; the JLH is the worker's memory, not the ring's. Global with mode=redis"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction, of either ceiling, above which /readyz fails"},
		{Sec: "ring", Key: "max_inflight", Kind: KInt, Def: "2048", Ptr: &c.Ring.MaxInflight,
			Desc: "concurrent synchronous writes when mode=none; without it a slow sink grows goroutines without bound"},

		// ----------------------------------------------------------- ring.sqlite
		{Sec: "ring.sqlite", Key: "path", Kind: KString, Def: "", Ptr: &c.RingSQL.Path,
			Desc: "ring database file; empty derives <server.data_dir>/ring.db"},
		{Sec: "ring.sqlite", Key: "synchronous", Kind: KEnum, Def: "FULL", Ptr: &c.RingSQL.Synchronous,
			Enum: syncModes,
			Desc: "FULL survives power loss and binds p99 to the disk"},
		{Sec: "ring.sqlite", Key: "commit_batch", Kind: KInt, Def: "256", Ptr: &c.RingSQL.CommitBatch,
			Desc: "items per group commit"},
		{Sec: "ring.sqlite", Key: "commit_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.RingSQL.CommitMaxDelay,
			Desc: "maximum wait before committing a partial group"},
		{Sec: "ring.sqlite", Key: "busy_timeout", Kind: KDuration, Def: "5s", Ptr: &c.RingSQL.BusyTimeout,
			Desc: "SQLITE_BUSY retry window"},
		{Sec: "ring.sqlite", Key: "wal_autocheckpoint", Kind: KInt, Def: "1000", Ptr: &c.RingSQL.WALAutocheckpoint,
			Desc: "WAL pages before an automatic checkpoint"},

		// ------------------------------------------------------------ ring.redis
		{Sec: "ring.redis", Key: "addrs", Kind: KList, Def: "127.0.0.1:6379", Ptr: &c.RingRedis.Addrs,
			Desc: "Redis of the ring, or Sentinel addresses when sentinel_master is set. The same server as sink.redis degrades the ring to memory"},
		{Sec: "ring.redis", Key: "username", Kind: KString, Def: "", Ptr: &c.RingRedis.Username,
			Desc: "ACL username"},
		{Sec: "ring.redis", Key: "password", Kind: KString, Def: "", Ptr: &c.RingRedis.Password, Redact: RedactAll,
			Desc: "ACL password; redacted in the effective-config dump"},
		{Sec: "ring.redis", Key: "db", Kind: KInt, Def: "0", Ptr: &c.RingRedis.DB,
			Desc: "database index"},
		{Sec: "ring.redis", Key: "sentinel_master", Kind: KString, Def: "", Ptr: &c.RingRedis.SentinelMaster,
			Desc: "master name; non-empty selects Sentinel mode"},
		{Sec: "ring.redis", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.RingRedis.TLS,
			Desc: "TLS to the ring's Redis, verified against the system pool or tls_ca"},
		{Sec: "ring.redis", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.RingRedis.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "ring.redis", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.RingRedis.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "ring.redis", Key: "stream", Kind: KString, Def: "syslog:ring", Ptr: &c.RingRedis.Stream,
			Desc: "stream shared by every instance; the byte counter lives at <stream>:bytes"},
		{Sec: "ring.redis", Key: "group", Kind: KString, Def: "syslog-ring", Ptr: &c.RingRedis.Group,
			Desc: "consumer group; each instance consumes as its AgentUUID"},
		{Sec: "ring.redis", Key: "claim_idle", Kind: KDuration, Def: "30s", Ptr: &c.RingRedis.ClaimIdle,
			Desc: "idle time after which XAUTOCLAIM takes a pending entry from a dead replica"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "type", Kind: KEnum, Def: SinkTypeNATS, Ptr: &c.Sink.Type,
			Enum: []string{SinkTypeNATS, SinkTypeRedis, SinkTypeSQLite, SinkTypePostgres, SinkTypeMySQL, SinkTypeClickHouse, SinkTypeNone},
			Desc: "the final destination of the JLH; nats because LogHub is the eventbus. Chosen by the scheme of loghub.eb_url when that is set"},
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines, which also run the full conversion; forced to 1 when type=sqlite, lowered to 4 when type=postgres, mysql or clickhouse and not set"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per Dequeue; at most 3640 when type=mysql (65535 placeholders, 18 per row)"},
		{Sec: "sink", Key: "batch_max_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.Sink.BatchMaxBytes,
			Desc: "ceiling of JLH bytes per chunk; the JLH reaches 12x the raw line, and workers x batch x worst JLH would pass 1GiB"},
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
			Desc: "how long boot waits for the sink (and a Redis ring) before exiting 3"},
		{Sec: "sink", Key: "retry_backoff_min", Kind: KDuration, Def: "100ms", Ptr: &c.Sink.RetryBackoffMin,
			Desc: "initial backoff when a chunk fails"},
		{Sec: "sink", Key: "retry_backoff_max", Kind: KDuration, Def: "2s", Ptr: &c.Sink.RetryBackoffMax,
			Desc: "backoff ceiling when a chunk fails"},

		// ------------------------------------------------------------- sink.nats
		//
		// There is no subject key: the subject is loghub.<space>.syslog, the
		// rule of the JLH, not a choice (SPEC §3.8, §10.7).
		{Sec: "sink.nats", Key: "urls", Kind: KList, Def: "nats://127.0.0.1:4222", Ptr: &c.SinkNATS.URLs, Redact: RedactURL,
			Desc: "NATS server URLs, comma separated for a cluster; userinfo is redacted in the dump"},
		{Sec: "sink.nats", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkNATS.Username,
			Desc: "user for password authentication"},
		{Sec: "sink.nats", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkNATS.Password, Redact: RedactAll,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "token", Kind: KString, Def: "", Ptr: &c.SinkNATS.Token, Redact: RedactAll,
			Desc: "token authentication; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "credentials", Kind: KString, Def: "", Ptr: &c.SinkNATS.Credentials,
			Desc: "path to a .creds file (JWT + NKey seed); one authentication method only"},
		{Sec: "sink.nats", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkNATS.TLS,
			Desc: "TLS to NATS, verified against the system pool or tls_ca"},
		{Sec: "sink.nats", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkNATS.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.nats", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkNATS.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "sink.nats", Key: "mode", Kind: KEnum, Def: NATSModeJetStream, Ptr: &c.SinkNATS.Mode,
			Enum: []string{NATSModeJetStream, NATSModeCore},
			Desc: "jetstream waits for a PubAck per message; core is best effort: the server received it, nobody promised to keep it"},
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "LOGHUB_SYSLOG", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that must cover the subject; validated at boot, never created; ignored in core"},
		{Sec: "sink.nats", Key: "subjects", Kind: KEnum, Def: SubjectsExact, Ptr: &c.SinkNATS.Subjects,
			Enum: []string{SubjectsExact, SubjectsFacilitySeverity},
			Desc: "exact publishes to loghub.<space>.syslog; facility-severity appends .<facility>.<severity> and breaks exact subscriptions (opt-in)"},

		// ------------------------------------------------------------ sink.redis
		{Sec: "sink.redis", Key: "addrs", Kind: KList, Def: "127.0.0.1:6379", Ptr: &c.SinkRedis.Addrs,
			Desc: "Redis addresses, or Sentinel addresses when sentinel_master is set"},
		{Sec: "sink.redis", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkRedis.Username,
			Desc: "ACL username"},
		{Sec: "sink.redis", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkRedis.Password, Redact: RedactAll,
			Desc: "ACL password; redacted in the effective-config dump"},
		{Sec: "sink.redis", Key: "db", Kind: KInt, Def: "0", Ptr: &c.SinkRedis.DB,
			Desc: "database index"},
		{Sec: "sink.redis", Key: "sentinel_master", Kind: KString, Def: "", Ptr: &c.SinkRedis.SentinelMaster,
			Desc: "master name; non-empty selects Sentinel mode"},
		{Sec: "sink.redis", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkRedis.TLS,
			Desc: "TLS to Redis, verified against the system pool or tls_ca"},
		{Sec: "sink.redis", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkRedis.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.redis", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkRedis.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "sink.redis", Key: "stream", Kind: KString, Def: "loghub.{space}.syslog", Ptr: &c.SinkRedis.Stream,
			Desc: "target stream; {space} is expanded to the Workspace at boot, the same name as the NATS subject. Any literal name is accepted"},
		{Sec: "sink.redis", Key: "maxlen", Kind: KInt64, Def: "0", Ptr: &c.SinkRedis.MaxLen,
			Desc: "MAXLEN ~ N trimming; 0 = off, because trimming discards events silently"},
		{Sec: "sink.redis", Key: "ring_scale_each_conn", Kind: KInt, Def: "10", Ptr: &c.SinkRedis.RingScaleEachConn,
			Desc: "rueidis pipeline ring size as a power of two"},
		{Sec: "sink.redis", Key: "pipeline_multiplex", Kind: KInt, Def: "0", Ptr: &c.SinkRedis.PipelineMultiplex,
			Desc: "rueidis connections per node as a power of two; -1 uses a single connection"},
		{Sec: "sink.redis", Key: "read_buffer_each_conn", Kind: KBytes, Def: "512KiB", Ptr: &c.SinkRedis.ReadBufferEachConn,
			Desc: "rueidis read buffer per connection"},
		{Sec: "sink.redis", Key: "write_buffer_each_conn", Kind: KBytes, Def: "512KiB", Ptr: &c.SinkRedis.WriteBufferEachConn,
			Desc: "rueidis write buffer per connection"},

		// ----------------------------------------------------------- sink.sqlite
		{Sec: "sink.sqlite", Key: "path", Kind: KString, Def: "", Ptr: &c.SinkSQL.Path,
			Desc: "events database file; empty derives <server.data_dir>/events.db"},
		{Sec: "sink.sqlite", Key: "rotate", Kind: KEnum, Def: RotateNone, Ptr: &c.SinkSQL.Rotate,
			Enum: []string{RotateNone, RotateHourly, RotateDaily},
			Desc: "rotation period, evaluated in UTC"},
		{Sec: "sink.sqlite", Key: "keep_files", Kind: KInt, Def: "0", Ptr: &c.SinkSQL.KeepFiles, Reload: true,
			Desc: "rotated files to keep; 0 never deletes anything"},
		{Sec: "sink.sqlite", Key: "synchronous", Kind: KEnum, Def: "FULL", Ptr: &c.SinkSQL.Synchronous,
			Enum: syncModes,
			Desc: "FULL survives power loss"},
		{Sec: "sink.sqlite", Key: "busy_timeout", Kind: KDuration, Def: "5s", Ptr: &c.SinkSQL.BusyTimeout,
			Desc: "SQLITE_BUSY retry window"},
		{Sec: "sink.sqlite", Key: "wal_autocheckpoint", Kind: KInt, Def: "1000", Ptr: &c.SinkSQL.WALAutocheckpoint,
			Desc: "WAL pages before an automatic checkpoint"},
	}

	p = append(p, sqlParams("sink.postgres", &c.SinkPG, "127.0.0.1:5432",
		"host:port, or a unix socket directory starting with /",
		"role; needs INSERT on the table and nothing else",
		"loghub.inbox",
		"the LogHub queue (loghub.inbox DDL), name or schema.name in lowercase; never created by the server, only validated")...)
	p = append(p, sqlParams("sink.mysql", &c.SinkMySQL, "127.0.0.1:3306",
		"host:port, or a unix socket path starting with /",
		"user; needs INSERT and UPDATE on the table, because ON DUPLICATE KEY UPDATE demands both (UPDATE may be limited to the uuid column)",
		"syslog_events",
		"events table, name or database.name with its case kept; never created by the server, only validated")...)

	p = append(p, []*Param{
		// ------------------------------------------------------- sink.clickhouse
		{Sec: "sink.clickhouse", Key: "addrs", Kind: KList, Def: "127.0.0.1:9000", Ptr: &c.SinkCH.Addrs,
			Desc: "host:port of the native protocol (9000; 9440 with TLS), comma separated"},
		{Sec: "sink.clickhouse", Key: "username", Kind: KString, Def: "syslog", Ptr: &c.SinkCH.Username,
			Desc: "user; needs INSERT on the ten columns the server writes, nothing else"},
		{Sec: "sink.clickhouse", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkCH.Password, Redact: RedactAll,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: "sink.clickhouse", Key: "database", Kind: KString, Def: "syslog", Ptr: &c.SinkCH.Database,
			Desc: "database name"},
		{Sec: "sink.clickhouse", Key: "table", Kind: KString, Def: "inbox_syslog", Ptr: &c.SinkCH.Table,
			Desc: "the inbox_syslog table (docker/clickhouse/inbox_syslog.sql), name or database.name with its case kept; never created by the server, only validated"},
		{Sec: "sink.clickhouse", Key: "pool_size", Kind: KInt, Def: "0", Ptr: &c.SinkCH.PoolSize,
			Desc: "connection pool ceiling; 0 derives sink.workers + 1"},
		{Sec: "sink.clickhouse", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkCH.TLS,
			Desc: "TLS to ClickHouse, verified against the system pool or tls_ca"},
		{Sec: "sink.clickhouse", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkCH.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.clickhouse", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkCH.TLSInsecure,
			Desc: "skip certificate verification"},

		// ---------------------------------------------------------------- loghub
		{Sec: "loghub", Key: "eb_url", Kind: KString, Def: "", Ptr: &c.LogHub.EBURL, Redact: RedactURL,
			EnvName: loghubEnv("eb_url"),
			Desc:    "LogHub eventbus URL; its scheme selects sink.type and the URL fills that sink's connection: nats:// tls:// redis:// rediss:// postgres:// postgresql://. An explicit conflicting sink key is an error (SPEC §10.9)"},
		{Sec: "loghub", Key: "beacon_interval", Kind: KSeconds, Def: "15", Ptr: &c.LogHub.BeaconInterval,
			EnvName: loghubEnv("beacon_interval"),
			Desc:    "period of the heartbeat beacon (sink=nats only); a bare integer is seconds, as the skill defines it; at least 1s; 0 disables the periodic beacon, not the one on connect"},
		{Sec: "loghub", Key: "ctl_namespace", Kind: KString, Def: "loghub._ctl", Ptr: &c.LogHub.CtlNamespace,
			EnvName: loghubEnv("ctl_namespace"),
			Desc:    "control-plane prefix; the beacon goes to <ctl_namespace>.beacon over NATS core, never JetStream"},

		// ------------------------------------------------------------- emergency
		{Sec: "emergency", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Emergency.Enabled,
			Desc: "dump a non-empty memory ring to disk on FATAL or drain timeout; only ring.mode=memory has anything to dump"},
		{Sec: "emergency", Key: "dir", Kind: KString, Def: "", Ptr: &c.Emergency.Dir,
			Desc: "emergency file directory; empty derives <server.data_dir>/emergency. Created at boot: a directory created during the incident fails during the incident"},

		// ----------------------------------------------------------------- admin
		{Sec: "admin", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Admin.Enabled,
			Desc: "serve /healthz, /readyz and /metrics"},
		{Sec: "admin", Key: "bind", Kind: KString, Def: "127.0.0.1:9102", Ptr: &c.Admin.Bind,
			Desc: "admin listener; 9102 and not 9101, so it never collides with go-webhook-server on the same host"},

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
	}...)

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

// sqlParams declares the identical key set of [sink.postgres] and [sink.mysql]
// (SPEC §16.5); only the defaults and the descriptions differ.
func sqlParams(sec string, d *SQLDB, addr, addrDesc, userDesc, table, tableDesc string) []*Param {
	return []*Param{
		{Sec: sec, Key: "addr", Kind: KString, Def: addr, Ptr: &d.Addr, Desc: addrDesc},
		{Sec: sec, Key: "username", Kind: KString, Def: "syslog", Ptr: &d.Username, Desc: userDesc},
		{Sec: sec, Key: "password", Kind: KString, Def: "", Ptr: &d.Password, Redact: RedactAll,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: sec, Key: "database", Kind: KString, Def: "syslog", Ptr: &d.Database,
			Desc: "database name"},
		{Sec: sec, Key: "table", Kind: KString, Def: table, Ptr: &d.Table, Desc: tableDesc},
		{Sec: sec, Key: "pool_size", Kind: KInt, Def: "0", Ptr: &d.PoolSize,
			Desc: "connection pool ceiling; 0 derives sink.workers + 1"},
		{Sec: sec, Key: "tls", Kind: KBool, Def: "false", Ptr: &d.TLS,
			Desc: "TLS to the database, verified against the system pool or tls_ca"},
		{Sec: sec, Key: "tls_ca", Kind: KString, Def: "", Ptr: &d.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: sec, Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &d.TLSInsecure,
			Desc: "skip certificate verification"},
	}
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
		// goes to stderr and, on a SIGHUP, to the log (SPEC §16.2, pendência
		// 30 de 2026-10-11).
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("unknown parameter (the name holds characters no parameter " +
				"name has, so it is not shown; a value goes after an '=')")
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
// section of the registry: "sink.nats.subjet" and "filter.min_severty" do,
// "hunter2" and "x" do not (SPEC §16.2).
func (ix *index) knownSectionOf(name string) bool {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 {
		return false
	}
	return ix.knownSection(name[:i])
}

func (ix *index) knownSection(sec string) bool {
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
// ("token = eyJhbGciOiJI" then "UzI1NiJ9abcDEF==") and the continuation reads
// as a key.
var iniKeyRE = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// lookupINI is lookup for an INI entry. An unknown key is named only when its
// section is one this server has, its key has the shape of a key, and the
// line before it was not a secret: a key right after a password or a token is
// most likely the rest of that value on the next line ("password = abc" then
// "ffd3a9c1b2=") -- the error goes to stderr and, on a SIGHUP, to the server
// log (SPEC §16.2; independent review of go-relplog-nats-server, 2026-10-13).
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
