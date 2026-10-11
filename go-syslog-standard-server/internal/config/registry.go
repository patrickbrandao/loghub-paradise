package config

import (
	"fmt"
	"sort"
	"strings"
)

// Param is one tunable. The registry is the single source from which the INI
// parser, the flag parser, the environment reader, --help and the
// effective-config dump are all derived (SPEC §15.2), so the five can never
// disagree.
type Param struct {
	Sec  string // INI section, e.g. "sink.redis"
	Key  string // INI key, e.g. "addrs"
	Kind Kind
	Def  string // default, in the same textual form a user would write
	Ptr  any    // pointer into a Config field
	Desc string

	// Reload marks a parameter that SIGHUP may change at runtime (SPEC §15.3).
	// Everything else is logged as ignored when it differs after a reload.
	Reload bool
}

// Name is the canonical dotted name: "sink.redis.addrs".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.redis.addrs".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "SYSLOG_SINK_REDIS_ADDRS".
func (p *Param) Env() string {
	return "SYSLOG_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

// Registry returns the parameter table bound to c. Every parameter in SPEC
// §15.5 appears exactly once.
func Registry(c *Config) []*Param {
	p := []*Param{
		// ------------------------------------------------------------------ node
		{Sec: "node", Key: "id", Kind: KString, Def: "", Ptr: &c.Node.ID,
			Desc: "instance identity in the envelope; empty derives from hostname"},

		// ---------------------------------------------------------------- listen
		//
		// The default ports are 1514 and 20514, not 514. The container image
		// runs as uid 65532 and cannot bind a privileged port, so a default of
		// 514 would mean the project's own compose files do not start. The
		// compose publishes "514:1514" instead, and a host install that wants
		// 514 uses setcap or ip_unprivileged_port_start (SPEC §4.1).
		{Sec: "listen", Key: "udp", Kind: KString, Def: ":1514", Ptr: &c.Listen.UDP,
			Desc: "UDP address (RFC 5426); empty disables the transport"},
		{Sec: "listen", Key: "udp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.UDPListeners,
			Desc: "number of UDP sockets; >1 enables SO_REUSEPORT and is the only way past one core"},
		{Sec: "listen", Key: "udp_read_buffer", Kind: KBytes, Def: "8MiB", Ptr: &c.Listen.UDPReadBuffer,
			Desc: "SO_RCVBUF; what does not fit is dropped by the kernel before this process sees it"},
		{Sec: "listen", Key: "tcp", Kind: KString, Def: ":1514", Ptr: &c.Listen.TCP,
			Desc: "TCP address (RFC 6587); empty disables the transport"},
		{Sec: "listen", Key: "tcp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.TCPListeners,
			Desc: "number of TCP listeners; >1 enables SO_REUSEPORT"},
		{Sec: "listen", Key: "relp", Kind: KString, Def: "", Ptr: &c.Listen.RELP,
			Desc: "RELP address; :20514 is the convention. The only transport that can acknowledge"},
		{Sec: "listen", Key: "relp_listeners", Kind: KInt, Def: "1", Ptr: &c.Listen.RELPListeners,
			Desc: "number of RELP listeners; >1 enables SO_REUSEPORT"},
		{Sec: "listen", Key: "unix", Kind: KString, Def: "", Ptr: &c.Listen.Unix,
			Desc: "unix datagram socket path; /dev/log is the convention"},
		{Sec: "listen", Key: "unix_mode", Kind: KOctal, Def: "0666", Ptr: &c.Listen.UnixMode,
			Desc: "socket mode; any process has to be able to log, which is why /dev/log is 0666 everywhere"},
		{Sec: "listen", Key: "unix_read_buffer", Kind: KBytes, Def: "8MiB", Ptr: &c.Listen.UnixReadBuffer,
			Desc: "SO_RCVBUF of the unix socket"},

		// ------------------------------------------------------------------- tcp
		{Sec: "tcp", Key: "framing", Kind: KString, Def: "auto", Ptr: &c.TCP.Framing,
			Desc: "auto | octet | lf -- decided once per connection, never per message"},
		{Sec: "tcp", Key: "max_frame_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.TCP.MaxFrameBytes,
			Desc: "fuse against a connection that never closes the frame; no real emitter comes close"},
		{Sec: "tcp", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.TCP.IdleTimeout,
			Desc: "a connection with no traffic is closed; generous, because an appliance logging hourly is normal; 0 = never"},
		{Sec: "tcp", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConns,
			Desc: "concurrent connection ceiling for the transport, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "tcp", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.TCP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the transport, all listeners together; 0 = unlimited; closes immediately"},
		{Sec: "tcp", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.TCP.BackpressureTimeout,
			Desc: "how long to stop reading while the ring is full before closing the connection"},

		// ------------------------------------------------------------------ relp
		{Sec: "relp", Key: "max_frame_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.RELP.MaxFrameBytes,
			Desc: "ceiling on one RELP frame; a declared length above it closes the connection"},
		{Sec: "relp", Key: "idle_timeout", Kind: KDuration, Def: "120s", Ptr: &c.RELP.IdleTimeout,
			Desc: "a connection with no traffic is closed; 0 = never"},
		{Sec: "relp", Key: "max_conns", Kind: KInt, Def: "0", Ptr: &c.RELP.MaxConns,
			Desc: "concurrent connection ceiling for the transport, all listeners together; 0 = unlimited; blocks in accept"},
		{Sec: "relp", Key: "max_conns_per_ip", Kind: KInt, Def: "0", Ptr: &c.RELP.MaxConnsPerIP,
			Desc: "per-IP connection ceiling for the transport, all listeners together; 0 = unlimited; closes immediately"},
		{Sec: "relp", Key: "window", Kind: KInt, Def: "128", Ptr: &c.RELP.Window,
			Desc: "unacknowledged transactions held before the reader stops pulling"},
		{Sec: "relp", Key: "backpressure_timeout", Kind: KDuration, Def: "30s", Ptr: &c.RELP.BackpressureTimeout,
			Desc: "how long to wait on a full ring before answering 500 ring full"},

		// ----------------------------------------------------------------- parse
		{Sec: "parse", Key: "prefer", Kind: KString, Def: "auto", Ptr: &c.Parse.Prefer,
			Desc: "auto | 5424 | 3164 -- fixes the format when the operator knows what arrives"},
		{Sec: "parse", Key: "default_facility", Kind: KInt, Def: "1", Ptr: &c.Parse.DefaultFacility,
			Desc: "facility when the PRI is absent; RFC 3164 4.3.3 prescribes user"},
		{Sec: "parse", Key: "default_severity", Kind: KInt, Def: "5", Ptr: &c.Parse.DefaultSeverity,
			Desc: "severity when the PRI is absent; RFC 3164 4.3.3 prescribes notice"},
		{Sec: "parse", Key: "sd_max_elements", Kind: KInt, Def: "32", Ptr: &c.Parse.SDMaxElements,
			Desc: "structured-data elements processed; the rest is ignored"},
		{Sec: "parse", Key: "sd_max_params", Kind: KInt, Def: "64", Ptr: &c.Parse.SDMaxParams,
			Desc: "parameters per structured-data element; bounds a map built once per message"},

		// ---------------------------------------------------------------- filter
		{Sec: "filter", Key: "min_severity", Kind: KString, Def: "7", Ptr: &c.Filter.MinSeverity, Reload: true,
			Desc: "accept severity <= this; name or number. Lower is MORE severe, so 4 keeps emerg..warning"},
		{Sec: "filter", Key: "facilities", Kind: KList, Def: "", Ptr: &c.Filter.Facilities, Reload: true,
			Desc: "accepted facilities by name or number; empty accepts all"},

		// ------------------------------------------------------------- ratelimit
		{Sec: "ratelimit", Key: "enabled", Kind: KBool, Def: "false", Ptr: &c.RateLimit.Enabled, Reload: true,
			Desc: "off by default: a limiter keyed on the wrong source is worse than none"},
		{Sec: "ratelimit", Key: "rate", Kind: KFloat, Def: "1000", Ptr: &c.RateLimit.Rate, Reload: true,
			Desc: "tokens per second per network prefix; ten times the webhook default, because a /24 of hosts logs a lot"},
		{Sec: "ratelimit", Key: "burst", Kind: KFloat, Def: "2000", Ptr: &c.RateLimit.Burst, Reload: true,
			Desc: "bucket capacity per network prefix"},
		{Sec: "ratelimit", Key: "v4_mask", Kind: KInt, Def: "24", Ptr: &c.RateLimit.V4Mask, Reload: true,
			Desc: "IPv4 prefix length used as the bucket key"},
		{Sec: "ratelimit", Key: "v6_mask", Kind: KInt, Def: "48", Ptr: &c.RateLimit.V6Mask, Reload: true,
			Desc: "IPv6 prefix length used as the bucket key"},
		{Sec: "ratelimit", Key: "exempt", Kind: KList, Def: "", Ptr: &c.RateLimit.Exempt, Reload: true,
			Desc: "CIDRs exempt from rate limiting"},
		{Sec: "ratelimit", Key: "max_buckets", Kind: KInt, Def: "1000000", Ptr: &c.RateLimit.MaxBuckets, Reload: true,
			Desc: "hard ceiling on tracked prefixes; beyond it new keys pass unlimited"},
		{Sec: "ratelimit", Key: "sweep_interval", Kind: KDuration, Def: "30s", Ptr: &c.RateLimit.SweepInterval, Reload: true,
			Desc: "how often full buckets are evicted"},

		// ------------------------------------------------------------------ ring
		{Sec: "ring", Key: "mode", Kind: KString, Def: "memory", Ptr: &c.Ring.Mode,
			Desc: "memory | sqlite | none -- where a message waits before delivery"},
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling; with 200-byte messages this is the one that binds"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling, counting raw lines only; the envelope is the worker's memory, not the ring's"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction above which /readyz fails"},
		{Sec: "ring", Key: "max_inflight", Kind: KInt, Def: "2048", Ptr: &c.Ring.MaxInflight,
			Desc: "concurrent synchronous writes when mode=none"},
		{Sec: "ring", Key: "data_dir", Kind: KString, Def: "./data", Ptr: &c.Ring.DataDir,
			Desc: "root of every file the service writes; always /data in a container"},

		// ----------------------------------------------------------- ring.sqlite
		{Sec: "ring.sqlite", Key: "path", Kind: KString, Def: "", Ptr: &c.RingSQL.Path,
			Desc: "ring database file; empty derives <ring.data_dir>/ring.db"},
		{Sec: "ring.sqlite", Key: "synchronous", Kind: KString, Def: "FULL", Ptr: &c.RingSQL.Synchronous,
			Desc: "FULL | NORMAL | OFF -- FULL survives power loss and binds p99 to the disk"},
		{Sec: "ring.sqlite", Key: "commit_batch", Kind: KInt, Def: "256", Ptr: &c.RingSQL.CommitBatch,
			Desc: "items per group commit"},
		{Sec: "ring.sqlite", Key: "commit_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.RingSQL.CommitMaxDelay,
			Desc: "maximum wait before committing a partial group"},
		{Sec: "ring.sqlite", Key: "busy_timeout", Kind: KDuration, Def: "5s", Ptr: &c.RingSQL.BusyTimeout,
			Desc: "SQLITE_BUSY retry window"},
		{Sec: "ring.sqlite", Key: "wal_autocheckpoint", Kind: KInt, Def: "1000", Ptr: &c.RingSQL.WALAutocheckpoint,
			Desc: "WAL pages before an automatic checkpoint"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "type", Kind: KString, Def: "redis", Ptr: &c.Sink.Type,
			Desc: "redis | sqlite | postgres | mysql | nats | none -- the final destination"},
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines, which also run the full parse; forced to 1 when type=sqlite, lowered to 4 when type=postgres or mysql and not set"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per write; at most 3855 when type=mysql (65535 placeholders, 17 per row)"},
		{Sec: "sink", Key: "batch_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.Sink.BatchMaxDelay,
			Desc: "maximum wait to fill a batch"},
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
			Desc: "how long boot waits for the sink before exiting 3"},
		{Sec: "sink", Key: "retry_backoff_min", Kind: KDuration, Def: "100ms", Ptr: &c.Sink.RetryBackoffMin,
			Desc: "initial backoff when a batch fails"},
		{Sec: "sink", Key: "retry_backoff_max", Kind: KDuration, Def: "2s", Ptr: &c.Sink.RetryBackoffMax,
			Desc: "backoff ceiling when a batch fails"},

		// ------------------------------------------------------------ sink.redis
		{Sec: "sink.redis", Key: "addrs", Kind: KList, Def: "127.0.0.1:6379", Ptr: &c.SinkRedis.Addrs,
			Desc: "Redis addresses, or Sentinel addresses when sentinel_master is set"},
		{Sec: "sink.redis", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkRedis.Username,
			Desc: "ACL username"},
		{Sec: "sink.redis", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkRedis.Password,
			Desc: "ACL password; redacted in the effective-config dump"},
		{Sec: "sink.redis", Key: "db", Kind: KInt, Def: "0", Ptr: &c.SinkRedis.DB,
			Desc: "database index"},
		{Sec: "sink.redis", Key: "sentinel_master", Kind: KString, Def: "", Ptr: &c.SinkRedis.SentinelMaster,
			Desc: "master name; non-empty selects Sentinel mode"},
		{Sec: "sink.redis", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkRedis.TLS,
			Desc: "TLS to Redis. Unrelated to the ingestion ports, where TLS is out of scope: no proxy solves this one"},
		{Sec: "sink.redis", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkRedis.TLSCA,
			Desc: "CA bundle path; empty uses the system pool"},
		{Sec: "sink.redis", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkRedis.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "sink.redis", Key: "stream", Kind: KString, Def: "syslog:in", Ptr: &c.SinkRedis.Stream,
			Desc: "target stream key; separate from the webhook stream on purpose"},
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
			Desc: "events database file; empty derives <ring.data_dir>/events.db"},
		{Sec: "sink.sqlite", Key: "rotate", Kind: KString, Def: "none", Ptr: &c.SinkSQL.Rotate,
			Desc: "none | hourly | daily -- rotation period, evaluated in UTC"},
		{Sec: "sink.sqlite", Key: "keep_files", Kind: KInt, Def: "0", Ptr: &c.SinkSQL.KeepFiles, Reload: true,
			Desc: "rotated files to keep; 0 never deletes anything"},
		{Sec: "sink.sqlite", Key: "synchronous", Kind: KString, Def: "FULL", Ptr: &c.SinkSQL.Synchronous,
			Desc: "FULL | NORMAL | OFF"},
		{Sec: "sink.sqlite", Key: "busy_timeout", Kind: KDuration, Def: "5s", Ptr: &c.SinkSQL.BusyTimeout,
			Desc: "SQLITE_BUSY retry window"},
		{Sec: "sink.sqlite", Key: "wal_autocheckpoint", Kind: KInt, Def: "1000", Ptr: &c.SinkSQL.WALAutocheckpoint,
			Desc: "WAL pages before an automatic checkpoint"},

		// --------------------------------------------------------- sink.postgres
		{Sec: "sink.postgres", Key: "addr", Kind: KString, Def: "127.0.0.1:5432", Ptr: &c.SinkPG.Addr,
			Desc: "host:port, or a unix socket directory starting with /"},
		{Sec: "sink.postgres", Key: "username", Kind: KString, Def: "syslog", Ptr: &c.SinkPG.Username,
			Desc: "role; needs INSERT on the table and nothing else"},
		{Sec: "sink.postgres", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkPG.Password,
			Desc: "role password; redacted in the effective-config dump"},
		{Sec: "sink.postgres", Key: "database", Kind: KString, Def: "syslog", Ptr: &c.SinkPG.Database,
			Desc: "database name"},
		{Sec: "sink.postgres", Key: "table", Kind: KString, Def: "events", Ptr: &c.SinkPG.Table,
			Desc: "events table, name or schema.name in lowercase; never created by the server, only validated"},
		{Sec: "sink.postgres", Key: "pool_size", Kind: KInt, Def: "0", Ptr: &c.SinkPG.PoolSize,
			Desc: "connection pool ceiling; 0 derives sink.workers + 1"},
		{Sec: "sink.postgres", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkPG.TLS,
			Desc: "TLS to PostgreSQL, verified against the system pool or tls_ca"},
		{Sec: "sink.postgres", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkPG.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.postgres", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkPG.TLSInsecure,
			Desc: "skip certificate verification"},

		// ------------------------------------------------------------ sink.mysql
		{Sec: "sink.mysql", Key: "addr", Kind: KString, Def: "127.0.0.1:3306", Ptr: &c.SinkMySQL.Addr,
			Desc: "host:port, or a unix socket path starting with /"},
		{Sec: "sink.mysql", Key: "username", Kind: KString, Def: "syslog", Ptr: &c.SinkMySQL.Username,
			Desc: "user; needs INSERT and UPDATE on the table, because ON DUPLICATE KEY UPDATE demands both (UPDATE may be limited to the id column)"},
		{Sec: "sink.mysql", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkMySQL.Password,
			Desc: "user password; redacted in the effective-config dump"},
		{Sec: "sink.mysql", Key: "database", Kind: KString, Def: "syslog", Ptr: &c.SinkMySQL.Database,
			Desc: "database name"},
		{Sec: "sink.mysql", Key: "table", Kind: KString, Def: "events", Ptr: &c.SinkMySQL.Table,
			Desc: "events table, name or database.name with its case kept; never created by the server, only validated"},
		{Sec: "sink.mysql", Key: "pool_size", Kind: KInt, Def: "0", Ptr: &c.SinkMySQL.PoolSize,
			Desc: "connection pool ceiling; 0 derives sink.workers + 1"},
		{Sec: "sink.mysql", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkMySQL.TLS,
			Desc: "TLS to MySQL, verified against the system pool or tls_ca"},
		{Sec: "sink.mysql", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkMySQL.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.mysql", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkMySQL.TLSInsecure,
			Desc: "skip certificate verification"},

		// ------------------------------------------------------------- sink.nats
		{Sec: "sink.nats", Key: "urls", Kind: KList, Def: "nats://127.0.0.1:4222", Ptr: &c.SinkNATS.URLs,
			Desc: "NATS server URLs, comma separated for a cluster"},
		{Sec: "sink.nats", Key: "username", Kind: KString, Def: "", Ptr: &c.SinkNATS.Username,
			Desc: "user for password authentication"},
		{Sec: "sink.nats", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkNATS.Password,
			Desc: "password; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "token", Kind: KString, Def: "", Ptr: &c.SinkNATS.Token,
			Desc: "token authentication; redacted in the effective-config dump"},
		{Sec: "sink.nats", Key: "credentials", Kind: KString, Def: "", Ptr: &c.SinkNATS.Credentials,
			Desc: "path to a .creds file (JWT + NKey seed)"},
		{Sec: "sink.nats", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkNATS.TLS,
			Desc: "TLS to NATS, verified against the system pool or tls_ca"},
		{Sec: "sink.nats", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkNATS.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.nats", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkNATS.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "SYSLOG", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that owns the subject; separate from the webhook stream, never created by the server, only validated"},
		{Sec: "sink.nats", Key: "subject", Kind: KString, Def: "syslog.in", Ptr: &c.SinkNATS.Subject,
			Desc: "subject every message is published to; must belong to the stream"},

		// ------------------------------------------------------------- emergency
		{Sec: "emergency", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Emergency.Enabled,
			Desc: "dump a non-empty memory ring to disk on FATAL; only affects ring.mode=memory"},
		{Sec: "emergency", Key: "dir", Kind: KString, Def: "", Ptr: &c.Emergency.Dir,
			Desc: "emergency file directory; empty derives <ring.data_dir>/emergency"},

		// ----------------------------------------------------------------- admin
		{Sec: "admin", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Admin.Enabled,
			Desc: "serve /healthz, /readyz and /metrics"},
		{Sec: "admin", Key: "bind", Kind: KString, Def: "127.0.0.1:9102", Ptr: &c.Admin.Bind,
			Desc: "admin listener; 9102 and not 9101, so it never collides with go-webhook-server on the same host"},

		// ------------------------------------------------------------------- log
		{Sec: "log", Key: "level", Kind: KString, Def: "info", Ptr: &c.Log.Level, Reload: true,
			Desc: "debug | info | warn | error"},
		{Sec: "log", Key: "format", Kind: KString, Def: "json", Ptr: &c.Log.Format,
			Desc: "json | text"},
		{Sec: "log", Key: "access", Kind: KBool, Def: "false", Ptr: &c.Log.Access, Reload: true,
			Desc: "per-message logging; off by default because here it is literally logging log"},
		{Sec: "log", Key: "access_sample", Kind: KFloat, Def: "0.01", Ptr: &c.Log.AccessSample, Reload: true,
			Desc: "fraction of messages logged when access logging is on"},

		// -------------------------------------------------------------- shutdown
		{Sec: "shutdown", Key: "pre_stop_delay", Kind: KDuration, Def: "0s", Ptr: &c.Shutdown.PreStopDelay,
			Desc: "keep serving after SIGTERM so the load balancer can drop this endpoint; matters more here, because a UDP emitter gets no error at all"},
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
		return nil, fmt.Errorf("unknown parameter %q", name)
	}
	return p, nil
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
