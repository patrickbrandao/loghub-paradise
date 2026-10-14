package config

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Param is one tunable. The registry is the single source from which the INI
// parser, the flag parser, the environment reader, --help and the
// effective-config dump are all derived (SPEC §13.2), so the five can never
// disagree.
type Param struct {
	Sec  string // INI section, e.g. "sink.redis"
	Key  string // INI key, e.g. "addrs"
	Kind Kind
	Def  string // default, in the same textual form a user would write
	Ptr  any    // pointer into a Config field
	Desc string

	// Reload marks a parameter that SIGHUP may change at runtime (SPEC §13.3).
	// Everything else is logged as ignored when it differs after a reload.
	Reload bool
}

// Name is the canonical dotted name: "sink.redis.addrs".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.redis.addrs".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "WHS_SINK_REDIS_ADDRS".
func (p *Param) Env() string {
	return "WHS_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(p.Name()))
}

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
		{Sec: "proxy", Key: "trusted", Kind: KList, Def: "127.0.0.0/8,::1", Ptr: &c.Proxy.Trusted, Reload: true,
			Desc: "CIDRs whose forwarding headers are believed; every deploy behind a proxy must set this"},
		{Sec: "proxy", Key: "max_xff_entries", Kind: KInt, Def: "32", Ptr: &c.Proxy.MaxXFFEntries, Reload: true,
			Desc: "X-Forwarded-For entries processed before the rest is ignored"},

		// ------------------------------------------------------------- ratelimit
		{Sec: "ratelimit", Key: "enabled", Kind: KBool, Def: "false", Ptr: &c.RateLimit.Enabled, Reload: true,
			Desc: "off by default: a limiter with misconfigured proxy.trusted is a self-DoS"},
		{Sec: "ratelimit", Key: "rate", Kind: KFloat, Def: "100", Ptr: &c.RateLimit.Rate, Reload: true,
			Desc: "tokens per second per network prefix"},
		{Sec: "ratelimit", Key: "burst", Kind: KFloat, Def: "200", Ptr: &c.RateLimit.Burst, Reload: true,
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
			Desc: "memory | sqlite | none -- where an event waits before delivery"},
		{Sec: "ring", Key: "capacity_items", Kind: KInt, Def: "65536", Ptr: &c.Ring.CapacityItems,
			Desc: "item ceiling; exceeding it returns 429"},
		{Sec: "ring", Key: "max_bytes", Kind: KBytes, Def: "512MiB", Ptr: &c.Ring.MaxBytes,
			Desc: "byte ceiling; exceeding it returns 429. memory: the buffer each item retains (pool class + 256 bytes); sqlite: the JLH bytes stored"},
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
			Desc: "redis | sqlite | postgres | mysql | nats | clickhouse | none -- the final destination"},
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "8", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines; forced to 1 when type=sqlite; lowered to 4 when type=postgres, mysql or clickhouse and not set"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per write"},
		{Sec: "sink", Key: "batch_max_bytes", Kind: KBytes, Def: "16MiB", Ptr: &c.Sink.BatchMaxBytes,
			Desc: "ceiling of JLH bytes per write; a batch goes out in pieces under it and an envelope above it goes alone; 1 byte..256MiB"},
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
			Desc: "TLS to Redis; unlike HTTP, no reverse proxy solves this"},
		{Sec: "sink.redis", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkRedis.TLSCA,
			Desc: "CA bundle path; empty uses the system pool"},
		{Sec: "sink.redis", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkRedis.TLSInsecure,
			Desc: "skip certificate verification"},
		{Sec: "sink.redis", Key: "stream", Kind: KString, Def: "loghub.{space}.{type}", Ptr: &c.SinkRedis.Stream,
			Desc: "target stream key; {space} and {type} are expanded per envelope"},
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
		{Sec: "sink.postgres", Key: "username", Kind: KString, Def: "webhook", Ptr: &c.SinkPG.Username,
			Desc: "role; needs INSERT on the table and SELECT on its uuid column (ON CONFLICT reads it), nothing else"},
		{Sec: "sink.postgres", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkPG.Password,
			Desc: "role password; redacted in the effective-config dump"},
		{Sec: "sink.postgres", Key: "database", Kind: KString, Def: "webhook", Ptr: &c.SinkPG.Database,
			Desc: "database name"},
		{Sec: "sink.postgres", Key: "table", Kind: KString, Def: "loghub.inbox", Ptr: &c.SinkPG.Table,
			Desc: "the LogHub queue (loghub.inbox DDL), name or schema.name in lowercase; never created by the server, only validated"},
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
		{Sec: "sink.mysql", Key: "username", Kind: KString, Def: "webhook", Ptr: &c.SinkMySQL.Username,
			Desc: "user; needs INSERT and UPDATE on the table (UPDATE may be limited to the uuid column)"},
		{Sec: "sink.mysql", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkMySQL.Password,
			Desc: "user password; redacted in the effective-config dump"},
		{Sec: "sink.mysql", Key: "database", Kind: KString, Def: "webhook", Ptr: &c.SinkMySQL.Database,
			Desc: "database name"},
		{Sec: "sink.mysql", Key: "table", Kind: KString, Def: "inbox", Ptr: &c.SinkMySQL.Table,
			Desc: "the inbox table that mirrors loghub.inbox, name or database.name; never created by the server, only validated"},
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
			Desc: "NATS server URLs, comma separated for a cluster; userinfo is redacted in the dump"},
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
		{Sec: "sink.nats", Key: "stream", Kind: KString, Def: "LOGHUB_WEBHOOK", Ptr: &c.SinkNATS.Stream,
			Desc: "JetStream stream that owns the subject; never created by the server, only validated"},
		{Sec: "sink.nats", Key: "subject", Kind: KString, Def: "loghub.{space}.{type}", Ptr: &c.SinkNATS.Subject,
			Desc: "subject every event is published to; {space} and {type} are expanded per envelope, and this workspace's subject must belong to the stream"},

		// ------------------------------------------------------ sink.clickhouse
		{Sec: "sink.clickhouse", Key: "addrs", Kind: KList, Def: "127.0.0.1:9000", Ptr: &c.SinkCH.Addrs,
			Desc: "host:port of the native protocol (9000; 9440 with TLS), comma separated"},
		{Sec: "sink.clickhouse", Key: "username", Kind: KString, Def: "webhook", Ptr: &c.SinkCH.Username,
			Desc: "user; needs INSERT on the ten columns the server writes, nothing else"},
		{Sec: "sink.clickhouse", Key: "password", Kind: KString, Def: "", Ptr: &c.SinkCH.Password,
			Desc: "user password; redacted in the effective-config dump"},
		{Sec: "sink.clickhouse", Key: "database", Kind: KString, Def: "webhook", Ptr: &c.SinkCH.Database,
			Desc: "database name"},
		{Sec: "sink.clickhouse", Key: "table", Kind: KString, Def: "inbox", Ptr: &c.SinkCH.Table,
			Desc: "the inbox table that mirrors loghub.inbox, name or database.name; never created by the server, only validated"},
		{Sec: "sink.clickhouse", Key: "pool_size", Kind: KInt, Def: "0", Ptr: &c.SinkCH.PoolSize,
			Desc: "connection pool ceiling; 0 derives sink.workers + 1"},
		{Sec: "sink.clickhouse", Key: "tls", Kind: KBool, Def: "false", Ptr: &c.SinkCH.TLS,
			Desc: "TLS to ClickHouse, verified against the system pool or tls_ca"},
		{Sec: "sink.clickhouse", Key: "tls_ca", Kind: KString, Def: "", Ptr: &c.SinkCH.TLSCA,
			Desc: "CA bundle path; requires tls=true"},
		{Sec: "sink.clickhouse", Key: "tls_insecure", Kind: KBool, Def: "false", Ptr: &c.SinkCH.TLSInsecure,
			Desc: "skip certificate verification"},

		// ------------------------------------------------------------- emergency
		{Sec: "emergency", Key: "enabled", Kind: KBool, Def: "true", Ptr: &c.Emergency.Enabled,
			Desc: "dump what a shutdown could not deliver (FATAL or drain_timeout) to disk; ring.mode=memory only. Dumps already on disk are reingested at every boot regardless"},
		{Sec: "emergency", Key: "dir", Kind: KString, Def: "", Ptr: &c.Emergency.Dir,
			Desc: "emergency file directory; empty derives <ring.data_dir>/emergency"},

		// --------------------------------------------------------------- headers
		{Sec: "headers", Key: "exclude", Kind: KList, Def: "", Ptr: &c.Headers.Exclude, Reload: true,
			Desc: "header names omitted from the envelope; matched case-insensitively"},

		// ------------------------------------------------------------------- log
		{Sec: "log", Key: "level", Kind: KString, Def: "info", Ptr: &c.Log.Level, Reload: true,
			Desc: "debug | info | warn | error"},
		{Sec: "log", Key: "format", Kind: KString, Def: "json", Ptr: &c.Log.Format, Reload: false,
			Desc: "json | text"},
		{Sec: "log", Key: "access", Kind: KBool, Def: "false", Ptr: &c.Log.Access, Reload: true,
			Desc: "per-request logging; off by default because it caps throughput"},
		{Sec: "log", Key: "access_sample", Kind: KFloat, Def: "0.01", Ptr: &c.Log.AccessSample, Reload: true,
			Desc: "fraction of requests logged when access logging is on"},

		// -------------------------------------------------------------- shutdown
		{Sec: "shutdown", Key: "pre_stop_delay", Kind: KDuration, Def: "0s", Ptr: &c.Shutdown.PreStopDelay,
			Desc: "keep serving after SIGTERM so the load balancer can drop this endpoint"},
		{Sec: "shutdown", Key: "drain_timeout", Kind: KDuration, Def: "30s", Ptr: &c.Shutdown.DrainTimeout,
			Desc: "one deadline for the shutdown drain: open connections and the ring together"},
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
