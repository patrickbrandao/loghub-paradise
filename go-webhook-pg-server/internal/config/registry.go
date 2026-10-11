package config

import (
	"fmt"
	"sort"
	"strings"
)

// Param is one tunable. The registry is the single source from which the INI
// parser, the flag parser, the environment reader, --help and the
// effective-config dump are all derived (SPEC §13.2), so the five can never
// disagree.
type Param struct {
	Sec  string // INI section, e.g. "sink.postgres"
	Key  string // INI key, e.g. "addr"
	Kind Kind
	Def  string // default, in the same textual form a user would write
	Ptr  any    // pointer into a Config field
	Desc string
}

// Name is the canonical dotted name: "sink.postgres.addr".
func (p *Param) Name() string { return p.Sec + "." + p.Key }

// Flag is the command-line form: "--sink.postgres.addr".
func (p *Param) Flag() string { return "--" + p.Name() }

// Env is the environment form: "WHS_SINK_POSTGRES_ADDR".
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
			Desc: "byte ceiling; exceeding it returns 429"},
		{Sec: "ring", Key: "high_water", Kind: KFloat, Def: "0.80", Ptr: &c.Ring.HighWater,
			Desc: "occupancy fraction above which /readyz fails"},
		{Sec: "ring", Key: "data_dir", Kind: KString, Def: "./data", Ptr: &c.Ring.DataDir,
			Desc: "root of every file the service writes; always /data in a container"},

		// ------------------------------------------------------------------ sink
		{Sec: "sink", Key: "type", Kind: KString, Def: "postgres", Ptr: &c.Sink.Type,
			Desc: "compatibility selector: only postgres (SPEC §0)"},
		{Sec: "sink", Key: "workers", Kind: KInt, Def: "4", Ptr: &c.Sink.Workers,
			Desc: "drain goroutines; each holds a PostgreSQL connection while it writes"},
		{Sec: "sink", Key: "batch", Kind: KInt, Def: "256", Ptr: &c.Sink.Batch,
			Desc: "items per write"},
		{Sec: "sink", Key: "batch_max_delay", Kind: KDuration, Def: "5ms", Ptr: &c.Sink.BatchMaxDelay,
			Desc: "maximum wait to fill a batch"},
		{Sec: "sink", Key: "op_timeout", Kind: KDuration, Def: "5s", Ptr: &c.Sink.OpTimeout,
			Desc: "deadline for one write, and for the boot schema validation"},
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
