package config

import (
	"fmt"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Defaults that only apply to the relational destinations (SPEC §8.5.3 and
// §8.6.3). They live here rather than in the registry because the registry
// holds ONE default per parameter, and these are conditional on another
// parameter's value.
const (
	// dbDefaultWorkers replaces the registry default of sink.workers when the
	// operator did not set it and the destination is postgres or mysql. Every
	// worker holds a connection, and the server's connection budget (100 by
	// default in PostgreSQL, 151 in MySQL) is shared by every instance.
	dbDefaultWorkers = 4

	// dbWarnWorkers is the explicit value above which the boot warns.
	dbWarnWorkers = 16

	// mysqlMaxBatch is the most rows one INSERT can carry: the binary
	// protocol addresses at most 65 535 placeholders per statement, and each
	// row of the inbox uses ten (SPEC §8.6.4).
	mysqlMaxBatch = 5000

	// pgParamCeiling is the largest value one protocol parameter can carry.
	// A batch is a single parameter per column, so a batch of large bodies can
	// reach it (SPEC §8.5.4).
	pgParamCeiling = int64(1) << 30
)

// pgIdent is what sink.postgres.table must look like: a plain lowercase
// identifier, optionally schema-qualified. Lowercase only, so the name can be
// handed to to_regclass unquoted and mean the same table the INSERT names
// quoted; uppercase would fold in one place and not the other.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// mysqlIdent is what sink.mysql.table must look like: an unquoted identifier,
// optionally database-qualified, up to 64 characters each. Case is kept: MySQL
// table names are case-sensitive on Linux, and the name is compared verbatim
// against information_schema and quoted with backticks in every statement.
var mysqlIdent = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}(\.[A-Za-z0-9_]{1,64})?$`)

// chIdent is what sink.clickhouse.table must look like: an unquoted
// identifier, optionally database-qualified. Case is kept, as in MySQL:
// ClickHouse names are case-sensitive, compared verbatim against system.tables
// and quoted with backticks in the INSERT.
var chIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63})?$`)

// natsToken is one dot-separated token of a subject or a stream name: no
// whitespace, no wildcards, no separators. Wildcards are refused because this
// server PUBLISHES to the subject, and a wildcard is not a publishable subject.
var natsToken = regexp.MustCompile(`^[^\s.*>/\\]+$`)

// Validate checks every constraint the server depends on and returns the boot
// warnings from SPEC §10.1. An error here is exit code 1: the process must not
// start.
//
// setIn says which parameters were written by the file, the environment or the
// command line, as opposed to carrying their registry default. It is consulted
// only where a default is conditional on another parameter (SPEC §8.5.3); nil
// means "everything is a default".
func Validate(c *Config, setIn map[string]bool) (warnings []string, err error) {
	warn := func(format string, a ...any) { warnings = append(warnings, fmt.Sprintf(format, a...)) }

	// --- enums --------------------------------------------------------------
	switch c.Ring.Mode {
	case RingMemory, RingSQLiteMode, RingNone:
	default:
		return nil, fmt.Errorf("ring.mode: %q is not one of memory, sqlite, none", c.Ring.Mode)
	}
	switch c.Sink.Type {
	case SinkRedisType, SinkSQLiteType, SinkPostgresType, SinkMySQLType, SinkNATSType, SinkClickHouseType, SinkNoneType:
	default:
		return nil, fmt.Errorf("sink.type: %q is not one of redis, sqlite, postgres, mysql, nats, clickhouse, none", c.Sink.Type)
	}
	if err := checkSynchronous("ring.sqlite.synchronous", c.RingSQL.Synchronous); err != nil {
		return nil, err
	}
	if err := checkSynchronous("sink.sqlite.synchronous", c.SinkSQL.Synchronous); err != nil {
		return nil, err
	}
	switch c.SinkSQL.Rotate {
	case "none", "hourly", "daily":
	default:
		return nil, fmt.Errorf("sink.sqlite.rotate: %q is not one of none, hourly, daily", c.SinkSQL.Rotate)
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("log.level: %q is not one of debug, info, warn, error", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return nil, fmt.Errorf("log.format: %q is not one of json, text", c.Log.Format)
	}

	// --- ranges -------------------------------------------------------------
	if c.Server.Listeners < 1 {
		return nil, fmt.Errorf("server.listeners: must be at least 1, got %d", c.Server.Listeners)
	}
	if c.Server.MaxBodyBytes <= 0 {
		return nil, fmt.Errorf("server.max_body_bytes: must be positive")
	}
	if c.Server.MaxHeaderBytes <= 0 {
		return nil, fmt.Errorf("server.max_header_bytes: must be positive")
	}
	if c.Server.MaxURIBytes <= 0 {
		return nil, fmt.Errorf("server.max_uri_bytes: must be positive")
	}
	if c.Server.MaxConns < 0 || c.Server.MaxConnsPerIP < 0 {
		return nil, fmt.Errorf("server.max_conns and server.max_conns_per_ip cannot be negative")
	}
	if c.Server.RejectDrainBytes < 0 {
		return nil, fmt.Errorf("server.reject_drain_bytes cannot be negative")
	}
	if c.Server.HTTP2MaxConcurrentStreams < 1 {
		return nil, fmt.Errorf("server.http2_max_concurrent_streams: must be at least 1")
	}
	if c.Ring.CapacityItems < 1 {
		return nil, fmt.Errorf("ring.capacity_items: must be at least 1")
	}
	if c.Ring.MaxBytes < 1 {
		return nil, fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return nil, fmt.Errorf("ring.high_water: must be in (0, 1], got %g", c.Ring.HighWater)
	}
	if c.Ring.MaxInflight < 1 {
		return nil, fmt.Errorf("ring.max_inflight: must be at least 1")
	}
	if c.RingSQL.CommitBatch < 1 {
		return nil, fmt.Errorf("ring.sqlite.commit_batch: must be at least 1")
	}
	if c.Sink.Workers < 1 {
		return nil, fmt.Errorf("sink.workers: must be at least 1")
	}
	if c.Sink.Batch < 1 {
		return nil, fmt.Errorf("sink.batch: must be at least 1")
	}
	if c.Sink.MaxRetries < 1 {
		return nil, fmt.Errorf("sink.max_retries: must be at least 1")
	}
	if c.Sink.HealthTimeout > c.Sink.HealthInterval {
		warn("sink.health_timeout (%s) exceeds sink.health_interval (%s): checks will overlap",
			c.Sink.HealthTimeout, c.Sink.HealthInterval)
	}
	// Zero is not "unlimited" for these. A zero op_timeout fails every write
	// before it starts and is retried forever without ever turning fatal; a
	// zero drain_timeout turns every shutdown into an emergency dump and exit
	// 3; a zero health cadence, busy timeout or backoff floor used to be
	// silently replaced by a default that --dump-config did not show. Refusing
	// here is what keeps the dump and the running process telling the same
	// story (SPEC §13.5).
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"sink.op_timeout", c.Sink.OpTimeout},
		{"sink.health_interval", c.Sink.HealthInterval},
		{"sink.health_timeout", c.Sink.HealthTimeout},
		{"sink.boot_timeout", c.Sink.BootTimeout},
		{"sink.retry_backoff_min", c.Sink.RetryBackoffMin},
		{"sink.retry_backoff_max", c.Sink.RetryBackoffMax},
		{"ring.sqlite.commit_max_delay", c.RingSQL.CommitMaxDelay},
		{"ring.sqlite.busy_timeout", c.RingSQL.BusyTimeout},
		{"sink.sqlite.busy_timeout", c.SinkSQL.BusyTimeout},
		{"ratelimit.sweep_interval", c.RateLimit.SweepInterval},
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout},
	} {
		if d.v <= 0 {
			return nil, fmt.Errorf("%s: must be positive, got %s", d.name, d.v)
		}
	}
	if c.Sink.RetryBackoffMin > c.Sink.RetryBackoffMax {
		return nil, fmt.Errorf("sink.retry_backoff_min (%s) exceeds sink.retry_backoff_max (%s)",
			c.Sink.RetryBackoffMin, c.Sink.RetryBackoffMax)
	}
	if c.SinkRedis.MaxLen < 0 {
		return nil, fmt.Errorf("sink.redis.maxlen cannot be negative")
	}
	if c.SinkSQL.KeepFiles < 0 {
		return nil, fmt.Errorf("sink.sqlite.keep_files cannot be negative")
	}
	if c.SinkPG.PoolSize < 0 {
		return nil, fmt.Errorf("sink.postgres.pool_size cannot be negative")
	}
	if c.SinkMySQL.PoolSize < 0 {
		return nil, fmt.Errorf("sink.mysql.pool_size cannot be negative")
	}
	if c.SinkCH.PoolSize < 0 {
		return nil, fmt.Errorf("sink.clickhouse.pool_size cannot be negative")
	}
	if c.Log.AccessSample < 0 || c.Log.AccessSample > 1 {
		return nil, fmt.Errorf("log.access_sample: must be in [0, 1], got %g", c.Log.AccessSample)
	}

	// --- rate limit ---------------------------------------------------------
	if c.RateLimit.V4Mask < 0 || c.RateLimit.V4Mask > 32 {
		return nil, fmt.Errorf("ratelimit.v4_mask: must be in [0, 32], got %d", c.RateLimit.V4Mask)
	}
	if c.RateLimit.V6Mask < 0 || c.RateLimit.V6Mask > 128 {
		return nil, fmt.Errorf("ratelimit.v6_mask: must be in [0, 128], got %d", c.RateLimit.V6Mask)
	}
	if c.RateLimit.MaxBuckets < 1 {
		return nil, fmt.Errorf("ratelimit.max_buckets: must be at least 1")
	}
	if c.RateLimit.Enabled {
		if c.RateLimit.Rate <= 0 {
			return nil, fmt.Errorf("ratelimit.rate: must be positive when the limiter is enabled")
		}
		if c.RateLimit.Burst < 1 {
			return nil, fmt.Errorf("ratelimit.burst: must be at least 1 when the limiter is enabled")
		}
	}

	// --- CIDR lists ---------------------------------------------------------
	if _, err := ParsePrefixes(c.Proxy.Trusted); err != nil {
		return nil, fmt.Errorf("proxy.trusted: %w", err)
	}
	if _, err := ParsePrefixes(c.RateLimit.Exempt); err != nil {
		return nil, fmt.Errorf("ratelimit.exempt: %w", err)
	}
	if c.Proxy.MaxXFFEntries < 1 {
		return nil, fmt.Errorf("proxy.max_xff_entries: must be at least 1")
	}

	// --- sink specifics -----------------------------------------------------
	if c.Sink.Type == SinkRedisType {
		if len(c.SinkRedis.Addrs) == 0 {
			return nil, fmt.Errorf("sink.redis.addrs: at least one address is required when sink.type=redis")
		}
		if c.SinkRedis.Stream == "" {
			return nil, fmt.Errorf("sink.redis.stream: cannot be empty")
		}
		if c.SinkRedis.SentinelMaster != "" && c.SinkRedis.DB != 0 {
			warn("sink.redis.db=%d with Sentinel: verify the replica set exposes that database", c.SinkRedis.DB)
		}
	}

	if c.Sink.Type == SinkPostgresType {
		if err := validatePostgres(c, setIn, warn); err != nil {
			return nil, err
		}
	}
	if c.Sink.Type == SinkMySQLType {
		if err := validateMySQL(c, setIn, warn); err != nil {
			return nil, err
		}
	}
	if c.Sink.Type == SinkNATSType {
		if err := validateNATS(c); err != nil {
			return nil, err
		}
	}
	if c.Sink.Type == SinkClickHouseType {
		if err := validateClickHouse(c, setIn, warn); err != nil {
			return nil, err
		}
	}

	// The same file used as both queue and destination would have the drain
	// deleting from the very table it writes into, under one write lock
	// (SPEC §10.1).
	if c.Ring.Mode == RingSQLiteMode && c.Sink.Type == SinkSQLiteType {
		ringPath, err1 := filepath.Abs(c.RingSQL.Path)
		sinkPath, err2 := filepath.Abs(c.SinkSQL.Path)
		if err1 == nil && err2 == nil && ringPath == sinkPath {
			return nil, fmt.Errorf(
				"ring.sqlite.path and sink.sqlite.path are the same file (%s): "+
					"the ring is a queue and events is an append-only destination; use separate files",
				ringPath)
		}
		warn("ring.mode=sqlite with sink.type=sqlite: every event pays TWO fsyncs before delivery. " +
			"ring.mode=memory with sink.type=sqlite gives the same durability for half the cost, " +
			"unless the two files are deliberately on different disks")
	}

	if c.Sink.Type == SinkSQLiteType && c.Sink.Workers > 1 {
		warn("sink.type=sqlite forces sink.workers to 1 (was %d): SQLite serialises writers, "+
			"so extra goroutines would only produce SQLITE_BUSY", c.Sink.Workers)
		c.Sink.Workers = 1
	}

	// --- exposure warnings --------------------------------------------------
	if !c.RateLimit.Enabled && !isLoopbackBind(c.Server.Bind) {
		warn("ratelimit.enabled=false with server.bind=%q: this instance is reachable off-host "+
			"with no rate protection. Configure proxy.trusted first, then enable the limiter",
			c.Server.Bind)
	}
	if !isLoopbackBind(c.Admin.Bind) {
		warn("admin.bind=%q is not loopback: /metrics and /readyz are reachable off-host", c.Admin.Bind)
	}
	if c.Ring.Mode == RingNone && c.Sink.Type == SinkNoneType {
		warn("ring.mode=none with sink.type=none: every request is answered 200 and DISCARDED. " +
			"This is the pure-HTTP benchmark mode and must never run in production")
	} else if c.Sink.Type == SinkNoneType {
		warn("sink.type=none: events traverse the ring and are then DISCARDED. " +
			"This is a benchmark mode and must never run in production")
	}
	if c.Ring.Mode == RingMemory && !c.Emergency.Enabled {
		warn("ring.mode=memory with emergency.enabled=false: a fatal sink failure will drop " +
			"the whole ring with no chance of recovery")
	}
	if c.SinkRedis.MaxLen > 0 && c.Sink.Type == SinkRedisType {
		warn("sink.redis.maxlen=%d: the stream will discard old events SILENTLY once it is full, "+
			"which defeats the backpressure this server is built around", c.SinkRedis.MaxLen)
	}

	return warnings, nil
}

// validatePostgres holds the [sink.postgres] rules of SPEC §8.5 and §10.1. It
// may rewrite sink.workers and sink.postgres.pool_size, which is why it runs
// before anything that reads them.
func validatePostgres(c *Config, setIn map[string]bool, warn func(string, ...any)) error {
	pg := &c.SinkPG
	if pg.Addr == "" {
		return fmt.Errorf("sink.postgres.addr: cannot be empty when sink.type=postgres")
	}
	if pg.Username == "" {
		return fmt.Errorf("sink.postgres.username: cannot be empty when sink.type=postgres")
	}
	if pg.Database == "" {
		return fmt.Errorf("sink.postgres.database: cannot be empty when sink.type=postgres")
	}
	if !pgIdent.MatchString(pg.Table) {
		return fmt.Errorf("sink.postgres.table: %q is not a plain identifier "+
			"(name or schema.name; lowercase letters, digits and _; up to 63 chars each)", pg.Table)
	}
	if pg.TLSCA != "" && !pg.TLS {
		// Someone who points at a CA believes TLS is on. Silently connecting
		// in the clear is exactly what they were trying to prevent.
		return fmt.Errorf("sink.postgres.tls_ca is set but sink.postgres.tls=false: enable tls or drop the CA")
	}
	if pg.TLS && strings.HasPrefix(pg.Addr, "/") {
		return fmt.Errorf("sink.postgres.tls=true with a unix socket address (%s): TLS is a TCP feature", pg.Addr)
	}

	// The registry default of 8 workers is right for Redis and wrong here:
	// each worker pins a connection for the life of the process, and the
	// budget is the SERVER's, shared by every instance (SPEC §8.5.3). Only a
	// value nobody wrote is replaced; an explicit number is the operator's.
	if !setIn["sink.workers"] && c.Sink.Workers > dbDefaultWorkers {
		warn("sink.type=postgres lowers the default sink.workers from %d to %d: every worker "+
			"holds a connection, and the server's max_connections is shared by every instance. "+
			"Set sink.workers explicitly to override", c.Sink.Workers, dbDefaultWorkers)
		c.Sink.Workers = dbDefaultWorkers
	} else if c.Sink.Workers > dbWarnWorkers {
		warn("sink.workers=%d with sink.type=postgres: each worker holds a connection, so N "+
			"instances need N x %d of the server's max_connections", c.Sink.Workers, c.Sink.Workers)
	}

	// One connection per worker plus one for the health check, which runs
	// beside the workers and cannot queue for a connection without blowing
	// sink.health_timeout.
	if pg.PoolSize == 0 {
		pg.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && pg.PoolSize < c.Sink.Workers+1 {
		warn("sink.postgres.pool_size=%d is below sink.workers+1=%d: workers will queue for a "+
			"connection, and the health check competes with them", pg.PoolSize, c.Sink.Workers+1)
	}

	// With ring=none the writers are the handlers, bounded by max_inflight.
	// Beyond the pool they wait INSIDE the pool, a queue with none of the
	// ring's accounting -- the very thing the semaphore exists to prevent
	// (SPEC §7.5).
	if c.Ring.Mode == RingNone && c.Ring.MaxInflight > pg.PoolSize {
		warn("ring.mode=none with ring.max_inflight=%d above sink.postgres.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's accounting. "+
			"Raise sink.postgres.pool_size or lower ring.max_inflight", c.Ring.MaxInflight, pg.PoolSize)
	}

	// A batch is one parameter per column, and one parameter tops out at
	// 1 GiB (SPEC §8.5.4).
	if worst := WorstBatchBytes(c); worst > pgParamCeiling {
		warn("sink.type=postgres: one batch can carry up to %s of envelopes, above the 1 GiB "+
			"ceiling of a single protocol parameter; lower sink.batch, server.max_body_bytes "+
			"or ring.max_bytes", FormatBytes(worst))
	}
	return nil
}

// WorstBatchBytes is the most envelope bytes one sink write can carry (SPEC
// §8.5.4, §8.8.5). With ring=none a write is one request, so it is the largest
// envelope (SPEC §3.2). Otherwise a batch is sink.batch envelopes, but never
// more than the ring may hold: the count-only batch limit cannot see bytes, and
// the ring's byte ceiling is what bounds them in practice, so the smaller of
// the two worst cases is used rather than one that always fires.
func WorstBatchBytes(c *Config) int64 {
	event := envelope.MaxEventBytes(c.Server.MaxBodyBytes, c.Server.MaxHeaderBytes, c.Server.MaxURIBytes)
	if c.Ring.Mode == RingNone {
		return event
	}
	worst := event * int64(c.Sink.Batch)
	if c.Ring.MaxBytes < worst {
		worst = c.Ring.MaxBytes
	}
	return worst
}

// validateMySQL holds the [sink.mysql] rules of SPEC §8.6 and §10.1. They are
// the postgres rules with two differences that come from the protocol: the
// table name keeps its case, and a batch is capped by the placeholder limit.
func validateMySQL(c *Config, setIn map[string]bool, warn func(string, ...any)) error {
	my := &c.SinkMySQL
	if my.Addr == "" {
		return fmt.Errorf("sink.mysql.addr: cannot be empty when sink.type=mysql")
	}
	if my.Username == "" {
		return fmt.Errorf("sink.mysql.username: cannot be empty when sink.type=mysql")
	}
	if my.Database == "" {
		return fmt.Errorf("sink.mysql.database: cannot be empty when sink.type=mysql")
	}
	if !mysqlIdent.MatchString(my.Table) {
		return fmt.Errorf("sink.mysql.table: %q is not a plain identifier "+
			"(name or database.name; letters, digits and _; up to 64 chars each)", my.Table)
	}
	if my.TLSCA != "" && !my.TLS {
		return fmt.Errorf("sink.mysql.tls_ca is set but sink.mysql.tls=false: enable tls or drop the CA")
	}
	if my.TLS && strings.HasPrefix(my.Addr, "/") {
		return fmt.Errorf("sink.mysql.tls=true with a unix socket address (%s): TLS is a TCP feature", my.Addr)
	}
	// Ten placeholders per row against the protocol's ceiling of 65 535
	// per statement. The sink never splits a batch beyond what the server's
	// packet limit demands (SPEC §8.6.4), so the count limit is enforced here,
	// where the operator can see it, rather than discovered as a refused write.
	if c.Sink.Batch > mysqlMaxBatch {
		return fmt.Errorf("sink.batch: %d exceeds %d, the most rows one MySQL INSERT can carry "+
			"(65535 placeholders, 10 per row)", c.Sink.Batch, mysqlMaxBatch)
	}

	if !setIn["sink.workers"] && c.Sink.Workers > dbDefaultWorkers {
		warn("sink.type=mysql lowers the default sink.workers from %d to %d: every worker "+
			"holds a connection, and the server's max_connections is shared by every instance. "+
			"Set sink.workers explicitly to override", c.Sink.Workers, dbDefaultWorkers)
		c.Sink.Workers = dbDefaultWorkers
	} else if c.Sink.Workers > dbWarnWorkers {
		warn("sink.workers=%d with sink.type=mysql: each worker holds a connection, so N "+
			"instances need N x %d of the server's max_connections", c.Sink.Workers, c.Sink.Workers)
	}

	if my.PoolSize == 0 {
		my.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && my.PoolSize < c.Sink.Workers+1 {
		warn("sink.mysql.pool_size=%d is below sink.workers+1=%d: workers will queue for a "+
			"connection, and the health check competes with them", my.PoolSize, c.Sink.Workers+1)
	}
	if c.Ring.Mode == RingNone && c.Ring.MaxInflight > my.PoolSize {
		warn("ring.mode=none with ring.max_inflight=%d above sink.mysql.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's accounting. "+
			"Raise sink.mysql.pool_size or lower ring.max_inflight", c.Ring.MaxInflight, my.PoolSize)
	}
	return nil
}

// validateClickHouse holds the [sink.clickhouse] rules of SPEC §8.8 and §10.1:
// the connection budget of the relational destinations, and fewer workers for
// a second reason -- every INSERT is a part on the server's disk (SPEC §8.8.2).
func validateClickHouse(c *Config, setIn map[string]bool, warn func(string, ...any)) error {
	ch := &c.SinkCH
	if len(ch.Addrs) == 0 {
		return fmt.Errorf("sink.clickhouse.addrs: at least one address is required when sink.type=clickhouse")
	}
	for _, a := range ch.Addrs {
		if _, _, err := net.SplitHostPort(a); err != nil {
			return fmt.Errorf("sink.clickhouse.addrs: %q is not host:port: %w", a, err)
		}
	}
	if ch.Username == "" {
		return fmt.Errorf("sink.clickhouse.username: cannot be empty when sink.type=clickhouse")
	}
	if ch.Database == "" {
		return fmt.Errorf("sink.clickhouse.database: cannot be empty when sink.type=clickhouse")
	}
	if !chIdent.MatchString(ch.Table) {
		return fmt.Errorf("sink.clickhouse.table: %q is not a plain identifier "+
			"(name or database.name; letters, digits and _, not starting with a digit; up to 64 chars each)", ch.Table)
	}
	if ch.TLSCA != "" && !ch.TLS {
		return fmt.Errorf("sink.clickhouse.tls_ca is set but sink.clickhouse.tls=false: enable tls or drop the CA")
	}

	if !setIn["sink.workers"] && c.Sink.Workers > dbDefaultWorkers {
		warn("sink.type=clickhouse lowers the default sink.workers from %d to %d: every worker "+
			"holds a connection, and every INSERT is a part the server has to merge. "+
			"Set sink.workers explicitly to override", c.Sink.Workers, dbDefaultWorkers)
		c.Sink.Workers = dbDefaultWorkers
	} else if c.Sink.Workers > dbWarnWorkers {
		warn("sink.workers=%d with sink.type=clickhouse: each worker holds a connection and writes "+
			"its own parts; fewer, larger INSERTs are what ClickHouse merges best", c.Sink.Workers)
	}

	if ch.PoolSize == 0 {
		ch.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && ch.PoolSize < c.Sink.Workers+1 {
		warn("sink.clickhouse.pool_size=%d is below sink.workers+1=%d: workers will queue for a "+
			"connection, and the health check competes with them", ch.PoolSize, c.Sink.Workers+1)
	}
	if c.Ring.Mode == RingNone && c.Ring.MaxInflight > ch.PoolSize {
		warn("ring.mode=none with ring.max_inflight=%d above sink.clickhouse.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's accounting. "+
			"Raise sink.clickhouse.pool_size or lower ring.max_inflight", c.Ring.MaxInflight, ch.PoolSize)
	}
	return nil
}

// validateNATS holds the [sink.nats] rules of SPEC §8.7 and §10.1.
func validateNATS(c *Config) error {
	n := &c.SinkNATS
	if len(n.URLs) == 0 {
		return fmt.Errorf("sink.nats.urls: at least one URL is required when sink.type=nats")
	}
	// Each URL is parsed here, and never quoted in the message: a password
	// with an unescaped %, / or # failed inside nats.Connect as a network
	// error, with the password in every log line, until sink.boot_timeout
	// (SPEC §8.7.9; found in go-webhook-nats-server).
	urlUserinfo := false
	for _, raw := range n.URLs {
		raw = strings.TrimSpace(raw)
		if !strings.Contains(raw, "://") {
			// nats.go dials a bare host:port as nats://.
			raw = "nats://" + raw
		}
		u, err := parseURL(raw)
		if err != nil {
			return fmt.Errorf("sink.nats.urls: an entry is %v (%s)", err, percentEncodeHint)
		}
		if u.Hostname() == "" {
			return fmt.Errorf("sink.nats.urls: an entry has no host (%s)", percentEncodeHint)
		}
		// It is also what catches the password that parses:
		// nats://user:1234/5678@host is host "user", port 1234 and a path,
		// with no credentials at all, and the client would dial it until
		// sink.boot_timeout.
		if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("sink.nats.urls: an entry carries a path, a query or a fragment, "+
				"which a NATS URL has none of (%s)", percentEncodeHint)
		}
		if u.User != nil {
			urlUserinfo = true
		}
	}
	// nats.go prefers the URL's userinfo over every explicit credential: the
	// two together would leave the explicit one unused, silently.
	if urlUserinfo && (n.Username != "" || n.Password != "" || n.Token != "" || n.Credentials != "") {
		return fmt.Errorf("sink.nats.urls carries credentials (user:password@ or token@) and " +
			"sink.nats.username/password/token/credentials is set too: the client would use the " +
			"URL's and ignore the other; keep one")
	}
	if !natsToken.MatchString(n.Stream) {
		return fmt.Errorf("sink.nats.stream: %q is not a valid stream name "+
			"(no whitespace, dots, wildcards or slashes)", n.Stream)
	}
	if n.Subject == "" {
		return fmt.Errorf("sink.nats.subject: cannot be empty when sink.type=nats")
	}
	for _, tok := range strings.Split(n.Subject, ".") {
		if !natsToken.MatchString(tok) {
			return fmt.Errorf("sink.nats.subject: %q is not a publishable subject "+
				"(dot-separated tokens, no whitespace, no * or >)", n.Subject)
		}
	}
	// The client library accepts every credential it is given and picks one
	// at connect time. Two of them in one configuration is a mistake nobody
	// intended, so it is refused instead of resolved.
	methods := 0
	for _, set := range []bool{n.Credentials != "", n.Token != "", n.Username != ""} {
		if set {
			methods++
		}
	}
	if methods > 1 {
		return fmt.Errorf("sink.nats: use only one of credentials, token and username/password")
	}
	if n.Password != "" && n.Username == "" {
		return fmt.Errorf("sink.nats.password is set without sink.nats.username")
	}
	if n.TLSCA != "" && !n.TLS {
		return fmt.Errorf("sink.nats.tls_ca is set but sink.nats.tls=false: enable tls or drop the CA")
	}
	return nil
}

func checkSynchronous(name, v string) error {
	switch v {
	case "FULL", "NORMAL", "OFF":
		return nil
	}
	return fmt.Errorf("%s: %q is not one of FULL, NORMAL, OFF", name, v)
}

// ParsePrefixes parses a list of CIDRs, accepting a bare address as a
// full-length prefix so "10.0.0.5" and "10.0.0.5/32" mean the same thing.
// Addresses are unmapped first, so "::ffff:10.0.0.5" and "10.0.0.5" agree.
func ParsePrefixes(list []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(list))
	for _, raw := range list {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if strings.Contains(s, "/") {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR %q: %w", raw, err)
			}
			// Masked() drops host bits so "10.0.0.5/24" behaves as "10.0.0.0/24"
			// instead of never matching.
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid address %q: %w", raw, err)
		}
		a = a.Unmap()
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// isLoopbackBind reports whether a listen address is reachable only from this
// host. An empty host (":8080") binds every interface and is not loopback.
func isLoopbackBind(bind string) bool {
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		host = bind
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return a.Unmap().IsLoopback()
}
