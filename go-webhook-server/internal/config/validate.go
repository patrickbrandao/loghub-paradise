package config

import (
	"fmt"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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

// Ceilings of SPEC §10.1 for the keys that size memory or a count handed to a
// library (go-relplog-pg-server's and go-relplog-nats-server's review, ported
// from the syslog/RELP family on 2026-10-13). Without them a typo of a few
// digits panicked the boot (ring.capacity_items and ring.sqlite.commit_batch:
// makechan; sink.redis.ring_scale_each_conn and the rueidis buffers:
// makeslice), allocated a gigabyte per Dequeue (sink.batch), wrapped around in
// the pool's int32 (sink.postgres.pool_size 4294967300 became 4), or turned
// the NATS MaxPending -- workers x batch + ring.max_inflight -- negative,
// which the sink silently replaced with 4096.
//
// maxRingItems is the memory ring's: only that ring makes a channel of
// capacity_items slots. ring=sqlite counts rows in an int64, so a 50-million-
// row ring.db is legitimate there; it takes maxPersistentRingItems, a sanity
// ceiling against a typo, and so does ring=none, which never reads the key.
// maxSinkBatch is 1 Mi: a ClickHouse batch of 100 000 rows is legitimate, and
// a Dequeue slice of 1 Mi pointers is 8 MiB; MySQL keeps its own, lower,
// ceiling (mysqlMaxBatch). The MySQL and ClickHouse pools take the PostgreSQL
// one: thousands of connections per instance are a typo, never a budget.
const (
	maxRingItems           = 1 << 24 // 16 Mi items: 128 MiB of channel slots alone
	maxPersistentRingItems = 1 << 40
	maxInflight            = 1 << 20
	maxCommitBatch         = 65536
	maxSinkBatch           = 1 << 20
	maxSinkWorkers         = 1024
	maxPoolSize            = 4096
	maxListeners           = 256

	// rueidis: a ring of 2^n slots per connection, each with a channel and
	// two condition variables (about 20 MiB at 16, 300 MiB at 20); 2^(n-1)
	// overflows past 63. PipelineMultiplex above 8 is refused by rueidis
	// itself, at connect time, where it was retried until sink.boot_timeout.
	maxRedisRingScale   = 20
	maxRedisMultiplex   = 8
	maxRedisBufferBytes = 256 << 20
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
	if c.Server.Listeners > maxListeners {
		return nil, fmt.Errorf("server.listeners: %d is above %d", c.Server.Listeners, maxListeners)
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
	if c.Ring.CapacityItems > maxPersistentRingItems {
		return nil, fmt.Errorf("ring.capacity_items: %d is above %d", c.Ring.CapacityItems, maxPersistentRingItems)
	}
	if c.Ring.Mode == RingMemory && c.Ring.CapacityItems > maxRingItems {
		return nil, fmt.Errorf("ring.capacity_items: %d is above %d, the ceiling of ring.mode=memory "+
			"(a channel of that many slots)", c.Ring.CapacityItems, maxRingItems)
	}
	if c.Ring.MaxBytes < 1 {
		return nil, fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return nil, fmt.Errorf("ring.high_water: must be in (0, 1], got %g", c.Ring.HighWater)
	}
	// ring.max_inflight is a semaphore and one term of the NATS
	// PublishAsyncMaxPending, 2 × (workers × batch + max_inflight) (SPEC
	// §8.7.2), which an unbounded value overflowed into a negative number that
	// the sink silently replaced with 4096 (go-syslog-server review,
	// 2026-10-13, C3). 1 Mi writers in flight (maxInflight, with the other
	// ceilings of SPEC §10.1) is far above any budget.
	if c.Ring.MaxInflight < 1 || c.Ring.MaxInflight > maxInflight {
		return nil, fmt.Errorf("ring.max_inflight: %d is outside 1..%d", c.Ring.MaxInflight, maxInflight)
	}
	if c.RingSQL.CommitBatch < 1 {
		return nil, fmt.Errorf("ring.sqlite.commit_batch: must be at least 1")
	}
	if c.RingSQL.CommitBatch > maxCommitBatch {
		return nil, fmt.Errorf("ring.sqlite.commit_batch: %d is above %d", c.RingSQL.CommitBatch, maxCommitBatch)
	}
	if c.Sink.Workers < 1 {
		return nil, fmt.Errorf("sink.workers: must be at least 1")
	}
	if c.Sink.Workers > maxSinkWorkers {
		return nil, fmt.Errorf("sink.workers: %d is above %d", c.Sink.Workers, maxSinkWorkers)
	}
	if c.Sink.Batch < 1 {
		return nil, fmt.Errorf("sink.batch: must be at least 1")
	}
	if c.Sink.Batch > maxSinkBatch {
		return nil, fmt.Errorf("sink.batch: %d is above %d", c.Sink.Batch, maxSinkBatch)
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
	for _, n := range []struct {
		name string
		v    int
	}{
		{"sink.postgres.pool_size", c.SinkPG.PoolSize},
		{"sink.mysql.pool_size", c.SinkMySQL.PoolSize},
		{"sink.clickhouse.pool_size", c.SinkCH.PoolSize},
	} {
		if n.v > maxPoolSize {
			return nil, fmt.Errorf("%s: %d is above %d; 0 derives sink.workers + 1", n.name, n.v, maxPoolSize)
		}
	}
	if r := c.SinkRedis.RingScaleEachConn; r < 0 || r > maxRedisRingScale {
		return nil, fmt.Errorf("sink.redis.ring_scale_each_conn: %d is outside 0..%d (a ring of 2^n "+
			"slots per connection; 0 is the rueidis default)", r, maxRedisRingScale)
	}
	if m := c.SinkRedis.PipelineMultiplex; m < -1 || m > maxRedisMultiplex {
		return nil, fmt.Errorf("sink.redis.pipeline_multiplex: %d is outside -1..%d", m, maxRedisMultiplex)
	}
	for _, b := range []struct {
		name string
		v    int64
	}{
		{"sink.redis.read_buffer_each_conn", c.SinkRedis.ReadBufferEachConn},
		{"sink.redis.write_buffer_each_conn", c.SinkRedis.WriteBufferEachConn},
	} {
		if b.v > maxRedisBufferBytes {
			return nil, fmt.Errorf("%s: %s is above 256MiB", b.name, FormatBytes(b.v))
		}
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
		// rueidis dials every entry as TCP host:port. One without a port, or
		// with a blank, failed in the dial, a network error retried for the
		// whole sink.boot_timeout (ported from the syslog/RELP family,
		// 2026-10-13). The comma is the list's separator: there is no Redis
		// URL here whose host could hold one.
		for _, a := range c.SinkRedis.Addrs {
			host, port, err := net.SplitHostPort(a)
			if err != nil || host == "" || strings.ContainsAny(host, ", \t") || !validPort(port) || !oneHost(host) {
				return nil, fmt.Errorf("sink.redis.addrs: %q is not host:port (one server, port 1..65535)", a)
			}
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
	if err := validatePostgresAddr(pg.Addr); err != nil {
		return err
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

	// One connection per worker, plus one spare. The spare used to be the
	// health check's; the check now has a connection of its own outside the
	// pool (SPEC §8.5.3, §8.5.8), and the derivation stays because a pool
	// opens connections lazily: the spare costs nothing until something uses
	// it. Below the workers they queue for a connection -- throughput, not
	// correctness, so a warning (the check no longer starves: a pool equal to
	// the workers is a legitimate configuration).
	if pg.PoolSize == 0 {
		pg.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && pg.PoolSize < c.Sink.Workers {
		warn("sink.postgres.pool_size=%d is below sink.workers=%d: workers will queue for a "+
			"connection", pg.PoolSize, c.Sink.Workers)
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
	if err := validateDBName("sink.mysql.database", my.Database); err != nil {
		return err
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

	// The postgres rule (validatePostgres): the health check has its own
	// connection (SPEC §8.6.2).
	if my.PoolSize == 0 {
		my.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && my.PoolSize < c.Sink.Workers {
		warn("sink.mysql.pool_size=%d is below sink.workers=%d: workers will queue for a "+
			"connection", my.PoolSize, c.Sink.Workers)
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
	if err := validateDBName("sink.clickhouse.database", ch.Database); err != nil {
		return err
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

	// The postgres rule (validatePostgres): the health check has its own
	// connection (SPEC §8.8.2).
	if ch.PoolSize == 0 {
		ch.PoolSize = c.Sink.Workers + 1
	} else if c.Ring.Mode != RingNone && ch.PoolSize < c.Sink.Workers {
		warn("sink.clickhouse.pool_size=%d is below sink.workers=%d: workers will queue for a "+
			"connection", ch.PoolSize, c.Sink.Workers)
	}
	if c.Ring.Mode == RingNone && c.Ring.MaxInflight > ch.PoolSize {
		warn("ring.mode=none with ring.max_inflight=%d above sink.clickhouse.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's accounting. "+
			"Raise sink.clickhouse.pool_size or lower ring.max_inflight", c.Ring.MaxInflight, ch.PoolSize)
	}
	return nil
}

// maxDBName is the longest database name MySQL accepts, in CHARACTERS (64),
// and the ceiling used for ClickHouse too.
const maxDBName = 64

// validateDBName holds sink.mysql.database and sink.clickhouse.database to
// what can go between backticks (SPEC §8.6.3; ported from the syslog/RELP
// family, 2026-10-13). Both sinks put the name raw inside `...` in the INSERT,
// and the table name is checked precisely so nothing typed in the
// configuration can change a statement: a backtick ends the quoting, a
// backslash is an escape to ClickHouse inside a quoted identifier, and control
// bytes are no name either server would show back. Longer than 64 characters
// either server refuses it anyway, after the first check. Characters, not
// bytes: a legitimate name of 64 accented letters is 128 bytes, and the byte
// count refused it (go-syslog-server review, 2026-10-13). The sinks'
// constructors check the same rule (checkQuotedName) for a caller that
// bypasses this.
func validateDBName(key, v string) error {
	if n := utf8.RuneCountInString(v); n > maxDBName {
		return fmt.Errorf("%s: %d characters is above %d", key, n, maxDBName)
	}
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case c == '`' || c == '\\':
			return fmt.Errorf("%s: %q holds a backtick or a backslash, which would break the "+
				"quoting of the name in every statement", key, v)
		case c < 0x20 || c == 0x7f:
			return fmt.Errorf("%s: %q holds a control byte", key, v)
		}
	}
	return nil
}

// validateNATS holds the [sink.nats] rules of SPEC §8.7 and §10.1.
func validateNATS(c *Config) error {
	n := &c.SinkNATS
	if len(n.URLs) == 0 {
		return fmt.Errorf("sink.nats.urls: at least one URL is required when sink.type=nats")
	}
	// Each URL is parsed, and never quoted in the message (natsurl.go).
	if err := validateNATSURLs(n); err != nil {
		return err
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

// validatePostgresAddr accepts one server, by host:port or by socket
// directory (with an optional :port, which names the socket file:
// /run/pg:5433 is /run/pg/.s.PGSQL.5433). Anything else -- "h1,h2:5432", an
// empty host, port 0, a tab, "[h:5432]:5433" -- passed here and then failed in
// the DNS or the dial, retried as a network error for the whole
// sink.boot_timeout (SPEC §8.5.2; go-syslog-pg-server and
// go-relplog-pg-server, ported 2026-10-13).
func validatePostgresAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	switch {
	case strings.HasPrefix(addr, "/"):
		// A comma would be a second socket directory to libpq ("/run/pg,/x"),
		// which pgx then tries as a list: refused like "h1,h2" below.
		if strings.ContainsAny(addr, ",\t ") {
			return fmt.Errorf("sink.postgres.addr: %q: a socket directory names one server, "+
				"with no comma or blank", addr)
		}
		if err != nil {
			return nil // a bare socket directory: port 5432
		}
		if !validPort(port) {
			return fmt.Errorf("sink.postgres.addr: %q: the port after the socket directory "+
				"must be 1..65535", addr)
		}
	case err != nil || host == "" || strings.ContainsAny(host, ", \t") || !validPort(port) || !oneHost(host):
		return fmt.Errorf("sink.postgres.addr: %q is neither host:port (one server, port "+
			"1..65535) nor a unix socket directory starting with /", addr)
	}
	return nil
}

// oneHost reports whether host, the part before the port, names one server: a
// name or an IP literal. A ':' is only an IPv6 literal's: "[h:5432]:5433"
// passed as host "h:5432", a DNS name no resolver has, retried for the whole
// sink.boot_timeout.
func oneHost(host string) bool {
	if !strings.Contains(host, ":") {
		return true
	}
	_, err := netip.ParseAddr(host)
	return err == nil
}

// validPort is a decimal port 1..65535 written the canonical way: "+80" and
// "080" parse with Atoi and would be dialled as something other than written.
func validPort(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == s
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
