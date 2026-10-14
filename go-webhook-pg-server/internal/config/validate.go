package config

import (
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
)

const (
	// warnWorkers is the sink.workers value above which the boot warns: every
	// worker pins a connection, and the server's connection budget (100 by
	// default) is shared by every instance (SPEC §8.5.3).
	warnWorkers = 16

	// pgParamCeiling is the largest value one protocol parameter can carry.
	// A batch is a single parameter per column, so a batch of large bodies can
	// reach it (SPEC §8.5.4).
	pgParamCeiling = int64(1) << 30
)

// Ceilings of SPEC §10.1 for the keys that size memory or a count handed to a
// library (go-relplog-pg-server's and go-relplog-nats-server's review, ported
// from the syslog/RELP family on 2026-10-13). Without them a typo of a few
// digits panicked the boot (ring.capacity_items: makechan), allocated a
// gigabyte per Dequeue (sink.batch) or wrapped around in the pool's int32
// (sink.postgres.pool_size 4294967300 became 4). maxSinkBatch is 1 Mi, the
// family's (go-webhook-fast-server takes ClickHouse batches of 100 000 rows):
// a Dequeue slice of 1 Mi pointers is 8 MiB.
const (
	maxRingItems   = 1 << 24 // 16 Mi items: 128 MiB of channel slots alone
	maxSinkBatch   = 1 << 20
	maxSinkWorkers = 1024
	maxPoolSize    = 4096
	maxListeners   = 256
)

// pgIdent is what sink.postgres.table must look like: a plain lowercase
// identifier, optionally schema-qualified. Lowercase only, so the name can be
// handed to to_regclass unquoted and mean the same table the INSERT names
// quoted; uppercase would fold in one place and not the other.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// Validate checks every constraint the server depends on and returns the boot
// warnings from SPEC §10.1. An error here is exit code 1: the process must not
// start.
func Validate(c *Config) (warnings []string, err error) {
	warn := func(format string, a ...any) { warnings = append(warnings, fmt.Sprintf(format, a...)) }

	// --- compatibility selectors (SPEC §0) ----------------------------------
	// Each one still loads with the single value this server implements, so a
	// go-webhook-fast-server configuration for memory -> postgres starts as is.
	// Any other value asks for a feature that is not here, and starting
	// without it would be the silent surprise these checks exist to prevent.
	if c.Ring.Mode != RingMemory {
		return nil, fmt.Errorf("ring.mode: %q is not available: go-webhook-pg-server has only the "+
			"memory ring (SPEC §0); use go-webhook-fast-server for sqlite or none", c.Ring.Mode)
	}
	if c.Sink.Type != SinkPostgresType {
		return nil, fmt.Errorf("sink.type: %q is not available: go-webhook-pg-server writes only to "+
			"PostgreSQL (SPEC §0); use go-webhook-fast-server for other destinations", c.Sink.Type)
	}
	if c.RateLimit.Enabled {
		return nil, fmt.Errorf("ratelimit.enabled=true: go-webhook-pg-server has no rate limiter " +
			"(SPEC §6); limit at the reverse proxy and set it to false or remove it")
	}

	// --- enums --------------------------------------------------------------
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
	if c.Ring.CapacityItems > maxRingItems {
		return nil, fmt.Errorf("ring.capacity_items: %d is above %d, the ceiling of the memory ring "+
			"(a channel of that many slots)", c.Ring.CapacityItems, maxRingItems)
	}
	if c.Ring.MaxBytes < 1 {
		return nil, fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return nil, fmt.Errorf("ring.high_water: must be in (0, 1], got %g", c.Ring.HighWater)
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
	// 3; a zero health cadence or backoff floor used to be silently replaced by
	// a default that --dump-config did not show. Refusing here is what keeps
	// the dump and the running process telling the same story (SPEC §13.5).
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
	if c.SinkPG.PoolSize < 0 {
		return nil, fmt.Errorf("sink.postgres.pool_size cannot be negative")
	}
	if c.SinkPG.PoolSize > maxPoolSize {
		return nil, fmt.Errorf("sink.postgres.pool_size: %d is above %d; 0 derives sink.workers + 1",
			c.SinkPG.PoolSize, maxPoolSize)
	}
	if c.Log.AccessSample < 0 || c.Log.AccessSample > 1 {
		return nil, fmt.Errorf("log.access_sample: must be in [0, 1], got %g", c.Log.AccessSample)
	}

	// --- CIDR lists ---------------------------------------------------------
	if _, err := ParsePrefixes(c.Proxy.Trusted); err != nil {
		return nil, fmt.Errorf("proxy.trusted: %w", err)
	}
	if c.Proxy.MaxXFFEntries < 1 {
		return nil, fmt.Errorf("proxy.max_xff_entries: must be at least 1")
	}

	// --- the destination ----------------------------------------------------
	if err := validatePostgres(c, warn); err != nil {
		return nil, err
	}

	// --- exposure warnings --------------------------------------------------
	if !isLoopbackBind(c.Admin.Bind) {
		warn("admin.bind=%q is not loopback: /metrics and /readyz are reachable off-host", c.Admin.Bind)
	}
	if !c.Emergency.Enabled {
		warn("emergency.enabled=false: a fatal sink failure or a drain timeout will drop " +
			"the whole ring with no chance of recovery")
	}

	return warnings, nil
}

// validatePostgres holds the [sink.postgres] rules of SPEC §8.5 and §10.1. It
// may derive sink.postgres.pool_size, which is why it runs before anything
// that reads it.
func validatePostgres(c *Config, warn func(string, ...any)) error {
	pg := &c.SinkPG
	if pg.Addr == "" {
		return fmt.Errorf("sink.postgres.addr: cannot be empty")
	}
	if err := validatePostgresAddr(pg.Addr); err != nil {
		return err
	}
	if pg.Username == "" {
		return fmt.Errorf("sink.postgres.username: cannot be empty")
	}
	if pg.Database == "" {
		return fmt.Errorf("sink.postgres.database: cannot be empty")
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

	// Each worker pins a connection for the life of the process, and the
	// budget is the SERVER's, shared by every instance (SPEC §8.5.3). The
	// registry default (4) already accounts for that; a larger explicit value
	// is the operator's call, worth one line in the boot log.
	if c.Sink.Workers > warnWorkers {
		warn("sink.workers=%d: each worker holds a PostgreSQL connection, so N instances need "+
			"N x %d of the server's max_connections", c.Sink.Workers, c.Sink.Workers)
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
	} else if pg.PoolSize < c.Sink.Workers {
		warn("sink.postgres.pool_size=%d is below sink.workers=%d: workers will queue for a "+
			"connection", pg.PoolSize, c.Sink.Workers)
	}

	// A batch is one parameter per column, and one parameter tops out at
	// 1 GiB (SPEC §8.5.4).
	if worst := WorstBatchBytes(c); worst > pgParamCeiling {
		warn("one batch can carry up to %s of envelopes, above the 1 GiB ceiling of a single "+
			"protocol parameter; lower sink.batch, server.max_body_bytes or ring.max_bytes",
			FormatBytes(worst))
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

// WorstBatchBytes is the most envelope bytes one sink write can carry (SPEC
// §8.5.4): sink.batch envelopes, but never more than the ring may hold. The
// count-only batch limit cannot see bytes, and the ring's byte ceiling is what
// bounds them in practice, so the smaller of the two worst cases is used
// rather than one that always fires.
func WorstBatchBytes(c *Config) int64 {
	worst := MaxEventBytes(c) * int64(c.Sink.Batch)
	if c.Ring.MaxBytes < worst {
		worst = c.Ring.MaxBytes
	}
	return worst
}

// MaxEventBytes is the largest envelope one accepted request can produce
// (SPEC §3.2): the ceiling of one line of the emergency dump (SPEC §9.2).
func MaxEventBytes(c *Config) int64 {
	return envelope.MaxEventBytes(c.Server.MaxBodyBytes, c.Server.MaxHeaderBytes, c.Server.MaxURIBytes)
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
