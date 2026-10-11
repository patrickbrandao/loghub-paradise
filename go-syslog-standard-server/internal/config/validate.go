package config

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Defaults and ceilings that only apply to the relational destinations
// (SPEC §10.5.3, §10.7.2, §10.7.3). They live here rather than in the registry
// because the registry holds ONE default per parameter, and these are
// conditional on another parameter's value.
const (
	// dbDefaultWorkers replaces the registry default of sink.workers when the
	// operator did not set it and the destination is postgres or mysql. Every
	// worker holds a connection, and the server's connection budget -- 100 by
	// default in PostgreSQL, 151 in MySQL -- is shared by every instance.
	dbDefaultWorkers = 4

	// dbWarnWorkers is the explicit value above which the boot warns.
	dbWarnWorkers = 16

	// pgParamCeiling is the largest value one protocol parameter can carry. A
	// batch is one parameter per column, so a batch of large messages can in
	// principle reach it (SPEC §10.5.4).
	pgParamCeiling = int64(1) << 30

	// mysqlMaxBatch is the most rows one INSERT can carry: the binary protocol
	// addresses 65535 placeholders per statement, and each row of the table of
	// SPEC §3.7 uses seventeen -- 65535 / 17 is exactly 3855. The sink splits
	// at the same number (sink.mysqlMaxRows); refusing a larger batch here
	// is what lets the operator see the ceiling instead of discovering it.
	mysqlMaxBatch = 3855
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

// natsToken is one dot-separated token of a subject, or a whole stream name: no
// whitespace, no wildcards, no separators. Wildcards are refused because this
// server PUBLISHES to the subject, and a wildcard is not a publishable subject.
var natsToken = regexp.MustCompile(`^[^\s.*>/\\]+$`)

// Validate checks every constraint the server depends on and returns the boot
// warnings of SPEC §12.1. An error here is exit code 1: the process must not
// start.
//
// setIn says which parameters were written by the file, the environment or the
// command line, as opposed to carrying their registry default. It is consulted
// only where a default is conditional on another parameter; nil means
// "everything is a default".
func Validate(c *Config, setIn map[string]bool) (warnings []string, err error) {
	warn := func(format string, a ...any) { warnings = append(warnings, fmt.Sprintf(format, a...)) }
	if setIn == nil {
		setIn = map[string]bool{}
	}

	if err := validateEnums(c); err != nil {
		return nil, err
	}
	if err := validateListeners(c); err != nil {
		return nil, err
	}
	if err := validateRanges(c); err != nil {
		return nil, err
	}
	if err := validateParseAndFilter(c); err != nil {
		return nil, err
	}
	if err := validatePaths(c); err != nil {
		return nil, err
	}
	if err := validateSinkTLS(c); err != nil {
		return nil, err
	}
	switch c.Sink.Type {
	case SinkMySQLType:
		if err := validateMySQL(c); err != nil {
			return nil, err
		}
	case SinkNATSType:
		if err := validateNATS(c); err != nil {
			return nil, err
		}
	}

	validateStorageWarnings(c, warn)
	validateExposure(c, warn)

	if err := validateSinkWorkers(c, setIn, warn); err != nil {
		return nil, err
	}
	return warnings, nil
}

func validateEnums(c *Config) error {
	switch c.Ring.Mode {
	case RingMemory, RingSQLiteMode, RingNone:
	default:
		return fmt.Errorf("ring.mode: %q is not one of memory, sqlite, none", c.Ring.Mode)
	}
	switch c.Sink.Type {
	case SinkRedisType, SinkSQLiteType, SinkPostgresType, SinkMySQLType, SinkNATSType, SinkNoneType:
	default:
		return fmt.Errorf("sink.type: %q is not one of redis, sqlite, postgres, mysql, nats, none", c.Sink.Type)
	}
	switch c.TCP.Framing {
	case FramingAuto, FramingOctet, FramingLF:
	default:
		return fmt.Errorf("tcp.framing: %q is not one of auto, octet, lf", c.TCP.Framing)
	}
	switch c.Parse.Prefer {
	case PreferAuto, Prefer5424, Prefer3164:
	default:
		return fmt.Errorf("parse.prefer: %q is not one of auto, 5424, 3164", c.Parse.Prefer)
	}
	if err := checkSynchronous("ring.sqlite.synchronous", c.RingSQL.Synchronous); err != nil {
		return err
	}
	if err := checkSynchronous("sink.sqlite.synchronous", c.SinkSQL.Synchronous); err != nil {
		return err
	}
	switch c.SinkSQL.Rotate {
	case "none", "hourly", "daily":
	default:
		return fmt.Errorf("sink.sqlite.rotate: %q is not one of none, hourly, daily", c.SinkSQL.Rotate)
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level: %q is not one of debug, info, warn, error", c.Log.Level)
	}
	switch c.Log.Format {
	case "json", "text":
	default:
		return fmt.Errorf("log.format: %q is not one of json, text", c.Log.Format)
	}
	return nil
}

// validateListeners enforces the transport rules of SPEC §12.1.
func validateListeners(c *Config) error {
	// A server listening to nothing has no function, and would sit in a
	// crashloop-free idle that looks healthy on every probe.
	if !c.AnyListener() {
		return fmt.Errorf("no transport configured: set at least one of " +
			"listen.udp, listen.tcp, listen.relp or listen.unix")
	}

	type entry struct{ name, addr string }
	net4 := []entry{
		{"listen.udp", c.Listen.UDP},
		{"listen.tcp", c.Listen.TCP},
		{"listen.relp", c.Listen.RELP},
	}
	for _, e := range net4 {
		if e.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(e.addr); err != nil {
			return fmt.Errorf("%s: %q is not host:port (a bare \":1514\" is fine)", e.name, e.addr)
		}
	}

	// UDP and TCP may share a port -- they are different protocols on
	// different sockets, and 514 on both is the classic deployment. TCP and
	// RELP may not: RELP speaks its own framing, and two readers on one
	// stream socket cannot both be right.
	if c.Listen.TCP != "" && c.Listen.TCP == c.Listen.RELP {
		return fmt.Errorf("listen.tcp and listen.relp are both %q: they are different "+
			"protocols and cannot share one stream socket", c.Listen.TCP)
	}

	for _, n := range []struct {
		name string
		v    int
	}{
		{"listen.udp_listeners", c.Listen.UDPListeners},
		{"listen.tcp_listeners", c.Listen.TCPListeners},
		{"listen.relp_listeners", c.Listen.RELPListeners},
	} {
		if n.v < 1 {
			return fmt.Errorf("%s: %d is below 1", n.name, n.v)
		}
	}
	if c.Listen.Unix != "" && !filepath.IsAbs(c.Listen.Unix) {
		return fmt.Errorf("listen.unix: %q must be an absolute path", c.Listen.Unix)
	}
	// A unix socket path lives in a fixed-size sun_path field. Past it, the
	// kernel answers bind with a bare "invalid argument" that names nothing, and
	// the operator is left guessing at permissions. Checking the length here
	// turns that into an error that says what is wrong.
	if max := unixPathMax(); len(c.Listen.Unix) > max {
		return fmt.Errorf("listen.unix: %q is %d bytes, above the %d this platform allows "+
			"for a socket path (sun_path); choose a shorter path", c.Listen.Unix,
			len(c.Listen.Unix), max)
	}
	if c.Listen.UnixMode == 0 && c.Listen.Unix != "" {
		return fmt.Errorf("listen.unix_mode: 0 makes the socket unwritable by anyone, " +
			"which silently disables the transport")
	}
	return nil
}

func validateRanges(c *Config) error {
	if c.Ring.CapacityItems < 1 {
		return fmt.Errorf("ring.capacity_items: %d is below 1", c.Ring.CapacityItems)
	}
	if c.Ring.MaxBytes < 1 {
		return fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return fmt.Errorf("ring.high_water: %g is outside (0, 1]", c.Ring.HighWater)
	}
	if c.Ring.MaxInflight < 1 {
		return fmt.Errorf("ring.max_inflight: %d is below 1", c.Ring.MaxInflight)
	}
	if c.Sink.Workers < 1 {
		return fmt.Errorf("sink.workers: %d is below 1", c.Sink.Workers)
	}
	if c.Sink.Batch < 1 {
		return fmt.Errorf("sink.batch: %d is below 1", c.Sink.Batch)
	}
	if c.Sink.MaxRetries < 1 {
		return fmt.Errorf("sink.max_retries: %d is below 1", c.Sink.MaxRetries)
	}
	if c.RingSQL.CommitBatch < 1 {
		return fmt.Errorf("ring.sqlite.commit_batch: %d is below 1", c.RingSQL.CommitBatch)
	}
	// Zero is not "unlimited" for these. A zero op_timeout fails every write
	// before it starts and is retried forever without ever turning fatal; a
	// zero drain_timeout turns every shutdown into an emergency dump and exit
	// 3; a zero backpressure_timeout closes every held connection at the first
	// poll, which is the transport's whole reason to exist switched off; a
	// zero health cadence, busy timeout or backoff floor used to be silently
	// replaced by a default that --dump-config did not show. Refusing here is
	// what keeps the dump and the running process telling the same story
	// (SPEC §15.5).
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
		{"tcp.backpressure_timeout", c.TCP.BackpressureTimeout},
		{"relp.backpressure_timeout", c.RELP.BackpressureTimeout},
	} {
		if d.v <= 0 {
			return fmt.Errorf("%s: must be positive, got %s", d.name, d.v)
		}
	}
	if c.Sink.RetryBackoffMax < c.Sink.RetryBackoffMin {
		return fmt.Errorf("sink.retry_backoff_max (%s) is below sink.retry_backoff_min (%s)",
			c.Sink.RetryBackoffMax, c.Sink.RetryBackoffMin)
	}
	if c.TCP.MaxFrameBytes < 1024 {
		return fmt.Errorf("tcp.max_frame_bytes: %s is below 1KiB, which would reject "+
			"ordinary messages", FormatBytes(c.TCP.MaxFrameBytes))
	}
	if c.RELP.MaxFrameBytes < 1024 {
		return fmt.Errorf("relp.max_frame_bytes: %s is below 1KiB", FormatBytes(c.RELP.MaxFrameBytes))
	}
	if c.RELP.Window < 1 {
		return fmt.Errorf("relp.window: %d is below 1", c.RELP.Window)
	}
	if c.RateLimit.V4Mask < 0 || c.RateLimit.V4Mask > 32 {
		return fmt.Errorf("ratelimit.v4_mask: %d is outside 0..32", c.RateLimit.V4Mask)
	}
	if c.RateLimit.V6Mask < 0 || c.RateLimit.V6Mask > 128 {
		return fmt.Errorf("ratelimit.v6_mask: %d is outside 0..128", c.RateLimit.V6Mask)
	}
	if c.RateLimit.Enabled {
		if c.RateLimit.Rate <= 0 {
			return fmt.Errorf("ratelimit.rate: %g must be positive when the limiter is on", c.RateLimit.Rate)
		}
		if c.RateLimit.Burst < 1 {
			return fmt.Errorf("ratelimit.burst: %g is below 1", c.RateLimit.Burst)
		}
	}
	if c.Log.AccessSample < 0 || c.Log.AccessSample > 1 {
		return fmt.Errorf("log.access_sample: %g is outside [0, 1]", c.Log.AccessSample)
	}
	if _, err := ParsePrefixes(c.RateLimit.Exempt); err != nil {
		return fmt.Errorf("ratelimit.exempt: %w", err)
	}
	return nil
}

// validateParseAndFilter rejects a facility or severity the parser could never
// produce. Left unchecked, parse.default_severity = 9 would write events no
// filter can match and no severity index can find.
func validateParseAndFilter(c *Config) error {
	if c.Parse.DefaultFacility < 0 || c.Parse.DefaultFacility > 23 {
		return fmt.Errorf("parse.default_facility: %d is outside 0..23", c.Parse.DefaultFacility)
	}
	if c.Parse.DefaultSeverity < 0 || c.Parse.DefaultSeverity > 7 {
		return fmt.Errorf("parse.default_severity: %d is outside 0..7", c.Parse.DefaultSeverity)
	}
	if c.Parse.SDMaxElements < 1 {
		return fmt.Errorf("parse.sd_max_elements: %d is below 1", c.Parse.SDMaxElements)
	}
	if c.Parse.SDMaxParams < 1 {
		return fmt.Errorf("parse.sd_max_params: %d is below 1", c.Parse.SDMaxParams)
	}
	if _, ok := syslog.ParseSeverity(c.Filter.MinSeverity); !ok {
		return fmt.Errorf("filter.min_severity: %q is not a severity name or a number 0..7",
			c.Filter.MinSeverity)
	}
	for _, f := range c.Filter.Facilities {
		if _, ok := syslog.ParseFacility(strings.ToLower(strings.TrimSpace(f))); !ok {
			return fmt.Errorf("filter.facilities: %q is not a facility name or a number 0..23", f)
		}
	}
	return nil
}

// validatePaths enforces the one rule that outlives every other: the ring queue
// and the events database are different things and may never be the same file.
func validatePaths(c *Config) error {
	if c.Ring.Mode != RingSQLiteMode || c.Sink.Type != SinkSQLiteType {
		return nil
	}
	ring, err := filepath.Abs(c.RingSQL.Path)
	if err != nil {
		return fmt.Errorf("ring.sqlite.path: %w", err)
	}
	sink, err := filepath.Abs(c.SinkSQL.Path)
	if err != nil {
		return fmt.Errorf("sink.sqlite.path: %w", err)
	}
	if ring == sink {
		return fmt.Errorf("ring.sqlite.path and sink.sqlite.path are both %q: the ring is a "+
			"QUEUE whose rows are deleted after delivery, the sink is an append-only "+
			"DESTINATION. One file cannot be both", ring)
	}
	return nil
}

// validateSinkTLS covers outbound TLS only. TLS on the ingestion ports is out
// of scope for v1 (SPEC §1.2); these are clients of someone else's
// infrastructure, where no reverse proxy helps.
func validateSinkTLS(c *Config) error {
	if c.SinkRedis.TLSCA != "" && !c.SinkRedis.TLS {
		return fmt.Errorf("sink.redis.tls_ca is set but sink.redis.tls is false: the CA " +
			"would be ignored and the connection would be plaintext")
	}
	if c.SinkPG.TLSCA != "" && !c.SinkPG.TLS {
		return fmt.Errorf("sink.postgres.tls_ca is set but sink.postgres.tls is false")
	}
	if c.SinkMySQL.TLSCA != "" && !c.SinkMySQL.TLS {
		return fmt.Errorf("sink.mysql.tls_ca is set but sink.mysql.tls is false")
	}
	if c.SinkNATS.TLSCA != "" && !c.SinkNATS.TLS {
		return fmt.Errorf("sink.nats.tls_ca is set but sink.nats.tls is false")
	}
	if c.Sink.Type == SinkPostgresType && !pgIdent.MatchString(c.SinkPG.Table) {
		return fmt.Errorf("sink.postgres.table: %q must be a lowercase identifier, "+
			"optionally schema-qualified", c.SinkPG.Table)
	}
	return nil
}

// validateMySQL holds the [sink.mysql] rules of SPEC §10.7 and §12.1. They are
// the postgres rules with two differences that come from the protocol: the
// table name keeps its case, and a batch is capped by the placeholder count.
func validateMySQL(c *Config) error {
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
			"(name or database.name; letters, digits and _; up to 64 characters each)", my.Table)
	}
	if my.TLS && strings.HasPrefix(my.Addr, "/") {
		return fmt.Errorf("sink.mysql.tls=true with a unix socket address (%s): TLS is a TCP feature", my.Addr)
	}
	if c.Sink.Batch > mysqlMaxBatch {
		return fmt.Errorf("sink.batch: %d exceeds %d, the most rows one MySQL INSERT can carry "+
			"(65535 placeholders, 17 per row)", c.Sink.Batch, mysqlMaxBatch)
	}
	return nil
}

// validateNATS holds the [sink.nats] rules of SPEC §10.8.9 and §12.1.
func validateNATS(c *Config) error {
	n := &c.SinkNATS
	if len(n.URLs) == 0 {
		return fmt.Errorf("sink.nats.urls: at least one URL is required when sink.type=nats")
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
	// The client library accepts every credential it is given and picks one at
	// connect time. Two of them in one configuration is a mistake nobody
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
	return nil
}

func validateStorageWarnings(c *Config, warn func(string, ...any)) {
	if c.Ring.Mode == RingSQLiteMode && c.Sink.Type == SinkSQLiteType {
		warn("ring.mode=sqlite with sink.type=sqlite: every message pays TWO fsyncs before " +
			"delivery. ring.mode=memory with sink.type=sqlite gives the same destination " +
			"durability at half the cost, unless the two files are on different disks")
	}

	// Paying for durability on a transport that cannot offer it. A UDP
	// datagram may already have been dropped by the sender's kernel, by a
	// switch, or by our own SO_RCVBUF; an fsync only guarantees what did
	// arrive, at the cost of the throughput that would have let more of it
	// arrive (SPEC §9.4).
	if c.Ring.Mode == RingSQLiteMode && c.Listen.TCP == "" && c.Listen.RELP == "" {
		warn("ring.mode=sqlite with only datagram transports configured: the fsync buys " +
			"durability for messages that the network may already have dropped without " +
			"telling anyone. Durability end to end needs listen.relp, or at least listen.tcp")
	}

	if c.Emergency.Enabled && c.Ring.Mode != RingMemory {
		warn("emergency.enabled has no effect with ring.mode=" + c.Ring.Mode +
			": only a memory ring has anything to dump")
	}
}

func validateExposure(c *Config, warn func(string, ...any)) {
	if c.RateLimit.Enabled {
		return
	}
	var exposed []string
	for _, e := range []struct{ name, addr string }{
		{"listen.udp", c.Listen.UDP},
		{"listen.tcp", c.Listen.TCP},
		{"listen.relp", c.Listen.RELP},
	} {
		if e.addr != "" && !isLoopbackBind(e.addr) {
			exposed = append(exposed, e.name+"="+e.addr)
		}
	}
	if len(exposed) > 0 {
		warn("ratelimit.enabled=false with %s reachable off this host: the service accepts "+
			"messages at whatever rate they arrive. The limiter is off by default because "+
			"one keyed on the wrong source is worse than none, not because it is unneeded",
			strings.Join(exposed, ", "))
	}
}

func validateSinkWorkers(c *Config, setIn map[string]bool, warn func(string, ...any)) error {
	switch c.Sink.Type {
	case SinkSQLiteType:
		// SQLite serialises writers by construction; more than one would only
		// produce SQLITE_BUSY and a slower server (SPEC §10.4).
		if c.Sink.Workers > 1 {
			warn("sink.type=sqlite with sink.workers=%d: forced to 1, because SQLite "+
				"serialises writers and the extra workers would only collect SQLITE_BUSY",
				c.Sink.Workers)
			c.Sink.Workers = 1
		}

	case SinkMySQLType:
		return validateDBWorkers(c, "mysql", &c.SinkMySQL.PoolSize, setIn, warn)

	case SinkPostgresType:
		if err := validateDBWorkers(c, "postgres", &c.SinkPG.PoolSize, setIn, warn); err != nil {
			return err
		}

		// One batch is one parameter per column, and a parameter tops out at
		// 1 GiB (SPEC §10.5.4). Syslog messages never come close, so this
		// fires only for an operator who raised the frame ceiling far past
		// anything a real emitter sends.
		worst := c.TCP.MaxFrameBytes
		if c.Ring.Mode != RingNone {
			worst = c.TCP.MaxFrameBytes * int64(c.Sink.Batch)
			if c.Ring.MaxBytes < worst {
				worst = c.Ring.MaxBytes
			}
		}
		if worst > pgParamCeiling {
			warn("sink.type=postgres: one batch could carry up to %s, above the 1 GiB ceiling "+
				"of a single protocol parameter; lower sink.batch, tcp.max_frame_bytes or "+
				"ring.max_bytes", FormatBytes(worst))
		}
	}
	return nil
}

// validateDBWorkers applies the connection budget of SPEC §10.5.3, which
// postgres and mysql share (§10.7.2): every worker pins a connection out of a
// budget the whole fleet shares, so the default worker count is lowered, the
// pool is derived from it, and a pool smaller than the workers is refused.
func validateDBWorkers(c *Config, sinkType string, poolSize *int, setIn map[string]bool,
	warn func(string, ...any)) error {
	if !setIn["sink.workers"] {
		warn("sink.type=%s: sink.workers lowered from %d to %d, because each worker "+
			"holds a pool connection and the server's connection budget is shared with "+
			"every other instance. Set sink.workers explicitly to override",
			sinkType, c.Sink.Workers, dbDefaultWorkers)
		c.Sink.Workers = dbDefaultWorkers
	} else if c.Sink.Workers > dbWarnWorkers {
		warn("sink.type=%s with sink.workers=%d: above %d, a larger pool trades throughput "+
			"for lock contention inside the database", sinkType, c.Sink.Workers, dbWarnWorkers)
	}
	if *poolSize == 0 {
		*poolSize = c.Sink.Workers + 1
	}
	if *poolSize < c.Sink.Workers {
		return fmt.Errorf("sink.%s.pool_size=%d is below sink.workers=%d: a worker would "+
			"block waiting for a connection another worker holds",
			sinkType, *poolSize, c.Sink.Workers)
	}
	// With ring=none the receiver writes through, and the semaphore is the only
	// accounting there is. Writers beyond the pool queue INSIDE the pool, which
	// has no accounting at all -- exactly what the semaphore exists to prevent
	// (SPEC §9.5).
	if c.Ring.Mode == RingNone && c.Ring.MaxInflight > *poolSize {
		warn("ring.mode=none with ring.max_inflight=%d above sink.%s.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's "+
			"accounting. Raise the pool or lower max_inflight",
			c.Ring.MaxInflight, sinkType, *poolSize)
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
			// Masked() drops host bits so "10.0.0.5/24" behaves as
			// "10.0.0.0/24" instead of never matching.
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
// host. An empty host (":1514") binds every interface and is not loopback.
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

// unixPathMax is the longest socket path the platform accepts, leaving room for
// the terminating NUL: sun_path is 108 bytes on Linux and 104 on the BSDs,
// macOS included.
func unixPathMax() int {
	if runtime.GOOS == "linux" {
		return 107
	}
	return 103
}
