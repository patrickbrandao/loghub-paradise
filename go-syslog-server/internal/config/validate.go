package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// Defaults and ceilings that only apply to the relational destinations
// (SPEC §10.5, §10.6). They live here rather than in the registry because the
// registry holds ONE default per parameter, and these are conditional on
// another parameter's value.
const (
	// dbDefaultWorkers replaces the registry default of sink.workers when the
	// operator did not set it and the destination is postgres or mysql. Every
	// worker holds a connection, and the server's connection budget -- 100 by
	// default in PostgreSQL, 151 in MySQL -- is shared by every instance.
	dbDefaultWorkers = 4

	// dbWarnWorkers is the explicit value above which the boot warns.
	dbWarnWorkers = 16

	// mysqlColumns is the column count of the table of SPEC §3.7.
	mysqlColumns = 18

	// mysqlMaxBatch is the most rows one INSERT can carry: the binary protocol
	// addresses 65535 placeholders per statement, and each row uses eighteen
	// -- 65535 / 18 is 3640. The sink splits at the same number; refusing a
	// larger batch here is what lets the operator see the ceiling instead of
	// discovering it (SPEC §10.6).
	mysqlMaxBatch = 65535 / mysqlColumns
)

// Ceilings of SPEC §13.2 for the keys that size memory or a count handed to a
// library (independent review of go-relplog-pg-server and
// go-relplog-nats-server, 2026-10-13, ported). Without them a typo of a few
// digits panicked the boot (ring.capacity_items and ring.sqlite.commit_batch:
// makechan), allocated a gigabyte per Dequeue (sink.batch), wrapped around in
// the pool's int32 (sink.postgres.pool_size 4294967300 became 4) or built a
// chunk whose Bind passes libpq's 1 GiB message limit and fails with no
// SQLSTATE -- a network error retried forever (sink.batch_max_bytes). The
// MySQL and ClickHouse pools take the PostgreSQL ceiling: a pool of thousands
// of connections per instance is a typo, never a budget.
//
// maxRingItems is the memory ring's: only that ring makes a channel of
// capacity_items slots. ring=sqlite counts rows in an int64 and ring=redis
// hands the number to a Lua script that compares doubles, so a 50-million-row
// ring.db is a legitimate configuration there and was refused (independent
// review of the port, 2026-10-13); they take maxPersistentRingItems, a
// sanity ceiling against a typo. maxSinkBatch is 1 Mi for the same reason: a
// ClickHouse batch of 100 000 rows is a legitimate configuration; a Dequeue
// slice of 1 Mi pointers is 8 MiB, not the gigabyte of an unbounded typo.
// MySQL keeps its own, lower, ceiling (mysqlMaxBatch). maxInflight bounds
// ring.max_inflight: it is a semaphore and one term of the NATS MaxPending,
// 2*(workers*batch + max_inflight), which an unbounded value overflowed into
// a negative number that NewNATS silently replaced with 4096.
const (
	maxRingItems           = 1 << 24 // 16 Mi items: 128 MiB of channel slots alone
	maxPersistentRingItems = 1 << 40
	maxInflight            = 1 << 20
	maxCommitBatch         = 65536
	maxSinkBatch           = 1 << 20
	maxBatchMaxBytes       = 256 << 20
	maxSinkWorkers         = 1024
	maxPoolSize            = 4096
	maxListeners           = 256
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
// server PUBLISHES, and a wildcard is not a publishable subject.
var natsToken = regexp.MustCompile(`^[^\s.*>/\\]+$`)

// Validate checks every configuration-only constraint of SPEC §13.2, §9.7 and
// §16.5 and returns the WARN-level findings. An error is exit code 1: the
// process must not start. Validate may ADJUST c where the SPEC says a value is
// forced or degraded (sink.workers, pool sizes, ring.mode); Load records why
// in Loaded.Notes.
//
// setIn says which parameters were written by the file, the environment or the
// command line, as opposed to carrying their registry default. It is consulted
// only where a default is conditional on another parameter; nil means
// "everything is a default".
//
// The GOMEMLIMIT row is checked by Load, which owns the environment.
func Validate(c *Config, setIn map[string]bool) (warnings []string, err error) {
	warnings, _, err = validate(c, setIn)
	return warnings, err
}

// validator collects warnings and the notes that explain adjustments.
type validator struct {
	c        *Config
	setIn    map[string]bool
	warnings []string
	notes    map[string]string
}

func (v *validator) warn(format string, a ...any) {
	v.warnings = append(v.warnings, fmt.Sprintf(format, a...))
}

// outsideDataDir warns about the files this server writes outside
// server.data_dir (SPEC §20.16). They are allowed -- an operator may put the
// ring on a faster disk -- but a read-only root with one writable volume, the
// shape of the container, then fails at boot, and the warning says why before
// it does.
func (v *validator) outsideDataDir() {
	c := v.c
	under := func(p string) bool {
		rel, err := filepath.Rel(c.Server.DataDir, p)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	for _, x := range []struct {
		name, path string
		used       bool
	}{
		{"ring.sqlite.path", c.RingSQL.Path, c.Ring.Mode == RingModeSQLite},
		{"sink.sqlite.path", c.SinkSQL.Path, c.Sink.Type == SinkTypeSQLite},
		{"emergency.dir", c.Emergency.Dir, c.Emergency.Enabled},
	} {
		if x.used && x.path != "" && !under(x.path) {
			v.warn("%s=%q is outside server.data_dir (%q): everything else this server writes "+
				"lives under data_dir, and a container with a read-only root and one volume "+
				"will refuse this path (SPEC §20.16)", x.name, x.path, c.Server.DataDir)
		}
	}
}

func validate(c *Config, setIn map[string]bool) ([]string, map[string]string, error) {
	if setIn == nil {
		setIn = map[string]bool{}
	}
	v := &validator{c: c, setIn: setIn, notes: map[string]string{}}

	for _, check := range []func(*Config) error{
		validateListeners,
		validateTLSAndProxy,
		validateSyslog,
		validateFilter,
		validateZeroDurations, // before the ordering of backoff min/max
		validateRanges,
		validatePaths,
		validateClientTLS,
		validateRedis,
		validateSQL,
		validateClickHouse,
		validateNATS,
	} {
		if err := check(c); err != nil {
			return nil, nil, err
		}
	}

	v.ringSinkCombinations()
	if err := v.memoryRingItems(); err != nil {
		return nil, nil, err
	}
	if err := v.ringBytesFloor(); err != nil {
		return nil, nil, err
	}
	v.outsideDataDir()
	v.exposure()
	v.natsMode()
	if err := v.sinkWorkers(); err != nil {
		return nil, nil, err
	}
	return v.warnings, v.notes, nil
}

// validateListeners enforces the transport rules of SPEC §4 and §13.2.
func validateListeners(c *Config) error {
	// A server listening to nothing has no function, and would sit in a
	// crashloop-free idle that looks healthy on every probe.
	if !c.AnyListener() {
		return fmt.Errorf("no transport configured: set at least one of " +
			"listen.udp, listen.tcp, listen.tls or listen.unix")
	}

	for _, e := range []struct{ name, addr string }{
		{"listen.udp", c.Listen.UDP},
		{"listen.tcp", c.Listen.TCP},
		{"listen.tls", c.Listen.TLS},
	} {
		if e.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(e.addr); err != nil {
			return fmt.Errorf("%s: %q is not host:port (a bare \":1514\" is fine)", e.name, e.addr)
		}
	}

	// UDP and TCP may share a port -- they are different protocols on
	// different sockets, and 1514 on both is the default. TCP and TLS may not:
	// both are stream sockets, and with SO_REUSEPORT (listeners > 1) the kernel
	// would even accept the double bind and hand plaintext connections to the
	// TLS reader and the other way round. A wildcard host overlaps every
	// concrete host on the same port.
	if c.Listen.TCP != "" && c.Listen.TLS != "" && sameStreamSocket(c.Listen.TCP, c.Listen.TLS) {
		return fmt.Errorf("listen.tcp (%q) and listen.tls (%q) are, or may resolve to, the same TCP "+
			"address: they are different protocols and cannot share one stream socket (to share a "+
			"port, use two distinct IP literals)", c.Listen.TCP, c.Listen.TLS)
	}

	for _, n := range []struct {
		name string
		v    int
	}{
		{"listen.udp_listeners", c.Listen.UDPListeners},
		{"listen.tcp_listeners", c.Listen.TCPListeners},
		{"listen.tls_listeners", c.Listen.TLSListeners},
	} {
		if n.v < 1 || n.v > maxListeners {
			return fmt.Errorf("%s: %d is outside 1..%d", n.name, n.v, maxListeners)
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
	// An empty admin.bind would make net.Listen bind every interface on a
	// random port, publishing /metrics wherever the host is reachable: the
	// opposite of the "empty disables" of listen.*. admin.enabled=false is how
	// the admin port is turned off.
	if c.Admin.Enabled {
		if c.Admin.Bind == "" {
			return fmt.Errorf("admin.bind is empty with admin.enabled=true: set host:port, " +
				"or admin.enabled=false to turn the admin port off")
		}
		if _, _, err := net.SplitHostPort(c.Admin.Bind); err != nil {
			return fmt.Errorf("admin.bind: %q is not host:port", c.Admin.Bind)
		}
		// Refused here, not at boot step 12 after the admin port served the
		// whole boot -- and on macOS/BSD not even there: SO_REUSEADDR lets
		// 127.0.0.1:P and *:P coexist, and the admin port silently took
		// localhost's connections (SPEC §13.2, §14). Only the stream
		// listeners: UDP is another protocol, listen.unix a path.
		for _, e := range []struct{ name, addr string }{
			{"listen.tcp", c.Listen.TCP},
			{"listen.tls", c.Listen.TLS},
		} {
			if e.addr != "" && sameStreamSocket(e.addr, c.Admin.Bind) {
				return fmt.Errorf("admin.bind (%q) and %s (%q) are, or may resolve to, the same "+
					"TCP address: the admin port and a syslog listener cannot share one stream "+
					"socket (to share a port, use two distinct IP literals)", c.Admin.Bind, e.name, e.addr)
			}
		}
	}
	if c.Listen.UnixMode == 0 && c.Listen.Unix != "" {
		return fmt.Errorf("listen.unix_mode: 0 makes the socket unwritable by anyone, " +
			"which silently disables the transport")
	}
	return nil
}

// sameStreamSocket reports whether two host:port binds would collide. Port 0
// asks the kernel for a free port and never collides.
func sameStreamSocket(a, b string) bool {
	ha, pa, errA := net.SplitHostPort(a)
	hb, pb, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil {
		return false
	}
	// Values, not spellings (independent review of 2026-10-14, SPEC §13.2):
	// the port is the number net.Listen would bind, read the way it reads it
	// -- "0514" is 514, a service name is its number -- and a port that is
	// neither fails at bind anyway, so it is refused here with both keys
	// named. Compared as text, "0514" and "514" passed as different ports.
	na, errA := net.LookupPort("tcp", pa)
	nb, errB := net.LookupPort("tcp", pb)
	if errA != nil || errB != nil {
		return true
	}
	if na != nb || na == 0 {
		return false
	}
	// Any unspecified literal is the wildcard ("::0", "0:0::0"), not only
	// the three usual spellings.
	wild := func(h string) bool {
		if h == "" {
			return true
		}
		ip, err := netip.ParseAddr(h)
		return err == nil && ip.Unmap().IsUnspecified()
	}
	if wild(ha) || wild(hb) {
		return true
	}
	ia, errA := netip.ParseAddr(ha)
	ib, errB := netip.ParseAddr(hb)
	if errA == nil && errB == nil {
		return ia.Unmap() == ib.Unmap()
	}
	// A host NAME on the same port may resolve to the other address
	// ("localhost" and "127.0.0.1"), and resolving here would make the
	// validation depend on DNS. Same port with a name on either side is a
	// collision; IP literals are how two stream listeners share a port.
	return true
}

// validateTLSAndProxy holds the [tls] and [proxy] rows of SPEC §13.2. File
// readability is CheckFiles', because it touches the disk.
func validateTLSAndProxy(c *Config) error {
	if c.Listen.TLS != "" && (c.TLS.CertFile == "" || c.TLS.KeyFile == "") {
		return fmt.Errorf("listen.tls is set but tls.cert_file and tls.key_file are not both " +
			"set: a TLS listener needs a certificate (SPEC §4.5)")
	}
	if c.TLS.ClientAuth == ClientAuthRequire && c.TLS.ClientCA == "" {
		return fmt.Errorf("tls.client_auth=require without tls.client_ca: there would be " +
			"nothing to verify client certificates against, and every handshake would fail")
	}
	if c.Proxy.Protocol != ProxyOff && len(c.Proxy.Trusted) == 0 {
		return fmt.Errorf("proxy.protocol=%s with an empty proxy.trusted: no peer could ever "+
			"send a header that is believed (SPEC §4.6)", c.Proxy.Protocol)
	}
	return nil
}

// validateSyslog holds the [syslog] rows of SPEC §13.2.
func validateSyslog(c *Config) error {
	s := &c.Syslog
	if s.DefaultPriority < 0 || s.DefaultPriority > 191 {
		return fmt.Errorf("syslog.default_priority: %d is outside 0..191", s.DefaultPriority)
	}
	// "Local" is known to time.LoadLocation, and means whatever zone this host
	// has. Two replicas draining the same Redis ring would then convert the
	// same raw line into different bytes under the same uuid (SPEC §3.5).
	if strings.EqualFold(s.RFC3164Timezone, "Local") {
		return fmt.Errorf("syslog.rfc3164_timezone: Local depends on the host and would make " +
			"the conversion differ between replicas; name the IANA zone (America/Sao_Paulo) or UTC")
	}
	if _, err := time.LoadLocation(s.RFC3164Timezone); err != nil {
		return fmt.Errorf("syslog.rfc3164_timezone: %q is not a known IANA zone: %v",
			s.RFC3164Timezone, err)
	}
	if s.MaxMessageBytes < MinMessageBytes || s.MaxMessageBytes > MaxMessageBytes {
		return fmt.Errorf("syslog.max_message_bytes: %s is outside 480..16MiB (480 is the "+
			"minimum RFC 5424 obliges a receiver to accept)", FormatBytes(s.MaxMessageBytes))
	}
	if s.SDMaxElements < 1 {
		return fmt.Errorf("syslog.sd_max_elements: %d is below 1", s.SDMaxElements)
	}
	if s.SDMaxParams < 1 {
		return fmt.Errorf("syslog.sd_max_params: %d is below 1", s.SDMaxParams)
	}
	return nil
}

// validateFilter rejects a facility or severity the parser could never
// produce: a filter that cannot match is a filter that silently drops nothing,
// or everything.
func validateFilter(c *Config) error {
	_, _, err := c.Filter.Resolve()
	return err
}

func validateRanges(c *Config) error {
	// The memory ring's own ceiling is checked after ringSinkCombinations,
	// which may degrade ring=redis to memory (memoryRingItems).
	if c.Ring.CapacityItems < 1 || c.Ring.CapacityItems > maxPersistentRingItems {
		return fmt.Errorf("ring.capacity_items: %d is outside 1..%d", c.Ring.CapacityItems, maxPersistentRingItems)
	}
	if c.Ring.MaxBytes < 1 {
		return fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return fmt.Errorf("ring.high_water: %g is outside (0, 1]", c.Ring.HighWater)
	}
	if c.Ring.MaxInflight < 1 || c.Ring.MaxInflight > maxInflight {
		return fmt.Errorf("ring.max_inflight: %d is outside 1..%d", c.Ring.MaxInflight, maxInflight)
	}
	if c.Sink.Workers < 1 || c.Sink.Workers > maxSinkWorkers {
		return fmt.Errorf("sink.workers: %d is outside 1..%d", c.Sink.Workers, maxSinkWorkers)
	}
	if c.Sink.Batch < 1 || c.Sink.Batch > maxSinkBatch {
		return fmt.Errorf("sink.batch: %d is outside 1..%d", c.Sink.Batch, maxSinkBatch)
	}
	if c.Sink.BatchMaxBytes < 1 || c.Sink.BatchMaxBytes > maxBatchMaxBytes {
		return fmt.Errorf("sink.batch_max_bytes: %s is outside 1..256MiB (a chunk is one write, "+
			"and libpq refuses a message above 1GiB with no SQLSTATE)", FormatBytes(c.Sink.BatchMaxBytes))
	}
	if c.Sink.MaxRetries < 1 {
		return fmt.Errorf("sink.max_retries: %d is below 1", c.Sink.MaxRetries)
	}
	if c.RingSQL.CommitBatch < 1 || c.RingSQL.CommitBatch > maxCommitBatch {
		return fmt.Errorf("ring.sqlite.commit_batch: %d is outside 1..%d", c.RingSQL.CommitBatch, maxCommitBatch)
	}
	if c.Sink.RetryBackoffMax < c.Sink.RetryBackoffMin {
		return fmt.Errorf("sink.retry_backoff_max (%s) is below sink.retry_backoff_min (%s)",
			c.Sink.RetryBackoffMax, c.Sink.RetryBackoffMin)
	}
	for _, n := range []struct {
		name string
		v    int
	}{
		{"tcp.max_conns", c.TCP.MaxConns},
		{"tcp.max_conns_per_ip", c.TCP.MaxConnsPerIP},
		{"tls.max_conns", c.TLS.MaxConns},
		{"tls.max_conns_per_ip", c.TLS.MaxConnsPerIP},
	} {
		if n.v < 0 {
			return fmt.Errorf("%s: %d is negative; 0 means unlimited", n.name, n.v)
		}
	}
	if c.RateLimit.V4Mask < 0 || c.RateLimit.V4Mask > 32 {
		return fmt.Errorf("ratelimit.v4_mask: %d is outside 0..32", c.RateLimit.V4Mask)
	}
	if c.RateLimit.V6Mask < 0 || c.RateLimit.V6Mask > 128 {
		return fmt.Errorf("ratelimit.v6_mask: %d is outside 0..128", c.RateLimit.V6Mask)
	}
	if c.RateLimit.Enabled {
		// The limiter treats a non-positive ceiling as "no limiter at all":
		// an enabled limiter that limits nothing, silently.
		if c.RateLimit.MaxBuckets < 1 {
			return fmt.Errorf("ratelimit.max_buckets: %d must be at least 1 when the limiter is on", c.RateLimit.MaxBuckets)
		}
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
	// "15ms" typed for "15s", or a bare 1ns, is a loop that floods
	// <ctl_namespace>.beacon and competes with the data for the one NATS
	// connection, not a heartbeat. Zero keeps its meaning: no periodic beacon
	// (SPEC §11; pendência 21 de 2026-10-11). Checked whatever the sink, so a
	// configuration that is refused with sink=nats is not accepted with
	// another one.
	if b := c.LogHub.BeaconInterval; b != 0 && b < time.Second {
		return fmt.Errorf("loghub.beacon_interval: %s is below 1s; 0 disables the periodic beacon", b)
	}
	return nil
}

// validateZeroDurations is the zero-duration rule of SPEC §16.5. Zero is not
// "unlimited" for these. A zero op_timeout fails every write before it starts
// and is retried forever without ever turning fatal; a zero drain_timeout turns
// every shutdown into an emergency dump and exit 3; a zero backpressure_timeout
// closes every held connection at the first poll, which is the transport's
// whole reason to exist switched off; a zero handshake or PROXY header deadline
// fails every connection; a zero claim_idle lets every replica steal every
// in-flight entry; a zero health cadence, busy timeout or backoff floor used to
// be silently replaced by a default that --dump-config did not show. Refusing
// here is what keeps the dump and the running process telling the same story.
//
// Zero IS legitimate where it says what it does: shutdown.pre_stop_delay,
// sink.batch_max_delay, sink.fail_timeout, tcp/tls.idle_timeout and
// loghub.beacon_interval.
func validateZeroDurations(c *Config) error {
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
		{"ring.redis.claim_idle", c.RingRedis.ClaimIdle},
		{"sink.sqlite.busy_timeout", c.SinkSQL.BusyTimeout},
		{"ratelimit.sweep_interval", c.RateLimit.SweepInterval},
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout},
		{"tcp.backpressure_timeout", c.TCP.BackpressureTimeout},
		{"tls.backpressure_timeout", c.TLS.BackpressureTimeout},
		{"tls.handshake_timeout", c.TLS.HandshakeTimeout},
		{"proxy.header_timeout", c.Proxy.HeaderTimeout},
	} {
		if d.v <= 0 {
			return fmt.Errorf("%s: must be positive, got %s; zero here is not \"no limit\" "+
				"(SPEC §16.5)", d.name, d.v)
		}
	}
	return nil
}

// validatePaths enforces the one rule that outlives every other: the ring queue
// and the events database are different things and may never be the same file
// (SPEC §9.7). Both paths are absolute after derive.
func validatePaths(c *Config) error {
	if c.Ring.Mode != RingModeSQLite || c.Sink.Type != SinkTypeSQLite {
		return nil
	}
	ring, sink := filepath.Clean(c.RingSQL.Path), filepath.Clean(c.SinkSQL.Path)
	if ring == sink {
		return fmt.Errorf("ring.sqlite.path and sink.sqlite.path are both %q: the ring is a "+
			"QUEUE whose rows are deleted after delivery, the sink is an append-only "+
			"DESTINATION. One file cannot be both", ring)
	}
	return nil
}

// validateClientTLS is the "tls_ca without tls in any client" row of SPEC
// §13.2: the CA would be ignored and the connection would be plaintext, which
// is the opposite of what the operator who wrote it wanted.
func validateClientTLS(c *Config) error {
	for _, x := range []struct {
		sec   string
		tls   bool
		tlsCA string
	}{
		{"ring.redis", c.RingRedis.TLS, c.RingRedis.TLSCA},
		{"sink.redis", c.SinkRedis.TLS, c.SinkRedis.TLSCA},
		{"sink.nats", c.SinkNATS.TLS, c.SinkNATS.TLSCA},
		{"sink.postgres", c.SinkPG.TLS, c.SinkPG.TLSCA},
		{"sink.mysql", c.SinkMySQL.TLS, c.SinkMySQL.TLSCA},
		{"sink.clickhouse", c.SinkCH.TLS, c.SinkCH.TLSCA},
	} {
		if x.tlsCA != "" && !x.tls {
			return fmt.Errorf("%s.tls_ca is set but %s.tls is false: the CA would be ignored "+
				"and the connection would be plaintext", x.sec, x.sec)
		}
	}
	return nil
}

// validateRedis holds the structural rules of the two Redis clients.
func validateRedis(c *Config) error {
	if c.Ring.Mode == RingModeRedis {
		if len(c.RingRedis.Addrs) == 0 {
			return fmt.Errorf("ring.redis.addrs: at least one address is required when ring.mode=redis")
		}
		if strings.TrimSpace(c.RingRedis.Stream) == "" || strings.TrimSpace(c.RingRedis.Group) == "" {
			return fmt.Errorf("ring.redis.stream and ring.redis.group cannot be empty when ring.mode=redis")
		}
		// A worker in retry renews its claim every claim_idle/3, but never in
		// the middle of an attempt, which can last sink.op_timeout. If one
		// attempt outlived the idle window, another replica's XAUTOCLAIM would
		// take a batch that is alive and both would deliver it (SPEC §9.5).
		if c.RingRedis.ClaimIdle <= 2*c.Sink.OpTimeout {
			return fmt.Errorf("ring.redis.claim_idle (%s) must be more than twice sink.op_timeout (%s): "+
				"a longer write would let another replica claim a live batch",
				c.RingRedis.ClaimIdle, c.Sink.OpTimeout)
		}
	}
	if c.Sink.Type == SinkTypeRedis {
		if len(c.SinkRedis.Addrs) == 0 {
			return fmt.Errorf("sink.redis.addrs: at least one address is required when sink.type=redis")
		}
		if strings.TrimSpace(c.SinkRedis.Stream) == "" {
			return fmt.Errorf("sink.redis.stream: cannot be empty when sink.type=redis")
		}
		if c.SinkRedis.MaxLen < 0 {
			return fmt.Errorf("sink.redis.maxlen: %d is negative; 0 disables trimming", c.SinkRedis.MaxLen)
		}
	}
	return nil
}

// validateSQL holds the [sink.postgres] and [sink.mysql] rules of SPEC §10.5,
// §10.6 and §13.2. They differ where the protocol does: the table name's case,
// and the placeholder ceiling of a MySQL statement.
func validateSQL(c *Config) error {
	var d *SQLDB
	var sec string
	switch c.Sink.Type {
	case SinkTypePostgres:
		d, sec = &c.SinkPG, "sink.postgres"
	case SinkTypeMySQL:
		d, sec = &c.SinkMySQL, "sink.mysql"
	default:
		return nil
	}
	for _, f := range []struct{ key, v string }{
		{"addr", d.Addr}, {"username", d.Username}, {"database", d.Database},
	} {
		if strings.TrimSpace(f.v) == "" {
			return fmt.Errorf("%s.%s: cannot be empty when sink.type=%s", sec, f.key, c.Sink.Type)
		}
	}
	if c.Sink.Type == SinkTypePostgres {
		if err := validatePostgresAddr(d.Addr); err != nil {
			return err
		}
		// The sink sets a TLSConfig after the parse, and pgx then sends an
		// SSLRequest over the socket, which the server refuses: a network
		// error retried for the whole sink.boot_timeout (libpq itself ignores
		// sslmode on a socket). Parity with MySQL below (independent review
		// of the port, 2026-10-13).
		if d.TLS && strings.HasPrefix(d.Addr, "/") {
			return fmt.Errorf("sink.postgres.tls=true with a unix socket address: TLS is a TCP " +
				"feature (with LOGHUB_EB_URL, an sslmode of require, verify-ca or verify-full " +
				"next to host=)")
		}
		if !pgIdent.MatchString(d.Table) {
			return fmt.Errorf("sink.postgres.table: %q must be a lowercase identifier, "+
				"optionally schema-qualified", d.Table)
		}
		return nil
	}
	if err := validateDBName("sink.mysql.database", d.Database); err != nil {
		return err
	}
	if !mysqlIdent.MatchString(d.Table) {
		return fmt.Errorf("sink.mysql.table: %q is not a plain identifier "+
			"(name or database.name; letters, digits and _; up to 64 characters each)", d.Table)
	}
	if d.TLS && strings.HasPrefix(d.Addr, "/") {
		return fmt.Errorf("sink.mysql.tls=true with a unix socket address (%s): TLS is a TCP feature", d.Addr)
	}
	if c.Sink.Batch > mysqlMaxBatch {
		return fmt.Errorf("sink.batch: %d exceeds %d, the most rows one MySQL INSERT can carry "+
			"(65535 placeholders, %d per row)", c.Sink.Batch, mysqlMaxBatch, mysqlColumns)
	}
	return nil
}

// maxDBName is the longest database name MySQL accepts: 64 CHARACTERS, not
// bytes -- 64 'ç' are a valid name of 128 bytes, and a 65th is 1059 -- and
// the ceiling used for ClickHouse too, whose own limit is the file name of
// the %-encoded name in its metadata directory (255 bytes: 28 CJK
// characters, more ASCII ones than 64). Counted in bytes, a legitimate
// multibyte name was refused (independent review of the port, 2026-10-13).
const maxDBName = 64

// validateDBName holds sink.mysql.database and sink.clickhouse.database to
// what can go between backticks (pendência 12 de 2026-10-11). Both sinks put
// the name raw inside `...` in the INSERT and in the table probe, which exists
// precisely so nothing typed in the configuration can change a statement: a
// backtick ends the quoting (and in MySQL the 1064 that follows was classed
// network, so the probe waited sink.boot_timeout), a backslash is an escape to
// ClickHouse inside a quoted identifier, and control bytes are no name either
// server would show back. Longer than 64 characters MySQL refuses it anyway, after
// the wait (SPEC §10.6, §10.10.5, §13.2). The sinks' constructors check the
// same rule (checkQuotedName) for a caller that bypasses this.
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

// validatePostgresAddr accepts one server, by host:port or by socket
// directory (with an optional :port, which names the socket file:
// /run/pg:5433 is /run/pg/.s.PGSQL.5433, what LOGHUB_EB_URL fills from
// postgres://u@:5433/db?host=/run/pg). Anything else -- "h1,h2:5432", an
// empty host, port 0, a tab, "[h:5432]:5433" -- passed here and then failed in
// the DNS or the dial, retried as a network error for the whole
// sink.boot_timeout (SPEC §10.5, §13.2; pendência 22 de 2026-10-11, and the
// independent review of go-relplog-pg-server, 2026-10-13).
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
// (what postgres://u@h:5432:5433/db becomes) passed as host "h:5432", a DNS
// name no resolver has, retried for the whole sink.boot_timeout.
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

// chIdent is what sink.clickhouse.table must look like: an unquoted
// identifier, optionally database-qualified. Case is kept, as in MySQL:
// ClickHouse names are case-sensitive, compared verbatim against system.tables
// and quoted with backticks in the INSERT.
var chIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63})?$`)

// validateClickHouse holds the structural [sink.clickhouse] rules of SPEC
// §10.10 and §13.2; the connection budget is dbWorkers', as for postgres and
// mysql.
func validateClickHouse(c *Config) error {
	if c.Sink.Type != SinkTypeClickHouse {
		return nil
	}
	ch := &c.SinkCH
	if len(ch.Addrs) == 0 {
		return fmt.Errorf("sink.clickhouse.addrs: at least one address is required when sink.type=clickhouse")
	}
	for _, a := range ch.Addrs {
		if _, _, err := net.SplitHostPort(a); err != nil {
			return fmt.Errorf("sink.clickhouse.addrs: %q is not host:port: %w", a, err)
		}
	}
	for _, f := range []struct{ key, v string }{{"username", ch.Username}, {"database", ch.Database}} {
		if strings.TrimSpace(f.v) == "" {
			return fmt.Errorf("sink.clickhouse.%s: cannot be empty when sink.type=clickhouse", f.key)
		}
	}
	if err := validateDBName("sink.clickhouse.database", ch.Database); err != nil {
		return err
	}
	if !chIdent.MatchString(ch.Table) {
		return fmt.Errorf("sink.clickhouse.table: %q is not a plain identifier "+
			"(name or database.name; letters, digits and _, not starting with a digit; up to 64 characters each)", ch.Table)
	}
	if ch.PoolSize < 0 {
		return fmt.Errorf("sink.clickhouse.pool_size: %d is negative; 0 derives sink.workers", ch.PoolSize)
	}
	return nil
}

// validateNATS holds the [sink.nats] and [loghub] rules of SPEC §10.7, §11 and
// §13.2.
func validateNATS(c *Config) error {
	if c.Sink.Type != SinkTypeNATS {
		return nil
	}
	n := &c.SinkNATS
	if len(n.URLs) == 0 {
		return fmt.Errorf("sink.nats.urls: at least one URL is required when sink.type=nats")
	}
	if err := validateNATSURLs(n); err != nil {
		return err
	}
	if n.Mode == NATSModeJetStream && !natsToken.MatchString(n.Stream) {
		return fmt.Errorf("sink.nats.stream: %q is not a valid stream name "+
			"(no whitespace, dots, wildcards or slashes)", n.Stream)
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
	// The beacon publishes to <ctl_namespace>.beacon: every token must be
	// publishable, or the first beacon fails at runtime instead of here.
	ns := c.LogHub.CtlNamespace
	if ns == "" {
		return fmt.Errorf("loghub.ctl_namespace: cannot be empty when sink.type=nats")
	}
	for _, tok := range strings.Split(ns, ".") {
		if !natsToken.MatchString(tok) {
			return fmt.Errorf("loghub.ctl_namespace: %q is not a publishable subject prefix "+
				"(dot-separated tokens, no whitespace, no * or >)", ns)
		}
	}
	return nil
}

// ringSinkCombinations holds the degrading rows of SPEC §9.7.
func (v *validator) ringSinkCombinations() {
	c := v.c
	if c.Ring.Mode == RingModeSQLite && c.Sink.Type == SinkTypeSQLite {
		v.warn("ring.mode=sqlite with sink.type=sqlite: every message pays TWO fsyncs before " +
			"delivery. ring.mode=memory with sink.type=sqlite gives the same destination " +
			"durability at half the cost, unless the two files are on different disks")
	}

	// Paying for durability on a transport that cannot offer it. A UDP
	// datagram may already have been dropped by the sender's kernel, by a
	// switch, or by our own SO_RCVBUF; an fsync only guarantees what did
	// arrive, at the cost of the throughput that would have let more of it
	// arrive (SPEC §9.4).
	if c.Ring.Mode == RingModeSQLite && c.Listen.TCP == "" && c.Listen.TLS == "" {
		v.warn("ring.mode=sqlite with only datagram transports configured: the fsync buys " +
			"durability for messages that the network may already have dropped without " +
			"telling anyone. Durability end to end needs listen.tcp or listen.tls")
	}

	// The ring stream is shared: with sink=none this instance consumes, and
	// discards, entries other replicas -- and earlier lives -- enqueued for a
	// real destination. Unlike ring.db's rows, nothing tells this run's
	// entries from theirs, so the boot cannot keep them aside; it can only
	// say so (SPEC §9.7; independent review of the port, 2026-10-13).
	if c.Ring.Mode == RingModeRedis && c.Sink.Type == SinkTypeNone {
		v.warn("ring.mode=redis with sink.type=none: the ring stream is shared, and this " +
			"instance reads and DISCARDS entries that other replicas or earlier lives " +
			"enqueued for a destination. Point ring.redis.stream at a stream of its own " +
			"unless this one is only for tests")
	}

	// A ring on the very Redis the sink writes to protects nothing the sink
	// does not lose with it: when that server is down, both are. Memory at
	// least keeps the queue on a different failure domain (SPEC §9.7).
	if c.Ring.Mode == RingModeRedis && c.Sink.Type == SinkTypeRedis &&
		rediscli.SameServer(c.RingRedis.ClientConfig(""), c.SinkRedis.ClientConfig("")) {
		v.warn("ring.mode=redis with sink.type=redis on the same server (same addresses and db): " +
			"the ring would protect nothing the destination does not lose with it, so the " +
			"effective ring is memory. Point ring.redis at another server to keep a shared ring")
		c.Ring.Mode = RingModeMemory
		v.notes["ring.mode"] = "degraded from redis: ring.redis and sink.redis are the same server (SPEC §9.7)"
	}
}

// memoryRingItems is the ceiling of the EFFECTIVE memory ring: the one asked
// for, or the one ring=redis degraded to (SPEC §9.7, §13.2). Only it makes a
// channel of capacity_items slots, and makechan panics past what the runtime
// can allocate.
func (v *validator) memoryRingItems() error {
	c := v.c
	if c.Ring.Mode != RingModeMemory || c.Ring.CapacityItems <= maxRingItems {
		return nil
	}
	if note := v.notes["ring.mode"]; note != "" {
		return fmt.Errorf("ring.capacity_items: %d is above %d, the ceiling of the memory ring "+
			"that ring.mode=redis was %s", c.Ring.CapacityItems, maxRingItems, note)
	}
	return fmt.Errorf("ring.capacity_items: %d is outside 1..%d with ring.mode=memory",
		c.Ring.CapacityItems, maxRingItems)
}

// ringBytesFloor refuses a ring.max_bytes below what one message of
// syslog.max_message_bytes costs the EFFECTIVE ring, in that ring's measure
// (SPEC §9.2, §13.2, §15.1): below it that message could never be queued, even
// in an empty ring -- a TCP connection stalls behind it until the backpressure
// timeout every time, and UDP drops it as ring_full with nothing queued. The
// floor of the smallest class is not enough: a 4 KiB ring takes the short
// lines and refuses any 3 KiB one for ever. Runs after ringSinkCombinations,
// which may degrade ring=redis to memory, and after validateSyslog has bounded
// max_message_bytes.
func (v *validator) ringBytesFloor() error {
	c := v.c
	var floor int64
	switch c.Ring.Mode {
	case RingModeMemory:
		floor = ringMinBytes(c.Syslog.MaxMessageBytes)
	case RingModeSQLite, RingModeRedis:
		// They count len(raw), what they store, so the truncated message
		// itself is what one of them costs.
		floor = c.Syslog.MaxMessageBytes
	default:
		return nil // ring=none does not use the key
	}
	if c.Ring.MaxBytes >= floor {
		return nil
	}
	return fmt.Errorf("ring.max_bytes (%s) is below what one message of syslog.max_message_bytes "+
		"(%s) costs the %s ring, %s: that message could never be queued, even in an empty ring",
		FormatBytes(c.Ring.MaxBytes), FormatBytes(c.Syslog.MaxMessageBytes), c.Ring.Mode, FormatBytes(floor))
}

// exposure is the "ratelimit.enabled=false with a listener off loopback" row.
func (v *validator) exposure() {
	c := v.c
	if c.RateLimit.Enabled {
		return
	}
	var exposed []string
	for _, e := range []struct{ name, addr string }{
		{"listen.udp", c.Listen.UDP},
		{"listen.tcp", c.Listen.TCP},
		{"listen.tls", c.Listen.TLS},
	} {
		if e.addr != "" && !isLoopbackBind(e.addr) {
			exposed = append(exposed, e.name+"="+e.addr)
		}
	}
	if len(exposed) > 0 {
		v.warn("ratelimit.enabled=false with %s reachable off this host: the service accepts "+
			"messages at whatever rate they arrive. The limiter is off by default because "+
			"one keyed on the wrong source is worse than none, not because it is unneeded",
			strings.Join(exposed, ", "))
	}
}

func (v *validator) natsMode() {
	if v.c.Sink.Type == SinkTypeNATS && v.c.SinkNATS.Mode == NATSModeCore {
		v.warn("sink.nats.mode=core is best effort: success means the NATS server received " +
			"the messages, not that anyone kept them; without a stream, a message with no " +
			"subscriber is gone (SPEC §10.7)")
	}
}

func (v *validator) sinkWorkers() error {
	c := v.c
	switch c.Sink.Type {
	case SinkTypeSQLite:
		// SQLite serialises writers by construction; more than one would only
		// produce SQLITE_BUSY and a slower server (SPEC §10.4).
		if c.Sink.Workers > 1 {
			v.warn("sink.type=sqlite with sink.workers=%d: forced to 1, because SQLite "+
				"serialises writers and the extra workers would only collect SQLITE_BUSY",
				c.Sink.Workers)
			c.Sink.Workers = 1
			v.notes["sink.workers"] = "forced to 1: sink.type=sqlite"
		}
	case SinkTypeMySQL:
		return v.dbWorkers("mysql", &c.SinkMySQL.PoolSize)
	case SinkTypePostgres:
		return v.dbWorkers("postgres", &c.SinkPG.PoolSize)
	case SinkTypeClickHouse:
		// The fleet-wide connection budget, and a second reason: every INSERT
		// is a part the server has to merge (SPEC §10.10).
		return v.dbWorkers("clickhouse", &c.SinkCH.PoolSize)
	}
	return nil
}

// dbWorkers applies the connection budget of SPEC §10.5, which postgres and
// mysql share (§10.6): every worker pins a connection out of a budget the whole
// fleet shares, so the default worker count is lowered, the pool is derived
// from it, and a pool smaller than the workers is refused.
func (v *validator) dbWorkers(sinkType string, poolSize *int) error {
	c := v.c
	if !v.setIn["sink.workers"] {
		v.warn("sink.type=%s: sink.workers lowered from %d to %d, because each worker "+
			"holds a pool connection and the server's connection budget is shared with "+
			"every other instance. Set sink.workers explicitly to override",
			sinkType, c.Sink.Workers, dbDefaultWorkers)
		c.Sink.Workers = dbDefaultWorkers
		v.notes["sink.workers"] = "lowered to 4: sink.type=" + sinkType + " and sink.workers not set"
	} else if c.Sink.Workers > dbWarnWorkers {
		v.warn("sink.type=%s with sink.workers=%d: above %d, a larger pool trades throughput "+
			"for lock contention inside the database", sinkType, c.Sink.Workers, dbWarnWorkers)
	}
	if *poolSize < 0 || *poolSize > maxPoolSize {
		return fmt.Errorf("sink.%s.pool_size: %d is outside 0..%d; 0 derives sink.workers",
			sinkType, *poolSize, maxPoolSize)
	}
	if *poolSize == 0 {
		*poolSize = c.Sink.Workers
		v.notes["sink."+sinkType+".pool_size"] = "derived: sink.workers"
	}
	// The pool is the writers': the health check has a connection of its own,
	// outside it, in all three sinks (SPEC §10.5). It used to take one from
	// the pool, and "pool_size >= workers + 1" was meant to leave it one; with
	// ring=none the writers are the receivers, up to ring.max_inflight, and
	// the spare connection protected nothing -- the check starved behind slow
	// writes, the sink went DOWN and, past fail_timeout, FATAL with nothing
	// wrong with the database (independent review of the port, 2026-10-13).
	// The instance still opens workers + 1 connections by default.
	if *poolSize < c.Sink.Workers {
		return fmt.Errorf("sink.%s.pool_size=%d is below sink.workers=%d: every worker holds a "+
			"connection while it writes", sinkType, *poolSize, c.Sink.Workers)
	}
	// With ring=none the receiver writes through, and the semaphore is the only
	// accounting there is. Writers beyond the pool queue INSIDE the pool, which
	// has no accounting at all -- exactly what the semaphore exists to prevent
	// (SPEC §9.6).
	if c.Ring.Mode == RingModeNone && c.Ring.MaxInflight > *poolSize {
		v.warn("ring.mode=none with ring.max_inflight=%d above sink.%s.pool_size=%d: writers "+
			"beyond the pool wait for a connection inside it, invisible to the ring's "+
			"accounting. Raise the pool or lower max_inflight",
			c.Ring.MaxInflight, sinkType, *poolSize)
	}
	return nil
}

// CheckFiles is the disk half of the listen.tls row of SPEC §13.2: the
// certificate, the key and the client CA must be readable. It is separate from
// Load on purpose. Load also runs on SIGHUP, and the certificate is read at
// boot only (§4.5): a reload must not fail -- and refuse a filter change --
// because a file the running process no longer needs was rotated away. The app
// calls CheckFiles once, at boot, right after Load; an error is exit 1.
//
// The contents are checked too, not only the permissions: a certificate that
// does not parse, a key that does not match it or a CA file with no
// certificate in it would otherwise surface at boot step 12, after the sink
// wait -- and with an unreachable sink the boot ends with exit 3 at
// sink.boot_timeout without ever logging the TLS error, a crashloop that
// hides its own cause.
func CheckFiles(c *Config) error {
	if c.Listen.TLS == "" {
		return nil
	}
	for _, f := range []struct{ name, path string }{
		{"tls.cert_file", c.TLS.CertFile},
		{"tls.key_file", c.TLS.KeyFile},
		{"tls.client_ca", c.TLS.ClientCA},
	} {
		if f.path == "" {
			continue
		}
		fh, err := os.Open(f.path)
		if err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
		fh.Close()
	}
	if _, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile); err != nil {
		return fmt.Errorf("tls.cert_file/tls.key_file: %w", err)
	}
	if c.TLS.ClientCA != "" {
		pem, err := os.ReadFile(c.TLS.ClientCA)
		if err != nil {
			return fmt.Errorf("tls.client_ca: %w", err)
		}
		if !x509.NewCertPool().AppendCertsFromPEM(pem) {
			return fmt.Errorf("tls.client_ca: %q holds no PEM certificate", c.TLS.ClientCA)
		}
	}
	return nil
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
// macOS included (SPEC §4.3).
func unixPathMax() int {
	if runtime.GOOS == "linux" {
		return 107
	}
	return 103
}

// ringMinBytes is what one n-byte message costs the memory ring (SPEC §9.2):
// the capacity of the pooled buffer the receiver copies it into, plus the
// per-item overhead. It is the ring's own heldBytes, computed before the copy
// exists.
func ringMinBytes(n int64) int64 {
	return int64(item.PooledCap(int(n))) + ring.ItemOverhead
}
