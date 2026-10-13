package config

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// dbWarnWorkers is the sink.workers value above which the boot warns
// (SPEC §10.5): every worker pins a connection out of a budget -- 100 by
// default in PostgreSQL -- that the whole fleet shares, and past a point a
// larger pool trades throughput for lock contention inside the database.
const dbWarnWorkers = 16

// Ceilings of SPEC §13.2 for the keys that size memory or a count handed to a
// library (go-relplog-pg-server's independent review, 2026-10-13, ported).
// Without them a typo of a few digits panicked the boot (ring.capacity_items:
// makechan), allocated a gigabyte per Dequeue (sink.batch), wrapped around in
// the pool's int32 (sink.postgres.pool_size 4294967300 became 4) or built a
// chunk whose Bind passes libpq's 1 GiB message limit and fails with no
// SQLSTATE -- a network error retried forever (sink.batch_max_bytes).
const (
	maxRingItems     = 1 << 24 // 16 Mi items: 128 MiB of channel slots alone
	maxSinkBatch     = 65536
	maxBatchMaxBytes = 256 << 20
	maxSinkWorkers   = 1024
	maxPoolSize      = 4096
	maxListeners     = 256
)

// pgIdent is what sink.postgres.table must look like: a plain lowercase
// identifier, optionally schema-qualified. Lowercase only, so the name can be
// handed to to_regclass unquoted and mean the same table the INSERT names
// quoted; uppercase would fold in one place and not the other.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// Validate checks every configuration-only constraint of SPEC §13.2 and §16.5
// and returns the WARN-level findings. An error is exit code 1: the process
// must not start. Validate may ADJUST c where the SPEC says a value is derived
// (sink.postgres.pool_size); Load records why in Loaded.Notes.
//
// setIn says which parameters were written by the file, the environment or the
// command line, as opposed to carrying their registry default; nil means
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

// outsideDataDir warns about emergency.dir outside server.data_dir (SPEC
// §20.16). It is allowed -- an operator may put the dumps on another disk --
// but a read-only root with one writable volume, the shape of the container,
// then fails at boot, and the warning says why before it does.
func (v *validator) outsideDataDir() {
	c := v.c
	rel, err := filepath.Rel(c.Server.DataDir, c.Emergency.Dir)
	under := err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	if c.Emergency.Enabled && c.Emergency.Dir != "" && !under {
		v.warn("emergency.dir=%q is outside server.data_dir (%q): everything else this server writes "+
			"lives under data_dir, and a container with a read-only root and one volume "+
			"will refuse this path (SPEC §20.16)", c.Emergency.Dir, c.Server.DataDir)
	}
}

func validate(c *Config, setIn map[string]bool) ([]string, map[string]string, error) {
	if setIn == nil {
		setIn = map[string]bool{}
	}
	v := &validator{c: c, setIn: setIn, notes: map[string]string{}}

	for _, check := range []func(*Config) error{
		validateListeners,
		validateProxy,
		validateSyslog,
		validateZeroDurations, // before the ordering of backoff min/max
		validateRanges,
		validatePostgres,
	} {
		if err := check(c); err != nil {
			return nil, nil, err
		}
	}

	v.outsideDataDir()
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
			"listen.udp, listen.tcp or listen.unix")
	}

	// UDP and TCP may share a port -- they are different protocols on
	// different sockets, and 1514 on both is the default.
	for _, e := range []struct{ name, addr string }{
		{"listen.udp", c.Listen.UDP},
		{"listen.tcp", c.Listen.TCP},
	} {
		if e.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(e.addr); err != nil {
			return fmt.Errorf("%s: %q is not host:port (a bare \":1514\" is fine)", e.name, e.addr)
		}
	}

	for _, n := range []struct {
		name string
		v    int
	}{
		{"listen.udp_listeners", c.Listen.UDPListeners},
		{"listen.tcp_listeners", c.Listen.TCPListeners},
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
	}
	if c.Listen.UnixMode == 0 && c.Listen.Unix != "" {
		return fmt.Errorf("listen.unix_mode: 0 makes the socket unwritable by anyone, " +
			"which silently disables the transport")
	}
	return nil
}

// validateProxy holds the [proxy] row of SPEC §13.2.
func validateProxy(c *Config) error {
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
	// has. A dump written on one host and reingested on another, or two hosts
	// behind one balancer, would then convert the same raw line differently
	// (SPEC §3.5).
	if strings.EqualFold(s.RFC3164Timezone, "Local") {
		return fmt.Errorf("syslog.rfc3164_timezone: Local depends on the host and would make " +
			"the conversion differ between hosts; name the IANA zone (America/Sao_Paulo) or UTC")
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

func validateRanges(c *Config) error {
	if c.Ring.CapacityItems < 1 || c.Ring.CapacityItems > maxRingItems {
		return fmt.Errorf("ring.capacity_items: %d is outside 1..%d", c.Ring.CapacityItems, maxRingItems)
	}
	if c.Ring.MaxBytes < 1 {
		return fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return fmt.Errorf("ring.high_water: %g is outside (0, 1]", c.Ring.HighWater)
	}
	if c.Sink.Workers < 1 || c.Sink.Workers > maxSinkWorkers {
		return fmt.Errorf("sink.workers: %d is outside 1..%d", c.Sink.Workers, maxSinkWorkers)
	}
	if c.Sink.Batch < 1 || c.Sink.Batch > maxSinkBatch {
		return fmt.Errorf("sink.batch: %d is outside 1..%d", c.Sink.Batch, maxSinkBatch)
	}
	if c.Sink.BatchMaxBytes < 1 || c.Sink.BatchMaxBytes > maxBatchMaxBytes {
		return fmt.Errorf("sink.batch_max_bytes: %s is outside 1..256MiB (a chunk is one INSERT, "+
			"and libpq refuses a message above 1GiB with no SQLSTATE)", FormatBytes(c.Sink.BatchMaxBytes))
	}
	if c.Sink.MaxRetries < 1 {
		return fmt.Errorf("sink.max_retries: %d is below 1", c.Sink.MaxRetries)
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
	} {
		if n.v < 0 {
			return fmt.Errorf("%s: %d is negative; 0 means unlimited", n.name, n.v)
		}
	}
	if c.Log.AccessSample < 0 || c.Log.AccessSample > 1 {
		return fmt.Errorf("log.access_sample: %g is outside [0, 1]", c.Log.AccessSample)
	}
	return nil
}

// validateZeroDurations is the zero-duration rule of SPEC §16.5. Zero is not
// "unlimited" for these. A zero op_timeout fails every write before it starts
// and is retried forever without ever turning fatal; a zero drain_timeout turns
// every shutdown into an emergency dump and exit 3; a zero backpressure_timeout
// closes every held connection at the first poll, which is the transport's
// whole reason to exist switched off; a zero PROXY header deadline fails every
// connection; a zero health cadence or backoff floor used to be silently
// replaced by a default that --dump-config did not show. Refusing here is what
// keeps the dump and the running process telling the same story.
//
// Zero IS legitimate where it says what it does: shutdown.pre_stop_delay,
// sink.batch_max_delay, sink.fail_timeout and tcp.idle_timeout.
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
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout},
		{"tcp.backpressure_timeout", c.TCP.BackpressureTimeout},
		{"proxy.header_timeout", c.Proxy.HeaderTimeout},
	} {
		if d.v <= 0 {
			return fmt.Errorf("%s: must be positive, got %s; zero here is not \"no limit\" "+
				"(SPEC §16.5)", d.name, d.v)
		}
	}
	return nil
}

// validatePostgres holds the [sink.postgres] rules of SPEC §10.5 and §13.2.
func validatePostgres(c *Config) error {
	d := &c.SinkPG
	for _, f := range []struct{ key, v string }{
		{"addr", d.Addr}, {"username", d.Username}, {"database", d.Database},
	} {
		if strings.TrimSpace(f.v) == "" {
			return fmt.Errorf("sink.postgres.%s: cannot be empty", f.key)
		}
	}
	// One server, by host:port or by socket directory (with an optional
	// :port, which names the socket file). Anything else -- an empty host,
	// "h1,h2", port 0 -- would pass here and then be retried as a network
	// failure for the whole sink.boot_timeout, or refused only at boot step 7,
	// after the admin port opened.
	host, port, err := net.SplitHostPort(d.Addr)
	switch {
	case strings.HasPrefix(d.Addr, "/"):
		// A comma would be a second socket directory to libpq ("/run/pg,/x"),
		// which pgx then tries as a list: refused like "h1,h2" below.
		if strings.ContainsAny(d.Addr, ",\t ") {
			return fmt.Errorf("sink.postgres.addr: %q: a socket directory names one server, "+
				"with no comma or blank", d.Addr)
		}
		if err != nil {
			break // a bare socket directory: port 5432
		}
		if !validPort(port) {
			return fmt.Errorf("sink.postgres.addr: %q: the port after the socket directory "+
				"must be 1..65535", d.Addr)
		}
	case err != nil || host == "" || strings.ContainsAny(host, ", \t") || !validPort(port) || !oneHost(host):
		return fmt.Errorf("sink.postgres.addr: %q is neither host:port (one server, port "+
			"1..65535) nor a unix socket directory starting with /", d.Addr)
	}
	if !pgIdent.MatchString(d.Table) {
		return fmt.Errorf("sink.postgres.table: %q must be a lowercase identifier, "+
			"optionally schema-qualified", d.Table)
	}
	if d.PoolSize < 0 || d.PoolSize > maxPoolSize {
		return fmt.Errorf("sink.postgres.pool_size: %d is outside 0..%d; 0 derives sink.workers + 1",
			d.PoolSize, maxPoolSize)
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

// sinkWorkers applies the connection budget of SPEC §10.5: the pool is derived
// from the workers, and a pool without a connection to spare for the health
// probe is refused.
func (v *validator) sinkWorkers() error {
	c := v.c
	if c.Sink.Workers > dbWarnWorkers {
		v.warn("sink.workers=%d: above %d, a larger pool trades throughput for lock contention "+
			"inside the database, and every instance takes as many connections out of a budget "+
			"the whole fleet shares", c.Sink.Workers, dbWarnWorkers)
	}
	if c.SinkPG.PoolSize == 0 {
		c.SinkPG.PoolSize = c.Sink.Workers + 1
		v.notes["sink.postgres.pool_size"] = "derived: sink.workers + 1"
	}
	// workers + 1, not workers: the health probe takes its connection out of
	// the same pool. With pool_size = workers, slow but working writes held
	// every connection, the probe timed out waiting for one, the sink went
	// DOWN and, past fail_timeout, FATAL: exit 3 with nothing wrong with the
	// database (go-relplog-pg-server's independent review, 2026-10-13).
	if c.SinkPG.PoolSize < c.Sink.Workers+1 {
		return fmt.Errorf("sink.postgres.pool_size=%d is below sink.workers + 1 = %d: every worker "+
			"holds a connection while it writes, and the health probe needs one more",
			c.SinkPG.PoolSize, c.Sink.Workers+1)
	}
	return nil
}

// validPort reports whether s is a TCP port number, 1..65535.
func validPort(s string) bool {
	n, err := strconv.Atoi(s)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == s
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
