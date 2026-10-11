package config

import (
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// natsToken is one dot-separated token of a subject, or a whole stream name: no
// whitespace, no wildcards, no separators. Wildcards are refused because this
// server PUBLISHES, and a wildcard is not a publishable subject.
var natsToken = regexp.MustCompile(`^[^\s.*>/\\]+$`)

// Validate checks every configuration-only constraint of SPEC §13.2 and §16.5
// and returns the WARN-level findings. An error is exit code 1: the process
// must not start.
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
		validateNATS,
	} {
		if err := check(c); err != nil {
			return nil, nil, err
		}
	}

	v.outsideDataDir()
	if c.SinkNATS.Mode == NATSModeCore {
		v.warn("sink.nats.mode=core is best effort: success means the NATS server received " +
			"the messages, not that anyone kept them; without a stream, a message with no " +
			"subscriber is gone (SPEC §10.7)")
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
	if c.Ring.CapacityItems < 1 {
		return fmt.Errorf("ring.capacity_items: %d is below 1", c.Ring.CapacityItems)
	}
	if c.Ring.MaxBytes < 1 {
		return fmt.Errorf("ring.max_bytes: must be positive")
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return fmt.Errorf("ring.high_water: %g is outside (0, 1]", c.Ring.HighWater)
	}
	if c.Sink.Workers < 1 {
		return fmt.Errorf("sink.workers: %d is below 1", c.Sink.Workers)
	}
	if c.Sink.Batch < 1 {
		return fmt.Errorf("sink.batch: %d is below 1", c.Sink.Batch)
	}
	if c.Sink.BatchMaxBytes < 1 {
		return fmt.Errorf("sink.batch_max_bytes: must be positive")
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
// sink.batch_max_delay, sink.fail_timeout, tcp.idle_timeout and
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

// validateNATS holds the [sink.nats] and [loghub] rules of SPEC §10.7, §11 and
// §13.2.
func validateNATS(c *Config) error {
	n := &c.SinkNATS
	if len(n.URLs) == 0 {
		return fmt.Errorf("sink.nats.urls: at least one URL is required")
	}
	// nats.go also dials tls://, ws:// and wss:// (and a bare host:port as
	// nats://). This server has no TLS (SPEC §1.2), and a websocket URL is not
	// a NATS one: anything but nats:// is refused here rather than dialled
	// into a handshake the client was never configured for.
	userinfo := false
	for _, u := range n.URLs {
		scheme, _, ok := strings.Cut(u, "://")
		if !ok || !strings.EqualFold(scheme, "nats") {
			return fmt.Errorf("sink.nats.urls: every URL must be nats:// (tls://, ws:// and wss:// "+
				"are refused: this server has no TLS); got an entry with scheme %q", schemeOf(u))
		}
		// Parsed here, with an error that never quotes the URL: the client's
		// own parse would fail at connect time, classed network, retried for
		// the whole sink.boot_timeout -- and Go's url.Error quotes the input,
		// so an unescaped '%', '/' or '#' in a password went to the log on
		// every retry. parseURL drops even the piece the parser's own message
		// quotes (SPEC §13.2).
		pu, err := parseURL(u)
		if err != nil {
			return fmt.Errorf("sink.nats.urls: an entry is %v (%s)", err, percentEncodeHint)
		}
		if pu.Hostname() == "" {
			return fmt.Errorf("sink.nats.urls: an entry has no host")
		}
		// The rule of LOGHUB_EB_URL (SPEC §10.9). It is also what catches the
		// secret that parses: nats://tok/en@host is host "tok" and a path,
		// with no credentials at all, and the client's dial error quoted that
		// host -- the token up to the slash -- in every log line until
		// sink.boot_timeout (SPEC §13.2).
		if (pu.Path != "" && pu.Path != "/") || pu.RawQuery != "" || pu.Fragment != "" {
			return fmt.Errorf("sink.nats.urls: an entry carries a path, a query or a fragment, "+
				"which a NATS URL has none of (%s)", percentEncodeHint)
		}
		if pu.User != nil {
			userinfo = true
		}
	}
	// nats.go prefers the URL's userinfo over every explicit credential: the
	// keys would be accepted and silently ignored. LOGHUB_EB_URL refuses the
	// same conflict; the plain keys must too.
	if userinfo && (n.Username != "" || n.Password != "" || n.Token != "" || n.Credentials != "") {
		return fmt.Errorf("sink.nats.urls carries credentials (user:password@ or token@) and " +
			"sink.nats.username/password/token/credentials is set too: the client would use the " +
			"URL's and ignore the key; keep one of them")
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
	// A beacon every nanosecond is a loop that floods the control plane, not
	// a heartbeat. Zero keeps its meaning: no periodic beacon.
	if b := c.LogHub.BeaconInterval; b != 0 && b < time.Second {
		return fmt.Errorf("loghub.beacon_interval: %s is below 1s; 0 disables the periodic beacon", b)
	}
	// The beacon publishes to <ctl_namespace>.beacon: every token must be
	// publishable, or the first beacon fails at runtime instead of here.
	ns := c.LogHub.CtlNamespace
	if ns == "" {
		return fmt.Errorf("loghub.ctl_namespace: cannot be empty")
	}
	for _, tok := range strings.Split(ns, ".") {
		if !natsToken.MatchString(tok) {
			return fmt.Errorf("loghub.ctl_namespace: %q is not a publishable subject prefix "+
				"(dot-separated tokens, no whitespace, no * or >)", ns)
		}
	}
	return nil
}

// schemeOf names a URL's scheme for an error without echoing the URL, which may
// carry a password.
func schemeOf(u string) string {
	scheme, _, ok := strings.Cut(u, "://")
	if !ok {
		return "none"
	}
	if !schemeRE.MatchString(scheme) {
		return "unrecognised"
	}
	return strings.ToLower(scheme)
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
