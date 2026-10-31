package config

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ring"
)

// natsToken is one dot-separated token of a subject, or a whole stream name: no
// whitespace, no wildcards, no separators. Wildcards are refused because this
// server PUBLISHES, and a wildcard is not a publishable subject.
var natsToken = regexp.MustCompile(`^[^\s.*>/\\]+$`)

// Ceilings of SPEC §13.2 for the keys that size memory or a count handed to a
// library (go-relplog-pg-server's and go-relplog-nats-server's independent review,
// 2026-10-13, ported).
// Without them a typo of a few digits panicked the boot (ring.capacity_items:
// makechan) or allocated a gigabyte per Dequeue (sink.batch). The two NATS
// ones are this server's own (docs/DECISOES.md §2.2):
//
//   - sink.workers: every worker publishes on the ONE NATS connection, whose
//     writes go through one mutex and one socket, so past a few dozen workers
//     only contention grows. And the client is told 2 x workers x batch as
//     its PublishAsyncMaxPending (SPEC §10.7): with these ceilings that is at
//     most 2 x 256 x 65536 = 33554432 futures, an int on every platform,
//     where unbounded keys could wrap it negative and fail the boot with an
//     error about an option nobody set.
//   - sink.batch_max_bytes: NATS has no limit per chunk -- each envelope is
//     its own PUB, bounded by max_payload (at most 64 MiB in nats-server) --
//     so the ceiling only bounds memory: a chunk is held whole while its
//     futures are pending, and a tainted one until they time out. 256 MiB,
//     the number of the family, is far above any chunk worth building.
const (
	maxRingItems     = 1 << 24 // 16 Mi items: 128 MiB of channel slots alone
	maxSinkBatch     = 65536
	maxBatchMaxBytes = 256 << 20
	maxSinkWorkers   = 256
	maxListeners     = 256

	// listen.udp_reassembly_* bounds (SPEC §4.2, §16.5).
	minReassemblyBytes    = 1 << 20
	maxReassemblyBytes    = 4 << 30
	maxReassemblyMessages = 1 << 20
)

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
		validateConvert,
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
			"listen.udp, listen.tcp or listen.http")
	}

	stream := c.streamListeners()
	for _, e := range append([]namedAddr{{"listen.udp", c.Listen.UDP}}, stream...) {
		if e.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(e.addr); err != nil {
			return fmt.Errorf("%s: %q is not host:port (a bare \":12201\" is fine)", e.name, e.addr)
		}
	}
	// UDP and TCP may share a port -- they are different protocols on
	// different sockets, and 12201 on both is the default. TCP and HTTP may
	// not share one: with SO_REUSEPORT (listeners > 1) the kernel would even
	// accept the double bind and hand GELF frames to the HTTP server, and the
	// other way round. A wildcard host overlaps every concrete host on the
	// same port.
	if a, b := stream[0], stream[1]; a.addr != "" && b.addr != "" && sameStreamSocket(a.addr, b.addr) {
		return fmt.Errorf("%s (%q) and %s (%q) are, or may resolve to, the same TCP "+
			"address: they are different protocols and cannot share one stream socket (to share a "+
			"port, use two distinct IP literals)", a.name, a.addr, b.name, b.addr)
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
	// The reassembly ceilings bound the memory a flood of first chunks can
	// pin for the 5 s deadline (SPEC §4.2). Below 1 MiB not even a handful of
	// maximum-size chunked documents fit; above 4 GiB it is no ceiling.
	if b := c.Listen.UDPReassemblyMaxBytes; b < minReassemblyBytes || b > maxReassemblyBytes {
		return fmt.Errorf("listen.udp_reassembly_max_bytes: %s is outside 1MiB..4GiB", FormatBytes(b))
	}
	if n := c.Listen.UDPReassemblyMaxMessages; n < 1 || n > maxReassemblyMessages {
		return fmt.Errorf("listen.udp_reassembly_max_messages: %d is outside 1..%d", n, maxReassemblyMessages)
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
		// Refused here, not when the transport opens after the admin port
		// served the whole boot -- and on macOS/BSD not even there:
		// SO_REUSEADDR lets 127.0.0.1:P and *:P coexist, and the admin port
		// silently took localhost's TCP connections (SPEC §13.2, §14). Only
		// the stream listeners: UDP is another protocol.
		for _, e := range stream {
			if e.addr != "" && sameStreamSocket(e.addr, c.Admin.Bind) {
				return fmt.Errorf("admin.bind (%q) and %s (%q) are, or may resolve to, the same "+
					"TCP address: the admin port and a GELF listener cannot share one stream "+
					"socket (to share a port, use two distinct IP literals)", c.Admin.Bind, e.name, e.addr)
			}
		}
	}
	return nil
}

// namedAddr is one listener address with its parameter name, for errors.
type namedAddr struct{ name, addr string }

// streamListeners are the TCP-based listeners, in registry order: they must
// be pairwise distinct sockets, and distinct from admin.bind (SPEC §13.2).
func (c *Config) streamListeners() []namedAddr {
	return []namedAddr{
		{"listen.tcp", c.Listen.TCP},
		{"listen.http", c.Listen.HTTP},
	}
}

// sameStreamSocket reports whether two host:port binds would collide; the
// function of go-gelf-server, where it also compares listen.tls and
// listen.https. Port 0 asks the kernel for a free port and never collides.
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
	// collision; IP literals are how two stream sockets share a port.
	return true
}

// validateProxy holds the [proxy] row of SPEC §13.2.
func validateProxy(c *Config) error {
	if c.Proxy.Protocol != ProxyOff && len(c.Proxy.Trusted) == 0 {
		return fmt.Errorf("proxy.protocol=%s with an empty proxy.trusted: no peer could ever "+
			"send a header that is believed (SPEC §4.6)", c.Proxy.Protocol)
	}
	return nil
}

// validateConvert holds the [convert] rows of SPEC §13.2. keep_raw is an
// enum and the registry already refused anything else.
func validateConvert(c *Config) error {
	if b := c.Convert.MaxMessageBytes; b < MinMessageBytes || b > MaxMessageBytes {
		return fmt.Errorf("convert.max_message_bytes: %s is outside 1KiB..16MiB", FormatBytes(b))
	}
	// Checked even with http.bulk off, so turning it on later cannot meet a
	// value that was always wrong. Below convert.max_message_bytes it is
	// raised to it (one line may always be a whole document), so the default
	// works with any max_message_bytes.
	if b := c.HTTP.BulkMaxBytes; b < MinMessageBytes || b > MaxBulkBytes {
		return fmt.Errorf("http.bulk_max_bytes: %s is outside 1KiB..64MiB", FormatBytes(b))
	}
	return nil
}

// MaxBulkBytes is the most http.bulk_max_bytes accepts: a body is held whole
// (and once more decompressed) while its lines go into the ring.
const MaxBulkBytes = 64 << 20

// ringMinBytes is what one n-byte message costs the memory ring (SPEC §9.2):
// the capacity of the pooled buffer the receiver copies it into
// (item.CopyRawBox, the class of the document's length), plus the per-item
// overhead. It is the ring's own heldBytes, computed before the copy exists,
// as ring.Memory.FullFor computes it.
func ringMinBytes(n int64) int64 {
	return int64(item.PooledCap(int(n))) + ring.ItemOverhead
}

func validateRanges(c *Config) error {
	if c.Ring.CapacityItems < 1 || c.Ring.CapacityItems > maxRingItems {
		return fmt.Errorf("ring.capacity_items: %d is outside 1..%d", c.Ring.CapacityItems, maxRingItems)
	}
	// One document of the largest size accepted must fit an empty
	// ring. Below it that message could never be queued: a TCP connection
	// stalls behind it until tcp.backpressure_timeout every time, and UDP
	// drops it as ring_full with nothing queued. The floor of the smallest
	// class is not enough: a 4 KiB ring takes the short lines and refuses any
	// 3 KiB one for ever (SPEC §13.2, §15.1). validateConvert has already
	// bounded max_message_bytes.
	if floor := ringMinBytes(c.Convert.MaxMessageBytes); c.Ring.MaxBytes < floor {
		return fmt.Errorf("ring.max_bytes (%s) is below what one message of convert.max_message_bytes "+
			"(%s) costs the ring, %s (its pool class plus %d bytes): that message could never be "+
			"queued, even in an empty ring",
			FormatBytes(c.Ring.MaxBytes), FormatBytes(c.Convert.MaxMessageBytes), FormatBytes(floor), ring.ItemOverhead)
	}
	if c.Ring.HighWater <= 0 || c.Ring.HighWater > 1 {
		return fmt.Errorf("ring.high_water: %g is outside (0, 1]", c.Ring.HighWater)
	}
	if c.Sink.Workers < 1 || c.Sink.Workers > maxSinkWorkers {
		return fmt.Errorf("sink.workers: %d is outside 1..%d (they share one NATS connection)",
			c.Sink.Workers, maxSinkWorkers)
	}
	if c.Sink.Batch < 1 || c.Sink.Batch > maxSinkBatch {
		return fmt.Errorf("sink.batch: %d is outside 1..%d", c.Sink.Batch, maxSinkBatch)
	}
	if c.Sink.BatchMaxBytes < 1 || c.Sink.BatchMaxBytes > maxBatchMaxBytes {
		return fmt.Errorf("sink.batch_max_bytes: %s is outside 1..256MiB (a chunk is held whole "+
			"while its publishes are pending)", FormatBytes(c.Sink.BatchMaxBytes))
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
		{"http.max_conns", c.HTTP.MaxConns},
		{"http.max_conns_per_ip", c.HTTP.MaxConnsPerIP},
		{"http.max_requests_per_conn", c.HTTP.MaxRequestsPerConn},
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
// whole reason to exist switched off; a zero PROXY header or HTTP read
// deadline fails every connection; a zero health cadence or backoff floor used to be silently
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
		{"http.read_header_timeout", c.HTTP.ReadHeaderTimeout},
		{"http.read_timeout", c.HTTP.ReadTimeout},
		{"http.idle_timeout", c.HTTP.IdleTimeout},
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
	for i, u := range n.URLs {
		scheme, _, ok := strings.Cut(u, "://")
		if !ok && i > 0 {
			// The list is split at commas, so an unescaped ',' in a password
			// ("nats://u:12,cret@h") leaves a piece with no scheme. Saying
			// only "no scheme" would send the operator looking for a bare
			// host:port that is not there.
			return fmt.Errorf("sink.nats.urls: an entry has no scheme; every URL must be nats://, "+
				"and a ',' in a user, password or token splits the list (%s)", percentEncodeHint)
		}
		if !ok || !strings.EqualFold(scheme, "nats") {
			return fmt.Errorf("sink.nats.urls: every URL must be nats:// (tls://, ws:// and wss:// "+
				"are refused: this server has no TLS); got an entry with %s", describeScheme(u))
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

// describeScheme is schemeOf for a message: a real scheme is quoted, the two
// placeholders are not, so "none" never reads as a scheme someone typed.
func describeScheme(u string) string {
	switch s := schemeOf(u); s {
	case "none":
		return "no scheme"
	case "unrecognised":
		return "an unrecognised scheme"
	default:
		return "scheme " + strconv.Quote(s+"://")
	}
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
