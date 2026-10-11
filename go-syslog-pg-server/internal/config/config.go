// Package config is the single registry of every tunable of go-syslog-pg-server
// (SPEC §16). One table (registry.go) declares each parameter, and from it come
// the INI parser, the command-line flags, the environment, --help,
// --dump-config, the SIGHUP comparison and the validation. A new parameter is
// added there and nowhere else.
//
// The package is pure configuration: it never opens a socket, never touches the
// identity library and never writes to disk. The checks of SPEC §13.2 that need
// the network or the identity (space grammar, the table) are the application's;
// the ones that depend only on configuration are here.
package config

import (
	"net/netip"
	"time"

	// The image embeds the timezone database so syslog.rfc3164_timezone means
	// the same thing on every host (SPEC §15.4). Importing it here, next to the
	// validation that calls time.LoadLocation, makes the guarantee local: any
	// binary that accepts a zone at boot can also load it in the converter,
	// whatever the host's /usr/share/zoneinfo holds.
	_ "time/tzdata"
)

// Config holds every tunable of the server. Field values are written only by
// the registry (see registry.go) and by the derivation and validation steps of
// Load; nothing else parses configuration input.
//
// After a successful Load:
//   - every path the service writes is absolute (Server.DataDir, Emergency.Dir);
//   - enumerations hold their canonical spelling (the constants below);
//   - LOGHUB_EB_URL, when set, has already been expanded into the sink keys;
//   - validation adjustments are applied (the pool size derived).
//
// Fields marked reloadable in the registry may change on SIGHUP; readers of
// those go through a snapshot rather than reading Config directly.
type Config struct {
	Server    Server
	Listen    Listen
	TCP       TCP
	Proxy     Proxy
	Syslog    Syslog
	Ring      Ring
	Sink      Sink
	SinkPG    SinkPostgres // [sink.postgres]
	LogHub    LogHub
	Emergency Emergency
	Admin     Admin
	Log       Log
	Shutdown  Shutdown
}

// Server is the root of everything the service writes (SPEC §16.6).
type Server struct {
	// DataDir is server.data_dir (env DATADIR), always absolute after Load:
	// a relative value is resolved against the working directory. The app
	// exports it as DATADIR before lhident.Initialize() so the identity lands
	// in the same root (SPEC §13.1 step 2).
	DataDir string
}

// Listen holds one address per transport. An empty address disables that
// transport, and all three empty is a configuration error (SPEC §4): a server
// that listens to nothing has no function.
type Listen struct {
	UDP            string // listen.udp
	UDPListeners   int    // listen.udp_listeners; >1 enables SO_REUSEPORT
	UDPReadBuffer  int64  // listen.udp_read_buffer, bytes (SO_RCVBUF)
	TCP            string // listen.tcp
	TCPListeners   int    // listen.tcp_listeners
	Unix           string // listen.unix, absolute path
	UnixMode       int    // listen.unix_mode, a file mode (0666 = 0o666)
	UnixReadBuffer int64  // listen.unix_read_buffer, bytes
}

// ConnLimits are the connection knobs of [tcp] (SPEC §4.4). A struct of its own
// because the receiver takes them as one value per stream transport.
type ConnLimits struct {
	IdleTimeout         time.Duration // 0 = never
	MaxConns            int           // 0 = unlimited; blocks in accept
	MaxConnsPerIP       int           // 0 = unlimited; closes at once
	BackpressureTimeout time.Duration // never 0 (SPEC §16.5)
}

// TCP is [tcp] (SPEC §4.4, §5).
type TCP struct {
	Framing string // tcp.framing: FramingAuto | FramingOctet | FramingLF
	ConnLimits
}

// Proxy is [proxy] (SPEC §4.6).
type Proxy struct {
	Protocol      string         // proxy.protocol: ProxyOff | ProxyOptional | ProxyRequired
	Trusted       []netip.Prefix // proxy.trusted; masked prefixes, bare IPs as /32 or /128
	HeaderTimeout time.Duration  // proxy.header_timeout; never 0
}

// Syslog is [syslog]: the conversion config. Every key requires a restart
// (SPEC §16.3), because the JLH must be a pure function of header + raw +
// this struct (§3.5).
type Syslog struct {
	DefaultPriority int    // syslog.default_priority, 0..191
	RFC3164Timezone string // syslog.rfc3164_timezone, IANA name; see Location
	MaxMessageBytes int64  // syslog.max_message_bytes, 480..16MiB
	PayloadRaw      bool   // syslog.payload_raw
	LegacyCharset   string // syslog.legacy_charset: "" | CharsetCP1252 | CharsetLatin1, or the aliases windows-1252 and iso-8859-1
	AcceptMalformed bool   // syslog.accept_malformed
	SDMaxElements   int    // syslog.sd_max_elements
	SDMaxParams     int    // syslog.sd_max_params
}

// Location loads syslog.rfc3164_timezone. Validation already proved it loads,
// so an error here means the Config was not produced by Load.
func (s Syslog) Location() (*time.Location, error) {
	return time.LoadLocation(s.RFC3164Timezone)
}

// Ring is [ring] (SPEC §9). The ring is always in memory: there is no mode.
type Ring struct {
	CapacityItems int
	MaxBytes      int64   // bytes of raw lines
	HighWater     float64 // (0, 1]
}

// Sink is [sink] (SPEC §10.1).
type Sink struct {
	Workers         int
	Batch           int
	BatchMaxBytes   int64 // ceiling of JLH bytes per chunk
	BatchMaxDelay   time.Duration
	OpTimeout       time.Duration
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	FailTimeout     time.Duration // 0 = no time fuse
	MaxRetries      int
	BootTimeout     time.Duration
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
}

// SinkPostgres is [sink.postgres] (SPEC §10.5). The table is never created by
// the server. There is no TLS: the connection is always sslmode=disable
// (SPEC §1.2), so the database is reached over a trusted network or a unix
// socket.
type SinkPostgres struct {
	Addr     string // host:port, or a unix socket directory starting with /
	Username string
	Password string // secret
	Database string
	Table    string // name or schema.name, lowercase
	PoolSize int    // after Load: never 0 (workers + 1)
}

// LogHub is [loghub] (SPEC §10.9).
type LogHub struct {
	EBURL string // loghub.eb_url (LOGHUB_EB_URL); already applied to the sink keys
}

// Emergency is [emergency] (SPEC §12).
type Emergency struct {
	Enabled bool
	Dir     string // absolute; derived <data_dir>/emergency when not set
}

// Admin is [admin] (SPEC §14).
type Admin struct {
	Enabled bool
	Bind    string
}

// Log is [log] (SPEC §14.2).
type Log struct {
	Level        string  // reloadable: debug | info | warn | error
	Format       string  // json | text
	Access       bool    // reloadable
	AccessSample float64 // reloadable, [0, 1]
}

// Shutdown is [shutdown] (SPEC §13.4).
type Shutdown struct {
	PreStopDelay time.Duration // 0 = no wait
	DrainTimeout time.Duration // never 0
}

// TCP framing modes (SPEC §5.2).
const (
	FramingAuto  = "auto"
	FramingOctet = "octet"
	FramingLF    = "lf"
)

// PROXY protocol policies (SPEC §4.6).
const (
	ProxyOff      = "off"
	ProxyOptional = "optional"
	ProxyRequired = "required"
)

// Legacy charsets (SPEC §6.7); "" = none.
const (
	CharsetCP1252 = "cp1252"
	CharsetLatin1 = "latin1"
)

// Bounds of syslog.max_message_bytes (SPEC §5.5, §13.2). 480 is the minimum
// RFC 5424 §6.1 obliges a receiver to accept; 16 MiB bounds the worst JLH of
// §3.9 at about 192 MiB, under the 255 MiB a jsonb value may hold.
const (
	MinMessageBytes = 480
	MaxMessageBytes = 16 << 20
)

// eventFixedBytes is the constant part of the worst JLH (SPEC §3.9): keys,
// numbers, host, x-proxy, parse and the envelope header.
const eventFixedBytes = 8 << 10

// MaxEventBytesFor is the worst JLH for a raw line of at most l bytes
// (SPEC §3.9): converted text <= 6l (every byte a control escaped as \u00XX,
// the fields being disjoint slices of the raw), raw <= 6l, plus 8 KiB fixed.
//
// The number is pessimistic on purpose: a "realistic" factor would let one
// broken emitter take down batches of already-acknowledged messages from
// everyone else, because a size refusal is `logic` and drops the whole chunk.
func MaxEventBytesFor(l int64) int64 { return 12*l + eventFixedBytes }

// MaxEventBytes is the worst JLH under c (12 × syslog.max_message_bytes +
// 8 KiB; 776 KiB with the 64 KiB default). The emergency reader uses it as the
// longest line it accepts (SPEC §12.2).
func MaxEventBytes(c *Config) int64 { return MaxEventBytesFor(c.Syslog.MaxMessageBytes) }

// AnyListener reports whether at least one transport is configured. None is a
// boot error rather than a silent idle process.
func (c *Config) AnyListener() bool {
	return c.Listen.UDP != "" || c.Listen.TCP != "" || c.Listen.Unix != ""
}
