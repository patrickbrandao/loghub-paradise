// Package config is the single registry of every tunable of go-syslog-server
// (SPEC §16). One table (registry.go) declares each parameter, and from it come
// the INI parser, the command-line flags, the environment, --help,
// --dump-config, the SIGHUP comparison and the validation. A new parameter is
// added there and nowhere else.
//
// The package is pure configuration: it never opens a socket, never touches the
// identity library and never writes to disk. The checks of SPEC §13.2 that need
// the network or the identity (space grammar, stream coverage, max_payload) are
// the application's; the ones that depend only on configuration are here.
package config

import (
	"fmt"
	"net/netip"
	"strings"
	"time"

	// The image embeds the timezone database so syslog.rfc3164_timezone means
	// the same thing on every host (SPEC §15.4). Importing it here, next to the
	// validation that calls time.LoadLocation, makes the guarantee local: any
	// binary that accepts a zone at boot can also load it in the converter,
	// whatever the host's /usr/share/zoneinfo holds.
	_ "time/tzdata"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
)

// Config holds every tunable of the server. Field values are written only by
// the registry (see registry.go) and by the derivation and validation steps of
// Load; nothing else parses configuration input.
//
// After a successful Load:
//   - every path the service writes is absolute (Server.DataDir, RingSQL.Path,
//     SinkSQL.Path, Emergency.Dir);
//   - enumerations hold their canonical spelling (the constants below);
//   - LOGHUB_EB_URL, when set, has already been expanded into the sink keys;
//   - validation adjustments are applied (sink.workers forced or lowered,
//     pool sizes derived, ring.mode degraded to memory when ring and sink are
//     the same Redis).
//
// Fields marked reloadable in the registry may change on SIGHUP; readers of
// those go through a snapshot rather than reading Config directly.
type Config struct {
	Server    Server
	Listen    Listen
	TCP       TCP
	TLS       TLS
	Proxy     Proxy
	Syslog    Syslog
	Filter    Filter
	RateLimit RateLimit
	Ring      Ring
	RingSQL   RingSQLite
	RingRedis RingRedis
	Sink      Sink
	SinkNATS  SinkNATS
	SinkRedis SinkRedis
	SinkSQL   SinkSQLite
	SinkPG    SQLDB // [sink.postgres]
	SinkMySQL SQLDB // [sink.mysql]
	SinkCH    SinkClickHouse
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
// transport, and all four empty is a configuration error (SPEC §4): a server
// that listens to nothing has no function.
type Listen struct {
	UDP            string // listen.udp
	UDPListeners   int    // listen.udp_listeners; >1 enables SO_REUSEPORT
	UDPReadBuffer  int64  // listen.udp_read_buffer, bytes (SO_RCVBUF)
	TCP            string // listen.tcp
	TCPListeners   int    // listen.tcp_listeners
	TLS            string // listen.tls
	TLSListeners   int    // listen.tls_listeners
	Unix           string // listen.unix, absolute path
	UnixMode       int    // listen.unix_mode, a file mode (0666 = 0o666)
	UnixReadBuffer int64  // listen.unix_read_buffer, bytes
}

// ConnLimits are the per-transport connection knobs that [tcp] and [tls] both
// carry, with the same defaults: they are distinct transports with distinct
// limiters (SPEC §4.5), so neither inherits the other's values.
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

// TLS is [tls] (SPEC §4.5). The certificate is read at boot only.
type TLS struct {
	CertFile         string        // tls.cert_file; required with listen.tls
	KeyFile          string        // tls.key_file; required with listen.tls
	ClientAuth       string        // tls.client_auth: ClientAuthOptional | ClientAuthRequire | ClientAuthNone
	ClientCA         string        // tls.client_ca; required with client_auth=require
	MinVersion       string        // tls.min_version: TLSVersion12 | TLSVersion13
	HandshakeTimeout time.Duration // tls.handshake_timeout; never 0
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

// Filter drops messages before the ring, by facility and severity. Both fields
// are reloadable: it is the knob an operator reaches for at three in the
// morning, when an emitter goes mad (SPEC §7). Use Resolve to get the codes.
type Filter struct {
	MinSeverity string   // filter.min_severity: lowercase name or number; resolved to 0..7
	Facilities  []string // filter.facilities: lowercase names or numbers; empty accepts all
}

// Resolve turns the textual filter into codes: the minimum severity (accept
// severity <= it) and the accepted facilities (nil = all).
func (f Filter) Resolve() (minSeverity int, facilities []int, err error) {
	sev, ok := ParseSeverity(f.MinSeverity)
	if !ok {
		return 0, nil, fmt.Errorf("filter.min_severity: %q is not a severity name or a number 0..7", f.MinSeverity)
	}
	for _, s := range f.Facilities {
		fac, ok := ParseFacility(s)
		if !ok {
			return 0, nil, fmt.Errorf("filter.facilities: %q is not a facility name or a number 0..23", s)
		}
		facilities = append(facilities, fac)
	}
	return sev, facilities, nil
}

// RateLimit is [ratelimit] (SPEC §8). All fields reloadable.
type RateLimit struct {
	Enabled       bool
	Rate          float64 // tokens per second per prefix
	Burst         float64
	V4Mask        int
	V6Mask        int
	Exempt        []netip.Prefix
	MaxBuckets    int
	SweepInterval time.Duration
}

// Ring is [ring] (SPEC §9).
type Ring struct {
	Mode          string  // ring.mode: RingModeMemory | RingModeSQLite | RingModeRedis | RingModeNone
	CapacityItems int     // global with redis
	MaxBytes      int64   // bytes of raw lines; global with redis
	HighWater     float64 // (0, 1]
	MaxInflight   int     // concurrent synchronous writes with mode=none
}

// RingSQLite is [ring.sqlite] (SPEC §9.4).
type RingSQLite struct {
	Path              string // absolute; derived <data_dir>/ring.db when not set
	Synchronous       string // FULL | NORMAL | OFF
	CommitBatch       int
	CommitMaxDelay    time.Duration
	BusyTimeout       time.Duration
	WALAutocheckpoint int
}

// RedisConn is the connection block shared by [ring.redis] and [sink.redis].
type RedisConn struct {
	Addrs          []string
	Username       string
	Password       string // secret: redacted in the dump
	DB             int
	SentinelMaster string
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
}

// ClientConfig converts the block for internal/rediscli. clientName is what
// the server shows in CLIENT LIST (AgentName/Hostname, never a node id).
func (r RedisConn) ClientConfig(clientName string) rediscli.Config {
	return rediscli.Config{
		Addrs:          append([]string(nil), r.Addrs...),
		Username:       r.Username,
		Password:       r.Password,
		DB:             r.DB,
		SentinelMaster: r.SentinelMaster,
		TLS:            r.TLS,
		TLSCA:          r.TLSCA,
		TLSInsecure:    r.TLSInsecure,
		ClientName:     clientName,
	}
}

// RingRedis is [ring.redis] (SPEC §9.5).
type RingRedis struct {
	RedisConn
	Stream    string        // ring.redis.stream
	Group     string        // ring.redis.group
	ClaimIdle time.Duration // ring.redis.claim_idle; never 0
}

// Sink is [sink] (SPEC §10.1).
type Sink struct {
	Type            string // sink.type: SinkType* constants
	Workers         int    // after Load: 1 with sqlite; 4 with postgres/mysql when not set
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

// SinkNATS is [sink.nats] (SPEC §10.7). The subject is derived, never
// configured: loghub.<space>.syslog (or the facility-severity form, §3.8).
// At most one of Username/Password, Token and Credentials is set.
type SinkNATS struct {
	URLs        []string // secret-bearing: userinfo redacted in the dump
	Username    string
	Password    string // secret
	Token       string // secret
	Credentials string // path to a .creds file
	TLS         bool
	TLSCA       string
	TLSInsecure bool
	Mode        string // NATSModeJetStream | NATSModeCore
	Stream      string // validated, never created; ignored in core
	Subjects    string // SubjectsExact | SubjectsFacilitySeverity
}

// SinkRedis is [sink.redis] (SPEC §10.3).
type SinkRedis struct {
	RedisConn
	Stream              string // may contain {space}; see StreamFor
	MaxLen              int64  // 0 = no trimming
	RingScaleEachConn   int
	PipelineMultiplex   int
	ReadBufferEachConn  int64
	WriteBufferEachConn int64
}

// ClientConfig is RedisConn.ClientConfig plus the rueidis memory knobs that
// only the sink exposes.
func (s SinkRedis) ClientConfig(clientName string) rediscli.Config {
	c := s.RedisConn.ClientConfig(clientName)
	c.RingScaleEachConn = s.RingScaleEachConn
	c.PipelineMultiplex = s.PipelineMultiplex
	c.ReadBufferEachConn = s.ReadBufferEachConn
	c.WriteBufferEachConn = s.WriteBufferEachConn
	return c
}

// StreamFor expands {space} in sink.redis.stream to the Workspace (SPEC §3.6):
// the default loghub.{space}.syslog is the same name as the NATS subject.
func (s SinkRedis) StreamFor(space string) string {
	return strings.ReplaceAll(s.Stream, SpacePlaceholder, space)
}

// SinkSQLite is [sink.sqlite] (SPEC §10.4).
type SinkSQLite struct {
	Path              string // absolute; derived <data_dir>/events.db when not set
	Rotate            string // RotateNone | RotateHourly | RotateDaily (UTC)
	KeepFiles         int    // reloadable; 0 = never delete
	Synchronous       string // FULL | NORMAL | OFF
	BusyTimeout       time.Duration
	WALAutocheckpoint int
}

// SQLDB is [sink.postgres] and [sink.mysql] (SPEC §10.5, §10.6): the same keys,
// different defaults. The table is never created by the server.
type SQLDB struct {
	Addr        string // host:port, or a unix socket starting with /
	Username    string
	Password    string // secret
	Database    string
	Table       string // name or schema.name (postgres: lowercase; mysql: case kept)
	PoolSize    int    // after Load: never 0 when this sink is selected (sink.workers)
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

// SinkClickHouse is [sink.clickhouse] (SPEC §10.10). Its own struct rather than
// SQLDB: the native protocol takes several addresses, and there is no unix
// socket.
type SinkClickHouse struct {
	Addrs       []string // host:port of the native protocol
	Username    string
	Password    string // secret
	Database    string
	Table       string // name or database.name, case kept
	PoolSize    int    // after Load: never 0 when this sink is selected (sink.workers)
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

// LogHub is [loghub] (SPEC §10.9, §11).
type LogHub struct {
	EBURL          string        // loghub.eb_url (LOGHUB_EB_URL); already applied to the sink keys
	BeaconInterval time.Duration // loghub.beacon_interval (LOGHUB_BEACON_INTERVAL); 0 = no periodic beacon
	CtlNamespace   string        // loghub.ctl_namespace (LOGHUB_CTL_NAMESPACE)
}

// BeaconSubject is <ctl_namespace>.beacon (SPEC §11).
func (l LogHub) BeaconSubject() string { return l.CtlNamespace + ".beacon" }

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

// Ring modes (SPEC §9).
const (
	RingModeMemory = "memory"
	RingModeSQLite = "sqlite"
	RingModeRedis  = "redis"
	RingModeNone   = "none"
)

// Sink types (SPEC §10).
const (
	SinkTypeNATS       = "nats"
	SinkTypeRedis      = "redis"
	SinkTypeSQLite     = "sqlite"
	SinkTypePostgres   = "postgres"
	SinkTypeMySQL      = "mysql"
	SinkTypeClickHouse = "clickhouse"
	SinkTypeNone       = "none"
)

// TCP framing modes (SPEC §5.2).
const (
	FramingAuto  = "auto"
	FramingOctet = "octet"
	FramingLF    = "lf"
)

// Client certificate policies (SPEC §4.5).
const (
	ClientAuthOptional = "optional"
	ClientAuthRequire  = "require"
	ClientAuthNone     = "none"
)

// TLS minimum versions (SPEC §4.5).
const (
	TLSVersion12 = "1.2"
	TLSVersion13 = "1.3"
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

// NATS publish modes and subject layouts (SPEC §3.8, §10.7).
const (
	NATSModeJetStream        = "jetstream"
	NATSModeCore             = "core"
	SubjectsExact            = "exact"
	SubjectsFacilitySeverity = "facility-severity"
)

// sink.sqlite.rotate values (SPEC §10.4).
const (
	RotateNone   = "none"
	RotateHourly = "hourly"
	RotateDaily  = "daily"
)

// SpacePlaceholder is expanded to the Workspace in sink.redis.stream.
const SpacePlaceholder = "{space}"

// Bounds of syslog.max_message_bytes (SPEC §5.5, §13.2). 480 is the minimum
// RFC 5424 §6.1 obliges a receiver to accept; 16 MiB keeps the worst JLH of
// §3.9 within reach of a raised NATS max_payload.
const (
	MinMessageBytes = 480
	MaxMessageBytes = 16 << 20
)

// eventFixedBytes is the constant part of the worst JLH (SPEC §3.9): keys,
// numbers, host, tls (peer-subject capped at 1 KiB), x-proxy, parse and the
// envelope header.
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
// 8 KiB; 776 KiB with the 64 KiB default). The boot compares it with the NATS
// max_payload and the stream's max_msg_size and refuses with exit 3 when it
// does not fit.
func MaxEventBytes(c *Config) int64 { return MaxEventBytesFor(c.Syslog.MaxMessageBytes) }

// MaxMySQLRowBytes is the largest row the MySQL sink can send (SPEC §3.9,
// §10.6): the worst JLH in env, plus the sanitised text columns (at most 3 × L
// together, since U+FFFD is 3 bytes per original octet), plus 512 bytes of
// fixed columns. max_allowed_packet must be at least this.
func MaxMySQLRowBytes(c *Config) int64 {
	l := c.Syslog.MaxMessageBytes
	return MaxEventBytesFor(l) + 3*l + 512
}

// WorstChunkBytes is the most JLH bytes one sink write can carry (SPEC §10.1,
// §10.10): a chunk closes past sink.batch_max_bytes, and a JLH larger than the
// ceiling forms a chunk of its own. The ClickHouse boot compares the server's
// memory limit with what an INSERT of that size costs.
func WorstChunkBytes(c *Config) int64 {
	return max(c.Sink.BatchMaxBytes, MaxEventBytes(c))
}

// AnyListener reports whether at least one transport is configured. None is a
// boot error rather than a silent idle process.
func (c *Config) AnyListener() bool {
	return c.Listen.UDP != "" || c.Listen.TCP != "" ||
		c.Listen.TLS != "" || c.Listen.Unix != ""
}
