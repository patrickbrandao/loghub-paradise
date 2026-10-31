// Package config is the single registry of every tunable of go-gelf-server
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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/rediscli"
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
	HTTP      HTTP
	Proxy     Proxy
	Convert   Convert
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
// transport, and all five empty is a configuration error (SPEC §4): a server
// that listens to nothing has no function.
type Listen struct {
	UDP                      string // listen.udp
	UDPListeners             int    // listen.udp_listeners; >1 enables SO_REUSEPORT
	UDPReadBuffer            int64  // listen.udp_read_buffer, bytes (SO_RCVBUF)
	UDPReassemblyMaxBytes    int64  // listen.udp_reassembly_max_bytes, 1MiB..4GiB
	UDPReassemblyMaxMessages int    // listen.udp_reassembly_max_messages, 1..1048576
	TCP                      string // listen.tcp
	TCPListeners             int    // listen.tcp_listeners
	TLS                      string // listen.tls
	TLSListeners             int    // listen.tls_listeners
	HTTP                     string // listen.http
	HTTPS                    string // listen.https; uses the certificate of [tls]
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

// TCP is [tcp] (SPEC §4.4, §5). GELF over TCP is always \0-framed, so there
// is no framing key.
type TCP struct {
	ConnLimits
}

// TLS is [tls] (SPEC §4.5). The certificate is read at boot only.
type TLS struct {
	CertFile         string        // tls.cert_file; required with listen.tls or listen.https
	KeyFile          string        // tls.key_file; required with listen.tls or listen.https
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

// HTTP is [http] (SPEC §4.5): the keys of both http and https, each with
// its own limiters. The three timeouts are never 0.
type HTTP struct {
	ReadHeaderTimeout  time.Duration // http.read_header_timeout
	ReadTimeout        time.Duration // http.read_timeout
	IdleTimeout        time.Duration // http.idle_timeout; keep-alive wait between requests
	MaxRequestsPerConn int           // http.max_requests_per_conn; 0 = no cap
	MaxConns           int           // http.max_conns; 0 = unlimited; blocks in accept
	MaxConnsPerIP      int           // http.max_conns_per_ip; 0 = unlimited; closes at once
	Bulk               bool          // http.bulk; one document per line of the body (SPEC §4.5.1)
	BulkMaxBytes       int64         // http.bulk_max_bytes; the decompressed body with bulk
}

// Convert is [convert]: the conversion config. Every key requires a restart
// (SPEC §16.3), because the JLH must be a pure function of header + raw +
// this struct (§3.5).
type Convert struct {
	KeepRaw         string // convert.keep_raw: KeepRawMalformed | KeepRawAlways
	MaxMessageBytes int64  // convert.max_message_bytes, MinMessageBytes..MaxMessageBytes
	AcceptMalformed bool   // convert.accept_malformed
}

// KeepRawAlways reports whether payload.raw is written for every document.
func (c Convert) KeepRawAlways() bool { return c.KeepRaw == KeepRawAlways }

// Filter drops documents before the ring, by GELF level. Both fields are
// reloadable: it is the knob an operator reaches for at three in the morning,
// when an emitter goes mad (SPEC §7). internal/filter.Compile turns them into
// a rule; Resolve parses them with the same filter.ParseLevel, so a value
// validation accepts is by construction one the filter compiles (the
// syslog server kept a copy of the name table here, pinned by a test).
type Filter struct {
	MinLevel string   // filter.min_level: lowercase name or number; resolved to 0..7
	Levels   []string // filter.levels: lowercase names or numbers; empty accepts all
}

// Resolve turns the textual filter into codes: the minimum level (accept
// level <= it) and the accepted levels (nil = all). Blank list entries are
// skipped, as internal/filter.Compile skips them.
func (f Filter) Resolve() (minLevel int, levels []int, err error) {
	minLevel, ok := filter.ParseLevel(f.MinLevel)
	if !ok {
		return 0, nil, fmt.Errorf("filter.min_level: %q is not a level name (%s) or a number 0..7",
			f.MinLevel, strings.Join(convert.LevelNames[:], ", "))
	}
	for _, s := range f.Levels {
		if strings.TrimSpace(s) == "" {
			continue
		}
		l, ok := filter.ParseLevel(s)
		if !ok {
			return 0, nil, fmt.Errorf("filter.levels: %q is not a level name (%s) or a number 0..7",
				s, strings.Join(convert.LevelNames[:], ", "))
		}
		levels = append(levels, l)
	}
	return minLevel, levels, nil
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
// configured: loghub.<space>.gelf (§3.8).
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
// the default loghub.{space}.gelf is the same name as the NATS subject.
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

// convert.keep_raw values (SPEC §6.9).
const (
	KeepRawMalformed = "malformed"
	KeepRawAlways    = "always"
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

// NATS publish modes (SPEC §10.7).
const (
	NATSModeJetStream = "jetstream"
	NATSModeCore      = "core"
)

// sink.sqlite.rotate values (SPEC §10.4).
const (
	RotateNone   = "none"
	RotateHourly = "hourly"
	RotateDaily  = "daily"
)

// SpacePlaceholder is expanded to the Workspace in sink.redis.stream.
const SpacePlaceholder = "{space}"

// Bounds of convert.max_message_bytes (SPEC §5.5, §13.2). 1 KiB is below any
// GELF document worth receiving and above the smallest a UDP chunk carries;
// 16 MiB keeps the worst JLH of §3.9 within reach of a raised NATS
// max_payload.
const (
	MinMessageBytes = 1 << 10
	MaxMessageBytes = 16 << 20
)

// MaxEventBytesFor is the worst JLH for a document of at most l bytes
// (SPEC §3.9): convert.WorstJLH, 6 × l + 8 KiB. The bound is the converter's
// and is proved by its tests; config only asks it, so the two cannot drift.
//
// The number is pessimistic on purpose: a "realistic" factor would let one
// broken emitter take down batches of already-acknowledged messages from
// everyone else, because a size refusal is `logic` and drops the whole chunk.
func MaxEventBytesFor(l int64) int64 { return int64(convert.WorstJLH(int(l))) }

// MaxEventBytes is the worst JLH under c (6 × convert.max_message_bytes +
// 8 KiB; 776 KiB with the 128 KiB default). The boot compares it with the
// NATS max_payload and the stream's max_msg_size and refuses with exit 3 when
// it does not fit.
func MaxEventBytes(c *Config) int64 { return MaxEventBytesFor(c.Convert.MaxMessageBytes) }

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
	return c.Listen.UDP != "" || c.Listen.TCP != "" || c.Listen.TLS != "" ||
		c.Listen.HTTP != "" || c.Listen.HTTPS != ""
}
