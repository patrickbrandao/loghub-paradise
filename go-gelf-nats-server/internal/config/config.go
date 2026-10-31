// Package config is the single registry of every tunable of go-gelf-nats-server
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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/convert"
)

// Config holds every tunable of the server. Field values are written only by
// the registry (see registry.go) and by the derivation and validation steps of
// Load; nothing else parses configuration input.
//
// After a successful Load:
//   - every path the service writes is absolute (Server.DataDir, Emergency.Dir);
//   - enumerations hold their canonical spelling (the constants below);
//   - LOGHUB_EB_URL, when set, has already been expanded into the sink keys;
//   - the strings are trimmed where a space would change the meaning.
//
// Fields marked reloadable in the registry may change on SIGHUP; readers of
// those go through a snapshot rather than reading Config directly.
type Config struct {
	Server    Server
	Listen    Listen
	TCP       TCP
	HTTP      HTTP
	Proxy     Proxy
	Convert   Convert
	Ring      Ring
	Sink      Sink
	SinkNATS  SinkNATS // [sink.nats]
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
	UDP                      string // listen.udp
	UDPListeners             int    // listen.udp_listeners; >1 enables SO_REUSEPORT
	UDPReadBuffer            int64  // listen.udp_read_buffer, bytes (SO_RCVBUF)
	UDPReassemblyMaxBytes    int64  // listen.udp_reassembly_max_bytes, 1MiB..4GiB
	UDPReassemblyMaxMessages int    // listen.udp_reassembly_max_messages, 1..1048576
	TCP                      string // listen.tcp
	TCPListeners             int    // listen.tcp_listeners
	HTTP                     string // listen.http
}

// ConnLimits are the connection knobs of [tcp] (SPEC §4.4). A struct of its own
// because the receiver takes them as one value per stream transport.
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

// Proxy is [proxy] (SPEC §4.6).
type Proxy struct {
	Protocol      string         // proxy.protocol: ProxyOff | ProxyOptional | ProxyRequired
	Trusted       []netip.Prefix // proxy.trusted; masked prefixes, bare IPs as /32 or /128
	HeaderTimeout time.Duration  // proxy.header_timeout; never 0
}

// HTTP is [http] (SPEC §4.5): the keys of the one http listener, with its own
// limiters (there is no https, §1.2). The three timeouts are never 0.
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

// SinkNATS is [sink.nats] (SPEC §10.7). The subject is derived, never
// configured: loghub.<space>.gelf (§3.8). At most one of Username/Password,
// Token and Credentials is set. There is no TLS: the connection is always
// plaintext (SPEC §1.2).
type SinkNATS struct {
	URLs        []string // secret-bearing: userinfo redacted in the dump
	Username    string
	Password    string // secret
	Token       string // secret
	Credentials string // path to a .creds file
	Mode        string // NATSModeJetStream | NATSModeCore
	Stream      string // validated, never created; ignored in core
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

// convert.keep_raw values (SPEC §6.9).
const (
	KeepRawMalformed = "malformed"
	KeepRawAlways    = "always"
)

// NATS publish modes (SPEC §10.7).
const (
	NATSModeJetStream = "jetstream"
	NATSModeCore      = "core"
)

// PROXY protocol policies (SPEC §4.6).
const (
	ProxyOff      = "off"
	ProxyOptional = "optional"
	ProxyRequired = "required"
)

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

// AnyListener reports whether at least one transport is configured. None is a
// boot error rather than a silent idle process.
func (c *Config) AnyListener() bool {
	return c.Listen.UDP != "" || c.Listen.TCP != "" || c.Listen.HTTP != ""
}
