package config

import "time"

// Config holds every tunable of the server. Field values are written only by
// the registry (see registry.go); nothing else parses configuration input.
//
// Fields marked reloadable in the registry may change on SIGHUP; readers of
// those go through a snapshot rather than reading Config directly.
type Config struct {
	Node      Node
	Listen    Listen
	TCP       TCP
	RELP      RELP
	Parse     Parse
	Filter    Filter
	RateLimit RateLimit
	Ring      Ring
	RingSQL   RingSQLite
	Sink      Sink
	SinkRedis SinkRedis
	SinkSQL   SinkSQLite
	SinkPG    SinkPostgres
	SinkMySQL SinkMySQL
	SinkNATS  SinkNATS
	Emergency Emergency
	Admin     Admin
	Log       Log
	Shutdown  Shutdown
}

type Node struct {
	ID string
}

// Listen holds one address per transport. An empty address disables that
// transport, and all four empty is a configuration error (SPEC §12.1): a server
// that listens to nothing has no function.
type Listen struct {
	UDP            string
	UDPListeners   int
	UDPReadBuffer  int64
	TCP            string
	TCPListeners   int
	RELP           string
	RELPListeners  int
	Unix           string
	UnixMode       int
	UnixReadBuffer int64
}

type TCP struct {
	Framing             string // auto | octet | lf
	MaxFrameBytes       int64
	IdleTimeout         time.Duration
	MaxConns            int
	MaxConnsPerIP       int
	BackpressureTimeout time.Duration
}

type RELP struct {
	MaxFrameBytes       int64
	IdleTimeout         time.Duration
	MaxConns            int
	MaxConnsPerIP       int
	Window              int
	BackpressureTimeout time.Duration
}

type Parse struct {
	Prefer          string // auto | 5424 | 3164
	DefaultFacility int
	DefaultSeverity int
	SDMaxElements   int
	SDMaxParams     int
}

// Filter drops messages before the ring, by facility and severity. Both fields
// are reloadable: it is the knob an operator reaches for at three in the
// morning, when an emitter goes mad (SPEC §7.1).
type Filter struct {
	MinSeverity string   // name or number; resolved to 0..7
	Facilities  []string // names or numbers; empty accepts all
}

type RateLimit struct {
	// All fields reloadable.
	Enabled       bool
	Rate          float64
	Burst         float64
	V4Mask        int
	V6Mask        int
	Exempt        []string
	MaxBuckets    int
	SweepInterval time.Duration
}

type Ring struct {
	Mode          string // memory | sqlite | none
	CapacityItems int
	MaxBytes      int64
	HighWater     float64
	MaxInflight   int
	DataDir       string
}

type RingSQLite struct {
	Path              string
	Synchronous       string // FULL | NORMAL | OFF
	CommitBatch       int
	CommitMaxDelay    time.Duration
	BusyTimeout       time.Duration
	WALAutocheckpoint int
}

type Sink struct {
	Type            string // redis | sqlite | postgres | mysql | nats | none
	Workers         int
	Batch           int
	BatchMaxDelay   time.Duration
	OpTimeout       time.Duration
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	FailTimeout     time.Duration
	MaxRetries      int
	BootTimeout     time.Duration
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
}

type SinkRedis struct {
	Addrs               []string
	Username            string
	Password            string
	DB                  int
	SentinelMaster      string
	TLS                 bool
	TLSCA               string
	TLSInsecure         bool
	Stream              string
	MaxLen              int64
	RingScaleEachConn   int
	PipelineMultiplex   int
	ReadBufferEachConn  int64
	WriteBufferEachConn int64
}

type SinkSQLite struct {
	Path              string
	Rotate            string // none | hourly | daily
	KeepFiles         int    // reloadable; 0 = never delete
	Synchronous       string
	BusyTimeout       time.Duration
	WALAutocheckpoint int
}

// SinkPostgres is the shared final destination of SPEC §10.5. The table is
// never created by the server: it validates the schema at boot and refuses to
// start when it does not match §3.6.
type SinkPostgres struct {
	Addr        string // host:port, or a unix socket directory
	Username    string
	Password    string
	Database    string
	Table       string // name or schema.name, lowercase
	PoolSize    int    // 0 derives sink.workers + 1
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

// SinkMySQL is the second shared, schema-bearing destination (SPEC §10.7). Like
// postgres, the table is validated at boot and never created, and the same
// connection budget applies, because every worker pins a connection.
type SinkMySQL struct {
	Addr        string // host:port, or a unix socket path starting with /
	Username    string
	Password    string
	Database    string
	Table       string // name or database.name, case kept
	PoolSize    int    // 0 derives sink.workers + 1
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

// SinkNATS is the JetStream destination (SPEC §10.8). The stream is validated
// at boot and never created. At most one of credentials, token and username may
// be set: the client library would otherwise pick one silently.
type SinkNATS struct {
	URLs        []string
	Username    string
	Password    string
	Token       string
	Credentials string // path to a .creds file (JWT + NKey seed)
	TLS         bool
	TLSCA       string
	TLSInsecure bool
	Stream      string
	Subject     string
}

type Emergency struct {
	Enabled bool
	Dir     string
}

type Admin struct {
	Enabled bool
	Bind    string
}

type Log struct {
	Level        string // reloadable
	Format       string
	Access       bool    // reloadable
	AccessSample float64 // reloadable
}

type Shutdown struct {
	PreStopDelay time.Duration
	DrainTimeout time.Duration
}

// Ring modes.
const (
	RingMemory     = "memory"
	RingSQLiteMode = "sqlite"
	RingNone       = "none"
)

// Sink types.
const (
	SinkRedisType    = "redis"
	SinkSQLiteType   = "sqlite"
	SinkPostgresType = "postgres"
	SinkMySQLType    = "mysql"
	SinkNATSType     = "nats"
	SinkNoneType     = "none"
)

// TCP framing modes (SPEC §5.2).
const (
	FramingAuto  = "auto"
	FramingOctet = "octet"
	FramingLF    = "lf"
)

// Parse preference (SPEC §6.2).
const (
	PreferAuto = "auto"
	Prefer5424 = "5424"
	Prefer3164 = "3164"
)

// AnyListener reports whether at least one transport is configured. None is a
// boot error rather than a silent idle process.
func (c *Config) AnyListener() bool {
	return c.Listen.UDP != "" || c.Listen.TCP != "" ||
		c.Listen.RELP != "" || c.Listen.Unix != ""
}
