package config

import "time"

// Config holds every tunable of the server. Field values are written only by the
// registry (see registry.go); nothing else parses configuration input.
//
// Fields marked reloadable in the registry may change on SIGHUP; readers of
// those go through an atomic snapshot rather than reading Config directly (the
// dynamic type in internal/server/server.go), and the reload itself compares
// Loaded snapshots, never this struct (internal/app/reload.go).
type Config struct {
	Server    Server
	Admin     Admin
	Proxy     Proxy
	RateLimit RateLimit
	Ring      Ring
	RingSQL   RingSQLite
	Sink      Sink
	SinkRedis SinkRedis
	SinkSQL   SinkSQLite
	SinkPG    SinkPostgres
	SinkMySQL SinkMySQL
	SinkNATS  SinkNATS
	SinkCH    SinkClickHouse
	Emergency Emergency
	Headers   Headers
	Log       Log
	Shutdown  Shutdown
}

type Server struct {
	Bind                      string
	Listeners                 int
	ReadHeaderTimeout         time.Duration
	ReadTimeout               time.Duration
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	MaxBodyBytes              int64
	MaxHeaderBytes            int64
	MaxURIBytes               int64
	MaxConns                  int
	MaxConnsPerIP             int
	RejectDrainBytes          int64
	HTTP2                     bool
	HTTP2MaxConcurrentStreams int
}

type Admin struct {
	Enabled bool
	Bind    string
}

type Proxy struct {
	Trusted       []string // CIDRs; reloadable
	MaxXFFEntries int      // reloadable
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

// SinkPostgres is the shared final destination of SPEC §8.5. The table is
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

// SinkMySQL is the second shared, schema-bearing destination (SPEC §8.6). Like
// postgres, the table is validated at boot and never created; the same
// connection budget applies, because every worker pins a connection.
type SinkMySQL struct {
	Addr        string // host:port, or a unix socket path starting with /
	Username    string
	Password    string
	Database    string
	Table       string // name or database.name
	PoolSize    int    // 0 derives sink.workers + 1
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

// SinkNATS is the JetStream destination (SPEC §8.7). The stream is validated
// at boot and never created. Exactly one of credentials, token and username
// may be set: the client library would otherwise pick one silently.
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

// SinkClickHouse is the analytical destination (SPEC §8.8). The table is
// validated at boot and never created, and every worker pins a connection out
// of the same fleet-wide budget as the relational destinations.
type SinkClickHouse struct {
	Addrs       []string // host:port of the native protocol
	Username    string
	Password    string
	Database    string
	Table       string // name or database.name
	PoolSize    int    // 0 derives sink.workers + 1
	TLS         bool
	TLSCA       string
	TLSInsecure bool
}

type Emergency struct {
	Enabled bool
	Dir     string
}

type Headers struct {
	Exclude []string // reloadable
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
	SinkRedisType      = "redis"
	SinkSQLiteType     = "sqlite"
	SinkPostgresType   = "postgres"
	SinkMySQLType      = "mysql"
	SinkNATSType       = "nats"
	SinkClickHouseType = "clickhouse"
	SinkNoneType       = "none"
)
