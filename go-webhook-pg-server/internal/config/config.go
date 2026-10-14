package config

import "time"

// Config holds every tunable of the server. Field values are written only by the
// registry (see registry.go); nothing else parses configuration input. It is
// read once, at boot: this server has no runtime reload (SPEC §13.3).
type Config struct {
	Server    Server
	Admin     Admin
	Proxy     Proxy
	RateLimit RateLimit
	Ring      Ring
	Sink      Sink
	SinkPG    SinkPostgres
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
	Trusted       []string // CIDRs
	MaxXFFEntries int
}

// RateLimit survives only as a compatibility selector (SPEC §0, §6): an INI
// written for go-webhook-fast-server may say enabled=false, and must still
// load; one that turns the limiter on must not start without it.
type RateLimit struct {
	Enabled bool
}

type Ring struct {
	Mode          string // compatibility selector: memory only (SPEC §0)
	CapacityItems int
	MaxBytes      int64
	HighWater     float64
	DataDir       string
}

type Sink struct {
	Type            string // compatibility selector: postgres only (SPEC §0)
	Workers         int
	Batch           int
	BatchMaxBytes   int64 // JLH bytes per write: a batch goes out in pieces under it (SPEC §8.1)
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

type Emergency struct {
	Enabled bool
	Dir     string
}

type Headers struct {
	Exclude []string
}

type Log struct {
	Level        string
	Format       string
	Access       bool
	AccessSample float64
}

type Shutdown struct {
	PreStopDelay time.Duration
	DrainTimeout time.Duration
}

// The one value each compatibility selector accepts (SPEC §0).
const (
	RingMemory       = "memory"
	SinkPostgresType = "postgres"
)
