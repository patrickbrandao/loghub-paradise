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
	SinkNATS  SinkNATS
	LogHub    LogHub
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
	Type            string // compatibility selector: nats only (SPEC §0)
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

// SinkNATS is the only destination, SPEC §8.7. The stream is validated at boot
// and never created; the subject is derived, never configured (§3.8). Exactly
// one of credentials, token and username may be set: the client library would
// otherwise pick one silently.
type SinkNATS struct {
	URLs        []string // nats:// only (SPEC §8.7.1)
	Username    string
	Password    string
	Token       string
	Credentials string // path to a .creds file (JWT + NKey seed)
	Mode        string // NATSModeJetStream | NATSModeCore
	Stream      string

	// Subject and TLS are compatibility selectors (SPEC §0): an INI written
	// for go-webhook-fast-server may spell either, and loads only with the
	// one value this server implements -- the derived subject and no TLS.
	Subject string
	TLS     bool
}

// LogHub is [loghub] (SPEC §8.7.10, §8.7.11): the keys whose environment names
// the LogHub skill fixes.
type LogHub struct {
	EBURL          string        // LOGHUB_EB_URL; already applied to the sink keys
	BeaconInterval time.Duration // LOGHUB_BEACON_INTERVAL; 0 = no periodic beacon
	CtlNamespace   string        // LOGHUB_CTL_NAMESPACE
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
	RingMemory          = "memory"
	SinkNATSType        = "nats"
	NATSSubjectTemplate = "loghub.{space}.{type}"
)

// NATS publish modes (SPEC §8.7.2).
const (
	NATSModeJetStream = "jetstream"
	NATSModeCore      = "core"
)
