package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// RedisConfig mirrors [sink.redis].
type RedisConfig struct {
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
	ClientName          string
}

// Redis writes each message as one XADD with three fields (SPEC §3.3):
//
//	XADD <stream> * v 1 e <envelope json> b <raw syslog line>
//
// The line goes in as raw bytes with no escaping and no base64: Redis stream
// field values are binary safe, and rueidis.BinaryString converts []byte to
// string without copying.
//
// The b field carries the syslog message and NOTHING of the transport: no octet
// count, no trailing newline, no RELP frame header. Those live in the envelope's
// transport and framing fields.
type Redis struct {
	cfg    RedisConfig
	client rueidis.Client
	log    *slog.Logger
	m      *obs.Metrics

	// probeKey is where the write half of the health check aims. It never
	// exists, and NOMKSTREAM guarantees the probe does not create it.
	probeKey string
}

// NewRedis connects and verifies the connection with one health check.
func NewRedis(ctx context.Context, cfg RedisConfig, log *slog.Logger, m *obs.Metrics) (*Redis, error) {
	opt := rueidis.ClientOption{
		InitAddress: cfg.Addrs,
		Username:    cfg.Username,
		Password:    cfg.Password,
		SelectDB:    cfg.DB,
		ClientName:  cfg.ClientName,
		// Client-side caching is a read optimisation; this client only writes.
		// Disabling it also drops the RESP3 requirement, so an older Redis or a
		// proxy in front of one still works.
		DisableCache: true,
	}
	if cfg.RingScaleEachConn > 0 {
		opt.RingScaleEachConn = cfg.RingScaleEachConn
	}
	if cfg.PipelineMultiplex != 0 {
		opt.PipelineMultiplex = cfg.PipelineMultiplex
	}
	if cfg.ReadBufferEachConn > 0 {
		opt.ReadBufferEachConn = int(cfg.ReadBufferEachConn)
	}
	if cfg.WriteBufferEachConn > 0 {
		opt.WriteBufferEachConn = int(cfg.WriteBufferEachConn)
	}
	if cfg.SentinelMaster != "" {
		opt.Sentinel = rueidis.SentinelOption{
			MasterSet: cfg.SentinelMaster,
			Username:  cfg.Username,
			Password:  cfg.Password,
		}
	}
	if cfg.TLS {
		tc, err := buildTLS(cfg)
		if err != nil {
			return nil, err
		}
		opt.TLSConfig = tc
		if cfg.SentinelMaster != "" {
			opt.Sentinel.TLSConfig = tc
		}
	}

	client, err := rueidis.NewClient(opt)
	if err != nil {
		// rueidis dials AND authenticates inside NewClient, so a bad password
		// or a missing ACL comes back here as a Redis error reply rather than
		// from the first command. It goes through the same classification as
		// any reply: fatal at once (SPEC §10.3), not retried as a connection
		// failure until sink.boot_timeout runs out.
		return nil, classifyRedis("dial", err, "")
	}

	r := &Redis{cfg: cfg, client: client, log: log, m: m,
		probeKey: cfg.Stream + ":healthprobe"}
	if err := r.Health(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return r, nil
}

func buildTLS(cfg RedisConfig) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLSInsecure {
		tc.InsecureSkipVerify = true
	}
	if cfg.TLSCA != "" {
		pem, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("sink redis: reading %s: %w", cfg.TLSCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("sink redis: %s contains no usable certificate", cfg.TLSCA)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func (r *Redis) Type() string { return "redis" }

// Write sends the batch as one pipelined DoMulti.
//
// The commands are BUILT FRESH on every call, and that is not an oversight to
// optimise away later. rueidis recycles a Completed command into a sync.Pool
// once it has been executed; reusing the slice on a retry would send corrupted
// data with no error anywhere. Since a failed batch is retried by the worker
// (SPEC §10.2), rebuilding here is what keeps the retry correct.
//
// Build().Pin() would be the alternative, but it makes correctness depend on
// nobody ever forgetting a Pin. Rebuilding cannot be forgotten.
func (r *Redis) Write(ctx context.Context, batch []Prepared) error {
	if len(batch) == 0 {
		return nil
	}

	cmds := make(rueidis.Commands, 0, len(batch))
	for i := range batch {
		cmds = append(cmds, r.buildXAdd(batch[i]))
	}

	resps := r.client.DoMulti(ctx, cmds...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			return classifyRedis("xadd", err, batch[i].Item.Head.ID)
		}
	}
	return nil
}

// buildXAdd assembles one XADD.
//
// The two branches repeat their tail because rueidis' intermediate builder types
// live in an internal package and cannot be named here, so the chain cannot be
// factored behind a local interface. Six duplicated lines are a better trade
// than dropping to the untyped Arbitrary() builder, where a mistyped field name
// would be a runtime error instead of a compile error.
func (r *Redis) buildXAdd(p Prepared) rueidis.Completed {
	b := r.client.B().Xadd().Key(r.cfg.Stream)

	// MAXLEN ~ N is the approximate form, far cheaper than the exact one. It
	// stays off by default: trimming discards events SILENTLY once the stream
	// fills, which is the failure mode this whole design exists to prevent.
	if r.cfg.MaxLen > 0 {
		return b.Maxlen().Almost().
			Threshold(strconv.FormatInt(r.cfg.MaxLen, 10)).
			Id("*").
			FieldValue().
			FieldValue("v", envelope.Version).
			FieldValue("e", rueidis.BinaryString(p.Env)).
			FieldValue("b", rueidis.BinaryString(p.Raw())).
			Build()
	}
	return b.Id("*").
		FieldValue().
		FieldValue("v", envelope.Version).
		FieldValue("e", rueidis.BinaryString(p.Env)).
		FieldValue("b", rueidis.BinaryString(p.Raw())).
		Build()
}

// Health asks whether this server can be WRITTEN to, not whether it is alive.
// It is the ONLY thing that moves the UP/DOWN state (SPEC §12.2).
//
// A PING would not do, and neither would XLEN alone: both are READS, and a
// replica promoted by mistake answers them happily while refusing every XADD
// with READONLY. The service would sit there dropping messages with the sink
// eternally UP -- the classic trivial-health-check trap.
//
// So the check is two commands in one pipeline:
//
//	XLEN <stream>                          -- reachability, and WRONGTYPE on the
//	                                          key that actually matters
//	XADD <stream>:healthprobe NOMKSTREAM ... -- a write probe with no side effect
//
// NOMKSTREAM means Redis returns nil and does NOT create the key when it is
// absent. But XADD is flagged `write denyoom`, so the command still goes through
// the same dispatch checks as a real write: READONLY on a replica, OOM on a full
// server, NOPERM on an insufficient ACL. Nothing is written and nothing is
// created.
func (r *Redis) Health(ctx context.Context) error {
	cmds := rueidis.Commands{
		r.client.B().Xlen().Key(r.cfg.Stream).Build(),
		r.client.B().Xadd().Key(r.probeKey).Nomkstream().Id("*").
			FieldValue().FieldValue("p", "1").Build(),
	}
	ops := [2]string{"xlen", "writeprobe"}
	for i, resp := range r.client.DoMulti(ctx, cmds...) {
		err := resp.Error()
		// A nil reply is the expected answer from the probe: the key is absent,
		// so nothing was appended. rueidis reports it as an error value, which
		// here is success.
		if rueidis.IsRedisNil(err) {
			continue
		}
		if err != nil {
			return classifyRedis(ops[i], err, "")
		}
	}
	return nil
}

func (r *Redis) Close() error {
	r.client.Close()
	return nil
}

// classifyRedisReply maps the TEXT of a Redis error reply onto a class.
//
// It is split out from classifyRedis so it can be tested directly:
// rueidis.RedisError cannot be constructed from outside its package, which would
// otherwise leave the single most safety-critical rule here -- that OOM is fatal
// -- with no test at all.
func classifyRedisReply(msg string) Class {
	upper := strings.ToUpper(msg)

	switch {
	case strings.HasPrefix(upper, "OOM"):
		// The server is ALIVE and refusing writes because it hit maxmemory.
		// Waiting cannot fix that, so retrying would only fill the ring and
		// start dropping messages while nothing drains.
		return ClassFatal

	case strings.HasPrefix(upper, "WRONGTYPE"),
		strings.HasPrefix(upper, "NOPERM"),
		strings.HasPrefix(upper, "NOAUTH"),
		strings.HasPrefix(upper, "WRONGPASS"):
		// The stream key is the wrong type, or the credentials cannot write to
		// it. Neither changes on its own, and both mean every batch will fail
		// identically -- so this is fatal at boot rather than a slow drip of
		// discarded batches in a process that looks healthy.
		return ClassFatal

	case strings.HasPrefix(upper, "LOADING"),
		strings.HasPrefix(upper, "BUSY"),
		strings.HasPrefix(upper, "MASTERDOWN"),
		strings.HasPrefix(upper, "READONLY"),
		strings.HasPrefix(upper, "CLUSTERDOWN"),
		strings.HasPrefix(upper, "TRYAGAIN"):
		// Alive but temporarily unable: a replica mid-failover, a loading
		// dataset. Retrying is exactly right, and if it persists the health
		// fuses of SPEC §12.2 escalate it on their own.
		return ClassNetwork
	}

	// An unrecognised reply from a live server is more likely permanent than
	// transient, but dropping data on a guess is worse than retrying a hopeless
	// batch: the ring fills, backpressure appears, and an operator sees it
	// (invariant §19.17).
	return ClassNetwork
}

// classifyRedis maps a Redis reply onto the class the state machine acts on
// (SPEC §10.3).
func classifyRedis(op string, err error, id string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return networkErr(op, err)
	}

	// A Redis error reply means the server answered: it is alive, and the
	// problem is with the command or the server's state, not the connection.
	var rerr *rueidis.RedisError
	if errors.As(err, &rerr) {
		class := classifyRedisReply(rerr.Error())
		if class != ClassNetwork && id != "" {
			return &Error{class, op + " id=" + id, err}
		}
		return &Error{class, op, err}
	}

	// Anything else -- dial failure, broken pipe, closed client -- is the
	// connection.
	return networkErr(op, err)
}
