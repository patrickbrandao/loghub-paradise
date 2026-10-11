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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
)

// RedisConfig mirrors [sink.redis].
type RedisConfig struct {
	Addrs          []string
	Username       string
	Password       string
	DB             int
	SentinelMaster string
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	// Stream is sink.redis.stream, default "loghub.{space}.{type}", expanded
	// per envelope (SPEC §3.3). Workspace is this instance's space: the
	// stream Health probes, and the precomputed common case.
	Stream              string
	Workspace           string
	MaxLen              int64
	RingScaleEachConn   int
	PipelineMultiplex   int
	ReadBufferEachConn  int64
	WriteBufferEachConn int64
	ClientName          string
}

// Redis writes each event as one XADD with one field (SPEC §3.3):
//
//	XADD <stream> * e <jlh>
//
// The JLH goes in as raw bytes: Redis stream field values are binary safe, and
// rueidis.BinaryString converts []byte to string without copying.
type Redis struct {
	probeKey string // <own stream>:healthprobe, the write probe of Health
	stream   topic
	cfg      RedisConfig
	client   rueidis.Client
	log      *slog.Logger
	m        *obs.Metrics
}

// NewRedis connects and verifies the connection with one PING.
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
		// any reply: fatal at once (SPEC §8.2.4), not retried as a connection
		// failure until sink.boot_timeout runs out.
		return nil, classifyRedis("dial", err, "")
	}

	st := newTopic(cfg.Stream, cfg.Workspace)
	r := &Redis{cfg: cfg, client: client, log: log, m: m, stream: st,
		probeKey: st.own + ":healthprobe"}
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
// (SPEC §10.4), rebuilding here is what keeps the retry correct.
//
// Build().Pin() would be the alternative, but it makes correctness depend on
// nobody ever forgetting a Pin. Rebuilding cannot be forgotten.
func (r *Redis) Write(ctx context.Context, items []*envelope.Item) error {
	if len(items) == 0 {
		return nil
	}

	cmds := make(rueidis.Commands, 0, len(items))
	for _, it := range items {
		// The stream comes from the envelope's own header. An item whose JLH
		// this server did not write is refused as data (logic), and the
		// commands already built are dropped unsent.
		h, _, err := envelope.ParseHead(it.Env)
		if err != nil {
			return &Error{Class: ClassLogic, Op: "xadd id=" + it.ID, Err: err}
		}
		cmds = append(cmds, r.buildXAdd(r.stream.For(h.Space, h.Type), it))
	}

	resps := r.client.DoMulti(ctx, cmds...)
	for i, resp := range resps {
		if err := resp.Error(); err != nil {
			return classifyRedis("xadd", err, items[i].ID)
		}
	}
	return nil
}

// buildXAdd assembles one XADD.
//
// The two branches repeat their tail because rueidis' intermediate builder
// types live in an internal package and cannot be named here, so the chain
// cannot be factored behind a local interface. Four duplicated lines are a
// better trade than dropping to the untyped Arbitrary() builder, where a
// mistyped field name would be a runtime error instead of a compile error.
func (r *Redis) buildXAdd(stream string, it *envelope.Item) rueidis.Completed {
	b := r.client.B().Xadd().Key(stream)

	// MAXLEN ~ N is the approximate form, far cheaper than the exact one. It
	// stays off by default: trimming discards events SILENTLY once the stream
	// fills, which is the failure mode this whole design exists to prevent.
	if r.cfg.MaxLen > 0 {
		return b.Maxlen().Almost().
			Threshold(strconv.FormatInt(r.cfg.MaxLen, 10)).
			Id("*").
			FieldValue().
			FieldValue("e", rueidis.BinaryString(it.Env)).
			Build()
	}
	return b.Id("*").
		FieldValue().
		FieldValue("e", rueidis.BinaryString(it.Env)).
		Build()
}

// Health asks whether this server can be WRITTEN to, not whether it is alive.
// It is the ONLY thing that moves the UP/DOWN state (SPEC §10.2).
//
// A PING would not do: it is a READ, and a replica promoted by mistake answers
// it happily while refusing every XADD with READONLY. The service would sit
// there answering 429 with the sink eternally UP -- the classic trivial
// health-check trap. So the check is two commands in one pipeline, the same
// probe the sibling service uses:
//
//	XLEN <stream>                            -- reachability, and WRONGTYPE on
//	                                            the key that actually matters
//	XADD <stream>:healthprobe NOMKSTREAM ... -- a write probe with no side effect
//
// NOMKSTREAM means Redis returns nil and does NOT create the key when it is
// absent. But XADD is flagged `write denyoom`, so the command still goes
// through the same dispatch checks as a real write: READONLY on a replica, OOM
// on a full server, NOPERM on an insufficient ACL. Nothing is written and
// nothing is created (SPEC §8.2.4).
func (r *Redis) Health(ctx context.Context) error {
	cmds := rueidis.Commands{
		r.client.B().Xlen().Key(r.stream.own).Build(),
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
// rueidis.RedisError cannot be constructed from outside its package, which
// would otherwise leave the single most safety-critical rule here -- that OOM
// is fatal -- with no test at all.
func classifyRedisReply(msg string) Class {
	upper := strings.ToUpper(msg)

	switch {
	case strings.HasPrefix(upper, "OOM"):
		// The server is ALIVE and refusing writes because it hit maxmemory.
		// Waiting cannot fix that, so retrying would only fill the ring and
		// 429 the world while nothing drains.
		return ClassFatal

	case strings.HasPrefix(upper, "WRONGTYPE"),
		strings.HasPrefix(upper, "NOPERM"),
		strings.HasPrefix(upper, "NOAUTH"),
		strings.HasPrefix(upper, "WRONGPASS"):
		// The stream key is the wrong type, or the credentials cannot write to
		// it. Neither changes on its own, and both mean EVERY batch will fail
		// identically -- so this is fatal, with the ring dumped and the process
		// restarted, rather than a slow drip of batches discarded after their
		// 200 in a process whose PING says it is healthy (SPEC §8.2.4).
		return ClassFatal

	case strings.HasPrefix(upper, "LOADING"),
		strings.HasPrefix(upper, "BUSY"),
		strings.HasPrefix(upper, "MASTERDOWN"),
		strings.HasPrefix(upper, "READONLY"),
		strings.HasPrefix(upper, "CLUSTERDOWN"),
		strings.HasPrefix(upper, "TRYAGAIN"):
		// Alive but temporarily unable: a replica mid-failover, a loading
		// dataset. Retrying is exactly right.
		return ClassNetwork
	}

	// An unrecognised reply from a live server is more likely permanent than
	// transient, but dropping data on a guess is worse than retrying a hopeless
	// batch: the ring fills, 429 appears, and an operator sees it. There is
	// deliberately no logic class for Redis: XADD takes any bytes, so nothing
	// a live server answers is a refusal of THIS event alone.
	return ClassNetwork
}

// classifyRedis maps a Redis reply onto the class the state machine acts on
// (SPEC §8.2.4).
//
// The distinction that matters most: OOM means the server is ALIVE and refusing
// writes because it hit maxmemory. Waiting cannot fix that, so it is fatal
// immediately rather than being retried until the ring fills.
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
			return &Error{Class: class, Op: op + " id=" + id, Err: err}
		}
		return &Error{Class: class, Op: op, Err: err}
	}

	// Anything else -- dial failure, broken pipe, closed client -- is the
	// connection.
	return networkErr(op, err)
}
