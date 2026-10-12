package sink

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
)

// RedisConfig mirrors [sink.redis].
type RedisConfig struct {
	// Config is the connection, shared in shape and in error classification
	// with ring=redis (SPEC §10.3, §9.5).
	rediscli.Config

	// Stream is sink.redis.stream, default "loghub.{space}.syslog". {space} is
	// expanded per envelope with the envelope's own space (SPEC §3.6): the same
	// name as the NATS subject, so a consumer of a Redis eventbus uses the same
	// convention. A literal name without {space} is used as is.
	Stream string
	// MaxLen is sink.redis.maxlen; 0 disables trimming.
	MaxLen int64
	// Workspace is this instance's own space: the stream the health probe aims
	// at when Stream holds {space}.
	Workspace string
}

// spacePlaceholder is the token SPEC §3.6 expands.
const spacePlaceholder = "{space}"

// Redis writes each envelope as one XADD with ONE field (SPEC §3.6):
//
//	XADD <stream> [MAXLEN ~ N] * e <jlh>
//
// No v (the contract's version is in the JLH, which is self-describing by its
// type) and no b (the raw line is in payload.raw, decision 4). The JLH goes in
// with rueidis.BinaryString, which converts []byte to string without copying:
// the chunk buffer is read by the pipeline, which is why a write that ends by
// the clock taints the chunk (SPEC §15.2).
type Redis struct {
	cfg RedisConfig
	log *slog.Logger
	m   *obs.Metrics

	// mu guards the lazy dial; client is set once and never replaced.
	mu     sync.Mutex
	client rueidis.Client

	// streams caches the expansion of Stream for the last space seen. One
	// space per instance is the norm (ring=redis drains other replicas' items,
	// all of the same Workspace in practice), so one entry is enough and is
	// bounded by construction.
	streams atomic.Pointer[[2]string]

	// validated is set once the eviction policy has been checked
	// (checkEviction): once per process, at the first check that reaches the
	// server, which is the boot's.
	validated atomic.Bool
	closed    atomic.Bool
}

// NewRedis prepares the client without dialling: the connection is opened by
// the first health check, and the boot's WaitReady owns the retry loop
// (SPEC §13.1 step 7). The errors here are configuration errors.
func NewRedis(cfg RedisConfig, log *slog.Logger, m *obs.Metrics) (*Redis, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("sink redis: no address")
	}
	if cfg.Stream == "" {
		cfg.Stream = "loghub." + spacePlaceholder + ".syslog"
	}
	if strings.Contains(cfg.Stream, spacePlaceholder) && cfg.Workspace == "" {
		return nil, errors.New("sink redis: the stream name has {space} and no workspace was given")
	}
	if cfg.TLS {
		// Load the CA now: a missing file deserves exit 1, not a boot retry
		// loop that ends in exit 3.
		if _, err := rediscli.TLSConfig(cfg.Config); err != nil {
			return nil, err
		}
	}
	return &Redis{cfg: cfg, log: log, m: m}, nil
}

func (r *Redis) Type() string { return "redis" }

// conn dials on first use.
//
// rueidis dials AND authenticates inside NewClient, so a bad password or a
// missing ACL comes back here as a Redis error reply rather than from the first
// command. It goes through the same classification as any reply: fatal at once
// (SPEC §10.3), not retried as a connection failure until sink.boot_timeout
// runs out.
func (r *Redis) conn() (rueidis.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		return r.client, nil
	}
	c, err := rediscli.New(r.cfg.Config)
	if err != nil {
		return nil, classifyRedis("dial", err, "")
	}
	r.client = c
	return c, nil
}

// streamFor expands the stream name for a space.
func (r *Redis) streamFor(space string) string {
	if !strings.Contains(r.cfg.Stream, spacePlaceholder) {
		return r.cfg.Stream
	}
	if p := r.streams.Load(); p != nil && p[0] == space {
		return p[1]
	}
	s := strings.ReplaceAll(r.cfg.Stream, spacePlaceholder, space)
	r.streams.Store(&[2]string{space, s})
	return s
}

// Write sends the chunk as one pipelined DoMulti.
//
// The commands are BUILT FRESH on every call, and that is not an oversight to
// optimise away later. rueidis recycles a Completed command into a sync.Pool
// once it has been executed; reusing the slice on a retry would send corrupted
// data with no error anywhere (invariant §20.11). Since a failed chunk is
// retried by the worker (SPEC §10.2), rebuilding here is what keeps the retry
// correct.
//
// Build().Pin() would be the alternative, but it makes correctness depend on
// nobody ever forgetting a Pin. Rebuilding cannot be forgotten.
func (r *Redis) Write(ctx context.Context, b *Batch) error {
	if len(b.Envelopes) == 0 {
		return nil
	}
	if r.closed.Load() {
		return ErrClosed
	}
	client, err := r.conn()
	if err != nil {
		return err
	}

	cmds := make(rueidis.Commands, 0, len(b.Envelopes))
	for i := range b.Envelopes {
		e := &b.Envelopes[i]
		cmds = append(cmds, r.buildXAdd(client, r.streamFor(e.Item.Head.Space), e.JLH))
	}

	for i, resp := range client.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			return classifyRedis("xadd", err, b.Envelopes[i].UUID())
		}
	}
	return nil
}

// buildXAdd assembles one XADD.
//
// The two branches repeat their tail because rueidis' intermediate builder types
// live in an internal package and cannot be named here, so the chain cannot be
// factored behind a local interface. Four duplicated lines are a better trade
// than dropping to the untyped Arbitrary() builder, where a mistyped field name
// would be a runtime error instead of a compile error.
func (r *Redis) buildXAdd(client rueidis.Client, stream string, jlh []byte) rueidis.Completed {
	b := client.B().Xadd().Key(stream)

	// MAXLEN ~ N is the approximate form, far cheaper than the exact one. It
	// stays off by default: trimming discards events SILENTLY once the stream
	// fills, which is the failure mode this whole design exists to prevent.
	if r.cfg.MaxLen > 0 {
		return b.Maxlen().Almost().
			Threshold(strconv.FormatInt(r.cfg.MaxLen, 10)).
			Id("*").
			FieldValue().
			FieldValue("e", rueidis.BinaryString(jlh)).
			Build()
	}
	return b.Id("*").
		FieldValue().
		FieldValue("e", rueidis.BinaryString(jlh)).
		Build()
}

// Health asks whether this server can be WRITTEN to, not whether it is alive.
// It is the ONLY thing that moves the UP/DOWN state (SPEC §13.3).
//
// A PING would not do, and neither would XLEN alone: both are READS, and a
// replica promoted by mistake answers them happily while refusing every XADD
// with READONLY. The service would sit there dropping messages with the sink
// eternally UP -- the classic trivial-health-check trap.
//
// So the check is two commands in one pipeline (SPEC §10.3):
//
//	XLEN <stream>                             reachability, and WRONGTYPE on
//	                                          the key that actually matters
//	XADD <stream>:healthprobe NOMKSTREAM ...  a write probe with no side effect
//
// NOMKSTREAM means Redis returns nil and does NOT create the key when it is
// absent. But XADD is flagged `write denyoom`, so the command still goes through
// the same dispatch checks as a real write: READONLY on a replica, OOM on a full
// server, NOPERM on an insufficient ACL. Nothing is written and nothing is
// created. The stream is this instance's own: {space} expanded with its
// Workspace.
func (r *Redis) Health(ctx context.Context) error {
	if r.closed.Load() {
		return ErrClosed
	}
	client, err := r.conn()
	if err != nil {
		return err
	}
	stream := r.streamFor(r.cfg.Workspace)
	cmds := rueidis.Commands{
		client.B().Xlen().Key(stream).Build(),
		client.B().Xadd().Key(stream+":healthprobe").Nomkstream().Id("*").
			FieldValue().FieldValue("p", "1").Build(),
	}
	ops := [2]string{"xlen", "writeprobe"}
	for i, resp := range client.DoMulti(ctx, cmds...) {
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
	if !r.validated.Load() {
		if err := r.checkEviction(ctx, client); err != nil {
			return err
		}
		r.validated.Store(true)
	}
	return nil
}

// checkEviction refuses a server whose maxmemory-policy is allkeys-* (SPEC
// §10.3), once, at the first check that reaches it. Under such a policy a full
// Redis never answers the OOM that is fatal here: it evicts keys, and each
// stream is one key, so every event already confirmed to its emitter goes at
// once while this sink stays UP. The write probe above cannot see it: under
// allkeys-* an XADD at maxmemory succeeds, by evicting.
//
// It runs after the probe, so a Redis that is not up yet stays a network
// failure retried by WaitReady, and only a server that answers is judged. An
// unreadable policy (an ACL without INFO and CONFIG) is a WARN with the
// premise, and the boot goes on.
func (r *Redis) checkEviction(ctx context.Context, client rueidis.Client) error {
	policy, err := rediscli.CheckEviction(ctx, client)
	switch {
	case errors.Is(err, rediscli.ErrPolicyUnknown):
		r.log.Warn("sink redis: cannot read maxmemory-policy; it must be noeviction (or volatile-*: the "+
			"streams never have a TTL), or a full Redis evicts whole streams of delivered events",
			"error", err)
		return nil
	case err != nil:
		return classifyRedis("maxmemory-policy", err, "")
	}
	r.log.Debug("sink redis: maxmemory-policy checked", "policy", policy)
	return nil
}

func (r *Redis) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil {
		r.client.Close()
	}
	return nil
}

// classifyRedis maps a Redis error onto the class the state machine acts on,
// through the table shared with ring=redis (SPEC §10.3). The uuid, when given,
// rides along in the operation so a fatal or logic refusal names the envelope.
func classifyRedis(op string, err error, uuid string) error {
	if err == nil {
		return nil
	}
	var class Class
	switch rediscli.Classify(err) {
	case rediscli.Fatal:
		class = ClassFatal
	case rediscli.Logic:
		class = ClassLogic
	default:
		class = ClassNetwork
	}
	if class != ClassNetwork && uuid != "" {
		op += " uuid=" + uuid
	}
	return &Error{Class: class, Op: op, Err: err}
}
