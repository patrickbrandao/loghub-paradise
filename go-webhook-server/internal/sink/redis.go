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
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
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
	// DialTimeout bounds each dial and handshake of the client, at boot and
	// on every reconnection: sink.op_timeout (SPEC §10.1 step 3). 0 keeps
	// rueidis's own 5s.
	DialTimeout time.Duration
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
	// policyWarned: the WARN of an unreadable maxmemory-policy is logged once
	// per process; at every sink.health_interval it would flood the log.
	policyWarned atomic.Bool
	// policyRead is which reading of the policy works, learned by the first
	// check that needed more than INFO: policyByInfo, policyByConfig (INFO
	// did not answer it, CONFIG GET did) or policyNone (both refused, the
	// WARN given). The probe pipelines that one command only, so it stays
	// one round trip (SPEC §8.2.5, W §19.1 item 42).
	policyRead atomic.Int32
}

const (
	policyByInfo int32 = iota
	policyByConfig
	policyNone
)

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
	if cfg.DialTimeout > 0 {
		opt.Dialer.Timeout = cfg.DialTimeout
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

	client, err := newRedisClient(ctx, opt)
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

// newRedisClient is rueidis.NewClient within ctx. NewClient takes no context:
// it dials and handshakes (HELLO, AUTH, CLIENT SETINFO) under the Dialer's
// timeout, so an address that accepts TCP and never speaks held a boot attempt
// past sink.boot_timeout (SPEC §10.1 step 3, W §19.1 item 38). It runs in a
// goroutine; when ctx ends first, the attempt is a network failure, retried
// by the boot loop, and the client that arrives late is closed.
func newRedisClient(ctx context.Context, opt rueidis.ClientOption) (rueidis.Client, error) {
	type result struct {
		c   rueidis.Client
		err error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := rueidis.NewClient(opt)
		ch <- result{c, err}
	}()
	select {
	case r := <-ch:
		return r.c, r.err
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.c != nil {
				r.c.Close()
			}
		}()
		return nil, ctx.Err()
	}
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
	// INFO memory rides in the same pipeline as the probe: the eviction check
	// (checkEviction) then costs no round trip of its own, and the probe's
	// latency, which sink.health_timeout bounds, stays one round trip.
	cmds := rueidis.Commands{
		r.client.B().Xlen().Key(r.stream.own).Build(),
		r.client.B().Xadd().Key(r.probeKey).Nomkstream().Id("*").
			FieldValue().FieldValue("p", "1").Build(),
	}
	mode := r.policyRead.Load()
	switch mode {
	case policyByInfo:
		cmds = append(cmds, r.client.B().Info().Section("memory").Build())
	case policyByConfig:
		cmds = append(cmds, r.client.B().ConfigGet().Parameter("maxmemory-policy").Build())
	}
	ops := [2]string{"xlen", "writeprobe"}
	resps := r.client.DoMulti(ctx, cmds...)
	for i, resp := range resps[:2] {
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
	switch mode {
	case policyNone:
		return nil
	case policyByConfig:
		kv, cerr := resps[2].AsStrMap()
		policy, err := evictionFromConfig(kv, cerr, nil)
		if errors.Is(err, ErrPolicyUnknown) {
			// The ACL changed under us: WARN (once) and learn again from
			// INFO at the next check.
			r.policyRead.Store(policyByInfo)
		}
		return r.judgePolicy(policy, err)
	}
	info, ierr := resps[2].ToString()
	return r.checkEviction(ctx, info, ierr)
}

// checkEviction refuses a server whose maxmemory-policy is allkeys-* (SPEC
// §8.2.5, W pendência 12), at every check: the boot's and each one after it,
// since CONFIG SET changes the policy of a live server. Under such a policy a
// full Redis never answers the OOM that is fatal here: it evicts keys, and a
// stream is one key, so every event already answered 200 goes at once while
// this sink stays UP. The write probe cannot see it: under allkeys-* an XADD
// at maxmemory succeeds, by evicting.
//
// It runs after the probe, so a Redis that is not up yet stays a network
// failure retried by WaitReady, and only a server that answers is judged. An
// unreadable policy (an ACL without INFO and CONFIG) is a WARN with the
// premise, once, and the check passes.
func (r *Redis) checkEviction(ctx context.Context, info string, ierr error) error {
	policy, src, err := evictionFromInfo(ctx, r.client, info, ierr)
	switch {
	case src == policyByConfig:
		r.policyRead.Store(policyByConfig)
	case errors.Is(err, ErrPolicyUnknown):
		r.policyRead.Store(policyNone)
	}
	return r.judgePolicy(policy, err)
}

// judgePolicy turns a reading of the policy into the probe's answer: fatal
// for allkeys-*, a WARN once when it could not be read.
func (r *Redis) judgePolicy(policy string, err error) error {
	switch {
	case errors.Is(err, ErrPolicyUnknown):
		if !r.policyWarned.Swap(true) {
			r.log.Warn("sink redis: cannot read maxmemory-policy; it must be noeviction (or volatile-*: the "+
				"streams never have a TTL), or a full Redis evicts whole streams of delivered events",
				"error", err)
		}
		return nil
	case err != nil:
		return classifyEviction(err)
	}
	r.log.Debug("sink redis: maxmemory-policy checked", "policy", policy)
	return nil
}

// ErrEvictionPolicy marks a Redis whose maxmemory-policy may evict keys that
// have no TTL (allkeys-lru, allkeys-lfu, allkeys-random). It is fatal: exit 3
// at boot, FATAL after it (SPEC §8.2.5). A warning would be read after the
// data is gone.
var ErrEvictionPolicy = errors.New("redis: maxmemory-policy evicts keys without a TTL")

// ErrPolicyUnknown means the policy could not be read: the ACL forbids both
// INFO and CONFIG GET, or a proxy in front of Redis does not implement them.
// Refusing it would lock out every deployment whose ACL is least-privilege.
var ErrPolicyUnknown = errors.New("redis: maxmemory-policy could not be read")

// evictsUnexpiredKeys reports whether a maxmemory-policy may evict a key that
// has no TTL. noeviction refuses writes with OOM instead; the volatile-*
// policies only evict keys WITH a TTL, and this sink never sets one, so to it
// a volatile-* server at maxmemory behaves like noeviction. Matching the
// prefix rather than three names keeps a future allkeys-* policy refused.
func evictsUnexpiredKeys(policy string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(policy)), "allkeys-")
}

// evictionVerdict is the policy rule of checkRedisEviction.
func evictionVerdict(policy string) error {
	if evictsUnexpiredKeys(policy) {
		return fmt.Errorf("%w: maxmemory-policy is %s, under which a full Redis deletes the stream "+
			"instead of answering OOM; set noeviction (or a volatile-* policy: these keys never "+
			"have a TTL)", ErrEvictionPolicy, policy)
	}
	return nil
}

// policyFromInfo extracts maxmemory_policy from the text of INFO memory, or
// returns false when the line is absent (a proxy, a fork of Redis).
func policyFromInfo(info string) (string, bool) {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimRight(line, "\r"), "maxmemory_policy:"); ok {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// checkRedisEviction reads the server's maxmemory-policy (ported from
// go-syslog-server's rediscli.CheckEviction). It returns the policy read and:
//
//	nil                         the policy keeps the keys: noeviction, volatile-*
//	an ErrEvictionPolicy error  allkeys-*: fatal (classifyEviction)
//	an ErrPolicyUnknown error   unreadable: the caller WARNs and carries on
//	any other error             the connection, or a transient reply: retry
//
// INFO memory comes first because it is what a managed Redis (which often
// renames or blocks CONFIG) still answers, and because the ACL category of
// INFO is narrower than that of CONFIG (@admin); CONFIG GET is the fallback
// for a server whose INFO is filtered. Any other error REPLY to both is
// "unreadable", not "retry": NOPERM or "unknown command" does not change by
// waiting, and retrying it would hold the boot until sink.boot_timeout over a
// check that is a precaution. With Sentinel the client's node is the primary,
// the one that holds the streams.
func checkRedisEviction(ctx context.Context, c rueidis.Client) (string, error) {
	info, ierr := c.Do(ctx, c.B().Info().Section("memory").Build()).ToString()
	policy, _, err := evictionFromInfo(ctx, c, info, ierr)
	return policy, err
}

// evictionFromInfo is checkRedisEviction from an INFO memory reply already in
// hand (the probe pipelines it): CONFIG GET is asked only when INFO did not
// answer the policy.
func evictionFromInfo(ctx context.Context, c rueidis.Client, info string, ierr error) (string, int32, error) {
	if ierr == nil {
		if policy, ok := policyFromInfo(info); ok {
			return policy, policyByInfo, evictionVerdict(policy)
		}
	} else if !redisReplyRefused(ierr) {
		return "", policyByInfo, ierr
	}
	kv, cerr := c.Do(ctx, c.B().ConfigGet().Parameter("maxmemory-policy").Build()).AsStrMap()
	policy, err := evictionFromConfig(kv, cerr, ierr)
	if errors.Is(err, ErrPolicyUnknown) || (cerr != nil && !redisReplyRefused(cerr)) {
		return policy, policyByInfo, err
	}
	return policy, policyByConfig, err
}

// evictionFromConfig reads the policy from a CONFIG GET reply; ierr is the
// INFO error that sent the probe here, if any, for the message.
func evictionFromConfig(kv map[string]string, cerr, ierr error) (string, error) {
	if cerr == nil {
		if policy, ok := kv["maxmemory-policy"]; ok {
			return policy, evictionVerdict(policy)
		}
	} else if !redisReplyRefused(cerr) {
		return "", cerr
	}
	reason := "INFO memory has no maxmemory_policy line"
	if ierr != nil {
		reason = ierr.Error()
	}
	if cerr != nil {
		reason += "; CONFIG GET: " + cerr.Error()
	}
	return "", fmt.Errorf("%w (%s)", ErrPolicyUnknown, reason)
}

// classifyEviction maps an error of checkRedisEviction onto a class: the
// verdict is fatal, the rest is classified like any reply of the probe.
func classifyEviction(err error) error {
	if errors.Is(err, ErrEvictionPolicy) {
		return &Error{Class: ClassFatal, Op: "maxmemory-policy", Err: err}
	}
	return classifyRedis("maxmemory-policy", err, "")
}

// redisReplyRefused reports whether err is an error REPLY that waiting cannot
// change (NOPERM, unknown command), as opposed to a transport failure or one
// of the transient replies of SPEC §8.2.4, which the caller retries.
func redisReplyRefused(err error) bool {
	rerr, ok := rueidis.IsRedisErr(err)
	return ok && !transientRedisReply(strings.ToUpper(rerr.Error()))
}

// transientRedisReply: alive but temporarily unable (SPEC §8.2.4).
func transientRedisReply(upper string) bool {
	for _, p := range []string{"LOADING", "BUSY", "MASTERDOWN", "READONLY", "CLUSTERDOWN", "TRYAGAIN"} {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
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

	case transientRedisReply(upper):
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
