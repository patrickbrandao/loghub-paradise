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
	// DialTimeout bounds each dial and handshake of the client, at boot and
	// on every reconnection: sink.op_timeout. 0 keeps rueidis's own 5s.
	DialTimeout time.Duration
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

	// policyWarned: the WARN of an unreadable maxmemory-policy is logged once
	// per process; at every sink.health_interval it would flood the log.
	policyWarned atomic.Bool
	// policyRead is which reading of the policy works, learned by the first
	// check that needed more than INFO: policyByInfo, policyByConfig (INFO
	// did not answer it, CONFIG GET did) or policyNone (both refused, the
	// WARN given). The probe pipelines that one command only, so it stays
	// one round trip (SPEC §10.3; ported from go-webhook-server).
	policyRead atomic.Int32

	// server and serverVersion are what INFO server said at boot (SPEC
	// §10.3): "redis", "valkey" or "unknown". They are logged, never used to
	// decide anything.
	server, serverVersion string
}

const (
	policyByInfo int32 = iota
	policyByConfig
	policyNone
)

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
	r.server, r.serverVersion = serverIdentity(ctx, client)
	return r, nil
}

// Server is what INFO server said at boot: the flavour ("redis", "valkey" or
// "unknown") and its version. For the boot log only (SPEC §10.3).
func (r *Redis) Server() (name, version string) { return r.server, r.serverVersion }

// newRedisClient is rueidis.NewClient within ctx. NewClient takes no context:
// it dials and handshakes (HELLO, AUTH, SELECT, CLIENT SETINFO) under the
// Dialer's timeout, so an address that accepts TCP and never speaks held a
// boot attempt past sink.boot_timeout (ported from go-webhook-server). It runs
// in a goroutine; when ctx ends first the attempt is a network failure,
// retried by the boot loop, and the client that arrives late is closed.
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

// serverIdentity reads INFO server once (SPEC §10.3). Valkey answers
// redis_version:7.2.4 in every release, its real version in valkey_version
// with server_name:valkey -- and Valkey 7.2 even says "redis" in HELLO -- so
// INFO server is the reliable source, and NOTHING here is ever decided by
// redis_version. An ACL without INFO, or a proxy, is "unknown", never an
// error: the identity is for the log.
func serverIdentity(ctx context.Context, c rueidis.Client) (string, string) {
	info, err := c.Do(ctx, c.B().Info().Section("server").Build()).ToString()
	if err != nil {
		return "unknown", ""
	}
	return parseServerInfo(info)
}

// parseServerInfo is serverIdentity on the text of INFO server.
func parseServerInfo(info string) (string, string) {
	fields := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), ":")
		if ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	if strings.EqualFold(fields["server_name"], "valkey") {
		return "valkey", fields["valkey_version"]
	}
	if v, ok := fields["redis_version"]; ok {
		return "redis", v
	}
	return "unknown", ""
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
//
// A third command rides in the same pipeline: the reading of maxmemory-policy
// (checkEviction), at every check and not only at boot, since CONFIG SET
// changes the policy of a live server (SPEC §10.3; pendência 13 de §21.1).
func (r *Redis) Health(ctx context.Context) error {
	cmds := rueidis.Commands{
		r.client.B().Xlen().Key(r.cfg.Stream).Build(),
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
	var resps []rueidis.RedisResult
	if len(cmds) > 2 && r.client.Mode() == rueidis.ClientModeCluster {
		// rueidis refuses (panics on) a DoMulti that mixes a keyless command
		// -- INFO, CONFIG GET -- with keyed ones in cluster mode, so there the
		// policy is read in a round trip of its own (found against a one-node
		// cluster, 2026-11-07).
		resps = append(r.client.DoMulti(ctx, cmds[:2]...), r.client.Do(ctx, cmds[2]))
	} else {
		resps = r.client.DoMulti(ctx, cmds...)
	}
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
// §10.3), at every check: the boot's and each one after it. Under such a
// policy a full Redis (or Valkey) never answers the OOM that is fatal here: it
// evicts keys, and the stream is one key, so every message already delivered
// goes at once while this sink stays UP. The write probe cannot see it: under
// allkeys-* an XADD at maxmemory succeeds, by evicting.
//
// It runs after the probe, so a server that is not up yet stays a network
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
				"stream never has a TTL), or a full Redis evicts the whole stream of delivered messages",
				"error", err)
		}
		return nil
	case err != nil:
		return classifyEviction(err)
	}
	r.log.Debug("sink redis: maxmemory-policy checked", "policy", policy)
	return nil
}

// ErrEvictionPolicy marks a server whose maxmemory-policy may evict keys that
// have no TTL (allkeys-lru, allkeys-lfu, allkeys-random). It is fatal: exit 3
// at boot, FATAL after it (SPEC §10.3). A warning would be read after the
// data is gone.
var ErrEvictionPolicy = errors.New("redis: maxmemory-policy evicts keys without a TTL")

// ErrPolicyUnknown means the policy could not be read: the ACL forbids both
// INFO and CONFIG GET, or a proxy in front of the server does not implement
// them. Refusing it would lock out every deployment whose ACL is
// least-privilege.
var ErrPolicyUnknown = errors.New("redis: maxmemory-policy could not be read")

// evictsUnexpiredKeys reports whether a maxmemory-policy may evict a key that
// has no TTL. noeviction refuses writes with OOM instead; the volatile-*
// policies only evict keys WITH a TTL, and this sink never sets one, so to it
// a volatile-* server at maxmemory behaves like noeviction. Matching the
// prefix rather than three names keeps a future allkeys-* policy refused.
func evictsUnexpiredKeys(policy string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(policy)), "allkeys-")
}

// evictionVerdict is the policy rule.
func evictionVerdict(policy string) error {
	if evictsUnexpiredKeys(policy) {
		return fmt.Errorf("%w: maxmemory-policy is %s, under which a full server deletes the stream "+
			"instead of answering OOM; set noeviction (or a volatile-* policy: the stream never "+
			"has a TTL)", ErrEvictionPolicy, policy)
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

// evictionFromInfo reads the policy from an INFO memory reply already in hand
// (the probe pipelines it): CONFIG GET is asked only when INFO did not answer
// it. It returns the policy, which reading worked, and:
//
//	nil                         the policy keeps the keys: noeviction, volatile-*
//	an ErrEvictionPolicy error  allkeys-*: fatal (classifyEviction)
//	an ErrPolicyUnknown error   unreadable: the caller WARNs and carries on
//	any other error             the connection, or a transient reply: retry
//
// INFO memory comes first because it is what a managed Redis (which often
// renames or blocks CONFIG) still answers, and because the ACL category of
// INFO is narrower than that of CONFIG (@admin). Any other error REPLY to both
// is "unreadable", not "retry": NOPERM or "unknown command" does not change by
// waiting, and retrying it would hold the boot until sink.boot_timeout over a
// check that is a precaution. With Sentinel the client's node is the primary,
// the one that holds the stream.
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

// classifyEviction maps an error of the policy check onto a class: the
// verdict is fatal, the rest is classified like any reply of the probe.
func classifyEviction(err error) error {
	if errors.Is(err, ErrEvictionPolicy) {
		return &Error{Class: ClassFatal, Op: "maxmemory-policy", Err: err}
	}
	return classifyRedis("maxmemory-policy", err, "")
}

// redisReplyRefused reports whether err is an error REPLY that waiting cannot
// change (NOPERM, unknown command), as opposed to a transport failure or one
// of the transient replies of SPEC §10.3, which the caller retries.
func redisReplyRefused(err error) bool {
	rerr, ok := rueidis.IsRedisErr(err)
	return ok && !transientRedisReply(strings.ToUpper(rerr.Error()))
}

// transientRedisReply: alive but temporarily unable (SPEC §10.3).
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

	case clusterDBReply(upper):
		// sink.redis.db != 0 against a cluster. The text is matched, not the
		// "ERR" prefix, which is far too generic. It does not change by
		// waiting, and as the unrecognised reply below it held the boot
		// until sink.boot_timeout (SPEC §10.3).
		return ClassFatal

	case transientRedisReply(upper):
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

// clusterDBReply matches the two answers of a cluster to a SELECT of a db
// other than 0 in the handshake: Redis Cluster and Valkey < 9 refuse SELECT
// outright, and Valkey 9 refuses a db beyond cluster-databases (default 1).
func clusterDBReply(upper string) bool {
	return strings.Contains(upper, "SELECT IS NOT ALLOWED IN CLUSTER MODE") ||
		strings.Contains(upper, "DB INDEX IS OUT OF RANGE")
}

// errClusterDB explains the fatal of clusterDBReply.
var errClusterDB = errors.New("sink.redis.db is not available on this server: Redis Cluster and " +
	"Valkey < 9 only have db 0; on Valkey 9 the db must exist (cluster-databases)")

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
		if clusterDBReply(strings.ToUpper(rerr.Error())) {
			return &Error{class, op, fmt.Errorf("%w (%w)", errClusterDB, err)}
		}
		if class != ClassNetwork && id != "" {
			return &Error{class, op + " id=" + id, err}
		}
		return &Error{class, op, err}
	}

	// Anything else -- dial failure, broken pipe, closed client -- is the
	// connection.
	return networkErr(op, err)
}
