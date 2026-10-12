// Package rediscli builds the rueidis client and classifies Redis errors, for
// the two packages that talk to Redis: sink (sink=redis, SPEC §10.3) and ring
// (ring=redis, SPEC §9.5).
//
// It exists so the two cannot drift. The classification table is the one place
// where "OOM is fatal" lives, and a ring that retried OOM forever while the sink
// gave up at once -- or the other way round -- would be the kind of asymmetry
// nobody notices until an incident. The ring still cannot see the sink package
// (invariant §20.15): both import this one instead.
package rediscli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/redis/rueidis"
)

// Config mirrors the connection keys shared by [sink.redis] and [ring.redis].
type Config struct {
	Addrs          []string
	Username       string
	Password       string
	DB             int
	SentinelMaster string
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ClientName     string

	// rueidis memory knobs; zero keeps the library default.
	RingScaleEachConn   int
	PipelineMultiplex   int
	ReadBufferEachConn  int64
	WriteBufferEachConn int64
}

// New builds the client. rueidis dials AND authenticates inside NewClient, so a
// bad password comes back here as a Redis error reply: pass the error through
// Classify, which makes it fatal at once rather than a connection failure
// retried until boot_timeout (SPEC §10.3: a fatal reply to the dial is FATAL
// at the first check).
func New(cfg Config) (rueidis.Client, error) {
	opt := rueidis.ClientOption{
		InitAddress: cfg.Addrs,
		Username:    cfg.Username,
		Password:    cfg.Password,
		SelectDB:    cfg.DB,
		ClientName:  cfg.ClientName,
		// Client-side caching is a read optimisation and both users only
		// write. Disabling it also drops the RESP3 requirement, so an older
		// Redis or a proxy in front of one still works.
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
		tc, err := TLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		opt.TLSConfig = tc
		if cfg.SentinelMaster != "" {
			opt.Sentinel.TLSConfig = tc
		}
	}
	return rueidis.NewClient(opt)
}

// TLSConfig is verification on by default, against the system pool or the
// configured CA; insecure only when asked.
func TLSConfig(cfg Config) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLSInsecure {
		tc.InsecureSkipVerify = true
	}
	if cfg.TLSCA != "" {
		pem, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("redis: reading %s: %w", cfg.TLSCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("redis: %s contains no usable certificate", cfg.TLSCA)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

// Class is how a failure must be treated (SPEC §10.3). The sink maps it onto
// its own classes; the ring onto ErrUnavailable or a fatal error.
type Class uint8

const (
	// Network: transient; retry.
	Network Class = iota
	// Logic: this data was refused for good; drop and count.
	Logic
	// Fatal: the server is alive and will refuse every write the same way.
	Fatal
)

// ClassifyReply maps the TEXT of a Redis error reply onto a class. It is split
// out so it can be tested directly: rueidis.RedisError cannot be built from
// outside its package, which would otherwise leave "OOM is fatal" untested.
func ClassifyReply(msg string) Class {
	upper := strings.ToUpper(msg)
	switch {
	case strings.HasPrefix(upper, "OOM"):
		// Alive and refusing writes at maxmemory. Waiting cannot fix it;
		// retrying only fills the ring and starts dropping while nothing drains.
		return Fatal
	case strings.HasPrefix(upper, "WRONGTYPE"),
		strings.HasPrefix(upper, "NOPERM"),
		strings.HasPrefix(upper, "NOAUTH"),
		strings.HasPrefix(upper, "WRONGPASS"):
		// The key is the wrong type, or the credentials cannot write. Neither
		// changes on its own, and as `logic` they would discard a batch every
		// few milliseconds, forever, in a process every probe calls healthy.
		return Fatal
	case transientReply(upper):
		// Alive but temporarily unable: failover, a loading dataset.
		return Network
	}
	// Unrecognised: retry. Dropping data on a guess is worse than retrying a
	// hopeless batch (invariant §20.18).
	return Network
}

// transientReply reports whether an upper-cased error reply is one of the
// "alive but temporarily unable" answers of the table.
func transientReply(upper string) bool {
	for _, p := range []string{"LOADING", "BUSY", "MASTERDOWN", "READONLY", "CLUSTERDOWN", "TRYAGAIN"} {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}

// Classify maps any error from rueidis onto a class.
func Classify(err error) Class {
	if err == nil {
		return Network
	}
	if errors.Is(err, ErrEvictionPolicy) {
		// Not a reply but a verdict on the server's configuration, reached by
		// CheckEviction; it ends the boot like an OOM would (SPEC §9.5, §10.3).
		return Fatal
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return Network
	}
	var rerr *rueidis.RedisError
	if errors.As(err, &rerr) {
		return ClassifyReply(rerr.Error())
	}
	// Dial failure, broken pipe, closed client: the connection.
	return Network
}

// SameServer reports whether two configurations point at the same Redis: the
// same set of addresses and the same database. ring=redis and sink=redis on the
// same server degrade the ring to memory (SPEC §9.7).
func SameServer(a, b Config) bool {
	if a.DB != b.DB || a.SentinelMaster != b.SentinelMaster {
		return false
	}
	norm := func(xs []string) []string {
		out := make([]string, 0, len(xs))
		for _, x := range xs {
			out = append(out, strings.ToLower(strings.TrimSpace(x)))
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	return slices.Equal(norm(a.Addrs), norm(b.Addrs))
}

// ErrEvictionPolicy marks a Redis whose maxmemory-policy may evict keys that
// have no TTL (allkeys-lru, allkeys-lfu, allkeys-random). Classify makes it
// Fatal, so both users end the boot with exit 3 (SPEC §9.5, §10.3).
//
// Why fatal and not a warning: under such a policy a full Redis never answers
// OOM -- the one reply that is fatal and stops the pipeline -- it DELETES keys
// to make room, and a stream is one key. The whole stream goes at once: for
// ring=redis every entry waiting in it and its byte counter (which then admits
// more than the ceiling, or goes negative on the next commit); for sink=redis
// every event already confirmed to the emitter. The service stays UP and no
// metric moves. A warning would be read after the data is gone.
var ErrEvictionPolicy = errors.New("redis: maxmemory-policy evicts keys without a TTL")

// ErrPolicyUnknown means the policy could not be read: the ACL forbids both
// INFO and CONFIG GET, or a proxy in front of Redis does not implement them.
// The caller logs a WARN with the premise and carries on: refusing it would
// lock out every deployment whose ACL is least-privilege, which is the
// configuration a security review asks for.
var ErrPolicyUnknown = errors.New("redis: maxmemory-policy could not be read")

// EvictsUnexpiredKeys reports whether a maxmemory-policy may evict a key that
// has no TTL. noeviction refuses writes with OOM instead; the volatile-*
// policies only evict keys WITH a TTL, and neither user ever sets one on its
// keys, so to them a volatile-* server at maxmemory behaves like noeviction.
// Matching the prefix rather than three names keeps a future allkeys-* policy
// refused rather than silently accepted.
func EvictsUnexpiredKeys(policy string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(policy)), "allkeys-")
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

// CheckEviction reads the server's maxmemory-policy and refuses one that may
// evict the stream (SPEC §9.5, §10.3). It returns the policy read and:
//
//	nil                         the policy keeps the keys: noeviction, volatile-*
//	an ErrEvictionPolicy error  allkeys-*: fatal (Classify), exit 3
//	an ErrPolicyUnknown error   unreadable: the caller WARNs and carries on
//	any other error             the connection, or a transient reply: retry
//
// INFO memory comes first because it is what a managed Redis (which often
// renames or blocks CONFIG) still answers, and because the ACL category of
// INFO is narrower than that of CONFIG (@admin); CONFIG GET is the fallback
// for a server whose INFO is filtered. Any other error REPLY to both is
// "unreadable", not "retry": NOPERM or "unknown command" does not change by
// waiting, and retrying it would hold the boot until sink.boot_timeout over a
// check that is a precaution.
//
// The check reads one node. A Redis Cluster whose nodes disagree on the policy
// is a configuration nobody runs on purpose, and checking every node would put
// a fan-out in the boot of both users for it.
func CheckEviction(ctx context.Context, c rueidis.Client) (string, error) {
	info, ierr := c.Do(ctx, c.B().Info().Section("memory").Build()).ToString()
	if ierr == nil {
		if policy, ok := policyFromInfo(info); ok {
			return policy, verdict(policy)
		}
	} else if !replyRefused(ierr) {
		return "", ierr
	}
	kv, cerr := c.Do(ctx, c.B().ConfigGet().Parameter("maxmemory-policy").Build()).AsStrMap()
	if cerr == nil {
		if policy, ok := kv["maxmemory-policy"]; ok {
			return policy, verdict(policy)
		}
	} else if !replyRefused(cerr) {
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

// verdict is the policy rule of CheckEviction.
func verdict(policy string) error {
	if EvictsUnexpiredKeys(policy) {
		return fmt.Errorf("%w: maxmemory-policy is %s, under which a full Redis deletes the stream "+
			"instead of answering OOM; set noeviction (or a volatile-* policy: these keys never "+
			"have a TTL)", ErrEvictionPolicy, policy)
	}
	return nil
}

// replyRefused reports whether err is an error REPLY that waiting cannot
// change (NOPERM, unknown command), as opposed to a transport failure or one
// of the transient replies of the table, which the caller retries.
func replyRefused(err error) bool {
	rerr, ok := rueidis.IsRedisErr(err)
	return ok && !transientReply(strings.ToUpper(rerr.Error()))
}
