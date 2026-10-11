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
	case strings.HasPrefix(upper, "LOADING"),
		strings.HasPrefix(upper, "BUSY"),
		strings.HasPrefix(upper, "MASTERDOWN"),
		strings.HasPrefix(upper, "READONLY"),
		strings.HasPrefix(upper, "CLUSTERDOWN"),
		strings.HasPrefix(upper, "TRYAGAIN"):
		// Alive but temporarily unable: failover, a loading dataset.
		return Network
	}
	// Unrecognised: retry. Dropping data on a guess is worse than retrying a
	// hopeless batch (invariant §20.18).
	return Network
}

// Classify maps any error from rueidis onto a class.
func Classify(err error) Class {
	if err == nil {
		return Network
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
