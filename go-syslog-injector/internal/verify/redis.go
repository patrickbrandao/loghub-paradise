package verify

import (
	"context"
	"errors"

	"github.com/redis/rueidis"
)

// redisReader reads the stream of sink=redis (SPEC §8.5; the server's SPEC
// §3.6: XADD <stream> * e <jlh>), incrementally by the last entry ID. Redis
// 7.4 and Valkey 7.2+ speak the same commands here.
type redisReader struct {
	t      *Target
	c      rueidis.Client
	stream string
	start  string // next XRANGE start: after the last entry at Open, then after the last read
}

func openRedis(ctx context.Context, t *Target) (reader, error) {
	opt, err := rueidis.ParseURL(t.URL)
	if err != nil {
		// The parser's error quotes the URL, which may carry a password.
		return nil, errors.New("redis: the URL does not parse (encode %, /, #, ? and @ in the password)")
	}
	// As the servers' rediscli: client-side caching is a read optimisation
	// this does not need, and dropping it drops the RESP3 requirement.
	opt.DisableCache = true
	opt.ClientName = "go-syslog-injector-verify"
	if t.TLSRoots != nil && opt.TLSConfig != nil {
		opt.TLSConfig.RootCAs = t.TLSRoots // --verify-tls-ca (SPEC §8.5)
	}
	c, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, t.clean("redis: connect", err)
	}
	stream := t.Key
	if stream == "" {
		stream = "loghub." + t.Space + ".syslog" // the sink.redis.stream default
	}
	r := &redisReader{t: t, c: c, stream: stream, start: "-"}
	if err := c.Do(ctx, c.B().Ping().Build()).Error(); err != nil {
		c.Close()
		return nil, t.clean("redis: ping", err)
	}
	// As with NATS: the run's entries come after the stream's last one now,
	// before the first send (SPEC §8.5); a stream not born yet is read whole.
	last, err := c.Do(ctx, c.B().Xrevrange().Key(stream).End("+").Start("-").Count(1).Build()).AsXRange()
	if err != nil && !rueidis.IsRedisNil(err) {
		c.Close()
		return nil, t.clean("redis: XREVRANGE "+stream, err)
	}
	if len(last) == 1 {
		r.start = "(" + last[0].ID
	}
	return r, nil
}

// A stream that never appears is every message lost (exit 1), as in the four
// injectors (SPEC §8.5): the server's XADD creates it, so a sink that wrote
// nothing lost everything. Until 2026-11-07 it was exit 4 here.
func (r *redisReader) full() bool { return false }

func (r *redisReader) poll(ctx context.Context, cnt *Counter) error {
	for {
		entries, err := r.c.Do(ctx, r.c.B().Xrange().Key(r.stream).Start(r.start).End("+").Count(1000).Build()).AsXRange()
		if err != nil {
			return r.t.clean("redis: XRANGE "+r.stream, err)
		}
		for _, e := range entries {
			if v, ok := e.FieldValues["e"]; ok {
				countJLH([]byte(v), r.t, cnt)
			}
			r.start = "(" + e.ID
		}
		if len(entries) < 1000 {
			return nil
		}
	}
}

func (r *redisReader) close() { r.c.Close() }
