package verify

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/rueidis"
)

// redisReader reads the stream of sink=redis (SPEC §8.5; the server's SPEC
// §3.6: XADD <stream> * e <jlh>), incrementally by the last entry ID. Redis
// 7.4 and Valkey 7.2+ speak the same commands here.
type redisReader struct {
	t      *Target
	c      rueidis.Client
	stream string
	start  string // next XRANGE start: "<ms>-0" first, then "(<last id>"
	seen   bool   // the stream existed at some poll
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
	c, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("redis: connect: %w", err)
	}
	stream := t.Key
	if stream == "" {
		stream = "loghub." + t.Space + ".syslog" // the sink.redis.stream default
	}
	r := &redisReader{t: t, c: c, stream: stream, start: strconv.FormatInt(t.Since.UnixMilli(), 10) + "-0"}
	if err := c.Do(ctx, c.B().Ping().Build()).Error(); err != nil {
		c.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return r, nil
}

func (r *redisReader) poll(ctx context.Context, cnt *Counter) error {
	for {
		entries, err := r.c.Do(ctx, r.c.B().Xrange().Key(r.stream).Start(r.start).End("+").Count(1000).Build()).AsXRange()
		if err != nil {
			return fmt.Errorf("redis: XRANGE %s: %w", r.stream, err)
		}
		if len(entries) > 0 {
			r.seen = true
		}
		for _, e := range entries {
			if v, ok := e.FieldValues["e"]; ok {
				countJLH([]byte(v), r.t, cnt)
			}
			r.start = "(" + e.ID
		}
		if len(entries) < 1000 {
			if !r.seen {
				n, err := r.c.Do(ctx, r.c.B().Exists().Key(r.stream).Build()).AsInt64()
				if err == nil && n > 0 {
					r.seen = true
				}
			}
			return nil
		}
	}
}

// final: a stream that never existed means the sink writes elsewhere -- the
// check cannot say anything (code 4), rather than "all lost".
func (r *redisReader) final() error {
	if !r.seen {
		return fmt.Errorf("redis: stream %s does not exist (sink.redis.stream elsewhere? give --verify-key)", r.stream)
	}
	return nil
}

func (r *redisReader) close() { r.c.Close() }
