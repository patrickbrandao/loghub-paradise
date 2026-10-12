package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/rediscli"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// What is worth proving here -- one XADD per envelope with the one field e, the
// {space} expansion, the write probe that creates nothing, WRONGTYPE as fatal,
// a retry that rebuilds its commands -- is what a fake would have to invent.
// These run against a real server addressed by RELPLOG_TEST_REDIS_ADDR and skip
// without it:
//
//	docker run --rm -d -p 16379:6379 --name sls-sink-redis redis:7-alpine
//	RELPLOG_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race ./internal/sink/
//
// Every test works in its own space, so its own stream, and deletes its keys.

const redisAddrEnv = "RELPLOG_TEST_REDIS_ADDR"

type redisFixture struct {
	t      *testing.T
	admin  rueidis.Client
	space  string
	stream string
	cfg    RedisConfig
}

func redisSetup(t *testing.T) *redisFixture {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		t.Skipf("set %s to run the redis sink tests", redisAddrEnv)
	}
	admin, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("%s is not reachable: %v", redisAddrEnv, err)
	}
	t.Cleanup(admin.Close)
	f := &redisFixture{t: t, admin: admin}
	f.space = fmt.Sprintf("t%d", time.Now().UnixNano())
	f.stream = "loghub." + f.space + ".syslog"
	f.cfg = RedisConfig{
		Config:    rediscli.Config{Addrs: []string{addr}, ClientName: "sls-test"},
		Workspace: f.space,
	}
	f.cleanup(f.stream, f.stream+":healthprobe")
	return f
}

func (f *redisFixture) cleanup(keys ...string) {
	f.t.Cleanup(func() {
		ctx := context.Background()
		for _, k := range keys {
			f.admin.Do(ctx, f.admin.B().Del().Key(k).Build())
		}
	})
}

func (f *redisFixture) sink(cfg RedisConfig) *Redis {
	f.t.Helper()
	r, err := NewRedis(cfg, discard(), metrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { r.Close() })
	return r
}

func (f *redisFixture) ready(r *Redis) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// entries reads a stream back as the field lists of its entries.
func (f *redisFixture) entries(stream string) []map[string]string {
	f.t.Helper()
	res, err := f.admin.Do(context.Background(), f.admin.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		f.t.Fatal(err)
	}
	out := make([]map[string]string, 0, len(res))
	for _, e := range res {
		out = append(out, e.FieldValues)
	}
	return out
}

func (f *redisFixture) items(n int) []*item.Item {
	out := make([]*item.Item, n)
	for i := range out {
		out[i] = newItemIn(f.space, fmt.Sprintf("<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 - redis %d", i))
	}
	return out
}

// SPEC §3.6: one entry per envelope, ONE field e holding the JLH byte for byte,
// in the stream named after the envelope's own space.
func TestRedisWriteStoresTheJLHInOneField(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	f.ready(r)

	b := batchFrom(t, f.items(5)...)
	if err := r.Write(context.Background(), b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.entries(f.stream)
	if len(got) != 5 {
		t.Fatalf("%d entries, want 5", len(got))
	}
	for i, e := range got {
		if len(e) != 1 {
			t.Errorf("entry %d has fields %v; SPEC §3.6 has exactly one, e", i, e)
		}
		if e["e"] != string(b.Envelopes[i].JLH) {
			t.Errorf("entry %d: e is not the JLH byte for byte", i)
		}
		if err := jlhcheck.Validate([]byte(e["e"])); err != nil {
			t.Errorf("entry %d: %v", i, err)
		}
	}
}

// {space} is expanded per envelope: one chunk with two spaces lands in two
// streams.
func TestRedisStreamFollowsTheEnvelopeSpace(t *testing.T) {
	f := redisSetup(t)
	other := f.space + "-b"
	otherStream := "loghub." + other + ".syslog"
	f.cleanup(otherStream)
	r := f.sink(f.cfg)
	f.ready(r)

	b := batchFrom(t, newItemIn(f.space, line5424), newItemIn(other, line3164), newItemIn(f.space, line3164))
	if err := r.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if a, o := len(f.entries(f.stream)), len(f.entries(otherStream)); a != 2 || o != 1 {
		t.Fatalf("own stream %d entries, other %d; want 2 and 1", a, o)
	}
}

// SPEC §10.3 and invariant §20.11: a retry rebuilds its commands. Writing the
// same chunk twice -- the shape of a retry after a timeout -- stores it twice
// (XADD * does not deduplicate; the consumer does, by (uuid, step)) and
// intact: a reused, recycled command would corrupt data with no error.
func TestRedisRetryRebuildsTheCommands(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	f.ready(r)

	b := batchFrom(t, f.items(20)...)
	for i := 0; i < 2; i++ {
		if err := r.Write(context.Background(), b); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	got := f.entries(f.stream)
	if len(got) != 40 {
		t.Fatalf("%d entries, want 40", len(got))
	}
	for i, e := range got {
		if e["e"] != string(b.Envelopes[i%20].JLH) {
			t.Fatalf("entry %d came back different", i)
		}
	}
}

// sink.redis.maxlen applies MAXLEN ~ N.
func TestRedisMaxLenTrims(t *testing.T) {
	f := redisSetup(t)
	cfg := f.cfg
	cfg.MaxLen = 10
	r := f.sink(cfg)
	f.ready(r)
	for i := 0; i < 10; i++ {
		if err := r.Write(context.Background(), batchFrom(t, f.items(100)...)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := f.admin.Do(context.Background(), f.admin.B().Xlen().Key(f.stream).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	if n >= 1000 {
		t.Fatalf("XLEN = %d after 1000 XADD with MAXLEN ~ 10: nothing was trimmed", n)
	}
}

// SPEC §10.3: the probe is XLEN plus XADD NOMKSTREAM on <stream>:healthprobe,
// which goes through the write checks and creates NOTHING.
func TestRedisHealthProbeCreatesNothing(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	for i := 0; i < 3; i++ {
		f.ready(r)
	}
	ctx := context.Background()
	for _, k := range []string{f.stream, f.stream + ":healthprobe"} {
		n, err := f.admin.Do(ctx, f.admin.B().Exists().Key(k).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("the health probe created %s", k)
		}
	}
}

// SPEC §10.3: WRONGTYPE on the stream key is fatal, at the probe and at the
// write -- as logic it would discard a chunk every few milliseconds, forever.
func TestRedisWrongTypeIsFatal(t *testing.T) {
	f := redisSetup(t)
	ctx := context.Background()
	if err := f.admin.Do(ctx, f.admin.B().Set().Key(f.stream).Value("not a stream").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	r := f.sink(f.cfg)
	if err := r.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health = %v, want fatal WRONGTYPE", err)
	}
	if err := r.Write(ctx, batchFrom(t, f.items(1)...)); !IsFatal(err) {
		t.Fatalf("Write = %v, want fatal WRONGTYPE", err)
	}
}

// The whole drain path against the real server: ring, workers, conversion,
// XADD.
func TestRedisWorkersEndToEnd(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	f.ready(r)

	rg := ring.NewMemory(1000, 0, time.Millisecond)
	m := metrics()
	w := NewWorkers(rg, r, NewHealth(r, HealthConfig{}, discard(), m), workerCfg(32), discard(), m)
	items := f.items(200)
	enqueueAll(t, rg, items)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 20*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 200 })
	cancel()
	w.Wait()

	got := f.entries(f.stream)
	if len(got) != 200 {
		t.Fatalf("%d entries, want 200", len(got))
	}
	seen := map[string]bool{}
	for _, e := range got {
		if err := jlhcheck.Validate([]byte(e["e"])); err != nil {
			t.Fatal(err)
		}
		seen[e["e"][9:45]] = true // the uuid
	}
	if len(seen) != 200 {
		t.Fatalf("%d distinct uuids, want 200", len(seen))
	}
	for _, it := range items {
		if !it.Released() {
			t.Fatal("an item was not released after delivery")
		}
	}
}
