package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// The first test here needs no server. The others run against a real Redis
// addressed by WHS_TEST_REDIS_ADDR and skip without it: what they prove --
// XADD with one binary-safe field, the stream chosen per envelope, the write
// probe that creates nothing, MAXLEN ~ -- is what a fake would have to invent.
//
//	WHS_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race ./internal/sink/
//
// Every test works in a Workspace of its own, so its streams are keys no
// other suite touches, and deletes them afterwards.

const redisAddrEnv = "WHS_TEST_REDIS_ADDR"

// SPEC §8.2.3: a JLH the header reader refuses never becomes a command. The
// batch fails as logic, and it fails before the client is touched -- this
// sink has no client at all.
func TestRedisRefusesAnEnvelopeBeforeBuildingCommands(t *testing.T) {
	err := (&Redis{}).Write(context.Background(), []*envelope.Item{badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write(v1 envelope) = %v (class %s), want logic", err, ClassOf(err))
	}
}

type redisFixture struct {
	t         *testing.T
	admin     rueidis.Client
	workspace string
	cfg       RedisConfig
}

func redisSetup(t *testing.T) *redisFixture {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		t.Skipf("set %s to run the redis sink tests (see docs/TESTING.md §5)", redisAddrEnv)
	}
	admin, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("%s=%s is not reachable: %v", redisAddrEnv, addr, err)
	}
	t.Cleanup(admin.Close)
	f := &redisFixture{t: t, admin: admin, workspace: fmt.Sprintf("whs-test-%d", time.Now().UnixNano())}
	f.cfg = RedisConfig{Addrs: []string{addr}, Stream: "loghub.{space}.{type}", Workspace: f.workspace,
		ClientName: "whs/test@test-host"}
	f.forget(redisStreamOf(f.workspace), redisStreamOf(f.workspace)+":healthprobe",
		redisStreamOf(otherSpace(f.workspace)))
	return f
}

// redisStreamOf is the default stream of a space (SPEC §3.3).
func redisStreamOf(space string) string { return "loghub." + space + "." + envelope.Type }

// forget deletes keys when the test ends.
func (f *redisFixture) forget(keys ...string) {
	f.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.admin.Do(ctx, f.admin.B().Del().Key(keys...).Build()).Error()
	})
}

func (f *redisFixture) sink(cfg RedisConfig) *Redis {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, cfg, quietLog(), testMetrics())
	if err != nil {
		f.t.Fatalf("NewRedis: %v", err)
	}
	f.t.Cleanup(func() { r.Close() })
	return r
}

func (f *redisFixture) entries(stream string) []rueidis.XRangeEntry {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, err := f.admin.Do(ctx, f.admin.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		f.t.Fatalf("XRANGE %s: %v", stream, err)
	}
	return e
}

func (f *redisFixture) exists(key string) bool {
	f.t.Helper()
	n, err := f.admin.Do(context.Background(), f.admin.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		f.t.Fatal(err)
	}
	return n == 1
}

func (f *redisFixture) xlen(stream string) int64 {
	f.t.Helper()
	n, err := f.admin.Do(context.Background(), f.admin.B().Xlen().Key(stream).Build()).AsInt64()
	if err != nil {
		f.t.Fatal(err)
	}
	return n
}

// SPEC §3.3: one entry per event with exactly one field, e, holding the JLH
// byte for byte -- binary body included -- on the stream of the envelope's
// own space: loghub.<space>.webhook.
func TestRedisWriteAddsTheEnvelope(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)

	raw := []byte("A\x00B\nC\xffD\r\x00")
	own := []*envelope.Item{
		itemIn(f.workspace, testRequest("/a", []byte(`{"a":1}`))),
		itemIn(f.workspace, testRequest("/bin", raw)),
		itemIn(f.workspace, testRequest("/empty", nil)),
	}
	other := itemIn(otherSpace(f.workspace), testRequest("/other", nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Write(ctx, append(own, other)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// The same batch again, the shape of a retry: the commands are rebuilt
	// (rueidis recycles them, SPEC §8.2.3), so the second copy is as exact as
	// the first. Redis has no deduplication: at-least-once is the consumer's.
	if err := r.Write(ctx, own); err != nil {
		t.Fatalf("second Write: %v", err)
	}

	got := f.entries(redisStreamOf(f.workspace))
	if len(got) != 2*len(own) {
		t.Fatalf("%d entries on the Workspace stream, want %d", len(got), 2*len(own))
	}
	for i, e := range got {
		it := own[i%len(own)]
		if len(e.FieldValues) != 1 {
			t.Errorf("entry %d has fields %v, want only e", i, e.FieldValues)
		}
		if e.FieldValues["e"] != string(it.Env) {
			t.Errorf("entry %d: e is not the JLH byte for byte\n%q\n%q", i, e.FieldValues["e"], it.Env)
			continue
		}
		env := validJLH(t, []byte(e.FieldValues["e"]))
		if env.UUID != it.ID || env.Space != f.workspace {
			t.Errorf("entry %d: uuid %s space %s", i, env.UUID, env.Space)
		}
	}
	body, err := validJLH(t, []byte(got[1].FieldValues["e"])).Payload.Body()
	if err != nil || string(body) != string(raw) {
		t.Errorf("binary body = %q (%v), want the bytes that arrived", body, err)
	}

	elsewhere := f.entries(redisStreamOf(otherSpace(f.workspace)))
	if len(elsewhere) != 1 || elsewhere[0].FieldValues["e"] != string(other.Env) {
		t.Fatalf("the envelope of space %s: %d entries on %s", otherSpace(f.workspace), len(elsewhere),
			redisStreamOf(otherSpace(f.workspace)))
	}
	validJLH(t, []byte(elsewhere[0].FieldValues["e"]))
}

// A stream name without placeholders is used as is, for every envelope.
func TestRedisLiteralStream(t *testing.T) {
	f := redisSetup(t)
	literal := fmt.Sprintf("whs-test-literal-%d", time.Now().UnixNano())
	f.forget(literal, literal+":healthprobe")
	cfg := f.cfg
	cfg.Stream = literal
	r := f.sink(cfg)

	items := []*envelope.Item{itemIn(f.workspace, testRequest("/a", nil)), itemIn(otherSpace(f.workspace), testRequest("/b", nil))}
	if err := r.Write(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	if n := f.xlen(literal); n != 2 {
		t.Errorf("%d entries on %s, want both envelopes", n, literal)
	}
	if f.exists(redisStreamOf(f.workspace)) || f.exists(redisStreamOf(otherSpace(f.workspace))) {
		t.Error("a literal stream name was expanded")
	}
}

// SPEC §8.2.4: the probe asks the Workspace's stream "can I write?" and
// creates nothing; a fatal answer about that stream is fatal at once, while
// the stream of another space is not what the probe looks at.
func TestRedisHealthProbesTheWorkspaceStream(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg) // NewRedis runs Health once
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if f.exists(redisStreamOf(f.workspace)) || f.exists(redisStreamOf(f.workspace)+":healthprobe") {
		t.Error("the probe created a key; XADD NOMKSTREAM must leave the server as it was")
	}

	// Another space's key of the wrong type: not the probe's business...
	otherKey := redisStreamOf(otherSpace(f.workspace))
	if err := f.admin.Do(ctx, f.admin.B().Set().Key(otherKey).Value("x").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := r.Health(ctx); err != nil {
		t.Errorf("Health looked at another space's key: %v", err)
	}
	// ...but a write to it is refused as fatal: WRONGTYPE repeats on every batch.
	if err := r.Write(ctx, []*envelope.Item{itemIn(otherSpace(f.workspace), testRequest("/x", nil))}); !IsFatal(err) {
		t.Errorf("XADD on a string key = %v (class %s), want fatal", err, ClassOf(err))
	}

	// The Workspace's own stream of the wrong type is fatal at the probe.
	if err := f.admin.Do(ctx, f.admin.B().Set().Key(redisStreamOf(f.workspace)).Value("x").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	err := r.Health(ctx)
	if err == nil || !IsFatal(err) {
		t.Errorf("Health with the Workspace stream a string = %v (class %s), want fatal", err, ClassOf(err))
	}
	if _, err := NewRedis(ctx, f.cfg, quietLog(), testMetrics()); err == nil || !IsFatal(err) {
		t.Errorf("NewRedis with the Workspace stream a string = %v, want fatal at boot", err)
	}
}

// SPEC §8.2.5: off by default -- nothing is trimmed -- and MAXLEN ~ N when
// configured, the approximate form, which trims whole nodes and so keeps at
// least N.
func TestRedisMaxLen(t *testing.T) {
	f := redisSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	write := func(r *Redis, space string, n int) {
		t.Helper()
		for i := 0; i < n; i += 50 {
			b := make([]*envelope.Item, 50)
			for j := range b {
				b[j] = itemIn(space, testRequest("/m", nil))
			}
			if err := r.Write(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
	}

	write(f.sink(f.cfg), f.workspace, 300)
	if n := f.xlen(redisStreamOf(f.workspace)); n != 300 {
		t.Errorf("maxlen=0 kept %d of 300 entries; trimming must be off by default", n)
	}

	cfg := f.cfg
	cfg.MaxLen = 10
	space := otherSpace(f.workspace)
	write(f.sink(cfg), space, 300)
	n := f.xlen(redisStreamOf(space))
	if n < 10 || n >= 300 {
		t.Errorf("maxlen=10 left %d of 300 entries; want at least 10 and fewer than 300", n)
	}
	last := f.entries(redisStreamOf(space))
	validJLH(t, []byte(last[len(last)-1].FieldValues["e"]))
}

// An item whose JLH this server did not write is refused as data (logic),
// and nothing of its batch is sent.
func TestRedisRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	f := redisSetup(t)
	r := f.sink(f.cfg)
	err := r.Write(context.Background(), []*envelope.Item{itemIn(f.workspace, testRequest("/good", nil)), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if f.exists(redisStreamOf(f.workspace)) {
		t.Errorf("%d entries were added by a refused batch", f.xlen(redisStreamOf(f.workspace)))
	}
}
