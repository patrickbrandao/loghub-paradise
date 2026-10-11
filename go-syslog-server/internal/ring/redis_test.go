package ring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
)

// The ring=redis suite runs against a real server (SPEC §18.6) and is skipped
// without one:
//
//	docker run --rm -d -p 16380:6379 --name sls-ring-redis redis:7-alpine
//	SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16380 go test -race ./internal/ring/

// Unit tests that need no server.

func TestHashTag(t *testing.T) {
	for in, want := range map[string]string{
		"syslog:ring":      "",
		"{syslog:ring}":    "syslog:ring",
		"{syslog}:ring":    "syslog",
		"a{b}c{d}":         "b",
		"{}:ring":          "",
		"{":                "",
		"x{y":              "",
		"{syslog:ring}:by": "syslog:ring",
	} {
		if got := hashTag(in); got != want {
			t.Errorf("hashTag(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCeiling(t *testing.T) {
	if ceiling(0) != unlimited || ceiling(-1) != unlimited || ceiling(5) != 5 {
		t.Fatal("ceiling maps zero and negatives to unlimited and keeps the rest")
	}
}

// A refused connection is a Redis that is not up yet: the app retries it until
// sink.boot_timeout, so it must NOT be fatal.
func TestRedisConnectionRefusedIsNotFatal(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = NewRedis(ctx, RedisConfig{Config: rediscli.Config{Addrs: []string{addr}}, Consumer: "c"}, discard())
	if err == nil {
		t.Fatal("NewRedis succeeded against a closed port")
	}
	if errors.Is(err, ErrFatal) {
		t.Fatalf("a refused connection was classified fatal: %v", err)
	}
	t.Logf("refused: %v", err)
}

func TestRedisEmptyConsumerIsFatal(t *testing.T) {
	_, err := NewRedis(context.Background(), RedisConfig{Config: rediscli.Config{Addrs: []string{"127.0.0.1:1"}}}, discard())
	if !errors.Is(err, ErrFatal) {
		t.Fatalf("err = %v, want ErrFatal", err)
	}
}

// ---------------------------------------------------------------- live server

func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("SYSLOG_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("SYSLOG_TEST_REDIS_ADDR not set")
	}
	return addr
}

// adminClient is a plain client for the test's own inspection and cleanup.
func adminClient(t *testing.T, addr string) rueidis.Client {
	t.Helper()
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// uniqueStream gives each test its own keys, deleted at the end.
func uniqueStream(t *testing.T, admin rueidis.Client) string {
	t.Helper()
	s := fmt.Sprintf("test:ring:%s:%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		admin.Do(context.Background(), admin.B().Del().Key(s, s+":bytes", s+":quarantine").Build())
	})
	return s
}

type redisOpts struct {
	capItems  int
	capBytes  int64
	claimIdle time.Duration
	metrics   *obs.Metrics
	addr      string // overrides the server address (the proxy test)
}

func newRedisRing(t *testing.T, addr, stream, consumer string, o redisOpts) *RedisRing {
	t.Helper()
	if o.claimIdle == 0 {
		o.claimIdle = time.Hour // no claim round unless the test asks for one
	}
	if o.addr != "" {
		addr = o.addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := NewRedis(ctx, RedisConfig{
		Config:       rediscli.Config{Addrs: []string{addr}},
		Stream:       stream,
		Group:        "syslog-ring",
		ClaimIdle:    o.claimIdle,
		CapItems:     o.capItems,
		CapBytes:     o.capBytes,
		Consumer:     consumer,
		BlockTimeout: 100 * time.Millisecond,
		OpTimeout:    time.Second,
		ClaimCount:   64,
		Metrics:      o.metrics,
	}, discard())
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// dequeueN reads until it has want items or the deadline passes.
func dequeueN(t *testing.T, r *RedisRing, want int, within time.Duration) []*item.Item {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var out []*item.Item
	for len(out) < want {
		items, err := r.Dequeue(ctx, want-len(out))
		if err != nil {
			t.Fatalf("Dequeue after %d of %d: %v", len(out), want, err)
		}
		out = append(out, items...)
	}
	return out
}

func globalStats(t *testing.T, r *RedisRing) Stats {
	t.Helper()
	if err := r.RefreshStats(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r.Stats()
}

func pendingCount(t *testing.T, admin rueidis.Client, stream string) int64 {
	t.Helper()
	vals, err := admin.Do(context.Background(), admin.B().Xpending().Key(stream).Group("syslog-ring").Build()).ToArray()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := vals[0].AsInt64()
	return n
}

// The item that leaves must be the item that entered (SPEC §3.5): same uuid,
// same clock sample, same addresses, same TLS session, same raw bytes.
func TestRedisRoundTrip(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	r := newRedisRing(t, addr, stream, "consumer-a", redisOpts{})

	orig := []*item.Item{
		mkTLSItem(1, "<190>1 2026-10-03T12:00:00Z h a - - - linha \x00\xff crua"),
		mkItem(2, 64),
		mkItem(3, 0),
	}
	for _, it := range orig {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	if s := globalStats(t, r); s.Items != 3 || s.Bytes != int64(len(orig[0].Raw)+64) {
		t.Fatalf("stats = %+v", s)
	}

	got := dequeueN(t, r, 3, 5*time.Second)
	for i, back := range got {
		if !reflect.DeepEqual(back.Head, orig[i].Head) {
			t.Fatalf("item %d head changed\n got %+v\nwant %+v", i, back.Head, orig[i].Head)
		}
		if string(back.Raw) != string(orig[i].Raw) {
			t.Fatalf("item %d raw changed: %q", i, back.Raw)
		}
		if back.StreamID == "" {
			t.Fatalf("item %d has no StreamID; Commit could not act on it", i)
		}
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, r); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats after commit = %+v, want empty", s)
	}
	if n := pendingCount(t, admin, stream); n != 0 {
		t.Fatalf("%d entries still pending after commit", n)
	}
	// An idle read is an empty batch, not an error: the workers stop on errors.
	items, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("idle Dequeue = %d %v, want an empty batch", len(items), err)
	}
}

// SPEC §9.5: the ceilings are GLOBAL. Two instances (two consumers) on one
// stream share them, and either one drains what the other accepted.
func TestRedisTwoInstancesShareTheGlobalCeiling(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	a := newRedisRing(t, addr, stream, "replica-a", redisOpts{capItems: 5, capBytes: 1000})
	b := newRedisRing(t, addr, stream, "replica-b", redisOpts{capItems: 5, capBytes: 1000})

	for i := 0; i < 3; i++ {
		if err := a.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 3; i < 5; i++ {
		if err := b.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Enqueue(context.Background(), mkItem(5, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("a: err = %v, want ErrFull at the shared item ceiling", err)
	}
	if err := b.Enqueue(context.Background(), mkItem(6, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("b: err = %v, want ErrFull at the shared item ceiling", err)
	}
	if sa, sb := globalStats(t, a), globalStats(t, b); sa != sb || sa.Items != 5 || sa.Bytes != 50 {
		t.Fatalf("stats a=%+v b=%+v, want the same global 5 items / 50 bytes", sa, sb)
	}

	// b drains everything, including what a accepted.
	got := dequeueN(t, b, 5, 5*time.Second)
	seen := map[int]bool{}
	for _, it := range got {
		seen[idx(it)] = true
	}
	for i := 0; i < 5; i++ {
		if !seen[i] {
			t.Fatalf("item %d missing from b's drain: %v", i, seen)
		}
	}
	if err := b.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, a); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("a sees %+v after b committed, want empty", s)
	}

	// The byte ceiling is shared the same way.
	c := newRedisRing(t, addr, stream, "replica-c", redisOpts{capItems: 100, capBytes: 100})
	d := newRedisRing(t, addr, stream, "replica-d", redisOpts{capItems: 100, capBytes: 100})
	if err := c.Enqueue(context.Background(), mkItem(10, 60)); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(context.Background(), mkItem(11, 60)); !errors.Is(err, ErrFull) {
		t.Fatalf("d: err = %v, want ErrFull at the shared byte ceiling", err)
	}
	if err := d.Enqueue(context.Background(), mkItem(12, 40)); err != nil {
		t.Fatalf("d: 60+40 fits in 100: %v", err)
	}
}

// SPEC §9.5: XAUTOCLAIM takes the pending entries of a consumer idle for more
// than claim_idle -- a replica that died with a batch in hand.
func TestRedisAutoclaimTakesADeadConsumersPending(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	m := obs.NewMetrics(obs.Info{})

	dead := newRedisRing(t, addr, stream, "replica-dead", redisOpts{})
	for i := 0; i < 4; i++ {
		if err := dead.Enqueue(context.Background(), mkItem(i, 20)); err != nil {
			t.Fatal(err)
		}
	}
	held := dequeueN(t, dead, 4, 5*time.Second)
	if len(held) != 4 {
		t.Fatal("setup")
	}
	dead.Close() // dies with the batch: nothing acknowledged

	live := newRedisRing(t, addr, stream, "replica-live", redisOpts{claimIdle: 400 * time.Millisecond, metrics: m})
	// The first Dequeue ends live's own-pending replay (it has none) and
	// finds nothing new: everything is pending under the dead consumer.
	if items, err := live.Dequeue(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("first Dequeue = %d %v", len(items), err)
	}

	got := dequeueN(t, live, 4, 10*time.Second) // the claim loop runs every 200 ms
	seen := map[int]bool{}
	for _, it := range got {
		seen[idx(it)] = true
	}
	if len(seen) != 4 {
		t.Fatalf("claimed %v, want the dead consumer's 4 entries", seen)
	}
	if n := m.RingRedisClaimedTotal.Load(); n != 4 {
		t.Fatalf("syslog_ring_redis_claimed_total = %d, want 4", n)
	}
	if err := live.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, live); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v, want empty", s)
	}
}

// SPEC §9.5: a worker holding a batch in retry renews it with Touch, and no
// replica claims a batch that is alive. Once the touching stops, the batch is
// claimable again -- which proves the first half was not luck.
func TestRedisTouchPreventsTheClaimOfALiveBatch(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	const idle = 600 * time.Millisecond

	holder := newRedisRing(t, addr, stream, "replica-holder", redisOpts{claimIdle: time.Hour})
	other := newRedisRing(t, addr, stream, "replica-other", redisOpts{claimIdle: idle})
	for i := 0; i < 3; i++ {
		if err := holder.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	batch := dequeueN(t, holder, 3, 5*time.Second)
	if items, err := other.Dequeue(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("other's first Dequeue = %d %v", len(items), err)
	}

	// Hold the batch for well over claim_idle, touching every idle/3 as the
	// workers do.
	stop := time.Now().Add(3 * idle)
	for time.Now().Before(stop) {
		if err := holder.Touch(batch); err != nil {
			t.Fatal(err)
		}
		time.Sleep(idle / 3)
		other.claimRound("0-0")
		if n := other.claimedLen(); n != 0 {
			t.Fatalf("another replica claimed %d entries of a batch that was being touched", n)
		}
	}

	// Stop touching: after claim_idle the batch is fair game.
	time.Sleep(idle + 100*time.Millisecond)
	got := dequeueN(t, other, 3, 10*time.Second)
	if len(got) != 3 {
		t.Fatalf("claimed %d after the touching stopped, want 3", len(got))
	}
}

// SPEC §9.5: the commit script decrements only what XDEL removed. A replica
// that froze past claim_idle comes back and commits a batch another replica
// already claimed, delivered and committed: the counter must stay exact.
func TestRedisCounterIsExactAfterADuplicateDelivery(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)

	frozen := newRedisRing(t, addr, stream, "replica-frozen", redisOpts{})
	taker := newRedisRing(t, addr, stream, "replica-taker", redisOpts{claimIdle: 300 * time.Millisecond})

	for i := 0; i < 3; i++ {
		if err := frozen.Enqueue(context.Background(), mkItem(i, 11)); err != nil {
			t.Fatal(err)
		}
	}
	batch := dequeueN(t, frozen, 3, 5*time.Second) // ... and then it freezes

	// taker's replay is empty and nothing new is there: its first Dequeue is
	// an empty batch. The next ones bring the frozen batch, claimed.
	if items, err := taker.Dequeue(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("taker's first Dequeue = %d %v", len(items), err)
	}
	dup := dequeueN(t, taker, 3, 10*time.Second)

	// Two more entries nobody reads yet: the counter must end at exactly their
	// bytes, not at zero and not below.
	for i := 3; i < 5; i++ {
		if err := frozen.Enqueue(context.Background(), mkItem(i, 7)); err != nil {
			t.Fatal(err)
		}
	}

	if err := taker.Commit(dup); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Commit(batch); err != nil { // the frozen replica wakes up
		t.Fatal(err)
	}
	if s := globalStats(t, taker); s.Items != 2 || s.Bytes != 14 {
		t.Fatalf("stats after the duplicate commit = %+v, want 2 items / 14 bytes", s)
	}
	rest := dequeueN(t, taker, 2, 5*time.Second)
	if err := taker.Commit(rest); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, taker); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("final stats = %+v, want exactly empty", s)
	}
}

// SPEC §9.5: a restarted instance re-reads its OWN pending entries before
// asking for new ones, and an entry deleted while it was down (another replica
// delivered it) is only acknowledged.
func TestRedisOwnPendingAreReplayedFirst(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)

	first := newRedisRing(t, addr, stream, "replica-x", redisOpts{})
	for i := 0; i < 4; i++ {
		if err := first.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	held := dequeueN(t, first, 4, 5*time.Second)
	first.Close() // a crash: nothing acknowledged

	// While it is down, held[3] is deleted from the stream but stays in the
	// PEL -- an XDEL or XTRIM from outside the ring. The replay must only
	// acknowledge it.
	ctx := context.Background()
	if err := admin.Do(ctx, admin.B().Xdel().Key(stream).Id(held[3].StreamID).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Do(ctx, admin.B().Decrby().Key(stream+":bytes").Decrement(10).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	other := newRedisRing(t, addr, stream, "replica-y", redisOpts{})
	for i := 4; i < 6; i++ {
		if err := other.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}

	again := newRedisRing(t, addr, stream, "replica-x", redisOpts{})
	replayed := dequeueN(t, again, 3, 5*time.Second)
	for i, it := range replayed {
		if idx(it) != i {
			t.Fatalf("replay position %d holds item %d, want the old pending in order", i, idx(it))
		}
	}
	fresh := dequeueN(t, again, 2, 5*time.Second)
	if idx(fresh[0]) != 4 || idx(fresh[1]) != 5 {
		t.Fatalf("new entries came back as %d, %d", idx(fresh[0]), idx(fresh[1]))
	}
	if err := again.Commit(append(replayed, fresh...)); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, admin, stream); n != 0 {
		t.Fatalf("%d entries still pending; the deleted one was never acknowledged", n)
	}
	if s := globalStats(t, again); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v, want empty", s)
	}
}

// SPEC §9.5: an undecodable entry goes to <stream>:quarantine with its fields
// and a reason, leaves the stream and the counter, and is counted.
func TestRedisUndecodableEntryIsQuarantined(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	m := obs.NewMetrics(obs.Info{})
	r := newRedisRing(t, addr, stream, "replica-q", redisOpts{metrics: m})

	if err := r.Enqueue(context.Background(), mkItem(0, 10)); err != nil {
		t.Fatal(err)
	}
	// Written as the enqueue script would, by a binary with another codec.
	ctx := context.Background()
	if err := admin.Do(ctx, admin.B().Xadd().Key(stream).Id("*").FieldValue().
		FieldValue("h", `{"v":2}`).FieldValue("b", "raw line").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Do(ctx, admin.B().Incrby().Key(stream+":bytes").Increment(8).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := r.Enqueue(context.Background(), mkItem(2, 10)); err != nil {
		t.Fatal(err)
	}

	got := dequeueN(t, r, 2, 5*time.Second)
	if idx(got[0]) != 0 || idx(got[1]) != 2 {
		t.Fatalf("got items %d, %d; want the two good ones", idx(got[0]), idx(got[1]))
	}
	if n := m.RingQuarantinedTotal.Load(); n != 1 {
		t.Fatalf("syslog_ring_quarantined_total = %d, want 1", n)
	}
	if s := globalStats(t, r); s.Items != 2 || s.Bytes != 20 {
		t.Fatalf("stats = %+v; the quarantined entry must leave the stream and the counter", s)
	}
	q, err := admin.Do(ctx, admin.B().Xrange().Key(stream+":quarantine").Start("-").End("+").Build()).AsXRange()
	if err != nil || len(q) != 1 {
		t.Fatalf("quarantine stream = %v %v", q, err)
	}
	f := q[0].FieldValues
	if f["b"] != "raw line" || f["h"] != `{"v":2}` || f["reason"] == "" || f["id"] == "" {
		t.Fatalf("quarantine entry = %v", f)
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if n := pendingCount(t, admin, stream); n != 0 {
		t.Fatalf("%d entries still pending", n)
	}
}

// SPEC §9.5: at boot, XLEN = 0 with a non-zero counter is zeroed.
func TestRedisOrphanCounterIsResetAtBoot(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	if err := admin.Do(context.Background(), admin.B().Set().Key(stream+":bytes").Value("999").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	r := newRedisRing(t, addr, stream, "replica-o", redisOpts{})
	if s := r.Stats(); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v, want the orphan counter reset", s)
	}
	v, err := admin.Do(context.Background(), admin.B().Get().Key(stream+":bytes").Build()).ToString()
	if err != nil || v != "0" {
		t.Fatalf("counter = %q %v, want 0", v, err)
	}
	// With entries present the counter is left alone: reconciling it is manual.
	if err := r.Enqueue(context.Background(), mkItem(0, 5)); err != nil {
		t.Fatal(err)
	}
	if err := admin.Do(context.Background(), admin.B().Set().Key(stream+":bytes").Value("777").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	r2 := newRedisRing(t, addr, stream, "replica-o2", redisOpts{})
	if s := r2.Stats(); s.Bytes != 777 {
		t.Fatalf("counter with a non-empty stream = %d, want it untouched (777)", s.Bytes)
	}
}

// Fatal replies at construction end the boot at once (exit 3) instead of being
// retried until boot_timeout.
func TestRedisFatalRepliesAtConstruction(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)

	// Credentials the server refuses: WRONGPASS.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := NewRedis(ctx, RedisConfig{
		Config:   rediscli.Config{Addrs: []string{addr}, Username: "nobody", Password: "wrong"},
		Stream:   "test:ring:never",
		Consumer: "c",
	}, discard())
	if !errors.Is(err, ErrFatal) {
		t.Fatalf("bad credentials: err = %v, want ErrFatal", err)
	}
	t.Logf("bad credentials: %v", err)

	// The stream key holding another type: WRONGTYPE.
	stream := uniqueStream(t, admin)
	if err := admin.Do(context.Background(), admin.B().Set().Key(stream).Value("x").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	_, err = NewRedis(ctx, RedisConfig{
		Config:   rediscli.Config{Addrs: []string{addr}},
		Stream:   stream,
		Consumer: "c",
	}, discard())
	if !errors.Is(err, ErrFatal) {
		t.Fatalf("wrong type: err = %v, want ErrFatal", err)
	}
	t.Logf("wrong type: %v", err)
}

// tcpProxy stands between the ring and Redis so a test can make the server
// disappear -- listener and live connections -- without touching the server.
type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

func startProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				c.Close()
				u.Close()
				return
			}
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			go func() { io.Copy(u, c); u.Close() }()
			go func() { io.Copy(c, u); c.Close() }()
		}
	}()
	t.Cleanup(p.kill)
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.ln.Close()
	for _, c := range p.conns {
		c.Close()
	}
}

// SPEC §9.5: with the ring's Redis gone, Enqueue is ErrUnavailable (never
// ErrFull, never nil), Commit fails without pretending, Dequeue backs off and
// returns an empty batch rather than an error that would stop the workers, and
// Available turns false for /readyz.
func TestRedisUnavailableWhenTheServerIsGone(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	proxy := startProxy(t, addr)
	r := newRedisRing(t, addr, stream, "replica-u", redisOpts{addr: proxy.addr()})

	if err := r.Enqueue(context.Background(), mkItem(0, 10)); err != nil {
		t.Fatal(err)
	}
	held := dequeueN(t, r, 1, 5*time.Second)
	if !r.Available() {
		t.Fatal("Available is false with the server up")
	}

	proxy.kill()

	err := r.Enqueue(context.Background(), mkItem(1, 10))
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Enqueue with the server gone: %v, want ErrUnavailable", err)
	}
	if errors.Is(err, ErrFull) || errors.Is(err, ErrFatal) {
		t.Fatalf("a lost connection must be neither full nor fatal: %v", err)
	}
	t.Logf("enqueue with the server gone: %v", err)
	if err := r.Commit(held); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Commit with the server gone: %v, want ErrUnavailable", err)
	}
	start := time.Now()
	items, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("Dequeue with the server gone = %d %v, want an empty batch and no error", len(items), err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("Dequeue returned without backing off; the workers would spin")
	}
	if r.Available() {
		t.Fatal("Available still true with the server gone")
	}
	// A cancelled worker still gets out promptly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Dequeue(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dequeue on a cancelled context: %v", err)
	}
	r.Close()
	if err := r.Enqueue(context.Background(), mkItem(2, 10)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close: %v, want ErrClosed", err)
	}
}

// Many receivers and workers at once, under -race: every accepted item is
// delivered exactly once by one of two replicas, and the counter ends at zero.
func TestRedisConcurrentEnqueueAndDrain(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	a := newRedisRing(t, addr, stream, "replica-ca", redisOpts{})
	b := newRedisRing(t, addr, stream, "replica-cb", redisOpts{})

	const producers, each = 4, 50
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			r := a
			if p%2 == 1 {
				r = b
			}
			for i := 0; i < each; i++ {
				if err := r.Enqueue(context.Background(), mkItem(p*each+i, 16)); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var mu sync.Mutex
	seen := map[int]int{}
	total := 0
	var dw sync.WaitGroup
	for w := 0; w < 4; w++ {
		r := a
		if w%2 == 1 {
			r = b
		}
		dw.Add(1)
		go func() {
			defer dw.Done()
			for ctx.Err() == nil {
				items, err := r.Dequeue(ctx, 16)
				if err != nil {
					return
				}
				if err := r.Commit(items); err != nil {
					t.Errorf("commit: %v", err)
				}
				mu.Lock()
				for _, it := range items {
					seen[idx(it)]++
				}
				total += len(items)
				done := total >= producers*each
				mu.Unlock()
				if done {
					cancel()
				}
			}
		}()
	}
	wg.Wait()
	dw.Wait()

	if total != producers*each || len(seen) != producers*each {
		t.Fatalf("delivered %d (%d distinct) of %d", total, len(seen), producers*each)
	}
	if s := globalStats(t, a); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v, want empty", s)
	}
}

// A stream deleted under a running ring (a manual DEL, a FLUSHALL) takes the
// group with it. The drain recreates the group instead of failing forever, the
// orphaned counter is zeroed as at boot, and the ring keeps working.
func TestRedisRecoversFromADeletedStream(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	r := newRedisRing(t, addr, stream, "replica-n", redisOpts{})

	if err := r.Enqueue(context.Background(), mkItem(0, 9)); err != nil {
		t.Fatal(err)
	}
	dequeueN(t, r, 1, 5*time.Second)
	if err := admin.Do(context.Background(), admin.B().Del().Key(stream).Build()).Error(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		items, err := r.Dequeue(ctx, 10)
		if err != nil {
			t.Fatalf("Dequeue after the stream was deleted: %v", err)
		}
		if len(items) != 0 {
			t.Fatalf("got %d items from a deleted stream", len(items))
		}
		if s := globalStats(t, r); s.Bytes == 0 {
			break
		}
	}
	if err := r.Enqueue(context.Background(), mkItem(1, 5)); err != nil {
		t.Fatal(err)
	}
	got := dequeueN(t, r, 1, 5*time.Second)
	if idx(got[0]) != 1 {
		t.Fatalf("got item %d", idx(got[0]))
	}
	if err := r.Commit(got); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, r); s.Items != 0 || s.Bytes != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// pendingIdle returns the idle time of every pending entry of the group, by id.
func pendingIdle(t *testing.T, admin rueidis.Client, stream string) map[string]time.Duration {
	t.Helper()
	rows, err := admin.Do(context.Background(), admin.B().Xpending().Key(stream).Group("syslog-ring").
		Start("-").End("+").Count(1000).Build()).ToArray()
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]time.Duration, len(rows))
	for _, row := range rows {
		f, err := row.ToArray()
		if err != nil || len(f) < 3 {
			t.Fatalf("XPENDING row: %v %v", f, err)
		}
		id, _ := f[0].ToString()
		ms, _ := f[2].AsInt64()
		out[id] = time.Duration(ms) * time.Millisecond
	}
	return out
}

// Review finding F3a: Available must follow WRITES. An ACL that loses +@write
// (NOPERM), like a server at maxmemory (OOM) or a replica promoted by mistake
// (READONLY), still answers XLEN and GET; a read-only refresh cleared the flag
// every second between two refused enqueues, and /readyz flapped. The write
// probe (XADD <stream>:probe NOMKSTREAM) keeps it false until writes work
// again, and never creates its key.
func TestRedisAvailabilityFollowsWritesNotReads(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	ctx := context.Background()
	user := fmt.Sprintf("ringtest-%d", time.Now().UnixNano())
	acl := func(rules ...string) {
		t.Helper()
		if err := admin.Do(ctx, admin.B().Arbitrary("ACL", "SETUSER").Args(append([]string{user}, rules...)...).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	acl("reset", "on", ">pw", "~*", "&*", "+@all")
	t.Cleanup(func() { admin.Do(ctx, admin.B().Arbitrary("ACL", "DELUSER").Args(user).Build()) })

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := NewRedis(tctx, RedisConfig{
		Config:       rediscli.Config{Addrs: []string{addr}, Username: user, Password: "pw"},
		Stream:       stream,
		ClaimIdle:    time.Hour,
		Consumer:     "replica-acl",
		BlockTimeout: 100 * time.Millisecond,
		OpTimeout:    time.Second,
	}, discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	if err := r.Enqueue(ctx, mkItem(0, 10)); err != nil || !r.Available() {
		t.Fatalf("healthy: enqueue %v, available %v", err, r.Available())
	}

	acl("-@write") // reads still work; every write is NOPERM
	if err := r.RefreshStats(ctx); err == nil {
		t.Fatal("the refresh passed with writes refused: it is still read-only")
	}
	if r.Available() {
		t.Fatal("Available true with every write refused")
	}
	if s := r.Stats(); s.Items != 1 || s.Bytes != 10 {
		t.Fatalf("stats = %+v: the reads still work and must still be published", s)
	}
	if err := r.Enqueue(ctx, mkItem(1, 10)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("enqueue with writes refused: %v, want ErrUnavailable", err)
	}
	// Across several rounds of the refresh loop it never flaps back.
	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if r.Available() {
			t.Fatal("Available flapped back to true while writes are refused")
		}
		time.Sleep(50 * time.Millisecond)
	}

	acl("+@all")
	deadline = time.Now().Add(5 * time.Second)
	for !r.Available() {
		if time.Now().After(deadline) {
			t.Fatal("Available did not recover after writes were allowed again")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n, err := admin.Do(ctx, admin.B().Exists().Key(stream+":probe").Build()).AsInt64(); err != nil || n != 0 {
		t.Fatalf("the probe key exists (%d, %v): NOMKSTREAM must never create it", n, err)
	}
}

// Review finding F6: an entry waiting in the claim queue may already be idle
// for up to claim_idle/2 when a worker takes it. It is renewed at hand-out, so
// its idle clock starts there and another replica cannot claim a batch half a
// claim_idle into its delivery.
func TestRedisClaimedEntriesAreRenewedAtHandOut(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)

	dead := newRedisRing(t, addr, stream, "replica-dead6", redisOpts{})
	for i := 0; i < 3; i++ {
		if err := dead.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	dequeueN(t, dead, 3, 5*time.Second)
	dead.Close()

	// The claim loop of this ring never ticks (claim_idle 1 h); the round is
	// driven by hand with a short min-idle, so nothing but takeClaimed can
	// renew the queue.
	live := newRedisRing(t, addr, stream, "replica-live6", redisOpts{})
	if items, err := live.Dequeue(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("first Dequeue = %d %v", len(items), err)
	}
	live.idleMS = "50"
	time.Sleep(100 * time.Millisecond)
	live.claimRound("0-0")
	if n := live.claimedLen(); n != 3 {
		t.Fatalf("claimed %d, want 3", n)
	}

	const wait = 700 * time.Millisecond
	time.Sleep(wait) // the entries age in the queue
	got, err := live.Dequeue(context.Background(), 10)
	if err != nil || len(got) != 3 {
		t.Fatalf("Dequeue = %d %v", len(got), err)
	}
	idle := pendingIdle(t, admin, stream)
	for _, it := range got {
		d, ok := idle[it.StreamID]
		if !ok {
			t.Fatalf("%s is not pending", it.StreamID)
		}
		if d >= wait/2 {
			t.Fatalf("%s handed out with %v of idle time; the renewal at hand-out is missing", it.StreamID, d)
		}
	}
}

// Review finding F8: Quiesce lets a shutdown finish the batches the workers
// hold without pulling more from a stream other replicas keep feeding.
func TestRedisQuiesceStopsPullingButFinishesHeldBatches(t *testing.T) {
	addr := redisAddr(t)
	admin := adminClient(t, addr)
	stream := uniqueStream(t, admin)
	m := obs.NewMetrics(obs.Info{})

	r := newRedisRing(t, addr, stream, "replica-q8", redisOpts{claimIdle: 300 * time.Millisecond, metrics: m})
	for i := 0; i < 5; i++ {
		if err := r.Enqueue(context.Background(), mkItem(i, 10)); err != nil {
			t.Fatal(err)
		}
	}
	held := dequeueN(t, r, 2, 5*time.Second) // a worker's batch in hand

	// A dead replica's entries that the claim loop would otherwise take.
	dead := newRedisRing(t, addr, stream, "replica-dead8", redisOpts{})
	dequeueN(t, dead, 2, 5*time.Second)
	dead.Close()

	r.Quiesce()
	r.Quiesce() // idempotent
	claimedBefore := m.RingRedisClaimedTotal.Load()

	start := time.Now()
	items, err := r.Dequeue(context.Background(), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("Dequeue after Quiesce = %d %v, want an empty batch and no error", len(items), err)
	}
	if d := time.Since(start); d < 20*time.Millisecond || d > time.Second {
		t.Fatalf("quiesced Dequeue took %v: it must wait briefly, not spin and not block", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Dequeue(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("quiesced Dequeue on a cancelled context: %v", err)
	}

	// Nothing new was read and nothing was claimed, through several claim
	// rounds: one entry stays unread, the dead replica's two stay its own.
	time.Sleep(time.Second)
	if n := m.RingRedisClaimedTotal.Load() - claimedBefore; n != 0 || r.claimedLen() != 0 {
		t.Fatalf("claimed %d (queue %d) after Quiesce", n, r.claimedLen())
	}
	if n := pendingCount(t, admin, stream); n != 4 {
		t.Fatalf("%d pending, want the 2 held + the dead replica's 2", n)
	}

	// The held batch still finishes.
	if err := r.Touch(held); err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(held); err != nil {
		t.Fatal(err)
	}
	if s := globalStats(t, r); s.Items != 3 || s.Bytes != 30 {
		t.Fatalf("stats after the held commit = %+v, want 3 / 30", s)
	}

	// The own-pending replay stops too: a restarted, quiesced instance does
	// not re-read what its previous life held.
	again := newRedisRing(t, addr, stream, "replica-dead8", redisOpts{})
	again.Quiesce()
	if items, err := again.Dequeue(context.Background(), 10); err != nil || len(items) != 0 {
		t.Fatalf("quiesced replay = %d %v", len(items), err)
	}
}
