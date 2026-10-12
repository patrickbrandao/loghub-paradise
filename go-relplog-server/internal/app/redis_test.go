package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// sink=redis and ring=redis against a real server addressed by
// RELPLOG_TEST_REDIS_ADDR; skipped without it:
//
//	docker run --rm -d -p 16381:6379 --name sls-app-redis redis:7-alpine
//	RELPLOG_TEST_REDIS_ADDR=127.0.0.1:16381 go test -race -run Redis ./internal/app/

const redisAddrEnv = "RELPLOG_TEST_REDIS_ADDR"

type redisEnv struct {
	addr  string
	admin rueidis.Client
}

func redisTest(t *testing.T) *redisEnv {
	t.Helper()
	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		t.Skipf("set %s to run the redis suite", redisAddrEnv)
	}
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		t.Fatalf("%s is not reachable: %v", redisAddrEnv, err)
	}
	t.Cleanup(c.Close)
	return &redisEnv{addr: addr, admin: c}
}

// del removes keys now and when the test ends.
func (e *redisEnv) del(t *testing.T, keys ...string) {
	t.Helper()
	do := func() {
		for _, k := range keys {
			e.admin.Do(context.Background(), e.admin.B().Del().Key(k).Build())
		}
	}
	do()
	t.Cleanup(do)
}

func (e *redisEnv) xlen(t *testing.T, key string) int64 {
	t.Helper()
	n, err := e.admin.Do(context.Background(), e.admin.B().Xlen().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// pendingOf is how many entries of the group consumer holds without an ack.
func (e *redisEnv) pendingOf(t *testing.T, stream, group, consumer string) int {
	t.Helper()
	res, err := e.admin.Do(context.Background(), e.admin.B().Xpending().Key(stream).Group(group).
		Start("-").End("+").Count(10000).Consumer(consumer).Build()).ToArray()
	if err != nil {
		if rueidis.IsRedisNil(err) || strings.Contains(err.Error(), "NOGROUP") {
			return 0
		}
		t.Fatal(err)
	}
	return len(res)
}

// SPEC §3.6, §10.3: ring=memory -> redis. One XADD per envelope on
// loghub.{space}.syslog, ONE field e holding the JLH byte for byte.
func TestRedisSinkEndToEnd(t *testing.T) {
	e := redisTest(t)
	const stream = "loghub.default.syslog"
	e.del(t, stream, stream+":healthprobe")

	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--sink.type=redis", "--sink.redis.addrs="+e.addr), nil)
	in.waitReady()
	lines := natsLines("redis-sink", 25)
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 20*time.Second, "every entry in the stream", func() bool { return e.xlen(t, stream) == int64(len(lines)) })

	entries, err := e.admin.Do(context.Background(), e.admin.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, x := range entries {
		if len(x.FieldValues) != 1 {
			t.Errorf("entry %s has fields %v, want only e", x.ID, keysOf(x.FieldValues))
		}
		env, ok := x.FieldValues["e"]
		if !ok {
			t.Errorf("entry %s has no field e", x.ID)
			continue
		}
		j := mustJLH(t, []byte(env))
		if j.From != testID.From || j.Space != "default" {
			t.Errorf("envelope identity %s/%s", j.From, j.Space)
		}
		seen[j.Payload.message()] = true
	}
	for i := range lines {
		if !seen[fmt.Sprintf("redis-sink-%04d", i)] {
			t.Errorf("redis-sink-%04d is missing", i)
		}
	}
	if got := in.metric(`relplog_config_info{ring_mode="memory",sink_type="redis",transports="tcp"}`); got != "1" {
		t.Errorf("relplog_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// sharedRing is a ring=redis stream and group unique to one test.
type sharedRing struct {
	stream, group string
}

func newSharedRing(t *testing.T, e *redisEnv) sharedRing {
	n := time.Now().UnixNano()
	r := sharedRing{stream: fmt.Sprintf("sls:app:ring:%d", n), group: fmt.Sprintf("sls-app-%d", n)}
	e.del(t, r.stream, r.stream+":bytes")
	return r
}

func (r sharedRing) args(e *redisEnv, dir string, extra ...string) []string {
	return append(append(baseArgs(dir),
		"--ring.mode=redis",
		"--ring.redis.addrs="+e.addr,
		"--ring.redis.stream="+r.stream,
		"--ring.redis.group="+r.group,
		"--ring.redis.claim_idle=1s",
		// claim_idle must exceed twice the longest write, or another replica
		// could claim a batch whose write is still in flight.
		"--sink.op_timeout=400ms",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=20ms",
	), extra...)
}

// rowsFrom reads an instance's events.db, validating every envelope, keyed by
// message.
func rowsFrom(t *testing.T, path string) map[string]jlhEnv {
	t.Helper()
	if sqliteCount(t, path, "syslog_events") <= 0 {
		return map[string]jlhEnv{}
	}
	return byMessage(t, sqliteRows(t, path))
}

// SPEC §9.5, §3.1, §3.5: two instances share one ring stream as consumers of
// one group, each with its own AgentUUID. Instance A receives every message;
// both drain. What B delivers carries A's from -- the instance that RECEIVED,
// not the one that drained -- and the union of both destinations is every
// message exactly once.
//
// The split is made deterministic: A's destination refuses writes while the
// first message is in its hands, so A's only worker is busy retrying and the
// rest can only be read by B; then A's destination recovers.
func TestRedisRingSharedByTwoInstances(t *testing.T) {
	e := redisTest(t)
	r := newSharedRing(t, e)
	dirA, dirB := t.TempDir(), t.TempDir()

	fsA := newFaultySink(false)
	fsA.failWrites.Store(true)
	a := launch(t, testID, r.args(e, dirA), fsA.wrap)
	a.waitReady()
	tcpA := a.app.TransportAddr("tcp")

	sendRELP(t, tcpA, "<13>1 2026-09-12T14:33:00Z host app - - - shared-first")
	waitFor(t, 10*time.Second, "A to hold the first message", func() bool {
		return e.pendingOf(t, r.stream, r.group, testID.From) == 1 && fsA.attempts.Load() > 1
	})

	b := launch(t, otherID, r.args(e, dirB), nil)
	b.waitReady()

	const n = 20
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - shared-%02d", i)
	}
	sendRELP(t, tcpA, lines...)
	waitFor(t, 20*time.Second, "B to deliver what A could not read", func() bool {
		return sqliteCount(t, filepath.Join(dirB, "events.db"), "syslog_events") == n
	})
	if got := sqliteCount(t, filepath.Join(dirA, "events.db"), "syslog_events"); got != 0 {
		t.Fatalf("A delivered %d rows through a destination that refuses writes", got)
	}

	fsA.failWrites.Store(false)
	waitFor(t, 20*time.Second, "A to deliver the message it held", func() bool {
		return sqliteCount(t, filepath.Join(dirA, "events.db"), "syslog_events") == 1
	})
	waitFor(t, 10*time.Second, "the stream to be empty", func() bool { return e.xlen(t, r.stream) == 0 })

	rowsA := rowsFrom(t, filepath.Join(dirA, "events.db"))
	rowsB := rowsFrom(t, filepath.Join(dirB, "events.db"))
	if _, ok := rowsA["shared-first"]; !ok || len(rowsA) != 1 {
		t.Errorf("A delivered %v, want only shared-first", keysOfEnv(rowsA))
	}
	uuids := map[string]bool{}
	for _, m := range []map[string]jlhEnv{rowsA, rowsB} {
		for msg, env := range m {
			if env.From != testID.From {
				t.Errorf("%s: from = %s, want the receiving instance %s", msg, env.From, testID.From)
			}
			if uuids[env.UUID] {
				t.Errorf("%s delivered twice", msg)
			}
			uuids[env.UUID] = true
		}
	}
	for _, l := range lines {
		if _, ok := rowsB[l[strings.LastIndexByte(l, ' ')+1:]]; !ok {
			t.Errorf("%q is missing from B", l)
		}
	}
	if got := e.admin.Do(context.Background(), e.admin.B().Get().Key(r.stream+":bytes").Build()); got.Error() == nil {
		if v, _ := got.ToString(); v != "0" {
			t.Errorf("the byte counter is %s with an empty stream", v)
		}
	}
	if code := b.stop(); code != ExitOK {
		t.Errorf("B exited %d", code)
	}
	if code := a.stop(); code != ExitOK {
		t.Errorf("A exited %d", code)
	}
}

func keysOfEnv(m map[string]jlhEnv) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// SPEC §9.5: a worker holding a batch in retry renews its claim (Touch), so a
// live instance's batch is never taken; once that instance is gone -- nothing
// acked, nothing touched any more -- another replica's XAUTOCLAIM takes the
// entries after claim_idle (1 s here) and delivers them, from intact.
//
// Stopping A leaves the stream exactly as a kill would: a ring=redis shutdown
// acks nothing it did not deliver (SPEC §13.4), and the touches stop.
func TestRedisRingClaimsFromADeadReplica(t *testing.T) {
	e := redisTest(t)
	r := newSharedRing(t, e)
	dirA, dirB := t.TempDir(), t.TempDir()

	fsA := newFaultySink(false)
	fsA.failWrites.Store(true)
	// A short drain: a ring=redis shutdown first gives the workers
	// drain_timeout to deliver what they hold, then surrenders it pending.
	a := launch(t, testID, r.args(e, dirA, "--shutdown.drain_timeout=300ms"), fsA.wrap)
	a.waitReady()

	const n = 10
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - claim-%02d", i)
	}
	sendRELP(t, a.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "A to accept everything and hold a batch", func() bool {
		return a.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n &&
			e.pendingOf(t, r.stream, r.group, testID.From) > 0 && fsA.attempts.Load() > 1
	})
	held := e.pendingOf(t, r.stream, r.group, testID.From)
	t.Logf("A holds %d of %d entries; B will read the other %d as new", held, n, n-held)

	b := launch(t, otherID, r.args(e, dirB), nil)
	b.waitReady()
	eventsB := filepath.Join(dirB, "events.db")
	// B takes whatever A never read at once; what A holds is not B's to take.
	waitFor(t, 10*time.Second, "B to deliver what A never read", func() bool {
		return sqliteCount(t, eventsB, "syslog_events") == n-held
	})
	// A is alive and touching: for three claim_idle periods, B claims nothing.
	// This is the one bounded observation of the suite -- an absence has no
	// condition to poll for.
	if eventually(3*time.Second, func() bool { return b.app.metrics.RingRedisClaimedTotal.Load() > 0 }) {
		t.Fatalf("B claimed %d entries from a LIVE replica whose worker is retrying them",
			b.app.metrics.RingRedisClaimedTotal.Load())
	}
	if got := e.pendingOf(t, r.stream, r.group, testID.From); got != held {
		t.Fatalf("A holds %d entries, want still %d", got, held)
	}

	// A goes away mid-retry.
	if code := a.stop(); code != ExitOK {
		t.Errorf("A exited %d: what it held stays in the ring, which is not a failure", code)
	}
	if got := e.pendingOf(t, r.stream, r.group, testID.From); got != held {
		t.Fatalf("A's shutdown changed its pending entries: %d, want %d", got, held)
	}
	waitFor(t, 15*time.Second, "B to claim and deliver A's batch", func() bool {
		return sqliteCount(t, eventsB, "syslog_events") == n
	})
	if got := b.app.metrics.RingRedisClaimedTotal.Load(); got != uint64(held) {
		t.Errorf("relplog_ring_redis_claimed_total = %d, want %d", got, held)
	}
	if got := sqliteCount(t, filepath.Join(dirA, "events.db"), "syslog_events"); got != 0 {
		t.Errorf("A delivered %d rows", got)
	}
	rows := rowsFrom(t, eventsB)
	for _, l := range lines {
		msg := l[strings.LastIndexByte(l, ' ')+1:]
		env, ok := rows[msg]
		switch {
		case !ok:
			t.Errorf("%s is missing", msg)
		case env.From != testID.From:
			t.Errorf("%s: from = %s, want the instance that received it", msg, env.From)
		}
	}
	waitFor(t, 10*time.Second, "the stream to be empty", func() bool { return e.xlen(t, r.stream) == 0 })
	if code := b.stop(); code != ExitOK {
		t.Errorf("B exited %d", code)
	}
}
