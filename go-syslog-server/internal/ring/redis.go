package ring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
)

// ErrFatal marks a ring=redis failure that waiting cannot fix: a wrong
// password, an ACL without permission, the stream key holding another type,
// a server at maxmemory (rediscli.Fatal), or a configuration this ring cannot
// work with (a Redis Cluster without a hash-tagged stream name).
//
// How the app uses it (SPEC §13.1 step 8, §13.5): NewRedis makes ONE attempt.
// An error that matches errors.Is(err, ErrFatal) ends the boot with exit 3 at
// once; any other error is a connection failure, and the app calls NewRedis
// again with backoff until sink.boot_timeout, then exits with 3. Without the
// distinction a bad password would be retried for five minutes in a crashloop
// that looks exactly like a Redis that is still starting (SPEC §10.3, whose
// table also classifies this client).
//
// After construction an Enqueue refused by a fatal reply matches BOTH
// ErrUnavailable and ErrFatal: the receiver treats it as unavailability (UDP
// drops with reason="ring_unavailable", TCP stops reading), and nothing in the
// ring's interface asks it to do more.
var ErrFatal = errors.New("ring: fatal")

// Defaults of [ring.redis] (SPEC §9.5, §16.5).
const (
	DefaultRedisStream    = "syslog:ring"
	DefaultRedisGroup     = "syslog-ring"
	DefaultRedisClaimIdle = 30 * time.Second
)

// RedisConfig mirrors [ring.redis] plus the two [ring] ceilings, which with
// this mode are GLOBAL: one stream, one ceiling, every replica (SPEC §9.5).
type RedisConfig struct {
	// Config is the connection: addrs, credentials, db, sentinel, TLS.
	rediscli.Config

	// Stream is ring.redis.stream. The byte counter lives in
	// "<Stream>:bytes" and undecodable entries go to "<Stream>:quarantine".
	// On a Redis Cluster the three keys must share a slot, so the name must
	// carry a hash tag, e.g. "{syslog:ring}"; NewRedis refuses it otherwise.
	Stream string
	// Group is ring.redis.group, created at construction if absent.
	Group string
	// ClaimIdle is ring.redis.claim_idle: how long a pending entry must be
	// idle before XAUTOCLAIM takes it from its consumer. The claim loop runs
	// every ClaimIdle/2; a worker holding a batch calls Touch every
	// ClaimIdle/3.
	ClaimIdle time.Duration

	// CapItems and CapBytes are ring.capacity_items and ring.max_bytes, over
	// the whole stream. Zero or less means no ceiling.
	CapItems int
	CapBytes int64

	// Consumer is this instance's AgentUUID: its name in the consumer group.
	// It must be stable across restarts, or the own-pending replay of the
	// next boot would look for another consumer's entries.
	Consumer string

	// BlockTimeout is the BLOCK of XREADGROUP (≈1 s): how long an idle
	// Dequeue waits before returning an empty batch.
	BlockTimeout time.Duration
	// OpTimeout bounds every other round trip: the enqueue script, the commit
	// script, XCLAIM, XAUTOCLAIM, the stats refresh. The app passes
	// sink.op_timeout. Default 5 s.
	OpTimeout time.Duration
	// ClaimCount is the COUNT of XAUTOCLAIM and the ceiling of claimed entries
	// waiting for a worker. The app passes sink.batch. Default 256.
	ClaimCount int

	// Metrics receives syslog_ring_quarantined_total and
	// syslog_ring_redis_claimed_total (SPEC §14.1). Nil is allowed.
	Metrics *obs.Metrics
}

// The scripts. Every write to the stream or the byte counter happens inside
// one of them, which is why the counter cannot drift by a crash: a script runs
// to completion or not at all (SPEC §9.5).
//
// rueidis.Lua.Exec sends EVALSHA and falls back to EVAL on NOSCRIPT, building
// a fresh command for each attempt -- the recycled-command trap of invariant
// §20.11 does not apply to it.

// enqueueScript is SPEC §9.5 verbatim. The ceiling is checked and the entry
// appended in one atomic step, so two replicas racing for the last slot cannot
// both win: the global ceiling is exact, not approximate.
const enqueueScript = `
-- KEYS[1]=stream KEYS[2]=bytes ARGV: cap_items cap_bytes nbytes head raw
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
local b = tonumber(redis.call('GET', KEYS[2]) or '0')
if b + tonumber(ARGV[3]) > tonumber(ARGV[2]) then return 0 end
redis.call('XADD', KEYS[1], '*', 'h', ARGV[4], 'b', ARGV[5])
redis.call('INCRBY', KEYS[2], ARGV[3])
return 1
`

// commitScript acknowledges and deletes delivered entries, and decrements the
// counter only by the entries XDEL actually removed.
//
// That last condition is what keeps the counter exact under at-least-once: an
// entry XAUTOCLAIMed by a replica while the original consumer was frozen is
// delivered twice, and both replicas commit it. Only the first XDEL removes
// it; decrementing for the second as well would drive the counter below the
// stream's real size, and the byte ceiling would then admit more than it
// should, forever.
//
// nbytes per id comes from the caller (len(raw), the same number the enqueue
// script added) rather than from an XRANGE inside the script: reading the
// entry back would copy up to sink.batch x 64 KiB through Lua on Redis's main
// thread on every commit.
const commitScript = `
-- KEYS[1]=stream KEYS[2]=bytes ARGV[1]=group ARGV[2..]=id nbytes id nbytes ...
local freed = 0
for i = 2, #ARGV, 2 do
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[i])
  if redis.call('XDEL', KEYS[1], ARGV[i]) == 1 then
    freed = freed + tonumber(ARGV[i+1])
  end
end
if freed > 0 then redis.call('DECRBY', KEYS[2], freed) end
return freed
`

// quarantineScript moves one undecodable entry to "<stream>:quarantine", so the
// entry is in exactly one of the two streams at any instant (the Redis twin of
// the quarantine table of SPEC §9.4). The bytes it frees are read from the
// entry itself: the caller could not decode it, so it has no trustworthy
// number of its own.
const quarantineScript = `
-- KEYS[1]=stream KEYS[2]=bytes KEYS[3]=quarantine
-- ARGV[1]=group ARGV[2]=id ARGV[3]=reason ARGV[4]=consumer
local e = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2])
local n = 0
if #e > 0 then
  local f = e[1][2]
  local args = {}
  for j = 1, #f, 2 do
    args[#args+1] = f[j]
    args[#args+1] = f[j+1]
    if f[j] == 'b' then n = string.len(f[j+1]) end
  end
  args[#args+1] = 'id'
  args[#args+1] = ARGV[2]
  args[#args+1] = 'reason'
  args[#args+1] = ARGV[3]
  args[#args+1] = 'consumer'
  args[#args+1] = ARGV[4]
  redis.call('XADD', KEYS[3], '*', unpack(args))
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if redis.call('XDEL', KEYS[1], ARGV[2]) == 1 and n > 0 then
  redis.call('DECRBY', KEYS[2], n)
end
return n
`

// orphanScript zeroes a counter left over from a stream that no longer has
// entries (SPEC §9.5: XLEN = 0 with a non-zero counter, at boot). It is a
// script and not an XLEN followed by a SET because another replica may be
// enqueueing at the same moment: between the two calls its XADD + INCRBY would
// land, and the SET would erase bytes that are really there.
const orphanScript = `
-- KEYS[1]=stream KEYS[2]=bytes
if redis.call('XLEN', KEYS[1]) ~= 0 then return false end
local b = redis.call('GET', KEYS[2])
if not b or b == '0' then return false end
redis.call('SET', KEYS[2], '0')
return b
`

var (
	luaEnqueue    = rueidis.NewLuaScript(enqueueScript)
	luaCommit     = rueidis.NewLuaScript(commitScript)
	luaQuarantine = rueidis.NewLuaScript(quarantineScript)
	luaOrphan     = rueidis.NewLuaScript(orphanScript)
)

// unlimited stands in for "no ceiling" in the enqueue script, which compares
// numbers as Lua doubles: 2^53 is exact there, and no stream gets near it.
const unlimited = int64(1) << 53

// idsPerScript bounds the ids of one commit or XCLAIM call. A worker's batch is
// sink.batch (256); the bound only matters for a caller passing more, and keeps
// one script from holding Redis's main thread for long.
const idsPerScript = 1024

// RedisRing is ring.mode = redis (SPEC §9.5, decision 11): a stream SHARED by
// every replica, in a Redis of the ring's own, read through a consumer group.
// It is the only ring that survives the loss of the node: what one instance
// accepted, another can deliver.
//
// The guarantee of Enqueue is that the enqueue script returned: the entry is in
// Redis, with whatever durability that Redis's appendfsync gives (SPEC §2.3).
//
// Delivery is at-least-once and FIFO per instance, not global. Duplicates come
// from XAUTOCLAIM taking the entries of a replica that froze for longer than
// claim_idle and then came back; the consumers deduplicate on (uuid, step)
// (SPEC §10.2), and the commit script keeps the byte counter exact regardless.
//
// The envelope's from and space come from the item head, the instance that
// RECEIVED the message; the consumer that drained it is invisible downstream.
// That is why every replica must share the conversion config (SPEC §3.5).
type RedisRing struct {
	cfg    RedisConfig
	log    *slog.Logger
	m      *obs.Metrics
	client rueidis.Client

	keys     []string // stream, bytes: read-only, shared by every call
	qkeys    []string // stream, bytes, quarantine
	probeKey string   // <stream>:probe, never created (RefreshStats)
	capItems string
	capBytes string
	idleMS   string

	// Own-pending replay (SPEC §9.5, recovery): on first use, the entries this
	// consumer read in a previous life and never acknowledged are re-read with
	// XREADGROUP ... 0, paging by the last id seen, before anything new is
	// asked for.
	replayMu     sync.Mutex
	replayCursor string
	replayed     int
	replayDone   atomic.Bool

	// claimed holds the entries XAUTOCLAIM took from idle consumers, until a
	// worker's Dequeue picks them up. It is bounded by ClaimCount, and the
	// claim loop renews their ownership every round, so a replica that cannot
	// drain does not keep other replicas from claiming the rest.
	claimMu sync.Mutex
	claimed []*item.Item

	// The global occupancy, refreshed every second (SPEC §9.5: /readyz and the
	// gauges read a cache, never Redis).
	statItems atomic.Int64
	statBytes atomic.Int64
	down      atomic.Bool

	backoffNS atomic.Int64

	// One sampler per kind of failure, so an outage logs each kind at most
	// once per 30 s instead of once per worker per retry (SPEC §14.2), and one
	// kind does not hide another.
	readLog   *obs.Sampler
	commitLog *obs.Sampler
	claimLog  *obs.Sampler

	// quiesced is set by Quiesce: no new reads, no hand-out of claimed
	// entries, no replay, no claims. Commit and Touch keep working.
	quiesced atomic.Bool

	closed atomic.Bool
	stop   chan struct{}
	wg     sync.WaitGroup
}

// NewRedis connects, creates the consumer group if it does not exist, zeroes an
// orphan byte counter and reads the global occupancy once, with the write
// probe of RefreshStats: a Redis that answers reads but refuses writes (OOM,
// READONLY, NOPERM) does not pass the boot (SPEC §13.1 step 8).
// The own pending entries are re-read lazily, by the first Dequeue.
//
// It makes ONE attempt; ctx bounds it. See ErrFatal for how the app tells a
// fatal refusal from a Redis that is not up yet.
func NewRedis(ctx context.Context, cfg RedisConfig, log *slog.Logger) (*RedisRing, error) {
	if cfg.Stream == "" {
		cfg.Stream = DefaultRedisStream
	}
	if cfg.Group == "" {
		cfg.Group = DefaultRedisGroup
	}
	if cfg.ClaimIdle <= 0 {
		cfg.ClaimIdle = DefaultRedisClaimIdle
	}
	if cfg.BlockTimeout <= 0 {
		cfg.BlockTimeout = time.Second
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	if cfg.ClaimCount < 1 {
		cfg.ClaimCount = 256
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.Consumer == "" {
		// Without a stable name the next boot cannot find its own pending
		// entries; this is a wiring bug, and retrying cannot fix it.
		return nil, fmt.Errorf("ring redis: %w: empty consumer name (the AgentUUID)", ErrFatal)
	}

	client, err := rediscli.New(cfg.Config)
	if err != nil {
		// rueidis dials AND authenticates inside NewClient: a bad password is
		// a Redis reply here, and fatal at once.
		return nil, bootErr("connect", err)
	}

	if client.Mode() == rueidis.ClientModeCluster && hashTag(cfg.Stream) == "" {
		// The scripts touch three keys, and in a cluster rueidis refuses a
		// multi-key command across slots by panicking when it is BUILT --
		// that would be the first Enqueue, on a receiver goroutine. Only a
		// hash tag puts "<stream>", "<stream>:bytes" and
		// "<stream>:quarantine" in the same slot.
		client.Close()
		return nil, fmt.Errorf("ring redis: %w: on a Redis Cluster ring.redis.stream needs a hash tag "+
			"so its keys share a slot, e.g. {%s}", ErrFatal, cfg.Stream)
	}

	r := &RedisRing{
		cfg:          cfg,
		log:          log,
		m:            cfg.Metrics,
		client:       client,
		keys:         []string{cfg.Stream, cfg.Stream + ":bytes"},
		qkeys:        []string{cfg.Stream, cfg.Stream + ":bytes", cfg.Stream + ":quarantine"},
		probeKey:     cfg.Stream + ":probe",
		capItems:     strconv.FormatInt(ceiling(int64(cfg.CapItems)), 10),
		capBytes:     strconv.FormatInt(ceiling(cfg.CapBytes), 10),
		idleMS:       strconv.FormatInt(cfg.ClaimIdle.Milliseconds(), 10),
		replayCursor: "0",
		readLog:      obs.NewSampler(30 * time.Second),
		commitLog:    obs.NewSampler(30 * time.Second),
		claimLog:     obs.NewSampler(30 * time.Second),
		stop:         make(chan struct{}),
	}

	if err := r.createGroup(ctx); err != nil {
		client.Close()
		return nil, bootErr("XGROUP CREATE", err)
	}
	if err := r.resetOrphanCounter(ctx); err != nil {
		client.Close()
		return nil, bootErr("orphan counter check", err)
	}
	if err := r.RefreshStats(ctx); err != nil {
		client.Close()
		return nil, bootErr("stats and write probe", err)
	}
	if b := r.statBytes.Load(); b < 0 {
		// Only a manual edit, or a commit that ran with a wrong nbytes, gets
		// here. The ceiling still works (it admits more than it should), and
		// the reconciliation is the manual procedure of docker/README.md.
		r.log.Warn("ring redis: the byte counter is negative; reconcile it with the traffic stopped",
			"key", r.keys[1], "bytes", b, "items", r.statItems.Load())
	}

	r.wg.Add(2)
	go r.claimLoop()
	go r.statsLoop()
	return r, nil
}

// ceiling maps "no ceiling" onto a number the enqueue script can compare.
func ceiling(v int64) int64 {
	if v <= 0 || v > unlimited {
		return unlimited
	}
	return v
}

// hashTag returns the Redis Cluster hash tag of a key: the text between the
// first '{' and the first '}' after it, when not empty.
func hashTag(key string) string {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return ""
	}
	j := strings.IndexByte(key[i+1:], '}')
	if j <= 0 {
		return ""
	}
	return key[i+1 : i+1+j]
}

// bootErr classifies a construction failure: fatal replies end the boot at
// once, everything else is retried by the app until sink.boot_timeout.
func bootErr(op string, err error) error {
	if rediscli.Classify(err) == rediscli.Fatal {
		return fmt.Errorf("ring redis: %s: %w: %w", op, ErrFatal, err)
	}
	return fmt.Errorf("ring redis: %s: %w", op, err)
}

// runtimeErr wraps a failure after construction as ErrUnavailable, adding
// ErrFatal for the replies waiting cannot fix.
func runtimeErr(op string, err error) error {
	if rediscli.Classify(err) == rediscli.Fatal {
		return fmt.Errorf("ring redis: %s: %w: %w: %w", op, ErrUnavailable, ErrFatal, err)
	}
	return fmt.Errorf("ring redis: %s: %w: %w", op, ErrUnavailable, err)
}

func (r *RedisRing) Mode() string { return "redis" }

// createGroup is XGROUP CREATE <stream> <group> 0 MKSTREAM. BUSYGROUP means
// another replica, or a previous life, created it already: success.
//
// The start id is 0, not $: a group created after entries were appended --
// the stream outlived a deleted group -- must deliver them, not skip them.
func (r *RedisRing) createGroup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	err := r.client.Do(ctx, r.client.B().XgroupCreate().Key(r.cfg.Stream).
		Group(r.cfg.Group).Id("0").Mkstream().Build()).Error()
	if err == nil || isReply(err, "BUSYGROUP") {
		return nil
	}
	return err
}

// resetOrphanCounter applies the boot rule of SPEC §9.5.
func (r *RedisRing) resetOrphanCounter(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	prev, err := luaOrphan.Exec(ctx, r.client, r.keys, nil).ToString()
	if rueidis.IsRedisNil(err) {
		return nil
	}
	if err != nil {
		return err
	}
	r.log.Warn("ring redis: the stream is empty but its byte counter was not; counter reset to 0",
		"key", r.keys[1], "was", prev)
	return nil
}

// isReply reports whether err is a Redis error reply starting with prefix.
func isReply(err error, prefix string) bool {
	rerr, ok := rueidis.IsRedisErr(err)
	return ok && strings.HasPrefix(rerr.Error(), prefix)
}

// Enqueue runs the enqueue script and returns when it does (SPEC §2.3).
//
// The raw line goes to rueidis as a BinaryString: no copy, and no escaping (a
// stream field value is binary safe). That is exactly why a failure here is
// ErrUnavailable and not something the receiver may pool on: a command that
// timed out may still sit in the pipeline, about to read the buffer
// (SPEC §15.2). ErrFull, on the other hand, is a reply -- the command has run
// to completion and nobody reads the buffer any more.
//
// The script runs on a context detached from the receiver's and bounded by
// OpTimeout, like the write of ring=none and for the same reason: the shutdown
// cancels the receive context while the last messages are still going into
// the ring (SPEC §13.4 phase 3), and abandoning a script rueidis has already
// queued would only turn a stored message into a counted drop.
func (r *RedisRing) Enqueue(ctx context.Context, it *item.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}
	// A fresh head buffer per call, never a pooled one: it travels as a
	// BinaryString too, and after a timeout it may still be read by the
	// pipeline, so only the GC may decide when it is free.
	head := item.EncodeHead(make([]byte, 0, 512), &it.Head)

	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.OpTimeout)
	defer cancel()
	ok, err := luaEnqueue.Exec(wctx, r.client, r.keys, []string{
		r.capItems,
		r.capBytes,
		strconv.Itoa(len(it.Raw)),
		rueidis.BinaryString(head),
		rueidis.BinaryString(it.Raw),
	}).AsInt64()
	if err != nil {
		r.down.Store(true)
		return runtimeErr("enqueue", err)
	}
	// The script ran: whatever its answer, the server accepted a write. A
	// refusal for capacity is not unavailability.
	r.down.Store(false)
	if ok == 0 {
		return ErrFull
	}
	return nil
}

// Dequeue hands up to n entries to a drain worker, in this order:
//
//  1. once per process, this consumer's OWN pending entries -- what the
//     previous life read and never acknowledged (SPEC §9.5);
//  2. entries the claim loop took from idle consumers (a dead replica);
//  3. new entries: XREADGROUP ... COUNT n BLOCK <BlockTimeout> ... >.
//
// An empty batch with a nil error means "ask again": the read timed out, every
// entry read was quarantined, or Redis could not be reached and the backoff has
// been slept already (SPEC §9.5: the drain retries with backoff). The drain
// workers treat a Dequeue error as "stop", so a Redis outage must never
// surface as one: only ctx and Close end a Dequeue with an error.
func (r *RedisRing) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	if n < 1 {
		n = 1
	}
	if r.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if r.quiesced.Load() {
		return r.quiescedWait(ctx)
	}

	if !r.replayDone.Load() {
		items, done, err := r.replayOwn(ctx, n)
		if err != nil {
			return r.dequeueFailed(ctx, "XREADGROUP 0", err)
		}
		if !done {
			return items, nil
		}
	}

	if items := r.takeClaimed(n); len(items) > 0 {
		return items, nil
	}
	return r.readNew(ctx, n)
}

// replayOwn reads the next page of this consumer's pending entries. done
// reports that the history is exhausted; from then on only new entries are
// read.
//
// The history is read with an explicit id, which Redis serves from the
// consumer's PEL and never from the stream tail. Re-reading from 0 on every
// call would return the same entries forever, since they stay pending until
// committed; the cursor advances to the last id seen instead, under a mutex,
// so two workers never receive the same page.
func (r *RedisRing) replayOwn(ctx context.Context, n int) (items []*item.Item, done bool, err error) {
	r.replayMu.Lock()
	defer r.replayMu.Unlock()
	if r.replayDone.Load() {
		return nil, true, nil
	}

	cctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	streams, err := r.client.Do(cctx, r.client.B().Xreadgroup().
		Group(r.cfg.Group, r.cfg.Consumer).Count(int64(n)).
		Streams().Key(r.cfg.Stream).Id(r.replayCursor).Build()).AsXRead()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, false, err
	}
	entries := streams[r.cfg.Stream]
	if len(entries) == 0 {
		r.replayDone.Store(true)
		if r.replayed > 0 {
			r.log.Info("ring redis: own pending entries from a previous run re-read",
				"entries", r.replayed, "consumer", r.cfg.Consumer)
		}
		return nil, true, nil
	}
	r.replayCursor = entries[len(entries)-1].ID
	r.replayed += len(entries)
	r.resetBackoff()
	// A non-nil, possibly empty slice: "not done", even when every entry of
	// the page was quarantined.
	return append(make([]*item.Item, 0, len(entries)), r.toItems(entries)...), false, nil
}

// readNew asks for entries never delivered to anyone.
//
// The read runs on a context DETACHED from the worker's, bounded by the BLOCK
// plus OpTimeout. Once Redis answers an XREADGROUP with >, the entries are
// already in this consumer's PEL; a worker context cancelled while the answer
// is on the wire would make rueidis drop it, and those entries would sit
// pending, in nobody's hands, until a claim_idle later. Detached, a shutdown
// waits at most one BLOCK for the read to come back, and what it brings is
// returned and delivered.
func (r *RedisRing) readNew(ctx context.Context, n int) ([]*item.Item, error) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.BlockTimeout+r.cfg.OpTimeout)
	defer cancel()
	streams, err := r.client.Do(rctx, r.client.B().Xreadgroup().
		Group(r.cfg.Group, r.cfg.Consumer).Count(int64(n)).
		Block(r.cfg.BlockTimeout.Milliseconds()).
		Streams().Key(r.cfg.Stream).Id(">").Build()).AsXRead()
	if rueidis.IsRedisNil(err) {
		// BLOCK expired with nothing new.
		r.resetBackoff()
		return nil, nil
	}
	if err != nil {
		return r.dequeueFailed(ctx, "XREADGROUP", err)
	}
	r.resetBackoff()
	return r.toItems(streams[r.cfg.Stream]), nil
}

// dequeueFailed turns a read failure into "ask again" after a backoff.
func (r *RedisRing) dequeueFailed(ctx context.Context, op string, err error) ([]*item.Item, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	if cerr := ctx.Err(); cerr != nil {
		return nil, cerr
	}
	if isReply(err, "NOGROUP") {
		// The stream or the group was deleted under us (a FLUSHALL, a manual
		// DEL). Recreating it is the only way forward; the entries that were
		// in it are gone with it, which no retry can change.
		r.log.Warn("ring redis: the consumer group is gone; recreating it",
			"stream", r.cfg.Stream, "group", r.cfg.Group)
		if gerr := r.createGroup(ctx); gerr == nil {
			// A new group has no history to replay. And if the stream went
			// with it but the counter did not (a DEL of the stream alone),
			// the boot rule applies now, atomically, as it would at boot.
			r.replayDone.Store(true)
			if oerr := r.resetOrphanCounter(ctx); oerr != nil {
				r.log.Warn("ring redis: cannot check the byte counter after recreating the group",
					"error", oerr)
			}
			return nil, nil
		}
	}
	if ok, suppressed := r.readLog.Allow(); ok {
		lvl := slog.LevelWarn
		if rediscli.Classify(err) == rediscli.Fatal {
			lvl = slog.LevelError
		}
		r.log.Log(ctx, lvl, "ring redis: cannot read the stream; the drain retries with backoff",
			"op", op, "error", err, "suppressed", suppressed)
	}
	d := r.nextBackoff()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stop:
		return nil, ErrClosed
	case <-t.C:
		return nil, nil
	}
}

// nextBackoff is shared by every worker: they are all looking at the same
// Redis, and one outage should not be probed eight times as fast.
func (r *RedisRing) nextBackoff() time.Duration {
	const minB, maxB = 100 * time.Millisecond, 2 * time.Second
	for {
		cur := r.backoffNS.Load()
		next := time.Duration(cur) * 2
		if next < minB {
			next = minB
		}
		if next > maxB {
			next = maxB
		}
		if r.backoffNS.CompareAndSwap(cur, int64(next)) {
			return next
		}
	}
}

func (r *RedisRing) resetBackoff() {
	if r.backoffNS.Load() != 0 {
		r.backoffNS.Store(0)
	}
}

// toItems turns stream entries into items.
//
// Every item owns its raw line, copied ONCE from the reply into a pooled
// buffer (SPEC §15.1, §15.2): the reply's strings belong to rueidis, and the
// buffer goes back to the pool when the sink confirms. StreamID is what
// Commit and Touch act on.
//
// Two kinds of entry never become items:
//
//   - an entry with no fields: it was deleted while still pending here --
//     another replica claimed it, delivered it and committed it. It is only
//     acknowledged, and the commit script frees nothing for it, because its
//     XDEL removes nothing;
//   - an entry whose head does not decode, or that lacks h or b: corruption,
//     a codec of another version, or a foreign writer on the stream. It goes
//     to the quarantine stream (SPEC §9.5).
func (r *RedisRing) toItems(entries []rueidis.XRangeEntry) []*item.Item {
	out := make([]*item.Item, 0, len(entries))
	var gone []string
	for _, e := range entries {
		if e.FieldValues == nil {
			gone = append(gone, e.ID)
			continue
		}
		h, okh := e.FieldValues["h"]
		b, okb := e.FieldValues["b"]
		if !okh || !okb {
			r.quarantine(e.ID, "entry without the h or b field")
			continue
		}
		head, err := item.DecodeHead([]byte(h))
		if err != nil {
			r.quarantine(e.ID, err.Error())
			continue
		}
		buf, class := item.GetRaw(len(b))
		buf = append(buf, b...)
		it := item.New(head, buf, class)
		it.StreamID = e.ID
		out = append(out, it)
	}
	if len(gone) > 0 {
		r.ackGone(gone)
	}
	return out
}

// quarantine moves one undecodable entry out of the drain's way. The entry
// leaves the stream and the counter in the same script, so the accounting
// never holds an entry nobody can deliver; the bytes stay in the quarantine
// stream for a human, with no ceiling and no automatic cleanup.
func (r *RedisRing) quarantine(id, reason string) {
	r.log.Error("ring redis: undecodable entry moved to the quarantine stream",
		"id", id, "quarantine", r.qkeys[2], "error", reason)
	if r.m != nil {
		r.m.RingQuarantinedTotal.Inc()
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
	defer cancel()
	err := luaQuarantine.Exec(ctx, r.client, r.qkeys,
		[]string{r.cfg.Group, id, reason, r.cfg.Consumer}).Error()
	if err != nil {
		// The entry stays pending under this consumer; the claim loop meets it
		// again after claim_idle and retries the move.
		r.log.Error("ring redis: cannot move the entry to quarantine; it stays pending",
			"id", id, "error", err)
	}
}

// ackGone acknowledges entries that were deleted while pending.
func (r *RedisRing) ackGone(ids []string) {
	args := make([]string, 0, 1+2*len(ids))
	args = append(args, r.cfg.Group)
	for _, id := range ids {
		args = append(args, id, "0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
	defer cancel()
	if err := luaCommit.Exec(ctx, r.client, r.keys, args).Error(); err != nil {
		r.log.Warn("ring redis: cannot acknowledge entries deleted elsewhere", "ids", len(ids), "error", err)
	}
}

// Commit acknowledges and deletes the delivered entries in the commit script.
//
// A failure leaves them pending under this consumer: they are claimed again
// after claim_idle and delivered a second time, which the destination
// deduplicates on (uuid, step). That is the at-least-once side of the trade,
// and the only one this ring may take: committing anything it cannot confirm
// would be loss.
func (r *RedisRing) Commit(items []*item.Item) error {
	if len(items) == 0 {
		return nil
	}
	if r.closed.Load() {
		return ErrClosed
	}
	var errs []error
	args := make([]string, 0, 1+2*min(len(items), idsPerScript))
	args = append(args, r.cfg.Group)
	flush := func() {
		if len(args) == 1 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
		defer cancel()
		if err := luaCommit.Exec(ctx, r.client, r.keys, args).Error(); err != nil {
			errs = append(errs, runtimeErr("commit", err))
		}
		args = args[:1]
	}
	for _, it := range items {
		if it.StreamID == "" {
			continue
		}
		args = append(args, it.StreamID, strconv.Itoa(len(it.Raw)))
		if len(args) > 2*idsPerScript {
			flush()
		}
	}
	flush()
	if len(errs) > 0 {
		if ok, suppressed := r.commitLog.Allow(); ok {
			r.log.Warn("ring redis: cannot commit delivered entries; they will be delivered again "+
				"after claim_idle", "error", errs[0], "suppressed", suppressed)
		}
		return errors.Join(errs...)
	}
	return nil
}

// Touch renews this consumer's claim on entries a worker is holding in a retry
// loop: XCLAIM <stream> <group> <self> 0 <ids> JUSTID resets their idle time,
// so no replica's XAUTOCLAIM takes a batch that is alive (SPEC §9.5). Without
// it a sink outage longer than claim_idle would make every replica deliver the
// same batch. JUSTID keeps the delivery counter untouched: nothing was
// delivered again.
func (r *RedisRing) Touch(items []*item.Item) error {
	if len(items) == 0 {
		return nil
	}
	if r.closed.Load() {
		return ErrClosed
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		if it.StreamID != "" {
			ids = append(ids, it.StreamID)
		}
	}
	_, err := r.xclaim(ids)
	return err
}

// xclaim runs XCLAIM ... 0 <ids> JUSTID in chunks and returns the ids Redis
// still had pending (an id acknowledged or deleted elsewhere is not returned).
func (r *RedisRing) xclaim(ids []string) (map[string]struct{}, error) {
	kept := make(map[string]struct{}, len(ids))
	for len(ids) > 0 {
		chunk := ids[:min(len(ids), idsPerScript)]
		ids = ids[len(chunk):]
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
		got, err := r.client.Do(ctx, r.client.B().Xclaim().Key(r.cfg.Stream).Group(r.cfg.Group).
			Consumer(r.cfg.Consumer).MinIdleTime("0").Id(chunk...).Justid().Build()).AsStrSlice()
		cancel()
		if err != nil {
			return kept, runtimeErr("XCLAIM", err)
		}
		for _, id := range got {
			kept[id] = struct{}{}
		}
	}
	return kept, nil
}

// claimLoop runs XAUTOCLAIM every claim_idle/2 (SPEC §9.5): the pending
// entries of ANY consumer idle for more than claim_idle -- a dead replica's,
// or this process's own that a cancelled read left in nobody's hands.
func (r *RedisRing) claimLoop() {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.ClaimIdle / 2)
	defer t.Stop()
	cursor := "0-0"
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
		}
		// Until the own-pending replay is over, this consumer's entries from a
		// previous life are idle too, and XAUTOCLAIM would hand them over a
		// second time alongside the replay. The first Dequeue ends the replay
		// within moments of the boot. After Quiesce nothing is taken any more:
		// what this instance would claim, it could not deliver.
		if !r.replayDone.Load() || r.quiesced.Load() {
			continue
		}
		r.touchClaimed()
		cursor = r.claimRound(cursor)
	}
}

// claimRound claims until the hand-off queue is full or the scan wraps, and
// returns the cursor to resume from.
//
// It claims only as many as there is room for: a claimed entry's idle time
// restarts at the claim, and an entry held in memory beyond claim_idle
// without a worker would simply be claimed back by another replica.
func (r *RedisRing) claimRound(cursor string) string {
	for {
		select {
		case <-r.stop:
			return cursor
		default:
		}
		free := r.cfg.ClaimCount - r.claimedLen()
		if free <= 0 {
			return cursor
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.OpTimeout)
		resp := r.client.Do(ctx, r.client.B().Xautoclaim().Key(r.cfg.Stream).Group(r.cfg.Group).
			Consumer(r.cfg.Consumer).MinIdleTime(r.idleMS).Start(cursor).Count(int64(free)).Build())
		next, entries, err := parseAutoclaim(resp)
		cancel()
		if err != nil {
			if isReply(err, "NOGROUP") {
				// Dequeue recreates the group; there is nothing to claim in
				// a group that does not exist.
				return "0-0"
			}
			if ok, suppressed := r.claimLog.Allow(); ok {
				r.log.Warn("ring redis: XAUTOCLAIM failed; retrying next round",
					"error", err, "suppressed", suppressed)
			}
			return cursor
		}
		if len(entries) > 0 {
			if r.m != nil {
				r.m.RingRedisClaimedTotal.Add(uint64(len(entries)))
			}
			r.log.Info("ring redis: claimed idle pending entries (a dead replica, or a read nobody received)",
				"entries", len(entries), "idle_ms", r.idleMS)
			items := r.toItems(entries)
			r.claimMu.Lock()
			r.claimed = append(r.claimed, items...)
			r.claimMu.Unlock()
		}
		if next == "0-0" || next == "" {
			return "0-0"
		}
		cursor = next
	}
}

// parseAutoclaim reads [next-cursor, entries, deleted-ids]. The third element
// (Redis 7+) lists entries that were deleted while pending; XAUTOCLAIM already
// removed them from the PEL, so they need nothing. Redis 6.2 returns them
// among the entries with no fields instead, and toItems acknowledges those.
func parseAutoclaim(resp rueidis.RedisResult) (string, []rueidis.XRangeEntry, error) {
	vals, err := resp.ToArray()
	if err != nil {
		return "", nil, err
	}
	if len(vals) < 2 {
		return "", nil, fmt.Errorf("XAUTOCLAIM: %d elements in the reply", len(vals))
	}
	next, err := vals[0].ToString()
	if err != nil {
		return "", nil, err
	}
	entries, err := vals[1].AsXRange()
	if err != nil {
		return "", nil, err
	}
	return next, entries, nil
}

func (r *RedisRing) claimedLen() int {
	r.claimMu.Lock()
	defer r.claimMu.Unlock()
	return len(r.claimed)
}

// takeClaimed hands up to n claimed items to a worker.
//
// They are renewed (XCLAIM ... 0 <ids> JUSTID) right before they leave: an
// entry may have waited in the queue for up to claim_idle/2 since the claim
// loop last touched it, and the worker's own Touch only starts after its
// first failed write. Without the renewal another replica's XAUTOCLAIM could
// take a batch half a claim_idle into its delivery; with it, the idle clock of
// every handed-out entry starts at the hand-out, as it does for a fresh read.
//
// Entries Redis no longer has pending -- acknowledged or deleted by a replica
// that delivered them meanwhile -- are dropped instead of delivered twice. If
// the renewal itself fails, the items are handed out anyway: they are pending
// under this consumer, and a worker that cannot deliver them touches them.
func (r *RedisRing) takeClaimed(n int) []*item.Item {
	r.claimMu.Lock()
	if len(r.claimed) == 0 {
		r.claimMu.Unlock()
		return nil
	}
	k := min(n, len(r.claimed))
	out := make([]*item.Item, k)
	copy(out, r.claimed)
	// A fresh slice for the rest, so the handed-out items are not kept alive
	// by the old backing array.
	r.claimed = append([]*item.Item(nil), r.claimed[k:]...)
	r.claimMu.Unlock()

	ids := make([]string, len(out))
	for i, it := range out {
		ids[i] = it.StreamID
	}
	kept, err := r.xclaim(ids)
	if err != nil {
		return out
	}
	live := out[:0]
	for _, it := range out {
		if _, ok := kept[it.StreamID]; ok {
			live = append(live, it)
		} else {
			it.Release() // never handed out: still this package's alone
		}
	}
	clear(out[len(live):])
	return live
}

// touchClaimed renews the entries still waiting in the hand-off queue, and
// drops those another replica acknowledged or deleted meanwhile: delivering
// them would only be a duplicate.
func (r *RedisRing) touchClaimed() {
	r.claimMu.Lock()
	ids := make([]string, 0, len(r.claimed))
	for _, it := range r.claimed {
		ids = append(ids, it.StreamID)
	}
	r.claimMu.Unlock()
	if len(ids) == 0 {
		return
	}
	kept, err := r.xclaim(ids)
	if err != nil {
		return
	}
	r.claimMu.Lock()
	defer r.claimMu.Unlock()
	live := r.claimed[:0]
	for _, it := range r.claimed {
		if _, ok := kept[it.StreamID]; ok {
			live = append(live, it)
		} else {
			// Never handed out: this package is still its only owner.
			it.Release()
		}
	}
	clear(r.claimed[len(live):])
	r.claimed = live
}

// statsLoop refreshes the global occupancy every second, whether or not the
// drain is moving: a stalled drain (sink down) with a growing stream is exactly
// when /readyz needs a fresh number.
func (r *RedisRing) statsLoop() {
	defer r.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), min(r.cfg.OpTimeout, time.Second))
		_ = r.RefreshStats(ctx)
		cancel()
	}
}

// RefreshStats reads XLEN and the byte counter into the cache Stats serves,
// and in the same pipeline asks whether this Redis can be WRITTEN to. The loop
// calls it every second; NewRedis calls it once, and the tests call it to
// avoid waiting for the loop.
//
// Reads alone would be the trivial-health-check trap the Redis sink documents:
// a server at maxmemory (OOM), a replica promoted by mistake (READONLY), an ACL
// that lost +@write (NOPERM) all answer XLEN and GET happily while refusing
// every enqueue, and /readyz would flap back to ready every second between
// two refused writes. So the third command is
//
//	XADD <stream>:probe NOMKSTREAM * p 1
//
// NOMKSTREAM makes Redis answer nil and create NOTHING when the key is absent,
// which it always is; but XADD is flagged write+denyoom, so it goes through
// the same checks as a real enqueue -- READONLY, OOM, NOPERM -- first. A nil
// reply is success.
//
// Available() turns false when any of the three fails, and only a successful
// probe -- or a successful enqueue -- turns it back.
func (r *RedisRing) RefreshStats(ctx context.Context) error {
	resps := r.client.DoMulti(ctx,
		r.client.B().Xlen().Key(r.keys[0]).Build(),
		r.client.B().Get().Key(r.keys[1]).Build(),
		r.client.B().Xadd().Key(r.probeKey).Nomkstream().Id("*").FieldValue().FieldValue("p", "1").Build(),
	)
	n, nerr := resps[0].AsInt64()
	b, berr := resps[1].AsInt64()
	if rueidis.IsRedisNil(berr) {
		b, berr = 0, nil
	}
	perr := resps[2].Error()
	if rueidis.IsRedisNil(perr) {
		perr = nil
	}
	if nerr == nil && berr == nil {
		// The occupancy is worth publishing even when writes are refused:
		// it is what an operator looks at to see the ring fill.
		r.statItems.Store(n)
		r.statBytes.Store(b)
	}
	if err := errors.Join(nerr, berr, perr); err != nil {
		r.down.Store(true)
		return err
	}
	r.down.Store(false)
	return nil
}

// Stats is the GLOBAL occupancy of the shared stream, from the cache: every
// replica reports the same numbers, and /readyz never waits on Redis.
func (r *RedisRing) Stats() Stats {
	capItems, capBytes := int64(r.cfg.CapItems), r.cfg.CapBytes
	if capItems < 0 {
		capItems = 0
	}
	if capBytes < 0 {
		capBytes = 0
	}
	return Stats{
		Items:    r.statItems.Load(),
		Bytes:    r.statBytes.Load(),
		CapItems: capItems,
		CapBytes: capBytes,
	}
}

// Available reports whether the ring's Redis accepts writes: false as soon as
// the write probe of RefreshStats or an Enqueue fails, true again only after a
// probe or an enqueue succeeds. Read failures of the drain do not move it --
// the probe, at most a second later, does. /readyz fails while it is false
// (SPEC §9.5: Redis of the ring unavailable).
func (r *RedisRing) Available() bool { return !r.down.Load() }

// Quiesce prepares a graceful shutdown with ring=redis (SPEC §13.4 phase 4).
//
// A drain with this ring cannot wait for Stats to reach zero: the occupancy is
// GLOBAL, and other replicas keep feeding the stream. What this instance owes
// is only the batches its workers already hold. After Quiesce:
//
//   - Dequeue reads nothing new, hands out no claimed entry and does not
//     replay: it waits briefly (at most BlockTimeout, less if ctx ends) and
//     returns an empty batch with a nil error, so a worker loop keeps
//     spinning gently without pulling more;
//   - the claim loop stops claiming, and the entries waiting in its hand-off
//     queue are let go -- they stay pending under this consumer, for the next
//     boot's replay or another replica's XAUTOCLAIM after claim_idle;
//   - Commit and Touch keep working, so held batches finish normally;
//   - a read already in flight when Quiesce is called still returns what it
//     brought; those entries are in this consumer's PEL and are delivered by
//     the worker that receives them.
//
// The app calls it at the start of the drain phase, then waits for the
// workers' held batches (Workers.Pending() empty) up to drain_timeout, then
// closes. It is idempotent; there is no way back short of a new ring.
func (r *RedisRing) Quiesce() {
	if r.quiesced.Swap(true) {
		return
	}
	r.claimMu.Lock()
	for _, it := range r.claimed {
		it.Release() // never handed out; still pending in Redis
	}
	r.claimed = nil
	r.claimMu.Unlock()
}

// quiescedWait is Dequeue after Quiesce: a short wait, then "ask again".
func (r *RedisRing) quiescedWait(ctx context.Context) ([]*item.Item, error) {
	t := time.NewTimer(min(r.cfg.BlockTimeout, 100*time.Millisecond))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stop:
		return nil, ErrClosed
	case <-t.C:
		return nil, nil
	}
}

// Drain returns nothing: the content is outside the process (SPEC §12), and
// what this instance held when it stopped stays pending under its consumer --
// re-read by its next boot, or claimed by another replica after claim_idle.
func (r *RedisRing) Drain() []*item.Item { return nil }

// Close stops the claim and stats loops and closes the client. Entries still
// pending under this consumer are not touched: see Drain.
func (r *RedisRing) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	close(r.stop)
	r.wg.Wait()

	r.claimMu.Lock()
	for _, it := range r.claimed {
		// Never handed to a worker; still pending in Redis.
		it.Release()
	}
	r.claimed = nil
	r.claimMu.Unlock()

	r.client.Close()
	return nil
}
