package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func metrics() *obs.Metrics {
	return obs.NewMetrics("memory", "none", "udp", obs.BuildInfo{Version: "test"})
}

// item builds an accepted item the way a receiver would: head filled from the
// wire, PRI already read, raw line owned.
func item(i int, line string) *envelope.Item {
	raw := []byte(line)
	h := envelope.Head{
		ID:        fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-%012d", i),
		Transport: envelope.TransportTCP,
		Framing:   envelope.FramingLF,
		Listener:  ":1514",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  51234,
	}
	h.SetTime(time.Unix(1757116800, int64(i)))
	if pri, _, ok := syslog.PRI(raw); ok {
		h.PRI, h.FromWire = pri, true
	} else {
		h.PRI = 13
	}
	return envelope.NewItem(h, raw, -1)
}

const line5424 = `<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] hello`
const line3164 = `<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`

// ------------------------------------------------------------------------ Prep

func TestPrepBuildsEnvelopeAndMeta(t *testing.T) {
	p := NewPrep("sls-a1", syslog.Options{}, metrics())
	batch := p.Build([]*envelope.Item{item(0, line5424), item(1, line3164)})
	defer p.Release()

	if len(batch) != 2 {
		t.Fatalf("batch = %d", len(batch))
	}
	var m map[string]any
	if err := json.Unmarshal(batch[0].Env, &m); err != nil {
		t.Fatalf("envelope is not JSON: %v", err)
	}
	if m["rfc"] != "5424" || m["hostname"] != "web-01" || m["node-id"] != "sls-a1" {
		t.Fatalf("envelope = %v", m)
	}
	if batch[0].Meta.Hostname != "web-01" || batch[0].Meta.Severity != 5 {
		t.Fatalf("meta = %+v", batch[0].Meta)
	}
	if batch[1].Meta.AppName != "su" || batch[1].Meta.ProcID != "230" {
		t.Fatalf("meta[1] = %+v", batch[1].Meta)
	}
	// Slicing the raw line by the envelope's offsets must yield the message.
	off := int(m["msg-offset"].(float64))
	size := int(m["msg-size"].(float64))
	if got := string(batch[0].Raw()[off : off+size]); got != "hello" {
		t.Fatalf("sliced msg = %q", got)
	}
}

// The reception instant comes from the item head, never from the clock. That is
// what makes reingestion exact: a message dumped in December and replayed in
// January must still get December's year for its RFC 3164 timestamp.
func TestPrepUsesTheHeadClockNotTheWallClock(t *testing.T) {
	it := item(0, `<34>Dec 31 23:59:59 host app: end of year`)
	it.Head.SetTime(time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC))

	p := NewPrep("n", syslog.Options{}, nil)
	batch := p.Build([]*envelope.Item{it})
	defer p.Release()

	var m map[string]any
	json.Unmarshal(batch[0].Env, &m)
	ts, ok := m["msg-ts"].(string)
	if !ok || ts == "" {
		t.Fatalf("msg-ts = %v", m["msg-ts"])
	}
	got, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatal(err)
	}
	if got.Year() != 2025 {
		t.Fatalf("year = %d, want 2025 (the reception year, from the head)", got.Year())
	}
}

// A Prep is reused for a worker's whole life. A field surviving from the previous
// batch would put one emitter's hostname on another's event.
func TestPrepReuseLeavesNothingBehind(t *testing.T) {
	p := NewPrep("n", syslog.Options{}, nil)

	first := p.Build([]*envelope.Item{item(0, line5424)})
	if len(first) != 1 {
		t.Fatal("first batch")
	}
	second := p.Build([]*envelope.Item{item(1, `<14>linha solta`)})
	if len(second) != 1 {
		t.Fatalf("second batch = %d", len(second))
	}
	var m map[string]any
	json.Unmarshal(second[0].Env, &m)
	if m["hostname"] != "" || m["app-name"] != "" || m["msgid"] != "" {
		t.Fatalf("stale fields leaked: %v", m)
	}
	if sd := m["structured-data"].(map[string]any); len(sd) != 0 {
		t.Fatalf("stale structured data: %v", sd)
	}
	p.Release()
}

func TestPrepCountsParseMetrics(t *testing.T) {
	m := metrics()
	p := NewPrep("n", syslog.Options{}, m)
	p.Build([]*envelope.Item{
		item(0, line5424),
		item(1, line3164),
		item(2, `sem pri nenhum`),
		item(3, `<34>1 - - - - - [a k="1"][a k="2"] m`), // duplicate SD-ID
	})
	p.Release()

	if got := m.ParseTotal[obs.RFC5424].Load(); got != 2 {
		t.Errorf("5424 count = %d, want 2", got)
	}
	if got := m.ParseTotal[obs.RFC3164].Load(); got != 1 {
		t.Errorf("3164 count = %d, want 1", got)
	}
	if got := m.ParseTotal[obs.RFCUnknown].Load(); got != 1 {
		t.Errorf("unknown count = %d, want 1", got)
	}
	if got := m.ParseAnomaliesTotal[obs.AnomDupSDID].Load(); got != 1 {
		t.Errorf("dup_sd_id = %d, want 1", got)
	}
	if m.ParseDuration.Count() == 0 {
		t.Error("parse duration not observed")
	}
}

// ------------------------------------------------------------- classification

func TestClassifyRedisReply(t *testing.T) {
	cases := []struct {
		reply string
		want  Class
	}{
		// OOM means the server is ALIVE and refusing writes. Retrying would fill
		// the ring and drop messages while nothing drains.
		{"OOM command not allowed when used memory > 'maxmemory'", ClassFatal},
		{"oom lowercase still fatal", ClassFatal},
		// A wrong key type or a missing privilege will fail every batch
		// identically, so it ends the process instead of dripping discards.
		{"WRONGTYPE Operation against a key holding the wrong kind of value", ClassFatal},
		{"NOPERM this user has no permissions", ClassFatal},
		{"WRONGPASS invalid username-password pair", ClassFatal},
		// Alive but temporarily unable.
		{"LOADING Redis is loading the dataset in memory", ClassNetwork},
		{"READONLY You can't write against a read only replica", ClassNetwork},
		{"CLUSTERDOWN The cluster is down", ClassNetwork},
		{"TRYAGAIN multiple keys request", ClassNetwork},
		// Unknown is retried, never dropped.
		{"ERR something nobody has seen", ClassNetwork},
		{"", ClassNetwork},
	}
	for _, c := range cases {
		if got := classifyRedisReply(c.reply); got != c.want {
			t.Errorf("classifyRedisReply(%q) = %v, want %v", c.reply, got, c.want)
		}
	}
}

// The rule that matters most in the whole package: class 42 is fatal. Classified
// as logic, a dropped table would discard a batch every few milliseconds, with an
// error log, forever, in a process every probe reports healthy.
func TestClassifySQLState(t *testing.T) {
	fatal := []string{
		"42P01", // undefined_table
		"42703", // undefined_column
		"42P10", // invalid_column_reference: no unique index for ON CONFLICT
		"42501", // insufficient_privilege
		"25006", // read_only_sql_transaction
		"53100", // disk_full
		"28P01", // invalid_password
		"3D000", // invalid_catalog_name
		"57P04", // database_dropped
		"XX001", // data_corrupted
		"23502", // not_null_violation: this server never sends NULL, so it is the table's
	}
	for _, c := range fatal {
		if got := classifySQLState(c); got != ClassFatal {
			t.Errorf("SQLSTATE %s = %v, want fatal", c, got)
		}
	}
	network := []string{"08006", "08003", "53300", "57P01", "57014", "40001", "40P01", "55P03", "", "9"}
	for _, c := range network {
		if got := classifySQLState(c); got != ClassNetwork {
			t.Errorf("SQLSTATE %s = %v, want network", c, got)
		}
	}
	logic := []string{"22021", "22003", "23505", "23514", "54000"}
	for _, c := range logic {
		if got := classifySQLState(c); got != ClassLogic {
			t.Errorf("SQLSTATE %s = %v, want logic", c, got)
		}
	}
}

func TestClassifySQLite(t *testing.T) {
	for _, msg := range []string{
		"database or disk is full", "database disk image is malformed",
		"attempt to write a readonly database", "file is not a database",
	} {
		if got := ClassOf(classifySQLite("insert", errors.New(msg))); got != ClassFatal {
			t.Errorf("%q = %v, want fatal", msg, got)
		}
	}
	for _, msg := range []string{"database is locked", "disk i/o error", "something new"} {
		if got := ClassOf(classifySQLite("insert", errors.New(msg))); got != ClassNetwork {
			t.Errorf("%q = %v, want network", msg, got)
		}
	}
	if classifySQLite("x", nil) != nil {
		t.Error("nil error classified")
	}
}

// An error with no class must be retried, not dropped: a filling ring is
// visible, a discarded batch is not.
func TestClassOfUnknownIsNetwork(t *testing.T) {
	if got := ClassOf(errors.New("plain")); got != ClassNetwork {
		t.Fatalf("got %v", got)
	}
	if ClassOf(nil) != ClassNetwork {
		t.Fatal("nil should not be fatal")
	}
	if IsFatal(errors.New("plain")) {
		t.Fatal("plain error is fatal")
	}
	if !IsFatal(fatalErr("op", errors.New("x"))) {
		t.Fatal("fatalErr is not fatal")
	}
}

func TestParseUUID(t *testing.T) {
	u, ok := parseUUID("019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f")
	if !ok {
		t.Fatal("valid uuid refused")
	}
	if u[0] != 0x01 || u[1] != 0x98 || u[15] != 0x6f {
		t.Fatalf("decoded wrong: %x", u)
	}
	for _, bad := range []string{"", "short", "019823f1_7a2c-7b3d-8e4f-1a2b3c4d5e6f",
		"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6g"} {
		if _, ok := parseUUID(bad); ok {
			t.Errorf("parseUUID(%q) accepted", bad)
		}
	}
}

func TestPGTextSanitises(t *testing.T) {
	if got := pgText("ok"); got != "ok" {
		t.Errorf("valid text changed: %q", got)
	}
	if got := pgText("a\xffb"); got != "a�b" {
		t.Errorf("invalid utf8: %q", got)
	}
	// NUL is valid UTF-8 and still rejected by PostgreSQL text.
	if got := pgText("a\x00b"); got != "a�b" {
		t.Errorf("NUL: %q", got)
	}
}

// A pgArgs returned to the pool still holding references would pin buffers that
// already went back to the raw-line pool, and the next batch would carry another
// emitter's line.
func TestPGArgsResetDropsReferences(t *testing.T) {
	p := NewPrep("n", syslog.Options{}, nil)
	batch := p.Build([]*envelope.Item{item(0, line5424)})

	a := &pgArgs{}
	if !a.add(&batch[0]) {
		t.Fatal("add refused a valid item")
	}
	if len(a.raw) != 1 || len(a.env) != 1 {
		t.Fatalf("add stored %d raw %d env", len(a.raw), len(a.env))
	}
	a.reset()

	if len(a.raw) != 0 || len(a.env) != 0 || len(a.id) != 0 {
		t.Fatal("reset did not truncate")
	}
	// Capacity is kept, but every slot must be cleared.
	for _, b := range a.raw[:cap(a.raw)] {
		if b != nil {
			t.Fatal("reset left a raw reference behind")
		}
	}
	for _, b := range a.env[:cap(a.env)] {
		if b != nil {
			t.Fatal("reset left an env reference behind")
		}
	}
	p.Release()
}

// ---------------------------------------------------------------- fake sink

type fakeSink struct {
	mu        sync.Mutex
	written   []string
	failWith  error
	failures  int // fail this many times, then succeed
	healthy   atomic.Bool
	healthErr error
}

func newFakeSink() *fakeSink {
	f := &fakeSink{}
	f.healthy.Store(true)
	return f
}

func (f *fakeSink) Write(ctx context.Context, batch []Prepared) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// failures > 0 fails that many times; failures < 0 fails forever.
	if f.failures != 0 {
		if f.failures > 0 {
			f.failures--
		}
		return f.failWith
	}
	for i := range batch {
		f.written = append(f.written, batch[i].Item.Head.ID)
	}
	return nil
}

func (f *fakeSink) Health(context.Context) error {
	if f.healthy.Load() {
		return nil
	}
	if f.healthErr != nil {
		return f.healthErr
	}
	return errors.New("unhealthy")
}
func (f *fakeSink) Close() error { return nil }
func (f *fakeSink) Type() string { return "fake" }

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.written)
}

// -------------------------------------------------------------------- workers

func workerCfg(batch int) WorkerConfig {
	return WorkerConfig{
		Workers: 2, Batch: batch, OpTimeout: 2 * time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
		NodeID: "n",
	}
}

func TestWorkersDeliverAndRelease(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newFakeSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(16), discard(), m)

	const n = 100
	items := make([]*envelope.Item, 0, n)
	for i := 0; i < n; i++ {
		it := item(i, line5424)
		items = append(items, it)
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for fs.count() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	w.Wait()

	if fs.count() != n {
		t.Fatalf("written %d of %d", fs.count(), n)
	}
	if got := m.EventsWrittenTotal.Load(); got != n {
		t.Fatalf("metric says %d", got)
	}
	// Ownership transfers on a successful write and nowhere else.
	for _, it := range items {
		if !it.Released() {
			t.Fatalf("%s not released after delivery", it.Head.ID)
		}
	}
	if len(w.Pending()) != 0 {
		t.Fatal("workers left something held after delivering everything")
	}
	if s := r.Stats(); s.Items != 0 {
		t.Fatalf("ring not drained: %+v", s)
	}
}

// A worker that retries HOLDS its batch outside the ring. If shutdown did not
// collect it from Pending(), those messages -- already confirmed on RELP --
// would be in neither the ring nor the dump.
func TestWorkersPendingHoldsARetryingBatch(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newFakeSink()
	fs.failWith = networkErr("write", errors.New("destination unreachable"))
	fs.failures = -1 // forever
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	w := NewWorkers(r, fs, h, workerCfg(4), discard(), m)

	items := make([]*envelope.Item, 0, 4)
	for i := 0; i < 4; i++ {
		it := item(i, line3164)
		items = append(items, it)
		r.Enqueue(context.Background(), it)
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	// Wait until at least one retry has been attempted.
	deadline := time.Now().Add(5 * time.Second)
	for m.SinkErrorsTotal[obs.SinkErrNetwork].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	w.Wait()

	held := w.Pending()
	if len(held) == 0 {
		t.Fatal("a retrying batch was not registered for the emergency dump")
	}
	for _, it := range held {
		if it.Released() {
			t.Fatalf("%s was released while still pending", it.Head.ID)
		}
	}
	// Pending drains the registry, so a second call returns nothing.
	if len(w.Pending()) != 0 {
		t.Fatal("Pending did not drain")
	}
}

func TestWorkersRetryThenSucceed(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newFakeSink()
	fs.failWith = networkErr("write", errors.New("temporary"))
	fs.failures = 3
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	it := item(0, line5424)
	r.Enqueue(context.Background(), it)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for fs.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	w.Wait()

	if fs.count() != 1 {
		t.Fatalf("written %d", fs.count())
	}
	if !it.Released() {
		t.Fatal("item not released after the retry succeeded")
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got < 3 {
		t.Fatalf("network errors counted = %d, want at least 3", got)
	}
}

// A fatal write escalates immediately, and the batch stays held for the dump.
func TestWorkersFatalHoldsAndEscalates(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newFakeSink()
	fs.failWith = fatalErr("write", errors.New("OOM"))
	fs.failures = -1
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	it := item(0, line5424)
	r.Enqueue(context.Background(), it)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(5 * time.Second):
		t.Fatal("fatal write did not escalate")
	}
	w.Wait()

	if h.State() != StateFatal || h.Up() {
		t.Fatalf("state = %v", h.State())
	}
	if it.Released() {
		t.Fatal("a fatally-failed item was released instead of held for the dump")
	}
	if len(w.Pending()) != 1 {
		t.Fatal("the item is not registered for the dump")
	}
	if m.SinkErrorsTotal[obs.SinkErrFatal].Load() == 0 {
		t.Fatal("fatal not counted")
	}
}

// A logic failure is a permanent refusal of THIS data. It is dropped, loudly and
// counted, and the worker carries on.
func TestWorkersLogicDropsAndContinues(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	fs := newFakeSink()
	fs.failWith = logicErr("write", errors.New("constraint"))
	fs.failures = 1
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	cfg := workerCfg(1)
	cfg.Workers = 1
	w := NewWorkers(r, fs, h, cfg, discard(), m)

	bad := item(0, line5424)
	good := item(1, line3164)
	r.Enqueue(context.Background(), bad)
	r.Enqueue(context.Background(), good)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for fs.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	w.Wait()

	if got := m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if !bad.Released() {
		t.Fatal("a permanently refused item was not released")
	}
	if fs.count() != 1 {
		t.Fatalf("the worker did not carry on: wrote %d", fs.count())
	}
}

// --------------------------------------------------------------------- health

func TestHealthUpDownRecover(t *testing.T) {
	fs := newFakeSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: time.Second,
		MaxRetries: 1000, FailAfter: time.Hour,
	}, discard(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	if !h.Up() {
		t.Fatal("not UP at start")
	}
	fs.healthy.Store(false)
	waitFor(t, 5*time.Second, func() bool { return h.State() == StateDown })
	if m.SinkUp.Load() != 0 {
		t.Fatal("syslog_sink_up still 1 while DOWN")
	}

	fs.healthy.Store(true)
	waitFor(t, 5*time.Second, func() bool { return h.State() == StateUp })
	if m.SinkUp.Load() != 1 {
		t.Fatal("syslog_sink_up still 0 after recovery")
	}
}

// The retry fuse: N consecutive failed checks end the process.
func TestHealthFuseByRetries(t *testing.T) {
	fs := newFakeSink()
	fs.healthy.Store(false)
	h := NewHealth(fs, HealthConfig{
		Interval: time.Millisecond, Timeout: time.Second,
		MaxRetries: 3, FailAfter: time.Hour,
	}, discard(), metrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(5 * time.Second):
		t.Fatal("the retry fuse never tripped")
	}
	if h.FatalErr() == nil {
		t.Fatal("no fatal error recorded")
	}
}

// The time fuse: DOWN for longer than fail_timeout ends the process even if the
// checks keep answering quickly.
func TestHealthFuseByTime(t *testing.T) {
	fs := newFakeSink()
	fs.healthy.Store(false)
	h := NewHealth(fs, HealthConfig{
		Interval: time.Millisecond, Timeout: time.Second,
		MaxRetries: 1 << 30, FailAfter: 30 * time.Millisecond,
	}, discard(), metrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(5 * time.Second):
		t.Fatal("the time fuse never tripped")
	}
}

func TestHealthMarkFatalIsOnce(t *testing.T) {
	h := NewHealth(newFakeSink(), HealthConfig{}, discard(), metrics())
	h.MarkFatal(errors.New("first"))
	h.MarkFatal(errors.New("second")) // must not panic on a second close
	if got := h.FatalErr().Error(); got != "first" {
		t.Fatalf("fatal error = %q, want the first one", got)
	}
}

// WaitReady must not sit out boot_timeout on an error that cannot change: a
// missing table, a wrong password.
func TestWaitReadyStopsOnFatal(t *testing.T) {
	fs := newFakeSink()
	fs.healthy.Store(false)
	fs.healthErr = fatalErr("validate", errors.New("table does not exist"))
	h := NewHealth(fs, HealthConfig{Interval: time.Second, Timeout: time.Second},
		discard(), metrics())

	start := time.Now()
	err := h.WaitReady(context.Background(), 30*time.Second, time.Millisecond, 10*time.Millisecond)
	if err == nil {
		t.Fatal("WaitReady succeeded on a fatal sink")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("WaitReady waited %v on an error that cannot change", d)
	}
}

func TestWaitReadyRetriesThenSucceeds(t *testing.T) {
	fs := newFakeSink()
	fs.healthy.Store(false)
	h := NewHealth(fs, HealthConfig{Interval: time.Second, Timeout: time.Second},
		discard(), metrics())

	go func() {
		time.Sleep(30 * time.Millisecond)
		fs.healthy.Store(true)
	}()
	if err := h.WaitReady(context.Background(), 10*time.Second,
		time.Millisecond, 10*time.Millisecond); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

// ----------------------------------------------------------------- none sink

func TestNoneSink(t *testing.T) {
	var s Sink = NewNone()
	if err := s.Write(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatal("sink=none must never leave UP")
	}
	if s.Type() != "none" {
		t.Fatalf("type = %q", s.Type())
	}
	s.Close()
}

// --------------------------------------------------------------- sqlite sink

func newSQLiteSink(t *testing.T, rotate string) (*SQLite, string, *obs.Metrics) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "events.db")
	m := metrics()
	s, err := NewSQLite(SQLiteConfig{Path: path, Rotate: rotate, Synchronous: "OFF"}, discard(), m)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path, m
}

func TestSQLiteSinkWritesQueryableColumns(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	p := NewPrep("n", syslog.Options{}, nil)
	batch := p.Build([]*envelope.Item{item(0, line5424), item(1, line3164)})
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	p.Release()

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The promoted columns exist so plain SQL works; this is the query they
	// exist for.
	var host, app, rfc string
	var sev int
	err = db.QueryRow(`SELECT hostname, app_name, rfc, severity FROM events
	                   WHERE hostname = 'web-01' AND ts > 0`).Scan(&host, &app, &rfc, &sev)
	if err != nil {
		t.Fatalf("the indexed query found nothing: %v", err)
	}
	if host != "web-01" || app != "nginx" || rfc != "5424" || sev != 5 {
		t.Fatalf("row = %q %q %q %d", host, app, rfc, sev)
	}

	// Text search goes against raw, which covers the header fields too. That is
	// why there is no msg column.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events
	                       WHERE CAST(raw AS TEXT) LIKE '%su root%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("raw LIKE found %d rows", n)
	}

	// substr with the stored offsets yields exactly the MSG.
	var msg string
	err = db.QueryRow(`SELECT substr(CAST(raw AS TEXT), msg_offset + 1, msg_size)
	                   FROM events WHERE app_name = 'nginx'`).Scan(&msg)
	if err != nil {
		t.Fatal(err)
	}
	if msg != "hello" {
		t.Fatalf("extracted msg = %q", msg)
	}
}

// The pipeline is at-least-once: a redelivered message must not take the batch
// down with it.
func TestSQLiteSinkIsIdempotent(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	p := NewPrep("n", syslog.Options{}, nil)
	batch := p.Build([]*envelope.Item{item(0, line5424)})

	for i := 0; i < 3; i++ {
		if err := s.Write(context.Background(), batch); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	p.Release()

	db, _ := sql.Open("sqlite", "file:"+path)
	defer db.Close()
	var n int
	db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d rows after three writes of the same id", n)
	}
}

func TestSQLiteSinkRotates(t *testing.T) {
	s, path, m := newSQLiteSink(t, "hourly")
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return now })

	p := NewPrep("n", syslog.Options{}, nil)
	if err := s.Write(context.Background(), p.Build([]*envelope.Item{item(0, line5424)})); err != nil {
		t.Fatal(err)
	}
	// The sink opened its file against the real clock, so this first write
	// already rotated into the fake clock's period. The boundary under test is
	// the next one.
	first := s.current
	baseline := m.SinkSQLiteRotationsTotal.Load()

	now = now.Add(time.Hour)
	if err := s.Write(context.Background(), p.Build([]*envelope.Item{item(1, line3164)})); err != nil {
		t.Fatal(err)
	}
	p.Release()

	if s.current == first {
		t.Fatal("the file did not rotate across the hour boundary")
	}
	if got := m.SinkSQLiteRotationsTotal.Load() - baseline; got != 1 {
		t.Fatalf("rotations across the boundary = %d, want 1", got)
	}
	// Each file holds its own event; nothing was split or lost at the boundary.
	for _, f := range []string{first, s.current} {
		db, _ := sql.Open("sqlite", "file:"+f)
		var n int
		db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
		db.Close()
		if n != 1 {
			t.Fatalf("%s holds %d rows, want 1", filepath.Base(f), n)
		}
	}
	_ = path
}

// SPEC §10.4: the probe is a WRITE, rewritten in place, so it proves the file
// is still writable and costs the same at any table size. quick_check, which
// reads the whole file, runs at boot only.
func TestSQLiteSinkHealthOnAFreshFile(t *testing.T) {
	s, _, _ := newSQLiteSink(t, "none")
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health on a healthy file: %v", err)
	}

	var first, second int64
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&first); err != nil {
		t.Fatalf("the heartbeat row was not written: %v", err)
	}
	time.Sleep(time.Millisecond)
	if err := s.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("the heartbeat did not advance: %d -> %d", first, second)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM health`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("health table has %d rows (%v), want exactly one rewritten in place", rows, err)
	}
	if err := s.integrityCheck(); err != nil {
		t.Fatalf("integrityCheck on a good database: %v", err)
	}

	s.Close()
	if err := s.Health(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Health after Close = %v", err)
	}
}

// A file that is not a database is refused at boot, before anything is
// accepted that could not be stored.
func TestSQLiteSinkRefusesADamagedFileAtBoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	junk := []byte(strings.Repeat("this is not a database ", 100))
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics()); err == nil {
		t.Fatal("a file that is not a database was opened as one")
	}
}

// keep_files defaults to 0, which removes NOTHING: silently deleting a log
// archive is not a decision this program makes for an operator who did not ask.
func TestSQLiteKeepFilesZeroRemovesNothing(t *testing.T) {
	s, _, m := newSQLiteSink(t, "hourly")
	now := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return now })

	p := NewPrep("n", syslog.Options{}, nil)
	for i := 0; i < 5; i++ {
		s.Write(context.Background(), p.Build([]*envelope.Item{item(i, line5424)}))
		now = now.Add(time.Hour)
	}
	p.Release()
	if m.SinkSQLiteFilesRemovedTotal.Load() != 0 {
		t.Fatal("keep_files=0 removed a file")
	}
}

// -------------------------------------------------------------- instrumented

// ring=none writes straight through, bypassing the workers. Without the wrapper
// that mode reports every sink metric as a permanent zero and a fatal write dies
// inside one receiver while the process keeps running.
func TestInstrumentedObservesAndEscalates(t *testing.T) {
	fs := newFakeSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	i := NewInstrumented(fs, h, m, "n", syslog.Options{})

	var _ ring.Writer = i // it is what the ring receives

	if err := i.Write(context.Background(), []*envelope.Item{item(0, line5424)}); err != nil {
		t.Fatal(err)
	}
	if m.EventsWrittenTotal.Load() != 1 || m.SinkDuration.Count() != 1 || m.SinkBatchSize.Count() != 1 {
		t.Fatal("a write through ring=none was not observed")
	}
	if fs.count() != 1 {
		t.Fatal("the inner sink did not receive the batch")
	}

	fs.failWith = fatalErr("write", errors.New("OOM"))
	fs.failures = -1
	if err := i.Write(context.Background(), []*envelope.Item{item(1, line5424)}); err == nil {
		t.Fatal("fatal write returned nil")
	}
	select {
	case <-h.Fatal():
	case <-time.After(time.Second):
		t.Fatal("a fatal write through ring=none did not escalate")
	}
}

// The pool matters: in ring=none up to max_inflight receiver goroutines are in
// Write at once, and a shared Prep would corrupt every one of them.
func TestInstrumentedIsSafeConcurrently(t *testing.T) {
	fs := newFakeSink()
	m := metrics()
	i := NewInstrumented(fs, nil, m, "n", syslog.Options{})

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 50; k++ {
				if err := i.Write(context.Background(),
					[]*envelope.Item{item(g*50+k, line5424)}); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if fs.count() != 16*50 {
		t.Fatalf("wrote %d of %d", fs.count(), 16*50)
	}
}

// SPEC §14.2: a write through ring=none that ends by the clock must not pool
// the envelopes it built -- the command may still be in flight.
func TestInstrumentedAbandonsEnvelopesOnAContextError(t *testing.T) {
	var seen *byte
	hs := &hookSink{write: func(ctx context.Context, batch []Prepared) error {
		seen = &batch[0].Env[:1][0]
		return networkErr("xadd", context.DeadlineExceeded)
	}}
	m := metrics()
	i := NewInstrumented(hs, nil, m, "n", syslog.Options{})

	it := item(0, line5424)
	err := i.Write(context.Background(), []*envelope.Item{it})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if seen == nil {
		t.Fatal("the inner sink never saw the batch")
	}
	if it.Released() {
		t.Fatal("the wrapper must never touch the item; the receiver decides")
	}
	// The envelope buffer must not come back from the pool: a pooled one would
	// be returned straight from this goroutine's per-P slot.
	fresh := envelope.GetEnv()
	if &fresh[:1][0] == seen {
		t.Fatal("the envelope of a timed-out write was pooled while the command may still be in flight")
	}
	envelope.PutEnv(fresh)
	// SPEC §13.1: a timeout is retried like a network failure but counted
	// apart, in the kind="timeout" series.
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() != 1 || m.SinkErrorsTotal[obs.SinkErrNetwork].Load() != 0 {
		t.Fatalf("timeout=%d network=%d, want 1 and 0",
			m.SinkErrorsTotal[obs.SinkErrTimeout].Load(), m.SinkErrorsTotal[obs.SinkErrNetwork].Load())
	}
}

// SPEC §14.2: an attempt that ends by the clock taints the batch. Even when the
// retry succeeds, the raw lines go to the GC rather than the pool.
func TestWorkersTaintABatchAfterATimedOutAttempt(t *testing.T) {
	r := ring.NewMemory(1000, 0, time.Millisecond)
	var attempts atomic.Int64
	hs := &hookSink{write: func(ctx context.Context, batch []Prepared) error {
		if attempts.Add(1) == 1 {
			<-ctx.Done()
			return networkErr("xadd", ctx.Err())
		}
		return nil
	}}
	m := metrics()
	h := NewHealth(hs, HealthConfig{}, discard(), m)
	cfg := workerCfg(4)
	cfg.Workers = 1
	cfg.OpTimeout = 20 * time.Millisecond
	w := NewWorkers(r, hs, h, cfg, discard(), m)

	items := make([]*envelope.Item, 0, 4)
	for i := 0; i < 4; i++ {
		it := item(i, line5424)
		items = append(items, it)
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 5*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 4 })
	cancel()
	w.Wait()

	if attempts.Load() < 2 {
		t.Fatalf("only %d attempts; the stall never happened", attempts.Load())
	}
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() == 0 {
		t.Error("the timed-out attempt was not counted in kind=\"timeout\"")
	}
	for i, it := range items {
		if !it.Released() {
			t.Fatalf("item %d not released after the retry succeeded", i)
		}
		if it.Raw == nil {
			t.Errorf("item %d was pooled after a timed-out attempt; the first command may still be in flight", i)
		}
	}
}

// hookSink hands every write to a test function.
type hookSink struct {
	write func(ctx context.Context, batch []Prepared) error
}

func (s *hookSink) Write(ctx context.Context, batch []Prepared) error { return s.write(ctx, batch) }
func (s *hookSink) Health(context.Context) error                      { return nil }
func (s *hookSink) Close() error                                      { return nil }
func (s *hookSink) Type() string                                      { return "hook" }

func TestAllSinksImplementTheInterface(t *testing.T) {
	var _ Sink = NewNone()
	s, _, _ := newSQLiteSink(t, "none")
	var _ Sink = s
	var _ Sink = (*Redis)(nil)
	var _ Sink = (*Postgres)(nil)
	var _ Sink = (*MySQL)(nil)
	var _ Sink = (*NATS)(nil)
	// Instrumented is deliberately NOT a Sink: it is the ring.Writer adapter,
	// and its Write takes bare items because that is what ring=none has. It is
	// what the RING receives, never what the workers receive.
	var _ ring.Writer = (*Instrumented)(nil)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
