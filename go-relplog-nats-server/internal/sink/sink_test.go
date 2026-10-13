package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

// ------------------------------------------------------------- classification

// An error with no class must be retried, not dropped: a filling ring is
// visible, a discarded chunk is not (invariant §20.18).
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
	if !IsFatal(fmt.Errorf("wrapped: %w", fatalErr("op", errors.New("x")))) {
		t.Fatal("a wrapped fatal error lost its class")
	}
}

// SPEC §15.2: what taints a chunk is a context error, or a destination that
// says a lower layer may still read the buffer -- and nothing else.
func TestTaints(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		networkErr("ack", context.Canceled),
		tainting(logicErr("ack", errors.New("too big"))),
		tainting(errors.New("unclassified")),
	} {
		if !taints(err) {
			t.Errorf("%v does not taint", err)
		}
	}
	for _, err := range []error{nil, errors.New("plain"), networkErr("x", errors.New("refused"))} {
		if taints(err) {
			t.Errorf("%v taints", err)
		}
	}
	if c := ClassOf(tainting(logicErr("ack", errors.New("x")))); c != ClassLogic {
		t.Errorf("tainting changed the class to %s", c)
	}
}

// The obs label names are the converter's, in the same order: a reordering in
// either package would label one warning with another's name.
func TestObsLabelsMirrorTheConverter(t *testing.T) {
	for w := convert.Warning(0); w < convert.NumWarnings; w++ {
		if obs.WarningNames[w] != w.String() {
			t.Errorf("warning %d: obs %q, convert %q", w, obs.WarningNames[w], w.String())
		}
	}
}

func TestTheSinkImplementsTheInterface(t *testing.T) {
	var _ Sink = (*NATS)(nil)
	// Instrumented is deliberately NOT a Sink: it has Deliver and no Write.
	// Handing it to the workers would observe every chunk twice, and its
	// Deliver applies the per-class rules the workers already apply themselves.
	if _, ok := any((*Instrumented)(nil)).(interface {
		Write(context.Context, *Batch) error
	}); ok {
		t.Error("Instrumented has a Write: it could be wired as a Sink and double every counter")
	}
}

// ----------------------------------------------------------------- §3.9 bound

// SPEC §3.9: the worst JLH is 12 × L + 8 KiB, proved here with lines built to
// inflate every part of an envelope at once -- control bytes escaped six times
// in message AND in raw, invalid bytes decoded threefold with raw in base64,
// structured data of control bytes, a malformed PRI, a truncated line -- behind
// the largest head the receivers can produce (64-byte space, IPv6, PROXY; there
// is no TLS, so no peer subject).
func TestWorstCaseJLHIsBounded(t *testing.T) {
	for _, L := range []int{480, 4096, 64 << 10} {
		ctl := func(n int) string { return strings.Repeat("\x01", n) }
		bad := func(n int) string { return strings.Repeat("\xff", n) }
		fit := func(s string) string {
			if len(s) > L {
				return s[:L]
			}
			return s + strings.Repeat("\x01", L-len(s))
		}
		raws := map[string]string{
			"control message":       fit("<13>"),
			"control 5424":          fit("<13>1 2026-10-05T12:00:00.123456789Z " + ctl(40) + " " + ctl(40) + " " + ctl(40) + " " + ctl(30) + " - "),
			"control 3164 hostname": fit("<13>Oct 11 22:14:15 " + ctl(L/3) + " app[" + ctl(10) + "]: "),
			"control sd":            fit(`<13>1 - h a p m [x@1 k="` + ctl(L/2) + `" j="` + strings.Repeat(`\"`, L/8) + `"] `),
			"invalid bytes":         fit(bad(L)),
			"invalid and control":   fit("<13>" + strings.Repeat("\x01\xff", L/2)),
			"bad pri":               fit("<300>" + ctl(L)),
			"quotes":                fit("<13>" + strings.Repeat(`"\`, L/2)),
		}
		largest := 0
		space := strings.Repeat("s", 64)
		for name, raw := range raws {
			h := item.Head{
				From: testFrom, Space: space,
				Transport: item.TransportTCP, Framing: item.FramingOctetCounting,
				SrcIP:   netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"),
				SrcPort: 65535,
				DstIP:   netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:fffe"),
				DstPort: 65535,
				PeerIP:  netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:fffd"), PeerPort: 65535,
				Size: 1 << 30, // truncated: the original was far longer
			}
			now := time.Date(2026, 10, 5, 12, 0, 0, 999999999, time.UTC)
			h.UUID = jlh.NewUUID(now)
			h.SetTime(now)
			cfg := convert.DefaultConfig()
			cfg.MaxMessageBytes = L
			// The production config: every envelope of this server carries
			// "x-transport": "relp" (SPEC §3.2), inside the fixed 8 KiB.
			cfg.XTransport = "relp"
			out, _ := convert.New(cfg).Append(nil, &h, []byte(raw))
			largest = max(largest, len(out))
			if bound := MaxEventBytes(int64(L)); int64(len(out)) > bound {
				t.Errorf("L=%d %s: JLH of %d bytes, above the bound %d", L, name, len(out), bound)
			}
			if err := jlhcheck.Validate(out); err != nil {
				t.Errorf("L=%d %s: %v", L, name, err)
			}
		}
		// The lines must actually be adversarial, or the bound proves nothing:
		// the largest envelope has to come close to it.
		if L >= 4096 && float64(largest) < 0.9*float64(MaxEventBytes(int64(L))-8<<10) {
			t.Errorf("L=%d: the largest envelope is %d bytes, far below 12 × L = %d; the lines are not adversarial",
				L, largest, 12*L)
		}
		t.Logf("L=%d: largest JLH %d bytes (%.2f × L), bound %d", L, largest, float64(largest)/float64(L), MaxEventBytes(int64(L)))
	}
}

// --------------------------------------------------------------------- health

func TestHealthUpDownRecover(t *testing.T) {
	fs := newRecSink()
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
		t.Fatal("relplog_sink_up still 1 while DOWN")
	}

	fs.healthy.Store(true)
	waitFor(t, 5*time.Second, func() bool { return h.State() == StateUp })
	if m.SinkUp.Load() != 1 {
		t.Fatal("relplog_sink_up still 0 after recovery")
	}
}

// The retry fuse: N consecutive failed checks end the process.
func TestHealthFuseByRetries(t *testing.T) {
	fs := newRecSink()
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
	fs := newRecSink()
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
	h := NewHealth(newRecSink(), HealthConfig{}, discard(), metrics())
	h.MarkFatal(errors.New("first"))
	h.MarkFatal(errors.New("second")) // must not panic on a second close
	if got := h.FatalErr().Error(); got != "first" {
		t.Fatalf("fatal error = %q, want the first one", got)
	}
}

// WaitReady must not sit out boot_timeout on an error that cannot change: a
// missing table, a wrong password.
func TestWaitReadyStopsOnFatal(t *testing.T) {
	fs := newRecSink()
	fs.healthy.Store(false)
	fs.healthErr = fatalErr("validate", errors.New("table does not exist"))
	h := NewHealth(fs, HealthConfig{Interval: time.Second, Timeout: time.Second}, discard(), metrics())

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
	fs := newRecSink()
	fs.healthy.Store(false)
	h := NewHealth(fs, HealthConfig{Interval: time.Second, Timeout: time.Second}, discard(), metrics())

	go func() {
		time.Sleep(30 * time.Millisecond)
		fs.healthy.Store(true)
	}()
	if err := h.WaitReady(context.Background(), 10*time.Second, time.Millisecond, 10*time.Millisecond); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestWaitReadyGivesUpAtTheDeadline(t *testing.T) {
	fs := newRecSink()
	fs.healthy.Store(false)
	h := NewHealth(fs, HealthConfig{Interval: time.Second, Timeout: time.Second}, discard(), metrics())
	err := h.WaitReady(context.Background(), 30*time.Millisecond, time.Millisecond, 5*time.Millisecond)
	if err == nil || IsFatal(err) {
		t.Fatalf("WaitReady = %v, want the last network error", err)
	}
}

// blockingSink holds the probe until its context expires.
type blockingSink struct{}

func (blockingSink) Write(context.Context, *Batch) error { return nil }
func (blockingSink) Health(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (blockingSink) Close() error { return nil }
func (blockingSink) Type() string { return "blocking" }

// SPEC §13.3: a permanent refusal answered to the PROBE -- WRONGTYPE on the
// stream key, a dropped table -- is FATAL at the first check. The fuses exist
// for a destination that is unreachable, not for one that answered "never".
func TestHealthFatalReplyIsFatalAtOnce(t *testing.T) {
	fs := newRecSink()
	fs.healthy.Store(false)
	fs.healthErr = fatalErr("writeprobe", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"))
	m := metrics()
	h := NewHealth(fs, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: time.Second,
		MaxRetries: 1000, FailAfter: time.Hour, // neither fuse can be what trips
	}, discard(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("a fatal reply to the probe did not end the process at once")
	}
	if !IsFatal(h.FatalErr()) {
		t.Fatalf("FatalErr = %v, want the probe's fatal class retained", h.FatalErr())
	}
	if got := m.SinkErrorsTotal[obs.SinkErrFatal].Load(); got != 1 {
		t.Errorf("kind=fatal counted %d, want exactly 1: one probe, one verdict", got)
	}
	if m.SinkUp.Load() != 0 {
		t.Error("relplog_sink_up still 1 after a fatal probe")
	}
}

// SPEC §14.1: a probe that outlives sink.health_timeout is a "slow" incident,
// counted as kind="timeout" like a slow write, not as the network refusing.
func TestHealthProbeTimeoutIsCountedApart(t *testing.T) {
	m := metrics()
	h := NewHealth(blockingSink{}, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 10 * time.Millisecond,
		MaxRetries: 1000, FailAfter: time.Hour,
	}, discard(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	waitFor(t, 3*time.Second, func() bool { return m.SinkErrorsTotal[obs.SinkErrTimeout].Load() > 0 })
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 0 {
		t.Errorf("kind=network counted %d for probes that only timed out", got)
	}
}

// -------------------------------------------------------------- instrumented

// Instrumented.Deliver is the boot's write path (SPEC §13.1 step 9, §12.2): the
// envelopes of an emergency dump, already built, written with the drain
// workers' chunking and per-class rules. The tests below hand it envelopes the
// way the boot does -- converted once, in buffers the caller owns.

// noWait fails the test if Deliver ever asks to wait: only a network failure
// may.
func noWait(t *testing.T) func(int, error) error {
	return func(attempt int, err error) error {
		t.Errorf("wait(%d, %v) called with no network failure", attempt, err)
		return err
	}
}

// chunkSums returns the JLH bytes of each run of envs as cut by chunks.
func chunkSums(envs []Envelope, chunks []int) []int {
	var out []int
	off := 0
	for _, c := range chunks {
		sum := 0
		for _, e := range envs[off : off+c] {
			sum += len(e.JLH)
		}
		out = append(out, sum)
		off += c
	}
	return out
}

// SPEC §10.1: Deliver cuts the envelopes into chunks of at most sink.batch, in
// order, each claiming the bytes of its own JLH, and observes each chunk once:
// what a dump of 7 events costs at the destination is three statements, and the
// histograms say so.
func TestDeliverChunksByBatch(t *testing.T) {
	fs := newRecSink()
	m := metrics()
	d := NewInstrumented(fs, nil, workerCfg(3), discard(), m)
	envs := batchN(t, 7).Envelopes
	if err := d.Deliver(context.Background(), envs, time.Second, noWait(t)); err != nil {
		t.Fatal(err)
	}
	written, _, chunks, sizes, _ := fs.snapshot()
	if !slices.Equal(chunks, []int{3, 3, 1}) {
		t.Fatalf("chunks %v, want [3 3 1]", chunks)
	}
	if want := chunkSums(envs, chunks); !slices.Equal(sizes, want) {
		t.Errorf("Batch.Bytes %v, want the JLH sums %v", sizes, want)
	}
	for i := range envs {
		if written[i] != envs[i].UUID() {
			t.Fatalf("order broken at %d", i)
		}
	}
	total := 0
	for _, e := range envs {
		total += len(e.JLH)
	}
	if m.EventsWrittenTotal.Load() != 7 || m.SinkDuration.Count() != 3 ||
		m.SinkBatchSize.Count() != 3 || m.SinkBatchBytes.Count() != 3 {
		t.Errorf("written %d, durations %d, batch sizes %d, batch bytes %d; want 7, 3, 3, 3",
			m.EventsWrittenTotal.Load(), m.SinkDuration.Count(), m.SinkBatchSize.Count(), m.SinkBatchBytes.Count())
	}
	if got := scrape(t, m, "relplog_sink_batch_size_sum"); got != "7" {
		t.Errorf("relplog_sink_batch_size_sum = %s, want 7 envelopes", got)
	}
	if got := scrape(t, m, "relplog_sink_batch_bytes_sum"); got != strconv.Itoa(total) {
		t.Errorf("relplog_sink_batch_bytes_sum = %s, want %d", got, total)
	}
}

// SPEC §10.1: a chunk closes when the sum of its JLH would pass
// sink.batch_max_bytes, and an envelope larger than the ceiling goes alone --
// never skipped, never holding back its neighbours. The control case (all
// small) proves the ceiling is what cuts, not sink.batch.
func TestDeliverChunksByBatchMaxBytes(t *testing.T) {
	small := func() Envelope { return batchOf(t, "<13>small").Envelopes[0] }
	big := batchOf(t, "<13>"+strings.Repeat("x", 3000)).Envelopes[0]
	one := len(small().JLH)
	ceiling := 2*one + one/2 // two small ones fit, the big one never does
	if len(big.JLH) <= ceiling {
		t.Fatalf("the big envelope is only %d bytes", len(big.JLH))
	}

	for _, tc := range []struct {
		name  string
		envs  []Envelope
		want  []int
		alone int // index of the oversized envelope, -1 if none
	}{
		{"all small", []Envelope{small(), small(), small(), small(), small()}, []int{2, 2, 1}, -1},
		{"big in the middle", []Envelope{small(), small(), big, small()}, []int{2, 1, 1}, 2},
		{"big first", []Envelope{big, small(), small()}, []int{1, 2}, 0},
		{"big last", []Envelope{small(), big}, []int{1, 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newRecSink()
			cfg := workerCfg(64)
			cfg.BatchMaxBytes = ceiling
			d := NewInstrumented(fs, nil, cfg, discard(), metrics())
			if err := d.Deliver(context.Background(), tc.envs, time.Second, noWait(t)); err != nil {
				t.Fatal(err)
			}
			written, jlhs, chunks, sizes, _ := fs.snapshot()
			if !slices.Equal(chunks, tc.want) {
				t.Fatalf("chunks %v, want %v", chunks, tc.want)
			}
			if want := chunkSums(tc.envs, chunks); !slices.Equal(sizes, want) {
				t.Errorf("Batch.Bytes %v, want %v", sizes, want)
			}
			for i, c := range chunks {
				if c > 1 && sizes[i] > ceiling {
					t.Errorf("chunk %d of %d envelopes has %d bytes, above the ceiling %d", i, c, sizes[i], ceiling)
				}
			}
			for i := range tc.envs {
				if written[i] != tc.envs[i].UUID() || !bytes.Equal(jlhs[i], tc.envs[i].JLH) {
					t.Fatalf("envelope %d skipped, reordered or altered", i)
				}
			}
		})
	}
}

// SPEC §10.2: a logic refusal drops THAT chunk, counted and logged with its
// uuids, and the call moves on. Stopping there would leave the later chunks
// unwritten while the boot, which treats a logic refusal as delivered so it
// cannot crash-loop, removes the whole dump.
func TestDeliverLogicRefusalCountsAndMovesOn(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	var written []string
	hs := &hookSink{write: func(_ context.Context, b *Batch) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 2 {
			return logicErr("publish", errors.New("value too long"))
		}
		for i := range b.Envelopes {
			written = append(written, b.Envelopes[i].UUID())
		}
		return nil
	}}
	m := metrics()
	h := &recHandler{}
	d := NewInstrumented(hs, nil, workerCfg(2), slog.New(h), m)
	envs := batchN(t, 5).Envelopes
	if err := d.Deliver(context.Background(), envs, time.Second, noWait(t)); err != nil {
		t.Fatalf("a logic refusal ended the call: %v", err)
	}
	if calls != 3 {
		t.Fatalf("%d writes, want 3: the refused chunk is never retried, the others are written", calls)
	}
	if want := []string{envs[0].UUID(), envs[1].UUID(), envs[4].UUID()}; !slices.Equal(written, want) {
		t.Fatalf("written %v, want %v", written, want)
	}
	if m.EventsWrittenTotal.Load() != 3 || m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 ||
		m.SinkErrorsTotal[obs.SinkErrLogic].Load() != 1 || m.SinkErrorsTotal[obs.SinkErrNetwork].Load() != 0 {
		t.Errorf("written %d, dropped %d, logic errors %d, network errors %d; want 3, 2, 1, 0",
			m.EventsWrittenTotal.Load(), m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(),
			m.SinkErrorsTotal[obs.SinkErrLogic].Load(), m.SinkErrorsTotal[obs.SinkErrNetwork].Load())
	}
	v, ok := h.attr("refused a chunk permanently", "uuids")
	lost := fmt.Sprint(v.Any())
	if !ok || !strings.Contains(lost, envs[2].UUID()) || !strings.Contains(lost, envs[3].UUID()) ||
		strings.Contains(lost, envs[1].UUID()) || strings.Contains(lost, envs[4].UUID()) {
		t.Errorf("logged as lost: %q; want exactly envelopes 2 and 3", lost)
	}
}

// Independent review of go-relplog-pg-server, 2026-10-13: a logic refusal
// returned by a write during which the caller's context ended (SIGTERM in the
// reingestion) is still settled -- counted AND logged with its uuids -- and
// the call moves on. The context check came first, so the refusal was counted
// by write and never logged, and the call returned the cancellation instead.
func TestDeliverLogicRefusalWinsOverTheCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs := &hookSink{write: func(context.Context, *Batch) error {
		cancel() // the signal lands during the write
		return logicErr("publish", errors.New("value too long"))
	}}
	m := metrics()
	h := &recHandler{}
	d := NewInstrumented(hs, nil, workerCfg(4), slog.New(h), m)
	envs := batchN(t, 2).Envelopes
	if err := d.Deliver(ctx, envs, time.Second, noWait(t)); err != nil {
		t.Fatalf("the refused chunk returned %v, want it settled", err)
	}
	if m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 {
		t.Fatalf("dropped %d, want 2", m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
	}
	v, ok := h.attr("refused a chunk permanently", "uuids")
	if lost := fmt.Sprint(v.Any()); !ok || !strings.Contains(lost, envs[0].UUID()) || !strings.Contains(lost, envs[1].UUID()) {
		t.Fatalf("the counted refusal was not logged with its uuids: %q", lost)
	}
}

// SPEC §10.2: a network failure asks wait, then retries the SAME chunk -- the
// same envelopes, the same bytes, nothing rebuilt -- each attempt under its own
// opTimeout. A timeout is retried like any network failure but counted apart
// (SPEC §14.1), and the chunk is observed once however many attempts it takes.
func TestDeliverRetriesNetworkThroughWait(t *testing.T) {
	const opTimeout = 3 * time.Second
	var mu sync.Mutex
	var seen []*byte
	var sizes []int
	calls := 0
	hs := &hookSink{write: func(ctx context.Context, b *Batch) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		seen = append(seen, &b.Envelopes[0].JLH[:1][0])
		sizes = append(sizes, b.Bytes)
		if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > opTimeout {
			t.Errorf("attempt %d without its own opTimeout (deadline %v, %v)", calls, dl, ok)
		}
		switch calls {
		case 1:
			return networkErr("publish", errors.New("connection reset by peer"))
		case 2:
			return networkErr("publish", context.DeadlineExceeded)
		}
		return nil
	}}
	m := metrics()
	health := NewHealth(hs, HealthConfig{}, discard(), m)
	d := NewInstrumented(hs, health, workerCfg(10), discard(), m)
	envs := batchN(t, 3).Envelopes

	var waits []int
	var waitErrs []error
	err := d.Deliver(context.Background(), envs, opTimeout, func(attempt int, err error) error {
		waits = append(waits, attempt)
		waitErrs = append(waitErrs, err)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(waits, []int{1, 2}) || ClassOf(waitErrs[0]) != ClassNetwork ||
		!errors.Is(waitErrs[1], context.DeadlineExceeded) {
		t.Fatalf("wait called with %v %v, want attempts 1 and 2 with each failure", waits, waitErrs)
	}
	if calls != 3 {
		t.Fatalf("%d attempts, want 3", calls)
	}
	for i := range seen {
		if seen[i] != &envs[0].JLH[:1][0] || sizes[i] != sizes[0] {
			t.Fatal("a retry did not resend the caller's envelopes as they were (SPEC §10.2)")
		}
	}
	if m.EventsWrittenTotal.Load() != 3 {
		t.Errorf("events_written_total = %d, want 3: counted once, on the success", m.EventsWrittenTotal.Load())
	}
	if m.SinkErrorsTotal[obs.SinkErrNetwork].Load() != 1 || m.SinkErrorsTotal[obs.SinkErrTimeout].Load() != 1 {
		t.Errorf("network=%d timeout=%d, want 1 and 1",
			m.SinkErrorsTotal[obs.SinkErrNetwork].Load(), m.SinkErrorsTotal[obs.SinkErrTimeout].Load())
	}
	if m.SinkDuration.Count() != 3 || m.SinkBatchSize.Count() != 1 || m.SinkBatchBytes.Count() != 1 {
		t.Errorf("durations %d, batch sizes %d, batch bytes %d; want 3 (every attempt), 1 and 1 (per chunk)",
			m.SinkDuration.Count(), m.SinkBatchSize.Count(), m.SinkBatchBytes.Count())
	}
	// A network failure never moves the health state (SPEC §13.3).
	if health.State() != StateUp {
		t.Errorf("state %s after network failures", health.State())
	}
}

// A fatal write is returned at once -- no wait, no retry, no later chunk -- and
// escalates to the health machine, which ends the process (SPEC §13.3).
func TestDeliverFatalReturnsAtOnceAndMarksFatal(t *testing.T) {
	calls := 0
	fatal := fatalErr("ack", errors.New("nats: stream not found"))
	hs := &hookSink{write: func(context.Context, *Batch) error {
		calls++
		if calls == 2 {
			return fatal
		}
		return nil
	}}
	m := metrics()
	health := NewHealth(hs, HealthConfig{}, discard(), m)
	d := NewInstrumented(hs, health, workerCfg(2), discard(), m)
	err := d.Deliver(context.Background(), batchN(t, 6).Envelopes, time.Second, noWait(t))
	if !errors.Is(err, fatal) || !IsFatal(err) {
		t.Fatalf("err = %v, want the fatal error itself", err)
	}
	if calls != 2 {
		t.Fatalf("%d writes, want 2: nothing after the fatal chunk", calls)
	}
	select {
	case <-health.Fatal():
	default:
		t.Fatal("a fatal write did not mark the health FATAL")
	}
	if !errors.Is(health.FatalErr(), fatal) || m.SinkUp.Load() != 0 {
		t.Errorf("FatalErr %v, sink_up %d", health.FatalErr(), m.SinkUp.Load())
	}
	if m.SinkErrorsTotal[obs.SinkErrFatal].Load() != 1 || m.EventsWrittenTotal.Load() != 2 ||
		m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 0 {
		t.Errorf("fatal %d written %d dropped %d; want 1, 2, 0: a fatal chunk is not lost, it stays in the dump",
			m.SinkErrorsTotal[obs.SinkErrFatal].Load(), m.EventsWrittenTotal.Load(),
			m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
	}

	// Without a health machine the fatal error is still returned and counted.
	m = metrics()
	d = NewInstrumented(&hookSink{write: func(context.Context, *Batch) error { return fatal }}, nil,
		workerCfg(2), discard(), m)
	if err := d.Deliver(context.Background(), batchN(t, 1).Envelopes, time.Second, noWait(t)); !IsFatal(err) {
		t.Fatalf("err = %v", err)
	}
	if m.SinkErrorsTotal[obs.SinkErrFatal].Load() != 1 {
		t.Error("a fatal write with no health machine was not counted")
	}
}

// The caller's context ending is returned at once, without a wait, and the
// attempt it cut short is not a sink failure (SPEC §14.1): SIGTERM during the
// boot must not show up as the destination failing.
func TestDeliverStopsWhenTheCallerGoesAway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	hs := &hookSink{write: func(wctx context.Context, _ *Batch) error {
		calls++
		cancel()
		<-wctx.Done()
		return networkErr("publish", wctx.Err())
	}}
	m := metrics()
	d := NewInstrumented(hs, nil, workerCfg(1), discard(), m)
	err := d.Deliver(ctx, batchN(t, 3).Envelopes, time.Second, noWait(t))
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err = %v after %d writes, want context.Canceled after 1", err, calls)
	}
	for k, n := range map[string]uint64{
		"network": m.SinkErrorsTotal[obs.SinkErrNetwork].Load(),
		"timeout": m.SinkErrorsTotal[obs.SinkErrTimeout].Load(),
	} {
		if n != 0 {
			t.Errorf("kind=%s counted %d for the caller's own cancellation", k, n)
		}
	}

	// Control: the same failure with the caller still there IS counted.
	m = metrics()
	d = NewInstrumented(&hookSink{write: func(context.Context, *Batch) error {
		return networkErr("publish", errors.New("connection refused"))
	}}, nil, workerCfg(1), discard(), m)
	stop := errors.New("give up")
	if err := d.Deliver(context.Background(), batchN(t, 1).Envelopes, time.Second,
		func(int, error) error { return stop }); err != stop {
		t.Fatalf("err = %v, want wait's own error returned as is", err)
	}
	if m.SinkErrorsTotal[obs.SinkErrNetwork].Load() != 1 {
		t.Error("a network failure was not counted")
	}
}

// Nothing to deliver is not a write.
func TestDeliverNothing(t *testing.T) {
	calls := 0
	d := NewInstrumented(&hookSink{write: func(context.Context, *Batch) error { calls++; return nil }},
		nil, workerCfg(4), discard(), metrics())
	if err := d.Deliver(context.Background(), nil, time.Second, noWait(t)); err != nil || calls != 0 {
		t.Fatalf("err %v, %d writes", err, calls)
	}
}

// Review finding: Deliver used to be Write in a retry loop, so a logic
// refusal of one chunk ended the call with the later chunks never written --
// and the boot, treating logic as delivered, removed the dump. All three rules
// in one call, chunk after chunk.
func TestDeliverAppliesTheClassRulesPerChunk(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	var first, retried []byte
	var written []string
	fs := &hookSink{write: func(_ context.Context, b *Batch) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		switch calls {
		case 1: // chunk 1: refused for good
			return logicErr("publish", errors.New("constraint"))
		case 2: // chunk 2: a transient failure, then the retry
			first = b.Envelopes[0].JLH
			return networkErr("publish", errors.New("reset"))
		case 3:
			retried = b.Envelopes[0].JLH
		}
		for i := range b.Envelopes {
			written = append(written, b.Envelopes[i].UUID())
		}
		return nil
	}}
	m := metrics()
	cfg := workerCfg(10)
	cfg.BatchMaxBytes = 1 // every envelope is a chunk of its own
	d := NewInstrumented(fs, nil, cfg, discard(), m)
	envs := batchN(t, 3).Envelopes

	waits := 0
	err := d.Deliver(context.Background(), envs, time.Second, func(attempt int, err error) error {
		waits++
		if attempt != 1 || ClassOf(err) != ClassNetwork {
			t.Errorf("wait(%d, %v)", attempt, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if waits != 1 {
		t.Fatalf("wait called %d times, want 1", waits)
	}
	if len(written) != 2 || written[0] != envs[1].UUID() || written[1] != envs[2].UUID() {
		t.Fatalf("written = %v: the chunks after the refused one must still be written", written)
	}
	if &first[0] != &retried[0] {
		t.Fatal("the retry rebuilt the chunk; SPEC §10.2 resends the sealed bytes")
	}
	if m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 1 {
		t.Fatal("the refused chunk was not counted as lost")
	}
}

// flapSink answers the probe alternately well and badly, as fast as asked, so
// the state flips UP/DOWN continuously.
type flapSink struct{ n atomic.Int64 }

func (s *flapSink) Write(context.Context, *Batch) error { return nil }
func (s *flapSink) Health(context.Context) error {
	if s.n.Add(1)%2 == 0 {
		return nil
	}
	return errors.New("flap")
}
func (s *flapSink) Close() error { return nil }
func (s *flapSink) Type() string { return "flap" }

// Review finding 6: FATAL is terminal. Checks flipping the state UP and DOWN as
// fast as they can, racing a MarkFatal from a worker, must never store UP or
// DOWN over FATAL, nor put relplog_sink_up back to 1.
func TestHealthNeverLeavesFatal(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	for round := 0; round < 3000 && time.Now().Before(deadline); round++ {
		m := metrics()
		h := NewHealth(&flapSink{}, HealthConfig{Timeout: time.Second, MaxRetries: 1 << 30, FailAfter: time.Hour},
			discard(), m)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						h.check(context.Background())
					}
				}
			}()
		}
		for i := 0; i < round%7; i++ {
			runtime.Gosched()
		}
		h.MarkFatal(errors.New("OOM command not allowed"))
		for i := 0; i < 20; i++ {
			runtime.Gosched()
		}
		close(stop)
		wg.Wait()
		if h.State() != StateFatal || h.Up() || m.SinkUp.Load() != 0 {
			t.Fatalf("round %d: state %s, relplog_sink_up %d after MarkFatal", round, h.State(), m.SinkUp.Load())
		}
		select {
		case <-h.Fatal():
		default:
			t.Fatal("the fatal channel is open")
		}
	}
}

// settleLogic never counts more than the chunk: bad indexes and repeats are
// ignored, and no Delivered means everything is lost.
func TestSettleLogic(t *testing.T) {
	envs := batchN(t, 3).Envelopes
	for _, tc := range []struct {
		err   error
		want  int
		lostN int
	}{
		{logicErr("x", errors.New("refused")), 0, 3},
		{&Error{Class: ClassLogic, Err: errors.New("x"), Delivered: []int{1}}, 1, 2},
		{&Error{Class: ClassLogic, Err: errors.New("x"), Delivered: []int{0, 0, 2, 7, -1}}, 2, 1},
		{tainting(&Error{Class: ClassLogic, Err: errors.New("x"), Delivered: []int{0, 1, 2}}), 3, 0},
		{fmt.Errorf("wrapped: %w", &Error{Class: ClassLogic, Err: errors.New("x"), Delivered: []int{2}}), 1, 2},
	} {
		stored, lost := settleLogic(tc.err, envs)
		if stored != tc.want || len(lost) != tc.lostN || stored+len(lost) != len(envs) {
			t.Errorf("%v: stored %d lost %d, want %d and %d", tc.err, stored, len(lost), tc.want, tc.lostN)
		}
	}
}

// Review finding 8 through the boot path: Deliver counts and logs only the
// envelopes the destination did not store (Error.Delivered).
func TestDeliverCountsOnlyTheUndeliveredOnLogic(t *testing.T) {
	partial := func() error {
		return &Error{Class: ClassLogic, Op: "ack", Err: errors.New("data too long"), Delivered: []int{1}}
	}
	m := metrics()
	h := &recHandler{}
	i := NewInstrumented(&hookSink{write: func(context.Context, *Batch) error { return partial() }},
		nil, workerCfg(10), slog.New(h), m)
	envs := batchN(t, 3).Envelopes
	if err := i.Deliver(context.Background(), envs, time.Second, noWait(t)); err != nil {
		t.Fatal(err)
	}
	if m.EventsWrittenTotal.Load() != 1 || m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 {
		t.Fatalf("written %d dropped %d, want 1 and 2",
			m.EventsWrittenTotal.Load(), m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
	}
	v, ok := h.attr("refused a chunk permanently", "uuids")
	lost := fmt.Sprint(v.Any())
	if !ok || strings.Contains(lost, envs[1].UUID()) ||
		!strings.Contains(lost, envs[0].UUID()) || !strings.Contains(lost, envs[2].UUID()) {
		t.Fatalf("logged as lost: %s; want envelopes 0 and 2 only", lost)
	}
}

// The builder fills the two fields the item's head cannot give from the
// conversion: Level is the severity of the PRI and SID the JLH's sid. An
// envelope's Level and SID must be the header the JLH itself carries -- for
// every format, malformed included -- because a reingested envelope gets them
// back from that header (SPEC §12.2).
func TestBuilderFillsLevelAndSID(t *testing.T) {
	b := batchOf(t, line5424, line3164, lineMalformed, "<15>1 - h a - - [x@1 k=\"v\"] m", "no pri at all")
	levels := map[int64]bool{}
	for i := range b.Envelopes {
		e := &b.Envelopes[i]
		var doc struct{ Level, SID int64 }
		if err := json.Unmarshal(e.JLH, &doc); err != nil {
			t.Fatal(err)
		}
		if int64(e.Level) != doc.Level || int64(e.SID) != doc.SID {
			t.Errorf("envelope %d: Level %d SID %d, the JLH says %d and %d", i, e.Level, e.SID, doc.Level, doc.SID)
		}
		levels[doc.Level] = true
	}
	// Control: the lines carry different severities, so a constant Level
	// could not pass.
	if len(levels) < 3 {
		t.Fatalf("only %d distinct levels; the lines do not exercise the mapping", len(levels))
	}
}
