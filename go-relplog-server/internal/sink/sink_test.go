package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
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

// SPEC §3.7: NUL becomes U+FFFD in the column copy, invalid UTF-8 too, and
// valid text is returned untouched.
func TestColText(t *testing.T) {
	cases := map[string]string{
		"ok":        "ok",
		"a\x00b":    "a�b",
		"a\xffb":    "a�b",
		"":          "",
		"é\x00\x00": "é��",
	}
	for in, want := range cases {
		if got := colText(in); got != want {
			t.Errorf("colText(%q) = %q, want %q", in, got, want)
		}
		if got := colTextLen(in); got != len(want) {
			t.Errorf("colTextLen(%q) = %d, want %d", in, got, len(want))
		}
	}
}

func TestNeedsColumns(t *testing.T) {
	for typ, want := range map[string]bool{
		"sqlite": true, "mysql": true,
		"postgres": false, "nats": false, "redis": false, "none": false,
	} {
		if NeedsColumns(typ) != want {
			t.Errorf("NeedsColumns(%q) = %v", typ, !want)
		}
	}
	for _, s := range []any{(*SQLite)(nil), (*MySQL)(nil)} {
		if cs, ok := s.(ColumnSink); !ok || !cs.NeedsColumns() {
			t.Errorf("%T is not a ColumnSink", s)
		}
	}
	// loghub.inbox (SPEC §3.7.1) takes the header and the payload only.
	for _, s := range []any{(*NATS)(nil), (*Redis)(nil), (*Postgres)(nil), NewNone()} {
		if _, ok := s.(ColumnSink); ok {
			t.Errorf("%T claims columns it does not store", s)
		}
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

func TestAllSinksImplementTheInterface(t *testing.T) {
	var _ Sink = NewNone()
	var _ Sink = (*SQLite)(nil)
	var _ Sink = (*Redis)(nil)
	var _ Sink = (*Postgres)(nil)
	var _ Sink = (*MySQL)(nil)
	var _ Sink = (*NATS)(nil)
	// Instrumented is deliberately NOT a Sink: it is the ring.Writer adapter,
	// and its Write takes bare items because that is what ring=none has. It is
	// what the RING receives, never what the workers receive.
	var _ ring.Writer = (*Instrumented)(nil)
}

// ----------------------------------------------------------------- §3.9 bound

// SPEC §3.9: the worst JLH is 12 × L + 8 KiB, proved here with lines built to
// inflate every part of an envelope at once -- control bytes escaped six times
// in message AND in raw, invalid bytes decoded threefold with raw in base64,
// structured data of control bytes, a malformed PRI, a truncated line -- behind
// the largest head the receivers can produce (64-byte space, IPv6, TLS with a
// 1 KiB peer subject, PROXY).
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
		peer := strings.Repeat("CN=x,", 1024/5)
		for name, raw := range raws {
			h := item.Head{
				From: testFrom, Space: space,
				Transport: item.TransportTLS, Framing: item.FramingOctetCounting,
				SrcIP:   netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"),
				SrcPort: 65535,
				DstIP:   netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:fffe"),
				DstPort: 65535,
				PeerIP:  netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:fffd"), PeerPort: 65535,
				TLS: &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_CHACHA20_POLY1305_SHA256",
					PeerSubject:     peer,
					PeerFingerprint: "sha-256:" + strings.Repeat("FF:", 31) + "FF"},
				Size: 1 << 30, // truncated: the original was far longer
			}
			now := time.Date(2026, 10, 5, 12, 0, 0, 999999999, time.UTC)
			h.UUID = jlh.NewUUID(now)
			h.SetTime(now)
			cfg := convert.DefaultConfig()
			cfg.MaxMessageBytes = L
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

// ----------------------------------------------------------------- none sink

func TestNoneSink(t *testing.T) {
	var s Sink = NewNone()
	if err := s.Write(context.Background(), batchN(t, 2)); err != nil {
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

// -------------------------------------------------------------- instrumented

// ring=none writes straight through, bypassing the workers. Without the wrapper
// that mode reports every sink metric as a permanent zero and a fatal write dies
// inside one receiver while the process keeps running.
func TestInstrumentedObservesAndEscalates(t *testing.T) {
	fs := newRecSink()
	m := metrics()
	h := NewHealth(fs, HealthConfig{}, discard(), m)
	i := NewInstrumented(fs, h, workerCfg(1), discard(), m)

	var _ ring.Writer = i // it is what the ring receives

	it := newItem(line5424)
	if err := i.Write(context.Background(), []*item.Item{it}); err != nil {
		t.Fatal(err)
	}
	if m.EventsWrittenTotal.Load() != 1 || m.SinkDuration.Count() != 1 || m.SinkBatchSize.Count() != 1 ||
		m.ConvertTotal[obs.FormatRFC5424].Load() != 1 {
		t.Fatal("a write through ring=none was not observed")
	}
	_, jlhs, _, _, _ := fs.snapshot()
	if len(jlhs) != 1 {
		t.Fatal("the inner sink did not receive the envelope")
	}
	if err := jlhcheck.Validate(jlhs[0]); err != nil {
		t.Fatal(err)
	}
	if it.Released() {
		t.Fatal("the wrapper must never touch the item; the receiver decides")
	}

	fs.failWith = fatalErr("write", errors.New("OOM"))
	fs.failures = -1
	if err := i.Write(context.Background(), []*item.Item{newItem(line5424)}); err == nil {
		t.Fatal("fatal write returned nil")
	}
	select {
	case <-h.Fatal():
	case <-time.After(time.Second):
		t.Fatal("a fatal write through ring=none did not escalate")
	}
}

// The pool matters: in ring=none up to max_inflight receiver goroutines are in
// Write at once, and a shared Converter would corrupt every one of them.
func TestInstrumentedIsSafeConcurrently(t *testing.T) {
	fs := newRecSink()
	m := metrics()
	i := NewInstrumented(fs, nil, workerCfg(1), discard(), m)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 50; k++ {
				line := fmt.Sprintf("<13>1 - host-%d app - - - message %d", g, k)
				if err := i.Write(context.Background(), []*item.Item{newItem(line)}); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	_, jlhs, _, _, _ := fs.snapshot()
	if len(jlhs) != 16*50 {
		t.Fatalf("wrote %d of %d", len(jlhs), 16*50)
	}
	// Each envelope carries its own host with its own message: a shared
	// converter would mix them, or lose some.
	seen := map[string]bool{}
	for _, b := range jlhs {
		if err := jlhcheck.Validate(b); err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		var e struct {
			Payload struct {
				Host    struct{ Hostname string } `json:"host"`
				Message string                    `json:"message"`
				Raw     string                    `json:"raw"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(b, &e); err != nil {
			t.Fatal(err)
		}
		p := e.Payload
		if !strings.Contains(p.Raw, " "+p.Host.Hostname+" ") || !strings.HasSuffix(p.Raw, " "+p.Message) {
			t.Fatalf("envelope mixes two messages: %s", b)
		}
		seen[p.Raw] = true
	}
	if len(seen) != 16*50 {
		t.Fatalf("%d distinct messages, want %d", len(seen), 16*50)
	}
}

// SPEC §15.2: a write through ring=none that ends by the clock must not pool
// the chunk buffer it built -- the command may still be in flight.
func TestInstrumentedAbandonsTheBufferOnAContextError(t *testing.T) {
	var seen *byte
	hs := &hookSink{write: func(ctx context.Context, b *Batch) error {
		seen = &b.Envelopes[0].JLH[:1][0]
		return networkErr("xadd", context.DeadlineExceeded)
	}}
	m := metrics()
	i := NewInstrumented(hs, nil, workerCfg(1), discard(), m)

	it := newItem(line5424)
	err := i.Write(context.Background(), []*item.Item{it})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if seen == nil {
		t.Fatal("the inner sink never saw the batch")
	}
	if it.Released() || it.Tainted() {
		t.Fatal("the wrapper must never touch the item; the receiver decides")
	}
	for k := 0; k < 64; k++ {
		b := i.pool.get()
		if cap(b) > 0 && &b[:1][0] == seen {
			t.Fatal("the chunk buffer of a timed-out write was pooled while the command may still be in flight")
		}
	}
	// SPEC §14.1: a timeout is retried like a network failure but counted
	// apart, in the kind="timeout" series.
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() != 1 || m.SinkErrorsTotal[obs.SinkErrNetwork].Load() != 0 {
		t.Fatalf("timeout=%d network=%d, want 1 and 0",
			m.SinkErrorsTotal[obs.SinkErrTimeout].Load(), m.SinkErrorsTotal[obs.SinkErrNetwork].Load())
	}
}

// A refused malformed message is a deliberate, counted drop: the call succeeds
// and nothing is written.
func TestInstrumentedRefusesMalformed(t *testing.T) {
	fs := newRecSink()
	m := metrics()
	cfg := workerCfg(1)
	cfg.AcceptMalformed = false
	i := NewInstrumented(fs, nil, cfg, discard(), m)
	if err := i.Write(context.Background(), []*item.Item{newItem(lineMalformed)}); err != nil {
		t.Fatal(err)
	}
	if fs.count() != 0 {
		t.Fatal("a refused malformed message was written")
	}
	if m.EventsDroppedTotal[obs.EventDropMalformed].Load() != 1 {
		t.Fatal("not counted")
	}
}

// A logic refusal through ring=none is counted as lost right there.
func TestInstrumentedCountsLogic(t *testing.T) {
	fs := newRecSink()
	fs.failWith = logicErr("insert", errors.New("constraint"))
	fs.failures = 1
	m := metrics()
	i := NewInstrumented(fs, nil, workerCfg(1), discard(), m)
	if err := i.Write(context.Background(), []*item.Item{newItem(line5424)}); ClassOf(err) != ClassLogic {
		t.Fatalf("err = %v", err)
	}
	if m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 1 || m.SinkErrorsTotal[obs.SinkErrLogic].Load() != 1 {
		t.Fatal("logic refusal not counted")
	}
}

// Deliver is the boot's write path (SPEC §13.1 steps 9 and 10). Review finding:
// it used to be Write in a retry loop, so a logic refusal of one chunk ended the
// call with the later chunks never written -- and the boot, treating logic as
// delivered, committed them all. And every retry converted the batch again.
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
			return logicErr("insert", errors.New("constraint"))
		case 2: // chunk 2: a transient failure, then the retry
			first = b.Envelopes[0].JLH
			return networkErr("insert", errors.New("reset"))
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
	cfg.BatchMaxBytes = 1 // every message is a chunk of its own
	d := NewInstrumented(fs, nil, cfg, discard(), m)
	items := itemsOf(lines(3)...)

	waits := 0
	err := d.Deliver(context.Background(), items, time.Second, func(attempt int, err error) error {
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
	if len(written) != 2 || written[0] != items[1].Head.UUID.String() || written[1] != items[2].Head.UUID.String() {
		t.Fatalf("written = %v: the chunks after the refused one must still be written", written)
	}
	if &first[0] != &retried[0] {
		t.Fatal("the retry rebuilt the chunk; SPEC §10.2 resends the sealed bytes")
	}
	if m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 1 {
		t.Fatal("the refused chunk was not counted as lost")
	}
	if got := m.ConvertTotal[convert.FormatRFC5424].Load() + m.ConvertTotal[convert.FormatRFC3164].Load() +
		m.ConvertTotal[convert.FormatMalformed].Load(); got != 3 {
		t.Fatalf("%d conversions for 3 messages", got)
	}
}

// A fatal error and the end of wait's patience both end the call at once.
func TestDeliverStopsOnFatalAndOnWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"fatal", fatalErr("insert", errors.New("permission"))}, {"network", networkErr("insert", errors.New("reset"))}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			fs := &hookSink{write: func(context.Context, *Batch) error { calls++; return tc.err }}
			d := NewInstrumented(fs, nil, workerCfg(10), discard(), metrics())
			stop := errors.New("give up")
			err := d.Deliver(context.Background(), itemsOf(lines(3)...), time.Second,
				func(int, error) error { return stop })
			if tc.name == "fatal" && ClassOf(err) != ClassFatal || tc.name == "network" && err != stop {
				t.Fatalf("err = %v", err)
			}
			if calls != 1 {
				t.Fatalf("%d writes", calls)
			}
		})
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

// Review finding 8 through ring=none and the boot path: Instrumented counts and
// logs only the envelopes the destination did not store.
func TestInstrumentedCountsOnlyTheUndeliveredOnLogic(t *testing.T) {
	partial := func() error {
		return &Error{Class: ClassLogic, Op: "insert", Err: errors.New("data too long"), Delivered: []int{1}}
	}
	t.Run("Write", func(t *testing.T) {
		m := metrics()
		i := NewInstrumented(&hookSink{write: func(context.Context, *Batch) error { return partial() }},
			nil, workerCfg(1), discard(), m)
		if err := i.Write(context.Background(), itemsOf(lines(3)...)); ClassOf(err) != ClassLogic {
			t.Fatalf("err = %v", err)
		}
		if m.EventsWrittenTotal.Load() != 1 || m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 {
			t.Fatalf("written %d dropped %d, want 1 and 2",
				m.EventsWrittenTotal.Load(), m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
		}
	})
	t.Run("Deliver", func(t *testing.T) {
		m := metrics()
		h := &recHandler{}
		i := NewInstrumented(&hookSink{write: func(context.Context, *Batch) error { return partial() }},
			nil, workerCfg(1), slog.New(h), m)
		items := itemsOf(lines(3)...)
		if err := i.Deliver(context.Background(), items, time.Second, func(int, error) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if m.EventsWrittenTotal.Load() != 1 || m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() != 2 {
			t.Fatalf("written %d dropped %d, want 1 and 2",
				m.EventsWrittenTotal.Load(), m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
		}
		v, ok := h.attr("refused a chunk permanently", "uuids")
		lost := fmt.Sprint(v.Any())
		if !ok || strings.Contains(lost, items[1].Head.UUID.String()) ||
			!strings.Contains(lost, items[0].Head.UUID.String()) || !strings.Contains(lost, items[2].Head.UUID.String()) {
			t.Fatalf("logged as lost: %s; want items 0 and 2 only", lost)
		}
	})
}
