package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ring"
)

// testFrom is a valid AgentUUID: the JLH from rule (lowercase, version 1-8, RFC
// variant).
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func metrics() *obs.Metrics {
	return obs.NewMetrics(obs.Info{RingMode: "memory", SinkType: "nats", Transports: "tcp", Version: "test"})
}

// scrape returns the value of one unlabelled series in the exposition, so a
// test can read what the histograms do not export as methods: the _sum of a
// batch-size histogram is what says how many envelopes the chunks carried, a
// _count alone only says how many chunks there were.
func scrape(t testing.TB, m *obs.Metrics, series string) string {
	t.Helper()
	var b strings.Builder
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(b.String(), "\n") {
		if v, ok := strings.CutPrefix(l, series+" "); ok {
			return v
		}
	}
	t.Fatalf("series %s is not in the exposition", series)
	return ""
}

// clock hands every test item its own instant, so uuids are distinct and
// ordered like real ones.
var clock atomic.Int64

// newItem builds an accepted item the way a receiver does: ONE clock sample for
// the uuid and the event (SPEC §3.1), canonical addresses, the decompressed
// GELF document copied into a pooled buffer that the item owns.
func newItem(doc string) *item.Item { return newItemIn("default", doc) }

func newItemIn(space, line string) *item.Item {
	n := clock.Add(1)
	t := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC).Add(time.Duration(n) * 1001 * time.Nanosecond)
	raw, class := item.CopyRaw([]byte(line))
	h := item.Head{
		UUID: jlh.NewUUID(t), From: testFrom, Space: space,
		Transport: item.TransportTCP,
		SrcIP:     netip.MustParseAddr("10.0.0.7"), SrcPort: 51234,
		DstIP: netip.MustParseAddr("10.0.0.10"), DstPort: 12201,
	}
	h.SetTime(t)
	return item.New(h, raw, class)
}

const (
	// docGELF is a GELF 1.1 document with a host, level 5 (notice) and two
	// additional fields, one a number.
	docGELF = `{"version":"1.1","host":"web-01","short_message":"hello","timestamp":1757687581.5,"level":5,"_a":1,"_k":"v"}`
	// docGELF10 is a GELF 1.0 document with the three keys 1.1 deprecated
	// (facility, file, line) and level 2 (crit).
	docGELF10 = `{"version":"1.0","host":"mymachine","short_message":"'su root' failed","level":2,"facility":"auth","file":"su.c","line":230}`
	// docMalformed is not JSON: the whole document becomes raw, format
	// "malformed" (SPEC §6.1).
	docMalformed = `{"version":"1.1","host":"x", not a gelf document`
)

func lines(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"version":"1.1","host":"web-01","short_message":"message %d","level":5,"_a":1,"_k":"v"}`, i)
	}
	return out
}

func itemsOf(ls ...string) []*item.Item {
	out := make([]*item.Item, len(ls))
	for i, l := range ls {
		out[i] = newItem(l)
	}
	return out
}

// batchFrom converts items into ONE batch the way a worker does, and checks
// every envelope against both schemas and the semantic rules (SPEC §18.2): a
// test that writes an envelope writes a valid one.
func batchFrom(t testing.TB, items ...*item.Item) *Batch {
	t.Helper()
	return batchWith(t, convert.DefaultConfig(), items...)
}

// batchWith is batchFrom under a conversion config of the test's own: a larger
// convert.max_message_bytes, for the worst-case envelopes of SPEC §3.9.
func batchWith(t testing.TB, cfg convert.Config, items ...*item.Item) *Batch {
	t.Helper()
	b := newBuilder(cfg, 1<<30, true, newBufPool(1<<30), nil)
	if n := b.fill(items); n != len(items) {
		t.Fatalf("builder consumed %d of %d items", n, len(items))
	}
	c := b.cur
	c.seal()
	for i := range c.Envelopes {
		if err := jlhcheck.Validate(c.Envelopes[i].JLH); err != nil {
			t.Fatalf("envelope %d is not a valid JLH: %v\n%s", i, err, c.Envelopes[i].JLH)
		}
	}
	batch := c.Batch
	return &batch
}

func batchOf(t testing.TB, ls ...string) *Batch { return batchFrom(t, itemsOf(ls...)...) }

func batchN(t testing.TB, n int) *Batch { return batchOf(t, lines(n)...) }

// ---------------------------------------------------------------- fake sinks

// recSink records what it was given: the uuid and a COPY of the JLH of every
// envelope written, and every attempt's bytes.
type recSink struct {
	mu        sync.Mutex
	written   []string
	jlhs      [][]byte
	attempts  [][][]byte
	chunks    []int // envelopes per successful write
	chunkSize []int // Bytes per successful write
	failWith  error
	failures  int // fail this many times, then succeed; < 0 forever
	healthy   atomic.Bool
	healthErr error
}

func newRecSink() *recSink {
	s := &recSink{}
	s.healthy.Store(true)
	return s
}

func (s *recSink) Write(_ context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var attempt [][]byte
	for i := range b.Envelopes {
		attempt = append(attempt, append([]byte(nil), b.Envelopes[i].JLH...))
	}
	s.attempts = append(s.attempts, attempt)
	if s.failures != 0 {
		if s.failures > 0 {
			s.failures--
		}
		return s.failWith
	}
	for i := range b.Envelopes {
		s.written = append(s.written, b.Envelopes[i].UUID())
		s.jlhs = append(s.jlhs, append([]byte(nil), b.Envelopes[i].JLH...))
	}
	s.chunks = append(s.chunks, len(b.Envelopes))
	s.chunkSize = append(s.chunkSize, b.Bytes)
	return nil
}

func (s *recSink) Health(context.Context) error {
	if s.healthy.Load() {
		return nil
	}
	if s.healthErr != nil {
		return s.healthErr
	}
	return errors.New("unhealthy")
}
func (s *recSink) Close() error { return nil }
func (s *recSink) Type() string { return "rec" }

func (s *recSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func (s *recSink) snapshot() (written []string, jlhs [][]byte, chunks, sizes []int, attempts [][][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.written...), append([][]byte(nil), s.jlhs...),
		append([]int(nil), s.chunks...), append([]int(nil), s.chunkSize...),
		append([][][]byte(nil), s.attempts...)
}

// hookSink hands every write to a test function.
type hookSink struct {
	write func(ctx context.Context, b *Batch) error
}

func (s *hookSink) Write(ctx context.Context, b *Batch) error { return s.write(ctx, b) }
func (s *hookSink) Health(context.Context) error              { return nil }
func (s *hookSink) Close() error                              { return nil }
func (s *hookSink) Type() string                              { return "hook" }

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

func workerCfg(batch int) WorkerConfig {
	return WorkerConfig{
		Workers: 2, Batch: batch, BatchMaxBytes: 16 << 20, OpTimeout: 2 * time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
		Convert: convert.DefaultConfig(), AcceptMalformed: true,
	}
}

func enqueueAll(t *testing.T, r ring.Ring, items []*item.Item) {
	t.Helper()
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
}

// docNUL carries a NUL (\u0000) in every kind of string the payload has: the
// host, short-message, full-message, facility, file, a field value, a field
// name, and the keys and values of a nested extra object. jsonb refuses all of
// them (SPEC §3.7.1). It also carries numbers no float64 holds exactly.
const docNUL = `{"version":"1.0","host":"web\u0000-01","short_message":"a\u0000b","full_message":"f\u0000",` +
	`"facility":"fa\u0000c","file":"fi\u0000le","line":7,"_x":"a\u0000b","_n":12345678901234567890,` +
	`"_big":1e400,"_k\u0000":"v","other\u0000key":{"in\u0000ner":["\u0000",1e400,{"k\u0000":"v\u0000"}]}}`

// gdoc is a GELF 1.1 document whose short_message is msg, for the tests that
// only need distinct, valid messages of a given size.
func gdoc(msg string) string {
	b, _ := json.Marshal(msg)
	return `{"version":"1.1","host":"h","short_message":` + string(b) + `}`
}
