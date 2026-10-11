package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// testFrom is a valid AgentUUID: the JLH from rule (lowercase, version 1-8, RFC
// variant).
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func metrics() *obs.Metrics {
	return obs.NewMetrics(obs.Info{RingMode: "memory", SinkType: "none", Transports: "tcp", Version: "test"})
}

// clock hands every test item its own instant, so uuids are distinct and
// ordered like real ones.
var clock atomic.Int64

// newItem builds an accepted item the way a receiver does: ONE clock sample for
// the uuid and the event (SPEC §3.1), canonical addresses, the raw line copied
// into a pooled buffer that the item owns.
func newItem(line string) *item.Item { return newItemIn("default", line) }

func newItemIn(space, line string) *item.Item {
	n := clock.Add(1)
	t := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC).Add(time.Duration(n) * 1001 * time.Nanosecond)
	raw, class := item.CopyRaw([]byte(line))
	h := item.Head{
		UUID: jlh.NewUUID(t), From: testFrom, Space: space,
		Transport: item.TransportTCP, Framing: item.FramingNonTransparent,
		SrcIP: netip.MustParseAddr("10.0.0.7"), SrcPort: 51234,
		DstIP: netip.MustParseAddr("10.0.0.10"), DstPort: 1514,
		Size: len(line),
	}
	h.SetTime(t)
	return item.New(h, raw, class)
}

const (
	line5424 = `<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] hello`
	line3164 = `<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`
	// lineMalformed has an invalid PRI: the whole line becomes message, format
	// "malformed" (SPEC §6.6).
	lineMalformed = `<999>not a syslog message`
)

func lines(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] message %d`, i)
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

func testConvert() convert.Config {
	cfg := convert.DefaultConfig()
	cfg.Columns = true
	return cfg
}

// batchFrom converts items into ONE batch the way a worker does, with the
// columns on, and checks every envelope against both schemas and the semantic
// rules (SPEC §18.2): a test that writes an envelope writes a valid one.
func batchFrom(t testing.TB, items ...*item.Item) *Batch {
	t.Helper()
	return batchWith(t, testConvert(), items...)
}

// batchWith is batchFrom with a given conversion config.
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
	columns   bool
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

// colSink is a recSink that asks for the promoted columns, like the SQL sinks.
type colSink struct {
	*recSink
	mu   sync.Mutex
	seen []convert.Result
}

func (c *colSink) NeedsColumns() bool { return true }
func (c *colSink) Write(ctx context.Context, b *Batch) error {
	c.mu.Lock()
	for i := range b.Envelopes {
		c.seen = append(c.seen, b.Envelopes[i].Res)
	}
	c.mu.Unlock()
	return c.recSink.Write(ctx, b)
}

// hookSink hands every write to a test function.
type hookSink struct {
	write func(ctx context.Context, b *Batch) error
}

func (s *hookSink) Write(ctx context.Context, b *Batch) error { return s.write(ctx, b) }
func (s *hookSink) Health(context.Context) error              { return nil }
func (s *hookSink) Close() error                              { return nil }
func (s *hookSink) Type() string                              { return "hook" }

// touchRing counts Touch calls on top of a real ring.
type touchRing struct {
	ring.Ring
	touches atomic.Int64
}

func (r *touchRing) Touch(items []*item.Item) error {
	r.touches.Add(1)
	return r.Ring.Touch(items)
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
