package receiver

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// testIdentity is assembled by hand, as SPEC §18.1 asks of every package that
// does not need the identity library itself.
var testIdentity = ident.Identity{
	From:  "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space: "default",
}

func logger() *obs.Logger { return obs.NewLogger(io.Discard, "text", "error") }

func newMetrics() *obs.Metrics { return obs.NewMetrics(obs.Info{RingMode: "memory", SinkType: "none"}) }

type upState struct{ up atomic.Bool }

func (u *upState) Up() bool { return u.up.Load() }

func newUp() *upState {
	u := &upState{}
	u.up.Store(true)
	return u
}

type harness struct {
	ring  *ring.Memory
	in    *Ingest
	m     *obs.Metrics
	sink  *upState
	filt  *filter.Filter
	limit *ratelimit.Limiter
	cfg   IngestConfig
}

func defaultIngestConfig() IngestConfig {
	return IngestConfig{Identity: testIdentity, DefaultPriority: 13, MaxMessageBytes: 64 << 10}
}

func newHarness(t *testing.T, capItems int, minSev string, facilities []string) *harness {
	t.Helper()
	return newHarnessCfg(t, capItems, minSev, facilities, defaultIngestConfig())
}

func newHarnessCfg(t *testing.T, capItems int, minSev string, facilities []string, cfg IngestConfig) *harness {
	t.Helper()
	rule, err := filter.Compile(minSev, facilities)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		ring: ring.NewMemory(capItems, 0, time.Millisecond),
		m:    newMetrics(),
		sink: newUp(),
		filt: filter.New(rule),
		cfg:  cfg,
	}
	h.in = NewIngest(h.ring, h.sink, h.filt, nil, h.m, logger(), cfg)
	return h
}

// withLimiter rebuilds the pipeline with a rate limiter.
func (h *harness) withLimiter(p ratelimit.Params) {
	h.limit = ratelimit.New(p, 4)
	h.in = NewIngest(h.ring, h.sink, h.filt, h.limit, h.m, logger(), h.cfg)
}

// drainRing takes everything the ring holds, so a test can assert on the items
// instead of on a counter.
func (h *harness) drainRing() []*item.Item { return h.ring.Drain() }

func (h *harness) waitItems(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.ring.Stats().Items >= int64(n) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d of %d items reached the ring", h.ring.Stats().Items, n)
}

// waitFor polls a condition, for the counters a transport goroutine updates.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// shortDir is a temporary directory with a SHORT path: t.TempDir() on macOS
// lives under /var/folders/... and a long test name pushes a socket path past
// the 103 bytes of sun_path.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "slr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	d, _ = filepath.EvalSymlinks(d)
	return d
}

// stubRing answers Enqueue with whatever the test needs and remembers the
// items, so a test can check what the pipeline did with their buffers.
type stubRing struct {
	errs  []error // consumed one per Enqueue; nil when exhausted
	items []*item.Item
}

func (r *stubRing) Enqueue(ctx context.Context, it *item.Item) error {
	r.items = append(r.items, it)
	if len(r.errs) == 0 {
		return nil
	}
	err := r.errs[0]
	r.errs = r.errs[1:]
	return err
}
func (r *stubRing) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	return nil, ring.ErrNoBacklog
}
func (r *stubRing) Commit(items []*item.Item) error { return nil }
func (r *stubRing) Touch(items []*item.Item) error  { return nil }
func (r *stubRing) Drain() []*item.Item             { return nil }
func (r *stubRing) Stats() ring.Stats               { return ring.Stats{} }
func (r *stubRing) Mode() string                    { return "stub" }
func (r *stubRing) Close() error                    { return nil }

var _ ring.Ring = (*stubRing)(nil)
