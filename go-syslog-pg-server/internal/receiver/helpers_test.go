package receiver

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
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
	ring *ring.Memory
	in   *Ingest
	m    *obs.Metrics
	sink *upState
	cfg  IngestConfig
}

func defaultIngestConfig() IngestConfig {
	return IngestConfig{Identity: testIdentity, MaxMessageBytes: 64 << 10}
}

// newHarness is a memory ring of capItems in front of a sink that is UP. There
// is nothing else to configure: the pipeline has no filter and no rate limiter
// (SPEC §1.2).
func newHarness(t *testing.T, capItems int) *harness {
	t.Helper()
	return newHarnessCfg(t, capItems, defaultIngestConfig())
}

func newHarnessCfg(t *testing.T, capItems int, cfg IngestConfig) *harness {
	t.Helper()
	h := &harness{
		ring: ring.NewMemory(capItems, 0, time.Millisecond),
		m:    newMetrics(),
		sink: newUp(),
		cfg:  cfg,
	}
	h.in = NewIngest(h.ring, h.sink, h.m, logger(), cfg)
	return h
}

// checkConverts is what the drain worker will do with an item the pipeline
// built: convert its head and raw into a JLH envelope that must pass both
// LogHub schemas and the semantic rules (invariant §20.5). The receive path
// never parses, so a head it fills wrongly would only surface there -- as a
// row the sink refuses -- unless a test asks here.
func checkConverts(t *testing.T, it *item.Item) []byte {
	t.Helper()
	env, _ := convert.New(convert.DefaultConfig()).Append(nil, &it.Head, it.Raw)
	if err := jlhcheck.Validate(env); err != nil {
		t.Errorf("%s/%s %q: the envelope is invalid: %v\n%s", it.Head.Transport, it.Head.Framing, it.Raw, err, env)
	}
	return env
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
	<-ctx.Done()
	return nil, ctx.Err()
}
func (r *stubRing) Commit(items []*item.Item) error { return nil }
func (r *stubRing) Drain() []*item.Item             { return nil }
func (r *stubRing) Stats() ring.Stats               { return ring.Stats{} }
func (r *stubRing) Mode() string                    { return "stub" }
func (r *stubRing) Close() error                    { return nil }

var _ ring.Ring = (*stubRing)(nil)
