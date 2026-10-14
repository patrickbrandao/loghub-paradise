package app

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
)

// sizesMemSink is the fake destination that also records the item count and
// the JLH bytes of every write it is handed.
type sizesMemSink struct {
	*memSink
	mu     sync.Mutex
	writes [][2]int
}

func (s *sizesMemSink) Write(ctx context.Context, items []*envelope.Item) error {
	n := 0
	for _, it := range items {
		n += len(it.Env)
	}
	s.mu.Lock()
	s.writes = append(s.writes, [2]int{len(items), n})
	s.mu.Unlock()
	return s.memSink.Write(ctx, items)
}

func (s *sizesMemSink) got() [][2]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]int(nil), s.writes...)
}

// SPEC §8.1 step 2, §9.2 (user's decision, 2026-10-14; port of
// go-webhook-server, W 11): every path that writes a batch -- the dump's
// reingestion and the workers the assembly builds -- writes it in pieces under
// sink.batch_max_bytes.
func TestBootWritesInPiecesUnderBatchMaxBytes(t *testing.T) {
	_, env := testJLH(t, testID, "/d/0", `{"i":0}`)
	ceiling := fmt.Sprint(2*len(env) + 8) // two records of this size, not three

	check := func(t *testing.T, rs *sizesMemSink) {
		t.Helper()
		total := 0
		for _, w := range rs.got() {
			if w[0] > 2 {
				t.Errorf("a write of %d items (%d bytes) under a ceiling of %s", w[0], w[1], ceiling)
			}
			total += w[0]
		}
		if total != 5 {
			t.Errorf("wrote %d items in %v, want 5", total, rs.got())
		}
	}

	t.Run("dump", func(t *testing.T) {
		dir := t.TempDir()
		plantDump(t, dir, 5)
		rs := &sizesMemSink{memSink: &memSink{}}
		in := startHooked(t, dir, rs.memSink, func(a *App) { a.sinkOverride = rs },
			"--sink.batch_max_bytes="+ceiling)
		check(t, rs)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
	})
	// And the workers, which get the key from the assembly: five posts inside
	// one sink.batch_max_delay are one Dequeue, written in pieces.
	t.Run("workers", func(t *testing.T) {
		dir := t.TempDir()
		rs := &sizesMemSink{memSink: &memSink{}}
		in := startHooked(t, dir, rs.memSink, func(a *App) { a.sinkOverride = rs },
			"--sink.workers=1", "--sink.batch_max_delay=500ms", "--sink.batch_max_bytes="+ceiling)
		if ids := in.postN(5); len(ids) != 5 {
			t.Fatalf("accepted %d of 5", len(ids))
		}
		if !waitFor(5*time.Second, func() bool {
			n := 0
			for _, w := range rs.got() {
				n += w[0]
			}
			return n == 5
		}) {
			t.Fatalf("the workers wrote %v, want 5 items", rs.got())
		}
		check(t, rs)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d, want 0", code)
		}
	})
}
