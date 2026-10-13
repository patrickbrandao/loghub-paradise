package sink

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// lifetimeSink records the lifetime each health check carries.
type lifetimeSink struct {
	None
	mu   sync.Mutex
	seen []context.Context
}

func (s *lifetimeSink) Health(ctx context.Context) error {
	life, _ := ctx.Value(lifetimeKey{}).(context.Context)
	s.mu.Lock()
	s.seen = append(s.seen, life)
	s.mu.Unlock()
	return nil
}

// Both callers of Sink.Health -- the boot's WaitReady and the periodic check
// -- hand the sink the context they were given as its lifetime, or a boot
// validation running past the check's deadline could not see the boot being
// cancelled (validationContext, SPEC §10.5). Ported from go-relplog-server
// (review_sql_test.go there).
func TestHealthChecksCarryTheirLifetime(t *testing.T) {
	s := &lifetimeSink{}
	h := NewHealth(s, HealthConfig{Interval: time.Hour, Timeout: time.Second}, discard(), obs.NewMetrics(obs.Info{}))
	boot, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := h.WaitReady(boot, time.Second, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	h.check(boot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) != 2 {
		t.Fatalf("%d health checks, want 2", len(s.seen))
	}
	for i, life := range s.seen {
		if life != boot {
			t.Errorf("check %d carried lifetime %v, want the caller's context", i, life)
		}
	}
}
