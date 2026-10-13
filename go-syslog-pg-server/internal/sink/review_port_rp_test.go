package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// Ported from go-relplog-pg-server (review_pg_test.go there): the half of the
// former SPEC §22.1 item 6 (the boot validation under sink.op_timeout, §10.5)
// that needs no server. The half that does is in review_port_rp_pg_test.go.

// ---------------------------------------------------------------- item 6

// The validation runs under sink.op_timeout, not under the deadline of the
// health check that triggers it: the check's deadline passing must leave it
// running.
func TestValidationContextOutlivesTheCheckDeadline(t *testing.T) {
	check, cancelCheck := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Second)
	defer stop()

	<-check.Done()
	select {
	case <-vctx.Done():
		t.Fatalf("validation ended with the health check's deadline: %v", vctx.Err())
	case <-time.After(50 * time.Millisecond):
	}
	if dl, ok := vctx.Deadline(); !ok || time.Until(dl) > time.Second {
		t.Errorf("validation deadline = %v (set %v), want at most sink.op_timeout from now", dl, ok)
	}
}

func TestValidationContextHasItsOwnDeadline(t *testing.T) {
	vctx, stop := validationContext(context.Background(), 20*time.Millisecond)
	defer stop()
	select {
	case <-vctx.Done():
		if !errors.Is(vctx.Err(), context.DeadlineExceeded) {
			t.Errorf("err = %v, want DeadlineExceeded", vctx.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink.op_timeout never ended the validation")
	}
}

// A boot cancelled while the check's own window is still open reaches the
// validation, with or without a lifetime in the context.
func TestValidationContextFollowsACancelledBoot(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	check, cancelCheck := context.WithTimeout(boot, time.Hour)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled boot did not cancel the validation")
	}
}

// A boot cancelled AFTER the check's deadline has passed: the check context
// can no longer say so, the lifetime it carries can. Health.WaitReady and
// Health.check must put it there (withLifetime) for this to work in the app.
func TestValidationContextFollowsABootCancelledAfterTheCheckDeadline(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	defer cancelBoot()
	check, cancelCheck := context.WithTimeout(withLifetime(boot), 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	<-check.Done()
	time.Sleep(20 * time.Millisecond)
	if vctx.Err() != nil {
		t.Fatalf("the check's deadline ended the validation: %v", vctx.Err())
	}
	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a boot cancelled after the check deadline never reached the validation")
	}
}

// lifetimeSink records the lifetime each health check carries.
type lifetimeSink struct {
	mu   sync.Mutex
	seen []context.Context
}

func (s *lifetimeSink) Write(context.Context, *Batch) error { return nil }
func (s *lifetimeSink) Close() error                        { return nil }
func (s *lifetimeSink) Type() string                        { return "lifetime" }

func (s *lifetimeSink) Health(ctx context.Context) error {
	life, _ := ctx.Value(lifetimeKey{}).(context.Context)
	s.mu.Lock()
	s.seen = append(s.seen, life)
	s.mu.Unlock()
	return nil
}

// Both callers of Sink.Health -- the boot's WaitReady and the periodic check
// -- hand the sink the context they were given as its lifetime, or a
// validation running past the check's deadline could not see the boot being
// cancelled (validationContext, SPEC §10.5).
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
