package sink

// Regression tests for the health probe: a permanent refusal answered to the
// probe is FATAL at the first check, without waiting for the fuses (SPEC
// §10.2), and a probe that outlives sink.health_timeout is counted as
// kind="timeout", apart from the network refusing (SPEC §11.1).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
)

// probeSink answers Health with a scripted error, or blocks until the probe's
// context expires.
type probeSink struct {
	err   error
	block bool
}

func (p *probeSink) Write(context.Context, []*envelope.Item) error { return nil }
func (p *probeSink) Health(ctx context.Context) error {
	if p.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.err
}
func (p *probeSink) Close() error { return nil }
func (p *probeSink) Type() string { return "probe" }

// SPEC §8.5.7: a permanent refusal answered to the PROBE -- a revoked grant,
// a dropped table -- is FATAL at the first check. The fuses exist for a destination that is unreachable, not for one
// that answered "never".
func TestHealthFatalReplyIsFatalAtOnce(t *testing.T) {
	m := testMetrics()
	refusal := fatalErr("validate", errors.New("table loghub.inbox does not exist"))
	h := NewHealth(&probeSink{err: refusal}, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: time.Second,
		MaxRetries: 1000, FailAfter: time.Hour, // neither fuse can be what trips
	}, quietLog(), m)

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
		t.Error("whs_sink_up still 1 after a fatal probe")
	}
}

// SPEC §11.1: a probe that outlives sink.health_timeout is a "slow" incident,
// counted as kind="timeout" like a slow write, not as the network refusing.
func TestHealthProbeTimeoutIsCountedApart(t *testing.T) {
	m := testMetrics()
	h := NewHealth(&probeSink{block: true}, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 10 * time.Millisecond,
		MaxRetries: 1000, FailAfter: time.Hour,
	}, quietLog(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for m.SinkErrorsTotal[obs.SinkErrTimeout].Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a probe that timed out was never counted as kind=timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 0 {
		t.Errorf("kind=network counted %d for probes that only timed out", got)
	}
}
