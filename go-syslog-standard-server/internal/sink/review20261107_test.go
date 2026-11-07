package sink

// Tests of the independent review of 2026-11-07 (SPEC §21.1, items 21-29)
// in this package: no boot attempt crosses the one sink.boot_timeout (item
// 25, SPEC §12.1); the constructor's name check without echo (item 26,
// SPEC §15.1).

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stuckHealthSink answers a health check only when its context ends.
type stuckHealthSink struct{ *fakeSink }

func (stuckHealthSink) Health(ctx context.Context) error {
	<-ctx.Done()
	return &Error{Class: ClassNetwork, Op: "health", Err: ctx.Err()}
}

// Each check of WaitReady gets min(sink.health_timeout, what is left of the
// boot deadline): one check with a 5 s health_timeout no longer runs 5 s past
// a 300 ms deadline.
func TestWaitReadyNoAttemptCrossesTheDeadline(t *testing.T) {
	h := NewHealth(stuckHealthSink{newFakeSink()}, HealthConfig{Timeout: 5 * time.Second}, discard(), metrics())
	start := time.Now()
	err := h.WaitReady(context.Background(), 300*time.Millisecond, 50*time.Millisecond, time.Second)
	if err == nil {
		t.Fatal("WaitReady passed a sink that never answers")
	}
	if el := time.Since(start); el > 800*time.Millisecond {
		t.Fatalf("WaitReady returned after %v with a 300ms deadline: one attempt crossed it", el)
	}
}

// The boot deadline reaches the SQL validation through the check's context:
// detached from the check's own deadline, it is still clamped to the boot's,
// and outside the boot (no deadline in the context) it keeps its own.
func TestValidationContextStopsAtTheBootDeadline(t *testing.T) {
	boot := time.Now().Add(200 * time.Millisecond)
	parent, cancel := context.WithTimeout(WithBootDeadline(context.Background(), boot), 10*time.Millisecond)
	defer cancel()
	vctx, vcancel := validationContext(parent, 5*time.Second)
	defer vcancel()
	d, ok := vctx.Deadline()
	if !ok || d.After(boot.Add(20*time.Millisecond)) {
		t.Fatalf("validation deadline %v, boot deadline %v: the validation runs past the boot", d, boot)
	}
	<-parent.Done() // the check's own deadline passes...
	if vctx.Err() != nil {
		t.Fatal("...and the validation followed it: it must only follow the boot's")
	}

	free, fcancel := validationContext(context.Background(), 5*time.Second)
	defer fcancel()
	if d, _ := free.Deadline(); time.Until(d) < 4*time.Second {
		t.Fatalf("outside the boot the validation got %v, want its own 5s", time.Until(d))
	}
}

// A writer that has held the lock past sink.op_timeout + sink.health_timeout
// is stuck, not busy: the probe fails instead of reporting it healthy forever
// (item 27, SPEC §10.4). The age is on the monotonic clock.
func TestSQLiteHealthReportsAStuckWriter(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF",
		StuckAfter: 150 * time.Millisecond}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.mu.Lock() // a Write in progress, from now
	s.stampWriteStart()
	s.writing.Store(true)
	probe := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		return s.Health(ctx)
	}
	if err := probe(); err != nil {
		t.Fatalf("Health = %v behind a writer busy for a moment", err)
	}
	time.Sleep(200 * time.Millisecond)
	err = probe()
	s.writing.Store(false)
	s.mu.Unlock() // before any Fatal: Close takes the lock
	if err == nil {
		t.Fatal("Health passed behind a writer stuck past StuckAfter")
	}
	if IsFatal(err) {
		t.Fatalf("a stuck writer is a network failure for the fuses, not fatal: %v", err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health after the writer let go: %v", err)
	}
}

// The constructor's check of the database name (for a caller that bypasses
// config) names what is wrong, never the name (SPEC §15.1).
func TestCheckQuotedNameDoesNotEchoTheName(t *testing.T) {
	const secret = "hunter2-S3CRET"
	for _, bad := range []string{secret + "`", secret + `\`, secret + "\x01", strings.Repeat(secret, 5)} {
		err := checkQuotedName("sink mysql: database", bad)
		if err == nil {
			t.Fatalf("%q accepted", bad)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the error echoes the name: %v", err)
		}
	}
}
