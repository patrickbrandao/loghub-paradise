package sink

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// holdWriterLock takes the writer's lock and stamps its start, as a chunk in
// progress does.
func holdWriterLock(s *SQLite) (release func()) {
	s.lock()
	s.stampWriteStart()
	return s.unlock
}

// SPEC §10.4 (decisão do usuário, 2026-10-14): the probe waits for the
// writer's lock only within its own deadline. Busy writer and a last Write
// that ended without error: UP without touching the file. Busy writer and a
// last Write that failed, or none yet: DOWN, as before. Deterministic: the
// test holds the lock itself instead of racing a slow chunk.
func TestSQLiteProbeDoesNotWaitForTheWriterPastItsDeadline(t *testing.T) {
	s, _, _ := newSQLiteSink(t, "none")

	probe := func(t *testing.T) (error, time.Duration) {
		t.Helper()
		release := holdWriterLock(s)
		done := make(chan struct{})
		go func() {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
			release()
		}()
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := s.Health(ctx)
		return err, time.Since(start)
	}

	t.Run("busy, no Write nor heartbeat yet", func(t *testing.T) {
		// First: a good heartbeat counts as a good Write (review A4).
		err, took := probe(t)
		if took > time.Second {
			t.Fatalf("the probe waited %s for the writer, past its 200ms deadline", took)
		}
		if err == nil || IsFatal(err) {
			t.Fatalf("busy writer with no Write nor heartbeat yet: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
		}
	})
	t.Run("lock free", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Health(ctx); err != nil {
			t.Fatalf("control: Health with the lock free: %v", err)
		}
	})
	t.Run("busy, last Write ok", func(t *testing.T) {
		if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
			t.Fatal(err)
		}
		err, took := probe(t)
		if took > time.Second {
			t.Fatalf("the probe waited %s for the writer, past its 200ms deadline", took)
		}
		if err != nil {
			t.Fatalf("busy writer after a good Write: %v, want UP", err)
		}
	})
	t.Run("busy, last Write failed", func(t *testing.T) {
		// Any error counts, whatever its class: a chunk cut by its own
		// context is as much a failed Write as a full disk.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.Write(ctx, batchOf(t, line3164)); err == nil {
			t.Fatal("control: a Write with a cancelled context succeeded")
		}
		err, took := probe(t)
		if took > time.Second {
			t.Fatalf("the probe waited %s for the writer, past its 200ms deadline", took)
		}
		if err == nil || IsFatal(err) {
			t.Fatalf("busy writer after a failed Write: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
		}
	})
	t.Run("busy past op_timeout + health_timeout", func(t *testing.T) {
		// Review A1: a writer stuck in a COMMIT or a rotation, which take no
		// context, held the lock forever while the chunk BEFORE it, good,
		// kept the probe UP. The chunk in progress counts as busy only for
		// sink.op_timeout + sink.health_timeout.
		st, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"),
			Synchronous: "OFF", StuckAfter: 300 * time.Millisecond}, discard(), metrics())
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if err := st.Write(context.Background(), batchOf(t, line5424)); err != nil {
			t.Fatal(err)
		}
		release := holdWriterLock(st) // the next chunk takes the lock...
		defer release()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := st.Health(ctx); err != nil {
			t.Fatalf("control: busy for less than StuckAfter after a good Write: %v, want UP", err)
		}
		cancel()
		time.Sleep(400 * time.Millisecond) // ...and never lets it go
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err = st.Health(ctx)
		if err == nil || IsFatal(err) {
			t.Fatalf("writer holding the lock past StuckAfter: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
		}
	})
	t.Run("lock free again", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Health(ctx); err != nil {
			t.Fatalf("Health with the lock free: %v", err)
		}
	})
}

// Review A2: a heartbeat that took the lock and failed counts as a failed
// Write, so the next probe, finding the writer busy, does not flip back to UP
// on the good chunk from before and reset the fuses' count (SPEC §10.4).
func TestSQLiteFailedHeartbeatCountsAsAFailedWrite(t *testing.T) {
	s, _, _ := newSQLiteSink(t, "none")
	if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
		t.Fatal(err)
	}
	_ = s.db.Close() // the file goes away underneath the open sink
	if err := s.Health(context.Background()); err == nil {
		t.Fatal("control: the heartbeat passed on a closed database")
	}
	release := holdWriterLock(s)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Health(ctx); err == nil || IsFatal(err) {
		t.Fatalf("busy writer after a failed heartbeat: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
	}
}

// Review A3 (2026-10-14): the age of the chunk in progress is measured on the
// monotonic clock. With the wall clock, an NTP step backwards while a writer
// was stuck gave a negative age and kept the sink UP for the size of the
// step; a step forwards made a normal writer DOWN. The test steps the wall
// clock the probe would read (wallClock) and expects neither to matter.
func TestSQLiteProbeAgeIgnoresWallClockSteps(t *testing.T) {
	newSink := func(t *testing.T) *SQLite {
		st, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"),
			Synchronous: "OFF", StuckAfter: 300 * time.Millisecond}, discard(), metrics())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		if err := st.Write(context.Background(), batchOf(t, line5424)); err != nil {
			t.Fatal(err)
		}
		return st
	}
	probe := func(st *SQLite) error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		return st.Health(ctx)
	}
	step := func(t *testing.T, d time.Duration) {
		wallClock = func() time.Time { return time.Now().Add(d) }
		t.Cleanup(func() { wallClock = time.Now })
	}

	t.Run("step back while stuck", func(t *testing.T) {
		st := newSink(t)
		release := holdWriterLock(st) // a chunk takes the lock and never lets go
		defer release()
		time.Sleep(400 * time.Millisecond)
		step(t, -time.Hour)
		if err := probe(st); err == nil {
			t.Fatal("writer stuck past StuckAfter, wall clock stepped back 1h: UP, want DOWN")
		}
	})
	t.Run("step forward while busy", func(t *testing.T) {
		st := newSink(t)
		release := holdWriterLock(st) // a chunk just took the lock
		defer release()
		step(t, time.Hour)
		if err := probe(st); err != nil {
			t.Fatalf("writer busy for ~0s, wall clock stepped forward 1h: %v, want UP", err)
		}
	})
}

// Review A4 (2026-10-14): a heartbeat that took the lock and wrote the file is
// the same proof as a good chunk. After a failed heartbeat (a full disk, freed
// later) the next good one clears the mark; without that, the first chunk
// longer than health_timeout made the probe that found it busy DOWN with
// everything sound.
func TestSQLiteGoodHeartbeatClearsAFailedWrite(t *testing.T) {
	s, _, _ := newSQLiteSink(t, "none")
	if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
		t.Fatal(err)
	}
	_ = s.db.Close() // the file goes away underneath the open sink...
	if err := s.Health(context.Background()); err == nil {
		t.Fatal("control: the heartbeat passed on a closed database")
	}
	if err := s.openPeriod(s.period); err != nil { // ...and comes back
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("control: heartbeat on the reopened database: %v", err)
	}
	release := holdWriterLock(s)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Health(ctx); err != nil {
		t.Fatalf("busy writer after a good heartbeat: %v, want UP", err)
	}
}

// Review A1 of the RELP family (2026-10-14): the probe can take the lock just
// before its deadline, and the heartbeat then fails with the probe's own
// context error. That says nothing about the file: the probe reports it, but
// does not mark the last Write as failed, or every following probe that finds
// the writer busy is DOWN until the next chunk ends.
func TestSQLiteHeartbeatCutByItsOwnContextIsNotAFailedWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		mk   func() (context.Context, context.CancelFunc)
	}{
		{"deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 50*time.Millisecond)
		}},
		{"cancelled", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := newSQLiteSink(t, "none")
			if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := tc.mk()
			defer cancel()
			afterProbeLock = func() { // the lock came at deadline - epsilon
				cancel()
				<-ctx.Done()
			}
			err := s.Health(ctx)
			afterProbeLock = nil
			if err == nil {
				t.Fatal("control: a heartbeat with an expired context succeeded")
			}
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				t.Fatalf("control: the heartbeat failed for another reason: %v", err)
			}
			release := holdWriterLock(s)
			defer release()
			pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer pcancel()
			if err := s.Health(pctx); err != nil {
				t.Fatalf("busy writer after a heartbeat cut by its own context: %v, want UP", err)
			}
		})
	}
}
