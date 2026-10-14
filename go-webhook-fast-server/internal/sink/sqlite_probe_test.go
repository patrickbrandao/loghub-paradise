package sink

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// holdWriterLock takes the writer's lock, as a Write in progress does.
func holdWriterLock(s *SQLite) (release func()) {
	s.lock()
	return s.unlock
}

// SPEC §8.3.5 (W pendência 23): the probe waits for the writer's lock only
// within its own deadline. Busy writer and a last Write that ended without
// error: UP without touching the file. Busy writer and a last Write that
// failed, or none yet: DOWN, as before. Deterministic: the test holds the lock
// itself instead of racing a slow batch.
func TestSQLiteProbeDoesNotWaitForTheWriterPastItsDeadline(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

	// First, before any heartbeat: a good heartbeat is a good write too.
	t.Run("busy, no Write yet", func(t *testing.T) {
		err, took := probe(t)
		if took > time.Second {
			t.Fatalf("the probe waited %s for the writer, past its 200ms deadline", took)
		}
		if err == nil || IsFatal(err) {
			t.Fatalf("busy writer with no Write yet: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
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
		if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
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
		if err := s.Write(context.Background(), []*envelope.Item{badItem()}); err == nil {
			t.Fatal("control: the bad envelope was written")
		}
		err, took := probe(t)
		if took > time.Second {
			t.Fatalf("the probe waited %s for the writer, past its 200ms deadline", took)
		}
		if err == nil || IsFatal(err) {
			t.Fatalf("busy writer after a failed Write: %v (class %s), want a network failure (DOWN)", err, ClassOf(err))
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

// SPEC §8.3.5 (W §19.1 item 36): the batch in progress is proof of life only
// while it has held the lock for at most sink.op_timeout +
// sink.health_timeout. A volume stuck in I/O left COMMIT without return and
// without context: the lock taken for ever, the last Write "ok", and every
// probe UP -- /readyz and whs_sink_up lying with no deadline.
func TestSQLiteProbeSeesAWriterStuckPastItsLimit(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF",
		HeldLimit: 300 * time.Millisecond}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
		t.Fatal(err)
	}
	probe := func(heldFor time.Duration) error {
		release := holdWriterLock(s)
		defer release()
		s.writeStart.Store(int64(s.since() - heldFor)) // what Write records
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		return s.Health(ctx)
	}
	if err := probe(0); err != nil {
		t.Fatalf("control: a batch that just took the lock, after a good Write: %v, want UP", err)
	}
	if err := probe(time.Second); err == nil || IsFatal(err) {
		t.Fatalf("a batch holding the lock past op_timeout + health_timeout: %v (class %s), want a network failure (DOWN)",
			err, ClassOf(err))
	}
}

// SPEC §8.3.5 (W §19.1 item 37): a heartbeat that fails counts as a failed
// Write. The next probe, with the writer busy, read the last Write "ok" and
// went UP, and the UP in between reset the run the fuses of §10.2 count.
func TestSQLiteFailedHeartbeatCountsAsAFailedWrite(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
		t.Fatal(err)
	}
	// The heartbeat's table goes away: the next heartbeat fails, the file
	// still opens.
	if _, err := s.db.Exec(`DROP TABLE health`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Health(ctx); err == nil {
		t.Fatal("control: the heartbeat on a dropped table succeeded")
	}
	release := holdWriterLock(s)
	defer release()
	s.writeStart.Store(int64(s.since()))
	pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pcancel()
	if err := s.Health(pctx); err == nil {
		t.Fatal("busy writer after a failed heartbeat: UP, want DOWN")
	}
}

// The age of the batch in progress is read on the sink's own monotonic clock
// (since), never on the wall clock (review of the syslog family, 2026-10-14,
// A3): with a wall-clock reading an NTP step kept a stuck writer UP, or made a
// normal one DOWN, by the size of the step. The clock is injected, so the
// test sees which one Write records and Health compares.
func TestSQLiteProbeMeasuresTheBatchOnTheMonotonicClock(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF",
		HeldLimit: 300 * time.Millisecond}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var clock atomic.Int64
	clock.Store(int64(10 * time.Hour))
	s.since = func() time.Duration { return time.Duration(clock.Load()) }
	if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
		t.Fatal(err)
	}
	if got := time.Duration(s.writeStart.Load()); got != 10*time.Hour {
		t.Fatalf("Write recorded %v, want the monotonic reading 10h", got)
	}
	probe := func() error {
		release := holdWriterLock(s)
		defer release()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		return s.Health(ctx)
	}
	clock.Store(int64(10*time.Hour + 100*time.Millisecond))
	if err := probe(); err != nil {
		t.Fatalf("held 100ms on the monotonic clock: %v, want UP", err)
	}
	clock.Store(int64(10*time.Hour + time.Second))
	if err := probe(); err == nil {
		t.Fatal("held 1s on the monotonic clock, past the 300ms limit: UP, want DOWN")
	}
}

// A heartbeat that succeeds counts as a good Write (review of the syslog
// family, 2026-10-14, A4): otherwise, after a failed heartbeat and the
// database's recovery, the first slow batch made the probe DOWN with the file
// sound.
func TestSQLiteGoodHeartbeatClearsAFailedOne(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.db.Exec(`DROP TABLE health`); err != nil {
		t.Fatal(err)
	}
	if err := s.Health(ctx); err == nil {
		t.Fatal("control: the heartbeat on a dropped table succeeded")
	}
	if _, err := s.db.Exec(`CREATE TABLE health (id INTEGER PRIMARY KEY CHECK (id = 1), ts INTEGER NOT NULL) STRICT`); err != nil {
		t.Fatal(err)
	}
	if err := s.Health(ctx); err != nil {
		t.Fatalf("heartbeat after the recovery: %v", err)
	}
	release := holdWriterLock(s)
	defer release()
	s.writeStart.Store(int64(s.since()))
	pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pcancel()
	if err := s.Health(pctx); err != nil {
		t.Fatalf("busy writer after a good heartbeat: %v, want UP", err)
	}
}

// lateCtx is live when Health selects on it for the lock and expired from
// then on: the probe that gets the lock at the very end of its deadline, its
// heartbeat failing with the probe's own context error.
type lateCtx struct {
	context.Context
	calls atomic.Int32
	done  chan struct{}
}

func (c *lateCtx) Done() <-chan struct{} {
	if c.calls.Add(1) == 1 {
		return nil // the select for the lock: the lock wins
	}
	return c.done
}

func (c *lateCtx) Err() error {
	if c.calls.Load() <= 1 {
		return nil
	}
	return context.DeadlineExceeded
}

// A heartbeat cut by the probe's own deadline says nothing about the file
// (review of the RELP family, 2026-10-14, A1): recorded as a failed Write, it
// brought back the DOWN cycle for slowness the rule of §8.3.5 removed.
func TestSQLiteHeartbeatCutByItsDeadlineIsNotAFailedWrite(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []*envelope.Item{mkItem("ok")}); err != nil {
		t.Fatal(err)
	}
	late := &lateCtx{Context: context.Background(), done: make(chan struct{})}
	close(late.done)
	if err := s.Health(late); err == nil {
		t.Fatal("control: a heartbeat with its deadline gone succeeded")
	}
	release := holdWriterLock(s)
	defer release()
	s.writeStart.Store(int64(s.since()))
	pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer pcancel()
	if err := s.Health(pctx); err != nil {
		t.Fatalf("busy writer after a heartbeat cut by its own deadline: %v, want UP", err)
	}
}
