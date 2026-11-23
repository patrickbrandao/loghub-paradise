package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/sink"
)

// stuckWriteSink wraps the real PostgreSQL sink: from the second write on,
// Write never returns until release is closed, whatever its context says.
// The pgx write honours its context (it arms the socket's deadline from it),
// so this is the shape of a write that does not -- what a COMMIT stuck in
// fsync is in go-relplog-server's sink=sqlite -- and the shutdown may not
// depend on every write being well-behaved. With failHealth, Health fails
// once a write is stuck, as a database whose connections hang would, so the
// fuses of §13.3 take the sink to FATAL.
type stuckWriteSink struct {
	sink.Sink
	failHealth bool
	writes     atomic.Int64
	stuck      atomic.Bool
	release    chan struct{}
}

func (s *stuckWriteSink) Write(ctx context.Context, b *sink.Batch) error {
	if s.writes.Add(1) >= 2 {
		s.stuck.Store(true)
		<-s.release // the context is ignored on purpose
		return nil
	}
	return s.Sink.Write(ctx, b)
}

func (s *stuckWriteSink) Health(ctx context.Context) error {
	if s.failHealth && s.stuck.Load() {
		return &sink.Error{Class: sink.ClassNetwork, Op: "health",
			Err: errors.New("the database stopped answering (injected)")}
	}
	return s.Sink.Health(ctx)
}

// SPEC §13.4: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the chunks the stuck workers hold go to the dump with the rest of the ring,
// and the process exits 3 -- on the FATAL path (the fuses of §13.3) and on a
// SIGTERM's. Before 2026-11-23 the shutdown waited in Workers.Wait for good:
// no dump, no exit, until the SIGKILL took the ring with nothing counting it
// (review of 2026-11-21, item 016).
//
// RELP (§5.4): the 200 is sent once a message is in the ring, and the
// emitter forgets it. So every message answered 200 must reach the queue --
// through the dump and the next life -- and nothing is answered 500, which
// librelp takes as a final refusal: the messages the sink DOWN held are left
// unanswered, for the emitter to send again.
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fatal bool // the FATAL shutdown; otherwise a SIGTERM's
	}{
		{"fatal, ring=memory", true},
		{"sigterm, ring=memory", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			args := baseArgs(t, dir) // the two lives share the queue
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := launch(t, testID, append(append([]string(nil), args...),
				"--sink.workers=2", "--sink.max_retries=2", "--sink.fail_timeout=1s",
				"--sink.batch=4", "--sink.op_timeout=1s", "--shutdown.drain_timeout=1s"),
				func(s sink.Sink) sink.Sink { ss.Sink = s; return ss })
			released := false
			t.Cleanup(func() {
				if !released {
					close(ss.release)
				}
				// The shutdown leaves the sink unclosed when a worker never
				// returned (closeSink); the pool is closed here. Close is
				// idempotent.
				if ss.Sink != nil {
					_ = ss.Sink.Close()
				}
			})
			in.waitReady()

			const n = 40
			lines := make([]string, n)
			for i := range lines {
				lines[i] = fmt.Sprintf("<13>1 - h app - - - stuck-%04d", i)
			}
			// One session, every frame on the wire at once (40 < relp.window);
			// the answers are read aside, until the session ends: with the
			// sink DOWN the server holds what it has not answered, and the
			// shutdown closes the session.
			rc := dialRELP(t, in.app.TransportAddr("tcp"))
			ids := rc.syslog(lines...)
			answers := make(chan []string, 1)
			go func() {
				got, _ := rc.responsesErr(ids)
				answers <- got
			}()
			waitFor(t, 10*time.Second, "a write stuck", ss.stuck.Load)

			start := time.Now()
			var code int
			if tc.fatal {
				code = in.waitExit(20 * time.Second)
			} else {
				code = in.stop()
			}
			took := time.Since(start)
			if code != ExitSink {
				t.Fatalf("exit %d after %s, want %d", code, took.Round(time.Millisecond), ExitSink)
			}
			if took > 15*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}

			var got []string
			select {
			case got = <-answers:
			case <-time.After(30 * time.Second):
				t.Fatal("the RELP session outlived the process")
			}
			var ok []string
			for i, body := range got {
				switch body {
				case relpRSPOK:
					ok = append(ok, fmt.Sprintf("stuck-%04d", i))
				case "":
				default:
					t.Errorf("message %d answered %q: librelp destroys it, and only the 200s may be answered here", i, body)
				}
			}

			accepted := int(in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load())
			written := int(in.app.metrics.EventsWrittenTotal.Load())
			t.Logf("accepted %d, answered 200 %d, written %d; shutdown %s",
				accepted, len(ok), written, took.Round(time.Millisecond))
			if len(ok) > accepted || accepted < 8 {
				t.Fatalf("%d answered 200 for %d accepted", len(ok), accepted)
			}
			dumps := dumpFiles(t, in.emgDir(), testID.From)
			if len(dumps) != 1 {
				t.Fatalf("dump files = %v, want 1", dumps)
			}
			dumped := dumpLines(t, dumps[0])
			if len(dumped)+written != accepted {
				t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted",
					len(dumped), written, accepted)
			}

			// The stuck writes return at last, successfully. Their chunks are
			// the shutdown's now: the workers settle nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writers next to the dump's reads.
			close(ss.release)
			released = true
			time.Sleep(200 * time.Millisecond)

			// The next life reingests the dump: every accepted message is in
			// the queue, once.
			next := launch(t, testID, args, nil)
			next.waitReady()
			rows := next.waitRows(accepted)
			if len(rows) != accepted {
				t.Errorf("%d rows for %d accepted messages", len(rows), accepted)
			}
			envs := byMessage(t, rows)
			for _, m := range ok {
				if _, found := envs[m]; !found {
					t.Errorf("%s was answered 200 and never delivered", m)
				}
			}
			if code := next.stop(); code != ExitOK {
				t.Errorf("the next life exited %d", code)
			}
			if t.Failed() {
				t.Logf("answered 200: %s", strings.Join(ok, " "))
			}
		})
	}
}
