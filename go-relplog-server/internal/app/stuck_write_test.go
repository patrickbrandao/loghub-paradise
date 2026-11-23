package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// stuckWriteSink wraps the real sink=sqlite: from the second write on, Write
// never returns until release is closed, whatever its context says -- the
// shape of a COMMIT (or a rotation, or the os.Stat of updateSizeGauge) stuck
// in fsync on a hung volume, none of which takes a context (SPEC §10.4, review
// A1), and of a NATS publish before its socket had a deadline (§10.7). With
// failHealth, Health fails as the real probe does for a writer that holds the
// database past StuckAfter, so the fuses take the sink to FATAL.
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
		return &sink.Error{Class: sink.ClassNetwork, Op: "heartbeat",
			Err: errors.New("the writer held the database past the probe's deadline (injected)")}
	}
	return s.Sink.Health(ctx)
}

func (s *stuckWriteSink) NeedsColumns() bool { return true }
func (s *stuckWriteSink) MaxWorkers() int    { return 1 }

// SPEC §13.4: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the chunk the stuck worker holds goes to the dump with the rest of the
// ring, and the process exits 3 -- on the FATAL path (the A1 fuse of §10.4)
// and on a SIGTERM's. With a persistent ring nothing is dumped: the rows stay
// for the next boot. Before 2026-11-23 the shutdown waited in Workers.Wait for
// good: no dump, no exit, until the SIGKILL took the ring with nothing
// counting it (review of 2026-11-21, item 016).
//
// RELP (§5.4): the 200 is sent once a message is in the ring, and the
// emitter forgets it. So every message answered 200 must reach the
// destination -- through the dump, or the persistent ring, and the next life
// -- and nothing is answered 500, which librelp takes as a final refusal: the
// messages the sink DOWN held are left unanswered, for the emitter to send
// again.
func TestShutdownDumpsWhileAWriteIgnoresIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ring  string
		fatal bool
		dump  bool
	}{
		{"fatal, ring=memory", "memory", true, true},
		{"sigterm, ring=memory", "memory", false, true},
		{"fatal, ring=sqlite", "sqlite", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			args := append(baseArgs(dir), "--ring.mode="+tc.ring)
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			in := launch(t, testID, append(append([]string(nil), args...),
				"--sink.max_retries=2", "--sink.fail_timeout=1s",
				"--sink.batch=4", "--sink.op_timeout=1s", "--shutdown.drain_timeout=1s"),
				func(s sink.Sink) sink.Sink { ss.Sink = s; return ss })
			released := false
			t.Cleanup(func() {
				if !released {
					close(ss.release)
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
			dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
			if !tc.dump {
				if len(dumps) != 0 {
					t.Errorf("dumps %v with a persistent ring, want none: the rows stay for the next boot", dumps)
				}
			} else {
				if len(dumps) != 1 {
					t.Fatalf("dump files = %v, want 1", dumps)
				}
				res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(dumps[0])
				if err != nil || len(res.Items)+written != accepted {
					t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted (err %v)",
						len(res.Items), written, accepted, err)
				}
			}

			// The stuck write returns at last, successfully. Its chunk is the
			// shutdown's now: the worker settles nothing -- no Commit, which
			// would delete ring=sqlite rows the next life needs, no Release
			// into the pool under the dump -- and the race detector sees the
			// late writer next to the dump's reads. The first life left its
			// sink open (closeSink); it is closed here, for the next one.
			close(ss.release)
			released = true
			time.Sleep(200 * time.Millisecond)
			_ = ss.Sink.Close()

			// The next life delivers every accepted message, once: the dump
			// reingested, or the ring's rows drained.
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
