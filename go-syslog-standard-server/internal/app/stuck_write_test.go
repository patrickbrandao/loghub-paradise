package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"
)

// stuckWriteSink wraps the real sink=sqlite: from the second write on, Write
// never returns until release is closed, whatever its context says -- the
// shape of a COMMIT (or a rotation) stuck in fsync on a hung volume, neither
// of which takes a context (SPEC §10.4, "Ocupada tem limite"). With
// failHealth, Health fails as the real probe does for a writer that holds the
// lock past sink.op_timeout + sink.health_timeout, so the fuses take the sink
// to FATAL.
type stuckWriteSink struct {
	sink.Sink
	failHealth bool
	writes     atomic.Int64
	stuck      atomic.Bool
	release    chan struct{}
}

func (s *stuckWriteSink) Write(ctx context.Context, batch []sink.Prepared) error {
	if s.writes.Add(1) >= 2 {
		s.stuck.Store(true)
		<-s.release // the context is ignored on purpose
		return nil
	}
	return s.Sink.Write(ctx, batch)
}

func (s *stuckWriteSink) Health(ctx context.Context) error {
	if s.failHealth && s.stuck.Load() {
		return &sink.Error{Class: sink.ClassNetwork, Op: "heartbeat",
			Err: errors.New("the writer held the database past the probe's deadline (injected)")}
	}
	return s.Sink.Health(ctx)
}

// SPEC §12.3: a write that ignores the shutdown does not keep the dump from
// being written. The workers are waited for sink.op_timeout at most; past it
// the batch the stuck worker holds goes to the dump with the rest of the
// ring, and the process exits 3 -- on the FATAL path and on a SIGTERM's. With
// ring=sqlite nothing is dumped: the rows stay for the next boot. Before
// 2026-11-23 the shutdown waited in Workers.Wait for good: no dump, no exit,
// until the SIGKILL took the ring with nothing counting it (review of
// 2026-11-21, item 016).
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
			ss := &stuckWriteSink{failHealth: tc.fatal, release: make(chan struct{})}
			args := []string{
				"--ring.data_dir=" + dir,
				"--admin.bind=" + freeAddr(t),
				"--listen.udp=",
				"--listen.tcp=" + freeAddr(t),
				"--listen.relp=",
				"--ring.mode=" + tc.ring,
				"--sink.type=sqlite",
				"--sink.sqlite.synchronous=OFF",
				"--ring.sqlite.synchronous=OFF",
				"--sink.health_interval=50ms",
				"--sink.batch_max_delay=1ms",
				"--sink.max_retries=2",
				"--sink.fail_timeout=1s",
				"--sink.batch=4",
				"--sink.op_timeout=1s",
				"--shutdown.drain_timeout=1s",
				"--log.level=error",
				"--node.id=stuck-write",
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan int, 1)
			exited := make(chan struct{})
			appCh := make(chan *App, 1)
			go func() {
				defer close(exited)
				done <- runContext(ctx, args, func(a *App) {
					a.abort = make(chan os.Signal, 1)
					a.ready = make(chan struct{})
					a.wrapSink = func(s sink.Sink) sink.Sink { ss.Sink = s; return ss }
					appCh <- a
				})
			}()
			// Whatever happens in the test, the instance does not outlive it.
			t.Cleanup(func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(30 * time.Second):
				}
			})
			var a *App
			select {
			case a = <-appCh:
			case code := <-done:
				t.Fatalf("the instance exited during boot with code %d", code)
			case <-time.After(15 * time.Second):
				t.Fatal("the instance never reached the hook")
			}
			select {
			case <-a.Ready():
			case code := <-done:
				t.Fatalf("the instance exited before its transports opened, code %d", code)
			case <-time.After(15 * time.Second):
				t.Fatal("the transports never opened")
			}

			c, err := net.Dial("tcp", a.TransportAddr("tcp"))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 40; i++ {
				fmt.Fprintf(c, "<13>1 - h app - - - stuck-%04d\n", i)
			}
			c.Close()
			// Not necessarily all 40: once the sink is DOWN the transport may
			// stop reading. What was accepted is what must not be lost.
			if !waitFor(10*time.Second, ss.stuck.Load) {
				t.Fatal("no write got stuck")
			}

			start := time.Now()
			if !tc.fatal {
				cancel()
			}
			code := -1
			select {
			case code = <-done:
			case <-time.After(20 * time.Second):
			}
			took := time.Since(start)
			if code != ExitSink {
				t.Fatalf("exit %d after %s, want %d (-1: still running)", code, took.Round(time.Millisecond), ExitSink)
			}
			if took > 15*time.Second {
				t.Errorf("the shutdown took %s", took.Round(time.Millisecond))
			}

			accepted := int(a.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load())
			written := int(a.metrics.EventsWrittenTotal.Load())
			if written == 0 {
				t.Fatal("the first write delivered nothing: the test needs a delivered batch next to the stuck one")
			}
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "events.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var rows int
			if err := db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&rows); err != nil || rows != written {
				t.Errorf("events.db holds %d rows and the sink counted %d written (err %v)", rows, written, err)
			}
			dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "slstd-emg-*.bin"))
			if !tc.dump {
				if len(dumps) != 0 {
					t.Errorf("dumps %v with a persistent ring, want none: the rows stay for the next boot", dumps)
				}
			} else {
				if len(dumps) != 1 {
					t.Fatalf("dump files = %v, want 1", dumps)
				}
				res, err := emergency.NewStore(filepath.Join(dir, "emergency"), "stuck-write").Read(dumps[0])
				if err != nil || len(res.Items)+written != accepted || accepted < 8 {
					t.Fatalf("the dump holds %d and the sink wrote %d, of %d accepted (err %v)",
						len(res.Items), written, accepted, err)
				}
			}

			// The stuck write returns at last, successfully. Its batch is the
			// shutdown's now: the worker settles nothing -- no Commit, no
			// Release into the pool under the dump -- and the race detector
			// sees the late writer next to the dump's reads.
			close(ss.release)
			time.Sleep(200 * time.Millisecond)
		})
	}
}
