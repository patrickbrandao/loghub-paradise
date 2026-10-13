package sink

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// D1 of the independent review of go-syslog-server (2026-10-13): there, with
// ring=none, the writers are the receivers -- up to ring.max_inflight -- and
// pool_size >= workers + 1 left the health probe without a connection. Here the
// ring is memory only, so the writers are sink.workers, each with one Write at
// a time (Workers; the reingestion runs before them and the probe, with
// Instrumented.Deliver), and the probe is one more: workers + 1 is enough
// (SPEC §10.5). Proved against a real PostgreSQL: every worker's Write stalled
// on a lock of the queue, holding its connection, and the probe still answers
// within sink.health_timeout. The control, pool_size = workers (refused by the
// config), starves it: the test can see the difference.
func TestPoolLeavesTheProbeAConnection(t *testing.T) {
	const workers = 3
	for _, tc := range []struct {
		pool    int
		starved bool
	}{{workers + 1, false}, {workers, true}} {
		t.Run(fmt.Sprintf("pool%d_workers%d", tc.pool, workers), func(t *testing.T) {
			f := pgSetup(t)
			cfg := f.cfg
			cfg.PoolSize = tc.pool
			// Its own name: the lock waits counted below are this sink's only.
			cfg.AppName = fmt.Sprintf("probe-%d", time.Now().UnixNano())
			p := f.sink(cfg)
			f.ready(p)
			ctx := context.Background()
			tx, err := f.conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, "LOCK TABLE "+f.table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, workers)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()
					errs <- p.Write(wctx, batchN(t, 3))
				}()
			}
			// Every Write holds its connection, waiting on the lock. f.conn is
			// inside the locking transaction, where pg_stat_activity is a
			// snapshot taken once: it is cleared on every poll.
			deadline := time.Now().Add(10 * time.Second)
			for {
				var waiting int
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity, pg_stat_clear_snapshot()
					WHERE wait_event_type = 'Lock' AND application_name = $1`, cfg.AppName).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting == workers {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("%d of %d writes waiting on the lock", waiting, workers)
				}
				time.Sleep(20 * time.Millisecond)
			}
			hctx, cancel := context.WithTimeout(ctx, time.Second) // sink.health_timeout
			herr := p.Health(hctx)
			cancel()
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatalf("a stalled write failed once released: %v", e)
				}
			}
			if starved := herr != nil; starved != tc.starved {
				t.Fatalf("probe error %v; want starved=%v", herr, tc.starved)
			}
		})
	}
}
