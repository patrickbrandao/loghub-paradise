package sink

// Regression tests of go-relplog-pg-server's independent review of 2026-10-13
// against the real PostgreSQL, ported (TESTING §9). They skip without
// SYSLOG_TEST_PG_DSN.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// K2: INSERT ... ON CONFLICT is refused on a table with an INSERT or UPDATE
// rule (0A000). The boot let such a table through, and every chunk then
// failed -- as network, forever, with the sink UP and /readyz green.
func TestPostgresRulesAreRefusedAtBoot(t *testing.T) {
	f := pgSetup(t)
	for _, tc := range []struct{ name, alter, want string }{
		{"INSERT rule", "CREATE RULE inbox_also AS ON INSERT TO $T DO ALSO NOTIFY inbox_also", "inbox_also"},
		{"UPDATE rule", "CREATE RULE inbox_upd AS ON UPDATE TO $T DO ALSO NOTIFY inbox_upd", "inbox_upd"},
		{"DELETE rule is tolerated", "CREATE RULE inbox_del AS ON DELETE TO $T DO ALSO NOTIFY inbox_del", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Table = f.newTable(tc.alter)
			p := f.sink(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := p.Health(ctx)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Health: %v", err)
				}
				if err := p.Write(ctx, batchN(t, 2)); err != nil {
					t.Fatalf("Write: %v", err)
				}
				return
			}
			if !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Health = %v (class %s), want fatal naming %s", err, ClassOf(err), tc.want)
			}
		})
	}
	// A rule created AFTER the boot: the write is 0A000, and fatal.
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("CREATE RULE inbox_late AS ON INSERT TO " + f.table + " DO ALSO NOTIFY inbox_late")
	err := p.Write(context.Background(), batchN(t, 1))
	if !IsFatal(err) || !strings.Contains(err.Error(), "0A000") {
		t.Fatalf("Write on a table with a rule = %v (class %s), want fatal 0A000", err, ClassOf(err))
	}
}

// K1, SPEC §10.5: the health probe takes its connection out of the pool the
// workers write through. With pool_size = workers, slow but working writes held
// every connection and the probe timed out: DOWN, then FATAL, with nothing
// wrong. With workers + 1 (the rule the configuration now enforces) the probe
// always has one. Shown with one worker, its INSERT stuck behind a lock.
func TestPostgresHealthHasAConnectionWhileTheWorkersWrite(t *testing.T) {
	for _, tc := range []struct {
		pool    int
		starved bool
	}{{1, true}, {2, false}} {
		f := pgSetup(t)
		cfg := f.cfg
		cfg.PoolSize = tc.pool
		p := f.sink(cfg)
		f.ready(p)
		tx, err := f.conn.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(context.Background(), "LOCK TABLE "+f.table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			done <- p.Write(ctx, batchN(t, 3)) // the worker: slow, not failing
		}()
		time.Sleep(200 * time.Millisecond)
		hctx, cancel := context.WithTimeout(context.Background(), time.Second) // sink.health_timeout
		herr := p.Health(hctx)
		cancel()
		_ = tx.Rollback(context.Background())
		if werr := <-done; werr != nil {
			t.Fatalf("pool %d: the slow write failed: %v", tc.pool, werr)
		}
		if starved := herr != nil; starved != tc.starved {
			t.Fatalf("pool_size=%d, one worker writing: health = %v, want starved=%v", tc.pool, herr, tc.starved)
		}
	}
}
