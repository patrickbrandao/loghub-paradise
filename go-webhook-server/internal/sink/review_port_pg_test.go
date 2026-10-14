package sink

// Port of the SQL corrections of the syslog/RELP family (2026-10-13) that need
// a real PostgreSQL: SPEC §8.5.5 (INSERT/UPDATE rules), §8.5.7 (23502 and
// 0A000 fatal) and §8.5.8 (the health check's own connection). They skip
// without WHS_TEST_PG_DSN, like the rest of postgres_integration_test.go.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// INSERT ... ON CONFLICT is refused on a table with an INSERT or UPDATE rule
// (0A000). The boot let such a table through, and every batch then failed --
// as network, forever, with the sink UP and /readyz green (ported from
// go-relplog-pg-server via go-syslog-pg-server).
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
				if err := p.Write(ctx, mkBatch(2)); err != nil {
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
	err := p.Write(context.Background(), mkBatch(1))
	if !IsFatal(err) || !strings.Contains(err.Error(), "0A000") {
		t.Fatalf("Write on a table with a rule = %v (class %s), want fatal 0A000", err, ClassOf(err))
	}
}

// A 23502 after the boot is the table -- here a required column added after
// the validation -- and fatal, not a logic drop of every batch (ported from
// go-syslog-pg-server, 2026-10-13).
func TestPostgresNotNullViolationIsFatal(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("ALTER TABLE " + f.table + " ADD COLUMN tenant text NOT NULL")
	err := p.Write(context.Background(), mkBatch(2))
	if err == nil {
		t.Fatal("Write succeeded into a table with a required column it never fills")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "23502") {
		t.Fatalf("Write = %v (class %s), want fatal with sqlstate 23502", err, ClassOf(err))
	}
}

// SPEC §8.5.8: the health check has a connection of its own, outside the
// pool. Writers that hold every pooled connection -- slow, not failing, here
// stuck behind a lock -- left a check inside the pool waiting past
// sink.health_timeout: DOWN, then FATAL with nothing wrong. pool = writers is
// what a ring gets with pool_size = sink.workers, and what ring=none gets
// whenever ring.max_inflight handlers write at once, which no pool_size the
// configuration demands prevents (independent review of the port to
// go-syslog-server, 2026-10-13).
func TestPostgresHealthHasItsOwnConnection(t *testing.T) {
	for _, tc := range []struct{ pool, writers int }{{1, 1}, {3, 3}} {
		t.Run(fmt.Sprintf("pool%d_writers%d", tc.pool, tc.writers), func(t *testing.T) {
			f := pgSetup(t)
			cfg := f.cfg
			cfg.PoolSize = tc.pool
			p := f.sink(cfg)
			f.ready(p)
			ctx := context.Background()
			tx, err := f.conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "LOCK TABLE "+f.table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			herr := slowWritersThenHealth(t, tc.writers,
				func(ctx context.Context) error { return p.Write(ctx, mkBatch(3)) },
				p.Health, func() { _ = tx.Rollback(ctx) })
			if herr != nil {
				t.Fatalf("pool_size=%d with %d writers holding it: health = %v, want nil -- the check "+
					"waited for a pooled connection", tc.pool, tc.writers, herr)
			}
		})
	}
}

// The health check's connection may die between two checks (a server
// restart, a proxy's idle timeout). The pool used to hide that by pinging an
// idle connection before reuse; the check's own connection is redialled once
// within the same deadline instead of reporting one failed check.
func TestPostgresHealthRedialsADeadConnection(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.AppName = fmt.Sprintf("whs-probe-%d", time.Now().UnixNano())
	p := f.sink(cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Every connection of this sink is gone: the pool's and the check's.
	var killed int
	if err := f.conn.QueryRow(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE application_name = $1`, cfg.AppName).Scan(&killed); err != nil {
		t.Fatal(err)
	}
	if killed == 0 {
		t.Fatal("no connection of the sink found to terminate")
	}
	waitFor := time.Now().Add(5 * time.Second)
	for {
		var left int
		if err := f.conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
			cfg.AppName).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 || time.Now().After(waitFor) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	hctx, hcancel := context.WithTimeout(context.Background(), time.Second)
	defer hcancel()
	if err := p.Health(hctx); err != nil {
		t.Fatalf("Health after the server closed its connection = %v, want nil (one redial)", err)
	}
}

// slowWritersThenHealth starts writers concurrent writes that block until
// release, runs one health check under a one-second deadline (the default
// sink.health_timeout) while they hold their connections, then releases them
// and fails the test if any write failed. It returns the check's error.
func slowWritersThenHealth(t *testing.T, writers int, write func(ctx context.Context) error,
	health func(ctx context.Context) error, release func()) error {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			errs <- write(ctx)
		}()
	}
	time.Sleep(400 * time.Millisecond)
	hctx, cancel := context.WithTimeout(context.Background(), time.Second)
	herr := health(hctx)
	cancel()
	release()
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatalf("a slow write failed: %v", e)
		}
	}
	return herr
}
