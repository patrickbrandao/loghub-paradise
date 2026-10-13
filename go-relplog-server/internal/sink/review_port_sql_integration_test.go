package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Findings of the reviews of go-relplog-pg-server and go-syslog-server
// (2026-10-13) that need the real SQL servers (RELPLOG_TEST_PG_DSN,
// RELPLOG_TEST_MYSQL_DSN, RELPLOG_TEST_CLICKHOUSE_DSN; each skips without
// its own).

// INSERT ... ON CONFLICT is refused on a table with an INSERT or UPDATE rule
// (0A000). The boot let such a table through, and every chunk then failed --
// as network, forever, with the sink UP and /readyz green (found in the
// review of go-relplog-pg-server, 2026-10-13).
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

// ------------------------------------- health probe outside the pool (D1)

// starveThePool runs writers slow but working writes at once, each holding a
// pool connection until release, and runs the health check while they all
// wait: the check must answer within sink.health_timeout (1 s here). With
// the probe in the pool, pool_size = writers left it no connection -- and
// with ring=none the writers are up to ring.max_inflight, so no "+1" in the
// pool protects it either: DOWN and then FATAL by the fuses with the
// database healthy (go-relplog-pg-server and go-syslog-server reviews,
// 2026-10-13). The probe has its own connection now, on all three drivers.
func starveThePool(t *testing.T, writers int, write func(ctx context.Context) error,
	health func(ctx context.Context) error, release func()) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- write(ctx)
		}()
	}
	time.Sleep(500 * time.Millisecond) // every writer holds its connection
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
	if herr != nil {
		t.Fatalf("the health check with every pool connection held by a slow write = %v, want UP", herr)
	}
}

func TestPostgresHealthProbeOutsideThePool(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("pool%d_writers%d", n, n), func(t *testing.T) {
			f := pgSetup(t)
			cfg := f.cfg
			cfg.PoolSize = n
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
			starveThePool(t, n, func(ctx context.Context) error { return p.Write(ctx, batchN(t, 3)) },
				p.Health, func() { _ = tx.Rollback(ctx) })
		})
	}
}

func TestMySQLHealthProbeOutsideThePool(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("pool%d_writers%d", n, n), func(t *testing.T) {
			f := mysqlSetup(t)
			cfg := f.cfg
			cfg.PoolSize = n
			p := f.sink(cfg)
			f.ready(p)
			ctx := context.Background()
			lk, err := f.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lk.Close()
			if _, err := lk.ExecContext(ctx, "LOCK TABLES `"+f.table+"` WRITE"); err != nil {
				t.Fatal(err)
			}
			starveThePool(t, n, func(ctx context.Context) error { return p.Write(ctx, batchN(t, 3)) },
				p.Health, func() { _, _ = lk.ExecContext(ctx, "UNLOCK TABLES") })
		})
	}
}

func TestClickHouseHealthProbeOutsideThePool(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(fmt.Sprintf("pool%d_writers%d", n, n), func(t *testing.T) {
			f := chSetup(t)
			cfg := f.cfg
			cfg.PoolSize = n
			cfg.ConnectTimeout = 10 * time.Second
			c := f.sink(cfg)
			f.ready(c)
			// Every INSERT into the table feeds a view that sleeps: a slow
			// write that holds its connection for ~2.7 s.
			mv := f.table + "_slow"
			f.exec("CREATE MATERIALIZED VIEW `" + mv + "` ENGINE = Null AS SELECT sleepEachRow(0.9) AS s FROM `" + f.table + "`")
			t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP VIEW IF EXISTS `"+mv+"`") })
			starveThePool(t, n, func(ctx context.Context) error { return c.Write(ctx, batchN(t, 3)) },
				c.Health, func() {})
		})
	}
}

// The probe's connection killed by the server (a restart, an idle timeout,
// pg_terminate_backend): one check may fail, network, and the next dials a
// fresh connection -- the probe never stays on a dead one.
func TestPostgresHealthProbeRedials(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	p.probeMu.Lock()
	pid := p.probe.PgConn().PID()
	p.probeMu.Unlock()
	ctx := context.Background()
	if _, err := f.conn.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := p.Health(ctx); err != nil && IsFatal(err) {
		t.Fatalf("Health on a killed probe connection = %v, want network at most", err)
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("the next Health = %v: the probe did not dial again", err)
	}
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	if p.probe == nil || p.probe.PgConn().PID() == pid {
		t.Fatal("the probe still holds the killed connection")
	}
}

// ------------------------------------------------ MySQL (X5, 2026-10-13)

// A column made NOT NULL after the boot: the probe proved the server took a
// NULL there, so the 1048 is the table's, as PostgreSQL's 23502 is -- fatal.
// As logic, every chunk with a NULL in that column was dropped, counted, with
// the sink UP (go-syslog-server review, 2026-10-13).
func TestMySQLNotNullAfterBootIsFatal(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("ALTER TABLE `" + f.table + "` MODIFY msgid VARCHAR(255) NOT NULL")
	// An RFC 3164 line has no MSGID: msgid is NULL.
	err := p.Write(context.Background(), batchOf(t, line3164, line3164))
	if err == nil || !strings.Contains(err.Error(), "1048") || !IsFatal(err) {
		t.Fatalf("Write with a NULL into a column made NOT NULL = %v (class %s), want fatal 1048", err, ClassOf(err))
	}
}

// MySQL's name limit is 64 CHARACTERS: a database the server itself created
// with a UTF-8 name of 26 characters (66 bytes) was refused by the byte count
// (go-syslog-server review, 2026-10-13). The sink accepts it and validates
// a table in it.
func TestMySQLMultibyteDatabaseName(t *testing.T) {
	f := mysqlSetup(t)
	name := strings.Repeat("日", 20) + fmt.Sprint(time.Now().UnixNano()%1e6) // 26 characters, 66 bytes
	f.exec("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4")
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+name+"`") })
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	f.exec(retargetMySQLSchema(string(b), name+"`.`syslog_events"))
	cfg := f.cfg
	cfg.Database, cfg.Table = name, "syslog_events"
	p, err := NewMySQL(cfg, discard(), metrics())
	if err != nil {
		t.Fatalf("NewMySQL refused a database MySQL itself created: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	f.ready(p)
	if err := p.Write(context.Background(), batchN(t, 2)); err != nil {
		t.Fatalf("Write into the multibyte database: %v", err)
	}
	// Control: 65 characters is still refused, by the constructor.
	cfg.Database = strings.Repeat("日", 65)
	if _, err := NewMySQL(cfg, discard(), metrics()); err == nil || !strings.Contains(err.Error(), "65 characters") {
		t.Fatalf("NewMySQL with a 65-character name = %v, want the refusal", err)
	}
}

// ClickHouse takes the same rule (the name goes between the same backticks):
// a 26-character UTF-8 database (66 bytes) is accepted end to end.
func TestClickHouseMultibyteDatabaseName(t *testing.T) {
	f := chSetup(t)
	name := strings.Repeat("日", 20) + fmt.Sprint(time.Now().UnixNano()%1e6) // 26 characters, 66 bytes
	f.exec("CREATE DATABASE `" + name + "`")
	t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+name+"` SYNC") })
	f.exec(strings.Replace(chDDLFor(t, "inbox_syslog"), "CREATE TABLE `inbox_syslog`",
		"CREATE TABLE `"+name+"`.`inbox_syslog`", 1))
	cfg := f.cfg
	cfg.Database, cfg.Table = name, "inbox_syslog"
	c, err := NewClickHouse(cfg, discard(), metrics())
	if err != nil {
		t.Fatalf("NewClickHouse refused a database ClickHouse itself created: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	f.ready(c)
	if err := c.Write(context.Background(), batchN(t, 2)); err != nil {
		t.Fatalf("Write into the multibyte database: %v", err)
	}
}
