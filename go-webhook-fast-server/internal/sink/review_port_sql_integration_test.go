package sink

// Port of the SQL corrections of the syslog/RELP family (2026-10-13) against a
// real MySQL and ClickHouse: the health check's own connection (SPEC §8.6.7,
// §8.8.8), autocommit read back at boot and 1048/1226 classified on the real
// server's answer (SPEC §8.6.1, §8.6.6), and the per-user memory limit
// counted by writers (SPEC §8.8.5). They skip without WHS_TEST_MYSQL_DSN or
// WHS_TEST_CLICKHOUSE_DSN. Nothing here changes a GLOBAL variable: the
// servers are shared with other suites (TESTING §5.3).

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// SPEC §8.6.7: the health check has its own connection. Writers holding every
// pooled connection (here behind LOCK TABLES) starved a check that waited for
// one: DOWN, then FATAL with nothing wrong (see TestPostgresHealthHasItsOwnConnection).
func TestMySQLHealthHasItsOwnConnection(t *testing.T) {
	for _, tc := range []struct{ pool, writers int }{{1, 1}, {3, 3}} {
		t.Run(fmt.Sprintf("pool%d_writers%d", tc.pool, tc.writers), func(t *testing.T) {
			f := mysqlSetup(t)
			cfg := f.cfg
			cfg.PoolSize = tc.pool
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
			herr := slowWritersThenHealth(t, tc.writers,
				func(ctx context.Context) error { return p.Write(ctx, mkBatch(3)) },
				p.Health, func() { _, _ = lk.ExecContext(ctx, "UNLOCK TABLES") })
			if herr != nil {
				t.Fatalf("pool_size=%d with %d writers holding it: health = %v, want nil", tc.pool, tc.writers, herr)
			}
		})
	}
}

// SPEC §8.6.1: the boot reads autocommit back from a pooled connection. A
// session with autocommit=0 -- a server or a proxy that ignored the SET --
// would keep every single-chunk INSERT in a transaction nobody commits. The
// session is turned off by hand on the pool's only connection (no SET
// GLOBAL on a shared server); the pin itself is TestMySQLSessionPinsAutocommit.
func TestMySQLAutocommitIsReadBackAtBoot(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	cfg.PoolSize = 1
	p := f.sink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := p.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecContext(ctx, "SET SESSION autocommit = 0"); err != nil {
		t.Fatal(err)
	}
	c.Close() // back to the pool, autocommit still off
	err = p.Health(ctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "autocommit") {
		t.Fatalf("Health on a session with autocommit=0 = %v (class %s), want fatal naming autocommit",
			err, ClassOf(err))
	}
	// A fresh sink -- the pin applies -- validates.
	f.ready(f.sink(f.cfg))
}

// SPEC §8.6.6: 1048 after the boot probe is the table, never this data -- the
// sink never sends NULL. Here a trigger created after the boot writes one.
func TestMySQLNotNullAfterBootIsFatal(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	trigger := f.table + "_null"
	f.exec("CREATE TRIGGER `" + trigger + "` BEFORE INSERT ON `" + f.table + "` FOR EACH ROW SET NEW.space = NULL")
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS `"+trigger+"`") })
	err := p.Write(context.Background(), mkBatch(2))
	if err == nil {
		t.Fatal("Write succeeded through a trigger writing NULL into a NOT NULL column")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "mysql=1048") {
		t.Fatalf("Write = %v (class %s), want fatal with 1048", err, ClassOf(err))
	}
}

// SPEC §8.6.6: a per-user connection limit (1226, SQLSTATE 42000) clears on
// its own and is network, not the fatal its class-42 state would make it --
// a rolling deploy overlapping the budget ended every instance with exit 3.
// On the real server's answer, with a user of its own (CREATE USER needed;
// skipped without it).
func TestMySQLUserConnectionLimitIsNetwork(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()
	user := fmt.Sprintf("whs_lim_%d", time.Now().UnixNano())
	if _, err := f.db.ExecContext(ctx, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'whs-test-pw' "+
		"WITH MAX_USER_CONNECTIONS 1"); err != nil {
		t.Skipf("cannot create users on this server: %v", err)
	}
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	f.exec("GRANT INSERT, UPDATE, SELECT ON `" + f.dbName + "`.`" + f.table + "` TO '" + user + "'@'%'")

	// The user's one connection, held by somebody else.
	mc := mysql.NewConfig()
	mc.User, mc.Passwd, mc.Net, mc.Addr, mc.DBName = user, "whs-test-pw", "tcp", f.cfg.Addr, f.dbName
	hdb, err := sql.Open("mysql", mc.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer hdb.Close()
	holder, err := hdb.Conn(ctx)
	if err != nil {
		t.Fatalf("the user's first connection: %v", err)
	}
	defer holder.Close()

	cfg := f.cfg
	cfg.Username, cfg.Password = user, "whs-test-pw"
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = f.sink(cfg).Health(cctx)
	if err == nil {
		t.Fatal("a second connection of a MAX_USER_CONNECTIONS 1 user was accepted; the limit did not apply")
	}
	if ClassOf(err) != ClassNetwork || !strings.Contains(err.Error(), "1226") {
		t.Fatalf("Health over the user's connection limit = %v (class %s), want network 1226", err, ClassOf(err))
	}
}

// SPEC §8.8.8: the ClickHouse health check has its own connection; writers
// holding every pooled one (slowed by a materialized view that sleeps) left a
// check inside the pool waiting for the driver's acquire.
func TestClickHouseHealthHasItsOwnConnection(t *testing.T) {
	for _, tc := range []struct{ pool, writers int }{{1, 1}, {3, 3}} {
		t.Run(fmt.Sprintf("pool%d_writers%d", tc.pool, tc.writers), func(t *testing.T) {
			f := chSetup(t)
			cfg := f.cfg
			cfg.PoolSize = tc.pool
			cfg.ConnectTimeout = 10 * time.Second
			c := f.sink(cfg)
			f.ready(c)
			mv := f.table + "_slow"
			f.exec("CREATE MATERIALIZED VIEW `" + mv + "` ENGINE = Null AS SELECT sleepEachRow(0.6) AS s FROM `" + f.table + "`")
			t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP VIEW IF EXISTS `"+mv+"`") })
			herr := slowWritersThenHealth(t, tc.writers,
				func(ctx context.Context) error { return c.Write(ctx, mkBatch(3)) },
				c.Health, func() {})
			if herr != nil {
				t.Fatalf("pool_size=%d with %d writers holding it: health = %v, want nil", tc.pool, tc.writers, herr)
			}
		})
	}
}

// SPEC §8.8.5: the per-user limit is compared with the INSERTs this instance
// can have in flight, min(pool_size, writers). A pool of sink.workers + 1 (the
// derived default) with a limit of exactly workers worst batches was refused
// at boot although no write could ever break it (independent review of the
// port to go-syslog-server, 2026-10-13).
func TestClickHouseUserMemoryLimitCountsTheWriters(t *testing.T) {
	f := chSetup(t)
	cfg := f.chUser("memw", "SETTINGS max_memory_usage_for_user = 2000000000", "GRANT INSERT ON $T TO $U")
	cfg.MaxBatchBytes, cfg.PoolSize = 64<<20, 4 // 640 MiB each: two fit, three do not
	cfg.Writers = 2
	f.ready(f.sink(cfg))
	cfg.Writers = 3
	err := f.sink(cfg).Health(context.Background())
	if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "3 concurrent") {
		t.Fatalf("Health = %v (class %s), want fatal naming 3 concurrent INSERTs", err, ClassOf(err))
	}
}
