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

// Sink findings of the independent review of the port of the R, RP and RN
// fixes into this server (2026-10-13). The real-server tests skip without
// their variables (docs/TESTING.md §6).

// probeUnderLoad holds `writers` writes in flight -- slow but working, the
// destination stalling them until release -- and runs one health check under
// sink.health_timeout (1 s) in the middle. It returns what the check said.
// Adopted from the reviewer's rvd_pool_test.go.
func probeUnderLoad(t *testing.T, writers int, write, health func(ctx context.Context) error, release func()) error {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- write(ctx)
		}()
	}
	time.Sleep(400 * time.Millisecond) // every writer holds its connection
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

// SPEC §10.5, §10.6, §10.10.2: the health check has a connection of its own,
// outside the writers' pool. Taken from the pool, it waited behind slow
// writes; with ring=none the writers are the receivers, up to
// ring.max_inflight, and "pool_size >= workers + 1" protected nothing: with as
// many writers as connections the check timed out, the sink went DOWN and,
// past fail_timeout, FATAL with nothing wrong with the database (independent
// review of the port, 2026-10-13; closes TESTING §8 item 14's "sem teste de
// integração").
func TestPostgresProbeIsNotStarvedByTheWriters(t *testing.T) {
	for _, n := range []int{2, 3} {
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
			herr := probeUnderLoad(t, n, func(ctx context.Context) error { return p.Write(ctx, batchN(t, 3)) },
				p.Health, func() { _ = tx.Rollback(ctx) })
			if herr != nil {
				t.Fatalf("with %d writers on a pool of %d, the health check failed: %v", n, n, herr)
			}
		})
	}
}

func TestMySQLProbeIsNotStarvedByTheWriters(t *testing.T) {
	for _, n := range []int{2, 3} {
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
			herr := probeUnderLoad(t, n, func(ctx context.Context) error { return p.Write(ctx, batchN(t, 3)) },
				p.Health, func() { _, _ = lk.ExecContext(ctx, "UNLOCK TABLES") })
			if herr != nil {
				t.Fatalf("with %d writers on a pool of %d, the health check failed: %v", n, n, herr)
			}
		})
	}
}

func TestClickHouseProbeIsNotStarvedByTheWriters(t *testing.T) {
	for _, n := range []int{2, 3} {
		t.Run(fmt.Sprintf("pool%d_writers%d", n, n), func(t *testing.T) {
			f := chSetup(t)
			cfg := f.cfg
			cfg.PoolSize = n
			cfg.ConnectTimeout = 10 * time.Second
			c := f.sink(cfg)
			f.ready(c)
			// A materialized view that sleeps 0.9 s per row: every INSERT of
			// three rows holds its connection for ~2.7 s.
			mv := f.table + "_slow"
			f.exec("CREATE MATERIALIZED VIEW `" + mv + "` ENGINE = Null AS SELECT sleepEachRow(0.9) AS s FROM `" + f.table + "`")
			t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP VIEW IF EXISTS `"+mv+"`") })
			herr := probeUnderLoad(t, n, func(ctx context.Context) error { return c.Write(ctx, batchN(t, 3)) },
				c.Health, func() {})
			if herr != nil {
				t.Fatalf("with %d writers on a pool of %d, the health check failed: %v", n, n, herr)
			}
		})
	}
}

// SPEC §10.6: a column made NOT NULL after the boot. The probe proved NULLs
// were accepted, so the 1048 is the table's -- the twin of PostgreSQL's
// 23502 -- and fatal; as logic, every chunk with a NULL there was dropped
// with the sink UP.
func TestMySQLNotNullAfterTheBootIsFatal(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("ALTER TABLE `" + f.table + "` MODIFY msgid VARCHAR(255) NOT NULL")
	// An RFC 3164 line has no MSGID: NULL in msgid.
	err := p.Write(context.Background(), batchOf(t, line3164, line3164))
	if err == nil || !strings.Contains(err.Error(), "1048") {
		t.Fatalf("Write = %v, want a 1048", err)
	}
	if ClassOf(err) != ClassFatal {
		t.Errorf("1048 after the boot is %s, want fatal", ClassOf(err))
	}
}

// SPEC §10.6, §13.2: MySQL's limit on a database name is 64 CHARACTERS. A name
// of 64 'ç' (128 bytes) is one MySQL creates, and the byte count refused it.
func TestMySQLMultibyteDatabaseName(t *testing.T) {
	f := mysqlSetup(t)
	db := strings.Repeat("ç", 64)
	f.exec("CREATE DATABASE IF NOT EXISTS `" + db + "` CHARACTER SET utf8mb4")
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+db+"`") })
	ddl := strings.Replace(mysqlSchemaFor(t, f.table), "CREATE TABLE `"+f.table+"`",
		"CREATE TABLE `"+db+"`.`"+f.table+"`", 1)
	f.exec(ddl)
	cfg := f.cfg
	cfg.Database = db
	p := f.sink(cfg)
	f.ready(p)
	if err := p.Write(context.Background(), batchN(t, 2)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var n int
	if err := f.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `"+db+"`.`"+f.table+"`").Scan(&n); err != nil || n != 2 {
		t.Fatalf("rows = %d (%v), want 2", n, err)
	}
	// 65 characters is MySQL's 1059: refused before any connection.
	cfg.Database = strings.Repeat("ç", 65)
	if _, err := NewMySQL(cfg, discard(), metrics()); err == nil || !strings.Contains(err.Error(), "65 characters") {
		t.Errorf("NewMySQL with 65 characters = %v", err)
	}
}

// The same for ClickHouse, whose own limit is the %-encoded name as a file
// name (255 bytes): 25 CJK characters, 75 bytes, are a database it creates,
// and the byte count refused it.
func TestClickHouseMultibyteDatabaseName(t *testing.T) {
	f := chSetup(t)
	db := strings.Repeat("日", 25)
	f.exec("CREATE DATABASE IF NOT EXISTS `" + db + "`")
	t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"`") })
	ddl := strings.Replace(chDDLFor(t, f.table), "CREATE TABLE `"+f.table+"`", "CREATE TABLE `"+db+"`.`"+f.table+"`", 1)
	f.exec(ddl)
	cfg := f.cfg
	cfg.Database = db
	c := f.sink(cfg)
	f.ready(c)
	if err := c.Write(context.Background(), batchN(t, 2)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var n uint64
	if err := f.admin.QueryRow(context.Background(), "SELECT count() FROM `"+db+"`.`"+f.table+"`").Scan(&n); err != nil || n != 2 {
		t.Fatalf("rows = %d (%v), want 2", n, err)
	}
}

// Without a server: the constructors count characters as the config does.
func TestQuotedNamesCountCharacters(t *testing.T) {
	for _, ok := range []string{strings.Repeat("d", 64), strings.Repeat("ç", 64), strings.Repeat("日", 64)} {
		if err := checkQuotedName("db", ok); err != nil {
			t.Errorf("%d bytes, %d characters refused: %v", len(ok), len([]rune(ok)), err)
		}
	}
	for _, bad := range []string{strings.Repeat("d", 65), strings.Repeat("ç", 65)} {
		if err := checkQuotedName("db", bad); err == nil {
			t.Errorf("%d characters accepted", len([]rune(bad)))
		}
	}
}

// SPEC §10.10.5: the per-user memory limit is held against the WRITERS -- the
// workers with a ring, min(pool_size, max_inflight) with ring=none --, not the
// pool: the health check's connection is a SELECT, never a chunk. With pool =
// workers + 1 a limit for exactly the workers' chunks was refused, and the
// message said to lower a pool that cannot go below the workers.
func TestClickHouseMemoryCountsTheWriters(t *testing.T) {
	const batch = 64 << 20
	need := chMemoryNeed(batch)
	if p := chMemoryProblem(batch, 4, 0, 4*need); p != "" {
		t.Errorf("a limit for the four writers' chunks refused: %s", p)
	}
	p := chMemoryProblem(batch, 5, 0, 4*need)
	if !strings.Contains(p, "5 concurrent") || strings.Contains(p, "lower the pool") ||
		!strings.Contains(p, "sink.workers") {
		t.Errorf("message = %q, want 5 writers and sink.workers, never the pool", p)
	}
}

// Against the server: a user whose per-user limit covers the four workers'
// chunks boots with a pool of five; without Writers the pool was the
// multiplier and the boot was refused.
func TestClickHouseUserMemoryLimitCountsTheWriters(t *testing.T) {
	f := chSetup(t)
	const batch = 64 << 20
	limit := 4 * chMemoryNeed(batch)
	cfg := f.chUser("memw", fmt.Sprintf("SETTINGS max_memory_usage_for_user = %d", limit), "GRANT INSERT ON $T TO $U")
	cfg.MaxBatchBytes, cfg.PoolSize, cfg.Writers = batch, 5, 4
	f.ready(f.sink(cfg))
	cfg.Writers = 0 // the pool as the multiplier: refused
	if err := f.sink(cfg).Health(context.Background()); err == nil || ClassOf(err) != ClassFatal {
		t.Fatalf("Health with the pool as the multiplier = %v, want fatal", err)
	}
}

// mysqlSchemaFor is docker/mysql/schema.sql retargeted at table, as newTable
// runs it, for a test that creates the table somewhere else.
func mysqlSchemaFor(t *testing.T, table string) string {
	t.Helper()
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	return retargetMySQLSchema(string(b), table)
}
