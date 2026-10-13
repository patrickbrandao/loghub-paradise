package sink

// Regression tests from the independent review of go-relplog-pg-server on
// 2026-10-13 against the real PostgreSQL (TESTING §9.3). They skip without
// RELPLOG_TEST_PG_DSN.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
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

// Mutants of the arbiter queries (M26, M27, M37, M40) that no case killed. The
// error each case expects names the query that must refuse it, so a mutant
// that only shifts the refusal to the other query is caught too.
func TestPostgresArbiterIndexRules(t *testing.T) {
	f := pgSetup(t)
	const noArbiter = "no unique index on exactly (uuid)"
	idx := func(name string) string {
		return "(SELECT i.indexrelid FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid " +
			"WHERE i.indrelid = '$T'::regclass AND c.relname = '" + name + "')"
	}
	for _, tc := range []struct{ name, alter, want string }{
		// indimmediate: a DEFERRABLE primary key is no arbiter.
		{"deferrable PK is no arbiter", "ALTER TABLE $T DROP CONSTRAINT inbox_pk; " +
			"ALTER TABLE $T ADD CONSTRAINT inbox_pk PRIMARY KEY (uuid) DEFERRABLE INITIALLY IMMEDIATE", noArbiter},
		// indisvalid: an invalid index (a failed CREATE UNIQUE INDEX
		// CONCURRENTLY) is no arbiter. The catalogue is edited to make one.
		{"invalid unique index is no arbiter", "ALTER TABLE $T DROP CONSTRAINT inbox_pk; " +
			"CREATE UNIQUE INDEX inbox_u ON $T (uuid); " +
			"UPDATE pg_catalog.pg_index SET indisvalid = false WHERE indexrelid = " + idx("inbox_u"), noArbiter},
		// indnkeyatts: INCLUDE columns are not keys; (uuid) INCLUDE (sid) is
		// an arbiter of ON CONFLICT (uuid).
		{"unique (uuid) INCLUDE (sid) is an arbiter", "ALTER TABLE $T DROP CONSTRAINT inbox_pk; " +
			"CREATE UNIQUE INDEX inbox_u ON $T (uuid) INCLUDE (sid)", ""},
		// indisready in the extra-unique query: an index that is not ready is
		// not maintained by inserts, and does not refuse the table.
		{"extra unique index not ready is tolerated", "CREATE UNIQUE INDEX inbox_x ON $T (from_uuid, sid); " +
			"UPDATE pg_catalog.pg_index SET indisready = false, indisvalid = false WHERE indexrelid = " + idx("inbox_x"), ""},
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
					t.Fatalf("Health refused a valid table: %v", err)
				}
				if err := p.Write(ctx, batchN(t, 2)); err != nil {
					t.Fatalf("Write after validation: %v", err)
				}
				return
			}
			if !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Health = %v, want fatal with %q", err, tc.want)
			}
		})
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

// A logic refusal of one row costs its chunk and nothing else: the
// neighbouring chunks are written, the refused one is counted once, and no
// batch stays held (SPEC §10.2, RP-6).
func TestPostgresLogicRefusalSparesTheNeighbours(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.Table = f.newTable(`ALTER TABLE $T ADD CONSTRAINT no_poison CHECK (payload->>'message' NOT LIKE '%POISON%')`)
	p := f.sink(cfg)
	f.ready(p)
	rg := ring.NewMemory(1000, 0, time.Millisecond)
	m := metrics()
	wc := workerCfg(10)
	wc.Workers = 1
	wc.Convert.XTransport = "relp"
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{}, discard(), m), wc, discard(), m)
	ls := lines(30)
	ls[15] += " POISON"
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	enqueueAll(t, rg, itemsOf(ls...))
	waitFor(t, 20*time.Second, func() bool {
		return m.EventsWrittenTotal.Load()+m.EventsDroppedTotal[obs.EventDropSinkLogic].Load() == 30
	})
	cancel()
	w.Wait()
	rows, dropped := f.count(cfg.Table), int(m.EventsDroppedTotal[obs.EventDropSinkLogic].Load())
	if m.SinkErrorsTotal[obs.SinkErrLogic].Load() != 1 || rows+dropped != 30 || rows < 20 || dropped > 10 {
		t.Fatalf("rows %d, dropped %d, logic errors %d: one chunk of at most 10 refused, the rest written",
			rows, dropped, m.SinkErrorsTotal[obs.SinkErrLogic].Load())
	}
	if n := len(w.Pending()); n != 0 {
		t.Fatalf("%d items still held after the refusal", n)
	}
}

// newTableIn applies the canonical DDL through conn -- another database --
// as loghub.inbox, which the database's drop removes.
func (f *pgFixture) newTableIn(conn *pgx.Conn) string {
	f.t.Helper()
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := conn.PgConn().Exec(context.Background(), string(b)).ReadAll(); err != nil {
		f.t.Fatal(err)
	}
	return "loghub.inbox"
}

// M30, M31: pg_is_in_recovery() is a replica. At boot it is fatal (a
// configuration mistake, exit 3); after the boot it is a failover in progress,
// DOWN (network). Without a replica, a database of its own puts a fake
// pg_is_in_recovery() ahead of pg_catalog in its search_path.
func TestPostgresRecoveryIsFatalAtBootAndDownAfter(t *testing.T) {
	f := pgSetup(t)
	db := fmt.Sprintf("rlpg_recovery_%d", time.Now().UnixNano())
	f.exec("CREATE DATABASE " + db)
	t.Cleanup(func() { f.exec("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)") })
	pc := f.conn.Config().Copy()
	pc.Database = db
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	recovery := func(on bool) {
		t.Helper()
		v := "false"
		if on {
			v = "true"
		}
		if _, err := conn.Exec(ctx, "CREATE OR REPLACE FUNCTION fake.pg_is_in_recovery() RETURNS boolean "+
			"LANGUAGE sql AS 'SELECT "+v+"'"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.Exec(ctx, "CREATE SCHEMA fake"); err != nil {
		t.Fatal(err)
	}
	recovery(true)
	if _, err := conn.Exec(ctx, "ALTER DATABASE "+db+" SET search_path = fake, pg_catalog, public"); err != nil {
		t.Fatal(err)
	}
	table := f.newTableIn(conn)

	cfg := f.cfg
	cfg.Database, cfg.Table = db, table
	p := f.sink(cfg)
	if err := p.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "recovery") {
		t.Fatalf("a replica at boot: Health = %v (class %s), want fatal", err, ClassOf(err))
	}

	recovery(false)
	p = f.sink(cfg)
	f.ready(p)
	recovery(true)
	err = p.Health(ctx)
	if err == nil || ClassOf(err) != ClassNetwork || !strings.Contains(err.Error(), "recovery") {
		t.Fatalf("a failover after the boot: Health = %v (class %s), want network (DOWN)", err, ClassOf(err))
	}
}
