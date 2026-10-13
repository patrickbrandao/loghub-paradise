package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// The destination is remote and has a schema, and what is worth proving -- the
// unnest INSERT into loghub.inbox with its jsonb and timestamptz arrays, ON
// CONFLICT (uuid), the CHECK constraints of the skill's DDL, the rows-affected
// arithmetic, the catalogue queries of the boot validation, the privilege
// check, class 42 as fatal -- is exactly what a fake would invent. These run
// against a real PostgreSQL 18 addressed by RELPLOG_TEST_PG_DSN and skip without
// it:
//
//	docker run --rm -d -p 15432:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
//	  -e POSTGRES_DB=sls_test --name sls-sink-pg postgres:18-alpine
//	RELPLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
//	  go test -race ./internal/sink/
//
// Every test creates its own queue from docker/postgres/inbox.sql, in a schema
// of its own (the DDL names its constraints and its index), and drops the
// schema afterwards.

const pgDSNEnv = "RELPLOG_TEST_PG_DSN"

type pgFixture struct {
	t     *testing.T
	conn  *pgx.Conn
	table string
	cfg   PostgresConfig
}

func pgSetup(t *testing.T) *pgFixture {
	t.Helper()
	dsn := os.Getenv(pgDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the postgres sink tests", pgDSNEnv)
	}
	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("%s: %v", pgDSNEnv, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		t.Fatalf("%s is not reachable: %v", pgDSNEnv, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	f := &pgFixture{t: t, conn: conn, cfg: PostgresConfig{
		Addr:     net.JoinHostPort(pc.Host, strconv.Itoa(int(pc.Port))),
		Username: pc.User,
		Password: pc.Password,
		Database: pc.Database,
		PoolSize: 4,
		AppName:  "sls-test",
	}}
	f.table = f.newTable("")
	f.cfg.Table = f.table
	return f
}

// newTable creates a queue from the canonical DDL in a schema of its own, with
// alter run afterwards ($T is the table), and registers its removal. Using the
// file operators apply is the point: the validation of SPEC §10.5 and the DDL
// the operator gets must agree.
func (f *pgFixture) newTable(alter string) string {
	f.t.Helper()
	schema := fmt.Sprintf("sls_test_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		f.t.Fatalf("reading the canonical DDL: %v", err)
	}
	ddl := strings.ReplaceAll(string(b), "SCHEMA IF NOT EXISTS loghub;", "SCHEMA "+schema+";")
	ddl = strings.ReplaceAll(ddl, "loghub.inbox", schema+".inbox")
	f.exec(ddl)
	table := schema + ".inbox"
	if alter != "" {
		f.exec(strings.ReplaceAll(alter, "$T", table))
	}
	f.t.Cleanup(func() { f.exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
	return table
}

// exec runs SQL by the simple protocol, which takes several statements.
func (f *pgFixture) exec(sql string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := f.conn.PgConn().Exec(ctx, sql).ReadAll(); err != nil {
		f.t.Fatalf("%v\n%s", err, sql)
	}
}

func (f *pgFixture) count(table string) int {
	f.t.Helper()
	var n int
	if err := f.conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *pgFixture) sink(cfg PostgresConfig) *Postgres {
	f.t.Helper()
	p, err := NewPostgres(cfg, discard(), metrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { p.Close() })
	return p
}

func (f *pgFixture) ready(p *Postgres) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// checkRow reads the row of e back and rebuilds the envelope from it with the
// skill's own SQL: it must be the JLH as a JSON value, with the payload the
// column holds (SPEC §3.7.1) -- equal once both are decoded --, and a valid
// type=syslog envelope, semantic rules included: raw stays exact. received_at
// must be the table's now().
func (f *pgFixture) checkRow(table string, e *Envelope) {
	f.t.Helper()
	var rebuilt []byte
	var lag time.Duration
	err := f.conn.QueryRow(context.Background(), `
	  SELECT jsonb_build_object(
	    'uuid', uuid::text, 'from', from_uuid::text, 'space', space, 'type', type,
	    'level', level, 'sid', sid, 'step', step,
	    'event', jsonb_build_object(
	      'uts', floor(extract(epoch FROM event_at))::bigint,
	      'sub', (extract(microseconds FROM event_at)::bigint % 1000000) * 1000 + event_nsec,
	      'iso', to_char(event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
	    'payload', payload)::text,
	    now() - received_at
	  FROM `+table+` WHERE uuid = $1::uuid`, e.UUID()).Scan(&rebuilt, &lag)
	if err != nil {
		f.t.Fatalf("reading %s back: %v", e.UUID(), err)
	}
	if lag < 0 || lag > time.Minute {
		f.t.Errorf("received_at is not the table's now(): lag %v", lag)
	}
	var got, want map[string]any
	if err := json.Unmarshal(rebuilt, &got); err != nil {
		f.t.Fatal(err)
	}
	if err := json.Unmarshal(e.JLH, &want); err != nil {
		f.t.Fatal(err)
	}
	payload, _ := jlh.Payload(e.JLH)
	var col map[string]any
	if err := json.Unmarshal(inboxPayload(payload), &col); err != nil {
		f.t.Fatal(err)
	}
	want["payload"] = col
	if !reflect.DeepEqual(got, want) {
		f.t.Errorf("envelope %s rebuilt from the row:\n%s\nwant the JLH:\n%s", e.UUID(), rebuilt, e.JLH)
	}
	if err := jlhcheck.Validate(rebuilt); err != nil {
		f.t.Errorf("envelope %s rebuilt from the row: %v", e.UUID(), err)
	}
}

// SPEC §3.7.1 and §10.5: every column lands from the JLH header, the payload
// is the JLH's payload as jsonb (NUL turned into U+FFFD), and the envelope
// rebuilds from the row without loss of information -- event to the
// nanosecond, with the skill's own SQL.
func TestPostgresWriteEnqueuesTheEnvelope(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	unix := newItem("<13>no header at all")
	unix.Head.Transport, unix.Head.Framing = item.TransportUnix, item.FramingDatagram
	b := batchFrom(t, newItem(line5424), newItem("<13>Oct 11 22:14:15 web\x00-01 su[230]: a\x00b"), unix,
		newItemIn("com.acme.store", "<11>1 - h a - - - in another space"))
	ctx := context.Background()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table); n != 4 {
		t.Fatalf("%d rows, want 4", n)
	}

	for i := range b.Envelopes {
		f.checkRow(f.table, &b.Envelopes[i])
	}
	if !strings.Contains(string(b.Envelopes[1].JLH), `a\u0000b`) {
		t.Error("the JLH lost the NUL: the replacement must touch the column copy only")
	}

	// The queue is read in uuid order, and the uuid is the time order.
	var ordered bool
	if err := f.conn.QueryRow(ctx, `SELECT bool_and(prev IS NULL OR prev < uuid) FROM (
	    SELECT uuid, lag(uuid) OVER (ORDER BY event_at, event_nsec) AS prev FROM `+f.table+`) q`).Scan(&ordered); err != nil || !ordered {
		t.Errorf("uuid order and event order disagree (%v)", err)
	}
}

// The consumer pattern of the skill works on what this server enqueues: two
// claims with FOR UPDATE SKIP LOCKED take disjoint rows, in uuid order, and a
// ROLLBACK gives them back. Nothing here runs in the server; it proves the rows
// are a queue and not just a table.
func TestPostgresEnqueuedRowsAreConsumable(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	if err := p.Write(context.Background(), batchN(t, 10)); err != nil {
		t.Fatal(err)
	}
	claim := `WITH job AS (SELECT uuid FROM ` + f.table + ` ORDER BY uuid LIMIT 3 FOR UPDATE SKIP LOCKED)
	          DELETE FROM ` + f.table + ` i USING job WHERE i.uuid = job.uuid RETURNING i.uuid::text`
	ctx := context.Background()
	txA, err := f.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a := collect(t, txA, claim)
	other, err := pgx.ConnectConfig(ctx, f.conn.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	txB, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bb := collect(t, txB, claim)
	if len(a) != 3 || len(bb) != 3 || a[2] >= bb[0] {
		t.Errorf("claims A=%v B=%v: want 3 each, disjoint, B after A", a, bb)
	}
	if err := txA.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := f.count(f.table); n != 7 {
		t.Errorf("%d rows left, want 7 (A rolled back, B committed)", n)
	}
}

func collect(t *testing.T, tx pgx.Tx, sql string) []string {
	t.Helper()
	rows, err := tx.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// SPEC §10.5: the pipeline is at-least-once; a redelivery adds nothing and is
// counted through the rows-affected arithmetic.
func TestPostgresToleratesRedeliveryAndCountsIt(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchN(t, 7)
	ctx := context.Background()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	fresh := batchOf(t, "<13>fresh one", "<13>fresh two")
	mixed := &Batch{Envelopes: append(append([]Envelope{}, b.Envelopes[:3]...), fresh.Envelopes...)}
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	// A uuid twice inside ONE statement: DO NOTHING absorbs it (DO UPDATE
	// would fail the whole chunk with 21000).
	twice := batchOf(t, "<13>twice")
	twice.Envelopes = append(twice.Envelopes, twice.Envelopes[0])
	if err := p.Write(ctx, twice); err != nil {
		t.Fatalf("a uuid twice in one chunk: %v", err)
	}
	if n := f.count(f.table); n != 10 {
		t.Errorf("%d rows, want 10: redelivery must not duplicate", n)
	}
	if got := p.m.SinkPostgresDuplicatesTotal.Load(); got != 11 {
		t.Errorf("duplicates counter = %d, want 11 (7 from the full redelivery, 3 from the overlap, 1 in-chunk)", got)
	}
}

// Many workers, one pool: every chunk lands, and the gauges reflect the
// ceiling and the waits.
func TestPostgresConcurrentWritersAndPoolGauges(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.PoolSize = 3
	p := f.sink(cfg)
	f.ready(p)

	const writers, per = 8, 50
	batches := make([]*Batch, writers)
	for w := range batches {
		batches[w] = batchN(t, per)
	}
	// Built first and released together: staggered writers would never meet
	// in the pool, and the waits counter would prove nothing.
	start := make(chan struct{})
	errs := make(chan error, writers)
	for _, b := range batches {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}()
	}
	close(start)
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Errorf("writer: %v", err)
		}
	}
	if n := f.count(f.table); n != writers*per {
		t.Errorf("%d rows, want %d", n, writers*per)
	}
	f.ready(p)
	if got := p.m.SinkPostgresPoolConns[2].Load(); got != 3 {
		t.Errorf(`pool_conns{state="max"} = %d, want 3`, got)
	}
	if got := p.m.SinkPostgresPoolWaitsTotal.Load(); got == 0 {
		t.Error("eight writers over three connections never waited, or the counter is not fed")
	}
	// Every connection went back: a leaked acquire would shrink the budget for
	// good.
	if st := p.pool.Stat(); st.AcquiredConns() != 0 || st.TotalConns() > st.MaxConns() {
		t.Errorf("after the writers: acquired=%d total=%d max=%d", st.AcquiredConns(), st.TotalConns(), st.MaxConns())
	}
}

// SPEC §10.5: each refusal is FATAL, so the boot stops at once instead of
// waiting out sink.boot_timeout on a table that will not appear.
func TestPostgresHealthValidatesTheSchema(t *testing.T) {
	f := pgSetup(t)
	cases := []struct {
		name  string
		table string
		want  string // substring of the fatal error; "" means accepted
	}{
		{"canonical DDL", f.table, ""},
		{"missing table", fmt.Sprintf("sls_missing_%d.inbox", time.Now().UnixNano()), "does not exist"},
		{"text payload", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_payload_ck, DROP CONSTRAINT inbox_syslog_ck; ALTER TABLE $T ALTER COLUMN payload TYPE text"), `"payload"`},
		{"json payload", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_payload_ck, DROP CONSTRAINT inbox_syslog_ck; ALTER TABLE $T ALTER COLUMN payload TYPE json USING payload::json"), `"payload"`},
		{"timestamp without zone", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_event_ck; ALTER TABLE $T ALTER COLUMN event_at TYPE timestamp"), `"event_at"`},
		{"missing column", f.newTable("ALTER TABLE $T DROP COLUMN event_nsec"), `"event_nsec"`},
		{"text from_uuid", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_from_v7_ck; ALTER TABLE $T ALTER COLUMN from_uuid TYPE text"), `"from_uuid"`},
		{"text sid", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_sid_ck; ALTER TABLE $T ALTER COLUMN sid TYPE text"), `"sid"`},
		// Review findings of 2026-10-06: tables the old validation accepted
		// and on which every INSERT then failed with the sink UP.
		{"integer sid", f.newTable("ALTER TABLE $T ALTER COLUMN sid TYPE integer"), `"sid"`},
		{"varchar(3) type", f.newTable("ALTER TABLE $T ALTER COLUMN type TYPE varchar(3)"), `"type"`},
		{"varchar(10) space", f.newTable("ALTER TABLE $T ALTER COLUMN space TYPE varchar(10)"), `"space"`},
		{"deferrable primary key", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; ALTER TABLE $T ADD CONSTRAINT inbox_pk PRIMARY KEY (uuid) DEFERRABLE INITIALLY IMMEDIATE"), "unique index"},
		{"no unique index", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk"), "unique index"},
		{"unique on (uuid, step) only", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid, step)"), "unique index"},
		{"partial unique index", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid) WHERE level < 8"), "unique index"},
		{"unique index instead of the PK is tolerated", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid)"), ""},
		// Review finding 2 of the first round (SPEC §20.18): a NOT NULL the
		// sink cannot satisfy refuses every row; caught at boot, never as a
		// logic drop with the sink UP.
		{"extra required column", f.newTable("ALTER TABLE $T ADD COLUMN tenant text NOT NULL"), `"tenant"`},
		{"received_at without its default", f.newTable("ALTER TABLE $T ALTER COLUMN received_at DROP DEFAULT"), `"received_at"`},
		{"no received_at at all is tolerated", f.newTable("ALTER TABLE $T DROP COLUMN received_at"), ""},
		{"extra NOT NULL with a default is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN tenant text NOT NULL DEFAULT 'x'"), ""},
		{"extra identity column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY"), ""},
		{"extra serial column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN n bigserial"), ""},
		{"extra generated column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN y bigint GENERATED ALWAYS AS (sid * 2) STORED NOT NULL"), ""},
		{"varchar space is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN space TYPE varchar(64)"), ""},
		{"unbounded varchar type is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN type TYPE varchar"), ""},
		{"integer level is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN level TYPE integer"), ""},
		// go-webhook-server review of 2026-10-06, same design. The typmod is
		// compared apart from the type: timestamptz(6) used to be cut at '('
		// into "timestamp" and refused as the wrong type; a precision below 6
		// rounds the microsecond.
		{"timestamptz(6) event_at is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(6)"), ""},
		{"timestamptz(3) event_at", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(3)"), "precision below 6"},
		{"timestamptz(0) event_at", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(0)"), `"event_at"`},
		// ON CONFLICT (uuid) absorbs collisions on its arbiter only: any other
		// unique index or exclusion constraint fails the whole chunk on a
		// collision (23505, 23P01), and a deferrable unique on uuid fails
		// every chunk (55000).
		{"extra unique index", f.newTable("CREATE UNIQUE INDEX inbox_from_sid_uq ON $T (from_uuid, sid)"), "(from_uuid, sid)"},
		{"extra unique constraint", f.newTable("ALTER TABLE $T ADD CONSTRAINT inbox_sid_uq UNIQUE (sid)"), "inbox_sid_uq"},
		{"unique (uuid, step) besides the PK", f.newTable("CREATE UNIQUE INDEX inbox_jlh_uq ON $T (uuid, step)"), "(uuid, step)"},
		{"deferrable unique (uuid) besides the PK", f.newTable("ALTER TABLE $T ADD CONSTRAINT inbox_uuid_def UNIQUE (uuid) DEFERRABLE INITIALLY IMMEDIATE"), "inbox_uuid_def"},
		{"exclusion constraint", f.newTable("ALTER TABLE $T ADD CONSTRAINT inbox_from_ex EXCLUDE USING btree (from_uuid WITH =)"), "EXCLUDE"},
		{"a non-unique extra index is tolerated", f.newTable("CREATE INDEX inbox_from_sid_ix ON $T (from_uuid, sid)"), ""},
		{"a second unique index on (uuid) is tolerated", f.newTable("CREATE UNIQUE INDEX inbox_uuid_uq ON $T (uuid)"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Table = tc.table
			p := f.sink(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := p.Health(ctx)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Health refused a valid table: %v", err)
				}
				if err := p.Write(ctx, batchN(t, 2)); err != nil {
					t.Errorf("Write after validation: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Health accepted a table that violates the contract")
			}
			if !IsFatal(err) {
				t.Errorf("class = %s, want fatal so the boot stops at once", ClassOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// SPEC §10.5: a role needs INSERT and SELECT (uuid) -- ON CONFLICT (uuid) reads
// the arbiter column, and the first chunk of a role with INSERT alone fails
// with 42501 (found by the go-webhook-server review of 2026-10-06; this test
// used to grant SELECT on the whole table first and hid it). UPDATE is never
// needed (DO NOTHING).
func TestPostgresHealthValidatesThePrivilege(t *testing.T) {
	f := pgSetup(t)
	role := fmt.Sprintf("sls_role_%d", time.Now().UnixNano())
	ctx := context.Background()
	if _, err := f.conn.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD 'sls-test-pw'"); err != nil {
		t.Skipf("cannot create roles on this server: %v", err)
	}
	t.Cleanup(func() {
		f.exec("DROP OWNED BY " + role + "; DROP ROLE IF EXISTS " + role)
	})
	f.exec("GRANT USAGE ON SCHEMA " + strings.Split(f.table, ".")[0] + " TO " + role)

	try := func() error {
		cfg := f.cfg
		cfg.Username, cfg.Password = role, "sls-test-pw"
		p := f.sink(cfg)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := p.Health(cctx); err != nil {
			return err
		}
		return p.Write(cctx, batchN(t, 1))
	}
	err := try()
	if err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "INSERT") {
		t.Fatalf("a role without INSERT: %v, want a fatal error naming INSERT", err)
	}
	// INSERT alone: the boot refuses it, so the write never meets the 42501.
	f.exec("GRANT INSERT ON " + f.table + " TO " + role)
	err = try()
	if err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "SELECT (uuid)") {
		t.Fatalf("a role with INSERT alone: %v, want a fatal error naming SELECT (uuid)", err)
	}
	// And the boot is right: with INSERT alone the write itself fails, 42501.
	cfg := f.cfg
	cfg.Username, cfg.Password = role, "sls-test-pw"
	if err := f.sink(cfg).Write(ctx, batchN(t, 1)); err == nil || !strings.Contains(err.Error(), "sqlstate=42501") {
		t.Fatalf("a write with INSERT alone = %v, want 42501", err)
	}
	f.exec("GRANT SELECT (uuid) ON " + f.table + " TO " + role)
	if err := try(); err != nil {
		t.Fatalf("a role with INSERT and SELECT (uuid) was refused: %v", err)
	}
}

// SPEC §10.5 and invariant §20.18: a write against a table dropped after boot is
// class 42, FATAL, never logic -- logic would drop this chunk and every
// following one, quietly.
func TestPostgresDroppedTableIsFatalNotLogic(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	f.exec("DROP TABLE " + f.table)
	err := p.Write(context.Background(), batchN(t, 1))
	if err == nil {
		t.Fatal("Write into a dropped table succeeded")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "sqlstate=42") {
		t.Errorf("err = %v (class %s), want fatal class 42", err, ClassOf(err))
	}
}

// A wrong password is fatal at the first check (class 28), not retried until
// boot_timeout.
//
// The server takes a connection slot BEFORE it checks the password: with the
// whole suite in parallel against one PostgreSQL (max_connections 100), the
// login can get 53300 "too many clients" -- network, correctly -- and the test
// failed now and then (the suspected flake of the review of 2026-10-13). A
// 53300 is waited out here; any other answer decides.
func TestPostgresWrongPasswordIsFatal(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.Password = "definitely-wrong"
	p := f.sink(cfg)
	var err error
	for deadline := time.Now().Add(20 * time.Second); ; {
		err = p.Health(context.Background())
		if err == nil || !strings.Contains(err.Error(), "sqlstate=53300") || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !IsFatal(err) {
		t.Fatalf("Health with a wrong password = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// The whole drain path against the real server. The queue takes no promoted
// columns (SPEC §3.7.1): the hostname is read from the jsonb payload.
func TestPostgresWorkersEndToEnd(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	rg := ring.NewMemory(1000, 0, time.Millisecond)
	m := metrics()
	cfg := workerCfg(64)
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{}, discard(), m), cfg, discard(), m)
	items := itemsOf(lines(300)...)
	enqueueAll(t, rg, items)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 30*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 300 })
	cancel()
	w.Wait()

	var n, hosts int
	if err := f.conn.QueryRow(context.Background(),
		"SELECT COUNT(*), COUNT(payload->'host'->>'hostname') FROM "+f.table).Scan(&n, &hosts); err != nil {
		t.Fatal(err)
	}
	if n != 300 || hosts != 300 {
		t.Fatalf("%d rows, %d with a hostname in the payload; want 300 and 300", n, hosts)
	}
}

// Review finding 4 of 2026-10-06: a partitioned queue passed the validation,
// and a row with no partition was 23514, a logic drop of every chunk. It is
// refused at boot, and a missing partition after boot is fatal, not logic.
func TestPostgresPartitionedTableIsRefused(t *testing.T) {
	f := pgSetup(t)
	schema := strings.Split(f.table, ".")[0]
	f.exec(`CREATE TABLE ` + schema + `.parted (LIKE ` + f.table + ` INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
	  PRIMARY KEY (uuid)) PARTITION BY RANGE (uuid)`)
	cfg := f.cfg
	cfg.Table = schema + ".parted"
	err := f.sink(cfg).Health(context.Background())
	if err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "partitioned") {
		t.Fatalf("Health on a partitioned table = %v, want fatal", err)
	}

	// The same failure at write time, on a plain table turned into a
	// partition-less parent behind the sink's back, is fatal.
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec(`DROP TABLE ` + f.table + `; CREATE TABLE ` + f.table + ` (LIKE ` + schema + `.parted INCLUDING ALL) PARTITION BY RANGE (uuid)`)
	err = p.Write(context.Background(), batchN(t, 1))
	if err == nil || !IsFatal(err) {
		t.Fatalf("Write with no partition = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// Review finding 3: a non-UTF8 database stored the JLH's text double-encoded,
// silently, or refused an escape as 22P05. It is refused at boot.
func TestPostgresNonUTF8DatabaseIsRefused(t *testing.T) {
	f := pgSetup(t)
	db := fmt.Sprintf("sls_latin1_%d", time.Now().UnixNano())
	ctx := context.Background()
	if _, err := f.conn.Exec(ctx, "CREATE DATABASE "+db+" ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"); err != nil {
		t.Skipf("cannot create databases here: %v", err)
	}
	t.Cleanup(func() { f.exec("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)") })
	cfg := f.cfg
	cfg.Database = db
	err := f.sink(cfg).Health(ctx)
	if err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("Health on a LATIN1 database = %v, want fatal naming the encoding", err)
	}
}

// Review finding 8: the read-only check had no test. A role whose sessions
// default to read-only is a replica as far as the sink is concerned.
func TestPostgresReadOnlySessionIsFatal(t *testing.T) {
	f := pgSetup(t)
	role := fmt.Sprintf("sls_ro_%d", time.Now().UnixNano())
	ctx := context.Background()
	if _, err := f.conn.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD 'sls-test-pw'"); err != nil {
		t.Skipf("cannot create roles on this server: %v", err)
	}
	t.Cleanup(func() { f.exec("DROP OWNED BY " + role + "; DROP ROLE IF EXISTS " + role) })
	f.exec("GRANT USAGE ON SCHEMA " + strings.Split(f.table, ".")[0] + " TO " + role +
		"; GRANT INSERT ON " + f.table + " TO " + role +
		"; ALTER ROLE " + role + " SET default_transaction_read_only = on")
	cfg := f.cfg
	cfg.Username, cfg.Password = role, "sls-test-pw"
	err := f.sink(cfg).Health(ctx)
	if err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Health with a read-only session = %v, want fatal", err)
	}
}

// SPEC §12.2: what the boot reingests from an emergency dump is not a chunk of
// the workers. Each dumped line is parsed back with emergency.ParseLine and
// becomes an Envelope whose Item carries only the head fields the queue reads
// -- uuid, from, space, event -- and no raw line, built as internal/app builds
// it; Instrumented.Deliver then writes it, in chunks. Nothing converts the
// message again, so every column must come from those head fields and from the
// Level and SID the line declared. The rows must rebuild into the JLH, and be
// the very rows the workers' path writes for the same messages.
func TestPostgresWritesReingestedEnvelopes(t *testing.T) {
	f := pgSetup(t)
	ctx := context.Background()
	p := f.sink(f.cfg)
	f.ready(p)
	cfgW := f.cfg
	cfgW.Table = f.newTable("")
	pw := f.sink(cfgW)
	f.ready(pw)

	unix := newItem("<13>no header at all")
	unix.Head.Transport, unix.Head.Framing = item.TransportUnix, item.FramingDatagram
	b := batchFrom(t, newItem(line5424), newItem(line3164), newItem("<13>Oct 11 22:14:15 web\x00-01 su[230]: a\x00b"),
		unix, newItemIn("com.acme.store", "<11>1 - h a - - - in another space"), newItem(lineMalformed))
	if err := pw.Write(ctx, b); err != nil {
		t.Fatalf("the workers' path: %v", err)
	}

	envs := make([]Envelope, len(b.Envelopes))
	levels := map[uint8]bool{}
	for i := range b.Envelopes {
		// A dumped line is the JLH in a buffer of its own: the chunk's buffer
		// is long gone when the next boot reads the file.
		line := append([]byte(nil), b.Envelopes[i].JLH...)
		r, err := emergency.ParseLine(line)
		if err != nil {
			t.Fatalf("envelope %d does not parse back as a dump line: %v\n%s", i, err, line)
		}
		h := item.Head{UUID: r.UUID, From: r.From, Space: r.Space, UTS: r.Event.UTS, Sub: r.Event.Sub}
		envs[i] = Envelope{Item: item.New(h, nil, -1), JLH: r.JLH, Level: r.Level, SID: r.SID}
		levels[r.Level] = true
	}
	if len(levels) < 3 {
		t.Fatalf("only %d distinct levels: the messages do not exercise the level column", len(levels))
	}

	m := metrics()
	d := NewInstrumented(p, nil, workerCfg(2), discard(), m) // three chunks: chunking is part of the path
	failOnWait := func(attempt int, err error) error {
		t.Errorf("wait(%d, %v): a healthy server failed a write", attempt, err)
		return err
	}
	if err := d.Deliver(ctx, envs, 10*time.Second, failOnWait); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if n := f.count(f.table); n != len(envs) {
		t.Fatalf("%d rows, want %d", n, len(envs))
	}
	if m.EventsWrittenTotal.Load() != uint64(len(envs)) || m.SinkBatchSize.Count() != 3 {
		t.Errorf("events_written %d, chunks %d; want %d and 3", m.EventsWrittenTotal.Load(),
			m.SinkBatchSize.Count(), len(envs))
	}
	for i := range envs {
		f.checkRow(f.table, &envs[i])
	}

	// Row for row what the workers wrote, received_at aside.
	rows := func(table string) string {
		var s string
		if err := f.conn.QueryRow(ctx, `SELECT jsonb_agg(to_jsonb(t) - 'received_at' ORDER BY uuid)::text FROM `+
			table+` t`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	if got, want := rows(f.table), rows(cfgW.Table); got != want || !strings.Contains(got, `"level": 2`) {
		t.Errorf("reingested rows differ from the workers' rows:\n%s\nwant\n%s", got, want)
	}

	// A boot that crashed after writing and before removing the dump writes it
	// again: ON CONFLICT (uuid) adds nothing, and the redelivery is counted.
	if err := d.Deliver(ctx, envs, 10*time.Second, failOnWait); err != nil {
		t.Fatalf("redelivered Deliver: %v", err)
	}
	if n := f.count(f.table); n != len(envs) {
		t.Errorf("%d rows after the redelivery, want %d", n, len(envs))
	}
	if got := p.m.SinkPostgresDuplicatesTotal.Load(); got != uint64(len(envs)) {
		t.Errorf("duplicates counter = %d, want %d", got, len(envs))
	}
}

// SPEC §1.2: the sink speaks plaintext whatever the libpq environment says.
// PGSSLMODE=require makes a libpq client refuse a server without SSL, and
// encrypt with one that has it; the sink must do neither: it validates, writes,
// and the session it got is not encrypted (pg_stat_ssl). The control is the
// unit test TestNewPostgresIgnoresTheLibpqEnvironment, which proves the same
// environment does yield a TLSConfig in pgx.
func TestPostgresIgnoresPGSSLMODE(t *testing.T) {
	f := pgSetup(t)
	t.Setenv("PGSSLMODE", "require")
	t.Setenv("PGSSLNEGOTIATION", "direct")
	p := f.sink(f.cfg)
	f.ready(p)
	ctx := context.Background()
	if err := p.Write(ctx, batchN(t, 2)); err != nil {
		t.Fatalf("Write with PGSSLMODE=require: %v", err)
	}
	var ssl bool
	if err := p.pool.QueryRow(ctx,
		"SELECT COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false)").Scan(&ssl); err != nil {
		t.Fatal(err)
	}
	if ssl {
		t.Error("the sink's session is encrypted: PGSSLMODE reached the dial")
	}
	if n := f.count(f.table); n != 2 {
		t.Errorf("%d rows, want 2", n)
	}
}
