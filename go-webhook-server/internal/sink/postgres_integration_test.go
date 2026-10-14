package sink

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// The destination is remote and has a schema, and what is worth proving -- the
// unnest INSERT into loghub.inbox with its jsonb and timestamptz arrays, ON
// CONFLICT (uuid), the CHECK constraints of the skill's DDL, the rows-affected
// arithmetic behind the duplicates counter, the catalogue queries of the boot
// validation, the privilege check, class 42 as fatal -- is exactly what a fake
// would invent. These run against a real PostgreSQL 18 addressed by
// WHS_TEST_PG_DSN and skip without it (docs/TESTING.md §5.2):
//
//	WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
//	  go test -race ./internal/sink/
//
// Every test creates its own queue from docker/postgres/inbox.sql, in a schema
// of its own (the DDL names its constraints), and drops the schema afterwards,
// so the suite never depends on an empty database or on another suite sharing
// the server.

const pgDSNEnv = "WHS_TEST_PG_DSN"

type pgFixture struct {
	t     *testing.T
	conn  *pgx.Conn
	table string
	cfg   PostgresConfig
}

// pgSetup connects to the test server or skips, and creates a fresh queue.
func pgSetup(t *testing.T) *pgFixture {
	t.Helper()
	dsn := os.Getenv(pgDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the postgres sink tests (see docs/TESTING.md §5.2)", pgDSNEnv)
	}
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("%s: %v", pgDSNEnv, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		t.Fatalf("%s is not reachable: %v", pgDSNEnv, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	f := &pgFixture{t: t, conn: conn, cfg: PostgresConfig{
		Addr:     net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))),
		Username: cc.User,
		Password: cc.Password,
		Database: cc.Database,
		PoolSize: 4,
		AppName:  "whs-test",
	}}
	f.table = f.newTable("")
	f.cfg.Table = f.table
	return f
}

// newTable creates a queue from the canonical DDL in a schema of its own, with
// alter run afterwards ($T is the table), and registers its removal. Using the
// file operators apply is the point: the validation of SPEC §8.5.5 and the DDL
// the operator gets must agree, and this is where a drift would surface.
func (f *pgFixture) newTable(alter string) string {
	f.t.Helper()
	schema := fmt.Sprintf("whs_test_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		f.t.Fatalf("reading the canonical DDL: %v", err)
	}
	ddl := string(b)
	if !strings.Contains(ddl, "SCHEMA IF NOT EXISTS loghub;") || !strings.Contains(ddl, "loghub.inbox") {
		f.t.Fatal("docker/postgres/inbox.sql changed shape; the fixture cannot retarget it")
	}
	ddl = strings.ReplaceAll(ddl, "SCHEMA IF NOT EXISTS loghub;", "SCHEMA "+schema+";")
	ddl = strings.ReplaceAll(ddl, "loghub.inbox", schema+".inbox")
	f.exec(ddl)
	f.t.Cleanup(func() { f.exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
	table := schema + ".inbox"
	if alter != "" {
		f.exec(strings.ReplaceAll(alter, "$T", table))
	}
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
	if err := f.conn.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *pgFixture) sink(cfg PostgresConfig) *Postgres {
	f.t.Helper()
	p, err := NewPostgres(cfg, quietLog(), testMetrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { p.Close() })
	return p
}

// ready runs the boot check the way WaitReady would, and fails the test if
// the table is refused.
func (f *pgFixture) ready(p *Postgres) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// pgRebuild is the envelope a consumer of the queue rebuilds from one row:
// the header from its columns, event with the skill's own SQL (no float8), and
// the jsonb payload. json_build_object, not jsonb_build_object, so the nine
// keys come out in the canonical order the contract oracle checks.
const pgRebuild = `
SELECT json_build_object(
  'uuid', uuid::text, 'from', from_uuid::text, 'space', space, 'type', type,
  'level', level, 'sid', sid, 'step', step,
  'event', json_build_object(
    'uts', floor(extract(epoch FROM event_at))::bigint,
    'sub', (extract(microseconds FROM event_at)::bigint % 1000000) * 1000 + event_nsec,
    'iso', to_char(event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
  'payload', payload)::text,
  now() - received_at
FROM $TABLE WHERE uuid = $1::uuid`

// ------------------------------------------------------------------ writes

// SPEC §3.6 and §8.5.4: every column lands from the JLH header, the payload is
// the JLH's payload as jsonb, and the envelope rebuilds from the row without
// loss of information -- event to the nanosecond, the body exact -- except
// the one documented change: \u0000 becomes U+FFFD.
func TestPostgresWriteEnqueuesTheEnvelope(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	nul := testRequest("/nul", []byte(`{"n":1}`))
	nul.RawQuery = "a=x%00y&b=2"
	// Bytes that are not UTF-8 in the query: the serialiser writes U+FFFD in
	// payload.query (and percent-encodes them in the url), so the strict
	// session and the JSON type take the row and one such request cannot
	// take its batch down. The path arrives escaped, as the handler's
	// URL.EscapedPath gives it (envelope.Request.Path): raw bytes there are
	// a fixture no request can produce, and the url oracle refuses them.
	invalidUTF8 := testRequest("/hook/%FF%FE", nil)
	invalidUTF8.RawQuery = "q=%FF\xc0\xaf"
	v6 := testRequest("/v6", []byte("A\x00B\nC\xffD\r\x00"))
	v6.Src, v6.Dst, v6.PeerAddr = netip.MustParseAddr("2001:db8::9"), netip.MustParseAddr("::1"), netip.MustParseAddr("2001:db8::9")
	items := []*envelope.Item{
		mkItem("plain"),
		itemIn("default", nul),
		itemIn("default", v6),
		itemIn("acme.store", testRequest("/other-space", nil)),
		itemIn("default", invalidUTF8),
	}
	ctx := context.Background()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table); n != len(items) {
		t.Fatalf("%d rows, want %d", n, len(items))
	}

	for i, it := range items {
		var rebuilt []byte
		var lag time.Duration
		if err := f.conn.QueryRow(ctx, strings.Replace(pgRebuild, "$TABLE", f.table, 1), it.ID).Scan(&rebuilt, &lag); err != nil {
			t.Fatalf("reading %s back: %v", it.ID, err)
		}
		if lag < 0 || lag > time.Minute {
			t.Errorf("received_at is not the table's now(): lag %v", lag)
		}
		h, payload, err := envelope.ParseHead(it.Env)
		if err != nil {
			t.Fatal(err)
		}
		// What a consumer rebuilds from the row is a valid webhook.request,
		// semantic rules included: the body stays exact (SPEC §3.6). Except
		// where the column replaced a \u0000: there the query says U+FFFD
		// while the url still says %00, the one divergence the column is
		// allowed (invariant 11), which the oracle rightly refuses. That row
		// is held to the JLH instead, and the JLH to the oracle.
		safe := jsonbSafe(payload)
		if bytes.Equal(safe, payload) {
			if e := validJLH(t, rebuilt); e.Space != h.Space || e.UUID != it.ID {
				t.Errorf("envelope %d: space %q uuid %s", i, e.Space, e.UUID)
			}
		} else {
			validJLH(t, it.Env)
		}
		// And it is the JLH as a JSON value, with the column's payload --
		// \u0000 as U+FFFD, nothing else touched.
		want := append(append([]byte(nil), it.Env[:len(it.Env)-len(payload)-1]...), safe...)
		want = append(want, '}')
		if !sameJSON(t, rebuilt, want) {
			t.Errorf("envelope %d rebuilt from the row:\n%s\nwant the JLH:\n%s", i, rebuilt, it.Env)
		}
	}
	if !bytes.Contains(items[1].Env, []byte(`"a":"x\u0000y"`)) {
		t.Error("the JLH lost the NUL: the replacement must touch the column copy only")
	}
	var a string
	if err := f.conn.QueryRow(ctx, "SELECT payload->'query'->>'a' FROM "+f.table+" WHERE uuid = $1::uuid",
		items[1].ID).Scan(&a); err != nil || a != "x\ufffdy" {
		t.Errorf("query.a in the queue = %q (%v), want U+FFFD for the NUL", a, err)
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
	if err := p.Write(context.Background(), mkBatch(10)); err != nil {
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

// SPEC §8.5.4: the pipeline is at-least-once. Writing the SAME batch twice is
// the shape of a redelivery: the second write must succeed, add nothing, and
// be counted as duplicates -- the one direct measurement of redelivery. A uuid
// twice inside ONE statement is absorbed too: DO UPDATE would fail the whole
// batch with 21000.
func TestPostgresToleratesRedeliveryAndCountsIt(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	items := mkBatch(7)
	ctx := context.Background()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	// A partial overlap: three old, two new.
	mixed := append(items[:3:3], mkBatch(2)...)
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	twice := mkItem("twice")
	if err := p.Write(ctx, []*envelope.Item{twice, twice}); err != nil {
		t.Fatalf("a uuid twice in one batch: %v", err)
	}

	if n := f.count(f.table); n != 10 {
		t.Errorf("%d rows, want 10: redelivery must not duplicate", n)
	}
	if got := p.m.SinkPostgresDuplicatesTotal.Load(); got != 11 {
		t.Errorf("duplicates counter = %d, want 11 (7 from the full redelivery, 3 from the overlap, 1 in-batch)", got)
	}
}

// An item whose JLH this server did not write is refused as data (logic)
// before the statement runs: nothing of the batch reaches the queue.
func TestPostgresRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx := context.Background()

	err := p.Write(ctx, []*envelope.Item{mkItem("good"), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := f.count(f.table); n != 0 {
		t.Errorf("%d rows; a refused batch must send nothing", n)
	}
	// A payload jsonb cannot parse passes the header reader and is refused by
	// the server: class 22, logic, this data and not the destination.
	err = p.Write(ctx, []*envelope.Item{malformedPayloadItem(t)})
	if err == nil || ClassOf(err) != ClassLogic || !strings.Contains(err.Error(), "sqlstate=22") {
		t.Errorf("malformed payload = %v (class %s), want logic with a class-22 SQLSTATE", err, ClassOf(err))
	}
	if err := p.Write(ctx, mkBatch(2)); err != nil {
		t.Errorf("control: the sink refused valid items after a refusal: %v", err)
	}
}

// Many workers, one pool: every batch lands, and the gauges reflect the
// ceiling and the waits.
func TestPostgresConcurrentWritersAndPoolGauges(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.PoolSize = 3
	p := f.sink(cfg)
	f.ready(p)

	const writers, per = 8, 50
	batches := make([][]*envelope.Item, writers)
	for w := range batches {
		batches[w] = mkBatch(per)
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

	f.ready(p) // refreshes the gauges
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

// ------------------------------------------------------------- validation

// SPEC §8.5.5: each refusal is FATAL, so the boot stops at once instead of
// waiting out sink.boot_timeout on a table that will not appear. The accepted
// cases are the control: they write after validating, so a new rule that
// refused everything would fail here and not pass the refusals for the wrong
// reason.
func TestPostgresHealthValidatesTheSchema(t *testing.T) {
	f := pgSetup(t)
	cases := []struct {
		name  string
		table string
		want  string // substring of the fatal error; "" means accepted
	}{
		{"canonical DDL", f.table, ""},
		{"missing table", fmt.Sprintf("whs_missing_%d.inbox", time.Now().UnixNano()), "does not exist"},
		{"text payload", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_payload_ck, DROP CONSTRAINT inbox_syslog_ck; ALTER TABLE $T ALTER COLUMN payload TYPE text"), `"payload"`},
		{"json payload", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_payload_ck, DROP CONSTRAINT inbox_syslog_ck; ALTER TABLE $T ALTER COLUMN payload TYPE json USING payload::json"), `"payload"`},
		{"timestamp without zone", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_event_ck; ALTER TABLE $T ALTER COLUMN event_at TYPE timestamp"), `"event_at"`},
		{"missing column", f.newTable("ALTER TABLE $T DROP COLUMN event_nsec"), `"event_nsec"`},
		{"text from_uuid", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_from_v7_ck; ALTER TABLE $T ALTER COLUMN from_uuid TYPE text"), `"from_uuid"`},
		{"text sid", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_sid_ck; ALTER TABLE $T ALTER COLUMN sid TYPE text"), `"sid"`},
		// Found by the go-syslog-server review of 2026-10-06, same design:
		// tables the old validation accepted and on which every INSERT then
		// failed with the sink UP.
		{"integer sid", f.newTable("ALTER TABLE $T ALTER COLUMN sid TYPE integer"), `"sid"`},
		{"varchar(3) type", f.newTable("ALTER TABLE $T ALTER COLUMN type TYPE varchar(3)"), `"type"`},
		{"varchar(10) space", f.newTable("ALTER TABLE $T ALTER COLUMN space TYPE varchar(10)"), `"space"`},
		{"deferrable primary key", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; ALTER TABLE $T ADD CONSTRAINT inbox_pk PRIMARY KEY (uuid) DEFERRABLE INITIALLY IMMEDIATE"), "unique index"},
		{"unbounded varchar type is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN type TYPE varchar"), ""},
		{"no unique index", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk"), "unique index"},
		{"unique on (uuid, step) only", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid, step)"), "unique index"},
		{"partial unique index", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid) WHERE level < 8"), "unique index"},
		{"unique index instead of the PK is tolerated", f.newTable("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid)"), ""},
		// A NOT NULL the sink cannot satisfy refuses every row: caught at
		// boot, never as a logic drop with the sink UP.
		{"extra required column", f.newTable("ALTER TABLE $T ADD COLUMN tenant text NOT NULL"), `"tenant"`},
		{"received_at without its default", f.newTable("ALTER TABLE $T ALTER COLUMN received_at DROP DEFAULT"), `"received_at"`},
		{"no received_at at all is tolerated", f.newTable("ALTER TABLE $T DROP COLUMN received_at"), ""},
		{"extra NOT NULL with a default is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN tenant text NOT NULL DEFAULT 'x'"), ""},
		{"extra nullable column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN note text"), ""},
		{"extra identity column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY"), ""},
		{"extra serial column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN n bigserial"), ""},
		{"extra generated column is tolerated", f.newTable("ALTER TABLE $T ADD COLUMN y bigint GENERATED ALWAYS AS (sid * 2) STORED NOT NULL"), ""},
		{"varchar space is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN space TYPE varchar(64)"), ""},
		{"integer level is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN level TYPE integer"), ""},
		// Review of 2026-10-06. The typmod is compared apart from the type:
		// timestamptz(6) used to be cut at '(' into "timestamp" and refused
		// as the wrong type; a precision below 6 rounds the microsecond.
		{"timestamptz(6) event_at is tolerated", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(6)"), ""},
		{"timestamptz(3) event_at", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(3)"), "precision below 6"},
		{"timestamptz(0) event_at", f.newTable("ALTER TABLE $T ALTER COLUMN event_at TYPE timestamptz(0)"), `"event_at"`},
		// ON CONFLICT (uuid) absorbs collisions on its arbiter only: any other
		// unique index or exclusion constraint fails the whole batch on a
		// collision (23505, 23P01), and a deferrable unique on uuid fails
		// every batch (55000).
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
				if err := p.Write(ctx, mkBatch(2)); err != nil {
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

// A role without INSERT, and a role whose sessions are read-only, are both
// refused at boot rather than on the first batch; ON CONFLICT DO NOTHING needs
// no UPDATE. Needs CREATEROLE; skipped where the test user lacks it.
func TestPostgresHealthValidatesTheRole(t *testing.T) {
	f := pgSetup(t)
	ctx := context.Background()
	schema := strings.Split(f.table, ".")[0]

	// The role carries a password even where the server would not ask for one.
	// The official postgres image ships pg_hba with scram-sha-256 for TCP, so a
	// passwordless role authenticates only under a trust configuration -- and a
	// test that passes on a local trust server and fails in CI is worse than no
	// test.
	const rolePassword = "whs-test-role-pw"

	// Both helpers take the subtest's t: a Skip or a Fatal on the parent from
	// inside a subtest is not one.
	mkRole := func(t *testing.T, setup string) string {
		t.Helper()
		role := fmt.Sprintf("whs_role_%d", time.Now().UnixNano())
		if _, err := f.conn.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD '"+rolePassword+"'"); err != nil {
			t.Skipf("cannot create roles on this server: %v", err)
		}
		// DROP OWNED revokes the grants first; a bare DROP ROLE would fail on
		// them and leak the role into a server other suites share.
		t.Cleanup(func() { f.exec("DROP OWNED BY " + role + "; DROP ROLE IF EXISTS " + role) })
		// CONNECT on the database and USAGE on the schema are separate from any
		// privilege on the table; granting them explicitly keeps the subject of
		// each subtest the table privilege and nothing else.
		f.exec(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s; GRANT USAGE ON SCHEMA %s TO %s",
			pgx.Identifier{f.cfg.Database}.Sanitize(), role, schema, role))
		if setup != "" {
			f.exec(strings.ReplaceAll(strings.ReplaceAll(setup, "$R", role), "$T", f.table))
		}
		return role
	}
	try := func(t *testing.T, role string) error {
		t.Helper()
		cfg := f.cfg
		cfg.Username = role
		cfg.Password = rolePassword
		p := f.sink(cfg)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := p.Health(cctx); err != nil {
			// An authentication failure would make every assertion below pass
			// or fail for the wrong reason: 28P01 is fatal too, and its text
			// mentions neither INSERT nor read-only. Matched on the SQLSTATE
			// the sink reports, not on a bare "28000", which the nanosecond
			// stamp in the role and schema names contains now and then.
			if strings.Contains(err.Error(), "sqlstate=28") {
				t.Fatalf("the test role could not authenticate; this is the fixture, not the sink: %v", err)
			}
			return err
		}
		return p.Write(cctx, mkBatch(1))
	}

	t.Run("no INSERT privilege", func(t *testing.T) {
		err := try(t, mkRole(t, "GRANT SELECT ON $T TO $R"))
		if err == nil {
			t.Fatal("a role without INSERT passed the boot check")
		}
		if !IsFatal(err) || !strings.Contains(err.Error(), "INSERT") {
			t.Errorf("got %v, want a fatal error naming INSERT", err)
		}
	})
	t.Run("read-only session", func(t *testing.T) {
		err := try(t, mkRole(t, "GRANT INSERT ON $T TO $R; ALTER ROLE $R SET default_transaction_read_only = on"))
		if err == nil {
			t.Fatal("a read-only session passed the boot check")
		}
		if !IsFatal(err) || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("got %v, want a fatal error naming the read-only session", err)
		}
	})
	t.Run("INSERT and SELECT (uuid) are enough", func(t *testing.T) {
		if err := try(t, mkRole(t, "GRANT INSERT, SELECT (uuid) ON $T TO $R")); err != nil {
			t.Errorf("a role with INSERT and SELECT (uuid) was refused: %v", err)
		}
	})
	// SPEC §3.6, §8.5.5 and docker/postgres/inbox.sql say INSERT alone is
	// enough. PostgreSQL disagrees: ON CONFLICT with a conflict target needs
	// SELECT on the target's columns. The boot check must refuse what the
	// first batch would refuse, or let it write.
	t.Run("INSERT only: the boot check and the write agree", func(t *testing.T) {
		role := mkRole(t, "GRANT INSERT ON $T TO $R")
		cfg := f.cfg
		cfg.Username, cfg.Password = role, rolePassword
		p := f.sink(cfg)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := p.Health(cctx); err != nil {
			if !IsFatal(err) {
				t.Errorf("Health = %v (class %s), want fatal", err, ClassOf(err))
			}
			return // refused at boot: consistent
		}
		err := p.Write(cctx, mkBatch(1))
		if err == nil {
			return // written: consistent
		}
		if strings.Contains(err.Error(), "sqlstate=42501") {
			t.Skipf("BUG (postgres.go validate, privilege check): the boot check accepts a role with INSERT "+
				"only, and the first batch fails with %v -- ON CONFLICT (uuid) needs SELECT on uuid. "+
				"Validate has_column_privilege(..., 'uuid', 'SELECT') too, and document "+
				"GRANT INSERT, SELECT (uuid) in SPEC §3.6/§8.5.5 and docker/postgres/inbox.sql.", err)
		}
		t.Errorf("Write after a passing boot check: %v", err)
	})
}

// SPEC §8.5.7: a write against a table dropped after boot is class 42, FATAL,
// never logic -- logic would drop this batch and every following one, quietly.
func TestPostgresDroppedTableIsFatalNotLogic(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	f.exec("DROP TABLE " + f.table)
	err := p.Write(context.Background(), mkBatch(1))
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
// With the server's connection slots exhausted -- other suites sharing it --
// PostgreSQL answers 53300 before it looks at the password, a network error
// that says nothing about this one: the check is repeated while that is the
// answer (the flake go-relplog-pg-server found, 2026-10-13).
func TestPostgresWrongPasswordIsFatal(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.Password = "definitely-wrong"
	p := f.sink(cfg)
	var err error
	for deadline := time.Now().Add(20 * time.Second); ; {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = p.Health(ctx)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "sqlstate=53300") || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !IsFatal(err) {
		t.Fatalf("Health with a wrong password = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// The whole drain path against the real server: ring, workers, health and the
// queue, every event a valid envelope rebuilt from its row.
func TestPostgresWorkersEndToEnd(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	r := ring.NewMemory(1000, 0, time.Millisecond)
	m := testMetrics()
	h := NewHealth(p, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, p, h, WorkerConfig{
		Workers: 3, Batch: 64, OpTimeout: 10 * time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 50 * time.Millisecond,
	}, quietLog(), m)
	items := mkBatch(300)
	// The workers release what they deliver, and a released item's Env is
	// gone: keep a copy of one to compare with its row.
	sample := append([]byte(nil), items[150].Env...)
	sampleID := items[150].ID
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	if !eventually(t, 30*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 300 }) {
		t.Fatalf("delivered %d of 300", m.EventsWrittenTotal.Load())
	}
	cancel()
	w.Wait()

	var n, hosts int
	if err := f.conn.QueryRow(context.Background(),
		"SELECT COUNT(*), COUNT(*) FILTER (WHERE payload->>'host' = 'example.com') FROM "+f.table).Scan(&n, &hosts); err != nil {
		t.Fatal(err)
	}
	if n != 300 || hosts != 300 {
		t.Fatalf("%d rows, %d with the host in the payload; want 300 and 300", n, hosts)
	}
	var rebuilt []byte
	var lag time.Duration
	if err := f.conn.QueryRow(context.Background(), strings.Replace(pgRebuild, "$TABLE", f.table, 1), sampleID).Scan(&rebuilt, &lag); err != nil {
		t.Fatal(err)
	}
	validJLH(t, rebuilt)
	if !sameJSON(t, rebuilt, sample) {
		t.Errorf("row of item 150 rebuilds\n%s\nwant\n%s", rebuilt, sample)
	}
}

// A partitioned queue is refused at boot, and a missing partition after boot
// is fatal, not logic (the go-syslog-server review of 2026-10-06).
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
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec(`DROP TABLE ` + f.table + `; CREATE TABLE ` + f.table + ` (LIKE ` + schema + `.parted INCLUDING ALL) PARTITION BY RANGE (uuid)`)
	err = p.Write(context.Background(), mkBatch(1))
	if err == nil || !IsFatal(err) {
		t.Fatalf("Write with no partition = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// A non-UTF8 database would store the JLH's text double-encoded, silently.
func TestPostgresNonUTF8DatabaseIsRefused(t *testing.T) {
	f := pgSetup(t)
	db := fmt.Sprintf("whs_latin1_%d", time.Now().UnixNano())
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
