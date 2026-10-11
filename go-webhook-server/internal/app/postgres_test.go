package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/jlhcheck"
)

// sink=postgres completes the matrix of SPEC §16.1. These run against a real
// PostgreSQL 18 addressed by WHS_TEST_PG_DSN and skip without it:
//
//	WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
//	  go test -race -timeout 900s ./internal/app/
//
// The destination is the loghub.inbox queue (SPEC §3.6). Each test creates its
// own from docker/postgres/inbox.sql -- the skill's DDL, which names its
// constraints -- in a schema of its own, and drops the schema afterwards, so
// the suite never depends on an empty database and never meets another
// suite's queue.

const pgDSNEnv = "WHS_TEST_PG_DSN"

type pgEnv struct {
	t                    *testing.T
	conn                 *pgx.Conn
	addr, user, pass, db string
}

func pgTest(t *testing.T) *pgEnv {
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
	return &pgEnv{
		t: t, conn: conn,
		addr: net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))),
		user: cc.User, pass: cc.Password, db: cc.Database,
	}
}

// table creates a fresh queue from the canonical DDL in a schema of its own
// and returns its name, <schema>.inbox. alter, when set, runs afterwards with
// $T standing for the table. Applying the operators' own file is the point:
// the boot validation of SPEC §8.5.5 and the DDL the operator gets must agree.
func (e *pgEnv) table(alter ...string) string {
	e.t.Helper()
	schema := fmt.Sprintf("whs_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		e.t.Fatalf("reading the canonical DDL: %v", err)
	}
	ddl := strings.ReplaceAll(string(b), "SCHEMA IF NOT EXISTS loghub;", "SCHEMA "+schema+";")
	ddl = strings.ReplaceAll(ddl, "loghub.inbox", schema+".inbox")
	e.t.Cleanup(func() { e.exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })
	e.exec(ddl)
	name := schema + ".inbox"
	for _, a := range alter {
		e.exec(strings.ReplaceAll(a, "$T", name))
	}
	return name
}

// exec runs SQL by the simple protocol, which takes several statements.
func (e *pgEnv) exec(sql string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := e.conn.PgConn().Exec(ctx, sql).ReadAll(); err != nil {
		e.t.Fatalf("%v\n%s", err, sql)
	}
}

// envelopes rebuilds every envelope in the queue with the skill's own SQL --
// what a consumer of loghub.inbox runs -- in uuid order. jsonb keeps no key
// order, so the nine top-level fields are put back in the canonical order the
// JLH fixes (SPEC §3.1) before the oracle sees them.
func (e *pgEnv) envelopes(table string) [][]byte {
	e.t.Helper()
	rows, err := e.conn.Query(context.Background(), `
		SELECT jsonb_build_object(
		  'uuid', uuid::text, 'from', from_uuid::text, 'space', space, 'type', type,
		  'level', level, 'sid', sid, 'step', step,
		  'event', jsonb_build_object(
		    'uts', floor(extract(epoch FROM event_at))::bigint,
		    'sub', (extract(microseconds FROM event_at)::bigint % 1000000) * 1000 + event_nsec,
		    'iso', to_char(event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),
		  'payload', payload)::text
		FROM `+table+` ORDER BY uuid`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var rebuilt string
		if err := rows.Scan(&rebuilt); err != nil {
			e.t.Fatal(err)
		}
		var c canonical
		if err := json.Unmarshal([]byte(rebuilt), &c); err != nil {
			e.t.Fatalf("the row does not rebuild into an envelope: %v\n%s", err, rebuilt)
		}
		out = append(out, c.bytes(e.t))
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// ids reads every uuid in the queue, each envelope checked by the oracle.
func (e *pgEnv) ids(table string, id ident.Identity, mode string) []string {
	e.t.Helper()
	return uuidsOf(verifyJSONB(e.t, e.envelopes(table), id, mode))
}

// verifyJSONB is verify for rows of loghub.inbox: the payload column is the
// jsonb copy, where a NUL of the query is U+FFFD (invariant 11, SPEC §17), so
// the oracle allows that one substitution against the url and nothing else.
func verifyJSONB(t *testing.T, raws [][]byte, id ident.Identity, mode string) []*jlhcheck.Envelope {
	t.Helper()
	return verifyWith(t, jlhcheck.ValidateJSONBCopy, raws, id, mode)
}

func (e *pgEnv) args(table string, extra ...string) []string {
	return append([]string{
		"--sink.type=postgres",
		"--sink.postgres.addr=" + e.addr,
		"--sink.postgres.username=" + e.user,
		"--sink.postgres.password=" + e.pass,
		"--sink.postgres.database=" + e.db,
		"--sink.postgres.table=" + table,
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §16.1: with sink=postgres every ring mode accepts traffic, reports the
// pair in whs_config_info, exits 0 and leaves every accepted uuid in the
// queue, each envelope with the mode its pair promises (SPEC §3.2).
func TestPostgresAllRingModes(t *testing.T) {
	e := pgTest(t)
	for _, tc := range realSinkModes {
		t.Run(tc.ring, func(t *testing.T) {
			table := e.table()
			in := start(t, t.TempDir(), e.args(table,
				"--ring.mode="+tc.ring,
				"--ring.sqlite.synchronous=OFF")...)

			const n = 40
			ids := in.postN(n)
			if len(ids) != n {
				t.Fatalf("%d of %d requests were accepted", len(ids), n)
			}
			if in.mode != tc.mode {
				t.Errorf("the 200 answered mode %q, want %q", in.mode, tc.mode)
			}
			m := in.metrics()
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="postgres"} 1`, tc.ring)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			got := e.ids(table, testID, tc.mode)
			if lost := missing(ids, got); len(lost) > 0 {
				t.Errorf("%d of %d accepted events never reached the queue", len(lost), len(ids))
			}
			if extra := missing(got, ids); len(extra) > 0 {
				t.Errorf("%d rows in the queue were never answered to a client", len(extra))
			}
		})
	}
}

// SPEC §11.1: ring=none writes through from the handler, and the wrapper of
// SPEC §10.2 must still observe every write and expose the pool gauges.
func TestPostgresRingNoneIsInstrumented(t *testing.T) {
	e := pgTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=none")...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	m := in.metrics()
	for _, want := range []string{
		"whs_events_written_total 5",
		"whs_sink_duration_seconds_count 5",
		"whs_sink_batch_size_count 5",
		`whs_sink_postgres_pool_conns{state="max"}`,
		"whs_sink_postgres_duplicates_total 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// --------------------------------------------------------- the wire contract

// SPEC §3.2 and §3.6 end to end through the queue: a binary body travels in
// base64 and an empty body as an empty content, both exact after jsonb; a
// query with %00 lands too -- jsonb refuses U+0000 (22P05, class 22, which
// would drop the whole batch as logic), so the copy in the payload column
// carries U+FFFD instead, and only there.
func TestPostgresNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	e := pgTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	binID, emptyID := postRoundTrip(in)
	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 2")
	}) {
		t.Error("the drain never reported both events as written")
	}
	if strings.Contains(in.metrics(), `whs_events_dropped_total{reason="sink_logic"} 1`) {
		t.Error("an event was dropped as sink_logic")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verifyJSONB(t, e.envelopes(table), testID, envelope.ModeFast)),
		binID, emptyID, true)
}

// SPEC §3.2: a request line carrying bytes that are not UTF-8 is a real
// event, and it must not take the rest of its batch down. The envelope writer
// turns them into valid JSON -- the path percent-encoded, the decoded query
// with U+FFFD -- so jsonb never sees an invalid string. Go's client would
// escape the bytes, so the request is written by hand. Only the path and the
// query carry them: net/http itself answers 400 to a Host header with bytes
// outside ASCII, before any handler runs.
func TestPostgresInvalidUTF8PathDoesNotDropTheBatch(t *testing.T) {
	e := pgTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	good := in.postN(3)
	badID := postInvalidUTF8(t, in)
	more := in.postN(3)

	// The drop counter is read while the admin port is still open, after the
	// drain has caught up; the queue check below is the stronger proof.
	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 7")
	}) {
		t.Error("the drain never reported all seven events as written")
	}
	if strings.Contains(in.metrics(), `whs_events_dropped_total{reason="sink_logic"} 1`) {
		t.Error("an event was dropped as sink_logic")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	got := byUUID(t, verifyJSONB(t, e.envelopes(table), testID, envelope.ModeFast))
	if lost := missing(append(append(good, badID), more...), keysOfEnv(got)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the queue; a class-22 refusal took the batch down", len(lost))
	}
	checkInvalidUTF8(t, got[badID])
}

// postInvalidUTF8 writes, by hand, a request whose path and query carry bytes
// that are not UTF-8, and returns the uuid it was answered with.
func postInvalidUTF8(t *testing.T, in *instance) string {
	t.Helper()
	c, err := net.Dial("tcp", in.pub)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /hook/\xff\xfe?x=\xc0 HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	a := checkAnswer(t, resp, b)
	if a.code != http.StatusOK {
		t.Fatalf("status = %d for a path that is not UTF-8; the server accepts it, so this is the sink", a.code)
	}
	return a.uuid
}

// checkInvalidUTF8 holds the envelope of postInvalidUTF8 to SPEC §3.2: the
// path is the escaped path, and the decoded query replaces what is not UTF-8.
func checkInvalidUTF8(t *testing.T, e *jlhcheck.Envelope) {
	t.Helper()
	if e == nil {
		return // reported as missing by the caller
	}
	if e.Payload.Path != "/hook/%FF%FE" {
		t.Errorf("path = %q, want the escaped path /hook/%%FF%%FE", e.Payload.Path)
	}
	if x, _ := e.Payload.Query["x"].(string); x != "�" {
		t.Errorf("query.x = %q, want U+FFFD for a byte that is not UTF-8", x)
	}
}

// --------------------------------------------------------------- boot checks

// SPEC §8.5.5 and §10.1: a queue that violates the contract stops the boot
// AT ONCE with exit 3 -- not after sink.boot_timeout -- and the public port
// never opens. The control is the DDL as shipped, which boots: a validation
// that refused everything would pass every other case.
func TestPostgresSchemaMismatchExitsThreeImmediately(t *testing.T) {
	e := pgTest(t)

	t.Run("control", func(t *testing.T) {
		in := start(t, t.TempDir(), e.args(e.table(), "--ring.mode=memory")...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("the canonical queue: exit code %d", code)
		}
	})

	cases := []struct {
		name  string
		table func() string
	}{
		{"missing table", func() string {
			return fmt.Sprintf("whs_missing_%d.inbox", time.Now().UnixNano())
		}},
		{"wrong column type", func() string {
			return e.table("ALTER TABLE $T DROP CONSTRAINT inbox_sid_ck; ALTER TABLE $T ALTER COLUMN sid TYPE text")
		}},
		{"no unique index", func() string {
			return e.table("ALTER TABLE $T DROP CONSTRAINT inbox_pk")
		}},
		{"unique index over (uuid, step)", func() string {
			return e.table("ALTER TABLE $T DROP CONSTRAINT inbox_pk; CREATE UNIQUE INDEX ON $T (uuid, step)")
		}},
		{"extra required column", func() string {
			return e.table("ALTER TABLE $T ADD COLUMN tenant text NOT NULL")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := startExpectingExit(t, t.TempDir(), e.args(tc.table(),
				"--ring.mode=memory", "--sink.boot_timeout=120s")...)

			start := time.Now()
			select {
			case code := <-in.done:
				if code != ExitSink {
					t.Errorf("exit code %d, want %d", code, ExitSink)
				}
			case <-time.After(20 * time.Second):
				in.cancel()
				t.Fatal("the process kept retrying a schema that will never match")
			}
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("exit took %s; a fatal boot check must not wait for boot_timeout", took)
			}
			if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
				c.Close()
				t.Error("the public port accepted a connection despite a refused schema")
			}
		})
	}
}

// SPEC §10.1: an unreachable PostgreSQL is retried until sink.boot_timeout and
// then exits 3 with the public port still shut. A refused connection has no
// SQLSTATE, so it is the network class and the boot keeps waiting, unlike the
// fatal refusals above.
func TestPostgresUnreachableKeepsPublicPortShut(t *testing.T) {
	if os.Getenv(pgDSNEnv) == "" {
		t.Skipf("set %s to run the postgres sink tests", pgDSNEnv)
	}
	dead := freePort(t)
	in := startExpectingExit(t, t.TempDir(),
		"--ring.mode=memory", "--sink.type=postgres",
		"--sink.postgres.addr="+dead, "--sink.boot_timeout=2s")

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		in.cancel()
		t.Fatal("the process never gave up on an unreachable PostgreSQL")
	}
	if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("the public port accepted a connection while the sink was unreachable")
	}
}

// ------------------------------------------------------------- availability

// SPEC §15.3: the destination goes away and comes back. While it is away the
// server refuses with 503 and /readyz fails; when it returns, nothing that
// was answered 200 is missing from the queue. The cut is a TCP proxy, so a
// shared server stays usable by everything else.
func TestPostgresDownThenBackLosesNothing(t *testing.T) {
	e := pgTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)

	args := e.args(table, "--ring.mode=memory",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.postgres.addr="+proxy.Addr())
	in := start(t, t.TempDir(), args...)

	before := in.postN(10)

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool {
		code, _ := in.post("/hook", `{"during":true}`)
		return code == http.StatusServiceUnavailable
	}) {
		t.Fatal("the server never started refusing while the sink was down")
	}
	if code, _ := in.readyz(); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while the sink was down, want 503", code)
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool {
		code, _ := in.readyz()
		return code == http.StatusOK
	}) {
		t.Fatal("the server never recovered after the sink came back")
	}

	after := in.postN(10)
	if len(after) != 10 {
		t.Errorf("%d of 10 requests accepted after recovery", len(after))
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if lost := missing(append(before, after...), e.ids(table, testID, envelope.ModeFast)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the queue", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §9 end to end against a destination WITH COLUMNS, which is what makes
// this different from the Redis version of the same cycle: the sink dies for
// good, the ring still holds events that were answered 200, they go to an
// emergency dump, the process exits 3, and the next boot reingests them.
//
// The dump carries only the JLH, so every column of the queue must come from
// the envelope itself (envelope.ParseHead). The v1 server kept the promoted
// values beside the envelope, and a reingestion that lost them wrote rows that
// were there but invisible to SQL. Here every reingested row must rebuild the
// envelope its client was answered for -- the oracle checks that the uuid's
// instant is event_at -- and be reachable by the columns a consumer filters
// on.
//
// This path is routine for this destination, not exotic: a failover makes the
// session read-only, and SPEC §8.5.7 classifies that as fatal on purpose.
func TestPostgresFatalDumpsAndReingestsWithQueryableColumns(t *testing.T) {
	e := pgTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)
	dir := t.TempDir()

	args := e.args(table, "--ring.mode=memory",
		"--sink.workers=1",
		// Hold the events in the ring so the dump has something to write.
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s",
		"--sink.postgres.addr="+proxy.Addr())

	in := start(t, dir, args...)
	const n = 8
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)

	proxy.Resume()
	in2 := start(t, dir, args...)
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}

	got := e.ids(table, testID, envelope.ModeFast)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Fatalf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}

	// Reachable by the columns, not only through the payload.
	var reachable int
	if err := e.conn.QueryRow(context.Background(), `
		SELECT count(*) FROM `+table+`
		 WHERE from_uuid = $1::uuid AND space = $2 AND type = 'webhook.request'
		   AND level = 9 AND step = 0 AND sid = $3
		   AND event_at > now() - interval '1 hour' AND event_at <= now()`,
		testID.From, testID.Space, os.Getpid()).Scan(&reachable); err != nil {
		t.Fatal(err)
	}
	if reachable != n {
		t.Errorf("%d of %d reingested rows are reachable by their columns", reachable, n)
	}
}

// SPEC §8.5.5: the schema validation gets sink.op_timeout, not the health
// check's deadline. The wiring is checked where it is made: NewPostgres would
// default a zero ValidateTimeout to the same value, which is exactly what made
// a removed line go unnoticed.
func TestPostgresConfigCarriesTheValidationDeadline(t *testing.T) {
	loaded, err := config.Load(config.Options{Args: []string{"--sink.type=postgres",
		"--sink.op_timeout=7s", "--sink.health_timeout=300ms"},
		LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	a := &App{cfg: loaded.Config, id: testID}
	pc := a.postgresConfig()
	if pc.ValidateTimeout != 7*time.Second || pc.ConnectTimeout != 7*time.Second {
		t.Errorf("ValidateTimeout %s ConnectTimeout %s, want sink.op_timeout (7s) for both",
			pc.ValidateTimeout, pc.ConnectTimeout)
	}
}
