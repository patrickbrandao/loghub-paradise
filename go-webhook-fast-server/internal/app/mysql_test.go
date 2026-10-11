package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ident"
)

// sink=mysql adds three pairs to the matrix of SPEC §16.1. These run against
// a real server addressed by WHS_TEST_MYSQL_DSN and skip without it:
//
//	WHS_TEST_MYSQL_DSN='root:whs@tcp(127.0.0.1:13306)/whs_test' \
//	  go test -race -timeout 900s ./internal/app/
//
// The destination is the inbox table that mirrors loghub.inbox (SPEC §3.7).
// Each test creates its own from docker/mysql/inbox.sql under a unique name --
// constraints included, since MySQL scopes their names to the database -- and
// drops it afterwards, so the suite never depends on an empty database.

const mysqlDSNEnv = "WHS_TEST_MYSQL_DSN"

type mysqlEnv struct {
	t                      *testing.T
	db                     *sql.DB
	addr, user, pass, name string
}

func mysqlTest(t *testing.T) *mysqlEnv {
	t.Helper()
	dsn := os.Getenv(mysqlDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the mysql sink tests (see docs/TESTING.md §5.3)", mysqlDSNEnv)
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %v", mysqlDSNEnv, err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("%s is not reachable: %v", mysqlDSNEnv, err)
	}
	t.Cleanup(func() { db.Close() })
	return &mysqlEnv{t: t, db: db, addr: mc.Addr, user: mc.User, pass: mc.Passwd, name: mc.DBName}
}

// table creates a fresh inbox from the canonical DDL under a unique name and
// returns it. alter, when set, runs afterwards with $T standing for the
// table.
func (e *mysqlEnv) table(alter ...string) string {
	e.t.Helper()
	name := fmt.Sprintf("whs_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/mysql/inbox.sql")
	if err != nil {
		e.t.Fatalf("reading the canonical DDL: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = strings.ReplaceAll(s[:strings.LastIndexByte(s, ';')], "inbox", name)
	e.t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") })
	e.exec(s)
	for _, a := range alter {
		e.exec(strings.ReplaceAll(a, "$T", "`"+name+"`"))
	}
	return name
}

func (e *mysqlEnv) exec(sql string) {
	e.t.Helper()
	if _, err := e.db.ExecContext(context.Background(), sql); err != nil {
		e.t.Fatalf("%v\n%s", err, sql)
	}
}

// envelopes rebuilds every envelope in the table from its columns, in uuid
// order (SPEC §3.7: the same mapping as loghub.inbox). The JSON column
// normalises the payload like jsonb does, so the nine top-level fields are put
// back in the canonical order the JLH fixes before the oracle sees them.
func (e *mysqlEnv) envelopes(table string) [][]byte {
	e.t.Helper()
	rows, err := e.db.QueryContext(context.Background(), `
		SELECT LOWER(HEX(uuid)), LOWER(HEX(from_uuid)), space, type, level, sid, step,
		       DATE_FORMAT(event_at, '%Y-%m-%dT%H:%i:%s.%fZ'), event_nsec, payload
		FROM `+"`"+table+"`"+` ORDER BY uuid`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var c canonical
		var at string
		var nsec int64
		var payload []byte
		if err := rows.Scan(&c.UUID, &c.From, &c.Space, &c.Type, &c.Level, &c.SID, &c.Step,
			&at, &nsec, &payload); err != nil {
			e.t.Fatal(err)
		}
		c.UUID, c.From = hexToUUID(c.UUID), hexToUUID(c.From)
		ts, err := time.Parse("2006-01-02T15:04:05.000000Z", at)
		if err != nil {
			e.t.Fatalf("event_at %q: %v", at, err)
		}
		c.setEvent(ts, nsec)
		c.Payload = json.RawMessage(payload)
		out = append(out, c.bytes(e.t))
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// ids reads every uuid in the table, each envelope checked by the oracle.
func (e *mysqlEnv) ids(table string, id ident.Identity, mode string) []string {
	e.t.Helper()
	return uuidsOf(verify(e.t, e.envelopes(table), id, mode))
}

func hexToUUID(h string) string {
	if len(h) != 32 {
		return h
	}
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func (e *mysqlEnv) args(table string, extra ...string) []string {
	return append([]string{
		"--sink.type=mysql",
		"--sink.mysql.addr=" + e.addr,
		"--sink.mysql.username=" + e.user,
		"--sink.mysql.password=" + e.pass,
		"--sink.mysql.database=" + e.name,
		"--sink.mysql.table=" + table,
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §16.1: with sink=mysql every ring mode accepts traffic, reports the
// pair in whs_config_info, exits 0 and leaves every accepted uuid in the
// table, each envelope with the mode its pair promises (SPEC §3.2).
func TestMySQLAllRingModes(t *testing.T) {
	e := mysqlTest(t)
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
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="mysql"} 1`, tc.ring)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			got := e.ids(table, testID, tc.mode)
			if lost := missing(ids, got); len(lost) > 0 {
				t.Errorf("%d of %d accepted events never reached the table", len(lost), len(ids))
			}
			if extra := missing(got, ids); len(extra) > 0 {
				t.Errorf("%d rows in the table were never answered to a client", len(extra))
			}
		})
	}
}

// SPEC §11.1: ring=none writes through from the handler, and the wrapper of
// SPEC §10.2 must still observe every write and expose the pool gauges.
func TestMySQLRingNoneIsInstrumented(t *testing.T) {
	e := mysqlTest(t)
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
		`whs_sink_mysql_pool_conns{state="max"}`,
		"whs_sink_mysql_duplicates_total 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// --------------------------------------------------------- the wire contract

// SPEC §3.2 and §3.7 end to end through the JSON column: a binary body
// travels in base64 and an empty body as an empty content, both exact after
// MySQL normalises the document; a query with %00 lands with its NUL, which
// the JSON type stores (unlike jsonb, SPEC §3.6).
func TestMySQLNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	e := mysqlTest(t)
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
	checkRoundTrip(t, byUUID(t, verify(t, e.envelopes(table), testID, envelope.ModeFast)),
		binID, emptyID, false)
}

// SPEC §3.2: a request line carrying bytes that are not UTF-8 is a real
// event, and under the strict sql_mode the session forces it must not take
// the rest of its batch down: the envelope writer turns them into valid
// JSON before any destination sees them.
func TestMySQLInvalidUTF8PathDoesNotDropTheBatch(t *testing.T) {
	e := mysqlTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	good := in.postN(3)
	badID := postInvalidUTF8(t, in)
	more := in.postN(3)

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

	got := byUUID(t, verify(t, e.envelopes(table), testID, envelope.ModeFast))
	if lost := missing(append(append(good, badID), more...), keysOfEnv(got)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the table; a refusal took the batch down", len(lost))
	}
	checkInvalidUTF8(t, got[badID])
}

// --------------------------------------------------------------- boot checks

// SPEC §8.6.4 and §10.1: a table that violates the contract stops the boot AT
// ONCE with exit 3 -- not after sink.boot_timeout -- and the public port never
// opens. The control is the DDL as shipped, which boots.
func TestMySQLSchemaMismatchExitsThreeImmediately(t *testing.T) {
	e := mysqlTest(t)

	t.Run("control", func(t *testing.T) {
		in := start(t, t.TempDir(), e.args(e.table(), "--ring.mode=memory")...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("the canonical table: exit code %d", code)
		}
	})

	cases := []struct {
		name  string
		table func() string
	}{
		{"missing table", func() string { return "whs_missing_" + strconv.FormatInt(time.Now().UnixNano(), 10) }},
		{"wrong column type", func() string {
			return e.table("ALTER TABLE $T MODIFY event_at BIGINT NOT NULL")
		}},
		{"no unique index", func() string {
			return e.table("ALTER TABLE $T DROP PRIMARY KEY")
		}},
		{"myisam", func() string {
			return e.table("ALTER TABLE $T ENGINE=MyISAM")
		}},
		// The probe INSERT is what finds it (1364 under the strict session):
		// every row would be refused, so it is fatal and not logic.
		{"extra required column", func() string {
			return e.table("ALTER TABLE $T ADD COLUMN tenant VARCHAR(16) NOT NULL")
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

// SPEC §10.1: an unreachable MySQL is retried until sink.boot_timeout and
// then exits 3 with the public port still shut.
func TestMySQLUnreachableKeepsPublicPortShut(t *testing.T) {
	if os.Getenv(mysqlDSNEnv) == "" {
		t.Skipf("set %s to run the mysql sink tests", mysqlDSNEnv)
	}
	dead := freePort(t)
	in := startExpectingExit(t, t.TempDir(),
		"--ring.mode=memory", "--sink.type=mysql",
		"--sink.mysql.addr="+dead, "--sink.boot_timeout=2s")

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		in.cancel()
		t.Fatal("the process never gave up on an unreachable MySQL")
	}
	if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("the public port accepted a connection while the sink was unreachable")
	}
}

// ------------------------------------------------------------- availability

// SPEC §15.3: the destination goes away and comes back. While it is away the
// server refuses with 503 and /readyz fails; when it returns, nothing that
// was answered 200 is missing from the table.
func TestMySQLDownThenBackLosesNothing(t *testing.T) {
	e := mysqlTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)

	args := e.args(table, "--ring.mode=memory",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.mysql.addr="+proxy.Addr())
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
		t.Errorf("%d events answered 200 are not in the table", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §9 end to end against a destination WITH COLUMNS: the sink dies for
// good, the ring still holds events answered 200, they go to an emergency
// dump, the process exits 3, and the next boot reingests them -- with every
// column filled from the envelope itself, so the rows rebuild the envelope
// their client was answered for and are reachable by SQL (the trap
// TestPostgresFatalDumpsAndReingestsWithQueryableColumns describes).
func TestMySQLFatalDumpsAndReingestsWithQueryableColumns(t *testing.T) {
	e := mysqlTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)
	dir := t.TempDir()

	args := e.args(table, "--ring.mode=memory",
		"--sink.workers=1",
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s",
		"--sink.mysql.addr="+proxy.Addr())

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

	var reachable int
	if err := e.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM `"+table+"`"+`
		 WHERE from_uuid = UNHEX(REPLACE(?, '-', '')) AND space = ? AND type = 'webhook'
		   AND level = 9 AND step = 0 AND sid = ?
		   AND event_at > UTC_TIMESTAMP(6) - INTERVAL 1 HOUR AND event_at <= UTC_TIMESTAMP(6)`,
		testID.From, testID.Space, os.Getpid()).Scan(&reachable); err != nil {
		t.Fatal(err)
	}
	if reachable != n {
		t.Errorf("%d of %d reingested rows are reachable by their columns", reachable, n)
	}
}
