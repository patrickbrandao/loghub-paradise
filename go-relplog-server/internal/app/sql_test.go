package app

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlhcheck"
)

// sink=postgres and sink=mysql against real servers, each addressed by its
// RELPLOG_TEST_* variable and skipped without it:
//
//	docker run --rm -d -p 15433:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
//	  -e POSTGRES_DB=sls_test --name sls-app-pg postgres:18-alpine
//	RELPLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15433/sls_test?sslmode=disable' \
//	  go test -race -run Postgres ./internal/app/
//
//	docker run --rm -d -p 13307:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
//	  --name sls-app-mysql mysql:8.4
//	RELPLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13307)/sls_test' go test -race -run MySQL ./internal/app/
//
// The server never runs DDL (invariant §20.17): each test creates its table
// from the file the operator applies -- docker/postgres/inbox.sql (in a schema
// of its own), docker/mysql/schema.sql (under a unique name) -- and drops it
// afterwards.

const (
	pgDSNEnv    = "RELPLOG_TEST_PG_DSN"
	mysqlDSNEnv = "RELPLOG_TEST_MYSQL_DSN"
)

// sqlLines is the traffic both suites send: every shape of column the table
// has -- all present, all absent (NILVALUE), absent procid and msgid (RFC 3164
// without PID), a NUL in the message (env must keep it), and a NUL in header
// fields, which PostgreSQL refuses in text (22021) and which the sinks turn into
// U+FFFD in the column only (SPEC §3.7).
var sqlLines = []string{
	"<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] sql-full \x00 com nul",
	"<13>1 2026-09-12T14:33:02Z - - - - - sql-nil",
	"<34>Oct 11 22:14:15 mymachine su: sql-nopid",
	"<13>Oct 11 22:14:15 ho\x00st ap\x00p[1\x002]: sql-nul-columns",
	"<190>Oct  1 00:00:00 host app: sql-latin \xff octet",
	"GET / HTTP/1.1 sql-malformed",
}

// checkSQLRows validates every envelope and its columns, and that each line
// arrived exactly once.
func checkSQLRows(t *testing.T, rows []columnsWithEnv) {
	t.Helper()
	if len(rows) != len(sqlLines) {
		t.Fatalf("%d rows, want %d", len(rows), len(sqlLines))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		e := mustJLH(t, r.env)
		checkColumns(t, r.columns, e)
		m := e.Payload.message()
		if seen[m] {
			t.Errorf("%q stored twice", m)
		}
		seen[m] = true
		if e.From != testID.From {
			t.Errorf("from = %s", e.From)
		}
		if strings.Contains(m, "sql-nul-columns") {
			if r.Hostname.String != "ho�st" || r.AppName.String != "ap�p" || r.ProcID.String != "1�2" {
				t.Errorf("NUL columns = %q %q %q, want U+FFFD for each NUL", r.Hostname.String, r.AppName.String, r.ProcID.String)
			}
		}
		if strings.Contains(m, "sql-full") && string(e.Payload.rawBytes()) != sqlLines[0] {
			t.Errorf("payload.raw changed: %q", e.Payload.rawBytes())
		}
	}
	for _, want := range []string{"sql-full \x00 com nul", "sql-nil", "sql-nopid", "sql-nul-columns"} {
		if !seen[want] {
			t.Errorf("%q is missing", want)
		}
	}
}

type columnsWithEnv struct {
	columns
	env []byte
}

// ------------------------------------------------------------ postgres

func TestPostgresMemoryEndToEnd(t *testing.T) {
	dsn := os.Getenv(pgDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the postgres suite", pgDSNEnv)
	}
	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("%s: %v", pgDSNEnv, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		t.Fatalf("%s is not reachable: %v", pgDSNEnv, err)
	}
	defer conn.Close(context.Background())

	table := pgQueue(t, conn, "")

	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--sink.type=postgres",
		"--sink.postgres.addr="+net.JoinHostPort(pc.Host, strconv.Itoa(int(pc.Port))),
		"--sink.postgres.username="+pc.User,
		"--sink.postgres.password="+pc.Password,
		"--sink.postgres.database="+pc.Database,
		"--sink.postgres.table="+table,
	), nil)
	in.waitReady()
	sendRELP(t, in.app.TransportAddr("tcp"), sqlLines...)
	count := func() int {
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor(t, 20*time.Second, "every row in postgres", func() bool { return count() == len(sqlLines) })
	if got := in.metric(`relplog_config_info{ring_mode="memory",sink_type="postgres",transports="tcp"}`); got != "1" {
		t.Errorf("relplog_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	// SPEC §3.7.1: what a consumer rebuilds from each row is a valid type=syslog
	// envelope from this instance, semantic rules included -- the NUL lines
	// too, whose raw the queue keeps exact in base64.
	rows, err := conn.Query(ctx, `
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
		t.Fatal(err)
	}
	envs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != len(sqlLines) {
		t.Fatalf("%d rows, want %d", len(envs), len(sqlLines))
	}
	seen := map[string]bool{}
	for _, env := range envs {
		if err := jlhcheck.Validate([]byte(env)); err != nil {
			t.Errorf("%v\n%s", err, env)
		}
		var e struct {
			From    string
			Payload struct {
				Raw    string
				RawEnc string `json:"raw-encoding"`
			}
		}
		if err := json.Unmarshal([]byte(env), &e); err != nil {
			t.Fatal(err)
		}
		if e.From != testID.From {
			t.Errorf("from %s, want this instance's AgentUUID", e.From)
		}
		raw := e.Payload.Raw
		if e.Payload.RawEnc == "base64" {
			b, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		// raw comes back exact: in base64 when the line has a NUL (the queue's
		// rebuild) or invalid UTF-8 (the converter's own rule), in text otherwise.
		var line string
		for _, l := range sqlLines {
			if l == raw {
				line = l
			}
		}
		if line == "" {
			t.Errorf("raw %q is no line that was sent", raw)
			continue
		}
		seen[line] = true
		wantB64 := strings.ContainsRune(line, 0) || !utf8.ValidString(line)
		if (e.Payload.RawEnc == "base64") != wantB64 {
			t.Errorf("%q: raw-encoding %q", line, e.Payload.RawEnc)
		}
	}
	if len(seen) != len(sqlLines) {
		t.Errorf("%d distinct lines, want %d", len(seen), len(sqlLines))
	}
}

// pgQueue creates a loghub.inbox from docker/postgres/inbox.sql in a schema of
// its own (the DDL names its constraints and index), runs alter ($T is the
// table) and registers the schema's removal. It returns schema.inbox.
func pgQueue(t *testing.T, conn *pgx.Conn, alter string) string {
	t.Helper()
	schema := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		t.Fatal(err)
	}
	ddl := strings.ReplaceAll(string(b), "SCHEMA IF NOT EXISTS loghub;", "SCHEMA "+schema+";")
	ddl = strings.ReplaceAll(ddl, "loghub.inbox", schema+".inbox")
	ctx := context.Background()
	if _, err := conn.Exec(ctx, ddl, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("applying docker/postgres/inbox.sql: %v", err)
	}
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	table := schema + ".inbox"
	if alter != "" {
		if _, err := conn.Exec(ctx, strings.ReplaceAll(alter, "$T", table), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatal(err)
		}
	}
	return table
}

// ------------------------------------------------------------ mysql

func TestMySQLMemoryEndToEnd(t *testing.T) {
	dsn := os.Getenv(mysqlDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the mysql suite", mysqlDSNEnv)
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %v", mysqlDSNEnv, err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("%s is not reachable: %v", mysqlDSNEnv, err)
	}

	table := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	var keep []string
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			keep = append(keep, line)
		}
	}
	stmt := strings.TrimSpace(strings.Join(keep, "\n"))
	stmt = strings.TrimSuffix(stmt, ";")
	stmt = strings.Replace(stmt, "CREATE TABLE IF NOT EXISTS syslog_events", "CREATE TABLE `"+table+"`", 1)
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("applying docker/mysql/schema.sql: %v\n%s", err, stmt)
	}
	t.Cleanup(func() { db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`") })

	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--sink.type=mysql",
		"--sink.mysql.addr="+mc.Addr,
		"--sink.mysql.username="+mc.User,
		"--sink.mysql.password="+mc.Passwd,
		"--sink.mysql.database="+mc.DBName,
		"--sink.mysql.table="+table,
	), nil)
	in.waitReady()
	sendRELP(t, in.app.TransportAddr("tcp"), sqlLines...)
	count := func() int {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor(t, 20*time.Second, "every row in mysql", func() bool { return count() == len(sqlLines) })
	if got := in.metric(`relplog_config_info{ring_mode="memory",sink_type="mysql",transports="tcp"}`); got != "1" {
		t.Errorf("relplog_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	rows, err := db.QueryContext(ctx, "SELECT uuid, step, space, level, sid, ev_uts, ev_sub, facility, severity, "+
		"format, transport, src_ip, hostname, appname, procid, msgid, env, nbytes FROM `"+table+"`")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []columnsWithEnv
	for rows.Next() {
		var r columnsWithEnv
		var id []byte
		if err := rows.Scan(&id, &r.Step, &r.Space, &r.Level, &r.SID, &r.EvUTS, &r.EvSub, &r.Facility, &r.Severity,
			&r.Format, &r.Transport, &r.SrcIP, &r.Hostname, &r.AppName, &r.ProcID, &r.MsgID, &r.env, &r.NBytes); err != nil {
			t.Fatal(err)
		}
		if len(id) != 16 {
			t.Fatalf("uuid column holds %d bytes, want BINARY(16)", len(id))
		}
		h := fmt.Sprintf("%x", id)
		r.UUID = h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
		r.EnvLen = len(r.env)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	checkSQLRows(t, out)
}
