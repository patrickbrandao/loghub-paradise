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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlhcheck"
)

// sink=postgres and sink=mysql against real servers, each addressed by its
// GELF_TEST_* variable and skipped without it:
//
//	docker run --rm -d -p 15433:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
//	  -e POSTGRES_DB=sls_test --name sls-app-pg postgres:18-alpine
//	GELF_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15433/sls_test?sslmode=disable' \
//	  go test -race -run Postgres ./internal/app/
//
//	docker run --rm -d -p 13307:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
//	  --name sls-app-mysql mysql:8.4
//	GELF_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13307)/sls_test' go test -race -run MySQL ./internal/app/
//
// The server never runs DDL (invariant §20.17): each test creates its table
// from the file the operator applies -- docker/postgres/inbox.sql (in a schema
// of its own), docker/mysql/schema.sql (under a unique name) -- and drops it
// afterwards.

const (
	pgDSNEnv    = "GELF_TEST_PG_DSN"
	mysqlDSNEnv = "GELF_TEST_MYSQL_DSN"
)

// sqlLines is the traffic both suites send: every shape of column the table
// has -- hostname present and absent, a NUL in the message (env must keep it,
// escaped), a NUL in the host, which PostgreSQL refuses in text (22021) and
// which the sinks turn into U+FFFD in the column only (SPEC §3.7), a document
// whose loss forces raw (a duplicate key), and two malformed documents, one of
// them with a raw NUL byte (sent over UDP; base64 only in the PostgreSQL rebuild) and one
// with invalid UTF-8 (raw in base64 everywhere).
var sqlLines = []string{
	`{"version":"1.1","host":"web-01","short_message":"sql-full \u0000 com nul","level":5,` +
		`"_k":"v\"q","_n":12345678901234567890,"custom":{"a":[1,2]}}`,
	`{"short_message":"sql-nil"}`,
	`{"version":"1.1","host":"ho\u0000st","short_message":"sql-nul-columns","level":2}`,
	`{"version":"1.1","host":"h","short_message":"sql-dup","level":3,"level":4}`,
	"GET / HTTP/1.1 sql-malformed \x00 nul",
	"sql-malformed-bin \xff",
}

// sqlWant is the short-message of every well-formed line.
var sqlWant = []string{"sql-full \x00 com nul", "sql-nil", "sql-nul-columns", "sql-dup"}

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
		if e.Payload.Format == "malformed" {
			m = string(e.Payload.rawBytes())
		}
		if seen[m] {
			t.Errorf("%q stored twice", m)
		}
		seen[m] = true
		if e.From != testID.From {
			t.Errorf("from = %s", e.From)
		}
		if m == "sql-nul-columns" && r.Hostname.String != "ho\uFFFDst" {
			t.Errorf("NUL column = %q, want U+FFFD for the NUL", r.Hostname.String)
		}
		if m == "sql-dup" && string(e.Payload.rawBytes()) != sqlLines[3] {
			t.Errorf("a duplicate key must keep payload.raw exact: %q", e.Payload.rawBytes())
		}
	}
	for _, want := range append(append([]string(nil), sqlWant...), sqlLines[4:]...) {
		if !seen[want] {
			t.Errorf("%q is missing", want)
		}
	}
}

// sendSQLLines sends the lines over TCP, except a line with a NUL byte, which
// the \0 framing would cut in two (SPEC §5.3): that one goes as a datagram.
func sendSQLLines(t *testing.T, in *instance, lines []string) {
	t.Helper()
	var tcp []string
	for _, l := range lines {
		if strings.IndexByte(l, 0) >= 0 {
			sendUDP(t, in.app.TransportAddr("udp"), l)
			continue
		}
		tcp = append(tcp, l)
	}
	sendNUL(t, in.app.TransportAddr("tcp"), tcp...)
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
	sendSQLLines(t, in, sqlLines)
	count := func() int {
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor(t, 20*time.Second, "every row in postgres", func() bool { return count() == len(sqlLines) })
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="postgres",transports="udp,tcp"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	// SPEC §3.7.1: what a consumer rebuilds from each row is a valid type=gelf
	// envelope from this instance, semantic rules included -- the NUL lines
	// too: jsonb refuses \u0000, so the queue holds a rebuilt copy, U+FFFD in
	// text and raw exact in base64.
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
		// The one rule the rebuilt copy breaks on purpose: a raw with a NUL
		// goes in base64 although it is UTF-8 (SPEC §3.7.1 over §6.9), because
		// jsonb refuses \u0000. Anything else the oracle says is a failure.
		if err := jlhcheck.Validate([]byte(env)); err != nil &&
			!(strings.Contains(err.Error(), "raw is UTF-8 and still went in base64") && strings.Contains(env, `"raw-encoding": "base64"`) && nulRaw(env)) {
			t.Errorf("%v\n%s", err, env)
		}
		var e struct {
			From    string
			Payload struct {
				Format string
				Short  string `json:"short-message"`
				Raw    *string
				RawEnc string `json:"raw-encoding"`
			}
		}
		if err := json.Unmarshal([]byte(env), &e); err != nil {
			t.Fatal(err)
		}
		if e.From != testID.From {
			t.Errorf("from %s, want this instance's AgentUUID", e.From)
		}
		if e.Payload.Raw == nil {
			if e.Payload.Format != "gelf" {
				t.Errorf("a %s row without raw", e.Payload.Format)
			}
			// The NUL of the message became U+FFFD in the rebuilt copy.
			seen[strings.ReplaceAll(e.Payload.Short, "\uFFFD", "\x00")] = true
			continue
		}
		raw := *e.Payload.Raw
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
	for _, want := range []string{sqlWant[0], sqlWant[1], sqlWant[2], sqlLines[3], sqlLines[4], sqlLines[5]} {
		if !seen[want] {
			t.Errorf("%q is missing from the queue (seen %v)", want, seen)
		}
	}
}

// nulRaw says whether the envelope's raw, decoded, holds a NUL byte.
func nulRaw(env string) bool {
	var e struct {
		Payload struct {
			Raw string `json:"raw"`
		} `json:"payload"`
	}
	if json.Unmarshal([]byte(env), &e) != nil {
		return false
	}
	b, err := base64.StdEncoding.DecodeString(e.Payload.Raw)
	return err == nil && strings.IndexByte(string(b), 0) >= 0
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
	stmt = strings.Replace(stmt, "CREATE TABLE IF NOT EXISTS gelf_events", "CREATE TABLE `"+table+"`", 1)
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
	sendSQLLines(t, in, sqlLines)
	count := func() int {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	waitFor(t, 20*time.Second, "every row in mysql", func() bool { return count() == len(sqlLines) })
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="mysql",transports="udp,tcp"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	rows, err := db.QueryContext(ctx, "SELECT uuid, step, space, level, sid, ev_uts, ev_sub, "+
		"format, transport, src_ip, hostname, env, nbytes FROM `"+table+"`")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []columnsWithEnv
	for rows.Next() {
		var r columnsWithEnv
		var id []byte
		if err := rows.Scan(&id, &r.Step, &r.Space, &r.Level, &r.SID, &r.EvUTS, &r.EvSub,
			&r.Format, &r.Transport, &r.SrcIP, &r.Hostname, &r.env, &r.NBytes); err != nil {
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
