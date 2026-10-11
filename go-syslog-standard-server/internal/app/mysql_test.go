package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// sink=mysql adds three pairs to the matrix of SPEC §18.1. These run the
// assembled server against a real MySQL addressed by SYSLOG_TEST_MYSQL_DSN and
// skip without it:
//
//	SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' \
//	  go test -race -timeout 600s ./internal/app/
//
// Each test creates its own table from docker/mysql/schema.sql and drops it
// afterwards. The frame ceilings are lowered so the largest row fits the
// 64 MiB default max_allowed_packet of MySQL 8, which the boot check of SPEC
// §3.9 insists on; the suite does not depend on how the test server is tuned.

const mysqlDSNEnv = "SYSLOG_TEST_MYSQL_DSN"

type mysqlEnv struct {
	t                      *testing.T
	db                     *sql.DB
	addr, user, pass, name string
}

func mysqlTest(t *testing.T) *mysqlEnv {
	t.Helper()
	dsn := os.Getenv(mysqlDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the mysql sink tests (see docs/TESTING.md §6.3)", mysqlDSNEnv)
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

// table creates a fresh events table from the canonical schema file, with alter
// applied afterwards ($T is the table).
func (e *mysqlEnv) table(alter string) string {
	e.t.Helper()
	name := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		e.t.Fatalf("reading the canonical schema: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = s[:strings.LastIndexByte(s, ';')]
	e.exec(strings.Replace(s, "CREATE TABLE IF NOT EXISTS events", "CREATE TABLE `"+name+"`", 1))
	if alter != "" {
		e.exec(strings.ReplaceAll(alter, "$T", name))
	}
	e.t.Cleanup(func() { _, _ = e.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") })
	return name
}

func (e *mysqlEnv) exec(stmt string) {
	e.t.Helper()
	if _, err := e.db.ExecContext(context.Background(), stmt); err != nil {
		e.t.Fatalf("%v\n%s", err, stmt)
	}
}

// raws lists the raw line of every row, in arrival order.
func (e *mysqlEnv) raws(table string) []string {
	e.t.Helper()
	rows, err := e.db.QueryContext(context.Background(), "SELECT raw FROM `"+table+"` ORDER BY ts, nts")
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	return out
}

func (e *mysqlEnv) args(table string, extra ...string) []string {
	return append([]string{
		"--sink.type=mysql",
		"--sink.mysql.addr=" + e.addr,
		"--sink.mysql.username=" + e.user,
		"--sink.mysql.password=" + e.pass,
		"--sink.mysql.database=" + e.name,
		"--sink.mysql.table=" + table,
		// The largest row stays far under any default max_allowed_packet
		// (SPEC §3.9, §10.7.4).
		"--tcp.max_frame_bytes=1MiB",
		"--relp.max_frame_bytes=1MiB",
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §18.1: with sink=mysql every ring mode accepts traffic, reports the pair
// in syslog_config_info, exits 0 and leaves every confirmed message in the
// table.
func TestMySQLAllRingModes(t *testing.T) {
	e := mysqlTest(t)
	for _, ringMode := range []string{"memory", "sqlite", "none"} {
		t.Run(ringMode, func(t *testing.T) {
			table := e.table("")
			in := start(t, t.TempDir(), e.args(table, "--ring.mode="+ringMode)...)

			lines := markedLines("mysql-"+ringMode, 40)
			confirmed := relpSend(t, in.app.TransportAddr("relp"), lines)
			if len(confirmed) != len(lines) {
				t.Fatalf("%d of %d messages were confirmed", len(confirmed), len(lines))
			}
			_, body := in.get("/metrics")
			want := fmt.Sprintf(`syslog_config_info{ring_mode="%s",sink_type="mysql"`, ringMode)
			if !strings.Contains(body, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			if lost := missing(confirmed, e.raws(table)); len(lost) > 0 {
				t.Errorf("%d of %d confirmed messages never reached the table: %q", len(lost), len(confirmed), lost[0])
			}
		})
	}
}

// SPEC §9.5: ring=none writes through from the receiver, and the instrumented
// wrapper must still observe every write.
func TestMySQLRingNoneIsInstrumented(t *testing.T) {
	e := mysqlTest(t)
	in := start(t, t.TempDir(), e.args(e.table(""), "--ring.mode=none")...)
	defer in.stop()

	if n := len(relpSend(t, in.app.TransportAddr("relp"), markedLines("instrumented", 5))); n != 5 {
		t.Fatalf("%d of 5 confirmed", n)
	}
	_, body := in.get("/metrics")
	for _, want := range []string{
		"syslog_events_written_total 5",
		"syslog_sink_duration_seconds_count 5",
		"syslog_sink_batch_size_count 5",
		"syslog_sink_mysql_duplicates_total 0",
		`syslog_sink_mysql_pool_conns{state="max"} 5`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// SPEC §3.7 end to end: the raw line comes back byte for byte, NUL and all,
// framing removed; the columns match the envelope; SUBSTRING with the promoted
// offsets is exactly the message.
func TestMySQLTheRowIsFaithful(t *testing.T) {
	e := mysqlTest(t)
	table := e.table("")
	in := start(t, t.TempDir(), e.args(table)...)

	line := "<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] a mensagem \x00 com nul"
	c, err := net.Dial("tcp", in.app.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "%d %s", len(line), line)
	c.Close()
	if !waitFor(10*time.Second, func() bool { return len(e.raws(table)) == 1 }) {
		t.Fatal("the message never reached the table")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	var (
		raw, env, msg                     []byte
		host, app, proc, msgid, transport string
		rfc, peer                         string
		facility, severity                int
	)
	err = e.db.QueryRow("SELECT raw, env, SUBSTRING(raw, msg_offset + 1, msg_size), hostname, app_name, "+
		"procid, msgid, transport, rfc, peer_ip, facility, severity FROM `"+table+"`").
		Scan(&raw, &env, &msg, &host, &app, &proc, &msgid, &transport, &rfc, &peer, &facility, &severity)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != line {
		t.Fatalf("raw line changed\n got %q\nwant %q", raw, line)
	}
	if string(msg) != "a mensagem \x00 com nul" {
		t.Errorf("SUBSTRING(raw, msg_offset + 1, msg_size) = %q", msg)
	}
	if host != "web-01" || app != "nginx" || proc != "42" || msgid != "ID7" ||
		transport != "tcp" || rfc != "5424" || peer != "127.0.0.1" || facility != 20 || severity != 5 {
		t.Errorf("columns = %q %q %q %q %q %q %q %d %d", host, app, proc, msgid, transport, rfc, peer, facility, severity)
	}
	var m map[string]any
	if err := json.Unmarshal(env, &m); err != nil {
		t.Fatalf("env is not JSON: %v", err)
	}
	if m["framing"] != "octet" || m["node-id"] != "test-node" || m["hostname"] != host {
		t.Errorf("envelope = %v", m)
	}
}

// --------------------------------------------------------------- boot checks

// SPEC §10.7.4 and §12.1: a table that violates the contract, or a packet
// ceiling that cannot hold the largest row, stops the boot AT ONCE with exit 3,
// and no transport ever opens.
func TestMySQLSchemaMismatchExitsThreeImmediately(t *testing.T) {
	e := mysqlTest(t)
	cases := []struct {
		name string
		args []string
	}{
		{"missing table", e.args(fmt.Sprintf("sls_missing_%d", time.Now().UnixNano()))},
		{"missing column", e.args(e.table("ALTER TABLE `$T` DROP COLUMN msgid"))},
		{"no unique index", e.args(e.table("ALTER TABLE `$T` DROP PRIMARY KEY"))},
		// 512 MiB frames make the largest row bigger than the 1 GiB ceiling
		// max_allowed_packet has on any server (SPEC §3.9).
		{"packet ceiling", e.args(e.table(""), "--tcp.max_frame_bytes=512MiB")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := startExpectingExit(t, t.TempDir(), append(tc.args, "--sink.boot_timeout=120s")...)
			began := time.Now()
			select {
			case code := <-b.done:
				if code != ExitSink {
					t.Errorf("exit code %d, want %d", code, ExitSink)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the process kept retrying a table that will never match")
			}
			if took := time.Since(began); took > 15*time.Second {
				t.Errorf("exit took %s; a fatal boot check must not wait for boot_timeout", took)
			}
			b.assertShut(t)
		})
	}
}

// SPEC §12.1: an unreachable MySQL is retried until sink.boot_timeout and then
// exits 3, with no transport ever open.
func TestMySQLUnreachableKeepsTransportsShut(t *testing.T) {
	if os.Getenv(mysqlDSNEnv) == "" {
		t.Skipf("set %s to run the mysql sink tests", mysqlDSNEnv)
	}
	b := startExpectingExit(t, t.TempDir(),
		"--sink.type=mysql", "--sink.mysql.addr="+freeAddr(t),
		"--tcp.max_frame_bytes=1MiB", "--relp.max_frame_bytes=1MiB",
		"--sink.boot_timeout=2s")
	select {
	case code := <-b.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the process never gave up on an unreachable MySQL")
	}
	b.assertShut(t)
}

// ------------------------------------------------------------- availability

// SPEC §12.2: the destination goes away and comes back. While it is away
// /readyz fails; when it returns, nothing that was confirmed is missing.
func TestMySQLDownThenBackLosesNothing(t *testing.T) {
	e := mysqlTest(t)
	table := e.table("")
	proxy := newTCPProxy(t, e.addr)

	in := start(t, t.TempDir(), e.args(table,
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.mysql.addr="+proxy.Addr())...)

	before := relpSend(t, in.app.TransportAddr("relp"), markedLines("before", 10))
	if len(before) != 10 {
		t.Fatalf("%d of 10 confirmed before the outage", len(before))
	}

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool { code, _ := in.get("/readyz"); return code == 503 }) {
		t.Fatal("/readyz never failed while the destination was away")
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool { code, _ := in.get("/readyz"); return code == 200 }) {
		t.Fatal("the server never recovered after the destination came back")
	}

	after := relpSend(t, in.app.TransportAddr("relp"), markedLines("after", 10))
	if len(after) != 10 {
		t.Errorf("%d of 10 confirmed after recovery", len(after))
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if lost := missing(append(before, after...), e.raws(table)); len(lost) > 0 {
		t.Errorf("%d confirmed messages are not in the table", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §11 end to end: the destination dies for good while the ring still
// holds confirmed messages; they go to an emergency dump, the process exits 3,
// and the next boot reingests them into the table with their columns.
func TestMySQLFatalDumpsAndTheNextBootReingests(t *testing.T) {
	e := mysqlTest(t)
	table := e.table("")
	proxy := newTCPProxy(t, e.addr)
	dir := t.TempDir()

	args := e.args(table, "--ring.mode=memory",
		"--sink.workers=1", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--sink.mysql.addr="+proxy.Addr())

	in := start(t, dir, append(args, "--sink.batch_max_delay=10s")...)
	confirmed := relpSend(t, in.app.TransportAddr("relp"), markedLines("dumped", 8))
	if len(confirmed) != 8 {
		t.Fatalf("%d of 8 confirmed", len(confirmed))
	}

	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if dumps := emergencyFiles(t, filepath.Join(dir, "emergency")); len(dumps) != 1 {
		t.Fatalf("%d emergency dumps on disk, want 1: %v", len(dumps), dumps)
	}

	proxy.Resume()
	second := start(t, dir, args...)
	if code := second.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if lost := missing(confirmed, e.raws(table)); len(lost) > 0 {
		t.Fatalf("%d of %d confirmed messages were lost across the crash", len(lost), len(confirmed))
	}
	var withColumns int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM `" + table + "` WHERE hostname = 'web-01' AND ts > 0").
		Scan(&withColumns); err != nil {
		t.Fatal(err)
	}
	if withColumns != len(confirmed) {
		t.Errorf("%d of %d reingested rows are visible to the query their columns exist for", withColumns, len(confirmed))
	}
}
