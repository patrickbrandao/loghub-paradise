package sink

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlh"
)

// The SQL items of SPEC §22 (pendências 6, 10, 11, 12) that need no server. The
// halves that do are in review_sql_integration_test.go.

// ---------------------------------------------------------------- item 6

// The validation runs under sink.op_timeout, not under the deadline of the
// health check that triggers it: the check's deadline passing must leave it
// running.
func TestValidationContextOutlivesTheCheckDeadline(t *testing.T) {
	check, cancelCheck := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Second)
	defer stop()

	<-check.Done()
	select {
	case <-vctx.Done():
		t.Fatalf("validation ended with the health check's deadline: %v", vctx.Err())
	case <-time.After(50 * time.Millisecond):
	}
	if dl, ok := vctx.Deadline(); !ok || time.Until(dl) > time.Second {
		t.Errorf("validation deadline = %v (set %v), want at most sink.op_timeout from now", dl, ok)
	}
}

func TestValidationContextHasItsOwnDeadline(t *testing.T) {
	vctx, stop := validationContext(context.Background(), 20*time.Millisecond)
	defer stop()
	select {
	case <-vctx.Done():
		if !errors.Is(vctx.Err(), context.DeadlineExceeded) {
			t.Errorf("err = %v, want DeadlineExceeded", vctx.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink.op_timeout never ended the validation")
	}
}

// A boot cancelled while the check's own window is still open reaches the
// validation, with or without a lifetime in the context.
func TestValidationContextFollowsACancelledBoot(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	check, cancelCheck := context.WithTimeout(boot, time.Hour)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled boot did not cancel the validation")
	}
}

// A boot cancelled AFTER the check's deadline has passed: the check context
// can no longer say so, the lifetime it carries can. Health.WaitReady and
// Health.check must put it there (withLifetime, the health checks' half of
// the rule) for this to work in the app; the context is built by hand here,
// so this test proves the sink's half on its own.
func TestValidationContextFollowsABootCancelledAfterTheCheckDeadline(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	defer cancelBoot()
	life := context.WithValue(boot, lifetimeKey{}, context.Context(boot))
	check, cancelCheck := context.WithTimeout(life, 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	<-check.Done()
	time.Sleep(20 * time.Millisecond)
	if vctx.Err() != nil {
		t.Fatalf("the check's deadline ended the validation: %v", vctx.Err())
	}
	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a boot cancelled after the check deadline never reached the validation")
	}
}

// --------------------------------------------------------------- item 11

// An unknown number falls back to the class of its SQLSTATE; a number the
// table lists keeps its class whatever the state says.
func TestClassifyMySQLFallsBackToTheSQLState(t *testing.T) {
	cases := []struct {
		n     uint16
		state string
		want  Class
	}{
		{4025, "23000", ClassLogic},   // MariaDB CHECK violation
		{3156, "22018", ClassLogic},   // invalid value for a cast
		{1064, "42000", ClassFatal},   // syntax error: our SQL and their server disagree
		{1227, "42000", ClassFatal},   // access denied, needs a privilege
		{1251, "08004", ClassNetwork}, // client does not support the auth protocol
		{1105, "HY000", ClassNetwork}, // unknown error: says nothing, retried
		{9999, "", ClassNetwork},      // no state at all
		{3819, "HY000", ClassLogic},   // listed: CHECK violated
		{1146, "42S02", ClassFatal},   // listed
		{1205, "HY000", ClassNetwork}, // listed: lock wait timeout
		{1290, "HY000", ClassFatal},   // listed: read-only, whatever the state
		// Server limits with SQLSTATE 42000 that clear on their own: network,
		// never the 42 fallback (independent review, 2026-10-11).
		{1203, "42000", ClassNetwork}, // max_user_connections
		{1226, "42000", ClassNetwork}, // per-user resource limit
		{1461, "42000", ClassNetwork}, // max_prepared_stmt_count
	}
	for _, tc := range cases {
		if got := classifyMySQLNumber(tc.n, tc.state); got != tc.want {
			t.Errorf("%d (%s): class %s, want %s", tc.n, tc.state, got, tc.want)
		}
		err := classifyMySQL("insert", &mysql.MySQLError{Number: tc.n, SQLState: state5(tc.state)})
		if got := ClassOf(err); got != tc.want {
			t.Errorf("%d (%s) through classifyMySQL: class %s, want %s", tc.n, tc.state, got, tc.want)
		}
	}
}

func state5(s string) [5]byte {
	var b [5]byte
	copy(b[:], s)
	return b
}

func kp(col string, sub int64) mysqlKeyPart {
	k := mysqlKeyPart{column: sql.NullString{String: col, Valid: col != ""}}
	if sub > 0 {
		k.subPart = sql.NullInt64{Int64: sub, Valid: true}
	}
	return k
}

// ON DUPLICATE KEY fires on ANY unique key, so only the whole-column
// (uuid, step) is the arbiter, an AUTO_INCREMENT surrogate is harmless, and
// everything else -- a prefix, an expression, another column set -- would
// swallow a new event as a duplicate.
func TestMySQLUniqueKind(t *testing.T) {
	cols := map[string]mysqlColumnInfo{"id": {autoInc: true}, "sid": {}}
	cases := []struct {
		name  string
		parts []mysqlKeyPart
		want  int
	}{
		{"(uuid, step)", []mysqlKeyPart{kp("uuid", 0), kp("step", 0)}, uniqueArbiter},
		{"(step, uuid)", []mysqlKeyPart{kp("step", 0), kp("UUID", 0)}, uniqueArbiter},
		{"(uuid(4), step)", []mysqlKeyPart{kp("uuid", 4), kp("step", 0)}, uniqueForeign},
		{"(uuid, step(1))", []mysqlKeyPart{kp("uuid", 0), kp("step", 1)}, uniqueForeign},
		{"(uuid)", []mysqlKeyPart{kp("uuid", 0)}, uniqueForeign},
		{"(uuid, step, level)", []mysqlKeyPart{kp("uuid", 0), kp("step", 0), kp("level", 0)}, uniqueForeign},
		{"(uuid, expression)", []mysqlKeyPart{kp("uuid", 0), kp("", 0)}, uniqueForeign},
		{"(sid)", []mysqlKeyPart{kp("sid", 0)}, uniqueForeign},
		{"(id) auto_increment", []mysqlKeyPart{kp("id", 0)}, uniqueSurrogate},
		{"(id, sid)", []mysqlKeyPart{kp("id", 0), kp("sid", 0)}, uniqueForeign},
	}
	for _, tc := range cases {
		if got := mysqlUniqueKind(tc.parts, cols); got != tc.want {
			t.Errorf("%s: kind %d, want %d", tc.name, got, tc.want)
		}
	}
}

// The probe's second row is the widest one this server can write, so a column
// too narrow for it refuses the boot instead of the drain's rows.
func TestMySQLProbeRowsAreTheExtremes(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Database: "d", MaxEventBytes: 123456}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	a := p.probeArgs()
	if len(a.vals) != 2*mysqlColumns {
		t.Fatalf("%d values, want two rows of %d", len(a.vals), mysqlColumns)
	}
	nulls, wide := a.vals[:mysqlColumns], a.vals[mysqlColumns:]
	col := func(name string) int {
		for i, c := range mysqlColumnFamilies {
			if c.name == name {
				return i
			}
		}
		t.Fatalf("no column %s", name)
		return -1
	}
	for _, name := range []string{"hostname", "appname", "procid", "msgid"} {
		if nulls[col(name)] != nil {
			t.Errorf("row 1: %s is %v, want NULL (a headerless message)", name, nulls[col(name)])
		}
	}
	// This server receives on unix too, and a unix row has no src_ip: the
	// NULL row is a unix row, as a real one is, and the wide row a network
	// row with the widest address. Both rows are rows a listener produces;
	// the old probe paired a NULL src_ip with "udp", a row no listener makes,
	// and so refused a legitimate table that ties the two together, CHECK
	// (transport = 'unix' OR src_ip IS NOT NULL) (go-relplog-server review,
	// 2026-10-11, adapted: there the server has no unix listener at all).
	if nulls[col("src_ip")] != nil || nulls[col("transport")] != "unix" {
		t.Errorf("row 1: src_ip %v, transport %v: want NULL and unix", nulls[col("src_ip")], nulls[col("transport")])
	}
	if wide[col("src_ip")] != mysqlProbeSrcIP || wide[col("transport")] != "tcp" {
		t.Errorf("row 2: src_ip %v, transport %v: want the widest address and tcp",
			wide[col("src_ip")], wide[col("transport")])
	}
	for name, want := range map[string]int64{
		"sid": jlh.MaxSID, "ev_uts": item.MaxTimestamp, "ev_sub": 999_999_999,
		"facility": 23, "severity": 7, "level": 7, "nbytes": 123456,
	} {
		if got := wide[col(name)]; got != want {
			t.Errorf("row 2: %s = %v, want %d", name, got, want)
		}
	}
	for name, min := range mysqlMinChars {
		row, which := wide, "row 2"
		if name == "transport" { // the longest, "unix", is the NULL row's
			row, which = nulls, "row 1"
		}
		if s, _ := row[col(name)].(string); len(s) != min {
			t.Errorf("%s: %s = %q (%d), want the widest value, %d characters", which, name, s, len(s), min)
		}
	}
	if mysqlMinChars["space"] != 64 || mysqlMinChars["format"] != len("malformed") ||
		mysqlMinChars["transport"] != len("unix") || mysqlMinChars["src_ip"] != 39 {
		t.Errorf("widths %v", mysqlMinChars)
	}
	if u0, u1 := wide[0].([]byte), nulls[0].([]byte); string(u0) == string(u1) {
		t.Error("the two probe rows share a uuid: the second would take the empty UPDATE and prove nothing")
	}
}

// --------------------------------------------------------------- item 12

// The database name goes raw between backticks in every INSERT, so the
// constructors refuse what cannot travel there.
func TestSQLConstructorsRefuseAnUnquotableDatabase(t *testing.T) {
	bad := map[string]string{
		"backtick":    "logs`; DROP TABLE x; --",
		"backslash":   `logs\x`,
		"newline":     "logs\nx",
		"NUL":         "logs\x00",
		"DEL":         "logs\x7f",
		"65 bytes":    strings.Repeat("d", 65),
		"empty":       "",
		"tab":         "lo\tgs",
		"escape byte": "logs\x1b[31m",
	}
	good := []string{"logs", "logs-prod", "logs.v2", "registros_ç", strings.Repeat("d", 64)}
	for name, db := range bad {
		if p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Database: db}, discard(), metrics()); err == nil {
			p.Close()
			t.Errorf("mysql %s: database %q accepted", name, db)
		}
		if c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:1"}, Database: db, Table: "inbox_syslog"},
			discard(), metrics()); err == nil {
			c.Close()
			t.Errorf("clickhouse %s: database %q accepted", name, db)
		}
	}
	for _, db := range good {
		p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Database: db}, discard(), metrics())
		if err != nil {
			t.Errorf("mysql: database %q refused: %v", db, err)
		} else {
			p.Close()
		}
		c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:1"}, Database: db, Table: "inbox_syslog"},
			discard(), metrics())
		if err != nil {
			t.Errorf("clickhouse: database %q refused: %v", db, err)
		} else {
			c.Close()
		}
	}
}
