package sink

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// The SQL items of SPEC §22.1 (6, 10, 11, 12) that need no server. The
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
// Health.check must put it there (withLifetime) for this to work in the app.
func TestValidationContextFollowsABootCancelledAfterTheCheckDeadline(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	defer cancelBoot()
	check, cancelCheck := context.WithTimeout(withLifetime(boot), 10*time.Millisecond)
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
	// This server only receives on tcp and tls: src_ip is never NULL and the
	// transport is never "unix", so the probe must not refuse a table that
	// says so (independent review, 2026-10-11).
	if nulls[col("src_ip")] == nil || nulls[col("transport")] != "tcp" {
		t.Errorf("row 1: src_ip %v, transport %v: want an address and tcp", nulls[col("src_ip")], nulls[col("transport")])
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
		if s, _ := wide[col(name)].(string); len(s) != min {
			t.Errorf("row 2: %s = %q (%d), want the widest value, %d characters", name, s, len(s), min)
		}
	}
	if mysqlMinChars["space"] != 64 || mysqlMinChars["format"] != len("malformed") ||
		mysqlMinChars["transport"] != 3 || mysqlMinChars["src_ip"] != 39 {
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

// lifetimeSink records the lifetime each health check carries.
type lifetimeSink struct {
	None
	mu   sync.Mutex
	seen []context.Context
}

func (s *lifetimeSink) Health(ctx context.Context) error {
	life, _ := ctx.Value(lifetimeKey{}).(context.Context)
	s.mu.Lock()
	s.seen = append(s.seen, life)
	s.mu.Unlock()
	return nil
}

// Both callers of Sink.Health -- the boot's WaitReady and the periodic check
// -- hand the sink the context they were given as its lifetime, or a
// validation running past the check's deadline could not see the boot being
// cancelled (validationContext, SPEC §10.5).
func TestHealthChecksCarryTheirLifetime(t *testing.T) {
	s := &lifetimeSink{}
	h := NewHealth(s, HealthConfig{Interval: time.Hour, Timeout: time.Second}, discard(), obs.NewMetrics(obs.Info{}))
	boot, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := h.WaitReady(boot, time.Second, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	h.check(boot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) != 2 {
		t.Fatalf("%d health checks, want 2", len(s.seen))
	}
	for i, life := range s.seen {
		if life != boot {
			t.Errorf("check %d carried lifetime %v, want the caller's context", i, life)
		}
	}
}
