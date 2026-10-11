package sink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
)

// Nothing in this file needs a server. The integration half lives in
// mysql_integration_test.go and skips without WHS_TEST_MYSQL_DSN.

// SPEC §8.6.6 is the table; this is the table read back. Schema and privilege
// numbers are fatal, never logic; a read-only server is fatal like 25006 on
// PostgreSQL; a number the table does not list falls back on the class of its
// SQLSTATE; only what neither explains is retried.
func TestClassifyMySQLNumber(t *testing.T) {
	cases := []struct {
		n     uint16
		state string
		want  Class
	}{
		// Waiting fixes it.
		{1040, "08004", ClassNetwork}, // too many connections
		{1053, "08S01", ClassNetwork}, // server shutdown in progress
		{1159, "08S01", ClassNetwork}, // net read timeout
		{1205, "HY000", ClassNetwork}, // lock wait timeout
		{1213, "40001", ClassNetwork}, // deadlock
		{1317, "70100", ClassNetwork}, // query execution was interrupted
		{1927, "70100", ClassNetwork}, // connection was killed
		{2006, "", ClassNetwork},      // server has gone away
		{2013, "", ClassNetwork},      // lost connection during query
		{3024, "HY000", ClassNetwork}, // max_execution_time exceeded

		// Waiting never fixes it: end the process.
		{1290, "HY000", ClassFatal}, // --read-only: the deliberate hard choice
		{1836, "HY000", ClassFatal}, // running in read-only mode
		{1021, "HY000", ClassFatal}, // disk full
		{1030, "HY000", ClassFatal}, // got error from storage engine
		{1041, "HY000", ClassFatal}, // out of memory
		{1114, "HY000", ClassFatal}, // table is full
		{1044, "42000", ClassFatal}, // access denied for database
		{1045, "28000", ClassFatal}, // access denied for user
		{1049, "42000", ClassFatal}, // unknown database
		{1130, "HY000", ClassFatal}, // host not allowed to connect
		{1142, "42000", ClassFatal}, // command denied: no INSERT
		{1143, "42000", ClassFatal}, // column access denied
		{1054, "42S22", ClassFatal}, // unknown column
		{1146, "42S02", ClassFatal}, // table does not exist
		{1364, "HY000", ClassFatal}, // field has no default value

		// The destination is alive and refused THIS data.
		{1048, "23000", ClassLogic}, // column cannot be null
		{1062, "23000", ClassLogic}, // duplicate entry: unreachable under ON DUPLICATE KEY
		{1153, "08S01", ClassLogic}, // packet bigger than max_allowed_packet: the number wins over 08
		{1264, "22003", ClassLogic}, // out of range
		{1366, "HY000", ClassLogic}, // incorrect string value
		{1406, "22001", ClassLogic}, // data too long
		{1690, "22003", ClassLogic}, // value is out of range
		{3819, "HY000", ClassLogic}, // a CHECK of the inbox violated
		{3140, "22032", ClassLogic}, // invalid JSON text for the payload column
		{3144, "22032", ClassLogic}, // JSON from a binary-charset string

		// Not in the number table: the SQLSTATE decides (review of
		// 2026-10-06). Before the fallback, 3151 was retried forever with
		// the sink UP.
		{3151, "22032", ClassLogic}, // a JSON key longer than 64 KiB
		{4025, "23000", ClassLogic}, // MariaDB's CHECK violation
		{1292, "22007", ClassLogic}, // incorrect datetime value
		{1064, "42000", ClassFatal}, // syntax error: our SQL and their server disagree
		{1698, "28000", ClassFatal}, // access denied (auth_socket and friends)
		{9998, "08S01", ClassNetwork},

		// Unknown, and a state that says nothing: retried, not dropped.
		{0, "", ClassNetwork},
		{1064, "", ClassNetwork},
		{9999, "HY000", ClassNetwork},
		{9999, "40001", ClassNetwork},
	}
	for _, tc := range cases {
		if got := classifyMySQLNumber(tc.n, tc.state); got != tc.want {
			t.Errorf("error %d (%s): class = %s, want %s", tc.n, tc.state, got, tc.want)
		}
	}
}

// Anything without a server error number never reached a server that
// answered: it is the connection, and it is retried. The one exception is the
// driver's own refusal to send an oversized packet, which repeats on retry.
func TestClassifyMySQLTransportErrors(t *testing.T) {
	for _, err := range []error{
		errors.New("dial tcp 127.0.0.1:1: connect: connection refused"),
		errors.New("unexpected EOF"),
		context.DeadlineExceeded,
		context.Canceled,
		driver.ErrBadConn,
		mysql.ErrInvalidConn,
		fmt.Errorf("wrapped: %w", mysql.ErrInvalidConn),
	} {
		if got := ClassOf(classifyMySQL("insert", err)); got != ClassNetwork {
			t.Errorf("%v: class = %s, want network", err, got)
		}
	}
	if got := ClassOf(classifyMySQL("insert", mysql.ErrPktTooLarge)); got != ClassLogic {
		t.Errorf("ErrPktTooLarge: class = %s, want logic", got)
	}
	if classifyMySQL("insert", nil) != nil {
		t.Error("a nil error must classify as nil")
	}
}

// database/sql wraps the driver error in places; errors.As must find it
// through the wrapping, and the class must follow the number, not the text.
func TestClassifyMySQLUnwrapsDriverError(t *testing.T) {
	inner := &mysql.MySQLError{Number: 1146, Message: "Table 'webhook.inbox' doesn't exist"}
	err := classifyMySQL("insert", fmt.Errorf("exec: %w", inner))

	if !IsFatal(err) {
		t.Fatalf("1146 through a wrapper classified as %s, want fatal", ClassOf(err))
	}
	if !strings.Contains(err.Error(), "mysql=1146") {
		t.Errorf("error %q does not carry the number for the operator", err)
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		t.Error("the classified error must still unwrap to the MySQLError")
	}
	if got := ClassOf(classifyMySQL("insert", &mysql.MySQLError{Number: 1048})); got != ClassLogic {
		t.Errorf("1048 = %s, want logic", got)
	}
	if got := ClassOf(classifyMySQL("insert", &mysql.MySQLError{Number: 1213})); got != ClassNetwork {
		t.Errorf("1213 = %s, want network", got)
	}
	// The SQLSTATE travels as five bytes; an unlisted number is classed by it,
	// and without one it is retried.
	jsonKey := &mysql.MySQLError{Number: 3151, SQLState: [5]byte{'2', '2', '0', '3', '2'}}
	if got := ClassOf(classifyMySQL("insert", jsonKey)); got != ClassLogic {
		t.Errorf("3151 (22032) = %s, want logic", got)
	}
	if got := ClassOf(classifyMySQL("insert", &mysql.MySQLError{Number: 3151})); got != ClassNetwork {
		t.Errorf("3151 without a SQLSTATE = %s, want network", got)
	}
}

// SPEC §8.6.3: one placeholder group of ten per row, ON DUPLICATE KEY UPDATE
// uuid = uuid and never INSERT IGNORE, the table quoted per part, and the
// columns in the order mysqlArgs fills them.
func TestMySQLInsertStatement(t *testing.T) {
	p := &MySQL{quoted: "`db`.`inbox`"}
	s := p.insertFor(3)
	if !strings.HasPrefix(s, "INSERT INTO `db`.`inbox` (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload) VALUES ") {
		t.Errorf("unexpected head: %s", s)
	}
	if !strings.HasSuffix(s, " ON DUPLICATE KEY UPDATE uuid = uuid") {
		t.Errorf("unexpected tail: %s", s)
	}
	if n := strings.Count(s, "(?,?,?,?,?,?,?,?,?,?)"); n != 3 {
		t.Errorf("%d placeholder groups, want 3", n)
	}
	if strings.Contains(s, "IGNORE") {
		t.Error("INSERT IGNORE would truncate data silently; the statement must not use it")
	}
	if n := strings.Count(p.insertFor(1), "?"); n != mysqlColumns {
		t.Errorf("%d placeholders for one row, want %d", n, mysqlColumns)
	}
	// The validation contract and the statement name the same ten columns in
	// the same order: a column validated but not written, or written but not
	// validated, would surface only against a real table.
	var names []string
	for _, c := range mysqlColumnFamilies {
		names = append(names, c.name)
	}
	if want := "(" + strings.Join(names, ", ") + ")"; !strings.Contains(s, want) {
		t.Errorf("statement columns differ from the validation contract %s: %s", want, s)
	}
}

// SPEC §8.6.3: a batch is cut at the row ceiling and at the byte ceiling, and
// an item that alone exceeds the limit travels by itself rather than being
// skipped. The cost of a row is its whole JLH, the bound of its payload.
func TestMySQLChunking(t *testing.T) {
	sized := func(n, body int) []*envelope.Item {
		out := make([]*envelope.Item, n)
		for i := range out {
			out[i] = sizedItem(body)
		}
		return out
	}

	many := sized(mysqlMaxRows+1, 8)
	if end := chunkEnd(many, 0, 1<<40); end != mysqlMaxRows {
		t.Errorf("row ceiling: first chunk ends at %d, want %d", end, mysqlMaxRows)
	}
	if end := chunkEnd(many, mysqlMaxRows, 1<<40); end != len(many) {
		t.Errorf("row ceiling: second chunk ends at %d, want %d", end, len(many))
	}

	items := sized(5, 100)
	if c := rowCost(items[0]); c != int64(len(items[0].Env))+mysqlRowOverhead {
		t.Errorf("rowCost = %d, want the JLH (%d) plus the overhead", c, len(items[0].Env))
	}
	limit := 2*rowCost(items[0]) + 1 // room for two, not three
	var ends []int
	for start := 0; start < len(items); {
		end := chunkEnd(items, start, limit)
		ends = append(ends, end)
		start = end
	}
	if fmt.Sprint(ends) != "[2 4 5]" {
		t.Errorf("byte ceiling: chunk ends %v, want [2 4 5]", ends)
	}

	mixed := []*envelope.Item{sized(1, 10)[0], sized(1, 100_000)[0], sized(1, 10)[0]}
	limit = rowCost(mixed[0]) * 2
	if end := chunkEnd(mixed, 0, limit); end != 1 {
		t.Errorf("oversized neighbour: first chunk ends at %d, want 1", end)
	}
	if end := chunkEnd(mixed, 1, limit); end != 2 {
		t.Errorf("oversized item must travel alone: chunk ends at %d, want 2", end)
	}
	if end := chunkEnd(mixed, 2, limit); end != 3 {
		t.Errorf("tail: chunk ends at %d, want 3", end)
	}
}

// SPEC §8.6.3: the ten values of a row come from the JLH header -- the uuids
// in sixteen bytes, event_at in UTC to the microsecond, event_nsec the rest --
// and the payload is the JLH's payload text, sent as a string (a []byte would
// be the binary character set, which the JSON column refuses with 3144).
func TestMySQLArgsMapTheEnvelope(t *testing.T) {
	it := itemIn("acme.store", testRequest("/hook", []byte(`{"a":1}`)))
	h, payload, err := envelope.ParseHead(it.Env)
	if err != nil {
		t.Fatal(err)
	}
	a := &mysqlArgs{}
	a.grow(1)
	if err := a.add(it, 0); err != nil {
		t.Fatal(err)
	}
	if len(a.vals) != mysqlColumns {
		t.Fatalf("%d values, want %d", len(a.vals), mysqlColumns)
	}
	id, from := a.vals[0].([]byte), a.vals[1].([]byte)
	if [16]byte(id) != [16]byte(h.UUID) || uuidv7.UUID([16]byte(from)).String() != testFrom {
		t.Errorf("uuid %x from %x, want %s and %s", id, from, it.ID, testFrom)
	}
	at := a.vals[2].(time.Time)
	nsec := a.vals[6].(int64)
	if at.Location() != time.UTC || at.Nanosecond()%1000 != 0 || at.Unix() != h.Event.UTS ||
		int64(at.Nanosecond())+nsec != h.Event.Sub {
		t.Errorf("event_at %v + event_nsec %d is not event %+v without loss", at, nsec, h.Event)
	}
	if a.vals[3].(int64) != 1 || a.vals[4].(int64) != 0 || a.vals[5].(int64) != envelope.Level ||
		a.vals[7].(string) != "acme.store" || a.vals[8].(string) != envelope.Type {
		t.Errorf("sid/step/level/space/type = %v", a.vals[3:9])
	}
	if p, ok := a.vals[9].(string); !ok || p != string(payload) {
		t.Errorf("payload = %#v, want the JLH payload as a string", a.vals[9])
	}
}

// The argument slices hold the payloads and the uuid slices; they must go back
// to the pool cleared (SPEC §8.6.3, the cousin of §12.3).
func TestMySQLArgsResetDropsReferences(t *testing.T) {
	a := &mysqlArgs{}
	items := mkBatch(2)
	a.grow(len(items))
	for i, it := range items {
		if err := a.add(it, i); err != nil {
			t.Fatalf("add refused a valid item: %v", err)
		}
	}
	if len(a.vals) != 2*mysqlColumns {
		t.Fatalf("%d values, want %d", len(a.vals), 2*mysqlColumns)
	}
	if got := a.vals[0].([]byte); len(got) != 16 {
		t.Errorf("uuid travels as %d bytes, want 16", len(got))
	}
	a.reset()
	if len(a.vals) != 0 {
		t.Fatalf("reset left %d values", len(a.vals))
	}
	for i, v := range a.vals[:cap(a.vals)] {
		if v != nil {
			t.Fatalf("reset left a reference at %d: %T", i, v)
		}
	}
}

// An envelope the sink cannot map is refused as data (logic), never sent.
func TestMySQLArgsRefuseWhatTheyCannotMap(t *testing.T) {
	a := &mysqlArgs{}
	a.grow(1)
	if err := a.add(badItem(), 0); err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a v1 envelope: %v, want logic", err)
	}
	if len(a.vals) != 0 {
		t.Errorf("a refused item left %d values behind", len(a.vals))
	}
	if err := a.add(mkItem("control"), 0); err != nil {
		t.Errorf("control: a valid item was refused: %v", err)
	}
}

// SPEC §8.6.4: the probe row carries the fixed UUIDv7 of instant zero, so the
// inbox's version CHECK accepts it and the generator can never mint it.
func TestMySQLProbeUUID(t *testing.T) {
	if got := uuidv7.UUID(probeUUID).String(); got != "00000000-0000-7000-8000-000000000000" {
		t.Errorf("probe uuid = %s", got)
	}
}

// SPEC §8.6.4: the probe row sits at every limit the JLH allows, so a column
// too narrow for a value this server may write refuses the probe at boot. Its
// space is the longest the grammar admits: proven by building a real envelope
// with it and reading it back through the strict header parser.
func TestMySQLProbeRowIsAtTheLimits(t *testing.T) {
	r := probeRow()
	if r.SID != envelope.MaxSID || r.Step != envelope.MaxStep || r.Level != 15 || r.EventNsec != 999 {
		t.Errorf("sid/step/level/event_nsec = %d/%d/%d/%d, want %d/%d/15/999",
			r.SID, r.Step, r.Level, r.EventNsec, int64(envelope.MaxSID), envelope.MaxStep)
	}
	if r.Type != envelope.Type {
		t.Errorf("type = %q, want %q", r.Type, envelope.Type)
	}
	if len(r.Space) != 64 {
		t.Errorf("space has %d characters, want the grammar's 64", len(r.Space))
	}
	it := itemIn(r.Space, testRequest("/probe", nil))
	h, _, err := envelope.ParseHead(it.Env)
	if err != nil || h.Space != r.Space {
		t.Errorf("the probe space is outside the grammar: %v (space %q)", err, h.Space)
	}
	if r.UUID != probeUUID || r.From != probeUUID || string(r.Payload) != "{}" {
		t.Errorf("uuid/from/payload = %x/%x/%s", r.UUID, r.From, r.Payload)
	}
}

// SPEC §8.6.4: the column rules, one by one. A type family alone accepted
// tables on which every batch failed (1406, 1264) or the microsecond was
// rounded in silence (review of 2026-10-06).
func TestMySQLColumnRules(t *testing.T) {
	col := func(name string) mysqlColumn {
		for _, c := range mysqlColumnFamilies {
			if c.name == name {
				return c
			}
		}
		t.Fatalf("no contract for %q", name)
		return mysqlColumn{}
	}
	n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	null := sql.NullInt64{}
	cases := []struct {
		c    mysqlColumn
		got  mysqlColumnInfo
		want string // substring of the problem; "" means accepted
	}{
		// The canonical DDL.
		{col("uuid"), mysqlColumnInfo{dataType: "binary", columnType: "binary(16)", maxChars: n(16)}, ""},
		{col("event_at"), mysqlColumnInfo{dataType: "datetime", columnType: "datetime(6)", fsp: n(6)}, ""},
		{col("sid"), mysqlColumnInfo{dataType: "bigint", columnType: "bigint"}, ""},
		{col("step"), mysqlColumnInfo{dataType: "int", columnType: "int"}, ""},
		{col("level"), mysqlColumnInfo{dataType: "smallint", columnType: "smallint"}, ""},
		{col("event_nsec"), mysqlColumnInfo{dataType: "smallint", columnType: "smallint"}, ""},
		{col("space"), mysqlColumnInfo{dataType: "varchar", columnType: "varchar(64)", maxChars: n(64)}, ""},
		{col("type"), mysqlColumnInfo{dataType: "varchar", columnType: "varchar(64)", maxChars: n(64)}, ""},
		{col("payload"), mysqlColumnInfo{dataType: "json", columnType: "json"}, ""},
		{mysqlReceivedAt, mysqlColumnInfo{dataType: "datetime", columnType: "datetime(6)", fsp: n(6)}, ""},
		// Wider, unsigned, longer: tolerated.
		{col("level"), mysqlColumnInfo{dataType: "int", columnType: "int"}, ""},
		{col("event_nsec"), mysqlColumnInfo{dataType: "smallint", columnType: "smallint unsigned"}, ""},
		{col("sid"), mysqlColumnInfo{dataType: "bigint", columnType: "bigint unsigned"}, ""},
		{col("step"), mysqlColumnInfo{dataType: "bigint", columnType: "bigint"}, ""},
		{col("space"), mysqlColumnInfo{dataType: "varchar", columnType: "varchar(255)", maxChars: n(255)}, ""},
		{col("type"), mysqlColumnInfo{dataType: "tinytext", columnType: "tinytext", maxChars: n(255)}, ""},
		{col("type"), mysqlColumnInfo{dataType: "char", columnType: "char(64)", maxChars: n(64)}, ""},
		// Refused.
		{col("uuid"), mysqlColumnInfo{dataType: "varbinary", columnType: "varbinary(16)"}, "want binary"},
		{col("uuid"), mysqlColumnInfo{dataType: "binary", columnType: "binary(20)", maxChars: n(20)}, "want binary(16)"},
		{col("event_at"), mysqlColumnInfo{dataType: "datetime", columnType: "datetime", fsp: n(0)}, "datetime(6)"},
		{col("event_at"), mysqlColumnInfo{dataType: "datetime", columnType: "datetime(3)", fsp: n(3)}, "datetime(6)"},
		{col("event_at"), mysqlColumnInfo{dataType: "datetime", columnType: "datetime", fsp: null}, "datetime(6)"},
		{col("event_at"), mysqlColumnInfo{dataType: "timestamp", columnType: "timestamp(6)", fsp: n(6)}, "want datetime"},
		{mysqlReceivedAt, mysqlColumnInfo{dataType: "datetime", columnType: "datetime", fsp: n(0)}, "datetime(6)"},
		{col("sid"), mysqlColumnInfo{dataType: "int", columnType: "int"}, "narrower than the bigint"},
		{col("step"), mysqlColumnInfo{dataType: "smallint", columnType: "smallint"}, "narrower than the int"},
		{col("level"), mysqlColumnInfo{dataType: "tinyint", columnType: "tinyint"}, "narrower than the smallint"},
		{col("event_nsec"), mysqlColumnInfo{dataType: "tinyint", columnType: "tinyint"}, "narrower than the smallint"},
		{col("space"), mysqlColumnInfo{dataType: "varchar", columnType: "varchar(8)", maxChars: n(8)}, "shorter than the 64"},
		{col("type"), mysqlColumnInfo{dataType: "varchar", columnType: "varchar(10)", maxChars: n(10)}, "shorter than the 64"},
		{col("type"), mysqlColumnInfo{dataType: "char", columnType: "char(63)", maxChars: n(63)}, "shorter than the 64"},
		{col("payload"), mysqlColumnInfo{dataType: "longtext", columnType: "longtext"}, "want json"},
	}
	for _, tc := range cases {
		got := tc.c.problem(tc.got)
		switch {
		case tc.want == "" && got != "":
			t.Errorf("%s %s: refused (%s), want accepted", tc.c.name, tc.got.columnType, got)
		case tc.want != "" && !strings.Contains(got, tc.want):
			t.Errorf("%s %s: problem %q, want one mentioning %q", tc.c.name, tc.got.columnType, got, tc.want)
		}
	}
}

// SPEC §8.6: MariaDB is refused by name. Its JSON is LONGTEXT, so even the
// canonical DDL reports longtext; the message must say so instead of a bare
// "want json". A MySQL with a LONGTEXT payload gets the ordinary refusal.
func TestMySQLMariaDBRefusal(t *testing.T) {
	msg := mysqlMariaDBRefusal("inbox", "longtext", "10.11.6-MariaDB-0+deb12u1")
	for _, want := range []string{"MariaDB is not supported", "LONGTEXT", "§8.6", "10.11.6-MariaDB"} {
		if !strings.Contains(msg, want) {
			t.Errorf("MariaDB refusal %q does not mention %q", msg, want)
		}
	}
	if msg := mysqlMariaDBRefusal("inbox", "longtext", "8.4.11"); msg != "" {
		t.Errorf("a MySQL server was refused as MariaDB: %q", msg)
	}
	if msg := mysqlMariaDBRefusal("inbox", "json", "10.11.6-MariaDB"); msg != "" {
		t.Errorf("a JSON payload was refused as MariaDB's: %q", msg)
	}
}

// SPEC §8.6.4: ON DUPLICATE KEY UPDATE fires on any unique key, so the only
// unique indexes allowed are the arbiter on the whole uuid column and a
// surrogate AUTO_INCREMENT key, which cannot collide.
func TestMySQLUniqueKind(t *testing.T) {
	col := func(name string) mysqlKeyPart {
		return mysqlKeyPart{column: sql.NullString{String: name, Valid: true}}
	}
	prefix := col("uuid")
	prefix.subPart = sql.NullInt64{Int64: 8, Valid: true}
	cols := map[string]mysqlColumnInfo{"id": {dataType: "bigint", autoInc: true}, "sid": {dataType: "bigint"}}
	cases := []struct {
		name  string
		parts []mysqlKeyPart
		want  int
	}{
		{"primary key (uuid)", []mysqlKeyPart{col("uuid")}, uniqueArbiter},
		{"UUID spelled upper", []mysqlKeyPart{col("UUID")}, uniqueArbiter},
		{"auto-increment surrogate", []mysqlKeyPart{col("id")}, uniqueSurrogate},
		{"(from_uuid, sid)", []mysqlKeyPart{col("from_uuid"), col("sid")}, uniqueForeign},
		{"(uuid, step)", []mysqlKeyPart{col("uuid"), col("step")}, uniqueForeign},
		{"(sid)", []mysqlKeyPart{col("sid")}, uniqueForeign},
		{"prefix uuid(8)", []mysqlKeyPart{prefix}, uniqueForeign},
		{"functional key part", []mysqlKeyPart{{}}, uniqueForeign},
	}
	for _, tc := range cases {
		if got := uniqueKind(tc.parts, cols); got != tc.want {
			t.Errorf("%s: kind %d, want %d", tc.name, got, tc.want)
		}
	}
	k := mysqlUniqueKey{name: "u", parts: []mysqlKeyPart{col("from_uuid"), prefix, {}}}
	if got := k.describe(); got != "from_uuid, uuid(8), expression" {
		t.Errorf("describe = %q", got)
	}
}

func TestNewMySQLIsLazy(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{
		Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "inbox", PoolSize: 2,
		ConnectTimeout: time.Second,
	}, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewMySQL against a dead port: %v", err)
	}
	if p.Type() != "mysql" {
		t.Errorf("Type = %q", p.Type())
	}
	if p.quoted != "`d`.`inbox`" || p.schema != "d" || p.name != "inbox" {
		t.Errorf("table split into quoted=%q schema=%q name=%q", p.quoted, p.schema, p.name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = p.Health(ctx)
	if err == nil {
		t.Fatal("Health against a dead port succeeded")
	}
	if ClassOf(err) != ClassNetwork {
		t.Errorf("dead port classified as %s, want network", ClassOf(err))
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Write(ctx, mkBatch(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := p.Health(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v, want ErrClosed", err)
	}
}

func TestNewMySQLQualifiedTable(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "Hooks.Inbox"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.quoted != "`Hooks`.`Inbox`" || p.schema != "Hooks" || p.name != "Inbox" {
		t.Errorf("quoted=%q schema=%q name=%q: case must be kept, MySQL table names are case-sensitive on Linux",
			p.quoted, p.schema, p.name)
	}
}

func TestNewMySQLRejectsBadConfig(t *testing.T) {
	missingCA := filepath.Join(t.TempDir(), "absent.pem")
	junkCA := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junkCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "inbox"}

	cases := []struct {
		name string
		mut  func(*MySQLConfig)
		want string
	}{
		{"sql in table", func(c *MySQLConfig) { c.Table = "inbox; drop table x" }, "identifier"},
		{"backtick in table", func(c *MySQLConfig) { c.Table = "in`box" }, "identifier"},
		{"three-part table", func(c *MySQLConfig) { c.Table = "a.b.c" }, "identifier"},
		{"bad addr", func(c *MySQLConfig) { c.Addr = "nohost" }, "addr"},
		{"empty database", func(c *MySQLConfig) { c.Database = "" }, "database"},
		{"missing CA", func(c *MySQLConfig) { c.TLS = true; c.TLSCA = missingCA }, "reading"},
		{"junk CA", func(c *MySQLConfig) { c.TLS = true; c.TLSCA = junkCA }, "no usable certificate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			p, err := NewMySQL(cfg, quietLog(), testMetrics())
			if err == nil {
				p.Close()
				t.Fatal("NewMySQL accepted the configuration")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}
