package sink

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Nothing in this file needs a server. The integration half lives in
// mysql_integration_test.go and skips without SYSLOG_TEST_MYSQL_DSN.

// SPEC §10.6 is the table; this is the table read back. Schema and privilege
// numbers are fatal, never logic; a read-only server is fatal like 25006 on
// PostgreSQL; unknown numbers are retried.
func TestClassifyMySQLNumber(t *testing.T) {
	cases := []struct {
		n    uint16
		want Class
	}{
		// Waiting fixes it.
		{1040, ClassNetwork}, {1053, ClassNetwork}, {1077, ClassNetwork},
		{1158, ClassNetwork}, {1159, ClassNetwork}, {1160, ClassNetwork}, {1161, ClassNetwork},
		{1205, ClassNetwork}, {1213, ClassNetwork}, {1317, ClassNetwork}, {1927, ClassNetwork},
		{2006, ClassNetwork}, {2013, ClassNetwork}, {3024, ClassNetwork},

		// Waiting never fixes it: end the process.
		{1290, ClassFatal}, {1836, ClassFatal},
		{1021, ClassFatal}, {1030, ClassFatal}, {1041, ClassFatal}, {1114, ClassFatal},
		{1044, ClassFatal}, {1045, ClassFatal}, {1049, ClassFatal}, {1130, ClassFatal},
		{1142, ClassFatal}, {1143, ClassFatal},
		{1054, ClassFatal}, {1146, ClassFatal}, {1364, ClassFatal},

		// The destination is alive and refused THIS data.
		{1048, ClassLogic}, {1062, ClassLogic}, {1153, ClassLogic}, {1264, ClassLogic},
		{1366, ClassLogic}, {1406, ClassLogic}, {1690, ClassLogic},

		// Unknown is retried, not dropped.
		{0, ClassNetwork}, {1064, ClassNetwork}, {9999, ClassNetwork},
	}
	for _, tc := range cases {
		if got := classifyMySQLNumber(tc.n); got != tc.want {
			t.Errorf("error %d: class = %s, want %s", tc.n, got, tc.want)
		}
	}
}

// Anything without a server error number never reached a server that answered:
// it is the connection, and it is retried. The one exception is the driver's
// own refusal to send an oversized packet, which repeats on retry.
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

// database/sql wraps the driver error in places; errors.As must find it through
// the wrapping, and the class must follow the number, not the text.
func TestClassifyMySQLUnwrapsDriverError(t *testing.T) {
	inner := &mysql.MySQLError{Number: 1146, Message: "Table 'syslog.syslog_events' doesn't exist"}
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
}

// SPEC §10.6: one placeholder group of eighteen per row, the columns of §3.7 in
// table order, ON DUPLICATE KEY UPDATE and never INSERT IGNORE.
func TestMySQLInsertStatement(t *testing.T) {
	p := &MySQL{quoted: "`syslog`.`syslog_events`"}
	s := p.insertFor(3)
	head := "INSERT INTO `syslog`.`syslog_events` (uuid, step, space, level, sid, ev_uts, ev_sub, " +
		"facility, severity, format, transport, src_ip, hostname, appname, procid, msgid, env, nbytes) VALUES "
	if !strings.HasPrefix(s, head) {
		t.Errorf("unexpected head: %s", s)
	}
	if !strings.HasSuffix(s, " ON DUPLICATE KEY UPDATE uuid = uuid") {
		t.Errorf("unexpected tail: %s", s)
	}
	group := "(" + strings.TrimSuffix(strings.Repeat("?,", mysqlColumns), ",") + ")"
	if n := strings.Count(s, group); n != 3 {
		t.Errorf("%d placeholder groups, want 3", n)
	}
	if strings.Contains(s, "IGNORE") {
		t.Error("INSERT IGNORE would truncate data silently; the statement must not use it")
	}
	if n := strings.Count(p.insertFor(1), "?"); n != mysqlColumns {
		t.Errorf("%d placeholders for one row, want %d", n, mysqlColumns)
	}
	cols := strings.Split(strings.TrimSuffix(strings.TrimPrefix(head,
		"INSERT INTO `syslog`.`syslog_events` ("), ") VALUES "), ", ")
	if len(cols) != len(mysqlColumnFamilies) || len(cols) != mysqlColumns {
		t.Fatalf("%d columns in the statement, %d in the contract", len(cols), len(mysqlColumnFamilies))
	}
	for i, c := range mysqlColumnFamilies {
		if cols[i] != c.name {
			t.Errorf("column %d is %q in the statement and %q in the contract", i, cols[i], c.name)
		}
	}
	if mysqlMaxRows != 3640 || mysqlMaxRows*mysqlColumns > 65535 || (mysqlMaxRows+1)*mysqlColumns <= 65535 {
		t.Errorf("mysqlMaxRows = %d is not the largest row count under 65535 placeholders (SPEC: 3640)", mysqlMaxRows)
	}
}

// retargetMySQLSchema keeps the one CREATE TABLE statement of the schema file
// and renames its table.
func retargetMySQLSchema(schema, table string) string {
	var lines []string
	for _, line := range strings.Split(schema, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = s[:strings.LastIndexByte(s, ';')] // one statement, without the terminator
	return strings.Replace(s, "CREATE TABLE IF NOT EXISTS syslog_events", "CREATE TABLE `"+table+"`", 1)
}

// SPEC §3.7: docker/mysql/schema.sql is the file operators apply and the file
// the integration suite builds its tables from. Without a server this still
// catches the drift that matters: a column the validation demands that the file
// lacks, in the wrong order, or of a type family the validation refuses.
func TestMySQLSchemaFileMatchesTheContract(t *testing.T) {
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	stmt := retargetMySQLSchema(string(b), "t")
	if !strings.HasPrefix(strings.TrimSpace(stmt), "CREATE TABLE `t` (") || strings.Contains(stmt, ";") {
		t.Fatalf("the schema file does not reduce to one CREATE TABLE statement:\n%s", stmt)
	}
	for _, want := range []string{"ENGINE=InnoDB", "COLLATE=utf8mb4_bin", "PRIMARY KEY (uuid, step)"} {
		if !strings.Contains(stmt, want) {
			t.Errorf("the schema lacks %s", want)
		}
	}
	if strings.Contains(strings.ToUpper(string(b)), "INSERT IGNORE") &&
		!strings.Contains(string(b), "Nunca INSERT IGNORE") {
		t.Error("the schema file recommends INSERT IGNORE")
	}

	var cols [][2]string // name, type as written
	for _, line := range strings.Split(stmt, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || strings.HasPrefix(f[0], "CREATE") || f[0] == "INDEX" || f[0] == "PRIMARY" ||
			strings.HasPrefix(f[0], ")") {
			continue
		}
		typ := strings.ToLower(strings.TrimSuffix(f[1], ","))
		full := typ
		if i := strings.IndexByte(typ, '('); i > 0 {
			typ = typ[:i]
		}
		cols = append(cols, [2]string{f[0], typ})
		if f[0] == "uuid" && full != "binary(16)" {
			t.Errorf("uuid is %s, want binary(16)", full)
		}
	}
	if len(cols) != len(mysqlColumnFamilies) {
		t.Fatalf("the schema file has %d columns, the contract %d: %v", len(cols), len(mysqlColumnFamilies), cols)
	}
	for i, c := range mysqlColumnFamilies {
		if cols[i][0] != c.name {
			t.Errorf("column %d is %q in the file and %q in the contract", i, cols[i][0], c.name)
		}
		if !contains(c.types, cols[i][1]) {
			t.Errorf("column %q is %s in the file, which the validation refuses (want %v)", c.name, cols[i][1], c.types)
		}
	}
	for _, c := range cols {
		switch c[0] {
		case "hostname", "appname", "procid", "msgid":
			if c[1] != "longtext" {
				t.Errorf("%s is %s, not longtext; a narrower type turns a long RFC 3164 token into a dropped chunk (SPEC §3.7)", c[0], c[1])
			}
		}
	}
}

// SPEC §10.6: a chunk is cut again at the row ceiling and at the byte ceiling,
// and an envelope that alone exceeds the limit travels by itself rather than
// being skipped.
func TestMySQLChunking(t *testing.T) {
	many := batchN(t, mysqlMaxRows+1).Envelopes
	if end := chunkEnd(many, 0, 1<<40); end != mysqlMaxRows {
		t.Errorf("row ceiling: first statement ends at %d, want %d", end, mysqlMaxRows)
	}
	if end := chunkEnd(many, mysqlMaxRows, 1<<40); end != len(many) {
		t.Errorf("row ceiling: second statement ends at %d, want %d", end, len(many))
	}

	five := batchN(t, 5).Envelopes
	limit := 2*rowCost(&five[0]) + 1 // room for two, not three
	var ends []int
	for start := 0; start < len(five); {
		end := chunkEnd(five, start, limit)
		ends = append(ends, end)
		start = end
	}
	if fmt.Sprint(ends) != "[2 4 5]" {
		t.Errorf("byte ceiling: statement ends %v, want [2 4 5]", ends)
	}

	mixed := batchOf(t, "<13>small", "<13>"+strings.Repeat("x", 100_000), "<13>small").Envelopes
	limit = rowCost(&mixed[0]) * 2
	if end := chunkEnd(mixed, 0, limit); end != 1 {
		t.Errorf("oversized neighbour: first statement ends at %d, want 1", end)
	}
	if end := chunkEnd(mixed, 1, limit); end != 2 {
		t.Errorf("an oversized envelope must travel alone: statement ends at %d, want 2", end)
	}
	if end := chunkEnd(mixed, 2, limit); end != 3 {
		t.Errorf("tail: statement ends at %d, want 3", end)
	}
}

// SPEC §3.9 and §10.6: the row the boot check sizes is never smaller than a row
// the chunker can meet. The lines are built to inflate every part of a row at
// once -- a hostname of invalid bytes grows threefold in its column AND in the
// envelope, a NUL grows threefold in the column.
func TestMySQLMaxRowBytesCoversEveryRow(t *testing.T) {
	const L = 8192
	for name, line := range map[string]string{
		"hostname of invalid bytes":  "<13>Oct 11 22:14:15 " + strings.Repeat("\xff", L-40) + " app: m",
		"hostname of NUL":            "<13>Oct 11 22:14:15 " + strings.Repeat("\x00", L-40) + " app: m",
		"hostname of control bytes":  "<13>Oct 11 22:14:15 " + strings.Repeat("\x01", L-40) + " app: m",
		"pid of digits":              "<13>Oct 11 22:14:15 h app[" + strings.Repeat("7", 120) + "]: " + strings.Repeat("\x01", L-200),
		"structured data of control": `<13>1 - h a p m [x a="` + strings.Repeat("\x01", L-40) + `"] m`,
		"plain message":              "<13>" + strings.Repeat("m", L-4),
	} {
		row := batchOf(t, line).Envelopes[0]
		p := &MySQL{cfg: MySQLConfig{MaxMessageBytes: L, MaxEventBytes: MaxEventBytes(L)}}
		if cost, bound := rowCost(&row), p.maxRowBytes(); cost > bound {
			t.Errorf("%s: a row costs %d bytes, above the %d the boot check sizes", name, cost, bound)
		}
	}
}

// The argument slices hold references to the chunk buffer; they must go back to
// the pool cleared (SPEC §10.5, the cousin of §15.2).
func TestMySQLArgsResetDropsReferences(t *testing.T) {
	a := &mysqlArgs{}
	b := batchN(t, 2)
	a.grow(len(b.Envelopes))
	for i := range b.Envelopes {
		a.add(&b.Envelopes[i], i)
	}
	if len(a.vals) != 2*mysqlColumns {
		t.Fatalf("%d values, want %d", len(a.vals), 2*mysqlColumns)
	}
	if got := a.vals[0].([]byte); len(got) != 16 || [16]byte(got) != [16]byte(b.Envelopes[0].Item.Head.UUID) {
		t.Errorf("uuid travels as %x, want the 16 bytes of the uuid", got)
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

// SPEC §3.7: the emitter's text columns are sanitised copies, NULL when absent;
// env is the untouched JLH, and an empty one is never NULL.
func TestMySQLArgsValues(t *testing.T) {
	line := "<13>Oct 11 22:14:15 web\x00-01 su[230]: msg"
	b := batchOf(t, line)
	a := &mysqlArgs{}
	a.grow(1)
	a.add(&b.Envelopes[0], 0)
	if host := a.vals[12].(string); host != "web�-01" {
		t.Errorf("hostname column = %q, want the NUL replaced", host)
	}
	if app, proc := a.vals[13].(string), a.vals[14].(string); app != "su" || proc != "230" {
		t.Errorf("appname, procid = %q, %q", app, proc)
	}
	if a.vals[15] != nil {
		t.Errorf("msgid = %#v, want NULL: RFC 3164 has none", a.vals[15])
	}
	if src := a.vals[11].(string); src != "10.0.0.7" {
		t.Errorf("src_ip = %q", src)
	}
	if env := a.vals[16].([]byte); string(env) != string(b.Envelopes[0].JLH) {
		t.Error("env was altered; the replacement must touch the column copy only")
	}
	if n := a.vals[17].(int64); n != int64(len(b.Envelopes[0].JLH)) {
		t.Errorf("nbytes = %d", n)
	}
	if f, tr := a.vals[9].(string), a.vals[10].(string); f != "rfc3164" || tr != "tcp" {
		t.Errorf("format, transport = %q, %q", f, tr)
	}

	empty := &mysqlArgs{}
	empty.grow(1)
	e := b.Envelopes[0]
	e.JLH = nil
	empty.add(&e, 0)
	if v, ok := empty.vals[16].([]byte); !ok || v == nil {
		t.Errorf("env = %#v, want an empty non-nil []byte", empty.vals[16])
	}
}

func TestNewMySQLIsLazy(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{
		Addr: "127.0.0.1:1", Username: "u", Database: "d", PoolSize: 2, ConnectTimeout: time.Second,
	}, discard(), metrics())
	if err != nil {
		t.Fatalf("NewMySQL against a dead port: %v", err)
	}
	if p.Type() != "mysql" {
		t.Errorf("Type = %q", p.Type())
	}
	if p.quoted != "`d`.`syslog_events`" || p.schema != "d" || p.name != "syslog_events" {
		t.Errorf("table split into quoted=%q schema=%q name=%q", p.quoted, p.schema, p.name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = p.Health(ctx)
	if err == nil {
		t.Fatal("Health against a dead port succeeded")
	}
	if ClassOf(err) != ClassNetwork {
		t.Errorf("dead port classified as %s, want network so the boot keeps waiting", ClassOf(err))
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Write(ctx, batchN(t, 1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := p.Health(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v, want ErrClosed", err)
	}
}

func TestNewMySQLQualifiedTable(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "Logs.Events"},
		discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.quoted != "`Logs`.`Events`" || p.schema != "Logs" || p.name != "Events" {
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
	base := MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "syslog_events"}

	cases := []struct {
		name string
		mut  func(*MySQLConfig)
		want string
	}{
		{"sql in table", func(c *MySQLConfig) { c.Table = "events; drop table x" }, "identifier"},
		{"backtick in table", func(c *MySQLConfig) { c.Table = "ev`ents" }, "identifier"},
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
			p, err := NewMySQL(cfg, discard(), metrics())
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
