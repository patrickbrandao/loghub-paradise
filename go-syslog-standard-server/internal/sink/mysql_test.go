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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Nothing in this file needs a server. The integration half lives in
// mysql_integration_test.go and skips without SYSLOG_TEST_MYSQL_DSN.

// prepared builds a batch the way a drain worker does. The Prep is released by
// the test's cleanup, so the envelopes stay valid for the whole test.
func prepared(t *testing.T, lines ...string) []Prepared {
	t.Helper()
	items := make([]*envelope.Item, len(lines))
	for i, l := range lines {
		items[i] = item(i, l)
	}
	p := NewPrep("sls-test", syslog.Options{}, nil)
	t.Cleanup(p.Release)
	return p.Build(items)
}

// batchOf is n distinct, valid messages.
func batchOf(t *testing.T, n int) []Prepared {
	t.Helper()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf(`<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] message %d`, i)
	}
	return prepared(t, lines...)
}

// SPEC §10.7.6 is the table; this is the table read back. Schema and privilege
// numbers are fatal, never logic; a read-only server is fatal like 25006 on
// PostgreSQL; unknown numbers are retried.
func TestClassifyMySQLNumber(t *testing.T) {
	cases := []struct {
		n    uint16
		want Class
	}{
		// Waiting fixes it.
		{1040, ClassNetwork}, // too many connections
		{1053, ClassNetwork}, // server shutdown in progress
		{1077, ClassNetwork}, // normal shutdown
		{1158, ClassNetwork}, // net read error
		{1159, ClassNetwork}, // net read timeout
		{1160, ClassNetwork}, // net write error
		{1161, ClassNetwork}, // net write timeout
		{1205, ClassNetwork}, // lock wait timeout
		{1213, ClassNetwork}, // deadlock
		{1317, ClassNetwork}, // query execution was interrupted
		{1927, ClassNetwork}, // connection was killed
		{2006, ClassNetwork}, // server has gone away
		{2013, ClassNetwork}, // lost connection during query
		{3024, ClassNetwork}, // max_execution_time exceeded

		// Waiting never fixes it: end the process.
		{1290, ClassFatal}, // --read-only: the deliberate hard choice
		{1836, ClassFatal}, // running in read-only mode
		{1021, ClassFatal}, // disk full
		{1030, ClassFatal}, // got error from storage engine
		{1041, ClassFatal}, // out of memory
		{1114, ClassFatal}, // table is full
		{1044, ClassFatal}, // access denied for database
		{1045, ClassFatal}, // access denied for user
		{1049, ClassFatal}, // unknown database
		{1130, ClassFatal}, // host not allowed to connect
		{1142, ClassFatal}, // command denied: no INSERT, or no UPDATE
		{1143, ClassFatal}, // column access denied
		{1054, ClassFatal}, // unknown column
		{1146, ClassFatal}, // table does not exist
		{1364, ClassFatal}, // field has no default value

		// The destination is alive and refused THIS data.
		{1048, ClassLogic}, // column cannot be null
		{1062, ClassLogic}, // duplicate entry on some OTHER unique key
		{1153, ClassLogic}, // packet bigger than max_allowed_packet
		{1264, ClassLogic}, // out of range
		{1366, ClassLogic}, // incorrect string value
		{1406, ClassLogic}, // data too long
		{1690, ClassLogic}, // value is out of range

		// Unknown is retried, not dropped.
		{0, ClassNetwork},
		{1064, ClassNetwork},
		{9999, ClassNetwork},
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
	inner := &mysql.MySQLError{Number: 1146, Message: "Table 'syslog.events' doesn't exist"}
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
}

// SPEC §10.7.3: one placeholder group of seventeen per row, the columns of §3.7
// in table order, ON DUPLICATE KEY UPDATE and never INSERT IGNORE.
func TestMySQLInsertStatement(t *testing.T) {
	p := &MySQL{quoted: "`syslog`.`events`"}
	s := p.insertFor(3)
	want := "INSERT INTO `syslog`.`events` (id, ts, nts, facility, severity, hostname, app_name, " +
		"procid, msgid, peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes) VALUES "
	if !strings.HasPrefix(s, want) {
		t.Errorf("unexpected head: %s", s)
	}
	if !strings.HasSuffix(s, " ON DUPLICATE KEY UPDATE id = id") {
		t.Errorf("unexpected tail: %s", s)
	}
	if n := strings.Count(s, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); n != 3 {
		t.Errorf("%d placeholder groups, want 3", n)
	}
	if strings.Contains(s, "IGNORE") {
		t.Error("INSERT IGNORE would truncate data silently; the statement must not use it")
	}
	if n := strings.Count(p.insertFor(1), "?"); n != mysqlColumns {
		t.Errorf("%d placeholders for one row, want %d", n, mysqlColumns)
	}
	// The column list must name exactly the columns the validation demands, in
	// the same order the values are appended.
	cols := strings.Split(strings.TrimSuffix(strings.TrimPrefix(want, "INSERT INTO `syslog`.`events` ("), ") VALUES "), ", ")
	if len(cols) != len(mysqlColumnFamilies) {
		t.Fatalf("%d columns in the statement, %d in the contract", len(cols), len(mysqlColumnFamilies))
	}
	for i, c := range mysqlColumnFamilies {
		if cols[i] != c.name {
			t.Errorf("column %d is %q in the statement and %q in the contract", i, cols[i], c.name)
		}
	}
	if mysqlMaxRows*mysqlColumns > 65535 || (mysqlMaxRows+1)*mysqlColumns <= 65535 {
		t.Errorf("mysqlMaxRows = %d is not the largest row count under 65535 placeholders", mysqlMaxRows)
	}
}

// SPEC §3.7: docker/mysql/schema.sql is the file operators apply and the file
// the integration suite builds its tables from. Without a server this still
// catches the drift that matters: a column the validation demands that the
// file lacks, in the wrong order, or of a type family the validation refuses.
func TestMySQLSchemaFileMatchesTheContract(t *testing.T) {
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	stmt := retargetSchema(string(b), "t")
	// Inline comments go too: a ';' inside one is harmless to the server and
	// splits the statement for any tool that cuts a file on semicolons.
	var code []string
	for _, line := range strings.Split(stmt, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			if strings.Contains(line[i:], ";") {
				t.Errorf("a ';' inside the comment of %q", strings.TrimSpace(line))
			}
			line = line[:i]
		}
		code = append(code, line)
	}
	stmt = strings.Join(code, "\n")
	if !strings.HasPrefix(strings.TrimSpace(stmt), "CREATE TABLE `t` (") || strings.Contains(stmt, ";") {
		t.Fatalf("the schema file does not reduce to one CREATE TABLE statement:\n%s", stmt)
	}
	for _, want := range []string{"ENGINE=InnoDB", "COLLATE=utf8mb4_bin", "PRIMARY KEY"} {
		if !strings.Contains(stmt, want) {
			t.Errorf("the schema lacks %s", want)
		}
	}

	var cols [][2]string // name, type as written
	for _, line := range strings.Split(stmt, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || strings.HasPrefix(f[0], "CREATE") || f[0] == "INDEX" || strings.HasPrefix(f[0], ")") {
			continue
		}
		typ := strings.ToLower(f[1])
		if i := strings.IndexByte(typ, '('); i > 0 {
			typ = typ[:i]
		}
		cols = append(cols, [2]string{f[0], typ})
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
		case "hostname", "app_name", "procid", "msgid":
			if c[1] != "longtext" {
				t.Errorf("%s is %s, not longtext; a narrower type turns a long RFC 3164 token into a dropped batch (SPEC §3.7)", c[0], c[1])
			}
		}
	}
}

// SPEC §10.7.3: a batch is cut at the row ceiling and at the byte ceiling, and a
// message that alone exceeds the limit travels by itself rather than being
// skipped.
func TestMySQLChunking(t *testing.T) {
	many := batchOf(t, mysqlMaxRows+1)
	if end := chunkEnd(many, 0, 1<<40); end != mysqlMaxRows {
		t.Errorf("row ceiling: first chunk ends at %d, want %d", end, mysqlMaxRows)
	}
	if end := chunkEnd(many, mysqlMaxRows, 1<<40); end != len(many) {
		t.Errorf("row ceiling: second chunk ends at %d, want %d", end, len(many))
	}

	items := batchOf(t, 5)
	limit := 2*rowCost(&items[0]) + 1 // room for two, not three
	var ends []int
	for start := 0; start < len(items); {
		end := chunkEnd(items, start, limit)
		ends = append(ends, end)
		start = end
	}
	if fmt.Sprint(ends) != "[2 4 5]" {
		t.Errorf("byte ceiling: chunk ends %v, want [2 4 5]", ends)
	}

	mixed := prepared(t, "<13>small", "<13>"+strings.Repeat("x", 100_000), "<13>small")
	limit = rowCost(&mixed[0]) * 2
	if end := chunkEnd(mixed, 0, limit); end != 1 {
		t.Errorf("oversized neighbour: first chunk ends at %d, want 1", end)
	}
	if end := chunkEnd(mixed, 1, limit); end != 2 {
		t.Errorf("oversized message must travel alone: chunk ends at %d, want 2", end)
	}
	if end := chunkEnd(mixed, 2, limit); end != 3 {
		t.Errorf("tail: chunk ends at %d, want 3", end)
	}
}

// SPEC §3.9 and §10.7.4: the row the boot check sizes is never smaller than a
// row the chunker can meet. The lines are the ones built to inflate every part
// of a row at once -- a hostname of invalid bytes grows three times in its
// column and three times in the envelope.
func TestMySQLMaxRowBytesCoversEveryRow(t *testing.T) {
	for name, line := range map[string]string{
		"hostname of invalid bytes":  "<13>Oct 11 22:14:15 " + strings.Repeat("\xff", 8192) + " app: m",
		"hostname of control bytes":  "<13>Oct 11 22:14:15 " + strings.Repeat("\x01", 8192) + " app: m",
		"pid of digits":              "<13>Oct 11 22:14:15 h app[" + strings.Repeat("7", 8192) + "]: m",
		"structured data of control": `<13>1 - h a p m [x a="` + strings.Repeat("\x01", 8192) + `"] m`,
		"plain message":              "<13>" + strings.Repeat("m", 8192),
	} {
		row := prepared(t, line)[0]
		raw := int64(len(line))
		p := &MySQL{cfg: MySQLConfig{
			MaxRawBytes:      raw,
			MaxEnvelopeBytes: envelope.MaxBytes(raw, int64(len(row.Item.Head.Listener)), int64(len("sls-test"))),
		}}
		if cost, bound := rowCost(&row), p.maxRowBytes(); cost > bound {
			t.Errorf("%s: a row costs %d bytes, above the %d the boot check sizes", name, cost, bound)
		}
	}
}

// The argument slices hold references to env and raw; they must go back to the
// pool cleared (SPEC §10.7.3, the cousin of §14.2).
func TestMySQLArgsResetDropsReferences(t *testing.T) {
	a := &mysqlArgs{}
	batch := batchOf(t, 2)
	a.grow(len(batch))
	for i := range batch {
		if !a.add(&batch[i], i) {
			t.Fatal("add refused a valid message")
		}
	}
	if len(a.vals) != 2*mysqlColumns {
		t.Fatalf("%d values, want %d", len(a.vals), 2*mysqlColumns)
	}
	if got := a.vals[0].([]byte); len(got) != 16 {
		t.Errorf("id travels as %d bytes, want 16", len(got))
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

// SPEC §3.7 and §10.7.3: the emitter's text columns are sanitised copies, the
// blobs are the untouched bytes, and an empty blob is never NULL.
func TestMySQLArgsValues(t *testing.T) {
	line := "<13>Oct 11 22:14:15 web\xff\x00-01 su[230]: msg"
	b := prepared(t, line)
	a := &mysqlArgs{}
	a.grow(1)
	if !a.add(&b[0], 0) {
		t.Fatal("add refused a valid message")
	}
	if host := a.vals[5].(string); host != "web��-01" {
		t.Errorf("hostname column = %q, want the invalid byte and the NUL replaced", host)
	}
	if app, proc := a.vals[6].(string), a.vals[7].(string); app != "su" || proc != "230" {
		t.Errorf("app_name, procid = %q, %q", app, proc)
	}
	if raw := a.vals[15].([]byte); string(raw) != line {
		t.Errorf("raw = %q, want the line byte for byte", raw)
	}
	if env := a.vals[14].([]byte); string(env) != string(b[0].Env) {
		t.Error("env was altered; the replacement must touch the column copy only")
	}
	if n := a.vals[16].(int64); n != int64(len(b[0].Env)+len(line)) {
		t.Errorf("nbytes = %d", n)
	}

	empty := &mysqlArgs{}
	empty.grow(1)
	it := envelope.NewItem(envelope.Head{ID: nilUUID}, nil, -1)
	if !empty.add(&Prepared{Item: it}, 0) {
		t.Fatal("add refused the message")
	}
	for _, i := range []int{14, 15} {
		if v, ok := empty.vals[i].([]byte); !ok || v == nil {
			t.Errorf("value %d = %#v, want an empty non-nil []byte", i, empty.vals[i])
		}
	}
	bad := &mysqlArgs{}
	bad.grow(1)
	if bad.add(&Prepared{Item: envelope.NewItem(envelope.Head{ID: "not-a-uuid"}, nil, -1)}, 0) {
		t.Error("a malformed id must be refused, not sent")
	}
}

func TestNewMySQLIsLazy(t *testing.T) {
	p, err := NewMySQL(MySQLConfig{
		Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "events", PoolSize: 2,
		ConnectTimeout: time.Second,
	}, discard(), metrics())
	if err != nil {
		t.Fatalf("NewMySQL against a dead port: %v", err)
	}
	if p.Type() != "mysql" {
		t.Errorf("Type = %q", p.Type())
	}
	if p.quoted != "`d`.`events`" || p.schema != "d" || p.name != "events" {
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
	if err := p.Write(ctx, batchOf(t, 1)); !errors.Is(err, ErrClosed) {
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
	base := MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "events"}

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
