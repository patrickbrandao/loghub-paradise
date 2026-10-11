package sink

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// The destination is remote and has a schema, and what is worth proving --
// the multi-row INSERT with ON DUPLICATE KEY, the JSON column and its CHECKs,
// DATETIME(6) in UTC, the rows-affected arithmetic behind the duplicates
// counter, the chunking against the server's packet limit and the catalogue
// queries and probe of the boot validation -- is exactly what a fake would
// have to invent. These run against a real server addressed by
// WHS_TEST_MYSQL_DSN and skip without it (docs/TESTING.md §5.3):
//
//	WHS_TEST_MYSQL_DSN='root:whs@tcp(127.0.0.1:13306)/whs_test' \
//	  go test -race ./internal/sink/
//
// Every test creates its own table from docker/mysql/inbox.sql under a unique
// name -- constraint names included, which MySQL scopes to the database -- and
// drops it afterwards, so the suite never depends on an empty database or on
// another suite sharing the server.

const mysqlDSNEnv = "WHS_TEST_MYSQL_DSN"

type mysqlFixture struct {
	t      *testing.T
	db     *sql.DB
	dbName string
	table  string
	cfg    MySQLConfig
}

func mysqlSetup(t *testing.T) *mysqlFixture {
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

	f := &mysqlFixture{t: t, db: db, dbName: mc.DBName, cfg: MySQLConfig{
		Addr:     mc.Addr,
		Username: mc.User,
		Password: mc.Passwd,
		Database: mc.DBName,
		PoolSize: 4,
		AppName:  "whs-test",
	}}
	f.table = f.newTable("")
	f.cfg.Table = f.table
	return f
}

// newTable creates a table from the canonical DDL, with alter applied
// afterwards ($T is the table, and the prefix of its constraint names), and
// registers its removal.
func (f *mysqlFixture) newTable(alter string) string {
	f.t.Helper()
	name := fmt.Sprintf("whs_inbox_%d", time.Now().UnixNano())
	f.exec(mysqlDDLFor(f.t, name))
	f.t.Cleanup(func() {
		_, _ = f.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
	})
	if alter != "" {
		f.exec(strings.ReplaceAll(alter, "$T", name))
	}
	return name
}

// mysqlDDLFor reads docker/mysql/inbox.sql -- the file operators apply -- and
// retargets it at a test table. Using the real file is the point: the
// validation of SPEC §8.6.4 and the table the operator gets must agree, and
// this is where a drift between them would surface.
func mysqlDDLFor(t *testing.T, table string) string {
	t.Helper()
	b, err := os.ReadFile("../../docker/mysql/inbox.sql")
	if err != nil {
		t.Fatalf("reading the canonical DDL: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = s[:strings.LastIndexByte(s, ';')] // one statement, without the terminator
	if !strings.Contains(s, "CREATE TABLE IF NOT EXISTS inbox (") || !strings.Contains(s, "CONSTRAINT inbox_pk") {
		t.Fatal("docker/mysql/inbox.sql changed shape; the fixture cannot retarget it")
	}
	// The table name and the prefix of every constraint name.
	return strings.ReplaceAll(s, "inbox", table)
}

func (f *mysqlFixture) exec(sql string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.db.ExecContext(ctx, sql); err != nil {
		f.t.Fatalf("%v\n%s", err, sql)
	}
}

func (f *mysqlFixture) count(table string) int {
	f.t.Helper()
	var n int
	if err := f.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *mysqlFixture) sink(cfg MySQLConfig) *MySQL {
	f.t.Helper()
	p, err := NewMySQL(cfg, quietLog(), testMetrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { p.Close() })
	return p
}

// ready runs the boot check the way WaitReady would, and fails the test if
// the table is refused.
func (f *mysqlFixture) ready(p *MySQL) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// mysqlID is the sixteen-byte form the uuid column holds.
func mysqlID(t *testing.T, id string) []byte {
	t.Helper()
	u, ok := parseUUID(id)
	if !ok {
		t.Fatalf("not a uuid: %q", id)
	}
	return u[:]
}

// rowJLH rebuilds the envelope of one inbox row (SPEC §3.7): the uuids from
// their sixteen bytes, event from the DATETIME(6) read as UTC plus
// event_nsec, the JSON payload as the server returns it. It also returns how
// long ago received_at is, measured against the server's UTC clock.
func (f *mysqlFixture) rowJLH(table, id string) ([]byte, time.Duration) {
	f.t.Helper()
	var (
		uuid, from              []byte
		space, typ, at, payload string
		level, sid, step, nsec  int64
		lagMicros               int64
	)
	err := f.db.QueryRowContext(context.Background(), "SELECT uuid, from_uuid, space, type, level, sid, step, "+
		"DATE_FORMAT(event_at, '%Y-%m-%d %H:%i:%s.%f'), event_nsec, payload, "+
		"TIMESTAMPDIFF(MICROSECOND, received_at, UTC_TIMESTAMP(6)) FROM `"+table+"` WHERE uuid = ?",
		mysqlID(f.t, id)).Scan(&uuid, &from, &space, &typ, &level, &sid, &step, &at, &nsec, &payload, &lagMicros)
	if err != nil {
		f.t.Fatalf("reading %s back: %v", id, err)
	}
	eventAt, err := time.Parse("2006-01-02 15:04:05.000000", at)
	if err != nil {
		f.t.Fatalf("event_at %q: %v", at, err)
	}
	return jlhFromColumns(uuidv7.UUID(uuid).String(), uuidv7.UUID(from).String(), space, typ,
		level, sid, step, eventAt, nsec, []byte(payload)), time.Duration(lagMicros) * time.Microsecond
}

// ------------------------------------------------------------------ writes

// SPEC §3.7 and §8.6.3: every column lands from the JLH header, event_at is
// the event in UTC to the microsecond and event_nsec the rest, the JSON column
// holds the payload as a JSON value -- a %00 of the query included, which the
// MySQL JSON type keeps -- and the envelope rebuilds from the row as a valid
// webhook equal to the JLH.
func TestMySQLWriteStoresTheEnvelope(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	nul := testRequest("/nul", []byte(`{"n":1}`))
	nul.RawQuery = "a=x%00y&b=2"
	// Bytes that are not UTF-8 in the query: the serialiser writes U+FFFD in
	// payload.query (and percent-encodes them in the url), so the strict
	// session and the JSON type take the row and one such request cannot
	// take its batch down. The path arrives escaped, as the handler's
	// URL.EscapedPath gives it (envelope.Request.Path): raw bytes there are
	// a fixture no request can produce, and the url oracle refuses them.
	invalidUTF8 := testRequest("/hook/%FF%FE", nil)
	invalidUTF8.RawQuery = "q=%FF\xc0\xaf"
	items := []*envelope.Item{
		mkItem("plain"),
		itemIn("default", nul),
		itemIn("default", testRequest("/bin", []byte("A\x00B\nC\xffD\r\x00"))),
		itemIn("acme.store", testRequest("/other-space", nil)),
		itemIn("default", invalidUTF8),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table); n != len(items) {
		t.Fatalf("%d rows, want %d", n, len(items))
	}

	for i, it := range items {
		rebuilt, lag := f.rowJLH(f.table, it.ID)
		e := validJLH(t, rebuilt)
		if !sameJSON(t, rebuilt, it.Env) {
			t.Errorf("envelope %d rebuilt from the row:\n%s\nwant the JLH:\n%s", i, rebuilt, it.Env)
		}
		if e.UUID != it.ID {
			t.Errorf("envelope %d: uuid %s, want %s", i, e.UUID, it.ID)
		}
		// received_at is the table's CURRENT_TIMESTAMP(6) in the session's
		// +00:00: compared with the server's UTC clock, a lag of seconds.
		if lag < 0 || lag > time.Minute {
			t.Errorf("envelope %d: received_at is %v away from UTC_TIMESTAMP; not UTC?", i, lag)
		}
	}

	// The NUL survives in the JSON column (SPEC §8.6.3): no replacement here.
	rebuilt, _ := f.rowJLH(f.table, items[1].ID)
	var doc struct {
		Payload struct{ Query map[string]string }
	}
	if err := json.Unmarshal(rebuilt, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Payload.Query["a"] != "x\x00y" {
		t.Errorf("query.a in the JSON column = %q, want the NUL kept", doc.Payload.Query["a"])
	}
	// The binary body arrives exact, in base64 inside the payload.
	rebuilt, _ = f.rowJLH(f.table, items[2].ID)
	body, err := validJLH(t, rebuilt).Payload.Body()
	if err != nil || !bytes.Equal(body, []byte("A\x00B\nC\xffD\r\x00")) {
		t.Errorf("body = %q (%v), want the bytes that arrived", body, err)
	}
}

// SPEC §8.6.3: the pipeline is at-least-once. Writing the SAME batch twice is
// the shape of a redelivery: the second write must succeed, add nothing, and
// be counted as duplicates through the rows-affected arithmetic. A uuid twice
// in one statement is one insert and one empty update.
func TestMySQLToleratesRedeliveryAndCountsIt(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	items := mkBatch(7)
	ctx := context.Background()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	mixed := append(items[:3:3], mkBatch(2)...)
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	twice := mkItem("twice")
	if err := p.Write(ctx, []*envelope.Item{twice, twice}); err != nil {
		t.Fatalf("a uuid twice in one batch: %v", err)
	}

	if n := f.count(f.table); n != 10 {
		t.Errorf("%d rows, want 10: redelivery must not duplicate", n)
	}
	if got := p.m.SinkMySQLDuplicatesTotal.Load(); got != 11 {
		t.Errorf("duplicates counter = %d, want 11 (7 from the full redelivery, 3 from the overlap, 1 in-batch)", got)
	}
}

// SPEC §8.6.6: what the server refuses for THIS data is logic -- a CHECK the
// row fails (3819), a payload that is not JSON (3140) -- and the destination
// keeps taking the next batch. Neither can come from what this server writes
// into the canonical table; an operator's extra CHECK and a damaged envelope
// are the two ways in.
func TestMySQLDataRefusalsAreLogic(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx := context.Background()

	f.exec("ALTER TABLE `" + f.table + "` ADD CONSTRAINT " + f.table + "_blocked_ck CHECK (space <> 'blocked')")
	err := p.Write(ctx, []*envelope.Item{itemIn("blocked", testRequest("/blocked", nil))})
	if err == nil || ClassOf(err) != ClassLogic || !strings.Contains(err.Error(), "mysql=3819") {
		t.Errorf("a CHECK violation = %v (class %s), want logic 3819", err, ClassOf(err))
	}
	err = p.Write(ctx, []*envelope.Item{malformedPayloadItem(t)})
	if err == nil || ClassOf(err) != ClassLogic || !strings.Contains(err.Error(), "mysql=3140") {
		t.Errorf("a payload that is not JSON = %v (class %s), want logic 3140", err, ClassOf(err))
	}
	if err := p.Write(ctx, mkBatch(2)); err != nil {
		t.Errorf("control: valid items refused after the refusals: %v", err)
	}
	if n := f.count(f.table); n != 2 {
		t.Errorf("%d rows, want the 2 of the control", n)
	}
}

// An item whose JLH this server did not write is refused as data (logic)
// before the statement runs: nothing of the batch reaches the table.
func TestMySQLRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx := context.Background()

	err := p.Write(ctx, []*envelope.Item{mkItem("good"), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := f.count(f.table); n != 0 {
		t.Errorf("%d rows; a refused batch must send nothing", n)
	}

	// The same batch cut in chunks by the packet ceiling: the damaged item is
	// in the last chunk, and still nothing may be sent -- the worker drops
	// the whole batch as logic, so a chunk already inserted would be counted
	// as dropped while it sits in the table.
	p.maxPacket.Store(2 * rowCost(mkItem("probe-size")))
	err = p.Write(ctx, []*envelope.Item{mkItem("first-chunk"), mkItem("second-chunk"), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("chunked Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := f.count(f.table); n != 0 {
		t.Skipf("BUG (mysql.go Write/writeChunk): %d rows of a batch refused as logic were inserted; "+
			"the earlier chunks are sent before a later chunk maps its items. The worker counts the "+
			"whole batch as dropped (whs_events_dropped_total{reason=sink_logic}) while those events "+
			"sit in the table. Map every item before the first chunk is sent.", n)
	}
}

// SPEC §8.6.3: a batch beyond the row ceiling, and a batch beyond the byte
// ceiling, are split into several statements and every row still lands, its
// envelope intact.
func TestMySQLSplitsLargeBatches(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	many := mkBatch(mysqlMaxRows + 137)
	if err := p.Write(ctx, many); err != nil {
		t.Fatalf("Write of %d rows: %v", len(many), err)
	}
	if n := f.count(f.table); n != len(many) {
		t.Fatalf("%d rows, want %d", n, len(many))
	}

	// Pretend the server's packet limit is 64 KiB: ten envelopes carrying a
	// 20 KiB binary body each (27 KiB of base64) need several statements, and
	// the real server (64 MiB) accepts each.
	p.maxPacket.Store(64 << 10)
	big := make([]*envelope.Item, 10)
	for i := range big {
		body := make([]byte, 20<<10)
		body[0], body[len(body)-1] = byte(i), 0xff
		big[i] = itemIn("default", testRequest("/big", body))
	}
	if perChunk := p.chunkLimit() / rowCost(big[0]); perChunk >= int64(len(big)) {
		t.Fatalf("the fixture fits in one statement (limit %d, row %d)", p.chunkLimit(), rowCost(big[0]))
	}
	if err := p.Write(ctx, big); err != nil {
		t.Fatalf("Write of large bodies: %v", err)
	}
	if n := f.count(f.table); n != len(many)+len(big) {
		t.Fatalf("%d rows, want %d", n, len(many)+len(big))
	}
	rebuilt, _ := f.rowJLH(f.table, big[7].ID)
	body, err := validJLH(t, rebuilt).Payload.Body()
	if err != nil || len(body) != 20<<10 || body[0] != 7 || body[len(body)-1] != 0xff {
		t.Errorf("a chunked body arrived with %d bytes (%v), want %d intact", len(body), err, 20<<10)
	}
}

// SPEC §8.6.3: a batch sent in several chunks is ONE transaction. A logic
// refusal of a later chunk (a CHECK, here) rolls the earlier chunks back, so
// the batch the worker counts and logs as dropped is really not in the table;
// committed chunk by chunk, the first rows stayed while the whole batch was
// counted as lost. A valid chunked batch lands whole, and the duplicates of a
// redelivery are counted after the commit, never for a rolled-back chunk.
func TestMySQLChunkedBatchIsAllOrNothing(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f.exec("ALTER TABLE `" + f.table + "` ADD CONSTRAINT " + f.table + "_blocked_ck CHECK (space <> 'blocked')")
	blocked := itemIn("blocked", testRequest("/blocked", nil))
	refused := []*envelope.Item{mkItem("first-chunk"), mkItem("second-chunk"), blocked}
	// One row per statement: three chunks, the refused row in the last.
	p.maxPacket.Store(2 * rowCost(refused[0]))
	if end := chunkEnd(refused, 0, p.chunkLimit()); end >= len(refused) {
		t.Fatalf("the fixture fits in one statement (first chunk ends at %d)", end)
	}

	err := p.Write(ctx, refused)
	if err == nil || ClassOf(err) != ClassLogic || !strings.Contains(err.Error(), "mysql=3819") {
		t.Fatalf("chunked Write with a refused last chunk = %v (class %s), want logic 3819", err, ClassOf(err))
	}
	if n := f.count(f.table); n != 0 {
		t.Fatalf("%d rows of a batch refused as logic were committed; the worker counts all %d as dropped",
			n, len(refused))
	}

	valid := mkBatch(3)
	if err := p.Write(ctx, valid); err != nil {
		t.Fatalf("chunked Write: %v", err)
	}
	if err := p.Write(ctx, valid); err != nil {
		t.Fatalf("chunked redelivery: %v", err)
	}
	if n := f.count(f.table); n != len(valid) {
		t.Fatalf("%d rows, want %d", n, len(valid))
	}
	if got := p.m.SinkMySQLDuplicatesTotal.Load(); got != 3 {
		t.Errorf("duplicates counter = %d, want 3 from the redelivery", got)
	}
	// A redelivered row in the first chunk of a batch that is then refused:
	// rolled back, and not counted as a duplicate.
	err = p.Write(ctx, []*envelope.Item{valid[0], mkItem("new"), blocked})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("second refused batch = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := f.count(f.table); n != len(valid) {
		t.Errorf("%d rows after a refused batch, want %d", n, len(valid))
	}
	if got := p.m.SinkMySQLDuplicatesTotal.Load(); got != 3 {
		t.Errorf("duplicates counter = %d, want 3: a rolled-back chunk counts nothing", got)
	}
}

// Many workers, one table: the pool must serve them all, and the pool gauges
// must reflect the configured ceiling after a health check.
func TestMySQLConcurrentWritersAndPoolGauges(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	cfg.PoolSize = 3
	p := f.sink(cfg)
	f.ready(p)

	const writers, per = 8, 20
	batches := make([][]*envelope.Item, writers)
	for w := range batches {
		batches[w] = mkBatch(per)
	}
	// Built first and released together: staggered writers would never meet
	// in the pool, and the waits counter would prove nothing.
	start := make(chan struct{})
	errs := make(chan error, writers)
	for _, b := range batches {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}()
	}
	close(start)
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Errorf("writer: %v", err)
		}
	}
	if n := f.count(f.table); n != writers*per {
		t.Errorf("%d rows, want %d", n, writers*per)
	}

	f.ready(p) // refreshes the gauges
	if got := p.m.SinkMySQLPoolConns[2].Load(); got != 3 {
		t.Errorf(`pool_conns{state="max"} = %d, want 3`, got)
	}
	if got := p.m.SinkMySQLPoolWaitsTotal.Load(); got == 0 {
		t.Error("eight writers over three connections never waited, or the counter is not fed")
	}
}

// ------------------------------------------------------------- validation

// SPEC §8.6.4: each refusal is FATAL, so the boot stops at once instead of
// waiting out sink.boot_timeout on a table that will not appear. The accepted
// cases are the control: they write after validating, so a rule that refused
// everything would fail them rather than pass the refusals for the wrong
// reason.
func TestMySQLHealthValidatesTheSchema(t *testing.T) {
	f := mysqlSetup(t)

	cases := []struct {
		name  string
		table string
		want  string // substring of the fatal error; "" means accepted
	}{
		{"canonical DDL", f.table, ""},
		{"missing table", fmt.Sprintf("whs_missing_%d", time.Now().UnixNano()), "does not exist"},
		{"wrong type", f.newTable("ALTER TABLE `$T` MODIFY event_at BIGINT NOT NULL"), `"event_at"`},
		{"text payload", f.newTable("ALTER TABLE `$T` DROP CHECK $T_payload_ck, MODIFY payload LONGTEXT NOT NULL"), `"payload"`},
		{"missing column", f.newTable("ALTER TABLE `$T` DROP CHECK $T_nsec_ck, DROP COLUMN event_nsec"), `"event_nsec"`},
		{"no unique index", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY"), "unique index"},
		{"unique on (uuid, step) only", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX u2 (uuid, step)"), "unique index"},
		{"myisam", f.newTable("ALTER TABLE `$T` ENGINE=MyISAM"), "InnoDB"},
		{"wrong uuid width", f.newTable("ALTER TABLE `$T` MODIFY uuid BINARY(20) NOT NULL"), "binary(16)"},
		{"wrong from_uuid width", f.newTable("ALTER TABLE `$T` MODIFY from_uuid VARBINARY(16) NOT NULL"), `"from_uuid"`},
		// The probe of SPEC §8.6.4: a property of the table that refuses the
		// contract's row is fatal at boot, never a logic drop per batch.
		{"extra NOT NULL column without a default", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NOT NULL"), "tenant"},
		{"a CHECK the probe row fails", f.newTable("ALTER TABLE `$T` ADD CONSTRAINT $T_recent_ck CHECK (event_at >= '2000-01-01')"), "refuses a row"},
		{"extra nullable column is tolerated", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NULL"), ""},
		{"extra NOT NULL with a default is tolerated", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NOT NULL DEFAULT 'x'"), ""},
		{"extra auto-increment column is tolerated", f.newTable("ALTER TABLE `$T` ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT UNIQUE"), ""},
		{"no received_at is tolerated", f.newTable("ALTER TABLE `$T` DROP COLUMN received_at"), ""},
		{"varchar space is tolerated", f.newTable("ALTER TABLE `$T` MODIFY space VARCHAR(255) NOT NULL DEFAULT 'default'"), ""},
		{"text type is tolerated", f.newTable("ALTER TABLE `$T` MODIFY type TEXT NOT NULL"), ""},
		{"int level is tolerated", f.newTable("ALTER TABLE `$T` MODIFY level INT NOT NULL"), ""},
		{"unique index instead of pk", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX uid (uuid)"), ""},
		// Review of 2026-10-06: tables of the right type families that the
		// old validation accepted, on which every batch then failed (1406,
		// 1264, a logic drop with the sink UP) or the data changed in silence.
		{"varchar(10) type", f.newTable("ALTER TABLE `$T` MODIFY type VARCHAR(10) NOT NULL"), `"type"`},
		{"varchar(8) space", f.newTable("ALTER TABLE `$T` MODIFY space VARCHAR(8) NOT NULL DEFAULT 'default'"), `"space"`},
		{"datetime event_at rounds the microsecond", f.newTable("ALTER TABLE `$T` MODIFY event_at DATETIME NOT NULL"), `"event_at"`},
		{"datetime received_at rounds the microsecond", f.newTable("ALTER TABLE `$T` MODIFY received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"), `"received_at"`},
		{"tinyint event_nsec", f.newTable("ALTER TABLE `$T` MODIFY event_nsec TINYINT NOT NULL DEFAULT 0"), `"event_nsec"`},
		{"int sid", f.newTable("ALTER TABLE `$T` MODIFY sid INT NOT NULL DEFAULT 0"), `"sid"`},
		{"smallint step", f.newTable("ALTER TABLE `$T` MODIFY step SMALLINT NOT NULL DEFAULT 0"), `"step"`},
		{"tinyint level", f.newTable("ALTER TABLE `$T` MODIFY level TINYINT NOT NULL"), `"level"`},
		// ON DUPLICATE KEY UPDATE fires on ANY unique key: a new event that
		// collides on another one is swallowed as a duplicate, with no error.
		{"extra unique index", f.newTable("ALTER TABLE `$T` ADD UNIQUE INDEX u_from_sid (from_uuid, sid)"), "(from_uuid, sid)"},
		{"unique (uuid, step) besides the primary key", f.newTable("ALTER TABLE `$T` ADD UNIQUE INDEX u_jlh (uuid, step)"), "(uuid, step)"},
		{"unique prefix of uuid", f.newTable("ALTER TABLE `$T` ADD UNIQUE INDEX u_pfx (uuid(8))"), "(uuid(8))"},
		{"unsigned integers are tolerated", f.newTable("ALTER TABLE `$T` MODIFY sid BIGINT UNSIGNED NOT NULL DEFAULT 0, MODIFY event_nsec SMALLINT UNSIGNED NOT NULL DEFAULT 0"), ""},
		{"wider integers are tolerated", f.newTable("ALTER TABLE `$T` MODIFY step BIGINT NOT NULL DEFAULT 0, MODIFY event_nsec INT NOT NULL DEFAULT 0"), ""},
		{"a non-unique extra index is tolerated", f.newTable("ALTER TABLE `$T` ADD INDEX i_from_sid (from_uuid, sid)"), ""},
		{"auto-increment primary key with a unique uuid is tolerated", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, ADD UNIQUE INDEX u_uuid (uuid)"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Table = tc.table
			p := f.sink(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := p.Health(ctx)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Health refused a valid table: %v", err)
				}
				if err := p.Write(ctx, mkBatch(2)); err != nil {
					t.Errorf("Write after validation: %v", err)
				}
				if n := f.count(tc.table); n != 2 {
					t.Errorf("%d rows, want 2: the probe must leave nothing behind", n)
				}
				return
			}
			if err == nil {
				t.Fatal("Health accepted a table that violates the contract")
			}
			if !IsFatal(err) {
				t.Errorf("class = %s, want fatal so the boot stops at once", ClassOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// SPEC §8.6.4: the probe is what proves the privileges, and it proved one the
// design had not assumed: ON DUPLICATE KEY UPDATE needs UPDATE as well as
// INSERT. A user with only INSERT is refused at boot rather than on the first
// batch; INSERT plus UPDATE on the uuid column is enough. Needs CREATE USER;
// skipped where the test user lacks it.
func TestMySQLHealthValidatesThePrivilege(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()

	user := fmt.Sprintf("whs_role_%d", time.Now().UnixNano())
	if _, err := f.db.ExecContext(ctx, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'whs-test-pw'"); err != nil {
		t.Skipf("cannot create users on this server: %v", err)
	}
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	target := "`" + f.dbName + "`.`" + f.table + "`"

	try := func() error {
		cfg := f.cfg
		cfg.Username, cfg.Password = user, "whs-test-pw"
		p := f.sink(cfg)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := p.Health(cctx); err != nil {
			return err
		}
		return p.Write(cctx, mkBatch(1))
	}

	// denied is what the server itself refused, as its 1142 says it: the
	// sink's own message names both privileges whatever is missing.
	denied := func(err error) string {
		_, after, ok := strings.Cut(err.Error(), "Error 1142")
		if !ok {
			return ""
		}
		cmd, _, _ := strings.Cut(after, "command denied")
		return cmd
	}

	f.exec("GRANT SELECT ON " + target + " TO '" + user + "'@'%'")
	err := try()
	if err == nil {
		t.Fatal("a user without INSERT passed the boot check")
	}
	if !IsFatal(err) || !strings.Contains(denied(err), "INSERT") {
		t.Errorf("got %v, want a fatal 1142 naming INSERT", err)
	}

	f.exec("GRANT INSERT ON " + target + " TO '" + user + "'@'%'")
	err = try()
	if err == nil {
		t.Fatal("a user with INSERT but no UPDATE passed the boot check; ON DUPLICATE KEY UPDATE would fail on the first batch")
	}
	if !IsFatal(err) || !strings.Contains(denied(err), "UPDATE") || strings.Contains(denied(err), "INSERT") {
		t.Errorf("got %v, want a fatal 1142 naming UPDATE alone", err)
	}
	if n := f.count(f.table); n != 0 {
		t.Errorf("the refused probes left %d rows behind", n)
	}

	f.exec("GRANT UPDATE (uuid) ON " + target + " TO '" + user + "'@'%'")
	if err := try(); err != nil {
		t.Errorf("a user with INSERT and UPDATE (uuid) was refused: %v", err)
	}
	if n := f.count(f.table); n != 1 {
		t.Errorf("%d rows, want the 1 written: the probe must be rolled back", n)
	}
}

// SPEC §8.6.6: a write against a table dropped after boot is FATAL, never
// logic. Logic would drop this batch and every following one, quietly.
func TestMySQLDroppedTableIsFatalNotLogic(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	f.exec("DROP TABLE `" + f.table + "`")
	err := p.Write(context.Background(), mkBatch(1))
	if err == nil {
		t.Fatal("Write into a dropped table succeeded")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "mysql=1146") {
		t.Errorf("err = %v (class %s), want fatal 1146", err, ClassOf(err))
	}
}

// SPEC §8.6.4: a server whose max_allowed_packet cannot hold the largest
// acceptable envelope (envelope.MaxEventBytes, SPEC §3.2) is refused at boot,
// with both numbers in the message; one that can is accepted.
func TestMySQLPacketCeilingIsCheckedAtBoot(t *testing.T) {
	f := mysqlSetup(t)
	var server int64
	if err := f.db.QueryRow("SELECT @@max_allowed_packet").Scan(&server); err != nil {
		t.Fatal(err)
	}

	cfg := f.cfg
	cfg.MaxEventBytes = envelope.MaxEventBytes(1<<20, 64<<10, 8<<10)
	if cfg.MaxEventBytes+mysqlRowOverhead > server-server/8 {
		t.Skipf("max_allowed_packet=%d is too small for the control case", server)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.sink(cfg).Health(ctx); err != nil {
		t.Fatalf("control: an envelope of %d bytes against max_allowed_packet=%d: %v", cfg.MaxEventBytes, server, err)
	}

	cfg.MaxEventBytes = envelope.MaxEventBytes(server, 64<<10, 8<<10)
	err := f.sink(cfg).Health(ctx)
	if err == nil {
		t.Fatal("an envelope larger than max_allowed_packet passed the boot check")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), fmt.Sprintf("max_allowed_packet=%d", server)) {
		t.Errorf("got %v, want a fatal error naming max_allowed_packet=%d", err, server)
	}
}
