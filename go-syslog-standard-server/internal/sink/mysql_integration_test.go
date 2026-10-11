package sink

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The destination is remote and has a schema, and what is worth proving -- the
// multi-row INSERT with ON DUPLICATE KEY, the rows-affected arithmetic behind
// the duplicates counter, the chunking against the server's packet limit, the
// catalogue queries of the boot validation and the privilege probe -- is
// exactly what a fake would have to invent. These run against a real server
// addressed by SYSLOG_TEST_MYSQL_DSN and skip without it (docs/TESTING.md §6.3):
//
//	SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' \
//	  go test -race ./internal/sink/
//
// Every test creates its own table from docker/mysql/schema.sql and drops it
// afterwards, so the suite never depends on an empty database.

const mysqlDSNEnv = "SYSLOG_TEST_MYSQL_DSN"

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

	f := &mysqlFixture{t: t, db: db, dbName: mc.DBName, cfg: MySQLConfig{
		Addr:     mc.Addr,
		Username: mc.User,
		Password: mc.Passwd,
		Database: mc.DBName,
		PoolSize: 4,
		AppName:  "sls-test",
	}}
	f.table = f.newTable("")
	f.cfg.Table = f.table
	return f
}

// newTable creates a table from the canonical schema, with the given SQL
// applied afterwards, and registers its removal.
func (f *mysqlFixture) newTable(alter string) string {
	f.t.Helper()
	name := fmt.Sprintf("sls_test_%d", time.Now().UnixNano())
	f.exec(mysqlSchemaFor(f.t, name))
	if alter != "" {
		f.exec(strings.ReplaceAll(alter, "$T", name))
	}
	f.t.Cleanup(func() {
		_, _ = f.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
	})
	return name
}

// mysqlSchemaFor reads docker/mysql/schema.sql -- the file operators apply --
// and retargets it at a test table. Using the real file is the point: the
// validation of SPEC §10.7.4 and the schema the operator gets must agree, and
// this is where a drift between them would surface.
func mysqlSchemaFor(t *testing.T, table string) string {
	t.Helper()
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		t.Fatalf("reading the canonical schema: %v", err)
	}
	return retargetSchema(string(b), table)
}

// retargetSchema keeps the one CREATE TABLE statement of the schema file and
// renames its table.
func retargetSchema(schema, table string) string {
	var lines []string
	for _, line := range strings.Split(schema, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = s[:strings.LastIndexByte(s, ';')] // one statement, without the terminator
	return strings.Replace(s, "CREATE TABLE IF NOT EXISTS events", "CREATE TABLE `"+table+"`", 1)
}

func (f *mysqlFixture) exec(stmt string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := f.db.ExecContext(ctx, stmt); err != nil {
		f.t.Fatalf("%v\n%s", err, stmt)
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
	p, err := NewMySQL(cfg, discard(), metrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { p.Close() })
	return p
}

// ready runs the boot check the way WaitReady would, and fails the test if the
// table is refused.
func (f *mysqlFixture) ready(p *MySQL) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// mysqlID is the sixteen-byte form the id column holds.
func mysqlID(t *testing.T, id string) []byte {
	t.Helper()
	u, ok := parseUUID(id)
	if !ok {
		t.Fatalf("not a uuid: %q", id)
	}
	return u[:]
}

// ------------------------------------------------------------------ writes

// SPEC §3.7 and §10.7.3: every promoted column lands, typed, and the blobs are
// byte-exact.
func TestMySQLWriteStoresQueryableColumns(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := batchOf(t, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table); n != 10 {
		t.Fatalf("%d rows, want 10", n)
	}

	it := batch[3]
	var (
		id, host, app, proc, msgid, peer, transport, rfc string
		ts, nts, facility, severity, off, size, nbytes   int64
		env, raw                                         []byte
	)
	err := f.db.QueryRowContext(ctx, "SELECT LOWER(HEX(id)), ts, nts, facility, severity, hostname, "+
		"app_name, procid, msgid, peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes FROM `"+
		f.table+"` WHERE id = ?", mysqlID(t, it.Item.Head.ID)).
		Scan(&id, &ts, &nts, &facility, &severity, &host, &app, &proc, &msgid, &peer, &transport, &rfc,
			&off, &size, &env, &raw, &nbytes)
	if err != nil {
		t.Fatalf("reading the row back: %v", err)
	}
	m := it.Meta
	if id != hex.EncodeToString(mysqlID(t, it.Item.Head.ID)) || ts != m.TS || nts != m.NTS ||
		int(facility) != m.Facility || int(severity) != m.Severity || host != m.Hostname ||
		app != m.AppName || proc != m.ProcID || msgid != m.MsgID || peer != m.PeerIP ||
		transport != m.Transport || rfc != m.RFC || int(off) != m.MsgOffset || int(size) != m.MsgSize ||
		nbytes != int64(len(it.Env)+len(it.Raw())) {
		t.Errorf("columns do not match the message:\n got id=%s ts=%d nts=%d fac=%d sev=%d %q %q %q %q %q %q %q off=%d size=%d nbytes=%d\nwant %+v",
			id, ts, nts, facility, severity, host, app, proc, msgid, peer, transport, rfc, off, size, nbytes, m)
	}
	if string(env) != string(it.Env) {
		t.Errorf("env = %q, want %q", env, it.Env)
	}
	if string(raw) != string(it.Raw()) {
		t.Errorf("raw = %q, want %q", raw, it.Raw())
	}
}

// LONGBLOB must carry NUL, newline, CR and a byte that is not UTF-8, unchanged;
// an empty line is an empty blob, never NULL.
func TestMySQLPreservesBinaryAndEmptyRaw(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	line := "<13>1 - h a - - - A\x00B\nC\xffD\r\x00"
	batch := prepared(t, line, "")
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var raw []byte
	if err := f.db.QueryRowContext(ctx, "SELECT raw FROM `"+f.table+"` WHERE id = ?",
		mysqlID(t, batch[0].Item.Head.ID)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if string(raw) != line {
		t.Errorf("raw = %q, want %q byte for byte", raw, line)
	}
	var isNull bool
	var n int
	if err := f.db.QueryRowContext(ctx, "SELECT raw IS NULL, LENGTH(raw) FROM `"+f.table+"` WHERE id = ?",
		mysqlID(t, batch[1].Item.Head.ID)).Scan(&isNull, &n); err != nil {
		t.Fatal(err)
	}
	if isNull || n != 0 {
		t.Errorf("empty raw is_null=%v length=%d, want an empty, non-null blob", isNull, n)
	}
}

// SPEC §10.7.3: the pipeline is at-least-once. Writing the SAME batch twice is
// the shape of a redelivery: the second write must succeed, add nothing, and be
// counted as duplicates through the rows-affected arithmetic.
func TestMySQLToleratesRedeliveryAndCountsIt(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := batchOf(t, 7)
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	fresh := prepared(t, "<13>fresh one", "<13>fresh two")
	for i := range fresh {
		fresh[i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-%012d", 900+i)
	}
	if err := p.Write(ctx, append(batch[:3:3], fresh...)); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	if n := f.count(f.table); n != 9 {
		t.Errorf("%d rows, want 9: redelivery must not duplicate", n)
	}
	if got := p.m.SinkMySQLDuplicatesTotal.Load(); got != 10 {
		t.Errorf("duplicates counter = %d, want 10 (7 from the full redelivery, 3 from the overlap)", got)
	}
}

// SPEC §3.7: one hostname that is not UTF-8 must not take the batch down under
// the strict sql_mode the session forces; the column holds the sanitised copy
// and env and raw keep the original bytes.
func TestMySQLInvalidUTF8HostnameDoesNotDropTheBatch(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	bad := "<13>Oct 11 22:14:15 web\xff\xfe\x00-01 app[1]: message"
	batch := prepared(t, "<13>Oct 11 22:14:15 ok-01 app: one", bad, "<13>Oct 11 22:14:15 ok-02 app: three")
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("Write with an invalid hostname: %v", err)
	}
	if n := f.count(f.table); n != 3 {
		t.Errorf("%d rows, want 3", n)
	}
	var host string
	var env, raw []byte
	if err := f.db.QueryRowContext(ctx, "SELECT hostname, env, raw FROM `"+f.table+"` WHERE id = ?",
		mysqlID(t, batch[1].Item.Head.ID)).Scan(&host, &env, &raw); err != nil {
		t.Fatal(err)
	}
	if host != "web���-01" {
		t.Errorf("hostname column = %q, want the invalid bytes and the NUL replaced", host)
	}
	if string(env) != string(batch[1].Env) || string(raw) != bad {
		t.Error("env or raw was altered; the replacement must touch the column copy only")
	}
}

// SPEC §3.7: an RFC 3164 hostname has no ceiling but the frame, and the
// canonical LONGTEXT takes one far past the 64 KiB a TEXT would -- where the
// refusal is 1406, logic, and the batch is dropped.
func TestMySQLLongHostnameFitsTheCanonicalSchema(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	host := strings.Repeat("h", 100<<10)
	batch := prepared(t, "<13>Oct 11 22:14:15 "+host+" app: m")
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("a 100 KiB hostname was refused by the canonical schema: %v", err)
	}
	var n int
	if err := f.db.QueryRowContext(ctx, "SELECT LENGTH(hostname) FROM `"+f.table+"` WHERE id = ?",
		mysqlID(t, batch[0].Item.Head.ID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(host) {
		t.Errorf("hostname stored with %d bytes, want %d", n, len(host))
	}

	narrow := f.cfg
	narrow.Table = f.newTable("ALTER TABLE `$T` MODIFY hostname TEXT NOT NULL")
	q := f.sink(narrow)
	f.ready(q)
	err := q.Write(ctx, prepared(t, "<13>Oct 11 22:14:15 "+host+" app: m"))
	if err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a TEXT hostname took 100 KiB or refused it as %v; the trade-off §3.7 documents is a logic drop", err)
	}
}

// SPEC §3.7: text search works on the blob without a cast, and SUBSTRING with
// the promoted offsets returns exactly the MSG.
func TestMySQLSearchesTheRawLine(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := prepared(t,
		"<165>1 2026-09-12T14:33:01Z web-01 nginx 42 ID7 - disk failure on sda",
		"<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed",
	)
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var msg string
	if err := f.db.QueryRowContext(ctx, "SELECT CONVERT(SUBSTRING(raw, msg_offset + 1, msg_size) USING utf8mb4) FROM `"+
		f.table+"` WHERE raw LIKE '%disk failure%'").Scan(&msg); err != nil {
		t.Fatalf("LIKE on raw found nothing: %v", err)
	}
	if msg != "disk failure on sda" {
		t.Errorf("SUBSTRING returned %q", msg)
	}
}

// SPEC §10.7.3: a batch beyond the row ceiling, and a batch beyond the byte
// ceiling, are split into several statements and every row still lands.
func TestMySQLSplitsLargeBatches(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	many := batchOf(t, mysqlMaxRows+137)
	for i := range many {
		many[i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-%012d", i)
	}
	if err := p.Write(ctx, many); err != nil {
		t.Fatalf("Write of %d rows: %v", len(many), err)
	}
	if n := f.count(f.table); n != len(many) {
		t.Fatalf("%d rows, want %d", n, len(many))
	}

	// Pretend the server's packet limit is 64 KiB: ten 20 KiB lines need
	// several statements, and the real server accepts each.
	p.maxPacket.Store(64 << 10)
	lines := make([]string, 10)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>%02d", i) + strings.Repeat("x", 20<<10)
	}
	big := prepared(t, lines...)
	for i := range big {
		big[i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-9999-%012d", i)
	}
	if err := p.Write(ctx, big); err != nil {
		t.Fatalf("Write of large lines: %v", err)
	}
	if n := f.count(f.table); n != len(many)+len(big) {
		t.Fatalf("%d rows, want %d", n, len(many)+len(big))
	}
	var length int
	if err := f.db.QueryRowContext(ctx, "SELECT LENGTH(raw) FROM `"+f.table+"` WHERE id = ?",
		mysqlID(t, big[7].Item.Head.ID)).Scan(&length); err != nil {
		t.Fatal(err)
	}
	if length != len(lines[7]) {
		t.Errorf("a chunked line arrived with %d bytes, want %d", length, len(lines[7]))
	}
}

// Many workers, one table: the pool must serve them all, and the pool gauges
// must reflect the configured ceiling after a health check.
func TestMySQLConcurrentWritersAndPoolGauges(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	cfg.PoolSize = 5
	p := f.sink(cfg)
	f.ready(p)

	const writers, per = 8, 20
	batches := make([][]Prepared, writers)
	for w := range batches {
		batches[w] = batchOf(t, per)
		for i := range batches[w] {
			batches[w][i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-%04d-%012d", w, i)
		}
	}
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(b []Prepared) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}(batches[w])
	}
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Errorf("writer: %v", err)
		}
	}
	if n := f.count(f.table); n != writers*per {
		t.Errorf("%d rows, want %d", n, writers*per)
	}

	f.ready(p) // refreshes the gauges
	if got := p.m.SinkMySQLPoolConns[2].Load(); got != 5 {
		t.Errorf(`pool_conns{state="max"} = %d, want 5`, got)
	}
	if got := p.m.SinkMySQLPoolWaitsTotal.Load(); got == 0 {
		t.Error("eight writers over five connections never waited, or the counter is not fed")
	}
}

// ------------------------------------------------------------- validation

// SPEC §10.7.4: each refusal is FATAL, so the boot stops at once instead of
// waiting out sink.boot_timeout on a table that will not appear.
func TestMySQLHealthValidatesTheSchema(t *testing.T) {
	f := mysqlSetup(t)

	cases := []struct {
		name  string
		table string
		want  string // substring of the fatal error; "" means accepted
	}{
		{"canonical schema", f.table, ""},
		{"missing table", fmt.Sprintf("sls_missing_%d", time.Now().UnixNano()), "does not exist"},
		{"wrong type", f.newTable("ALTER TABLE `$T` MODIFY ts INT NOT NULL"), `"ts"`},
		{"missing column", f.newTable("ALTER TABLE `$T` DROP COLUMN nbytes"), `"nbytes"`},
		{"text raw", f.newTable("ALTER TABLE `$T` MODIFY raw LONGTEXT NOT NULL"), `"raw"`},
		{"no unique index", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY"), "unique index"},
		// MyISAM caps an index key at 1000 bytes, below the 1020 of the utf8mb4
		// hostname prefix, so the prefix index goes first.
		{"myisam", f.newTable("ALTER TABLE `$T` DROP INDEX ev_host, ENGINE=MyISAM"), "InnoDB"},
		{"wrong id width", f.newTable("ALTER TABLE `$T` MODIFY id BINARY(20) NOT NULL"), "binary(16)"},
		{"extra column is tolerated", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NULL"), ""},
		{"varchar hostname is tolerated", f.newTable("ALTER TABLE `$T` DROP INDEX ev_host, MODIFY hostname VARCHAR(255) NOT NULL"), ""},
		{"mediumblob raw is tolerated", f.newTable("ALTER TABLE `$T` MODIFY raw MEDIUMBLOB NOT NULL"), ""},
		{"int facility is tolerated", f.newTable("ALTER TABLE `$T` MODIFY facility INT NOT NULL"), ""},
		{"unique index instead of pk", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX uid (id)"), ""},
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
				if err := p.Write(ctx, batchOf(t, 2)); err != nil {
					t.Errorf("Write after validation: %v", err)
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

// SPEC §10.7.4: the probe is what proves the privileges, and ON DUPLICATE KEY
// UPDATE needs UPDATE as well as INSERT. A user with only INSERT is refused at
// boot rather than on the first batch; INSERT plus UPDATE on the id column is
// enough. Needs CREATE USER; skipped where the test user lacks it.
func TestMySQLHealthValidatesThePrivilege(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()

	user := fmt.Sprintf("sls_role_%d", time.Now().UnixNano())
	if _, err := f.db.ExecContext(ctx, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'sls-test-pw'"); err != nil {
		t.Skipf("cannot create users on this server: %v", err)
	}
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	target := "`" + f.dbName + "`.`" + f.table + "`"

	try := func() error {
		cfg := f.cfg
		cfg.Username, cfg.Password = user, "sls-test-pw"
		p := f.sink(cfg)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return p.Health(cctx)
	}

	f.exec("GRANT SELECT ON " + target + " TO '" + user + "'@'%'")
	err := try()
	if err == nil {
		t.Fatal("a user without INSERT passed the boot check")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "INSERT") {
		t.Errorf("got %v, want a fatal error naming INSERT", err)
	}

	f.exec("GRANT INSERT ON " + target + " TO '" + user + "'@'%'")
	err = try()
	if err == nil {
		t.Fatal("a user with INSERT but no UPDATE passed the boot check; ON DUPLICATE KEY UPDATE would fail on the first batch")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "UPDATE") {
		t.Errorf("got %v, want a fatal error naming UPDATE", err)
	}

	f.exec("GRANT UPDATE (id) ON " + target + " TO '" + user + "'@'%'")
	if err := try(); err != nil {
		t.Errorf("a user with INSERT and UPDATE (id) was refused: %v", err)
	}
	if n := f.count(f.table); n != 0 {
		t.Errorf("the probe left %d rows behind; it must be rolled back", n)
	}
}

// SPEC §10.7.6: a write against a table dropped after boot is FATAL, never
// logic. Logic would drop this batch and every following one, quietly.
func TestMySQLDroppedTableIsFatalNotLogic(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	f.exec("DROP TABLE `" + f.table + "`")
	err := p.Write(context.Background(), batchOf(t, 1))
	if err == nil {
		t.Fatal("Write into a dropped table succeeded")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s, want fatal", ClassOf(err))
	}
}

// SPEC §3.9 and §10.7.4: a server whose max_allowed_packet cannot hold the
// largest row this server could accept is refused at boot, with both numbers
// in the message.
func TestMySQLPacketCeilingIsCheckedAtBoot(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	cfg.MaxRawBytes, cfg.MaxEnvelopeBytes = 1<<30, 6<<30
	p := f.sink(cfg)
	err := p.Health(context.Background())
	if err == nil {
		t.Fatal("a row larger than max_allowed_packet passed the boot check")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "max_allowed_packet") {
		t.Errorf("got %v, want a fatal error naming max_allowed_packet", err)
	}
}
