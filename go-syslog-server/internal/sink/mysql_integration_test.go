package sink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ring"
)

// The destination is remote and has a schema, and what is worth proving -- the
// multi-row INSERT with ON DUPLICATE KEY, the rows-affected arithmetic behind
// the duplicates counter, the split against the row and packet ceilings, the
// catalogue queries of the boot validation and the privilege probe -- is exactly
// what a fake would have to invent. These run against a real server addressed
// by SYSLOG_TEST_MYSQL_DSN and skip without it:
//
//	docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
//	  --name sls-sink-mysql mysql:8.4
//	SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' go test -race ./internal/sink/
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
		t.Skipf("set %s to run the mysql sink tests", mysqlDSNEnv)
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
// applied afterwards ($T is the table), and registers its removal. Using the
// real file is the point: the validation of SPEC §10.6 and the schema the
// operator gets must agree, and this is where a drift between them surfaces.
func (f *mysqlFixture) newTable(alter string) string {
	f.t.Helper()
	name := fmt.Sprintf("sls_test_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/mysql/schema.sql")
	if err != nil {
		f.t.Fatalf("reading the canonical schema: %v", err)
	}
	f.exec(retargetMySQLSchema(string(b), name))
	if alter != "" {
		f.exec(strings.ReplaceAll(alter, "$T", name))
	}
	f.t.Cleanup(func() {
		_, _ = f.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
	})
	return name
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

func mysqlID(e *Envelope) []byte {
	u := [16]byte(e.Item.Head.UUID)
	return u[:]
}

// SPEC §3.7 and §10.6: every promoted column lands, typed, NULL for what did not
// come, and env is byte-exact.
func TestMySQLWriteStoresQueryableColumns(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchOf(t, line5424, "<13>Oct 11 22:14:15 web\x00\xff-01 su[230]: m")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table); n != 2 {
		t.Fatalf("%d rows, want 2", n)
	}

	e := &b.Envelopes[0]
	var (
		uuid, space, format, transport string
		srcIP, host, app, proc, msgid  sql.NullString
		step, level, sid, uts, sub     int64
		fac, sev, nbytes               int64
		env                            []byte
	)
	q := "SELECT LOWER(HEX(uuid)), step, space, level, sid, ev_uts, ev_sub, facility, severity, format, " +
		"transport, src_ip, hostname, appname, procid, msgid, env, nbytes FROM `" + f.table + "` WHERE uuid = ? AND step = 0"
	if err := f.db.QueryRowContext(ctx, q, mysqlID(e)).Scan(&uuid, &step, &space, &level, &sid, &uts, &sub,
		&fac, &sev, &format, &transport, &srcIP, &host, &app, &proc, &msgid, &env, &nbytes); err != nil {
		t.Fatalf("reading the row back: %v", err)
	}
	h := e.Item.Head
	if uuid != strings.ReplaceAll(h.UUID.String(), "-", "") || step != 0 || space != "default" || level != 5 ||
		sid != int64(e.Res.SID) || uts != h.UTS || sub != h.Sub || fac != 20 || sev != 5 ||
		format != "rfc5424" || transport != "tcp" || srcIP.String != "10.0.0.7" || host.String != "web-01" ||
		app.String != "nginx" || proc.String != "1234" || msgid.String != "ID47" || nbytes != int64(len(e.JLH)) {
		t.Errorf("columns do not match the envelope: %s %d %s %d %d %d %d %d %d %s %s %v %v %v %v %v %d",
			uuid, step, space, level, sid, uts, sub, fac, sev, format, transport, srcIP, host, app, proc, msgid, nbytes)
	}
	if string(env) != string(e.JLH) {
		t.Errorf("env is not the JLH byte for byte")
	}
	if err := jlhcheck.Validate(env); err != nil {
		t.Error(err)
	}

	// The 3164 row: NUL and the invalid byte replaced in the column only, and
	// no msgid at all.
	e = &b.Envelopes[1]
	if err := f.db.QueryRowContext(ctx, "SELECT hostname, msgid, env FROM `"+f.table+"` WHERE uuid = ?",
		mysqlID(e)).Scan(&host, &msgid, &env); err != nil {
		t.Fatal(err)
	}
	if host.String != "web��-01" || msgid.Valid {
		t.Errorf("hostname = %q, msgid = %v; want the NUL and the invalid byte replaced, msgid NULL", host.String, msgid)
	}
	if string(env) != string(e.JLH) {
		t.Error("env was altered; the replacement must touch the column copy only")
	}

	// The text search of SPEC §3.7 works on env.
	var n int
	if err := f.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+f.table+"` WHERE env LIKE '%hello%'").Scan(&n); err != nil || n != 1 {
		t.Errorf("env LIKE found %d rows (%v)", n, err)
	}
}

// SPEC §10.6: the pipeline is at-least-once. Writing the SAME chunk twice is
// the shape of a redelivery: the second write must succeed, add nothing, and be
// counted as duplicates through the rows-affected arithmetic.
func TestMySQLToleratesRedeliveryAndCountsIt(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchN(t, 7)
	ctx := context.Background()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	fresh := batchOf(t, "<13>fresh one", "<13>fresh two")
	mixed := &Batch{Envelopes: append(append([]Envelope{}, b.Envelopes[:3]...), fresh.Envelopes...)}
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	if n := f.count(f.table); n != 9 {
		t.Errorf("%d rows, want 9: redelivery must not duplicate", n)
	}
	if got := p.m.SinkMySQLDuplicatesTotal.Load(); got != 10 {
		t.Errorf("duplicates counter = %d, want 10 (7 from the full redelivery, 3 from the overlap)", got)
	}
}

// SPEC §3.7: an RFC 3164 hostname has no ceiling but the frame, and its
// sanitised copy can be three times its span. The canonical LONGTEXT takes one
// far past the 64 KiB a TEXT would -- where the refusal is 1406, logic, and the
// chunk is dropped.
func TestMySQLLongHostnameFitsTheCanonicalSchema(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	line := "<13>Oct 11 22:14:15 " + strings.Repeat("\xff", 30000) + " app: m"
	b := batchOf(t, line)
	ctx := context.Background()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("a 90 KB hostname was refused by the canonical schema: %v", err)
	}
	var n int
	if err := f.db.QueryRowContext(ctx, "SELECT LENGTH(hostname) FROM `"+f.table+"` WHERE uuid = ?",
		mysqlID(&b.Envelopes[0])).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 90000 {
		t.Errorf("hostname stored with %d bytes, want 90000", n)
	}

	narrow := f.cfg
	narrow.Table = f.newTable("ALTER TABLE `$T` DROP INDEX sev_host, MODIFY hostname TEXT")
	q := f.sink(narrow)
	f.ready(q)
	err := q.Write(ctx, batchOf(t, line))
	if err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a TEXT hostname took 90 KB or refused it as %v; the trade-off §3.7 documents is a logic drop", err)
	}
}

// SPEC §10.6: a chunk beyond the row ceiling, and one beyond the byte ceiling,
// are split into several statements and every row still lands.
func TestMySQLSplitsLargeChunks(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	many := batchN(t, mysqlMaxRows+137)
	if err := p.Write(ctx, many); err != nil {
		t.Fatalf("Write of %d rows: %v", len(many.Envelopes), err)
	}
	if n := f.count(f.table); n != len(many.Envelopes) {
		t.Fatalf("%d rows, want %d", n, len(many.Envelopes))
	}

	// Pretend the server's packet limit is 64 KiB: ten 20 KiB lines need
	// several statements, and the real server accepts each.
	p.maxPacket.Store(64 << 10)
	ls := make([]string, 10)
	for i := range ls {
		ls[i] = fmt.Sprintf("<13>%02d", i) + strings.Repeat("x", 20<<10)
	}
	big := batchOf(t, ls...)
	if err := p.Write(ctx, big); err != nil {
		t.Fatalf("Write of large lines: %v", err)
	}
	if n := f.count(f.table); n != len(many.Envelopes)+len(big.Envelopes) {
		t.Fatalf("%d rows, want %d", n, len(many.Envelopes)+len(big.Envelopes))
	}
	var env []byte
	if err := f.db.QueryRowContext(ctx, "SELECT env FROM `"+f.table+"` WHERE uuid = ?",
		mysqlID(&big.Envelopes[7])).Scan(&env); err != nil {
		t.Fatal(err)
	}
	if string(env) != string(big.Envelopes[7].JLH) {
		t.Errorf("a split envelope arrived different (%d bytes, want %d)", len(env), len(big.Envelopes[7].JLH))
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
	batches := make([]*Batch, writers)
	for w := range batches {
		batches[w] = batchN(t, per)
	}
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
	if got := p.m.SinkMySQLPoolConns[2].Load(); got != 5 {
		t.Errorf(`pool_conns{state="max"} = %d, want 5`, got)
	}
}

// SPEC §10.6: each refusal is FATAL, so the boot stops at once instead of
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
		{"varchar ev_uts", f.newTable("ALTER TABLE `$T` MODIFY ev_uts VARCHAR(20) NOT NULL"), `"ev_uts"`},
		{"missing column", f.newTable("ALTER TABLE `$T` DROP COLUMN nbytes"), `"nbytes"`},
		{"text env", f.newTable("ALTER TABLE `$T` MODIFY env LONGTEXT NOT NULL"), `"env"`},
		{"no unique index", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY"), "unique index"},
		{"unique on uuid only", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX u (uuid)"), "unique index"},
		{"unique with a third column", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX u (uuid, step, level)"), "unique index"},
		// MyISAM caps an index key at 1000 bytes, below the 1020 of the utf8mb4
		// hostname prefix, so the prefix index goes first.
		{"myisam", f.newTable("ALTER TABLE `$T` DROP INDEX sev_host, ENGINE=MyISAM"), "InnoDB"},
		{"wrong uuid width", f.newTable("ALTER TABLE `$T` MODIFY uuid BINARY(20) NOT NULL"), "binary(16)"},
		// Review finding 3: whatever the server answers the boot probe is
		// fatal, not just 1142 -- the drain's rows would be refused the same
		// way, as logic, with the sink UP.
		{"NOT NULL hostname refuses the probe (1048)", f.newTable("ALTER TABLE `$T` MODIFY hostname LONGTEXT NOT NULL"), "hostname"},
		{"extra required column refuses the probe (1364)", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NOT NULL"), "tenant"},
		{"too narrow a column refuses the probe (1406)", f.newTable("ALTER TABLE `$T` MODIFY space VARCHAR(2) NOT NULL"), "space"},
		{"a CHECK refuses the probe (3819, unknown to the runtime table)", f.newTable("ALTER TABLE `$T` ADD CONSTRAINT `$T_chk` CHECK (level > 0)"), "probe"},
		{"extra column is tolerated", f.newTable("ALTER TABLE `$T` ADD COLUMN tenant VARCHAR(32) NULL"), ""},
		{"varchar hostname is tolerated", f.newTable("ALTER TABLE `$T` DROP INDEX sev_host, MODIFY hostname VARCHAR(255)"), ""},
		{"mediumblob env is tolerated", f.newTable("ALTER TABLE `$T` MODIFY env MEDIUMBLOB NOT NULL"), ""},
		{"int facility is tolerated", f.newTable("ALTER TABLE `$T` MODIFY facility INT NOT NULL"), ""},
		{"unique index instead of pk", f.newTable("ALTER TABLE `$T` DROP PRIMARY KEY, ADD UNIQUE INDEX u (step, uuid)"), ""},
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
				if err := p.Write(ctx, batchN(t, 2)); err != nil {
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

// SPEC §10.6: the probe is what proves the privileges, and ON DUPLICATE KEY
// UPDATE needs UPDATE as well as INSERT. A user with only INSERT is refused at
// boot rather than on the first chunk; INSERT plus UPDATE on the uuid column is
// enough. Needs CREATE USER; skipped where the test user lacks it.
func TestMySQLHealthValidatesThePrivilege(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()

	user := fmt.Sprintf("sls_role_%d", time.Now().UnixNano()%1e9)
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
		t.Fatal("a user with INSERT but no UPDATE passed the boot check; ON DUPLICATE KEY UPDATE would fail on the first chunk")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "UPDATE") {
		t.Errorf("got %v, want a fatal error naming UPDATE", err)
	}

	f.exec("GRANT UPDATE (uuid) ON " + target + " TO '" + user + "'@'%'")
	if err := try(); err != nil {
		t.Errorf("a user with INSERT and UPDATE (uuid) was refused: %v", err)
	}
	if n := f.count(f.table); n != 0 {
		t.Errorf("the probe left %d rows behind; it must be rolled back", n)
	}
}

// SPEC §10.6: a write against a table dropped after boot is FATAL, never logic.
// Logic would drop this chunk and every following one, quietly.
func TestMySQLDroppedTableIsFatalNotLogic(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	f.exec("DROP TABLE `" + f.table + "`")
	err := p.Write(context.Background(), batchN(t, 1))
	if err == nil {
		t.Fatal("Write into a dropped table succeeded")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s, want fatal", ClassOf(err))
	}
}

// SPEC §3.9 and §10.6: a server whose max_allowed_packet cannot hold the largest
// row this server could produce is refused at boot, with both numbers in the
// message.
func TestMySQLPacketCeilingIsCheckedAtBoot(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	cfg.MaxMessageBytes = 1 << 30
	cfg.MaxEventBytes = MaxEventBytes(cfg.MaxMessageBytes)
	p := f.sink(cfg)
	err := p.Health(context.Background())
	if err == nil {
		t.Fatal("a row larger than max_allowed_packet passed the boot check")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "max_allowed_packet") {
		t.Errorf("got %v, want a fatal error naming max_allowed_packet", err)
	}

	// The default L = 64 KiB fits the 64 MiB default of MySQL 8 by far.
	cfg.MaxMessageBytes = 64 << 10
	cfg.MaxEventBytes = MaxEventBytes(cfg.MaxMessageBytes)
	f.ready(f.sink(cfg))
}

// The whole drain path against the real server, with the columns switched on by
// the sink itself.
func TestMySQLWorkersEndToEnd(t *testing.T) {
	f := mysqlSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	rg := ring.NewMemory(1000, 0, time.Millisecond)
	m := metrics()
	cfg := workerCfg(64)
	cfg.Convert = convert.DefaultConfig()
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{}, discard(), m), cfg, discard(), m)
	enqueueAll(t, rg, itemsOf(lines(300)...))
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 30*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 300 })
	cancel()
	w.Wait()

	var n, hosts int
	if err := f.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*), COUNT(hostname) FROM `"+f.table+"`").Scan(&n, &hosts); err != nil {
		t.Fatal(err)
	}
	if n != 300 || hosts != 300 {
		t.Fatalf("%d rows, %d with a hostname; want 300 and 300", n, hosts)
	}
}

// Review finding 3: @@read_only = 1 at the FIRST validation is a replica or a
// server configured so -- fatal, exit 3, like PostgreSQL in recovery (SPEC
// §10.5, inherited by §10.6). After boot the same answer is a failover in
// progress: DOWN (network), and the fuses decide.
func TestMySQLReadOnlyIsFatalAtBootAndDownAfter(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()
	validated := f.sink(f.cfg)
	f.ready(validated)

	if _, err := f.db.ExecContext(ctx, "SET GLOBAL read_only = ON"); err != nil {
		t.Skipf("cannot set read_only on this server: %v", err)
	}
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "SET GLOBAL read_only = OFF") })

	fresh := f.sink(f.cfg)
	err := fresh.Health(ctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "read-only") {
		t.Errorf("first validation on a read-only server = %v (class %s), want fatal", err, ClassOf(err))
	}
	err = validated.Health(ctx)
	if err == nil || IsFatal(err) || ClassOf(err) != ClassNetwork {
		t.Errorf("a validated sink on a read-only server = %v (class %s), want network (DOWN)", err, ClassOf(err))
	}
}

// Review finding 8: statements run in order and autocommit one by one, so when
// statement k is refused for good the ones before it are stored. The refusal
// carries their envelopes in Error.Delivered, and only the rest is lost.
func TestMySQLLogicRefusalReportsTheStoredPrefix(t *testing.T) {
	f := mysqlSetup(t)
	cfg := f.cfg
	// A TEXT hostname refuses a 90 KB one with 1406, logic (SPEC §3.7).
	cfg.Table = f.newTable("ALTER TABLE `$T` DROP INDEX sev_host, MODIFY hostname TEXT")
	p := f.sink(cfg)
	f.ready(p)
	// One small statement for rows 0 and 1, row 2 alone, row 3 alone.
	p.maxPacket.Store(64 << 10)

	b := batchOf(t, "<13>Oct 11 22:14:15 h0 app: zero", "<13>Oct 11 22:14:15 h1 app: one",
		"<13>Oct 11 22:14:15 "+strings.Repeat("\xff", 30000)+" app: two", "<13>Oct 11 22:14:15 h3 app: three")
	err := p.Write(context.Background(), b)
	if ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	var e *Error
	if !errors.As(err, &e) || fmt.Sprint(e.Delivered) != "[0 1]" {
		t.Fatalf("Delivered = %v, want [0 1]: the statement before the refused one is stored", e.Delivered)
	}
	if n := f.count(cfg.Table); n != 2 {
		t.Fatalf("%d rows stored, want 2", n)
	}
	stored, lost := settleLogic(err, b.Envelopes)
	if stored != 2 || len(lost) != 2 {
		t.Fatalf("settled as %d stored, %d lost; want 2 and 2", stored, len(lost))
	}
}
