package sink

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// The SQL items of SPEC §22.1 against the real servers (RELPLOG_TEST_PG_DSN,
// RELPLOG_TEST_MYSQL_DSN, RELPLOG_TEST_CLICKHOUSE_DSN); each test skips
// without its server.

// ---------------------------------------------------------------- item 6

// slowProxy forwards TCP to a real server, holding each chunk for delay
// before passing it on: the dozen round trips of a validation then take
// several times what the health check's own query does -- the ~55 ms of RTT
// that made the boot fail in the first place.
type slowProxy struct {
	ln       net.Listener
	upstream string
	delay    atomic.Int64

	mu    sync.Mutex
	conns []net.Conn
}

func newSlowProxy(t *testing.T, upstream string) *slowProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &slowProxy{ln: ln, upstream: upstream}
	go p.serve()
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			c.Close()
		}
	})
	return p
}

func (p *slowProxy) Addr() string { return p.ln.Addr().String() }

func (p *slowProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		u, err := net.Dial("tcp", p.upstream)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		go p.pipe(c, u)
		go p.pipe(u, c)
	}
}

func (p *slowProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(time.Duration(p.delay.Load()))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// checkValidationDeadline drives one sink through both halves of item 6.
// fresh builds a sink behind the proxy with a validation deadline far above
// anything here; warm opens its connection with a trivial round trip, so what
// the delay slows down is the validation, not the dial.
func checkValidationDeadline(t *testing.T, proxy *slowProxy,
	fresh func() (health func(context.Context) error, warm func(context.Context) error)) {
	t.Helper()
	const delay = 30 * time.Millisecond

	// The check's deadline: a few round trips through the proxy, enough for
	// the check's own query, far short of the validation's dozen.
	health, warm := fresh()
	proxy.delay.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := warm(ctx); err != nil {
		cancel()
		t.Fatalf("warm: %v", err)
	}
	cancel()
	proxy.delay.Store(int64(delay))
	start := time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	if err := warm(ctx); err != nil {
		cancel()
		t.Fatalf("warm through the delay: %v", err)
	}
	cancel()
	rtt := time.Since(start)
	checkDeadline := 4*rtt + 100*time.Millisecond

	t.Run("the validation outlives the check deadline", func(t *testing.T) {
		check, cancel := context.WithTimeout(context.Background(), checkDeadline)
		defer cancel()
		start := time.Now()
		err := health(check)
		took := time.Since(start)
		if err != nil {
			t.Fatalf("Health under a %s check deadline: %v (after %s) -- the validation ran on the "+
				"check's deadline, not sink.op_timeout", checkDeadline, err, took)
		}
		if took <= checkDeadline {
			t.Fatalf("the validation took %s, within the %s check deadline: the delay proves nothing", took, checkDeadline)
		}
	})

	t.Run("a boot cancelled during the validation reaches it", func(t *testing.T) {
		health, warm := fresh()
		proxy.delay.Store(0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := warm(ctx); err != nil {
			t.Fatalf("warm: %v", err)
		}
		proxy.delay.Store(int64(delay))

		// The boot is cancelled three round trips in: past the check's own
		// query, inside the validation (a dozen round trips). The check's
		// window is still open, which is what the check context alone can
		// forward (validationContext without a lifetime, as Health.WaitReady
		// calls it today); the validation must stop there instead of running
		// on for its 30 s.
		boot, cancelBoot := context.WithCancel(context.Background())
		defer cancelBoot()
		check, cancelCheck := context.WithTimeout(boot, time.Hour)
		defer cancelCheck()
		time.AfterFunc(3*rtt, cancelBoot)
		start := time.Now()
		if err := health(check); err == nil {
			t.Fatal("Health validated the table although the boot was cancelled during the validation")
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("the cancelled validation ran for %s", took)
		}
	})
}

func TestPostgresValidationHasItsOwnDeadline(t *testing.T) {
	f := pgSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addr)
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addr = proxy.Addr()
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		cfg.ValidateTimeout = 30 * time.Second
		p := f.sink(cfg)
		return p.Health, p.pool.Ping
	})
}

func TestMySQLValidationHasItsOwnDeadline(t *testing.T) {
	f := mysqlSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addr)
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addr = proxy.Addr()
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		p := f.sink(cfg)
		return p.Health, p.db.PingContext
	})
}

func TestClickHouseValidationHasItsOwnDeadline(t *testing.T) {
	f := chSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addrs[0])
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addrs = []string{proxy.Addr()}
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		c := f.sink(cfg)
		return c.Health, c.conn.Ping
	})
}

// ------------------------------------------------------------ items 7, 8

// A 23502 after the boot is the table -- here a required column added after
// the validation -- and fatal, not a logic drop of every chunk.
func TestPostgresNotNullViolationIsFatal(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("ALTER TABLE " + f.table + " ADD COLUMN tenant text NOT NULL")
	err := p.Write(context.Background(), batchN(t, 2))
	if err == nil {
		t.Fatal("Write succeeded into a table with a required column it never fills")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "23502") {
		t.Fatalf("Write = %v (class %s), want fatal with sqlstate 23502", err, ClassOf(err))
	}
}

// sink.postgres.database that does not exist is 3D000 at the login: fatal at
// the first check, not a network failure retried until boot_timeout.
func TestPostgresUnknownDatabaseIsFatal(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.Database = "rls_no_such_database"
	p := f.sink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := p.Health(ctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "3D000") {
		t.Fatalf("Health on a database that does not exist = %v (class %s), want fatal 3D000", err, ClassOf(err))
	}
}

// --------------------------------------------------------------- item 10

// autocommitOffUser creates a MySQL user of this test whose sessions start
// with autocommit=0, the way a my.cnf or an init_connect would leave them,
// and returns a config that logs in as it. The init_connect is conditional on
// that user: every other session -- the suite runs in parallel against one
// server, shared with other suites -- keeps its own autocommit. The tests
// once ran SET GLOBAL autocommit = 0, which every concurrent test inherited
// until the cleanup (go-syslog-server review, 2026-10-13). init_connect does
// not run for a user with CONNECTION_ADMIN, which this one lacks.
func autocommitOffUser(t *testing.T, f *mysqlFixture) MySQLConfig {
	t.Helper()
	ctx := context.Background()
	user := fmt.Sprintf("ac0_%d", time.Now().UnixNano()%1e12)
	f.exec("CREATE USER '" + user + "'@'%' IDENTIFIED BY 'ac0'")
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'") })
	f.exec("GRANT SELECT, INSERT, UPDATE ON `" + f.dbName + "`.* TO '" + user + "'@'%'")
	var prev string
	if err := f.db.QueryRowContext(ctx, "SELECT @@GLOBAL.init_connect").Scan(&prev); err != nil {
		t.Fatal(err)
	}
	if prev != "" && !strings.Contains(prev, "ac0_") {
		t.Skipf("the server already has an init_connect of its own: %q", prev)
	}
	// One user per test, so the condition names a prefix: two of these tests
	// in parallel each find their own user in it.
	const ic = `SET autocommit = IF(SUBSTRING_INDEX(USER(), '@', 1) LIKE 'ac0\\_%', 0, @@autocommit)`
	if _, err := f.db.ExecContext(ctx, "SET GLOBAL init_connect = ?", ic); err != nil {
		t.Skipf("cannot set init_connect here: %v", err)
	}
	t.Cleanup(func() { _, _ = f.db.ExecContext(context.Background(), "SET GLOBAL init_connect = ''") })
	// The control: a plain session of that user really starts with
	// autocommit=0, or the tests below would prove nothing.
	mc := mysql.NewConfig()
	mc.User, mc.Passwd, mc.Net, mc.Addr, mc.DBName = user, "ac0", "tcp", f.cfg.Addr, f.cfg.Database
	db, err := sql.Open("mysql", mc.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ac int
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.autocommit").Scan(&ac); err != nil {
		t.Fatal(err)
	}
	if ac != 0 {
		t.Fatalf("control: a session of %s has autocommit=%d, want 0 from init_connect", user, ac)
	}
	cfg := f.cfg
	cfg.Username, cfg.Password = user, "ac0"
	return cfg
}

// With autocommit=0 as the server's default, every INSERT used to sit in an
// implicit transaction nobody committed: Write returned nil and the rows
// vanished with the connection. The session parameter pins it to 1.
func TestMySQLWritesCommitUnderAGlobalAutocommitOff(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()
	cfg := autocommitOffUser(t, f)
	p, err := NewMySQL(cfg, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	f.ready(p)
	if err := p.Write(ctx, batchN(t, 3)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	p.Close() // an uncommitted transaction dies here
	if n := f.count(f.table); n != 3 {
		t.Fatalf("%d rows after Write returned nil for 3: the INSERTs were never committed", n)
	}
}

// The validation reads autocommit back: a session where the parameter did not
// take (here, a pool built without it, for a user whose sessions start with
// autocommit=0) is refused.
func TestMySQLValidationRefusesASessionWithoutAutocommit(t *testing.T) {
	f := mysqlSetup(t)
	ctx := context.Background()
	cfg := autocommitOffUser(t, f)

	p := f.sink(cfg)
	mc := mysql.NewConfig()
	mc.User, mc.Passwd, mc.Net, mc.Addr, mc.DBName = cfg.Username, cfg.Password, "tcp", cfg.Addr, cfg.Database
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		t.Fatal(err)
	}
	p.db.Close()
	p.db = sql.OpenDB(connector)

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = p.Health(cctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "autocommit") {
		t.Fatalf("Health on a session with autocommit=0 = %v, want fatal naming autocommit", err)
	}
}

// --------------------------------------------------------------- item 11

// The unique indexes and the column widths of item 11, each against the real
// catalogue. The canonical schema and a surrogate AUTO_INCREMENT key are the
// controls: without them a rule could refuse everything for the wrong reason.
func TestMySQLValidationRefusesSilentLossShapes(t *testing.T) {
	f := mysqlSetup(t)
	cases := []struct {
		name  string
		alter string
		want  string // substring of the fatal error; "" means accepted
	}{
		{"canonical", "", ""},
		{"auto_increment surrogate key", "ALTER TABLE `$T` ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT UNIQUE", ""},
		{"second unique index", "ALTER TABLE `$T` ADD UNIQUE INDEX u_sid (sid)", "u_sid"},
		{"unique on (uuid, step, level) beside the key", "ALTER TABLE `$T` ADD UNIQUE INDEX u3 (uuid, step, level)", "u3"},
		{"unique hostname prefix", "ALTER TABLE `$T` ADD UNIQUE INDEX u_host (hostname(32))", "hostname(32)"},
		{"prefix primary key", "ALTER TABLE `$T` DROP PRIMARY KEY, ADD PRIMARY KEY (uuid(4), step)", "without a prefix"},
		{"space varchar(16)", "ALTER TABLE `$T` MODIFY space VARCHAR(16) NOT NULL", `"space"`},
		{"format varchar(7)", "ALTER TABLE `$T` MODIFY format VARCHAR(7) NOT NULL", `"format"`},
		{"src_ip varchar(16)", "ALTER TABLE `$T` MODIFY src_ip VARCHAR(16)", `"src_ip"`},
		{"sid int", "ALTER TABLE `$T` MODIFY sid INT NOT NULL", "sid"},
		{"ev_uts int", "ALTER TABLE `$T` MODIFY ev_uts INT NOT NULL", "ev_uts"},
		{"nbytes smallint", "ALTER TABLE `$T` MODIFY nbytes SMALLINT NOT NULL", "nbytes"},
		// What only this server's rows look like is legitimate: tcp and tls,
		// never unix, so src_ip is never NULL (independent review, 2026-10-11).
		{"src_ip NOT NULL", "ALTER TABLE `$T` MODIFY src_ip VARCHAR(64) NOT NULL", ""},
		{"transport char(3)", "ALTER TABLE `$T` MODIFY transport CHAR(3) NOT NULL", ""},
		{"transport check tcp/tls", "ALTER TABLE `$T` ADD CONSTRAINT ck_tr CHECK (transport IN ('tcp','tls'))", ""},
		{"transport char(2)", "ALTER TABLE `$T` MODIFY transport CHAR(2) NOT NULL", `"transport"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Table = f.newTable(tc.alter)
			cfg.MaxEventBytes = MaxEventBytes(64 << 10)
			cfg.MaxMessageBytes = 64 << 10
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
				if n := f.count(cfg.Table); n != 2 {
					t.Errorf("%d rows, want 2 (and none left by the probe)", n)
				}
				return
			}
			if !IsFatal(err) {
				t.Fatalf("Health = %v (class %s), want fatal", err, ClassOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}
