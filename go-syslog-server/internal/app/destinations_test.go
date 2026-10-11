package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// Boot validation and availability of the real destinations (SPEC §10.5,
// §10.6, §10.7, §11, §13.1, §13.3). Each test skips without its SYSLOG_TEST_*
// variable.

// refusedBoot runs an instance whose destination validation must refuse it:
// exit 3 well before sink.boot_timeout (a permanent refusal is not retried),
// with no transport ever opened.
func refusedBoot(t *testing.T, args []string) {
	t.Helper()
	tcp := freePort(t)
	in := launch(t, testID, append(args, "--listen.udp=", "--listen.tcp="+tcp, "--sink.boot_timeout=120s"), nil)
	began := time.Now()
	code := in.waitExit(60 * time.Second)
	if code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if took := time.Since(began); took > 20*time.Second {
		t.Errorf("the refusal took %s: a permanent refusal must not wait for boot_timeout", took)
	}
	if c, err := net.DialTimeout("tcp", tcp, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("a transport is open after a refused boot")
	}
}

// SPEC §10.7, §3.9, invariants §20.17, §20.20, §20.21: the stream is validated,
// never created: missing, not covering the subject, capturing the control
// plane, or too small for the largest possible event -- each is exit 3 at once.
func TestNATSBootValidationExitsThree(t *testing.T) {
	e := natsTest(t)
	streamFor := func(t *testing.T, subjects ...string) string {
		t.Helper()
		b, err := os.ReadFile("../../docker/nats/stream.json")
		if err != nil {
			t.Fatal(err)
		}
		var cfg jetstream.StreamConfig
		if err := json.Unmarshal(b, &cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Name = fmt.Sprintf("SLS_APP_BAD_%d", time.Now().UnixNano())
		cfg.Subjects = subjects
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := e.js.CreateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.js.DeleteStream(context.Background(), cfg.Name) })
		return cfg.Name
	}
	t.Run("missing stream", func(t *testing.T) {
		refusedBoot(t, e.args(t.TempDir(), fmt.Sprintf("SLS_MISSING_%d", time.Now().UnixNano())))
	})
	t.Run("stream of another space", func(t *testing.T) {
		refusedBoot(t, e.args(t.TempDir(), streamFor(t, "loghub.other.syslog", "loghub.other.syslog.>")))
	})
	t.Run("stream capturing the control plane", func(t *testing.T) {
		refusedBoot(t, e.args(t.TempDir(), streamFor(t, "loghub.>")))
	})
	t.Run("largest event above max_payload", func(t *testing.T) {
		// 12 x 16 MiB + 8 KiB against the default 1 MiB max_payload (SPEC §3.9).
		refusedBoot(t, e.args(t.TempDir(), e.stream(t), "--syslog.max_message_bytes=16MiB"))
	})
	t.Run("the canonical stream boots", func(t *testing.T) {
		in := launch(t, testID, e.args(t.TempDir(), e.stream(t)), nil)
		in.waitReady()
		if code := in.stop(); code != ExitOK {
			t.Errorf("exit %d", code)
		}
	})
}

// SPEC §10.5, §10.6, invariant §20.17: the table is validated, never created.
func TestSQLBootValidationExitsThree(t *testing.T) {
	t.Run("postgres missing table", func(t *testing.T) {
		dsn := os.Getenv(pgDSNEnv)
		if dsn == "" {
			t.Skipf("set %s", pgDSNEnv)
		}
		pc, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		table := fmt.Sprintf("sls_missing_%d", time.Now().UnixNano())
		refusedBoot(t, append(baseArgs(t.TempDir()), "--sink.type=postgres",
			"--sink.postgres.addr="+net.JoinHostPort(pc.Host, strconv.Itoa(int(pc.Port))),
			"--sink.postgres.username="+pc.User, "--sink.postgres.password="+pc.Password,
			"--sink.postgres.database="+pc.Database, "--sink.postgres.table="+table))
		conn, err := pgx.ConnectConfig(context.Background(), pc)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(context.Background())
		var reg *string
		if err := conn.QueryRow(context.Background(), "SELECT to_regclass($1)::text", table).Scan(&reg); err != nil || reg != nil {
			t.Errorf("the server created the table it validates (%v, %v)", reg, err)
		}
	})
	mysqlArgs := func(t *testing.T, table string, extra ...string) []string {
		dsn := os.Getenv(mysqlDSNEnv)
		if dsn == "" {
			t.Skipf("set %s", mysqlDSNEnv)
		}
		mc, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		return append(append(baseArgs(t.TempDir()), "--sink.type=mysql",
			"--sink.mysql.addr="+mc.Addr, "--sink.mysql.username="+mc.User,
			"--sink.mysql.password="+mc.Passwd, "--sink.mysql.database="+mc.DBName,
			"--sink.mysql.table="+table), extra...)
	}
	t.Run("mysql missing table", func(t *testing.T) {
		refusedBoot(t, mysqlArgs(t, fmt.Sprintf("sls_missing_%d", time.Now().UnixNano())))
	})
	t.Run("mysql largest row above max_allowed_packet", func(t *testing.T) {
		dsn := os.Getenv(mysqlDSNEnv)
		if dsn == "" {
			t.Skipf("set %s", mysqlDSNEnv)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		table := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
		b, err := os.ReadFile("../../docker/mysql/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		var keep []string
		for _, l := range strings.Split(string(b), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "--") {
				keep = append(keep, l)
			}
		}
		stmt := strings.TrimSuffix(strings.TrimSpace(strings.Join(keep, "\n")), ";")
		stmt = strings.Replace(stmt, "CREATE TABLE IF NOT EXISTS syslog_events", "CREATE TABLE `"+table+"`", 1)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		defer db.Exec("DROP TABLE IF EXISTS `" + table + "`")
		// 12 x 16 MiB + 3 x 16 MiB + 8.5 KiB against the 64 MiB default.
		refusedBoot(t, mysqlArgs(t, table, "--syslog.max_message_bytes=16MiB"))
	})
}

// ------------------------------------------------------------ outage

// tcpProxy stands between the server and a destination, so a test can take
// the destination away and bring it back without touching the real server.
type tcpProxy struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	paused bool
	closed bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		refuse := p.paused || p.closed
		p.mu.Unlock()
		if refuse {
			// A dead destination looks like a refused connection, not a stall.
			c.Close()
			continue
		}
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(client net.Conn) {
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	if p.paused || p.closed {
		p.mu.Unlock()
		client.Close()
		up.Close()
		return
	}
	p.conns[client], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { io.Copy(up, client); done <- struct{}{} }()
	go func() { io.Copy(client, up); done <- struct{}{} }()
	<-done
	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, up)
	p.mu.Unlock()
	client.Close()
	up.Close()
}

// pause cuts every live connection and refuses new ones.
func (p *tcpProxy) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for c := range p.conns {
		c.Close()
	}
}

func (p *tcpProxy) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *tcpProxy) close() {
	p.mu.Lock()
	p.closed = true
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.ln.Close()
}

// SPEC §10.7, §11, §13.3: NATS goes away and comes back. While it is away the
// sink is DOWN and /readyz says so; TCP holds what it reads; the client
// reconnects by itself, a beacon goes out on the reconnection, and nothing that
// was accepted is missing from the stream.
func TestNATSDownThenBackLosesNothingAndBeaconsAgain(t *testing.T) {
	e := natsTest(t)
	stream := e.stream(t)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	sub, err := e.nc.SubscribeSync("loghub._ctl.beacon")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	e.nc.Flush()

	in := launch(t, testID, e.args(t.TempDir(), stream,
		"--sink.nats.urls=nats://"+proxy.addr(),
		"--loghub.beacon_interval=0", // only the connection beacons
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000",
		"--sink.op_timeout=1s", "--sink.health_timeout=500ms"), nil)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")

	beacons := func() int {
		n := 0
		for {
			msg, err := sub.NextMsg(10 * time.Millisecond)
			if err != nil {
				return n
			}
			var b struct {
				From string `json:"from"`
				Type string `json:"type"`
			}
			if json.Unmarshal(msg.Data, &b) == nil && b.From == testID.From && b.Type == "beacon" {
				n++
			}
		}
	}
	seen := 0
	waitFor(t, 10*time.Second, "the connection beacon", func() bool { seen += beacons(); return seen >= 1 })

	sendLF(t, tcp, natsLines("before", 10)...)
	waitFor(t, 10*time.Second, "the first ten in the stream", func() bool { return e.count(stream) == 10 })

	proxy.pause()
	waitFor(t, 20*time.Second, "/readyz to fail while NATS is away", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "sink is DOWN")
	})
	sendLF(t, tcp, natsLines("during", 10)...)
	waitFor(t, 10*time.Second, "the held line read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() >= 11 })

	proxy.resume()
	waitFor(t, 60*time.Second, "the recovery", func() bool { code, _ := in.get("/readyz"); return code == 200 })
	waitFor(t, 30*time.Second, "everything in the stream", func() bool { return e.count(stream) == 20 })
	waitFor(t, 10*time.Second, "a beacon on the reconnection", func() bool { seen += beacons(); return seen >= 2 })
	if got := m.SinkNATSReconnectsTotal.Load(); got < 1 {
		t.Errorf("syslog_sink_nats_reconnects_total = %d after a reconnection", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	msgs := e.consume(t, stream, "loghub.default.syslog", 20)
	got := map[string]bool{}
	for _, msg := range msgs {
		got[msg.env.Payload.message()] = true
	}
	for _, prefix := range []string{"before", "during"} {
		for i := 0; i < 10; i++ {
			if k := fmt.Sprintf("%s-%04d", prefix, i); !got[k] {
				t.Errorf("%s is missing from the stream", k)
			}
		}
	}
	if d := m.MessagesDroppedTotal[obs.DropSinkDown].Load(); d != 0 {
		t.Errorf("TCP dropped %d messages while the sink was DOWN; it must hold them", d)
	}
}

// SPEC §13.3, §12, §18.8 with a real destination: NATS goes away for good
// while confirmed messages are still in the memory ring. The fuses
// (fail_timeout, max_retries) turn DOWN into FATAL, the ring goes to an
// emergency dump, the process exits 3, and the next boot reingests the dump
// into the stream once NATS is back.
func TestNATSFatalDumpsAndTheNextBootReingests(t *testing.T) {
	e := natsTest(t)
	stream := e.stream(t)
	proxy := newTCPProxy(t, strings.TrimPrefix(e.url, "nats://"))
	dir := t.TempDir()
	args := e.args(dir, stream, "--ring.mode=memory", "--listen.udp=",
		"--sink.nats.urls=nats://"+proxy.addr(),
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--sink.op_timeout=1s", "--sink.health_timeout=300ms")

	// Batches wait ten seconds to fill, so the accepted messages are still in
	// the ring when the destination goes away.
	first := launch(t, testID, append(args, "--sink.batch_max_delay=10s"), nil)
	first.waitReady()
	lines := natsLines("dumped", 8)
	sendLF(t, first.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == uint64(len(lines))
	})
	proxy.pause()
	if code := first.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if n := e.count(stream); n != 0 {
		t.Fatalf("%d messages reached the stream before the outage; the test needs them in the ring", n)
	}
	if dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}

	proxy.resume()
	second := launch(t, testID, args, nil)
	second.waitReady()
	waitFor(t, 20*time.Second, "the reingested messages in the stream", func() bool { return e.count(stream) == uint64(len(lines)) })
	got := map[string]bool{}
	for _, m := range e.consume(t, stream, "loghub.default.syslog", len(lines)) {
		checkHeaders(t, m)
		got[m.env.Payload.message()] = true
	}
	for i := range lines {
		if k := fmt.Sprintf("dumped-%04d", i); !got[k] {
			t.Errorf("%s was lost across the FATAL", k)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// SPEC §10.5, §10.6: a table that breaks the contract is refused at boot --
// a missing column, no unique index on (uuid, step) -- with exit 3 at once.
func TestSQLSchemaMismatchExitsThree(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv(pgDSNEnv)
		if dsn == "" {
			t.Skipf("set %s", pgDSNEnv)
		}
		pc, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgx.ConnectConfig(context.Background(), pc)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(context.Background())
		for name, alter := range map[string]string{
			"missing column":  "ALTER TABLE $T DROP COLUMN event_nsec",
			"no unique index": "ALTER TABLE $T DROP CONSTRAINT inbox_pk",
		} {
			t.Run(name, func(t *testing.T) {
				table := pgQueue(t, conn, alter)
				refusedBoot(t, append(baseArgs(t.TempDir()), "--sink.type=postgres",
					"--sink.postgres.addr="+net.JoinHostPort(pc.Host, strconv.Itoa(int(pc.Port))),
					"--sink.postgres.username="+pc.User, "--sink.postgres.password="+pc.Password,
					"--sink.postgres.database="+pc.Database, "--sink.postgres.table="+table))
			})
		}
	})
	t.Run("mysql", func(t *testing.T) {
		dsn := os.Getenv(mysqlDSNEnv)
		if dsn == "" {
			t.Skipf("set %s", mysqlDSNEnv)
		}
		mc, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		b, err := os.ReadFile("../../docker/mysql/schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		var keep []string
		for _, l := range strings.Split(string(b), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "--") {
				keep = append(keep, l)
			}
		}
		base := strings.TrimSuffix(strings.TrimSpace(strings.Join(keep, "\n")), ";")
		for name, alter := range map[string]string{
			"missing column":  "ALTER TABLE `$T` DROP COLUMN msgid",
			"no unique index": "ALTER TABLE `$T` DROP PRIMARY KEY",
		} {
			t.Run(name, func(t *testing.T) {
				table := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
				if _, err := db.Exec(strings.Replace(base, "CREATE TABLE IF NOT EXISTS syslog_events",
					"CREATE TABLE `"+table+"`", 1)); err != nil {
					t.Fatal(err)
				}
				defer db.Exec("DROP TABLE IF EXISTS `" + table + "`")
				if _, err := db.Exec(strings.ReplaceAll(alter, "$T", table)); err != nil {
					t.Fatal(err)
				}
				refusedBoot(t, append(baseArgs(t.TempDir()), "--sink.type=mysql",
					"--sink.mysql.addr="+mc.Addr, "--sink.mysql.username="+mc.User,
					"--sink.mysql.password="+mc.Passwd, "--sink.mysql.database="+mc.DBName,
					"--sink.mysql.table="+table))
			})
		}
	})
}
