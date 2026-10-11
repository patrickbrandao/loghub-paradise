package sink

import (
	"context"
	"database/sql"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

func newSQLiteSink(t *testing.T, rotate string) (*SQLite, string, *obs.Metrics) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.db")
	m := metrics()
	s, err := NewSQLite(SQLiteConfig{Path: path, Rotate: rotate, Synchronous: "OFF"}, discard(), m)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path, m
}

func openRO(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// SPEC §3.7: every promoted column lands, typed; env is the JLH byte for byte;
// a field that did not come is NULL, never ”.
func TestSQLiteSinkWritesQueryableColumns(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	b := batchOf(t, line5424, line3164)
	if err := s.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	db := openRO(t, path)

	e := b.Envelopes[0]
	var (
		uuid, space, format, transport string
		srcIP, host, app, proc, msgid  sql.NullString
		step, level, sid, uts, sub     int64
		fac, sev, nbytes               int64
		env                            []byte
	)
	err := db.QueryRow(`SELECT uuid, step, space, level, sid, ev_uts, ev_sub, facility, severity,
	  format, transport, src_ip, hostname, appname, procid, msgid, env, nbytes
	  FROM syslog_events WHERE hostname = 'web-01' AND ev_uts > 0`).Scan(
		&uuid, &step, &space, &level, &sid, &uts, &sub, &fac, &sev, &format, &transport,
		&srcIP, &host, &app, &proc, &msgid, &env, &nbytes)
	if err != nil {
		t.Fatalf("the indexed query found nothing: %v", err)
	}
	h := e.Item.Head
	if uuid != h.UUID.String() || step != 0 || space != "default" || level != 5 ||
		sid != int64(e.Res.SID) || uts != h.UTS || sub != h.Sub || fac != 20 || sev != 5 ||
		format != "rfc5424" || transport != "tcp" || srcIP.String != "10.0.0.7" ||
		host.String != "web-01" || app.String != "nginx" || proc.String != "1234" ||
		msgid.String != "ID47" || nbytes != int64(len(e.JLH)) {
		t.Fatalf("row does not match: %s %d %s %d %d %d %d %d %d %s %s %v %v %v %v %v %d",
			uuid, step, space, level, sid, uts, sub, fac, sev, format, transport,
			srcIP, host, app, proc, msgid, nbytes)
	}
	if string(env) != string(e.JLH) {
		t.Fatal("env is not the JLH byte for byte")
	}
	if err := jlhcheck.Validate(env); err != nil {
		t.Fatal(err)
	}

	// RFC 3164 has no msgid: NULL, "did not come".
	var msgidNull bool
	if err := db.QueryRow(`SELECT msgid IS NULL FROM syslog_events WHERE appname = 'su'`).Scan(&msgidNull); err != nil {
		t.Fatal(err)
	}
	if !msgidNull {
		t.Fatal("an absent msgid was stored as a value, not NULL")
	}

	// Text search is over env, with the CAST: LIKE over a BLOB matches nothing.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM syslog_events
	                       WHERE CAST(env AS TEXT) LIKE '%su root%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("CAST(env AS TEXT) LIKE found %d rows", n)
	}
}

// SPEC §3.7: unix has no addresses, so src_ip is NULL; a message without a
// hostname leaves hostname NULL.
func TestSQLiteSinkNullsWhatDidNotCome(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	it := newItem("<13>no header at all")
	it.Head.Transport, it.Head.Framing = item.TransportUnix, item.FramingDatagram
	it.Head.SrcIP, it.Head.SrcPort, it.Head.DstIP, it.Head.DstPort = netip.Addr{}, 0, netip.Addr{}, 0
	b := batchFrom(t, it)
	if err := s.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	var srcNull, hostNull, appNull bool
	var transport string
	if err := openRO(t, path).QueryRow(`SELECT src_ip IS NULL, hostname IS NULL, appname IS NULL, transport
	  FROM syslog_events`).Scan(&srcNull, &hostNull, &appNull, &transport); err != nil {
		t.Fatal(err)
	}
	if !srcNull || !hostNull || !appNull || transport != "unix" {
		t.Fatalf("src_ip null=%v hostname null=%v appname null=%v transport=%s", srcNull, hostNull, appNull, transport)
	}
}

// SPEC §3.7: NUL becomes U+FFFD in the text column ONLY; env keeps the JLH,
// with the NUL escaped as \u0000, byte for byte.
func TestSQLiteSinkSanitisesColumnsNotEnv(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	b := batchOf(t, "<13>Oct 11 22:14:15 web\x00-01 app[1]: m")
	if err := s.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	var host string
	var env []byte
	if err := openRO(t, path).QueryRow(`SELECT hostname, env FROM syslog_events`).Scan(&host, &env); err != nil {
		t.Fatal(err)
	}
	if host != "web�-01" {
		t.Errorf("hostname column = %q, want the NUL replaced", host)
	}
	if string(env) != string(b.Envelopes[0].JLH) || !strings.Contains(string(env), `web\u0000-01`) {
		t.Errorf("env was altered: %s", env)
	}
}

// The pipeline is at-least-once: a redelivered envelope must not take the
// chunk down with it, nor duplicate the row (ON CONFLICT (uuid, step)).
func TestSQLiteSinkIsIdempotent(t *testing.T) {
	s, path, _ := newSQLiteSink(t, "none")
	b := batchOf(t, line5424)
	for i := 0; i < 3; i++ {
		if err := s.Write(context.Background(), b); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	var n int
	openRO(t, path).QueryRow(`SELECT COUNT(*) FROM syslog_events`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d rows after three writes of the same uuid", n)
	}
}

func TestSQLiteSinkRotates(t *testing.T) {
	s, _, m := newSQLiteSink(t, "hourly")
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return now })

	if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
		t.Fatal(err)
	}
	// The sink opened its file against the real clock, so this first write
	// already rotated into the fake clock's period. The boundary under test is
	// the next one.
	first := s.current
	baseline := m.SinkSQLiteRotationsTotal.Load()

	now = now.Add(time.Hour)
	if err := s.Write(context.Background(), batchOf(t, line3164)); err != nil {
		t.Fatal(err)
	}
	if s.current == first {
		t.Fatal("the file did not rotate across the hour boundary")
	}
	if got := m.SinkSQLiteRotationsTotal.Load() - baseline; got != 1 {
		t.Fatalf("rotations across the boundary = %d, want 1", got)
	}
	// Each file holds its own event; nothing was split or lost at the boundary.
	for _, f := range []string{first, s.current} {
		var n int
		openRO(t, f).QueryRow(`SELECT COUNT(*) FROM syslog_events`).Scan(&n)
		if n != 1 {
			t.Fatalf("%s holds %d rows, want 1", filepath.Base(f), n)
		}
	}
}

// keep_files prunes the oldest rotated files, never the current one.
func TestSQLiteKeepFilesPrunes(t *testing.T) {
	s, _, m := newSQLiteSink(t, "hourly")
	now := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return now })
	s.SetKeepFiles(2)
	for i := 0; i < 5; i++ {
		if err := s.Write(context.Background(), batchOf(t, line5424)); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Hour)
	}
	if m.SinkSQLiteFilesRemovedTotal.Load() == 0 {
		t.Fatal("keep_files=2 removed nothing")
	}
	if _, err := os.Stat(s.current); err != nil {
		t.Fatalf("the current file is gone: %v", err)
	}
}

// keep_files defaults to 0, which removes NOTHING: silently deleting a log
// archive is not a decision this program makes for an operator who did not ask.
func TestSQLiteKeepFilesZeroRemovesNothing(t *testing.T) {
	s, _, m := newSQLiteSink(t, "hourly")
	now := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time { return now })
	for i := 0; i < 5; i++ {
		s.Write(context.Background(), batchOf(t, line5424))
		now = now.Add(time.Hour)
	}
	if m.SinkSQLiteFilesRemovedTotal.Load() != 0 {
		t.Fatal("keep_files=0 removed a file")
	}
}

// SPEC §10.4: the probe is a WRITE, rewritten in place, so it proves the file
// is still writable and costs the same at any table size. quick_check, which
// reads the whole file, runs at boot only.
func TestSQLiteSinkHealthOnAFreshFile(t *testing.T) {
	s, _, _ := newSQLiteSink(t, "none")
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health on a healthy file: %v", err)
	}
	var first, second int64
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&first); err != nil {
		t.Fatalf("the heartbeat row was not written: %v", err)
	}
	time.Sleep(time.Millisecond)
	if err := s.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("the heartbeat did not advance: %d -> %d", first, second)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM health`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("health table has %d rows (%v), want exactly one rewritten in place", rows, err)
	}
	if err := s.integrityCheck(); err != nil {
		t.Fatalf("integrityCheck on a good database: %v", err)
	}

	s.Close()
	if err := s.Health(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Health after Close = %v", err)
	}
	if err := s.Write(context.Background(), batchOf(t, line5424)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after Close = %v", err)
	}
}

// A file that is not a database is refused at boot, before anything is
// accepted that could not be stored.
func TestSQLiteSinkRefusesADamagedFileAtBoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	junk := []byte(strings.Repeat("this is not a database ", 100))
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics())
	if err == nil {
		t.Fatal("a file that is not a database was opened as one")
	}
	if !IsFatal(err) {
		t.Fatalf("err = %v (class %s), want fatal", err, ClassOf(err))
	}
}

// SPEC §10.4: the heartbeat proves the file is still WRITABLE. A file that
// turned read-only underneath the process -- a remount, a permission change --
// fails the probe with the fatal class, because waiting cannot heal it.
func TestSQLiteHeartbeatDetectsAReadOnlyFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health on a fresh file: %v", err)
	}
	s.Close()

	sidecars := []string{path, path + "-wal", path + "-shm"}
	for _, p := range sidecars {
		if _, err := os.Stat(p); err == nil {
			if err := os.Chmod(p, 0o444); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() {
		for _, p := range sidecars {
			_ = os.Chmod(p, 0o600)
		}
	})

	s, err = NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics())
	if err != nil {
		t.Fatalf("reopening a read-only events.db: %v -- the heartbeat, not the open, is what must notice", err)
	}
	defer s.Close()

	err = s.Health(context.Background())
	if err == nil {
		t.Fatal("Health passed on a read-only file: the probe does not prove writability")
	}
	if !IsFatal(err) {
		t.Fatalf("Health = %v (class %s), want fatal: a read-only file does not heal by waiting", err, ClassOf(err))
	}
}

// SPEC §10.4: SQLITE_FULL and SQLITE_CORRUPT are fatal, SQLITE_BUSY is network,
// a constraint is logic. Real driver errors first, then the text fallback.
func TestClassifySQLite(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (a INTEGER NOT NULL, b TEXT UNIQUE) STRICT`); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO t (a) VALUES (NULL)`)
	if got := ClassOf(classifySQLite("insert", err)); err == nil || got != ClassLogic {
		t.Errorf("NOT NULL violation (%v) = %v, want logic", err, got)
	}
	_, err = db.Exec(`INSERT INTO t (a, b) VALUES ('x', 'y')`)
	if got := ClassOf(classifySQLite("insert", err)); err == nil || got != ClassLogic {
		t.Errorf("STRICT type mismatch (%v) = %v, want logic", err, got)
	}

	for _, msg := range []string{
		"database or disk is full", "database disk image is malformed",
		"attempt to write a readonly database", "file is not a database",
	} {
		if got := ClassOf(classifySQLite("insert", errors.New(msg))); got != ClassFatal {
			t.Errorf("%q = %v, want fatal", msg, got)
		}
	}
	for _, msg := range []string{"database is locked", "disk i/o error", "something new"} {
		if got := ClassOf(classifySQLite("insert", errors.New(msg))); got != ClassNetwork {
			t.Errorf("%q = %v, want network", msg, got)
		}
	}
	if got := ClassOf(classifySQLite("insert", errors.New("UNIQUE constraint failed: t.b"))); got != ClassLogic {
		t.Errorf("constraint text = %v, want logic", got)
	}
	if classifySQLite("x", nil) != nil {
		t.Error("nil error classified")
	}
}

// SPEC §10.4: sink.workers is forced to 1 for SQLite, whatever was asked.
func TestSQLiteForcesOneWorker(t *testing.T) {
	s, _, m := newSQLiteSink(t, "none")
	cfg := workerCfg(16)
	cfg.Workers = 8
	w := NewWorkers(nil, s, nil, cfg, discard(), m)
	if w.cfg.Workers != 1 {
		t.Fatalf("workers = %d, want 1", w.cfg.Workers)
	}
	if !w.cfg.Convert.Columns {
		t.Fatal("columns not switched on for the SQLite sink")
	}
}
