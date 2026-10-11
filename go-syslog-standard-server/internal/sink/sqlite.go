package sink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"

	_ "modernc.org/sqlite"
)

// SQLiteConfig mirrors [sink.sqlite].
type SQLiteConfig struct {
	Path              string
	Rotate            string // none | hourly | daily, evaluated in UTC
	KeepFiles         int    // 0 never deletes anything
	Synchronous       string // FULL | NORMAL | OFF
	BusyTimeout       time.Duration
	WALAutocheckpoint int
}

// SQLite is the standalone final destination (SPEC §10.4). It needs no Redis.
//
// The table carries the parsed fields as real columns as well as the intact
// envelope and the intact raw line. That is deliberate: this is the FINAL
// destination, so it has to be queryable with plain SQL
// (WHERE hostname = ? AND ts > ?). Storing only the blob would force
// json_extract() over a full scan and reduce the database to a dump file.
//
// There is no msg column. Text search is `raw LIKE '%...%'`, which is BETTER
// than a message column: it also covers the hostname, the tag and the structured
// data, which is where half of an investigation actually ends. Whoever needs
// exactly the MSG uses substr(raw, msg_offset + 1, msg_size).
//
// Exactly one goroutine ever writes. SQLite serialises writers by construction,
// so additional ones would only produce SQLITE_BUSY; sink.workers is forced to 1
// by configuration validation.
type SQLite struct {
	cfg SQLiteConfig
	log *slog.Logger
	m   *obs.Metrics

	mu      sync.Mutex
	db      *sql.DB
	insert  *sql.Stmt
	current string // path of the open file
	period  string // rotation period the open file belongs to

	// now is the clock the rotation reads, a field only so a test can walk it
	// across a period boundary. Waiting an hour for the real one is not a test,
	// and an untested rotation is the feature most likely to lose an event the
	// first time it fires in production.
	now func() time.Time

	closed bool
}

// Three indexes, not four. (app_name, ts) is the obvious fourth and is left out
// on purpose: every index is another write per row on a destination whose
// bottleneck is writing, and app_name is in raw for anyone searching by text.
// Whoever queries by application all day creates the index; the server does not
// impose the cost on whoever does not.
const sqliteSchema = `
CREATE TABLE IF NOT EXISTS events (
  id         TEXT    PRIMARY KEY,
  ts         INTEGER NOT NULL,
  nts        INTEGER NOT NULL,
  facility   INTEGER NOT NULL,
  severity   INTEGER NOT NULL,
  hostname   TEXT    NOT NULL,
  app_name   TEXT    NOT NULL,
  procid     TEXT    NOT NULL,
  msgid      TEXT    NOT NULL,
  peer_ip    TEXT    NOT NULL,
  transport  TEXT    NOT NULL,
  rfc        TEXT    NOT NULL,
  msg_offset INTEGER NOT NULL,
  msg_size   INTEGER NOT NULL,
  env        BLOB    NOT NULL,
  raw        BLOB    NOT NULL,
  nbytes     INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ev_ts   ON events(ts);
CREATE INDEX IF NOT EXISTS ev_host ON events(hostname, ts);
CREATE INDEX IF NOT EXISTS ev_sev  ON events(severity, ts);
CREATE TABLE IF NOT EXISTS health (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  ts INTEGER NOT NULL
) STRICT;
`

// INSERT OR IGNORE, not plain INSERT.
//
// This pipeline is at-least-once by design: a crash between a successful write
// and the ring's delete causes redelivery, and that is the correct trade. The id
// is a UUIDv7 primary key, so ignoring an exact duplicate is precisely right.
//
// A plain INSERT would raise a constraint error for the whole batch, and one
// redelivered message would take every message beside it down with it.
const sqliteInsert = `INSERT OR IGNORE INTO events
  (id, ts, nts, facility, severity, hostname, app_name, procid, msgid,
   peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`

// sqliteHeartbeat is the Health probe: one row, rewritten in place, so the cost
// is constant whatever the size of the events table (SPEC §10.4).
const sqliteHeartbeat = `INSERT INTO health (id, ts) VALUES (1, ?)
  ON CONFLICT(id) DO UPDATE SET ts = excluded.ts`

// NewSQLite opens the destination database, creating it and its schema.
func NewSQLite(cfg SQLiteConfig, log *slog.Logger, m *obs.Metrics) (*SQLite, error) {
	if cfg.Synchronous == "" {
		cfg.Synchronous = "FULL"
	}
	if cfg.BusyTimeout <= 0 {
		cfg.BusyTimeout = 5 * time.Second
	}
	if cfg.WALAutocheckpoint <= 0 {
		cfg.WALAutocheckpoint = 1000
	}

	s := &SQLite{cfg: cfg, log: log, m: m, now: time.Now}
	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("sink sqlite: %w", err)
	}
	if err := s.openPeriod(periodOf(time.Now().UTC(), cfg.Rotate)); err != nil {
		return nil, err
	}
	// The integrity scan runs ONCE, here, where reading the whole file is
	// acceptable; the periodic Health is a constant-cost write probe instead
	// (SPEC §10.4).
	if err := s.integrityCheck(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLite) Type() string { return "sqlite" }

// dsn builds the connection string. modernc.org/sqlite applies _pragma
// parameters to every new connection, which is the only way to be sure they are
// set on a connection the pool created later.
func (s *SQLite) dsn(path string) string {
	return "file:" + path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(" + s.cfg.Synchronous + ")" +
		"&_pragma=busy_timeout(" + strconv.Itoa(int(s.cfg.BusyTimeout.Milliseconds())) + ")" +
		"&_pragma=wal_autocheckpoint(" + strconv.Itoa(s.cfg.WALAutocheckpoint) + ")" +
		"&_txlock=immediate"
}

// openPeriod opens the file for a rotation period, replacing any open one.
func (s *SQLite) openPeriod(period string) error {
	path := s.pathFor(period)

	db, err := sql.Open("sqlite", s.dsn(path))
	if err != nil {
		return fmt.Errorf("sink sqlite: opening %s: %w", path, err)
	}
	// One writer, always. More connections would only contend for the single
	// write lock this engine allows.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return fmt.Errorf("sink sqlite: creating schema in %s: %w", path, err)
	}
	stmt, err := db.Prepare(sqliteInsert)
	if err != nil {
		db.Close()
		return fmt.Errorf("sink sqlite: preparing insert: %w", err)
	}

	if s.insert != nil {
		s.insert.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
	s.db, s.insert, s.current, s.period = db, stmt, path, period
	s.updateSizeGauge()
	return nil
}

// pathFor turns a rotation period into a filename: events.db, or
// events-2026-09-06.db / events-2026-09-06-14.db when rotating.
func (s *SQLite) pathFor(period string) string {
	if period == "" {
		return s.cfg.Path
	}
	ext := filepath.Ext(s.cfg.Path)
	base := strings.TrimSuffix(s.cfg.Path, ext)
	return base + "-" + period + ext
}

// periodOf returns the rotation period label for an instant, in UTC. An empty
// string means rotation is off.
//
// UTC and not local time, for the same reason the RFC 3164 year rule uses UTC:
// two nodes of one cluster in different zones must not disagree about which file
// an event belongs in.
func periodOf(t time.Time, rotate string) string {
	switch rotate {
	case "hourly":
		return t.UTC().Format("2006-01-02-15")
	case "daily":
		return t.UTC().Format("2006-01-02")
	}
	return ""
}

// maybeRotate switches files at a period boundary.
//
// It runs BETWEEN batches, never inside a transaction, so an event can never be
// split across two files or lost at the boundary.
func (s *SQLite) maybeRotate(now time.Time) error {
	want := periodOf(now, s.cfg.Rotate)
	if want == s.period {
		return nil
	}
	s.log.Info("rotating the events database", "from", s.current, "to", s.pathFor(want))
	if err := s.openPeriod(want); err != nil {
		return err
	}
	s.m.SinkSQLiteRotationsTotal.Inc()
	s.pruneRotated()
	return nil
}

// setClock replaces the rotation clock. Test-only; the writer holds the same
// lock, so the swap cannot land in the middle of a batch.
func (s *SQLite) setClock(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = f
}

// SetKeepFiles changes the retention at runtime. It is the one [sink.sqlite] key
// SPEC §15.3 allows SIGHUP to change, because retention is a policy an operator
// revises when a disk starts filling -- exactly the moment when restarting the
// ingestion server is the least welcome option.
//
// The lock is the same one the writer holds: pruneRotated runs from Write, so the
// value can only be read between batches, never in the middle of one.
func (s *SQLite) SetKeepFiles(n int) {
	if n < 0 {
		n = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg.KeepFiles = n
}

// pruneRotated removes the oldest rotated files beyond keep_files.
//
// The default of 0 removes NOTHING: silently deleting a log archive is not a
// decision this program makes for an operator who did not ask.
func (s *SQLite) pruneRotated() {
	keep := s.cfg.KeepFiles
	if keep <= 0 || s.cfg.Rotate == "none" || s.cfg.Rotate == "" {
		return
	}

	ext := filepath.Ext(s.cfg.Path)
	base := filepath.Base(strings.TrimSuffix(s.cfg.Path, ext))
	dir := filepath.Dir(s.cfg.Path)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var rotated []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, base+"-") || !strings.HasSuffix(n, ext) {
			continue
		}
		p := filepath.Join(dir, n)
		if p == s.current {
			continue // never remove the file being written
		}
		rotated = append(rotated, p)
	}
	if len(rotated) <= keep {
		return
	}
	// Names embed a zero-padded UTC stamp, so lexical order is chronological.
	sort.Strings(rotated)

	for _, p := range rotated[:len(rotated)-keep] {
		// The WAL and shared-memory sidecars go with it, or they linger forever.
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(p + suffix)
		}
		s.m.SinkSQLiteFilesRemovedTotal.Inc()
		s.log.Info("removed a rotated events database", "path", p, "keep_files", keep)
	}
}

func (s *SQLite) updateSizeGauge() {
	if fi, err := os.Stat(s.current); err == nil {
		s.m.SinkSQLiteFileBytes.Set(fi.Size())
	}
}

// Write inserts the whole batch in one transaction.
func (s *SQLite) Write(ctx context.Context, batch []Prepared) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if err := s.maybeRotate(s.now()); err != nil {
		return classifySQLite("rotate", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classifySQLite("begin", err)
	}
	stmt := tx.StmtContext(ctx, s.insert)

	for i := range batch {
		p := &batch[i]
		m := &p.Meta
		raw := p.Raw()
		// SQLite accepts any byte sequence in TEXT, so the promoted columns go
		// in exactly as parsed. No sanitising happens here, unlike
		// sink=postgres, which refuses invalid UTF-8 outright (SPEC §3.6).
		_, err := stmt.ExecContext(ctx,
			p.Item.Head.ID, m.TS, m.NTS, m.Facility, m.Severity,
			m.Hostname, m.AppName, m.ProcID, m.MsgID,
			m.PeerIP, m.Transport, m.RFC, m.MsgOffset, m.MsgSize,
			p.Env, raw, len(p.Env)+len(raw))
		if err != nil {
			_ = tx.Rollback()
			return classifySQLite("insert", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return classifySQLite("commit", err)
	}
	s.updateSizeGauge()
	return nil
}

// Health verifies the database is still WRITABLE, not merely open or intact: a
// volume unmounted underneath an open handle, a file gone read-only, a full
// disk, all surface here rather than at the next batch.
//
// It is a heartbeat UPSERT, not PRAGMA quick_check. quick_check reads every
// page of the file, under the same lock the writer needs, and the events file
// grows without bound at the default rotate=none: past some size the check
// outlasts sink.health_timeout, the sink is marked DOWN, the fuses escalate to
// FATAL, and the next boot fails the same way, forever. The integrity scan is
// still run once, at boot (SPEC §10.4).
func (s *SQLite) Health(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}
	if _, err := s.db.ExecContext(ctx, sqliteHeartbeat, time.Now().UnixNano()); err != nil {
		return classifySQLite("heartbeat", err)
	}
	return nil
}

// integrityCheck runs PRAGMA quick_check on the open file and reports a fatal
// error when the structure is damaged. Boot only: see Health for why.
func (s *SQLite) integrityCheck() error {
	var txt string
	if err := s.db.QueryRow("PRAGMA quick_check(1)").Scan(&txt); err != nil {
		return classifySQLite("quick_check", err)
	}
	if txt != "ok" {
		return fatalErr("quick_check", errors.New(txt))
	}
	return nil
}

func (s *SQLite) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.insert != nil {
		s.insert.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// classifySQLite maps a driver error onto the class the state machine acts on
// (SPEC §10.4).
//
// The distinction that matters: BUSY and a transient I/O error are worth
// retrying, while a full disk, a corrupt file or a read-only database will never
// resolve on their own and must end the process rather than have it sit there
// dropping messages forever.
func classifySQLite(op string, err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(msg, "disk is full"),
		strings.Contains(msg, "database or disk is full"),
		strings.Contains(msg, "sqlite_full"):
		return fatalErr(op, err)

	case strings.Contains(msg, "malformed"),
		strings.Contains(msg, "not a database"),
		strings.Contains(msg, "corrupt"):
		return fatalErr(op, err)

	case strings.Contains(msg, "readonly"),
		strings.Contains(msg, "attempt to write a readonly database"):
		return fatalErr(op, err)

	case strings.Contains(msg, "database is locked"),
		strings.Contains(msg, "busy"),
		strings.Contains(msg, "disk i/o error"):
		return networkErr(op, err)
	}
	return networkErr(op, err)
}
