package ring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"

	_ "modernc.org/sqlite"
)

// SQLiteConfig mirrors [ring.sqlite].
type SQLiteConfig struct {
	Path              string
	Synchronous       string // FULL | NORMAL | OFF
	CommitBatch       int
	CommitMaxDelay    time.Duration
	BusyTimeout       time.Duration
	WALAutocheckpoint int

	CapacityItems int
	CapacityBytes int64

	// KeepBacklog leaves the rows a previous run left alone: the cursor
	// starts after the last of them and none is counted, so this run neither
	// delivers them nor fills up on them, and a later ring opened without it
	// reads them all (SPEC §7.4). The app asks for it with sink.type=none,
	// which would discard them; the ring never learns why (SPEC §17 item 12).
	KeepBacklog bool
}

// SQLiteRing is the persistent ring (SPEC §7.4), for maximum retention even at
// the cost of throughput.
//
// Enqueue returns only after the group COMMIT that includes the item has
// finished, so the 200 means the event is on disk. With synchronous=FULL that
// survives power loss; with NORMAL it survives a process crash but not a
// kernel one.
//
// Exactly TWO goroutines touch the database:
//
//   - the writer owns the write connection, batches INSERTs into group commits
//     and performs the DELETEs of already-delivered batches;
//   - the reader owns a separate connection (WAL allows a reader concurrent
//     with a writer) and feeds the drain workers.
//
// More writers would only produce SQLITE_BUSY: SQLite serialises them by
// construction. A claim column was rejected for doubling the writes (an UPDATE
// plus a DELETE per item) on a path where every COMMIT costs an fsync.
type SQLiteRing struct {
	cfg SQLiteConfig
	log *slog.Logger

	writeDB *sql.DB
	readDB  *sql.DB
	insert  *sql.Stmt

	// pending carries items from handlers to the writer goroutine.
	pending chan *envelope.Item
	// deletes carries the ids of delivered batches back to the writer.
	deletes chan []int64

	items atomic.Int64
	bytes atomic.Int64

	// resItems and resBytes are the reservations of items on their way to the
	// next group commit: Enqueue claims them before handing an item to the
	// writer, so the ceilings see what is about to be committed and not only
	// what already was (SPEC §7.4).
	resItems atomic.Int64
	resBytes atomic.Int64

	// cursor is the highest rowid_ handed to a worker. Reads resume strictly
	// after it, which is what guarantees a row is served at most once.
	//
	// An earlier version filtered an over-fetched window through an in-memory
	// "in flight" set instead. That deadlocks: once the window is entirely in
	// flight the same rows come back every time, the filter empties the result,
	// and the drain stalls with rows still undelivered. At the default
	// workers=8 x batch=256 the in-flight set exceeds any fixed window, so the
	// stall was not a corner case -- it was the normal configuration.
	//
	// A cursor cannot stall: it only moves forward. Rows a worker abandons are
	// left behind it, which is correct, because abandonment only happens on the
	// paths that end the process, and the next boot starts the cursor at zero.
	cursorMu sync.Mutex
	cursor   int64

	// kept is how many rows of a previous run KeepBacklog set aside. Written
	// once, before the writer goroutine starts.
	kept int64

	closed atomic.Bool
	wg     sync.WaitGroup
	stop   chan struct{}
	// writerDone is closed when the writer goroutine has returned, so a
	// handler that handed an item over just as the ring was closing can tell
	// "acknowledged on the way out" from "never picked up".
	writerDone chan struct{}
}

// The ring table stores the item's uuid beside the JLH (SPEC §7.4).
//
// The uuid has its own column for the log and for Commit; nothing else is
// promoted. The envelope is the whole event -- body included -- and a
// destination that needs a header field reads it from the JLH itself
// (envelope.ParseHead), so an item read back is indistinguishable from the one
// that went in.
//
// The table is jlh_ring, not ring: the v1 table held the pre-LogHub envelope
// and the body apart, which no destination understands any more. NewSQLite
// drops it when it is empty and leaves it untouched, with an ERROR, when it is
// not (legacyRing).
const ringSchema = `
CREATE TABLE IF NOT EXISTS jlh_ring (
  rowid_  INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid    TEXT    NOT NULL,
  jlh     BLOB    NOT NULL,
  nbytes  INTEGER NOT NULL
) STRICT;
`

const ringInsert = `INSERT INTO jlh_ring (uuid, jlh, nbytes) VALUES (?,?,?)`

const ringSelect = `SELECT rowid_, uuid, jlh FROM jlh_ring WHERE rowid_ > ? ORDER BY rowid_ LIMIT ?`

// NewSQLite opens the ring database and starts the writer goroutine.
func NewSQLite(cfg SQLiteConfig, log *slog.Logger) (*SQLiteRing, error) {
	if cfg.Synchronous == "" {
		cfg.Synchronous = "FULL"
	}
	if cfg.CommitBatch < 1 {
		cfg.CommitBatch = 256
	}
	if cfg.CommitMaxDelay <= 0 {
		cfg.CommitMaxDelay = 5 * time.Millisecond
	}
	if cfg.BusyTimeout <= 0 {
		cfg.BusyTimeout = 5 * time.Second
	}
	if cfg.WALAutocheckpoint <= 0 {
		cfg.WALAutocheckpoint = 1000
	}

	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("ring sqlite: %w", err)
	}

	r := &SQLiteRing{
		cfg:        cfg,
		log:        log,
		pending:    make(chan *envelope.Item, cfg.CommitBatch*2),
		deletes:    make(chan []int64, 64),
		stop:       make(chan struct{}),
		writerDone: make(chan struct{}),
	}

	dsn := "file:" + cfg.Path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(" + cfg.Synchronous + ")" +
		"&_pragma=busy_timeout(" + strconv.Itoa(int(cfg.BusyTimeout.Milliseconds())) + ")" +
		"&_pragma=wal_autocheckpoint(" + strconv.Itoa(cfg.WALAutocheckpoint) + ")" +
		"&_txlock=immediate"

	var err error
	if r.writeDB, err = sql.Open("sqlite", dsn); err != nil {
		return nil, fmt.Errorf("ring sqlite: %w", err)
	}
	r.writeDB.SetMaxOpenConns(1)
	r.writeDB.SetMaxIdleConns(1)
	r.writeDB.SetConnMaxLifetime(0)

	if _, err := r.writeDB.Exec(ringSchema); err != nil {
		r.writeDB.Close()
		return nil, fmt.Errorf("ring sqlite: creating schema: %w", err)
	}
	if err := r.legacyRing(); err != nil {
		r.writeDB.Close()
		return nil, err
	}
	if r.insert, err = r.writeDB.Prepare(ringInsert); err != nil {
		r.writeDB.Close()
		return nil, fmt.Errorf("ring sqlite: preparing insert: %w", err)
	}

	// A separate reader connection: WAL permits one reader concurrent with the
	// writer, which is what keeps the drain from contending with group commits.
	if r.readDB, err = sql.Open("sqlite", dsn); err != nil {
		r.writeDB.Close()
		return nil, fmt.Errorf("ring sqlite: opening the reader: %w", err)
	}
	r.readDB.SetMaxOpenConns(1)
	r.readDB.SetMaxIdleConns(1)

	// A crash leaves rows behind; they are the backlog the boot drain consumes.
	if err := r.recount(); err != nil {
		r.writeDB.Close()
		r.readDB.Close()
		return nil, err
	}

	r.wg.Add(1)
	go r.writerLoop()
	return r, nil
}

func (r *SQLiteRing) Mode() string { return "sqlite" }

// legacyRing deals with the v1 table of SPEC §7.4: empty, it is dropped; with
// rows, it stays exactly as it is -- nothing lost, nothing mixed into the
// LogHub ring -- and an ERROR says how many events wait for the previous
// version to drain them.
func (r *SQLiteRing) legacyRing() error {
	var exists int
	if err := r.writeDB.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ring'`).Scan(&exists); err != nil {
		return fmt.Errorf("ring sqlite: looking for the v1 table: %w", err)
	}
	if exists == 0 {
		return nil
	}
	var n int64
	if err := r.writeDB.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&n); err != nil {
		return fmt.Errorf("ring sqlite: counting the v1 table: %w", err)
	}
	if n == 0 {
		if _, err := r.writeDB.Exec(`DROP TABLE ring`); err != nil {
			return fmt.Errorf("ring sqlite: dropping the empty v1 table: %w", err)
		}
		r.log.Info("ring sqlite: dropped the empty pre-LogHub table", "path", r.cfg.Path)
		return nil
	}
	r.log.Error("ring sqlite: the pre-LogHub table still holds events in the old envelope format; "+
		"they are left untouched and NOT delivered -- drain them with the previous version (SPEC §7.4)",
		"table", "ring", "events", n, "path", r.cfg.Path)
	return nil
}

// recount loads the backlog left by a previous run into the accounting -- or,
// with KeepBacklog, sets it aside.
func (r *SQLiteRing) recount() error {
	var n, b, last sql.NullInt64
	err := r.readDB.QueryRow(`SELECT COUNT(*), COALESCE(SUM(nbytes),0), COALESCE(MAX(rowid_),0)
		FROM jlh_ring`).Scan(&n, &b, &last)
	if err != nil {
		return fmt.Errorf("ring sqlite: counting the backlog: %w", err)
	}
	if r.cfg.KeepBacklog {
		// AUTOINCREMENT gives every later row a larger rowid_ than any the
		// table ever held, so a cursor starting at the last one reads this
		// run's rows and only those. The caller reports the rest (Kept).
		r.cursor = last.Int64
		r.kept = n.Int64
		return nil
	}
	r.items.Store(n.Int64)
	r.bytes.Store(b.Int64)
	if n.Int64 > 0 {
		r.log.Warn("ring backlog recovered from the previous run",
			"items", n.Int64, "bytes", b.Int64, "path", r.cfg.Path)
	}
	return nil
}

// Backlog reports the rows left from a previous run, for the boot drain.
func (r *SQLiteRing) Backlog() int64 { return r.items.Load() }

// Kept reports the rows of a previous run that KeepBacklog set aside. They are
// not in Stats: this run never delivers them.
func (r *SQLiteRing) Kept() int64 { return r.kept }

// Left is what a ring database holds, as ProbeSQLite finds it.
type Left struct {
	// Ring says the file holds a ring: jlh_ring or the v1 table.
	Ring bool
	// Shared says it holds other tables too -- the events.db of a life that
	// ran ring=sqlite with ring.sqlite.path on it -- so the file is somebody
	// else's as well, and never the ring's to remove.
	Shared bool
	// Rows are the events waiting in jlh_ring; Legacy, those in the v1 table,
	// which nothing in this version delivers (SPEC §7.4).
	Rows, Legacy int64
}

// ProbeSQLite reports what the file at path holds as a ring, creating no file
// and no table (SPEC §7.4).
//
// It is for a boot whose ring mode is not sqlite, which must neither make a
// ring.db to learn there is none nor add jlh_ring to a database that is
// somebody else's: the events.db, when ring.sqlite.path and sink.sqlite.path
// coincide under another ring -- NewSQLite would do both.
func ProbeSQLite(path string) (Left, error) {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Left{}, nil
	}
	if err != nil {
		return Left{}, fmt.Errorf("ring sqlite: %w", err)
	}
	if !fi.Mode().IsRegular() {
		// Only a regular file is a ring.db. SQLite refuses the others as well
		// -- a FIFO with a bare "disk I/O error" -- but without saying why.
		return Left{}, fmt.Errorf("ring sqlite: %s is not a regular file", path)
	}

	// mode=rw opens the file or fails; it never creates one. And no
	// journal_mode: on a database that is not a ring it would be a write.
	db, err := sql.Open("sqlite", "file:"+path+"?mode=rw&_pragma=busy_timeout(5000)")
	if err != nil {
		return Left{}, fmt.Errorf("ring sqlite: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	names, err := tableNames(db)
	if err != nil {
		return Left{}, fmt.Errorf("ring sqlite: reading %s: %w", path, err)
	}
	var left Left
	for _, name := range names {
		var rows *int64
		switch {
		case name == "jlh_ring":
			rows = &left.Rows
		case name == "ring":
			rows = &left.Legacy
		case strings.HasPrefix(name, "sqlite_"):
			// SQLite's own: sqlite_sequence for AUTOINCREMENT, sqlite_stat*.
			continue
		default:
			left.Shared = true
			continue
		}
		left.Ring = true
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + name).Scan(rows); err != nil {
			return Left{}, fmt.Errorf("ring sqlite: counting %s in %s: %w", name, path, err)
		}
	}
	return left, nil
}

func tableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// Enqueue reserves room, hands the item to the writer and waits for its group
// COMMIT.
//
// The reservation is what makes the ceilings hold (SPEC §7.4). Compared
// against counters that only grow after the COMMIT, a whole group -- up to
// 2 x commit_batch waiting for the writer, plus the group being built --
// passed the check together, and with large bodies ring.db went past
// ring.max_bytes by gigabytes (until 2026-10-14; port of go-syslog-server).
func (r *SQLiteRing) Enqueue(ctx context.Context, it *envelope.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}

	size := it.Size()
	if !r.reserve(size) {
		return ErrFull
	}

	done := it.Done()
	select {
	case r.pending <- it:
	default:
		// The writer is behind. Refusing here rather than blocking keeps
		// backpressure visible as 429 instead of as growing latency.
		r.unreserve(1, size)
		return ErrFull
	}

	// The result is awaited even if ctx is cancelled underneath. Once the item
	// is with the writer, its INSERT will read Env and Body and its row will be
	// committed whatever the handler does next, so returning early would only
	// make the handler tell the client 503 about a row that is on its way to
	// disk -- and, worse, let it pool buffers the writer is about to read
	// (SPEC §7.4, §12.3). The wait is bounded by the group commit itself:
	// commit_max_delay plus one fsync.
	select {
	case err := <-done:
		return err
	case <-r.writerDone:
		// The ring closed underneath. The writer flushes what it already had
		// before leaving, so the item was either acknowledged on the way out or
		// never picked up; the buffered channel tells which.
		select {
		case err := <-done:
			return err
		default:
			// Never picked up: the writer will not commit it, so the
			// reservation goes back.
			r.unreserve(1, size)
			return ErrClosed
		}
	}
}

// reserve claims room for one item against both ceilings, or refuses. Each Add
// returns a total that includes every concurrent reservation, so two handlers
// can never both take the last slot.
func (r *SQLiteRing) reserve(size int64) bool {
	n := r.resItems.Add(1)
	if r.cfg.CapacityItems > 0 && r.items.Load()+n > int64(r.cfg.CapacityItems) {
		r.resItems.Add(-1)
		return false
	}
	b := r.resBytes.Add(size)
	if r.cfg.CapacityBytes > 0 && r.bytes.Load()+b > r.cfg.CapacityBytes {
		r.resItems.Add(-1)
		r.resBytes.Add(-size)
		return false
	}
	return true
}

func (r *SQLiteRing) unreserve(n, b int64) {
	r.resItems.Add(-n)
	r.resBytes.Add(-b)
}

// writerLoop owns the write connection. It accumulates items and commits either
// when the batch is full or when the delay expires, then releases every waiting
// handler at once.
func (r *SQLiteRing) writerLoop() {
	defer r.wg.Done()
	defer close(r.writerDone)

	batch := make([]*envelope.Item, 0, r.cfg.CommitBatch)
	timer := time.NewTimer(r.cfg.CommitMaxDelay)
	timer.Stop()
	timerRunning := false

	flush := func() {
		if len(batch) == 0 {
			return
		}
		r.commitBatch(batch)
		batch = batch[:0]
		if timerRunning {
			timer.Stop()
			timerRunning = false
		}
	}

	for {
		select {
		case <-r.stop:
			// Drain anything still queued so no handler waits forever, and
			// apply the DELETEs of batches the sink already confirmed. Left
			// in the channel -- the select picks stop or deletes at random
			// when both are ready -- those rows came back at the next boot
			// and were delivered twice (TESTING.md §8 item 40), and a ring.db
			// the boot drained but must keep kept them (SPEC §7.4).
			for {
				select {
				case it := <-r.pending:
					batch = append(batch, it)
					continue
				case ids := <-r.deletes:
					r.deleteRows(ids)
					continue
				default:
				}
				break
			}
			flush()
			return

		case ids := <-r.deletes:
			r.deleteRows(ids)

		case it := <-r.pending:
			batch = append(batch, it)
			if len(batch) >= r.cfg.CommitBatch {
				flush()
				continue
			}
			if !timerRunning {
				timer.Reset(r.cfg.CommitMaxDelay)
				timerRunning = true
			}

		case <-timer.C:
			timerRunning = false
			flush()
		}
	}
}

// commitBatch inserts the batch in one transaction and acknowledges every
// waiting handler with the same result.
func (r *SQLiteRing) commitBatch(batch []*envelope.Item) {
	ctx := context.Background()

	// What the batch reserved in Enqueue, read before anything can release a
	// buffer: Size is len(Env), and Release empties Env.
	var n, b int64
	for _, it := range batch {
		n++
		b += it.Size()
	}

	tx, err := r.writeDB.BeginTx(ctx, nil)
	if err != nil {
		r.unreserve(n, b)
		r.ackAll(batch, fmt.Errorf("ring sqlite: begin: %w", err))
		return
	}
	stmt := tx.StmtContext(ctx, r.insert)

	ids := make([]int64, len(batch))
	for i, it := range batch {
		res, err := stmt.ExecContext(ctx, it.ID, it.Env, it.Size())
		if err != nil {
			_ = tx.Rollback()
			r.unreserve(n, b)
			r.ackAll(batch, fmt.Errorf("ring sqlite: insert: %w", err))
			return
		}
		ids[i], _ = res.LastInsertId()
	}

	// This is where the fsync happens with synchronous=FULL, and it is the
	// reason a handler's 200 is bound to the disk in this mode.
	if err := tx.Commit(); err != nil {
		r.unreserve(n, b)
		r.ackAll(batch, fmt.Errorf("ring sqlite: commit: %w", err))
		return
	}

	for i, it := range batch {
		it.RowID = ids[i]
	}
	// Committed first, reservation released second: the sum the ceilings see
	// may briefly count the batch twice -- refusing a little early -- but can
	// never count it zero times and admit past the ceiling.
	r.items.Add(n)
	r.bytes.Add(b)
	r.unreserve(n, b)

	// The row is the event now: the workers read it back from ring.db into
	// items of their own, and the handler only needs its id and the body
	// length once acknowledged. So the JLH goes back to its pool here, BEFORE
	// the ack, which orders it ahead of anything the handler does next (SPEC
	// §12.3). Left to the GC, every request of ring=sqlite took a fresh
	// buffer from New and the pool never refilled.
	for _, it := range batch {
		it.Release()
	}

	// Only now is the event durable, so only now may a handler answer 200.
	r.ackAll(batch, nil)
}

func (r *SQLiteRing) ackAll(batch []*envelope.Item, err error) {
	for _, it := range batch {
		it.Ack(err)
	}
}

// Dequeue reads the oldest rows that are not already with a worker.
func (r *SQLiteRing) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	if n < 1 {
		n = 1
	}
	for {
		items, err := r.selectRows(ctx, n)
		if len(items) > 0 {
			// A read that failed part way serves what it read: those rows are
			// already behind the cursor, and dropping them here stranded them
			// until the next boot (SPEC §7.4). The failing row was not passed,
			// so the next call meets it again.
			return items, nil
		}
		if err != nil {
			return nil, err
		}
		// Nothing available: wait briefly rather than spin. The commit delay is
		// the natural cadence, since that is how fast new rows can appear.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.stop:
			return nil, ErrClosed
		case <-time.After(r.cfg.CommitMaxDelay):
		}
	}
}

func (r *SQLiteRing) selectRows(ctx context.Context, n int) ([]*envelope.Item, error) {
	// The cursor is held across the query so two workers cannot read the same
	// range. The query is indexed on the primary key and bounded by LIMIT, so
	// the critical section is short.
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()

	rows, err := r.readDB.QueryContext(ctx, ringSelect, r.cursor, n)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("ring sqlite: select: %w", err)
	}
	defer rows.Close()

	var out []*envelope.Item
	for rows.Next() && len(out) < n {
		var (
			rowID int64
			id    string
			env   []byte
		)
		if err := rows.Scan(&rowID, &id, &env); err != nil {
			// The cursor has not moved past this row: Dequeue serves the
			// items already built, and the row is met again next time.
			return out, fmt.Errorf("ring sqlite: scan: %w", err)
		}
		if rowID > r.cursor {
			r.cursor = rowID
		}

		// Reconstructed in full: the item leaving the ring is the item that
		// entered it, uuid and envelope.
		it := envelope.NewItem(id, env, -1)
		it.RowID = rowID
		out = append(out, it)
	}
	return out, rows.Err()
}

// Commit deletes the delivered rows.
//
// The DELETE is handed to the writer goroutine rather than executed here: it is
// a write, and only one goroutine may write.
func (r *SQLiteRing) Commit(items []*envelope.Item) error {
	if len(items) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(items))
	var n, b int64
	for _, it := range items {
		if it.RowID != 0 {
			ids = append(ids, it.RowID)
		}
		n++
		b += it.Size()
	}
	r.items.Add(-n)
	r.bytes.Add(-b)

	select {
	case r.deletes <- ids:
	case <-r.stop:
		return nil
	}
	return nil
}

func (r *SQLiteRing) deleteRows(ids []int64) {
	if len(ids) == 0 {
		return
	}
	tx, err := r.writeDB.Begin()
	if err != nil {
		r.log.Error("ring sqlite: cannot begin a delete; rows will be redelivered", "error", err)
		return
	}
	for _, id := range ids {
		if _, err := tx.Exec(`DELETE FROM jlh_ring WHERE rowid_ = ?`, id); err != nil {
			_ = tx.Rollback()
			r.log.Error("ring sqlite: delete failed; rows will be redelivered", "error", err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		r.log.Error("ring sqlite: delete commit failed; rows will be redelivered", "error", err)
	}
}

// Drain returns nothing: the items are already on disk and will be picked up by
// the next boot's backlog drain, which is a stronger guarantee than an
// emergency dump (SPEC §9).
func (r *SQLiteRing) Drain() []*envelope.Item { return nil }

// Stats counts committed rows plus the reservations on their way to the next
// group commit: what the ceilings are checked against (SPEC §7.4).
func (r *SQLiteRing) Stats() Stats {
	return Stats{
		Items:    r.items.Load() + r.resItems.Load(),
		Bytes:    r.bytes.Load() + r.resBytes.Load(),
		CapItems: int64(r.cfg.CapacityItems),
		CapBytes: r.cfg.CapacityBytes,
	}
}

func (r *SQLiteRing) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	close(r.stop)
	r.wg.Wait()

	if r.insert != nil {
		r.insert.Close()
	}
	var errs []error
	if r.writeDB != nil {
		errs = append(errs, r.writeDB.Close())
	}
	if r.readDB != nil {
		errs = append(errs, r.readDB.Close())
	}
	return errors.Join(errs...)
}
