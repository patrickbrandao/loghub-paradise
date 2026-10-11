package ring

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"

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

	// Quarantined, when set, is told how many rows were moved to the
	// quarantine table (SPEC §9.4). It is a callback rather than a metrics
	// handle so this package stays free of the obs package.
	Quarantined func(n int)
}

// SQLiteRing is the persistent ring (SPEC §9.4), for maximum retention even at
// the cost of throughput.
//
// Enqueue returns only after the group COMMIT that includes the item has
// finished, so the confirmation means the message is on disk. With
// synchronous=FULL that survives power loss; with NORMAL it survives a process
// crash but not a kernel one.
//
// It only earns its cost on TCP or RELP. On UDP, spending an fsync to guarantee
// a message the sender's kernel may already have dropped is paying a lot for a
// guarantee the transport does not offer, and the throughput it costs is
// throughput that would have let more datagrams arrive. The boot warns about it.
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

	// pending carries items from receivers to the writer goroutine.
	pending chan *envelope.Item
	// deletes carries the ids of delivered batches back to the writer.
	deletes chan []int64
	// quarantine carries the rows the reader could not decode to the writer,
	// which moves them out of the drain's way.
	quarantine chan quarantineReq

	items atomic.Int64
	bytes atomic.Int64

	// cursor is the highest seq handed to a worker. Reads resume strictly after
	// it, which is what guarantees a row is served at most once.
	//
	// An earlier version of the sibling service filtered an over-fetched window
	// through an in-memory "in flight" set instead. That deadlocks: once the
	// window is entirely in flight the same rows come back every time, the
	// filter empties the result, and the drain stalls with rows still
	// undelivered. At the default workers=8 x batch=256 the in-flight set
	// exceeds any fixed window, so the stall was not a corner case -- it was
	// the normal configuration.
	//
	// A cursor cannot stall: it only moves forward. Rows a worker abandons are
	// left behind it, which is correct, because abandonment only happens on the
	// paths that end the process, and the next boot starts the cursor at zero.
	cursorMu sync.Mutex
	cursor   int64

	closed atomic.Bool
	wg     sync.WaitGroup
	stop   chan struct{}
	// writerDone is closed when the writer goroutine has returned, so a
	// receiver that handed an item over just as the ring was closing can tell
	// "acknowledged on the way out" from "never picked up".
	writerDone chan struct{}
}

// The ring table stores head + raw line, which is the whole item (SPEC §3.2).
// There is nothing else to store: every other envelope field is derived from
// those two deterministically, in the drain worker.
//
// The id has its own column even though the head already carries it. That is
// deliberate: losing an item's identity on the way back out is how the sibling
// service once discarded 149 of 150 events to a primary-key collision at the
// destination. A separate column makes identity verifiable with a plain SELECT,
// without decoding anything.
//
// nbytes is len(raw) and NOT len(head)+len(raw), so that the boot recount
// produces exactly the number the in-memory accounting would (SPEC §9.4).
// Including the head would make the byte ceiling behave one way before a crash
// and another way after it.
const ringSchema = `
CREATE TABLE IF NOT EXISTS ring (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,
  id     TEXT    NOT NULL,
  head   BLOB    NOT NULL,
  raw    BLOB    NOT NULL,
  nbytes INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS quarantine (
  seq    INTEGER PRIMARY KEY,
  id     TEXT    NOT NULL,
  head   BLOB    NOT NULL,
  raw    BLOB    NOT NULL,
  nbytes INTEGER NOT NULL,
  reason TEXT    NOT NULL,
  ts     INTEGER NOT NULL
) STRICT;
`

const ringInsert = `INSERT INTO ring (id, head, raw, nbytes) VALUES (?,?,?,?)`

const ringSelect = `SELECT seq, id, head, raw, nbytes FROM ring WHERE seq > ? ORDER BY seq LIMIT ?`

// A row whose head does not decode is moved, not deleted and not left behind.
//
// Left in ring it would be counted at every boot and never delivered, and
// before the accounting fix that meant a boot drain waiting forever for a
// counter that never reached zero, every transport closed. Deleted, it would
// be an accepted message destroyed on this program's initiative. The
// quarantine table keeps the bytes for a human, in the same file, out of the
// drain's way (SPEC §9.4).
const ringQuarantine = `INSERT INTO quarantine (seq, id, head, raw, nbytes, reason, ts)
  SELECT seq, id, head, raw, nbytes, ?, ? FROM ring WHERE seq = ?`

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
		quarantine: make(chan quarantineReq, 64),
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
	if err := r.Recount(); err != nil {
		r.writeDB.Close()
		r.readDB.Close()
		return nil, err
	}

	r.wg.Add(1)
	go r.writerLoop()
	return r, nil
}

func (r *SQLiteRing) Mode() string { return "sqlite" }

// Recount reloads the accounting from the table: at boot, for the backlog a
// previous run left behind, and from the boot drain's no-progress guard, when
// the counters and the table are suspected to disagree.
func (r *SQLiteRing) Recount() error {
	var n, b sql.NullInt64
	err := r.readDB.QueryRow(`SELECT COUNT(*), COALESCE(SUM(nbytes),0) FROM ring`).Scan(&n, &b)
	if err != nil {
		return fmt.Errorf("ring sqlite: counting the backlog: %w", err)
	}
	r.items.Store(n.Int64)
	r.bytes.Store(b.Int64)
	if n.Int64 > 0 {
		r.log.Warn("ring backlog counted from ring.db",
			"items", n.Int64, "bytes", b.Int64, "path", r.cfg.Path)
	}
	return nil
}

// Backlog reports the rows left from a previous run, for the boot drain.
func (r *SQLiteRing) Backlog() int64 { return r.items.Load() }

// Enqueue hands the item to the writer and waits for its group COMMIT.
func (r *SQLiteRing) Enqueue(ctx context.Context, it *envelope.Item) error {
	if r.closed.Load() {
		return ErrClosed
	}

	size := it.Size()
	if r.cfg.CapacityBytes > 0 && r.bytes.Load()+size > r.cfg.CapacityBytes {
		return ErrFull
	}
	if r.cfg.CapacityItems > 0 && r.items.Load() >= int64(r.cfg.CapacityItems) {
		return ErrFull
	}

	done := it.Done()
	select {
	case r.pending <- it:
	default:
		// The writer is behind. Refusing here rather than blocking keeps
		// backpressure visible through the transport instead of as growing
		// latency inside this process.
		return ErrFull
	}

	// The result is awaited even if ctx is cancelled underneath. Once the item
	// is with the writer, its INSERT will read Raw and its row will be
	// committed whatever the receiver does next, so returning early would only
	// make RELP answer "500 shutdown" -- a guaranteed duplicate on
	// retransmission -- for a row that is on its way to disk, and, worse, let
	// the receiver pool a buffer the writer is about to read (SPEC §9.4,
	// §14.2). The wait is bounded by the group commit itself: commit_max_delay
	// plus one fsync.
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
			return ErrClosed
		}
	}
}

// writerLoop owns the write connection. It accumulates items and commits either
// when the batch is full or when the delay expires, then releases every waiting
// receiver at once.
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
			// Drain anything still queued so no receiver waits forever, and
			// finish the quarantine moves already decided: a row left behind
			// would be re-quarantined -- and re-counted -- by the next boot.
			for {
				select {
				case it := <-r.pending:
					batch = append(batch, it)
					continue
				case q := <-r.quarantine:
					r.quarantineRow(q)
					continue
				default:
				}
				break
			}
			flush()
			return

		case ids := <-r.deletes:
			r.deleteRows(ids)

		case q := <-r.quarantine:
			r.quarantineRow(q)

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
// waiting receiver with the same result.
func (r *SQLiteRing) commitBatch(batch []*envelope.Item) {
	ctx := context.Background()

	tx, err := r.writeDB.BeginTx(ctx, nil)
	if err != nil {
		r.ackAll(batch, fmt.Errorf("ring sqlite: begin: %w", err))
		return
	}
	stmt := tx.StmtContext(ctx, r.insert)

	// One head buffer, reused across the batch. Each Exec copies the bytes into
	// the driver before the next iteration overwrites them.
	head := make([]byte, 0, 256)

	ids := make([]int64, len(batch))
	for i, it := range batch {
		head = envelope.EncodeHead(head[:0], &it.Head)
		res, err := stmt.ExecContext(ctx, it.Head.ID, head, it.Raw, it.Size())
		if err != nil {
			_ = tx.Rollback()
			r.ackAll(batch, fmt.Errorf("ring sqlite: insert: %w", err))
			return
		}
		ids[i], _ = res.LastInsertId()
	}

	// This is where the fsync happens with synchronous=FULL, and it is the
	// reason a confirmation is bound to the disk in this mode.
	if err := tx.Commit(); err != nil {
		r.ackAll(batch, fmt.Errorf("ring sqlite: commit: %w", err))
		return
	}

	var n, b int64
	for i, it := range batch {
		it.RowID = ids[i]
		n++
		b += it.Size()
	}
	r.items.Add(n)
	r.bytes.Add(b)

	// Only now is the message durable, so only now may a transport confirm.
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
		items, skipped, err := r.selectRows(ctx, n)
		if err != nil {
			return nil, err
		}
		if len(items) > 0 || skipped > 0 {
			// An empty result after a quarantine is still progress: the caller
			// re-reads the accounting and, in the boot drain, sees the backlog
			// reach zero instead of waiting out a timeout for rows that are
			// no longer in the table.
			return items, nil
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

// selectRows reads the next window after the cursor. skipped counts the rows
// that were quarantined instead of returned.
func (r *SQLiteRing) selectRows(ctx context.Context, n int) (out []*envelope.Item, skipped int, err error) {
	// The cursor is held across the query so two workers cannot read the same
	// range. The query is indexed on the primary key and bounded by LIMIT, so
	// the critical section is short.
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()

	rows, err := r.readDB.QueryContext(ctx, ringSelect, r.cursor, n)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("ring sqlite: select: %w", err)
	}
	defer rows.Close()

	for rows.Next() && len(out) < n {
		var (
			seq    int64
			id     string
			head   []byte
			raw    []byte
			nbytes int64
		)
		if err := rows.Scan(&seq, &id, &head, &raw, &nbytes); err != nil {
			return out, skipped, fmt.Errorf("ring sqlite: scan: %w", err)
		}
		if seq > r.cursor {
			r.cursor = seq
		}

		h, err := envelope.DecodeHead(head)
		if err != nil {
			// The row is unusable as an item but it is real, committed data:
			// something told an emitter we had it. It leaves the accounting
			// NOW -- a row that is counted but never delivered is a boot drain
			// that waits forever -- and the writer moves it to the quarantine
			// table, where the bytes stay for inspection (SPEC §9.4).
			r.log.Error("ring sqlite: undecodable item head; row moved to the quarantine table",
				"seq", seq, "id", id, "error", err)
			r.items.Add(-1)
			r.bytes.Add(-nbytes)
			if r.cfg.Quarantined != nil {
				r.cfg.Quarantined(1)
			}
			select {
			case r.quarantine <- quarantineReq{seq: seq, reason: err.Error()}:
			case <-r.stop:
			}
			skipped++
			continue
		}
		// The head carries the id already; the column is the cross-check.
		if h.ID != id {
			r.log.Warn("ring sqlite: id column disagrees with the item head; trusting the column",
				"seq", seq, "column", id, "head", h.ID)
			h.ID = id
		}

		// Reconstructed in full: the item leaving the ring is the item that
		// entered it. The buffer came from the driver, not from a pool, so its
		// class is -1 and Release must never file it anywhere.
		it := envelope.NewItem(h, raw, -1)
		it.RowID = seq
		out = append(out, it)
	}
	return out, skipped, rows.Err()
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
		if _, err := tx.Exec(`DELETE FROM ring WHERE seq = ?`, id); err != nil {
			_ = tx.Rollback()
			r.log.Error("ring sqlite: delete failed; rows will be redelivered", "error", err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		r.log.Error("ring sqlite: delete commit failed; rows will be redelivered", "error", err)
	}
}

// quarantineReq is one undecodable row on its way to the quarantine table.
type quarantineReq struct {
	seq    int64
	reason string
}

// quarantineRow moves one row from ring to quarantine in a single transaction,
// so the row is in exactly one of the two tables at any instant.
func (r *SQLiteRing) quarantineRow(q quarantineReq) {
	tx, err := r.writeDB.Begin()
	if err != nil {
		r.log.Error("ring sqlite: cannot begin the quarantine; the row stays in ring",
			"seq", q.seq, "error", err)
		return
	}
	if _, err := tx.Exec(ringQuarantine, q.reason, time.Now().UnixNano(), q.seq); err != nil {
		_ = tx.Rollback()
		r.log.Error("ring sqlite: cannot copy the row to quarantine; it stays in ring",
			"seq", q.seq, "error", err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM ring WHERE seq = ?`, q.seq); err != nil {
		_ = tx.Rollback()
		r.log.Error("ring sqlite: cannot remove the quarantined row from ring",
			"seq", q.seq, "error", err)
		return
	}
	if err := tx.Commit(); err != nil {
		r.log.Error("ring sqlite: quarantine commit failed; the row stays in ring",
			"seq", q.seq, "error", err)
	}
}

// Drain returns nothing: the items are already on disk and will be picked up by
// the next boot's backlog drain, which is a stronger guarantee than an
// emergency dump (SPEC §11).
func (r *SQLiteRing) Drain() []*envelope.Item { return nil }

func (r *SQLiteRing) Stats() Stats {
	return Stats{
		Items:    r.items.Load(),
		Bytes:    r.bytes.Load(),
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
