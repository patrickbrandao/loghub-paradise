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

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"

	_ "modernc.org/sqlite"
)

// SQLiteConfig mirrors [ring.sqlite] plus the two [ring] ceilings.
type SQLiteConfig struct {
	Path              string
	Synchronous       string // FULL | NORMAL | OFF
	CommitBatch       int
	CommitMaxDelay    time.Duration
	BusyTimeout       time.Duration
	WALAutocheckpoint int

	CapacityItems int
	CapacityBytes int64

	// Metrics, when set, receives relplog_ring_quarantined_total (SPEC §9.4,
	// §14.1). Nil is allowed, for tests and tools.
	Metrics *obs.Metrics
}

// SQLiteRing is the persistent ring (SPEC §9.4), for maximum retention even at
// the cost of throughput.
//
// Enqueue returns only after the group COMMIT that includes the item has
// finished, so the confirmation means the message is on disk (SPEC §2.3). With
// synchronous=FULL that survives power loss; with NORMAL it survives a process
// crash but not a kernel one.
//
// It only earns its cost on TCP or TLS. On UDP, spending an fsync to guarantee
// a message the sender's kernel may already have dropped is paying a lot for a
// guarantee the transport does not offer, and the throughput it costs is
// throughput that would have let more datagrams arrive. The boot warns about it
// (SPEC §9.7).
//
// Exactly TWO goroutines touch the database:
//
//   - the writer owns the write connection, batches INSERTs into group commits
//     and performs the DELETEs of already-delivered batches and the moves to
//     the quarantine table;
//   - the reader owns a separate connection (WAL allows a reader concurrent
//     with a writer) and feeds the drain workers.
//
// More writers would only produce SQLITE_BUSY: SQLite serialises them by
// construction. A claim column was rejected for doubling the writes (an UPDATE
// plus a DELETE per item) on a path where every COMMIT costs an fsync.
type SQLiteRing struct {
	cfg SQLiteConfig
	log *slog.Logger
	m   *obs.Metrics

	writeDB *sql.DB
	readDB  *sql.DB
	insert  *sql.Stmt

	// pending carries items from receivers to the writer goroutine.
	pending chan *item.Item
	// deletes carries the seqs of delivered batches back to the writer.
	deletes chan []int64
	// quarantine carries the rows the reader could not decode to the writer,
	// which moves them out of the drain's way.
	quarantine chan quarantineReq

	// items and bytes count the rows COMMITTED in ring.db: exactly what
	// Recount measures with COUNT(*) and SUM(nbytes).
	items atomic.Int64
	bytes atomic.Int64

	// resItems and resBytes are reservations: items an Enqueue has admitted
	// and handed to the writer whose group commit has not finished. The
	// ceilings are checked against committed + reserved, and a reservation is
	// taken BEFORE the hand-off, atomically, like the memory ring does.
	// Checking the committed counters alone let every receiver that arrived
	// within one commit_max_delay through: they all saw the same pre-commit
	// numbers, and 200 concurrent Enqueues passed a ceiling of 10.
	//
	// Keeping the reservations apart is what keeps Recount exact: it replaces
	// the committed counters with what the table holds and leaves in-flight
	// reservations alone, which the commit then moves across or undoes.
	resItems atomic.Int64
	resBytes atomic.Int64

	// down is set by a failed group commit and cleared by the next one that
	// succeeds; Available reports it to /readyz.
	down atomic.Bool

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

// The ring table stores head + raw line, which is the whole item (SPEC §2.2,
// §3.4). There is nothing else to store: every other envelope field is derived
// from those two deterministically, in the drain worker (SPEC §3.5).
//
// The uuid has its own column even though the head already carries it. That is
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
  uuid   TEXT    NOT NULL,
  head   BLOB    NOT NULL,
  raw    BLOB    NOT NULL,
  nbytes INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS quarantine (
  seq    INTEGER PRIMARY KEY,
  uuid   TEXT    NOT NULL,
  head   BLOB    NOT NULL,
  raw    BLOB    NOT NULL,
  nbytes INTEGER NOT NULL,
  reason TEXT    NOT NULL,
  ts     INTEGER NOT NULL
) STRICT;
`

const ringInsert = `INSERT INTO ring (uuid, head, raw, nbytes) VALUES (?,?,?,?)`

const ringSelect = `SELECT seq, uuid, head, raw, nbytes FROM ring WHERE seq > ? ORDER BY seq LIMIT ?`

// A row whose head does not decode is moved, not deleted and not left behind.
//
// Left in ring it would be counted at every boot and never delivered, and
// before the accounting fix that meant a boot drain waiting forever for a
// counter that never reached zero, every transport closed. Deleted, it would
// be an accepted message destroyed on this program's initiative. The
// quarantine table keeps the bytes for a human, in the same file, out of the
// drain's way (SPEC §9.4). There is no ceiling and no automatic cleanup: the
// operator inspects and empties it with the process stopped.
const ringQuarantine = `INSERT INTO quarantine (seq, uuid, head, raw, nbytes, reason, ts)
  SELECT seq, uuid, head, raw, nbytes, ?, ? FROM ring WHERE seq = ?`

// NewSQLite opens the ring database, counts the backlog a previous run left
// behind and starts the writer goroutine (SPEC §13.1 step 8).
func NewSQLite(cfg SQLiteConfig, log *slog.Logger) (*SQLiteRing, error) {
	if cfg.Path == "" {
		return nil, errors.New("ring sqlite: empty path")
	}
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
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.Path), 0o750); err != nil {
		return nil, fmt.Errorf("ring sqlite: %w", err)
	}

	r := &SQLiteRing{
		cfg:        cfg,
		log:        log,
		m:          cfg.Metrics,
		pending:    make(chan *item.Item, cfg.CommitBatch*2),
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
		r.insert.Close()
		r.writeDB.Close()
		return nil, fmt.Errorf("ring sqlite: opening the reader: %w", err)
	}
	r.readDB.SetMaxOpenConns(1)
	r.readDB.SetMaxIdleConns(1)

	// A crash leaves rows behind; they are the backlog the boot drain consumes
	// before any transport opens (SPEC §13.1 step 10).
	if err := r.Recount(); err != nil {
		r.insert.Close()
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
// previous run left behind, and from the boot drain's no-progress guard
// (SPEC §9.4), when the counters and the table are suspected to disagree.
//
// The guard is the app's: a Dequeue that finds nothing for 5 s while Stats
// still reports items calls Recount once; if the accounting is still above zero
// and the next wait again yields nothing, the boot exits with 3 rather than
// keeping every transport closed forever with no exit code for a supervisor.
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

// Backlog reports the committed rows the accounting holds, for the boot drain:
// right after NewSQLite it is what a previous run left behind.
func (r *SQLiteRing) Backlog() int64 { return r.items.Load() }

// Enqueue reserves room, hands the item to the writer and waits for its group
// COMMIT.
//
// A failed commit (disk full, an I/O error, the lock held past busy_timeout)
// comes back wrapping ErrUnavailable: the ring's own storage refused, which is
// what that error means, and the receiver counts it as ring_unavailable and
// keeps the transport's backpressure -- not sink_down, which would send an
// operator to look at the wrong system (SPEC §2.3, §15.2).
func (r *SQLiteRing) Enqueue(ctx context.Context, it *item.Item) error {
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
		// backpressure visible through the transport instead of as growing
		// latency inside this process.
		r.unreserve(1, size)
		return ErrFull
	}

	// The result is awaited even if ctx is cancelled underneath. Once the item
	// is with the writer, its INSERT will read Raw and its row will be
	// committed whatever the receiver does next, so returning early would only
	// make the receiver count as lost a row that is on its way to disk, and,
	// worse, let it pool a buffer the writer is about to read (SPEC §9.4,
	// §13.4, §15.2). The wait is bounded by the group commit itself:
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
// returns a total that includes every concurrent reservation, so two
// receivers can never both take the last slot.
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

// Available reports whether the last group commit succeeded. /readyz fails
// while it is false: the receivers are getting ErrUnavailable, and a load
// balancer should send the traffic elsewhere. It turns true again with the
// next commit that succeeds -- the next Enqueue is the probe.
func (r *SQLiteRing) Available() bool { return !r.down.Load() }

// writerLoop owns the write connection. It accumulates items and commits either
// when the batch is full or when the delay expires, then releases every waiting
// receiver at once.
func (r *SQLiteRing) writerLoop() {
	defer r.wg.Done()
	defer close(r.writerDone)

	batch := make([]*item.Item, 0, r.cfg.CommitBatch)
	timer := time.NewTimer(r.cfg.CommitMaxDelay)
	timer.Stop()
	timerRunning := false

	flush := func() {
		if len(batch) == 0 {
			return
		}
		r.commitBatch(batch)
		clear(batch) // drop the references, so delivered items can be collected
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
			// finish the quarantine moves and deletes already decided: a row
			// left behind would be re-quarantined -- or redelivered -- by the
			// next boot.
			for {
				select {
				case it := <-r.pending:
					batch = append(batch, it)
					continue
				case q := <-r.quarantine:
					r.quarantineRow(q)
					continue
				case seqs := <-r.deletes:
					r.deleteRows(seqs)
					continue
				default:
				}
				break
			}
			flush()
			return

		case seqs := <-r.deletes:
			r.deleteRows(seqs)

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
//
// The reservations of the batch are settled BEFORE the acknowledgement, so a
// receiver that returns from Enqueue sees accounting that already includes --
// or no longer reserves -- its item.
func (r *SQLiteRing) commitBatch(batch []*item.Item) {
	ctx := context.Background()

	var n, b int64
	for _, it := range batch {
		n++
		b += it.Size()
	}

	tx, err := r.writeDB.BeginTx(ctx, nil)
	if err != nil {
		r.failBatch(batch, n, b, "begin", err)
		return
	}
	stmt := tx.StmtContext(ctx, r.insert)

	// One head buffer, reused across the batch. Each Exec copies the bytes into
	// the driver before the next iteration overwrites them.
	head := make([]byte, 0, 512)

	seqs := make([]int64, len(batch))
	for i, it := range batch {
		head = item.EncodeHead(head[:0], &it.Head)
		res, err := stmt.ExecContext(ctx, it.Head.UUID.String(), head, it.Raw, it.Size())
		if err != nil {
			_ = tx.Rollback()
			r.failBatch(batch, n, b, "insert", err)
			return
		}
		seqs[i], _ = res.LastInsertId()
	}

	// This is where the fsync happens with synchronous=FULL, and it is the
	// reason a confirmation is bound to the disk in this mode.
	if err := tx.Commit(); err != nil {
		// database/sql hands the connection back after a failed Commit, but
		// SQLite may keep the transaction open (a COMMIT refused with BUSY or
		// by a deferred constraint does). modernc.org/sqlite v1.58 rolls it
		// back itself; this ROLLBACK is for a driver that does not. The pool
		// holds exactly one connection, so it lands on the right one, and
		// without it every later BEGIN would fail with "cannot start a
		// transaction within a transaction": the ring would stay unavailable
		// after the disk recovered. With no transaction open it is a harmless
		// error. TestSQLiteFailedCommitIsUnavailableAndRecovers pins the
		// outcome.
		_, _ = r.writeDB.Exec("ROLLBACK")
		r.failBatch(batch, n, b, "commit", err)
		return
	}

	for i, it := range batch {
		it.RowID = seqs[i]
	}
	// Committed first, reservation released second: the sum the ceilings see
	// may briefly count the batch twice -- refusing a little early -- but can
	// never count it zero times and admit past the ceiling.
	r.items.Add(n)
	r.bytes.Add(b)
	r.unreserve(n, b)
	r.down.Store(false)

	// Only now is the message durable, so only now may a transport confirm.
	r.ackAll(batch, nil)
}

// failBatch undoes the reservations of a batch that did not commit, marks the
// ring unavailable and tells every waiting receiver.
func (r *SQLiteRing) failBatch(batch []*item.Item, n, b int64, op string, err error) {
	r.unreserve(n, b)
	if !r.down.Swap(true) {
		r.log.Error("ring sqlite: group commit failed; receivers get ring_unavailable until a commit succeeds",
			"op", op, "items", n, "path", r.cfg.Path, "error", err)
	}
	r.ackAll(batch, fmt.Errorf("ring sqlite: %s: %w: %w", op, ErrUnavailable, err))
}

func (r *SQLiteRing) ackAll(batch []*item.Item, err error) {
	for _, it := range batch {
		it.Ack(err)
	}
}

// Dequeue reads the oldest rows that are not already with a worker.
func (r *SQLiteRing) Dequeue(ctx context.Context, n int) ([]*item.Item, error) {
	if n < 1 {
		n = 1
	}
	for {
		if r.closed.Load() {
			return nil, ErrClosed
		}
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
		t := time.NewTimer(r.cfg.CommitMaxDelay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-r.stop:
			t.Stop()
			return nil, ErrClosed
		case <-t.C:
		}
	}
}

// selectRows reads the next window after the cursor. skipped counts the rows
// that were quarantined instead of returned.
func (r *SQLiteRing) selectRows(ctx context.Context, n int) (out []*item.Item, skipped int, err error) {
	// The cursor is held across the query so two workers cannot read the same
	// range. The query is indexed on the primary key and bounded by LIMIT, so
	// the critical section is short.
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()

	// Close takes this same lock before closing the connections, so checking
	// here -- under the lock -- is what keeps a worker from querying a closed
	// database.
	if r.closed.Load() {
		return nil, 0, ErrClosed
	}

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
			col    string
			head   sql.RawBytes
			raw    sql.RawBytes
			nbytes int64
		)
		// RawBytes point into the driver's row and are only valid until the
		// next Next: the head is decoded in place (DecodeHead copies what it
		// keeps into strings) and the raw line is copied ONCE, straight into a
		// pooled buffer that goes back to the pool after the sink confirms
		// (SPEC §15.1, §15.2). Scanning into []byte would copy it a second time
		// for nothing.
		if err := rows.Scan(&seq, &col, &head, &raw, &nbytes); err != nil {
			// The cursor has not moved past this row, so the items already
			// built are served and the failing row is met again on the next
			// call. Discarding them instead would strand them behind the
			// cursor until the next boot.
			if len(out) > 0 {
				return out, skipped, nil
			}
			return nil, skipped, fmt.Errorf("ring sqlite: scan: %w", err)
		}
		if seq > r.cursor {
			r.cursor = seq
		}

		h, err := item.DecodeHead(head)
		if err != nil {
			r.quarantineDecided(seq, col, nbytes, err.Error())
			skipped++
			continue
		}
		// The head carries the uuid already; the column is the cross-check.
		// When the two disagree the row is corrupt in a way nobody can resolve
		// from here; the column wins when it is itself a valid UUIDv7, because
		// it is what an operator can SELECT and what the boot of the sibling
		// service was taught to trust.
		if h.UUID.String() != col {
			if u, perr := uuidv7.FromString(col); perr == nil && u.IsValid() {
				r.log.Warn("ring sqlite: uuid column disagrees with the item head; trusting the column",
					"seq", seq, "column", col, "head", h.UUID.String())
				h.UUID = u
			} else {
				r.log.Warn("ring sqlite: uuid column is not a UUIDv7; trusting the item head",
					"seq", seq, "head", h.UUID.String())
			}
		}

		// Reconstructed in full: the item leaving the ring is the item that
		// entered it.
		buf, class := item.CopyRaw(raw)
		it := item.New(h, buf, class)
		it.RowID = seq
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		if len(out) > 0 {
			// Rows already past the cursor are served rather than abandoned
			// until the next boot.
			return out, skipped, nil
		}
		return nil, skipped, fmt.Errorf("ring sqlite: rows: %w", err)
	}
	return out, skipped, nil
}

// quarantineDecided takes an undecodable row out of the accounting and hands
// the move to the writer.
//
// The row is unusable as an item but it is real, committed data: something
// told an emitter we had it. It leaves the accounting NOW -- a row that is
// counted but never delivered is a boot drain that waits forever -- and the
// writer moves it to the quarantine table, where the bytes stay for inspection
// (SPEC §9.4).
func (r *SQLiteRing) quarantineDecided(seq int64, uuid string, nbytes int64, reason string) {
	r.log.Error("ring sqlite: undecodable item head; row moved to the quarantine table",
		"seq", seq, "uuid", uuid, "error", reason)
	r.items.Add(-1)
	r.bytes.Add(-nbytes)
	if r.m != nil {
		r.m.RingQuarantinedTotal.Inc()
	}
	select {
	case r.quarantine <- quarantineReq{seq: seq, reason: reason}:
	case <-r.stop:
	}
}

// Commit deletes the delivered rows.
//
// The DELETE is handed to the writer goroutine rather than executed here: it is
// a write, and only one goroutine may write. The accounting drops at once, so
// the rows outlive it for a moment; a crash in that window redelivers them,
// which is the correct at-least-once bias, since every destination
// deduplicates on (uuid, step) (SPEC §10.2).
func (r *SQLiteRing) Commit(items []*item.Item) error {
	if len(items) == 0 {
		return nil
	}
	seqs := make([]int64, 0, len(items))
	var n, b int64
	for _, it := range items {
		if it.RowID != 0 {
			seqs = append(seqs, it.RowID)
		}
		n++
		b += it.Size()
	}
	r.items.Add(-n)
	r.bytes.Add(-b)

	select {
	case r.deletes <- seqs:
	case <-r.stop:
		return nil
	}
	return nil
}

func (r *SQLiteRing) deleteRows(seqs []int64) {
	if len(seqs) == 0 {
		return
	}
	tx, err := r.writeDB.Begin()
	if err != nil {
		r.log.Error("ring sqlite: cannot begin a delete; rows will be redelivered", "error", err)
		return
	}
	for _, seq := range seqs {
		if _, err := tx.Exec(`DELETE FROM ring WHERE seq = ?`, seq); err != nil {
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

// Touch is a no-op: nothing else can claim a row this process is delivering.
func (r *SQLiteRing) Touch([]*item.Item) error { return nil }

// Drain returns nothing: the items are already on disk and will be picked up by
// the next boot's backlog drain, which is a stronger guarantee than an
// emergency dump (SPEC §12).
func (r *SQLiteRing) Drain() []*item.Item { return nil }

// Stats counts committed rows plus the reservations on their way to the next
// group commit: what the ceilings are checked against.
func (r *SQLiteRing) Stats() Stats {
	return Stats{
		Items:    r.items.Load() + r.resItems.Load(),
		Bytes:    r.bytes.Load() + r.resBytes.Load(),
		CapItems: int64(r.cfg.CapacityItems),
		CapBytes: r.cfg.CapacityBytes,
	}
}

// Close stops the writer after it has flushed what it holds, then closes both
// connections. Rows not yet delivered stay in ring.db for the next boot
// (SPEC §13.4).
func (r *SQLiteRing) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	close(r.stop)
	r.wg.Wait()

	// The reader may be inside a SELECT; holding the cursor lock waits it out,
	// so the connection is not closed under a query in flight.
	r.cursorMu.Lock()
	defer r.cursorMu.Unlock()

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
