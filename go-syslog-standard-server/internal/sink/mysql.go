package sink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// MySQLConfig mirrors [sink.mysql].
type MySQLConfig struct {
	Addr           string // host:port, or a unix socket path starting with /
	Username       string
	Password       string
	Database       string
	Table          string // name or database.name, case kept; validated by config
	PoolSize       int
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ConnectTimeout time.Duration
	AppName        string // program_name connection attribute, visible in performance_schema

	// MaxRawBytes and MaxEnvelopeBytes are the longest raw line an enabled
	// transport can accept and the largest envelope that line can produce
	// (SPEC §3.9). The boot check adds them up into the largest row and
	// compares it with the server's max_allowed_packet (§10.7.4), so a batch
	// can never carry a message the server would refuse. Zero skips the check.
	MaxRawBytes      int64
	MaxEnvelopeBytes int64
}

// MySQL is the second shared, schema-bearing destination (SPEC §10.7). Schema
// in §3.7. Everything the postgres sink learned applies here -- validate and
// never create, one connection per worker out of a fleet-wide budget,
// durability decided by the server -- plus two ceilings that force a batch to
// be split before it is sent: the placeholder count and max_allowed_packet.
//
// It is an adapted copy of the sibling service's sink, which ran against a real
// server first. What changed is the table: seventeen columns instead of
// thirteen, which lowers the row ceiling, and four text columns that come from
// arbitrary emitters instead of an HTTP request line.
type MySQL struct {
	cfg MySQLConfig
	db  *sql.DB
	log *slog.Logger
	m   *obs.Metrics

	// quoted is the table as every statement names it: `db`.`t`.
	quoted string
	// schema and name are what information_schema is asked about.
	schema, name string

	// args pools the per-row argument slices per worker, in the discipline of
	// SPEC §10.5.2: they hold references to the batch's buffers and go back
	// cleared.
	args sync.Pool

	// maxPacket is the server's max_allowed_packet once validated. The chunker
	// keeps every statement under it.
	maxPacket atomic.Int64
	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64
}

const (
	mysqlColumns = 17

	// mysqlMaxRows is the most rows one INSERT carries: the binary protocol
	// addresses 65535 placeholders per statement, seventeen per row, and 65535
	// / 17 is exactly 3855. Config refuses a larger sink.batch with the same
	// number (SPEC §10.7.3); the chunker cuts here regardless.
	mysqlMaxRows = 65535 / mysqlColumns

	// mysqlRowOverhead is what one row costs in the execute packet beyond its
	// env, raw and emitter text: the id and the seven integers (72 bytes), a
	// length prefix per variable column (up to 81), the null bitmap and the
	// type list (37), and peer_ip, transport and rfc, which this server writes
	// and bounds (56). That is about 250 bytes; the constant doubles it rather
	// than sit on the edge of a number the boot check depends on.
	mysqlRowOverhead = 512

	// mysqlTextExpansion is how much larger a sanitised text column can be
	// than the span it came from: pgText turns one invalid byte into the three
	// of U+FFFD.
	mysqlTextExpansion = 3

	// mysqlDefaultPacket is the chunk ceiling used before the boot check has
	// read the server's real max_allowed_packet: the smallest default any
	// supported server ships with (MariaDB).
	mysqlDefaultPacket = 16 << 20
)

// mysqlIdent mirrors the config rule; it is checked again here so a caller that
// bypasses config cannot inject SQL through the table name.
var mysqlIdent = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}(\.[A-Za-z0-9_]{1,64})?$`)

var (
	mysqlTextTypes = []string{"char", "varchar", "tinytext", "text", "mediumtext", "longtext"}
	mysqlBlobTypes = []string{"varbinary", "tinyblob", "blob", "mediumblob", "longblob"}
	mysqlIntTypes  = []string{"tinyint", "smallint", "mediumint", "int", "bigint"}
)

// mysqlColumnFamilies is the schema contract of SPEC §3.7, in table order, as
// the DATA_TYPE column of information_schema reports it.
var mysqlColumnFamilies = []struct {
	name  string
	types []string
}{
	{"id", []string{"binary"}},
	{"ts", []string{"bigint"}},
	{"nts", []string{"bigint"}},
	{"facility", mysqlIntTypes},
	{"severity", mysqlIntTypes},
	{"hostname", mysqlTextTypes},
	{"app_name", mysqlTextTypes},
	{"procid", mysqlTextTypes},
	{"msgid", mysqlTextTypes},
	{"peer_ip", mysqlTextTypes},
	{"transport", mysqlTextTypes},
	{"rfc", mysqlTextTypes},
	{"msg_offset", []string{"bigint"}},
	{"msg_size", []string{"bigint"}},
	{"env", mysqlBlobTypes},
	{"raw", mysqlBlobTypes},
	{"nbytes", []string{"bigint"}},
}

var errMySQLReadOnly = errors.New("server is read-only (@@read_only = 1): a replica, or configured that way")

// nilUUID is the id of the boot probe (SPEC §10.7.4): a row the generator can
// never mint, inserted inside a transaction that is rolled back.
const nilUUID = "00000000-0000-0000-0000-000000000000"

// NewMySQL builds the sink without opening a connection: sql.OpenDB is lazy, and
// the boot's WaitReady owns the retry loop (SPEC §10.7.1). The errors here are
// configuration errors and never a network condition.
func NewMySQL(cfg MySQLConfig, log *slog.Logger, m *obs.Metrics) (*MySQL, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if !mysqlIdent.MatchString(cfg.Table) {
		return nil, fmt.Errorf("sink mysql: table %q is not a plain identifier", cfg.Table)
	}
	if cfg.Database == "" {
		return nil, errors.New("sink mysql: database is empty")
	}

	mc := mysql.NewConfig()
	mc.User = cfg.Username
	mc.Passwd = cfg.Password
	mc.DBName = cfg.Database
	host := cfg.Addr
	if strings.HasPrefix(cfg.Addr, "/") {
		mc.Net, mc.Addr = "unix", cfg.Addr
	} else {
		h, _, err := net.SplitHostPort(cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("sink mysql: addr %q: %w", cfg.Addr, err)
		}
		host = h
		mc.Net, mc.Addr = "tcp", cfg.Addr
	}
	mc.Timeout = cfg.ConnectTimeout
	// Zero makes the driver read max_allowed_packet from the server at connect,
	// so its own packet ceiling is the real one rather than a compiled-in guess
	// (SPEC §10.7.3).
	mc.MaxAllowedPacket = 0
	// The binary protocol carries the blobs raw; interpolation would escape
	// them into the statement text and double the worst case (SPEC §10.7.1).
	mc.InterpolateParams = false
	mc.ParseTime = false
	mc.MultiStatements = false
	// Rows affected must count inserts, not matched rows: that arithmetic is
	// the duplicates counter (SPEC §10.7.3).
	mc.ClientFoundRows = false
	// Strict for this session only. Outside strict mode a value too long or a
	// byte that is not UTF-8 is truncated with a warning instead of refused,
	// and a truncated raw line would be stored with nothing to count it.
	mc.Params = map[string]string{"sql_mode": "'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"}
	if cfg.AppName != "" {
		// The driver splits the attribute list on commas; a node.id holding
		// one would lose its tail in performance_schema, not break anything.
		mc.ConnectionAttributes = "program_name:" + strings.ReplaceAll(cfg.AppName, ",", "_")
	}
	// The driver reports dying connections on its own logger, which defaults to
	// stderr. During an outage that is one line per connection per check; it
	// belongs in the structured log, at the level a retryable failure has.
	mc.Logger = mysqlLogger{log}
	if cfg.TLS {
		tc, err := loadTLS("sink mysql", cfg.TLSCA, cfg.TLSInsecure, host)
		if err != nil {
			return nil, err
		}
		mc.TLS = tc
	}

	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, fmt.Errorf("sink mysql: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(cfg.PoolSize)
	db.SetMaxIdleConns(cfg.PoolSize)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	schema, name := cfg.Database, cfg.Table
	if i := strings.IndexByte(cfg.Table, '.'); i >= 0 {
		schema, name = cfg.Table[:i], cfg.Table[i+1:]
	}
	p := &MySQL{
		cfg:    cfg,
		db:     db,
		log:    log,
		m:      m,
		quoted: "`" + schema + "`.`" + name + "`",
		schema: schema,
		name:   name,
	}
	p.args.New = func() any { return &mysqlArgs{} }
	return p, nil
}

func (p *MySQL) Type() string { return "mysql" }

// mysqlLogger adapts the driver's Print-style logger to slog.
type mysqlLogger struct{ log *slog.Logger }

func (l mysqlLogger) Print(v ...any) { l.log.Warn("mysql driver: " + fmt.Sprint(v...)) }

// insertFor builds the statement for n rows (SPEC §10.7.3). It is built per
// chunk: microseconds of string work against a network round trip, and a cache
// keyed by row count would hold up to 3855 variants for nothing.
func (p *MySQL) insertFor(rows int) string {
	var b strings.Builder
	b.Grow(240 + rows*36)
	b.WriteString("INSERT INTO ")
	b.WriteString(p.quoted)
	b.WriteString(" (id, ts, nts, facility, severity, hostname, app_name, procid, msgid, " +
		"peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes) VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE id = id")
	return b.String()
}

// mysqlArgs is one chunk's worth of placeholder values. The ids live in their
// own array so the sixteen-byte slices handed to the driver stay valid for the
// whole statement; vals holds references into the batch's env and raw buffers,
// which is why reset clears it before the pool sees it again.
type mysqlArgs struct {
	ids  [][16]byte
	vals []any
}

func (a *mysqlArgs) grow(rows int) {
	if cap(a.ids) < rows {
		a.ids = make([][16]byte, rows)
	} else {
		a.ids = a.ids[:rows]
	}
	if cap(a.vals) < rows*mysqlColumns {
		a.vals = make([]any, 0, rows*mysqlColumns)
	}
}

func (a *mysqlArgs) add(p *Prepared, i int) bool {
	u, ok := parseUUID(p.Item.Head.ID)
	if !ok {
		return false
	}
	a.ids[i] = u
	m := &p.Meta
	raw := p.Raw()
	// The four columns an emitter writes are sanitised copies, for the reason
	// §3.6 gives postgres: under the strict sql_mode a byte that is not UTF-8
	// is 1366 and drops the whole chunk. NUL is replaced too, although MySQL
	// would take it, so the two shared databases answer the same WHERE the same
	// way (SPEC §3.7). peer_ip, transport and rfc are written by this server
	// and are valid by construction.
	a.vals = append(a.vals,
		a.ids[i][:], m.TS, m.NTS, int64(m.Facility), int64(m.Severity),
		pgText(m.Hostname), pgText(m.AppName), pgText(m.ProcID), pgText(m.MsgID),
		m.PeerIP, m.Transport, m.RFC, int64(m.MsgOffset), int64(m.MsgSize),
		nonNil(p.Env), nonNil(raw), int64(len(p.Env)+len(raw)))
	return true
}

// reset drops every reference and keeps the capacity.
func (a *mysqlArgs) reset() {
	clear(a.vals)
	a.vals = a.vals[:0]
}

// rowCost estimates what one message costs inside the execute packet. The text
// columns are counted at their sanitised worst case, so the estimate can only
// split a batch early, never late; it has the shape of maxRowBytes, so no row
// the chunker meets can cost more than the row the boot check sized.
func rowCost(p *Prepared) int64 {
	m := &p.Meta
	text := len(m.Hostname) + len(m.AppName) + len(m.ProcID) + len(m.MsgID)
	return int64(len(p.Env)+len(p.Item.Raw)+mysqlTextExpansion*text) + mysqlRowOverhead
}

// maxRowBytes is the largest row this server can produce: a raw line at the
// transport ceiling, its envelope at the bound of SPEC §3.9, and the four text
// columns -- disjoint spans of the line -- at their sanitised worst case.
func (p *MySQL) maxRowBytes() int64 {
	return p.cfg.MaxRawBytes + p.cfg.MaxEnvelopeBytes +
		mysqlTextExpansion*p.cfg.MaxRawBytes + mysqlRowOverhead
}

// chunkLimit is the byte ceiling of one statement: seven eighths of the
// server's max_allowed_packet, or of the smallest default before the boot check
// has read the real value.
func (p *MySQL) chunkLimit() int64 {
	mp := p.maxPacket.Load()
	if mp <= 0 {
		mp = mysqlDefaultPacket
	}
	return mp - mp/8
}

// chunkEnd returns where the chunk starting at start ends: at mysqlMaxRows, or
// before the message that would push the estimated cost past limit. A message
// that alone exceeds the limit travels by itself; the boot check makes that
// unreachable, and if the server lowered its limit afterwards the refusal is a
// logic error with the ids in the log rather than a silent skip.
func chunkEnd(batch []Prepared, start int, limit int64) int {
	var total int64
	end := start
	for end < len(batch) && end-start < mysqlMaxRows {
		c := rowCost(&batch[end])
		if end > start && total+c > limit {
			break
		}
		total += c
		end++
	}
	return end
}

// Write inserts the batch, in as many statements as the two ceilings demand
// (SPEC §10.7.3). A retry after a partial success rewrites chunks that are
// already there, which the ON DUPLICATE KEY makes a no-op.
func (p *MySQL) Write(ctx context.Context, batch []Prepared) error {
	if len(batch) == 0 {
		return nil
	}
	if p.closed.Load() {
		return ErrClosed
	}
	limit := p.chunkLimit()
	for start := 0; start < len(batch); {
		end := chunkEnd(batch, start, limit)
		if err := p.writeChunk(ctx, batch[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (p *MySQL) writeChunk(ctx context.Context, batch []Prepared) error {
	a := p.args.Get().(*mysqlArgs)
	defer func() {
		a.reset()
		p.args.Put(a)
	}()

	a.grow(len(batch))
	for i := range batch {
		if !a.add(&batch[i], i) {
			// Ids come from this process's own generator, so this is a bug,
			// not data. Dropping the batch loudly beats skipping the message.
			return logicErr("insert id="+batch[i].Item.Head.ID, errMalformedID)
		}
	}

	res, err := p.db.ExecContext(ctx, p.insertFor(len(batch)), a.vals...)
	if err != nil {
		return classifyMySQL("insert", err)
	}
	// One per inserted row, zero per row the empty UPDATE left alone: the
	// difference is the redelivery count (SPEC §10.7.3).
	if n, err := res.RowsAffected(); err == nil {
		if dup := int64(len(batch)) - n; dup > 0 {
			p.m.SinkMySQLDuplicatesTotal.Add(uint64(dup))
		}
	}
	return nil
}

// Health asks whether the server is writable (SPEC §10.7.7). A replica answers
// SELECT 1 happily; @@read_only is what a failover flips.
//
// The first successful check also runs the one-time validation of SPEC
// §10.7.4, and every check refreshes the pool gauges.
func (p *MySQL) Health(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	var readOnly int64
	if err := p.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly); err != nil {
		return classifyMySQL("health", err)
	}
	p.observePool()
	if readOnly != 0 {
		return networkErr("health", errMySQLReadOnly)
	}
	if !p.validated.Load() {
		if err := p.validate(ctx); err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("events table validated", "table", p.cfg.Table,
			"max_allowed_packet", p.maxPacket.Load(), "largest_row", p.maxRowBytes())
	}
	return nil
}

// validate is the boot contract of SPEC §10.7.4. Everything it refuses is
// ClassFatal: none of it changes by waiting.
func (p *MySQL) validate(ctx context.Context) error {
	var engine sql.NullString
	err := p.db.QueryRowContext(ctx,
		"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		p.schema, p.name).Scan(&engine)
	if errors.Is(err, sql.ErrNoRows) {
		return fatalErr("validate", fmt.Errorf(
			"table %s does not exist, or this user cannot see it; apply docker/mysql/schema.sql", p.cfg.Table))
	}
	if err != nil {
		return classifyMySQL("validate", err)
	}
	if !strings.EqualFold(engine.String, "InnoDB") {
		return fatalErr("validate", fmt.Errorf(
			"table %s uses engine %q, want InnoDB (transactions and crash recovery; see docs/SPEC.md §3.7)",
			p.cfg.Table, engine.String))
	}

	rows, err := p.db.QueryContext(ctx,
		"SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		p.schema, p.name)
	if err != nil {
		return classifyMySQL("validate", err)
	}
	have := map[string][2]string{}
	for rows.Next() {
		var name, dataType, columnType string
		if err := rows.Scan(&name, &dataType, &columnType); err != nil {
			rows.Close()
			return classifyMySQL("validate", err)
		}
		have[strings.ToLower(name)] = [2]string{strings.ToLower(dataType), strings.ToLower(columnType)}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classifyMySQL("validate", err)
	}
	for _, c := range mysqlColumnFamilies {
		typ, ok := have[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.7", p.cfg.Table, c.name))
		}
		if !contains(c.types, typ[0]) {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is %s, want %s", p.cfg.Table, c.name, typ[0],
				strings.Join(c.types, " or ")))
		}
		if c.name == "id" && typ[1] != "binary(16)" {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column \"id\" is %s, want binary(16)", p.cfg.Table, typ[1]))
		}
	}

	// ON DUPLICATE KEY needs a unique index over exactly (id) to collide on.
	var unique int
	if err := p.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
		  SELECT INDEX_NAME FROM information_schema.STATISTICS
		   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
		   GROUP BY INDEX_NAME
		  HAVING COUNT(*) = 1 AND MIN(COLUMN_NAME) = 'id') u`,
		p.schema, p.name).Scan(&unique); err != nil {
		return classifyMySQL("validate", err)
	}
	if unique == 0 {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on (id); ON DUPLICATE KEY needs one", p.cfg.Table))
	}

	var maxPacket int64
	if err := p.db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&maxPacket); err != nil {
		return classifyMySQL("validate", err)
	}
	p.maxPacket.Store(maxPacket)
	if need := p.maxRowBytes(); p.cfg.MaxRawBytes > 0 && need > p.chunkLimit() {
		return fatalErr("validate", fmt.Errorf(
			"max_allowed_packet=%d leaves %d bytes per statement, below the largest row this server "+
				"can accept (%d bytes: a raw line of up to %d, its envelope and the text columns; "+
				"docs/SPEC.md §3.9); raise max_allowed_packet or lower tcp.max_frame_bytes and "+
				"relp.max_frame_bytes", maxPacket, p.chunkLimit(), need, p.cfg.MaxRawBytes))
	}

	// The probe: the only reliable answer to "may this user write?" is to try,
	// because a grant through a role or through db.* never shows in
	// information_schema.TABLE_PRIVILEGES. ON DUPLICATE KEY UPDATE needs the
	// UPDATE privilege as well as INSERT, even though the update is empty, so
	// the probe exercises exactly the statement the drain will run. Rolled
	// back, so the table never gains a row -- which is why InnoDB is required
	// above.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyMySQL("validate", err)
	}
	probe := &mysqlArgs{}
	probe.grow(1)
	probe.add(&Prepared{Item: envelope.NewItem(envelope.Head{ID: nilUUID}, []byte{}, -1)}, 0)
	_, err = tx.ExecContext(ctx, p.insertFor(1), probe.vals...)
	_ = tx.Rollback()
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1142 {
			return fatalErr("validate", fmt.Errorf(
				"user %q lacks INSERT or UPDATE on %s (ON DUPLICATE KEY UPDATE needs both; "+
					"UPDATE may be granted on the id column alone): %w", p.cfg.Username, p.cfg.Table, err))
		}
		return classifyMySQL("validate", err)
	}
	return nil
}

// observePool exports the pool occupancy of SPEC §10.7.8. WaitCount is
// cumulative, so only its growth since the last check is added.
func (p *MySQL) observePool() {
	st := p.db.Stats()
	p.m.SinkMySQLPoolConns[obs.PoolAcquired].Set(int64(st.InUse))
	p.m.SinkMySQLPoolConns[obs.PoolIdle].Set(int64(st.Idle))
	p.m.SinkMySQLPoolConns[obs.PoolMax].Set(int64(st.MaxOpenConnections))
	if prev := p.lastWaits.Swap(st.WaitCount); st.WaitCount > prev {
		p.m.SinkMySQLPoolWaitsTotal.Add(uint64(st.WaitCount - prev))
	}
}

// Close releases the pool. It waits for connections a worker still holds, which
// is fine: the workers are stopped before the sink is closed.
func (p *MySQL) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	return p.db.Close()
}

// classifyMySQL maps a driver error onto the class the state machine acts on.
// The table is SPEC §10.7.6; keep the two in step.
func classifyMySQL(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return networkErr(op, err)
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return &Error{classifyMySQLNumber(me.Number), op + " mysql=" + strconv.Itoa(int(me.Number)), err}
	}
	if errors.Is(err, mysql.ErrPktTooLarge) {
		// The driver refused to send a statement above the ceiling it read from
		// the server: this data does not fit, and retrying repeats it.
		return logicErr(op, err)
	}
	// No server error number means the server never answered: a refused dial,
	// a broken pipe, driver.ErrBadConn, mysql.ErrInvalidConn. All of it is the
	// connection.
	return networkErr(op, err)
}

// classifyMySQLNumber is the error-number table of SPEC §10.7.6.
//
// The lines that matter most: schema and privilege errors are FATAL, never
// logic, and a read-only server (1290) is fatal for the same reasons 25006 is on
// PostgreSQL. Unknown numbers are retried, not dropped (invariant §19.17).
func classifyMySQLNumber(n uint16) Class {
	switch n {
	case 1290, 1836: // read-only server, read-only mode
		return ClassFatal
	case 1021, 1030, 1041, 1114: // disk full, storage engine error, out of memory, table full
		return ClassFatal
	case 1044, 1045, 1049, 1130, 1142, 1143: // access denied, unknown database, host not allowed, command/column denied
		return ClassFatal
	case 1054, 1146, 1364: // unknown column, no such table, field has no default
		return ClassFatal
	case 1048, 1062, 1153, 1264, 1366, 1406, 1690: // NULL refused, duplicate on another key, packet too large, out of range, bad string, data too long
		return ClassLogic
	case 1040, 1053, 1077, 1927, // too many connections, shutdown in progress, connection killed
		1158, 1159, 1160, 1161, // net read/write errors and timeouts
		1205, 1213, // lock wait timeout, deadlock
		1317, 3024, // query interrupted, max_execution_time
		2006, 2013: // server has gone away, lost connection
		return ClassNetwork
	}
	return ClassNetwork
}
