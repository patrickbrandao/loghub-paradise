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
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
)

// MySQLConfig mirrors [sink.mysql].
type MySQLConfig struct {
	Addr           string // host:port, or a unix socket path starting with /
	Username       string
	Password       string
	Database       string
	Table          string // name or database.name; validated by config
	PoolSize       int
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ConnectTimeout time.Duration
	AppName        string // program_name connection attribute, visible in performance_schema

	// MaxEventBytes is the largest envelope one accepted request can produce
	// (SPEC §3.2). The boot check compares it with the server's
	// max_allowed_packet (SPEC §8.6.4), so a batch can never carry an event
	// the server would refuse. Zero skips the check.
	MaxEventBytes int64
}

// MySQL is the second shared, schema-bearing destination (SPEC §8.6). Schema
// in §3.7. Everything the postgres sink learned applies here -- validate and
// never create, one connection per worker out of a fleet-wide budget,
// durability decided by the server -- plus two ceilings that force a batch to
// be split before it is sent: the placeholder count and max_allowed_packet.
type MySQL struct {
	cfg MySQLConfig
	db  *sql.DB
	log *slog.Logger
	m   *obs.Metrics

	// probe is the health check's own pool of ONE connection, from the same
	// connector, outside db (SPEC §8.6.7): the reason is Postgres.probe's --
	// writers holding every pooled connection starved a check that waited
	// for one, DOWN and then FATAL with a healthy server, and with ring=none
	// no pool_size keeps one free. database/sql redials it after a broken
	// connection on its own.
	probe *sql.DB

	// quoted is the table as every statement names it: `db`.`t`.
	quoted string
	// schema and name are what information_schema is asked about.
	schema, name string

	// args pools the per-row argument slices per worker, in the discipline
	// of SPEC §8.5.4: they hold references to the items' buffers and go back
	// cleared.
	args sync.Pool

	// maxPacket is the server's max_allowed_packet once validated. The
	// chunker keeps every statement under it.
	maxPacket atomic.Int64
	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64
}

const (
	// mysqlMaxRows is the most rows one INSERT carries: the binary protocol
	// addresses 65 535 placeholders per statement, ten per row.
	mysqlMaxRows = 5000

	// mysqlRowOverhead is a generous estimate of what one row costs on the
	// wire beyond its payload: the two uuids, the datetime, the integers, the
	// space and type, the execute packet's type list, null bitmap and length
	// prefixes.
	mysqlRowOverhead = 512

	// mysqlDefaultPacket is the chunk ceiling used before the boot check has
	// read the server's real max_allowed_packet: a quarter of MySQL 8's
	// default, so a batch written before validation never trips an older or
	// more conservative server.
	mysqlDefaultPacket = 16 << 20

	mysqlColumns = 10

	// mysqlMaxNameChars is the longest database name MySQL accepts: 64
	// CHARACTERS, not bytes (ClickHouse takes the same ceiling here).
	mysqlMaxNameChars = 64
)

// checkQuotedName refuses a database name that cannot travel between
// backticks as is: sink.mysql.database and sink.clickhouse.database are
// spliced into every INSERT as `name`, and the table name is checked against
// injection precisely so that this text is the only SQL nobody wrote by hand.
// A backtick closes the quote (`a`b` is a syntax error at best, SQL at worst);
// a backslash is an escape to ClickHouse inside backticks and changes the name
// it reads; a control byte has no business in a name and would reach the logs
// raw. More than 64 characters is longer than either server accepts, so
// refusing it here only says so at exit 1 instead of as a server error on the
// first check. The limit counts characters, not bytes: a legitimate name of 64
// accented letters is 128 bytes (go-syslog-server review, 2026-10-13). Other
// characters (a dash, a dot, UTF-8) are legal inside backticks and stay
// accepted. Config checks the same rule (SPEC §8.6.3); this is the copy for a
// caller that bypasses it (W pendência of 2026-10-11, ported from the
// syslog/RELP family, 2026-10-13).
func checkQuotedName(what, name string) error {
	if name == "" {
		return fmt.Errorf("%s is empty", what)
	}
	if n := utf8.RuneCountInString(name); n > mysqlMaxNameChars {
		return fmt.Errorf("%s %q has %d characters; the limit is %d", what, name, n, mysqlMaxNameChars)
	}
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c == '`' || c == '\\':
			return fmt.Errorf("%s %q has %q, which cannot appear between the backticks of the INSERT", what, name, c)
		case c < 0x20 || c == 0x7f:
			return fmt.Errorf("%s %q has the control byte 0x%02x", what, name, c)
		}
	}
	return nil
}

// mysqlIdent mirrors the config rule; it is checked again here so a caller
// that bypasses config cannot inject SQL through the table name.
var mysqlIdent = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}(\.[A-Za-z0-9_]{1,64})?$`)

var (
	mysqlTextTypes = []string{"char", "varchar", "tinytext", "text", "mediumtext", "longtext"}
	// mysqlIntTypes is in order of width: minInt compares positions in it.
	mysqlIntTypes = []string{"tinyint", "smallint", "mediumint", "int", "bigint"}
)

// mysqlColumn is what the boot validation of SPEC §8.6.4 demands of one column
// of the mirror (SPEC §3.7). The type family alone is not enough: the review
// of 2026-10-06 built tables of the right families on which every batch failed
// (1406, 1264) or the data changed in silence (a DATETIME rounding the
// microsecond), all with the sink UP.
type mysqlColumn struct {
	name  string
	types []string // DATA_TYPE values accepted
	// columnType, when set, is the exact COLUMN_TYPE: binary(16) for the
	// uuids, which a wider BINARY would pad with zero bytes.
	columnType string
	// minInt is the narrowest integer type accepted: the one
	// docker/mysql/inbox.sql declares. Wider is tolerated, narrower refused --
	// a TINYINT event_nsec takes the nanoseconds of macOS (always 0) and
	// refuses the first non-zero one of Linux as 1264, a logic drop of the
	// whole batch. UNSIGNED is tolerated at any width: every value this server
	// writes is >= 0, which the CHECKs of the DDL already demand.
	minInt string
	// minChars is the length a char or varchar must allow. space has up to 64
	// characters (SPEC §3.0) and type follows the same grammar; a shorter
	// column refuses the longer value as 1406 on every batch that carries one.
	// The grammars are ASCII, so characters and bytes agree.
	minChars int64
	// fsp is the fractional-seconds precision a DATETIME must have. Fewer
	// digits ROUND the value, with no warning even in strict mode, so a
	// DATETIME event_at would store an instant the envelope never had.
	fsp int64
}

// mysqlColumnFamilies is the part of the inbox contract this server writes
// (SPEC §3.7), in statement order. received_at is not here: the table fills
// it (mysqlReceivedAt).
var mysqlColumnFamilies = []mysqlColumn{
	{name: "uuid", types: []string{"binary"}, columnType: "binary(16)"},
	{name: "from_uuid", types: []string{"binary"}, columnType: "binary(16)"},
	{name: "event_at", types: []string{"datetime"}, fsp: 6},
	{name: "sid", types: mysqlIntTypes, minInt: "bigint"},
	{name: "step", types: mysqlIntTypes, minInt: "int"},
	{name: "level", types: mysqlIntTypes, minInt: "smallint"},
	{name: "event_nsec", types: mysqlIntTypes, minInt: "smallint"},
	{name: "space", types: mysqlTextTypes, minChars: 64},
	{name: "type", types: mysqlTextTypes, minChars: 64},
	{name: "payload", types: []string{"json"}},
}

// mysqlReceivedAt is the one column of the DDL this server never writes. The
// table may leave it out; when it has it, it is the DATETIME(6) the DDL
// declares, or the arrival a consumer measures the queue's lag by is rounded
// to the second.
var mysqlReceivedAt = mysqlColumn{name: "received_at", types: []string{"datetime"}, fsp: 6}

// mysqlColumnInfo is one row of information_schema.COLUMNS, lowercased.
type mysqlColumnInfo struct {
	dataType   string
	columnType string
	maxChars   sql.NullInt64 // CHARACTER_MAXIMUM_LENGTH
	fsp        sql.NullInt64 // DATETIME_PRECISION
	autoInc    bool          // EXTRA says auto_increment
}

// problem says what is wrong with one column against its contract, or "" when
// nothing is. A pure function, so every rule is unit-tested without a server.
func (c mysqlColumn) problem(got mysqlColumnInfo) string {
	if !contains(c.types, got.dataType) {
		return fmt.Sprintf("is %s, want %s", got.dataType, strings.Join(c.types, " or "))
	}
	if c.columnType != "" && got.columnType != c.columnType {
		return fmt.Sprintf("is %s, want %s", got.columnType, c.columnType)
	}
	if c.minInt != "" && intWidth(got.dataType) < intWidth(c.minInt) {
		return fmt.Sprintf("is %s, narrower than the %s of docker/mysql/inbox.sql (SPEC §3.7); "+
			"a value it cannot hold is refused as 1264, a logic drop of the whole batch", got.columnType, c.minInt)
	}
	if c.minChars > 0 && got.maxChars.Valid && got.maxChars.Int64 < c.minChars {
		return fmt.Sprintf("is %s, shorter than the %d characters it must hold; a longer value is "+
			"refused as 1406, a logic drop of the whole batch", got.columnType, c.minChars)
	}
	if c.fsp > 0 && (!got.fsp.Valid || got.fsp.Int64 != c.fsp) {
		return fmt.Sprintf("is %s, want datetime(%d): fewer fractional digits round the microseconds "+
			"in silence", got.columnType, c.fsp)
	}
	return ""
}

// intWidth is the position of an integer type in mysqlIntTypes; -1 for any
// other type.
func intWidth(dataType string) int {
	for i, t := range mysqlIntTypes {
		if t == dataType {
			return i
		}
	}
	return -1
}

// mysqlMariaDBRefusal explains why a MariaDB server is refused (SPEC §8.6), or
// returns "" when the server is not MariaDB or the payload is not LONGTEXT.
// MariaDB's JSON is an alias of LONGTEXT with a json_valid CHECK, so even the
// DDL of docker/mysql/inbox.sql applied verbatim reports longtext, and the
// mirror's promise -- a JSON column that a consumer reads as MySQL's -- does
// not hold. The mirror was never tested against MariaDB either; refusing it by
// name beats a "want json" that sends the operator looking for a typo.
func mysqlMariaDBRefusal(table, dataType, version string) string {
	if dataType != "longtext" || !strings.Contains(version, "MariaDB") {
		return ""
	}
	return fmt.Sprintf("table %s: column \"payload\" is longtext on %s: MariaDB is not supported by "+
		"sink=mysql, because its JSON type is an alias of LONGTEXT and the inbox mirror needs a real "+
		"JSON column (docs/SPEC.md §8.6); use MySQL 8.0.16+", table, version)
}

// Unique indexes as ON DUPLICATE KEY UPDATE sees them (SPEC §8.6.4).
const (
	// uniqueArbiter is over exactly the whole uuid column: the key a
	// redelivery must collide on.
	uniqueArbiter = iota
	// uniqueSurrogate is over one whole AUTO_INCREMENT column: every row gets
	// a fresh value, and InnoDB never moves the counter below the largest one
	// stored, so it cannot collide. MySQL demands an index on such a column,
	// and an operator's surrogate key is a common one.
	uniqueSurrogate
	// uniqueForeign is any other: ON DUPLICATE KEY UPDATE fires on ANY unique
	// key, so a new event that collides there with an older row is swallowed
	// as a duplicate -- Write returns nil and the event is gone.
	uniqueForeign
)

// mysqlKeyPart is one key part of a unique index, from
// information_schema.STATISTICS.
type mysqlKeyPart struct {
	column  sql.NullString // NULL for a functional key part
	subPart sql.NullInt64  // the prefix length; NULL for the whole column
}

// uniqueKind classifies one unique index. A prefix of uuid is not the arbiter:
// UNIQUE (uuid(8)) collides on eight bytes, and two events of the same
// millisecond share more than that.
func uniqueKind(parts []mysqlKeyPart, cols map[string]mysqlColumnInfo) int {
	if len(parts) != 1 || !parts[0].column.Valid || parts[0].subPart.Valid {
		return uniqueForeign
	}
	name := strings.ToLower(parts[0].column.String)
	if name == "uuid" {
		return uniqueArbiter
	}
	if cols[name].autoInc {
		return uniqueSurrogate
	}
	return uniqueForeign
}

var errMySQLReadOnly = errors.New("server is read-only (@@read_only = 1): a replica, or configured that way")

// probeUUID is the uuid of the boot probe's row: a UUIDv7 of instant zero, so
// the inbox's version CHECK accepts it (SPEC §8.6.4). The row is never kept.
var probeUUID = [16]byte{6: 0x70, 8: 0x80}

// probeRow is the boot probe's row: every column at the limit the JLH allows,
// so a table whose column cannot hold a value this server may write refuses
// the probe at boot instead of a batch later. The catalogue checks above it
// catch the known shapes by name; the probe is the backstop for the rest.
func probeRow() inboxRow {
	return inboxRow{
		UUID: probeUUID, From: probeUUID,
		EventAt:   time.Unix(0, 0).UTC(),
		EventNsec: 999,
		SID:       envelope.MaxSID,
		Step:      envelope.MaxStep,
		Level:     15, // the JLH ceiling, which envelope.ParseHead enforces
		Space:     probeSpace,
		Type:      envelope.Type,
		Payload:   []byte("{}"),
	}
}

// probeSpace is the longest space the grammar of SPEC §3.0 admits: 64
// characters.
var probeSpace = "probe." + strings.Repeat("x", 58)

// NewMySQL builds the sink without opening a connection: sql.OpenDB is lazy,
// and the boot's WaitReady owns the retry loop (SPEC §8.6.1). The errors here
// are configuration errors and never a network condition.
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
	if err := checkQuotedName("sink mysql: database", cfg.Database); err != nil {
		return nil, err
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
	// Zero makes the driver read max_allowed_packet from the server at
	// connect, so its own packet ceiling is the real one rather than a
	// compiled-in guess (SPEC §8.6.3).
	mc.MaxAllowedPacket = 0
	// The binary protocol carries the blobs raw; interpolation would escape
	// them into the statement text and double the worst case (SPEC §8.6.1).
	mc.InterpolateParams = false
	mc.ParseTime = false
	mc.MultiStatements = false
	// Rows affected must count inserts, not matched rows: that arithmetic is
	// the duplicates counter (SPEC §8.6.3).
	mc.ClientFoundRows = false
	// Strict for this session only. Outside strict mode a value too long or a
	// byte that is not UTF-8 is truncated with a warning instead of refused,
	// and a truncated body would be stored with nothing to count it.
	//
	// time_zone is UTC so the CURRENT_TIMESTAMP(6) default of received_at is
	// UTC like event_at, which the driver sends in UTC (mc.Loc).
	//
	// autocommit is pinned for the same reason the mode is: it is the server's
	// to default, and this sink depends on it. With autocommit=0 in my.cnf or
	// init_connect, every single-chunk INSERT opens an implicit transaction
	// nobody commits: Write returns success, the 200 goes out, and the rows
	// vanish when the connection closes -- a silent loss of everything
	// confirmed (SPEC §8.6.1). The driver sends these as SET after the
	// handshake, so they win over init_connect, which the server runs before.
	// The boot validation reads it back.
	mc.Params = mysqlSessionParams()
	mc.Loc = time.UTC
	if cfg.AppName != "" {
		mc.ConnectionAttributes = "program_name:" + cfg.AppName
	}
	// The driver reports dying connections on its own logger, which defaults
	// to stderr. During an outage that is one line per connection per check;
	// it belongs in the structured log, at the level a retryable failure has.
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
	probe := sql.OpenDB(connector)
	probe.SetMaxOpenConns(1)
	probe.SetMaxIdleConns(1)

	schema, name := cfg.Database, cfg.Table
	if i := strings.IndexByte(cfg.Table, '.'); i >= 0 {
		schema, name = cfg.Table[:i], cfg.Table[i+1:]
	}
	p := &MySQL{
		cfg:    cfg,
		db:     db,
		probe:  probe,
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

// mysqlSessionParams is what every connection SETs after the handshake (see
// NewMySQL for why each one is there).
func mysqlSessionParams() map[string]string {
	return map[string]string{
		"sql_mode":   "'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'",
		"time_zone":  "'+00:00'",
		"autocommit": "1",
	}
}

// mysqlLogger adapts the driver's Print-style logger to slog.
type mysqlLogger struct{ log *slog.Logger }

func (l mysqlLogger) Print(v ...any) { l.log.Warn("mysql driver: " + fmt.Sprint(v...)) }

// insertFor builds the statement for n rows (SPEC §8.6.3). It is built per
// batch: microseconds of string work against a network round trip, and a
// cache keyed by row count would hold up to 5 000 variants for nothing.
func (p *MySQL) insertFor(rows int) string {
	var b strings.Builder
	b.Grow(160 + rows*30)
	b.WriteString("INSERT INTO ")
	b.WriteString(p.quoted)
	b.WriteString(" (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload) VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?,?,?,?,?,?,?)")
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE uuid = uuid")
	return b.String()
}

// mysqlArgs is one chunk's worth of placeholder values. The uuids live in
// their own array so the sixteen-byte slices handed to the driver stay valid
// for the whole statement. vals is cleared before the pool sees it again: a
// slice returned full would keep the last chunk's rows alive for nothing.
type mysqlArgs struct {
	ids  [][16]byte // uuid and from_uuid of each row, two per row
	vals []any
}

func (a *mysqlArgs) grow(rows int) {
	if cap(a.ids) < 2*rows {
		a.ids = make([][16]byte, 2*rows)
	} else {
		a.ids = a.ids[:2*rows]
	}
	if cap(a.vals) < rows*mysqlColumns {
		a.vals = make([]any, 0, rows*mysqlColumns)
	}
}

func (a *mysqlArgs) add(it *envelope.Item, i int) error {
	r, err := inboxRowOf(it)
	if err != nil {
		return err
	}
	a.addRow(r, i)
	return nil
}

// addRow appends one row's ten values. The payload is sent as a string: the
// JSON column refuses a value in the binary character set (3144), which is
// what a []byte parameter would be.
func (a *mysqlArgs) addRow(r inboxRow, i int) {
	a.ids[2*i], a.ids[2*i+1] = r.UUID, r.From
	a.vals = append(a.vals,
		a.ids[2*i][:], a.ids[2*i+1][:], r.EventAt,
		r.SID, int64(r.Step), int64(r.Level), int64(r.EventNsec),
		r.Space, r.Type, string(r.Payload))
}

// reset drops every reference and keeps the capacity.
func (a *mysqlArgs) reset() {
	clear(a.vals)
	a.vals = a.vals[:0]
}

// rowCost estimates what one item costs inside the execute packet: its
// payload is at most the whole JLH.
func rowCost(it *envelope.Item) int64 {
	return it.Size() + mysqlRowOverhead
}

// chunkLimit is the byte ceiling of one statement: seven eighths of the
// server's max_allowed_packet, or of the smallest default before the boot
// check has read the real value.
func (p *MySQL) chunkLimit() int64 {
	mp := p.maxPacket.Load()
	if mp <= 0 {
		mp = mysqlDefaultPacket
	}
	return mp - mp/8
}

// chunkEnd returns where the chunk starting at start ends: at mysqlMaxRows,
// or before the item that would push the estimated cost past limit. An item
// that alone exceeds the limit travels by itself; the boot check makes that
// unreachable, and if the server lowered its limit afterwards the refusal is
// a logic error with the ids in the log rather than a silent skip.
func chunkEnd(items []*envelope.Item, start int, limit int64) int {
	var total int64
	end := start
	for end < len(items) && end-start < mysqlMaxRows {
		c := rowCost(items[end])
		if end > start && total+c > limit {
			break
		}
		total += c
		end++
	}
	return end
}

// Write inserts the batch, in as many statements as the two ceilings demand
// (SPEC §8.6.3), and all or nothing: a batch the worker counts as dropped is
// one the table does not hold.
func (p *MySQL) Write(ctx context.Context, items []*envelope.Item) error {
	if len(items) == 0 {
		return nil
	}
	if p.closed.Load() {
		return ErrClosed
	}
	// Every row is mapped before the first chunk is sent: an envelope that
	// does not map is damage, not data this server wrote, and the batch is
	// refused whole -- with earlier chunks already inserted, the worker would
	// count as dropped events that were delivered.
	rows := make([]inboxRow, len(items))
	for i, it := range items {
		r, err := inboxRowOf(it)
		if err != nil {
			return err
		}
		rows[i] = r
	}
	limit := p.chunkLimit()
	if chunkEnd(items, 0, limit) == len(items) {
		// One statement is atomic on its own: no transaction, no extra
		// round trips. This is every batch but one near the packet ceiling.
		dups, err := p.writeChunk(ctx, p.db, rows)
		if err != nil {
			return err
		}
		p.countDuplicates(dups)
		return nil
	}
	return p.writeChunked(ctx, items, rows, limit)
}

// writeChunked sends a batch that needs several statements inside ONE
// transaction. Committing chunk by chunk was rejected: a logic error on chunk
// k (a CHECK, a 1406) arrives after chunks 0..k-1 are committed, and the
// worker counts and logs the whole batch as dropped while part of it sits in
// the table. In one transaction the refusal rolls everything back, and "the
// batch was dropped" is true -- the all-or-nothing of the single multi-row
// INSERT of PostgreSQL (SPEC §8.5.4). A retry after a commit whose answer was
// lost rewrites rows that are there, which ON DUPLICATE KEY makes a no-op.
//
// The rows were mapped by Write before the transaction began, and each
// chunk's argument slices go back to the pool as soon as its statement
// returns: the driver keeps no reference past ExecContext.
func (p *MySQL) writeChunked(ctx context.Context, items []*envelope.Item, rows []inboxRow, limit int64) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyMySQL("begin", err)
	}
	var dups int64
	for start := 0; start < len(items); {
		end := chunkEnd(items, start, limit)
		n, err := p.writeChunk(ctx, tx, rows[start:end])
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		dups += n
		start = end
	}
	if err := tx.Commit(); err != nil {
		return classifyMySQL("commit", err)
	}
	// Counted after the commit: a rolled-back chunk's "duplicates" are not.
	p.countDuplicates(dups)
	return nil
}

// mysqlExecer is what one chunk needs: the pool, or the transaction of a
// batch sent in several chunks.
type mysqlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// writeChunk runs one statement and returns how many of its rows were
// duplicates.
func (p *MySQL) writeChunk(ctx context.Context, ex mysqlExecer, rows []inboxRow) (int64, error) {
	a := p.args.Get().(*mysqlArgs)
	defer func() {
		a.reset()
		p.args.Put(a)
	}()

	a.grow(len(rows))
	for i, r := range rows {
		a.addRow(r, i)
	}

	res, err := ex.ExecContext(ctx, p.insertFor(len(rows)), a.vals...)
	if err != nil {
		return 0, classifyMySQL("insert", err)
	}
	// One per inserted row, zero per row the empty UPDATE left alone: the
	// difference is the redelivery count (SPEC §8.6.3).
	if n, err := res.RowsAffected(); err == nil {
		if dup := int64(len(rows)) - n; dup > 0 {
			return dup, nil
		}
	}
	return 0, nil
}

func (p *MySQL) countDuplicates(n int64) {
	if n > 0 {
		p.m.SinkMySQLDuplicatesTotal.Add(uint64(n))
	}
}

// Health asks whether the server is writable (SPEC §8.6.7). A replica answers
// SELECT 1 happily; @@read_only is what a failover flips.
//
// The first successful check also runs the one-time validation of SPEC
// §8.6.4, and every check refreshes the pool gauges.
func (p *MySQL) Health(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	var readOnly int64
	if err := p.probe.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly); err != nil {
		return classifyMySQL("health", err)
	}
	p.observePool()
	if readOnly != 0 {
		return networkErr("health", errMySQLReadOnly)
	}
	if !p.validated.Load() {
		// A dozen catalogue round trips and the probe INSERT: they get
		// sink.op_timeout (ConnectTimeout), not the health check's one
		// second (SPEC §8.5.5, §8.6.4).
		vctx, cancel := validationContext(ctx, p.cfg.ConnectTimeout)
		err := p.validate(vctx)
		cancel()
		if err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("inbox table validated", "table", p.cfg.Table,
			"max_allowed_packet", p.maxPacket.Load())
	}
	return nil
}

// validate is the boot contract of SPEC §8.6.4. Everything it refuses is
// ClassFatal: none of it changes by waiting.
func (p *MySQL) validate(ctx context.Context) error {
	var engine sql.NullString
	err := p.db.QueryRowContext(ctx,
		"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		p.schema, p.name).Scan(&engine)
	if errors.Is(err, sql.ErrNoRows) {
		return fatalErr("validate", fmt.Errorf(
			"table %s does not exist, or this user cannot see it; apply docker/mysql/inbox.sql", p.cfg.Table))
	}
	if err != nil {
		return classifyMySQL("validate", err)
	}
	if !strings.EqualFold(engine.String, "InnoDB") {
		return fatalErr("validate", fmt.Errorf(
			"table %s uses engine %q, want InnoDB (transactions and crash recovery; see docs/SPEC.md §3.7)",
			p.cfg.Table, engine.String))
	}

	cols, err := p.columns(ctx)
	if err != nil {
		return err
	}
	for _, c := range mysqlColumnFamilies {
		got, ok := cols[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.7", p.cfg.Table, c.name))
		}
		problem := c.problem(got)
		if problem == "" {
			continue
		}
		if c.name == "payload" && got.dataType == "longtext" {
			var version string
			if err := p.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
				return classifyMySQL("validate", err)
			}
			if msg := mysqlMariaDBRefusal(p.cfg.Table, got.dataType, version); msg != "" {
				return fatalErr("validate", errors.New(msg))
			}
		}
		return fatalErr("validate", fmt.Errorf("table %s: column %q %s", p.cfg.Table, c.name, problem))
	}
	if got, ok := cols[mysqlReceivedAt.name]; ok {
		if problem := mysqlReceivedAt.problem(got); problem != "" {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q %s", p.cfg.Table, mysqlReceivedAt.name, problem))
		}
	}

	// ON DUPLICATE KEY needs a unique index over exactly (uuid) to collide
	// on -- and no other unique index it could collide on instead. The
	// statement fires on ANY unique key: with UNIQUE (from_uuid, sid) next to
	// the primary key, a new event colliding there with an older row took the
	// empty UPDATE, Write returned nil, and the event was gone (reproduced by
	// the review of 2026-10-06: 1 of 3 rows stored). There is no 1062 to
	// classify; the loss is silent, so it is refused here.
	keys, err := p.uniqueKeys(ctx)
	if err != nil {
		return err
	}
	arbiter := false
	for _, k := range keys {
		if uniqueKind(k.parts, cols) == uniqueArbiter {
			arbiter = true
		}
	}
	if !arbiter {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on exactly (uuid); ON DUPLICATE KEY needs one", p.cfg.Table))
	}
	for _, k := range keys {
		if uniqueKind(k.parts, cols) == uniqueForeign {
			return fatalErr("validate", fmt.Errorf(
				"table %s: unique index %q on (%s) besides the one on (uuid): ON DUPLICATE KEY UPDATE "+
					"fires on any unique key, so a new event colliding there would be dropped as a "+
					"duplicate without an error; make it non-unique (docs/SPEC.md §8.6.4)",
				p.cfg.Table, k.name, k.describe()))
		}
	}

	// autocommit, read back from a pooled connection: the session parameter
	// set it, and a server or proxy that ignored the SET would turn every
	// Write into a transaction nobody commits (see NewMySQL).
	var autocommit int64
	if err := p.db.QueryRowContext(ctx, "SELECT @@session.autocommit").Scan(&autocommit); err != nil {
		return classifyMySQL("validate", err)
	}
	if autocommit != 1 {
		return fatalErr("validate", errors.New(
			"the session has autocommit=0 although this sink sets it to 1; every INSERT would stay in "+
				"a transaction nobody commits, and its rows would vanish with the connection"))
	}

	var maxPacket int64
	if err := p.db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&maxPacket); err != nil {
		return classifyMySQL("validate", err)
	}
	p.maxPacket.Store(maxPacket)
	if need := p.cfg.MaxEventBytes + mysqlRowOverhead; p.cfg.MaxEventBytes > 0 && need > p.chunkLimit() {
		return fatalErr("validate", fmt.Errorf(
			"max_allowed_packet=%d is below the largest event this server can accept (%d bytes); "+
				"raise max_allowed_packet or lower server.max_body_bytes", maxPacket, need))
	}

	// The probe: the only reliable answer to "may this user write?" is to
	// try, because a grant through a role or through db.* never shows in
	// information_schema.TABLE_PRIVILEGES. ON DUPLICATE KEY UPDATE needs the
	// UPDATE privilege as well as INSERT, even though the update is empty,
	// so the probe exercises exactly the statement the drain will run.
	// Rolled back, so the table never gains a row -- which is why InnoDB is
	// required above. The row satisfies every CHECK of SPEC §3.7, so a refusal
	// of this data -- a CHECK the server's rows would fail too, an extra NOT
	// NULL column without a default -- is a property of the table: fatal, not
	// logic, or the boot would accept it and every batch would be dropped.
	// Every value is at the limit the JLH allows (probeRow): a "probe" type
	// with event_nsec 0 passed a TINYINT event_nsec that Linux nanoseconds
	// then broke on every batch.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyMySQL("validate", err)
	}
	probe := &mysqlArgs{}
	probe.grow(1)
	probe.addRow(probeRow(), 0)
	_, err = tx.ExecContext(ctx, p.insertFor(1), probe.vals...)
	_ = tx.Rollback()
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1142 {
			return fatalErr("validate", fmt.Errorf(
				"user %q lacks INSERT or UPDATE on %s (ON DUPLICATE KEY UPDATE needs both; "+
					"UPDATE may be granted on the uuid column alone): %w", p.cfg.Username, p.cfg.Table, err))
		}
		if ClassOf(classifyMySQL("validate", err)) == ClassLogic {
			return fatalErr("validate", fmt.Errorf(
				"table %s refuses a row of the inbox contract (a CHECK or an extra required column "+
					"this server never fills; see docs/SPEC.md §3.7): %w", p.cfg.Table, err))
		}
		return classifyMySQL("validate", err)
	}
	return nil
}

// columns reads the table's columns from information_schema.COLUMNS, keyed by
// lowercase name.
func (p *MySQL) columns(ctx context.Context) (map[string]mysqlColumnInfo, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, EXTRA
		  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		p.schema, p.name)
	if err != nil {
		return nil, classifyMySQL("validate", err)
	}
	defer rows.Close()
	cols := map[string]mysqlColumnInfo{}
	for rows.Next() {
		var name, dataType, columnType, extra string
		var c mysqlColumnInfo
		if err := rows.Scan(&name, &dataType, &columnType, &c.maxChars, &c.fsp, &extra); err != nil {
			return nil, classifyMySQL("validate", err)
		}
		c.dataType, c.columnType = strings.ToLower(dataType), strings.ToLower(columnType)
		c.autoInc = strings.Contains(strings.ToLower(extra), "auto_increment")
		cols[strings.ToLower(name)] = c
	}
	if err := rows.Err(); err != nil {
		return nil, classifyMySQL("validate", err)
	}
	return cols, nil
}

// mysqlUniqueKey is one unique index of the table, its key parts in order.
type mysqlUniqueKey struct {
	name  string
	parts []mysqlKeyPart
}

// describe renders the key parts for the operator: a prefix as col(n), a
// functional part as "expression".
func (k mysqlUniqueKey) describe() string {
	out := make([]string, len(k.parts))
	for i, kp := range k.parts {
		switch {
		case !kp.column.Valid:
			out[i] = "expression"
		case kp.subPart.Valid:
			out[i] = fmt.Sprintf("%s(%d)", kp.column.String, kp.subPart.Int64)
		default:
			out[i] = kp.column.String
		}
	}
	return strings.Join(out, ", ")
}

// uniqueKeys reads every unique index of the table from
// information_schema.STATISTICS, in index and key-part order.
func (p *MySQL) uniqueKeys(ctx context.Context) ([]mysqlUniqueKey, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT INDEX_NAME, COLUMN_NAME, SUB_PART
		  FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
		 ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
		p.schema, p.name)
	if err != nil {
		return nil, classifyMySQL("validate", err)
	}
	defer rows.Close()
	var keys []mysqlUniqueKey
	for rows.Next() {
		var name string
		var kp mysqlKeyPart
		if err := rows.Scan(&name, &kp.column, &kp.subPart); err != nil {
			return nil, classifyMySQL("validate", err)
		}
		if n := len(keys); n == 0 || keys[n-1].name != name {
			keys = append(keys, mysqlUniqueKey{name: name})
		}
		keys[len(keys)-1].parts = append(keys[len(keys)-1].parts, kp)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyMySQL("validate", err)
	}
	return keys, nil
}

// observePool exports the pool occupancy of SPEC §8.6.9. WaitCount is
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

// Close releases the pool and the health check's connection. It waits for
// connections a worker still holds, which is fine: the workers are stopped
// before the sink is closed.
func (p *MySQL) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	return errors.Join(p.db.Close(), p.probe.Close())
}

// classifyMySQL maps a driver error onto the class the state machine acts on.
// The table is SPEC §8.6.6; keep the two in step.
func classifyMySQL(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return networkErr(op, err)
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		var state string
		if me.SQLState != [5]byte{} {
			state = string(me.SQLState[:])
		}
		return &Error{Class: classifyMySQLNumber(me.Number, state), Op: op + " mysql=" + strconv.Itoa(int(me.Number)), Err: err}
	}
	if errors.Is(err, mysql.ErrPktTooLarge) {
		// The driver refused to send a statement above the ceiling it read
		// from the server: this data does not fit, and retrying repeats it.
		return logicErr(op, err)
	}
	// No server error number means the server never answered: a refused
	// dial, a broken pipe, driver.ErrBadConn, mysql.ErrInvalidConn. All of it
	// is the connection.
	return networkErr(op, err)
}

// classifyMySQLNumber is the error table of SPEC §8.6.6: the number first,
// and for a number the table does not list, the class of its SQLSTATE.
//
// The lines that matter most: schema and privilege errors are FATAL, never
// logic, and a read-only server (1290) is fatal for the same reasons 25006
// is on PostgreSQL. The SQLSTATE fallback exists because "unknown is
// network" alone retried forever, with the sink UP, a refusal of the data the
// table does not list -- 3151 (22032), a JSON key over 64 KiB, or MariaDB's
// CHECK violation 4025 (23000) -- and a ring that fills behind one bad event
// is no better than a dropped batch. Only what neither the number nor the
// state explains is retried.
func classifyMySQLNumber(n uint16, sqlState string) Class {
	if c, ok := mysqlNumberClass(n); ok {
		return c
	}
	if len(sqlState) < 2 {
		return ClassNetwork
	}
	switch sqlState[:2] {
	case "22", "23": // data exception, integrity constraint violation: THIS data
		return ClassLogic
	case "42", "28": // syntax or access rule, invalid authorization: our SQL and their server disagree
		return ClassFatal
	case "08": // connection exception
		return ClassNetwork
	}
	// HY000, the state of most server errors, says nothing: retried rather
	// than dropped, as a filling ring is visible and a discarded batch is not.
	return ClassNetwork
}

// mysqlNumberClass is the error-number half of SPEC §8.6.6; ok is false for
// a number it does not list.
func mysqlNumberClass(n uint16) (Class, bool) {
	switch n {
	case 1290, 1836: // read-only server, read-only mode
		return ClassFatal, true
	case 1021, 1030, 1041, 1114: // disk full, storage engine error, out of memory, table full
		return ClassFatal, true
	case 1044, 1045, 1049, 1130, 1142, 1143: // access denied, unknown database, host not allowed, command/column denied
		return ClassFatal, true
	case 1054, 1146, 1364: // unknown column, no such table, field has no default
		return ClassFatal, true
	// 1048 (column cannot be null) is 1364's twin and PostgreSQL's 23502: the
	// sink fills all ten columns and none of its values is NULL, so after the
	// boot probe it comes from the TABLE -- a trigger writing NULL, a column
	// changed under it. As logic it dropped every batch with the sink UP
	// (go-syslog-server review, 2026-10-13).
	case 1048:
		return ClassFatal, true
	// 1062 cannot come from the INSERT: ON DUPLICATE KEY absorbs a collision
	// on ANY unique key, which is why an extra one is refused at boot
	// (SPEC §8.6.4) instead of surfacing here. Listed for what it would be.
	case 1062, 1153, 1264, 1366, 1406, 1690: // duplicate entry, packet too large, out of range, bad string, data too long
		return ClassLogic, true
	case 3819, 3140, 3144: // CHECK violated, invalid JSON text, JSON from a binary string
		return ClassLogic, true
	case 1040, 1053, 1077, 1927, // too many connections, shutdown in progress, connection killed
		1158, 1159, 1160, 1161, // net read/write errors and timeouts
		1205, 1213, // lock wait timeout, deadlock
		1317, 3024, // query interrupted, max_execution_time
		2006, 2013, // server has gone away, lost connection
		// Server limits that clear on their own, and that report SQLSTATE
		// 42000: the "42 is fatal" fallback would end the process -- a dump
		// and exit 3, maybe on the whole fleet at once, freeing nothing --
		// when a rolling deploy briefly overlaps the connection budget
		// (go-relplog-server review, 2026-10-11; SPEC §8.6.6).
		1203, // max_user_connections (global) reached
		1226, // a per-user resource limit: MAX_USER_CONNECTIONS, *_PER_HOUR
		1461: // max_prepared_stmt_count reached (the binary protocol prepares on every Exec)
		return ClassNetwork, true
	}
	return ClassNetwork, false
}
