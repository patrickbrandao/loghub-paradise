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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// MySQLConfig mirrors [sink.mysql].
type MySQLConfig struct {
	Addr           string // host:port, or a unix socket path starting with /
	Username       string
	Password       string
	Database       string
	Table          string // name or database.name, case kept
	PoolSize       int
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ConnectTimeout time.Duration
	// ValidateTimeout bounds the one-time boot validation of SPEC §10.6:
	// sink.op_timeout, not the health check's deadline. Zero takes
	// ConnectTimeout, which the app sets to sink.op_timeout.
	ValidateTimeout time.Duration
	AppName         string // program_name connection attribute, visible in performance_schema

	// MaxMessageBytes (L = syslog.max_message_bytes) and MaxEventBytes (the
	// worst JLH, MaxEventBytes(L), SPEC §3.9) size the largest row this server
	// can produce: env plus the text columns, which are disjoint spans of the
	// raw line and grow at most threefold when sanitised. The boot check
	// compares that row with the server's max_allowed_packet, so a chunk can
	// never carry a message the server would refuse. Zero skips the check.
	MaxMessageBytes int64
	MaxEventBytes   int64
}

// MySQL is the second shared, schema-bearing destination (SPEC §10.6). Schema
// in §3.7. Everything the postgres sink learned applies here -- validate and
// never create, one connection per worker out of a fleet-wide budget,
// durability decided by the server -- plus two ceilings that force a chunk to be
// split again before it is sent: the placeholder count and max_allowed_packet.
type MySQL struct {
	cfg MySQLConfig
	db  *sql.DB
	log *slog.Logger
	m   *obs.Metrics

	// quoted is the table as every statement names it: `db`.`t`.
	quoted string
	// schema and name are what information_schema is asked about.
	schema, name string

	// args pools the per-row argument slices, in the discipline of SPEC §10.5:
	// they hold references to the chunk buffer and go back cleared.
	args sync.Pool

	// maxPacket is the server's max_allowed_packet once validated. The chunker
	// keeps every statement under seven eighths of it.
	maxPacket atomic.Int64
	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64
}

const (
	mysqlColumns = 18

	// mysqlMaxRows is the most rows one INSERT carries: the binary protocol
	// addresses 65535 placeholders per statement, eighteen per row, and
	// 65535 / 18 is 3640. Config refuses a larger sink.batch with the same
	// number (SPEC §10.6); the chunker cuts here regardless.
	mysqlMaxRows = 65535 / mysqlColumns

	// mysqlRowOverhead is what one row costs in the execute packet beyond its
	// env and its emitter text (SPEC §10.6): the uuid and the eight integers
	// (about 50 bytes), a length prefix per variable column (up to 90), the
	// null bitmap and the type list (about 40), and space, src_ip, format and
	// transport, which this server writes and bounds (about 125). That is
	// about 300 bytes; the constant rounds up rather than sit on the edge of a
	// number the boot check depends on.
	mysqlRowOverhead = 512

	// mysqlDefaultPacket is the chunk ceiling used before the boot check has
	// read the server's real max_allowed_packet: the smallest default any
	// supported server ships with (MariaDB).
	mysqlDefaultPacket = 16 << 20
)

// mysqlIdent is checked here so a caller that bypasses config cannot inject SQL
// through the table name.
var mysqlIdent = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}(\.[A-Za-z0-9_]{1,64})?$`)

// checkQuotedName refuses a database name that cannot travel between
// backticks as is: sink.mysql.database and sink.clickhouse.database are
// spliced into every INSERT as `name`, and the table name is checked against
// injection precisely so that this text is the only SQL nobody wrote by hand.
// A backtick closes the quote (`a`b` is a syntax error at best, SQL at worst);
// a backslash is an escape to ClickHouse inside backticks and changes the name
// it reads; a control byte has no business in a name and would reach the logs
// raw. More than 64 bytes is longer than either server accepts, so refusing
// it here only says so at exit 1 instead of as a server error on the first
// check. On MySQL a refused statement would also be 1064, which the runtime
// table classifies as network: the boot would wait sink.boot_timeout for a
// typo. Other characters (a dash, a dot, UTF-8) are legal inside backticks
// and stay accepted. Config checks the same rule; this is the copy for a
// caller that bypasses it.
func checkQuotedName(what, name string) error {
	if name == "" {
		return fmt.Errorf("%s is empty", what)
	}
	if len(name) > 64 {
		return fmt.Errorf("%s %q has %d bytes; the limit is 64", what, name, len(name))
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
	{"uuid", []string{"binary"}},
	{"step", mysqlIntTypes},
	{"space", mysqlTextTypes},
	{"level", mysqlIntTypes},
	{"sid", mysqlIntTypes},
	{"ev_uts", mysqlIntTypes},
	{"ev_sub", mysqlIntTypes},
	{"facility", mysqlIntTypes},
	{"severity", mysqlIntTypes},
	{"format", mysqlTextTypes},
	{"transport", mysqlTextTypes},
	{"src_ip", mysqlTextTypes},
	{"hostname", mysqlTextTypes},
	{"appname", mysqlTextTypes},
	{"procid", mysqlTextTypes},
	{"msgid", mysqlTextTypes},
	{"env", mysqlBlobTypes},
	{"nbytes", mysqlIntTypes},
}

var errMySQLReadOnly = errors.New("server is read-only (@@read_only = 1): a replica, or configured that way")

// NewMySQL builds the sink without opening a connection: sql.OpenDB is lazy, and
// the boot's WaitReady owns the retry loop. The errors here are configuration
// errors and never a network condition.
func NewMySQL(cfg MySQLConfig, log *slog.Logger, m *obs.Metrics) (*MySQL, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.ValidateTimeout <= 0 {
		cfg.ValidateTimeout = cfg.ConnectTimeout
	}
	if cfg.Table == "" {
		cfg.Table = "syslog_events"
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
	// Zero makes the driver read max_allowed_packet from the server at connect,
	// so its own packet ceiling is the real one rather than a compiled-in guess.
	mc.MaxAllowedPacket = 0
	// The binary protocol carries env raw; interpolation would escape it into
	// the statement text and double the worst case (SPEC §10.6).
	mc.InterpolateParams = false
	mc.ParseTime = false
	mc.MultiStatements = false
	// Rows affected must count inserts, not matched rows: that arithmetic is
	// the duplicates counter.
	mc.ClientFoundRows = false
	// Strict for this session only. Outside strict mode a value too long or a
	// byte that is not UTF-8 is truncated with a warning instead of refused, and
	// a truncated column would be stored with nothing to count it.
	//
	// autocommit is pinned for the same reason the mode is: it is the server's
	// to default, and this sink depends on it. With autocommit=0 in my.cnf or
	// init_connect, every INSERT opens an implicit transaction nobody commits:
	// Write returns success, the duplicates arithmetic looks right, and the
	// rows vanish when the connection closes -- a silent loss of everything
	// confirmed. The partial delivery of SPEC §10.2 (Error.Delivered) also
	// rests on each statement being committed on its own. The driver sends
	// these as SET after the handshake, so they win over init_connect, which
	// the server runs before. The boot validation reads it back.
	mc.Params = map[string]string{
		"sql_mode":   "'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'",
		"autocommit": "1",
	}
	if cfg.AppName != "" {
		// The driver splits the attribute list on commas; a name holding one
		// would lose its tail in performance_schema, not break anything.
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

// NeedsColumns: the table has the promoted columns of SPEC §3.7.
func (p *MySQL) NeedsColumns() bool { return true }

// mysqlLogger adapts the driver's Print-style logger to slog.
type mysqlLogger struct{ log *slog.Logger }

func (l mysqlLogger) Print(v ...any) { l.log.Warn("mysql driver: " + fmt.Sprint(v...)) }

// insertFor builds the statement for n rows (SPEC §10.6). It is built per
// statement: microseconds of string work against a network round trip, and a
// cache keyed by row count would hold up to 3640 variants for nothing.
//
// ON DUPLICATE KEY UPDATE uuid = uuid, and NEVER INSERT IGNORE: IGNORE demotes
// 1406 and 1366 to warnings and truncates silently, which is exactly the loss
// the strict sql_mode exists to refuse.
func (p *MySQL) insertFor(rows int) string {
	var b strings.Builder
	b.Grow(260 + rows*40)
	b.WriteString("INSERT INTO ")
	b.WriteString(p.quoted)
	b.WriteString(" (uuid, step, space, level, sid, ev_uts, ev_sub, facility, severity, " +
		"format, transport, src_ip, hostname, appname, procid, msgid, env, nbytes) VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE uuid = uuid")
	return b.String()
}

// mysqlArgs is one statement's worth of placeholder values. The uuids live in
// their own array so the sixteen-byte slices handed to the driver stay valid
// for the whole statement; vals holds references into the chunk buffer, which is
// why reset clears it before the pool sees it again.
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

// mysqlNull is a nullable text value: NULL when the field did not come, the
// sanitised copy otherwise (SPEC §3.7).
func mysqlNull(ns convert.NullString) any {
	if !ns.Valid {
		return nil
	}
	return colText(ns.S)
}

func (a *mysqlArgs) add(e *Envelope, i int) {
	h := &e.Item.Head
	r := &e.Res
	a.ids[i] = [16]byte(h.UUID)
	// The four columns an emitter writes are sanitised copies for the reason
	// SPEC §3.7: NUL is replaced although MySQL would take it, so the
	// databases answer the same WHERE the same way. space,
	// src_ip, format and transport are written by this server and valid by
	// construction.
	a.vals = append(a.vals,
		a.ids[i][:], int64(0), h.Space, int64(r.Severity()), int64(r.SID),
		h.UTS, h.Sub, int64(r.Facility()), int64(r.Severity()),
		r.Format.String(), h.Transport.String(),
		mysqlNull(r.SrcIP), mysqlNull(r.Hostname), mysqlNull(r.AppName),
		mysqlNull(r.ProcID), mysqlNull(r.MsgID),
		nonNil(e.JLH), int64(len(e.JLH)))
}

// empty stands in for a nil slice. The driver sends []byte(nil) as NULL, which
// would violate NOT NULL, classify as logic and drop the whole chunk.
var empty = []byte{}

func nonNil(b []byte) []byte {
	if b == nil {
		return empty
	}
	return b
}

// reset drops every reference and keeps the capacity.
func (a *mysqlArgs) reset() {
	clear(a.vals)
	a.vals = a.vals[:0]
}

// rowCost estimates what one envelope costs inside the execute packet
// (SPEC §10.6): env, the text columns as they will be sent (sanitised), and the
// fixed overhead. The sanitised text of the four emitter columns is at most
// three times their span of the raw line, and the spans are disjoint, so no row
// can cost more than the row maxRowBytes sizes for the boot check.
func rowCost(e *Envelope) int64 {
	r := &e.Res
	text := colTextLen(r.Hostname.S) + colTextLen(r.AppName.S) +
		colTextLen(r.ProcID.S) + colTextLen(r.MsgID.S)
	return int64(len(e.JLH)+text) + mysqlRowOverhead
}

// maxRowBytes is the largest row this server can produce (SPEC §3.9): the worst
// JLH, plus 3 × L of sanitised text columns, plus the fixed overhead.
func (p *MySQL) maxRowBytes() int64 {
	return p.cfg.MaxEventBytes + 3*p.cfg.MaxMessageBytes + mysqlRowOverhead
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

// chunkEnd returns where the statement starting at start ends: at mysqlMaxRows,
// or before the envelope that would push the estimated cost past limit. An
// envelope that alone exceeds the limit travels by itself; the boot check makes
// that unreachable, and if the server lowered its limit afterwards the refusal
// is a logic error with the uuids in the log rather than a silent skip.
func chunkEnd(envs []Envelope, start int, limit int64) int {
	var total int64
	end := start
	for end < len(envs) && end-start < mysqlMaxRows {
		c := rowCost(&envs[end])
		if end > start && total+c > limit {
			break
		}
		total += c
		end++
	}
	return end
}

// Write inserts the chunk, in as many statements as the two ceilings demand
// (SPEC §10.6). A retry after a partial success rewrites statements that are
// already there, which the ON DUPLICATE KEY makes a no-op.
//
// A statement refused for good (logic) ends the chunk; the statements before
// it are stored, autocommitted one by one, so their envelopes go back in
// Error.Delivered and are not counted as lost (SPEC §10.2). The statements
// after it are not attempted: the chunk is settled as refused, as one unit.
func (p *MySQL) Write(ctx context.Context, b *Batch) error {
	if len(b.Envelopes) == 0 {
		return nil
	}
	if p.closed.Load() {
		return ErrClosed
	}
	limit := p.chunkLimit()
	for start := 0; start < len(b.Envelopes); {
		end := chunkEnd(b.Envelopes, start, limit)
		if err := p.writeRows(ctx, b.Envelopes[start:end]); err != nil {
			var e *Error
			if start > 0 && errors.As(err, &e) && e.Class == ClassLogic {
				e.Delivered = make([]int, start)
				for i := range e.Delivered {
					e.Delivered[i] = i
				}
			}
			return err
		}
		start = end
	}
	return nil
}

func (p *MySQL) writeRows(ctx context.Context, envs []Envelope) error {
	a := p.args.Get().(*mysqlArgs)
	defer func() {
		a.reset()
		p.args.Put(a)
	}()

	a.grow(len(envs))
	for i := range envs {
		a.add(&envs[i], i)
	}

	res, err := p.db.ExecContext(ctx, p.insertFor(len(envs)), a.vals...)
	if err != nil {
		return classifyMySQL("insert", err)
	}
	// One per inserted row, zero per row the empty UPDATE left alone: the
	// difference is the redelivery count.
	if n, err := res.RowsAffected(); err == nil {
		if dup := int64(len(envs)) - n; dup > 0 {
			p.m.SinkMySQLDuplicatesTotal.Add(uint64(dup))
		}
	}
	return nil
}

// Health asks whether the server is writable (SPEC §10.6). A replica answers
// SELECT 1 happily; @@read_only is what a failover flips.
//
// The first successful check also runs the one-time validation, and every
// check refreshes the pool gauges.
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
		if !p.validated.Load() {
			// At boot a read-only server is a replica, or a server configured
			// that way: a configuration mistake, refused at once with exit 3,
			// as PostgreSQL refuses one in recovery (SPEC §10.5, inherited by
			// §10.6). Retrying it until sink.boot_timeout would only postpone
			// the same verdict.
			return fatalErr("validate", errMySQLReadOnly)
		}
		// After boot it is a failover in progress: DOWN, and the fuses of
		// SPEC §13.3 decide whether it lasts too long.
		return networkErr("health", errMySQLReadOnly)
	}
	if !p.validated.Load() {
		// Its own deadline, sink.op_timeout, cancellable by the boot: the
		// validation is a dozen round trips plus a probe transaction, and
		// under sink.health_timeout (1 s) a server ~55 ms away failed every
		// attempt until sink.boot_timeout (validationContext, SPEC §10.6).
		vctx, cancel := validationContext(ctx, p.cfg.ValidateTimeout)
		err := p.validate(vctx)
		cancel()
		if err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("events table validated", "table", p.cfg.Table,
			"max_allowed_packet", p.maxPacket.Load(), "largest_row", p.maxRowBytes())
	}
	return nil
}

// validate is the boot contract of SPEC §10.6. Everything it refuses is
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
			"table %s uses engine %q, want InnoDB (the boot probe needs a transaction to roll back; "+
				"see docs/SPEC.md §3.7)", p.cfg.Table, engine.String))
	}

	have, err := p.columns(ctx)
	if err != nil {
		return err
	}
	for _, c := range mysqlColumnFamilies {
		typ, ok := have[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.7", p.cfg.Table, c.name))
		}
		if !contains(c.types, typ.dataType) {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is %s, want %s", p.cfg.Table, c.name, typ.dataType,
				strings.Join(c.types, " or ")))
		}
		if c.name == "uuid" && typ.columnType != "binary(16)" {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column \"uuid\" is %s, want binary(16)", p.cfg.Table, typ.columnType))
		}
		// The text columns this server fills itself have a longest value it
		// can name; a narrower column refuses those rows with 1406 -- logic,
		// the whole statement dropped, with the sink UP. The probe below
		// would catch it too, with the server's message; this one says what
		// the column must hold. The emitter's columns (hostname, appname,
		// procid, msgid) have no such bound, and how wide they are is the
		// operator's call (SPEC §3.7).
		if min := mysqlMinChars[c.name]; min > 0 && typ.maxChars.Valid && typ.maxChars.Int64 < int64(min) {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is %s, shorter than the %d characters this server writes into it; "+
					"every longer row would be refused (1406)", p.cfg.Table, c.name, typ.columnType, min))
		}
	}

	// ON DUPLICATE KEY needs a unique index over exactly (uuid, step) to
	// collide on: one with fewer columns would collide where the JLH does not,
	// one with more would never collide at all, and one with a PREFIX --
	// UNIQUE (uuid(4), step) -- collides on four bytes, which two events of
	// the same millisecond share. And it must be the ONLY unique index: the
	// statement fires on ANY unique key, so with UNIQUE (sid) next to the
	// primary key, a new event colliding there with an older row takes the
	// empty UPDATE, Write returns nil, and the event is gone. There is no 1062
	// to classify -- the loss is silent -- so it is refused here (found by
	// the webhook review of 2026-10-06: 1 of 3 rows stored).
	keys, err := p.uniqueKeys(ctx)
	if err != nil {
		return err
	}
	arbiter := false
	for _, k := range keys {
		if mysqlUniqueKind(k.parts, have) == uniqueArbiter {
			arbiter = true
		}
	}
	if !arbiter {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on exactly (uuid, step), whole columns without a prefix; "+
				"ON DUPLICATE KEY needs one", p.cfg.Table))
	}
	for _, k := range keys {
		if mysqlUniqueKind(k.parts, have) == uniqueForeign {
			return fatalErr("validate", fmt.Errorf(
				"table %s: unique index %q on (%s) besides the one on (uuid, step): ON DUPLICATE KEY "+
					"UPDATE fires on any unique key, so a new event colliding there would be dropped as "+
					"a duplicate without an error; make it non-unique (docs/SPEC.md §10.6)",
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
	if need := p.maxRowBytes(); p.cfg.MaxEventBytes > 0 && need > p.chunkLimit() {
		return fatalErr("validate", fmt.Errorf(
			"max_allowed_packet=%d leaves %d bytes per statement, below the largest row this server "+
				"can produce (%d bytes: the worst JLH of %d and the text columns; docs/SPEC.md §3.9); "+
				"raise max_allowed_packet or lower syslog.max_message_bytes",
			maxPacket, p.chunkLimit(), need, p.cfg.MaxEventBytes))
	}

	// The probe: the only reliable answer to "may this user write?" is to try,
	// because a grant through a role or through db.* never shows in
	// information_schema.TABLE_PRIVILEGES. ON DUPLICATE KEY UPDATE needs the
	// UPDATE privilege as well as INSERT, even though the update is empty, so
	// the probe runs exactly the statement the drain runs. It carries two rows
	// (probeArgs): one with every NULL a row may have, one with every column
	// at the widest value this server can write. Their uuids are version-0
	// UUIDs, which the generator can never mint, and the transaction is
	// rolled back, so the table never gains a row -- which is why InnoDB is
	// required above.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyMySQL("validate", err)
	}
	probe := p.probeArgs()
	_, err = tx.ExecContext(ctx, p.insertFor(2), probe.vals...)
	_ = tx.Rollback()
	if err != nil {
		var me *mysql.MySQLError
		if !errors.As(err, &me) {
			// No server answer -- a dropped connection, a deadline: the probe
			// did not run, so it proved nothing either way. Retried.
			return classifyMySQL("validate", err)
		}
		if me.Number == 1142 {
			return fatalErr("validate", fmt.Errorf(
				"user %q lacks INSERT or UPDATE on %s (ON DUPLICATE KEY UPDATE needs both; "+
					"UPDATE may be granted on the uuid column alone): %w", p.cfg.Username, p.cfg.Table, err))
		}
		// ANY answer the server gives the probe is fatal, whatever the runtime
		// table says about its number. The probe is exactly the statement the
		// drain runs, with the NULLs a row without hostname carries and the
		// widest values one can carry: if the server refuses it now -- 1048
		// on a column made NOT NULL, 1264 on a sid narrower than BIGINT, 1364 on an
		// extra column without default, 3819 on a CHECK, 1406 on a column too
		// narrow -- it will refuse rows of the drain the same way, as logic,
		// with the sink UP (the trap of SPEC §20.18). At boot that is exit 3,
		// not 300 s of retries.
		return fatalErr("validate", fmt.Errorf(
			"the boot probe -- the drain's own INSERT, rolled back -- was refused on %s; every row "+
				"like it would be (docs/SPEC.md §3.7, §10.6): %w", p.cfg.Table, err))
	}
	return nil
}

// mysqlMinChars is the longest value this server writes into each text column
// it fills itself (SPEC §3.7): space has up to 64 characters (SPEC §13.2),
// format is the longest of its closed set, transport the longest of the four
// this server receives on ("unix", SPEC §4), and src_ip the longest canonical
// address text -- a full IPv6, the zone removed and an IPv4-mapped address
// unmapped by item.Canonical (SPEC §4.2).
var mysqlMinChars = map[string]int{
	"space":     64,
	"format":    longestName(int(convert.NumFormats), func(i int) string { return convert.Format(i).String() }),
	"transport": longestName(item.NumTransports, func(i int) string { return item.Transport(i).String() }),
	"src_ip":    len(mysqlProbeSrcIP),
}

// mysqlProbeSrcIP is the widest canonical src_ip text: eight full groups.
const mysqlProbeSrcIP = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"

// The probe rows are rows a listener of this server produces, never a mix no
// listener makes. A unix row has no src_ip (SPEC §3.7) and the transport
// "unix", the longest of the four; every other row has an address. The probe
// once paired a NULL src_ip with "udp", a row that cannot exist, and refused a
// legitimate table tying the two together -- CHECK ((transport = 'unix') =
// (src_ip IS NULL)) -- (go-relplog-server review, 2026-10-11, adapted: the
// RELP server has no unix listener and probes tcp alone). A table that
// refuses a unix row (src_ip NOT NULL, transport CHAR(3)) is still refused:
// the unix listener writes such rows, and so may a persistent ring or a dump
// from a life that had it on.
var (
	mysqlProbeNullTransport = item.TransportUnix.String()
	mysqlProbeWideTransport = item.TransportTCP.String()
)

func longestName(n int, name func(int) string) int {
	m := 0
	for i := 0; i < n; i++ {
		m = max(m, len(name(i)))
	}
	return m
}

// longestOf returns the longest of a closed set of names, for the probe.
func longestOf(n int, name func(int) string) string {
	var out string
	for i := 0; i < n; i++ {
		if s := name(i); len(s) > len(out) {
			out = s
		}
	}
	return out
}

// probeArgs is the boot probe's two rows (SPEC §10.6). The first carries every
// NULL a real row of this server can carry (a message without a header has no
// hostname, appname, procid or msgid; one received on unix has no src_ip), so
// a column made NOT NULL is refused at boot; it is a unix row, whose
// transport is the longest. The second, a tcp row, carries every other column
// at the widest value this server can write -- the 64-character space, the
// longest format, the widest src_ip, the largest sid (2^53-1), the last second the
// JLH can carry, nanoseconds 999999999, facility 23, the largest envelope --
// so a column too narrow for them (space VARCHAR(16), sid INT, nbytes
// SMALLINT) refuses the probe with 1406 or 1264 instead of refusing the
// drain's rows as logic with the sink UP. The emitter's text columns stay NULL
// in both: they have no width this server could claim as the widest.
//
// The uuids are version 0, 00..00 and 00..01: the generator only mints
// version 7, so the probe can never collide with, or hide, a real row.
func (p *MySQL) probeArgs() *mysqlArgs {
	a := &mysqlArgs{}
	a.grow(2)
	a.ids[1][15] = 1
	nbytes := p.cfg.MaxEventBytes
	if nbytes <= 0 {
		nbytes = MaxEventBytes(64 << 10)
	}
	a.vals = append(a.vals,
		a.ids[0][:], int64(0), "probe", int64(0), int64(0),
		int64(0), int64(0), int64(0), int64(0),
		"rfc5424", mysqlProbeNullTransport, nil, nil, nil, nil, nil, empty, int64(0))
	a.vals = append(a.vals,
		a.ids[1][:], int64(0), strings.Repeat("p", 64), int64(7), int64(jlh.MaxSID),
		int64(item.MaxTimestamp), int64(999_999_999), int64(23), int64(7),
		longestOf(int(convert.NumFormats), func(i int) string { return convert.Format(i).String() }),
		mysqlProbeWideTransport, mysqlProbeSrcIP, nil, nil, nil, nil, empty, nbytes)
	return a
}

// mysqlColumnInfo is one row of information_schema.COLUMNS, lowercased.
type mysqlColumnInfo struct {
	dataType   string
	columnType string
	maxChars   sql.NullInt64 // CHARACTER_MAXIMUM_LENGTH
	autoInc    bool          // EXTRA says auto_increment
}

// columns reads the table's columns from information_schema.COLUMNS, keyed by
// lowercase name.
func (p *MySQL) columns(ctx context.Context) (map[string]mysqlColumnInfo, error) {
	rows, err := p.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, EXTRA
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
		if err := rows.Scan(&name, &dataType, &columnType, &c.maxChars, &extra); err != nil {
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

// Unique indexes as ON DUPLICATE KEY UPDATE sees them (SPEC §10.6).
const (
	// uniqueArbiter is over exactly the whole uuid and step columns, in
	// either order: the key a redelivery must collide on.
	uniqueArbiter = iota
	// uniqueSurrogate is over one whole AUTO_INCREMENT column: every row gets
	// a fresh value and InnoDB never moves the counter below the largest one
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

// mysqlUniqueKind classifies one unique index. A prefix is never the arbiter:
// UNIQUE (uuid(4), step) collides on four bytes, and two events of the same
// millisecond share more than that -- a new event would be taken for a
// redelivery and dropped without an error.
func mysqlUniqueKind(parts []mysqlKeyPart, cols map[string]mysqlColumnInfo) int {
	for _, kp := range parts {
		if !kp.column.Valid || kp.subPart.Valid {
			return uniqueForeign
		}
	}
	names := make([]string, len(parts))
	for i, kp := range parts {
		names[i] = strings.ToLower(kp.column.String)
	}
	switch {
	case len(names) == 2 && (names[0] == "uuid" && names[1] == "step" || names[0] == "step" && names[1] == "uuid"):
		return uniqueArbiter
	case len(names) == 1 && cols[names[0]].autoInc:
		return uniqueSurrogate
	}
	return uniqueForeign
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

// observePool exports the pool occupancy. WaitCount is cumulative, so only its
// growth since the last check is added.
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
// The table is SPEC §10.6; keep the two in step.
func classifyMySQL(op string, err error) error {
	if err == nil {
		return nil
	}
	if isContextErr(err) {
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
		// The driver refused to send a statement above the ceiling it read from
		// the server: this data does not fit, and retrying repeats it.
		return logicErr(op, err)
	}
	// No server error number means the server never answered: a refused dial,
	// a broken pipe, driver.ErrBadConn, mysql.ErrInvalidConn. All of it is the
	// connection.
	return networkErr(op, err)
}

// classifyMySQLNumber is the error table of SPEC §10.6: the number first, and
// for a number the table does not list, the class of its SQLSTATE.
//
// The lines that matter most: schema and privilege errors are FATAL, never
// logic, and a read-only server (1290) is fatal for the same reasons 25006 is
// on PostgreSQL. The SQLSTATE fallback exists because "unknown is network"
// alone retried forever, with the sink UP, a refusal of the data the table
// does not list -- MariaDB's CHECK violation 4025 (23000), a 3156 (22018) --
// and a ring that fills behind one bad event is no better than a dropped
// chunk; and it made a 1064 (42000), the syntax error a name the INSERT could
// not quote would give, wait sink.boot_timeout as "network". Only what neither
// the number nor the state explains is retried: HY000, the state of most
// server errors, says nothing.
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
	return ClassNetwork
}

// mysqlNumberClass is the error-number half of SPEC §10.6; ok is false for a
// number it does not list.
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
	// 1062 cannot come from the INSERT: ON DUPLICATE KEY absorbs a collision
	// on ANY unique key -- which is why a second unique index is refused at
	// boot instead of surfacing here as an error that never comes. What is
	// left is a duplicate raised by something else in the statement (a
	// trigger writing to another table): that row, so logic.
	case 1048, 1062, 1153, 1264, 1366, 1406, 1690: // NULL refused, duplicate entry, packet too large, out of range, bad string, data too long
		return ClassLogic, true
	// 3819 is listed because its SQLSTATE is HY000, which the fallback cannot
	// read: a CHECK the row fails is that row (the boot probe makes a CHECK
	// every row fails fatal before the drain starts).
	case 3819: // check constraint violated
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
		// (independent review, 2026-10-11; SPEC §10.6).
		1203, // max_user_connections (global) reached
		1226, // a per-user resource limit: MAX_USER_CONNECTIONS, *_PER_HOUR
		1461: // max_prepared_stmt_count reached (the binary protocol prepares on every Exec)
		return ClassNetwork, true
	}
	return ClassNetwork, false
}
