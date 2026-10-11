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
	AppName        string // program_name connection attribute, visible in performance_schema

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
	if cfg.Table == "" {
		cfg.Table = "syslog_events"
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
	mc.Params = map[string]string{"sql_mode": "'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"}
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
		if err := p.validate(ctx); err != nil {
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
		if c.name == "uuid" && typ[1] != "binary(16)" {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column \"uuid\" is %s, want binary(16)", p.cfg.Table, typ[1]))
		}
	}

	// ON DUPLICATE KEY needs a unique index over exactly (uuid, step) to
	// collide on: one with fewer columns would collide where the JLH does not,
	// one with more would never collide at all.
	var unique int
	if err := p.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
		  SELECT INDEX_NAME FROM information_schema.STATISTICS
		   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
		   GROUP BY INDEX_NAME
		  HAVING COUNT(*) = 2
		     AND SUM(LOWER(COLUMN_NAME) = 'uuid') = 1
		     AND SUM(LOWER(COLUMN_NAME) = 'step') = 1) u`,
		p.schema, p.name).Scan(&unique); err != nil {
		return classifyMySQL("validate", err)
	}
	if unique == 0 {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on exactly (uuid, step); ON DUPLICATE KEY needs one", p.cfg.Table))
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
	// the probe runs exactly the statement the drain runs. Its uuid is sixteen
	// zero bytes, which the generator can never mint, and the transaction is
	// rolled back, so the table never gains a row -- which is why InnoDB is
	// required above.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return classifyMySQL("validate", err)
	}
	probe := &mysqlArgs{}
	probe.grow(1)
	probe.vals = append(probe.vals,
		probe.ids[0][:], int64(0), "probe", int64(0), int64(0),
		int64(0), int64(0), int64(0), int64(0),
		"rfc5424", "udp", nil, nil, nil, nil, nil, empty, int64(0))
	_, err = tx.ExecContext(ctx, p.insertFor(1), probe.vals...)
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
		// drain runs, with the NULLs a row without hostname carries: if the
		// server refuses it now -- 1048 on a column made NOT NULL, 1364 on an
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
		return &Error{Class: classifyMySQLNumber(me.Number), Op: op + " mysql=" + strconv.Itoa(int(me.Number)), Err: err}
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

// classifyMySQLNumber is the error-number table of SPEC §10.6.
//
// The lines that matter most: schema and privilege errors are FATAL, never
// logic, and a read-only server (1290) is fatal for the same reasons 25006 is on
// PostgreSQL. Unknown numbers are retried, not dropped (invariant §20.18).
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
