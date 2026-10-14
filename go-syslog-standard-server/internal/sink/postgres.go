package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// PostgresConfig mirrors [sink.postgres].
type PostgresConfig struct {
	Addr           string // host:port, or a unix socket directory starting with /
	Username       string
	Password       string
	Database       string
	Table          string // name or schema.name, lowercase; validated by config
	PoolSize       int
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ConnectTimeout time.Duration
	AppName        string // application_name, visible in pg_stat_activity
}

// Postgres is the shared final destination (SPEC §10.5). Schema in §3.6.
//
// It is the only destination that is both remote AND has a schema, and the
// server owns neither: the table is validated at boot and never created
// (§10.5.1), every worker pins a connection out of a budget the whole fleet
// shares (§10.5.3), and the durability behind a confirmation is whatever the
// server's synchronous_commit says (§10.5.6).
type Postgres struct {
	cfg  PostgresConfig
	pool *pgxpool.Pool
	log  *slog.Logger
	m    *obs.Metrics

	// insert is the one statement this sink ever runs: arrays plus unnest,
	// seventeen parameters whatever the batch size, ON CONFLICT DO NOTHING.
	insert string
	// regname is the table as handed to to_regclass, unquoted. Config only
	// admits lowercase identifiers precisely so that this spelling and the
	// quoted one inside insert name the same table.
	regname string

	// args pools the seventeen argument slices per worker, in the same
	// discipline as the raw-line and envelope pools of SPEC §14.
	args sync.Pool

	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64
}

// pgIdent mirrors the config rule; it is checked again here so a caller that
// bypasses config cannot inject SQL through the table name.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// pgInsert is built once per sink. SELECT * FROM unnest(...) with N arrays
// yields N columns in argument order, so the column list and the parameter list
// must stay aligned position by position.
const pgInsert = `INSERT INTO %s ` +
	`(id, ts, nts, facility, severity, hostname, app_name, procid, msgid, ` +
	`peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes) ` +
	`SELECT * FROM unnest($1::uuid[], $2::bigint[], $3::bigint[], $4::smallint[], ` +
	`$5::smallint[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], ` +
	`$11::text[], $12::text[], $13::bigint[], $14::bigint[], $15::bytea[], ` +
	`$16::bytea[], $17::bigint[]) ` +
	`ON CONFLICT (id) DO NOTHING`

// pgColumn is what the boot validation of SPEC §10.5.5 demands of one column.
// Types are as format_type() prints them, with any length modifier stripped.
type pgColumn struct {
	name  string
	types []string
}

var (
	pgTextTypes  = []string{"text", "character varying"}
	pgSmallTypes = []string{"smallint", "integer"}
)

// pgColumns is the schema contract of SPEC §3.6, in table order.
var pgColumns = []pgColumn{
	{"id", []string{"uuid"}},
	{"ts", []string{"bigint"}},
	{"nts", []string{"bigint"}},
	{"facility", pgSmallTypes},
	{"severity", pgSmallTypes},
	{"hostname", pgTextTypes},
	{"app_name", pgTextTypes},
	{"procid", pgTextTypes},
	{"msgid", pgTextTypes},
	{"peer_ip", pgTextTypes},
	{"transport", pgTextTypes},
	{"rfc", pgTextTypes},
	{"msg_offset", []string{"bigint"}},
	{"msg_size", []string{"bigint"}},
	{"env", []string{"bytea"}},
	{"raw", []string{"bytea"}},
	{"nbytes", []string{"bigint"}},
}

var (
	errInRecovery  = errors.New("server is in recovery: a read-only replica, not a primary")
	errMalformedID = errors.New("event id is not a UUID; the batch cannot be inserted")
)

// NewPostgres builds the sink without opening a connection: the pool is lazy,
// and the boot's WaitReady owns the retry loop. The errors here are
// configuration errors -- an address that does not parse, a CA file that does
// not load -- and never a network condition.
func NewPostgres(cfg PostgresConfig, log *slog.Logger, m *obs.Metrics) (*Postgres, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if !pgIdent.MatchString(cfg.Table) {
		return nil, fmt.Errorf("sink postgres: table %q is not a plain lowercase identifier", cfg.Table)
	}
	host, port, err := splitPostgresAddr(cfg.Addr)
	if err != nil {
		return nil, err
	}

	// ParseConfig("") yields the libpq defaults, which also means the libpq
	// ENVIRONMENT (PGHOST, PGSSLMODE, PGPASSFILE...). Every field the registry
	// owns is overwritten below so the environment cannot shadow it; the
	// fallbacks are dropped for the same reason -- otherwise a refused
	// connection to the configured host would be retried against localhost.
	pc, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("sink postgres: %w", err)
	}
	cc := pc.ConnConfig
	cc.Host = host
	cc.Port = port
	cc.User = cfg.Username
	cc.Password = cfg.Password
	cc.Database = cfg.Database
	cc.ConnectTimeout = cfg.ConnectTimeout
	cc.Fallbacks = nil
	cc.TLSConfig = nil
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = map[string]string{}
	}
	cc.RuntimeParams["application_name"] = cfg.AppName
	if cfg.TLS {
		tc, err := buildPostgresTLS(cfg, host)
		if err != nil {
			return nil, err
		}
		cc.TLSConfig = tc
	}
	pc.MaxConns = int32(cfg.PoolSize)
	pc.MinConns = 0

	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, fmt.Errorf("sink postgres: %w", err)
	}

	p := &Postgres{
		cfg:     cfg,
		pool:    pool,
		log:     log,
		m:       m,
		insert:  fmt.Sprintf(pgInsert, pgx.Identifier(strings.Split(cfg.Table, ".")).Sanitize()),
		regname: cfg.Table,
	}
	p.args.New = func() any { return &pgArgs{} }
	return p, nil
}

// splitPostgresAddr accepts host:port, [v6]:port, or a unix socket directory
// with or without a port suffix.
func splitPostgresAddr(addr string) (host string, port uint16, err error) {
	if addr == "" {
		return "", 0, errors.New("sink postgres: addr is empty")
	}
	h, ps, serr := net.SplitHostPort(addr)
	if serr != nil {
		if strings.HasPrefix(addr, "/") {
			return addr, 5432, nil
		}
		return "", 0, fmt.Errorf("sink postgres: addr %q: %w", addr, serr)
	}
	n, perr := strconv.ParseUint(ps, 10, 16)
	if perr != nil || n == 0 {
		return "", 0, fmt.Errorf("sink postgres: addr %q: bad port", addr)
	}
	return h, uint16(n), nil
}

// buildPostgresTLS verifies by default: ServerName is the configured host, so a
// managed database's certificate is checked the way libpq's verify-full does.
func buildPostgresTLS(cfg PostgresConfig, host string) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host}
	if cfg.TLSInsecure {
		tc.InsecureSkipVerify = true
	}
	if cfg.TLSCA != "" {
		pem, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("sink postgres: reading %s: %w", cfg.TLSCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("sink postgres: %s contains no usable certificate", cfg.TLSCA)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func (p *Postgres) Type() string { return "postgres" }

// pgArgs is one batch's worth of column arrays. The slices are plain Go values,
// not commands recycled by a library as in rueidis, so a retry may reuse them
// safely. The hazard is the opposite one: env and raw hold references into the
// items' pooled buffers, so a pgArgs must be CLEARED before it goes back to the
// pool, or it pins buffers that have already been returned -- and the next batch
// would carry another emitter's line (SPEC §10.5.2, §14.2).
type pgArgs struct {
	id        [][16]byte
	ts        []int64
	nts       []int64
	facility  []int16
	severity  []int16
	hostname  []string
	appName   []string
	procID    []string
	msgID     []string
	peerIP    []string
	transport []string
	rfc       []string
	msgOffset []int64
	msgSize   []int64
	env       [][]byte
	raw       [][]byte
	nbytes    []int64
}

// empty stands in for a nil slice. pgx encodes []byte(nil) as NULL, which would
// violate NOT NULL, classify as logic and drop the whole batch.
var empty = []byte{}

func nonNil(b []byte) []byte {
	if b == nil {
		return empty
	}
	return b
}

func (a *pgArgs) add(p *Prepared) bool {
	u, ok := parseUUID(p.Item.Head.ID)
	if !ok {
		return false
	}
	m := &p.Meta
	raw := p.Raw()
	a.id = append(a.id, u)
	a.ts = append(a.ts, m.TS)
	a.nts = append(a.nts, m.NTS)
	a.facility = append(a.facility, int16(m.Facility))
	a.severity = append(a.severity, int16(m.Severity))
	a.hostname = append(a.hostname, pgText(m.Hostname))
	a.appName = append(a.appName, pgText(m.AppName))
	a.procID = append(a.procID, pgText(m.ProcID))
	a.msgID = append(a.msgID, pgText(m.MsgID))
	a.peerIP = append(a.peerIP, m.PeerIP)
	a.transport = append(a.transport, m.Transport)
	a.rfc = append(a.rfc, m.RFC)
	a.msgOffset = append(a.msgOffset, int64(m.MsgOffset))
	a.msgSize = append(a.msgSize, int64(m.MsgSize))
	a.env = append(a.env, nonNil(p.Env))
	a.raw = append(a.raw, nonNil(raw))
	a.nbytes = append(a.nbytes, int64(len(p.Env)+len(raw)))
	return true
}

// reset drops every reference and keeps the capacity.
func (a *pgArgs) reset() {
	clear(a.id)
	clear(a.ts)
	clear(a.nts)
	clear(a.facility)
	clear(a.severity)
	clear(a.hostname)
	clear(a.appName)
	clear(a.procID)
	clear(a.msgID)
	clear(a.peerIP)
	clear(a.transport)
	clear(a.rfc)
	clear(a.msgOffset)
	clear(a.msgSize)
	clear(a.env)
	clear(a.raw)
	clear(a.nbytes)

	a.id, a.ts, a.nts = a.id[:0], a.ts[:0], a.nts[:0]
	a.facility, a.severity = a.facility[:0], a.severity[:0]
	a.hostname, a.appName = a.hostname[:0], a.appName[:0]
	a.procID, a.msgID = a.procID[:0], a.msgID[:0]
	a.peerIP, a.transport, a.rfc = a.peerIP[:0], a.transport[:0], a.rfc[:0]
	a.msgOffset, a.msgSize = a.msgOffset[:0], a.msgSize[:0]
	a.env, a.raw, a.nbytes = a.env[:0], a.raw[:0], a.nbytes[:0]
}

// pgText makes a value safe for a text column.
//
// PostgreSQL rejects invalid UTF-8 and NUL with 22021, a class-22 error that
// drops the WHOLE batch (SPEC §10.5.7). This matters more here than in the
// sibling service: there the promoted columns are a path and a method, here they
// are a hostname and an app-name that came out of vendor firmware. An appliance
// speaking latin-1 is not a remote hypothesis, it is Tuesday -- and one of its
// messages would take 255 innocent ones down with it.
//
// The replacement happens in the queryable COPY only; the intact bytes stay in
// env and raw (invariant §19.7).
func pgText(s string) string {
	if utf8.ValidString(s) && strings.IndexByte(s, 0) < 0 {
		return s
	}
	s = strings.ToValidUTF8(s, "�")
	return strings.ReplaceAll(s, "\x00", "�")
}

// parseUUID decodes the canonical 36-character form into the 16 bytes the uuid
// column takes in binary. Done here rather than by the server (text[] cast to
// uuid[]) so the CPU is spent on this instance, not on the one resource every
// instance shares.
func parseUUID(s string) (u [16]byte, ok bool) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, false
	}
	j := 0
	for i := 0; i < 36; i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		hi, ok1 := hexVal(s[i])
		lo, ok2 := hexVal(s[i+1])
		if !ok1 || !ok2 {
			return u, false
		}
		u[j] = hi<<4 | lo
		j++
		i++
	}
	return u, true
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// Write inserts the whole batch with one statement (SPEC §10.5.2).
//
// ON CONFLICT (id) DO NOTHING is the exact counterpart of the SQLite sink's
// INSERT OR IGNORE: the pipeline is at-least-once, and a redelivered message
// must not take the batch down with it. The rows-affected count says how many
// were already there, which is the only direct measurement of redelivery there
// is.
func (p *Postgres) Write(ctx context.Context, batch []Prepared) error {
	if len(batch) == 0 {
		return nil
	}
	if p.closed.Load() {
		return ErrClosed
	}

	a := p.args.Get().(*pgArgs)
	defer func() {
		a.reset()
		p.args.Put(a)
	}()

	for i := range batch {
		if !a.add(&batch[i]) {
			// Ids come from this process's own generator, so this is a bug, not
			// data. Dropping the batch loudly is better than skipping the item
			// silently and uncounted.
			return logicErr("insert id="+batch[i].Item.Head.ID, errMalformedID)
		}
	}

	tag, err := p.pool.Exec(ctx, p.insert,
		a.id, a.ts, a.nts, a.facility, a.severity, a.hostname, a.appName,
		a.procID, a.msgID, a.peerIP, a.transport, a.rfc, a.msgOffset,
		a.msgSize, a.env, a.raw, a.nbytes)
	if err != nil {
		return classifyPostgres("insert", err)
	}
	if dup := int64(len(batch)) - tag.RowsAffected(); dup > 0 {
		p.m.SinkPostgresDuplicatesTotal.Add(uint64(dup))
	}
	return nil
}

// Health asks whether this server is a primary (SPEC §10.5.5). A trivial query
// would answer happily on a promoted replica and the state would never leave UP;
// pg_is_in_recovery() is what makes a failover visible to the state machine
// within one interval.
//
// The first successful check also runs the one-time schema validation, and every
// check refreshes the pool gauges -- doing that in Write would take the pool's
// lock on the hot path.
func (p *Postgres) Health(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	var inRecovery bool
	if err := p.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return classifyPostgres("health", err)
	}
	p.observePool()
	if inRecovery {
		return networkErr("health", errInRecovery)
	}
	if !p.validated.Load() {
		if err := p.validate(ctx); err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("events table validated", "table", p.cfg.Table)
	}
	return nil
}

// validate is the boot contract of SPEC §10.5.5. Everything it refuses is
// ClassFatal: none of it changes by waiting, and WaitReady stops retrying on a
// fatal answer rather than sitting out sink.boot_timeout.
func (p *Postgres) validate(ctx context.Context) error {
	var readOnly string
	if err := p.pool.QueryRow(ctx,
		"SELECT current_setting('transaction_read_only')").Scan(&readOnly); err != nil {
		return classifyPostgres("validate", err)
	}
	if readOnly == "on" {
		return fatalErr("validate", errors.New(
			"the session is read-only (transaction_read_only=on): a replica, or a server configured that way"))
	}

	rows, err := p.pool.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`,
		p.regname)
	if err != nil {
		return classifyPostgres("validate", err)
	}
	have := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			rows.Close()
			return classifyPostgres("validate", err)
		}
		// "character varying(255)" -> "character varying"
		if i := strings.IndexByte(typ, '('); i > 0 {
			typ = typ[:i]
		}
		have[name] = typ
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classifyPostgres("validate", err)
	}
	if len(have) == 0 {
		return fatalErr("validate", fmt.Errorf(
			"table %s does not exist, or this role cannot see it; apply docker/postgres/schema.sql",
			p.regname))
	}
	for _, c := range pgColumns {
		typ, ok := have[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.6", p.regname, c.name))
		}
		if !contains(c.types, typ) {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is %s, want %s", p.regname, c.name, typ,
				strings.Join(c.types, " or ")))
		}
	}

	// ON CONFLICT (id) needs a unique index over exactly (id) and nothing else.
	// Without it EVERY write fails with 42P10, so it is checked here rather than
	// discovered on the first batch.
	var unique bool
	if err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM pg_catalog.pg_index i
		  JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		  WHERE i.indrelid = to_regclass($1)
		    AND i.indisunique AND i.indnkeyatts = 1 AND i.indpred IS NULL
		    AND a.attname = 'id')`, p.regname).Scan(&unique); err != nil {
		return classifyPostgres("validate", err)
	}
	if !unique {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on (id); ON CONFLICT (id) needs one", p.regname))
	}

	var canInsert bool
	if err := p.pool.QueryRow(ctx,
		"SELECT COALESCE(has_table_privilege(to_regclass($1), 'INSERT'), false)",
		p.regname).Scan(&canInsert); err != nil {
		return classifyPostgres("validate", err)
	}
	if !canInsert {
		return fatalErr("validate", fmt.Errorf(
			"role %q has no INSERT privilege on %s", p.cfg.Username, p.regname))
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// observePool exports the pool occupancy. EmptyAcquireCount is cumulative, so
// only its growth since the last check is added to the counter.
func (p *Postgres) observePool() {
	st := p.pool.Stat()
	p.m.SinkPostgresPoolConns[obs.PoolAcquired].Set(int64(st.AcquiredConns()))
	p.m.SinkPostgresPoolConns[obs.PoolIdle].Set(int64(st.IdleConns()))
	p.m.SinkPostgresPoolConns[obs.PoolMax].Set(int64(st.MaxConns()))
	waits := st.EmptyAcquireCount()
	if prev := p.lastWaits.Swap(waits); waits > prev {
		p.m.SinkPostgresPoolWaitsTotal.Add(uint64(waits - prev))
	}
}

// Close releases the pool. It waits for connections a worker still holds, which
// is fine: the workers are stopped before the sink is closed.
func (p *Postgres) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.pool.Close()
	return nil
}

// classifyPostgres maps a driver error onto the class the state machine acts on.
// The table is SPEC §10.5.7; keep the two in step.
func classifyPostgres(op string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		// No SQLSTATE means the server never answered: a refused dial, a
		// deadline, a broken pipe, a closed pool. All of it is the connection.
		return networkErr(op, err)
	}
	return &Error{classifySQLState(pgErr.Code), op + " sqlstate=" + pgErr.Code, err}
}

// classifySQLState is the SQLSTATE table of SPEC §10.5.7.
//
// The line that matters most: everything in class 42 is FATAL, not logic. A
// dropped table or a missing unique index would otherwise be a logic error --
// and logic means "drop this batch, log it, move on", which against a destination
// that will refuse every batch is silent total loss with a perfectly
// healthy-looking process.
func classifySQLState(code string) Class {
	switch code {
	case "25006": // read_only_sql_transaction: promoted replica, or configured so
		return ClassFatal
	case "53100", "53200", "53400": // disk full, out of memory, config limit
		return ClassFatal
	case "53300": // too_many_connections: somebody else's problem, and transient
		return ClassNetwork
	case "57P01", "57P02", "57P03": // admin shutdown, crash shutdown, cannot connect now
		return ClassNetwork
	case "57P04": // database_dropped
		return ClassFatal
	case "23502":
		// not_null_violation. This server never sends NULL, so it can only
		// come from the table: an extra NOT NULL column without a default, a
		// default dropped after boot, a trigger. Every batch would hit it, and
		// as class 23 (logic) each one would be dropped with the sink UP --
		// the same case as MySQL's 1364, which is fatal (SPEC §10.5.7).
		return ClassFatal
	case "57014": // query_canceled: statement_timeout on the server
		return ClassNetwork
	case "40001", "40P01", "55P03": // serialization, deadlock, lock not available
		return ClassNetwork
	case "28000", "28P01", "3D000": // bad credential, bad password, no such database
		return ClassFatal
	}
	if len(code) < 2 {
		return ClassNetwork
	}
	switch code[:2] {
	case "08": // connection exception
		return ClassNetwork
	case "42": // syntax or access rule: our SQL and their schema disagree
		return ClassFatal
	case "XX": // internal error, data corrupted, index corrupted
		return ClassFatal
	case "22", "23", "54": // data exception, constraint violation, program limit
		return ClassLogic
	}
	// Unknown is retried rather than dropped: a filling ring is visible, a
	// discarded batch is not (invariant §19.17).
	return ClassNetwork
}
