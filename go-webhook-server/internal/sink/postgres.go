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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
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
	// ValidateTimeout bounds the one-time schema validation of SPEC §8.5.5:
	// sink.op_timeout, not the health check's deadline. Zero takes
	// ConnectTimeout.
	ValidateTimeout time.Duration
	AppName         string // application_name, visible in pg_stat_activity
}

// Postgres enqueues into the LogHub queue loghub.inbox (SPEC §3.6, §8.5).
//
// It is remote AND has a schema, and the server owns neither: the table is
// validated at boot and never created (§8.5.1), every worker pins a connection
// out of a budget the whole fleet shares (§8.5.3), and the durability behind a
// 200 is whatever the server's synchronous_commit says (§8.5.6). The queue is
// shared with every other LogHub producer, and its consumer -- DELETE ... FOR
// UPDATE SKIP LOCKED in uuid order -- is somebody else's program.
type Postgres struct {
	cfg  PostgresConfig
	pool *pgxpool.Pool
	log  *slog.Logger
	m    *obs.Metrics

	// insert is the one statement this sink ever runs: ten arrays plus
	// unnest, ten parameters whatever the batch size, ON CONFLICT (uuid) DO
	// NOTHING.
	insert string
	// regname is the table as handed to to_regclass, unquoted. Config only
	// admits lowercase identifiers precisely so that this spelling and the
	// quoted one inside insert name the same table.
	regname string

	// args pools the ten argument slices per worker, in the same discipline
	// as the envelope and body pools of SPEC §12.
	args sync.Pool

	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64

	// probe is the health check's own connection, OUTSIDE the pool (SPEC
	// §8.5.8), dialled on the first check and again after any failure;
	// probeCfg is the pool's connection configuration, copied before the pool
	// took it. With the check inside the pool, writers that held every
	// connection -- slow, not failing -- left it waiting for one past
	// sink.health_timeout: DOWN, then FATAL and exit 3 with nothing wrong
	// with the database. pool_size >= sink.workers + 1 kept a connection for
	// it with a ring, but with ring=none the writers are the handlers, up to
	// ring.max_inflight, and no pool size the configuration can demand keeps
	// one free (independent review of go-relplog-pg-server and of the port to
	// go-syslog-server, 2026-10-13). probeMu serialises the checks on it.
	probeMu  sync.Mutex
	probe    *pgx.Conn
	probeCfg *pgx.ConnConfig
}

// pgIdent mirrors the config rule; it is checked again here so a caller that
// bypasses config cannot inject SQL through the table name.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// pgInsert is built once per sink. SELECT * FROM unnest(...) with N arrays
// yields N columns in argument order, so the column list and the parameter
// list must stay aligned position by position -- pgColumns is the same list,
// and a test holds them together.
//
// ON CONFLICT (uuid) and not (uuid, step): the queue's key is the uuid alone
// (skill loghub-postgres-table-inbox), and this bridge-in only emits step 0, so
// the two keys are the same here (SPEC §3.6). DO NOTHING also absorbs a uuid
// repeated inside one statement; DO UPDATE would not.
const pgInsert = `INSERT INTO %s ` +
	`(uuid, from_uuid, space, type, level, sid, step, event_at, event_nsec, payload) ` +
	`SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::smallint[], ` +
	`$6::bigint[], $7::integer[], $8::timestamptz[], $9::smallint[], $10::jsonb[]) ` +
	`ON CONFLICT (uuid) DO NOTHING`

// pgColumn is what the boot validation of SPEC §8.5.5 demands of one column.
// Types are as format_type(type, NULL) prints them: the base type, without
// the modifier, which pg_attribute.atttypmod carries apart. Cutting the
// printed type at '(' was rejected: timestamptz(6) prints as
// "timestamp(6) with time zone" and became "timestamp", refused with a
// message that sent the operator after the wrong problem.
type pgColumn struct {
	name  string
	types []string
}

var (
	pgTextTypes = []string{"text", "character varying"}
	// level, step and event_nsec fit any integer width; sid does not.
	pgIntTypes = []string{"smallint", "integer", "bigint"}
)

// pgMinChars is the length a character varying column must allow: a shorter
// one would refuse every row as 22001, a logic drop with the sink UP. space has
// up to 64 characters (SPEC §3.0), and type (envelope.Type) follows the same
// grammar.
var pgMinChars = map[string]int{"space": 64, "type": 64}

// pgColumns is the part of the loghub.inbox contract this server writes (SPEC
// §3.6), in statement order. received_at is not here: the table fills it.
var pgColumns = []pgColumn{
	{"uuid", []string{"uuid"}},
	{"from_uuid", []string{"uuid"}},
	{"space", pgTextTypes},
	{"type", pgTextTypes},
	{"level", pgIntTypes},
	// bigint only: a narrower column refuses the sid of a busy host as 22003
	// on every row, with the sink UP (the PID tops out at 2^22 on Linux, but
	// sid is up to 2^53-1 by contract).
	{"sid", []string{"bigint"}},
	{"step", pgIntTypes},
	{"event_at", []string{"timestamp with time zone"}},
	{"event_nsec", pgIntTypes},
	{"payload", []string{"jsonb"}},
}

var errInRecovery = errors.New("server is in recovery: a read-only replica, not a primary")

// NewPostgres builds the sink without opening a connection: the pool is lazy,
// and the boot's WaitReady owns the retry loop (SPEC §8.5.2). The errors here
// are configuration errors -- an address that does not parse, a CA file that
// does not load -- and never a network condition.
func NewPostgres(cfg PostgresConfig, log *slog.Logger, m *obs.Metrics) (*Postgres, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.ValidateTimeout <= 0 {
		cfg.ValidateTimeout = cfg.ConnectTimeout
	}
	if !pgIdent.MatchString(cfg.Table) {
		return nil, fmt.Errorf("sink postgres: table %q is not a plain lowercase identifier", cfg.Table)
	}
	host, port, err := splitPostgresAddr(cfg.Addr)
	if err != nil {
		return nil, err
	}

	// ParseConfig reads the libpq ENVIRONMENT (PGHOST, PGSSLMODE, PGSERVICE,
	// PGCHANNELBINDING, PGCONNECT_TIMEOUT...), so it runs with every PG*
	// hidden (SPEC §8.5.2, W pendência 15): an invalid PGSSLMODE or a missing
	// PGSERVICE failed the parse (exit 1), and the fields nothing below
	// overwrites (channel binding, require_auth, sslnegotiation, the protocol
	// versions) refused every connection with no SQLSTATE. Every field the
	// registry owns is still overwritten below; the fallbacks are dropped --
	// otherwise a refused connection to the configured host would be retried
	// against localhost or in another sslmode --, the runtime parameters are
	// rebuilt from nothing, and ValidateConnect goes too.
	pc, err := parseWithoutLibpqEnv("sslmode=disable")
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
	cc.DialFunc = dialPostgres
	cc.Fallbacks = nil
	cc.TLSConfig = nil
	cc.ValidateConnect = nil
	// client_encoding is pinned: pgx does not set it, so the session would
	// take the database's, and a LATIN1 database stores the UTF-8 the JLH is
	// made of double-encoded, without an error (SPEC §8.5.5). The boot also
	// refuses a server_encoding other than UTF8.
	cc.RuntimeParams = map[string]string{"client_encoding": "UTF8", "application_name": cfg.AppName}
	if cfg.TLS {
		tc, err := buildPostgresTLS(cfg, host)
		if err != nil {
			return nil, err
		}
		cc.TLSConfig = tc
	}
	pc.MaxConns = int32(cfg.PoolSize)
	pc.MinConns = 0
	probeCfg := cc.Copy()

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
	p.probeCfg = probeCfg
	p.args.New = func() any { return &pgArgs{} }
	return p, nil
}

// libpqEnvMu serialises parseWithoutLibpqEnv: the environment is process-wide.
var libpqEnvMu sync.Mutex

// parseWithoutLibpqEnv is pgxpool.ParseConfig with every PG* environment
// variable hidden for the duration of the parse and restored after it. The
// parse is the only place pgx reads them, it runs once, at boot (NewPostgres),
// and nothing else in this process reads PG* -- so the window is harmless,
// and it is the only way to keep the ones a conninfo cannot neutralise
// (PGSERVICE pulls in a whole service file) from reaching the pool. The
// alternative, a conninfo naming every key with its default, breaks with the
// first variable a pgx release adds. (From go-syslog-server.)
func parseWithoutLibpqEnv(conninfo string) (*pgxpool.Config, error) {
	libpqEnvMu.Lock()
	defer libpqEnvMu.Unlock()
	saved := map[string]string{}
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, "PG") {
			saved[k] = v
			os.Unsetenv(k)
		}
	}
	defer func() {
		for k, v := range saved {
			os.Setenv(k, v)
		}
	}()
	return pgxpool.ParseConfig(conninfo)
}

// dialPostgres is the pool's dialer, with no timeout of its own: the connect
// deadline is ConnConfig.ConnectTimeout (sink.op_timeout), which pgconn
// applies through the context. The DialFunc pgx builds for a connect_timeout
// setting (PGCONNECT_TIMEOUT, makeConnectTimeoutDialFunc) carries that value
// in its net.Dialer and cut every dial short of the configured deadline; the
// hidden environment already keeps it out, and this keeps the dialer ours
// whatever a later pgx reads (SPEC §8.5.2).
func dialPostgres(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
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

// pgArgs is one batch's worth of column arrays. The slices are plain Go
// values, not commands recycled by a library as in rueidis (SPEC §8.2.3), so a
// retry may reuse them safely. The hazard is the opposite one: payload holds
// references into the items' pooled buffers, so a pgArgs must be CLEARED before
// it goes back to the pool, or it pins buffers that have already been returned
// -- and the next batch would carry another client's envelope (SPEC §8.5.4,
// §12.3).
type pgArgs struct {
	uuid      [][16]byte
	from      [][16]byte
	space     []string
	typ       []string
	level     []int16
	sid       []int64
	step      []int32
	eventAt   []time.Time
	eventNsec []int16
	payload   [][]byte
}

func (a *pgArgs) add(it *envelope.Item) error {
	r, err := inboxRowOf(it)
	if err != nil {
		return err
	}
	a.uuid = append(a.uuid, r.UUID)
	a.from = append(a.from, r.From)
	a.space = append(a.space, r.Space)
	a.typ = append(a.typ, r.Type)
	a.level = append(a.level, r.Level)
	a.sid = append(a.sid, r.SID)
	a.step = append(a.step, r.Step)
	a.eventAt = append(a.eventAt, r.EventAt)
	a.eventNsec = append(a.eventNsec, r.EventNsec)
	a.payload = append(a.payload, jsonbSafe(r.Payload))
	return nil
}

// reset drops every reference and keeps the capacity.
func (a *pgArgs) reset() {
	clear(a.uuid)
	clear(a.from)
	clear(a.space)
	clear(a.typ)
	clear(a.level)
	clear(a.sid)
	clear(a.step)
	clear(a.eventAt)
	clear(a.eventNsec)
	clear(a.payload)
	a.uuid, a.from, a.space, a.typ = a.uuid[:0], a.from[:0], a.space[:0], a.typ[:0]
	a.level, a.sid, a.step = a.level[:0], a.sid[:0], a.step[:0]
	a.eventAt, a.eventNsec, a.payload = a.eventAt[:0], a.eventNsec[:0], a.payload[:0]
}

// values returns the ten parameters in pgInsert order.
func (a *pgArgs) values() []any {
	return []any{a.uuid, a.from, a.space, a.typ, a.level, a.sid, a.step,
		a.eventAt, a.eventNsec, a.payload}
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

// Write enqueues the whole batch with one statement (SPEC §8.5.4).
//
// ON CONFLICT (uuid) DO NOTHING is the queue's idempotence: the pipeline is
// at-least-once, and a redelivered event must not take the batch down with it. The rows-affected count says how many were
// already there, which is the only direct measurement of redelivery there is.
func (p *Postgres) Write(ctx context.Context, items []*envelope.Item) error {
	if len(items) == 0 {
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

	for _, it := range items {
		// An envelope that does not map is damage, not data this server
		// wrote: the batch is dropped loudly rather than the item skipped
		// silently and uncounted.
		if err := a.add(it); err != nil {
			return err
		}
	}

	tag, err := p.pool.Exec(ctx, p.insert, a.values()...)
	if err != nil {
		return classifyPostgres("insert", err)
	}
	if dup := int64(len(items)) - tag.RowsAffected(); dup > 0 {
		p.m.SinkPostgresDuplicatesTotal.Add(uint64(dup))
	}
	return nil
}

// Health asks whether this server is a primary (SPEC §8.5.8). A trivial query
// would answer happily on a promoted replica and the state would never leave
// UP; pg_is_in_recovery() is what makes a failover visible to the state
// machine within one interval.
//
// The first successful check also runs the one-time schema validation of
// SPEC §8.5.5, and every check refreshes the pool gauges -- doing that in
// Write would take the pool's lock on the hot path.
func (p *Postgres) Health(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	var inRecovery bool
	if err := p.probeScan(ctx, "SELECT pg_is_in_recovery()", &inRecovery); err != nil {
		return classifyPostgres("health", err)
	}
	p.observePool()
	if inRecovery {
		return networkErr("health", errInRecovery)
	}
	if !p.validated.Load() {
		vctx, cancel := validationContext(ctx, p.cfg.ValidateTimeout)
		err := p.validate(vctx)
		cancel()
		if err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("queue table validated", "table", p.cfg.Table)
	}
	return nil
}

// probeScan runs one query of the health check on its own connection (see
// Postgres.probe) and scans the row into dest. A connection that fails is
// dropped, so the next check dials afresh; one that was reused and failed is
// retried once on a fresh dial, inside the same deadline, because a
// connection idle for a whole sink.health_interval may simply have died under
// it (a proxy's idle timeout, a server restart) and the pool used to hide
// that by pinging before reuse. A failure on a fresh connection is the
// answer.
func (p *Postgres) probeScan(ctx context.Context, sql string, dest ...any) error {
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	for {
		if p.closed.Load() {
			return ErrClosed
		}
		fresh := false
		if p.probe == nil {
			c, err := pgx.ConnectConfig(ctx, p.probeCfg)
			if err != nil {
				return err
			}
			p.probe, fresh = c, true
		}
		err := p.probe.QueryRow(ctx, sql).Scan(dest...)
		if err == nil {
			return nil
		}
		p.dropProbe()
		if fresh || ctx.Err() != nil {
			return err
		}
	}
}

// dropProbe closes the health check's connection; the caller holds probeMu.
func (p *Postgres) dropProbe() {
	if p.probe == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = p.probe.Close(ctx)
	cancel()
	p.probe = nil
}

// validate is the boot contract of SPEC §8.5.5. Everything it refuses is
// ClassFatal: none of it changes by waiting, and WaitReady stops retrying on a
// fatal answer rather than sitting out sink.boot_timeout.
func (p *Postgres) validate(ctx context.Context) error {
	var readOnly, encoding string
	if err := p.pool.QueryRow(ctx,
		"SELECT current_setting('transaction_read_only'), current_setting('server_encoding')").Scan(
		&readOnly, &encoding); err != nil {
		return classifyPostgres("validate", err)
	}
	if readOnly == "on" {
		return fatalErr("validate", errors.New(
			"the session is read-only (transaction_read_only=on): a replica, or a server configured that way"))
	}
	// A LATIN1 or SQL_ASCII database cannot hold what the JLH carries: the
	// text would be converted, or refused as 22P05 for an escape jsonb cannot
	// store in that encoding, a logic drop of the whole batch.
	if encoding != "UTF8" {
		return fatalErr("validate", fmt.Errorf(
			"the database encoding is %s; loghub.inbox needs a UTF8 database", encoding))
	}

	// A partitioned table is refused: a row with no partition for it is
	// 23514 at write time, every batch a logic drop with the sink UP. The
	// queue is a plain table (skill loghub-postgres-table-inbox); it is
	// loghub.envelope, with the same columns, that is partitioned.
	var relkind string
	if err := p.pool.QueryRow(ctx,
		"SELECT COALESCE((SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = to_regclass($1)), '')",
		p.regname).Scan(&relkind); err != nil {
		return classifyPostgres("validate", err)
	}
	if relkind == "p" {
		return fatalErr("validate", fmt.Errorf(
			"table %s is partitioned; the queue is a plain table (apply docker/postgres/inbox.sql)", p.regname))
	}

	// required is a column every INSERT must fill: NOT NULL with no default,
	// no identity and no generation expression. The sink fills exactly the
	// ten columns of the contract, so an EXTRA required column refuses every
	// row (23502). received_at is not one: the table's own DEFAULT now().
	rows, err := p.pool.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, NULL), a.atttypmod,
		       a.attnotnull AND NOT a.atthasdef AND a.attidentity = '' AND a.attgenerated = ''
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`,
		p.regname)
	if err != nil {
		return classifyPostgres("validate", err)
	}
	type pgAttr struct {
		typ      string
		typmod   int32 // character varying(n): n+4; timestamptz(p): p; -1 when unbounded
		required bool
	}
	have := map[string]pgAttr{}
	for rows.Next() {
		var name string
		var a pgAttr
		if err := rows.Scan(&name, &a.typ, &a.typmod, &a.required); err != nil {
			rows.Close()
			return classifyPostgres("validate", err)
		}
		have[name] = a
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return classifyPostgres("validate", err)
	}
	if len(have) == 0 {
		return fatalErr("validate", fmt.Errorf(
			"table %s does not exist, or this role cannot see it; apply docker/postgres/inbox.sql",
			p.regname))
	}
	// Required columns are checked here, from the catalogue, and not by a
	// probe INSERT rolled back as on MySQL: a probe would fire the operator's
	// triggers and consume the sequences of identity or serial columns, and its
	// refusal would not say which column. An extra NOT NULL column is the same
	// trap as a missing one: the table accepts the boot, then every batch is
	// 23502, class 23, a logic drop every few milliseconds with the sink UP.
	// The CHECK constraints of the inbox are not inspected: what this server
	// produces satisfies every one of them (SPEC §3.6).
	contract := map[string]bool{}
	for _, c := range pgColumns {
		contract[c.name] = true
		a, ok := have[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.6", p.regname, c.name))
		}
		if !contains(c.types, a.typ) {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is %s, want %s", p.regname, c.name, a.typ,
				strings.Join(c.types, " or ")))
		}
		if min := pgMinChars[c.name]; a.typ == "character varying" && a.typmod >= 0 && int(a.typmod)-4 < min {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is character varying(%d), shorter than the %d characters it must hold",
				p.regname, c.name, a.typmod-4, min))
		}
		// event_at holds the microsecond (SPEC §3.6): timestamptz or
		// timestamptz(6). A lower precision ROUNDS the value, with no error,
		// and the row stores an instant the envelope never had.
		if a.typ == "timestamp with time zone" && a.typmod >= 0 && a.typmod != 6 {
			return fatalErr("validate", fmt.Errorf(
				"table %s: column %q is timestamp(%d) with time zone; a precision below 6 rounds the "+
					"microseconds of every event in silence (want timestamptz or timestamptz(6))",
				p.regname, c.name, a.typmod))
		}
	}
	for name, a := range have {
		if !contract[name] && a.required {
			return fatalErr("validate", fmt.Errorf(
				"table %s: extra column %q is NOT NULL with no default, identity or generation "+
					"expression; this server never fills it, so every row would be refused", p.regname, name))
		}
	}

	// ON CONFLICT (uuid) needs a unique index over exactly (uuid), without a
	// predicate or an expression, immediate (a DEFERRABLE constraint is no
	// arbiter: 55000 on every write, retried forever) and valid (a failed
	// CREATE UNIQUE INDEX CONCURRENTLY leaves one that is not). Without it EVERY write fails with 42P10, so
	// it is checked here rather than discovered on the first batch. A unique
	// index over (uuid, step) does not qualify: it admits two rows per uuid.
	var unique bool
	if err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM pg_catalog.pg_index i
		  JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		  WHERE i.indrelid = to_regclass($1)
		    AND i.indisunique AND i.indnkeyatts = 1
		    AND i.indpred IS NULL AND i.indexprs IS NULL
		    AND i.indimmediate AND i.indisvalid AND i.indisready
		    AND a.attname = 'uuid')`, p.regname).Scan(&unique); err != nil {
		return classifyPostgres("validate", err)
	}
	if !unique {
		return fatalErr("validate", fmt.Errorf(
			"table %s has no unique index on exactly (uuid); ON CONFLICT (uuid) needs one", p.regname))
	}

	// ... and no other unique index or exclusion constraint. ON CONFLICT
	// (uuid) absorbs a collision on its arbiter only: a new event that
	// collides on UNIQUE (from_uuid, sid), say, fails the whole multi-row
	// INSERT with 23505 (an exclusion constraint with 23P01), class 23, a
	// logic drop of the batch on every collision with the sink UP. A
	// DEFERRABLE unique index on (uuid) next to the primary key is worse: the
	// inference picks it too and every write fails with 55000, retried
	// forever. The queue carries the primary key's index alone (skill
	// loghub-postgres-table-inbox, SPEC §3.6). An index that is not ready is
	// not maintained by inserts and is ignored; an invalid but ready one (a
	// failed CREATE UNIQUE INDEX CONCURRENTLY) still enforces uniqueness, and
	// counts.
	var extra string
	if err := p.pool.QueryRow(ctx, `
		SELECT COALESCE((
		  SELECT def FROM (
		    SELECT pg_get_indexdef(i.indexrelid) AS def
		      FROM pg_catalog.pg_index i
		     WHERE i.indrelid = to_regclass($1) AND i.indisunique AND i.indisready
		       AND NOT (i.indnkeyatts = 1 AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indimmediate
		                AND i.indkey[0] = (SELECT a.attnum FROM pg_catalog.pg_attribute a
		                                    WHERE a.attrelid = i.indrelid AND a.attname = 'uuid'
		                                      AND NOT a.attisdropped))
		    UNION ALL
		    SELECT 'CONSTRAINT ' || quote_ident(conname) || ' ' || pg_get_constraintdef(oid)
		      FROM pg_catalog.pg_constraint
		     WHERE conrelid = to_regclass($1) AND contype = 'x'
		  ) x ORDER BY def LIMIT 1), '')`, p.regname).Scan(&extra); err != nil {
		return classifyPostgres("validate", err)
	}
	if extra != "" {
		return fatalErr("validate", fmt.Errorf(
			"table %s has a unique index or exclusion constraint besides the arbiter of ON CONFLICT (uuid): "+
				"%s -- a new event colliding on it fails the whole batch (23505, 23P01; a DEFERRABLE one on "+
				"uuid fails every batch with 55000); the queue carries the primary key's index alone "+
				"(docs/SPEC.md §8.5.5)", p.regname, extra))
	}

	// No INSERT or UPDATE rule. PostgreSQL refuses "INSERT ... ON CONFLICT"
	// on a table with one (0A000, feature_not_supported): every batch would
	// fail with the sink UP and /readyz green (go-relplog-pg-server's
	// independent review, 2026-10-13). A rule ON DELETE or ON SELECT (a
	// view's) does not touch the INSERT and is left alone.
	var rules string
	if err := p.pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT string_agg(quote_ident(rulename), ', ' ORDER BY rulename)
		                   FROM pg_catalog.pg_rewrite
		                  WHERE ev_class = to_regclass($1) AND ev_type IN ('2', '3')), '')`,
		p.regname).Scan(&rules); err != nil {
		return classifyPostgres("validate", err)
	}
	if rules != "" {
		return fatalErr("validate", fmt.Errorf(
			"table %s has an INSERT or UPDATE rule (%s); INSERT ... ON CONFLICT cannot run on such a "+
				"table (0A000): drop the rule, or use a trigger (docs/SPEC.md §8.5.5)", p.regname, rules))
	}

	// INSERT, and SELECT on uuid: ON CONFLICT (uuid) reads the arbiter
	// column, and a role with INSERT alone fails the first batch with 42501.
	var canInsert, canSelectUUID bool
	if err := p.pool.QueryRow(ctx, `
		SELECT COALESCE(has_table_privilege(to_regclass($1), 'INSERT'), false),
		       COALESCE(has_column_privilege(to_regclass($1), 'uuid', 'SELECT'), false)`,
		p.regname).Scan(&canInsert, &canSelectUUID); err != nil {
		return classifyPostgres("validate", err)
	}
	if !canInsert || !canSelectUUID {
		return fatalErr("validate", fmt.Errorf(
			"role %q needs INSERT and SELECT (uuid) on %s -- ON CONFLICT (uuid) reads the arbiter "+
				"column: GRANT INSERT, SELECT (uuid) ON %s TO ...", p.cfg.Username, p.regname, p.regname))
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

// observePool exports the pool occupancy of SPEC §8.5.10. EmptyAcquireCount is
// cumulative, so only its growth since the last check is added to the counter.
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

// Close releases the pool and the health check's connection. It waits for
// connections a worker still holds, which is fine: the workers are stopped
// before the sink is closed.
func (p *Postgres) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.pool.Close()
	p.probeMu.Lock()
	p.dropProbe()
	p.probeMu.Unlock()
	return nil
}

// classifyPostgres maps a driver error onto the class the state machine acts
// on. The table is SPEC §8.5.7; keep the two in step.
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
	class := classifySQLState(pgErr.Code)
	if pgErr.Code == "23514" && pgErr.ConstraintName == "" {
		// A check violation that names no constraint is "no partition of
		// relation ... found for row": the table, not this data. Every batch
		// would fail the same way, so it is fatal, never logic. A real CHECK
		// always carries its name.
		class = ClassFatal
	}
	return &Error{Class: class, Op: op + " sqlstate=" + pgErr.Code, Err: err}
}

// classifySQLState is the SQLSTATE table of SPEC §8.5.7.
//
// The line that matters most: everything in class 42 is FATAL, not logic. A
// dropped table or a missing unique index would otherwise be a logic error --
// and logic means "drop this batch, log it, move on", which against a
// destination that will refuse every batch is silent total loss with a
// perfectly healthy-looking process.
func classifySQLState(code string) Class {
	switch code {
	case "25006": // read_only_sql_transaction: promoted replica, or configured so
		return ClassFatal
	case "0A000":
		// feature_not_supported: what INSERT ... ON CONFLICT gets from a table
		// with an INSERT or UPDATE rule, refused at boot by validate. A rule
		// created after the boot fails every batch the same way; as network
		// it was retried forever with the sink UP (go-relplog-pg-server's
		// independent review, 2026-10-13).
		return ClassFatal
	case "23502":
		// not_null_violation is class 23 but never THIS data: the sink fills
		// all ten columns of the contract and none of its values is NULL, so a
		// 23502 after the boot comes from the TABLE -- a required column added,
		// a DEFAULT dropped, a trigger writing NULL. As logic it would drop
		// every batch, one every few milliseconds, with the sink UP (SPEC
		// §8.5.7; go-syslog-pg-server, 2026-10-13).
		return ClassFatal
	case "53100", "53200", "53400": // disk full, out of memory, config limit
		return ClassFatal
	case "53300": // too_many_connections: somebody else's problem, and transient
		return ClassNetwork
	case "57P01", "57P02", "57P03": // admin shutdown, crash shutdown, cannot connect now
		return ClassNetwork
	case "57P04": // database_dropped
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
	// discarded batch is not.
	return ClassNetwork
}
