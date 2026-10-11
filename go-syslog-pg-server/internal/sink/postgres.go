package sink

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// DefaultPostgresTable is the LogHub queue of SPEC §3.7.1.
const DefaultPostgresTable = "loghub.inbox"

// PostgresConfig mirrors [sink.postgres].
type PostgresConfig struct {
	Addr           string // host:port, or a unix socket directory starting with /
	Username       string
	Password       string
	Database       string
	Table          string // name or schema.name, lowercase
	PoolSize       int
	ConnectTimeout time.Duration
	AppName        string // application_name, visible in pg_stat_activity
}

// Postgres enqueues into the LogHub queue loghub.inbox (SPEC §3.7.1, §10.5).
//
// It is remote AND has a schema, and the server owns neither: the table is
// validated at boot and never created (invariant §20.17), every worker takes a
// connection out of a budget the whole fleet shares, and the durability behind
// a confirmation is whatever the server's synchronous_commit says. The queue is
// shared with every other LogHub producer, and its consumer -- DELETE ... FOR
// UPDATE SKIP LOCKED in uuid order -- is somebody else's program.
type Postgres struct {
	cfg  PostgresConfig
	pool *pgxpool.Pool
	log  *slog.Logger
	m    *obs.Metrics

	// insert is the one statement this sink ever runs: ten arrays plus
	// unnest, ten parameters whatever the chunk size, ON CONFLICT (uuid) DO
	// NOTHING.
	insert string
	// regname is the table as handed to to_regclass, unquoted. Only lowercase
	// identifiers are admitted precisely so that this spelling and the quoted
	// one inside insert name the same table.
	regname string

	// args pools the ten argument slices, in the same discipline as the
	// raw-line and chunk pools of SPEC §15: they go back ZEROED.
	args sync.Pool

	validated atomic.Bool
	closed    atomic.Bool
	lastWaits atomic.Int64
}

// pgIdent is checked here so a caller that bypasses config cannot inject SQL
// through the table name.
var pgIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)

// pgInsert is built once per sink. SELECT * FROM unnest(...) with N arrays
// yields N columns in argument order, so the column list and the parameter list
// must stay aligned position by position -- pgColumns is the same list, and a
// test holds them together.
//
// ON CONFLICT (uuid) and not (uuid, step): the queue's key is the uuid alone
// (skill loghub-postgres-table-inbox), and this Bridge-IN only emits step 0, so
// the two keys are the same here (SPEC §3.7.1). DO NOTHING also absorbs a uuid
// repeated inside one statement; DO UPDATE would not.
const pgInsert = `INSERT INTO %s ` +
	`(uuid, from_uuid, space, type, level, sid, step, event_at, event_nsec, payload) ` +
	`SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::smallint[], ` +
	`$6::bigint[], $7::integer[], $8::timestamptz[], $9::smallint[], $10::jsonb[]) ` +
	`ON CONFLICT (uuid) DO NOTHING`

// pgColumn is what the boot validation of SPEC §10.5 demands of one column.
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
	// A character varying(n) is accepted only when n holds the longest value
	// the column can get (pgMinChars): a shorter one would refuse every row
	// as 22001, a logic drop with the sink UP.
	pgTextTypes = []string{"text", "character varying"}
	// level, step and event_nsec fit any integer width; sid does not.
	pgIntTypes = []string{"smallint", "integer", "bigint"}
)

// pgMinChars is the length a character varying column must allow: space has
// up to 64 characters (SPEC §13.2), type is "syslog".
var pgMinChars = map[string]int{"space": 64, "type": 64}

// pgColumns is the part of the loghub.inbox contract this server writes (SPEC
// §3.7.1), in statement order. received_at is not here: the table fills it.
var pgColumns = []pgColumn{
	{"uuid", []string{"uuid"}},
	{"from_uuid", []string{"uuid"}},
	{"space", pgTextTypes},
	{"type", pgTextTypes},
	{"level", pgIntTypes},
	// bigint only: the sid of a network message is a 53-bit hash, and a
	// narrower column would refuse nearly every chunk as 22003 with the sink
	// UP (SPEC §20.18).
	{"sid", []string{"bigint"}},
	{"step", pgIntTypes},
	{"event_at", []string{"timestamp with time zone"}},
	{"event_nsec", pgIntTypes},
	{"payload", []string{"jsonb"}},
}

var errInRecovery = errors.New("server is in recovery: a read-only replica, not a primary")

// NewPostgres builds the sink without opening a connection: the pool is lazy,
// and the boot's WaitReady owns the retry loop. The errors here are
// configuration errors -- an address that does not parse -- and never a
// network condition.
func NewPostgres(cfg PostgresConfig, log *slog.Logger, m *obs.Metrics) (*Postgres, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.Table == "" {
		cfg.Table = DefaultPostgresTable
	}
	if !pgIdent.MatchString(cfg.Table) {
		return nil, fmt.Errorf("sink postgres: table %q is not a plain lowercase identifier", cfg.Table)
	}
	host, port, err := splitPostgresAddr(cfg.Addr)
	if err != nil {
		return nil, err
	}

	// The pool is built with the libpq ENVIRONMENT out of reach (SPEC §10.5).
	// ParseConfig reads PGHOST, PGSSLMODE, PGCHANNELBINDING, PGREQUIREAUTH,
	// PGSERVICE, PGTARGETSESSIONATTRS, PG{MIN,MAX}PROTOCOLVERSION... and
	// overwriting the fields afterwards is not enough: a PGSSLMODE=bogus or an
	// unreadable PGSSLROOTCERT fails the parse itself (exit 1 over TLS
	// settings, in a server without TLS), and PGCHANNELBINDING=require or
	// PGREQUIREAUTH=md5 refuse every connection with no SQLSTATE -- retried as
	// network until sink.boot_timeout. So the parse runs with every PG*
	// variable hidden, on an explicit sslmode=disable, and the fields the
	// registry owns are then set below. The fallbacks are dropped too --
	// otherwise a refused connection to the configured host would be retried
	// against localhost --, the runtime parameters are rebuilt from nothing,
	// and ValidateConnect goes.
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
	cc.Fallbacks = nil
	// No TLS, ever (SPEC §1.2): the connection is sslmode=disable whatever
	// PGSSLMODE says. A nil TLSConfig is plaintext in pgconn, and with the
	// fallbacks gone there is no second attempt that could negotiate one. A
	// server whose pg_hba.conf only has hostssl lines refuses the login with
	// class 28, which is fatal at boot with the server's own words.
	cc.TLSConfig = nil
	cc.ValidateConnect = nil
	// client_encoding is pinned: pgx does not set it, so the session would
	// take the database's, and a LATIN1 database stores the UTF-8 the JLH is
	// made of double-encoded, without an error (SPEC §10.5). The boot also
	// refuses a server_encoding other than UTF8.
	cc.RuntimeParams = map[string]string{"client_encoding": "UTF8"}
	if cfg.AppName != "" {
		cc.RuntimeParams["application_name"] = cfg.AppName
	}
	// pool_size is this instance's share of a connection budget the whole fleet
	// divides (SPEC §10.5). MinConns = 0 keeps the pool lazy: NewWithConfig
	// dials nothing, the first acquire does, and the boot's WaitReady owns the
	// retry of that first dial.
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

// libpqEnvMu serialises parseWithoutLibpqEnv: the environment is process-wide.
var libpqEnvMu sync.Mutex

// parseWithoutLibpqEnv is pgxpool.ParseConfig with every PG* environment
// variable hidden for the duration of the parse and restored after it. The
// parse is the only place pgx reads them, it happens once, at boot, and
// nothing else in this process reads PG* -- so the window is harmless, and it
// is the only way to keep the ones a conninfo cannot override (PGSERVICE)
// from reaching the pool.
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

// Type is the configured name, for logs and syslog_config_info.
func (p *Postgres) Type() string { return "postgres" }

// pgArgs is one chunk's worth of column arrays. The slices are plain Go values,
// not commands recycled by a library as in rueidis, so a retry may reuse them
// safely. The hazard is the opposite one: payload holds references into the
// chunk buffer, so a pgArgs must be ZEROED before it goes back to the pool, or
// it pins a buffer that has already been returned -- and the next chunk would
// carry another emitter's envelope (SPEC §10.5).
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

	// fromText and fromUUID cache the last from parsed: every envelope of a
	// process carries the same AgentUUID, so the parse runs once per chunk.
	fromText string
	fromUUID [16]byte
}

const syslogType = "syslog"

// add appends one envelope. The error is a logic one: an envelope this sink
// cannot map is refused by this data, not by the destination.
func (a *pgArgs) add(e *Envelope) error {
	h := &e.Item.Head
	if h.From != a.fromText || a.fromText == "" {
		u, err := uuidv7.FromString(h.From)
		if err != nil || !u.IsValid() {
			// The queue's from_uuid CHECK wants version 7 (go-loghub-ident
			// always makes one); the emergency reader refuses anything else,
			// so this is a last line of defence.
			return logicErr("insert", fmt.Errorf("envelope %s: from %q is not a UUIDv7", e.UUID(), h.From))
		}
		a.fromText, a.fromUUID = h.From, [16]byte(u)
	}
	payload, ok := jlh.Payload(e.JLH)
	if !ok {
		return logicErr("insert", fmt.Errorf("envelope %s: no payload object in the JLH", e.UUID()))
	}
	at, nsec := inboxEvent(h.UTS, h.Sub)
	a.uuid = append(a.uuid, [16]byte(h.UUID))
	a.from = append(a.from, a.fromUUID)
	a.space = append(a.space, h.Space)
	a.typ = append(a.typ, syslogType)
	a.level = append(a.level, int16(e.Level))
	a.sid = append(a.sid, int64(e.SID))
	a.step = append(a.step, 0)
	a.eventAt = append(a.eventAt, at)
	a.eventNsec = append(a.eventNsec, nsec)
	a.payload = append(a.payload, inboxPayload(payload))
	return nil
}

// inboxEvent splits event.uts + event.sub into the event_at and event_nsec
// columns of SPEC §3.7.1: the microsecond instant PostgreSQL can hold, and the
// 0-999 ns it cannot. Integer arithmetic only: to_timestamp(uts + sub/1e9) in
// float8 can be off by one microsecond.
func inboxEvent(uts, sub int64) (time.Time, int16) {
	return time.Unix(uts, sub).UTC().Truncate(time.Microsecond), int16(sub % 1000)
}

// nulEscape is the JSON escape jsonb refuses with 22P05.
var nulEscape = []byte(`\u0000`)

// hasNULEscape reports whether payload holds a real \u0000 escape -- not the
// text "\\u0000", whose backslash is itself escaped. The substring test runs
// first: nearly every payload has neither.
func hasNULEscape(payload []byte) bool {
	if !bytes.Contains(payload, nulEscape) {
		return false
	}
	for i := 0; i+1 < len(payload); i++ {
		if payload[i] != '\\' {
			continue
		}
		if payload[i+1] != 'u' {
			i++
			continue
		}
		if i+6 <= len(payload) && bytes.Equal(payload[i:i+6], nulEscape) {
			return true
		}
		i += 5
	}
	return false
}

// jsonbSafe returns payload with every \u0000 escape turned into \ufffd
// (SPEC §3.7.1). jsonb refuses U+0000, and a 22P05 is class 22: the whole chunk
// would be dropped as logic for one message with a NUL in it. It is the
// byte-level fallback of inboxPayload, for a payload that does not decode.
//
// The common case returns payload itself. Otherwise the result is a copy, so
// the JLH in the chunk -- the bytes every retry must resend unchanged -- is
// never written to. Only a real escape is replaced: in \\u0000 the backslash is
// itself escaped and "u0000" is text.
func jsonbSafe(payload []byte) []byte {
	if !hasNULEscape(payload) {
		return payload
	}
	var out []byte
	for i := 0; i < len(payload); i++ {
		if payload[i] != '\\' || i+1 >= len(payload) {
			continue
		}
		if payload[i+1] != 'u' {
			i++ // a two-character escape: skip what it escapes
			continue
		}
		if i+6 <= len(payload) && bytes.Equal(payload[i:i+6], nulEscape) {
			if out == nil {
				out = append([]byte(nil), payload...)
			}
			copy(out[i:], `\ufffd`)
		}
		i += 5
	}
	if out == nil {
		return payload
	}
	return out
}

// inboxPayload is the type=syslog payload as the jsonb column can hold it
// (SPEC §3.7.1). Without a \u0000 escape -- nearly always -- it is payload
// itself, a sub-slice of the chunk's JLH.
//
// With one, jsonb would refuse the row (22P05), so a copy is rebuilt: raw, the
// message byte for byte, goes in base64 with raw-encoding=base64 -- the form the
// syslog payload schema already has for text that cannot travel as is -- so it
// stays exact and size still counts its octets; every other string (message,
// hostname, SD values...) gets U+FFFD for the NUL, the same swap as the text
// columns of SPEC §3.7. jsonb keeps no key order, so rebuilding through a map
// loses nothing the column would have kept.
func inboxPayload(payload []byte) []byte {
	if !hasNULEscape(payload) {
		return payload
	}
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return jsonbSafe(payload)
	}
	if raw, ok := doc["raw"].(string); ok && strings.IndexByte(raw, 0) >= 0 {
		if enc, _ := doc["raw-encoding"].(string); enc == "" || enc == "utf-8" {
			doc["raw"] = base64.StdEncoding.EncodeToString([]byte(raw))
			doc["raw-encoding"] = "base64"
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(withoutNUL(doc)); err != nil {
		return jsonbSafe(payload)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
}

// withoutNUL replaces U+0000 with U+FFFD in every string and key of a decoded
// JSON value.
func withoutNUL(v any) any {
	switch x := v.(type) {
	case string:
		return strings.ReplaceAll(x, "\x00", "\uFFFD")
	case []any:
		for i := range x {
			x[i] = withoutNUL(x[i])
		}
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[strings.ReplaceAll(k, "\x00", "\uFFFD")] = withoutNUL(e)
		}
		return out
	}
	return v
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

// Write enqueues the whole chunk with one statement (SPEC §10.5).
//
// ON CONFLICT (uuid) DO NOTHING is the queue's idempotence (SPEC §3.7.1): the
// pipeline is at-least-once, and a redelivered message must not take the chunk
// down with it. The rows-affected count says how many were already there, which
// is the only direct measurement of redelivery there is.
func (p *Postgres) Write(ctx context.Context, b *Batch) error {
	if len(b.Envelopes) == 0 {
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
	for i := range b.Envelopes {
		if err := a.add(&b.Envelopes[i]); err != nil {
			return err
		}
	}

	tag, err := p.pool.Exec(ctx, p.insert, a.values()...)
	if err != nil {
		return classifyPostgres("insert", err)
	}
	if dup := int64(len(b.Envelopes)) - tag.RowsAffected(); dup > 0 {
		p.m.SinkPostgresDuplicatesTotal.Add(uint64(dup))
	}
	return nil
}

// Health asks whether this server is a primary (SPEC §10.5). A trivial query
// would answer happily on a promoted replica and the state would never leave UP;
// pg_is_in_recovery() is what makes a failover visible to the state machine
// within one interval.
//
// The first successful check also runs the one-time validation, and every check
// refreshes the pool gauges -- doing that in Write would take the pool's lock on
// the hot path.
func (p *Postgres) Health(ctx context.Context) error {
	if p.closed.Load() {
		return ErrClosed
	}
	var inRecovery bool
	if err := p.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return classifyPostgres("health", err)
	}
	p.observePool()
	if !p.validated.Load() {
		if inRecovery {
			// At boot a replica is a configuration mistake, not an outage:
			// refused at once (SPEC §10.5, exit 3).
			return fatalErr("validate", errInRecovery)
		}
		if err := p.validate(ctx); err != nil {
			return err
		}
		p.validated.Store(true)
		p.log.Info("queue table validated", "table", p.cfg.Table)
		return nil
	}
	if inRecovery {
		// After boot it is a failover in progress: DOWN, and the fuses of
		// SPEC §13.3 decide whether it lasts too long.
		return networkErr("health", errInRecovery)
	}
	return nil
}

// validate is the boot contract of SPEC §10.5. Everything it refuses is
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
	// store in that encoding, a logic drop of the whole chunk.
	if encoding != "UTF8" {
		return fatalErr("validate", fmt.Errorf(
			"the database encoding is %s; loghub.inbox needs a UTF8 database", encoding))
	}

	// required is a column every INSERT must fill: NOT NULL with no default,
	// no identity and no generation expression. The sink fills exactly the
	// ten columns of the contract, so an EXTRA required column refuses every
	// row (23502). received_at is not one: the table's own DEFAULT now().
	// A partitioned table is refused: a row with no partition for it is
	// 23514 at write time, every chunk a logic drop with the sink UP. The
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
	// probe INSERT rolled back: a probe would fire the operator's
	// triggers and consume the sequences of identity or serial columns, and its
	// refusal would not say which column. The SPEC §20.18 trap is the same one
	// as a missing column: the table accepts the boot, then every chunk is
	// 23502, class 22-23, a logic drop every 5 ms, forever, with the sink UP.
	// The CHECK constraints of the inbox are not inspected: what this server
	// produces satisfies every one of them (SPEC §3.7.1), and a refusal would be
	// a bug here, not a property of the table.
	contract := map[string]bool{}
	for _, c := range pgColumns {
		contract[c.name] = true
		a, ok := have[c.name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s has no column %q; see docs/SPEC.md §3.7.1", p.regname, c.name))
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
		// event_at holds the microsecond (SPEC §3.7.1): timestamptz or
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

	// ON CONFLICT (uuid) needs a unique index over exactly that column,
	// without a predicate or an expression, immediate (a DEFERRABLE
	// constraint is no arbiter: 55000 on every write, retried forever) and
	// valid (a failed CREATE UNIQUE INDEX CONCURRENTLY leaves one that is not). Without it EVERY write fails with
	// 42P10, so it is checked here rather than discovered on the first chunk.
	// A unique index over (uuid, step) -- the key of the other SQL tables --
	// does not qualify: it would admit two rows with one uuid.
	var unique bool
	if err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1
		  FROM pg_catalog.pg_index i
		  JOIN pg_catalog.pg_attribute a0 ON a0.attrelid = i.indrelid AND a0.attnum = i.indkey[0]
		  WHERE i.indrelid = to_regclass($1)
		    AND i.indisunique AND i.indnkeyatts = 1
		    AND i.indpred IS NULL AND i.indexprs IS NULL
		    AND i.indimmediate AND i.indisvalid AND i.indisready
		    AND a0.attname = 'uuid')`, p.regname).Scan(&unique); err != nil {
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
	// logic drop of the chunk on every collision with the sink UP. A
	// DEFERRABLE unique index on (uuid) next to the primary key is worse: the
	// inference picks it too and every write fails with 55000, retried
	// forever. The queue carries the primary key's index alone (skill
	// loghub-postgres-table-inbox, SPEC §3.7.1). An index that is not ready
	// is not maintained by inserts and is ignored; an invalid but ready one (a
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
				"%s -- a new event colliding on it fails the whole chunk (23505, 23P01; a DEFERRABLE one on "+
				"uuid fails every chunk with 55000); the queue carries the primary key's index alone "+
				"(docs/SPEC.md §10.5)", p.regname, extra))
	}

	// INSERT, and SELECT on uuid: ON CONFLICT (uuid) reads the arbiter
	// column, and a role with INSERT alone fails the first chunk with 42501.
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

// observePool exports the pool occupancy (SPEC §14.1). EmptyAcquireCount --
// acquires that found no idle connection and had to wait for one to be
// released or created -- is cumulative, so only its growth since the last check
// is added to the counter.
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
// The table is SPEC §10.5; keep the two in step.
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
		// relation ... found for row": the table, not this data. Every chunk
		// would fail the same way, so it is fatal, never logic (§20.18). A
		// real CHECK always carries its name.
		class = ClassFatal
	}
	return &Error{Class: class, Op: op + " sqlstate=" + pgErr.Code, Err: err}
}

// classifySQLState is the SQLSTATE table of SPEC §10.5:
//
//	08, 53, 57, 40                    network
//	22, 23 (outside the ON CONFLICT)  logic
//	42 (schema), 28, 2F, 3D, 42501, 25006 fatal
//	anything else                     network
//
// The line that matters most: everything in class 42 is FATAL, never logic. A
// dropped table or a missing unique index would otherwise be a logic error --
// and logic means "drop this chunk, log it, move on", which against a
// destination that refuses every chunk is silent total loss, a chunk every
// 5 ms, forever, with a perfectly healthy-looking process (invariant §20.18).
// This table and TestClassifySQLState change together.
func classifySQLState(code string) Class {
	if code == "25006" { // read_only_sql_transaction: a promoted replica, or configured so
		return ClassFatal
	}
	if len(code) < 2 {
		return ClassNetwork
	}
	switch code[:2] {
	case "42", "28", "2F": // schema and access rules, credentials, SQL routine
		return ClassFatal
	case "3D": // invalid_catalog_name: the database does not exist, and waiting
		// sink.boot_timeout for it to appear only delays the exit with the
		// right reason.
		return ClassFatal
	case "22", "23": // data exception, constraint violation: this data
		return ClassLogic
	case "08", "53", "57", "40": // connection, resources, operator intervention, rollback
		return ClassNetwork
	}
	// Unknown is retried rather than dropped: a filling ring is visible, a
	// discarded chunk is not (invariant §20.18).
	return ClassNetwork
}
