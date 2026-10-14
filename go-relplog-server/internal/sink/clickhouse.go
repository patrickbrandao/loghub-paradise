package sink

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// ClickHouseConfig mirrors [sink.clickhouse].
type ClickHouseConfig struct {
	Addrs          []string // host:port of the native protocol
	Username       string
	Password       string
	Database       string
	Table          string // name or database.name; validated by config
	PoolSize       int
	TLS            bool
	TLSCA          string
	TLSInsecure    bool
	ConnectTimeout time.Duration
	AppName        string // client_name, visible in system.query_log

	// MaxBatchBytes is the most envelope bytes one Write can carry
	// (config.WorstChunkBytes). The boot compares the server's memory limit
	// with what an INSERT of that size costs (SPEC §10.10), so a legitimate
	// batch can never be the MEMORY_LIMIT_EXCEEDED that is fatal at run time.
	// Zero skips the check.
	MaxBatchBytes int64

	// Writers is how many INSERTs this instance can have in flight at once:
	// sink.workers with a ring, min(pool_size, ring.max_inflight) with
	// ring=none (config.Config.SinkWriters). The per-user memory check
	// multiplies the worst chunk by it, not by the pool: the pool may be
	// larger than the writers, and the health check runs on a connection of
	// its own that only SELECTs (go-syslog-server review, 2026-10-13).
	// Zero means PoolSize.
	Writers int
}

// ClickHouse is the analytical destination (SPEC §10.10). Table in §3.7.2. It
// is the third remote destination with a schema the server does not own, and
// the first without transactions: the boot probe is an empty INSERT, and
// idempotency is a deduplication token per chunk plus a ReplacingMergeTree
// that collapses whatever the token missed.
type ClickHouse struct {
	cfg  ClickHouseConfig
	conn driver.Conn
	// probe is the health check's own one-connection client, outside conn's
	// pool (SPEC §10.10.8): in the pool, slow but working INSERTs held every
	// connection and the check waited past sink.health_timeout, DOWN and then
	// FATAL with the server healthy (go-syslog-server review, 2026-10-13).
	probe driver.Conn
	log   *slog.Logger
	m     *obs.Metrics

	// insert is the one statement this sink ever writes with, the column list
	// in chColumns order.
	insert string
	// database and name are what system.tables is asked about.
	database, name string

	// cols pools the per-column slices per worker, in the discipline of SPEC
	// §10.5: payload holds references to the items' buffers until the driver
	// has copied them, and the slices go back cleared.
	cols sync.Pool

	// replicated is learnt by the validation: only a Replicated* table has a
	// row in system.replicas for the health check to read (SPEC §10.10).
	replicated atomic.Bool
	validated  atomic.Bool
	closed     atomic.Bool
}

// chMinVersion is the oldest server with json_type_escape_dots_in_keys, one of
// the two JSON settings every INSERT carries (SPEC §10.10).
var chMinVersion = [2]int{25, 8}

// chMemoryBase and chMemoryFactor estimate what an INSERT of n bytes of JSON
// payload costs the server (SPEC §10.10). Measured on 25.8: 179 MiB for one
// 8 MiB row, 411 MiB for 64 MiB, 1539 MiB for 256 MiB -- about 6x the batch
// plus ~130 MiB of fixed cost. The factor keeps a margin above the 6.
const (
	chMemoryBase   = 128 << 20
	chMemoryFactor = 8
)

// chIdent mirrors the config rule; it is checked again here so a caller that
// bypasses config cannot inject SQL through the table name.
var chIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63})?$`)

// chColumnList is the part of the inbox contract this server writes (SPEC
// §3.7.2), in the order of the INSERT and of chColumns.appendTo. received_at is
// not here: the table fills it.
var chColumnList = []string{"uuid", "from_uuid", "event_at", "sid", "step", "level",
	"event_nsec", "space", "type", "payload"}

// chEngines are the engines whose merge collapses a redelivery (SPEC §10.10).
// The Shared one is ClickHouse Cloud's.
var chEngines = []string{"ReplacingMergeTree", "ReplicatedReplacingMergeTree", "SharedReplacingMergeTree"}

var errCHReplicaReadOnly = errors.New("the table's replica is read-only (system.replicas.is_readonly): it lost ClickHouse Keeper")

// NewClickHouse builds the sink without opening a connection: clickhouse.Open
// is lazy, and the boot's WaitReady owns the retry loop (SPEC §10.10). The
// errors here are configuration errors and never a network condition.
func NewClickHouse(cfg ClickHouseConfig, log *slog.Logger, m *obs.Metrics) (*ClickHouse, error) {
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 2
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("sink clickhouse: no address")
	}
	if !chIdent.MatchString(cfg.Table) {
		return nil, fmt.Errorf("sink clickhouse: table %q is not a plain identifier", cfg.Table)
	}
	// The database goes between backticks in the INSERT; the rule and the
	// reason are checkQuotedName's (SPEC §10.10.5). Inside backticks
	// ClickHouse reads a backslash as an escape, so a name with one would
	// address another database than the one validated.
	if err := checkQuotedName("sink clickhouse: database", cfg.Database); err != nil {
		return nil, err
	}

	opts := &clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     cfg.Addrs,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:  cfg.ConnectTimeout,
		MaxOpenConns: cfg.PoolSize,
		MaxIdleConns: cfg.PoolSize,
		// The goal of this destination is throughput, and the JLH compresses
		// well: the block goes compressed on the wire (SPEC §10.10).
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		// An idle connection would otherwise keep the buffers of the largest
		// chunk it ever sent -- two to three times its bytes -- until
		// ConnMaxLifetime, memory outside every account of the ring.
		FreeBufOnConnRelease: true,
		// No connection-level settings: the two JSON settings travel with the
		// INSERT alone (chInsertSettings), so a server too old to know them
		// still answers the health check and is refused by the version check
		// with a message, not by a 115 on every query.
	}
	if cfg.AppName != "" {
		// The driver prints each product as name/version: "whs/<agent>@<host>"
		// split at its slash reads back whole, where a bare name would gain a
		// trailing slash.
		name, version, _ := strings.Cut(cfg.AppName, "/")
		opts.ClientInfo.Products = append(opts.ClientInfo.Products, struct {
			Name    string
			Version string
		}{Name: name, Version: version})
	}
	if cfg.TLS {
		// ServerName stays empty: tls.DialWithDialer takes it from each
		// address dialled, and addrs may name several servers.
		tc, err := loadTLS("sink clickhouse", cfg.TLSCA, cfg.TLSInsecure, "")
		if err != nil {
			return nil, err
		}
		opts.TLS = tc
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("sink clickhouse: %w", err)
	}
	po := *opts
	po.MaxOpenConns, po.MaxIdleConns = 1, 1
	probe, err := clickhouse.Open(&po)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("sink clickhouse: %w", err)
	}

	database, name := cfg.Database, cfg.Table
	if i := strings.IndexByte(cfg.Table, '.'); i >= 0 {
		database, name = cfg.Table[:i], cfg.Table[i+1:]
	}
	c := &ClickHouse{
		cfg:      cfg,
		conn:     conn,
		probe:    probe,
		log:      log,
		m:        m,
		insert:   "INSERT INTO `" + database + "`.`" + name + "` (" + strings.Join(chColumnList, ", ") + ")",
		database: database,
		name:     name,
	}
	c.cols.New = func() any { return &chColumns{} }
	return c, nil
}

func (c *ClickHouse) Type() string { return "clickhouse" }

// chColumns is one batch laid out in columns, the way the native protocol
// sends it. payload references the items' buffers until appendTo has handed
// it to the driver, which copies every value into its own column buffer
// (ColStr.AppendBytes of ch-go): after appendTo nothing of the INSERT points
// at an item (SPEC §10.10). reset clears it anyway before the pool sees it, as
// in SPEC §10.5.
type chColumns struct {
	ids       [][16]byte // the uuids again, as bytes: the token is derived from them
	uuid      []string
	from      []string
	eventAt   []time.Time
	sid       []uint64
	step      []uint16
	level     []uint8
	eventNsec []uint16
	space     []string
	typ       []string
	payload   [][]byte
}

// add appends one envelope. The error is a logic one: an envelope this sink
// cannot map is refused by this data, not by the destination -- and only data
// of another ring or dump could produce one.
func (a *chColumns) add(e *Envelope) error {
	h := &e.Item.Head
	if u, err := uuidv7.FromString(h.From); err != nil || !u.IsValid() {
		return logicErr("insert", fmt.Errorf("envelope %s: from %q is not a UUIDv7", e.UUID(), h.From))
	}
	payload, ok := jlh.Payload(e.JLH)
	if !ok {
		return logicErr("insert", fmt.Errorf("envelope %s: no payload object in the JLH", e.UUID()))
	}
	at, nsec := inboxEvent(h.UTS, h.Sub)
	a.ids = append(a.ids, [16]byte(h.UUID))
	a.uuid = append(a.uuid, e.UUID())
	a.from = append(a.from, h.From)
	a.eventAt = append(a.eventAt, at)
	a.sid = append(a.sid, e.Res.SID)
	a.step = append(a.step, 0)
	a.level = append(a.level, uint8(e.Res.Severity()))
	a.eventNsec = append(a.eventNsec, uint16(nsec))
	a.space = append(a.space, h.Space)
	a.typ = append(a.typ, syslogType)
	a.payload = append(a.payload, payload)
	return nil
}

// appendTo hands the ten columns to the batch, in chColumnList order. A
// [][]byte puts the JSON column in string mode: the server parses the text.
func (a *chColumns) appendTo(b driver.Batch) error {
	for i, col := range []any{a.uuid, a.from, a.eventAt, a.sid, a.step, a.level,
		a.eventNsec, a.space, a.typ, a.payload} {
		if err := b.Column(i).Append(col); err != nil {
			return fmt.Errorf("column %s: %w", chColumnList[i], err)
		}
	}
	return nil
}

// reset drops every reference and keeps the capacity.
func (a *chColumns) reset() {
	clear(a.uuid)
	clear(a.from)
	clear(a.space)
	clear(a.typ)
	clear(a.payload)
	a.ids, a.uuid, a.from, a.eventAt = a.ids[:0], a.uuid[:0], a.from[:0], a.eventAt[:0]
	a.sid, a.step, a.level, a.eventNsec = a.sid[:0], a.step[:0], a.level[:0], a.eventNsec[:0]
	a.space, a.typ, a.payload = a.space[:0], a.typ[:0], a.payload[:0]
}

// dedupToken is the insert_deduplication_token of one chunk (SPEC §10.10):
// "rls-" and the first 32 hex digits of the SHA-256 of its uuids, in order.
// The same chunk -- a worker retry resends the same chunk, byte for byte
// (SPEC §10.2) -- gets the same token, and the server drops the repeat. It comes from the
// content and never from a counter: a token repeated for DIFFERENT data would
// make the server drop the second batch in silence.
func dedupToken(ids [][16]byte) string {
	h := sha256.New()
	for i := range ids {
		h.Write(ids[i][:])
	}
	var sum [sha256.Size]byte
	return "rls-" + hex.EncodeToString(h.Sum(sum[:0])[:16])
}

// chInsertSettings are the settings every INSERT carries (SPEC §10.10), and
// the only ones: async_insert, wait_for_async_insert, insert_quorum and
// fsync_after_insert are the ClickHouse administrator's (SPEC §10.10).
//
// The two JSON settings are a defence: the keys an emitter controls (the
// SD-IDs and PARAM-NAMEs) are Map keys of the payload hint and never become
// paths. Outside a hint, a key with a dot would be stored as a leaf and as a
// prefix and read back as JSON with a repeated key, and two keys landing on
// the same path would refuse the INSERT with 117 -- a logic drop of the whole
// chunk because of one emitter (SPEC §3.7.2).
func chInsertSettings(token string) clickhouse.Settings {
	return clickhouse.Settings{
		"insert_deduplication_token":      token,
		"json_type_escape_dots_in_keys":   1,
		"type_json_skip_duplicated_paths": 1,
		// The JSON type infers a type for every string it parses, and by
		// default a string that looks like a date becomes a Date, DateTime or
		// DateTime64. That rewrites the value ("2020-02-30" reads back
		// "2020-03-01") and, worse, once a path holds such a variant in a
		// block, the strings of OTHER rows on that path are read through it:
		// "" became "1970-01-01 00:00:00.000000000" and "1531420618" a date,
		// in the next client's row (independent review of 2026-10-07). Every
		// string of the JLH is a string; nothing is inferred from one.
		"input_format_try_infer_dates":                     0,
		"input_format_try_infer_datetimes":                 0,
		"input_format_json_try_infer_numbers_from_strings": 0,
		// A chunk spanning more partitions than max_partitions_per_insert_block
		// (a reingested backlog of months, or an hourly partition key) is
		// refused with 252 by default -- the code of TOO_MANY_PARTS, which is
		// retried, so the worker would retry it forever. The parts are written
		// anyway, the merge copes, and the boot already refuses a partition
		// key that is not a function of event_at.
		"throw_on_max_partitions_per_insert_block": 0,
	}
}

// Write inserts the chunk as one INSERT of one block (SPEC §10.10). On a
// failure nothing of it is stored -- the server parses the whole block before
// it writes a part -- so a logic error carries no Delivered list.
func (c *ClickHouse) Write(ctx context.Context, b *Batch) error {
	if len(b.Envelopes) == 0 {
		return nil
	}
	if c.closed.Load() {
		return ErrClosed
	}
	cols := c.cols.Get().(*chColumns)
	defer func() {
		cols.reset()
		c.cols.Put(cols)
	}()
	// Every row is mapped before the INSERT leaves: an envelope that does not
	// map is damage, refused whole as logic (SPEC §10.6).
	for i := range b.Envelopes {
		if err := cols.add(&b.Envelopes[i]); err != nil {
			return err
		}
	}
	return c.send(ctx, cols, dedupToken(cols.ids), "insert")
}

// send runs one INSERT with the given columns -- none at all for the boot
// probe -- and counts the blocks the server dropped as duplicates.
func (c *ClickHouse) send(ctx context.Context, cols *chColumns, token, op string) error {
	// The server reports a dropped block as a profile event and in no other
	// way: the summary counts the rows sent either way (SPEC §10.10).
	var dupBlocks atomic.Int64
	qctx := clickhouse.Context(ctx,
		clickhouse.WithSettings(chInsertSettings(token)),
		clickhouse.WithProfileEvents(func(events []clickhouse.ProfileEvent) {
			for _, e := range events {
				if e.Name == "DuplicatedInsertedBlocks" && e.Value > 0 {
					dupBlocks.Add(e.Value)
				}
			}
		}))
	b, err := c.conn.PrepareBatch(qctx, c.insert)
	if err != nil {
		return classifyClickHouse(op, err)
	}
	if err := cols.appendTo(b); err != nil {
		_ = b.Abort()
		return classifyClickHouse(op, err)
	}
	if err := b.Send(); err != nil {
		return classifyClickHouse(op, err)
	}
	if n := dupBlocks.Load(); n > 0 {
		c.m.SinkClickHouseDedupBlocksTotal.Add(uint64(n))
	}
	return nil
}

// Health asks whether this user may write (SPEC §10.10). readonly belongs to
// the user's profile, not to a failover, and does not change by waiting: it is
// fatal at once. A Replicated table that lost Keeper is the read-only replica
// of PostgreSQL's pg_is_in_recovery: DOWN, and back by itself.
//
// The first successful check also runs the one-time validation of SPEC
// §10.10, and every check refreshes the pool gauges.
func (c *ClickHouse) Health(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	var readonly uint64
	if err := c.probe.QueryRow(ctx, "SELECT toUInt64(getSetting('readonly'))").Scan(&readonly); err != nil {
		return classifyClickHouse("health", err)
	}
	c.observePool()
	if readonly != 0 {
		return fatalErr("health", fmt.Errorf(
			"user %q has readonly=%d in its ClickHouse profile: it cannot INSERT; give the user a "+
				"profile with readonly=0", c.cfg.Username, readonly))
	}
	if c.replicated.Load() {
		if err := c.replicaHealth(ctx); err != nil {
			return err
		}
	}
	if !c.validated.Load() {
		// The validation is a dozen round trips and ends with the probe
		// INSERT, so it gets a deadline of its own rather than
		// sink.health_timeout: one second of twelve round trips failed every
		// attempt against a server 60 ms away. And the deadline is
		// sink.op_timeout, the drain's: the driver turns a deadline into
		// max_execution_time, and a probe sent with another one than the
		// drain's passed a profile that refuses the drain's (452). A
		// cancelled boot still reaches it (validationContext): a bare
		// WithoutCancel kept a SIGTERM waiting for the whole op_timeout.
		vctx, cancel := validationContext(ctx, c.cfg.ConnectTimeout)
		err := c.validate(vctx)
		cancel()
		if err != nil {
			return err
		}
		c.validated.Store(true)
		c.log.Info("inbox table validated", "table", c.cfg.Table, "replicated", c.replicated.Load())
	}
	return nil
}

// replicaHealth reads system.replicas for a Replicated* table: is_readonly is
// the replica that lost Keeper, DOWN until it comes back. Reading that system
// table needs a grant the INSERT-only user may lack (the official image sets
// select_from_system_db_requires_grant): a 497 here is not the destination
// refusing writes, so the check is dropped with a warning instead of ending
// the process -- a 242 on a write still says the same thing, fatally.
func (c *ClickHouse) replicaHealth(ctx context.Context) error {
	var ro uint8
	err := c.probe.QueryRow(ctx,
		"SELECT is_readonly FROM system.replicas WHERE database = ? AND table = ?",
		c.database, c.name).Scan(&ro)
	var ex *clickhouse.Exception
	switch {
	case errors.As(err, &ex) && ex.Code == 497:
		c.replicated.Store(false)
		c.log.Warn("cannot read system.replicas; the read-only replica check is off",
			"table", c.cfg.Table, "grant", "GRANT SELECT(database, table, is_readonly) ON system.replicas",
			"error", err)
		return nil
	case errors.Is(err, sql.ErrNoRows):
		// Not registered yet; the validation already proved the table exists.
		return nil
	case err != nil:
		return classifyClickHouse("health", err)
	case ro != 0:
		return networkErr("health", errCHReplicaReadOnly)
	}
	return nil
}

// validate is the boot contract of SPEC §10.10. Everything it refuses is
// ClassFatal: none of it changes by waiting.
func (c *ClickHouse) validate(ctx context.Context) error {
	var version string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return classifyClickHouse("validate", err)
	}
	if !chVersionAtLeast(version, chMinVersion) {
		return fatalErr("validate", fmt.Errorf(
			"ClickHouse %s is older than %d.%d: the INSERT needs json_type_escape_dots_in_keys "+
				"(docs/SPEC.md §10.10)", version, chMinVersion[0], chMinVersion[1]))
	}

	var engine, engineFull, sortingKey, partitionKey, createQuery string
	var dependents []string
	err := c.conn.QueryRow(ctx,
		"SELECT engine, engine_full, sorting_key, partition_key, create_table_query, dependencies_table "+
			"FROM system.tables WHERE database = ? AND name = ?",
		c.database, c.name).Scan(&engine, &engineFull, &sortingKey, &partitionKey, &createQuery, &dependents)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fatalErr("validate", fmt.Errorf(
				"table %s.%s does not exist, or this user cannot see it; apply docker/clickhouse/inbox_syslog.sql",
				c.database, c.name))
		}
		return classifyClickHouse("validate", err)
	}
	if problem := chTableProblem(engine, engineFull, sortingKey, partitionKey, createQuery); problem != "" {
		return fatalErr("validate", fmt.Errorf("table %s.%s %s", c.database, c.name, problem))
	}
	if len(dependents) > 0 {
		// A materialized view runs inside the INSERT: a refusal of its own
		// (a cast of a payload field it cannot do) fails the INSERT after the
		// rows are stored, and the chunk counted as dropped is in the table.
		c.log.Warn("the inbox table feeds materialized views; a refusal of theirs fails the INSERT "+
			"after the rows are stored, and the events are then counted as dropped though they are kept",
			"table", c.cfg.Table, "views", dependents)
	}
	c.replicated.Store(strings.HasPrefix(engine, "Replicated"))
	if c.replicated.Load() {
		if err := c.replicaHealth(ctx); err != nil {
			return err
		}
	}

	cols, err := c.columns(ctx)
	if err != nil {
		return err
	}
	for _, name := range chColumnList {
		got, ok := cols[name]
		if !ok {
			return fatalErr("validate", fmt.Errorf(
				"table %s.%s has no column %q; see docs/SPEC.md §3.7.2", c.database, c.name, name))
		}
		if problem := chColumnProblem(name, got); problem != "" {
			return fatalErr("validate", fmt.Errorf("table %s.%s: column %q %s", c.database, c.name, name, problem))
		}
	}
	if got, ok := cols["received_at"]; ok {
		if problem := chColumnProblem("received_at", got); problem != "" {
			return fatalErr("validate", fmt.Errorf(
				"table %s.%s: column \"received_at\" %s", c.database, c.name, problem))
		}
	}

	var maxMem, maxMemUser uint64
	if err := c.conn.QueryRow(ctx,
		"SELECT toUInt64(getSetting('max_memory_usage')), toUInt64(getSetting('max_memory_usage_for_user'))").
		Scan(&maxMem, &maxMemUser); err != nil {
		return classifyClickHouse("validate", err)
	}
	if problem := chMemoryProblem(c.cfg.MaxBatchBytes, c.memoryWriters(), maxMem, maxMemUser); problem != "" {
		return fatalErr("validate", errors.New(problem))
	}

	// The probe: the only reliable answer to "may this user write?" is to
	// try, with the very statement and settings the drain uses. ClickHouse
	// has no transaction to roll a probe row back, so the INSERT carries no
	// row at all: the server checks the column grants and readonly when it
	// receives the statement, before reading any data, and zero rows make no
	// part (SPEC §10.10).
	if err := c.send(ctx, &chColumns{}, "rls-probe", "validate"); err != nil {
		var ex *clickhouse.Exception
		if errors.As(err, &ex) {
			switch ex.Code {
			case 497:
				return fatalErr("validate", fmt.Errorf(
					"user %q lacks INSERT on the ten columns of %s.%s: %w", c.cfg.Username, c.database, c.name, err))
			case 164:
				return fatalErr("validate", fmt.Errorf(
					"user %q is readonly and cannot INSERT into %s.%s: %w", c.cfg.Username, c.database, c.name, err))
			}
		}
		if ClassOf(err) == ClassLogic {
			// No data was sent, so nothing about THIS data can be wrong: a
			// refusal is the table's, and accepting it would drop every batch.
			return fatalErr("validate", fmt.Errorf("table %s.%s refuses an empty INSERT: %w", c.database, c.name, err))
		}
		return err
	}
	return nil
}

// chColumnInfo is one row of system.columns.
type chColumnInfo struct {
	typ         string
	defaultKind string // DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL, or ""
}

// columns reads the table's columns from system.columns.
func (c *ClickHouse) columns(ctx context.Context) (map[string]chColumnInfo, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT name, type, default_kind FROM system.columns WHERE database = ? AND table = ?",
		c.database, c.name)
	if err != nil {
		return nil, classifyClickHouse("validate", err)
	}
	defer rows.Close()
	cols := map[string]chColumnInfo{}
	for rows.Next() {
		var name string
		var ci chColumnInfo
		if err := rows.Scan(&name, &ci.typ, &ci.defaultKind); err != nil {
			return nil, classifyClickHouse("validate", err)
		}
		cols[name] = ci
	}
	if err := rows.Err(); err != nil {
		return nil, classifyClickHouse("validate", err)
	}
	return cols, nil
}

// chDateTime64 matches DateTime64(P) and DateTime64(P, 'zone').
var chDateTime64 = regexp.MustCompile(`^DateTime64\((\d)(, *'[^']*')?\)$`)

// chExactTypes is what the driver converts the Go values of chColumns into
// without complaint. It converts on the CLIENT, inside Append, and refuses a
// type it was not given -- an Int64 for a []uint64, a Nullable -- before
// anything is sent: a column of another type would be fatal on the first
// batch, so the boot refuses it by name.
var chExactTypes = map[string][]string{
	"uuid":       {"UUID"},
	"from_uuid":  {"UUID"},
	"sid":        {"UInt64"},
	"step":       {"UInt16"},
	"level":      {"UInt8"},
	"event_nsec": {"UInt16"},
	"space":      {"String", "LowCardinality(String)"},
	"type":       {"String", "LowCardinality(String)"},
}

// chColumnProblem says what is wrong with one column against the contract of
// SPEC §3.7.2, or "" when nothing is. A pure function, so every rule is
// unit-tested without a server.
func chColumnProblem(name string, got chColumnInfo) string {
	// A column the server writes must store what it is given. An EPHEMERAL one
	// accepts the INSERT and keeps nothing -- the whole envelope gone, with
	// Write returning nil --, a MATERIALIZED one refuses it with 44 and an
	// ALIAS one with 16.
	if name != "received_at" && got.defaultKind != "" && got.defaultKind != "DEFAULT" {
		return fmt.Sprintf("is %s: the server writes it, and an EPHEMERAL column stores nothing (the "+
			"value is gone without an error) while MATERIALIZED and ALIAS refuse the INSERT; declare "+
			"it a plain column", got.defaultKind)
	}
	switch name {
	case "event_at", "received_at":
		m := chDateTime64.FindStringSubmatch(got.typ)
		if m == nil {
			return fmt.Sprintf("is %s, want DateTime64(6, 'UTC')", got.typ)
		}
		if p, _ := strconv.Atoi(m[1]); p < 6 {
			return fmt.Sprintf("is %s, want a precision of at least 6: fewer digits truncate the "+
				"microseconds in silence", got.typ)
		}
		if name == "received_at" && got.defaultKind != "DEFAULT" && got.defaultKind != "MATERIALIZED" {
			return "has no DEFAULT: the server never writes it, so every row would get 1970 and the " +
				"lag a consumer measures by it would be a lie; declare DEFAULT now64(6)"
		}
		return ""
	case "payload":
		return chPayloadProblem(got.typ)
	}
	want, ok := chExactTypes[name]
	if !ok || contains(want, got.typ) {
		return ""
	}
	return fmt.Sprintf("is %s, want %s: the driver converts on the client and refuses any other type, "+
		"which would be fatal on the first chunk", got.typ, strings.Join(want, " or "))
}

// chPayloadHints are the typed paths the payload column must declare (SPEC
// §3.7.2), as system.columns prints them. The JSON type drops an empty object:
// without the hint, an SD element without parameters -- [origin] -- vanished
// from the row read back. As Map keys, the SD-IDs and PARAM-NAMEs -- the only
// object keys an emitter controls -- stay literal instead of becoming paths,
// and two of them that would flatten to the same path (a -> b.c and a.b -> c)
// cannot refuse the INSERT. The price: a row without structured-data reads
// back with "structured-data":{}, which this server never writes (SPEC §3.2),
// so {} means absent.
var chPayloadHints = []string{"`structured-data` Map(String, Map(String, Dynamic))"}

// chPayloadProblem says what is wrong with the payload column's type, or "".
// Besides the two hints only the path limits are accepted: a SKIP would drop
// part of the envelope in silence, and any other typed path could refuse a
// chunk that does not fit it (size UInt8) -- every table the boot
// accepts must accept every row this server writes.
func chPayloadProblem(typ string) string {
	want := "JSON(" + strings.Join(chPayloadHints, ", ") + ")"
	if !strings.HasPrefix(typ, "JSON(") || !strings.HasSuffix(typ, ")") {
		return fmt.Sprintf("is %s, want %s: the JSON type drops empty objects, and without the hint "+
			"an SD element without parameters vanishes from the rows (docs/SPEC.md §3.7.2)", typ, want)
	}
	have := map[string]bool{}
	for _, arg := range splitTopLevel(typ[len("JSON(") : len(typ)-1]) {
		if strings.HasPrefix(arg, "max_dynamic_paths=") || strings.HasPrefix(arg, "max_dynamic_types=") {
			continue
		}
		if !contains(chPayloadHints, arg) {
			return fmt.Sprintf("is %s: %q is not allowed; besides %s only max_dynamic_paths and "+
				"max_dynamic_types are -- a SKIP drops envelope data in silence, and another typed path "+
				"can refuse a whole chunk", typ, arg, strings.Join(chPayloadHints, " and "))
		}
		have[arg] = true
	}
	for _, h := range chPayloadHints {
		if !have[h] {
			return fmt.Sprintf("is %s, want %s: without %q the JSON type drops the empty SD element, and "+
				"the rows read back are not the envelopes this server wrote (docs/SPEC.md §3.7.2)", typ, want, h)
		}
	}
	return ""
}

// chCheckConstraint finds a CHECK constraint in a CREATE TABLE as
// system.tables prints it, the name bare or quoted with backticks (a name with
// a space is printed quoted). ASSUME constraints are not evaluated on INSERT
// and are tolerated.
var chCheckConstraint = regexp.MustCompile("\\bCONSTRAINT\\s+(`(?:[^`\\\\]|\\\\.)*`|\\S+)\\s+CHECK\\b")

// chIdentToken finds the identifiers of an expression; one followed by "(" is
// a function name.
var chIdentToken = regexp.MustCompile("`[^`]*`|[A-Za-z_][A-Za-z0-9_]*\\(?")

// chTableProblem says what is wrong with the table's engine, sorting key,
// partition key and constraints (SPEC §10.10.5), or "" when nothing is.
func chTableProblem(engine, engineFull, sortingKey, partitionKey, createQuery string) string {
	if !contains(chEngines, engine) {
		why := "a MergeTree would keep every redelivery outside the token's window forever"
		if engine == "Distributed" {
			why = "an INSERT into a Distributed table reaches the shards asynchronously, and the 200 " +
				"would promise nothing (docs/SPEC.md §21)"
		}
		return fmt.Sprintf("uses engine %s, want %s: %s", engine, strings.Join(chEngines, ", "), why)
	}
	if engineArgs(engineFull, engine) > 1 {
		return fmt.Sprintf("uses %s with an is_deleted column: the merge removes the rows it marks, and "+
			"a value other than 0 or 1 refuses the INSERT; use at most a version column", engine)
	}
	if !sortingKeyHasUUID(sortingKey) {
		return fmt.Sprintf("has ORDER BY (%s) without uuid as an element: the merge would collapse two "+
			"DIFFERENT events that share the rest of the key, a silent loss; use ORDER BY (event_at, uuid)",
			sortingKey)
	}
	if col := partitionColumnOtherThanEventAt(partitionKey); col != "" {
		return fmt.Sprintf("has PARTITION BY %s on %s: a redelivery would land in another partition and "+
			"never collapse, and a key not bounded by time can spread one batch over hundreds of "+
			"partitions; partition by a function of event_at alone", partitionKey, col)
	}
	if chCheckConstraint.MatchString(createQuery) {
		return "has a CHECK constraint: the empty boot probe cannot exercise it, and a violation would " +
			"drop whole chunks as logic (docs/SPEC.md §3.7.2); drop the constraint"
	}
	return ""
}

// engineArgs counts the column arguments of the engine in engine_full: the
// ver and is_deleted of ReplacingMergeTree, after the Keeper path and replica
// name a Replicated* engine may carry as string literals.
func engineArgs(engineFull, engine string) int {
	i := strings.Index(engineFull, engine+"(")
	if i < 0 {
		return 0
	}
	rest := engineFull[i+len(engine)+1:]
	depth, end := 1, -1
	for k := 0; k < len(rest) && end < 0; k++ {
		switch rest[k] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				end = k
			}
		}
	}
	if end < 0 {
		return 0
	}
	n := 0
	for _, a := range splitTopLevel(rest[:end]) {
		if a != "" && !strings.HasPrefix(a, "'") {
			n++
		}
	}
	return n
}

// partitionColumnOtherThanEventAt returns a column the partition key reads
// other than event_at, or "" when it reads event_at alone (or nothing).
func partitionColumnOtherThanEventAt(key string) string {
	for _, tok := range chIdentToken.FindAllString(key, -1) {
		if strings.HasSuffix(tok, "(") {
			continue
		}
		name := strings.Trim(tok, "`")
		if name != "event_at" {
			return name
		}
	}
	return ""
}

// sortingKeyHasUUID reports whether the column uuid itself -- not an
// expression of it -- is one element of the sorting key.
func sortingKeyHasUUID(key string) bool {
	for _, e := range splitTopLevel(key) {
		if e == "uuid" || e == "`uuid`" {
			return true
		}
	}
	return false
}

// splitTopLevel splits at the commas outside parentheses and trims each
// element: "toStartOfHour(event_at), uuid" has two, and so has
// "params Map(String, String), query Map(String, Dynamic)".
func splitTopLevel(s string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// chVersionAtLeast compares the major.minor of a version() string.
func chVersionAtLeast(version string, want [2]int) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	return major > want[0] || major == want[0] && minor >= want[1]
}

// memoryWriters is the number of concurrent INSERTs the memory check counts:
// Writers, or the pool when the app did not say.
func (c *ClickHouse) memoryWriters() int {
	if c.cfg.Writers > 0 {
		return c.cfg.Writers
	}
	return c.cfg.PoolSize
}

// chMemoryNeed is what the server needs to INSERT a batch of n bytes.
func chMemoryNeed(n int64) uint64 {
	return chMemoryBase + chMemoryFactor*uint64(n)
}

// chMemoryProblem compares the session's memory limits with the worst batch
// (SPEC §10.10.5). max_memory_usage holds for one query; max_memory_usage_for_user
// for every query of the user at once, and this instance alone can have
// writers INSERTs in flight -- four concurrent legitimate batches broke a
// per-user limit the single-batch comparison had accepted. Instances sharing
// the user add up on top, which no one instance can see. Zero is ClickHouse's
// "unlimited": the server-wide ceiling then applies, which this session cannot
// read.
func chMemoryProblem(maxBatch int64, writers int, maxMem, maxMemUser uint64) string {
	if maxBatch <= 0 {
		return ""
	}
	need := chMemoryNeed(maxBatch)
	if maxMem != 0 && maxMem < need {
		return fmt.Sprintf("max_memory_usage=%d is below the %d bytes an INSERT of the largest chunk this "+
			"server can write (%d bytes of JLH) costs the server; MEMORY_LIMIT_EXCEEDED is fatal, so "+
			"raise the limit or lower sink.batch_max_bytes or syslog.max_message_bytes", maxMem, need, maxBatch)
	}
	if writers < 1 {
		writers = 1
	}
	if all := need * uint64(writers); maxMemUser != 0 && maxMemUser < all {
		// The pool is not the lever: the writers are (go-syslog-server review,
		// 2026-10-13). A pool lowered below them is refused by config.
		return fmt.Sprintf("max_memory_usage_for_user=%d is below the %d bytes of %d concurrent INSERTs of "+
			"the largest chunk (%d bytes of JLH each, one per writer: sink.workers, or ring.max_inflight "+
			"with ring.mode=none); every instance on the same user adds as much again. "+
			"MEMORY_LIMIT_EXCEEDED is fatal: raise the limit, give each instance its own user, or lower "+
			"the writers and sink.batch_max_bytes", maxMemUser, all, writers, maxBatch)
	}
	return ""
}

// observePool exports the pool occupancy of SPEC §10.10.
func (c *ClickHouse) observePool() {
	st := c.conn.Stats()
	acquired := st.Open - st.Idle
	if acquired < 0 {
		acquired = 0
	}
	c.m.SinkClickHousePoolConns[obs.PoolAcquired].Set(int64(acquired))
	c.m.SinkClickHousePoolConns[obs.PoolIdle].Set(int64(st.Idle))
	c.m.SinkClickHousePoolConns[obs.PoolMax].Set(int64(st.MaxOpenConns))
}

// Close releases the pool. The workers are stopped before the sink is closed.
func (c *ClickHouse) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	_ = c.probe.Close()
	return c.conn.Close()
}

// classifyClickHouse maps a driver error onto the class the state machine
// acts on. The table is SPEC §10.10; keep the two in step.
func classifyClickHouse(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return networkErr(op, err)
	}
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return &Error{Class: classifyClickHouseCode(ex.Code), Op: op + " clickhouse=" + strconv.Itoa(int(ex.Code)), Err: err}
	}
	// The driver converts every value on the client, inside Append; a value
	// it refuses means a column changed type after the boot validated it, and
	// every chunk would be refused the same way.
	var conv *column.ColumnConverterError
	if errors.As(err, &conv) || errors.Is(err, clickhouse.ErrBatchInvalid) {
		return fatalErr(op, err)
	}
	// No server exception means the server never answered: a refused dial, a
	// broken pipe, an exhausted pool (ErrAcquireConnTimeout). All of it is the
	// connection.
	return networkErr(op, err)
}

// classifyClickHouseCode is the exception table of SPEC §10.10. The lines
// that matter most: schema and privilege errors are FATAL, never logic; a
// read-only user or replica is fatal for the reasons 25006 is on PostgreSQL;
// a memory limit is fatal like Redis's OOM, which is why the boot compares the
// limit with the worst batch; and an unknown code is retried, because a ring
// that fills is visible and a dropped batch is not.
func classifyClickHouseCode(code int32) Class {
	switch code {
	case 164, 242: // READONLY, TABLE_IS_READ_ONLY
		return ClassFatal
	case 241, 173, 243: // MEMORY_LIMIT_EXCEEDED, CANNOT_ALLOCATE_MEMORY, NOT_ENOUGH_SPACE
		return ClassFatal
	case 192, 193, 194, 195, 497, 516: // unknown user, wrong/required password, IP not allowed, access denied, authentication failed
		return ClassFatal
	case 16, 47, 53, 60, 81: // no such column, unknown identifier, type mismatch, unknown table, unknown database
		return ClassFatal
	case 36, 44, 62, 115, 452: // bad arguments, illegal column, syntax error, unknown setting, setting constraint violated
		return ClassFatal
	case 6, 26, 27, 41, 69, 70, 72, 117, 131, 469: // the data: unparsable, out of bound, invalid JSON, too large, CHECK violated
		return ClassLogic
	case 3, 32, 33, 209, 210, // unexpected EOF, read after EOF, cannot read all data, socket timeout, network error
		159, 160, 394, // timeout exceeded, too slow, query was cancelled
		202, 203, 439, 473, // too many simultaneous queries, no free connection, cannot schedule task, deadlock avoided
		252,                // TOO_MANY_PARTS: the merge is behind, the server asks to slow down
		225, 285, 319, 999, // no Keeper, too few live replicas, unknown status of insert, Keeper exception
		236, 425, 1000, 1001, 1002: // aborted, system error, generic exceptions
		return ClassNetwork
	}
	return ClassNetwork
}
