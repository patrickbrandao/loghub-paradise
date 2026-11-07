package verify

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
	_ "modernc.org/sqlite"
)

// Entry is one envelope read from the destination: the payload's message and
// raw (go-relplog-server SPEC §3.2) and the envelope's uuid.
type Entry struct {
	Message, Raw, RawEncoding, UUID string
}

// reader reads the destination. Read calls fn for the envelopes; an
// incremental reader gives only what is new since its last Read, a full one
// gives everything every time.
type reader interface {
	Read(ctx context.Context, fn func(Entry)) error
	Incremental() bool
	Close()
}

// jlh is what the check needs of an envelope.
type jlh struct {
	UUID    string `json:"uuid"`
	Payload struct {
		Message     string `json:"message"`
		Raw         string `json:"raw"`
		RawEncoding string `json:"raw-encoding"`
	} `json:"payload"`
}

func parseJLH(b []byte) (Entry, bool) {
	var e jlh
	if json.Unmarshal(b, &e) != nil {
		return Entry{}, false
	}
	return Entry{Message: e.Payload.Message, Raw: e.Payload.Raw, RawEncoding: e.Payload.RawEncoding, UUID: e.UUID}, true
}

// Margin is how far before the start of the run the PostgreSQL reader looks,
// for clocks that disagree between the injector and the destination. NATS
// and Redis start after the stream's last entry at the Probe instead (SPEC
// §8.5).
const Margin = 10 * time.Minute

// Where the streams were when Probe ran, before the first send (SPEC §8.5;
// ported from go-gelf-injector, 2026-11-07): everything of the run lands
// after it. Starting there needs no clock and reads nothing of the past; the
// time window minus Margin re-read every message of the last 10 minutes of a
// busy stream (~250 000 in the GI's run, past --verify-timeout).
type streamStart struct {
	marked    bool
	natsAfter uint64 // the stream's last sequence
	redisFrom string // the XRANGE start: "(<last id>", or "-" for no entry
	// sqliteAfter is the table's max(rowid): the run's rows come after it,
	// and an earlier run with the same --run is not read (SPEC §8.5; it read
	// from rowid 0 until 2026-11-07).
	sqliteAfter int64
}

// ErrRunPresent: the PostgreSQL window already holds envelopes of --run
// (SPEC §8.5); the run is refused before anything is sent (exit 2).
var ErrRunPresent = errors.New("--run already present at the destination; use another one")

// readSlice bounds one NATS read: a stream with constant foreign traffic on
// the subject never gives an empty batch (SPEC §8.5).
var readSlice = 2 * time.Second

// open connects a reader; mark (Probe, before the send) records where the
// streams are, the start of every later read.
func open(ctx context.Context, t *Target, since time.Time, run string, mark bool) (reader, error) {
	switch t.Kind {
	case KindNATS:
		return openNATS(ctx, t, since, mark)
	case KindRedis:
		return openRedis(ctx, t, since, mark)
	case KindPostgres:
		return openPostgres(ctx, t, since, run)
	case KindSQLite:
		return openSQLite(ctx, t, run, mark)
	}
	return nil, fmt.Errorf("unknown destination kind %q", t.Kind)
}

func caPool(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file)
	if err != nil {
		return nil, errors.New("the ca file cannot be read")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("the ca file has no PEM certificate")
	}
	return pool, nil
}

// --- NATS JetStream (go-relplog-server SPEC §3.8, §10.7) ---

type natsReader struct {
	t    *Target
	nc   *nats.Conn
	cons jetstream.Consumer
	// last stream sequence counted: an ordered consumer that resets (it
	// recreates itself after a gap or a missed heartbeat) delivers again
	// from its start; what was counted is skipped, not counted twice
	// (found by the SI against a real NATS: FetchNoWait restarted every call).
	last uint64
}

func openNATS(ctx context.Context, t *Target, since time.Time, mark bool) (reader, error) {
	opts := []nats.Option{nats.Name("go-relplog-injector verify"), nats.MaxReconnects(-1)}
	if t.tls {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if t.ca != "" {
			pool, err := caPool(t.ca)
			if err != nil {
				return nil, err
			}
			tc.RootCAs = pool
		}
		opts = append(opts, nats.Secure(tc))
	}
	nc, err := nats.Connect(t.url, opts...)
	if err != nil {
		return nil, t.clean("nats", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, t.clean("nats", err)
	}
	if mark {
		st, err := js.Stream(ctx, t.stream)
		if err != nil {
			nc.Close()
			return nil, t.clean("nats stream "+t.stream, err)
		}
		info, err := st.Info(ctx)
		if err != nil {
			nc.Close()
			return nil, t.clean("nats stream "+t.stream, err)
		}
		t.start = streamStart{marked: true, natsAfter: info.State.LastSeq}
	}
	cfg := natsStart(t, since)
	cons, err := js.OrderedConsumer(ctx, t.stream, cfg)
	if err != nil {
		nc.Close()
		return nil, t.clean("nats stream "+t.stream, err)
	}
	return &natsReader{t: t, nc: nc, cons: cons}, nil
}

// natsStart is the consumer's start: after the sequence marked by Probe, or,
// with no Probe (never in a run), the time window.
func natsStart(t *Target, since time.Time) jetstream.OrderedConsumerConfig {
	cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{"loghub." + t.space + ".syslog", "loghub." + t.space + ".syslog.>"}}
	if !t.start.marked {
		start := since.Add(-Margin)
		cfg.DeliverPolicy, cfg.OptStartTime = jetstream.DeliverByStartTimePolicy, &start
		return cfg
	}
	cfg.DeliverPolicy, cfg.OptStartSeq = jetstream.DeliverByStartSequencePolicy, t.start.natsAfter+1
	return cfg
}

func (r *natsReader) Incremental() bool { return true }
func (r *natsReader) Close()            { r.nc.Close() }

func (r *natsReader) Read(ctx context.Context, fn func(Entry)) error {
	until := time.Now().Add(readSlice)
	for ctx.Err() == nil {
		batch, err := r.cons.Fetch(1000, jetstream.FetchMaxWait(300*time.Millisecond))
		if err != nil {
			return r.t.clean("nats fetch", err)
		}
		n := 0
		pending := uint64(1)
		for m := range batch.Messages() {
			n++
			if md, err := m.Metadata(); err == nil {
				pending = md.NumPending
				if md.Sequence.Stream <= r.last {
					continue
				}
				r.last = md.Sequence.Stream
			}
			if e, ok := parseJLH(m.Data()); ok {
				fn(e)
			} else {
				fn(Entry{})
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return r.t.clean("nats fetch", err)
		}
		// Caught up: an empty batch, or the last message says nothing is
		// left on the filter. Under constant foreign traffic neither may
		// come, so a read also ends after readSlice; the next goes on from
		// there (SPEC §8.5).
		if n == 0 || pending == 0 || !time.Now().Before(until) {
			return nil
		}
	}
	return ctx.Err()
}

// --- Redis / Valkey stream (go-relplog-server SPEC §3.6) ---

type redisReader struct {
	t      *Target
	c      rueidis.Client
	stream string
	next   string // XRANGE start
}

func openRedis(ctx context.Context, t *Target, since time.Time, mark bool) (reader, error) {
	opt, err := rueidis.ParseURL(t.url)
	if err != nil {
		return nil, errors.New("redis: the URL is not valid") // rueidis quotes it
	}
	opt.DisableCache = true // a read once; no RESP3 tracking needed
	if t.tls && t.ca != "" && opt.TLSConfig != nil {
		pool, err := caPool(t.ca)
		if err != nil {
			return nil, err
		}
		opt.TLSConfig.RootCAs = pool
	}
	c, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, t.clean("redis", err)
	}
	stream := t.redisStream()
	if mark {
		last, err := c.Do(ctx, c.B().Xrevrange().Key(stream).End("+").Start("-").Count(1).Build()).AsXRange()
		if err != nil && !rueidis.IsRedisNil(err) {
			c.Close()
			return nil, t.clean("redis XREVRANGE", err)
		}
		t.start = streamStart{marked: true, redisFrom: "-"} // a stream not born yet is read whole
		if len(last) == 1 {
			t.start.redisFrom = "(" + last[0].ID
		}
	}
	if t.start.marked {
		return &redisReader{t: t, c: c, stream: stream, next: t.start.redisFrom}, nil
	}
	start := since.Add(-Margin).UnixMilli()
	if start < 0 {
		start = 0
	}
	return &redisReader{t: t, c: c, stream: stream, next: strconv.FormatInt(start, 10) + "-0"}, nil
}

func (r *redisReader) Incremental() bool { return true }
func (r *redisReader) Close()            { r.c.Close() }

func (r *redisReader) Read(ctx context.Context, fn func(Entry)) error {
	for ctx.Err() == nil {
		res, err := r.c.Do(ctx, r.c.B().Xrange().Key(r.stream).Start(r.next).End("+").Count(1000).Build()).AsXRange()
		if err != nil {
			return r.t.clean("redis XRANGE", err)
		}
		for _, x := range res {
			if e, ok := parseJLH([]byte(x.FieldValues["e"])); ok {
				fn(e)
			} else {
				fn(Entry{})
			}
			r.next = "(" + x.ID // exclusive: Redis 6.2+, every Valkey
		}
		if len(res) < 1000 {
			return nil
		}
	}
	return ctx.Err()
}

// --- PostgreSQL loghub.inbox (go-relplog-server SPEC §3.7.1) ---

type pgReader struct {
	t     *Target
	c     *pgx.Conn
	query string
	args  []any
}

func openPostgres(ctx context.Context, t *Target, since time.Time, run string) (reader, error) {
	cfg, err := pgx.ParseConfig(t.url)
	if err != nil {
		return nil, errors.New("postgres: the URL is not valid") // pgx may quote it
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, t.clean("postgres", err)
	}
	table := pgx.Identifier(strings.Split(t.table, ".")).Sanitize()
	q := `SELECT uuid::text, coalesce(payload->>'message',''), coalesce(payload->>'raw',''), coalesce(payload->>'raw-encoding','')
	        FROM ` + table + `
	       WHERE type = 'syslog' AND received_at >= $1
	         AND (payload->>'message' LIKE $2 OR payload->>'raw' LIKE $2 OR payload->>'raw-encoding' = 'base64')`
	args := []any{since.Add(-Margin), "%" + likeEscape(run) + "%"}
	if t.spaceSet {
		q += ` AND space = $3`
		args = append(args, t.space)
	}
	return &pgReader{t: t, c: c, query: q, args: args}, nil
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (r *pgReader) Incremental() bool { return false }
func (r *pgReader) Close()            { r.c.Close(context.Background()) }

func (r *pgReader) Read(ctx context.Context, fn func(Entry)) error {
	rows, err := r.c.Query(ctx, r.query, r.args...)
	if err != nil {
		return r.t.clean("postgres", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.UUID, &e.Message, &e.Raw, &e.RawEncoding); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		fn(e)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

// --- SQLite syslog_events (go-relplog-server SPEC §3.7, §10.4) ---

type sqliteReader struct {
	db    *sql.DB
	query string
	args  func(last int64) []any
	last  int64
}

// safeRun: a run id that JSON never escapes can prefilter with LIKE on the
// envelope text.
func safeRun(run string) bool {
	for _, c := range run {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return false
		}
	}
	return run != ""
}

func openSQLite(ctx context.Context, t *Target, run string, mark bool) (reader, error) {
	if _, err := os.Stat(t.file); err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	// Read only, and wait for the server's writer instead of failing busy.
	db, err := sql.Open("sqlite", "file:"+t.file+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if mark {
		var last int64
		if err := db.QueryRowContext(ctx, `SELECT coalesce(max(rowid), 0) FROM `+t.table).Scan(&last); err != nil {
			db.Close()
			return nil, fmt.Errorf("sqlite: %w", err)
		}
		t.start = streamStart{marked: true, sqliteAfter: last}
	}
	r := &sqliteReader{db: db, last: t.start.sqliteAfter}
	if safeRun(run) {
		r.query = `SELECT rowid, env FROM ` + t.table + ` WHERE rowid > ? AND CAST(env AS TEXT) LIKE ? ORDER BY rowid LIMIT 5000`
		pat := "%" + run + "%"
		r.args = func(last int64) []any { return []any{last, pat} }
	} else {
		r.query = `SELECT rowid, env FROM ` + t.table + ` WHERE rowid > ? ORDER BY rowid LIMIT 5000`
		r.args = func(last int64) []any { return []any{last} }
	}
	return r, nil
}

func (r *sqliteReader) Incremental() bool { return true }
func (r *sqliteReader) Close()            { r.db.Close() }

func (r *sqliteReader) Read(ctx context.Context, fn func(Entry)) error {
	for ctx.Err() == nil {
		rows, err := r.db.QueryContext(ctx, r.query, r.args(r.last)...)
		if err != nil {
			return fmt.Errorf("sqlite: %w", err)
		}
		n := 0
		for rows.Next() {
			var id int64
			var env []byte
			if err := rows.Scan(&id, &env); err != nil {
				rows.Close()
				return fmt.Errorf("sqlite: %w", err)
			}
			n++
			r.last = id
			if e, ok := parseJLH(env); ok {
				fn(e)
			} else {
				fn(Entry{})
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("sqlite: %w", err)
		}
		if n < 5000 {
			return nil
		}
	}
	return ctx.Err()
}

// Probe opens the destination and reads it once, before anything is sent
// (SPEC §8.5): a check that cannot run makes the run pointless. It marks
// where the streams and the SQLite table are; a PostgreSQL window that
// already holds envelopes of the run is ErrRunPresent.
func Probe(ctx context.Context, t *Target, run string, m *Matcher) error {
	r, err := open(ctx, t, time.Now(), run, true)
	if err != nil {
		return err
	}
	defer r.Close()
	if p, ok := r.(prober); ok {
		return p.probe(ctx, m)
	}
	return nil
}

// prober is a cheap read that proves the destination answers.
type prober interface {
	probe(ctx context.Context, m *Matcher) error
}

func (r *redisReader) probe(ctx context.Context, _ *Matcher) error {
	if err := r.c.Do(ctx, r.c.B().Ping().Build()).Error(); err != nil {
		return r.t.clean("redis", err)
	}
	return nil
}

// probe reads the window once: rows of the run there are an earlier run with
// the same --run, which would pass for this one (SPEC §8.5).
func (r *pgReader) probe(ctx context.Context, m *Matcher) error {
	found := false
	if err := r.Read(ctx, func(e Entry) {
		if _, ok := m.Seq(e); ok {
			found = true
		}
	}); err != nil {
		return err
	}
	if found {
		return ErrRunPresent
	}
	return nil
}

func (r *sqliteReader) probe(ctx context.Context, _ *Matcher) error {
	rows, err := r.db.QueryContext(ctx, r.query, r.args(1<<62)...)
	if err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	return rows.Close()
}
