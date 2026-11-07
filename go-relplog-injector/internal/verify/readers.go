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

// Margin is how far before the start of the run the readers look, for clocks
// that disagree between the injector and the destination.
const Margin = 10 * time.Minute

func open(ctx context.Context, t *Target, since time.Time, run string) (reader, error) {
	switch t.Kind {
	case KindNATS:
		return openNATS(ctx, t, since)
	case KindRedis:
		return openRedis(ctx, t, since)
	case KindPostgres:
		return openPostgres(ctx, t, since, run)
	case KindSQLite:
		return openSQLite(t, run)
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
	nc   *nats.Conn
	cons jetstream.Consumer
}

func openNATS(ctx context.Context, t *Target, since time.Time) (reader, error) {
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
		return nil, fmt.Errorf("nats: %w", cleanErr(err, t.url))
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: %w", err)
	}
	start := since.Add(-Margin)
	cons, err := js.OrderedConsumer(ctx, t.stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{"loghub." + t.space + ".syslog", "loghub." + t.space + ".syslog.>"},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &start,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats stream %s: %w", t.stream, err)
	}
	return &natsReader{nc: nc, cons: cons}, nil
}

func (r *natsReader) Incremental() bool { return true }
func (r *natsReader) Close()            { r.nc.Close() }

func (r *natsReader) Read(ctx context.Context, fn func(Entry)) error {
	for ctx.Err() == nil {
		batch, err := r.cons.Fetch(1000, jetstream.FetchMaxWait(300*time.Millisecond))
		if err != nil {
			return fmt.Errorf("nats fetch: %w", err)
		}
		n := 0
		for m := range batch.Messages() {
			n++
			if e, ok := parseJLH(m.Data()); ok {
				fn(e)
			} else {
				fn(Entry{})
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return fmt.Errorf("nats fetch: %w", err)
		}
		if n == 0 {
			return nil
		}
	}
	return ctx.Err()
}

// --- Redis / Valkey stream (go-relplog-server SPEC §3.6) ---

type redisReader struct {
	c      rueidis.Client
	stream string
	next   string // XRANGE start
}

func openRedis(ctx context.Context, t *Target, since time.Time) (reader, error) {
	opt, err := rueidis.ParseURL(t.url)
	if err != nil {
		return nil, errors.New("redis: the URL is not valid") // rueidis quotes it
	}
	opt.DisableCache = true // a read once; no RESP3 tracking needed
	if t.tls && t.ca != "" {
		pool, err := caPool(t.ca)
		if err != nil {
			return nil, err
		}
		opt.TLSConfig.RootCAs = pool
	}
	c, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", cleanErr(err, t.url))
	}
	start := since.Add(-Margin).UnixMilli()
	if start < 0 {
		start = 0
	}
	return &redisReader{c: c, stream: t.stream, next: strconv.FormatInt(start, 10) + "-0"}, nil
}

func (r *redisReader) Incremental() bool { return true }
func (r *redisReader) Close()            { r.c.Close() }

func (r *redisReader) Read(ctx context.Context, fn func(Entry)) error {
	for ctx.Err() == nil {
		res, err := r.c.Do(ctx, r.c.B().Xrange().Key(r.stream).Start(r.next).End("+").Count(1000).Build()).AsXRange()
		if err != nil {
			return fmt.Errorf("redis XRANGE: %w", err)
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
	c     *pgx.Conn
	query string
	since time.Time
	like  string
}

func openPostgres(ctx context.Context, t *Target, since time.Time, run string) (reader, error) {
	cfg, err := pgx.ParseConfig(t.url)
	if err != nil {
		return nil, errors.New("postgres: the URL is not valid") // pgx may quote it
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", cleanErr(err, t.url))
	}
	table := pgx.Identifier(strings.Split(t.table, ".")).Sanitize()
	q := `SELECT uuid::text, coalesce(payload->>'message',''), coalesce(payload->>'raw',''), coalesce(payload->>'raw-encoding','')
	        FROM ` + table + `
	       WHERE type = 'syslog' AND received_at >= $1
	         AND (payload->>'message' LIKE $2 OR payload->>'raw' LIKE $2 OR payload->>'raw-encoding' = 'base64')`
	return &pgReader{c: c, query: q, since: since.Add(-Margin), like: "%" + likeEscape(run) + "%"}, nil
}

func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

func (r *pgReader) Incremental() bool { return false }
func (r *pgReader) Close()            { r.c.Close(context.Background()) }

func (r *pgReader) Read(ctx context.Context, fn func(Entry)) error {
	rows, err := r.c.Query(ctx, r.query, r.since, r.like)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
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

func openSQLite(t *Target, run string) (reader, error) {
	if _, err := os.Stat(t.file); err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	// Read only, and wait for the server's writer instead of failing busy.
	db, err := sql.Open("sqlite", "file:"+t.file+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	r := &sqliteReader{db: db}
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

// cleanErr removes the URL from a library error, in case it quotes it (it
// may carry a password).
func cleanErr(err error, u string) error {
	s := err.Error()
	if u != "" && strings.Contains(s, u) {
		return errors.New(strings.ReplaceAll(s, u, "<url>"))
	}
	return err
}
