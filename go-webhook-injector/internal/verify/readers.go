package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
	_ "modernc.org/sqlite" // the driver go-webhook-server uses, same version
)

// --- NATS JetStream (W SPEC §3.8) ---

// natsStreams are the default stream names of W (LOGHUB_WEBHOOK_REQUEST) and
// of F and N (LOGHUB_WEBHOOK).
var natsStreams = []string{"LOGHUB_WEBHOOK_REQUEST", "LOGHUB_WEBHOOK"}

type natsReader struct {
	nc     *nats.Conn
	stream string
	cons   jetstream.Consumer
}

func openNATS(ctx context.Context, rawURL, stream string, timeout time.Duration) (*natsReader, error) {
	nc, err := nats.Connect(rawURL, nats.Name("go-webhook-injector verify"), nats.Timeout(timeout),
		nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("--verify-url nats: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("--verify-url nats: %v", err)
	}
	var st jetstream.Stream
	if stream == "" {
		var found []string
		for _, s := range natsStreams {
			if x, err := js.Stream(ctx, s); err == nil {
				found, st = append(found, s), x
			} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
				nc.Close()
				return nil, fmt.Errorf("--verify-url nats: stream %s: %v", s, err)
			}
		}
		if len(found) != 1 {
			nc.Close()
			return nil, fmt.Errorf("--verify-url nats: found %d of the streams %s; name it with --verify-stream",
				len(found), strings.Join(natsStreams, ", "))
		}
		stream = found[0]
	} else if st, err = js.Stream(ctx, stream); err != nil {
		nc.Close()
		return nil, fmt.Errorf("--verify-url nats: stream %s: %v", stream, err)
	}
	// This runs before the first request: everything of the run lands
	// after the stream's last sequence now. Starting there reads nothing
	// of the past and needs no clock (SPEC §8.6.3; a start time a minute
	// back re-read every message of a busy stream: go-gelf-injector found
	// ~250 000 of them, past --verify-timeout).
	info, err := st.Info(ctx)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("--verify-url nats: stream %s: %v", stream, err)
	}
	// An ordered consumer is ephemeral, unacknowledged, and resumes where
	// it stopped: each read continues the last one.
	cons, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: info.State.LastSeq + 1})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("--verify-url nats: consumer on %s: %v", stream, err)
	}
	return &natsReader{nc: nc, stream: stream, cons: cons}, nil
}

func (r *natsReader) read(ctx context.Context, fn func(record)) error {
	for ctx.Err() == nil {
		batch, err := r.cons.Fetch(1000, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			return fmt.Errorf("nats fetch: %v", err)
		}
		n := 0
		pending := uint64(0)
		for m := range batch.Messages() {
			n++
			meta, err := m.Metadata()
			if err != nil {
				return fmt.Errorf("nats metadata: %v", err)
			}
			pending = meta.NumPending
			fn(record{key: "n" + strconv.FormatUint(meta.Sequence.Stream, 10), jlh: m.Data()})
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("nats fetch: %v", err)
		}
		if n == 0 || pending == 0 {
			return nil
		}
	}
	return ctx.Err()
}

func (r *natsReader) close() { r.nc.Close() }

// --- PostgreSQL loghub.inbox (W SPEC §3.6) ---

type pgReader struct {
	c     *pgx.Conn
	query string
	floor string
}

func openPG(ctx context.Context, rawURL, table string, since time.Time) (*pgReader, error) {
	c, err := pgx.Connect(ctx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("--verify-url postgres: %v", err)
	}
	var ok bool
	if err := c.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&ok); err != nil || !ok {
		c.Close(context.Background())
		if err == nil {
			err = errors.New("does not exist, or this role cannot see it")
		}
		return nil, fmt.Errorf("--verify-url postgres: table %s: %v", table, err)
	}
	q := fmt.Sprintf(`SELECT uuid::text, coalesce(payload #>> '{content,encoding}', ''), coalesce(payload #>> '{content,body}', '') FROM %s WHERE uuid >= $1::uuid`,
		pgx.Identifier(strings.Split(table, ".")).Sanitize())
	return &pgReader{c: c, query: q, floor: uuidFloor(since)}, nil
}

// read re-reads the whole window: the queue has no insertion order to
// resume from, and the uuid is its primary key, so a re-read record is
// ignored by its key.
func (r *pgReader) read(ctx context.Context, fn func(record)) error {
	rows, err := r.c.Query(ctx, r.query, r.floor)
	if err != nil {
		return fmt.Errorf("postgres: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rec record
		if err := rows.Scan(&rec.uuid, &rec.encoding, &rec.body); err != nil {
			return fmt.Errorf("postgres: %v", err)
		}
		rec.key = rec.uuid
		fn(rec)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: %v", err)
	}
	return nil
}

func (r *pgReader) close() { r.c.Close(context.Background()) }

// --- Redis / Valkey stream (W SPEC §3.3) ---

// redisPattern finds the streams of W and F, loghub.{space}.{type}, in any
// Workspace.
const redisPattern = "loghub.*.webhook*"

// xrangeBatch is the COUNT of one XRANGE.
const xrangeBatch = 5000

type redisReader struct {
	c    rueidis.Client
	key  string            // "" = every key of redisPattern
	last map[string]string // stream key -> last ID read (or there at open)
}

func openRedis(ctx context.Context, rawURL, key string) (*redisReader, error) {
	// valkey:// is redis:// (the protocol is the same, SPEC §8.6.1).
	switch {
	case strings.HasPrefix(rawURL, "valkey://"):
		rawURL = "redis://" + strings.TrimPrefix(rawURL, "valkey://")
	case strings.HasPrefix(rawURL, "valkeys://"):
		rawURL = "rediss://" + strings.TrimPrefix(rawURL, "valkeys://")
	}
	opt, err := rueidis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("--verify-url redis: the URL does not parse")
	}
	opt.DisableCache = true
	c, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("--verify-url redis: %v", err)
	}
	// The stream is born at the server's first XADD: before the run only
	// the connection, and the type of a key that already exists, are checked.
	if key != "" {
		t, err := c.Do(ctx, c.B().Type().Key(key).Build()).ToString()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("--verify-url redis: %v", err)
		}
		if t != "none" && t != "stream" {
			c.Close()
			return nil, fmt.Errorf("--verify-url redis: key %s is a %s, not a stream", key, t)
		}
	} else if err := c.Do(ctx, c.B().Ping().Build()).Error(); err != nil {
		c.Close()
		return nil, fmt.Errorf("--verify-url redis: %v", err)
	}
	r := &redisReader{c: c, key: key, last: map[string]string{}}
	// As with NATS: every stream is read after its last entry now, before
	// the first request (SPEC §8.6.3); a stream born later is read whole.
	keys, err := r.keys(ctx)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("--verify-url redis: scan: %v", err)
	}
	for _, k := range keys {
		es, err := c.Do(ctx, c.B().Xrevrange().Key(k).End("+").Start("-").Count(1).Build()).AsXRange()
		if err != nil && !rueidis.IsRedisNil(err) {
			c.Close()
			return nil, fmt.Errorf("--verify-url redis: %s: %v", k, err)
		}
		if len(es) == 1 {
			r.last[k] = es[0].ID
		}
	}
	return r, nil
}

func (r *redisReader) keys(ctx context.Context) ([]string, error) {
	if r.key != "" {
		return []string{r.key}, nil
	}
	var out []string
	var cursor uint64
	for {
		e, err := r.c.Do(ctx, r.c.B().Scan().Cursor(cursor).Match(redisPattern).Count(1000).Type("stream").Build()).AsScanEntry()
		if err != nil {
			return nil, err
		}
		out = append(out, e.Elements...)
		cursor = e.Cursor
		if cursor == 0 {
			return out, nil
		}
	}
}

// read continues every stream after the last ID it read: XADD IDs grow, so
// nothing written later is behind the cursor. The same envelope added twice
// has two IDs, and is two records.
func (r *redisReader) read(ctx context.Context, fn func(record)) error {
	keys, err := r.keys(ctx)
	if err != nil {
		return fmt.Errorf("redis scan: %v", err)
	}
	for _, k := range keys {
		for {
			start := "-"
			if id, ok := r.last[k]; ok {
				start = "(" + id
			}
			es, err := r.c.Do(ctx, r.c.B().Xrange().Key(k).Start(start).End("+").Count(xrangeBatch).Build()).AsXRange()
			if err != nil {
				if rueidis.IsRedisNil(err) {
					break
				}
				return fmt.Errorf("redis xrange %s: %v", k, err)
			}
			for _, e := range es {
				fn(record{key: "r" + k + " " + e.ID, jlh: []byte(e.FieldValues["e"])})
				r.last[k] = e.ID
			}
			if len(es) < xrangeBatch {
				break
			}
		}
	}
	return nil
}

func (r *redisReader) close() { r.c.Close() }

// --- SQLite inbox (W SPEC §3.7) ---

type sqliteReader struct {
	db    *sql.DB
	query string
	floor string
	rowid int64
}

func openSQLite(ctx context.Context, path, table string, since time.Time) (*sqliteReader, error) {
	if !ValidTable(table) {
		return nil, errors.New("--verify-table: want name or schema.name of [a-z_][a-z0-9_]*")
	}
	// Read only: the server is the writer, and its WAL lets a reader in.
	dsn := "file:" + (&url.URL{Path: path}).EscapedPath() + "?mode=ro&_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("--verify-sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	parts := strings.Split(table, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	t := strings.Join(parts, ".")
	if _, err := db.ExecContext(ctx, "SELECT 1 FROM "+t+" LIMIT 1"); err != nil {
		db.Close()
		return nil, fmt.Errorf("--verify-sqlite: table %s: %v", table, err)
	}
	q := `SELECT rowid, uuid, coalesce(json_extract(payload, '$.content.encoding'), ''),
	  coalesce(json_extract(payload, '$.content.body'), '') FROM ` + t +
		` WHERE rowid > ? AND uuid >= ? ORDER BY rowid LIMIT 5000`
	return &sqliteReader{db: db, query: q, floor: uuidFloor(since)}, nil
}

// read continues after the last rowid: the inbox is a rowid table, and a
// row inserted later gets a larger one.
func (r *sqliteReader) read(ctx context.Context, fn func(record)) error {
	for {
		rows, err := r.db.QueryContext(ctx, r.query, r.rowid, r.floor)
		if err != nil {
			return fmt.Errorf("sqlite: %v", err)
		}
		n := 0
		for rows.Next() {
			var rec record
			if err := rows.Scan(&r.rowid, &rec.uuid, &rec.encoding, &rec.body); err != nil {
				rows.Close()
				return fmt.Errorf("sqlite: %v", err)
			}
			rec.key = rec.uuid
			fn(rec)
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return fmt.Errorf("sqlite: %v", err)
		}
		if n < 5000 {
			return nil
		}
	}
}

func (r *sqliteReader) close() { r.db.Close() }
