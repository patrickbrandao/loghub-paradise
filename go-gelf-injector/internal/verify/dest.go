package verify

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
	_ "modernc.org/sqlite" // the driver go-gelf-server writes events.db with
)

// openTimeout bounds the connection and the check of a destination.
const openTimeout = 10 * time.Second

// ---- NATS JetStream (go-gelf-server SPEC §3.8, §10.7) ----

type natsDest struct {
	s     *Spec
	run   string
	since time.Time
	nc    *nats.Conn
	cons  jetstream.Consumer
	// last is the highest stream sequence counted: a message at or below it
	// is never counted twice, whatever the consumer redelivers (an ordered
	// consumer that resets starts over from its start time; go-syslog-
	// injector found FetchNoWait doing that on every call).
	last uint64
}

func (d *natsDest) Incremental() bool { return true }

func (d *natsDest) Check(ctx context.Context) error {
	opts := []nats.Option{nats.Name("go-gelf-injector verify"), nats.Timeout(openTimeout), nats.MaxReconnects(-1)}
	if d.s.TLSRoots != nil {
		opts = append(opts, nats.Secure(&tls.Config{RootCAs: d.s.TLSRoots, MinVersion: tls.VersionTLS12}))
	}
	nc, err := nats.Connect(d.s.raw, opts...)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	d.nc = nc
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	st, err := js.Stream(cctx, d.s.stream)
	if err != nil {
		return fmt.Errorf("nats: stream %s: %w", d.s.stream, err)
	}
	// The check runs before the first send: everything of the run lands
	// after the stream's last sequence now. Starting there needs no clock
	// and reads nothing of the past (a start time with the clock slack of
	// the window read every message of the last 10 minutes: 250 000 in the
	// E2E, past any --verify-timeout).
	info, err := st.Info(cctx)
	if err != nil {
		return fmt.Errorf("nats: stream %s: %w", d.s.stream, err)
	}
	// Ordered and ephemeral: nothing stays on the server, and the
	// consumer resumes where the last read stopped.
	d.cons, err = st.OrderedConsumer(cctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{d.s.subject},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    info.State.LastSeq + 1,
	})
	if err != nil {
		return fmt.Errorf("nats: consumer: %w", err)
	}
	return nil
}

func (d *natsDest) Read(ctx context.Context, c *Counter) error {
	if d.cons == nil {
		if err := d.Check(ctx); err != nil {
			return err
		}
	}
	for ctx.Err() == nil {
		b, err := d.cons.Fetch(1000, jetstream.FetchMaxWait(300*time.Millisecond))
		if err != nil {
			return fmt.Errorf("nats: fetch: %w", err)
		}
		n := 0
		var pending uint64
		for m := range b.Messages() {
			n++
			md, err := m.Metadata()
			if err != nil {
				return fmt.Errorf("nats: metadata: %w", err)
			}
			pending = md.NumPending
			if md.Sequence.Stream <= d.last {
				continue
			}
			d.last = md.Sequence.Stream
			c.AddJLH(m.Data(), d.run)
		}
		if err := b.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("nats: fetch: %w", err)
		}
		if n == 0 || pending == 0 {
			return nil // caught up
		}
	}
	return ctx.Err()
}

func (d *natsDest) Close() {
	if d.nc != nil {
		d.nc.Close()
	}
}

// ---- PostgreSQL loghub.inbox (go-gelf-server SPEC §3.7.1) ----

type pgDest struct {
	s     *Spec
	run   string
	since time.Time
	conn  *pgx.Conn
}

func (d *pgDest) Incremental() bool { return false }

func (d *pgDest) Check(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	conn, err := pgx.Connect(cctx, d.s.raw)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	d.conn = conn
	// The columns the check reads, and the right to read them (the server's
	// role has only SELECT (uuid), SPEC §10.2).
	if _, err := conn.Exec(cctx, `SELECT uuid, type, payload FROM loghub.inbox LIMIT 0`); err != nil {
		return fmt.Errorf("postgres: loghub.inbox: %w", err)
	}
	return nil
}

func (d *pgDest) Read(ctx context.Context, c *Counter) error {
	if d.conn == nil || d.conn.IsClosed() {
		if err := d.Check(ctx); err != nil {
			return err
		}
	}
	// The floor of the UUIDv7 uses the primary key: the queue is scanned
	// from the window on, not whole.
	rows, err := d.conn.Query(ctx, `SELECT payload->'fields'->>'seq' FROM loghub.inbox
		WHERE uuid >= $1::uuid AND type = 'gelf' AND space = $3 AND payload->'fields'->>'run' = $2`,
		uuidFloor(d.since), d.run, d.s.space)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq *string
		if err := rows.Scan(&seq); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		if seq == nil {
			c.Unparsable++
			continue
		}
		c.AddSeq(*seq)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

func (d *pgDest) Close() {
	if d.conn != nil {
		_ = d.conn.Close(context.Background())
	}
}

// ---- Redis / Valkey stream (go-gelf-server SPEC §3.6, §10.3) ----

type redisDest struct {
	s     *Spec
	run   string
	since time.Time
	cl    rueidis.Client
	next  string // XRANGE start: the window's first ID, then after the last read
}

func (d *redisDest) Incremental() bool { return true }

func (d *redisDest) Check(ctx context.Context) error {
	opt, err := rueidis.ParseURL(d.s.raw)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	opt.DisableCache = true // no CLIENT TRACKING: plain reads
	opt.Dialer.Timeout = openTimeout
	if d.s.TLSRoots != nil && opt.TLSConfig != nil {
		opt.TLSConfig.RootCAs = d.s.TLSRoots
	}
	cl, err := rueidis.NewClient(opt)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	d.cl = cl
	cctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	typ, err := cl.Do(cctx, cl.B().Type().Key(d.s.stream).Build()).ToString()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	// "none": the server creates the stream at its first XADD.
	if typ != "stream" && typ != "none" {
		return fmt.Errorf("redis: key %s is a %s, not a stream", d.s.stream, typ)
	}
	// As with NATS: the run's entries come after the last one now.
	d.next = "-"
	if typ == "stream" {
		last, err := cl.Do(cctx, cl.B().Xrevrange().Key(d.s.stream).End("+").Start("-").Count(1).Build()).AsXRange()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if len(last) == 1 {
			d.next = "(" + last[0].ID
		}
	}
	return nil
}

func (d *redisDest) Read(ctx context.Context, c *Counter) error {
	if d.cl == nil {
		if err := d.Check(ctx); err != nil {
			return err
		}
	}
	for ctx.Err() == nil {
		es, err := d.cl.Do(ctx, d.cl.B().Xrange().Key(d.s.stream).Start(d.next).End("+").Count(1000).Build()).AsXRange()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				return nil
			}
			return fmt.Errorf("redis: %w", err)
		}
		for _, e := range es {
			if v, ok := e.FieldValues["e"]; ok {
				c.AddJLH([]byte(v), d.run)
			}
			d.next = "(" + e.ID // exclusive: after the last one read
		}
		if len(es) < 1000 {
			return nil
		}
	}
	return ctx.Err()
}

func (d *redisDest) Close() {
	if d.cl != nil {
		d.cl.Close()
	}
}

// ---- SQLite gelf_events (go-gelf-server SPEC §3.7, §10.4) ----

type sqliteDest struct {
	s     *Spec
	run   string
	since time.Time
	db    *sql.DB
}

func (d *sqliteDest) Incremental() bool { return false }

func (d *sqliteDest) Check(ctx context.Context) error {
	if _, err := os.Stat(d.s.path); err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	// Read-only, next to a server that writes it (WAL): never create it,
	// never take the writer's lock.
	dsn := "file:" + (&url.URL{Path: d.s.path}).EscapedPath() + "?mode=ro&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	d.db = db
	cctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	if _, err := db.ExecContext(cctx, `SELECT uuid, env FROM gelf_events LIMIT 0`); err != nil {
		return fmt.Errorf("sqlite: gelf_events: %w", err)
	}
	return nil
}

func (d *sqliteDest) Read(ctx context.Context, c *Counter) error {
	if d.db == nil {
		if err := d.Check(ctx); err != nil {
			return err
		}
	}
	// env is a BLOB: json_extract would read a BLOB as JSONB, so it is cast
	// to TEXT first. The run compares as text, whatever its JSON type.
	rows, err := d.db.QueryContext(ctx, `SELECT CAST(json_extract(CAST(env AS TEXT), '$.payload.fields.seq') AS TEXT)
		FROM gelf_events WHERE uuid >= ? AND space = ?
		AND CAST(json_extract(CAST(env AS TEXT), '$.payload.fields.run') AS TEXT) = ?`, uuidFloor(d.since), d.s.space, d.run)
	if err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq sql.NullString
		if err := rows.Scan(&seq); err != nil {
			return fmt.Errorf("sqlite: %w", err)
		}
		if !seq.Valid {
			c.Unparsable++
			continue
		}
		c.AddSeq(seq.String)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	return nil
}

func (d *sqliteDest) Close() {
	if d.db != nil {
		d.db.Close()
	}
}
