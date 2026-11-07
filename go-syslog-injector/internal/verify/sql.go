package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "modernc.org/sqlite" // the driver the servers' sink=sqlite uses
)

// pgReader reads the loghub.inbox queue of sink=postgres (SPEC §8.5; the
// server's SPEC §3.7.1), rereading the rows of the run at every poll: the
// uuid order is not the commit order of concurrent workers, so an
// incremental cursor could skip a row.
type pgReader struct {
	t     *Target
	c     *pgx.Conn
	query string
}

func openPostgres(ctx context.Context, t *Target) (reader, error) {
	cfg, err := pgx.ParseConfig(t.URL)
	if err != nil {
		return nil, errors.New("postgres: the URL does not parse")
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	table := pgx.Identifier{"loghub", "inbox"}
	if t.Key != "" {
		table = pgx.Identifier(strings.Split(t.Key, "."))
	}
	q := "SELECT payload->>'message', payload->>'raw' FROM " + table.Sanitize() +
		" WHERE type = 'syslog' AND space = $1 AND event_at >= $2" +
		" AND (strpos(payload->>'message', $3) > 0 OR strpos(payload->>'raw', $3) > 0)"
	r := &pgReader{t: t, c: c, query: q}
	// Prove the table and the columns now, before anything is sent.
	if _, err := c.Exec(ctx, "SELECT 1 FROM "+table.Sanitize()+" WHERE type = 'syslog' AND space = $1 AND event_at >= $2 AND payload ? 'message' LIMIT 0", t.Space, t.Since); err != nil {
		c.Close(context.Background())
		return nil, fmt.Errorf("postgres: table %s: %w", table.Sanitize(), err)
	}
	return r, nil
}

func (r *pgReader) poll(ctx context.Context, cnt *Counter) error {
	rows, err := r.c.Query(ctx, r.query, r.t.Space, r.t.Since, "run="+r.t.Run)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer rows.Close()
	cnt.Reset()
	for rows.Next() {
		var msg, raw *string
		if err := rows.Scan(&msg, &raw); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		if seq, ok := fromPayload(deref(msg), deref(raw), r.t.Run); ok {
			cnt.Add(seq)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (r *pgReader) final() error { return nil }
func (r *pgReader) close()       { r.c.Close(context.Background()) }

// sqliteReader reads the events.db of sink=sqlite (SPEC §8.5; the server's
// SPEC §3.7), read-only, while the server writes it in WAL mode.
type sqliteReader struct {
	t     *Target
	db    *sql.DB
	query string
}

func openSQLite(ctx context.Context, t *Target) (reader, error) {
	db, err := sql.Open("sqlite", "file:"+t.Path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	table := "syslog_events"
	if t.Key != "" {
		table = t.Key
	}
	q := `SELECT CAST(env AS TEXT) FROM "` + table + `" WHERE space = ? AND ev_uts >= ? AND instr(CAST(env AS TEXT), ?) > 0`
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM "`+table+`" WHERE space = ? AND ev_uts >= ? LIMIT 0`, t.Space, t.Since.Unix()); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: table %s: %w", table, err)
	}
	return &sqliteReader{t: t, db: db, query: q}, nil
}

func (r *sqliteReader) poll(ctx context.Context, cnt *Counter) error {
	rows, err := r.db.QueryContext(ctx, r.query, r.t.Space, r.t.Since.Unix(), "run="+r.t.Run)
	if err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	defer rows.Close()
	cnt.Reset()
	for rows.Next() {
		var env string
		if err := rows.Scan(&env); err != nil {
			return fmt.Errorf("sqlite: %w", err)
		}
		countJLH([]byte(env), r.t, cnt)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: %w", err)
	}
	return nil
}

func (r *sqliteReader) final() error { return nil }
func (r *sqliteReader) close()       { r.db.Close() }
