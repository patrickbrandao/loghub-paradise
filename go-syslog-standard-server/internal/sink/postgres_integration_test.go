package sink

// Opt-in suite of sink=postgres against a real server (docs/TESTING.md §6.2):
//
//	SLSTD_TEST_PG_DSN='postgres://slstd:slstd@127.0.0.1:38360/slstd_test?sslmode=disable'
//
// Each test creates its table from docker/postgres/schema.sql in a schema of
// its own and drops it at the end. It covers the §21.1 items 9 (an extra NOT
// NULL column refused at boot) and 10 (the validation outlives
// sink.health_timeout); item 11 is TestPostgresIgnoresTheLibpqEnvironment.

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func pgDSN(t *testing.T) *url.URL {
	t.Helper()
	dsn := os.Getenv("SLSTD_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("SLSTD_TEST_PG_DSN not set")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// pgSchema creates a schema with the events table of schema.sql, plus extra
// DDL, and returns the table name schema.events.
func pgSchema(t *testing.T, dsn *url.URL, extra string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	ddl, err := os.ReadFile("../../docker/postgres/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("t%d", time.Now().UnixNano())
	for _, stmt := range []string{
		"CREATE SCHEMA " + schema,
		"SET search_path TO " + schema,
		string(ddl),
		extra,
	} {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", strings.SplitN(stmt, "\n", 2)[0], err)
		}
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), dsn.String())
		if err == nil {
			c.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			c.Close(context.Background())
		}
	})
	return schema + ".events"
}

func pgConfig(t *testing.T, dsn *url.URL, addr, table string) PostgresConfig {
	pw, _ := dsn.User.Password()
	return PostgresConfig{
		Addr: addr, Username: dsn.User.Username(), Password: pw,
		Database: strings.TrimPrefix(dsn.Path, "/"), Table: table,
		ConnectTimeout: 5 * time.Second, AppName: "slstd-test",
	}
}

// Item 9: an extra NOT NULL column without a default is fatal at boot, with
// the column named; with a default, an identity or nullable, it passes.
func TestPostgresIntegrationRefusesAnExtraRequiredColumn(t *testing.T) {
	dsn := pgDSN(t)
	cases := []struct {
		extra string
		fatal bool
	}{
		{"", false},
		{"ALTER TABLE events ADD COLUMN tenant text NOT NULL", true},
		{"ALTER TABLE events ADD COLUMN tenant text NOT NULL DEFAULT 'x'", false},
		{"ALTER TABLE events ADD COLUMN serial_id bigint GENERATED ALWAYS AS IDENTITY", false},
		{"ALTER TABLE events ADD COLUMN note text", false},
	}
	for _, c := range cases {
		t.Run(c.extra, func(t *testing.T) {
			table := pgSchema(t, dsn, c.extra)
			p, err := NewPostgres(pgConfig(t, dsn, dsn.Host, table), discard(), metrics())
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			err = p.Health(context.Background())
			switch {
			case c.fatal && (!IsFatal(err) || !strings.Contains(err.Error(), `"tenant"`)):
				t.Fatalf("Health = %v, want fatal naming the column", err)
			case !c.fatal && err != nil:
				t.Fatalf("Health = %v, want nil", err)
			}
			if c.fatal {
				return
			}
			batch, release := redisBatch(3, "pg")
			defer release()
			if err := p.Write(context.Background(), batch); err != nil {
				t.Fatalf("Write: %v", err)
			}
		})
	}
}

// Item 10: a database ~10 ms away per round trip with sink.health_timeout =
// 50 ms. The probe passes, the dozen round trips of the validation do not
// fit in it -- and must not have to.
func TestPostgresIntegrationValidationOutlivesTheHealthTimeout(t *testing.T) {
	dsn := pgDSN(t)
	table := pgSchema(t, dsn, "")
	proxy := newDelayProxy(t, dsn.Host, 10*time.Millisecond)
	p, err := NewPostgres(pgConfig(t, dsn, proxy, table), discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var lastErr error
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		lastErr = p.Health(ctx)
		cancel()
		if lastErr == nil {
			return
		}
	}
	t.Fatalf("the validation never finished under a 50 ms health timeout: %v", lastErr)
}

// newDelayProxy forwards TCP to upstream, holding each chunk for delay in
// both directions: a distant server, on loopback.
func newDelayProxy(t *testing.T, upstream string, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				time.Sleep(delay)
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		dst.Close()
		src.Close()
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", upstream)
			if err != nil {
				c.Close()
				continue
			}
			go pipe(u, c)
			go pipe(c, u)
		}
	}()
	return ln.Addr().String()
}
