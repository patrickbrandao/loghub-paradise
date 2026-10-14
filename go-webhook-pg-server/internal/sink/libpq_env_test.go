package sink

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SPEC §8.5.2 (port of go-webhook-server, W 15): the pool is parsed with every PG* hidden. Only
// what the registry owns survives, plus client_encoding. No PostgreSQL needed:
// the pool is lazy. Ported from go-syslog-server's
// TestNewPostgresIgnoresTheLibpqEnvironment.
func TestNewPostgresParsesWithoutTheLibpqEnvironment(t *testing.T) {
	t.Setenv("PGOPTIONS", "-c search_path=nowhere")
	t.Setenv("PGTARGETSESSIONATTRS", "standby")
	t.Setenv("PGAPPNAME", "from-env")
	t.Setenv("PGHOST", "10.255.255.1,10.255.255.2")

	build := func(t *testing.T) *pgxpool.Config {
		t.Helper()
		p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", Table: "loghub.inbox", AppName: "whs",
			ConnectTimeout: 7 * time.Second}, quietLog(), testMetrics())
		if err != nil {
			t.Fatalf("the libpq environment reached the parse: %v", err)
		}
		t.Cleanup(func() { p.Close() })
		return p.pool.Config()
	}

	// These failed the parse itself: exit 1 over a variable the registry does
	// not know.
	for name, kv := range map[string][2]string{
		"PGSSLMODE=bogus":         {"PGSSLMODE", "bogus"},
		"PGPORT not a port":       {"PGPORT", "not-a-port"},
		"PGSERVICE missing":       {"PGSERVICE", "no-such-service"},
		"PGCONNECT_TIMEOUT bogus": {"PGCONNECT_TIMEOUT", "soon"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("PGSERVICEFILE", filepath.Join(t.TempDir(), "absent.conf"))
			t.Setenv(kv[0], kv[1])
			if _, err := pgxpool.ParseConfig(""); err == nil {
				t.Fatalf("control: pgx parsed %s=%s; the environment did not reach the parser", kv[0], kv[1])
			}
			build(t)
			if os.Getenv(kv[0]) != kv[1] {
				t.Errorf("%s was not restored after the parse", kv[0])
			}
		})
	}

	// These parse, and landed in fields nothing overwrote: every connection
	// refused with no SQLSTATE -- network, retried until boot_timeout.
	t.Run("connection settings", func(t *testing.T) {
		t.Setenv("PGSSLMODE", "require")
		t.Setenv("PGCHANNELBINDING", "require")
		t.Setenv("PGREQUIREAUTH", "md5")
		t.Setenv("PGSSLNEGOTIATION", "direct")
		t.Setenv("PGMINPROTOCOLVERSION", "3.2")
		t.Setenv("PGMAXPROTOCOLVERSION", "3.2")
		t.Setenv("PGCONNECT_TIMEOUT", "1")
		ctl, err := pgxpool.ParseConfig("")
		if err != nil {
			t.Fatal(err)
		}
		if c := ctl.ConnConfig; c.ChannelBinding != "require" || c.RequireAuth != "md5" ||
			c.SSLNegotiation != "direct" || c.MinProtocolVersion != "3.2" || len(c.Fallbacks) == 0 ||
			c.ConnectTimeout != time.Second {
			t.Fatalf("control: the environment did not reach the parser (%q %q %q %q, %d fallbacks, %v)",
				c.ChannelBinding, c.RequireAuth, c.SSLNegotiation, c.MinProtocolVersion, len(c.Fallbacks),
				c.ConnectTimeout)
		}

		cc := build(t).ConnConfig
		if cc.ValidateConnect != nil {
			t.Error("ValidateConnect from PGTARGETSESSIONATTRS survived")
		}
		if cc.TLSConfig != nil {
			t.Error("PGSSLMODE reached the pool as a TLSConfig; TLS comes from [sink.postgres] alone")
		}
		if len(cc.Fallbacks) != 0 {
			t.Errorf("%d fallbacks survived", len(cc.Fallbacks))
		}
		if cc.Host != "127.0.0.1" || cc.Port != 1 {
			t.Errorf("host %s port %d, want the configured 127.0.0.1:1 over PGHOST", cc.Host, cc.Port)
		}
		if cc.ChannelBinding == "require" || cc.RequireAuth != "" || cc.SSLNegotiation == "direct" ||
			cc.MinProtocolVersion == "3.2" || cc.MaxProtocolVersion == "3.2" {
			t.Errorf("libpq settings survived: channel_binding=%q require_auth=%q sslnegotiation=%q "+
				"min_protocol=%q max_protocol=%q", cc.ChannelBinding, cc.RequireAuth, cc.SSLNegotiation,
				cc.MinProtocolVersion, cc.MaxProtocolVersion)
		}
		if cc.ConnectTimeout != 7*time.Second {
			t.Errorf("connect timeout %v, want the configured 7s", cc.ConnectTimeout)
		}
		// PGCONNECT_TIMEOUT also made pgx build a DialFunc whose net.Dialer
		// carries that timeout (pgconn makeConnectTimeoutDialFunc), cutting
		// every dial short of the configured deadline. The pool dials with a
		// dialer of its own, whose only deadline is the context's.
		if name := runtime.FuncForPC(reflect.ValueOf(cc.DialFunc).Pointer()).Name(); !strings.HasSuffix(name, ".dialPostgres") {
			t.Errorf("DialFunc is %s, want the sink's dialPostgres", name)
		}
		want := map[string]string{"client_encoding": "UTF8", "application_name": "whs"}
		if !reflect.DeepEqual(cc.RuntimeParams, want) {
			t.Errorf("runtime params %v, want %v", cc.RuntimeParams, want)
		}
	})
}
