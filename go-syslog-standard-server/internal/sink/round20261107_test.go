package sink

// Tests of the §21.1 items fixed on 2026-11-07 in this package (9, 10, 11, 12,
// 13, 16) and of the Redis/Valkey rules of SPEC §10.3 that need no server.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Redis / Valkey (SPEC §10.3) ----------------------------------------------

// sink.redis.db != 0 against a cluster: both answers are fatal at once, never
// the unrecognised reply retried until sink.boot_timeout.
func TestClassifyClusterDBRepliesAreFatal(t *testing.T) {
	for _, reply := range []string{
		"ERR SELECT is not allowed in cluster mode",
		"ERR DB index is out of range",
	} {
		if got := classifyRedisReply(reply); got != ClassFatal {
			t.Errorf("classifyRedisReply(%q) = %v, want fatal", reply, got)
		}
	}
	// The match is on the text, not the generic ERR prefix.
	if got := classifyRedisReply("ERR unknown subcommand"); got != ClassNetwork {
		t.Errorf("a generic ERR became %v", got)
	}
}

// The four INFO server answers of redis-valkey-05: Valkey's real version is
// valkey_version, and redis_version:7.2.4 is never what is reported for it.
func TestParseServerInfo(t *testing.T) {
	cases := []struct {
		info          string
		name, version string
	}{
		{"# Server\r\nredis_version:7.4.12\r\nredis_mode:standalone\r\n", "redis", "7.4.12"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:7.2.15\r\n", "valkey", "7.2.15"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:8.1.11\r\n", "valkey", "8.1.11"},
		{"# Server\r\nredis_version:7.2.4\r\nserver_name:valkey\r\nvalkey_version:9.0.7\r\n", "valkey", "9.0.7"},
		{"", "unknown", ""},
	}
	for _, c := range cases {
		name, version := parseServerInfo(c.info)
		if name != c.name || version != c.version {
			t.Errorf("parseServerInfo(%q) = %s %s, want %s %s", c.info, name, version, c.name, c.version)
		}
	}
}

// pendência 13: allkeys-* is fatal, noeviction and volatile-* pass.
func TestEvictionVerdict(t *testing.T) {
	for _, p := range []string{"allkeys-lru", "allkeys-lfu", "allkeys-random", " ALLKEYS-LRU "} {
		if err := evictionVerdict(p); !errors.Is(err, ErrEvictionPolicy) {
			t.Errorf("evictionVerdict(%q) = %v, want ErrEvictionPolicy", p, err)
		}
		if err := classifyEviction(evictionVerdict(p)); !IsFatal(err) {
			t.Errorf("%q is not fatal: %v", p, err)
		}
	}
	for _, p := range []string{"noeviction", "volatile-lru", "volatile-ttl", "volatile-random"} {
		if err := evictionVerdict(p); err != nil {
			t.Errorf("evictionVerdict(%q) = %v, want nil", p, err)
		}
	}
	if p, ok := policyFromInfo("# Memory\r\nused_memory:1\r\nmaxmemory_policy:allkeys-lru\r\n"); !ok || p != "allkeys-lru" {
		t.Fatalf("policyFromInfo = %q, %v", p, ok)
	}
	if _, ok := policyFromInfo("# Memory\r\nused_memory:1\r\n"); ok {
		t.Fatal("policyFromInfo found a policy in an INFO without one")
	}
	if _, err := evictionFromConfig(nil, nil, errors.New("NOPERM")); !errors.Is(err, ErrPolicyUnknown) {
		t.Fatalf("a CONFIG GET with no policy = %v, want ErrPolicyUnknown", err)
	}
}

// --- pendência 10: the validation context -------------------------------------

// The health check's deadline does not end the validation; a cancellation of
// the boot does.
func TestValidationContextOutlivesTheHealthDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-parent.Done()
	vctx, vcancel := validationContext(parent, time.Second)
	defer vcancel()
	if err := vctx.Err(); err != nil {
		t.Fatalf("the validation context died with the health deadline: %v", err)
	}

	boot, bootCancel := context.WithCancel(context.Background())
	hctx, hcancel := context.WithTimeout(boot, time.Hour)
	defer hcancel()
	vctx2, vcancel2 := validationContext(hctx, time.Hour)
	defer vcancel2()
	bootCancel()
	select {
	case <-vctx2.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled boot did not reach the validation")
	}
}

// --- pendência 11: the libpq environment ----------------------------------------

func TestPostgresIgnoresTheLibpqEnvironment(t *testing.T) {
	t.Setenv("PGSSLMODE", "bogus")
	t.Setenv("PGPORT", "not-a-port")
	t.Setenv("PGCHANNELBINDING", "require")
	t.Setenv("PGTARGETSESSIONATTRS", "standby")
	t.Setenv("PGCONNECT_TIMEOUT", "1")
	p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:5432", Username: "u", Database: "d",
		Table: "events", ConnectTimeout: 3 * time.Second}, discard(), metrics())
	if err != nil {
		t.Fatalf("an environment variable of libpq broke the boot: %v", err)
	}
	defer p.Close()
	cc := p.pool.Config().ConnConfig
	if cc.TLSConfig != nil || cc.ValidateConnect != nil || len(cc.Fallbacks) != 0 {
		t.Fatalf("PG* reached the pool: tls=%v validate=%v fallbacks=%d",
			cc.TLSConfig != nil, cc.ValidateConnect != nil, len(cc.Fallbacks))
	}
	if cc.ConnectTimeout != 3*time.Second || cc.Port != 5432 {
		t.Fatalf("connect_timeout = %v, port = %d", cc.ConnectTimeout, cc.Port)
	}
	if v, ok := cc.RuntimeParams["target_session_attrs"]; ok {
		t.Fatalf("target_session_attrs = %q reached the session", v)
	}
	if got := mustEnv(t, "PGSSLMODE"); got != "bogus" {
		t.Fatalf("the environment was not restored: PGSSLMODE = %q", got)
	}
}

func mustEnv(t *testing.T, k string) string {
	t.Helper()
	v, _ := os.LookupEnv(k)
	return v
}

// --- pendência 12: the MySQL database name --------------------------------------

func TestMySQLConstructorRefusesAQuotedDatabase(t *testing.T) {
	for _, db := range []string{"sys`log", `sys\log`, "a\x00b", strings.Repeat("d", 65), ""} {
		if _, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:3306", Database: db, Table: "events"},
			discard(), metrics()); err == nil {
			t.Errorf("database %q accepted", db)
		}
	}
	m, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:3306", Database: "syslog-prod", Table: "events"},
		discard(), metrics())
	if err != nil {
		t.Fatalf("a valid database name refused: %v", err)
	}
	m.Close()
}

// --- pendência 16: the sqlite probe and the writer's lock -----------------------

func TestSQLiteHealthDoesNotWaitOutABusyWriter(t *testing.T) {
	s, err := NewSQLite(SQLiteConfig{Path: filepath.Join(t.TempDir(), "events.db"), Synchronous: "OFF"},
		discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A batch in progress: the writer holds the lock.
	s.mu.Lock()
	s.writing.Store(true)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		done <- s.Health(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Health = %v behind a busy writer; a long batch is not a DOWN database", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Health waited for the writer past its own deadline")
	}

	// Held by something that is not a write: the deadline is a failure.
	s.writing.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = s.Health(ctx)
	cancel()
	if err == nil {
		t.Fatal("Health passed with the lock held and no write in progress")
	}
	s.mu.Unlock()
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health after the lock came back: %v", err)
	}
}
