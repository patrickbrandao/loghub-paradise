package sink

// Port of the SQL corrections of the syslog/RELP family (2026-10-13) that
// need no server: the database name between backticks (SPEC §8.6.3, §8.8.3),
// the session's autocommit (SPEC §8.6.1) and the memory check's count of
// concurrent INSERTs (SPEC §8.8.5). The halves against a real MySQL and
// ClickHouse are in review_port_sql_integration_test.go.

import (
	"strings"
	"testing"
)

// The constructors check the database name as config does, for a caller that
// bypasses it: 64 CHARACTERS (a name of 64 accented letters is 128 bytes and
// legitimate), and nothing that breaks the backticks.
func TestSQLDatabaseNameBetweenBackticks(t *testing.T) {
	long := strings.Repeat("é", 64)
	bad := map[string]string{
		"65 characters": strings.Repeat("é", 65),
		"backtick":      "web`hook",
		"backslash":     `web\hook`,
		"control byte":  "web\nhook",
		"empty":         "",
	}
	t.Run("mysql", func(t *testing.T) {
		p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: long, Table: "inbox"},
			quietLog(), testMetrics())
		if err != nil {
			t.Fatalf("64 characters refused: %v", err)
		}
		p.Close()
		for name, db := range bad {
			p, err := NewMySQL(MySQLConfig{Addr: "127.0.0.1:1", Username: "u", Database: db, Table: "inbox"},
				quietLog(), testMetrics())
			if err == nil {
				p.Close()
				t.Errorf("%s: database %q accepted", name, db)
			}
		}
	})
	t.Run("clickhouse", func(t *testing.T) {
		c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:9000"}, Database: long, Table: "inbox"},
			quietLog(), testMetrics())
		if err != nil {
			t.Fatalf("64 characters refused: %v", err)
		}
		c.Close()
		for name, db := range bad {
			c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:9000"}, Database: db, Table: "inbox"},
				quietLog(), testMetrics())
			if err == nil {
				c.Close()
				t.Errorf("%s: database %q accepted", name, db)
			}
		}
	})
}

// SPEC §8.6.1: every connection pins autocommit=1 beside the strict mode.
// With the server's default at 0 (my.cnf, init_connect) a single-chunk INSERT
// opened a transaction nobody committed: 200, and the rows gone with the
// connection. The read-back at boot is proved against the real server
// (TestMySQLAutocommitIsReadBackAtBoot).
func TestMySQLSessionPinsAutocommit(t *testing.T) {
	ps := mysqlSessionParams()
	if ps["autocommit"] != "1" {
		t.Errorf("session params %v do not pin autocommit=1", ps)
	}
	if !strings.Contains(ps["sql_mode"], "STRICT_TRANS_TABLES") || ps["time_zone"] != "'+00:00'" {
		t.Errorf("session params %v lost the strict mode or the UTC time zone", ps)
	}
}

// SPEC §8.8.5: the per-user memory limit is compared with the worst batch
// times the INSERTs this instance can have in flight -- min(pool_size,
// writers) -- not times the pool: the health check's connection is outside
// it, and the spare slot of the derived sink.workers + 1 has no writer to
// fill it. A limit of exactly workers worst batches was refused.
func TestClickHouseMemoryCountsTheWritersNotThePool(t *testing.T) {
	const batch = 64 << 20
	need := chMemoryNeed(batch)
	for _, tc := range []struct {
		name          string
		pool, writers int
		want          int
	}{
		{"ring: pool = workers + 1", 5, 4, 4},
		{"ring: pool below the workers", 2, 4, 2},
		{"ring=none: max_inflight above the pool", 5, 2048, 5},
		{"writers unknown", 5, 0, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &ClickHouse{cfg: ClickHouseConfig{PoolSize: tc.pool, Writers: tc.writers}}
			if got := c.concurrentInserts(); got != tc.want {
				t.Fatalf("concurrentInserts = %d, want %d", got, tc.want)
			}
			if p := chMemoryProblem(batch, c.concurrentInserts(), 0, need*uint64(tc.want)); p != "" {
				t.Errorf("a per-user limit of exactly %d worst batches refused: %s", tc.want, p)
			}
			p := chMemoryProblem(batch, c.concurrentInserts(), 0, need*uint64(tc.want)-1)
			if !strings.Contains(p, "max_memory_usage_for_user=") {
				t.Fatalf("a limit one byte short passed: %q", p)
			}
			// The advice must not contradict the pool rule: lowering the pool
			// below the workers only makes them queue (SPEC §8.5.3).
			if strings.Contains(p, "lower the pool") {
				t.Errorf("the refusal still advises lowering the pool: %s", p)
			}
			if !strings.Contains(p, "sink.workers") {
				t.Errorf("the refusal does not name the writers it counts: %s", p)
			}
		})
	}
}
