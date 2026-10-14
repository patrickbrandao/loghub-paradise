package config

// Port of the SQL corrections of the syslog/RELP family (2026-10-13): the
// database name between backticks (SPEC §8.6.3) and the pool rule now that
// the health check has a connection of its own (SPEC §8.5.3).

import (
	"strings"
	"testing"
)

// sink.mysql.database and sink.clickhouse.database go raw between backticks
// in every INSERT: what would break the quoting is refused at exit 1, and the
// 64-character ceiling counts characters, not bytes.
func TestDatabaseNamesBetweenBackticks(t *testing.T) {
	long := strings.Repeat("é", 64) // 64 characters, 128 bytes: legitimate
	for _, sinkType := range []string{"mysql", "clickhouse"} {
		key := "--sink." + sinkType + ".database="
		t.Run(sinkType, func(t *testing.T) {
			for _, ok := range []string{long, "loghub-prod", "a.b", "ünïcode"} {
				if _, err := Load(Options{Args: []string{"--sink.type=" + sinkType, key + ok},
					LookupEnv: envMap(nil)}); err != nil {
					t.Errorf("database %q refused: %v", ok, err)
				}
			}
			for name, bad := range map[string]string{
				"65 characters": strings.Repeat("é", 65),
				"backtick":      "web`hook",
				"backslash":     `web\hook`,
				"control byte":  "web\x01hook",
				"delete byte":   "web\x7fhook",
			} {
				_, err := Load(Options{Args: []string{"--sink.type=" + sinkType, key + bad},
					LookupEnv: envMap(nil)})
				if err == nil || !strings.Contains(err.Error(), "sink."+sinkType+".database") {
					t.Errorf("%s: database %q = %v, want a refusal naming the key", name, bad, err)
				}
			}
		})
	}
	// The rule bites only when that destination is the sink.
	if _, err := Load(Options{Args: []string{"--sink.type=sqlite", "--sink.mysql.database=a`b",
		"--sink.clickhouse.database=a`b"}, LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("database rules applied to sink.type=sqlite: %v", err)
	}
}

// The health check has its own connection outside the pool (SPEC §8.5.3,
// §8.6.2, §8.8.2): a pool of exactly sink.workers is legitimate, and only a
// smaller one -- the workers queueing for a connection -- warns.
func TestSQLPoolOfExactlyTheWorkersIsLegitimate(t *testing.T) {
	for _, sinkType := range []string{"postgres", "mysql", "clickhouse"} {
		key := "--sink." + sinkType + ".pool_size="
		l := load(t, Options{Args: []string{"--sink.type=" + sinkType, "--sink.workers=4", key + "4"}})
		if hasWarning(l.Warnings, "is below sink.workers") {
			t.Errorf("%s: pool_size = sink.workers warned: %v", sinkType, l.Warnings)
		}
		l = load(t, Options{Args: []string{"--sink.type=" + sinkType, "--sink.workers=4", key + "3"}})
		if !hasWarning(l.Warnings, "sink."+sinkType+".pool_size=3 is below sink.workers=4") {
			t.Errorf("%s: pool_size below the workers: warnings = %v, want the queueing warning",
				sinkType, l.Warnings)
		}
		// ring=none: the workers never get a batch; the hidden-queue warning
		// of max_inflight is the one that applies.
		l = load(t, Options{Args: []string{"--sink.type=" + sinkType, "--ring.mode=none", "--sink.workers=4",
			key + "3", "--ring.max_inflight=3"}})
		if hasWarning(l.Warnings, "is below sink.workers") {
			t.Errorf("%s: ring=none warned about the workers: %v", sinkType, l.Warnings)
		}
	}
}
