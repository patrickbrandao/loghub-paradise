package config

// Regression tests from the independent review of 2026-10-11 (SPEC §10.9,
// §13.2, §16.2): each failed before its fix, or pins a configuration the
// fixes must keep accepting.

import "testing"

func TestReviewAccepts(t *testing.T) {
	for _, env := range []map[string]string{
		{"RELPLOG_SINK_NATS_URLS": "nats://[::1]:4222"},
		{"RELPLOG_SINK_NATS_URLS": "[::1]:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats://[fe80::1%25eth0]:4222"},
		{"RELPLOG_SINK_NATS_URLS": "wss://nats.example:443/ws/nats"},
		{"RELPLOG_SINK_NATS_URLS": "nats://u:p%2Fq%40r%23s@h:4222,nats://u:p%2Fq%40r%23s@h2:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats://u:p@ss@h:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats.example:4222"},
		{"LOGHUB_EB_URL": "nats://[::1]:4222"},
		{"LOGHUB_EB_URL": "tls://u:p%2F@h1:4222,tls://u:p%2F@h2:4222"},
		{"LOGHUB_EB_URL": "redis://:p%2Fw@[::1]:6380/2"},
		{"LOGHUB_EB_URL": "rediss://u:p@ss@redis.example/0"},
		{"LOGHUB_EB_URL": "postgres://u:p%2Fw@[2001:db8::1]:5433/loghub?sslmode=verify-full"},
		{"LOGHUB_EB_URL": "postgresql://u:p@/loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgresql://u:p@:5433/loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgresql:///loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgres://u:p@db/loghub"},
		{"LOGHUB_EB_URL": "postgres://u@db"},
		{"RELPLOG_SINK_TYPE": "postgres", "RELPLOG_SINK_POSTGRES_ADDR": "/var/run/postgresql"},
		{"RELPLOG_SINK_TYPE": "postgres", "RELPLOG_SINK_POSTGRES_ADDR": "/var/run/postgresql:5433"},
		{"RELPLOG_SINK_TYPE": "postgres", "RELPLOG_SINK_POSTGRES_ADDR": "[::1]:5432"},
		{"RELPLOG_SINK_TYPE": "mysql", "RELPLOG_SINK_MYSQL_DATABASE": "my-db.prod"},
		{"RELPLOG_SINK_TYPE": "clickhouse", "RELPLOG_SINK_CLICKHOUSE_DATABASE": "logs-2026", "RELPLOG_SINK_CLICKHOUSE_ADDRS": "[::1]:9000"},
		{"LOGHUB_BEACON_INTERVAL": "0"},
		{"LOGHUB_BEACON_INTERVAL": "1"},
		{"RELPLOG_LISTEN_TCP": "[::1]:20514", "RELPLOG_LISTEN_TLS": "127.0.0.1:20514", "RELPLOG_TLS_CERT_FILE": "/c", "RELPLOG_TLS_KEY_FILE": "/k"},
	} {
		if _, err := load(t, nil, env); err != nil {
			t.Errorf("%v refused: %v", env, err)
		}
	}
}
