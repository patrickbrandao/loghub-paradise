package config

// Regression tests from the independent review of 2026-10-11 of
// go-relplog-server (SPEC §10.9, §13.2, §16.2), kept for what this server
// still parses: nats:// only. Each failed before its fix, or pins a
// configuration the fixes must keep accepting.

import "testing"

func TestReviewAccepts(t *testing.T) {
	for _, env := range []map[string]string{
		{"RELPLOG_SINK_NATS_URLS": "nats://[::1]:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats://[fe80::1%25eth0]:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats://u:p%2Fq%40r%23s@h:4222,nats://u:p%2Fq%40r%23s@h2:4222"},
		{"RELPLOG_SINK_NATS_URLS": "nats://u:p@ss@h:4222"},
		{"LOGHUB_EB_URL": "nats://[::1]:4222"},
		{"LOGHUB_EB_URL": "nats://u:p%2F@h1:4222,nats://u:p%2F@h2:4222"},
		{"LOGHUB_BEACON_INTERVAL": "0"},
		{"LOGHUB_BEACON_INTERVAL": "1"},
		{"RELPLOG_LISTEN_TCP": "[::1]:20514"},
	} {
		if _, err := load(t, nil, env); err != nil {
			t.Errorf("%v refused: %v", env, err)
		}
	}
	// What go-relplog-server accepts and this server refuses: TLS and
	// websocket URLs, a bare host:port (SPEC §1.2).
	for _, env := range []map[string]string{
		{"RELPLOG_SINK_NATS_URLS": "wss://nats.example:443/ws/nats"},
		{"RELPLOG_SINK_NATS_URLS": "nats.example:4222"},
		{"LOGHUB_EB_URL": "tls://u:p%2F@h1:4222,tls://u:p%2F@h2:4222"},
		{"LOGHUB_EB_URL": "postgres://u:p@db/loghub"},
	} {
		if _, err := load(t, nil, env); err == nil {
			t.Errorf("%v accepted", env)
		}
	}
}
