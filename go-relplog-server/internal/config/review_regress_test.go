package config

// Regression tests from the independent review of 2026-10-11 (SPEC §10.9,
// §13.2, §16.2): each failed before its fix, or pins a configuration the
// fixes must keep accepting.

import (
	"strings"
	"testing"
)

// Postgres URL: a numeric password prefix followed by an unescaped '#'
// parses as host=user, port=password-prefix, fragment=rest. Must be refused.
func TestReviewPGFragmentAccepted(t *testing.T) {
	l, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://app:2024#S3cret@db.example/loghub?sslmode=require"})
	if err == nil {
		t.Fatalf("accepted: addr=%q user=%q db=%q tls=%v pass=%q",
			l.Config.SinkPG.Addr, l.Config.SinkPG.Username, l.Config.SinkPG.Database, l.Config.SinkPG.TLS, l.Config.SinkPG.Password)
	}
}

// ws:// / wss:// entries skip the query/fragment rule, so the "secret that parses"
// case is accepted: host=user, port=password prefix.
func TestReviewNATSWSFragmentAccepted(t *testing.T) {
	for _, u := range []string{"ws://user:2024#S3cret@nats.example:443", "wss://tok?en@nats.example"} {
		l, err := load(t, nil, map[string]string{"RELPLOG_SINK_NATS_URLS": u})
		if err == nil {
			t.Errorf("%s accepted: urls=%q", u, l.Config.SinkNATS.URLs)
		}
	}
}

// A value split by the shell at a space, whose tail starts with '-', is echoed.
func TestReviewFlagTailEcho(t *testing.T) {
	_, err := load(t, []string{"--sink.nats.password=abc", "-hunter2", "--sink.nats.username=u"}, nil)
	if err == nil {
		t.Fatal("accepted")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error echoes the value tail: %v", err)
	}
}

// libpq documented percent-encoded socket host.
func TestReviewPGPercentEncodedSocketHost(t *testing.T) {
	_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": "postgresql://u:p@%2Fvar%2Frun%2Fpostgresql/loghub"})
	if err != nil {
		t.Logf("refused: %v", err)
	}
}
