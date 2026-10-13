package config

// Regression tests from the independent review of 2026-10-11 of
// go-relplog-server (SPEC §10.9, §13.2, §16.2).

import (
	"strings"
	"testing"
)

// ws:// / wss:// entries skipped the query/fragment rule in go-relplog-server,
// so the "secret that parses" case was accepted there: host=user, port=password
// prefix. Here they are refused by their scheme, before any parse.
func TestReviewNATSWSFragmentAccepted(t *testing.T) {
	for _, u := range []string{"ws://user:2024#S3cret@nats.example:443", "wss://tok?en@nats.example"} {
		l, err := load(t, nil, map[string]string{"RELPLOG_SINK_NATS_URLS": u})
		if err == nil {
			t.Errorf("%s accepted: urls=%q", u, l.Config.SinkNATS.URLs)
		} else if strings.Contains(err.Error(), "S3cret") || strings.Contains(err.Error(), "tok?en") {
			t.Errorf("%s: the error echoes the secret: %v", u, err)
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
