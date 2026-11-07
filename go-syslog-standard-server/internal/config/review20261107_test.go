package config

// Tests of the independent review of 2026-11-07 (SPEC §21.1, items 21-29) in
// this package: configuration errors name the key and the problem, never the
// value (item 26, SPEC §15.1).

import (
	"strings"
	"testing"
)

// Every value a parser or a rule refuses, each carrying the secret: the
// error names the key and never echoes the value (a secret typed into the
// wrong key, or a value that IS a secret, would otherwise reach stderr or,
// on SIGHUP, the log).
func TestConfigErrorsNeverEchoTheValue(t *testing.T) {
	s := secret
	cases := []struct {
		key  string
		args []string
	}{
		{"sink.workers", []string{"--sink.workers=" + s}},
		{"ring.capacity_items", []string{"--ring.capacity_items=" + s}},
		{"ring.high_water", []string{"--ring.high_water=" + s}},
		{"sink.op_timeout", []string{"--sink.op_timeout=" + s}},
		{"sink.op_timeout", []string{"--sink.op_timeout=-" + s}},
		{"ring.max_bytes", []string{"--ring.max_bytes=" + s}},
		{"ring.max_bytes", []string{"--ring.max_bytes=" + s + "MiB"}},
		{"listen.unix_mode", []string{"--listen.unix_mode=" + s}},
		{"sink.redis.tls", []string{"--sink.redis.tls=" + s}},
		{"ring.mode", []string{"--ring.mode=" + s}},
		{"sink.type", []string{"--sink.type=" + s}},
		{"tcp.framing", []string{"--tcp.framing=" + s}},
		{"parse.prefer", []string{"--parse.prefer=" + s}},
		{"ring.sqlite.synchronous", []string{"--ring.sqlite.synchronous=" + s}},
		{"sink.sqlite.rotate", []string{"--sink.sqlite.rotate=" + s}},
		{"log.level", []string{"--log.level=" + s}},
		{"log.format", []string{"--log.format=" + s}},
		{"listen.tcp", []string{"--listen.tcp=" + s}},
		{"listen.relp", []string{"--listen.tcp=127.0.0.1:" + s, "--listen.relp=127.0.0.1:" + s}},
		{"listen.unix", []string{"--listen.unix=" + s}},
		{"listen.unix", []string{"--listen.unix=/" + strings.Repeat(s, 20)}},
		{"filter.min_severity", []string{"--filter.min_severity=" + s}},
		{"filter.facilities", []string{"--filter.facilities=user," + s}},
		{"ratelimit.exempt", []string{"--ratelimit.exempt=" + s}},
		{"ratelimit.exempt", []string{"--ratelimit.exempt=" + s + "/8"}},
		{"sink.sqlite.path", []string{"--ring.mode=sqlite", "--sink.type=sqlite",
			"--ring.sqlite.path=/data/" + s + ".db", "--sink.sqlite.path=/data/" + s + ".db"}},
		{"sink.postgres.table", []string{"--sink.type=postgres", "--sink.postgres.table=" + s}},
		{"sink.mysql.table", []string{"--sink.type=mysql", "--sink.workers=1", "--sink.mysql.table=" + s + "-x"}},
		{"sink.mysql.database", []string{"--sink.type=mysql", "--sink.workers=1", "--sink.mysql.database=" + s + "`"}},
		{"sink.mysql.database", []string{"--sink.type=mysql", "--sink.workers=1", "--sink.mysql.database=" + s + "\x01"}},
		{"sink.mysql.addr", []string{"--sink.type=mysql", "--sink.workers=1", "--sink.mysql.tls=true",
			"--sink.mysql.addr=/run/" + s + ".sock"}},
		{"sink.nats.stream", []string{"--sink.type=nats", "--sink.nats.stream=" + s + ".x"}},
		{"sink.nats.subject", []string{"--sink.type=nats", "--sink.nats.subject=" + s + ".*"}},
	}
	for _, c := range cases {
		_, err := load(t, c.args, nil)
		if err == nil {
			t.Fatalf("%v accepted", c.args)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("%v: the error echoes the value: %v", c.args, err)
		}
		if !strings.Contains(err.Error(), c.key) {
			t.Fatalf("%v: the error does not name %s: %v", c.args, c.key, err)
		}
	}
}

// Item 29 (SPEC §15.2): the alias rule compares the LAST value of each
// spelling in the source, once, at its end. Compared at every occurrence,
// `--sink.valkey.db=1 --sink.redis.db=2 --sink.valkey.db=2` was refused over a
// value the third flag had already replaced.
func TestValkeyAliasComparesTheLastValueOfEachSpelling(t *testing.T) {
	l := mustLoad(t, []string{"--sink.valkey.db=1", "--sink.redis.db=2", "--sink.valkey.db=2"}, nil)
	if l.Config.SinkRedis.DB != 2 {
		t.Fatalf("db = %d, want 2", l.Config.SinkRedis.DB)
	}
	mustLoad(t, []string{"--config", writeINI(t, "[sink.valkey]\ndb = 1\n[sink.redis]\ndb = 2\n[sink.valkey]\ndb = 2\n")}, nil)

	// The last values still have to agree.
	for _, args := range [][]string{
		{"--sink.valkey.db=2", "--sink.redis.db=2", "--sink.valkey.db=1"},
		{"--sink.redis.db=2", "--sink.valkey.db=2", "--sink.redis.db=3"},
		{"--config", writeINI(t, "[sink.redis]\ndb = 2\n[sink.valkey]\ndb = 2\n[sink.redis]\ndb = 4\n")},
	} {
		if _, err := load(t, args, nil); err == nil {
			t.Fatalf("%v accepted with different last values", args)
		}
	}
}
