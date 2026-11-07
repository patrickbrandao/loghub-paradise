package config

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// SPEC §13.2, §8.2.6: Valkey speaks the Redis protocol. sink.type=valkey is
// redis, and [sink.valkey], --sink.valkey.* and WHS_SINK_VALKEY_* are the keys
// of [sink.redis], in every source. The canonical names are what the dump
// shows.
func TestValkeyAliasInEverySource(t *testing.T) {
	ini := writeINI(t, "[sink]\ntype = valkey\n[sink.valkey]\naddrs = 10.0.0.1:6379\npassword = s3cret\n")
	for name, o := range map[string]Options{
		"ini": {Args: []string{"--config", ini}, LookupEnv: envMap(nil)},
		"env": {LookupEnv: envMap(map[string]string{"WHS_SINK_TYPE": "Valkey",
			"WHS_SINK_VALKEY_ADDRS": "10.0.0.1:6379", "WHS_SINK_VALKEY_PASSWORD": "s3cret"})},
		"flag": {Args: []string{"--sink.type=valkey", "--sink.valkey.addrs=10.0.0.1:6379",
			"--sink.valkey.password", "s3cret"}, LookupEnv: envMap(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			l := load(t, o)
			c := l.Config
			if c.Sink.Type != SinkRedisType {
				t.Errorf("sink.type = %q, want the canonical redis", c.Sink.Type)
			}
			if len(c.SinkRedis.Addrs) != 1 || c.SinkRedis.Addrs[0] != "10.0.0.1:6379" || c.SinkRedis.Password != "s3cret" {
				t.Errorf("sink.redis = %+v, want the values set through sink.valkey", c.SinkRedis)
			}
			if !l.SetIn["sink.redis.addrs"] || l.Source["sink.redis.addrs"] == FromDefault {
				t.Errorf("sink.redis.addrs not recorded as set: %v %v", l.SetIn["sink.redis.addrs"], l.Source["sink.redis.addrs"])
			}
			var buf bytes.Buffer
			if err := l.DumpINI(&buf); err != nil {
				t.Fatal(err)
			}
			dump := buf.String()
			if strings.Contains(dump, "valkey") {
				t.Errorf("the dump names the alias; it shows only canonical names:\n%s", dump)
			}
			if !regexp.MustCompile(`(?m)^type +\= redis$`).MatchString(dump) ||
				!regexp.MustCompile(`(?m)^addrs +\= 10\.0\.0\.1:6379$`).MatchString(dump) {
				t.Errorf("the dump lacks the canonical values:\n%s", dump)
			}
		})
	}
}

// The precedence does not change: the alias is the same key in its source, so
// a flag through either name wins over the environment through the other.
func TestValkeyAliasKeepsThePrecedence(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.valkey.db=2"},
		LookupEnv: envMap(map[string]string{"WHS_SINK_REDIS_DB": "1"})})
	if l.Config.SinkRedis.DB != 2 || l.Source["sink.redis.db"] != FromFlag {
		t.Errorf("db = %d from %s, want 2 from the flag", l.Config.SinkRedis.DB, l.Source["sink.redis.db"])
	}
	l = load(t, Options{Args: []string{"--sink.redis.db=3"},
		LookupEnv: envMap(map[string]string{"WHS_SINK_VALKEY_DB": "1"})})
	if l.Config.SinkRedis.DB != 3 {
		t.Errorf("db = %d, want 3 from the flag", l.Config.SinkRedis.DB)
	}
}

// The same key through both names in one source: exit 1 only when the
// values differ, naming both keys and never the values (one may be the
// password). Equal values -- equal once parsed -- pass.
func TestValkeyAliasConflict(t *testing.T) {
	const secret = "hunter2-secret"
	refused := []struct {
		name string
		o    Options
		keys []string
	}{
		{"ini", Options{Args: []string{"--config", writeINI(t,
			"[sink.redis]\npassword = "+secret+"\n[sink.valkey]\npassword = other-"+secret+"\n")},
			LookupEnv: envMap(nil)}, []string{"[sink.redis] password", "[sink.valkey] password"}},
		{"ini, alias first", Options{Args: []string{"--config", writeINI(t,
			"[sink.valkey]\ndb = 1\n[sink.redis]\ndb = 2\n")},
			LookupEnv: envMap(nil)}, []string{"[sink.valkey] db", "[sink.redis] db"}},
		{"env", Options{LookupEnv: envMap(map[string]string{
			"WHS_SINK_REDIS_PASSWORD": secret, "WHS_SINK_VALKEY_PASSWORD": "other-" + secret})},
			[]string{"WHS_SINK_REDIS_PASSWORD", "WHS_SINK_VALKEY_PASSWORD"}},
		{"flag", Options{Args: []string{"--sink.redis.password=" + secret, "--sink.valkey.password=other-" + secret},
			LookupEnv: envMap(nil)}, []string{"--sink.redis.password", "--sink.valkey.password"}},
		{"flag repeated after agreeing", Options{Args: []string{"--sink.redis.db=1", "--sink.valkey.db=1",
			"--sink.redis.db=2"}, LookupEnv: envMap(nil)}, []string{"--sink.valkey.db", "--sink.redis.db"}},
	}
	for _, tc := range refused {
		_, err := Load(tc.o)
		if err == nil {
			t.Errorf("%s: two different values through the two names were accepted", tc.name)
			continue
		}
		msg := err.Error()
		for _, k := range tc.keys {
			if !strings.Contains(msg, k) {
				t.Errorf("%s: %q does not name %s", tc.name, msg, k)
			}
		}
		if strings.Contains(msg, secret) {
			t.Errorf("%s: the error echoes a value: %q", tc.name, msg)
		}
	}

	accepted := []Options{
		{Args: []string{"--config", writeINI(t, "[sink.redis]\nread_buffer_each_conn = 1MiB\n[sink.valkey]\nread_buffer_each_conn = 1048576\n")},
			LookupEnv: envMap(nil)},
		{LookupEnv: envMap(map[string]string{"WHS_SINK_REDIS_DB": "4", "WHS_SINK_VALKEY_DB": " 4"})},
		{Args: []string{"--sink.redis.tls=true", "--sink.valkey.tls"}, LookupEnv: envMap(nil)},
		// Across sources it is the precedence, not a conflict.
		{Args: []string{"--sink.valkey.db=5"}, LookupEnv: envMap(map[string]string{"WHS_SINK_REDIS_DB": "4"})},
	}
	for i, o := range accepted {
		if _, err := Load(o); err != nil {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

// SPEC §13.2 (independent review of the Valkey round, 2026-11-07): the guard
// compares the LAST value of each spelling, once, at the end of the source.
// Checked at every line it depended on the order: the flags below ended with
// 2 through both names and were refused. Repeated INI sections the same.
func TestValkeyAliasComparesTheLastValueOfEachSpelling(t *testing.T) {
	for name, o := range map[string]Options{
		"flag": {Args: []string{"--sink.valkey.db=1", "--sink.redis.db=2", "--sink.valkey.db=2"},
			LookupEnv: envMap(nil)},
		"ini": {Args: []string{"--config", writeINI(t,
			"[sink.valkey]\ndb = 1\n[sink.redis]\ndb = 2\n[sink.valkey]\ndb = 2\n")}, LookupEnv: envMap(nil)},
	} {
		l, err := Load(o)
		if err != nil {
			t.Errorf("%s: the last value of each spelling is 2, and it was refused: %v", name, err)
			continue
		}
		if l.Config.SinkRedis.DB != 2 {
			t.Errorf("%s: db = %d, want 2", name, l.Config.SinkRedis.DB)
		}
	}
	// The last values still decide a refusal: an earlier agreement does not
	// excuse a later disagreement.
	_, err := Load(Options{Args: []string{"--config", writeINI(t,
		"[sink.redis]\ndb = 1\n[sink.valkey]\ndb = 1\n[sink.redis]\ndb = 3\n")}, LookupEnv: envMap(nil)})
	if err == nil || !strings.Contains(err.Error(), "[sink.redis] db") || !strings.Contains(err.Error(), "[sink.valkey] db") {
		t.Errorf("repeated sections ending in disagreement: %v", err)
	}
}

// The no-echo rules hold for the alias as for the canonical name: a name that
// starts with an alias parameter and goes on is not shown, and a typo inside
// [sink.valkey] is named, like one inside [sink.redis].
func TestValkeyAliasUnknownNames(t *testing.T) {
	_, err := Load(Options{Args: []string{"--sink.valkey.passwordhunter2"}, LookupEnv: envMap(nil)})
	if err == nil || strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "goes on") {
		t.Errorf("value glued to an alias name: %v", err)
	}
	_, err = Load(Options{Args: []string{"--sink.valkey.adrs=x"}, LookupEnv: envMap(nil)})
	if err == nil || !strings.Contains(err.Error(), `"sink.valkey.adrs"`) {
		t.Errorf("typo inside [sink.valkey]: %v", err)
	}
	_, err = Load(Options{Args: []string{"--config", writeINI(t, "[sink.valkey]\nadrs = x\n")}, LookupEnv: envMap(nil)})
	if err == nil || !strings.Contains(err.Error(), `"sink.valkey.adrs"`) {
		t.Errorf("typo inside [sink.valkey] in an INI: %v", err)
	}
	// A secret through the alias is still a secret: the line after it is
	// not quoted.
	_, err = Load(Options{Args: []string{"--config", writeINI(t, "[sink.valkey]\npassword = abc\nffd3a9c1b2 = x\n")},
		LookupEnv: envMap(nil)})
	if err == nil || strings.Contains(err.Error(), "ffd3a9c1b2") {
		t.Errorf("continuation of a secret set through the alias: %v", err)
	}
}

// --help says the alias exists in one line, without a second table.
func TestValkeyAliasInHelp(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	help := buf.String()
	if !strings.Contains(help, "[sink.valkey]") || !strings.Contains(help, "WHS_SINK_VALKEY_") {
		t.Errorf("--help does not mention the valkey alias")
	}
	if strings.Count(help, "[sink.valkey]") != 1 {
		t.Errorf("--help repeats [sink.valkey]: the alias is one line, not a table")
	}
}
