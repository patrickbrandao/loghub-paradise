package config

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// SPEC §16.2.1: valkey is an exact alias of redis -- values, sections, flags,
// environment -- and the canonical name is what is stored and dumped.
func TestValkeyValueAliases(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=valkey", "--ring.mode=VALKEY",
		"--ring.redis.addrs=ring-redis:6379"}, nil)
	if l.Config.Sink.Type != SinkTypeRedis || l.Config.Ring.Mode != RingModeRedis {
		t.Fatalf("sink.type=%q ring.mode=%q, want both canonical %q",
			l.Config.Sink.Type, l.Config.Ring.Mode, "redis")
	}
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(buf.String()), "valkey") {
		t.Errorf("the dump must show only canonical names and values:\n%s", buf.String())
	}
	// A value that is not an alias is still refused.
	mustFail(t, []string{"--sink.type=keydb"}, nil, "is not one of")
}

func TestValkeyKeyAliases(t *testing.T) {
	want := func(t *testing.T, l *Loaded, origin string) {
		t.Helper()
		r := l.Config.SinkRedis
		if !reflect.DeepEqual(r.Addrs, []string{"vk-1:6379"}) || r.DB != 3 {
			t.Fatalf("addrs=%v db=%d, want [vk-1:6379] 3", r.Addrs, r.DB)
		}
		if !l.SetIn["sink.redis.addrs"] || !strings.Contains(l.Origin["sink.redis.addrs"], origin) {
			t.Errorf("origin %q, want it to mention %q", l.Origin["sink.redis.addrs"], origin)
		}
	}
	t.Run("flag", func(t *testing.T) {
		l := mustLoad(t, []string{"--sink.valkey.addrs=vk-1:6379", "--sink.valkey.db", "3"}, nil)
		want(t, l, "--sink.valkey.addrs")
	})
	t.Run("env", func(t *testing.T) {
		l := mustLoad(t, nil, map[string]string{
			"RELPLOG_SINK_VALKEY_ADDRS": "vk-1:6379", "RELPLOG_SINK_VALKEY_DB": "3"})
		want(t, l, "RELPLOG_SINK_VALKEY_ADDRS")
	})
	t.Run("ini", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "s.ini")
		writeFile(t, f, "[sink.valkey]\naddrs = vk-1:6379\ndb = 3\n[ring.valkey]\nstream = {r}\n")
		l := mustLoad(t, []string{"--config=" + f}, nil)
		want(t, l, "line 2")
		if l.Config.RingRedis.Stream != "{r}" {
			t.Errorf("ring.valkey.stream did not reach ring.redis.stream: %q", l.Config.RingRedis.Stream)
		}
	})
	t.Run("ring env", func(t *testing.T) {
		l := mustLoad(t, nil, map[string]string{"RELPLOG_RING_VALKEY_GROUP": "g2"})
		if l.Config.RingRedis.Group != "g2" {
			t.Errorf("ring.redis.group = %q", l.Config.RingRedis.Group)
		}
	})
	t.Run("precedence unchanged", func(t *testing.T) {
		// flag (alias) beats env (canonical) beats ini (alias).
		f := filepath.Join(t.TempDir(), "s.ini")
		writeFile(t, f, "[sink.valkey]\ndb = 1\n")
		l := mustLoad(t, []string{"--config=" + f, "--sink.valkey.db=3"},
			map[string]string{"RELPLOG_SINK_REDIS_DB": "2"})
		if l.Config.SinkRedis.DB != 3 || l.Source["sink.redis.db"] != FromFlag {
			t.Errorf("db=%d from %v, want 3 from the flag", l.Config.SinkRedis.DB, l.Source["sink.redis.db"])
		}
		l = mustLoad(t, []string{"--config=" + f}, map[string]string{"RELPLOG_SINK_REDIS_DB": "2"})
		if l.Config.SinkRedis.DB != 2 {
			t.Errorf("db=%d, want 2: env beats the file whatever the spelling", l.Config.SinkRedis.DB)
		}
	})
}

// Both names in one source: equal values pass, different values are exit 1
// naming both keys and never the values (they may be passwords).
func TestValkeyAliasConflict(t *testing.T) {
	const secretA, secretB = "Sup3rSecretA", "Sup3rSecretB"
	noValues := func(t *testing.T, err error) {
		t.Helper()
		if strings.Contains(err.Error(), secretA) || strings.Contains(err.Error(), secretB) {
			t.Errorf("the error echoes a value: %v", err)
		}
	}

	// Equal values, every source.
	mustLoad(t, []string{"--sink.redis.password=" + secretA, "--sink.valkey.password=" + secretA}, nil)
	mustLoad(t, nil, map[string]string{"RELPLOG_SINK_REDIS_DB": "2", "RELPLOG_SINK_VALKEY_DB": "02"})
	f := filepath.Join(t.TempDir(), "same.ini")
	writeFile(t, f, "[sink.redis]\npassword = "+secretA+"\n[sink.valkey]\npassword = "+secretA+"\n")
	mustLoad(t, []string{"--config=" + f}, nil)

	err := mustFail(t, []string{"--sink.redis.password=" + secretA, "--sink.valkey.password=" + secretB},
		nil, "--sink.valkey.password")
	if !strings.Contains(err.Error(), "--sink.redis.password") {
		t.Errorf("the error must name both keys: %v", err)
	}
	noValues(t, err)

	err = mustFail(t, nil, map[string]string{
		"RELPLOG_RING_REDIS_PASSWORD": secretA, "RELPLOG_RING_VALKEY_PASSWORD": secretB},
		"RELPLOG_RING_VALKEY_PASSWORD")
	if !strings.Contains(err.Error(), "RELPLOG_RING_REDIS_PASSWORD") {
		t.Errorf("the error must name both keys: %v", err)
	}
	noValues(t, err)

	g := filepath.Join(t.TempDir(), "diff.ini")
	writeFile(t, g, "[sink.valkey]\npassword = "+secretA+"\n[sink.redis]\npassword = "+secretB+"\n")
	err = mustFail(t, []string{"--config=" + g}, nil, "sink.valkey.password")
	if !strings.Contains(err.Error(), "sink.redis.password") || !strings.Contains(err.Error(), "line 4") {
		t.Errorf("the error must name both keys and lines: %v", err)
	}
	noValues(t, err)

	// Different sources are precedence, not conflict.
	mustLoad(t, []string{"--sink.valkey.db=3"}, map[string]string{"RELPLOG_SINK_REDIS_DB": "2"})
}

// LOGHUB_EB_URL owns sink.redis.*: an alias key counts as the same key.
func TestValkeyAliasCountsForEBURL(t *testing.T) {
	mustFail(t, []string{"--sink.valkey.addrs=x:6379"},
		map[string]string{"LOGHUB_EB_URL": "redis://r:6379"}, "sink.redis.addrs is also set explicitly")
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "valkey://r:6379", "RELPLOG_SINK_VALKEY_DB": "1"},
		"sink.redis.db is also set explicitly")
	mustFail(t, []string{"--sink.type=nats"}, map[string]string{"LOGHUB_EB_URL": "valkey://r"},
		"selects sink.type=redis")
	// sink.type=valkey agrees with a redis/valkey URL.
	mustLoad(t, []string{"--sink.type=valkey"}, map[string]string{"LOGHUB_EB_URL": "redis://r"})
	// A key the URL does not own stays free under either name.
	l := mustLoad(t, []string{"--sink.valkey.stream=s1"}, map[string]string{"LOGHUB_EB_URL": "valkey://r"})
	if l.Config.SinkRedis.Stream != "s1" {
		t.Errorf("stream = %q", l.Config.SinkRedis.Stream)
	}
}

// SPEC §10.9: valkey:// = redis://, valkeys:// = rediss://.
func TestEBURLValkeySchemes(t *testing.T) {
	for _, tc := range []struct {
		url  string
		addr string
		db   int
		tls  bool
		user string
		pass string
	}{
		{"valkey://vk-1", "vk-1:6379", 0, false, "", ""},
		{"valkey://vk-1:6380/2", "vk-1:6380", 2, false, "", ""},
		{"VALKEY://vk-1/1", "vk-1:6379", 1, false, "", ""},
		{"valkeys://app:s3cret@vk-1", "vk-1:6379", 0, true, "app", "s3cret"},
		{"valkeys://vk-1:6380/0", "vk-1:6380", 0, true, "", ""},
	} {
		l, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url})
		if err != nil {
			t.Errorf("%s: %v", tc.url, err)
			continue
		}
		r := l.Config.SinkRedis
		if l.Config.Sink.Type != SinkTypeRedis || !reflect.DeepEqual(r.Addrs, []string{tc.addr}) ||
			r.DB != tc.db || r.TLS != tc.tls || r.Username != tc.user || r.Password != tc.pass {
			t.Errorf("%s: type=%s addrs=%v db=%d tls=%v user=%q", tc.url, l.Config.Sink.Type,
				r.Addrs, r.DB, r.TLS, r.Username)
		}
	}
	// Refused exactly like redis://.
	for _, tc := range []struct{ url, want string }{
		{"valkey://a,b:6379", "holds a ','"},
		{"valkey://r/0?dial_timeout=3s", "not supported"},
		{"valkey://r/x", "database number"},
		{"valkeys://a:6379,valkeys://b:6379", "only nats:// takes a list"},
		{"valkey:///0", "needs a host"},
	} {
		mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
	}
}

func TestHelpMentionsValkeyAlias(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "[sink.valkey] and [ring.valkey] are aliases") {
		t.Error("--help must say the valkey alias exists")
	}
	if strings.Contains(buf.String(), "[sink.valkey]\n") {
		t.Error("--help must not duplicate the table under the alias")
	}
}
