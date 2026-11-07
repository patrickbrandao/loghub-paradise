package config

// Tests of the §21.1 items fixed on 2026-11-07 in this package (4, 5, 12 and
// 2's key) and of the valkey alias of SPEC §15.2.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const secret = "hunter2-S3CRET"

func writeINI(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "syslog.ini")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// --- valkey alias (SPEC §15.2) -----------------------------------------------

func TestValkeyAliasEverySource(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"flag", []string{"--sink.valkey.addrs=10.0.0.9:6379"}, nil},
		{"env", nil, map[string]string{"SYSLOG_SINK_VALKEY_ADDRS": "10.0.0.9:6379"}},
		{"ini", []string{"--config", writeINI(t, "[sink.valkey]\naddrs = 10.0.0.9:6379\n")}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := mustLoad(t, c.args, c.env)
			if got := strings.Join(l.Config.SinkRedis.Addrs, ","); got != "10.0.0.9:6379" {
				t.Fatalf("sink.redis.addrs = %q", got)
			}
			if !l.SetIn["sink.redis.addrs"] {
				t.Fatal("the alias did not mark the canonical key as set")
			}
		})
	}
}

func TestValkeySinkTypeNormalises(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=Valkey"}, nil)
	if l.Config.Sink.Type != SinkRedisType {
		t.Fatalf("sink.type = %q, want redis", l.Config.Sink.Type)
	}
	var out bytes.Buffer
	if err := l.DumpINI(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "valkey") {
		t.Fatalf("the dump shows a valkey name; only the canonical ones belong there:\n%s", out.String())
	}
}

// The same key under both names in one source: equal values pass (parsed:
// "yes" and "true" agree), different ones are exit 1, and the message names the
// two keys and neither value.
func TestValkeyAliasConflict(t *testing.T) {
	agree := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"flags", []string{"--sink.redis.tls=yes", "--sink.valkey.tls=true"}, nil},
		{"env", nil, map[string]string{"SYSLOG_SINK_REDIS_DB": "2", "SYSLOG_SINK_VALKEY_DB": "2"}},
		{"ini", []string{"--config", writeINI(t, "[sink.redis]\ndb = 3\n[sink.valkey]\ndb = 3\n")}, nil},
	}
	for _, c := range agree {
		t.Run("agree/"+c.name, func(t *testing.T) { mustLoad(t, c.args, c.env) })
	}

	differ := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"flags", []string{"--sink.redis.password=" + secret, "--sink.valkey.password=other-" + secret}, nil},
		{"env", nil, map[string]string{"SYSLOG_SINK_REDIS_PASSWORD": secret, "SYSLOG_SINK_VALKEY_PASSWORD": "x" + secret}},
		{"ini", []string{"--config", writeINI(t, "[sink.valkey]\npassword = "+secret+"\n[sink.redis]\npassword = y"+secret+"\n")}, nil},
	}
	for _, c := range differ {
		t.Run("differ/"+c.name, func(t *testing.T) {
			_, err := load(t, c.args, c.env)
			if err == nil {
				t.Fatal("conflicting values under the two names were accepted")
			}
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, strings.ToLower(secret)) {
				t.Fatalf("the error echoes a value: %s", msg)
			}
			if !strings.Contains(msg, "valkey") || !strings.Contains(msg, "redis") || !strings.Contains(msg, "password") {
				t.Fatalf("the error does not name both keys: %s", msg)
			}
		})
	}

	// Across sources the precedence stands: a flag under one name beats the
	// environment under the other.
	l := mustLoad(t, []string{"--sink.valkey.db=5"}, map[string]string{"SYSLOG_SINK_REDIS_DB": "4"})
	if l.Config.SinkRedis.DB != 5 || l.Source["sink.redis.db"] != FromFlag {
		t.Fatalf("db = %d from %s, want 5 from flag", l.Config.SinkRedis.DB, l.Source["sink.redis.db"])
	}
}

func TestUsageNamesTheValkeyAlias(t *testing.T) {
	var out bytes.Buffer
	if err := Usage(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "--sink.valkey.<key>") || !strings.Contains(out.String(), "SYSLOG_SINK_VALKEY_<KEY>") {
		t.Fatal("--help does not say the valkey alias exists")
	}
	if strings.Contains(out.String(), "[sink.valkey]\n") {
		t.Fatal("--help duplicates the [sink.valkey] table; one line is the rule")
	}
}

// --- item 5: errors never echo a value ---------------------------------------

func TestINIErrorsDoNotEchoTheLine(t *testing.T) {
	for _, body := range []string{
		"[sink.nats]\npassword: " + secret + "\n",
		"[sink.nats " + secret + "\n",
		"[sink.nats]\npassword = abc\n" + secret + "x = 1\n",
		"[sink.nats]\npass" + secret + " = 1\n",
	} {
		_, err := load(t, []string{"--config", writeINI(t, body)}, nil)
		if err == nil {
			t.Fatalf("accepted %q", body)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("INI error echoes the secret: %v", err)
		}
		if !strings.Contains(err.Error(), "line ") {
			t.Fatalf("INI error does not name the line: %v", err)
		}
	}
}

func TestFlagErrorsDoNotEchoTheValue(t *testing.T) {
	for _, args := range [][]string{
		{"sink.nats.password=" + secret},
		{"--sink.nats.password", "--" + secret},
		{"--sink.workers", "--sink.nats.password=" + secret},
		{"--sink.nats.password" + secret},
		{"--sink.nats.password=abc", secret},
	} {
		_, err := load(t, args, nil)
		if err == nil {
			t.Fatalf("accepted %q", args)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("flag error echoes the secret for %q: %v", args, err)
		}
	}
}

// --- item 4: sink.nats.urls ---------------------------------------------------

func TestNATSURLsValidation(t *testing.T) {
	nats := func(urls string, extra ...string) []string {
		return append([]string{"--sink.type=nats", "--sink.nats.urls=" + urls}, extra...)
	}
	for _, ok := range [][]string{
		nats("nats://127.0.0.1:4222"),
		nats("nats://u:p@h1:4222,tls://u:p@h2:4222"),
		nats("h1:4222"),
		nats("wss://h/ws"),
	} {
		mustLoad(t, ok, nil)
	}
	for _, bad := range [][]string{
		nats("nats://user:" + secret + "/x@host:4222"),          // '/' in the password
		nats("nats://tok#" + secret + "@host"),                  // '#' in the token
		nats("nats://host:4222/path"),                           // a path
		nats("nats://host:4222?" + secret),                      // a query
		nats("http://" + secret + "@host"),                      // a scheme nats.go does not dial
		nats("nats://"),                                         // no host
		nats("nats://u:"+secret+"@h", "--sink.nats.username=u"), // URL and key credentials
		nats("nats://"+secret+"@h", "--sink.nats.token=t"),
	} {
		_, err := load(t, bad, nil)
		if err == nil {
			t.Fatalf("accepted %q", bad)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the URL error echoes the credential: %v", err)
		}
	}
}

func TestNATSURLsAreRedactedInTheDump(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=nats", "--sink.nats.urls=nats://u:" + secret + "@h1:4222,nats://" + secret + "@h2:4222"}, nil)
	var out bytes.Buffer
	if err := l.DumpINI(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), secret) {
		t.Fatalf("the dump shows the credential:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "nats://u:***@h1:4222,nats://***@h2:4222") {
		t.Fatalf("the dump does not keep the URLs readable:\n%s", out.String())
	}
	if strings.Contains(strings.Join(RedactURLs(l.Config.SinkNATS.URLs), ","), secret) {
		t.Fatal("RedactURLs, used by the boot log, shows the credential")
	}
	if strings.Contains(l.Effective()["sink.nats.urls"], secret) {
		t.Fatal("Effective, the loggable form, shows the credential")
	}
}

// --- item 12: sink.mysql.database --------------------------------------------

func TestMySQLDatabaseNameIsValidated(t *testing.T) {
	base := []string{"--sink.type=mysql", "--sink.workers=1"}
	mustLoad(t, append(base, "--sink.mysql.database=syslog-prod"), nil)
	for _, bad := range []string{"sys`log", `sys\log`, "sys\x01log", strings.Repeat("d", 65)} {
		if _, err := load(t, append(base, "--sink.mysql.database="+bad), nil); err == nil {
			t.Fatalf("database %q accepted", bad)
		}
	}
}

// --- item 2: relp.inflight_max_bytes -------------------------------------------

func TestRELPInflightBudgetMustHoldOneFrame(t *testing.T) {
	relp := []string{"--listen.relp=127.0.0.1:20514"}
	mustLoad(t, relp, nil)
	if _, err := load(t, append(relp, "--relp.inflight_max_bytes=1MiB", "--relp.max_frame_bytes=2MiB"), nil); err == nil {
		t.Fatal("a budget below one frame was accepted")
	}
	// Without RELP the key does not matter.
	mustLoad(t, []string{"--relp.inflight_max_bytes=1MiB", "--relp.max_frame_bytes=2MiB"}, nil)
}
