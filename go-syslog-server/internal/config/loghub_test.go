package config

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// SPEC §10.9: the scheme picks the sink, the URL fills its connection.
func TestEBURLMapping(t *testing.T) {
	for _, tc := range []struct {
		url  string
		sink string
		chk  func(*Config) bool
	}{
		{"nats://nats-1:4222", SinkTypeNATS, func(c *Config) bool {
			return reflect.DeepEqual(c.SinkNATS.URLs, []string{"nats://nats-1:4222"}) && !c.SinkNATS.TLS
		}},
		{"nats://u:p@nats-1:4222", SinkTypeNATS, func(c *Config) bool {
			// The whole URL, credentials included: the client reads them there.
			return reflect.DeepEqual(c.SinkNATS.URLs, []string{"nats://u:p@nats-1:4222"}) &&
				c.SinkNATS.Username == "" && c.SinkNATS.Password == ""
		}},
		{"tls://nats-1:4222", SinkTypeNATS, func(c *Config) bool {
			return reflect.DeepEqual(c.SinkNATS.URLs, []string{"tls://nats-1:4222"}) && c.SinkNATS.TLS
		}},
		{"nats://a:4222, nats://b:4222", SinkTypeNATS, func(c *Config) bool {
			return reflect.DeepEqual(c.SinkNATS.URLs, []string{"nats://a:4222", "nats://b:4222"})
		}},
		{"redis://redis-1", SinkTypeRedis, func(c *Config) bool {
			r := c.SinkRedis
			return reflect.DeepEqual(r.Addrs, []string{"redis-1:6379"}) && r.DB == 0 && !r.TLS &&
				r.Username == "" && r.Password == ""
		}},
		{"redis://app:s3cret@redis-1:6380/2", SinkTypeRedis, func(c *Config) bool {
			r := c.SinkRedis
			return reflect.DeepEqual(r.Addrs, []string{"redis-1:6380"}) && r.DB == 2 &&
				r.Username == "app" && r.Password == "s3cret" && !r.TLS
		}},
		{"redis://:only%40pass@redis-1/", SinkTypeRedis, func(c *Config) bool {
			return c.SinkRedis.Username == "" && c.SinkRedis.Password == "only@pass"
		}},
		{"rediss://redis-1:6380/0", SinkTypeRedis, func(c *Config) bool {
			return c.SinkRedis.TLS && c.SinkRedis.Addrs[0] == "redis-1:6380"
		}},
		{"redis://[::1]:6379", SinkTypeRedis, func(c *Config) bool {
			return c.SinkRedis.Addrs[0] == "[::1]:6379"
		}},
		{"postgres://loghub:pw@pg-1/logs", SinkTypePostgres, func(c *Config) bool {
			p := c.SinkPG
			return p.Addr == "pg-1:5432" && p.Username == "loghub" && p.Password == "pw" &&
				p.Database == "logs" && !p.TLS
		}},
		{"postgresql://loghub:pw@pg-1:6432/logs?sslmode=require", SinkTypePostgres, func(c *Config) bool {
			return c.SinkPG.Addr == "pg-1:6432" && c.SinkPG.TLS
		}},
		{"postgres://pg-1/logs?sslmode=verify-full", SinkTypePostgres, func(c *Config) bool {
			// No userinfo: the registry defaults stay, the password is empty.
			return c.SinkPG.TLS && c.SinkPG.Username == "syslog" && c.SinkPG.Password == ""
		}},
		{"postgres://u:p@pg-1/logs?sslmode=disable", SinkTypePostgres, func(c *Config) bool {
			return !c.SinkPG.TLS
		}},
		{"postgres://u:p@/logs?host=/var/run/postgresql", SinkTypePostgres, func(c *Config) bool {
			return c.SinkPG.Addr == "/var/run/postgresql" && c.SinkPG.Database == "logs"
		}},
	} {
		l, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url})
		if err != nil {
			t.Errorf("%s: %v", tc.url, err)
			continue
		}
		if l.Config.Sink.Type != tc.sink || l.Source["sink.type"] != FromEBURL {
			t.Errorf("%s: sink.type = %q from %v, want %q from loghub.eb_url", tc.url,
				l.Config.Sink.Type, l.Source["sink.type"], tc.sink)
		}
		if !tc.chk(l.Config) {
			t.Errorf("%s: filled %+v / %+v / %+v", tc.url, l.Config.SinkNATS, l.Config.SinkRedis, l.Config.SinkPG)
		}
	}
}

func TestEBURLUnsupportedOrMalformed(t *testing.T) {
	for _, tc := range []struct{ url, want string }{
		{"amqp://guest:guest@rabbit:5672/", `"amqp://" is not supported`},
		{"kafka://k:9092", `"kafka://" is not supported`},
		{"https://bus.example", `"https://" is not supported`},
		{"nats-1:4222", "not a URL"},
		{"nats://a:4222,tls://b:4222", "mixes nats:// and tls://"},
		{"nats://a:4222,redis://b", "every entry"},
		{"nats://a:4222/path", "no path"},
		{"nats://", "needs a host"},
		{"redis://r/abc", "database number"},
		{"redis://r/0?dial_timeout=3s", "not supported"},
		{"redis://a:6379,redis://b:6379", "only nats:// takes a list"},
		{"postgres://a/db,postgres://b/db", "only nats:// takes a list"},
		{"postgres://u:p@pg/db?application_name=x", `"application_name"`},
		{"postgres://u:p@pg/db?sslmode=sometimes", "sslmode"},
		{"postgres:///db", "needs a host"},
		{"postgres://pg/db?host=relative", "unix socket"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if !strings.Contains(err.Error(), "LOGHUB_EB_URL") {
			t.Errorf("%s: error does not name its source: %v", tc.url, err)
		}
	}
	// The password never reaches an error message, whatever went wrong.
	for _, u := range []string{
		"amqp://guest:hunter2@rabbit:5672/",
		"redis://u:hunter2@r:notaport/0",
		"postgres://u:hunter2@pg/db?sslmode=bogus",
		"hunter2",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// Empty means "not set".
	if l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""}); l.Config.Sink.Type != SinkTypeNATS {
		t.Fatalf("empty eb_url changed the sink: %q", l.Config.Sink.Type)
	}
}

// SPEC §10.9, "one source of truth": an explicit sink.type that disagrees, or
// an explicit connection key of that sink, is exit 1 naming both sources.
func TestEBURLConflicts(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[sink.redis]\npassword = from-file\n")

	for _, tc := range []struct {
		args []string
		env  map[string]string
		want []string
	}{
		{[]string{"--sink.type=nats"}, map[string]string{"LOGHUB_EB_URL": "redis://r"},
			[]string{"sink.type=redis", "sink.type=nats", "env LOGHUB_EB_URL", "flag --sink.type"}},
		{nil, map[string]string{"LOGHUB_EB_URL": "redis://r", "SYSLOG_SINK_TYPE": "none"},
			[]string{"env SYSLOG_SINK_TYPE", "env LOGHUB_EB_URL"}},
		{nil, map[string]string{"LOGHUB_EB_URL": "redis://r", "SYSLOG_SINK_REDIS_PASSWORD": "x"},
			[]string{"sink.redis.password", "env SYSLOG_SINK_REDIS_PASSWORD"}},
		{[]string{"--config=" + ini}, map[string]string{"LOGHUB_EB_URL": "redis://:p@r"},
			[]string{"sink.redis.password", "file " + ini + " line 2"}},
		{[]string{"--loghub.eb_url=nats://n:4222", "--sink.nats.urls=nats://other:4222"}, nil,
			[]string{"sink.nats.urls", "flag --loghub.eb_url", "flag --sink.nats.urls"}},
		{[]string{"--sink.nats.tls"}, map[string]string{"LOGHUB_EB_URL": "tls://n:4222"},
			[]string{"sink.nats.tls"}},
		{[]string{"--sink.nats.token=t"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
			[]string{"sink.nats.token"}},
		{[]string{"--sink.postgres.database=other"}, map[string]string{"LOGHUB_EB_URL": "postgres://pg"},
			[]string{"sink.postgres.database"}},
		{[]string{"--sink.redis.db=3"}, map[string]string{"LOGHUB_EB_URL": "redis://r/3"},
			[]string{"sink.redis.db"}},
	} {
		err := mustFail(t, tc.args, tc.env, tc.want[0])
		for _, w := range tc.want[1:] {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%v %v: err = %v, want it to name %q", tc.args, tc.env, err, w)
			}
		}
	}

	// An explicit sink.type that agrees with the scheme is no conflict.
	l := mustLoad(t, []string{"--sink.type=redis"}, map[string]string{"LOGHUB_EB_URL": "redis://r"})
	if l.Source["sink.type"] != FromFlag || l.Config.SinkRedis.Addrs[0] != "r:6379" {
		t.Fatalf("agreeing sink.type: %v %v", l.Source["sink.type"], l.Config.SinkRedis.Addrs)
	}
	// Keys a URL cannot express complement it.
	mustLoad(t, []string{"--sink.nats.credentials=/etc/syslog/sls.creds", "--sink.nats.tls_ca=/ca",
		"--sink.nats.stream=LOGS", "--sink.nats.mode=core"},
		map[string]string{"LOGHUB_EB_URL": "tls://n:4222"})
	mustLoad(t, []string{"--sink.redis.stream=x", "--sink.redis.maxlen=10", "--sink.redis.tls_insecure"},
		map[string]string{"LOGHUB_EB_URL": "rediss://r"})
	mustLoad(t, []string{"--sink.postgres.table=logs.ev", "--sink.postgres.pool_size=9", "--sink.workers=8"},
		map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db"})
	// Keys of another sink are not "that sink's" keys.
	mustLoad(t, []string{"--sink.redis.password=x"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"})
	// The selected sink is validated like any other: a tls_ca with nats://
	// still means the CA would be ignored.
	mustFail(t, []string{"--sink.nats.tls_ca=/ca"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
		"sink.nats.tls_ca")
}

// SPEC §10.9: the dump shows the derived values, with the password redacted,
// and stays re-loadable.
func TestEBURLDump(t *testing.T) {
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "redis://app:hunter2@redis-1:6380/2"})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("password in the dump:\n%s", out)
	}
	for _, want := range []string{
		"redis://app:***@redis-1:6380/2",
		"# derived from loghub.eb_url",
		"# addrs                        = redis-1:6380",
		"# db                           = 2",
		"# password                     = ***",
		"# type                         = redis",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dump lacks %q", want)
		}
	}
	if l.Effective()["loghub.eb_url"] != "redis://app:***@redis-1:6380/2" || l.Effective()["sink.redis.password"] != "***" {
		t.Fatalf("Effective = %q %q", l.Effective()["loghub.eb_url"], l.Effective()["sink.redis.password"])
	}

	// Without a secret the dump reads back to the same configuration: the
	// keys the URL decides are commented out -- filled ones and the ones it
	// leaves at their default -- so the URL decides them again instead of
	// conflicting with an explicit copy.
	for _, u := range []string{
		"postgres://pg-1/logs?sslmode=require",
		"tls://n1:4222,tls://n2:4222",
		"rediss://r:6380/1",
	} {
		l = mustLoad(t, []string{"--sink.workers=3"}, map[string]string{"LOGHUB_EB_URL": u})
		buf.Reset()
		if err := l.DumpINI(&buf); err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(t.TempDir(), "dump.ini")
		writeFile(t, f, buf.String())
		back := mustLoad(t, []string{"--config=" + f}, nil)
		if !reflect.DeepEqual(back.Config, l.Config) {
			t.Fatalf("%s: dump did not round-trip\n got %+v\nwant %+v\n%s", u, back.Config, l.Config, buf.String())
		}
		if back.Source["sink.type"] != FromEBURL {
			t.Fatalf("%s: read back, sink.type comes from %v", u, back.Source["sink.type"])
		}
	}
}

// SPEC §16.2: the [identity] block is appended by the app, commented, and the
// dump stays re-loadable.
func TestDumpInfoSection(t *testing.T) {
	l := mustLoad(t, nil, nil)
	var buf bytes.Buffer
	err := l.DumpINI(&buf, InfoSection{
		Name: "identity",
		Note: "resolved by go-loghub-ident from the environment; informative, not configuration",
		Entries: []InfoEntry{
			{"agent_uuid", "0190f5d2-0000-7000-8000-000000000001"},
			{"workspace", "acme.prod"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"# [identity]", "# agent_uuid", "= 0190f5d2-0000-7000-8000-000000000001",
		"# workspace", "acme.prod"} {
		if !strings.Contains(out, want) {
			t.Errorf("dump lacks %q:\n%s", want, out)
		}
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, out)
	mustLoad(t, []string{"--config=" + f}, nil)
}

// SPEC §16.3, the two references: a
// reloadable key is compared with what RUNS, so reverting it to the boot value
// is applied; a restart-only key is compared with the BOOT snapshot, so it is
// warned about on every SIGHUP until the restart.
func TestReloadTwoReferences(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	write := func(minSev, maxMsg string) {
		writeFile(t, ini, "[filter]\nmin_severity = "+minSev+"\n[syslog]\nmax_message_bytes = "+maxMsg+"\n")
	}
	opts := Options{
		Args:      []string{"--config=" + ini},
		LookupEnv: func(string) (string, bool) { return "", false },
		WorkDir:   testWorkDir,
	}

	write("7", "64KiB")
	boot, err := Load(opts)
	if err != nil {
		t.Fatal(err)
	}
	var running *Loaded // nil = boot

	step := func(label string, wantApplied, wantIgnored []string) {
		t.Helper()
		res, err := Reload(opts, boot, running)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if !equalNames(res.Applied, wantApplied) || !equalNames(res.Ignored, wantIgnored) {
			t.Fatalf("%s: applied=%v ignored=%v, want applied=%v ignored=%v",
				label, res.Applied, res.Ignored, wantApplied, wantIgnored)
		}
		if res.Changed() != (len(wantApplied)+len(wantIgnored) > 0) {
			t.Fatalf("%s: Changed() = %v", label, res.Changed())
		}
		if len(res.Applied) > 0 {
			running = res.Fresh // what the app does after applying
		}
	}

	step("unchanged", nil, nil)

	write("4", "64KiB")
	step("tighten the filter", []string{"filter.min_severity"}, nil)
	if running.Config.Filter.MinSeverity != "4" {
		t.Fatalf("running filter = %q", running.Config.Filter.MinSeverity)
	}

	// Revert the filter to its BOOT value and touch a restart-only key. A
	// comparison against boot would see no filter change and leave 4 running.
	write("7", "32KiB")
	step("revert + restart key", []string{"filter.min_severity"}, []string{"syslog.max_message_bytes"})

	// Nothing changed since the last SIGHUP, yet the restart key is still
	// pending: a comparison against running would go silent here.
	step("again", nil, []string{"syslog.max_message_bytes"})
	step("and again", nil, []string{"syslog.max_message_bytes"})

	// Back to the boot value: nothing to warn about any more.
	write("7", "64KiB")
	step("restart key reverted", nil, nil)

	// An invalid file applies nothing and says why.
	write("loud", "64KiB")
	if _, err := Reload(opts, boot, running); err == nil || !strings.Contains(err.Error(), "filter.min_severity") {
		t.Fatalf("invalid reload: err = %v", err)
	}
}

// A changed secret is a change, even though only its name is ever reported.
func TestReloadSeesSecrets(t *testing.T) {
	boot := mustLoad(t, []string{"--sink.redis.password=a"}, nil)
	fresh := mustLoad(t, []string{"--sink.redis.password=b"}, nil)
	applied, ignored := Compare(boot, nil, fresh)
	if len(applied) != 0 || !equalNames(ignored, []string{"sink.redis.password"}) {
		t.Fatalf("applied=%v ignored=%v", applied, ignored)
	}
	// Every [syslog] key requires a restart (SPEC §16.3).
	fresh = mustLoad(t, []string{"--syslog.payload_raw=false", "--syslog.rfc3164_timezone=Asia/Tokyo",
		"--log.level=debug", "--ratelimit.enabled", "--sink.sqlite.keep_files=3"}, nil)
	applied, ignored = Compare(boot, boot, fresh)
	if !equalNames(applied, []string{"ratelimit.enabled", "sink.sqlite.keep_files", "log.level"}) ||
		!equalNames(ignored, []string{"syslog.rfc3164_timezone", "syslog.payload_raw", "sink.redis.password"}) {
		t.Fatalf("applied=%v ignored=%v", applied, ignored)
	}
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, x := range a {
		seen[x]++
	}
	for _, x := range b {
		if seen[x] == 0 {
			return false
		}
		seen[x]--
	}
	return true
}

// Review finding (Docker): with every reloadable key also set by env, editing
// the INI and sending SIGHUP reported "configuration unchanged" and nothing
// said why. The reload now names the keys the file sets but an env or a flag
// decides.
func TestReloadReportsShadowedKeys(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[filter]\nmin_severity = debug\n[ratelimit]\nenabled = false\n")
	env := map[string]string{"SYSLOG_FILTER_MIN_SEVERITY": "debug"}
	opts := Options{
		Args:      []string{"--config=" + ini, "--log.level=info"},
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		WorkDir:   testWorkDir,
	}
	boot, err := Load(opts)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, ini, "[filter]\nmin_severity = warning\n[ratelimit]\nenabled = true\n[log]\nlevel = debug\n"+
		"[syslog]\npayload_raw = false\n")
	env["SYSLOG_PAYLOAD_RAW"] = "true" // a restart-only key the restart would not apply either
	res, err := Reload(opts, boot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !equalNames(res.Applied, []string{"ratelimit.enabled"}) {
		t.Fatalf("applied = %v, want only ratelimit.enabled", res.Applied)
	}
	if !equalNames(res.Shadowed, []string{"syslog.payload_raw", "filter.min_severity", "log.level"}) {
		t.Fatalf("shadowed = %v, want syslog.payload_raw and filter.min_severity (env), log.level (flag)", res.Shadowed)
	}
}
