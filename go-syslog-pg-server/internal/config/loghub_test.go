package config

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// ebOwnedKeys are the [sink.postgres] keys LOGHUB_EB_URL decides (SPEC §10.9).
var ebOwnedKeys = []string{"sink.postgres.addr", "sink.postgres.username",
	"sink.postgres.password", "sink.postgres.database"}

// SPEC §10.9: the URL fills the connection of [sink.postgres]; there is no
// sink to pick any more, only PostgreSQL.
func TestEBURLMapping(t *testing.T) {
	for _, tc := range []struct {
		url string
		chk func(SinkPostgres) bool
	}{
		{"postgres://loghub:pw@pg-1/logs", func(p SinkPostgres) bool {
			return p.Addr == "pg-1:5432" && p.Username == "loghub" && p.Password == "pw" && p.Database == "logs"
		}},
		{"postgresql://loghub:pw@pg-1:6432/logs?sslmode=disable", func(p SinkPostgres) bool {
			return p.Addr == "pg-1:6432" && p.Database == "logs"
		}},
		{"postgres://loghub@pg-1/logs?sslmode=prefer", func(p SinkPostgres) bool {
			// No password: it is empty -- the URL decides it.
			return p.Addr == "pg-1:5432" && p.Username == "loghub" && p.Password == ""
		}},
		{"postgres://u:p@pg-1/logs?sslmode=allow", func(p SinkPostgres) bool {
			return p.Addr == "pg-1:5432"
		}},
		{"postgres://u:p%40ss@pg-1/logs", func(p SinkPostgres) bool {
			return p.Username == "u" && p.Password == "p@ss"
		}},
		{"postgres://u:p@[::1]:5433/logs", func(p SinkPostgres) bool {
			return p.Addr == "[::1]:5433"
		}},
		{"postgres://u:p@/logs?host=/var/run/postgresql", func(p SinkPostgres) bool {
			return p.Addr == "/var/run/postgresql" && p.Database == "logs"
		}},
		// Review B #2: the port names the socket file; dropping it dialled
		// .s.PGSQL.5432, and the URL owns the addr the operator would fix.
		{"postgres://u:p@:5433/logs?host=/var/run/postgresql", func(p SinkPostgres) bool {
			return p.Addr == "/var/run/postgresql:5433"
		}},
		{"POSTGRES://u:p@pg-1/logs", func(p SinkPostgres) bool {
			return p.Addr == "pg-1:5432"
		}},
	} {
		l, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url})
		if err != nil {
			t.Errorf("%s: %v", tc.url, err)
			continue
		}
		if l.Source["sink.postgres.addr"] != FromEBURL || l.Origin["sink.postgres.addr"] != "loghub.eb_url" {
			t.Errorf("%s: sink.postgres.addr comes from %v (%q), want loghub.eb_url", tc.url,
				l.Source["sink.postgres.addr"], l.Origin["sink.postgres.addr"])
		}
		// Filling the connection is not "setting" it: SetIn stays for the
		// explicit sources the conflict rule compares with.
		if l.SetIn["sink.postgres.addr"] {
			t.Errorf("%s: a value from the URL is marked as explicitly set", tc.url)
		}
		if !tc.chk(l.Config.SinkPG) {
			t.Errorf("%s: filled %+v", tc.url, l.Config.SinkPG)
		}
	}
}

func TestEBURLUnsupportedOrMalformed(t *testing.T) {
	// Control: a well-formed postgres URL from the same env variable loads, so
	// every refusal below is about the URL and not about how it is passed.
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db"})

	for _, tc := range []struct{ url, want string }{
		// The eventbus schemes of the multi-sink server: a URL left over from a
		// sibling deployment must fail the boot, not be ignored.
		{"nats://nats-1:4222", `"nats://" is not supported`},
		{"tls://nats-1:4222", `"tls://" is not supported`},
		{"redis://redis-1:6379/0", `"redis://" is not supported`},
		{"rediss://redis-1:6380/0", `"rediss://" is not supported`},
		{"amqp://guest:guest@rabbit:5672/", `"amqp://" is not supported`},
		{"kafka://k:9092", `"kafka://" is not supported`},
		{"https://bus.example", `"https://" is not supported`},
		{"pg-1:5432", "not a URL"},
		{"postgres://a/db,postgres://b/db", "names one server"},
		{"postgres://u:p@pg/db?application_name=x", `"application_name"`},
		{"postgres://u:p@pg/db?sslmode=sometimes", "sslmode"},
		{"postgres:///db", "needs a host"},
		{"postgres://pg/db?host=relative", "unix socket"},
		{"postgres://pg/db?host=/run/pg", "no host before the path"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if !strings.Contains(err.Error(), "LOGHUB_EB_URL") {
			t.Errorf("%s: error does not name its source: %v", tc.url, err)
		}
	}
	// The unsupported-scheme error says what IS supported.
	err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"}, "postgres://")
	if !strings.Contains(err.Error(), "PostgreSQL") {
		t.Errorf("nats://: %v", err)
	}
	// The password never reaches an error message, whatever went wrong.
	for _, u := range []string{
		"amqp://guest:hunter2@rabbit:5672/",
		"redis://u:hunter2@r:6379/0",
		"postgres://u:hunter2@pg:notaport/db",
		"postgres://u:hunter2@pg/db?sslmode=bogus",
		"postgres://u:hunter2@pg/db?sslmode=require",
		"hunter2",
		"u:hunter2@pg:5432/db",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// Empty means "not set": nothing is filled.
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""})
	if l.Config.SinkPG.Addr != "127.0.0.1:5432" || l.Source["sink.postgres.addr"] != FromDefault {
		t.Fatalf("empty eb_url filled the sink: %q from %v", l.Config.SinkPG.Addr, l.Source["sink.postgres.addr"])
	}
}

// SPEC §1.2 and §10.9: there is no TLS to PostgreSQL. An sslmode that demands
// TLS is refused rather than downgraded, because the operator who wrote it
// asked for the password and every event NOT to travel in clear; one that
// tolerates plaintext passes.
func TestEBURLRefusesTLS(t *testing.T) {
	const base = "postgres://loghub:pw@pg-1:5432/logs?sslmode="
	for _, mode := range []string{"require", "verify-ca", "verify-full"} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": base + mode}, "TLS")
		for _, w := range []string{"sslmode=" + mode, "LOGHUB_EB_URL"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("sslmode=%s: err = %v, want it to name %q", mode, err, w)
			}
		}
		// The flag spelling is the same parameter and the same refusal.
		mustFail(t, []string{"--loghub.eb_url=" + base + mode}, nil, "TLS")
	}
	// Control: the same URL with a plaintext-tolerant mode, or none, loads and
	// fills the connection, so the refusal above is the mode and only the mode.
	for _, mode := range []string{"", "disable", "allow", "prefer"} {
		u := base + mode
		if mode == "" {
			u = strings.TrimSuffix(base, "?sslmode=") // no sslmode at all; an empty one is refused
		}
		l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if l.Config.SinkPG.Addr != "pg-1:5432" || l.Config.SinkPG.Password != "pw" {
			t.Errorf("sslmode=%q: filled %+v", mode, l.Config.SinkPG)
		}
	}
}

// SPEC §10.9, "one source of truth": an explicit connection key next to the
// URL is exit 1 naming both sources.
func TestEBURLConflicts(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[sink.postgres]\npassword = from-file\n")

	for _, tc := range []struct {
		args []string
		env  map[string]string
		want []string
	}{
		{[]string{"--sink.postgres.addr=other:5432"}, map[string]string{"LOGHUB_EB_URL": "postgres://u@pg-1/db"},
			[]string{"sink.postgres.addr", "env LOGHUB_EB_URL", "flag --sink.postgres.addr"}},
		// The same value is still a second source: the next edit of one of
		// them would silently lose to the other.
		{[]string{"--sink.postgres.addr=pg-1:5432"}, map[string]string{"LOGHUB_EB_URL": "postgres://u@pg-1/db"},
			[]string{"sink.postgres.addr"}},
		{nil, map[string]string{"LOGHUB_EB_URL": "postgres://u@pg-1/db", "SYSLOG_SINK_POSTGRES_PASSWORD": "x"},
			[]string{"sink.postgres.password", "env SYSLOG_SINK_POSTGRES_PASSWORD"}},
		{[]string{"--config=" + ini}, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg-1/db"},
			[]string{"sink.postgres.password", "file " + ini + " line 2"}},
		{[]string{"--loghub.eb_url=postgres://u@pg-1/db", "--sink.postgres.username=other"}, nil,
			[]string{"sink.postgres.username", "flag --loghub.eb_url", "flag --sink.postgres.username"}},
		// The URL has no password, yet it still owns it.
		{[]string{"--sink.postgres.password=other"}, map[string]string{"LOGHUB_EB_URL": "postgres://u@pg-1/db"},
			[]string{"sink.postgres.password"}},
	} {
		err := mustFail(t, tc.args, tc.env, tc.want[0])
		for _, w := range tc.want[1:] {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%v %v: err = %v, want it to name %q", tc.args, tc.env, err, w)
			}
		}
	}

	// Keys a URL cannot express complement it.
	l := mustLoad(t, []string{"--sink.postgres.table=logs.ev", "--sink.postgres.pool_size=9", "--sink.workers=8"},
		map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db"})
	if l.Config.SinkPG.Table != "logs.ev" || l.Config.SinkPG.PoolSize != 9 || l.Config.SinkPG.Addr != "pg:5432" {
		t.Fatalf("complemented = %+v", l.Config.SinkPG)
	}
	// The connection the URL fills is validated like any other: a pool below
	// the workers is still refused next to it.
	mustFail(t, []string{"--sink.postgres.pool_size=2", "--sink.workers=8"},
		map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db"}, "pool_size=2")
}

// SPEC §10.9: the dump shows the derived values, with the password redacted,
// and stays re-loadable.
func TestEBURLDump(t *testing.T) {
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://app:hunter2@pg-1:6432/logs"})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("password in the dump:\n%s", out)
	}
	for _, want := range []string{
		"postgres://app:***@pg-1:6432/logs",
		"# derived from loghub.eb_url",
		"# addr                         = pg-1:6432",
		"# username                     = app",
		"# database                     = logs",
		"# password                     = ***",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dump lacks %q", want)
		}
	}
	if l.Effective()["loghub.eb_url"] != "postgres://app:***@pg-1:6432/logs" || l.Effective()["sink.postgres.password"] != "***" {
		t.Fatalf("Effective = %q %q", l.Effective()["loghub.eb_url"], l.Effective()["sink.postgres.password"])
	}

	// With the URL given again, the dump reads back to the same
	// configuration: the keys the URL decides are commented out -- filled
	// ones and the ones it leaves at their default -- so the URL decides them
	// again instead of conflicting with an explicit copy. The URL itself is
	// redacted in the dump (a bare user too: it may be a pasted token), and
	// since every URL now names a user (SPEC §10.9), it is given again here as
	// the environment that wins over the file.
	for _, u := range []string{
		"postgres://app@pg-1/logs?sslmode=prefer",
		"postgresql://app@pg-1:6432/logs",
		"postgres://app@/logs?host=/var/run/postgresql",
	} {
		env := map[string]string{"LOGHUB_EB_URL": u}
		l = mustLoad(t, []string{"--sink.workers=3"}, env)
		buf.Reset()
		if err := l.DumpINI(&buf); err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(t.TempDir(), "dump.ini")
		writeFile(t, f, buf.String())
		back := mustLoad(t, []string{"--config=" + f}, env)
		if !reflect.DeepEqual(back.Config, l.Config) {
			t.Fatalf("%s: dump did not round-trip\n got %+v\nwant %+v\n%s", u, back.Config, l.Config, buf.String())
		}
		for _, name := range ebOwnedKeys {
			if back.SetIn[name] {
				t.Fatalf("%s: read back, %s is set explicitly by the dump:\n%s", u, name, buf.String())
			}
		}
		if back.Source["sink.postgres.addr"] != FromEBURL {
			t.Fatalf("%s: read back, sink.postgres.addr comes from %v", u, back.Source["sink.postgres.addr"])
		}
	}
	// A URL without a password fills it empty, and the dump still comments it
	// out: it is the URL's to decide.
	l = mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://app@pg-1/logs"})
	buf.Reset()
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "# derived from loghub.eb_url") ||
		!strings.Contains(buf.String(), "# password                     = ") {
		t.Fatalf("default owned key not commented:\n%s", buf.String())
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

// SPEC §16.3, the two references (bugs 009 and 010 of the standard): a
// reloadable key is compared with what RUNS, so reverting it to the boot value
// is applied; a restart-only key is compared with the BOOT snapshot, so it is
// warned about on every SIGHUP until the restart.
func TestReloadTwoReferences(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	write := func(level, maxMsg string) {
		writeFile(t, ini, "[log]\nlevel = "+level+"\n[syslog]\nmax_message_bytes = "+maxMsg+"\n")
	}
	opts := Options{
		Args:      []string{"--config=" + ini},
		LookupEnv: func(string) (string, bool) { return "", false },
		WorkDir:   testWorkDir,
	}

	write("info", "64KiB")
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

	write("debug", "64KiB")
	step("turn debug on", []string{"log.level"}, nil)
	if running.Config.Log.Level != "debug" {
		t.Fatalf("running level = %q", running.Config.Log.Level)
	}

	// Revert the level to its BOOT value and touch a restart-only key. A
	// comparison against boot would see no level change and leave debug on.
	write("info", "32KiB")
	step("revert + restart key", []string{"log.level"}, []string{"syslog.max_message_bytes"})

	// Nothing changed since the last SIGHUP, yet the restart key is still
	// pending: a comparison against running would go silent here.
	step("again", nil, []string{"syslog.max_message_bytes"})
	step("and again", nil, []string{"syslog.max_message_bytes"})

	// Back to the boot value: nothing to warn about any more.
	write("info", "64KiB")
	step("restart key reverted", nil, nil)

	// An invalid file applies nothing and says why.
	write("loud", "64KiB")
	if _, err := Reload(opts, boot, running); err == nil || !strings.Contains(err.Error(), "log.level") {
		t.Fatalf("invalid reload: err = %v", err)
	}
}

// A changed secret is a change, even though only its name is ever reported.
func TestReloadSeesSecrets(t *testing.T) {
	boot := mustLoad(t, []string{"--sink.postgres.password=a"}, nil)
	fresh := mustLoad(t, []string{"--sink.postgres.password=b"}, nil)
	applied, ignored := Compare(boot, nil, fresh)
	if len(applied) != 0 || !equalNames(ignored, []string{"sink.postgres.password"}) {
		t.Fatalf("applied=%v ignored=%v", applied, ignored)
	}
	// Every [syslog] key, and everything outside [log], requires a restart
	// (SPEC §16.3); the three [log] keys apply. sink.postgres.pool_size,
	// derived from sink.workers in both, is not reported (2026-10-14).
	fresh = mustLoad(t, []string{"--syslog.payload_raw=false", "--syslog.rfc3164_timezone=Asia/Tokyo",
		"--log.level=debug", "--log.access", "--log.access_sample=0.5", "--sink.workers=6",
		"--log.format=text"}, nil)
	applied, ignored = Compare(boot, boot, fresh)
	if !equalNames(applied, []string{"log.level", "log.access", "log.access_sample"}) ||
		!equalNames(ignored, []string{"syslog.rfc3164_timezone", "syslog.payload_raw", "sink.postgres.password",
			"sink.workers", "log.format"}) {
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
	writeFile(t, ini, "[log]\naccess_sample = 0.5\naccess = false\n")
	env := map[string]string{"SYSLOG_LOG_ACCESS_SAMPLE": "0.5"}
	opts := Options{
		Args:      []string{"--config=" + ini, "--log.level=info"},
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		WorkDir:   testWorkDir,
	}
	boot, err := Load(opts)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, ini, "[log]\naccess_sample = 0.9\naccess = true\nlevel = debug\n"+
		"[syslog]\npayload_raw = false\n")
	env["SYSLOG_PAYLOAD_RAW"] = "true" // a restart-only key the restart would not apply either
	res, err := Reload(opts, boot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !equalNames(res.Applied, []string{"log.access"}) {
		t.Fatalf("applied = %v, want only log.access", res.Applied)
	}
	if !equalNames(res.Shadowed, []string{"syslog.payload_raw", "log.access_sample", "log.level"}) {
		t.Fatalf("shadowed = %v, want syslog.payload_raw and log.access_sample (env), log.level (flag)", res.Shadowed)
	}
}

// Review B #3: url.Values.Get takes the first value and libpq the last, so a
// repeated sslmode slipped a require past the TLS refusal. Any repeated
// parameter is refused; a single one is the control.
func TestEBURLRepeatedParameterIsRefused(t *testing.T) {
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg-1/logs?sslmode=disable"})
	for _, u := range []string{
		"postgres://u:p@pg-1/logs?sslmode=disable&sslmode=require",
		"postgres://u:p@/logs?host=/a&host=/b",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || !strings.Contains(err.Error(), "repeated") {
			t.Errorf("%s: err = %v, want a refusal of the repeated parameter", u, err)
		}
	}
}
