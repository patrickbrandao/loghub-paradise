package config

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// ebOwnedKeys are the [sink.nats] keys LOGHUB_EB_URL decides (SPEC §10.9): the
// ones a NATS URL can express. credentials, mode and stream stay free.
var ebOwnedKeys = []string{"sink.nats.urls", "sink.nats.username", "sink.nats.password", "sink.nats.token"}

// SPEC §10.9: the URL fills sink.nats.urls; there is no sink to pick, only NATS.
func TestEBURLMapping(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want []string
	}{
		{"nats://nats-1:4222", []string{"nats://nats-1:4222"}},
		// The whole URL, credentials included: the client reads them there.
		{"nats://u:p@nats-1:4222", []string{"nats://u:p@nats-1:4222"}},
		{"nats://tok3n@nats-1:4222", []string{"nats://tok3n@nats-1:4222"}},
		{"nats://a:4222, nats://b:4222", []string{"nats://a:4222", "nats://b:4222"}},
		{"nats://a:4222,nats://b:4222,nats://c:4222", []string{"nats://a:4222", "nats://b:4222", "nats://c:4222"}},
		{"NATS://nats-1:4222", []string{"NATS://nats-1:4222"}},
		{"nats://[::1]:4222", []string{"nats://[::1]:4222"}},
		{"nats://nats-1:4222/", []string{"nats://nats-1:4222/"}},
		{"  nats://nats-1:4222  ", []string{"nats://nats-1:4222"}},
	} {
		l, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url})
		if err != nil {
			t.Errorf("%s: %v", tc.url, err)
			continue
		}
		if !reflect.DeepEqual(l.Config.SinkNATS.URLs, tc.want) {
			t.Errorf("%s: urls = %q, want %q", tc.url, l.Config.SinkNATS.URLs, tc.want)
		}
		if l.Source["sink.nats.urls"] != FromEBURL || l.Origin["sink.nats.urls"] != "loghub.eb_url" {
			t.Errorf("%s: sink.nats.urls comes from %v (%q), want loghub.eb_url", tc.url,
				l.Source["sink.nats.urls"], l.Origin["sink.nats.urls"])
		}
		// Filling the connection is not "setting" it: SetIn stays for the
		// explicit sources the conflict rule compares with.
		if l.SetIn["sink.nats.urls"] {
			t.Errorf("%s: a value from the URL is marked as explicitly set", tc.url)
		}
		// The credentials travel inside the URL; the separate keys stay empty.
		if n := l.Config.SinkNATS; n.Username != "" || n.Password != "" || n.Token != "" {
			t.Errorf("%s: the URL filled the auth keys: %+v", tc.url, n)
		}
	}
	// The flag is the same parameter.
	l := mustLoad(t, []string{"--loghub.eb_url=nats://n:4222"}, nil)
	if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://n:4222"}) || l.Source["sink.nats.urls"] != FromEBURL {
		t.Fatalf("flag: %q from %v", l.Config.SinkNATS.URLs, l.Source["sink.nats.urls"])
	}
}

func TestEBURLUnsupportedOrMalformed(t *testing.T) {
	// Control: a well-formed nats URL from the same env variable loads, so
	// every refusal below is about the URL and not about how it is passed.
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n:4222"})

	for _, tc := range []struct{ url, want string }{
		// The eventbus schemes of the multi-sink server: a URL left over from a
		// sibling deployment must fail the boot, not be ignored.
		{"postgres://u:p@pg/db", `"postgres://" is not supported`},
		{"postgresql://u:p@pg/db", `"postgresql://" is not supported`},
		{"redis://redis-1:6379/0", `"redis://" is not supported`},
		{"rediss://redis-1:6380/0", `"rediss://" is not supported`},
		{"amqp://guest:guest@rabbit:5672/", `"amqp://" is not supported`},
		{"kafka://k:9092", `"kafka://" is not supported`},
		{"https://bus.example", `"https://" is not supported`},
		{"ws://n:8080", `"ws://" is not supported`},
		{"wss://n:8443", `"wss://" is not supported`},
		{"nats-1:4222", "not a URL"},
		{"nats://", "needs a host"},
		{"nats://a:4222/path", "no path or query"},
		{"nats://a:4222?x=1", "no path or query"},
		{"nats://a:4222#f", "no path or query"},
		{"nats://a:4222,redis://b", "every entry"},
		{"nats://a:4222,b:4222", "every entry"},
		{"nats://a:notaport", "not a valid URL"},
		{"1nats://a:4222", "an unrecognised scheme"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if !strings.Contains(err.Error(), "LOGHUB_EB_URL") {
			t.Errorf("%s: error does not name its source: %v", tc.url, err)
		}
	}
	// The unsupported-scheme error says what IS supported.
	err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://pg/db"}, "nats://")
	if !strings.Contains(err.Error(), "NATS") {
		t.Errorf("postgres://: %v", err)
	}
	// The password never reaches an error message, whatever went wrong.
	for _, u := range []string{
		"amqp://guest:hunter2@rabbit:5672/",
		"redis://u:hunter2@r:6379/0",
		"postgres://u:hunter2@pg/db",
		"tls://u:hunter2@n:4222",
		"nats://u:hunter2@n:notaport",
		"nats://u:hunter2@n:4222/path",
		"nats://u:hunter2@n:4222,tls://u:hunter2@m:4222",
		"nats://u:hunter2@n:4222,m:4222",
		"hunter2",
		"u:hunter2@n:4222",
		"hunt er2://x",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "hunt er2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// Empty means "not set": nothing is filled. Only blanks is set to nothing
	// visible, refused (TestEBURLOnlyBlanksIsRefused).
	for _, v := range []string{""} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": v})
		if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}) ||
			l.Source["sink.nats.urls"] != FromDefault {
			t.Fatalf("eb_url %q filled the sink: %q from %v", v, l.Config.SinkNATS.URLs, l.Source["sink.nats.urls"])
		}
	}
}

// SPEC §1.2 and §10.9: there is no TLS to NATS. tls:// is refused rather than
// downgraded to nats://, because the operator who wrote it asked for the
// credentials and every event NOT to travel in clear.
func TestEBURLRefusesTLS(t *testing.T) {
	for _, u := range []string{"tls://n:4222", "TLS://u:p@n:4222", "tls://a:4222,tls://b:4222"} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "TLS")
		for _, w := range []string{"tls://", "LOGHUB_EB_URL", "nats://"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: err = %v, want it to name %q", u, err, w)
			}
		}
		// The flag spelling is the same parameter and the same refusal.
		mustFail(t, []string{"--loghub.eb_url=" + u}, nil, "TLS")
	}
	// A tls:// hidden in a list is refused too, naming TLS.
	for _, u := range []string{"nats://a:4222,tls://b:4222", "nats://a:4222, TLS://b:4222"} {
		mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "TLS")
	}
	// Control: the same hosts over nats:// load and fill the connection, so the
	// refusal above is the scheme and only the scheme.
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n:4222"})
	if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://u:p@n:4222"}) {
		t.Fatalf("nats://: filled %+v", l.Config.SinkNATS)
	}
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://a:4222,nats://b:4222"})
}

// SPEC §10.9, "one source of truth": an explicit connection key next to the
// URL is exit 1 naming both sources.
func TestEBURLConflicts(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[sink.nats]\npassword = from-file\n")

	for _, tc := range []struct {
		args []string
		env  map[string]string
		want []string
	}{
		{[]string{"--sink.nats.urls=nats://other:4222"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
			[]string{"sink.nats.urls", "env LOGHUB_EB_URL", "flag --sink.nats.urls"}},
		// The same value is still a second source: the next edit of one of
		// them would silently lose to the other.
		{[]string{"--sink.nats.urls=nats://n:4222"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
			[]string{"sink.nats.urls"}},
		{nil, map[string]string{"LOGHUB_EB_URL": "nats://n:4222", "SYSLOG_SINK_NATS_PASSWORD": "x"},
			[]string{"sink.nats.password", "env SYSLOG_SINK_NATS_PASSWORD"}},
		{[]string{"--config=" + ini}, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n:4222"},
			[]string{"sink.nats.password", "file " + ini + " line 2"}},
		{[]string{"--loghub.eb_url=nats://n:4222", "--sink.nats.username=other"}, nil,
			[]string{"sink.nats.username", "flag --loghub.eb_url", "flag --sink.nats.username"}},
		// The URL has no userinfo, yet it still owns the token.
		{[]string{"--sink.nats.token=t"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
			[]string{"sink.nats.token", "flag --sink.nats.token"}},
	} {
		err := mustFail(t, tc.args, tc.env, tc.want[0])
		for _, w := range tc.want[1:] {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%v %v: err = %v, want it to name %q", tc.args, tc.env, err, w)
			}
		}
		if !strings.Contains(err.Error(), "one source of truth") {
			t.Errorf("%v %v: err = %v", tc.args, tc.env, err)
		}
	}

	// Keys a URL cannot express complement it.
	l := mustLoad(t, []string{"--sink.nats.credentials=/etc/syslog/sls.creds", "--sink.nats.stream=LOGS",
		"--sink.nats.mode=core", "--sink.workers=3"},
		map[string]string{"LOGHUB_EB_URL": "nats://n:4222"})
	if n := l.Config.SinkNATS; n.Credentials != "/etc/syslog/sls.creds" || n.Stream != "LOGS" ||
		n.Mode != NATSModeCore || !reflect.DeepEqual(n.URLs, []string{"nats://n:4222"}) {
		t.Fatalf("complemented = %+v", n)
	}
	// The connection the URL fills is validated like any other: the stream
	// rule still applies next to it.
	mustFail(t, []string{"--sink.nats.stream=a.b"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
		"sink.nats.stream")
}

// SPEC §10.9: the dump shows the derived values, with the password redacted,
// and stays re-loadable.
func TestEBURLDump(t *testing.T) {
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://app:hunter2@n-1:4222,nats://app:hunter2@n-2:4222"})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("password in the dump:\n%s", out)
	}
	for _, want := range []string{
		"nats://app:***@n-1:4222,nats://app:***@n-2:4222",
		"# derived from loghub.eb_url",
		"# urls                         = nats://app:***@n-1:4222,nats://app:***@n-2:4222",
		"# decided by loghub.eb_url, which leaves it at its default",
		"# password                     = \n",
		"# token                        = \n",
		"# username                     = \n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dump lacks %q:\n%s", want, out)
		}
	}
	if l.Effective()["loghub.eb_url"] != "nats://app:***@n-1:4222,nats://app:***@n-2:4222" ||
		l.Effective()["sink.nats.urls"] != "nats://app:***@n-1:4222,nats://app:***@n-2:4222" {
		t.Fatalf("Effective = %q %q", l.Effective()["loghub.eb_url"], l.Effective()["sink.nats.urls"])
	}

	// Without a secret the dump reads back to the same configuration: the
	// keys the URL decides are commented out -- filled ones and the ones it
	// leaves at their default -- so the URL decides them again instead of
	// conflicting with an explicit copy.
	for _, u := range []string{
		"nats://n-1:4222",
		"nats://n-1:4222, nats://n-2:4222",
		"NATS://[::1]:4222",
	} {
		l = mustLoad(t, []string{"--sink.workers=3", "--sink.nats.mode=core"}, map[string]string{"LOGHUB_EB_URL": u})
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
		for _, name := range ebOwnedKeys {
			if back.SetIn[name] {
				t.Fatalf("%s: read back, %s is set explicitly by the dump:\n%s", u, name, buf.String())
			}
		}
		if back.Source["sink.nats.urls"] != FromEBURL {
			t.Fatalf("%s: read back, sink.nats.urls comes from %v", u, back.Source["sink.nats.urls"])
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
	boot := mustLoad(t, []string{"--sink.nats.token=a"}, nil)
	fresh := mustLoad(t, []string{"--sink.nats.token=b"}, nil)
	applied, ignored := Compare(boot, nil, fresh)
	if len(applied) != 0 || !equalNames(ignored, []string{"sink.nats.token"}) {
		t.Fatalf("applied=%v ignored=%v", applied, ignored)
	}
	// A secret inside a URL counts the same way.
	boot = mustLoad(t, []string{"--sink.nats.urls=nats://u:a@n:4222"}, nil)
	fresh = mustLoad(t, []string{"--sink.nats.urls=nats://u:b@n:4222"}, nil)
	if applied, ignored := Compare(boot, nil, fresh); len(applied) != 0 || !equalNames(ignored, []string{"sink.nats.urls"}) {
		t.Fatalf("url secret: applied=%v ignored=%v", applied, ignored)
	}
	boot = mustLoad(t, []string{"--sink.nats.token=a"}, nil)
	// Every [syslog] key, and everything outside [log], requires a restart
	// (SPEC §16.3); the three [log] keys apply.
	fresh = mustLoad(t, []string{"--syslog.payload_raw=false", "--syslog.rfc3164_timezone=Asia/Tokyo",
		"--log.level=debug", "--log.access", "--log.access_sample=0.5", "--sink.workers=6",
		"--log.format=text", "--loghub.beacon_interval=30", "--loghub.ctl_namespace=acme._ctl",
		"--sink.nats.mode=core"}, nil)
	applied, ignored = Compare(boot, boot, fresh)
	if !equalNames(applied, []string{"log.level", "log.access", "log.access_sample"}) ||
		!equalNames(ignored, []string{"syslog.rfc3164_timezone", "syslog.payload_raw", "sink.nats.token",
			"sink.workers", "log.format", "loghub.beacon_interval", "loghub.ctl_namespace", "sink.nats.mode"}) {
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
