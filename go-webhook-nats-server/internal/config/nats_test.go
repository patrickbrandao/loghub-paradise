package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The [sink.nats] and [loghub] tests, ported from go-syslog-nats-server with
// its review findings (SPEC §8.7, §10.1).

func tryLoad(args []string, env map[string]string) (*Loaded, error) {
	return Load(Options{Args: args, LookupEnv: envMap(env)})
}

func mustLoad(t *testing.T, args []string, env map[string]string) *Loaded {
	t.Helper()
	l, err := tryLoad(args, env)
	if err != nil {
		t.Fatalf("%v %v: %v", args, env, err)
	}
	return l
}

// mustFail asserts a refusal that names want, and returns it.
func mustFail(t *testing.T, args []string, env map[string]string, want string) error {
	t.Helper()
	_, err := tryLoad(args, env)
	if err == nil {
		t.Fatalf("%v %v was accepted; want an error naming %q", args, env, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%v %v: err = %v, want it to contain %q", args, env, err, want)
	}
	return err
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBeaconIntervalSeconds(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"15": 15 * time.Second, "15s": 15 * time.Second, " 30 ": 30 * time.Second,
		"2m": 2 * time.Minute, "0": 0, "0s": 0, "1500ms": 1500 * time.Millisecond,
	} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw})
		if got := l.Config.LogHub.BeaconInterval; got != want {
			t.Errorf("LOGHUB_BEACON_INTERVAL=%q -> %s, want %s", raw, got, want)
		}
		if l.Source["loghub.beacon_interval"] != FromEnv {
			t.Errorf("LOGHUB_BEACON_INTERVAL=%q: source = %v", raw, l.Source["loghub.beacon_interval"])
		}
	}
	for _, raw := range []string{"-1", "-5s", "abc", "1.5", "", "15 s"} {
		mustFail(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw}, "LOGHUB_BEACON_INTERVAL")
	}
	// There is no WHS_ spelling of a [loghub] key: it would be a second name
	// for one value, and the skill fixes the one.
	l := mustLoad(t, nil, map[string]string{"WHS_LOGHUB_BEACON_INTERVAL": "99"})
	if l.Config.LogHub.BeaconInterval != 15*time.Second {
		t.Fatalf("WHS_LOGHUB_BEACON_INTERVAL was read: %s", l.Config.LogHub.BeaconInterval)
	}
	// The flag and the file take the same forms.
	if l := mustLoad(t, []string{"--loghub.beacon_interval=20"}, nil); l.Config.LogHub.BeaconInterval != 20*time.Second {
		t.Fatalf("flag: %s", l.Config.LogHub.BeaconInterval)
	}
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[loghub]\nbeacon_interval = 45\n")
	if l := mustLoad(t, []string{"--config=" + ini}, nil); l.Config.LogHub.BeaconInterval != 45*time.Second {
		t.Fatalf("file: %s", l.Config.LogHub.BeaconInterval)
	}
}

func TestNATSValidation(t *testing.T) {
	// One method at a time is fine, and a cluster is a list.
	for _, args := range [][]string{
		{"--sink.nats.username=u", "--sink.nats.password=p"},
		{"--sink.nats.username=u"},
		{"--sink.nats.token=t"},
		{"--sink.nats.credentials=/etc/webhook/whs.creds"},
		{"--sink.nats.urls=nats://a:4222,nats://b:4222"},
		{"--sink.nats.urls=NATS://a:4222"},
		{"--sink.nats.urls=nats://u:p@a:4222"},
		{"--sink.nats.stream=LOGS"},
		{"--sink.nats.stream=LOGHUB-WEBHOOK_2"},
		// The stream is ignored in core mode, so a dotted name there is no error.
		{"--sink.nats.mode=core", "--sink.nats.stream=a.b"},
		{"--sink.nats.mode=core", "--sink.nats.stream="},
		{"--sink.nats.mode=JetStream"},
	} {
		mustLoad(t, args, nil)
	}

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.nats.urls="}, "sink.nats.urls"},
		{[]string{"--sink.nats.urls= , "}, "sink.nats.urls"},
		// Stream names: no dots, whitespace, wildcards or slashes, never empty
		// in jetstream mode.
		{[]string{"--sink.nats.stream=WEB.HOOK"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=WEB HOOK"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=WEB*"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=WEB>"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=a/b"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=a\\b"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream="}, "sink.nats.stream"},
		{[]string{"--sink.nats.mode=jetstream", "--sink.nats.stream=a.b"}, "sink.nats.stream"},
		{[]string{"--sink.nats.mode=best-effort"}, "jetstream, core"},
		// Authentication: one method only, and a password needs its user.
		{[]string{"--sink.nats.token=t", "--sink.nats.username=u"}, "only one of"},
		{[]string{"--sink.nats.credentials=/c", "--sink.nats.token=t"}, "only one of"},
		{[]string{"--sink.nats.credentials=/c", "--sink.nats.username=u", "--sink.nats.password=p"}, "only one of"},
		{[]string{"--sink.nats.password=p"}, "without sink.nats.username"},
		// The beacon's prefix must be publishable, token by token.
		{[]string{"--loghub.ctl_namespace="}, "loghub.ctl_namespace"},
		{[]string{"--loghub.ctl_namespace=loghub.>"}, "publishable"},
		{[]string{"--loghub.ctl_namespace=loghub.*"}, "publishable"},
		{[]string{"--loghub.ctl_namespace=loghub..ctl"}, "publishable"},
		{[]string{"--loghub.ctl_namespace=.loghub"}, "publishable"},
		{[]string{"--loghub.ctl_namespace=loghub."}, "publishable"},
		{[]string{"--loghub.ctl_namespace=log hub"}, "publishable"},
	} {
		mustFail(t, tc.args, nil, tc.want)
	}
	// Control: the same shapes with valid values load.
	for _, ns := range []string{"loghub._ctl", "acme", "a.b.c", "LOGHUB-ctl_2"} {
		mustLoad(t, []string{"--loghub.ctl_namespace=" + ns}, nil)
	}
}

// SPEC §1.2: there is no TLS to NATS, and nats.go would otherwise dial tls://,
// ws:// and wss:// -- or read a bare host:port as nats:// -- with a handshake
// this server was never configured for. Only nats:// passes.
func TestNATSURLSchemes(t *testing.T) {
	// Control: nats:// in every spelling the client accepts loads.
	for _, u := range []string{"nats://n:4222", "nats://u:p@n:4222", "nats://tok@n:4222",
		"nats://a:4222,nats://b:4222", "nats://[::1]:4222"} {
		l := mustLoad(t, []string{"--sink.nats.urls=" + u}, nil)
		if len(l.Config.SinkNATS.URLs) == 0 {
			t.Fatalf("%s: no URL stored", u)
		}
	}
	for _, tc := range []struct{ urls, scheme string }{
		{"tls://n:4222", `"tls://"`},
		{"ws://n:8080", `"ws://"`},
		{"wss://n:8443", `"wss://"`},
		{"TLS://n:4222", `"tls://"`},
		{"n:4222", "without a scheme"},
		{"127.0.0.1:4222", "without a scheme"},
		{"nats://a:4222,tls://b:4222", `"tls://"`},
		{"http://n:8222", `"http://"`},
	} {
		err := mustFail(t, []string{"--sink.nats.urls=" + tc.urls}, nil, "sink.nats.urls")
		for _, w := range []string{"nats://", "no TLS", tc.scheme} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: err = %v, want it to name %s", tc.urls, err, w)
			}
		}
		// The environment is the same parameter and the same refusal.
		mustFail(t, nil, map[string]string{"WHS_SINK_NATS_URLS": tc.urls}, "sink.nats.urls")
	}
	// A piece of the list with no scheme after the first: a bare host:port,
	// or the tail of a password an unescaped ',' split. The message says both
	// (go-syslog-nats-server, porte da família syslog/RELP, 2026-10-13).
	for _, u := range []string{"nats://a:4222,b:4222", "nats://u:12,cret@h:4222"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "no scheme")
		if !strings.Contains(err.Error(), "nats://") || !strings.Contains(err.Error(), "','") ||
			strings.Contains(err.Error(), "cret") {
			t.Errorf("%s: err = %v, want nats:// and the ',' named, and nothing of the URL", u, err)
		}
	}
	// The refusal names the scheme, never the URL: it may carry a password.
	for _, u := range []string{"tls://u:hunter2@n:4222", "ws://hunter2@n:80", "u:hunter2@n:4222",
		"hunter2", "nats://a:4222,wss://u:hunter2@b:443"} {
		_, err := tryLoad([]string{"--sink.nats.urls=" + u}, nil)
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
}

// SPEC §8.7.2: core is best effort, and the boot says so; jetstream does not.
func TestCoreModeWarns(t *testing.T) {
	l := mustLoad(t, []string{"--sink.nats.mode=core"}, nil)
	if !hasWarning(l.Warnings, "best effort") || !hasWarning(l.Warnings, "sink.nats.mode=core") {
		t.Fatalf("core: warnings = %q", l.Warnings)
	}
	if l := mustLoad(t, []string{"--sink.nats.mode=jetstream"}, nil); hasWarning(l.Warnings, "best effort") {
		t.Fatalf("jetstream warned: %q", l.Warnings)
	}
}

func TestSecretsAreRedacted(t *testing.T) {
	for _, key := range []string{"sink.nats.password", "sink.nats.token"} {
		args := []string{"--" + key + "=hunter2"}
		if key == "sink.nats.password" {
			args = append(args, "--sink.nats.username=u")
		}
		l := mustLoad(t, args, nil)
		var buf bytes.Buffer
		if err := l.DumpINI(&buf); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(buf.Bytes(), []byte("hunter2")) {
			t.Errorf("%s printed in the dump", key)
		}
		if !bytes.Contains(buf.Bytes(), []byte("WARNING: secrets")) {
			t.Errorf("%s: the dump does not warn that it is not re-loadable", key)
		}
		if l.Effective()[key] != "***" {
			t.Errorf("%s not redacted in Effective()", key)
		}
	}
	// A URL in the sink keys loses its userinfo in the dump and the boot log.
	l := mustLoad(t, []string{"--sink.nats.urls=nats://u:hunter2@n-1:4222"}, nil)
	if got := l.Effective()["sink.nats.urls"]; got != "nats://u:***@n-1:4222" {
		t.Errorf("Effective sink.nats.urls = %q", got)
	}
	if l.Config.SinkNATS.URLs[0] != "nats://u:hunter2@n-1:4222" {
		t.Error("redaction must not alter the loaded value")
	}

	// A URL keeps its host and loses its userinfo. The comma and the slash in
	// the password are the reason redactURL works on the text: a split on the
	// comma would print "pa" in clear.
	for in, want := range map[string]string{
		"nats://u:hunter2@n-1:4222":                 "nats://u:***@n-1:4222",
		"nats://u:pa,ss/hunter2@n-1:4222":           "nats://u:***@n-1:4222",
		"nats://tok3n@n-1:4222":                     "nats://***@n-1:4222",
		"nats://n-1:4222":                           "nats://n-1:4222",
		"nats://u:hunter2@a:4222,nats://tok3n@b:42": "nats://u:***@a:4222,nats://***@b:42",
		"hunter2": "***",
	} {
		if got := redactURL(in); got != want {
			t.Errorf("redactURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// The review findings of go-syslog-nats-server, held here too.
func TestNATSURLAndBeaconReviewFindings(t *testing.T) {
	mustLoad(t, []string{"--sink.nats.urls=nats://bob:s3cr%25t@n1:4222"}, nil) // control: encoded
	for _, bad := range []string{"nats://bob:s3cr%t@n1:4222", "nats://bob:p/w@n1:4222"} {
		_, err := tryLoad([]string{"--sink.nats.urls=" + bad}, nil)
		if err == nil || strings.Contains(err.Error(), "s3cr") || strings.Contains(err.Error(), "p/w") {
			t.Errorf("%q: err = %v, want a refusal that does not echo the password", bad, err)
		}
	}
	mustFail(t, []string{"--sink.nats.urls=nats://:4222"}, nil, "no host")

	// Credentials in the URL and in a key: the client would silently use the URL's.
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", "--sink.nats.username=other"}, nil, "carries credentials")
	mustFail(t, []string{"--sink.nats.urls=nats://tok@n1:4222", "--sink.nats.token=x"}, nil, "carries credentials")
	mustLoad(t, []string{"--sink.nats.urls=nats://n1:4222", "--sink.nats.username=u", "--sink.nats.password=p"}, nil)

	// A beacon every nanosecond floods the control plane; 0 and 1 s are fine.
	mustFail(t, []string{"--loghub.beacon_interval=1ns"}, nil, "below 1s")
	mustFail(t, []string{"--loghub.beacon_interval=500ms"}, nil, "below 1s")
	mustLoad(t, []string{"--loghub.beacon_interval=0"}, nil)
	mustLoad(t, []string{"--loghub.beacon_interval=1"}, nil)
}

// ------------------------------------------------------------ LOGHUB_EB_URL

// ebOwnedKeys are the [sink.nats] keys LOGHUB_EB_URL decides (SPEC §8.7.11):
// the ones a NATS URL can express. credentials, mode and stream stay free.
var ebOwnedKeys = []string{"sink.nats.urls", "sink.nats.username", "sink.nats.password", "sink.nats.token"}

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
		{"NATS://nats-1:4222", []string{"NATS://nats-1:4222"}},
		{"nats://[::1]:4222", []string{"nats://[::1]:4222"}},
		{"nats://nats-1:4222/", []string{"nats://nats-1:4222/"}},
		{"  nats://nats-1:4222  ", []string{"nats://nats-1:4222"}},
	} {
		l, err := tryLoad(nil, map[string]string{"LOGHUB_EB_URL": tc.url})
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
		// The eventbus schemes of sibling servers: a URL left over from one
		// must fail the boot, not be ignored.
		{"postgres://u:p@pg/db", `"postgres://" is not supported`},
		{"postgresql://u:p@pg/db", `"postgresql://" is not supported`},
		{"redis://redis-1:6379/0", `"redis://" is not supported`},
		{"amqp://guest:guest@rabbit:5672/", `"amqp://" is not supported`},
		{"https://bus.example", `"https://" is not supported`},
		{"ws://n:8080", `"ws://" is not supported`},
		{"wss://n:8443", `"wss://" is not supported`},
		{"nats-1:4222", "not a URL"},
		{"nats://", "needs a host"},
		{"nats://a:4222/path", "no path, query or fragment"},
		{"nats://a:4222?x=1", "no path, query or fragment"},
		{"nats://a:4222#f", "no path, query or fragment"},
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
	// The password never reaches an error message, whatever went wrong.
	for _, u := range []string{
		"amqp://guest:hunter2@rabbit:5672/",
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
		_, err := tryLoad(nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "hunt er2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// Empty means "not set": nothing is filled. Only blanks is no URL and
	// no "unset" either: refused (TestFamilyPortEBURL).
	for _, v := range []string{""} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": v})
		if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}) ||
			l.Source["sink.nats.urls"] != FromDefault {
			t.Fatalf("eb_url %q filled the sink: %q from %v", v, l.Config.SinkNATS.URLs, l.Source["sink.nats.urls"])
		}
	}
}

// tls:// is refused rather than downgraded to nats://, because the operator who
// wrote it asked for the credentials and every event NOT to travel in clear.
func TestEBURLRefusesTLS(t *testing.T) {
	for _, u := range []string{"tls://n:4222", "TLS://u:p@n:4222", "tls://a:4222,tls://b:4222"} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "TLS")
		for _, w := range []string{"tls://", "LOGHUB_EB_URL", "nats://"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: err = %v, want it to name %q", u, err, w)
			}
		}
		mustFail(t, []string{"--loghub.eb_url=" + u}, nil, "TLS")
	}
	for _, u := range []string{"nats://a:4222,tls://b:4222", "nats://a:4222, TLS://b:4222"} {
		mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "TLS")
	}
	// Control: the same hosts over nats:// load.
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://a:4222,nats://b:4222"})
}

// "One source of truth": an explicit connection key next to the URL is exit 1
// naming both sources (SPEC §8.7.11).
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
		{nil, map[string]string{"LOGHUB_EB_URL": "nats://n:4222", "WHS_SINK_NATS_PASSWORD": "x"},
			[]string{"sink.nats.password", "env WHS_SINK_NATS_PASSWORD"}},
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
	l := mustLoad(t, []string{"--sink.nats.credentials=/etc/webhook/whs.creds", "--sink.nats.stream=LOGS",
		"--sink.nats.mode=core", "--sink.workers=3"},
		map[string]string{"LOGHUB_EB_URL": "nats://n:4222"})
	if n := l.Config.SinkNATS; n.Credentials != "/etc/webhook/whs.creds" || n.Stream != "LOGS" ||
		n.Mode != NATSModeCore || !reflect.DeepEqual(n.URLs, []string{"nats://n:4222"}) {
		t.Fatalf("complemented = %+v", n)
	}
	// The connection the URL fills is validated like any other.
	mustFail(t, []string{"--sink.nats.stream=a.b"}, map[string]string{"LOGHUB_EB_URL": "nats://n:4222"},
		"sink.nats.stream")
}

// The dump shows the derived values, with the password redacted, and stays
// re-loadable.
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
	// keys the URL decides are commented out, so the URL decides them again
	// instead of conflicting with an explicit copy.
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

// The findings of this module's own review (docs/TESTING.md §9).
func TestConfigReviewFindings(t *testing.T) {
	// No piece of a URL that does not parse reaches the message: url.Error's
	// inner text quoted the password up to the first '/', '#', '?' or ','.
	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"nats://bob:Sup3rSecret#xyz@n1:4222",
		"nats://bob:Sup3rSecret?xyz@n1:4222",
		"nats://bob:Sup3rSecret%zz@n1:4222",
		"nats://bob:Sup3rSecret,xyz@n1:4222",
	} {
		_, err1 := tryLoad([]string{"--sink.nats.urls=" + u}, nil)
		_, err2 := tryLoad(nil, map[string]string{"LOGHUB_EB_URL": u})
		for _, err := range []error{err1, err2} {
			if err == nil || strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") {
				t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL", u, err)
			}
		}
	}
	// A password that parses as a port and a path: no credentials at all,
	// dialled until boot_timeout. Refused like LOGHUB_EB_URL refuses it; the
	// '@' after the '/', '?' or '#' names the escape to use (porte da família
	// syslog/RELP, 2026-10-13).
	for _, u := range []string{"nats://user:1234/5678@n1:4222", "nats://user:1234#5678@n1:4222",
		"nats://user:1234?5678@n1:4222"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "%2F, %3F, %23")
		if strings.Contains(err.Error(), "5678") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// Without the '@', the path, query or fragment rule.
	for _, u := range []string{"nats://n1:4222/x", "nats://n1:4222?x=1", "nats://n1:4222#f"} {
		mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "path, a query or a fragment")
	}
	// A percent-escape in the host is about the host, not the password.
	if err := mustFail(t, []string{"--sink.nats.urls=nats://bob:pw@n%31:4222"}, nil, "percent-encoded host"); strings.Contains(err.Error(), "pw") {
		t.Errorf("the password is echoed: %v", err)
	}
	mustLoad(t, []string{"--sink.nats.urls=nats://user:1234%2F5678@n1:4222/"}, nil) // control

	// NaN passes "< 0 || > 1"; a NaN high water never saturates the ring.
	mustFail(t, []string{"--ring.high_water=NaN"}, nil, "ring.high_water")
	mustFail(t, []string{"--log.access_sample=NaN"}, nil, "log.access_sample")

	// An empty admin.bind would listen on every interface, on a random port.
	mustFail(t, []string{"--admin.bind="}, nil, "admin.bind is empty")
	mustFail(t, []string{"--admin.bind=9101"}, nil, "not host:port")
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind="}, nil)

	// A parser error names the line or the flag, never the value on it.
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[sink.nats]\npassword: hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, "line 2"); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("INI error echoes the line: %v", err)
	}
	writeFile(t, ini, "[sink.nats hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, "line 1"); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("INI error echoes the header: %v", err)
	}
	err := mustFail(t, []string{"sink.nats.password=hunter2"}, nil, "unexpected argument")
	if strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "sink.nats.password") {
		t.Errorf("flag error = %v, want the name and not the value", err)
	}
	// A flag missing its value never swallows the next flag (a $WORKERS that
	// expanded to nothing printed the password in the type error), and a
	// value typed after a space past the '=' is refused unquoted (porte de
	// volta, 2026-10-11; SPEC §13.2).
	for _, args := range [][]string{
		{"--sink.workers", "--sink.nats.password=hunter2"},
		{"--sink.nats.stream", "--sink.nats.token=hunter2"},
		{"--sink.op_timeout", "--sink.nats.urls=nats://u:hunter2@h:4222"},
		{"--sink.workers", "--no.such.key=hunter2"},
		{"--sink.workers", "-sink.nats.password=hunter2"},
		{"--config", "--sink.nats.password=hunter2"},
		{"--sink.nats.password=", "hunter2"},
		{"--sink.nats.password", "=", "hunter2"},
		// The review of this port: a value starting with "--" in the
		// separated form was named as "the flag --hunter2"; a dash-value
		// after an empty '=' as an unknown parameter; the tail of a value
		// the shell split at a space as a stray word.
		{"--sink.nats.password", "--hunter2"},
		{"--sink.nats.password=", "-hunter2"},
		{"--sink.nats.password", "=", "-hunter2"},
		{"--sink.nats.password=", "--hunter2=x"},
		{"--sink.nats.password=foo", "hunter2"},
		{"--sink.nats.password", "foo", "hunter2"},
	} {
		_, err := tryLoad(args, nil)
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: err = %v, want a refusal that does not echo the secret", args, err)
		}
	}
	// Controls: a value of the separated form is still taken, a negative
	// number is a value and not a flag, and a stray word is still named.
	if l := mustLoad(t, []string{"--sink.workers", "4", "--sink.nats.stream", "LOGHUB_X"}, nil); l.Config.Sink.Workers != 4 ||
		l.Config.SinkNATS.Stream != "LOGHUB_X" {
		t.Errorf("separated values: workers=%d stream=%q", l.Config.Sink.Workers, l.Config.SinkNATS.Stream)
	}
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "must be at least 1")
	mustFail(t, []string{"--sink.nats.stream", "--help"}, nil, "the next argument is the flag --help")
	mustFail(t, []string{"--sink.nats.password=", "--server.http2", "extra"}, nil, `"extra"`)
	// Each says what went wrong: the space after the '=', or a value with a
	// space left unquoted.
	mustFail(t, []string{"--sink.nats.password=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password", "=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password=foo", "hunter2"}, nil, "goes in quotes")
	// After a value, a flag outside every section may be the tail of that
	// value (knownSectionOf); a typo inside one is still named.
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, "unexpected argument after --sink.workers")
	mustFail(t, []string{"--sink.workers=4", "--sink.wrkers=1"}, nil, `"sink.wrkers"`)
	mustFail(t, []string{"--sink.nats.password", "--sink.workers=4"}, nil, "the flag --sink.workers")

	// An explicit EMPTY owned key does not compete with LOGHUB_EB_URL: it is
	// what a compose file forwarding ${KEY:-} produces (docker/).
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n:4222",
		"WHS_SINK_NATS_URLS": "", "WHS_SINK_NATS_USERNAME": "", "WHS_SINK_NATS_PASSWORD": "",
		"WHS_SINK_NATS_TOKEN": ""})
	if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://u:p@n:4222"}) || l.Source["sink.nats.urls"] != FromEBURL {
		t.Fatalf("eb_url with empty explicit keys: %q from %v", l.Config.SinkNATS.URLs, l.Source["sink.nats.urls"])
	}
	writeFile(t, ini, "[sink.nats]\npassword =\n")
	mustLoad(t, []string{"--config=" + ini}, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n:4222"})
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://n:4222", "WHS_SINK_NATS_USERNAME": "x"},
		"one source of truth") // control: a value still conflicts

	// Values with surrounding whitespace or quotes survive --dump-config.
	l = mustLoad(t, []string{"--sink.nats.stream='LOGHUB_X'", "--sink.nats.credentials= /etc/padded.creds ",
		"--emergency.dir= /tmp/e ", `--headers.exclude="Cookie"`}, nil)
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, buf.String())
	back := mustLoad(t, []string{"--config=" + f}, nil)
	if !reflect.DeepEqual(back.Config, l.Config) {
		t.Fatalf("the dump did not read back:\n%+v\n%+v\n%s", back.Config, l.Config, buf.String())
	}
}
