package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
)

// envMap builds a LookupEnv over a fixed map.
func envMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// load is the common harness: no env, no file, unless overridden.
func load(t *testing.T, o Options) *Loaded {
	t.Helper()
	if o.LookupEnv == nil {
		o.LookupEnv = envMap(nil)
	}
	l, err := Load(o)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return l
}

func writeINI(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "conf.ini")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// --------------------------------------------------------------------- defaults

func TestDefaultsMatchSpec(t *testing.T) {
	c := load(t, Options{}).Config

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"server.bind", c.Server.Bind, ":8080"},
		{"server.read_header_timeout", c.Server.ReadHeaderTimeout, 5 * time.Second},
		{"server.read_timeout", c.Server.ReadTimeout, 30 * time.Second},
		{"server.max_body_bytes", c.Server.MaxBodyBytes, int64(5 << 20)},
		{"server.max_header_bytes", c.Server.MaxHeaderBytes, int64(64 << 10)},
		{"server.max_uri_bytes", c.Server.MaxURIBytes, int64(8 << 10)},
		{"server.reject_drain_bytes", c.Server.RejectDrainBytes, int64(64 << 10)},
		{"server.listeners", c.Server.Listeners, 1},
		{"admin.bind", c.Admin.Bind, "127.0.0.1:9101"},
		{"ratelimit.enabled", c.RateLimit.Enabled, false},
		{"ring.mode", c.Ring.Mode, RingMemory},
		{"ring.capacity_items", c.Ring.CapacityItems, 65536},
		{"ring.max_bytes", c.Ring.MaxBytes, int64(512 << 20)},
		{"ring.high_water", c.Ring.HighWater, 0.80},
		{"sink.type", c.Sink.Type, SinkNATSType},
		{"sink.workers", c.Sink.Workers, 8},
		{"sink.batch", c.Sink.Batch, 256},
		{"sink.health_interval", c.Sink.HealthInterval, 2 * time.Second},
		{"sink.fail_timeout", c.Sink.FailTimeout, 30 * time.Second},
		{"sink.boot_timeout", c.Sink.BootTimeout, 300 * time.Second},
		{"sink.op_timeout", c.Sink.OpTimeout, 5 * time.Second},
		{"sink.nats.mode", c.SinkNATS.Mode, NATSModeJetStream},
		{"sink.nats.stream", c.SinkNATS.Stream, "LOGHUB_WEBHOOK"},
		{"sink.nats.subject", c.SinkNATS.Subject, "loghub.{space}.{type}"},
		{"sink.nats.tls", c.SinkNATS.TLS, false},
		{"sink.nats.username", c.SinkNATS.Username, ""},
		{"loghub.eb_url", c.LogHub.EBURL, ""},
		{"loghub.beacon_interval", c.LogHub.BeaconInterval, 15 * time.Second},
		{"loghub.ctl_namespace", c.LogHub.CtlNamespace, "loghub._ctl"},
		{"emergency.enabled", c.Emergency.Enabled, true},
		{"log.access", c.Log.Access, false},
		{"log.access_sample", c.Log.AccessSample, 0.01},
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout, 30 * time.Second},
	}
	for _, ck := range checks {
		if ck.got != ck.want {
			t.Errorf("%s = %v, want %v", ck.name, ck.got, ck.want)
		}
	}

	if want := []string{"127.0.0.0/8", "::1"}; !eqList(c.Proxy.Trusted, want) {
		t.Errorf("proxy.trusted = %v, want %v", c.Proxy.Trusted, want)
	}
	if want := []string{"nats://127.0.0.1:4222"}; !eqList(c.SinkNATS.URLs, want) {
		t.Errorf("sink.nats.urls = %v, want %v", c.SinkNATS.URLs, want)
	}
}

func eqList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------ precedence

func TestPrecedenceAllFourSources(t *testing.T) {
	ini := writeINI(t, "[server]\nbind = :1111\n[ring]\nmode = memory\ncapacity_items = 111\n")

	// default < file
	l := load(t, Options{Args: []string{"--config", ini}})
	if got := l.Config.Server.Bind; got != ":1111" {
		t.Errorf("file should beat default: bind = %q", got)
	}
	if l.Source["server.bind"] != FromFile {
		t.Errorf("source = %v, want file", l.Source["server.bind"])
	}

	// file < env
	l = load(t, Options{
		Args:      []string{"--config", ini},
		LookupEnv: envMap(map[string]string{"WHS_SERVER_BIND": ":2222"}),
	})
	if got := l.Config.Server.Bind; got != ":2222" {
		t.Errorf("env should beat file: bind = %q", got)
	}
	if l.Source["server.bind"] != FromEnv {
		t.Errorf("source = %v, want env", l.Source["server.bind"])
	}

	// env < flag
	l = load(t, Options{
		Args:      []string{"--config", ini, "--server.bind=:3333"},
		LookupEnv: envMap(map[string]string{"WHS_SERVER_BIND": ":2222"}),
	})
	if got := l.Config.Server.Bind; got != ":3333" {
		t.Errorf("flag should beat env: bind = %q", got)
	}
	if l.Source["server.bind"] != FromFlag {
		t.Errorf("source = %v, want flag", l.Source["server.bind"])
	}

	// A value set only in the file is untouched by the higher sources.
	if got := l.Config.Ring.CapacityItems; got != 111 {
		t.Errorf("ring.capacity_items = %d, want 111", got)
	}
}

func TestPrecedencePerKind(t *testing.T) {
	ini := writeINI(t, strings.Join([]string{
		"[server]",
		"max_body_bytes = 1MiB", // bytes
		"http2 = false",         // bool
		"listeners = 2",         // int
		"read_timeout = 11s",    // duration
		"[ring]",
		"high_water = 0.5", // float
		"[proxy]",
		"trusted = 10.0.0.0/8", // list
	}, "\n"))

	l := load(t, Options{
		Args: []string{"--config", ini},
		LookupEnv: envMap(map[string]string{
			"WHS_SERVER_MAX_BODY_BYTES": "2MiB",
			"WHS_SERVER_HTTP2":          "true",
			"WHS_SERVER_LISTENERS":      "3",
			"WHS_SERVER_READ_TIMEOUT":   "12s",
			"WHS_RING_HIGH_WATER":       "0.6",
			"WHS_PROXY_TRUSTED":         "10.0.0.0/8,192.168.0.0/16",
		}),
	})
	c := l.Config
	if c.Server.MaxBodyBytes != 2<<20 {
		t.Errorf("bytes: %d", c.Server.MaxBodyBytes)
	}
	if !c.Server.HTTP2 {
		t.Error("bool: want true")
	}
	if c.Server.Listeners != 3 {
		t.Errorf("int: %d", c.Server.Listeners)
	}
	if c.Server.ReadTimeout != 12*time.Second {
		t.Errorf("duration: %s", c.Server.ReadTimeout)
	}
	if c.Ring.HighWater != 0.6 {
		t.Errorf("float: %g", c.Ring.HighWater)
	}
	if !eqList(c.Proxy.Trusted, []string{"10.0.0.0/8", "192.168.0.0/16"}) {
		t.Errorf("list: %v", c.Proxy.Trusted)
	}
}

// ----------------------------------------------------------------------- flags

func TestFlagForms(t *testing.T) {
	// --name=value, --name value, bare bool, single dash.
	l := load(t, Options{Args: []string{
		"--server.bind=:9999",
		"--ring.capacity_items", "1024",
		"--server.http2",
		"-log.level=debug",
	}})
	c := l.Config
	if c.Server.Bind != ":9999" {
		t.Errorf("bind = %q", c.Server.Bind)
	}
	if c.Ring.CapacityItems != 1024 {
		t.Errorf("capacity = %d", c.Ring.CapacityItems)
	}
	if !c.Server.HTTP2 {
		t.Error("bare bool flag should mean true")
	}
	if c.Log.Level != "debug" {
		t.Errorf("level = %q", c.Log.Level)
	}
}

func TestFlagErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"--server.nope=1"}, "unknown parameter"},
		{"positional", []string{"serve"}, "unexpected argument"},
		{"missing value", []string{"--server.bind"}, "requires a value"},
		{"bad duration", []string{"--server.read_timeout=30"}, "invalid duration"},
		{"bad bytes", []string{"--server.max_body_bytes=5PB"}, "invalid byte size"},
		{"bad bool", []string{"--server.http2=maybe"}, "invalid bool"},
		{"bad int", []string{"--sink.workers=eight"}, "invalid integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(Options{Args: tc.args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestMetaSwitches(t *testing.T) {
	for _, arg := range []string{"--help", "-h", "--version"} {
		_, err := Load(Options{Args: []string{arg}, LookupEnv: envMap(nil)})
		if err != ErrExitEarly {
			t.Errorf("%s: err = %v, want ErrExitEarly", arg, err)
		}
	}
	l := load(t, Options{Args: []string{"--dump-config"}})
	if !l.Meta.DumpConfig {
		t.Error("--dump-config not recorded")
	}
}

// ------------------------------------------------------------------------- ini

func TestINIParsing(t *testing.T) {
	// A '#' inside a value is literal: truncating a password at a comment
	// character would be a silent credential corruption.
	ini := writeINI(t, strings.Join([]string{
		"# leading comment",
		"; another style",
		"",
		"[sink.nats]",
		"username = u",
		"password = p#ss;word",
		"",
		"[headers]",
		`exclude = " Authorization , Cookie "`,
	}, "\n"))

	c := load(t, Options{Args: []string{"--config", ini}}).Config
	if got := c.SinkNATS.Password; got != "p#ss;word" {
		t.Errorf("password = %q, want the literal value with # and ;", got)
	}
	if !eqList(c.Headers.Exclude, []string{"Authorization", "Cookie"}) {
		t.Errorf("exclude = %v", c.Headers.Exclude)
	}
}

func TestINIErrors(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"key before section", "bind = :8080\n", "before any [section]"},
		{"malformed section", "[server\nbind = :1\n", "malformed section header"},
		{"no equals", "[server]\nbind :8080\n", "expected 'key = value'"},
		{"unknown key", "[server]\nnope = 1\n", "unknown parameter"},
		{"empty key", "[server]\n = x\n", "empty key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(Options{
				Args: []string{"--config", writeINI(t, tc.body)}, LookupEnv: envMap(nil),
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestWHSConfigEnvSelectsFile(t *testing.T) {
	ini := writeINI(t, "[server]\nbind = :7777\n")
	c := load(t, Options{LookupEnv: envMap(map[string]string{"WHS_CONFIG": ini})}).Config
	if c.Server.Bind != ":7777" {
		t.Errorf("bind = %q, want the file selected by WHS_CONFIG", c.Server.Bind)
	}
}

// ----------------------------------------------------------------------- bytes

func TestParseBytes(t *testing.T) {
	ok := []struct {
		in   string
		want int64
	}{
		{"0", 0}, {"1024", 1024},
		{"1KiB", 1 << 10}, {"5MiB", 5 << 20}, {"2GiB", 2 << 30},
		{"1KB", 1000}, {"1MB", 1000 * 1000}, {"1GB", 1000 * 1000 * 1000},
		{"1K", 1 << 10}, {"1M", 1 << 20}, {"1G", 1 << 30},
		{"512 MiB", 512 << 20}, {" 64KiB ", 64 << 10}, {"8B", 8},
		{"5mib", 5 << 20}, // suffix match is case-insensitive
	}
	for _, tc := range ok {
		got, err := parseBytes(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"", "abc", "-1", "-1MiB", "5PB", "1.5MiB", "MiB"} {
		if _, err := parseBytes(in); err == nil {
			t.Errorf("parseBytes(%q) should have failed", in)
		}
	}
}

func TestFormatBytesRoundTrips(t *testing.T) {
	for _, n := range []int64{0, 1, 1023, 1 << 10, 64 << 10, 5 << 20, 512 << 20, 2 << 30, 1234567} {
		s := FormatBytes(n)
		back, err := parseBytes(s)
		if err != nil || back != n {
			t.Errorf("FormatBytes(%d) = %q, which parses to %d (err %v)", n, s, back, err)
		}
	}
}

// ---------------------------------------------------------------------- derive

func TestDerivedPaths(t *testing.T) {
	c := load(t, Options{Args: []string{"--ring.data_dir=/srv/whs"}}).Config
	if want := filepath.Join("/srv/whs", "emergency"); c.Emergency.Dir != want {
		t.Errorf("emergency.dir = %q, want %q", c.Emergency.Dir, want)
	}

	// An explicit path is never overwritten by the derivation.
	c = load(t, Options{Args: []string{"--ring.data_dir=/srv/whs", "--emergency.dir=/srv/whs/dumps"}}).Config
	if c.Emergency.Dir != "/srv/whs/dumps" {
		t.Errorf("explicit emergency.dir was overwritten: %q", c.Emergency.Dir)
	}
}

// SPEC §3.5: server.node_id is gone -- the identity comes from go-loghub-ident
// and is never configured. An INI that still carries it is refused as unknown,
// like any other key the registry does not have, rather than silently ignored:
// an operator who believes they named the node must find out at boot.
func TestNodeIDIsGone(t *testing.T) {
	// The control: the same section without the key loads, so the refusal
	// below is about node_id and nothing else.
	if _, err := Load(Options{Args: []string{"--config", writeINI(t, "[server]\nbind = :8080\n")},
		LookupEnv: envMap(nil)}); err != nil {
		t.Fatalf("control: %v", err)
	}

	_, err := Load(Options{Args: []string{"--config", writeINI(t, "[server]\nbind = :8080\nnode_id = gws-a\n")},
		LookupEnv: envMap(nil)})
	if err == nil || !strings.Contains(err.Error(), "unknown parameter") || !strings.Contains(err.Error(), "server.node_id") {
		t.Errorf("an INI with server.node_id: err = %v, want unknown parameter \"server.node_id\"", err)
	}
	_, err = Load(Options{Args: []string{"--server.node_id=gws-a"}, LookupEnv: envMap(nil)})
	if err == nil || !strings.Contains(err.Error(), "unknown parameter") {
		t.Errorf("--server.node_id: err = %v, want unknown parameter", err)
	}
	for _, p := range Registry(&Config{}) {
		if p.Key == "node_id" {
			t.Errorf("%s is still in the registry", p.Name())
		}
	}
}

// -------------------------------------------------------------------- validate

func TestValidationRejects(t *testing.T) {
	cases := []struct{ name, arg, want string }{
		{"log level", "--log.level=verbose", "not one of debug, info, warn, error"},
		{"high water high", "--ring.high_water=1.5", "must be in (0, 1]"},
		{"high water zero", "--ring.high_water=0", "must be in (0, 1]"},
		{"access sample", "--log.access_sample=2", "must be in [0, 1]"},
		{"listeners", "--server.listeners=0", "at least 1"},
		{"workers", "--sink.workers=0", "at least 1"},
		{"capacity", "--ring.capacity_items=0", "at least 1"},
		{"bad cidr", "--proxy.trusted=10.0.0.0/33", "proxy.trusted"},
		{"backoff order", "--sink.retry_backoff_min=5s", "exceeds sink.retry_backoff_max"},
		// SPEC §13.5: zero is refused where it is not "unlimited" but "already
		// expired" or "silently replaced by a default".
		{"zero op_timeout", "--sink.op_timeout=0s", "sink.op_timeout: must be positive"},
		{"zero drain_timeout", "--shutdown.drain_timeout=0s", "shutdown.drain_timeout: must be positive"},
		{"zero health_interval", "--sink.health_interval=0s", "sink.health_interval: must be positive"},
		{"zero health_timeout", "--sink.health_timeout=0s", "sink.health_timeout: must be positive"},
		{"zero boot_timeout", "--sink.boot_timeout=0s", "sink.boot_timeout: must be positive"},
		{"zero backoff min", "--sink.retry_backoff_min=0s", "sink.retry_backoff_min: must be positive"},
		{"zero backoff max", "--sink.retry_backoff_max=0s", "sink.retry_backoff_max: must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(Options{Args: []string{tc.arg}, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

// ... and zero stays legitimate where it means what it says: no pre-stop wait,
// no wait to fill a batch, no idle limit on the HTTP server.
func TestLegitimateZeroDurationsAreAccepted(t *testing.T) {
	for _, arg := range []string{
		"--shutdown.pre_stop_delay=0s",
		"--sink.batch_max_delay=0s",
		"--server.idle_timeout=0s",
	} {
		if _, err := Load(Options{Args: []string{arg}, LookupEnv: envMap(nil)}); err != nil {
			t.Errorf("%s: %v", arg, err)
		}
	}
}

func TestExposureWarnings(t *testing.T) {
	// The rate-limit exposure warning of go-webhook-fast-server is gone with
	// the limiter (SPEC §6): there is nothing to turn on.
	l := load(t, Options{Args: []string{"--server.bind=:8080"}})
	if hasWarning(l.Warnings, "rate") {
		t.Errorf("warnings = %v: there is no rate limiter to warn about", l.Warnings)
	}

	l = load(t, Options{Args: []string{"--admin.bind=0.0.0.0:9101"}})
	if !hasWarning(l.Warnings, "is not loopback") {
		t.Errorf("warnings = %v, want the exposed-admin warning", l.Warnings)
	}

	// Without the dump, a fatal sink loses the whole ring (SPEC §9.3).
	l = load(t, Options{Args: []string{"--emergency.enabled=false"}})
	if !hasWarning(l.Warnings, "emergency.enabled=false") {
		t.Errorf("warnings = %v, want the no-dump warning", l.Warnings)
	}
	if hasWarning(load(t, Options{}).Warnings, "emergency.enabled=false") {
		t.Error("the default configuration warns about a disabled dump")
	}
}

func TestIsLoopbackBind(t *testing.T) {
	cases := map[string]bool{
		":8080":            false,
		"0.0.0.0:8080":     false,
		"[::]:8080":        false,
		"127.0.0.1:9101":   true,
		"127.5.5.5:9101":   true,
		"[::1]:9101":       true,
		"localhost:9101":   true,
		"10.0.0.5:8080":    false,
		"example.com:8080": false,
	}
	for bind, want := range cases {
		if got := isLoopbackBind(bind); got != want {
			t.Errorf("isLoopbackBind(%q) = %v, want %v", bind, got, want)
		}
	}
}

func TestParsePrefixes(t *testing.T) {
	got, err := ParsePrefixes([]string{"10.0.0.0/8", " 192.168.1.5 ", "::1", "::ffff:10.1.1.1"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.0/8", "192.168.1.5/32", "::1/128", "10.1.1.1/32"}
	for i, w := range want {
		if got[i].String() != w {
			t.Errorf("prefix[%d] = %s, want %s", i, got[i], w)
		}
	}

	// Host bits are masked off, so 10.0.0.5/24 matches the /24 rather than
	// silently never matching.
	p, err := ParsePrefixes([]string{"10.0.0.5/24"})
	if err != nil {
		t.Fatal(err)
	}
	if p[0].String() != "10.0.0.0/24" {
		t.Errorf("got %s, want the masked prefix 10.0.0.0/24", p[0])
	}
}

func hasWarning(ws []string, sub string) bool {
	for _, w := range ws {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------- render

// The dump must be a valid config file: feeding it back must reproduce the same
// values, which is what makes it trustworthy as a boot record.
func TestDumpINIRoundTrips(t *testing.T) {
	first := load(t, Options{Args: []string{
		"--ring.mode=memory",
		"--sink.type=nats",
		"--sink.nats.urls=nats://10.0.0.5:4222,nats://10.0.0.6:4222",
		"--sink.nats.mode=core",
		"--loghub.beacon_interval=30",
		"--proxy.trusted=10.0.0.0/8,192.168.0.0/16",
		"--server.max_body_bytes=2MiB",
		"--ratelimit.enabled=false",
		"--headers.exclude=Cookie,Authorization",
		"--shutdown.pre_stop_delay=7s",
	}})

	var buf bytes.Buffer
	if err := first.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}

	path := writeINI(t, buf.String())
	second := load(t, Options{Args: []string{"--config", path}})

	a, b := first.Effective(), second.Effective()
	for k, av := range a {
		if bv := b[k]; av != bv {
			t.Errorf("%s did not round-trip: %q -> %q", k, av, bv)
		}
	}
}

func TestUsageCoversEveryParameter(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, p := range Registry(&Config{}) {
		if !strings.Contains(out, "["+p.Sec+"]") {
			t.Errorf("--help is missing section [%s]", p.Sec)
		}
		if !strings.Contains(out, p.Key) {
			t.Errorf("--help is missing key %s", p.Name())
		}
		if p.Desc == "" {
			t.Errorf("%s has no description; --help is generated from the registry", p.Name())
		}
	}
}

func TestEnvNameMapping(t *testing.T) {
	cases := map[string]string{
		"server.bind":             "WHS_SERVER_BIND",
		"sink.nats.urls":          "WHS_SINK_NATS_URLS",
		"shutdown.pre_stop_delay": "WHS_SHUTDOWN_PRE_STOP_DELAY",
		// The [loghub] keys read the names the LogHub skill fixes (SPEC §13.1).
		"loghub.eb_url":          "LOGHUB_EB_URL",
		"loghub.beacon_interval": "LOGHUB_BEACON_INTERVAL",
		"loghub.ctl_namespace":   "LOGHUB_CTL_NAMESPACE",
	}
	ix := newIndex(Registry(&Config{}))
	for name, want := range cases {
		p, err := ix.lookup(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := p.Env(); got != want {
			t.Errorf("%s env = %q, want %q", name, got, want)
		}
	}
}

// SPEC §8.7.1: the workers share one connection, so the default is the fast's
// 8 for NATS, and no value warns.
func TestWorkersDefaultAndNoWarning(t *testing.T) {
	l := load(t, Options{})
	if l.Config.Sink.Workers != 8 || hasWarning(l.Warnings, "sink.workers") {
		t.Errorf("default workers %d, warnings %v", l.Config.Sink.Workers, l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.workers=64"}})
	if l.Config.Sink.Workers != 64 || hasWarning(l.Warnings, "sink.workers") {
		t.Errorf("workers=64: %d, warnings %v", l.Config.Sink.Workers, l.Warnings)
	}
}

// The largest envelope is the ceiling of one line of the emergency dump (SPEC
// §9.2) and the Data of the largest NATS message (SPEC §8.7.5).
func TestMaxEventBytes(t *testing.T) {
	l := load(t, Options{})
	event := envelope.MaxEventBytes(l.Config.Server.MaxBodyBytes, l.Config.Server.MaxHeaderBytes,
		l.Config.Server.MaxURIBytes)
	if got := MaxEventBytes(l.Config); got != event || got != 8022700 {
		t.Errorf("MaxEventBytes = %d, want %d (8022700 with the defaults, SPEC §3.2)", got, event)
	}
}

// ------------------------------------------------- compatibility (SPEC §0)

// The five selectors still load, from every source, with the one value this
// server implements: a go-webhook-fast-server configuration for memory ->
// nats starts here unchanged.
func TestCompatibilitySelectorsAcceptTheirValue(t *testing.T) {
	fastINI := writeINI(t, strings.Join([]string{
		"[ratelimit]",
		"enabled = false",
		"[ring]",
		"mode = memory",
		"capacity_items = 4096",
		"[sink]",
		"type = nats",
		"workers = 8",
		"[sink.nats]",
		"urls = nats://10.0.0.5:4222",
		"stream = LOGHUB_WEBHOOK",
		"subject = loghub.{space}.{type}",
		"tls = false",
	}, "\n"))
	for name, o := range map[string]Options{
		"fast ini": {Args: []string{"--config", fastINI}},
		"flags": {Args: []string{"--ring.mode=memory", "--sink.type=nats", "--ratelimit.enabled=false",
			"--sink.nats.tls=false", "--sink.nats.subject=loghub.{space}.{type}"}},
		"env": {LookupEnv: envMap(map[string]string{
			"WHS_RING_MODE": "memory", "WHS_SINK_TYPE": "nats", "WHS_RATELIMIT_ENABLED": "false",
			"WHS_SINK_NATS_TLS": "false", "WHS_SINK_NATS_SUBJECT": "loghub.{space}.{type}"})},
		"case and spaces": {Args: []string{"--ring.mode= Memory ", "--sink.type=NATS"}},
	} {
		t.Run(name, func(t *testing.T) {
			c := load(t, o).Config
			if c.Ring.Mode != RingMemory || c.Sink.Type != SinkNATSType || c.RateLimit.Enabled ||
				c.SinkNATS.TLS || c.SinkNATS.Subject != NATSSubjectTemplate {
				t.Errorf("mode %q type %q ratelimit %v tls %v subject %q", c.Ring.Mode, c.Sink.Type,
					c.RateLimit.Enabled, c.SinkNATS.TLS, c.SinkNATS.Subject)
			}
		})
	}
}

// Any other value asks for a feature that is not here, and must not start
// without it.
func TestCompatibilitySelectorsRefuseRetiredFeatures(t *testing.T) {
	cases := []struct{ arg, want string }{
		{"--ring.mode=sqlite", `ring.mode: "sqlite" is not available`},
		{"--ring.mode=none", `ring.mode: "none" is not available`},
		{"--sink.type=redis", `sink.type: "redis" is not available`},
		{"--sink.type=sqlite", `sink.type: "sqlite" is not available`},
		{"--sink.type=mysql", `sink.type: "mysql" is not available`},
		{"--sink.type=postgres", `sink.type: "postgres" is not available`},
		{"--sink.type=clickhouse", `sink.type: "clickhouse" is not available`},
		{"--sink.type=none", `sink.type: "none" is not available`},
		{"--ratelimit.enabled=true", "has no rate limiter"},
		{"--sink.nats.tls=true", "has no TLS to NATS"},
		{"--sink.nats.subject=webhooks.in", `sink.nats.subject: "webhooks.in" is not available`},
		{"--sink.nats.subject=loghub.{space}.webhook", "is not available"},
	}
	for _, tc := range cases {
		_, err := Load(Options{Args: []string{tc.arg}, LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.arg, err, tc.want)
		}
	}
}

// Every other key of a retired feature is an unknown parameter, like
// server.node_id: a configuration that tunes a feature that does not exist
// cannot start in silence.
func TestRetiredKeysAreUnknown(t *testing.T) {
	retired := []string{
		"ring.max_inflight",
		"ring.sqlite.path", "ring.sqlite.synchronous",
		"ratelimit.rate", "ratelimit.burst", "ratelimit.exempt", "ratelimit.sweep_interval",
		"sink.redis.addrs", "sink.redis.stream",
		"sink.sqlite.path", "sink.sqlite.keep_files",
		"sink.mysql.addr", "sink.clickhouse.addrs",
		"sink.postgres.addr", "sink.postgres.table", "sink.postgres.pool_size",
		"sink.nats.tls_ca", "sink.nats.tls_insecure",
	}
	for _, name := range retired {
		_, err := Load(Options{Args: []string{"--" + name + "=x"}, LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), "unknown parameter") {
			t.Errorf("--%s: err = %v, want unknown parameter", name, err)
		}
		sec, key := name[:strings.LastIndexByte(name, '.')], name[strings.LastIndexByte(name, '.')+1:]
		_, err = Load(Options{Args: []string{"--config", writeINI(t, "["+sec+"]\n"+key+" = x\n")},
			LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), "unknown parameter") {
			t.Errorf("[%s] %s in an INI: err = %v, want unknown parameter", sec, key, err)
		}
	}
	for _, p := range Registry(&Config{}) {
		for _, name := range retired {
			if p.Name() == name {
				t.Errorf("%s is still in the registry", name)
			}
		}
	}
}

// The environment is not the INI: the loader reads only the WHS_* names the
// registry knows, so a retired knob left in the environment is inert, as any
// stray WHS_* variable always was (SPEC §0). The go-webhook-fast-server's own
// common.yml exports WHS_RATELIMIT_RATE/BURST with the limiter off, and that
// environment must start here. A SELECTOR in the environment still refuses a
// retired feature: that is the line between tuning something absent and asking
// for it.
func TestRetiredKnobsInTheEnvironmentAreInert(t *testing.T) {
	l := load(t, Options{LookupEnv: envMap(map[string]string{
		"WHS_RATELIMIT_ENABLED": "false",
		"WHS_RATELIMIT_RATE":    "100",
		"WHS_RATELIMIT_BURST":   "200",
		"WHS_RING_MAX_INFLIGHT": "3",
		"WHS_SINK_REDIS_ADDRS":  "redis:6379",
		// The PostgreSQL keys of go-webhook-pg-server's environment.
		"WHS_SINK_POSTGRES_ADDR":     "postgres:5432",
		"WHS_SINK_POSTGRES_PASSWORD": "webhook",
		"WHS_SINK_NATS_TLS_CA":       "/etc/ca.pem",
	})})
	if l.Config.RateLimit.Enabled {
		t.Error("ratelimit.enabled came out true")
	}
	for _, env := range []map[string]string{
		{"WHS_RING_MODE": "sqlite"},
		{"WHS_SINK_TYPE": "redis"},
		{"WHS_SINK_TYPE": "postgres"},
		{"WHS_RATELIMIT_ENABLED": "true"},
		{"WHS_SINK_NATS_TLS": "true"},
	} {
		if _, err := Load(Options{LookupEnv: envMap(env)}); err == nil {
			t.Errorf("%v was accepted; a selector asking for a retired feature must refuse", env)
		}
	}
}
