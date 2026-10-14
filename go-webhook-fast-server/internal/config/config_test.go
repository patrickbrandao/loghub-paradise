package config

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
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
		{"ratelimit.v4_mask", c.RateLimit.V4Mask, 24},
		{"ratelimit.v6_mask", c.RateLimit.V6Mask, 48},
		{"ring.mode", c.Ring.Mode, RingMemory},
		{"ring.capacity_items", c.Ring.CapacityItems, 65536},
		{"ring.max_bytes", c.Ring.MaxBytes, int64(512 << 20)},
		{"ring.high_water", c.Ring.HighWater, 0.80},
		{"ring.max_inflight", c.Ring.MaxInflight, 2048},
		{"ring.sqlite.synchronous", c.RingSQL.Synchronous, "FULL"},
		{"ring.sqlite.commit_batch", c.RingSQL.CommitBatch, 256},
		{"ring.sqlite.commit_max_delay", c.RingSQL.CommitMaxDelay, 5 * time.Millisecond},
		{"sink.type", c.Sink.Type, SinkRedisType},
		{"sink.workers", c.Sink.Workers, 8},
		{"sink.batch", c.Sink.Batch, 256},
		{"sink.health_interval", c.Sink.HealthInterval, 2 * time.Second},
		{"sink.fail_timeout", c.Sink.FailTimeout, 30 * time.Second},
		{"sink.boot_timeout", c.Sink.BootTimeout, 300 * time.Second},
		{"sink.redis.stream", c.SinkRedis.Stream, "loghub.{space}.{type}"},
		{"sink.redis.maxlen", c.SinkRedis.MaxLen, int64(0)},
		{"sink.sqlite.rotate", c.SinkSQL.Rotate, "none"},
		{"sink.sqlite.keep_files", c.SinkSQL.KeepFiles, 0},
		{"sink.postgres.addr", c.SinkPG.Addr, "127.0.0.1:5432"},
		{"sink.postgres.username", c.SinkPG.Username, "webhook"},
		{"sink.postgres.database", c.SinkPG.Database, "webhook"},
		{"sink.postgres.table", c.SinkPG.Table, "loghub.inbox"},
		{"sink.postgres.pool_size", c.SinkPG.PoolSize, 0},
		{"sink.postgres.tls", c.SinkPG.TLS, false},
		{"sink.mysql.addr", c.SinkMySQL.Addr, "127.0.0.1:3306"},
		{"sink.mysql.username", c.SinkMySQL.Username, "webhook"},
		{"sink.mysql.database", c.SinkMySQL.Database, "webhook"},
		{"sink.mysql.table", c.SinkMySQL.Table, "inbox"},
		{"sink.mysql.pool_size", c.SinkMySQL.PoolSize, 0},
		{"sink.mysql.tls", c.SinkMySQL.TLS, false},
		{"sink.clickhouse.username", c.SinkCH.Username, "webhook"},
		{"sink.clickhouse.database", c.SinkCH.Database, "webhook"},
		{"sink.clickhouse.table", c.SinkCH.Table, "inbox"},
		{"sink.clickhouse.pool_size", c.SinkCH.PoolSize, 0},
		{"sink.clickhouse.tls", c.SinkCH.TLS, false},
		{"sink.nats.stream", c.SinkNATS.Stream, "LOGHUB_WEBHOOK"},
		{"sink.nats.subject", c.SinkNATS.Subject, "loghub.{space}.{type}"},
		{"sink.nats.tls", c.SinkNATS.TLS, false},
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
	if want := []string{"127.0.0.1:6379"}; !eqList(c.SinkRedis.Addrs, want) {
		t.Errorf("sink.redis.addrs = %v, want %v", c.SinkRedis.Addrs, want)
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
	ini := writeINI(t, "[server]\nbind = :1111\n[ring]\nmode = sqlite\ncapacity_items = 111\n")

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
		"--ratelimit.enabled",
		"-log.level=debug",
	}})
	c := l.Config
	if c.Server.Bind != ":9999" {
		t.Errorf("bind = %q", c.Server.Bind)
	}
	if c.Ring.CapacityItems != 1024 {
		t.Errorf("capacity = %d", c.Ring.CapacityItems)
	}
	if !c.RateLimit.Enabled {
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
		"[sink.redis]",
		"password = p#ss;word",
		"stream = webhooks:in",
		"",
		"[headers]",
		`exclude = " Authorization , Cookie "`,
	}, "\n"))

	c := load(t, Options{Args: []string{"--config", ini}}).Config
	if got := c.SinkRedis.Password; got != "p#ss;word" {
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
	if want := filepath.Join("/srv/whs", "ring.db"); c.RingSQL.Path != want {
		t.Errorf("ring.sqlite.path = %q, want %q", c.RingSQL.Path, want)
	}
	if want := filepath.Join("/srv/whs", "events.db"); c.SinkSQL.Path != want {
		t.Errorf("sink.sqlite.path = %q, want %q", c.SinkSQL.Path, want)
	}
	if want := filepath.Join("/srv/whs", "emergency"); c.Emergency.Dir != want {
		t.Errorf("emergency.dir = %q, want %q", c.Emergency.Dir, want)
	}

	// An explicit path is never overwritten by the derivation.
	c = load(t, Options{Args: []string{"--ring.data_dir=/srv/whs", "--sink.sqlite.path=/mnt/big/e.db"}}).Config
	if c.SinkSQL.Path != "/mnt/big/e.db" {
		t.Errorf("explicit sink.sqlite.path was overwritten: %q", c.SinkSQL.Path)
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

// SPEC §3.3, §3.6, §3.7, §3.8: the LogHub defaults are not only the right
// strings, they pass the boot validation of their own destination untouched --
// a default that its own sink refuses would make the default deploy exit 1.
func TestLogHubDefaultsValidate(t *testing.T) {
	for _, sinkType := range []string{"redis", "postgres", "mysql", "nats", "clickhouse"} {
		l, err := Load(Options{Args: []string{"--sink.type=" + sinkType}, LookupEnv: envMap(nil)})
		if err != nil {
			t.Errorf("sink.type=%s with the defaults: %v", sinkType, err)
			continue
		}
		c := l.Config
		switch sinkType {
		case "redis":
			if c.SinkRedis.Stream != "loghub.{space}.{type}" {
				t.Errorf("redis stream = %q", c.SinkRedis.Stream)
			}
		case "postgres":
			if c.SinkPG.Table != "loghub.inbox" {
				t.Errorf("postgres table = %q", c.SinkPG.Table)
			}
		case "mysql":
			if c.SinkMySQL.Table != "inbox" {
				t.Errorf("mysql table = %q", c.SinkMySQL.Table)
			}
		case "nats":
			if c.SinkNATS.Stream != "LOGHUB_WEBHOOK" || c.SinkNATS.Subject != "loghub.{space}.{type}" {
				t.Errorf("nats stream %q subject %q", c.SinkNATS.Stream, c.SinkNATS.Subject)
			}
		case "clickhouse":
			if c.SinkCH.Table != "inbox" || strings.Join(c.SinkCH.Addrs, ",") != "127.0.0.1:9000" {
				t.Errorf("clickhouse table %q addrs %q", c.SinkCH.Table, c.SinkCH.Addrs)
			}
		}
	}
}

// The NATS subject is a template: {space} and {type} are expanded per envelope
// by the sink (SPEC §8.7), so the boot check must take the braces as ordinary
// token characters while still refusing what no expansion can make publishable.
func TestNATSSubjectTemplatePassesValidation(t *testing.T) {
	load := func(subject string) error {
		_, err := Load(Options{Args: []string{"--sink.type=nats", "--sink.nats.subject=" + subject},
			LookupEnv: envMap(nil)})
		return err
	}
	for _, ok := range []string{
		"loghub.{space}.{type}",  // the default, spelled out
		"loghub.default.webhook", // what it expands to for the default workspace
		"hooks.{space}.in",
	} {
		if err := load(ok); err != nil {
			t.Errorf("subject %q was refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"loghub.*.{type}", "loghub.{space}.>", "loghub..{type}", "loghub.{space} .x"} {
		if err := load(bad); err == nil || !strings.Contains(err.Error(), "not a publishable subject") {
			t.Errorf("subject %q: err = %v, want refused as not publishable", bad, err)
		}
	}
}

// -------------------------------------------------------------------- validate

func TestValidationRejects(t *testing.T) {
	cases := []struct{ name, arg, want string }{
		{"ring mode", "--ring.mode=disk", "not one of memory, sqlite, none"},
		{"sink type", "--sink.type=kafka", "not one of redis, sqlite, postgres, mysql, nats, clickhouse, none"},
		{"synchronous", "--ring.sqlite.synchronous=SOMETIMES", "not one of FULL, NORMAL, OFF"},
		{"rotate", "--sink.sqlite.rotate=weekly", "not one of none, hourly, daily"},
		{"log level", "--log.level=verbose", "not one of debug, info, warn, error"},
		{"high water high", "--ring.high_water=1.5", "must be in (0, 1]"},
		{"high water zero", "--ring.high_water=0", "must be in (0, 1]"},
		{"v4 mask", "--ratelimit.v4_mask=33", "must be in [0, 32]"},
		{"v6 mask", "--ratelimit.v6_mask=129", "must be in [0, 128]"},
		{"access sample", "--log.access_sample=2", "must be in [0, 1]"},
		{"listeners", "--server.listeners=0", "at least 1"},
		{"workers", "--sink.workers=0", "at least 1"},
		{"capacity", "--ring.capacity_items=0", "at least 1"},
		{"bad cidr", "--proxy.trusted=10.0.0.0/33", "proxy.trusted"},
		{"bad exempt", "--ratelimit.exempt=not-an-ip", "ratelimit.exempt"},
		{"empty redis addrs", "--sink.redis.addrs=", "at least one address"},
		{"backoff order", "--sink.retry_backoff_min=5s", "exceeds sink.retry_backoff_max"},
		// SPEC §13.5: zero is refused where it is not "unlimited" but "already
		// expired" or "silently replaced by a default".
		{"zero op_timeout", "--sink.op_timeout=0s", "sink.op_timeout: must be positive"},
		{"zero drain_timeout", "--shutdown.drain_timeout=0s", "shutdown.drain_timeout: must be positive"},
		{"zero health_interval", "--sink.health_interval=0s", "sink.health_interval: must be positive"},
		{"zero health_timeout", "--sink.health_timeout=0s", "sink.health_timeout: must be positive"},
		{"zero boot_timeout", "--sink.boot_timeout=0s", "sink.boot_timeout: must be positive"},
		{"zero backoff min", "--sink.retry_backoff_min=0s", "sink.retry_backoff_min: must be positive"},
		{"zero commit_max_delay", "--ring.sqlite.commit_max_delay=0s", "ring.sqlite.commit_max_delay: must be positive"},
		{"zero sweep_interval", "--ratelimit.sweep_interval=0s", "ratelimit.sweep_interval: must be positive"},
		{"zero backoff max", "--sink.retry_backoff_max=0s", "sink.retry_backoff_max: must be positive"},
		{"zero ring busy_timeout", "--ring.sqlite.busy_timeout=0s", "ring.sqlite.busy_timeout: must be positive"},
		{"zero sink busy_timeout", "--sink.sqlite.busy_timeout=0s", "sink.sqlite.busy_timeout: must be positive"},
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

// SPEC §10.1: the ring is a queue and events is an append-only destination.
// Pointing both at one file would have the drain deleting from the table it
// writes into, under a single write lock.
func TestRingAndSinkSQLiteSameFileRejected(t *testing.T) {
	dir := t.TempDir()
	same := filepath.Join(dir, "one.db")

	_, err := Load(Options{
		Args: []string{
			"--ring.mode=sqlite", "--sink.type=sqlite",
			"--ring.sqlite.path=" + same, "--sink.sqlite.path=" + same,
		},
		LookupEnv: envMap(nil),
	})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("err = %v, want a same-file rejection", err)
	}

	// Distinct files are allowed, but must warn about the double fsync.
	l := load(t, Options{Args: []string{
		"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=" + filepath.Join(dir, "ring.db"),
		"--sink.sqlite.path=" + filepath.Join(dir, "events.db"),
	}})
	if !hasWarning(l.Warnings, "TWO fsyncs") {
		t.Errorf("warnings = %v, want the double-fsync warning", l.Warnings)
	}
}

// Different relative spellings of one path must still be caught.
func TestRingAndSinkSQLiteSamePathDifferentSpelling(t *testing.T) {
	_, err := Load(Options{
		Args: []string{
			"--ring.mode=sqlite", "--sink.type=sqlite",
			"--ring.sqlite.path=./data/x.db",
			"--sink.sqlite.path=data/./x.db",
		},
		LookupEnv: envMap(nil),
	})
	if err == nil || !strings.Contains(err.Error(), "same file") {
		t.Fatalf("err = %v, want a same-file rejection", err)
	}
}

func TestSQLiteSinkForcesSingleWorker(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=sqlite", "--sink.workers=8"}})
	if l.Config.Sink.Workers != 1 {
		t.Errorf("workers = %d, want 1", l.Config.Sink.Workers)
	}
	if !hasWarning(l.Warnings, "forces sink.workers to 1") {
		t.Errorf("warnings = %v, want the forced-worker warning", l.Warnings)
	}
}

func TestExposureWarnings(t *testing.T) {
	// Rate limit off and reachable off-host: SPEC §6.1 requires a boot warning.
	l := load(t, Options{Args: []string{"--server.bind=:8080"}})
	if !hasWarning(l.Warnings, "no rate protection") {
		t.Errorf("warnings = %v, want the unprotected-exposure warning", l.Warnings)
	}

	// Loopback bind is not an exposure, so no warning.
	l = load(t, Options{Args: []string{"--server.bind=127.0.0.1:8080"}})
	if hasWarning(l.Warnings, "no rate protection") {
		t.Errorf("loopback bind should not warn: %v", l.Warnings)
	}

	// Benchmark modes must announce themselves loudly.
	l = load(t, Options{Args: []string{"--ring.mode=none", "--sink.type=none"}})
	if !hasWarning(l.Warnings, "DISCARDED") {
		t.Errorf("warnings = %v, want the discard warning", l.Warnings)
	}

	// Silent trimming defeats the backpressure design.
	l = load(t, Options{Args: []string{"--sink.redis.maxlen=1000000"}})
	if !hasWarning(l.Warnings, "SILENTLY") {
		t.Errorf("warnings = %v, want the maxlen warning", l.Warnings)
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

// ---------------------------------------------------------------- reload scope

// SPEC §13.3: SIGHUP reloads only what is safe to swap at runtime.
func TestReloadableSetMatchesSpec(t *testing.T) {
	want := map[string]bool{
		"log.level": true, "log.access": true, "log.access_sample": true,
		"proxy.trusted": true, "proxy.max_xff_entries": true,
		"headers.exclude":        true,
		"sink.sqlite.keep_files": true,
	}
	for _, p := range Registry(&Config{}) {
		if strings.HasPrefix(p.Name(), "ratelimit.") {
			want[p.Name()] = true
		}
	}

	got := map[string]bool{}
	for _, p := range Registry(&Config{}) {
		if p.Reload {
			got[p.Name()] = true
		}
	}
	for n := range want {
		if !got[n] {
			t.Errorf("%s should be reloadable", n)
		}
	}
	for n := range got {
		if !want[n] {
			t.Errorf("%s is marked reloadable but is not in the spec's reload set", n)
		}
	}
}

// ---------------------------------------------------------------------- render

// The dump must be a valid config file: feeding it back must reproduce the same
// values, which is what makes it trustworthy as a boot record.
func TestDumpINIRoundTrips(t *testing.T) {
	first := load(t, Options{Args: []string{
		"--ring.mode=sqlite",
		"--sink.type=redis",
		"--sink.redis.addrs=10.0.0.5:6379,10.0.0.6:6379",
		"--server.max_body_bytes=2MiB",
		"--ratelimit.enabled=true",
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

func TestDumpRedactsPassword(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.redis.password=hunter2"}})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "hunter2") {
		t.Error("the effective-config dump leaked the password")
	}
	if !strings.Contains(buf.String(), "***") {
		t.Error("the password should be redacted, not omitted")
	}
	if l.Config.SinkRedis.Password != "hunter2" {
		t.Error("redaction must not alter the loaded value")
	}
	if l.Effective()["sink.redis.password"] != "***" {
		t.Error("Effective() must redact too; it goes to the boot log")
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

// --help and the --dump-config header name this program: the fast was born as
// a copy of go-webhook-server and kept introducing itself as it, and an
// operator comparing two dumps could not tell which binary wrote which.
func TestHelpAndDumpNameThisProgram(t *testing.T) {
	const name = "go-webhook-fast-server"
	var help bytes.Buffer
	if err := Usage(&help); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(help.String(), name+" -- ") || !strings.Contains(help.String(), "\n  "+name+" [--config FILE]") {
		t.Errorf("--help does not introduce itself as %s:\n%s", name, help.String()[:200])
	}
	var dump bytes.Buffer
	if err := load(t, Options{}).DumpINI(&dump); err != nil {
		t.Fatal(err)
	}
	if first, _, _ := strings.Cut(dump.String(), "\n"); first != "# effective configuration of "+name {
		t.Errorf("--dump-config header = %q", first)
	}
}

func TestEnvNameMapping(t *testing.T) {
	cases := map[string]string{
		"server.bind":                  "WHS_SERVER_BIND",
		"sink.redis.addrs":             "WHS_SINK_REDIS_ADDRS",
		"ring.sqlite.commit_max_delay": "WHS_RING_SQLITE_COMMIT_MAX_DELAY",
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

// ---------------------------------------------------------------- postgres

// SPEC §8.5 and §10.1: the [sink.postgres] rules that are errors.
func TestPostgresValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"uppercase table", []string{"--sink.postgres.table=Events"}, "not a plain identifier"},
		{"sql in table", []string{"--sink.postgres.table=events;drop"}, "not a plain identifier"},
		{"three-part table", []string{"--sink.postgres.table=a.b.c"}, "not a plain identifier"},
		{"empty table", []string{"--sink.postgres.table="}, "not a plain identifier"},
		{"empty addr", []string{"--sink.postgres.addr="}, "cannot be empty"},
		{"empty database", []string{"--sink.postgres.database="}, "cannot be empty"},
		{"empty username", []string{"--sink.postgres.username="}, "cannot be empty"},
		{"tls_ca without tls", []string{"--sink.postgres.tls_ca=/etc/ca.pem"}, "tls_ca is set but"},
		{"tls on a socket", []string{"--sink.postgres.addr=/var/run/postgresql", "--sink.postgres.tls=true"}, "unix socket"},
		{"negative pool", []string{"--sink.postgres.pool_size=-1"}, "cannot be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--sink.type=postgres"}, tc.args...)
			_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// The same values are fine for every other destination: the rules only
	// bite when postgres is the sink.
	if _, err := Load(Options{Args: []string{"--sink.type=sqlite", "--sink.postgres.table=Events"},
		LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("postgres rules applied to sink.type=sqlite: %v", err)
	}
}

// SPEC §8.5.3: the registry default of 8 workers is replaced by 4 ONLY when
// nobody wrote it. An explicit value is the operator's, from any source.
func TestPostgresLowersDefaultWorkersOnly(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=postgres"}})
	if l.Config.Sink.Workers != 4 {
		t.Errorf("default workers = %d, want 4", l.Config.Sink.Workers)
	}
	if !hasWarning(l.Warnings, "lowers the default sink.workers from 8 to 4") {
		t.Errorf("warnings = %v, want the lowered-default warning", l.Warnings)
	}
	if l.Config.SinkPG.PoolSize != 5 {
		t.Errorf("pool_size = %d, want workers+1 = 5", l.Config.SinkPG.PoolSize)
	}

	for _, src := range []Options{
		{Args: []string{"--sink.type=postgres", "--sink.workers=8"}},
		{Args: []string{"--sink.type=postgres"}, LookupEnv: envMap(map[string]string{"WHS_SINK_WORKERS": "8"})},
		{Args: []string{"--sink.type=postgres", "--config=" + writeINI(t, "[sink]\nworkers = 8\n")}},
	} {
		l := load(t, src)
		if l.Config.Sink.Workers != 8 {
			t.Errorf("explicit workers = %d, want 8 kept", l.Config.Sink.Workers)
		}
		if hasWarning(l.Warnings, "lowers the default") {
			t.Errorf("an explicit value must not be lowered: %v", l.Warnings)
		}
		if l.Config.SinkPG.PoolSize != 9 {
			t.Errorf("pool_size = %d, want 9", l.Config.SinkPG.PoolSize)
		}
	}

	// Writing the default value explicitly is still explicit.
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--sink.workers=32"}})
	if l.Config.Sink.Workers != 32 || !hasWarning(l.Warnings, "sink.workers=32 with sink.type=postgres") {
		t.Errorf("workers=32: got %d, warnings %v", l.Config.Sink.Workers, l.Warnings)
	}

	// Other destinations keep the registry default untouched.
	if w := load(t, Options{Args: []string{"--sink.type=redis"}}).Config.Sink.Workers; w != 8 {
		t.Errorf("redis workers = %d, want the registry default 8", w)
	}
}

func TestPostgresPoolSizeWarnings(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=postgres", "--sink.postgres.pool_size=2"}})
	if l.Config.SinkPG.PoolSize != 2 {
		t.Errorf("explicit pool_size = %d, want 2 kept", l.Config.SinkPG.PoolSize)
	}
	if !hasWarning(l.Warnings, "pool_size=2 is below sink.workers=4") {
		t.Errorf("warnings = %v, want the small-pool warning", l.Warnings)
	}

	// SPEC §8.5.3: the health check has its own connection, outside the
	// pool, so a pool of exactly sink.workers is legitimate.
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--sink.postgres.pool_size=4"}})
	if hasWarning(l.Warnings, "pool_size=4 is below") {
		t.Errorf("pool_size = sink.workers must not warn: %v", l.Warnings)
	}

	// ring=none: the writers are the handlers, bounded by max_inflight.
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--ring.mode=none"}})
	if !hasWarning(l.Warnings, "ring.max_inflight=2048 above sink.postgres.pool_size=5") {
		t.Errorf("warnings = %v, want the hidden-queue warning", l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--ring.mode=none",
		"--ring.max_inflight=16", "--sink.postgres.pool_size=16"}})
	if hasWarning(l.Warnings, "above sink.postgres.pool_size") {
		t.Errorf("max_inflight within the pool must not warn: %v", l.Warnings)
	}
}

// SPEC §8.5.4: the byte ceiling warning uses the SMALLER of the two worst
// cases, so it does not fire on the defaults and mean nothing.
func TestPostgresByteCeilingWarning(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=postgres"}})
	if hasWarning(l.Warnings, "ceiling of a single protocol parameter") {
		t.Errorf("the defaults are bounded by ring.max_bytes and must not warn: %v", l.Warnings)
	}

	l = load(t, Options{Args: []string{"--sink.type=postgres",
		"--server.max_body_bytes=8MiB", "--sink.batch=256", "--ring.max_bytes=4GiB"}})
	if !hasWarning(l.Warnings, "ceiling of a single protocol parameter") {
		t.Errorf("2 GiB per batch must warn: %v", l.Warnings)
	}

	// With ring=none a batch is one item, so only an absurd body warns.
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--ring.mode=none",
		"--server.max_body_bytes=64MiB", "--ring.max_bytes=4GiB"}})
	if hasWarning(l.Warnings, "ceiling of a single protocol parameter") {
		t.Errorf("ring=none carries one body per write and must not warn: %v", l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.type=postgres", "--ring.mode=none",
		"--server.max_body_bytes=2GiB"}})
	if !hasWarning(l.Warnings, "ceiling of a single protocol parameter") {
		t.Errorf("a 2 GiB body must warn even with ring=none: %v", l.Warnings)
	}
}

func TestDumpRedactsPostgresPassword(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=postgres", "--sink.postgres.password=hunter2"}})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "hunter2") {
		t.Error("the effective-config dump leaked the postgres password")
	}
	if l.Effective()["sink.postgres.password"] != "***" {
		t.Error("Effective() must redact the postgres password; it goes to the boot log")
	}
	if l.Config.SinkPG.Password != "hunter2" {
		t.Error("redaction must not alter the loaded value")
	}
}

// ------------------------------------------------------------ sink=mysql

func TestMySQLValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"sql in table", []string{"--sink.mysql.table=events;drop"}, "not a plain identifier"},
		{"three-part table", []string{"--sink.mysql.table=a.b.c"}, "not a plain identifier"},
		{"empty table", []string{"--sink.mysql.table="}, "not a plain identifier"},
		{"empty addr", []string{"--sink.mysql.addr="}, "cannot be empty"},
		{"empty database", []string{"--sink.mysql.database="}, "cannot be empty"},
		{"empty username", []string{"--sink.mysql.username="}, "cannot be empty"},
		{"tls_ca without tls", []string{"--sink.mysql.tls_ca=/etc/ca.pem"}, "tls_ca is set but"},
		{"tls on a socket", []string{"--sink.mysql.addr=/var/run/mysqld/mysqld.sock", "--sink.mysql.tls=true"}, "unix socket"},
		{"negative pool", []string{"--sink.mysql.pool_size=-1"}, "cannot be negative"},
		{"batch beyond the placeholder ceiling", []string{"--sink.batch=5001"}, "exceeds 5000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--sink.type=mysql"}, tc.args...)
			_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// MySQL keeps the case of a table name (SPEC §3.7), and the rules only
	// bite when mysql is the sink.
	if _, err := Load(Options{Args: []string{"--sink.type=mysql", "--sink.mysql.table=Hooks.Events"},
		LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("a mixed-case table was refused: %v", err)
	}
	if _, err := Load(Options{Args: []string{"--sink.type=sqlite", "--sink.mysql.table=a;b", "--sink.batch=6000"},
		LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("mysql rules applied to sink.type=sqlite: %v", err)
	}
}

func TestMySQLLowersDefaultWorkersOnly(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=mysql"}})
	if l.Config.Sink.Workers != 4 {
		t.Errorf("default workers = %d, want 4", l.Config.Sink.Workers)
	}
	if !hasWarning(l.Warnings, "sink.type=mysql lowers the default sink.workers from 8 to 4") {
		t.Errorf("warnings = %v, want the lowered-default warning", l.Warnings)
	}
	if l.Config.SinkMySQL.PoolSize != 5 {
		t.Errorf("pool_size = %d, want workers+1 = 5", l.Config.SinkMySQL.PoolSize)
	}
	l = load(t, Options{Args: []string{"--sink.type=mysql", "--sink.workers=8"}})
	if l.Config.Sink.Workers != 8 || hasWarning(l.Warnings, "lowers the default") || l.Config.SinkMySQL.PoolSize != 9 {
		t.Errorf("explicit workers: got %d, pool %d, warnings %v", l.Config.Sink.Workers, l.Config.SinkMySQL.PoolSize, l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.type=mysql", "--ring.mode=none", "--sink.mysql.pool_size=4", "--ring.max_inflight=64"}})
	if !hasWarning(l.Warnings, "above sink.mysql.pool_size=4") {
		t.Errorf("warnings = %v, want the hidden-queue warning", l.Warnings)
	}
}

// ------------------------------------------------------- sink=clickhouse

func TestClickHouseValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"sql in table", []string{"--sink.clickhouse.table=events;drop"}, "not a plain identifier"},
		{"three-part table", []string{"--sink.clickhouse.table=a.b.c"}, "not a plain identifier"},
		{"leading digit", []string{"--sink.clickhouse.table=1inbox"}, "not a plain identifier"},
		{"empty table", []string{"--sink.clickhouse.table="}, "not a plain identifier"},
		{"no addrs", []string{"--sink.clickhouse.addrs="}, "at least one address"},
		{"addr without port", []string{"--sink.clickhouse.addrs=clickhouse"}, "not host:port"},
		{"empty database", []string{"--sink.clickhouse.database="}, "cannot be empty"},
		{"empty username", []string{"--sink.clickhouse.username="}, "cannot be empty"},
		{"tls_ca without tls", []string{"--sink.clickhouse.tls_ca=/etc/ca.pem"}, "tls_ca is set but"},
		{"negative pool", []string{"--sink.clickhouse.pool_size=-1"}, "cannot be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--sink.type=clickhouse"}, tc.args...)
			_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	for _, ok := range [][]string{
		{"--sink.clickhouse.table=Hooks.Events"}, // case is kept, as in MySQL
		{"--sink.clickhouse.addrs=a:9000,b:9000", "--sink.clickhouse.tls=true", "--sink.clickhouse.tls_ca=/ca.pem"},
		{"--sink.batch=20000"}, // no placeholder ceiling in the native protocol
	} {
		if _, err := Load(Options{Args: append([]string{"--sink.type=clickhouse"}, ok...),
			LookupEnv: envMap(nil)}); err != nil {
			t.Errorf("%v was refused: %v", ok, err)
		}
	}
	if _, err := Load(Options{Args: []string{"--sink.type=sqlite", "--sink.clickhouse.table=a;b"},
		LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("clickhouse rules applied to sink.type=sqlite: %v", err)
	}
}

func TestClickHouseLowersDefaultWorkersOnly(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=clickhouse"}})
	if l.Config.Sink.Workers != 4 {
		t.Errorf("default workers = %d, want 4", l.Config.Sink.Workers)
	}
	if !hasWarning(l.Warnings, "sink.type=clickhouse lowers the default sink.workers from 8 to 4") {
		t.Errorf("warnings = %v, want the lowered-default warning", l.Warnings)
	}
	if l.Config.SinkCH.PoolSize != 5 {
		t.Errorf("pool_size = %d, want workers+1 = 5", l.Config.SinkCH.PoolSize)
	}
	l = load(t, Options{Args: []string{"--sink.type=clickhouse", "--sink.workers=8"}})
	if l.Config.Sink.Workers != 8 || hasWarning(l.Warnings, "lowers the default") || l.Config.SinkCH.PoolSize != 9 {
		t.Errorf("explicit workers: got %d, pool %d, warnings %v", l.Config.Sink.Workers, l.Config.SinkCH.PoolSize, l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.type=clickhouse", "--sink.workers=20"}})
	if !hasWarning(l.Warnings, "sink.workers=20 with sink.type=clickhouse") {
		t.Errorf("warnings = %v, want the many-workers warning", l.Warnings)
	}
	l = load(t, Options{Args: []string{"--sink.type=clickhouse", "--ring.mode=none", "--sink.clickhouse.pool_size=4", "--ring.max_inflight=64"}})
	if !hasWarning(l.Warnings, "above sink.clickhouse.pool_size=4") {
		t.Errorf("warnings = %v, want the hidden-queue warning", l.Warnings)
	}
}

// SPEC §8.8.5: the worst batch the ClickHouse boot compares with the memory
// limit -- one envelope with ring=none, otherwise sink.batch envelopes capped
// by ring.max_bytes.
func TestWorstBatchBytes(t *testing.T) {
	l := load(t, Options{Args: []string{"--ring.mode=none"}})
	event := envelope.MaxEventBytes(l.Config.Server.MaxBodyBytes, l.Config.Server.MaxHeaderBytes,
		l.Config.Server.MaxURIBytes)
	if got := WorstBatchBytes(l.Config); got != event {
		t.Errorf("ring=none: %d, want one envelope %d", got, event)
	}
	l = load(t, Options{Args: []string{"--ring.mode=memory", "--ring.max_bytes=64MiB"}})
	if got := WorstBatchBytes(l.Config); got != 64<<20 {
		t.Errorf("ring capped: %d, want ring.max_bytes", got)
	}
	l = load(t, Options{Args: []string{"--ring.mode=memory", "--sink.batch=2"}})
	if got := WorstBatchBytes(l.Config); got != 2*event {
		t.Errorf("batch capped: %d, want 2 envelopes %d", got, 2*event)
	}
}

// ------------------------------------------------------------- sink=nats

func TestNATSValidationRejects(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no urls", []string{"--sink.nats.urls="}, "at least one URL"},
		{"stream with a space", []string{"--sink.nats.stream=my stream"}, "not a valid stream name"},
		{"stream with a dot", []string{"--sink.nats.stream=a.b"}, "not a valid stream name"},
		{"stream with a wildcard", []string{"--sink.nats.stream=EV*"}, "not a valid stream name"},
		{"empty subject", []string{"--sink.nats.subject="}, "cannot be empty"},
		{"subject with a wildcard", []string{"--sink.nats.subject=webhooks.*"}, "not a publishable subject"},
		{"subject with a full wildcard", []string{"--sink.nats.subject=webhooks.>"}, "not a publishable subject"},
		{"subject with an empty token", []string{"--sink.nats.subject=a..b"}, "not a publishable subject"},
		{"subject with a space", []string{"--sink.nats.subject=a b"}, "not a publishable subject"},
		{"token and password", []string{"--sink.nats.token=t", "--sink.nats.username=u"}, "only one of"},
		{"credentials and token", []string{"--sink.nats.credentials=/x.creds", "--sink.nats.token=t"}, "only one of"},
		{"password without username", []string{"--sink.nats.password=p"}, "without sink.nats.username"},
		{"tls_ca without tls", []string{"--sink.nats.tls_ca=/etc/ca.pem"}, "tls_ca is set but"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--sink.type=nats"}, tc.args...)
			_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	for _, ok := range [][]string{
		{"--sink.nats.subject=a.b.c", "--sink.nats.username=u", "--sink.nats.password=p"},
		{"--sink.nats.credentials=/x.creds"},
		{"--sink.nats.token=t", "--sink.nats.tls=true", "--sink.nats.tls_ca=/ca.pem"},
		{"--sink.nats.urls=nats://a:4222,nats://b:4222", "--sink.nats.stream=EVENTS-2"},
	} {
		if _, err := Load(Options{Args: append([]string{"--sink.type=nats"}, ok...),
			LookupEnv: envMap(nil)}); err != nil {
			t.Errorf("%v was refused: %v", ok, err)
		}
	}
	// The default workers stay at 8: one multiplexed connection (SPEC §8.7.1).
	if w := load(t, Options{Args: []string{"--sink.type=nats"}}).Config.Sink.Workers; w != 8 {
		t.Errorf("nats workers = %d, want the registry default 8", w)
	}
}

// SPEC §8.7.9: every URL of sink.nats.urls is checked here, and the message
// quotes none of it. A password with an unescaped %, / or # failed inside
// nats.Connect as a network error, with the password in every log line until
// sink.boot_timeout; and net/url's own error quotes a piece of the input
// (`invalid port ":Sup3r" after host` is the password up to the slash). A
// URL with userinfo next to an explicit credential is refused: nats.go would
// use the URL's and ignore the other, silently (found in
// go-webhook-nats-server).
func TestNATSURLsAreCheckedWithoutEcho(t *testing.T) {
	const secret = "Sup3rS3cret"
	cases := []struct {
		name, url string
		extra     []string
		want      string
	}{
		{"slash in the password", "nats://bob:" + secret + "/x@h:4222", nil, "not a valid URL"},
		{"hash in the password", "nats://bob:" + secret + "#x@h:4222", nil, "not a valid URL"},
		{"bad escape in the password", "nats://bob:" + secret + "%zz@h:4222", nil, "percent-escape"},
		// An '@' after the '/' names the escape to use (natsurl.go).
		{"a password that parses as a port and a path", "nats://bob:1234/" + secret + "@h:4222", nil, "%2F"},
		{"a token that parses as a host and a path", "nats://" + secret + "/x@h:4222", nil, "%2F"},
		{"query", "nats://h:4222/?" + secret, nil, "query"},
		{"no host", "nats://:4222", nil, "no host"},
		{"userinfo and username", "nats://bob:" + secret + "@h:4222", []string{"--sink.nats.username=u"}, "keep one"},
		{"userinfo and token", "nats://" + secret + "@h:4222", []string{"--sink.nats.token=t"}, "keep one"},
		{"userinfo and credentials", "nats://" + secret + "@h:4222", []string{"--sink.nats.credentials=/x.creds"}, "keep one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--sink.type=nats", "--sink.nats.urls=" + tc.url}, tc.extra...)
			_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
			if err == nil {
				t.Fatalf("%v was accepted", args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "Sup3r") {
				t.Errorf("the error quotes the secret: %q", err)
			}
		})
	}
	for _, ok := range []string{
		"nats://bob:" + url.QueryEscape(secret+"/#%") + "@h:4222",
		"nats://" + secret + "@h:4222",
		"tls://h:4222,nats://h2:4222/",
		"h:4222",
	} {
		if _, err := Load(Options{Args: []string{"--sink.type=nats", "--sink.nats.urls=" + ok},
			LookupEnv: envMap(nil)}); err != nil {
			t.Errorf("%q was refused: %v", ok, err)
		}
	}
}

// SPEC §8.7.1: a URL's user part may be a password or a token, so the dump and
// the boot log show it masked, and the host stays readable.
func TestDumpRedactsTheUserinfoOfNATSURLs(t *testing.T) {
	l := load(t, Options{Args: []string{"--sink.type=nats",
		"--sink.nats.urls=nats://bob:hunter6@h1:4222,nats://hunter7@h2:4222,h3:4222"}})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"hunter6", "hunter7"} {
		if strings.Contains(buf.String(), secret) || strings.Contains(l.Effective()["sink.nats.urls"], secret) {
			t.Errorf("the dump or the boot log leaked %q", secret)
		}
	}
	if got, want := l.Effective()["sink.nats.urls"], "nats://bob:***@h1:4222,nats://***@h2:4222,h3:4222"; got != want {
		t.Errorf("Effective()[sink.nats.urls] = %q, want %q", got, want)
	}
	if !strings.Contains(buf.String(), "secrets are shown as ***") {
		t.Error("the dump does not warn that it cannot be read back as is")
	}
	if l.Config.SinkNATS.URLs[0] != "nats://bob:hunter6@h1:4222" {
		t.Error("redaction must not alter the loaded values")
	}
}

func TestDumpRedactsMySQLNATSAndClickHouseSecrets(t *testing.T) {
	l := load(t, Options{Args: []string{
		"--sink.type=mysql", "--sink.mysql.password=hunter2",
		"--sink.nats.password=hunter3", "--sink.nats.token=hunter4",
		"--sink.clickhouse.password=hunter5",
	}})
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"hunter2", "hunter3", "hunter4", "hunter5"} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("the effective-config dump leaked %q", secret)
		}
	}
	eff := l.Effective()
	for _, k := range []string{"sink.mysql.password", "sink.nats.password", "sink.nats.token", "sink.clickhouse.password"} {
		if eff[k] != "***" {
			t.Errorf("Effective()[%s] = %q, want ***", k, eff[k])
		}
	}
	if l.Config.SinkMySQL.Password != "hunter2" || l.Config.SinkNATS.Token != "hunter4" {
		t.Error("redaction must not alter the loaded values")
	}
}

// SPEC §13: the INI and flag parsers name the line or the flag, never the
// value: "password: x" in an INI, "sink.nats.password=x" without its dashes, a
// flag left without its value that swallows the next one, or a value typed
// after a space past the '=' all printed the secret (the review of
// go-webhook-nats-server and of this port, 2026-10-11) -- and so did a value
// starting with a dash, or split by the shell at a space (the review of the
// port back into go-webhook-nats-server, SPEC §13.2).
func TestParserErrorsDoNotEchoValues(t *testing.T) {
	dir := t.TempDir()
	for _, body := range []string{"[sink.nats]\npassword: hunter2\n", "[sink.nats hunter2\n",
		"[sink.nats]\nurls nats://bob:hunter2@h:4222\n"} {
		ini := filepath.Join(dir, "c.ini")
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(Options{Args: []string{"--config=" + ini}, LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), "line ") || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("INI %q: err = %v, want the line number and not the value", body, err)
		}
	}
	for _, args := range [][]string{
		{"sink.nats.password=hunter2"},
		{"--sink.workers", "--sink.nats.password=hunter2"},
		{"--sink.nats.subject", "--sink.nats.token=hunter2"},
		{"--sink.op_timeout", "--sink.nats.urls=nats://u:hunter2@h:4222"},
		{"--sink.workers", "--no.such.key=hunter2"},
		{"--sink.nats.password=", "hunter2"},
		{"--sink.nats.password", "=", "hunter2"},
		{"--config", "--sink.nats.password=hunter2"},
		{"--config=", "hunter2"},
		{"--sink.workers", "-sink.nats.password=hunter2"},
		// The review of the port back into go-webhook-nats-server: a value
		// starting with "--" in the separated form was named as "the flag
		// --hunter2"; a dash-value after an empty '=' as an unknown
		// parameter; the tail of a value the shell split at a space as a
		// stray word.
		{"--sink.nats.password", "--hunter2"},
		{"--sink.nats.password=", "-hunter2"},
		{"--sink.nats.password", "=", "-hunter2"},
		{"--sink.nats.password=", "--hunter2=x"},
		{"--sink.nats.password=foo", "hunter2"},
		{"--sink.nats.password", "foo", "hunter2"},
	} {
		_, err := Load(Options{Args: args, LookupEnv: envMap(nil)})
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: err = %v, want a refusal that does not echo the secret", args, err)
		}
	}
	// Controls: well-formed lines load, and a stray word is still named.
	ini := filepath.Join(dir, "ok.ini")
	if err := os.WriteFile(ini, []byte("[sink.nats]\nusername = u\npassword = hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(Options{Args: []string{"--config=" + ini}, LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("a well-formed INI was refused: %v", err)
	}
	if _, err := Load(Options{Args: []string{"extra"}, LookupEnv: envMap(nil)}); err == nil ||
		!strings.Contains(err.Error(), `"extra"`) {
		t.Errorf("a stray word: %v, want it named", err)
	}
	// More controls: a value of the separated form is still taken, a
	// negative number is a value and not a flag, a switch is a flag, a typo
	// after a real value is still named, and each refusal says what went
	// wrong -- the space after the '=', or a value with a space unquoted.
	l, err := Load(Options{Args: []string{"--sink.workers", "4", "--sink.nats.stream", "LOGHUB_X"},
		LookupEnv: envMap(nil)})
	if err != nil || l.Config.Sink.Workers != 4 || l.Config.SinkNATS.Stream != "LOGHUB_X" {
		t.Errorf("separated values: %v", err)
	}
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.workers", "-1"}, "must be at least 1"},
		{[]string{"--sink.nats.stream", "--help"}, "the next argument is the flag --help"},
		{[]string{"--sink.nats.password=", "--server.http2", "extra"}, `"extra"`},
		{[]string{"--sink.nats.password=", "hunter2"}, "right after the '='"},
		{[]string{"--sink.nats.password", "=", "hunter2"}, "right after the '='"},
		{[]string{"--sink.nats.password=foo", "hunter2"}, "goes in quotes"},
		// After a value, a flag outside every section may be the tail of that
		// value (knownSectionOf); a typo inside one is still named.
		{[]string{"--sink.workers=4", "--typo.key=1"}, "unexpected argument after --sink.workers"},
		{[]string{"--sink.workers=4", "--sink.wrkers=1"}, `"sink.wrkers"`},
		{[]string{"--sink.nats.password", "--sink.workers=4"}, "the flag --sink.workers"},
	} {
		if _, err := Load(Options{Args: c.args, LookupEnv: envMap(nil)}); err == nil ||
			!strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: err = %v, want one mentioning %q", c.args, err, c.want)
		}
	}
}
