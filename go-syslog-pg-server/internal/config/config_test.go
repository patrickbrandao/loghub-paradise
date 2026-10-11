package config

import (
	"bytes"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
)

// testWorkDir is the fake working directory every test load resolves relative
// paths against. It never exists: derive only joins paths, it never touches
// the disk.
const testWorkDir = "/work"

func load(t *testing.T, args []string, env map[string]string) (*Loaded, error) {
	t.Helper()
	return Load(Options{
		Args: args,
		LookupEnv: func(k string) (string, bool) {
			v, ok := env[k]
			return v, ok
		},
		WorkDir: testWorkDir,
	})
}

func mustLoad(t *testing.T, args []string, env map[string]string) *Loaded {
	t.Helper()
	l, err := load(t, args, env)
	if err != nil {
		t.Fatalf("Load(%v, %v): %v", args, env, err)
	}
	return l
}

func mustFail(t *testing.T, args []string, env map[string]string, want string) error {
	t.Helper()
	_, err := load(t, args, env)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Load(%v, %v): err = %v, want one mentioning %q", args, env, err, want)
	}
	return err
}

func hasWarning(l *Loaded, sub string) bool {
	for _, w := range l.Warnings {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// The defaults of SPEC §16.5, one assertion per value that differs from the
// standard server or from the go-syslog-server this one was slimmed from, so a
// ported default that slipped through fails here.
func TestDefaults(t *testing.T) {
	c := mustLoad(t, nil, nil).Config
	checks := []struct {
		name      string
		got, want any
	}{
		{"server.data_dir", c.Server.DataDir, "/work/data"},
		{"listen.udp", c.Listen.UDP, ":1514"},
		{"listen.tcp", c.Listen.TCP, ":1514"},
		// unix is off by default: creating /dev/log uninvited would take over
		// the host's own syslog.
		{"listen.unix", c.Listen.Unix, ""},
		{"listen.unix_mode", c.Listen.UnixMode, 0o666},
		{"listen.udp_read_buffer", c.Listen.UDPReadBuffer, int64(8 << 20)},
		{"tcp.framing", c.TCP.Framing, FramingAuto},
		{"tcp.idle_timeout", c.TCP.IdleTimeout, 120 * time.Second},
		{"proxy.protocol", c.Proxy.Protocol, ProxyOff},
		{"proxy.trusted", c.Proxy.Trusted, []netip.Prefix{
			netip.MustParsePrefix("127.0.0.1/32"), netip.MustParsePrefix("::1/128")}},
		{"proxy.header_timeout", c.Proxy.HeaderTimeout, 5 * time.Second},
		{"syslog.default_priority", c.Syslog.DefaultPriority, 13},
		{"syslog.rfc3164_timezone", c.Syslog.RFC3164Timezone, "UTC"},
		{"syslog.max_message_bytes", c.Syslog.MaxMessageBytes, int64(64 << 10)},
		{"syslog.payload_raw", c.Syslog.PayloadRaw, true},
		{"syslog.legacy_charset", c.Syslog.LegacyCharset, ""},
		{"syslog.accept_malformed", c.Syslog.AcceptMalformed, true},
		{"syslog.sd_max_elements", c.Syslog.SDMaxElements, 32},
		{"syslog.sd_max_params", c.Syslog.SDMaxParams, 64},
		{"ring.capacity_items", c.Ring.CapacityItems, 65536},
		{"ring.max_bytes", c.Ring.MaxBytes, int64(512 << 20)},
		{"ring.high_water", c.Ring.HighWater, 0.80},
		// 4, not the 8 the multi-sink server lowered to 4: PostgreSQL is the
		// only sink, so the default is the database default directly.
		{"sink.workers", c.Sink.Workers, 4},
		{"sink.batch_max_bytes", c.Sink.BatchMaxBytes, int64(16 << 20)},
		{"sink.boot_timeout", c.Sink.BootTimeout, 300 * time.Second},
		{"sink.postgres.addr", c.SinkPG.Addr, "127.0.0.1:5432"},
		{"sink.postgres.username", c.SinkPG.Username, "syslog"},
		{"sink.postgres.database", c.SinkPG.Database, "syslog"},
		{"sink.postgres.table", c.SinkPG.Table, "loghub.inbox"},
		// Derived: workers + 1.
		{"sink.postgres.pool_size", c.SinkPG.PoolSize, 5},
		{"loghub.eb_url", c.LogHub.EBURL, ""},
		{"emergency.enabled", c.Emergency.Enabled, true},
		{"emergency.dir", c.Emergency.Dir, "/work/data/emergency"},
		{"admin.bind", c.Admin.Bind, "127.0.0.1:9102"},
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout, 30 * time.Second},
	}
	for _, ck := range checks {
		if !reflect.DeepEqual(ck.got, ck.want) {
			t.Errorf("%s = %#v, want %#v", ck.name, ck.got, ck.want)
		}
	}
}

// SPEC §16.6: every write path hangs off server.data_dir, and the emergency
// directory is the only one left to derive.
func TestDerivedPaths(t *testing.T) {
	l := mustLoad(t, []string{"--server.data_dir=/srv/x"}, nil)
	if got := l.Config.Emergency.Dir; got != "/srv/x/emergency" {
		t.Fatalf("derived emergency.dir = %q", got)
	}
	if l.Notes["emergency.dir"] == "" {
		t.Error("a derived path carries no note, so the dump would present it as a default")
	}
	// An explicit path wins and is made absolute against the working dir.
	l = mustLoad(t, []string{"--server.data_dir=/srv/x", "--emergency.dir=q/emg"}, nil)
	if got := l.Config.Emergency.Dir; got != "/work/q/emg" {
		t.Fatalf("relative emergency.dir = %q", got)
	}
	if l.Notes["emergency.dir"] != "" {
		t.Errorf("an explicit path is noted as derived: %q", l.Notes["emergency.dir"])
	}
	if got := mustLoad(t, []string{"--emergency.dir=/var/emg"}, nil).Config.Emergency.Dir; got != "/var/emg" {
		t.Fatalf("absolute emergency.dir = %q", got)
	}
}

// SPEC §13.1 step 2: a relative data_dir becomes absolute against the working
// directory, because go-loghub-ident refuses a relative DATADIR.
func TestRelativeDataDirIsResolved(t *testing.T) {
	for _, tc := range []struct {
		args []string
		env  map[string]string
		want string
	}{
		{nil, nil, "/work/data"},
		{[]string{"--server.data_dir=state"}, nil, "/work/state"},
		{[]string{"--server.data_dir=./a/../b"}, nil, "/work/b"},
		{nil, map[string]string{"DATADIR": "rel"}, "/work/rel"},
		{nil, map[string]string{"DATADIR": "/data"}, "/data"},
	} {
		l := mustLoad(t, tc.args, tc.env)
		if got := l.Config.Server.DataDir; got != tc.want {
			t.Errorf("%v %v: data_dir = %q, want %q", tc.args, tc.env, got, tc.want)
		}
		// The derived path follows the resolved root, not the raw value.
		if got := l.Config.Emergency.Dir; got != filepath.Join(tc.want, "emergency") {
			t.Errorf("%v %v: emergency dir = %q", tc.args, tc.env, got)
		}
	}
	// The real working directory is used when the option is empty.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	l, err := Load(Options{Args: []string{"--server.data_dir=x"}, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	if l.Config.Server.DataDir != filepath.Join(wd, "x") {
		t.Fatalf("data_dir = %q, want under %q", l.Config.Server.DataDir, wd)
	}
	mustFail(t, nil, map[string]string{"DATADIR": " "}, "server.data_dir")
}

func TestPrecedence(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "c.ini")
	writeFile(t, ini, "[ring]\ncapacity_items = 111\nmax_bytes = 1MiB\n")

	// file only
	l := mustLoad(t, []string{"--config=" + ini}, nil)
	if l.Config.Ring.CapacityItems != 111 || l.Source["ring.capacity_items"] != FromFile {
		t.Fatalf("file: %d %v", l.Config.Ring.CapacityItems, l.Source["ring.capacity_items"])
	}
	// SYSLOG_CONFIG names the file too.
	l = mustLoad(t, nil, map[string]string{"SYSLOG_CONFIG": ini})
	if l.Config.Ring.CapacityItems != 111 || l.File != ini {
		t.Fatalf("SYSLOG_CONFIG: %d %q", l.Config.Ring.CapacityItems, l.File)
	}
	// env beats file
	l = mustLoad(t, []string{"--config=" + ini}, map[string]string{"SYSLOG_RING_CAPACITY_ITEMS": "222"})
	if l.Config.Ring.CapacityItems != 222 || l.Source["ring.capacity_items"] != FromEnv {
		t.Fatalf("env: %d %v", l.Config.Ring.CapacityItems, l.Source["ring.capacity_items"])
	}
	// flag beats env
	l = mustLoad(t, []string{"--config=" + ini, "--ring.capacity_items=333"},
		map[string]string{"SYSLOG_RING_CAPACITY_ITEMS": "222"})
	if l.Config.Ring.CapacityItems != 333 || l.Source["ring.capacity_items"] != FromFlag {
		t.Fatalf("flag: %d %v", l.Config.Ring.CapacityItems, l.Source["ring.capacity_items"])
	}
	if l.Origin["ring.capacity_items"] != "flag --ring.capacity_items" {
		t.Fatalf("origin = %q", l.Origin["ring.capacity_items"])
	}
	// untouched keys keep the file value
	if l.Config.Ring.MaxBytes != 1<<20 || !strings.HasPrefix(l.Origin["ring.max_bytes"], "file "+ini+" line 3") {
		t.Fatalf("max_bytes = %d from %q", l.Config.Ring.MaxBytes, l.Origin["ring.max_bytes"])
	}
	// A default is not "set".
	if l.SetIn["sink.workers"] || l.Source["sink.workers"] != FromDefault {
		t.Fatalf("sink.workers set=%v source=%v", l.SetIn["sink.workers"], l.Source["sink.workers"])
	}
}

// SPEC §16.1 and §16.2: the default env rule, and the names the skills fixed.
func TestEnvNames(t *testing.T) {
	want := map[string]string{
		"ring.capacity_items":      "SYSLOG_RING_CAPACITY_ITEMS",
		"server.data_dir":          "DATADIR",
		"syslog.default_priority":  "SYSLOG_DEFAULT_PRIORITY",
		"syslog.rfc3164_timezone":  "SYSLOG_RFC3164_TIMEZONE",
		"syslog.max_message_bytes": "SYSLOG_MAX_MESSAGE_BYTES",
		"syslog.payload_raw":       "SYSLOG_PAYLOAD_RAW",
		"syslog.legacy_charset":    "SYSLOG_LEGACY_CHARSET",
		"syslog.accept_malformed":  "SYSLOG_ACCEPT_MALFORMED",
		"syslog.sd_max_elements":   "SYSLOG_SD_MAX_ELEMENTS",
		"syslog.sd_max_params":     "SYSLOG_SD_MAX_PARAMS",
		"loghub.eb_url":            "LOGHUB_EB_URL",
		"proxy.trusted":            "SYSLOG_PROXY_TRUSTED",
		"sink.postgres.addr":       "SYSLOG_SINK_POSTGRES_ADDR",
		"sink.postgres.pool_size":  "SYSLOG_SINK_POSTGRES_POOL_SIZE",
		"listen.unix_read_buffer":  "SYSLOG_LISTEN_UNIX_READ_BUFFER",
		"shutdown.pre_stop_delay":  "SYSLOG_SHUTDOWN_PRE_STOP_DELAY",
	}
	got := map[string]string{}
	for _, p := range Registry(&Config{}) {
		got[p.Name()] = p.Env()
		switch p.Sec {
		case "syslog":
			if p.Env() != "SYSLOG_"+strings.ToUpper(p.Key) {
				t.Errorf("%s env = %q: [syslog] keys are SYSLOG_<KEY>", p.Name(), p.Env())
			}
		case "loghub":
			if p.Env() != "LOGHUB_"+strings.ToUpper(p.Key) {
				t.Errorf("%s env = %q: [loghub] keys are LOGHUB_<KEY>", p.Name(), p.Env())
			}
		case "server":
		default:
			if !strings.HasPrefix(p.Env(), "SYSLOG_"+strings.ToUpper(strings.ReplaceAll(p.Sec, ".", "_"))+"_") {
				t.Errorf("%s env = %q lacks the SYSLOG_<SECTION>_ prefix", p.Name(), p.Env())
			}
		}
	}
	for name, env := range want {
		if got[name] != env {
			t.Errorf("%s env = %q, want %q", name, got[name], env)
		}
	}
}

// The exceptions are not only names in a table: Load reads them, and only them.
func TestEnvExceptionsAreRead(t *testing.T) {
	l := mustLoad(t, nil, map[string]string{
		"SYSLOG_MAX_MESSAGE_BYTES": "8KiB",
		"SYSLOG_DEFAULT_PRIORITY":  "14",
		"SYSLOG_RFC3164_TIMEZONE":  "America/Sao_Paulo",
		"SYSLOG_PAYLOAD_RAW":       "false",
		"SYSLOG_LEGACY_CHARSET":    "CP1252",
		"SYSLOG_ACCEPT_MALFORMED":  "no",
		"SYSLOG_SD_MAX_ELEMENTS":   "4",
		"SYSLOG_SD_MAX_PARAMS":     "5",
		"DATADIR":                  "/data",
	})
	c := l.Config
	if c.Syslog.MaxMessageBytes != 8192 || c.Syslog.DefaultPriority != 14 ||
		c.Syslog.RFC3164Timezone != "America/Sao_Paulo" || c.Syslog.PayloadRaw ||
		c.Syslog.LegacyCharset != CharsetCP1252 || c.Syslog.AcceptMalformed ||
		c.Syslog.SDMaxElements != 4 || c.Syslog.SDMaxParams != 5 {
		t.Fatalf("[syslog] from env = %+v", c.Syslog)
	}
	if c.Server.DataDir != "/data" || c.Emergency.Dir != "/data/emergency" {
		t.Fatalf("DATADIR: %q %q", c.Server.DataDir, c.Emergency.Dir)
	}
	if l.Origin["syslog.max_message_bytes"] != "env SYSLOG_MAX_MESSAGE_BYTES" {
		t.Fatalf("origin = %q", l.Origin["syslog.max_message_bytes"])
	}

	// The default-rule spelling of an exception is NOT read: a deploy that
	// used it would believe it took effect.
	l = mustLoad(t, nil, map[string]string{
		"SYSLOG_SYSLOG_MAX_MESSAGE_BYTES": "8KiB",
		"SYSLOG_SERVER_DATA_DIR":          "/elsewhere",
		"SYSLOG_LOGHUB_EB_URL":            "postgres://u:p@elsewhere:5432/db",
	})
	c = l.Config
	if c.Syslog.MaxMessageBytes != 64<<10 || c.Server.DataDir != "/work/data" ||
		c.LogHub.EBURL != "" || c.SinkPG.Addr != "127.0.0.1:5432" {
		t.Fatalf("default-rule names were read: %d %q %q %q", c.Syslog.MaxMessageBytes,
			c.Server.DataDir, c.LogHub.EBURL, c.SinkPG.Addr)
	}
}

// SPEC §16.2: sizes take KiB/MiB/GiB and plain bytes; durations take Go syntax.
func TestSizesAndDurations(t *testing.T) {
	for raw, want := range map[string]int64{
		"64KiB": 64 << 10, "64 kib": 64 << 10, "65536": 65536, "1GiB": 1 << 30,
		"16MiB": 16 << 20, "480": 480, " 2 MiB ": 2 << 20,
	} {
		v, err := parseBytes(raw)
		if err != nil || v != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", raw, v, err, want)
		}
		if back, err := parseBytes(FormatBytes(want)); err != nil || back != want {
			t.Errorf("FormatBytes(%d) = %q does not round-trip", want, FormatBytes(want))
		}
	}
	// KB/MB are refused on purpose: 10^n next to 2^n is a trap.
	for _, raw := range []string{"8MB", "8M", "8K", "-1", "1.5MiB", "MiB", "", "1TiB", "9999999999GiB"} {
		if v, err := parseBytes(raw); err == nil {
			t.Errorf("parseBytes(%q) = %d, want an error", raw, v)
		}
	}
	mustFail(t, []string{"--ring.max_bytes=512MB"}, nil, "KiB/MiB/GiB")

	l := mustLoad(t, []string{"--sink.op_timeout=1500ms", "--sink.boot_timeout=2m", "--tcp.idle_timeout=1h"}, nil)
	if l.Config.Sink.OpTimeout != 1500*time.Millisecond || l.Config.Sink.BootTimeout != 2*time.Minute ||
		l.Config.TCP.IdleTimeout != time.Hour {
		t.Fatalf("durations: %+v", l.Config.Sink)
	}
	// A bare number is refused for an ordinary duration: seconds or millis?
	mustFail(t, []string{"--sink.op_timeout=30"}, nil, "unit suffix")
	mustFail(t, []string{"--sink.op_timeout=-1s"}, nil, "negative")
}

// listen.unix_mode is the only octal parameter, and reading "0666" as decimal
// would produce mode 1232 octal: setuid plus a permission set nobody asked for.
func TestOctalMode(t *testing.T) {
	l := mustLoad(t, []string{"--listen.unix=/tmp/s", "--listen.unix_mode=0660"}, nil)
	if l.Config.Listen.UnixMode != 0o660 {
		t.Fatalf("mode = %o, want 660", l.Config.Listen.UnixMode)
	}
	l = mustLoad(t, []string{"--listen.unix=/tmp/s", "--listen.unix_mode=644"}, nil)
	if l.Config.Listen.UnixMode != 0o644 {
		t.Fatalf("mode without leading zero = %o, want 644", l.Config.Listen.UnixMode)
	}
	if _, err := load(t, []string{"--listen.unix_mode=999"}, nil); err == nil {
		t.Error("mode 999 accepted; 9 is not an octal digit")
	}
	mustFail(t, []string{"--listen.unix=/tmp/s", "--listen.unix_mode=0"}, nil, "unwritable")
}

func TestEnumsAreValidatedAndCanonical(t *testing.T) {
	c := mustLoad(t, []string{"--tcp.framing=LF", "--log.level=WARN", "--log.format=Text",
		"--proxy.protocol=Optional", "--syslog.legacy_charset=LATIN1"}, nil).Config
	if c.TCP.Framing != FramingLF || c.Log.Level != "warn" || c.Log.Format != "text" ||
		c.Proxy.Protocol != ProxyOptional || c.Syslog.LegacyCharset != CharsetLatin1 {
		t.Fatalf("enums not canonical: %q %q %q %q %q", c.TCP.Framing, c.Log.Level, c.Log.Format,
			c.Proxy.Protocol, c.Syslog.LegacyCharset)
	}
	for _, tc := range []struct{ arg, want string }{
		{"--tcp.framing=octet-counting", "auto, octet, lf"},
		{"--proxy.protocol=on", "off, optional, required"},
		{"--syslog.legacy_charset=utf-16", `"", cp1252, latin1`},
		{"--log.level=trace", "debug, info, warn, error"},
		{"--log.format=xml", "json, text"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.want)
	}
}

// Past sun_path the kernel says only "invalid argument". The length check turns
// that into an error that names the problem.
func TestUnixSocketPath(t *testing.T) {
	long := "/" + strings.Repeat("a", unixPathMax())
	mustFail(t, []string{"--listen.unix=" + long}, nil, "sun_path")
	mustFail(t, []string{"--listen.unix=dev/log"}, nil, "absolute")
	mustLoad(t, []string{"--listen.unix=/dev/log"}, nil)
}

func TestNoListenerIsFatal(t *testing.T) {
	mustFail(t, []string{"--listen.udp=", "--listen.tcp="}, nil, "no transport")
	// Any one transport is enough.
	mustLoad(t, []string{"--listen.udp=", "--listen.tcp=", "--listen.unix=/dev/log"}, nil)
	mustLoad(t, []string{"--listen.udp=", "--listen.tcp=:1514"}, nil)
	mustLoad(t, []string{"--listen.udp=:1514", "--listen.tcp="}, nil)
}

// SPEC §4 and §13.2: the address and listener-count rules of the transports
// that remain.
func TestListenAddresses(t *testing.T) {
	// UDP and TCP on the same port is the default and must pass: different
	// protocols, different sockets.
	mustLoad(t, []string{"--listen.udp=:1514", "--listen.tcp=:1514"}, nil)
	mustLoad(t, []string{"--listen.udp=127.0.0.1:1514", "--listen.tcp=[::1]:1514"}, nil)
	mustLoad(t, []string{"--listen.udp_listeners=4", "--listen.tcp_listeners=2"}, nil)
	for _, tc := range []struct{ arg, want string }{
		{"--listen.tcp=1514", "listen.tcp"},
		{"--listen.udp=1514", "listen.udp"},
		{"--listen.udp_listeners=0", "listen.udp_listeners"},
		{"--listen.tcp_listeners=0", "listen.tcp_listeners"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.want)
	}
}

// Review findings on the validation of SPEC §13.2.
func TestValidationReviewFindings(t *testing.T) {
	// NaN and ±Inf parse as floats and pass every range check.
	for _, v := range []string{"NaN", "nan", "Inf", "-Inf"} {
		mustFail(t, []string{"--ring.high_water=" + v}, nil, "ring.high_water")
		mustFail(t, []string{"--log.access_sample=" + v}, nil, "log.access_sample")
	}
	mustLoad(t, []string{"--ring.high_water=1", "--log.access_sample=0"}, nil)
	// An empty admin.bind bound every interface on a random port.
	mustFail(t, []string{"--admin.enabled=true", "--admin.bind="}, nil, "admin.bind")
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind="}, nil)
	// Files written outside data_dir are allowed, with a warning (SPEC §20.16).
	l := mustLoad(t, []string{"--server.data_dir=/data", "--emergency.dir=/fast/emg"}, nil)
	if !hasWarning(l, "emergency.dir") {
		t.Fatalf("no warning for an emergency dir outside data_dir: %q", l.Warnings)
	}
	for _, args := range [][]string{
		{"--server.data_dir=/data", "--emergency.dir=/data/sub/emg"},
		// Disabled dumps write nothing, so where they would go is moot.
		{"--server.data_dir=/data", "--emergency.dir=/fast/emg", "--emergency.enabled=false"},
	} {
		if l := mustLoad(t, args, nil); hasWarning(l, "outside server.data_dir") {
			t.Fatalf("%v: warned: %q", args, l.Warnings)
		}
	}
	// A prefix of the name is not "under": /data2 is outside /data.
	if l := mustLoad(t, []string{"--server.data_dir=/data", "--emergency.dir=/data2/emg"}, nil); !hasWarning(l, "outside server.data_dir") {
		t.Fatalf("/data2 counted as under /data: %q", l.Warnings)
	}
}

func TestProxyRules(t *testing.T) {
	mustFail(t, []string{"--proxy.protocol=optional", "--proxy.trusted="}, nil, "proxy.trusted")
	mustFail(t, []string{"--proxy.protocol=required", "--proxy.trusted=10.0.0.0/33"}, nil, "proxy.trusted")
	// An invalid CIDR fails even with the protocol off: a typo must fail loudly.
	mustFail(t, []string{"--proxy.trusted=not-a-cidr"}, nil, "proxy.trusted")
	// An empty list is fine while nothing reads it.
	mustLoad(t, []string{"--proxy.protocol=off", "--proxy.trusted="}, nil)
	c := mustLoad(t, []string{"--proxy.protocol=optional",
		"--proxy.trusted=10.0.0.5, 192.168.1.77/24, ::ffff:172.16.0.1, 2001:db8::1/64"}, nil).Config
	want := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.5/32"),
		netip.MustParsePrefix("192.168.1.0/24"),
		netip.MustParsePrefix("172.16.0.1/32"),
		netip.MustParsePrefix("2001:db8::/64"),
	}
	if !reflect.DeepEqual(c.Proxy.Trusted, want) {
		t.Fatalf("trusted = %v, want %v", c.Proxy.Trusted, want)
	}
}

// SPEC §10.5: PostgreSQL is the only sink, so its budget rule is the default
// itself. workers is 4 without a warning; above 16 the boot warns; the pool is
// derived as workers + 1, and a pool below the workers is refused.
func TestSinkWorkersAndPool(t *testing.T) {
	// The whole default configuration, with GOMEMLIMIT set, warns about
	// nothing: a WARN on every clean boot teaches operators to ignore WARN.
	l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "1GiB"})
	if len(l.Warnings) != 0 {
		t.Fatalf("default boot warns: %q", l.Warnings)
	}
	if l.Config.Sink.Workers != 4 || l.Config.SinkPG.PoolSize != 5 {
		t.Fatalf("workers = %d pool = %d, want 4 and 5", l.Config.Sink.Workers, l.Config.SinkPG.PoolSize)
	}
	if l.Notes["sink.postgres.pool_size"] != "derived: sink.workers + 1" {
		t.Fatalf("pool note = %q", l.Notes["sink.postgres.pool_size"])
	}
	// The workers are never adjusted any more: nothing notes them.
	if l.Notes["sink.workers"] != "" {
		t.Fatalf("workers note = %q", l.Notes["sink.workers"])
	}

	// 16 is the threshold itself, still quiet; 17 warns, and the pool follows.
	if l := mustLoad(t, []string{"--sink.workers=16"}, nil); hasWarning(l, "sink.workers") {
		t.Fatalf("16 workers warned: %q", l.Warnings)
	}
	l = mustLoad(t, []string{"--sink.workers=17"}, nil)
	if !hasWarning(l, "sink.workers=17") || !hasWarning(l, "lock contention") {
		t.Fatalf("17 workers: warnings = %q", l.Warnings)
	}
	if l.Config.Sink.Workers != 17 || l.Config.SinkPG.PoolSize != 18 {
		t.Fatalf("17 workers: workers = %d pool = %d", l.Config.Sink.Workers, l.Config.SinkPG.PoolSize)
	}

	// An explicit pool is respected, equal to the workers included, and is not
	// noted as derived.
	for _, pool := range []string{"8", "9", "40"} {
		l := mustLoad(t, []string{"--sink.workers=8", "--sink.postgres.pool_size=" + pool}, nil)
		if got := l.Config.SinkPG.PoolSize; strconv.Itoa(got) != pool || l.Notes["sink.postgres.pool_size"] != "" {
			t.Fatalf("pool %s: got %d, note %q", pool, got, l.Notes["sink.postgres.pool_size"])
		}
	}
	// A pool below the workers makes one wait on another's connection.
	mustFail(t, []string{"--sink.workers=8", "--sink.postgres.pool_size=4"}, nil, "pool_size=4")
	mustFail(t, []string{"--sink.workers=8", "--sink.postgres.pool_size=7"}, nil, "below sink.workers=8")
	mustFail(t, []string{"--sink.postgres.pool_size=-1"}, nil, "negative")
	mustFail(t, []string{"--sink.workers=0"}, nil, "sink.workers")
}

// SPEC §10.5 and §13.2.
func TestPostgresValidation(t *testing.T) {
	mustLoad(t, []string{"--sink.postgres.table=logs.syslog_events"}, nil)
	for _, addr := range []string{"pg-1:5432", "10.0.0.5:6432", "[::1]:5432", "/var/run/postgresql",
		"/var/run/postgresql:5433"} {
		if got := mustLoad(t, []string{"--sink.postgres.addr=" + addr}, nil).Config.SinkPG.Addr; got != addr {
			t.Errorf("addr %q stored as %q", addr, got)
		}
	}
	for _, tc := range []struct{ arg, want string }{
		{"--sink.postgres.table=Events", "lowercase"},
		{"--sink.postgres.table=a.b.c", "lowercase"},
		{"--sink.postgres.table=inbox; drop table x", "lowercase"},
		{"--sink.postgres.database=", "sink.postgres.database"},
		{"--sink.postgres.username=", "sink.postgres.username"},
		{"--sink.postgres.addr=", "sink.postgres.addr"},
		// A bare host has no port, and a relative path is not a socket dir:
		// libpq would read both as something other than what was meant.
		{"--sink.postgres.addr=pg-1", "neither host:port"},
		{"--sink.postgres.addr=var/run/postgresql", "neither host:port"},
		{"--sink.postgres.addr=pg-1:5432:1", "neither host:port"},
		// Review B #4: each of these passed and was then retried as a network
		// failure until sink.boot_timeout, or refused only at boot step 7.
		{"--sink.postgres.addr=:5432", "neither host:port"},
		{"--sink.postgres.addr=h1,h2:5432", "neither host:port"},
		{"--sink.postgres.addr=pg-1:0", "neither host:port"},
		{"--sink.postgres.addr=pg-1:99999", "neither host:port"},
		{"--sink.postgres.addr=pg-1:05432", "neither host:port"},
		{"--sink.postgres.addr=/var/run/postgresql:0", "socket directory"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.want)
	}
}

func TestSyslogRanges(t *testing.T) {
	mustLoad(t, []string{"--syslog.default_priority=0", "--syslog.max_message_bytes=480"}, nil)
	mustLoad(t, []string{"--syslog.default_priority=191", "--syslog.max_message_bytes=16MiB"}, nil)
	mustLoad(t, []string{"--syslog.rfc3164_timezone=America/Sao_Paulo"}, nil)
	for _, tc := range []struct{ arg, want string }{
		{"--syslog.default_priority=192", "0..191"},
		{"--syslog.default_priority=-1", "0..191"},
		{"--syslog.max_message_bytes=479", "480..16MiB"},
		{"--syslog.max_message_bytes=16385KiB", "480..16MiB"},
		{"--syslog.rfc3164_timezone=Mars/Olympus_Mons", "IANA"},
		{"--syslog.rfc3164_timezone=Local", "Local"},
		{"--syslog.sd_max_elements=0", "syslog.sd_max_elements"},
		{"--syslog.sd_max_params=0", "syslog.sd_max_params"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.want)
	}
	// The zone loads from the embedded database whatever the host has.
	c := mustLoad(t, []string{"--syslog.rfc3164_timezone=Asia/Tokyo"}, nil).Config
	if loc, err := c.Syslog.Location(); err != nil || loc.String() != "Asia/Tokyo" {
		t.Fatalf("Location = %v, %v", loc, err)
	}
}

// SPEC §16.5: zero is refused where it is not "unlimited" but "already expired"
// or "silently replaced by a default" -- and accepted where it means what it
// says.
func TestZeroDurationsAreRejectedWhereDangerous(t *testing.T) {
	for _, name := range []string{
		"sink.op_timeout", "sink.health_interval", "sink.health_timeout", "sink.boot_timeout",
		"sink.retry_backoff_min", "sink.retry_backoff_max", "shutdown.drain_timeout",
		"tcp.backpressure_timeout", "proxy.header_timeout",
	} {
		mustFail(t, []string{"--" + name + "=0s"}, nil, name+": must be positive")
	}
	for _, arg := range []string{
		"--shutdown.pre_stop_delay=0s",
		"--sink.batch_max_delay=0s",
		"--sink.fail_timeout=0s",
		"--tcp.idle_timeout=0s",
	} {
		mustLoad(t, []string{arg}, nil)
	}
	mustFail(t, []string{"--sink.retry_backoff_min=3s"}, nil, "below sink.retry_backoff_min")
}

func TestUnknownParameterIsFatal(t *testing.T) {
	mustFail(t, []string{"--ring.capacty_items=1"}, nil, "unknown parameter")
	// The keys the standard had and this server dropped are gone for good.
	for _, gone := range []string{"--node.id=x", "--listen.relp=:20514", "--parse.prefer=auto",
		"--ring.data_dir=/x", "--tcp.max_frame_bytes=1MiB"} {
		mustFail(t, []string{gone}, nil, "unknown parameter")
	}
	dir := t.TempDir()
	ini := filepath.Join(dir, "c.ini")
	writeFile(t, ini, "[ring]\ncapacity_items = 10\nbogus = 1\n")
	mustFail(t, []string{"--config=" + ini}, nil, "line 3")
}

// A parser error names the line or the flag, never the value on it (SPEC
// §13.2): "password: hunter2" (a ':' for the '=') or
// "sink.postgres.password=x" (the dashes forgotten) went whole to the
// container's stderr, and so did a value swallowed by a flag left without its
// own, typed after a space, starting with a dash or split by the shell at a
// space (the parser of go-webhook-nats-server and go-syslog-nats-server,
// ported 2026-10-11).
func TestParserErrorsDoNotEchoValues(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[sink.postgres]\npassword: hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, "line 2"); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("INI error echoes the line: %v", err)
	}
	writeFile(t, ini, "[sink.postgres hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, "line 1"); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("INI error echoes the header: %v", err)
	}
	err := mustFail(t, []string{"sink.postgres.password=hunter2"}, nil, "unexpected argument")
	if strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "sink.postgres.password") {
		t.Errorf("flag error = %v, want the name and not the value", err)
	}
	// A flag whose value was left out never swallows the next argument, and
	// nothing typed after a value -- past a space, starting with a dash, or
	// the tail of a value the shell split -- is printed.
	for _, args := range [][]string{
		{"--sink.workers", "--sink.postgres.password=hunter2"},
		{"--sink.postgres.table", "--sink.postgres.password=hunter2"},
		{"--sink.op_timeout", "--loghub.eb_url=postgres://u:hunter2@h/db"},
		{"--sink.workers", "--no.such.key=hunter2"},
		{"--sink.workers", "-sink.postgres.password=hunter2"},
		{"--config", "--sink.postgres.password=hunter2"},
		{"--config=", "hunter2"},
		{"--sink.postgres.password=", "hunter2"},
		{"--sink.postgres.password", "=", "hunter2"},
		{"--sink.postgres.password", "--hunter2"},
		{"--sink.postgres.password=", "-hunter2"},
		{"--sink.postgres.password", "=", "-hunter2"},
		{"--sink.postgres.password=", "--hunter2=x"},
		{"--sink.postgres.password=foo", "hunter2"},
		{"--sink.postgres.password", "foo", "hunter2"},
	} {
		err := mustFail(t, args, nil, "")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: the error echoes the secret: %v", args, err)
		}
	}
	// Controls: the same values on well-formed lines load, a positional
	// argument with no '=' is still named, a value of the separated form is
	// still taken, a negative number is a value and not a flag, a switch is a
	// flag, a typo after a real value is still named, and each refusal says
	// what went wrong -- the space after the '=', or a value with a space
	// unquoted.
	writeFile(t, ini, "[sink.postgres]\nusername = u\npassword = hunter2\n")
	mustLoad(t, []string{"--config=" + ini}, nil)
	mustFail(t, []string{"extra"}, nil, `"extra"`)
	if l := mustLoad(t, []string{"--sink.workers", "4", "--sink.postgres.table", "x.inbox"}, nil); l.Config.Sink.Workers != 4 ||
		l.Config.SinkPG.Table != "x.inbox" {
		t.Errorf("separated values: workers=%d table=%q", l.Config.Sink.Workers, l.Config.SinkPG.Table)
	}
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "-1 is below 1")
	mustFail(t, []string{"--sink.postgres.table", "-h"}, nil, "the next argument is the flag --h")
	mustFail(t, []string{"--sink.postgres.password=", "--admin.enabled", "extra"}, nil, `"extra"`)
	mustFail(t, []string{"--sink.postgres.password=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.postgres.password", "=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.postgres.password=foo", "hunter2"}, nil, "goes in quotes")
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, `"typo.key"`)
	mustFail(t, []string{"--sink.postgres.password", "--sink.workers=4"}, nil, "the flag --sink.workers")
}

// The keys of the features go-syslog-pg-server removed (other sinks, ring
// modes, TLS, filter, rate limit, beacon) must fail the boot, on the command
// line and in a file alike: a config carried over from go-syslog-server that
// silently lost its TLS or its filter would run doing something else than its
// operator believes.
func TestRemovedKeysAreUnknown(t *testing.T) {
	gone := []struct{ name, value string }{
		{"sink.type", "postgres"},
		{"ring.mode", "memory"},
		{"listen.tls", ":6514"},
		{"listen.tls_listeners", "1"},
		{"tls.cert_file", "/c"},
		{"filter.min_severity", "info"},
		{"ratelimit.enabled", "false"},
		{"sink.postgres.tls", "false"},
		{"sink.postgres.tls_ca", "/ca"},
		{"sink.postgres.tls_insecure", "false"},
		{"loghub.beacon_interval", "15"},
		{"loghub.ctl_namespace", "loghub._ctl"},
		{"ring.max_inflight", "4"},
		{"sink.nats.urls", "nats://n:4222"},
	}
	// Control: the same shapes with kept keys load, so the refusals below are
	// about the names and not about the way the test writes them.
	kept := []struct{ name, value string }{
		{"sink.workers", "4"},
		{"ring.capacity_items", "100"},
		{"listen.tcp", ":1514"},
		{"sink.postgres.addr", "pg-1:5432"},
		{"log.level", "debug"},
		{"emergency.enabled", "true"},
	}
	asINI := func(name, value string) string {
		i := strings.LastIndex(name, ".")
		return "# carried over\n[" + name[:i] + "]\n" + name[i+1:] + " = " + value + "\n"
	}
	dir := t.TempDir()
	for _, k := range kept {
		mustLoad(t, []string{"--" + k.name + "=" + k.value}, nil)
		ini := filepath.Join(dir, "kept.ini")
		writeFile(t, ini, asINI(k.name, k.value))
		mustLoad(t, []string{"--config=" + ini}, nil)
	}
	for _, k := range gone {
		err := mustFail(t, []string{"--" + k.name + "=" + k.value}, nil, "unknown parameter")
		if !strings.Contains(err.Error(), k.name) {
			t.Errorf("--%s: the error does not name the key: %v", k.name, err)
		}
		ini := filepath.Join(dir, "gone.ini")
		writeFile(t, ini, asINI(k.name, k.value))
		err = mustFail(t, []string{"--config=" + ini}, nil, "unknown parameter")
		if !strings.Contains(err.Error(), "line 3") || !strings.Contains(err.Error(), k.name) {
			t.Errorf("[%s] in a file: the error does not point at the line and key: %v", k.name, err)
		}
	}
	// No registry key mentions TLS, and no section survives that only a removed
	// feature had.
	for _, p := range Registry(&Config{}) {
		if strings.Contains(p.Key, "tls") {
			t.Errorf("%s: a TLS key survived", p.Name())
		}
		switch p.Sec {
		case "tls", "filter", "ratelimit", "ring.sqlite", "ring.redis", "sink.nats",
			"sink.redis", "sink.sqlite", "sink.mysql", "sink.clickhouse":
			t.Errorf("%s: section [%s] belongs to a removed feature", p.Name(), p.Sec)
		}
	}
}

// The dump shows the identity, but a copy of it in a real file must not look
// like it set anything.
func TestIdentitySectionIsNotConfiguration(t *testing.T) {
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[identity]\nworkspace = prod\n")
	mustFail(t, []string{"--config=" + ini}, nil, "go-loghub-ident")
}

func TestHelpAndVersionExitEarly(t *testing.T) {
	for _, arg := range []string{"--help", "-h", "--version"} {
		// Even with a broken value later on the line: --help must always work.
		// The key is a known one, so the value is what is broken.
		l, err := load(t, []string{arg, "--ring.high_water=bogus"}, nil)
		if !errors.Is(err, ErrExitEarly) || l == nil {
			t.Fatalf("%s: err = %v", arg, err)
		}
	}
	// The control: without --help the same line is an error.
	mustFail(t, []string{"--ring.high_water=bogus"}, nil, "ring.high_water")
	l := mustLoad(t, []string{"--dump-config"}, nil)
	if !l.Meta.DumpConfig {
		t.Fatal("--dump-config not reported")
	}
}

// The dump must be loadable back, or it is only decoration. Secrets break that
// on purpose and the dump says so.
func TestDumpRoundTrips(t *testing.T) {
	l := mustLoad(t, []string{
		"--listen.unix=/tmp/dev-log", "--listen.unix_mode=0640",
		"--syslog.max_message_bytes=32KiB", "--syslog.legacy_charset=cp1252",
		"--proxy.protocol=optional", "--proxy.trusted=10.0.0.0/8",
		"--server.data_dir=rel", "--ring.capacity_items=1000",
		"--sink.postgres.database= padded ", "--sink.postgres.addr=/var/run/postgresql",
		"--syslog.rfc3164_timezone=Europe/Lisbon", "--log.access_sample=0.25",
	}, nil)

	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("WARNING: secrets")) {
		t.Fatal("no secret was set, yet the dump claims it redacted one")
	}
	if !strings.HasPrefix(buf.String(), "# effective configuration of go-syslog-pg-server\n") {
		t.Errorf("dump header names another server:\n%s", strings.SplitN(buf.String(), "\n", 2)[0])
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, buf.String())
	back := mustLoad(t, []string{"--config=" + f}, nil)
	if !reflect.DeepEqual(back.Config, l.Config) {
		t.Fatalf("dump did not round-trip\n got %+v\nwant %+v\ndump:\n%s", back.Config, l.Config, buf.String())
	}
	// Adjusted and derived values say so, so the dump never presents them as
	// compiled-in defaults.
	for _, sub := range []string{"# derived: sink.workers + 1",
		"# derived from server.data_dir", "# resolved to an absolute path", "# from flag --listen.unix"} {
		if !strings.Contains(buf.String(), sub) {
			t.Errorf("dump lacks %q", sub)
		}
	}
}

func TestSecretsAreRedacted(t *testing.T) {
	const key = "sink.postgres.password"
	l := mustLoad(t, []string{"--" + key + "=hunter2"}, nil)
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
	if l.Values()[key] != "hunter2" {
		t.Errorf("%s redacted in Values(): a changed password would not count on SIGHUP", key)
	}

	// A URL keeps its host and loses its userinfo. The comma and the slash in
	// the password are the reason redactURL works on the text: a split on the
	// comma would print "pa" in clear.
	for in, want := range map[string]string{
		"postgres://u:hunter2@pg-1/db":           "postgres://u:***@pg-1/db",
		"postgres://u:pa,ss/hunter2@pg-1:6432/d": "postgres://u:***@pg-1:6432/d",
		"postgresql://tok3n@pg-1/db":             "postgresql://***@pg-1/db",
		"postgres://pg-1/db":                     "postgres://pg-1/db",
		"hunter2":                                "***",
	} {
		if got := redactURL(in); got != want {
			t.Errorf("redactURL(%q) = %q, want %q", in, got, want)
		}
	}
	l = mustLoad(t, []string{"--loghub.eb_url=postgres://u:hunter2@pg-1/db"}, nil)
	if got := l.Effective()["loghub.eb_url"]; got != "postgres://u:***@pg-1/db" {
		t.Fatalf("redacted eb_url = %q", got)
	}
}

func TestUsageCoversEveryParameter(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, p := range Registry(&Config{}) {
		if !strings.Contains(out, p.Key) {
			t.Errorf("--help omits %s", p.Name())
		}
		if !strings.Contains(out, "env "+p.Env()+"\n") {
			t.Errorf("--help omits the environment name of %s (%s)", p.Name(), p.Env())
		}
		if p.Desc == "" {
			t.Errorf("%s has no description", p.Name())
		}
	}
}

// --help is the first thing an operator reads: it must name this server, and
// it must not offer the TLS listener the slimmed copy no longer has.
func TestUsageNamesThisServer(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "go-syslog-pg-server ") || !strings.Contains(out, "\n  go-syslog-pg-server [--config FILE]") {
		t.Errorf("--help does not name go-syslog-pg-server:\n%s", out)
	}
	for _, bad := range []string{"go-syslog-server", "listen.tls", "tls_listeners", "6514", "[tls]",
		"cert_file", "nats://", "redis://", "[filter]", "[ratelimit]", "beacon"} {
		if strings.Contains(out, bad) {
			t.Errorf("--help mentions %q", bad)
		}
	}
	// It says so instead, so nobody goes looking for the key.
	if !strings.Contains(out, "There is no TLS") {
		t.Error("--help does not say there is no TLS")
	}
}

// SPEC §16.3: exactly these keys reload, and no [syslog] key is among them.
func TestReloadableKeysAreTheSpecList(t *testing.T) {
	want := []string{"log.access", "log.access_sample", "log.level"}
	var got []string
	for _, p := range Registry(&Config{}) {
		if p.Reload {
			got = append(got, p.Name())
		}
		if p.Sec == "syslog" && p.Reload {
			t.Errorf("%s is reloadable: it changes the JLH bytes (SPEC §3.5)", p.Name())
		}
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reloadable = %v\nwant        %v", got, want)
	}
	// --help marks the same three, and only them.
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(buf.String(), "(reloadable with SIGHUP)"); n != len(want) {
		t.Fatalf("--help marks %d keys reloadable, want %d", n, len(want))
	}
}

// SPEC §3.9: 12 x L + 8 KiB; 776 KiB with the 64 KiB default.
func TestMaxEventBytes(t *testing.T) {
	c := mustLoad(t, nil, nil).Config
	// 12 x 65536 + 8192 = 794624 = 776 KiB.
	if got := MaxEventBytes(c); got != 794624 {
		t.Fatalf("MaxEventBytes = %d, want 794624 (776 KiB)", got)
	}
	c = mustLoad(t, nil, map[string]string{"SYSLOG_MAX_MESSAGE_BYTES": "480"}).Config
	if got := MaxEventBytes(c); got != 12*480+8192 {
		t.Fatalf("MaxEventBytes(480) = %d", got)
	}
	// The ceiling of syslog.max_message_bytes exists so the worst JLH still
	// fits a jsonb value (255 MiB); a larger ceiling would accept messages the
	// INSERT can never store.
	if worst := MaxEventBytesFor(MaxMessageBytes); worst >= 255<<20 {
		t.Fatalf("worst JLH at the ceiling = %d, above the 255 MiB of a jsonb value", worst)
	}
}

// SPEC §13.2 and §15.4. The ring is always in memory, so the 2x rule always
// applies, whatever else is configured.
func TestGOMEMLIMITWarnings(t *testing.T) {
	if l := mustLoad(t, nil, nil); !hasWarning(l, "GOMEMLIMIT is not set") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	if l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "off"}); !hasWarning(l, "GOMEMLIMIT is not set") {
		t.Fatalf("off: warnings = %v", l.Warnings)
	}
	if l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "1GiB"}); hasWarning(l, "GOMEMLIMIT") {
		t.Fatalf("1GiB = 2 x 512MiB warned: %v", l.Warnings)
	}
	if l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "1073741823"}); !hasWarning(l, "below 2 x ring.max_bytes") {
		t.Fatalf("one byte under: warnings = %v", l.Warnings)
	}
	// The threshold follows ring.max_bytes, not a constant.
	small := []string{"--ring.max_bytes=64MiB"}
	if l := mustLoad(t, small, map[string]string{"GOMEMLIMIT": "128MiB"}); hasWarning(l, "GOMEMLIMIT") {
		t.Fatalf("128MiB = 2 x 64MiB warned: %v", l.Warnings)
	}
	if l := mustLoad(t, small, map[string]string{"GOMEMLIMIT": "127MiB"}); !hasWarning(l, "below 2 x ring.max_bytes (64MiB)") {
		t.Fatalf("127MiB with a 64MiB ring: warnings = %v", l.Warnings)
	}
	// A spelling the runtime would refuse is reported, never silently passed.
	if l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "1GB"}); !hasWarning(l, "not understood") {
		t.Fatalf("1GB: warnings = %v", l.Warnings)
	}
}

// Defaults is what other packages' tests build on: it must already be a
// configuration Validate accepts, apart from the derived paths.
func TestDefaultsValidate(t *testing.T) {
	c := Defaults()
	if c.Server.DataDir != "./data" || c.Emergency.Dir != "" || c.SinkPG.PoolSize != 0 {
		t.Fatalf("Defaults = %+v", c)
	}
	if _, err := Validate(c, nil); err != nil {
		t.Fatal(err)
	}
	// Validate is where the pool is derived; a caller of Defaults+Validate
	// gets the same pool Load would.
	if c.SinkPG.PoolSize != 5 {
		t.Fatalf("pool after Validate = %d, want 5", c.SinkPG.PoolSize)
	}
}

// Every value the registry accepts for syslog.legacy_charset must mean a
// charset to the converter: the app maps it with convert.ParseCharset and
// ignores ok, so a value accepted here and unknown there would silently disable
// the feature. windows-1252 is the skill's own example value (review finding).
func TestLegacyCharsetValuesAreTheConvertersValues(t *testing.T) {
	l := mustLoad(t, nil, map[string]string{"SYSLOG_LEGACY_CHARSET": "windows-1252"})
	if cs, ok := convert.ParseCharset(l.Config.Syslog.LegacyCharset); !ok || cs != convert.CharsetCP1252 {
		t.Fatalf("windows-1252 -> %v %v", cs, ok)
	}
	for _, p := range l.Params {
		if p.Sec != "syslog" || p.Key != "legacy_charset" {
			continue
		}
		for _, v := range p.Enum {
			if _, ok := convert.ParseCharset(v); !ok {
				t.Errorf("config accepts %q, the converter does not", v)
			}
		}
		return
	}
	t.Fatal("syslog.legacy_charset not in the registry")
}

// A parent go-syslog-server variable left in the environment is not silently
// ignored: one WARN names every one that is set. The list must never name a
// parameter this server has, or a valid setting would be reported as ignored.
func TestRemovedEnvIsWarned(t *testing.T) {
	ours := map[string]bool{}
	for _, p := range Registry(&Config{}) {
		ours[p.Env()] = true
	}
	for _, k := range removedEnv {
		if ours[k] || k == configFileEnv {
			t.Errorf("removedEnv names %s, which this server reads", k)
		}
	}

	env := map[string]string{"GOMEMLIMIT": "2GiB"}
	if l := mustLoad(t, nil, env); len(l.Warnings) != 0 {
		t.Fatalf("control: warnings without removed variables: %v", l.Warnings)
	}
	env["SYSLOG_LISTEN_TLS"] = ":6514"
	env["SYSLOG_FILTER_MIN_SEVERITY"] = "4"
	env["LOGHUB_BEACON_INTERVAL"] = "15"
	l := mustLoad(t, nil, env)
	if len(l.Warnings) != 1 {
		t.Fatalf("warnings = %v, want one line", l.Warnings)
	}
	for _, k := range []string{"SYSLOG_LISTEN_TLS", "SYSLOG_FILTER_MIN_SEVERITY", "LOGHUB_BEACON_INTERVAL", "Ignored"} {
		if !strings.Contains(l.Warnings[0], k) {
			t.Errorf("warning %q does not name %s", l.Warnings[0], k)
		}
	}
}
