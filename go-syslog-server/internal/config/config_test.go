package config

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
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
// standard server, so a ported default that slipped through fails here.
func TestDefaults(t *testing.T) {
	c := mustLoad(t, nil, nil).Config
	checks := []struct {
		name      string
		got, want any
	}{
		{"server.data_dir", c.Server.DataDir, "/work/data"},
		{"listen.udp", c.Listen.UDP, ":1514"},
		{"listen.tcp", c.Listen.TCP, ":1514"},
		// TLS and unix are off by default: TLS needs a certificate, and
		// creating /dev/log uninvited would take over the host's own syslog.
		{"listen.tls", c.Listen.TLS, ""},
		{"listen.unix", c.Listen.Unix, ""},
		{"listen.unix_mode", c.Listen.UnixMode, 0o666},
		{"listen.udp_read_buffer", c.Listen.UDPReadBuffer, int64(8 << 20)},
		{"tcp.framing", c.TCP.Framing, FramingAuto},
		{"tcp.idle_timeout", c.TCP.IdleTimeout, 120 * time.Second},
		{"tls.client_auth", c.TLS.ClientAuth, ClientAuthOptional},
		{"tls.min_version", c.TLS.MinVersion, TLSVersion12},
		{"tls.handshake_timeout", c.TLS.HandshakeTimeout, 10 * time.Second},
		{"tls.idle_timeout", c.TLS.IdleTimeout, 120 * time.Second},
		{"tls.backpressure_timeout", c.TLS.BackpressureTimeout, 30 * time.Second},
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
		{"filter.min_severity", c.Filter.MinSeverity, "7"},
		{"ring.mode", c.Ring.Mode, RingModeMemory},
		{"ring.max_bytes", c.Ring.MaxBytes, int64(512 << 20)},
		{"ring.redis.stream", c.RingRedis.Stream, "syslog:ring"},
		{"ring.redis.group", c.RingRedis.Group, "syslog-ring"},
		{"ring.redis.claim_idle", c.RingRedis.ClaimIdle, 30 * time.Second},
		{"sink.type", c.Sink.Type, SinkTypeNATS},
		{"sink.workers", c.Sink.Workers, 8},
		{"sink.batch_max_bytes", c.Sink.BatchMaxBytes, int64(16 << 20)},
		{"sink.boot_timeout", c.Sink.BootTimeout, 300 * time.Second},
		{"sink.nats.urls", c.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}},
		{"sink.nats.mode", c.SinkNATS.Mode, NATSModeJetStream},
		{"sink.nats.stream", c.SinkNATS.Stream, "LOGHUB_SYSLOG"},
		{"sink.nats.subjects", c.SinkNATS.Subjects, SubjectsExact},
		{"sink.redis.stream", c.SinkRedis.Stream, "loghub.{space}.syslog"},
		{"sink.postgres.table", c.SinkPG.Table, "loghub.inbox"},
		{"sink.mysql.table", c.SinkMySQL.Table, "syslog_events"},
		{"sink.postgres.addr", c.SinkPG.Addr, "127.0.0.1:5432"},
		{"sink.mysql.addr", c.SinkMySQL.Addr, "127.0.0.1:3306"},
		{"loghub.eb_url", c.LogHub.EBURL, ""},
		{"loghub.beacon_interval", c.LogHub.BeaconInterval, 15 * time.Second},
		{"loghub.ctl_namespace", c.LogHub.CtlNamespace, "loghub._ctl"},
		{"admin.bind", c.Admin.Bind, "127.0.0.1:9102"},
		{"shutdown.drain_timeout", c.Shutdown.DrainTimeout, 30 * time.Second},
	}
	for _, ck := range checks {
		if !reflect.DeepEqual(ck.got, ck.want) {
			t.Errorf("%s = %#v, want %#v", ck.name, ck.got, ck.want)
		}
	}
	if c.LogHub.BeaconSubject() != "loghub._ctl.beacon" {
		t.Errorf("beacon subject = %q", c.LogHub.BeaconSubject())
	}
}

// SPEC §16.6: every write path hangs off server.data_dir.
func TestDerivedPaths(t *testing.T) {
	l := mustLoad(t, []string{"--server.data_dir=/srv/x"}, nil)
	c := l.Config
	if c.RingSQL.Path != "/srv/x/ring.db" || c.SinkSQL.Path != "/srv/x/events.db" ||
		c.Emergency.Dir != "/srv/x/emergency" {
		t.Fatalf("derived paths: %q %q %q", c.RingSQL.Path, c.SinkSQL.Path, c.Emergency.Dir)
	}
	if l.Notes["ring.sqlite.path"] == "" {
		t.Error("a derived path carries no note, so the dump would present it as a default")
	}
	// An explicit path wins and is made absolute against the working dir.
	c = mustLoad(t, []string{"--server.data_dir=/srv/x", "--ring.sqlite.path=q/ring.db",
		"--emergency.dir=/var/emg"}, nil).Config
	if c.RingSQL.Path != "/work/q/ring.db" || c.Emergency.Dir != "/var/emg" ||
		c.SinkSQL.Path != "/srv/x/events.db" {
		t.Fatalf("explicit paths: %q %q %q", c.RingSQL.Path, c.Emergency.Dir, c.SinkSQL.Path)
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
		if got := l.Config.RingSQL.Path; got != filepath.Join(tc.want, "ring.db") {
			t.Errorf("%v %v: ring path = %q", tc.args, tc.env, got)
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
	if l.SetIn["ring.mode"] || l.Source["ring.mode"] != FromDefault {
		t.Fatalf("ring.mode set=%v source=%v", l.SetIn["ring.mode"], l.Source["ring.mode"])
	}
}

// SPEC §16.1 and §16.2: the default env rule, and the names the skills fixed.
func TestEnvNames(t *testing.T) {
	want := map[string]string{
		"sink.nats.urls":                    "SYSLOG_SINK_NATS_URLS",
		"ring.capacity_items":               "SYSLOG_RING_CAPACITY_ITEMS",
		"ring.redis.claim_idle":             "SYSLOG_RING_REDIS_CLAIM_IDLE",
		"server.data_dir":                   "DATADIR",
		"syslog.default_priority":           "SYSLOG_DEFAULT_PRIORITY",
		"syslog.rfc3164_timezone":           "SYSLOG_RFC3164_TIMEZONE",
		"syslog.max_message_bytes":          "SYSLOG_MAX_MESSAGE_BYTES",
		"syslog.payload_raw":                "SYSLOG_PAYLOAD_RAW",
		"syslog.legacy_charset":             "SYSLOG_LEGACY_CHARSET",
		"syslog.accept_malformed":           "SYSLOG_ACCEPT_MALFORMED",
		"syslog.sd_max_elements":            "SYSLOG_SD_MAX_ELEMENTS",
		"syslog.sd_max_params":              "SYSLOG_SD_MAX_PARAMS",
		"loghub.eb_url":                     "LOGHUB_EB_URL",
		"loghub.beacon_interval":            "LOGHUB_BEACON_INTERVAL",
		"loghub.ctl_namespace":              "LOGHUB_CTL_NAMESPACE",
		"sink.sqlite.keep_files":            "SYSLOG_SINK_SQLITE_KEEP_FILES",
		"tls.client_auth":                   "SYSLOG_TLS_CLIENT_AUTH",
		"proxy.trusted":                     "SYSLOG_PROXY_TRUSTED",
		"sink.postgres.pool_size":           "SYSLOG_SINK_POSTGRES_POOL_SIZE",
		"listen.unix_read_buffer":           "SYSLOG_LISTEN_UNIX_READ_BUFFER",
		"shutdown.pre_stop_delay":           "SYSLOG_SHUTDOWN_PRE_STOP_DELAY",
		"sink.redis.write_buffer_each_conn": "SYSLOG_SINK_REDIS_WRITE_BUFFER_EACH_CONN",
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
		"LOGHUB_BEACON_INTERVAL":   "15",
		"LOGHUB_CTL_NAMESPACE":     "acme._ctl",
		"DATADIR":                  "/data",
	})
	c := l.Config
	if c.Syslog.MaxMessageBytes != 8192 || c.Syslog.DefaultPriority != 14 ||
		c.Syslog.RFC3164Timezone != "America/Sao_Paulo" || c.Syslog.PayloadRaw ||
		c.Syslog.LegacyCharset != CharsetCP1252 || c.Syslog.AcceptMalformed ||
		c.Syslog.SDMaxElements != 4 || c.Syslog.SDMaxParams != 5 {
		t.Fatalf("[syslog] from env = %+v", c.Syslog)
	}
	if c.LogHub.BeaconInterval != 15*time.Second || c.LogHub.CtlNamespace != "acme._ctl" {
		t.Fatalf("[loghub] from env = %+v", c.LogHub)
	}
	if c.Server.DataDir != "/data" || c.Emergency.Dir != "/data/emergency" {
		t.Fatalf("DATADIR: %q %q", c.Server.DataDir, c.Emergency.Dir)
	}
	if l.Origin["syslog.max_message_bytes"] != "env SYSLOG_MAX_MESSAGE_BYTES" {
		t.Fatalf("origin = %q", l.Origin["syslog.max_message_bytes"])
	}

	// The default-rule spelling of an exception is NOT read: a deploy that
	// used it would believe it took effect.
	c = mustLoad(t, nil, map[string]string{
		"SYSLOG_SYSLOG_MAX_MESSAGE_BYTES": "8KiB",
		"SYSLOG_SERVER_DATA_DIR":          "/elsewhere",
		"SYSLOG_LOGHUB_EB_URL":            "redis://x",
	}).Config
	if c.Syslog.MaxMessageBytes != 64<<10 || c.Server.DataDir != "/work/data" || c.Sink.Type != SinkTypeNATS {
		t.Fatalf("default-rule names were read: %d %q %q", c.Syslog.MaxMessageBytes, c.Server.DataDir, c.Sink.Type)
	}
}

// SPEC §16.2: LOGHUB_BEACON_INTERVAL is seconds in the skill, and a Go
// duration everywhere else in this server; both spellings work.
func TestBeaconIntervalSeconds(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"15": 15 * time.Second, "15s": 15 * time.Second, " 30 ": 30 * time.Second,
		"2m": 2 * time.Minute, "0": 0, "0s": 0, "1500ms": 1500 * time.Millisecond,
	} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw})
		if got := l.Config.LogHub.BeaconInterval; got != want {
			t.Errorf("LOGHUB_BEACON_INTERVAL=%q -> %s, want %s", raw, got, want)
		}
	}
	for _, raw := range []string{"-1", "-5s", "abc", "1.5", ""} {
		mustFail(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw}, "LOGHUB_BEACON_INTERVAL")
	}
	// The flag takes the same forms.
	if l := mustLoad(t, []string{"--loghub.beacon_interval=20"}, nil); l.Config.LogHub.BeaconInterval != 20*time.Second {
		t.Fatalf("flag: %s", l.Config.LogHub.BeaconInterval)
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
	c := mustLoad(t, []string{"--ring.mode=SQLITE", "--ring.sqlite.synchronous=normal",
		"--tcp.framing=LF", "--log.level=WARN", "--sink.type=None", "--tls.client_auth=Require",
		"--tls.client_ca=/ca.pem"}, nil).Config
	if c.Ring.Mode != RingModeSQLite || c.RingSQL.Synchronous != "NORMAL" || c.TCP.Framing != FramingLF ||
		c.Log.Level != "warn" || c.Sink.Type != SinkTypeNone || c.TLS.ClientAuth != ClientAuthRequire {
		t.Fatalf("enums not canonical: %q %q %q %q %q %q", c.Ring.Mode, c.RingSQL.Synchronous,
			c.TCP.Framing, c.Log.Level, c.Sink.Type, c.TLS.ClientAuth)
	}
	for _, tc := range []struct{ arg, want string }{
		{"--ring.mode=disk", "memory, sqlite, redis, none"},
		{"--sink.type=kafka", "nats, redis, sqlite, postgres, mysql, clickhouse, none"},
		{"--tcp.framing=octet-counting", "auto, octet, lf"},
		{"--tls.client_auth=maybe", "optional, require, none"},
		{"--tls.min_version=1.1", "1.2, 1.3"},
		{"--proxy.protocol=on", "off, optional, required"},
		{"--syslog.legacy_charset=utf-16", `"", cp1252, latin1`},
		{"--sink.nats.mode=stream", "jetstream, core"},
		{"--sink.nats.subjects=granular", "exact, facility-severity"},
		{"--sink.sqlite.rotate=weekly", "none, hourly, daily"},
		{"--sink.sqlite.synchronous=EXTRA", "FULL, NORMAL, OFF"},
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
	mustLoad(t, []string{"--listen.udp=", "--listen.tcp=", "--listen.tls=:6514",
		"--tls.cert_file=/c", "--tls.key_file=/k"}, nil)
}

// SPEC §13.2: two listeners on the same address and protocol, TCP and TLS on
// the same port included.
func TestTCPAndTLSCannotShareAnAddress(t *testing.T) {
	tlsArgs := []string{"--tls.cert_file=/c", "--tls.key_file=/k"}
	for _, pair := range [][2]string{
		{":1514", ":1514"},
		{"127.0.0.1:1514", ":1514"},    // the wildcard covers the concrete host
		{"0.0.0.0:6514", "[::1]:6514"}, // IPv4 wildcard against a v6 host
		{"[::]:6514", "10.0.0.1:6514"}, // v6 wildcard
		{"10.0.0.1:6514", "10.0.0.1:6514"},
		{"[::ffff:10.0.0.1]:6514", "10.0.0.1:6514"},
	} {
		mustFail(t, append([]string{"--listen.tcp=" + pair[0], "--listen.tls=" + pair[1]}, tlsArgs...),
			nil, "same TCP address")
	}
	for _, pair := range [][2]string{
		{":1514", ":6514"},
		{"10.0.0.1:6514", "10.0.0.2:6514"},
		{"127.0.0.1:0", "127.0.0.1:0"}, // the kernel picks two ports
	} {
		mustLoad(t, append([]string{"--listen.tcp=" + pair[0], "--listen.tls=" + pair[1]}, tlsArgs...), nil)
	}
	// UDP and TCP on the same port is the default and must pass.
	mustLoad(t, []string{"--listen.udp=:1514", "--listen.tcp=:1514"}, nil)
	mustFail(t, []string{"--listen.tcp=1514"}, nil, "not host:port")
	mustFail(t, []string{"--listen.tls_listeners=0", "--listen.tls=:6514", "--tls.cert_file=/c", "--tls.key_file=/k"},
		nil, "listen.tls_listeners")
}

func TestTLSListenerRules(t *testing.T) {
	mustFail(t, []string{"--listen.tls=:6514"}, nil, "tls.cert_file")
	mustFail(t, []string{"--listen.tls=:6514", "--tls.cert_file=/c"}, nil, "tls.key_file")
	mustFail(t, []string{"--tls.client_auth=require"}, nil, "tls.client_ca")
	mustLoad(t, []string{"--listen.tls=:6514", "--tls.cert_file=/c", "--tls.key_file=/k",
		"--tls.client_auth=require", "--tls.client_ca=/ca", "--tls.min_version=1.3"}, nil)
	mustFail(t, []string{"--tls.max_conns=-1"}, nil, "negative")
}

// CheckFiles is the disk half of the listen.tls row, run once at boot.
// selfSigned writes a PEM certificate and its key, for the CheckFiles tests.
func selfSigned(t *testing.T, dir, name string) (cert, key string) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	cert, key = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	writeFile(t, cert, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, key, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})))
	return cert, key
}

// SPEC §13.1 step 1, §13.2: the TLS files are checked at step 1 -- readable,
// AND parseable, AND the key matches the certificate, AND the CA holds a
// certificate. Review finding: only the permissions were checked, so garbage
// surfaced at step 12, after the sink wait; with an unreachable sink the boot
// ended with exit 3 at boot_timeout without ever logging the TLS error.
func TestCheckFiles(t *testing.T) {
	dir := t.TempDir()
	cert, key := selfSigned(t, dir, "server")
	_, otherKey := selfSigned(t, dir, "other")
	garbage := filepath.Join(dir, "garbage.pem")
	writeFile(t, garbage, "x")
	check := func(extra ...string) error {
		t.Helper()
		return CheckFiles(mustLoad(t, append([]string{"--listen.tls=:6514"}, extra...), nil).Config)
	}
	if err := check("--tls.cert_file="+cert, "--tls.key_file="+key); err != nil {
		t.Fatal(err)
	}
	if err := check("--tls.cert_file="+cert, "--tls.key_file="+key, "--tls.client_ca="+cert); err != nil {
		t.Fatalf("a CA file with a certificate: %v", err)
	}
	for _, c := range []struct {
		name string
		args []string
		want string
	}{
		{"missing key", []string{"--tls.cert_file=" + cert, "--tls.key_file=" + filepath.Join(dir, "missing.pem")}, "tls.key_file"},
		{"garbage cert", []string{"--tls.cert_file=" + garbage, "--tls.key_file=" + key}, "tls.cert_file"},
		{"garbage key", []string{"--tls.cert_file=" + cert, "--tls.key_file=" + garbage}, "tls.key_file"},
		{"key of another cert", []string{"--tls.cert_file=" + cert, "--tls.key_file=" + otherKey}, "tls.key_file"},
		{"CA without a certificate", []string{"--tls.cert_file=" + cert, "--tls.key_file=" + key, "--tls.client_ca=" + garbage}, "tls.client_ca"},
	} {
		if err := check(c.args...); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one naming %s", c.name, err, c.want)
		}
	}
	// Without a TLS listener nothing is read.
	if err := CheckFiles(mustLoad(t, []string{"--tls.cert_file=/nope"}, nil).Config); err != nil {
		t.Fatal(err)
	}
}

// Review findings on the validation of SPEC §13.2.
func TestValidationReviewFindings(t *testing.T) {
	// NaN and ±Inf parse as floats and pass every range check.
	for _, v := range []string{"NaN", "nan", "Inf", "-Inf"} {
		mustFail(t, []string{"--ring.high_water=" + v}, nil, "ring.high_water")
		mustFail(t, []string{"--ratelimit.enabled=true", "--ratelimit.rate=" + v}, nil, "ratelimit.rate")
	}
	// An empty admin.bind bound every interface on a random port.
	mustFail(t, []string{"--admin.enabled=true", "--admin.bind="}, nil, "admin.bind")
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind="}, nil)
	// A host name on the port of the other stream listener may be the same
	// address: refused; two distinct IP literals may share a port.
	mustFail(t, []string{"--listen.tcp=localhost:19801", "--listen.tls=127.0.0.1:19801",
		"--tls.cert_file=/c", "--tls.key_file=/k"}, nil, "listen.tcp")
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:19801", "--listen.tls=127.0.0.2:19801",
		"--tls.cert_file=/c", "--tls.key_file=/k"}, nil)
	// Files written outside data_dir are allowed, with a warning (SPEC §20.16).
	l := mustLoad(t, []string{"--server.data_dir=/data", "--ring.mode=sqlite", "--ring.sqlite.path=/fast/ring.db"}, nil)
	if !strings.Contains(strings.Join(l.Warnings, "\n"), "ring.sqlite.path") {
		t.Fatalf("no warning for a ring outside data_dir: %q", l.Warnings)
	}
	l = mustLoad(t, []string{"--server.data_dir=/data", "--ring.mode=sqlite", "--ring.sqlite.path=/data/sub/ring.db"}, nil)
	if strings.Contains(strings.Join(l.Warnings, "\n"), "outside server.data_dir") {
		t.Fatalf("a path under data_dir was warned about: %q", l.Warnings)
	}
}

func TestProxyRules(t *testing.T) {
	mustFail(t, []string{"--proxy.protocol=optional", "--proxy.trusted="}, nil, "proxy.trusted")
	mustFail(t, []string{"--proxy.protocol=required", "--proxy.trusted=10.0.0.0/33"}, nil, "proxy.trusted")
	// An invalid CIDR fails even with the protocol off: a typo must fail loudly.
	mustFail(t, []string{"--proxy.trusted=not-a-cidr"}, nil, "proxy.trusted")
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
	c = mustLoad(t, []string{"--ratelimit.exempt=10.1.2.3,fd00::/8"}, nil).Config
	if len(c.RateLimit.Exempt) != 2 || c.RateLimit.Exempt[0].String() != "10.1.2.3/32" {
		t.Fatalf("exempt = %v", c.RateLimit.Exempt)
	}
}

func TestRingAndSinkSQLiteCannotShareAFile(t *testing.T) {
	_, err := load(t, []string{
		"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=/d/same.db", "--sink.sqlite.path=/d/./same.db",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "QUEUE") {
		t.Fatalf("err = %v, want the queue/destination error", err)
	}
	// A relative path is resolved before the comparison.
	mustFail(t, []string{"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=x.db", "--sink.sqlite.path=/work/x.db"}, nil, "QUEUE")
	// Different files are allowed, with a warning about the double fsync; the
	// derived defaults are different files.
	l := mustLoad(t, []string{"--ring.mode=sqlite", "--sink.type=sqlite"}, nil)
	if !hasWarning(l, "TWO fsyncs") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
}

// SPEC §9.7: ring=redis and sink=redis on the same server degrade the ring to
// memory with a warning; on distinct servers both stay.
func TestRingRedisOnTheSinkServerDegrades(t *testing.T) {
	l := mustLoad(t, []string{"--ring.mode=redis", "--sink.type=redis"}, nil)
	if l.Config.Ring.Mode != RingModeMemory {
		t.Fatalf("ring.mode = %q, want memory", l.Config.Ring.Mode)
	}
	if !hasWarning(l, "effective ring is memory") || !strings.Contains(l.Notes["ring.mode"], "degraded") {
		t.Fatalf("warnings = %v notes = %v", l.Warnings, l.Notes)
	}
	// Address spelling and order do not hide the same server.
	l = mustLoad(t, []string{"--ring.mode=redis", "--sink.type=redis",
		"--ring.redis.addrs=B:6379, a:6379", "--sink.redis.addrs=a:6379,b:6379"}, nil)
	if l.Config.Ring.Mode != RingModeMemory {
		t.Fatalf("same server with reordered addrs kept ring.mode=%q", l.Config.Ring.Mode)
	}
	for _, args := range [][]string{
		{"--ring.redis.db=1"},
		{"--ring.redis.addrs=10.0.0.9:6379"},
	} {
		l = mustLoad(t, append([]string{"--ring.mode=redis", "--sink.type=redis"}, args...), nil)
		if l.Config.Ring.Mode != RingModeRedis || hasWarning(l, "effective ring is memory") {
			t.Fatalf("%v: ring.mode = %q, warnings %v", args, l.Config.Ring.Mode, l.Warnings)
		}
	}
	// ring=redis with another sink is untouched.
	if l := mustLoad(t, []string{"--ring.mode=redis"}, nil); l.Config.Ring.Mode != RingModeRedis {
		t.Fatalf("ring.mode = %q", l.Config.Ring.Mode)
	}
	mustFail(t, []string{"--ring.mode=redis", "--ring.redis.addrs="}, nil, "ring.redis.addrs")
	mustFail(t, []string{"--ring.mode=redis", "--ring.redis.group="}, nil, "ring.redis.group")
}

// SPEC §10.5, §10.6 and §10.10: the relational destinations and ClickHouse
// share one connection budget rule.
func TestRelationalWorkerDefaults(t *testing.T) {
	for _, tc := range []struct {
		sink string
		pool func(*Config) int
	}{
		{"postgres", func(c *Config) int { return c.SinkPG.PoolSize }},
		{"mysql", func(c *Config) int { return c.SinkMySQL.PoolSize }},
		{"clickhouse", func(c *Config) int { return c.SinkCH.PoolSize }},
	} {
		t.Run(tc.sink, func(t *testing.T) {
			// Not set: lowered to 4 with a warning, pool derived.
			l := mustLoad(t, []string{"--sink.type=" + tc.sink}, nil)
			if l.Config.Sink.Workers != dbDefaultWorkers {
				t.Fatalf("workers = %d, want %d", l.Config.Sink.Workers, dbDefaultWorkers)
			}
			if got := tc.pool(l.Config); got != dbDefaultWorkers+1 {
				t.Fatalf("pool = %d, want %d", got, dbDefaultWorkers+1)
			}
			if !hasWarning(l, "lowered") || l.Notes["sink.workers"] == "" ||
				l.Notes["sink."+tc.sink+".pool_size"] == "" {
				t.Fatalf("warnings = %v notes = %v", l.Warnings, l.Notes)
			}
			// Set explicitly: respected, no lowering.
			l = mustLoad(t, []string{"--sink.type=" + tc.sink, "--sink.workers=12"}, nil)
			if l.Config.Sink.Workers != 12 {
				t.Fatalf("explicit workers = %d", l.Config.Sink.Workers)
			}
			if hasWarning(l, "lowered") {
				t.Fatalf("lowered an explicit value: %v", l.Warnings)
			}
			// Above the warn threshold.
			l = mustLoad(t, []string{"--sink.type=" + tc.sink, "--sink.workers=32"}, nil)
			if !hasWarning(l, "lock contention") {
				t.Fatalf("warnings = %v", l.Warnings)
			}
			// A pool below the workers makes one wait on another's connection.
			mustFail(t, []string{"--sink.type=" + tc.sink, "--sink.workers=8",
				"--sink." + tc.sink + ".pool_size=4"}, nil, "pool_size=4")
			// ring=none with more writers than connections hides a queue.
			l = mustLoad(t, []string{"--sink.type=" + tc.sink, "--ring.mode=none"}, nil)
			if !hasWarning(l, "inside it") {
				t.Fatalf("warnings = %v", l.Warnings)
			}
		})
	}
}

// SPEC §10.6 and §13.2.
func TestMySQLValidation(t *testing.T) {
	base := []string{"--sink.type=mysql"}
	l := mustLoad(t, base, nil)
	if c := l.Config.SinkMySQL; c.Addr != "127.0.0.1:3306" || c.Database != "syslog" || c.Table != "syslog_events" {
		t.Fatalf("defaults = %+v", c)
	}
	// The placeholder ceiling is exact: eighteen per row, 65535 per statement.
	if mysqlMaxBatch != 3640 || mysqlMaxBatch*mysqlColumns > 65535 || (mysqlMaxBatch+1)*mysqlColumns <= 65535 {
		t.Fatalf("mysqlMaxBatch = %d is not the largest row count under 65535 placeholders", mysqlMaxBatch)
	}
	mustLoad(t, append(base, "--sink.batch=3640"), nil)
	// Case is kept: MySQL table names are case-sensitive on Linux.
	if l := mustLoad(t, append(base, "--sink.mysql.table=Logs.Events"), nil); l.Config.SinkMySQL.Table != "Logs.Events" {
		t.Fatalf("table = %q", l.Config.SinkMySQL.Table)
	}

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.batch=3641"}, "3640"},
		{[]string{"--sink.mysql.table=events; drop table x"}, "plain identifier"},
		{[]string{"--sink.mysql.table=ev`ents"}, "plain identifier"},
		{[]string{"--sink.mysql.table=a.b.c"}, "plain identifier"},
		{[]string{"--sink.mysql.addr="}, "sink.mysql.addr"},
		{[]string{"--sink.mysql.username="}, "sink.mysql.username"},
		{[]string{"--sink.mysql.database="}, "sink.mysql.database"},
		{[]string{"--sink.mysql.addr=/run/mysqld/mysqld.sock", "--sink.mysql.tls=true"}, "unix socket"},
		{[]string{"--sink.mysql.tls_ca=/x"}, "sink.mysql.tls_ca"},
	} {
		mustFail(t, append(base, tc.args...), nil, tc.want)
	}
	// The batch ceiling belongs to mysql alone.
	mustLoad(t, []string{"--sink.type=redis", "--sink.batch=5000"}, nil)
}

// SPEC §10.10 and §13.2.
func TestClickHouseValidation(t *testing.T) {
	base := []string{"--sink.type=clickhouse"}
	l := mustLoad(t, base, nil)
	if c := l.Config.SinkCH; len(c.Addrs) != 1 || c.Addrs[0] != "127.0.0.1:9000" || c.Database != "syslog" ||
		c.Table != "inbox_syslog" || c.Username != "syslog" {
		t.Fatalf("defaults = %+v", c)
	}
	// Case is kept, several addresses are a list, and the native protocol has
	// no placeholder ceiling for sink.batch.
	if l := mustLoad(t, append(base, "--sink.clickhouse.table=Logs.Inbox"), nil); l.Config.SinkCH.Table != "Logs.Inbox" {
		t.Fatalf("table = %q", l.Config.SinkCH.Table)
	}
	mustLoad(t, append(base, "--sink.clickhouse.addrs=a:9000,b:9000", "--sink.batch=20000"), nil)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.clickhouse.table=events; drop table x"}, "plain identifier"},
		{[]string{"--sink.clickhouse.table=a.b.c"}, "plain identifier"},
		{[]string{"--sink.clickhouse.table=1inbox"}, "plain identifier"},
		{[]string{"--sink.clickhouse.addrs="}, "at least one address"},
		{[]string{"--sink.clickhouse.addrs=clickhouse"}, "not host:port"},
		{[]string{"--sink.clickhouse.username="}, "sink.clickhouse.username"},
		{[]string{"--sink.clickhouse.database="}, "sink.clickhouse.database"},
		{[]string{"--sink.clickhouse.pool_size=-1"}, "negative"},
		{[]string{"--sink.clickhouse.tls_ca=/x"}, "sink.clickhouse.tls_ca"},
	} {
		mustFail(t, append(base, tc.args...), nil, tc.want)
	}
	// The rules bite only when clickhouse is the sink.
	mustLoad(t, []string{"--sink.type=sqlite", "--sink.clickhouse.table=a;b"}, nil)
}

// SPEC §10.1 and §10.10: the worst chunk the ClickHouse boot compares with the
// memory limit is the chunk ceiling, or one JLH when a single one is larger.
func TestWorstChunkBytes(t *testing.T) {
	l := mustLoad(t, nil, nil)
	if got := WorstChunkBytes(l.Config); got != 16<<20 {
		t.Errorf("default = %d, want sink.batch_max_bytes", got)
	}
	l = mustLoad(t, []string{"--sink.batch_max_bytes=64KiB"}, nil)
	if got := WorstChunkBytes(l.Config); got != MaxEventBytes(l.Config) {
		t.Errorf("a JLH above the ceiling = %d, want the largest JLH %d", got, MaxEventBytes(l.Config))
	}
}

func TestPostgresValidation(t *testing.T) {
	base := []string{"--sink.type=postgres"}
	mustLoad(t, append(base, "--sink.postgres.table=logs.syslog_events"), nil)
	mustFail(t, append(base, "--sink.postgres.table=Events"), nil, "lowercase")
	mustFail(t, append(base, "--sink.postgres.database="), nil, "sink.postgres.database")
}

// SPEC §10.7 and §13.2.
func TestNATSValidation(t *testing.T) {
	base := []string{"--sink.type=nats"}
	l := mustLoad(t, base, nil)
	if c := l.Config.SinkNATS; c.Stream != "LOGHUB_SYSLOG" || len(c.URLs) != 1 || c.URLs[0] != "nats://127.0.0.1:4222" {
		t.Fatalf("defaults = %+v", c)
	}
	// One method at a time is fine, and a cluster is a list.
	mustLoad(t, append(base, "--sink.nats.username=u", "--sink.nats.password=p"), nil)
	mustLoad(t, append(base, "--sink.nats.token=t"), nil)
	mustLoad(t, append(base, "--sink.nats.credentials=/etc/syslog/sls.creds"), nil)
	mustLoad(t, append(base, "--sink.nats.urls=nats://a:4222,nats://b:4222"), nil)
	mustLoad(t, append(base, "--sink.nats.stream=LOGS"), nil)
	// The stream is ignored in core mode, so a dotted name there is no error.
	mustLoad(t, append(base, "--sink.nats.mode=core", "--sink.nats.stream=a.b"), nil)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.nats.urls="}, "sink.nats.urls"},
		{[]string{"--sink.nats.stream=SYS.LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=SYS LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream="}, "sink.nats.stream"},
		{[]string{"--sink.nats.token=t", "--sink.nats.username=u"}, "only one of"},
		{[]string{"--sink.nats.credentials=/c", "--sink.nats.token=t"}, "only one of"},
		{[]string{"--sink.nats.password=p"}, "without sink.nats.username"},
		{[]string{"--sink.nats.tls_ca=/x"}, "sink.nats.tls_ca"},
		{[]string{"--loghub.ctl_namespace="}, "loghub.ctl_namespace"},
		{[]string{"--loghub.ctl_namespace=loghub.>"}, "publishable"},
		{[]string{"--loghub.ctl_namespace=loghub..ctl"}, "publishable"},
	} {
		mustFail(t, append(base, tc.args...), nil, tc.want)
	}
	// There is no subject key: it is derived (SPEC §10.7).
	mustFail(t, append(base, "--sink.nats.subject=x"), nil, "unknown parameter")
	// Core is best effort, and the boot says so.
	if l := mustLoad(t, append(base, "--sink.nats.mode=core"), nil); !hasWarning(l, "best effort") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	if l := mustLoad(t, base, nil); hasWarning(l, "best effort") {
		t.Fatalf("jetstream warned best effort: %v", l.Warnings)
	}
}

func TestSQLiteForcesOneWorker(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=sqlite", "--sink.workers=8"}, nil)
	if l.Config.Sink.Workers != 1 {
		t.Fatalf("workers = %d, want 1", l.Config.Sink.Workers)
	}
	if !hasWarning(l, "SQLITE_BUSY") || l.Notes["sink.workers"] == "" {
		t.Fatalf("warnings = %v notes = %v", l.Warnings, l.Notes)
	}
}

func TestExposureWarning(t *testing.T) {
	l := mustLoad(t, []string{"--listen.udp=:1514"}, nil)
	if !hasWarning(l, "ratelimit.enabled=false") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	// Loopback only: no warning, nothing off this host can reach it.
	l = mustLoad(t, []string{"--listen.udp=127.0.0.1:1514", "--listen.tcp=[::1]:1514"}, nil)
	if hasWarning(l, "ratelimit.enabled=false") {
		t.Fatalf("warned about a loopback bind: %v", l.Warnings)
	}
	// A TLS listener counts too.
	l = mustLoad(t, []string{"--listen.udp=127.0.0.1:1514", "--listen.tcp=", "--listen.tls=:6514",
		"--tls.cert_file=/c", "--tls.key_file=/k"}, nil)
	if !hasWarning(l, "listen.tls=:6514") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	// The limiter on silences it.
	if l := mustLoad(t, []string{"--ratelimit.enabled"}, nil); hasWarning(l, "ratelimit.enabled=false") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
}

func TestDurabilityOnDatagramOnlyWarns(t *testing.T) {
	l := mustLoad(t, []string{"--ring.mode=sqlite", "--listen.tcp=", "--listen.udp=:1514"}, nil)
	if !hasWarning(l, "already have dropped") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	if l := mustLoad(t, []string{"--ring.mode=sqlite"}, nil); hasWarning(l, "already have dropped") {
		t.Fatalf("warned with TCP configured: %v", l.Warnings)
	}
}

func TestSyslogAndFilterRanges(t *testing.T) {
	mustLoad(t, []string{"--filter.min_severity=warning"}, nil)
	mustLoad(t, []string{"--filter.min_severity=WARNING", "--filter.facilities=local0, AUTH, 16, console, cron2"}, nil)
	mustLoad(t, []string{"--syslog.default_priority=0", "--syslog.max_message_bytes=480"}, nil)
	mustLoad(t, []string{"--syslog.default_priority=191", "--syslog.max_message_bytes=16MiB"}, nil)
	mustLoad(t, []string{"--syslog.rfc3164_timezone=America/Sao_Paulo"}, nil)
	for _, tc := range []struct{ arg, want string }{
		{"--filter.min_severity=loud", "filter.min_severity"},
		{"--filter.min_severity=8", "filter.min_severity"},
		{"--filter.facilities=nope", "filter.facilities"},
		{"--filter.facilities=24", "filter.facilities"},
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

func TestFilterResolve(t *testing.T) {
	c := mustLoad(t, []string{"--filter.min_severity=Warning", "--filter.facilities=local0,auth,4,3"}, nil).Config
	sev, facs, err := c.Filter.Resolve()
	if err != nil || sev != 4 || !reflect.DeepEqual(facs, []int{16, 4, 4, 3}) {
		t.Fatalf("Resolve = %d %v %v", sev, facs, err)
	}
	sev, facs, err = Defaults().Filter.Resolve()
	if err != nil || sev != 7 || facs != nil {
		t.Fatalf("default Resolve = %d %v %v", sev, facs, err)
	}
}

// The labels the filter accepts are the RFC 5427 ones the converter emits.
func TestNamesAreRFC5427(t *testing.T) {
	fac := []string{"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "console", "cron2",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7"}
	sev := []string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}
	for i, n := range fac {
		if v, ok := ParseFacility(n); !ok || v != i {
			t.Errorf("facility %q = %d %v, want %d", n, v, ok, i)
		}
	}
	for i, n := range sev {
		if v, ok := ParseSeverity(n); !ok || v != i {
			t.Errorf("severity %q = %d %v, want %d", n, v, ok, i)
		}
	}
	if _, ok := ParseSeverity("8"); ok {
		t.Error("severity 8 accepted")
	}
}

// SPEC §16.5: zero is refused where it is not "unlimited" but "already expired"
// or "silently replaced by a default" -- and accepted where it means what it
// says.
func TestZeroDurationsAreRejectedWhereDangerous(t *testing.T) {
	for _, name := range []string{
		"sink.op_timeout", "sink.health_interval", "sink.health_timeout", "sink.boot_timeout",
		"sink.retry_backoff_min", "sink.retry_backoff_max", "ring.sqlite.commit_max_delay",
		"ring.sqlite.busy_timeout", "ring.redis.claim_idle", "sink.sqlite.busy_timeout",
		"ratelimit.sweep_interval", "shutdown.drain_timeout", "tcp.backpressure_timeout",
		"tls.backpressure_timeout", "tls.handshake_timeout", "proxy.header_timeout",
	} {
		mustFail(t, []string{"--" + name + "=0s"}, nil, name+": must be positive")
	}
	for _, arg := range []string{
		"--shutdown.pre_stop_delay=0s",
		"--sink.batch_max_delay=0s",
		"--sink.fail_timeout=0s",
		"--tcp.idle_timeout=0s",
		"--tls.idle_timeout=0s",
		"--loghub.beacon_interval=0",
	} {
		mustLoad(t, []string{arg}, nil)
	}
	mustFail(t, []string{"--sink.retry_backoff_min=3s"}, nil, "below sink.retry_backoff_min")
}

func TestTLSCAWithoutTLS(t *testing.T) {
	for _, sec := range []string{"ring.redis", "sink.redis", "sink.postgres", "sink.mysql", "sink.nats", "sink.clickhouse"} {
		mustFail(t, []string{"--" + sec + ".tls_ca=/x"}, nil, sec+".tls_ca")
		mustLoad(t, []string{"--" + sec + ".tls_ca=/x", "--" + sec + ".tls"}, nil)
	}
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
	writeFile(t, ini, "[ring]\nmode = memory\nbogus = 1\n")
	mustFail(t, []string{"--config=" + ini}, nil, "line 3")
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
		l, err := load(t, []string{arg, "--ring.mode=bogus"}, nil)
		if !errors.Is(err, ErrExitEarly) || l == nil {
			t.Fatalf("%s: err = %v", arg, err)
		}
	}
	l := mustLoad(t, []string{"--dump-config"}, nil)
	if !l.Meta.DumpConfig {
		t.Fatal("--dump-config not reported")
	}
}

// The dump must be loadable back, or it is only decoration. Secrets break that
// on purpose and the dump says so.
func TestDumpRoundTrips(t *testing.T) {
	l := mustLoad(t, []string{
		"--ring.mode=sqlite", "--sink.type=postgres", "--listen.tls=:6514",
		"--tls.cert_file=/c", "--tls.key_file=/k",
		"--listen.unix=/tmp/dev-log", "--listen.unix_mode=0640",
		"--filter.min_severity=info", "--syslog.max_message_bytes=32KiB",
		"--proxy.protocol=optional", "--proxy.trusted=10.0.0.0/8",
		"--loghub.beacon_interval=45", "--server.data_dir=rel",
		"--sink.redis.stream= padded ", "--syslog.rfc3164_timezone=Europe/Lisbon",
	}, nil)

	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("WARNING: secrets")) {
		t.Fatal("no secret was set, yet the dump claims it redacted one")
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, buf.String())
	back := mustLoad(t, []string{"--config=" + f}, nil)
	if !reflect.DeepEqual(back.Config, l.Config) {
		t.Fatalf("dump did not round-trip\n got %+v\nwant %+v\ndump:\n%s", back.Config, l.Config, buf.String())
	}
	// Adjusted and derived values say so, so the dump never presents them as
	// compiled-in defaults.
	for _, sub := range []string{"# lowered to 4", "# derived: sink.workers + 1",
		"# derived from server.data_dir", "# resolved to an absolute path", "# from flag --ring.mode"} {
		if !strings.Contains(buf.String(), sub) {
			t.Errorf("dump lacks %q", sub)
		}
	}
}

func TestSecretsAreRedacted(t *testing.T) {
	for _, key := range []string{
		"ring.redis.password", "sink.redis.password", "sink.postgres.password",
		"sink.mysql.password", "sink.nats.password", "sink.nats.token", "sink.clickhouse.password",
	} {
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
		if l.Values()[key] != "hunter2" {
			t.Errorf("%s redacted in Values(): a changed password would not count on SIGHUP", key)
		}
	}
	// A URL keeps its host and loses its userinfo.
	l := mustLoad(t, []string{"--sink.nats.urls=nats://u:hunter2@a:4222,nats://tok3n@b:4222,nats://c:4222"}, nil)
	got := l.Effective()["sink.nats.urls"]
	if strings.Contains(got, "hunter2") || strings.Contains(got, "tok3n") ||
		got != "nats://u:***@a:4222,nats://***@b:4222,nats://c:4222" {
		t.Fatalf("redacted urls = %q", got)
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

// SPEC §16.3: exactly these keys reload, and no [syslog] key is among them.
func TestReloadableKeysAreTheSpecList(t *testing.T) {
	want := []string{
		"filter.facilities", "filter.min_severity",
		"log.access", "log.access_sample", "log.level",
		"ratelimit.burst", "ratelimit.enabled", "ratelimit.exempt", "ratelimit.max_buckets",
		"ratelimit.rate", "ratelimit.sweep_interval", "ratelimit.v4_mask", "ratelimit.v6_mask",
		"sink.sqlite.keep_files",
	}
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
}

// SPEC §3.9: 12 x L + 8 KiB; 776 KiB with the 64 KiB default.
func TestMaxEventBytes(t *testing.T) {
	c := mustLoad(t, nil, nil).Config
	// 12 x 65536 + 8192 = 794624 = 776 KiB.
	if got := MaxEventBytes(c); got != 794624 {
		t.Fatalf("MaxEventBytes = %d, want 794624 (776 KiB)", got)
	}
	if got := MaxMySQLRowBytes(c); got != 794624+3*65536+512 {
		t.Fatalf("MaxMySQLRowBytes = %d", got)
	}
	c = mustLoad(t, nil, map[string]string{"SYSLOG_MAX_MESSAGE_BYTES": "480"}).Config
	if got := MaxEventBytes(c); got != 12*480+8192 {
		t.Fatalf("MaxEventBytes(480) = %d", got)
	}
	// Past ~85 KiB (84.7) the worst event no longer fits the default 1 MiB
	// max_payload, which is why the boot computes it instead of trusting a
	// factor.
	if MaxEventBytesFor(84<<10) > 1<<20 || MaxEventBytesFor(85<<10) <= 1<<20 {
		t.Fatalf("the 85 KiB threshold of SPEC §3.9 moved")
	}
}

// SPEC §13.2 and §15.4.
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
	// The 2x rule is about the memory ring only.
	if l := mustLoad(t, []string{"--ring.mode=sqlite"}, map[string]string{"GOMEMLIMIT": "256MiB"}); hasWarning(l, "GOMEMLIMIT") {
		t.Fatalf("sqlite ring warned: %v", l.Warnings)
	}
}

func TestStreamFor(t *testing.T) {
	c := mustLoad(t, nil, nil).Config
	if got := c.SinkRedis.StreamFor("acme.prod"); got != "loghub.acme.prod.syslog" {
		t.Fatalf("StreamFor = %q", got)
	}
	c = mustLoad(t, []string{"--sink.redis.stream=literal"}, nil).Config
	if got := c.SinkRedis.StreamFor("x"); got != "literal" {
		t.Fatalf("StreamFor literal = %q", got)
	}
	rc := c.SinkRedis.ClientConfig("go-syslog-server@h1")
	if rc.ClientName != "go-syslog-server@h1" || rc.RingScaleEachConn != 10 || rc.ReadBufferEachConn != 512<<10 {
		t.Fatalf("ClientConfig = %+v", rc)
	}
}

// Defaults is what other packages' tests build on: it must already be a
// configuration Validate accepts, apart from the derived paths.
func TestDefaultsValidate(t *testing.T) {
	c := Defaults()
	if c.Sink.Type != SinkTypeNATS || c.Server.DataDir != "./data" || c.RingSQL.Path != "" {
		t.Fatalf("Defaults = %+v", c)
	}
	if _, err := Validate(c, nil); err != nil {
		t.Fatal(err)
	}
}

// The filter package compiles only RFC 5427 names and numbers (SPEC §7); a
// synonym accepted here would pass validation and then fail the boot there.
func TestFacilityAndSeverityHaveNoAliases(t *testing.T) {
	for _, s := range []string{"security", "alert", "clock", "solaris-cron"} {
		if _, ok := ParseFacility(s); ok {
			t.Errorf("facility alias %q accepted", s)
		}
	}
	for _, s := range []string{"warn", "error", "panic", "critical", "emergency", "informational"} {
		if _, ok := ParseSeverity(s); ok {
			t.Errorf("severity alias %q accepted", s)
		}
	}
}

// SPEC §9.5: a write attempt longer than the claim window would let another
// replica steal a live batch.
func TestRedisRingClaimIdleMustCoverTheWrite(t *testing.T) {
	mustLoad(t, []string{"--ring.mode=redis", "--ring.redis.claim_idle=30s", "--sink.op_timeout=5s"}, nil)
	mustFail(t, []string{"--ring.mode=redis", "--ring.redis.claim_idle=10s", "--sink.op_timeout=5s"}, nil,
		"ring.redis.claim_idle")
}

func TestRateLimitMaxBucketsMustBePositive(t *testing.T) {
	mustFail(t, []string{"--ratelimit.enabled=true", "--ratelimit.max_buckets=0"}, nil, "ratelimit.max_buckets")
	mustLoad(t, []string{"--ratelimit.enabled=false", "--ratelimit.max_buckets=0"}, nil)
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
