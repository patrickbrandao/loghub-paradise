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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/convert"
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
		// 8, the parent's: the workers share one NATS connection, so there is
		// no pool to size them against (the PostgreSQL sibling uses 4).
		{"sink.workers", c.Sink.Workers, 8},
		{"sink.batch", c.Sink.Batch, 256},
		{"sink.batch_max_bytes", c.Sink.BatchMaxBytes, int64(16 << 20)},
		{"sink.boot_timeout", c.Sink.BootTimeout, 300 * time.Second},
		{"sink.nats.urls", c.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}},
		{"sink.nats.mode", c.SinkNATS.Mode, NATSModeJetStream},
		{"sink.nats.stream", c.SinkNATS.Stream, "LOGHUB_SYSLOG"},
		{"sink.nats.username", c.SinkNATS.Username, ""},
		{"sink.nats.password", c.SinkNATS.Password, ""},
		{"sink.nats.token", c.SinkNATS.Token, ""},
		{"sink.nats.credentials", c.SinkNATS.Credentials, ""},
		{"loghub.eb_url", c.LogHub.EBURL, ""},
		{"loghub.beacon_interval", c.LogHub.BeaconInterval, 15 * time.Second},
		{"loghub.ctl_namespace", c.LogHub.CtlNamespace, "loghub._ctl"},
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
	if c.LogHub.BeaconSubject() != "loghub._ctl.beacon" {
		t.Errorf("beacon subject = %q", c.LogHub.BeaconSubject())
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
		"loghub.beacon_interval":   "LOGHUB_BEACON_INTERVAL",
		"loghub.ctl_namespace":     "LOGHUB_CTL_NAMESPACE",
		"sink.nats.urls":           "SYSLOG_SINK_NATS_URLS",
		"sink.nats.credentials":    "SYSLOG_SINK_NATS_CREDENTIALS",
		"sink.nats.mode":           "SYSLOG_SINK_NATS_MODE",
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
		"LOGHUB_BEACON_INTERVAL":   "30",
		"LOGHUB_CTL_NAMESPACE":     "acme._ctl",
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
	if c.LogHub.BeaconInterval != 30*time.Second || c.LogHub.CtlNamespace != "acme._ctl" {
		t.Fatalf("[loghub] from env = %+v", c.LogHub)
	}
	if l.Origin["syslog.max_message_bytes"] != "env SYSLOG_MAX_MESSAGE_BYTES" {
		t.Fatalf("origin = %q", l.Origin["syslog.max_message_bytes"])
	}

	// The default-rule spelling of an exception is NOT read: a deploy that
	// used it would believe it took effect.
	l = mustLoad(t, nil, map[string]string{
		"SYSLOG_SYSLOG_MAX_MESSAGE_BYTES": "8KiB",
		"SYSLOG_SERVER_DATA_DIR":          "/elsewhere",
		"SYSLOG_LOGHUB_EB_URL":            "nats://elsewhere:4222",
		"SYSLOG_LOGHUB_BEACON_INTERVAL":   "99",
		"SYSLOG_LOGHUB_CTL_NAMESPACE":     "elsewhere._ctl",
	})
	c = l.Config
	if c.Syslog.MaxMessageBytes != 64<<10 || c.Server.DataDir != "/work/data" ||
		c.LogHub.EBURL != "" || !reflect.DeepEqual(c.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}) ||
		c.LogHub.BeaconInterval != 15*time.Second || c.LogHub.CtlNamespace != "loghub._ctl" {
		t.Fatalf("default-rule names were read: %d %q %q %q %+v", c.Syslog.MaxMessageBytes,
			c.Server.DataDir, c.LogHub.EBURL, c.SinkNATS.URLs, c.LogHub)
	}
}

// SPEC §16.2: LOGHUB_BEACON_INTERVAL is seconds in the skill, and a Go
// duration everywhere else in this server; both spellings work, and 0 turns
// the periodic beacon off (SPEC §11).
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
	// The flag and the file take the same forms.
	if l := mustLoad(t, []string{"--loghub.beacon_interval=20"}, nil); l.Config.LogHub.BeaconInterval != 20*time.Second {
		t.Fatalf("flag: %s", l.Config.LogHub.BeaconInterval)
	}
	if l := mustLoad(t, []string{"--loghub.beacon_interval=0"}, nil); l.Config.LogHub.BeaconInterval != 0 {
		t.Fatalf("flag 0: %s", l.Config.LogHub.BeaconInterval)
	}
	ini := filepath.Join(t.TempDir(), "c.ini")
	writeFile(t, ini, "[loghub]\nbeacon_interval = 45\n")
	if l := mustLoad(t, []string{"--config=" + ini}, nil); l.Config.LogHub.BeaconInterval != 45*time.Second {
		t.Fatalf("file: %s", l.Config.LogHub.BeaconInterval)
	}
	// An ordinary duration still refuses the bare number: the seconds form is
	// the skill's, for this one key.
	mustFail(t, []string{"--sink.op_timeout=15"}, nil, "unit suffix")
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

// SPEC §10.1: the workers share the one NATS connection, so there is no pool
// to derive and nothing to adjust. 8 is the default and the only rule is >= 1.
func TestSinkWorkers(t *testing.T) {
	// The whole default configuration, with GOMEMLIMIT set, warns about
	// nothing: a WARN on every clean boot teaches operators to ignore WARN.
	l := mustLoad(t, nil, map[string]string{"GOMEMLIMIT": "1GiB"})
	if len(l.Warnings) != 0 {
		t.Fatalf("default boot warns: %q", l.Warnings)
	}
	if l.Config.Sink.Workers != 8 || l.Notes["sink.workers"] != "" {
		t.Fatalf("workers = %d note %q, want 8 and no note", l.Config.Sink.Workers, l.Notes["sink.workers"])
	}
	for _, n := range []string{"1", "17", "64"} {
		l := mustLoad(t, []string{"--sink.workers=" + n}, map[string]string{"GOMEMLIMIT": "1GiB"})
		if strconv.Itoa(l.Config.Sink.Workers) != n || len(l.Warnings) != 0 {
			t.Fatalf("workers %s: got %d, warnings %q", n, l.Config.Sink.Workers, l.Warnings)
		}
	}
	mustFail(t, []string{"--sink.workers=0"}, nil, "sink.workers")
	mustFail(t, []string{"--sink.workers=-1"}, nil, "sink.workers")
	mustFail(t, []string{"--sink.batch=0"}, nil, "sink.batch")
}

// SPEC §10.7 and §13.2.
func TestNATSValidation(t *testing.T) {
	// One method at a time is fine, and a cluster is a list.
	for _, args := range [][]string{
		{"--sink.nats.username=u", "--sink.nats.password=p"},
		{"--sink.nats.username=u"},
		{"--sink.nats.token=t"},
		{"--sink.nats.credentials=/etc/syslog/sls.creds"},
		{"--sink.nats.urls=nats://a:4222,nats://b:4222"},
		{"--sink.nats.urls=NATS://a:4222"},
		{"--sink.nats.urls=nats://u:p@a:4222"},
		{"--sink.nats.stream=LOGS"},
		{"--sink.nats.stream=LOGHUB-SYSLOG_2"},
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
		{[]string{"--sink.nats.stream=SYS.LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=SYS LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=SYS*"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=SYS>"}, "sink.nats.stream"},
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
		l := mustLoad(t, []string{"--loghub.ctl_namespace=" + ns}, nil)
		if l.Config.LogHub.BeaconSubject() != ns+".beacon" {
			t.Fatalf("%s: beacon subject = %q", ns, l.Config.LogHub.BeaconSubject())
		}
	}
	// There is no subject key: it is derived (SPEC §10.7).
	mustFail(t, []string{"--sink.nats.subject=x"}, nil, "unknown parameter")
	mustFail(t, []string{"--sink.nats.subjects=exact"}, nil, "unknown parameter")
}

// SPEC §1.2: there is no TLS, and nats.go would otherwise dial tls://, ws://
// and wss:// -- or read a bare host:port as nats:// -- with a handshake this
// server was never configured for. Only nats:// passes.
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
		{"tls://n:4222", `"tls"`},
		{"ws://n:8080", `"ws"`},
		{"wss://n:8443", `"wss"`},
		{"TLS://n:4222", `"tls"`},
		{"n:4222", `"none"`},
		{"127.0.0.1:4222", `"none"`},
		{"nats://a:4222,tls://b:4222", `"tls"`},
		{"nats://a:4222,b:4222", `"none"`},
		{"http://n:8222", `"http"`},
	} {
		err := mustFail(t, []string{"--sink.nats.urls=" + tc.urls}, nil, "sink.nats.urls")
		for _, w := range []string{"nats://", "no TLS", tc.scheme} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: err = %v, want it to name %s", tc.urls, err, w)
			}
		}
		// The environment and the file are the same parameter and the same
		// refusal.
		mustFail(t, nil, map[string]string{"SYSLOG_SINK_NATS_URLS": tc.urls}, "sink.nats.urls")
	}
	// The refusal names the scheme, never the URL: it may carry a password.
	for _, u := range []string{"tls://u:hunter2@n:4222", "ws://hunter2@n:80", "u:hunter2@n:4222",
		"hunter2", "nats://a:4222,wss://u:hunter2@b:443"} {
		_, err := load(t, []string{"--sink.nats.urls=" + u}, nil)
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
}

// SPEC §10.7: core is best effort, and the boot says so; jetstream does not.
func TestCoreModeWarns(t *testing.T) {
	env := map[string]string{"GOMEMLIMIT": "1GiB"}
	l := mustLoad(t, []string{"--sink.nats.mode=core"}, env)
	if !hasWarning(l, "best effort") || !hasWarning(l, "sink.nats.mode=core") || len(l.Warnings) != 1 {
		t.Fatalf("core: warnings = %q", l.Warnings)
	}
	if l := mustLoad(t, []string{"--sink.nats.mode=jetstream"}, env); len(l.Warnings) != 0 {
		t.Fatalf("jetstream warned: %q", l.Warnings)
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
		"--loghub.beacon_interval=0s",
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

// The keys of the features go-syslog-nats-server removed (other sinks, ring
// modes, TLS, filter, rate limit, facility-severity subjects) must fail the boot, on the command
// line and in a file alike: a config carried over from go-syslog-server that
// silently lost its TLS or its filter would run doing something else than its
// operator believes.
func TestRemovedKeysAreUnknown(t *testing.T) {
	gone := []struct{ name, value string }{
		{"sink.type", "nats"},
		{"ring.mode", "memory"},
		{"listen.tls", ":6514"},
		{"listen.tls_listeners", "1"},
		{"tls.cert_file", "/c"},
		{"tls.key_file", "/k"},
		{"tls.client_auth", "none"},
		{"filter.min_severity", "info"},
		{"filter.facilities", "kern"},
		{"ratelimit.enabled", "false"},
		{"ratelimit.rate", "100"},
		{"sink.nats.tls", "false"},
		{"sink.nats.tls_ca", "/ca"},
		{"sink.nats.tls_insecure", "false"},
		{"sink.nats.subjects", "exact"},
		{"sink.nats.subject", "loghub.default.syslog"},
		{"sink.postgres.addr", "pg-1:5432"},
		{"sink.postgres.pool_size", "5"},
		{"sink.redis.addrs", "r:6379"},
		{"ring.max_inflight", "4"},
		{"ring.sqlite.path", "/x"},
	}
	// Control: the same shapes with kept keys load, so the refusals below are
	// about the names and not about the way the test writes them.
	kept := []struct{ name, value string }{
		{"sink.workers", "4"},
		{"ring.capacity_items", "100"},
		{"listen.tcp", ":1514"},
		{"sink.nats.urls", "nats://n:4222"},
		{"sink.nats.mode", "core"},
		{"sink.nats.stream", "LOGS"},
		{"loghub.beacon_interval", "15"},
		{"loghub.ctl_namespace", "loghub._ctl"},
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
	// No registry key mentions TLS or subjects, and no section survives that
	// only a removed feature had.
	for _, p := range Registry(&Config{}) {
		if strings.Contains(p.Key, "tls") || strings.Contains(p.Key, "subject") {
			t.Errorf("%s: a removed key survived", p.Name())
		}
		switch p.Sec {
		case "tls", "filter", "ratelimit", "ring.sqlite", "ring.redis", "sink.postgres",
			"sink.redis", "sink.sqlite", "sink.mysql", "sink.clickhouse":
			t.Errorf("%s: section [%s] belongs to a removed feature", p.Name(), p.Sec)
		}
		if p.Name() == "sink.type" || p.Name() == "ring.mode" {
			t.Errorf("%s survived: there is one sink and one ring", p.Name())
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
		"--sink.nats.credentials= /etc/padded.creds ", "--sink.nats.urls=nats://a:4222, nats://b:4222",
		"--sink.nats.mode=core", "--sink.nats.stream=LOGS", "--sink.workers=3",
		"--loghub.beacon_interval=20", "--loghub.ctl_namespace=acme._ctl",
		"--syslog.rfc3164_timezone=Europe/Lisbon", "--log.access_sample=0.25",
	}, nil)

	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("WARNING: secrets")) {
		t.Fatal("no secret was set, yet the dump claims it redacted one")
	}
	if !strings.HasPrefix(buf.String(), "# effective configuration of go-syslog-nats-server\n") {
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
	for _, sub := range []string{
		"# derived from server.data_dir", "# resolved to an absolute path", "# from flag --listen.unix"} {
		if !strings.Contains(buf.String(), sub) {
			t.Errorf("dump lacks %q", sub)
		}
	}
	// The beacon interval comes back as the same duration, whichever spelling
	// set it, and 0 survives as "off".
	if back.Config.LogHub.BeaconInterval != 20*time.Second {
		t.Fatalf("beacon_interval read back as %s", back.Config.LogHub.BeaconInterval)
	}
	l = mustLoad(t, []string{"--loghub.beacon_interval=0"}, nil)
	buf.Reset()
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	writeFile(t, f, buf.String())
	if back := mustLoad(t, []string{"--config=" + f}, nil); back.Config.LogHub.BeaconInterval != 0 ||
		back.Source["loghub.beacon_interval"] != FromFile {
		t.Fatalf("beacon_interval=0 read back as %s from %v", back.Config.LogHub.BeaconInterval,
			back.Source["loghub.beacon_interval"])
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
		if l.Values()[key] != "hunter2" {
			t.Errorf("%s redacted in Values(): a changed password would not count on SIGHUP", key)
		}
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
	l := mustLoad(t, []string{"--sink.nats.urls=nats://u:hunter2@a:4222,nats://tok3n@b:4222,nats://c:4222"}, nil)
	got := l.Effective()["sink.nats.urls"]
	if strings.Contains(got, "hunter2") || strings.Contains(got, "tok3n") ||
		got != "nats://u:***@a:4222,nats://***@b:4222,nats://c:4222" {
		t.Fatalf("redacted urls = %q", got)
	}
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte("hunter2")) || bytes.Contains(buf.Bytes(), []byte("tok3n")) ||
		!bytes.Contains(buf.Bytes(), []byte("WARNING: secrets")) {
		t.Fatalf("urls in the dump:\n%s", buf.String())
	}
	l = mustLoad(t, []string{"--loghub.eb_url=nats://u:hunter2@n-1:4222"}, nil)
	if got := l.Effective()["loghub.eb_url"]; got != "nats://u:***@n-1:4222" {
		t.Fatalf("redacted eb_url = %q", got)
	}
	if got := l.Effective()["sink.nats.urls"]; got != "nats://u:***@n-1:4222" {
		t.Fatalf("redacted urls from eb_url = %q", got)
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
// it must not offer the TLS, the other sinks or the subjects this cut no longer
// has. The beacon stays, and so does its documentation.
func TestUsageNamesThisServer(t *testing.T) {
	var buf bytes.Buffer
	if err := Usage(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "go-syslog-nats-server ") || !strings.Contains(out, "\n  go-syslog-nats-server [--config FILE]") {
		t.Errorf("--help does not name go-syslog-nats-server:\n%s", out)
	}
	for _, bad := range []string{"go-syslog-server", "go-syslog-pg-server", "listen.tls", "tls_listeners",
		"6514", "[tls]", "cert_file", "tls_ca", "tls_insecure", "subjects", "postgres", "redis://",
		"[filter]", "[ratelimit]", "sink.type", "ring.mode", "\n  type "} {
		if strings.Contains(out, bad) {
			t.Errorf("--help mentions %q", bad)
		}
	}
	// It says so instead, so nobody goes looking for the key.
	if !strings.Contains(out, "There is no TLS") {
		t.Error("--help does not say there is no TLS")
	}
	for _, want := range []string{"[sink.nats]", "[loghub]", "beacon_interval", "env LOGHUB_BEACON_INTERVAL",
		"ctl_namespace", "env LOGHUB_CTL_NAMESPACE", "default nats://127.0.0.1:4222", "default LOGHUB_SYSLOG",
		"one of jetstream | core", "loghub.<space>.syslog"} {
		if !strings.Contains(out, want) {
			t.Errorf("--help lacks %q", want)
		}
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
	// Past ~85 KiB (84.7) the worst event no longer fits the default 1 MiB
	// max_payload, which is why the boot computes it instead of trusting a
	// factor.
	if MaxEventBytesFor(84<<10) > 1<<20 || MaxEventBytesFor(85<<10) <= 1<<20 {
		t.Fatalf("the 85 KiB threshold of SPEC §3.9 moved")
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
	if c.Server.DataDir != "./data" || c.Emergency.Dir != "" || c.Sink.Workers != 8 ||
		c.SinkNATS.Mode != NATSModeJetStream || c.LogHub.BeaconInterval != 15*time.Second {
		t.Fatalf("Defaults = %+v", c)
	}
	warns, err := Validate(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Fatalf("Validate(Defaults()) warns: %q", warns)
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
	// The beacon variables are this server's own: set, they configure, and
	// they are not reported as ignored.
	env["LOGHUB_BEACON_INTERVAL"] = "15"
	env["LOGHUB_CTL_NAMESPACE"] = "loghub._ctl"
	if l := mustLoad(t, nil, env); len(l.Warnings) != 0 {
		t.Fatalf("control: the beacon variables warned: %v", l.Warnings)
	}
	set := []string{"SYSLOG_LISTEN_TLS", "SYSLOG_FILTER_MIN_SEVERITY", "SYSLOG_SINK_NATS_TLS",
		"SYSLOG_SINK_NATS_TLS_CA", "SYSLOG_SINK_NATS_SUBJECTS", "SYSLOG_SINK_POSTGRES_ADDR", "SYSLOG_SINK_TYPE",
		"SYSLOG_RING_MODE"}
	for _, k := range set {
		env[k] = "x"
	}
	l := mustLoad(t, nil, env)
	if len(l.Warnings) != 1 {
		t.Fatalf("warnings = %v, want one line", l.Warnings)
	}
	for _, k := range append(set, "Ignored", "go-syslog-nats-server") {
		if !strings.Contains(l.Warnings[0], k) {
			t.Errorf("warning %q does not name %s", l.Warnings[0], k)
		}
	}
	// The warning must not deny a feature this server has: the beacon is kept.
	for _, k := range []string{"beacon", "LOGHUB_"} {
		if strings.Contains(l.Warnings[0], k) {
			t.Errorf("warning %q mentions %s", l.Warnings[0], k)
		}
	}
	// The values were ignored: the sink is still the default NATS one.
	if !reflect.DeepEqual(l.Config.SinkNATS.URLs, []string{"nats://127.0.0.1:4222"}) {
		t.Fatalf("a removed variable changed the config: %+v", l.Config.SinkNATS)
	}
}

// Review D #2, #6, #7: three sink.nats/loghub values that used to pass.
func TestNATSURLAndBeaconReviewFindings(t *testing.T) {
	mustLoad(t, []string{"--sink.nats.urls=nats://bob:s3cr%25t@n1:4222"}, nil) // control: encoded
	for _, bad := range []string{"nats://bob:s3cr%t@n1:4222", "nats://bob:p/w@n1:4222"} {
		_, err := load(t, []string{"--sink.nats.urls=" + bad}, nil)
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

// ------------------------------------------------ ported from go-webhook-nats-server

// No piece of a URL that does not parse reaches the message (SPEC §10.9,
// §13.2): url.Error's inner text quoted the password up to the first '/',
// '#', '?' or ',' (`invalid port ":Sup3rSecret" after host`), and the escape
// error quoted the escape. Both doors -- sink.nats.urls and LOGHUB_EB_URL --
// say how to fix it instead.
func TestURLErrorsQuoteNothingOfTheURL(t *testing.T) {
	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"nats://bob:Sup3rSecret#xyz@n1:4222",
		"nats://bob:Sup3rSecret?xyz@n1:4222",
		"nats://bob:Sup3rSecret%zz@n1:4222",
		"nats://bob:Sup3rSecret,xyz@n1:4222",
		"nats://Sup3rSecretToken/x@n1:4222",
	} {
		_, err1 := load(t, []string{"--sink.nats.urls=" + u}, nil)
		_, err2 := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		for _, err := range []error{err1, err2} {
			if err == nil || strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
				!strings.Contains(err.Error(), "percent-encode") {
				t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
			}
		}
	}
	// Control: the same credentials, escaped, load from both doors.
	escaped := "nats://bob:Sup3rSecret%2Fxyz@n1:4222"
	mustLoad(t, []string{"--sink.nats.urls=" + escaped}, nil)
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": escaped})

	// The secret that parses: a password that reads as a port and a path, or a
	// token that reads as a host. No credentials at all, dialled until
	// boot_timeout -- and the dial error quoted the host, the token up to the
	// slash. sink.nats.urls refuses it like LOGHUB_EB_URL always did.
	for _, u := range []string{"nats://user:1234/5678@n1:4222", "nats://user:1234#5678@n1:4222",
		"nats://user:1234?5678@n1:4222"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "path, a query or a fragment")
		if strings.Contains(err.Error(), "5678") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	mustLoad(t, []string{"--sink.nats.urls=nats://user:1234%2F5678@n1:4222/"}, nil) // control: escaped, root path
}

// A parser error names the line or the flag, never the value on it (SPEC
// §13.2): "password: hunter2" (a ':' for the '=') or "sink.nats.password=x"
// (the dashes forgotten) went whole to the container's stderr, and so did a
// value swallowed by a flag left without its own, typed after a space, starting
// with a dash or split by the shell at a space.
func TestParserErrorsDoNotEchoValues(t *testing.T) {
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
	// A flag whose value was left out swallows the next argument: a secret in
	// it is never printed, nor one typed after a space past the '='.
	for _, args := range [][]string{
		{"--sink.workers", "--sink.nats.password=hunter2"},
		{"--sink.nats.mode", "--sink.nats.token=hunter2"},
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
		err := mustFail(t, args, nil, "")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: the error echoes the secret: %v", args, err)
		}
	}
	// Controls: the same values on well-formed lines load, and a positional
	// argument with no '=' is still named, so an operator sees what was typed.
	writeFile(t, ini, "[sink.nats]\nusername = u\npassword = hunter2\n")
	mustLoad(t, []string{"--config=" + ini}, nil)
	mustFail(t, []string{"extra"}, nil, `"extra"`)
	// More controls: a value of the separated form is still taken, a
	// negative number is a value and not a flag, a switch is a flag, a typo
	// after a real value is still named, and each refusal says what went
	// wrong -- the space after the '=', or a value with a space unquoted.
	if l := mustLoad(t, []string{"--sink.workers", "4", "--sink.nats.stream", "LOGHUB_X"}, nil); l.Config.Sink.Workers != 4 ||
		l.Config.SinkNATS.Stream != "LOGHUB_X" {
		t.Errorf("separated values: workers=%d stream=%q", l.Config.Sink.Workers, l.Config.SinkNATS.Stream)
	}
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "-1 is below 1")
	mustFail(t, []string{"--sink.nats.stream", "-h"}, nil, "the next argument is the flag --h")
	mustFail(t, []string{"--sink.nats.password=", "--admin.enabled", "extra"}, nil, `"extra"`)
	mustFail(t, []string{"--sink.nats.password=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password", "=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password=foo", "hunter2"}, nil, "goes in quotes")
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, `"typo.key"`)
	mustFail(t, []string{"--sink.nats.password", "--sink.workers=4"}, nil, "the flag --sink.workers")
}
