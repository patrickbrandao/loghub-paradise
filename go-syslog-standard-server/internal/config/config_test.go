package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func load(t *testing.T, args []string, env map[string]string) (*Loaded, error) {
	t.Helper()
	return Load(Options{
		Args: args,
		LookupEnv: func(k string) (string, bool) {
			v, ok := env[k]
			return v, ok
		},
		Hostname: func() (string, error) { return "test-node", nil },
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

func TestDefaults(t *testing.T) {
	l := mustLoad(t, nil, nil)
	c := l.Config
	if c.Listen.UDP != ":1514" || c.Listen.TCP != ":1514" {
		t.Errorf("listen = %q %q", c.Listen.UDP, c.Listen.TCP)
	}
	// RELP and unix are off by default: RELP needs an emitter that speaks it,
	// and creating /dev/log uninvited would take over the host's own syslog.
	if c.Listen.RELP != "" || c.Listen.Unix != "" {
		t.Errorf("relp=%q unix=%q, want both empty", c.Listen.RELP, c.Listen.Unix)
	}
	if c.Ring.Mode != RingMemory || c.Sink.Type != SinkRedisType {
		t.Errorf("ring=%q sink=%q", c.Ring.Mode, c.Sink.Type)
	}
	if c.SinkRedis.Stream != "syslog:in" {
		t.Errorf("stream = %q, want syslog:in", c.SinkRedis.Stream)
	}
	if c.Admin.Bind != "127.0.0.1:9102" {
		t.Errorf("admin.bind = %q, want 9102 so it never collides with go-webhook-server", c.Admin.Bind)
	}
	if c.Node.ID != "test-node" {
		t.Errorf("node.id = %q", c.Node.ID)
	}
	if c.Listen.UnixMode != 0o666 {
		t.Errorf("unix_mode = %o, want 666", c.Listen.UnixMode)
	}
}

func TestDerivedPaths(t *testing.T) {
	l := mustLoad(t, []string{"--ring.data_dir=/srv/x"}, nil)
	c := l.Config
	if c.RingSQL.Path != filepath.Join("/srv/x", "ring.db") ||
		c.SinkSQL.Path != filepath.Join("/srv/x", "events.db") ||
		c.Emergency.Dir != filepath.Join("/srv/x", "emergency") {
		t.Fatalf("derived paths: %q %q %q", c.RingSQL.Path, c.SinkSQL.Path, c.Emergency.Dir)
	}
}

func TestPrecedence(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "c.ini")
	os.WriteFile(ini, []byte("[ring]\ncapacity_items = 111\nmax_bytes = 1MiB\n"), 0o600)

	// file only
	l := mustLoad(t, []string{"--config=" + ini}, nil)
	if l.Config.Ring.CapacityItems != 111 {
		t.Fatalf("file: %d", l.Config.Ring.CapacityItems)
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
	// untouched keys keep the file value
	if l.Config.Ring.MaxBytes != 1<<20 {
		t.Fatalf("max_bytes = %d", l.Config.Ring.MaxBytes)
	}
}

func TestEnvNameMatchesRegistry(t *testing.T) {
	for _, p := range Registry(&Config{}) {
		if !strings.HasPrefix(p.Env(), "SYSLOG_") {
			t.Fatalf("%s env name %q lacks the SYSLOG_ prefix", p.Name(), p.Env())
		}
	}
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
}

// Past sun_path the kernel says only "invalid argument". The length check turns
// that into an error that names the problem.
func TestUnixSocketPathLength(t *testing.T) {
	long := "/" + strings.Repeat("a", unixPathMax())
	_, err := load(t, []string{"--listen.unix=" + long}, nil)
	if err == nil || !strings.Contains(err.Error(), "sun_path") {
		t.Fatalf("err = %v, want a sun_path length error", err)
	}
	mustLoad(t, []string{"--listen.unix=/dev/log"}, nil)
}

func TestNoListenerIsFatal(t *testing.T) {
	_, err := load(t, []string{"--listen.udp=", "--listen.tcp="}, nil)
	if err == nil || !strings.Contains(err.Error(), "no transport") {
		t.Fatalf("err = %v, want a no-transport error", err)
	}
}

func TestTCPAndRELPCannotShareAPort(t *testing.T) {
	_, err := load(t, []string{"--listen.tcp=:1514", "--listen.relp=:1514"}, nil)
	if err == nil {
		t.Fatal("tcp and relp on one port accepted")
	}
	// UDP and TCP on the same port is the classic deployment and must pass.
	mustLoad(t, []string{"--listen.udp=:1514", "--listen.tcp=:1514"}, nil)
}

func TestRingAndSinkSQLiteCannotShareAFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "same.db")
	_, err := load(t, []string{
		"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=" + p, "--sink.sqlite.path=" + p,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "QUEUE") {
		t.Fatalf("err = %v, want the queue/destination error", err)
	}
	// Different files are allowed, with a warning about the double fsync.
	l := mustLoad(t, []string{
		"--ring.mode=sqlite", "--sink.type=sqlite",
		"--ring.sqlite.path=" + filepath.Join(dir, "a.db"),
		"--sink.sqlite.path=" + filepath.Join(dir, "b.db"),
	}, nil)
	if !hasWarning(l, "TWO fsyncs") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
}

// SPEC §10.5.3 and §10.7.2: the two relational destinations share one
// connection budget rule.
func TestRelationalWorkerDefaults(t *testing.T) {
	for _, tc := range []struct {
		sink string
		pool func(*Config) int
	}{
		{"postgres", func(c *Config) int { return c.SinkPG.PoolSize }},
		{"mysql", func(c *Config) int { return c.SinkMySQL.PoolSize }},
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
			if !hasWarning(l, "lowered") {
				t.Fatalf("warnings = %v", l.Warnings)
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
			_, err := load(t, []string{"--sink.type=" + tc.sink, "--sink.workers=8",
				"--sink." + tc.sink + ".pool_size=4"}, nil)
			if err == nil || !strings.Contains(err.Error(), "pool_size=4") {
				t.Fatalf("err = %v, want the pool refused", err)
			}
			// ring=none with more writers than connections hides a queue.
			l = mustLoad(t, []string{"--sink.type=" + tc.sink, "--ring.mode=none"}, nil)
			if !hasWarning(l, "inside it") {
				t.Fatalf("warnings = %v", l.Warnings)
			}
		})
	}
}

// SPEC §10.7 and §12.1.
func TestMySQLValidation(t *testing.T) {
	base := []string{"--sink.type=mysql"}
	l := mustLoad(t, base, nil)
	if c := l.Config.SinkMySQL; c.Addr != "127.0.0.1:3306" || c.Database != "syslog" || c.Table != "events" {
		t.Fatalf("defaults = %+v", c)
	}
	// The placeholder ceiling is exact: seventeen per row, 65535 per statement.
	if mysqlMaxBatch*17 > 65535 || (mysqlMaxBatch+1)*17 <= 65535 {
		t.Fatalf("mysqlMaxBatch = %d is not the largest row count under 65535 placeholders", mysqlMaxBatch)
	}
	mustLoad(t, append(base, "--sink.batch=3855"), nil)
	// Case is kept: MySQL table names are case-sensitive on Linux.
	if l := mustLoad(t, append(base, "--sink.mysql.table=Logs.Events"), nil); l.Config.SinkMySQL.Table != "Logs.Events" {
		t.Fatalf("table = %q", l.Config.SinkMySQL.Table)
	}

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.batch=3856"}, "3855"},
		{[]string{"--sink.mysql.table=events; drop table x"}, "plain identifier"},
		{[]string{"--sink.mysql.table=ev`ents"}, "plain identifier"},
		{[]string{"--sink.mysql.table=a.b.c"}, "plain identifier"},
		{[]string{"--sink.mysql.addr="}, "sink.mysql.addr"},
		{[]string{"--sink.mysql.username="}, "sink.mysql.username"},
		{[]string{"--sink.mysql.database="}, "sink.mysql.database"},
		{[]string{"--sink.mysql.addr=/run/mysqld/mysqld.sock", "--sink.mysql.tls=true"}, "unix socket"},
		{[]string{"--sink.mysql.tls_ca=/x"}, "sink.mysql.tls_ca"},
	} {
		_, err := load(t, append(base, tc.args...), nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err = %v, want one mentioning %q", tc.args, err, tc.want)
		}
	}
	// The batch ceiling belongs to mysql alone.
	mustLoad(t, []string{"--sink.type=redis", "--sink.batch=5000"}, nil)
}

// SPEC §10.8.9 and §12.1.
func TestNATSValidation(t *testing.T) {
	base := []string{"--sink.type=nats"}
	l := mustLoad(t, base, nil)
	if c := l.Config.SinkNATS; c.Stream != "SYSLOG" || c.Subject != "syslog.in" ||
		len(c.URLs) != 1 || c.URLs[0] != "nats://127.0.0.1:4222" {
		t.Fatalf("defaults = %+v", c)
	}
	// One method at a time is fine, and a cluster is a list.
	mustLoad(t, append(base, "--sink.nats.username=u", "--sink.nats.password=p"), nil)
	mustLoad(t, append(base, "--sink.nats.token=t"), nil)
	mustLoad(t, append(base, "--sink.nats.credentials=/etc/syslog/sls.creds"), nil)
	mustLoad(t, append(base, "--sink.nats.urls=nats://a:4222,nats://b:4222"), nil)
	mustLoad(t, append(base, "--sink.nats.subject=logs.syslog.in", "--sink.nats.stream=LOGS"), nil)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--sink.nats.urls="}, "sink.nats.urls"},
		{[]string{"--sink.nats.stream=SYS.LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream=SYS LOG"}, "sink.nats.stream"},
		{[]string{"--sink.nats.stream="}, "sink.nats.stream"},
		{[]string{"--sink.nats.subject="}, "sink.nats.subject"},
		{[]string{"--sink.nats.subject=syslog.*"}, "publishable"},
		{[]string{"--sink.nats.subject=syslog.>"}, "publishable"},
		{[]string{"--sink.nats.subject=syslog..in"}, "publishable"},
		{[]string{"--sink.nats.subject=sys log"}, "publishable"},
		{[]string{"--sink.nats.token=t", "--sink.nats.username=u"}, "only one of"},
		{[]string{"--sink.nats.credentials=/c", "--sink.nats.token=t"}, "only one of"},
		{[]string{"--sink.nats.password=p"}, "without sink.nats.username"},
		{[]string{"--sink.nats.tls_ca=/x"}, "sink.nats.tls_ca"},
	} {
		_, err := load(t, append(base, tc.args...), nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: err = %v, want one mentioning %q", tc.args, err, tc.want)
		}
	}
}

func TestSQLiteForcesOneWorker(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=sqlite", "--sink.workers=8"}, nil)
	if l.Config.Sink.Workers != 1 {
		t.Fatalf("workers = %d, want 1", l.Config.Sink.Workers)
	}
	if !hasWarning(l, "SQLITE_BUSY") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
}

func TestExposureWarning(t *testing.T) {
	l := mustLoad(t, []string{"--listen.udp=:1514"}, nil)
	if !hasWarning(l, "ratelimit.enabled=false") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
	// Loopback only: no warning, nothing off this host can reach it.
	l = mustLoad(t, []string{"--listen.udp=127.0.0.1:1514", "--listen.tcp="}, nil)
	if hasWarning(l, "ratelimit.enabled=false") {
		t.Fatalf("warned about a loopback bind: %v", l.Warnings)
	}
}

func TestDurabilityOnDatagramOnlyWarns(t *testing.T) {
	l := mustLoad(t, []string{"--ring.mode=sqlite", "--listen.tcp=", "--listen.udp=:1514"}, nil)
	if !hasWarning(l, "already have dropped") {
		t.Fatalf("warnings = %v", l.Warnings)
	}
}

func TestFilterAndParseRanges(t *testing.T) {
	mustLoad(t, []string{"--filter.min_severity=warning"}, nil)
	mustLoad(t, []string{"--filter.min_severity=4", "--filter.facilities=local0,auth"}, nil)
	for _, args := range [][]string{
		{"--filter.min_severity=loud"},
		{"--filter.min_severity=8"},
		{"--filter.facilities=nope"},
		{"--parse.default_facility=24"},
		{"--parse.default_severity=9"},
		{"--parse.prefer=rfc9999"},
	} {
		if _, err := load(t, args, nil); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	// "rfc5424" is what an operator naturally writes.
	l := mustLoad(t, []string{"--parse.prefer=rfc5424"}, nil)
	if l.Config.Parse.Prefer != Prefer5424 {
		t.Fatalf("prefer = %q", l.Config.Parse.Prefer)
	}
}

// SPEC §15.5: zero is refused where it is not "unlimited" but "already expired"
// or "silently replaced by a default" -- and accepted where it means what it
// says.
func TestZeroDurationsAreRejectedWhereDangerous(t *testing.T) {
	for _, tc := range []struct{ arg, want string }{
		{"--sink.op_timeout=0s", "sink.op_timeout: must be positive"},
		{"--shutdown.drain_timeout=0s", "shutdown.drain_timeout: must be positive"},
		{"--sink.health_interval=0s", "sink.health_interval: must be positive"},
		{"--sink.boot_timeout=0s", "sink.boot_timeout: must be positive"},
		{"--ring.sqlite.commit_max_delay=0s", "ring.sqlite.commit_max_delay: must be positive"},
		{"--ratelimit.sweep_interval=0s", "ratelimit.sweep_interval: must be positive"},
		{"--tcp.backpressure_timeout=0s", "tcp.backpressure_timeout: must be positive"},
		{"--relp.backpressure_timeout=0s", "relp.backpressure_timeout: must be positive"},
		{"--sink.health_timeout=0s", "sink.health_timeout: must be positive"},
		{"--sink.retry_backoff_min=0s", "sink.retry_backoff_min: must be positive"},
		{"--sink.retry_backoff_max=0s", "sink.retry_backoff_max: must be positive"},
		{"--ring.sqlite.busy_timeout=0s", "ring.sqlite.busy_timeout: must be positive"},
		{"--sink.sqlite.busy_timeout=0s", "sink.sqlite.busy_timeout: must be positive"},
	} {
		_, err := load(t, []string{tc.arg}, nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.arg, err, tc.want)
		}
	}
	for _, arg := range []string{
		"--shutdown.pre_stop_delay=0s",
		"--sink.batch_max_delay=0s",
		"--tcp.idle_timeout=0s",
		"--relp.idle_timeout=0s",
	} {
		if _, err := load(t, []string{arg}, nil); err != nil {
			t.Errorf("%s: %v", arg, err)
		}
	}
}

func TestTLSCAWithoutTLS(t *testing.T) {
	if _, err := load(t, []string{"--sink.redis.tls_ca=/x"}, nil); err == nil {
		t.Error("redis tls_ca without tls accepted")
	}
	if _, err := load(t, []string{"--sink.type=postgres", "--sink.postgres.tls_ca=/x"}, nil); err == nil {
		t.Error("postgres tls_ca without tls accepted")
	}
	if _, err := load(t, []string{"--sink.mysql.tls_ca=/x"}, nil); err == nil {
		t.Error("mysql tls_ca without tls accepted")
	}
	if _, err := load(t, []string{"--sink.nats.tls_ca=/x"}, nil); err == nil {
		t.Error("nats tls_ca without tls accepted")
	}
}

func TestUnknownParameterIsFatal(t *testing.T) {
	if _, err := load(t, []string{"--ring.capacty_items=1"}, nil); err == nil {
		t.Fatal("typo accepted; a deploy script would silently run on defaults")
	}
}

// The dump must be loadable back, or it is only decoration. Secrets break that
// on purpose and the dump says so.
func TestDumpRoundTrips(t *testing.T) {
	l := mustLoad(t, []string{
		"--ring.mode=sqlite", "--sink.type=none", "--listen.relp=:20514",
		"--listen.unix=/tmp/dev-log", "--listen.unix_mode=0640",
		"--filter.min_severity=info", "--tcp.max_frame_bytes=32MiB",
	}, nil)

	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	if err := os.WriteFile(f, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	back := mustLoad(t, []string{"--config=" + f}, nil)
	if !reflect.DeepEqual(back.Config, l.Config) {
		t.Fatalf("dump did not round-trip\n got %+v\nwant %+v", back.Config, l.Config)
	}
}

func TestSecretsAreRedacted(t *testing.T) {
	for _, key := range []string{
		"sink.redis.password", "sink.postgres.password", "sink.mysql.password",
		"sink.nats.password", "sink.nats.token",
	} {
		l := mustLoad(t, []string{"--" + key + "=hunter2"}, nil)
		var buf bytes.Buffer
		l.DumpINI(&buf)
		if bytes.Contains(buf.Bytes(), []byte("hunter2")) {
			t.Errorf("%s printed in the dump", key)
		}
		if l.Effective()[key] != "***" {
			t.Errorf("%s not redacted in Effective()", key)
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
		if !strings.Contains(out, p.Key) {
			t.Errorf("--help omits %s", p.Name())
		}
		if p.Desc == "" {
			t.Errorf("%s has no description", p.Name())
		}
	}
}

func hasWarning(l *Loaded, sub string) bool {
	for _, w := range l.Warnings {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}
