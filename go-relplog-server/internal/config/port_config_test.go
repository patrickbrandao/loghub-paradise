package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
)

// The configuration findings of the reviews of go-relplog-pg-server and
// go-relplog-nats-server (2026-10-13) and of go-syslog-server (2026-10-13),
// ported to this server. Each failed before its fix here, or pins a
// configuration the fixes must keep accepting. The first eight came with the
// fixes from go-syslog-server's port_config_test.go, which took them from the
// two cuts of this server.

// A secret that wraps onto the next line of the INI turns its tail into a
// key; the error printed it, and on SIGHUP into the server log. Same for a key
// before any section and a section header that is not one (SPEC §13.2).
func TestINIUnknownKeyNeverPrintsASecretContinuation(t *testing.T) {
	dir := t.TempDir()
	for i, tc := range []struct{ ini, secret string }{
		{"[sink.postgres]\npassword = eyJhbGciOiJI\nUzI1NiJ9abcDEF==\n", "UzI1NiJ9abcDEF"},
		{"[sink.postgres]\nusername = u\npassword = abc\nffd3a9c1b2=\n", "ffd3a9c1b2"},
		{"[sink.nats]\ntoken = abc\nffd3a9c1b2 = x\n", "ffd3a9c1b2"},
		{"Sup3rS3cret = 1\n[log]\nlevel = info\n", "Sup3rS3cret"},
		{"[hunter2]\nx = 1\n", "hunter2"},
	} {
		p := filepath.Join(dir, "s.ini")
		writeFile(t, p, tc.ini)
		_, err := load(t, []string{"--config=" + p}, nil)
		if err == nil || strings.Contains(err.Error(), tc.secret) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	// Control: a typo in a key of a real section, after a value that is not
	// secret, is still named.
	p := filepath.Join(dir, "typo.ini")
	writeFile(t, p, "[sink.postgres]\ntable = loghub.inbox\ntabel = x\n")
	err := mustFail(t, []string{"--config=" + p}, nil, "unknown parameter")
	if !strings.Contains(err.Error(), "sink.postgres.tabel") || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("typo: %v", err)
	}
}

// The '=' forgotten, the value glued to the name -- with or without a
// separator -- was printed as the name of an unknown parameter.
func TestGluedValueIsNotPrinted(t *testing.T) {
	for _, arg := range []string{"--sink.postgres.passwordHunter2xyz", "--sink.postgres.password-Hunter2xyz",
		"--sink.postgres.password_Hunter2xyz", "--sink.postgres.password.Hunter2xyz",
		"--sink.nats.tokenHunter2xyz"} {
		_, err := load(t, []string{arg}, nil)
		if err == nil || strings.Contains(err.Error(), "Hunter2xyz") {
			t.Errorf("%s: err = %v", arg, err)
		}
	}
	p := filepath.Join(t.TempDir(), "g.ini")
	writeFile(t, p, "[sink.postgres]\npasswordhunter2xyz = 1\n")
	if _, err := load(t, []string{"--config=" + p}, nil); err == nil || strings.Contains(err.Error(), "hunter2xyz") {
		t.Errorf("INI: err = %v", err)
	}
	// Control: a typo that is not a longer name is still named.
	mustFail(t, []string{"--sink.postgres.tabel=x"}, nil, `"sink.postgres.tabel"`)
}

// A switch takes no value; "--dump-config=false" dumped and exited 0.
func TestSwitchesTakeNoValue(t *testing.T) {
	for _, arg := range []string{"--dump-config=false", "--help=no", "--version=Zsecret", "-h=1", "-v=0"} {
		err := mustFail(t, []string{arg}, nil, "takes no value")
		if strings.Contains(err.Error(), "Zsecret") {
			t.Errorf("%s: value echoed: %v", arg, err)
		}
	}
	if l, err := load(t, []string{"--dump-config"}, nil); err != nil || !l.Meta.DumpConfig {
		t.Fatalf("control: --dump-config = %v, %v", l, err)
	}
}

// An explicit empty --config fell back to RELPLOG_CONFIG.
func TestEmptyConfigFlagIsRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), "env.ini")
	writeFile(t, p, "[log]\nlevel = debug\n")
	mustFail(t, []string{"--config="}, map[string]string{"RELPLOG_CONFIG": p}, "empty value")
	if l := mustLoad(t, nil, map[string]string{"RELPLOG_CONFIG": p}); l.Config.Log.Level != "debug" {
		t.Fatalf("control: RELPLOG_CONFIG not read: %q", l.Config.Log.Level)
	}
}

// A UTF-8 byte-order mark at the start of the INI is not part of the line.
func TestINIByteOrderMarkIsIgnored(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bom.ini")
	if err := os.WriteFile(p, []byte("\ufeff[log]\nlevel = debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if l := mustLoad(t, []string{"--config=" + p}, nil); l.Config.Log.Level != "debug" {
		t.Fatalf("level = %q", l.Config.Log.Level)
	}
}

// sink.postgres.addr and the URL that fills it name one server in every
// reading -- this server's and libpq's.
func TestPostgresAddrNamesOneServer(t *testing.T) {
	base := []string{"--sink.type=postgres"}
	for _, addr := range []string{"db\t1:5432", "[h:5432]:5433", "/run/pg,/x", "/run/p g", "h1,h2:5432"} {
		mustFail(t, append(base, "--sink.postgres.addr="+addr), nil, "sink.postgres.addr")
	}
	for _, u := range []string{
		"postgres://u:p@h:5432:5433/db",
		"postgres://u:p@/db?host=/run/pg,/x",
		"postgres://u:p@/db?host=/run/pg:5433",
	} {
		if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u}); err == nil {
			t.Errorf("%s accepted", u)
		}
	}
	// Control: one IPv6 literal, with or without a zone, and a socket with a port.
	for _, addr := range []string{"[::1]:5432", "[fe80::1%eth0]:5432", "/run/pg:5433"} {
		mustLoad(t, append(base, "--sink.postgres.addr="+addr), nil)
	}
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@:5433/db?host=/run/pg"})
}

// What a PostgreSQL URL must say, and the messages when it does not.
func TestEBURLMustNameItsParts(t *testing.T) {
	for _, tc := range []struct{ url, want string }{
		{"postgres://h/db", "names no user"},
		{"postgres://:Zsecret@h/db", "names no user"},
		{"postgres://u:Zsecret@h", "names no database"},
		{"postgres://u:Zsecret@h/", "names no database"},
		{"postgres://u:Zsecret@h/db?sslmode=", "sslmode of the PostgreSQL URL is empty"},
		{"postgres://u:Zsecret@%2Frun%2Fpg/db", "percent-encoded host"},
		{"postgres://u:Zs://ecret@h/db", "must be percent-encoded"},
		{"redis://u:Zs://ecret@h/0", "must be percent-encoded"},
		{"   ", "only blanks"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if strings.Contains(err.Error(), "Zsecret") || strings.Contains(err.Error(), "ecret") {
			t.Errorf("%s: echoed: %v", tc.url, err)
		}
	}
	// Control: unset is "no URL" (only blanks is not), and a complete URL
	// fills both keys.
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""})
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://app@h/logs"})
	if l.Config.SinkPG.Username != "app" || l.Config.SinkPG.Database != "logs" {
		t.Fatalf("user/db = %q/%q", l.Config.SinkPG.Username, l.Config.SinkPG.Database)
	}
}

// A synonym of the charset is not a change.
func TestCharsetSynonymsAreOneValue(t *testing.T) {
	boot := mustLoad(t, []string{"--syslog.legacy_charset=cp1252"}, nil)
	fresh := mustLoad(t, []string{"--syslog.legacy_charset=windows-1252"}, nil)
	if _, ignored := Compare(boot, nil, fresh); len(ignored) != 0 {
		t.Fatalf("a synonym reported as a restart-only change: %v", ignored)
	}
	fresh = mustLoad(t, []string{"--syslog.legacy_charset=latin1"}, nil)
	if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 {
		t.Fatalf("control: a real change not reported: %v", ignored)
	}
	// The table is convert's: each synonym names the charset its canonical
	// spelling does.
	for _, v := range []string{"", CharsetCP1252, CharsetLatin1, "windows-1252", "iso-8859-1"} {
		a, okA := convert.ParseCharset(v)
		b, okB := convert.ParseCharset(canonicalCharset(v))
		if !okA || !okB || a != b {
			t.Errorf("%q and its canonical %q are different charsets", v, canonicalCharset(v))
		}
	}
}

// Ceilings on the keys that size memory or a count handed to a library, exit
// 1 naming the key (SPEC §13.2).
func TestConfigCeilings(t *testing.T) {
	for _, tc := range []struct {
		args []string
		key  string
	}{
		{[]string{"--sink.batch_max_bytes=1GiB"}, "sink.batch_max_bytes"},
		{[]string{"--sink.workers=100000"}, "sink.workers"},
		{[]string{"--ring.capacity_items=35184372088832"}, "ring.capacity_items"},
		{[]string{"--sink.batch=1000000000"}, "sink.batch"},
		{[]string{"--ring.sqlite.commit_batch=35184372088832"}, "ring.sqlite.commit_batch"},
		{[]string{"--ring.max_inflight=4611686018427387904"}, "ring.max_inflight"},
		{[]string{"--ring.max_inflight=1048577"}, "ring.max_inflight"},
		{[]string{"--listen.tcp_listeners=100000"}, "listen.tcp_listeners"},
		{[]string{"--listen.tls_listeners=100000"}, "listen.tls_listeners"},
		{[]string{"--sink.type=postgres", "--sink.postgres.pool_size=4294967300"}, "sink.postgres.pool_size"},
		{[]string{"--sink.type=postgres", "--sink.postgres.pool_size=2147483648"}, "sink.postgres.pool_size"},
		{[]string{"--sink.type=mysql", "--sink.mysql.pool_size=4294967300"}, "sink.mysql.pool_size"},
		{[]string{"--sink.type=clickhouse", "--sink.clickhouse.pool_size=4294967300"}, "sink.clickhouse.pool_size"},
	} {
		mustFail(t, tc.args, nil, tc.key)
	}
	// Control: the ceilings themselves load.
	mustLoad(t, []string{"--sink.batch_max_bytes=256MiB", "--sink.workers=1024", "--ring.capacity_items=16777216",
		"--sink.batch=1048576", "--ring.sqlite.commit_batch=65536", "--ring.max_inflight=1048576",
		"--listen.tcp_listeners=256", "--listen.tls_listeners=256"}, nil)
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		mustLoad(t, []string{"--sink.type=" + s, "--sink.workers=8", "--sink." + s + ".pool_size=4096"}, nil)
	}
}

// The ceiling of ring.capacity_items is the MEMORY ring's: only it makes a
// channel of that many slots. ring=sqlite counts rows and ring=redis hands
// the number to a script, so a 50-million-message persistent ring is a
// legitimate configuration, and was refused (go-syslog-server review,
// 2026-10-13). The memory ring that ring=redis degrades to keeps the ceiling.
func TestRingCapacityCeilingIsTheMemoryRings(t *testing.T) {
	for _, mode := range []string{"sqlite", "redis"} {
		mustLoad(t, []string{"--ring.mode=" + mode, "--ring.capacity_items=50000000", "--ring.max_bytes=16GiB"}, nil)
	}
	mustFail(t, []string{"--ring.mode=memory", "--ring.capacity_items=50000000"}, nil, "ring.capacity_items")
	// A typo is still a typo on a persistent ring.
	mustFail(t, []string{"--ring.mode=sqlite", "--ring.capacity_items=35184372088832000"}, nil, "ring.capacity_items")
	// ring=redis on the sink's own Redis degrades to memory, and the memory
	// ring's ceiling applies to what will run.
	err := mustFail(t, []string{"--ring.mode=redis", "--sink.type=redis", "--ring.capacity_items=50000000"},
		nil, "ring.capacity_items")
	if !strings.Contains(err.Error(), "degraded") {
		t.Errorf("the refusal of the degraded ring does not say why it is a memory ring: %v", err)
	}
}

// sink.batch is a Dequeue slice, not a channel: a ClickHouse batch of 100 000
// rows under 64 MiB is legitimate and was refused by a 65536 ceiling
// (go-syslog-server review, 2026-10-13). MySQL keeps its own, lower one.
func TestSinkBatchCeiling(t *testing.T) {
	mustLoad(t, []string{"--sink.type=clickhouse", "--sink.batch=100000", "--sink.batch_max_bytes=64MiB"}, nil)
	mustFail(t, []string{"--sink.batch=1048577"}, nil, "sink.batch")
	mustFail(t, []string{"--sink.type=mysql", "--sink.batch=100000"}, nil, "MySQL")
}

// The health check of the SQL sinks has a connection of its own (SPEC §10.5,
// §10.6, §10.10.2), so the pool is the writers' and only the writers': it is
// derived as sink.workers, refused below it, and pool_size = workers --
// which left the probe without a connection while it shared the pool, and
// which go-relplog-pg-server refused for that reason -- is accepted.
func TestPoolIsTheWritersOnly(t *testing.T) {
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		mustLoad(t, []string{"--sink.type=" + s, "--sink.workers=4", "--sink." + s + ".pool_size=4"}, nil)
		mustFail(t, []string{"--sink.type=" + s, "--sink.workers=4", "--sink." + s + ".pool_size=3"}, nil,
			"below sink.workers")
		l := mustLoad(t, []string{"--sink.type=" + s, "--sink.workers=4"}, nil)
		var got int
		switch s {
		case "postgres":
			got = l.Config.SinkPG.PoolSize
		case "mysql":
			got = l.Config.SinkMySQL.PoolSize
		default:
			got = l.Config.SinkCH.PoolSize
		}
		if got != 4 {
			t.Errorf("%s: pool_size derived as %d with 4 workers, want 4", s, got)
		}
	}
}

// The writers a destination sees at once (SinkWriters): the drain workers
// with a ring; with ring=none each transaction writes, up to max_inflight at
// a time, and never more than the pool hands out. The ClickHouse memory check
// multiplies the worst chunk by it, not by the pool.
func TestSinkWriters(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=clickhouse", "--sink.workers=4", "--sink.clickhouse.pool_size=9"}, nil)
	if got := l.Config.SinkWriters(); got != 4 {
		t.Errorf("ring=memory: %d writers, want the 4 workers", got)
	}
	l = mustLoad(t, []string{"--sink.type=clickhouse", "--ring.mode=none", "--ring.max_inflight=6",
		"--sink.clickhouse.pool_size=9"}, nil)
	if got := l.Config.SinkWriters(); got != 6 {
		t.Errorf("ring=none, max_inflight 6, pool 9: %d writers, want 6", got)
	}
	l = mustLoad(t, []string{"--sink.type=clickhouse", "--ring.mode=none", "--ring.max_inflight=64",
		"--sink.clickhouse.pool_size=9"}, nil)
	if got := l.Config.SinkWriters(); got != 9 {
		t.Errorf("ring=none, max_inflight 64, pool 9: %d writers, want the pool's 9", got)
	}
}

// libpq reads an empty host= as "not set" and dials its default socket
// directory; this server dialled the URL's host instead. Two readers of one
// URL, two servers: refused, like an empty sslmode (go-syslog-server review,
// 2026-10-13).
func TestPostgresURLEmptyHostParamIsRefused(t *testing.T) {
	for _, u := range []string{"postgres://u:p@db.example/logs?host=", "postgres://u:p@/logs?host="} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "host=")
		if strings.Contains(err.Error(), "db.example") {
			t.Errorf("%s: echoed: %v", u, err)
		}
	}
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?host=/run/pg"})
}

// TLS on a unix socket: pgx set the TLS config after the parse and the
// SSLRequest over the socket failed on every attempt, retried as network for
// the whole sink.boot_timeout. Refused, as sink.mysql refuses it, in both the
// keys and the URL (go-syslog-server review, 2026-10-13).
func TestPostgresSocketWithTLSIsRefused(t *testing.T) {
	mustFail(t, []string{"--sink.type=postgres", "--sink.postgres.addr=/run/pg", "--sink.postgres.tls=true"}, nil,
		"unix socket")
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?host=/run/pg&sslmode=require"},
		"unix socket")
	// Controls: TLS on TCP, and a socket without TLS.
	mustLoad(t, []string{"--sink.type=postgres", "--sink.postgres.addr=db:5432", "--sink.postgres.tls=true"}, nil)
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?host=/run/pg&sslmode=disable"})
}

// A ',' in the host of a Redis URL made two addresses of one URL (the
// address list is comma-separated), a cluster nobody configured. Refused
// (go-syslog-server review, 2026-10-13).
func TestRedisURLHostWithCommaIsRefused(t *testing.T) {
	err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "redis://:Zsecret@r1,r2:6379/0"}, "one server")
	if strings.Contains(err.Error(), "Zsecret") {
		t.Errorf("echoed: %v", err)
	}
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "redis://:p@r1:6379/0"})
}

// ring=redis with sink=none keeps the queue in a stream other instances may
// share and nothing drains it to a destination: said at boot, WARN, as the F
// does for its leftover ring.db (SPEC §9.5; go-syslog-server review,
// 2026-10-13).
func TestRedisRingWithSinkNoneWarns(t *testing.T) {
	l := mustLoad(t, []string{"--ring.mode=redis", "--sink.type=none"}, nil)
	found := false
	for _, w := range l.Warnings {
		if strings.Contains(w, "ring.mode=redis with sink.type=none") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no WARN for ring=redis with sink=none: %q", l.Warnings)
	}
}

// A derived value is written commented in the dump, so reading the dump back
// derives it again instead of fixing it (go-relplog-pg-server review,
// 2026-10-13).
func TestDumpCommentsDerivedValues(t *testing.T) {
	l := mustLoad(t, []string{"--sink.type=postgres", "--sink.workers=3"}, nil)
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	_, pg, _ := strings.Cut(out, "[sink.postgres]\n")
	pg, _, _ = strings.Cut(pg, "\n[")
	if !strings.Contains(pg, "# pool_size                    = 3") || strings.Contains(pg, "\npool_size ") {
		t.Fatalf("derived pool_size not commented:\n%s", pg)
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, out)
	back := mustLoad(t, []string{"--config=" + f, "--sink.workers=7", "--server.data_dir=/elsewhere"}, nil)
	if back.Config.SinkPG.PoolSize != 7 {
		t.Fatalf("read back with 7 workers, pool_size = %d: the dump fixed the derived value", back.Config.SinkPG.PoolSize)
	}
	if back.Config.Emergency.Dir != "/elsewhere/emergency" {
		t.Fatalf("read back with another data_dir, emergency.dir = %q: the dump fixed the derived path",
			back.Config.Emergency.Dir)
	}
	if back.Config.RingSQL.Path != "/elsewhere/ring.db" {
		t.Fatalf("read back with another data_dir, ring.sqlite.path = %q: the dump fixed the derived path",
			back.Config.RingSQL.Path)
	}
}

var _ = os.WriteFile
