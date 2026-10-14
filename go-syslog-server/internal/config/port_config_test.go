package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
)

// Tests for the configuration items of SPEC §22 (pendências 21, 22 and 30) and for the
// configuration findings of the independent reviews of go-relplog-server
// (2026-10-12) and of go-relplog-pg-server / go-relplog-nats-server
// (2026-10-13), ported with adaptation on 2026-10-13. Each failed before its
// fix in this server, or pins a configuration the fixes must keep accepting.

// §22 pendência 21: a beacon interval below one second is a flood, not a
// heartbeat. Zero still disables the periodic beacon.
func TestBeaconIntervalHasAFloor(t *testing.T) {
	for _, raw := range []string{"1ns", "15ms", "500ms", "999ms"} {
		mustFail(t, []string{"--loghub.beacon_interval=" + raw}, nil, "below 1s")
		mustFail(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw}, "below 1s")
		// Whatever the sink: the same file must not be valid with one sink
		// and refused with another.
		mustFail(t, []string{"--sink.type=redis", "--loghub.beacon_interval=" + raw}, nil, "below 1s")
	}
	for _, raw := range []string{"0", "0s", "1", "1s", "1500ms", "15"} {
		mustLoad(t, []string{"--loghub.beacon_interval=" + raw}, nil)
	}
}

// §22 pendência 22: the PostgreSQL URL and sink.postgres.addr.
func TestPostgresURLAndAddr(t *testing.T) {
	// The port of a socket URL names the socket file: libpq dials
	// /run/pg/.s.PGSQL.5433.
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@:5433/logs?host=/run/pg"})
	if got := l.Config.SinkPG.Addr; got != "/run/pg:5433" {
		t.Errorf("socket URL with a port: addr = %q, want /run/pg:5433", got)
	}
	l = mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?host=/run/pg"})
	if got := l.Config.SinkPG.Addr; got != "/run/pg" {
		t.Errorf("socket URL without a port: addr = %q, want /run/pg", got)
	}

	// A repeated parameter: url.Values.Get takes the first, libpq the last.
	for _, q := range []string{"sslmode=disable&sslmode=require", "sslmode=require&sslmode=require",
		"host=/a&host=/b"} {
		mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?" + q}, "repeated")
	}

	// One server, by host:port or socket directory.
	base := []string{"--sink.type=postgres"}
	for _, ok := range []string{"pg-1:5432", "[::1]:5432", "10.0.0.1:6432", "/run/pg", "/run/pg:5433"} {
		mustLoad(t, append(base, "--sink.postgres.addr="+ok), nil)
	}
	for _, bad := range []string{"h1,h2:5432", "h1:5432,h2:5432", "pg-1", ":5432", "pg-1:0",
		"pg-1:65536", "pg-1:+80", "pg-1:080", "/run/pg:0", "/run/pg:x", "pg 1:5432"} {
		mustFail(t, append(base, "--sink.postgres.addr="+bad), nil, "sink.postgres.addr")
	}
	// The rule is PostgreSQL's: a MySQL socket path keeps working, and the
	// rule bites only when postgres is the sink.
	mustLoad(t, []string{"--sink.type=mysql", "--sink.mysql.addr=/run/mysqld/mysqld.sock"}, nil)
	mustLoad(t, []string{"--sink.type=nats", "--sink.postgres.addr=h1,h2:5432"}, nil)
}

// §22 pendência 30: no configuration error carries a piece of the input that
// may be a secret. A URL that does not parse, a query key libpq does not
// know, an sslmode that is no sslmode, an INI line without '=', a word on the
// command line, a value swallowed by a flag left without its own.
func TestConfigErrorsDoNotEchoSecrets(t *testing.T) {
	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"redis://bob:Sup3rSecret/xyz@r1:6379/0",
		"redis://bob:Sup3rSecret%zz@r1:6379/0",
		"redis://bob:Sup3rSecret@r1:notaport/0",
		"postgres://bob:Sup3rSecret/xyz@pg/db",
		"postgres://bob:Sup3rSecret%zz@pg/db",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// A query key is named only if libpq knows it; the value never.
	for _, u := range []string{
		"postgres://u:p@pg/db?Sup3rSecret",
		"postgres://u:p@pg/db?sslmode=disable&Sup3rSecret=1",
		"postgres://u:p@pg/db?sslmode=Sup3rSecret",
		"postgres://u:p@pg/db?application_name=Sup3rSecret",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "Sup3r") {
			t.Errorf("%q: err = %v, want a refusal without the secret", u, err)
		}
	}
	// Controls: a libpq key is still named, so the operator sees what to move.
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db?connect_timeout=5"}, `"connect_timeout"`)
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@pg/db?sslmode=sometimes"}, "not a libpq sslmode")

	ini := filepath.Join(t.TempDir(), "c.ini")
	for _, content := range []string{
		"[sink.nats]\npassword: hunter2\n",
		"[sink.nats hunter2\n",
		"[sink.nats]\npassword: hunter2 = x\n",
		"password: hunter2 = x\n",
	} {
		writeFile(t, ini, content)
		err := mustFail(t, []string{"--config=" + ini}, nil, "line ")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: the INI error echoes the line: %v", content, err)
		}
	}
	// Controls: the same values on well-formed lines load, and a misspelt key
	// that looks like a key is still named.
	writeFile(t, ini, "[sink.nats]\nusername = u\npassword = hunter2\n")
	mustLoad(t, []string{"--config=" + ini}, nil)
	writeFile(t, ini, "[sink.nats]\npasword = hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, `"sink.nats.pasword"`); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("unknown key error echoes the value: %v", err)
	}
	writeFile(t, ini, "username = u\n")
	mustFail(t, []string{"--config=" + ini}, nil, `"username"`)

	err := mustFail(t, []string{"sink.nats.password=hunter2"}, nil, "unexpected argument")
	if strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "sink.nats.password") {
		t.Errorf("flag error = %v, want the name and not the value", err)
	}
	// A flag whose value was left out swallows the next argument: a secret in
	// it is never printed, nor one typed after a space past the '=', nor one
	// a ':' put in the name.
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
		{"--sink.nats.password", "--hunter2"},
		{"--sink.nats.password=", "-hunter2"},
		{"--sink.nats.password", "=", "-hunter2"},
		{"--sink.nats.password=", "--hunter2=x"},
		{"--sink.nats.password=foo", "hunter2"},
		{"--sink.nats.password", "foo", "hunter2"},
		{"--sink.nats.password:hunter2"},
		{"--sink.nats.password hunter2"},
		// A ':' typed for the '=' after a misspelt name: not a longer name,
		// so only the shape of a name keeps it out (paramNameRE).
		{"--sink.nats.pasword:hunter2"},
		// After an empty value, even a flag-shaped word inside a known
		// section is that value.
		{"--sink.nats.password=", "--sink.nats.hunter2"},
		{"--sink.nats.password", "-h"},
	} {
		err := mustFail(t, args, nil, "")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: the error echoes the secret: %v", args, err)
		}
	}
	// Controls: a positional argument with no '=' is still named, a value of
	// the separated form is still taken, a negative number is a value and not
	// a flag, a switch is a flag, a typo after a real value is still named,
	// and each refusal says what went wrong.
	mustFail(t, []string{"extra"}, nil, `"extra"`)
	if l := mustLoad(t, []string{"--sink.workers", "4", "--sink.nats.stream", "LOGHUB_X"}, nil); l.Config.Sink.Workers != 4 ||
		l.Config.SinkNATS.Stream != "LOGHUB_X" {
		t.Errorf("separated values: workers=%d stream=%q", l.Config.Sink.Workers, l.Config.SinkNATS.Stream)
	}
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "-1 is outside")
	mustFail(t, []string{"--sink.nats.stream", "--help"}, nil, "the next argument is the flag --help")
	mustFail(t, []string{"--sink.nats.password=", "--admin.enabled", "extra"}, nil, `"extra"`)
	mustFail(t, []string{"--sink.nats.password=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password", "=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password=foo", "hunter2"}, nil, "goes in quotes")
	// After a value, an unknown flag outside every section may be the tail
	// of that value: not named. A typo inside a known section is
	// (knownSectionOf).
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, "unexpected argument after --sink.workers")
	mustFail(t, []string{"--sink.workers=4", "--sink.wrkers=1"}, nil, `"sink.wrkers"`)
	mustFail(t, []string{"--sink.nats.password", "--sink.workers=4"}, nil, "the flag --sink.workers")
	// A value split by the shell at a space, whose tail starts with '-'.
	if _, err := load(t, []string{"--sink.nats.password=abc", "-hunter2", "--sink.nats.username=u"}, nil); err == nil ||
		strings.Contains(err.Error(), "hunter2") {
		t.Errorf("split tail: %v", err)
	}
}

// An unescaped '/', '?' or '#' in a password makes net/url end the authority
// early: in "postgres://u:12/ss@h/db" the host was "u:12" and the password's
// tail went to the path, accepted and later echoed by a dial error. Refused,
// without quoting the URL; an escaped '/' (%2F) and an ordinary URL still
// parse.
func TestSlashInTheUserinfoIsRefusedWithoutEcho(t *testing.T) {
	for _, u := range []string{
		"postgres://u:12/ss@h/db",
		"redis://:pa/ss@h:6379/0",
		"nats://tok/en@h:4222",
		"postgres://app:39182#Zsecret9@db.example/loghub?sslmode=require",
		"postgres://app:2024?S3cret@db.example/loghub",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || !strings.Contains(err.Error(), "%2F") {
			t.Errorf("%s: err = %v, want the refusal with the escape hint", u, err)
			continue
		}
		for _, piece := range []string{"12/ss", "pa/ss", "tok/en", "ss@h", "39182", "Zsecret", "S3cret", "db.example"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
	}
	for _, u := range []string{"postgres://u:12%2Fss@h/db", "redis://h:6379/0", "nats://u:p@h:4222",
		"postgres://u:p@h/db?sslmode=require", "redis://:p@ss@h:6379/0"} {
		if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u}); err != nil {
			t.Errorf("%s refused: %v", u, err)
		}
	}
}

// libpq has no fragment: "#..." in a PostgreSQL URL is a password cut short or
// a query thrown away. Refused, without echo.
func TestPostgresURLFragmentIsRefused(t *testing.T) {
	for _, u := range []string{"postgres://u:p@db.example/loghub#Zfr4g", "postgres://u:p@db.example/loghub#"} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": u}, "no fragment")
		if strings.Contains(err.Error(), "Zfr4g") || strings.Contains(err.Error(), "db.example") {
			t.Errorf("%s: echoed: %v", u, err)
		}
	}
}

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

// An explicit empty --config fell back to SYSLOG_CONFIG.
func TestEmptyConfigFlagIsRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), "env.ini")
	writeFile(t, p, "[log]\nlevel = debug\n")
	mustFail(t, []string{"--config="}, map[string]string{"SYSLOG_CONFIG": p}, "empty value")
	if l := mustLoad(t, nil, map[string]string{"SYSLOG_CONFIG": p}); l.Config.Log.Level != "debug" {
		t.Fatalf("control: SYSLOG_CONFIG not read: %q", l.Config.Log.Level)
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
		{[]string{"--listen.tcp_listeners=100000"}, "listen.tcp_listeners"},
		{[]string{"--listen.udp_listeners=100000"}, "listen.udp_listeners"},
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
		"--sink.batch=65536", "--ring.sqlite.commit_batch=65536", "--listen.tcp_listeners=256",
		"--listen.udp_listeners=256", "--listen.tls_listeners=256"}, nil)
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		mustLoad(t, []string{"--sink.type=" + s, "--sink.workers=8", "--sink." + s + ".pool_size=4096"}, nil)
	}
}

// Every worker holds a pool connection while it writes, so the pool covers
// the workers. The health probe used to take its connection out of the same
// pool, and the rule was workers + 1; it has a connection of its own now
// (SPEC §10.5; independent review of the port, 2026-10-13), and pool_size =
// workers is legitimate.
func TestPoolCoversTheWorkers(t *testing.T) {
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		mustFail(t, []string{"--sink.type=" + s, "--sink.workers=4", "--sink." + s + ".pool_size=3"}, nil,
			"below sink.workers=4")
		mustLoad(t, []string{"--sink.type=" + s, "--sink.workers=4", "--sink." + s + ".pool_size=4"}, nil)
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

// A derived value is written commented in the dump, so reading the dump back
// derives it again instead of fixing it.
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
