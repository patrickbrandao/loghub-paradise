package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
)

// Regression tests of the independent review of go-relplog-pg-server and
// go-relplog-nats-server on 2026-10-13, ported from go-relplog-pg-server's
// review_cut_test.go (TESTING §9). Each failed here before its port.

// removedParams is removedEnv in the dotted spelling: the same 96 keys of
// go-syslog-server, so the env WARN and the flag/INI error name the same set.
func TestRemovedParamsAreTheRemovedEnv(t *testing.T) {
	var fromParams []string
	for name := range removedParams {
		sec, key := name[:strings.LastIndexByte(name, '.')], name[strings.LastIndexByte(name, '.')+1:]
		prefix := "SYSLOG_" + strings.ToUpper(strings.ReplaceAll(sec, ".", "_")) + "_"
		if sec == "loghub" {
			prefix = "LOGHUB_"
		}
		fromParams = append(fromParams, prefix+strings.ToUpper(key))
	}
	sort.Strings(fromParams)
	want := append([]string(nil), removedEnv...)
	sort.Strings(want)
	if !reflect.DeepEqual(fromParams, want) {
		t.Fatalf("removedParams and removedEnv differ:\n params %v\n env    %v", fromParams, want)
	}
	for sec := range removedSections {
		for _, p := range Registry(&Config{}) {
			if p.Sec == sec {
				t.Errorf("removed section [%s] still has %s", sec, p.Name())
			}
		}
	}
}

// A command line carried over from go-syslog-server puts a removed flag after
// a valued one. The tail rule (SPEC §16.2) read it as the split tail of that
// value and blamed --sink.workers; it is named, as when it stands alone, and
// says where it comes from.
func TestRemovedFlagAfterAValuedFlagIsNamed(t *testing.T) {
	for _, f := range []string{"--filter.min_severity=warning", "--tls.cert_file=/c",
		"--sink.nats.urls=nats://h", "--ratelimit.enabled=true", "--ring.sqlite.path=/x",
		"--sink.postgres.tls=true", "--ring.mode=memory", "--sink.type=postgres"} {
		err := mustFail(t, []string{"--sink.workers=4", f}, nil, "unknown parameter")
		name := strings.TrimPrefix(strings.SplitN(f, "=", 2)[0], "--")
		if !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "go-syslog-server has it") {
			t.Errorf("%s after a valued flag: %v", f, err)
		}
	}
	// Control: the split tail of a value is still not printed.
	_, err := load(t, []string{"--sink.postgres.password=abc", "-hunter2"}, nil)
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("split tail: %v", err)
	}
}

// A secret that wraps onto the next line of the INI turns its tail into a key;
// the error printed it, and on SIGHUP into the server log. Same for a key
// before any section and a section header that is not one (SPEC §13.2).
func TestINIUnknownKeyNeverPrintsASecretContinuation(t *testing.T) {
	dir := t.TempDir()
	for i, tc := range []struct{ ini, secret string }{
		{"[sink.postgres]\npassword = eyJhbGciOiJI\nUzI1NiJ9abcDEF==\n", "UzI1NiJ9abcDEF"},
		{"[sink.postgres]\nusername = u\npassword = abc\nffd3a9c1b2=\n", "ffd3a9c1b2"},
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

// R1: the '=' forgotten, the value glued to the name -- with or without a
// separator -- was printed as the name of an unknown parameter.
func TestGluedValueIsNotPrinted(t *testing.T) {
	for _, arg := range []string{"--sink.postgres.passwordHunter2xyz", "--sink.postgres.password-Hunter2xyz",
		"--sink.postgres.password_Hunter2xyz", "--sink.postgres.password.Hunter2xyz"} {
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

// R3: a switch takes no value; "--dump-config=false" dumped and exited 0.
func TestSwitchesTakeNoValue(t *testing.T) {
	for _, arg := range []string{"--dump-config=false", "--help=no", "--version=Zsecret", "-h=1"} {
		err := mustFail(t, []string{arg}, nil, "takes no value")
		if strings.Contains(err.Error(), "Zsecret") {
			t.Errorf("%s: value echoed: %v", arg, err)
		}
	}
	if l, err := load(t, []string{"--dump-config"}, nil); err != nil || !l.Meta.DumpConfig {
		t.Fatalf("control: --dump-config = %v, %v", l, err)
	}
}

// N7: an explicit empty --config fell back to SYSLOG_CONFIG.
func TestEmptyConfigFlagIsRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), "env.ini")
	writeFile(t, p, "[log]\nlevel = debug\n")
	mustFail(t, []string{"--config="}, map[string]string{"SYSLOG_CONFIG": p}, "empty value")
	if l := mustLoad(t, nil, map[string]string{"SYSLOG_CONFIG": p}); l.Config.Log.Level != "debug" {
		t.Fatalf("control: SYSLOG_CONFIG not read: %q", l.Config.Log.Level)
	}
}

// N8: a UTF-8 byte-order mark at the start of the INI is not part of the line.
func TestINIByteOrderMarkIsIgnored(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bom.ini")
	if err := os.WriteFile(p, []byte("\ufeff[log]\nlevel = debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if l := mustLoad(t, []string{"--config=" + p}, nil); l.Config.Log.Level != "debug" {
		t.Fatalf("level = %q", l.Config.Log.Level)
	}
}

// R4, N2, S-A: ceilings on the keys that size memory or a count handed to a
// library, exit 1 naming the key (SPEC §13.2).
func TestConfigCeilings(t *testing.T) {
	for _, tc := range []struct{ arg, key string }{
		{"--sink.batch_max_bytes=1GiB", "sink.batch_max_bytes"},
		{"--sink.postgres.pool_size=4294967300", "sink.postgres.pool_size"},
		{"--sink.postgres.pool_size=2147483648", "sink.postgres.pool_size"},
		{"--sink.workers=100000", "sink.workers"},
		{"--ring.capacity_items=35184372088832", "ring.capacity_items"},
		{"--sink.batch=1000000000", "sink.batch"},
		{"--listen.tcp_listeners=100000", "listen.tcp_listeners"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.key)
	}
	// Control: the ceilings themselves load.
	mustLoad(t, []string{"--sink.batch_max_bytes=256MiB", "--sink.workers=1024", "--sink.postgres.pool_size=4096",
		"--ring.capacity_items=16777216", "--sink.batch=65536", "--listen.tcp_listeners=256"}, nil)
}

// C2, C3, R2: sink.postgres.addr and the URL that fills it name one server in
// every reading -- this server's and libpq's.
func TestPostgresAddrNamesOneServer(t *testing.T) {
	for _, addr := range []string{"db\t1:5432", "[h:5432]:5433", "/run/pg,/x", "/run/p g", "h1,h2:5432"} {
		mustFail(t, []string{"--sink.postgres.addr=" + addr}, nil, "sink.postgres.addr")
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
		mustLoad(t, []string{"--sink.postgres.addr=" + addr}, nil)
	}
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@:5433/db?host=/run/pg"})
}

// N1/C5, N3, N4, N6: what the URL must say, and the messages when it does not.
func TestEBURLMustNameItsParts(t *testing.T) {
	for _, tc := range []struct{ url, want string }{
		{"postgres://h/db", "names no user"},
		{"postgres://:Zsecret@h/db", "names no user"},
		{"postgres://u:Zsecret@h", "names no database"},
		{"postgres://u:Zsecret@h/", "names no database"},
		{"postgres://u:Zsecret@h/db?sslmode=", "sslmode of the PostgreSQL URL is empty"},
		// An empty host= is "not set" to libpq and pgx (their default socket
		// or localhost), "h" to this server (independent review of
		// go-syslog-server, 2026-10-13, B4).
		{"postgres://u:Zsecret@h/db?host=", "host= of the PostgreSQL URL is empty"},
		{"postgres://u:Zsecret@h:5433/db?sslmode=disable&host=", "host= of the PostgreSQL URL is empty"},
		{"postgres://u:Zsecret@%2Frun%2Fpg/db", "percent-encoded host"},
		{"postgres://u:Zs://ecret@h/db", "must be percent-encoded"},
		{"   ", "only blanks"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if strings.Contains(err.Error(), "Zsecret") || strings.Contains(err.Error(), "ecret") {
			t.Errorf("%s: echoed: %v", tc.url, err)
		}
	}
	// Control: unset is "no URL" (only blanks is not).
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""})
}

// N9: a derived value is written commented in the dump, so reading the dump
// back derives it again instead of fixing it.
func TestDumpCommentsDerivedValues(t *testing.T) {
	l := mustLoad(t, []string{"--sink.workers=3"}, nil)
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "# pool_size                    = 4") || strings.Contains(out, "\npool_size ") {
		t.Fatalf("derived pool_size not commented:\n%s", out)
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, out)
	back := mustLoad(t, []string{"--config=" + f, "--sink.workers=7"}, nil)
	if back.Config.SinkPG.PoolSize != 8 {
		t.Fatalf("read back with 7 workers, pool_size = %d: the dump fixed the derived value", back.Config.SinkPG.PoolSize)
	}
}

// N10: a synonym of the charset is not a change.
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

// Test gaps the review's mutants found: each check below survived a mutant.
func TestReviewMutantGaps(t *testing.T) {
	// admin.enabled=true with an empty admin.bind.
	mustFail(t, []string{"--admin.bind="}, nil, "admin.bind is empty")
	mustLoad(t, []string{"--admin.bind=", "--admin.enabled=false"}, nil)
	// A raw '@' in the password: everything up to the LAST '@' is userinfo.
	if got := redactOneURL("postgres://u:p@ss@db/x"); got != "postgres://u:***@db/x" {
		t.Errorf("redactOneURL = %q", got)
	}
	// The scheme of a refused URL is named only when it looks like one.
	if _, err := parseEBURL("Zsecret!x://h"); err == nil || strings.Contains(err.Error(), "Zsecret") {
		t.Errorf("odd scheme: %v", err)
	}
	if _, err := parseEBURL("nats://h"); err == nil || !strings.Contains(err.Error(), `"nats://"`) {
		t.Errorf("a scheme that looks like one is named: %v", err)
	}
}
