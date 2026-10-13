package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/convert"
)

// Regression tests from the independent review of go-relplog-pg-server on
// 2026-10-13, ported here: the flags, the INI, the dump, the SIGHUP compare,
// the ceilings and the LOGHUB_EB_URL messages are common to the two servers
// (TESTING §9). Each failed here before its port.

// removedParams is removedEnv in the dotted spelling: the same keys of
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

// R1: the '=' forgotten, the value glued to the name -- with or without a
// separator -- was printed as the name of an unknown parameter.
func TestGluedValueIsNotPrinted(t *testing.T) {
	for _, arg := range []string{"--sink.nats.passwordHunter2xyz", "--sink.nats.password-Hunter2xyz",
		"--sink.nats.password_Hunter2xyz", "--sink.nats.password.Hunter2xyz", "--sink.nats.tokenHunter2xyz"} {
		_, err := load(t, []string{arg}, nil)
		if err == nil || strings.Contains(err.Error(), "Hunter2xyz") {
			t.Errorf("%s: err = %v", arg, err)
		}
	}
	p := filepath.Join(t.TempDir(), "g.ini")
	writeFile(t, p, "[sink.nats]\npasswordhunter2xyz = 1\n")
	if _, err := load(t, []string{"--config=" + p}, nil); err == nil || strings.Contains(err.Error(), "hunter2xyz") {
		t.Errorf("INI: err = %v", err)
	}
	// Control: a typo that is not a longer name is still named.
	mustFail(t, []string{"--sink.nats.subjet=x"}, nil, `"sink.nats.subjet"`)
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
// library, exit 1 naming the key (SPEC §13.2). sink.workers and
// sink.batch_max_bytes have the NATS ceilings of this server (DECISOES §2.2).
func TestConfigCeilings(t *testing.T) {
	for _, tc := range []struct{ arg, key string }{
		{"--sink.batch_max_bytes=1GiB", "sink.batch_max_bytes"},
		{"--sink.batch_max_bytes=257MiB", "sink.batch_max_bytes"},
		{"--sink.workers=257", "sink.workers"},
		{"--sink.workers=100000", "sink.workers"},
		{"--ring.capacity_items=35184372088832", "ring.capacity_items"},
		{"--sink.batch=1000000000", "sink.batch"},
		{"--listen.tcp_listeners=100000", "listen.tcp_listeners"},
	} {
		mustFail(t, []string{tc.arg}, nil, tc.key)
	}
	// Control: the ceilings themselves load, and with them the
	// PublishAsyncMaxPending the app derives (2 x workers x batch, SPEC
	// §10.7) is a positive int.
	l := mustLoad(t, []string{"--sink.batch_max_bytes=256MiB", "--sink.workers=256",
		"--ring.capacity_items=16777216", "--sink.batch=65536", "--listen.tcp_listeners=256"}, nil)
	if p := 2 * l.Config.Sink.Workers * l.Config.Sink.Batch; p != 1<<25 {
		t.Fatalf("2 x workers x batch at the ceilings = %d, want %d", p, 1<<25)
	}
}

// N3 and the NATS reading of N6: a LOGHUB_EB_URL of only blanks is set, to
// nothing, and a percent-encoded host is not a password problem.
func TestEBURLOnlyBlanksIsRefused(t *testing.T) {
	for _, tc := range []struct{ url, want string }{
		{"   ", "only blanks"},
		{"\t", "only blanks"},
		{"nats://u:Zsecret@%41h:4222", "percent-encoded host"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if strings.Contains(err.Error(), "Zsecret") {
			t.Errorf("%q: echoed: %v", tc.url, err)
		}
	}
	mustFail(t, []string{"--sink.nats.urls=nats://u:Zsecret@%41h:4222"}, nil, "percent-encoded host")
	// Controls: unset is "no URL"; an invalid escape in the password is still
	// said to be one.
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""})
	mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:Zs%zzecret@h:4222"}, "invalid percent-escape")
}

// N9: a derived value is written commented in the dump, so reading the dump
// back derives it again instead of fixing it. Here that is emergency.dir,
// derived from server.data_dir.
func TestDumpCommentsDerivedValues(t *testing.T) {
	dir := t.TempDir()
	l := mustLoad(t, []string{"--server.data_dir=" + dir}, nil)
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	want := "# dir                          = " + filepath.Join(dir, "emergency")
	if !strings.Contains(out, want) || strings.Contains(out, "\ndir ") {
		t.Fatalf("derived emergency.dir not commented:\n%s", out)
	}
	f := filepath.Join(t.TempDir(), "dump.ini")
	writeFile(t, f, out)
	other := t.TempDir()
	back := mustLoad(t, []string{"--config=" + f, "--server.data_dir=" + other}, nil)
	if back.Config.Emergency.Dir != filepath.Join(other, "emergency") {
		t.Fatalf("read back with another data_dir, emergency.dir = %s: the dump fixed the derived value",
			back.Config.Emergency.Dir)
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

// Test gaps the mutants of the review of go-relplog-pg-server found in the
// code the two servers share: each check below survived a mutant there.
func TestReviewMutantGaps(t *testing.T) {
	// admin.enabled=true with an empty admin.bind.
	mustFail(t, []string{"--admin.bind="}, nil, "admin.bind is empty")
	mustLoad(t, []string{"--admin.bind=", "--admin.enabled=false"}, nil)
	// A raw '@' in the password: everything up to the LAST '@' is userinfo.
	if got := redactOneURL("nats://u:p@ss@h:4222"); got != "nats://u:***@h:4222" {
		t.Errorf("redactOneURL = %q", got)
	}
	// The scheme of a refused URL is named only when it looks like one.
	if _, err := parseEBURL("Zsecret!x://h"); err == nil || strings.Contains(err.Error(), "Zsecret") {
		t.Errorf("odd scheme: %v", err)
	}
	if _, err := parseEBURL("postgres://h/db"); err == nil || !strings.Contains(err.Error(), `"postgres://"`) {
		t.Errorf("a scheme that looks like one is named: %v", err)
	}
}
