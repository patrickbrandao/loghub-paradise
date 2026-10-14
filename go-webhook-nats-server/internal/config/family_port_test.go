package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The configuration findings of the syslog/RELP family (go-relplog-server,
// go-relplog-pg-server, go-relplog-nats-server and their ports into
// go-syslog-server, go-syslog-pg-server and go-syslog-nats-server), ported
// into the webhook family on 2026-10-13 (SPEC §13.2, §10.1). Each test failed
// before its fix in this server, or pins a configuration the fixes must keep
// accepting. The helpers carry a prefix of their own so the file is the same
// in every module of the family.

func famTry(args []string, env map[string]string) (*Loaded, error) {
	return Load(Options{Args: args, LookupEnv: envMap(env)})
}

func famLoad(t *testing.T, args []string, env map[string]string) *Loaded {
	t.Helper()
	l, err := famTry(args, env)
	if err != nil {
		t.Fatalf("%v %v: %v", args, env, err)
	}
	return l
}

// famFail asserts a refusal that names want, and returns it.
func famFail(t *testing.T, args []string, env map[string]string, want string) error {
	t.Helper()
	_, err := famTry(args, env)
	if err == nil {
		t.Fatalf("%v %v was accepted; want an error naming %q", args, env, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("%v %v: err = %v, want it to contain %q", args, env, err, want)
	}
	return err
}

func famINI(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fam.ini")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// After a value, an unknown flag may be the tail of that value the shell split
// at a space: it is named only when it is a typo inside a section this server
// has (knownSectionOf). It used to come out as "unknown parameter" with the
// tail quoted -- the gap the port back into go-webhook-nats-server left open
// on purpose (2026-10-11), closed by go-relplog-server's rule.
func TestFamilyPortTailAfterAValueIsNotNamed(t *testing.T) {
	for _, args := range [][]string{
		{"--sink.nats.password=abc", "-hunter2"},
		{"--sink.nats.password=abc", "--hunter2=y"},
		{"--sink.nats.password=abc", "-hunter2", "--sink.nats.username=u"},
		{"--log.level=info", "-hunter2.x"},
	} {
		_, err := famTry(args, nil)
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: err = %v, want a refusal that does not echo the tail", args, err)
		}
	}
	// Controls: a typo inside a known section after a value is named; one
	// outside every section blames the value before it; standing alone, any
	// typo is named.
	famFail(t, []string{"--sink.workers=4", "--sink.wrkers=1"}, nil, `"sink.wrkers"`)
	famFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, "unexpected argument after --sink.workers")
	famFail(t, []string{"--typo.key=1"}, nil, `"typo.key"`)
}

// "-h" or "-v" after a flag that wants a value is also what a two-character
// value starting with a dash looks like: it is not named as the flag.
func TestFamilyPortShortDashValueIsNotNamedAsAFlag(t *testing.T) {
	for _, v := range []string{"-h", "-v"} {
		err := famFail(t, []string{"--sink.nats.password", v}, nil, "starts with a dash")
		if strings.Contains(err.Error(), "flag --"+v[1:]) {
			t.Errorf("%s: named as a flag: %v", v, err)
		}
	}
	famFail(t, []string{"--sink.nats.stream", "--help"}, nil, "the next argument is the flag --help")
}

// The '=' forgotten and the value glued to the name -- with or without a
// separator --, or a ':' typed for the '=', was printed as the name of an
// unknown parameter.
func TestFamilyPortGluedValueIsNotPrinted(t *testing.T) {
	for _, args := range [][]string{
		{"--sink.nats.passwordHunter2xyz"},
		{"--sink.nats.password-Hunter2xyz"},
		{"--sink.nats.password_Hunter2xyz"},
		{"--sink.nats.password.Hunter2xyz"},
		{"--sink.nats.passwordHunter2xyz=1"},
		{"--sink.nats.password:Hunter2xyz"},
		{"--sink.nats.pasword:Hunter2xyz"},
		{"--sink.nats.password Hunter2xyz"},
	} {
		_, err := famTry(args, nil)
		if err == nil || strings.Contains(err.Error(), "Hunter2xyz") {
			t.Errorf("%v: err = %v", args, err)
		}
	}
	for _, body := range []string{
		"[sink.nats]\npasswordhunter2xyz = 1\n",
		"[sink.nats]\npassword: hunter2xyz = 1\n",
		"password: hunter2xyz = 1\n",
		"[hunter2xyz]\nx = 1\n",
	} {
		if _, err := famTry([]string{"--config=" + famINI(t, body)}, nil); err == nil ||
			strings.Contains(err.Error(), "hunter2xyz") {
			t.Errorf("INI %q: err = %v", body, err)
		}
	}
	// Controls: a typo that is not a longer name is still named, on the
	// command line and in the INI, and so is a key before any section.
	famFail(t, []string{"--sink.nats.strem=x"}, nil, `"sink.nats.strem"`)
	famFail(t, []string{"--config=" + famINI(t, "[sink.nats]\nstrem = x\n")}, nil, `"sink.nats.strem"`)
	famFail(t, []string{"--config=" + famINI(t, "username = u\n")}, nil, `"username"`)
}

// A secret that wraps onto the next line of the INI turns its tail into a key,
// and the error printed it -- to stderr at boot and, on a SIGHUP, to the log.
func TestFamilyPortINISecretContinuationIsNotPrinted(t *testing.T) {
	for i, tc := range []struct{ ini, secret string }{
		{"[sink.nats]\npassword = eyJhbGciOiJI\nUzI1NiJ9abcDEF==\n", "UzI1NiJ9abcDEF"},
		{"[sink.nats]\nusername = u\npassword = abc\nffd3a9c1b2=\n", "ffd3a9c1b2"},
		{"[sink.nats]\npassword = abc\nffd3a9c1b2 = x\n", "ffd3a9c1b2"},
		{"Sup3rS3cret = 1\n[log]\nlevel = info\n", "Sup3rS3cret"},
		// Not key-shaped (case, a dash), even after a value that is not
		// secret.
		{"[sink.nats]\nurls = nats://u:eyJhbGciOiJI\nffd3a9c1b2 = 1\n", "ffd3a9c1b2"},
		{"[sink.nats]\ntoken = abc\nffd3a9c1b2 = x\n", "ffd3a9c1b2"},
		{"[sink.nats]\nstream = X\nAbC-Hunter2xyz = 1\n", "Hunter2xyz"},
	} {
		_, err := famTry([]string{"--config=" + famINI(t, tc.ini)}, nil)
		if err == nil || strings.Contains(err.Error(), tc.secret) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	// Control: a typo after a value that is not secret is still named, with
	// its line.
	err := famFail(t, []string{"--config=" + famINI(t, "[sink.nats]\nstream = X\nstrem = x\n")},
		nil, `"sink.nats.strem"`)
	if !strings.Contains(err.Error(), "line 3") {
		t.Errorf("typo: %v", err)
	}
}

// A switch takes no value: "--dump-config=false" dumped and exited 0.
func TestFamilyPortSwitchesTakeNoValue(t *testing.T) {
	for _, arg := range []string{"--dump-config=false", "--help=no", "--version=Zsecret", "-h=1", "-v=0",
		"--dump-config="} {
		err := famFail(t, []string{arg}, nil, "takes no value")
		if strings.Contains(err.Error(), "Zsecret") {
			t.Errorf("%s: value echoed: %v", arg, err)
		}
	}
	if l := famLoad(t, []string{"--dump-config"}, nil); !l.Meta.DumpConfig {
		t.Fatal("control: --dump-config not recorded")
	}
	if _, err := famTry([]string{"--help"}, nil); err != ErrExitEarly {
		t.Fatalf("control: --help = %v", err)
	}
}

// An explicit empty --config fell back to WHS_CONFIG, the opposite of what it
// says.
func TestFamilyPortEmptyConfigFlagIsRefused(t *testing.T) {
	p := famINI(t, "[log]\nlevel = debug\n")
	famFail(t, []string{"--config="}, map[string]string{"WHS_CONFIG": p}, "empty value")
	famFail(t, []string{"--config", ""}, map[string]string{"WHS_CONFIG": p}, "empty value")
	if l := famLoad(t, nil, map[string]string{"WHS_CONFIG": p}); l.Config.Log.Level != "debug" {
		t.Fatalf("control: WHS_CONFIG not read: %q", l.Config.Log.Level)
	}
}

// A UTF-8 byte-order mark at the start of the INI is not part of the line.
func TestFamilyPortINIByteOrderMarkIsIgnored(t *testing.T) {
	p := famINI(t, "\ufeff[log]\nlevel = debug\n")
	if l := famLoad(t, []string{"--config=" + p}, nil); l.Config.Log.Level != "debug" {
		t.Fatalf("level = %q", l.Config.Log.Level)
	}
	// Only on line 1: further down it is still an error.
	famFail(t, []string{"--config=" + famINI(t, "[log]\n\ufefflevel = debug\n")}, nil, "line 2")
}

// A derived value is written commented in the dump, so reading the dump back
// derives it again instead of fixing it: a pool that stops following
// sink.workers, or a path that stops following ring.data_dir (and lands
// outside the one writable mount, SPEC §13.6).
func TestFamilyPortDumpCommentsDerivedValues(t *testing.T) {
	l := famLoad(t, nil, nil)
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	_, em, _ := strings.Cut(out, "[emergency]\n")
	em, _, _ = strings.Cut(em, "\n[")
	if !strings.Contains(em, "# dir ") || strings.Contains(em, "\ndir ") || strings.HasPrefix(em, "dir ") ||
		!strings.Contains(em, "data/emergency") {
		t.Errorf("[emergency] dir not commented:\n%s", em)
	}
	back := famLoad(t, []string{"--config=" + famINI(t, out), "--ring.data_dir=/elsewhere"}, nil)
	if got := back.Config.Emergency.Dir; !strings.HasPrefix(got, "/elsewhere/") {
		t.Errorf("read back with another data_dir, emergency.dir = %q: the dump fixed the derived path", got)
	}
	// Control: an explicit value stays a key, and the dump reads back to the
	// same configuration.
	l = famLoad(t, []string{"--emergency.dir=/e"}, nil)
	buf.Reset()
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "\ndir                            = /e\n") {
		t.Errorf("an explicit value was commented:\n%s", buf.String())
	}
	back = famLoad(t, []string{"--config=" + famINI(t, buf.String())}, nil)
	for k, v := range l.Effective() {
		if back.Effective()[k] != v {
			t.Errorf("%s did not round-trip: %q -> %q", k, v, back.Effective()[k])
		}
	}
}

// Ceilings on the keys that size memory or a count handed to a library, exit
// 1 naming the key: without them a typo of a few digits panicked the boot
// (makechan, makeslice), wrapped around in the pool's int32, or overflowed the
// NATS MaxPending into a negative number replaced by 4096 in silence.
func TestFamilyPortConfigCeilings(t *testing.T) {
	for _, tc := range []struct {
		args []string
		key  string
	}{
		{[]string{"--ring.capacity_items=16777217"}, "ring.capacity_items"},
		{[]string{"--ring.capacity_items=35184372088832"}, "ring.capacity_items"},
		{[]string{"--sink.batch=1048577"}, "sink.batch"},
		{[]string{"--sink.workers=257"}, "sink.workers"},
		{[]string{"--server.listeners=257"}, "server.listeners"},
	} {
		famFail(t, tc.args, nil, tc.key)
	}
	// Control: the ceilings themselves load.
	famLoad(t, []string{"--ring.capacity_items=16777216", "--sink.batch=1048576", "--sink.workers=256",
		"--server.listeners=256"}, nil)
}

// LOGHUB_EB_URL (SPEC §8.7.11): a value of only blanks was read as "unset",
// and the [sink.nats] keys ran with nothing to say so; an '@' after the first
// '/', '?' or '#' -- a password that an unescaped character cut short -- and
// a percent-encoded host are refused saying what is wrong, and no message
// quotes the URL.
func TestFamilyPortEBURL(t *testing.T) {
	for _, v := range []string{"   ", "\t"} {
		famFail(t, nil, map[string]string{"LOGHUB_EB_URL": v}, "only blanks")
	}
	// A newline is refused earlier, as any control byte in a text value
	// (SPEC §13.2, W 41).
	famFail(t, nil, map[string]string{"LOGHUB_EB_URL": " \n "}, "control byte")
	for _, tc := range []struct{ url, want string }{
		{"nats://tok/en@h:4222", "%2F"},
		{"nats://app:39182#Zsecret9@nats-1:4222", "%2F"},
		{"nats://app:2024?S3cret@nats-1:4222", "%2F"},
		{"nats://u:p@h:4222,nats://u:12/Zsecret9@h2:4222", "%2F"},
		{"nats://u:Zsecret9@n%41ts:4222", "percent-encoded host"},
		// An escape past the host is not the host's.
		{"nats://u:Zsecret9@nats-1:4222/%zz", "invalid percent-escape"},
		{"nats://u:Zsecret9@nats-1:4222#Zfrag", "no path, query or fragment"},
		{"nats://u:Zsecret9@nats-1:4222#", "no path, query or fragment"},
		{"nats://u:Zsecret9@nats-1:4222?", "no path, query or fragment"},
	} {
		err := famFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		for _, piece := range []string{"tok/en", "39182", "Zsecret", "S3cret", "2024", "Zfrag", "nats-1", "n%41ts"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", tc.url, piece, err)
			}
		}
	}
	// Controls: unset is "no URL"; an escaped '/' and a raw '@' in the
	// password (everything up to the LAST '@' is userinfo) still parse; an
	// explicit empty key does not compete with the URL.
	famLoad(t, nil, map[string]string{"LOGHUB_EB_URL": ""})
	for _, u := range []string{"nats://u:12%2Fss@h:4222", "nats://u:p@ss@h:4222", "nats://h:4222/"} {
		famLoad(t, nil, map[string]string{"LOGHUB_EB_URL": u})
	}
	famLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@h:4222", "WHS_SINK_NATS_PASSWORD": ""})
}

// A configuration carried over from go-webhook-fast-server puts a key of a
// section this server dropped after a valued flag, or in the INI. The tail
// rule would read it as the split tail of the value before it and blame that
// flag; it is named, as when it stands alone (retiredSections).
func TestFamilyPortRetiredKeyAfterAValueIsNamed(t *testing.T) {
	for _, name := range []string{"sink.redis.addrs", "ring.sqlite.path", "sink.mysql.addr", "sink.clickhouse.addrs",
		"sink.sqlite.path", "sink.postgres.addr"} {
		famFail(t, []string{"--sink.workers=4", "--" + name + "=x"}, nil, `"`+name+`"`)
		sec, key := name[:strings.LastIndexByte(name, '.')], name[strings.LastIndexByte(name, '.')+1:]
		famFail(t, []string{"--config=" + famINI(t, "["+sec+"]\n"+key+" = x\n")}, nil, `"`+name+`"`)
	}
}
