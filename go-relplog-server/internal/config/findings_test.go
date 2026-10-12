package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// Tests for the configuration items of SPEC §22.1 fixed on 2026-10-11 (12, 13,
// 21, 22 and 30), ported with adaptation from go-syslog-nats-server,
// go-syslog-pg-server and go-webhook-nats-server.

// §22.1 item 12: both sinks put the database name raw between backticks, so a
// backtick, a backslash or a control byte would change the statement, and a
// name above 64 bytes is refused by MySQL only after the boot wait.
func TestDatabaseNamesFitBetweenBackticks(t *testing.T) {
	long := strings.Repeat("d", 64)
	for _, sec := range []string{"mysql", "clickhouse"} {
		base := []string{"--sink.type=" + sec}
		key := "--sink." + sec + ".database="
		// Controls: the default, a name at the ceiling, and names that are
		// odd but quote fine.
		for _, ok := range []string{"syslog", long, "logs-2026", "Dados.Brutos", "a b"} {
			mustLoad(t, append(base, key+ok), nil)
		}
		for _, tc := range []struct{ name, want string }{
			{"sys`log", "backtick"},
			{"sys`; DROP DATABASE x; --", "backtick"},
			{`sys\log`, "backslash"},
			{"sys\nlog", "control byte"},
			{"sys\tlog", "control byte"},
			{"sys\x00log", "control byte"},
			{"sys\x7flog", "control byte"},
			{long + "d", "above 64"},
		} {
			mustFail(t, append(base, key+tc.name), nil, "sink."+sec+".database")
			mustFail(t, append(base, key+tc.name), nil, tc.want)
		}
		// The rule bites only when that database is the sink.
		mustLoad(t, []string{"--sink.type=sqlite", key + "a`b"}, nil)
	}
}

// §22.1 item 13: every entry of sink.nats.urls is parsed at validation, and no
// error quotes it; credentials in the URL and in a key are refused together.
func TestNATSURLsAreParsedWithoutEcho(t *testing.T) {
	// Controls: escaped credentials, a bare host:port (nats.go dials it as
	// nats://), a root path, a cluster, and a websocket endpoint with a path.
	for _, u := range []string{
		"nats://bob:s3cr%25t@n1:4222",
		"n1:4222",
		"n1:4222,n2:4222",
		"[::1]:4222",
		"nats://n1:4222/",
		"tls://n1:4222",
		"ws://n1:8080/nats",
		"nats://user:1234%2F5678@n1:4222/",
	} {
		mustLoad(t, []string{"--sink.nats.urls=" + u}, nil)
	}

	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"nats://bob:Sup3rSecret#xyz@n1:4222",
		"nats://bob:Sup3rSecret?xyz@n1:4222",
		"nats://bob:Sup3rSecret%zz@n1:4222",
		"nats://bob:Sup3rSecret,xyz@n1:4222",
		"nats://Sup3rSecretToken/x@n1:4222",
		"tls://Sup3rSecretToken/x@n1:4222",
		"bob:Sup3rSecret/x@n1:4222",
	} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "sink.nats.urls")
		if strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// The secret that parses: a password that reads as a port and a path, or a
	// query or a fragment. No credentials at all, dialled until boot_timeout.
	for _, u := range []string{"tls://n1:4222/x", "nats://n1:4222?x=1", "nats://n1:4222#f"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "path, a query or a fragment")
		if strings.Contains(err.Error(), "5678") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// '/', '?' and '#' in a password are named for what they are: one of them
	// unescaped, which ended the authority early.
	for _, u := range []string{"nats://user:1234/5678@n1:4222", "nats://user:1234#5678@n1:4222",
		"nats://user:1234?5678@n1:4222"} {
		if err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "%2F, %3F, %23"); strings.Contains(err.Error(), "5678") {
			t.Errorf("err = %v", err)
		}
	}
	mustFail(t, []string{"--sink.nats.urls=nats://:4222"}, nil, "no host")
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@"}, nil, "no host")
	mustFail(t, []string{"--sink.nats.urls=http://n1:4222"}, nil, `"http://"`)
	if err := mustFail(t, []string{"--sink.nats.urls=Sup3r$ecret://n1:4222"}, nil, "sink.nats.urls"); strings.Contains(err.Error(), "Sup3r") {
		t.Errorf("the scheme of a mangled URL is echoed: %v", err)
	}
	if got := schemeOf("Sup3r$ecret://n1"); got != "unrecognised" {
		t.Errorf("schemeOf of a mangled scheme = %s", got)
	}

	// Credentials in the URL and in a key: the client would silently use the
	// URL's. Every key, the .creds file included, and through both doors.
	for _, key := range []string{"--sink.nats.username=other", "--sink.nats.token=x",
		"--sink.nats.credentials=/etc/x.creds"} {
		mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", key}, nil, "carries credentials")
		mustFail(t, []string{"--sink.nats.urls=tok@n1:4222", key}, nil, "carries credentials")
	}
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", "--sink.nats.username=u",
		"--sink.nats.password=p"}, nil, "carries credentials")
	mustFail(t, []string{"--sink.nats.credentials=/etc/x.creds"},
		map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"}, "carries credentials")
	// Controls: one place at a time.
	mustLoad(t, []string{"--sink.nats.urls=nats://n1:4222", "--sink.nats.username=u", "--sink.nats.password=p"}, nil)
	mustLoad(t, []string{"--sink.nats.credentials=/etc/x.creds"}, map[string]string{"LOGHUB_EB_URL": "nats://n1:4222"})
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"})
	// The URLs are a NATS matter only.
	mustLoad(t, []string{"--sink.type=redis", "--sink.nats.urls=nats://u:p/x@n1"}, nil)
}

// §22.1 item 21: a beacon interval below one second is a flood, not a
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

// §22.1 item 22: the PostgreSQL URL and sink.postgres.addr.
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
	// The rule is PostgreSQL's: a MySQL socket path keeps working.
	mustLoad(t, []string{"--sink.type=mysql", "--sink.mysql.addr=/run/mysqld/mysqld.sock"}, nil)
}

// §22.1 item 30: no configuration error carries a piece of the input that
// may be a secret. A URL that does not parse, a query key libpq does not
// know, an sslmode that is no sslmode, an INI line without '=', a word on the
// command line, a value swallowed by a flag left without its own.
func TestConfigErrorsDoNotEchoSecrets(t *testing.T) {
	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"redis://bob:Sup3rSecret/xyz@r1:6379/0",
		"redis://bob:Sup3rSecret%zz@r1:6379/0",
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
		// The three paths of the review of the port back into
		// go-webhook-nats-server: a value starting with "--" in the separated
		// form was named as "the flag --hunter2"; a dash-value after an empty
		// '=' as an unknown parameter; the tail of a value the shell split at
		// a space as a stray word.
		{"--sink.nats.password", "--hunter2"},
		{"--sink.nats.password=", "-hunter2"},
		{"--sink.nats.password", "=", "-hunter2"},
		{"--sink.nats.password=", "--hunter2=x"},
		{"--sink.nats.password=foo", "hunter2"},
		{"--sink.nats.password", "foo", "hunter2"},
		{"--sink.nats.password:hunter2"},
		{"--sink.nats.password hunter2"},
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
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "-1 is below 1")
	mustFail(t, []string{"--sink.nats.stream", "-h"}, nil, "the next argument is the flag --h")
	mustFail(t, []string{"--sink.nats.password=", "--admin.enabled", "extra"}, nil, `"extra"`)
	mustFail(t, []string{"--sink.nats.password=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password", "=", "hunter2"}, nil, "right after the '='")
	mustFail(t, []string{"--sink.nats.password=foo", "hunter2"}, nil, "goes in quotes")
	// After a value, an unknown flag outside every section may be the tail
	// of that value: not named. A typo inside a known section is.
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, "unexpected argument after --sink.workers")
	mustFail(t, []string{"--sink.workers=4", "--sink.wrkers=1"}, nil, `"sink.wrkers"`)
	mustFail(t, []string{"--sink.nats.password", "--sink.workers=4"}, nil, "the flag --sink.workers")
}

// An unescaped '/' in a password makes net/url end the authority early: in
// "postgres://u:12/ss@h/db" the host was "u:12" and the password's tail went
// to the path, accepted and later echoed by a dial error. Refused, without
// quoting the URL; an escaped '/' (%2F) and an ordinary URL still parse.
func TestSlashInTheUserinfoIsRefusedWithoutEcho(t *testing.T) {
	for _, u := range []string{
		"postgres://u:12/ss@h/db",
		"redis://:pa/ss@h:6379/0",
		"nats://tok/en@h:4222",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || !strings.Contains(err.Error(), "%2F") {
			t.Errorf("%s: err = %v, want the refusal with the escape hint", u, err)
			continue
		}
		for _, piece := range []string{"12/ss", "pa/ss", "tok/en", "ss@h"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
	}
	for _, u := range []string{"postgres://u:12%2Fss@h/db", "redis://h:6379/0", "nats://u:p@h:4222",
		"postgres://u:p@h/db?sslmode=require"} {
		if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u}); err != nil {
			t.Errorf("%s refused: %v", u, err)
		}
	}
}
