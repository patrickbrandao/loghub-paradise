package config

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// The configuration findings of the parent go-relplog-server that apply here,
// ported from its findings_test.go and review*_regress_test.go (S §22.1 items
// 22 and 30, and the independent review of 2026-10-11): the PostgreSQL half of
// each, since PostgreSQL is the only destination left. Each failed in the
// parent before its fix; the NATS, Redis, MySQL and ClickHouse halves went
// with those destinations (SPEC §1.2).

// S §22.1 item 22: a socket URL keeps its port, a repeated parameter is
// refused, and sink.postgres.addr names one server.
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
	for _, q := range []string{"sslmode=disable&sslmode=require", "sslmode=disable&sslmode=disable",
		"host=/a&host=/b"} {
		mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u:p@/logs?" + q}, "repeated")
	}

	// One server, by host:port or socket directory.
	for _, ok := range []string{"pg-1:5432", "[::1]:5432", "10.0.0.1:6432", "/run/pg", "/run/pg:5433"} {
		mustLoad(t, []string{"--sink.postgres.addr=" + ok}, nil)
	}
	for _, bad := range []string{"h1,h2:5432", "h1:5432,h2:5432", "pg-1", ":5432", "pg-1:0",
		"pg-1:65536", "pg-1:+80", "pg-1:080", "/run/pg:0", "/run/pg:x", "pg 1:5432"} {
		mustFail(t, []string{"--sink.postgres.addr=" + bad}, nil, "sink.postgres.addr")
	}
}

// S §22.1 item 30: no configuration error carries a piece of the input that
// may be a secret. A URL that does not parse, a query key libpq does not
// know, an sslmode that is no sslmode, an INI line without '=', a ':' typed
// for the '=' in a flag.
func TestConfigErrorsDoNotEchoSecrets(t *testing.T) {
	for _, u := range []string{
		"postgres://bob:Sup3rSecret/xyz@pg/db",
		"postgres://bob:Sup3rSecret%zz@pg/db",
		"postgresql://bob:Sup3rSecret?xyz@pg/db",
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
		"[sink.postgres]\npassword: hunter2\n",
		"[sink.postgres hunter2\n",
		"[sink.postgres]\npassword: hunter2 = x\n",
		"password: hunter2 = x\n",
	} {
		writeFile(t, ini, content)
		err := mustFail(t, []string{"--config=" + ini}, nil, "line ")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%q: the INI error echoes the line: %v", content, err)
		}
	}
	// Controls: a misspelt key that looks like a key is still named.
	writeFile(t, ini, "[sink.postgres]\npasword = hunter2\n")
	if err := mustFail(t, []string{"--config=" + ini}, nil, `"sink.postgres.pasword"`); strings.Contains(err.Error(), "hunter2") {
		t.Errorf("unknown key error echoes the value: %v", err)
	}
	writeFile(t, ini, "username = u\n")
	mustFail(t, []string{"--config=" + ini}, nil, `"username"`)

	for _, args := range [][]string{
		{"--sink.postgres.password:hunter2"},
		{"--sink.postgres.password hunter2"},
	} {
		err := mustFail(t, args, nil, "")
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%v: the error echoes the secret: %v", args, err)
		}
	}
	// After a value, an unknown flag outside every section may be the tail
	// of that value: not named. A typo inside a known section is.
	mustFail(t, []string{"--sink.workers=4", "--typo.key=1"}, nil, "unexpected argument after --sink.workers")
	mustFail(t, []string{"--sink.workers=4", "--sink.wrkers=1"}, nil, `"sink.wrkers"`)
}

// An unescaped '/' (or '?', '#') in a password makes net/url end the
// authority early: in "postgres://u:12/ss@h/db" the host was "u:12" and the
// password's tail went to the path, accepted and later echoed by a dial
// error. Refused, without quoting the URL; an escaped '/' (%2F) and an
// ordinary URL still parse.
func TestSlashInTheUserinfoIsRefusedWithoutEcho(t *testing.T) {
	for _, u := range []string{
		"postgres://u:12/ss@h/db",
		"postgres://u:12?ss@h/db",
		"postgresql://u:12#ss@h/db",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || !strings.Contains(err.Error(), "%2F") {
			t.Errorf("%s: err = %v, want the refusal with the escape hint", u, err)
			continue
		}
		for _, piece := range []string{"12/ss", "12?ss", "12#ss", "ss@h"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
	}
	for _, u := range []string{"postgres://u:12%2Fss@h/db", "postgres://u:p@h/db?sslmode=disable",
		"postgres://u:12%23ss%3F@h/db"} {
		if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u}); err != nil {
			t.Errorf("%s refused: %v", u, err)
		}
	}
}

// The parent's independent review of 2026-10-11: a PostgreSQL LOGHUB_EB_URL
// with an unescaped '#' in the password parsed, with the user as the host and
// the start of the password as the port, which then appeared in
// --dump-config, in Effective() and in a range error -- and, here, the
// ?sslmode=require thrown away in the fragment never reached the TLS refusal.
// It is refused now, without the URL in the error.
func TestReviewPGFragmentDumpLeaks(t *testing.T) {
	for _, u := range []string{
		"postgres://app:39182#Zsecret9@db.example/loghub?sslmode=require",
		"postgres://app:739182#Zsecret9@db.example/loghub",
		"postgres://app:2024#S3cret@db.example/loghub?sslmode=require",
		"postgres://u:p@db.example/loghub#frag",
		"postgres://u:p@db.example/loghub#",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil {
			t.Errorf("%s accepted", u)
			continue
		}
		for _, piece := range []string{"39182", "Zsecret", "S3cret", "db.example", "2024"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
	}
}

// A value split by the shell at a space, whose tail starts with '-', is not
// echoed (the parent's review, SPEC §16.2).
func TestReviewFlagTailEcho(t *testing.T) {
	_, err := load(t, []string{"--sink.postgres.password=abc", "-hunter2", "--sink.postgres.username=u"}, nil)
	if err == nil {
		t.Fatal("accepted")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error echoes the value tail: %v", err)
	}
}

// What the fixes must keep accepting.
func TestReviewAccepts(t *testing.T) {
	for _, env := range []map[string]string{
		{"LOGHUB_EB_URL": "postgres://u:p%2Fw@[2001:db8::1]:5433/loghub?sslmode=prefer"},
		{"LOGHUB_EB_URL": "postgresql://u:p@/loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgresql://u:p@:5433/loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgresql://u@/loghub?host=/var/run/postgresql"},
		{"LOGHUB_EB_URL": "postgres://u:p@db/loghub"},
		{"LOGHUB_EB_URL": "postgres://u@db/loghub"},
		{"LOGHUB_EB_URL": "postgres://u:p@ss@db/loghub"},
		{"RELPLOG_SINK_POSTGRES_ADDR": "/var/run/postgresql"},
		{"RELPLOG_SINK_POSTGRES_ADDR": "/var/run/postgresql:5433"},
		{"RELPLOG_SINK_POSTGRES_ADDR": "[::1]:5432"},
		{"RELPLOG_LISTEN_TCP": "[::1]:20514"},
	} {
		if _, err := load(t, nil, env); err != nil {
			t.Errorf("%v refused: %v", env, err)
		}
	}
	// libpq's own percent-encoded socket host. Not a socket here -- the
	// host is "/var/run/postgresql" spelt out, which sink.postgres.addr would
	// take as a socket directory -- so either answer is acceptable as long as
	// it does not echo the URL.
	if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": "postgresql://u:Zsecret9@%2Fvar%2Frun%2Fpostgresql/loghub"}); err != nil &&
		strings.Contains(err.Error(), "Zsecret9") {
		t.Errorf("percent-encoded socket host: the error echoes the password: %v", err)
	}
}

// The dump and Effective() never print the password a URL carried.
func TestReviewDumpRedacts(t *testing.T) {
	for _, u := range []string{
		"postgres://u:Zsecret9@db/loghub",
		"postgresql://u:Zsecret9@:5433/loghub?host=/run/pg",
	} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		var b bytes.Buffer
		_ = l.DumpINI(&b, InfoSection{Name: "identity"})
		if strings.Contains(b.String(), "Zsecret9") {
			t.Errorf("%s: dump leaks secret", u)
		}
		for k, v := range l.Effective() {
			if strings.Contains(v, "Zsecret9") {
				t.Errorf("%s: Effective()[%s] leaks secret", u, k)
			}
		}
	}
}
