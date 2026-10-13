package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// Tests for the configuration items of go-relplog-server's SPEC §22.1 (13, 21
// and 30; 12 and 22 were of the SQL sinks), ported on 2026-10-13 from
// go-relplog-nats-server's findings_test.go. The rules are go-syslog-server's,
// already in this server; what each test adds is the message of the fixes
// that came after (TESTING §9).

// go-syslog-server §22.1 item 13: every entry of sink.nats.urls is parsed at validation, and no
// error quotes it; credentials in the URL and in a key are refused together.
func TestNATSURLsAreParsedWithoutEcho(t *testing.T) {
	// Controls: escaped credentials, a root path, a cluster, IPv6.
	for _, u := range []string{
		"nats://bob:s3cr%25t@n1:4222",
		"nats://n1:4222,nats://n2:4222",
		"nats://[::1]:4222",
		"nats://n1:4222/",
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
	} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "sink.nats.urls")
		if strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// Not nats:// -- TLS, websocket, a bare host:port -- is refused before any
	// parse (SPEC §1.2), and the refusal quotes nothing of the entry either.
	for _, u := range []string{"tls://Sup3rSecretToken/x@n1:4222", "bob:Sup3rSecret/x@n1:4222",
		"ws://Sup3rSecret@n1:8080/nats", "n1:4222"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "must be nats://")
		if strings.Contains(err.Error(), "Sup3r") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL", u, err)
		}
	}
	// The secret that parses: a password that reads as a port and a path, or a
	// query or a fragment. No credentials at all, dialled until boot_timeout.
	for _, u := range []string{"nats://n1:4222/x", "nats://n1:4222?x=1", "nats://n1:4222#f"} {
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
		mustFail(t, []string{"--sink.nats.urls=nats://tok@n1:4222", key}, nil, "carries credentials")
	}
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", "--sink.nats.username=u",
		"--sink.nats.password=p"}, nil, "carries credentials")
	mustFail(t, []string{"--sink.nats.credentials=/etc/x.creds"},
		map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"}, "carries credentials")
	// Controls: one place at a time.
	mustLoad(t, []string{"--sink.nats.urls=nats://n1:4222", "--sink.nats.username=u", "--sink.nats.password=p"}, nil)
	mustLoad(t, []string{"--sink.nats.credentials=/etc/x.creds"}, map[string]string{"LOGHUB_EB_URL": "nats://n1:4222"})
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"})
}

// go-syslog-server §22.1 item 21: a beacon interval below one second is a flood, not a
// heartbeat. Zero still disables the periodic beacon.
func TestBeaconIntervalHasAFloor(t *testing.T) {
	for _, raw := range []string{"1ns", "15ms", "500ms", "999ms"} {
		mustFail(t, []string{"--loghub.beacon_interval=" + raw}, nil, "below 1s")
		mustFail(t, nil, map[string]string{"LOGHUB_BEACON_INTERVAL": raw}, "below 1s")
	}
	for _, raw := range []string{"0", "0s", "1", "1s", "1500ms", "15"} {
		mustLoad(t, []string{"--loghub.beacon_interval=" + raw}, nil)
	}
}

// go-syslog-server §22.1 item 30: no configuration error carries a piece of the input that
// may be a secret. A URL that does not parse, an INI line without '=', a word on the
// command line, a value swallowed by a flag left without its own.
func TestConfigErrorsDoNotEchoSecrets(t *testing.T) {
	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"nats://bob:Sup3rSecret%zz@n1:4222",
		"nats://n0:4222,nats://bob:Sup3rSecret#xyz@n1:4222",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// Any other scheme is named only when it has the shape of one.
	for _, u := range []string{"postgres://u:Sup3rSecret@pg/db", "redis://:Sup3rSecret@r:6379/0",
		"Sup3r$ecret://n1:4222"} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || strings.Contains(err.Error(), "Sup3r") {
			t.Errorf("%q: err = %v, want a refusal without the secret", u, err)
		}
	}

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
	mustFail(t, []string{"--sink.workers", "-1"}, nil, "-1 is outside 1..256")
	mustFail(t, []string{"--sink.nats.stream", "--help"}, nil, "the next argument is the flag --help")
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

// An unescaped '/' (or '?', '#') in a password makes net/url end the
// authority early: in "nats://u:12/ss@h:4222" the host was "u:12" and the
// password's tail went to the path, later echoed by a dial error. Refused,
// without quoting the URL, through both doors; an escaped '/' (%2F) and an
// ordinary URL still parse (go-relplog-server, SPEC §10.9, §16.2).
func TestSlashInTheUserinfoIsRefusedWithoutEcho(t *testing.T) {
	for _, u := range []string{
		"nats://u:12/ss@h:4222",
		"nats://u:12?ss@h:4222",
		"nats://u:12#ss@h:4222",
		"nats://tok/en@h:4222",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil || !strings.Contains(err.Error(), "%2F") {
			t.Errorf("%s: err = %v, want the refusal with the escape hint", u, err)
			continue
		}
		for _, piece := range []string{"12/ss", "12?ss", "12#ss", "tok/en", "ss@h"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
		if _, err := load(t, []string{"--sink.nats.urls=" + u}, nil); err == nil ||
			!strings.Contains(err.Error(), "%2F") || strings.Contains(err.Error(), "ss@h") {
			t.Errorf("sink.nats.urls=%s: err = %v, want the refusal with the escape hint", u, err)
		}
	}
	for _, u := range []string{"nats://u:12%2Fss@h:4222", "nats://u:p@h:4222", "nats://h:4222"} {
		if _, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u}); err != nil {
			t.Errorf("%s refused: %v", u, err)
		}
	}
}
