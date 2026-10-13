package config

import (
	"path/filepath"
	"strings"
	"testing"
)

// Findings of the independent review of 2026-10-13 (docs/TESTING.md). Each
// test failed before its fix.

// A command line carried over from go-relplog-server puts a removed flag after
// a valued one. The tail rule (SPEC §16.2) used to read it as the split tail
// of that value and blamed --log.level; it must be named, as when it stands
// alone, and say where it comes from.
func TestRemovedFlagAfterAValuedFlagIsNamed(t *testing.T) {
	for _, f := range []string{"--filter.min_severity=warning", "--tls.cert_file=/c",
		"--sink.postgres.addr=pg:5432", "--ratelimit.enabled=true", "--ring.sqlite.path=/x",
		"--sink.nats.subjects=exact", "--ring.mode=memory"} {
		err := mustFail(t, []string{"--log.level=info", f}, nil, "unknown parameter")
		name := strings.TrimPrefix(strings.SplitN(f, "=", 2)[0], "--")
		if !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "go-relplog-server has it") {
			t.Errorf("%s after a valued flag: %v", f, err)
		}
	}
	// Control: the split tail of a value is still not printed.
	_, err := load(t, []string{"--sink.nats.password=abc", "-hunter2"}, nil)
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("split tail: %v", err)
	}
}

// A secret that wraps onto the next line of the INI turns its tail into a
// key; the error used to print it ("unknown parameter "sink.nats.UzI1..."),
// and on SIGHUP into the server log. Same for a key before any section and a
// section header that is not one (SPEC §13.2).
func TestINIUnknownKeyNeverPrintsASecretContinuation(t *testing.T) {
	dir := t.TempDir()
	for i, tc := range []struct{ ini, secret string }{
		{"[sink.nats]\ntoken = eyJhbGciOiJI\nUzI1NiJ9abcDEF==\n", "UzI1NiJ9abcDEF"},
		{"[sink.nats]\nusername = u\npassword = abc\nffd3a9c1b2=\n", "ffd3a9c1b2"},
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
	writeFile(t, p, "[sink.nats]\nmode = core\nsubjet = x\n")
	err := mustFail(t, []string{"--config=" + p}, nil, "unknown parameter")
	if !strings.Contains(err.Error(), "sink.nats.subjet") || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("typo: %v", err)
	}
}

// An unencoded ',' in a password splits sink.nats.urls; the piece left said
// "scheme \"none\"", with no hint. Nothing of it may be printed.
func TestCommaInAPasswordSplitsTheURLListWithAHint(t *testing.T) {
	_, err := load(t, []string{"--sink.nats.urls=nats://u:12,cret@h:4222"}, nil)
	if err == nil || !strings.Contains(err.Error(), "splits the list") ||
		!strings.Contains(err.Error(), "percent-encode") || strings.Contains(err.Error(), "cret") {
		t.Errorf("err = %v", err)
	}
	_, err = load(t, []string{"--sink.nats.urls=n:4222"}, nil)
	if err == nil || strings.Contains(err.Error(), `"none"`) || !strings.Contains(err.Error(), "no scheme") {
		t.Errorf("bare host: %v", err)
	}
}
