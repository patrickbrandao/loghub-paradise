package config

// Regression tests from the independent review of 2026-10-11 (SPEC §10.9,
// §13.2, §16.2): each failed before its fix, or pins a configuration the
// fixes must keep accepting.

import (
	"bytes"
	"strings"
	"testing"
)

func TestReviewDumpRedacts(t *testing.T) {
	for _, u := range []string{
		"postgres://u:Zsecret9@db/loghub",
		"redis://:Zsecret9@r:6379/0",
		"nats://Zsecret9@n:4222",
		"nats://u:Zsecret9@n:4222,nats://u:Zsecret9@m:4222",
	} {
		l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		var b bytes.Buffer
		_ = l.DumpINI(&b, InfoSection{Name: "identity"})
		if strings.Contains(b.String(), "Zsecret9") {
			t.Errorf("%s: dump leaks secret", u)
		}
		if s := l.Effective(); strings.Contains(fmtMap(s), "Zsecret9") {
			t.Errorf("%s: Effective leaks secret", u)
		}
	}
}

func fmtMap(m map[string]string) string {
	var b strings.Builder
	for k, v := range m {
		b.WriteString(k + "=" + v + "\n")
	}
	return b.String()
}
