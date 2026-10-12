package config

import (
	"strings"
	"testing"
)

// Independent review of 2026-10-11: a PostgreSQL LOGHUB_EB_URL with an
// unescaped '#' in the password parsed, with the user as the host and the
// start of the password as the port, which then appeared in --dump-config,
// in Effective() and in a range error. It is refused now, without the URL in
// the error.
func TestReviewPGFragmentDumpLeaks(t *testing.T) {
	for _, u := range []string{
		"postgres://app:39182#Zsecret9@db.example/loghub?sslmode=require",
		"postgres://app:739182#Zsecret9@db.example/loghub",
		"postgres://u:p@db.example/loghub#frag",
	} {
		_, err := load(t, nil, map[string]string{"LOGHUB_EB_URL": u})
		if err == nil {
			t.Errorf("%s accepted", u)
			continue
		}
		for _, piece := range []string{"39182", "Zsecret", "db.example"} {
			if strings.Contains(err.Error(), piece) {
				t.Errorf("%s: the error echoes %q: %v", u, piece, err)
			}
		}
	}
}
