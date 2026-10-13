package config

import (
	"strings"
	"testing"
)

// Independent review of 2026-10-11 of go-relplog-server: a URL with an
// unescaped '#' in the password parsed, with the user as the host and the
// start of the password as the port, which then appeared in --dump-config, in
// Effective() and in a range error (found on a PostgreSQL URL; the same parse
// happens for nats://). It is refused now, without the URL in the error.
func TestReviewFragmentDumpLeaks(t *testing.T) {
	for _, u := range []string{
		"nats://app:39182#Zsecret9@nats.example:4222",
		"nats://app:739182#Zsecret9@nats.example",
		"nats://u:p@nats.example:4222#frag",
	} {
		for _, env := range []map[string]string{{"LOGHUB_EB_URL": u}, {"RELPLOG_SINK_NATS_URLS": u}} {
			_, err := load(t, nil, env)
			if err == nil {
				t.Errorf("%v accepted", env)
				continue
			}
			for _, piece := range []string{"39182", "Zsecret", "nats.example"} {
				if strings.Contains(err.Error(), piece) {
					t.Errorf("%v: the error echoes %q: %v", env, piece, err)
				}
			}
		}
	}
}
