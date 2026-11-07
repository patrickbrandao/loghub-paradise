package sink

// The compose files of docker/ against the rules of SPEC §17.5 that a typo
// would break silently: the Valkey files run valkey-server (the official
// image drops privileges only for it) behind SYSLOG_VALKEY_IMAGE, and nothing
// carries a name of go-syslog-server (pendência 20 de §21.1).

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestComposeValkeyFiles(t *testing.T) {
	for _, ring := range []string{"memory", "sqlite", "none"} {
		f := "../../docker/compose." + ring + "-valkey.yml"
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		s := string(b)
		for _, want := range []string{
			"${SYSLOG_VALKEY_IMAGE:-valkey/valkey:8.1-alpine}",
			"- valkey-server\n",
			`["CMD", "valkey-cli", "ping"]`,
			"SYSLOG_SINK_TYPE: valkey",
			"name: slstd-" + ring + "-valkey",
		} {
			if !strings.Contains(s, want) {
				t.Errorf("%s lacks %q", f, want)
			}
		}
		for _, bad := range []string{"redis-server", "redis-cli", "SYSLOG_REDIS_IMAGE"} {
			if strings.Contains(s, bad) {
				t.Errorf("%s still has %q", f, bad)
			}
		}
	}
}

var foreignName = regexp.MustCompile(`(^|[^a-z0-9-])sls-|go-syslog-server[^-]|(^|[\s"])(redis|nats|postgres|mysql)-data:`)

func TestComposeNamesAreThisServers(t *testing.T) {
	files, _ := filepath.Glob("../../docker/*.yml")
	files = append(files, "../../docker/.env.example", "../../docker/Dockerfile")
	if len(files) < 20 {
		t.Fatalf("found only %d docker files", len(files))
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if foreignName.MatchString(line + " ") {
				t.Errorf("%s:%d names go-syslog-server's stack: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}
