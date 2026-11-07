package sink

// The compose files of docker/ against the rules of SPEC §17.5 that a typo
// would break silently: the Valkey files run valkey-server (the official
// image drops privileges only for it) behind SYSLOG_VALKEY_IMAGE, and nothing
// carries a name of go-syslog-server (pendência 20 de §21.1).

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// The image variables are this server's (SLSTD_IMAGE, SLSTD_IMAGE_TAG), not
// go-syslog-server's SYSLOG_IMAGE: one .env shared by both stacks built one
// image under the other's name (independent review of 2026-11-07, SPEC
// §17.5).
var foreignImageVar = regexp.MustCompile(`\bSYSLOG_IMAGE(_TAG)?\b`)

func TestComposeImageVariablesAreThisServers(t *testing.T) {
	files, _ := filepath.Glob("../../docker/*.yml")
	files = append(files, "../../docker/.env.example", "../../docker/README.md")
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if foreignImageVar.Match(b) {
			t.Errorf("%s uses go-syslog-server's SYSLOG_IMAGE/SYSLOG_IMAGE_TAG", f)
		}
	}
	b, err := os.ReadFile("../../docker/common.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "${SLSTD_IMAGE:-go-syslog-standard-server}:${SLSTD_IMAGE_TAG:-local}") {
		t.Error("common.yml does not build the image from SLSTD_IMAGE/SLSTD_IMAGE_TAG")
	}
}

var defaultPort = regexp.MustCompile(`\$\{[A-Z_]+PORT:-([0-9]+)\}`)

// go-syslog-server's published defaults when the Valkey files were moved
// (2026-11-07): syslog 5141-5167, admin 19101-19127, Redis/Valkey
// 16301-16329 and the ring's 16418-16427. A Valkey file of this server on one
// of them failed with a bind error when both stacks ran on one host.
func inSiblingRange(p int) bool {
	return (p >= 5141 && p <= 5167) || (p >= 19101 && p <= 19127) ||
		(p >= 16301 && p <= 16329) || (p >= 16418 && p <= 16427)
}

func TestComposeValkeyPortsAreFree(t *testing.T) {
	seen := map[string]string{}
	for _, ring := range []string{"memory", "sqlite", "none"} {
		f := "../../docker/compose." + ring + "-valkey.yml"
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		ms := defaultPort.FindAllStringSubmatch(string(b), -1)
		if len(ms) < 4 {
			t.Fatalf("%s: only %d published defaults", f, len(ms))
		}
		for _, m := range ms {
			p, _ := strconv.Atoi(m[1])
			if inSiblingRange(p) {
				t.Errorf("%s publishes %d by default, a port of go-syslog-server's compose", f, p)
			}
			if other, ok := seen[m[1]]; ok && other != f {
				t.Errorf("%s and %s both publish %d", f, other, p)
			}
			seen[m[1]] = f
		}
	}
	// Nor does any other compose of this server publish them.
	others, _ := filepath.Glob("../../docker/compose.*.yml")
	for _, f := range others {
		if strings.HasSuffix(f, "-valkey.yml") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range defaultPort.FindAllStringSubmatch(string(b), -1) {
			if v, ok := seen[m[1]]; ok {
				t.Errorf("%s publishes %s, a default of %s", f, m[1], v)
			}
		}
	}
}
