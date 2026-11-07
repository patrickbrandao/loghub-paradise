package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var composePortDefault = regexp.MustCompile(`\$\{(WHS_[A-Z]+_PORT):-(\d+)\}`)

// SPEC §13.6, §8.2.6: every compose.<ring>-redis.yml has a
// compose.<ring>-valkey.yml beside it, whose server runs the Valkey image as
// valkey-server -- the only first argument with which the image's entrypoint
// drops root and owns /data -- with valkey-cli as the health check, and with
// a project, containers, volume and host ports of its own, so the two run
// side by side. The image is pinned in .env.example as WHS_VALKEY_IMAGE.
func TestComposeValkeyFiles(t *testing.T) {
	redis, _ := filepath.Glob(filepath.Join(dockerDir, "compose.*-redis.yml"))
	if len(redis) == 0 {
		t.Fatal("no compose.*-redis.yml")
	}
	valkey, _ := filepath.Glob(filepath.Join(dockerDir, "compose.*-valkey.yml"))
	if len(valkey) != len(redis) {
		t.Errorf("%d compose.*-valkey.yml for %d compose.*-redis.yml", len(valkey), len(redis))
	}
	must := []string{
		"image: ${WHS_VALKEY_IMAGE:-valkey/valkey:8.1-alpine}",
		"      - valkey-server\n",
		`test: ["CMD", "valkey-cli", "ping"]`,
		"WHS_SINK_TYPE: valkey\n",
		"WHS_SINK_REDIS_ADDRS: valkey:6379\n",
		"  valkey-data:\n",
	}
	mustNot := []string{"- redis-server", "\"redis-cli\"", "WHS_REDIS_IMAGE", "redis-data", "WHS_REDIS_PORT", "-redis\n"}
	ports := map[string]string{}
	for _, f := range redis {
		ring := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "compose."), "-redis.yml")
		vf := filepath.Join(dockerDir, "compose."+ring+"-valkey.yml")
		body, err := os.ReadFile(vf)
		if err != nil {
			t.Errorf("compose.%s-redis.yml has no Valkey twin: %v", ring, err)
			continue
		}
		s := string(body)
		for _, m := range must {
			if !strings.Contains(s, m) {
				t.Errorf("%s lacks %q", filepath.Base(vf), m)
			}
		}
		for _, m := range mustNot {
			if strings.Contains(s, m) {
				t.Errorf("%s still has %q", filepath.Base(vf), m)
			}
		}
		if !regexp.MustCompile(`(?m)^name: wh[sf]-` + ring + `-valkey$`).MatchString(s) {
			t.Errorf("%s: the project is not wh?-%s-valkey", filepath.Base(vf), ring)
		}
		for _, m := range composePortDefault.FindAllStringSubmatch(s, -1) {
			if other, dup := ports[m[2]]; dup {
				t.Errorf("%s and %s both default to host port %s", other, filepath.Base(vf), m[2])
			}
			ports[m[2]] = filepath.Base(vf)
		}
	}
	// No Valkey file shares a default host port with any other compose.
	all, _ := filepath.Glob(filepath.Join(dockerDir, "compose.*.yml"))
	for _, f := range all {
		if strings.HasSuffix(f, "-valkey.yml") {
			continue
		}
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range composePortDefault.FindAllStringSubmatch(string(body), -1) {
			if v, dup := ports[m[2]]; dup {
				t.Errorf("%s and %s both default to host port %s", v, filepath.Base(f), m[2])
			}
		}
	}
	example, err := os.ReadFile(filepath.Join(dockerDir, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^WHS_VALKEY_IMAGE=valkey/valkey:8\.1-alpine$`).Match(example) {
		t.Error(".env.example does not offer WHS_VALKEY_IMAGE=valkey/valkey:8.1-alpine")
	}
}
