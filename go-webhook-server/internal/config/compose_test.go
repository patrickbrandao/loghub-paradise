package config

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The deployment files live next to the code they configure; these tests read
// them as text, with no Docker, so a variable the operator is told to set and
// the compose never forwards fails here and not in production.
const dockerDir = "../../docker"

// sinkOfEnv maps the WHS_SINK_<X>_ prefix to the <sink> of compose.<ring>-<sink>.yml.
var sinkOfEnv = map[string]string{
	"REDIS": "redis", "SQLITE": "sqlite", "POSTGRES": "postgres",
	"MYSQL": "mysql", "NATS": "nats", "CLICKHOUSE": "clickhouse",
}

var envExampleSinkVar = regexp.MustCompile(`(?m)^#?\s*(WHS_SINK_([A-Z]+)_[A-Z0-9_]+)=`)

// SPEC §13.6 (W pendência 27): every WHS_SINK_<X>_* the .env.example offers is
// forwarded, as ${NAME...}, by every compose whose destination is <x>. The
// compose files pass an explicit environment: list, so a variable missing
// from it is silently ignored -- the TLS CA and the NATS credentials were.
func TestComposeForwardsEverySinkVariable(t *testing.T) {
	example, err := os.ReadFile(filepath.Join(dockerDir, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	vars := map[string][]string{} // sink -> names
	for _, m := range envExampleSinkVar.FindAllStringSubmatch(string(example), -1) {
		if sink, ok := sinkOfEnv[m[2]]; ok {
			vars[sink] = append(vars[sink], m[1])
		}
	}
	if len(vars["nats"]) == 0 || len(vars["postgres"]) == 0 {
		t.Fatalf("read no sink variables from .env.example: %v", vars)
	}
	checked := 0
	for sink, names := range vars {
		files, _ := filepath.Glob(filepath.Join(dockerDir, "compose.*-"+sink+".yml"))
		if len(files) == 0 {
			t.Errorf("no compose.*-%s.yml for the %s variables of .env.example", sink, sink)
		}
		sort.Strings(files)
		for _, f := range files {
			body, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range names {
				checked++
				if !strings.Contains(string(body), "${"+n+":") && !strings.Contains(string(body), "${"+n+"}") {
					t.Errorf("%s does not forward %s, which .env.example offers", filepath.Base(f), n)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("checked nothing")
	}
}

var (
	composeImage = regexp.MustCompile(`(?m)^\s*image:\s*(.+)$`)
	envImage     = regexp.MustCompile(`(?m)^#?\s*(WHS_[A-Z_]*IMAGE)=(.*)$`)
	defaultOf    = regexp.MustCompile(`\$\{[A-Z0-9_]+:-([^}]*)\}`)
)

// pinned says whether an image reference names a version: a tag that is not
// "latest" (a digest counts too).
func pinned(ref string) bool {
	if strings.Contains(ref, "@sha256:") {
		return true
	}
	name := ref[strings.LastIndex(ref, "/")+1:]
	i := strings.LastIndex(name, ":")
	return i > 0 && i < len(name)-1 && name[i+1:] != "latest"
}

// SPEC §13.6 (W pendência 28): the helper images are pinned. nats-box:latest
// made the stream creation of nats-init change with no change in this project.
// The server's own image (WHS_IMAGE, tagged by WHS_IMAGE_TAG) is the build.
func TestComposeImagesArePinned(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join(dockerDir, "*.yml"))
	if len(files) < 20 {
		t.Fatalf("found %d compose files", len(files))
	}
	n := 0
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range composeImage.FindAllStringSubmatch(string(body), -1) {
			ref := strings.TrimSpace(m[1])
			if strings.Contains(ref, "${WHS_IMAGE:") {
				continue
			}
			n++
			if d := defaultOf.FindStringSubmatch(ref); d != nil {
				ref = d[1]
			}
			if !pinned(ref) {
				t.Errorf("%s: image %q is not pinned to a version", filepath.Base(f), ref)
			}
		}
	}
	example, err := os.ReadFile(filepath.Join(dockerDir, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range envImage.FindAllStringSubmatch(string(example), -1) {
		if m[1] == "WHS_IMAGE" {
			continue
		}
		n++
		if !pinned(strings.TrimSpace(m[2])) {
			t.Errorf(".env.example: %s=%s is not pinned to a version", m[1], m[2])
		}
	}
	if n == 0 {
		t.Fatal("checked no image")
	}
	for ref, want := range map[string]bool{"natsio/nats-box:latest": false, "nats-box": false,
		"natsio/nats-box:0.20.0": true, "localhost:5000/x": false, "localhost:5000/x:1": true} {
		if pinned(ref) != want {
			t.Errorf("pinned(%q) = %v", ref, !want)
		}
	}
}
