package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The deployment files live next to the code they configure; these tests read
// them as text, with no Docker, so a variable the operator is told to set and
// the compose never forwards fails here and not in production (port of
// go-webhook-server's compose_test.go, W 27 and 28, 2026-10-14).
const dockerDir = "../../docker"

var envExampleSinkVar = regexp.MustCompile(`(?m)^#?\s*(WHS_SINK_[A-Z0-9_]+)=`)

// composeText is every compose file of docker/ and the common.yml fragment
// they extend: the service's environment is the union of the two.
func composeText(t *testing.T) string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dockerDir, "*.yml"))
	if len(files) < 2 {
		t.Fatalf("found %d compose files in %s", len(files), dockerDir)
	}
	var b strings.Builder
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(body)
	}
	return b.String()
}

// SPEC §13.6: every WHS_SINK_* the .env.example offers is forwarded, as
// ${NAME...}, by the compose. The compose passes an explicit environment:
// list, so a variable missing from it is silently ignored -- the TLS CA of
// the PostgreSQL destination was, with the .env.example and the README
// telling the operator to set it.
func TestComposeForwardsEverySinkVariable(t *testing.T) {
	example, err := os.ReadFile(filepath.Join(dockerDir, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	compose := composeText(t)
	names := envExampleSinkVar.FindAllStringSubmatch(string(example), -1)
	if len(names) == 0 {
		t.Fatal("read no WHS_SINK_* from .env.example")
	}
	for _, m := range names {
		// ${NAME:-x}, ${NAME-x} (an empty value is kept, as WHS_SINK_NATS_URLS
		// needs beside LOGHUB_EB_URL) or ${NAME}.
		if !strings.Contains(compose, "${"+m[1]+":") && !strings.Contains(compose, "${"+m[1]+"-") &&
			!strings.Contains(compose, "${"+m[1]+"}") {
			t.Errorf("the compose does not forward %s, which .env.example offers", m[1])
		}
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

// SPEC §13.6: the helper images are pinned; an unpinned one changes the stack
// with no change in this project. The server's own image (WHS_IMAGE, tagged by
// WHS_IMAGE_TAG) is the build.
func TestComposeImagesArePinned(t *testing.T) {
	n := 0
	for _, m := range composeImage.FindAllStringSubmatch(composeText(t), -1) {
		ref := strings.TrimSpace(m[1])
		if strings.Contains(ref, "${WHS_IMAGE:") {
			continue
		}
		n++
		if d := defaultOf.FindStringSubmatch(ref); d != nil {
			ref = d[1]
		}
		if !pinned(ref) {
			t.Errorf("image %q is not pinned to a version", ref)
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
