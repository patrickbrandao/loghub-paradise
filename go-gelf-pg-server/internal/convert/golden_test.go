package convert

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCorpus converts the golden corpus (SPEC §18.2): documents in the shape
// real emitters send them -- the Docker gelf driver, the GELF spec example,
// logstash-gelf (Java), graypy (Python), the Go gelf library, the Logstash gelf
// output, fluent-bit -- plus malformed ones. fluent-bit-gelf.gelf is a real
// capture of fluent-bit 4.0.3's out_gelf (2026-10-31): nested maps arrive
// flattened (_kubernetes_pod_name) and arrays joined as text ("1, a");
// nested-field-object.gelf keeps the nested-object case it used to stand for. Each NAME.gelf is converted as a
// UDP datagram with a fixed head, and the payload must equal NAME.want.json
// byte for byte once the file is compacted (json.Compact keeps key order and
// string bytes): the fixed payload order of §3.2 and the arrival order of
// fields/extra of §3.3/§6.6 are part of the contract. The corpus never shrinks.
//
// GELF_UPDATE_GOLDEN=1 rewrites the .want.json files; review the diff.
func TestCorpus(t *testing.T) {
	docs, err := filepath.Glob("testdata/corpus/*.gelf")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) < 15 {
		t.Fatalf("only %d corpus documents", len(docs))
	}
	update := os.Getenv("GELF_UPDATE_GOLDEN") == "1"
	for _, path := range docs {
		name := strings.TrimSuffix(filepath.Base(path), ".gelf")
		t.Run(name, func(t *testing.T) {
			doc, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			out, _, _ := conv(t, DefaultConfig(), udpHead(), string(doc))
			payload := out[bytes.Index(out, []byte(`"payload":`))+len(`"payload":`) : len(out)-1]
			want := strings.TrimSuffix(path, ".gelf") + ".want.json"
			if update {
				var buf bytes.Buffer
				if err := json.Indent(&buf, payload, "", "  "); err != nil {
					t.Fatal(err)
				}
				buf.WriteByte('\n')
				if err := os.WriteFile(want, buf.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			wb, err := os.ReadFile(want)
			if err != nil {
				t.Fatalf("%v (run with GELF_UPDATE_GOLDEN=1 and review)", err)
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, wb); err != nil {
				t.Fatalf("%s: %v", want, err)
			}
			if !bytes.Equal(payload, compact.Bytes()) {
				t.Fatalf("payload differs (bytes, key order included)\n got %s\nwant %s", payload, compact.Bytes())
			}
		})
	}
}
