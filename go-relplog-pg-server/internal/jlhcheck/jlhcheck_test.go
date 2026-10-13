package jlhcheck

import (
	"encoding/json"
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

// The oracle must be switched on: every valid example of the envelope skill
// passes, and every invalid one -- each named after the one rule it breaks --
// fails. Without this, a validator that accepts everything would make every
// other contract test pass without proving anything.
func TestSchemaEnvelopeExamples(t *testing.T) {
	for _, dir := range []string{"valid", "invalid"} {
		entries, err := fs.ReadDir(Files, "loghub/examples/"+dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) < 10 {
			t.Fatalf("%s: only %d examples", dir, len(entries))
		}
		for _, e := range entries {
			b, _ := Files.ReadFile("loghub/examples/" + dir + "/" + e.Name())
			err := ValidateEnvelopeSchema(b)
			if dir == "valid" && err != nil {
				t.Errorf("%s/%s rejected: %v", dir, e.Name(), err)
			}
			// Duplicate keys and 6.0-as-integer are outside JSON Schema; the
			// invalid examples are all schema-level.
			if dir == "invalid" && err == nil {
				t.Errorf("%s/%s accepted", dir, e.Name())
			}
		}
	}
}

// Every envelope the skill itself publishes as expected output must pass the
// whole oracle: both schemas and every semantic rule. A failure here is a bug
// in the port of validate_payload.py, not in the converter.
func TestSchemaGoldenExpectedPassTheOracle(t *testing.T) {
	b, _ := Files.ReadFile("loghub/vectors.json")
	var doc struct {
		Vectors []struct {
			ID       string            `json:"id"`
			Expected []json.RawMessage `json:"expected"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Vectors) != 24 {
		t.Fatalf("%d vectors, want 24", len(doc.Vectors))
	}
	for _, v := range doc.Vectors {
		for i, e := range v.Expected {
			if err := Validate(e); err != nil {
				t.Errorf("%s[%d]: %v", v.ID, i, err)
			}
		}
	}
}

func TestSchemaRejectsDuplicateKeys(t *testing.T) {
	b, _ := Files.ReadFile("loghub/vectors.json")
	var doc struct {
		Vectors []struct {
			Expected []json.RawMessage `json:"expected"`
		} `json:"vectors"`
	}
	_ = json.Unmarshal(b, &doc)
	env := string(doc.Vectors[0].Expected[0])
	dup := strings.Replace(env, `"type": "syslog",`, `"type": "syslog", "type": "syslog",`, 1)
	if dup == env {
		t.Fatal("fixture did not change")
	}
	if err := Validate([]byte(dup)); err == nil {
		t.Fatal("a duplicate key passed")
	}
}

// The schema's ^\S+$ is ECMAScript's, and the RE2 engine behind the schema step
// has a narrower \s. The semantic step closes the gap, with Python's extra
// characters too (review finding). And `from` must be a UUIDv7, as in the
// skill's validate_payload.py.
func TestSemanticCatchesWhatTheSchemaEngineMisses(t *testing.T) {
	b, _ := Files.ReadFile("loghub/vectors.json")
	var doc struct {
		Vectors []struct {
			Expected []json.RawMessage `json:"expected"`
		} `json:"vectors"`
	}
	_ = json.Unmarshal(b, &doc)
	hostRe := regexp.MustCompile(`"hostname": "[^"]*"`)
	var env string
	for _, v := range doc.Vectors {
		for _, e := range v.Expected {
			if hostRe.Match(e) {
				env = string(e)
				break
			}
		}
		if env != "" {
			break
		}
	}
	if env == "" {
		t.Fatal("no vector with a hostname")
	}
	for _, code := range []string{"00a0", "2028", "feff", "000b", "001c", "0085", "3000"} {
		ws := `\u` + code // a JSON escape, so the schema sees the character itself
		bad := hostRe.ReplaceAllString(env, `"hostname": "a`+ws+`b"`)
		if err := Validate([]byte(bad)); err == nil {
			t.Errorf("hostname with %s passed", ws)
		}
	}
	fromRe := regexp.MustCompile(`("from": "[0-9a-f]{8}-[0-9a-f]{4}-)7`)
	bad := fromRe.ReplaceAllString(env, `${1}4`)
	if bad == env {
		t.Fatal("fixture did not change")
	}
	if err := Validate([]byte(bad)); err == nil {
		t.Error("a UUIDv4 from passed")
	}
	if err := Validate([]byte(env)); err != nil {
		t.Fatalf("control: %v", err)
	}
}
