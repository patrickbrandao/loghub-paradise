package jlhcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
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

// gelfEnvelope builds a valid type=gelf envelope around payload, with uuid,
// event and sid consistent with each other, so each refusal below breaks
// exactly the rule it names.
func gelfEnvelope(t *testing.T, payload string) []byte {
	t.Helper()
	at := time.Date(2026, 10, 31, 12, 0, 0, 123456000, time.UTC)
	u := uuidv7.GenerateAt(uuidv7.Level3, at)
	var p map[string]any
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&p); err != nil {
		t.Fatalf("payload: %v", err)
	}
	host, _ := p["host"].(map[string]any)
	tr, _ := p["transport"].(string)
	level := "6"
	if lv, ok := p["level"].(map[string]any); ok {
		level = string(lv["code"].(json.Number))
	}
	return []byte(fmt.Sprintf(`{"uuid":%q,"from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","type":"gelf","level":%s,"sid":%d,"step":0,"event":{"uts":%d,"sub":%d,"iso":%q},"payload":%s}`,
		u.String(), level, SID(tr, host), at.Unix(), at.Nanosecond(), at.Format("2006-01-02T15:04:05.000Z"), payload))
}

const hostUDP = `"host":{"hostname":"web01","src-ip":"10.0.0.3","src-port":40000,"dst-ip":"10.0.0.10","dst-port":12201}`

// The oracle must be switched on: a valid envelope passes, and each mutation
// -- named after the one rule it breaks -- fails.
func TestOracleGELF(t *testing.T) {
	valid := map[string]string{
		"minimal":   `{` + hostUDP + `,"transport":"udp","format":"gelf","gelf-version":"1.1","level":{"code":6,"name":"info"},"short-message":"x","size":10}`,
		"full":      `{` + hostUDP + `,"transport":"udp","compression":"gzip","chunks":2,"format":"gelf","gelf-version":"1.1","level":{"code":3,"name":"err"},"facility":"app","timestamp":"2026-10-31T12:00:00.123456Z","short-message":"x","full-message":"y","file":"a.go","line":42,"fields":{"a":"b","n":1e400},"extra":{"message":{"k":[1,true]}},"size":3,"raw":"abc","parse":{"warnings":["key-unknown","duplicate-key"]}}`,
		"malformed": `{"host":{"src-ip":"10.0.0.3","src-port":1,"dst-port":2},"transport":"tcp","format":"malformed","level":{"code":6,"name":"info"},"short-message":"","size":1,"raw":"/w==","raw-encoding":"base64","parse":{"error":"UTF-8 inválido no documento"}}`,
		"https":     `{"host":{"src-ip":"::1","src-port":1,"dst-port":2},"transport":"https","compression":"deflate","tls":{"version":"TLS1.3"},"format":"gelf","level":{"code":6,"name":"info"},"short-message":"","size":2,"parse":{"warnings":["version-missing","host-missing","short-message-missing","level-default"]},"x-proxy":{"peer-ip":"127.0.0.1","peer-port":9}}`,
	}
	for name, p := range valid {
		if err := Validate(gelfEnvelope(t, p)); err != nil {
			t.Errorf("valid %s refused: %v", name, err)
		}
	}
	base := valid["minimal"]
	invalid := map[string]string{
		"level name":            strings.Replace(base, `"name":"info"`, `"name":"informational"`, 1),
		"framing is syslog":     strings.Replace(base, `"format":"gelf"`, `"framing":"datagram","format":"gelf"`, 1),
		"unknown transport":     strings.Replace(base, `"udp"`, `"unix"`, 1),
		"chunks on tcp":         strings.Replace(base, `"transport":"udp"`, `"transport":"tcp","chunks":2`, 1),
		"compression on tls":    strings.Replace(base, `"transport":"udp"`, `"transport":"tls","compression":"gzip"`, 1),
		"deflate on udp":        strings.Replace(base, `"transport":"udp"`, `"transport":"udp","compression":"deflate"`, 1),
		"tls on http":           strings.Replace(base, `"transport":"udp"`, `"transport":"http","tls":{"version":"TLS1.3"}`, 1),
		"hostname nbsp":         strings.Replace(base, `"web01"`, `"web 01"`, 1),
		"no short-message":      strings.Replace(base, `"short-message":"x",`, ``, 1),
		"unknown warning":       strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["truncated"]}`, 1),
		"warnings order":        strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["level-default","host-missing"]}`, 1),
		"lossy without raw":     strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["duplicate-key"]}`, 1),
		"raw size":              strings.Replace(base, `"size":10`, `"size":10,"raw":"abc"`, 1),
		"utf8 raw in base64":    strings.Replace(base, `"size":10`, `"size":3,"raw":"YWJj","raw-encoding":"base64"`, 1),
		"error on gelf":         strings.Replace(base, `"size":10`, `"size":10,"parse":{"error":"documento não é JSON válido"}`, 1),
		"free error text":       strings.Replace(valid["malformed"], `UTF-8 inválido no documento`, `boom`, 1),
		"malformed with fields": strings.Replace(valid["malformed"], `"size":1`, `"fields":{"a":"b"},"size":1`, 1),
		"malformed short":       strings.Replace(valid["malformed"], `"short-message":""`, `"short-message":"x"`, 1),
		"malformed hostname":    strings.Replace(valid["malformed"], `"src-ip"`, `"hostname":"h","src-ip"`, 1),
		"field bool":            strings.Replace(base, `"size":10`, `"fields":{"a":true},"size":10`, 1),
		"extra null":            strings.Replace(base, `"size":10`, `"extra":{"a":null},"size":10`, 1),
		"empty fields":          strings.Replace(base, `"size":10`, `"fields":{},"size":10`, 1),
		"line string":           strings.Replace(base, `"size":10`, `"line":"4","size":10`, 1),
		"timestamp 30 feb":      strings.Replace(base, `"size":10`, `"timestamp":"2026-02-30T00:00:00.000Z","size":10`, 1),
		"duplicate key":         strings.Replace(base, `"size":10`, `"fields":{"a":"1","a":"2"},"size":10`, 1),
		"proxy on udp":          strings.Replace(base, `"size":10`, `"size":10,"x-proxy":{"peer-ip":"127.0.0.1","peer-port":9}`, 1),
		// Warnings against content (review 2026-10-31, F3).
		"level-default not 6":     strings.Replace(strings.Replace(base, `"code":6,"name":"info"`, `"code":3,"name":"err"`, 1), `"size":10`, `"size":10,"parse":{"warnings":["level-default"]}`, 1),
		"short missing with text": strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["short-message-missing"]}`, 1),
		"key-unknown no extra":    strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["key-unknown"]}`, 1),
		"extra no key-unknown":    strings.Replace(base, `"size":10`, `"extra":{"a":"b"},"size":10`, 1),
		"id reserved no id":       strings.Replace(base, `"size":10`, `"fields":{"a":"b"},"size":10,"parse":{"warnings":["field-id-reserved"]}`, 1),
		"no version no warning":   strings.Replace(base, `"gelf-version":"1.1",`, ``, 1),
		"version 2 no warning":    strings.Replace(base, `"gelf-version":"1.1"`, `"gelf-version":"2.0"`, 1),
		"version-unknown on 1.1":  strings.Replace(base, `"size":10`, `"size":10,"parse":{"warnings":["version-unknown"]}`, 1),
		"no hostname no warning":  strings.Replace(base, `"hostname":"web01",`, ``, 1),
	}
	for name, p := range invalid {
		if p == base {
			t.Fatalf("%s: the mutation did not apply", name)
		}
		if err := Validate(gelfEnvelope(t, p)); err == nil {
			t.Errorf("invalid %s accepted", name)
		}
	}
	// A raw 0xFF inside a string: encoding/json would read it as U+FFFD.
	if err := Validate(bytes.Replace(gelfEnvelope(t, base), []byte(`"x"`), []byte("\"\xff\""), 1)); err == nil {
		t.Error("an envelope with invalid UTF-8 accepted")
	}
	// level of the envelope must be payload.level.code
	env := gelfEnvelope(t, base)
	bad := strings.Replace(string(env), `"level":6`, `"level":5`, 1)
	if err := Validate([]byte(bad)); err == nil {
		t.Error("envelope level != payload level accepted")
	}
	bad = regexp.MustCompile(`"sid":\d+`).ReplaceAllString(string(env), `"sid":1`)
	if err := Validate([]byte(bad)); err == nil {
		t.Error("wrong sid accepted")
	}
}
