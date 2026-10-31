// Package jlhcheck is the conformance oracle of the test suites (SPEC §18.2).
//
// It validates an envelope against the two normative JSON Schemas -- the JLH
// envelope of loghub-json-schema and the type=gelf payload of this
// repository (jlh-gelf-payload.schema.json) -- and against the semantic rules
// the schemas cannot express (SPEC §3, §6).
//
// ONLY _test.go files import this package. That keeps
// github.com/santhosh-tekuri/jsonschema/v6 out of the production binary: the
// decision of SPEC §18.2 (deviation D3) is that the schema runs in tests, CI and
// fuzzing, never per message.
package jlhcheck

import (
	"bytes"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Files holds the normative copies (see loghub/README.md).
//
//go:embed loghub
var Files embed.FS

var (
	once           sync.Once
	envelopeSchema *jsonschema.Schema
	payloadSchema  *jsonschema.Schema
	compileErr     error
)

func compile() {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for _, name := range []string{"jlh-envelope.schema.json", "jlh-gelf-payload.schema.json"} {
		b, err := Files.ReadFile("loghub/" + name)
		if err != nil {
			compileErr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			compileErr = err
			return
		}
		if err := c.AddResource(name, doc); err != nil {
			compileErr = err
			return
		}
	}
	if envelopeSchema, compileErr = c.Compile("jlh-envelope.schema.json"); compileErr != nil {
		return
	}
	payloadSchema, compileErr = c.Compile("jlh-gelf-payload.schema.json")
}

// ValidateEnvelopeSchema checks raw bytes against the JLH envelope schema only.
func ValidateEnvelopeSchema(raw []byte) error {
	once.Do(compile)
	if compileErr != nil {
		return compileErr
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return envelopeSchema.Validate(inst)
}

// Options relaxes rules that only hold for real-time reception.
type Options struct {
	// SkipFFFDRule disables "raw contains U+FFFD: non-UTF-8 bytes must go in
	// base64". The rule is the validator's heuristic, and a sender may well
	// send a literal U+FFFD in valid UTF-8; fuzzing finds exactly that.
	SkipFFFDRule bool
}

// Validate checks one type=gelf envelope: both schemas, then the semantic
// rules. It returns the first problem.
func Validate(raw []byte) error { return ValidateWith(raw, Options{}) }

// ValidateWith is Validate with options.
func ValidateWith(raw []byte, opt Options) error {
	once.Do(compile)
	if compileErr != nil {
		return compileErr
	}
	// encoding/json and jsonschema.UnmarshalJSON both turn an invalid UTF-8
	// byte into U+FFFD in silence: without this check a raw 0xFF in a string
	// of the envelope passed the oracle (review 2026-10-31, F3).
	if !utf8.Valid(raw) {
		return fmt.Errorf("the envelope is not valid UTF-8")
	}
	if !json.Valid(raw) {
		return fmt.Errorf("not valid JSON")
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	if err := envelopeSchema.Validate(inst); err != nil {
		return fmt.Errorf("envelope schema: %w", err)
	}
	env, _ := inst.(map[string]any)
	if err := payloadSchema.Validate(env["payload"]); err != nil {
		return fmt.Errorf("payload schema: %w", err)
	}
	if err := checkDuplicateKeys(raw); err != nil {
		return err
	}
	return semantic(env, opt)
}

var levelNames = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

// warningOrder is SPEC §6.8, duplicated from internal/convert on purpose: the
// oracle must not trust the code it checks.
var warningOrder = []string{
	"version-missing", "version-unknown", "version-invalid",
	"host-missing", "host-invalid",
	"short-message-missing", "short-message-coerced",
	"full-message-invalid",
	"level-default", "level-coerced", "level-invalid",
	"timestamp-coerced", "timestamp-invalid",
	"facility-coerced", "facility-invalid",
	"file-invalid",
	"line-coerced", "line-invalid",
	"field-coerced", "field-null", "field-id-reserved", "field-name-invalid",
	"key-unknown", "duplicate-key", "encoding-replaced",
}

func num(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	return i, err == nil
}

// semantic holds the rules of SPEC §3 and §6 that the payload schema cannot
// express.
func semantic(e map[string]any, opt Options) error {
	_ = opt
	p, _ := e["payload"].(map[string]any)
	if e["type"] != "gelf" {
		return fmt.Errorf("type is %v", e["type"])
	}
	lv, _ := p["level"].(map[string]any)
	code, _ := num(lv["code"])
	if lv["name"] != levelNames[code] {
		return fmt.Errorf("level name %v is not the RFC 5424 label of %d", lv["name"], code)
	}
	level, _ := num(e["level"])
	if level != code {
		return fmt.Errorf("level %d != payload.level.code %d", level, code)
	}
	if ts, ok := p["timestamp"].(string); ok {
		if _, err := time.Parse("2006-01-02T15:04:05", ts[:19]); err != nil {
			return fmt.Errorf("timestamp with an impossible date: %s", ts)
		}
	}

	var warnings []string
	if pw, ok := p["parse"].(map[string]any); ok {
		if ws, ok := pw["warnings"].([]any); ok {
			for _, w := range ws {
				warnings = append(warnings, w.(string))
			}
		}
	}
	last := -1
	for _, w := range warnings {
		idx := -1
		for i, o := range warningOrder {
			if o == w {
				idx = i
			}
		}
		if idx <= last {
			return fmt.Errorf("parse.warnings out of canonical order: %v", warnings)
		}
		last = idx
	}

	if err := warningsAgree(p, warnings); err != nil {
		return err
	}

	size, _ := num(p["size"])
	if rawStr, ok := p["raw"].(string); ok {
		var rawBytes []byte
		if p["raw-encoding"] == "base64" {
			b, err := base64.StdEncoding.DecodeString(rawStr)
			if err != nil {
				return fmt.Errorf("raw marked base64 does not decode")
			}
			if utf8.Valid(b) {
				return fmt.Errorf("raw is UTF-8 and still went in base64")
			}
			rawBytes = b
		} else {
			rawBytes = []byte(rawStr)
		}
		// GELF never truncates (SPEC §5.5): size is the document.
		if size != int64(len(rawBytes)) {
			return fmt.Errorf("size (%d) != raw octets (%d)", size, len(rawBytes))
		}
	}

	host, _ := p["host"].(map[string]any)
	if str, ok := host["hostname"].(string); ok {
		for _, r := range str {
			if isScriptSpace(r) {
				return fmt.Errorf("host.hostname %q contains whitespace U+%04X that ^\\S+$ forbids", str, r)
			}
		}
	}
	if fromStr, _ := e["from"].(string); true {
		u, err := uuidv7.FromString(fromStr)
		if err != nil || !u.IsValid() || strings.ToLower(fromStr) != fromStr {
			return fmt.Errorf("from %q is not a lowercase canonical UUIDv7", fromStr)
		}
	}

	sid, _ := num(e["sid"])
	if want := SID(p["transport"].(string), host); uint64(sid) != want {
		return fmt.Errorf("sid %d does not follow the rule (want %d)", sid, want)
	}

	ev, _ := e["event"].(map[string]any)
	uts, _ := num(ev["uts"])
	sub, _ := num(ev["sub"])
	wantISO := time.Unix(uts, sub).UTC().Format("2006-01-02T15:04:05.000Z")
	if ev["iso"] != wantISO {
		return fmt.Errorf("event.iso %v != %s", ev["iso"], wantISO)
	}
	u, err := uuidv7.FromString(e["uuid"].(string))
	if err != nil || !u.IsValid() {
		return fmt.Errorf("uuid is not a UUIDv7")
	}
	if strings.ToLower(e["uuid"].(string)) != e["uuid"].(string) {
		return fmt.Errorf("uuid is not lowercase")
	}
	t, _ := u.TimestampWithLevel(uuidv7.Level3)
	if t.UnixMilli() != time.Unix(uts, sub).UnixMilli() {
		return fmt.Errorf("uuid instant %s != event %s", t, time.Unix(uts, sub).UTC())
	}
	return nil
}

// warningsAgree holds the rules between parse.warnings and the content they
// describe (SPEC §6.5-§6.8) that need no access to the document: a warning
// says what the converter did, and the payload must show it. Without them an
// envelope claiming level-default with level 3, or a gelf document without
// gelf-version and without a version warning, passed (review 2026-10-31, F3).
func warningsAgree(p map[string]any, warnings []string) error {
	has := map[string]bool{}
	for _, w := range warnings {
		has[w] = true
	}
	lv, _ := p["level"].(map[string]any)
	code, _ := num(lv["code"])
	if (has["level-default"] || has["level-invalid"]) && code != 6 {
		return fmt.Errorf("level-default or level-invalid with level %d (must be 6)", code)
	}
	if has["short-message-missing"] && p["short-message"] != "" {
		return fmt.Errorf("short-message-missing with short-message %q", p["short-message"])
	}
	_, extra := p["extra"]
	if has["key-unknown"] != extra {
		return fmt.Errorf("key-unknown=%v but extra present=%v", has["key-unknown"], extra)
	}
	if has["field-id-reserved"] {
		f, _ := p["fields"].(map[string]any)
		if _, ok := f["id"]; !ok {
			return fmt.Errorf("field-id-reserved without fields.id")
		}
	}
	if p["format"] != "gelf" {
		return nil
	}
	v, hasV := p["gelf-version"].(string)
	switch {
	case !hasV && !has["version-missing"] && !has["version-invalid"]:
		return fmt.Errorf("no gelf-version and neither version-missing nor version-invalid")
	case hasV && v != "1.0" && v != "1.1" && !has["version-unknown"]:
		return fmt.Errorf("gelf-version %q without version-unknown", v)
	case has["version-unknown"] && (!hasV || v == "1.0" || v == "1.1"):
		return fmt.Errorf("version-unknown with gelf-version %q", v)
	}
	host, _ := p["host"].(map[string]any)
	if _, ok := host["hostname"]; !ok && !has["host-missing"] && !has["host-invalid"] {
		return fmt.Errorf("no host.hostname and neither host-missing nor host-invalid")
	}
	return nil
}

// SID recomputes the sid from payload.host and payload.transport.
func SID(transport string, host map[string]any) uint64 {
	switch transport {
	case "udp", "tcp", "tls", "http", "https":
	default:
		return 0
	}
	part := func(k string) string {
		v, ok := host[k]
		if !ok {
			return ""
		}
		switch x := v.(type) {
		case string:
			return x
		case json.Number:
			return string(x)
		}
		return fmt.Sprint(v)
	}
	f := fnv.New64a()
	f.Write([]byte(transport + "|" + part("src-ip") + "|" + part("src-port") + "|" +
		part("dst-ip") + "|" + part("dst-port")))
	return f.Sum64() & (1<<53 - 1)
}

// checkDuplicateKeys refuses an object with a repeated key: the schema cannot
// see them, and the effective value would depend on the consumer's parser.
func checkDuplicateKeys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	// Numbers stay text: a GELF field may carry 1e400 (SPEC §3.3), which a
	// float64 cannot hold.
	dec.UseNumber()
	return walk(dec)
}

func walk(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch tok {
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return err
			}
			ks := k.(string)
			if seen[ks] {
				return fmt.Errorf("duplicate key %q", ks)
			}
			seen[ks] = true
			if err := walk(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	case json.Delim('['):
		for dec.More() {
			if err := walk(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	}
	return nil
}

// Keys returns the keys of the top-level object and of payload, in document
// order, for the canonical-order assertions of SPEC §3.3.
func Keys(raw []byte) (top, payload []string, err error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err = dec.Token(); err != nil {
		return
	}
	for dec.More() {
		var k json.Token
		if k, err = dec.Token(); err != nil {
			return
		}
		top = append(top, k.(string))
		if k == "payload" {
			if _, err = dec.Token(); err != nil {
				return
			}
			for dec.More() {
				var pk json.Token
				if pk, err = dec.Token(); err != nil {
					return
				}
				payload = append(payload, pk.(string))
				var skip json.RawMessage
				if err = dec.Decode(&skip); err != nil {
					return
				}
			}
			if _, err = dec.Token(); err != nil {
				return
			}
			continue
		}
		var skip json.RawMessage
		if err = dec.Decode(&skip); err != nil {
			return
		}
	}
	return
}

// isScriptSpace is ECMAScript's \s plus Python's extra U+001C..U+001F and
// U+0085.
func isScriptSpace(r rune) bool {
	switch {
	case r == ' ', r >= '\t' && r <= '\r', r >= 0x1C && r <= 0x1F, r == 0x85, r == 0xA0,
		r == 0x1680, r >= 0x2000 && r <= 0x200A, r == 0x2028, r == 0x2029, r == 0x202F,
		r == 0x205F, r == 0x3000, r == 0xFEFF:
		return true
	}
	return false
}
