// Package jlhcheck is the conformance oracle of the test suites (SPEC §18.2).
//
// It validates an envelope against the two normative JSON Schemas -- the JLH
// envelope of loghub-json-schema and the type=syslog payload of
// loghub-type-syslog-schema -- and against the semantic rules the schemas cannot
// express, ported from the skill's validate_payload.py.
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
	for _, name := range []string{"jlh-envelope.schema.json", "jlh-syslog-payload.schema.json"} {
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
	payloadSchema, compileErr = c.Compile("jlh-syslog-payload.schema.json")
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

// Validate checks one type=syslog envelope: both schemas, then the semantic
// rules. It returns the first problem.
func Validate(raw []byte) error { return ValidateWith(raw, Options{}) }

// ValidateWith is Validate with options.
func ValidateWith(raw []byte, opt Options) error {
	once.Do(compile)
	if compileErr != nil {
		return compileErr
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

var facilityNames = [24]string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "console", "cron2",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}
var severityNames = [8]string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

func num(v any) (int64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	return i, err == nil
}

// semantic is the port of validate_payload.py (check_envelope + check_payload).
func semantic(e map[string]any, opt Options) error {
	p, _ := e["payload"].(map[string]any)
	if e["type"] != "syslog" {
		return fmt.Errorf("type is %v", e["type"])
	}
	fac, _ := p["facility"].(map[string]any)
	sev, _ := p["severity"].(map[string]any)
	fc, _ := num(fac["code"])
	sc, _ := num(sev["code"])
	pri, _ := num(p["priority"])
	if pri != fc*8+sc {
		return fmt.Errorf("priority %d != facility*8+severity (%d*8+%d)", pri, fc, sc)
	}
	if fac["name"] != facilityNames[fc] || sev["name"] != severityNames[sc] {
		return fmt.Errorf("facility/severity names are not RFC 5427")
	}
	level, _ := num(e["level"])
	if level != sc {
		return fmt.Errorf("level %d != severity.code %d", level, sc)
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
	has := func(w string) bool {
		for _, x := range warnings {
			if x == w {
				return true
			}
		}
		return false
	}
	order := []string{"pri-missing", "pri-invalid", "version-unsupported", "header-invalid",
		"timestamp-missing", "timestamp-invalid", "timestamp-year-inferred", "timestamp-zone-inferred",
		"hostname-missing", "sd-invalid", "sd-duplicate-id", "encoding-invalid", "encoding-legacy", "truncated"}
	last := -1
	for _, w := range warnings {
		idx := -1
		for i, o := range order {
			if o == w {
				idx = i
			}
		}
		if idx <= last {
			return fmt.Errorf("parse.warnings out of canonical order: %v", warnings)
		}
		last = idx
	}

	if rawStr, ok := p["raw"].(string); ok {
		var rawBytes []byte
		if p["raw-encoding"] == "base64" {
			b, err := base64.StdEncoding.DecodeString(rawStr)
			if err != nil {
				return fmt.Errorf("raw marked base64 does not decode")
			}
			rawBytes = b
		} else {
			rawBytes = []byte(rawStr)
			if !opt.SkipFFFDRule && strings.ContainsRune(rawStr, '�') {
				return fmt.Errorf("raw contains U+FFFD: non-UTF-8 bytes must go in base64")
			}
		}
		size, _ := num(p["size"])
		if has("truncated") {
			if size <= int64(len(rawBytes)) {
				return fmt.Errorf("'truncated' requires size (%d) > raw octets (%d)", size, len(rawBytes))
			}
		} else if size != int64(len(rawBytes)) {
			return fmt.Errorf("size (%d) != raw octets (%d) without 'truncated'", size, len(rawBytes))
		}
	}

	// The schema's ^\S+$ is ECMAScript's, but the Go engine of jsonschema/v6
	// (RE2) has a narrower \s: U+00A0, U+2028, U+FEFF and \v would pass the
	// schema step. Checked here with the full set, plus the extra characters
	// Python's \s matches (the skill's validator).
	host, _ := p["host"].(map[string]any)
	for _, f := range []struct {
		name string
		v    any
	}{{"host.hostname", host["hostname"]}, {"appname", p["appname"]}, {"procid", p["procid"]}, {"msgid", p["msgid"]}} {
		if str, ok := f.v.(string); ok {
			for _, r := range str {
				if isScriptSpace(r) {
					return fmt.Errorf("%s %q contains whitespace U+%04X that ^\\S+$ forbids", f.name, str, r)
				}
			}
		}
	}
	if fromStr, _ := e["from"].(string); true {
		u, err := uuidv7.FromString(fromStr)
		if err != nil || !u.IsValid() || strings.ToLower(fromStr) != fromStr {
			return fmt.Errorf("from %q is not a lowercase canonical UUIDv7 (validate_payload.py)", fromStr)
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
	// Real time: the uuid and event come from the same sample (SPEC §3.1).
	// An imported file line (Case 2) has an older event by design.
	t, _ := u.TimestampWithLevel(uuidv7.Level3)
	if p["transport"] != "file" && t.UnixMilli() != time.Unix(uts, sub).UnixMilli() {
		return fmt.Errorf("uuid instant %s != event %s", t, time.Unix(uts, sub).UTC())
	}
	return nil
}

// SID recomputes the sid from payload.host and payload.transport.
func SID(transport string, host map[string]any) uint64 {
	switch transport {
	case "udp", "tcp", "tls", "dtls", "beep":
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
