// Package jlhcheck is the test oracle of SPEC §15.1: it validates an envelope
// against the JLH envelope schema and the webhook.request schema of the LogHub
// skills, then checks the rules a JSON Schema cannot express -- the ones the
// skill's validate.py checks.
//
// Only tests import it. The production path never validates by schema (SPEC
// §3.0): the serialiser is proven by these contract tests and by fuzzing.
package jlhcheck

import (
	"bytes"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed loghub/*.json
var files embed.FS

var (
	once                          sync.Once
	envelopeSchema, webhookSchema *jsonschema.Schema
	compileErr                    error
)

func compile() {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for _, name := range []string{"jlh-envelope.schema.json", "jlh-webhook.schema.json"} {
		b, err := files.ReadFile("loghub/" + name)
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
	webhookSchema, compileErr = c.Compile("jlh-webhook.schema.json")
}

// Envelope is the decoded shape the semantic rules read.
type Envelope struct {
	UUID  string `json:"uuid"`
	From  string `json:"from"`
	Space string `json:"space"`
	Type  string `json:"type"`
	Level int    `json:"level"`
	SID   int64  `json:"sid"`
	Step  int    `json:"step"`
	Event struct {
		UTS int64  `json:"uts"`
		Sub int64  `json:"sub"`
		ISO string `json:"iso"`
	} `json:"event"`
	Payload Payload `json:"payload"`
}

// Payload is the webhook.request payload.
type Payload struct {
	Mode    string `json:"mode"`
	Network struct {
		IP struct {
			Version int    `json:"version"`
			Src     string `json:"src"`
			Dst     string `json:"dst"`
		} `json:"ip"`
		L4 struct {
			SPort int `json:"sport"`
			DPort int `json:"dport"`
		} `json:"l4"`
		Sec struct {
			Mode string `json:"mode"`
			FP2  string `json:"fp2"`
			FP3  string `json:"fp3"`
			FP4  string `json:"fp4"`
		} `json:"sec"`
		Meta []struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		} `json:"meta"`
	} `json:"network"`
	Method  string `json:"method"`
	Schema  string `json:"schema"`
	Host    string `json:"host"`
	Path    string `json:"path"`
	URL     string `json:"url"`
	Headers []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"headers"`
	Params  map[string]string `json:"params"`
	Query   map[string]any    `json:"query"`
	Content struct {
		Length   int    `json:"length"`
		Type     string `json:"type"`
		Encoding string `json:"encoding"`
		Body     string `json:"body"`
	} `json:"content"`
}

// Meta returns the value of one network.meta key.
func (p *Payload) Meta(key string) (any, bool) {
	for _, m := range p.Network.Meta {
		if m.Key == key {
			return m.Value, true
		}
	}
	return nil, false
}

// Body is content.body decoded: the bytes that arrived.
func (p *Payload) Body() ([]byte, error) {
	if p.Content.Encoding == "base64" {
		return base64.StdEncoding.DecodeString(p.Content.Body)
	}
	return []byte(p.Content.Body), nil
}

// Validate checks one webhook.request envelope: both schemas, then the rules
// the schemas cannot express. It returns the decoded envelope.
func Validate(raw []byte) (*Envelope, error) { return validate(raw, false) }

// ValidateJSONBCopy is Validate for an envelope rebuilt from the payload
// column of loghub.inbox (sink=postgres). The one change that copy may carry
// is invariant 11's (SPEC §17): every \u0000 of the JLH is U+FFFD there,
// because jsonb refuses NUL. So payload.query is compared with the url's
// query after that same substitution -- the url keeps its %00 --, and nothing
// else is relaxed.
func ValidateJSONBCopy(raw []byte) (*Envelope, error) { return validate(raw, true) }

func validate(raw []byte, jsonbCopy bool) (*Envelope, error) {
	once.Do(compile)
	if compileErr != nil {
		return nil, compileErr
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	if err := envelopeSchema.Validate(inst); err != nil {
		return nil, fmt.Errorf("jlh-envelope.schema.json: %w", err)
	}
	if err := webhookSchema.Validate(inst); err != nil {
		return nil, fmt.Errorf("jlh-webhook.schema.json: %w", err)
	}
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	if err := Keys(raw); err != nil {
		return nil, err
	}
	if err := NoDuplicateKeys(raw); err != nil {
		return nil, err
	}
	return &e, semantic(&e, jsonbCopy)
}

// Keys checks the canonical order of the nine top-level fields.
func Keys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return err
	}
	var keys []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		keys = append(keys, t.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return err
		}
	}
	want := []string{"uuid", "from", "space", "type", "level", "sid", "step", "event", "payload"}
	if !slices.Equal(keys, want) {
		return fmt.Errorf("top-level keys %v, want %v", keys, want)
	}
	return nil
}

func semantic(e *Envelope, jsonbCopy bool) error {
	p := &e.Payload
	// event.iso is uts+sub truncated to the millisecond.
	at := time.Unix(e.Event.UTS, e.Event.Sub).UTC()
	if iso := at.Format("2006-01-02T15:04:05.000Z"); iso != e.Event.ISO {
		return fmt.Errorf("event.iso %s, want %s from uts+sub", e.Event.ISO, iso)
	}
	// One clock sample: the uuid's level-3 instant IS the event (SPEC §3.1).
	u, err := uuidv7.FromString(e.UUID)
	if err != nil {
		return err
	}
	ut, ok := u.TimestampWithLevel(uuidv7.Level3)
	if !ok || !ut.Equal(at) {
		return fmt.Errorf("uuid instant %v differs from event %v", ut, at)
	}
	// content.length is the decoded body.
	body, err := p.Body()
	if err != nil {
		return fmt.Errorf("content.body does not decode: %w", err)
	}
	if len(body) != p.Content.Length {
		return fmt.Errorf("content.length %d, decoded body %d bytes", p.Content.Length, len(body))
	}
	if p.Content.Encoding == "utf-8" && strings.ContainsRune(p.Content.Body, 0) {
		return fmt.Errorf("a utf-8 body with NUL must go in base64 (SPEC §3.2)")
	}
	// content.type is the first content-type header when one is listed. With
	// none -- dropped by Connection or headers.exclude -- content.type still
	// says what arrived (skill: "como recebido"); validate.py only warns.
	for _, h := range p.Headers {
		if h.Name == "content-type" {
			if h.Value != p.Content.Type {
				return fmt.Errorf("content.type %q, content-type header %q", p.Content.Type, h.Value)
			}
			break
		}
	}
	// headers are sorted by name (SPEC §3.2).
	if !slices.IsSortedFunc(p.Headers, func(a, b struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}) int {
		return strings.Compare(a.Name, b.Name)
	}) {
		return fmt.Errorf("headers are not sorted by name")
	}
	// url = schema://host[:port]path[?query], read back as validate.py does.
	if err := checkURL(p, jsonbCopy); err != nil {
		return err
	}
	if err := checkFamily(p); err != nil {
		return err
	}
	if e.Type != "webhook.request" || e.Level != 9 || e.Step != 0 {
		return fmt.Errorf("type %s level %d step %d", e.Type, e.Level, e.Step)
	}
	return nil
}
