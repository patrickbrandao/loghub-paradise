package sink

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlh"
)

// chPayloadRef is the first chPayload, through encoding/json (review
// 2026-10-31, sinks 6): the oracle of the byte rewrite. Same values, keys
// sorted, U+2028 escaped -- equal once both are decoded.
func chPayloadRef(payload []byte) []byte {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return payload
	}
	marshal := func(v any) []byte {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(v)
		return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
	}
	for _, k := range [...]string{"fields", "extra"} {
		raw, ok := doc[k]
		if !ok {
			continue
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return payload
		}
		for name, v := range m {
			if len(v) > 0 && v[0] != '"' {
				m[name] = marshal(string(v))
			}
		}
		doc[k] = marshal(m)
	}
	return marshal(doc)
}

func decodeNumbers(t testing.TB, b []byte) any {
	t.Helper()
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	return v
}

// checkChPayload converts doc, rewrites its payload and compares it with the
// encoding/json reference: the same values, and the key order of the JLH kept.
func checkChPayload(t testing.TB, doc string) {
	b := batchOf(t, doc)
	in, ok := jlh.Payload(b.Envelopes[0].JLH)
	if !ok {
		t.Fatal("no payload")
	}
	got, want := chPayload(in), chPayloadRef(in)
	if !json.Valid(got) {
		t.Fatalf("not JSON: %s", got)
	}
	if !reflect.DeepEqual(decodeNumbers(t, got), decodeNumbers(t, want)) {
		t.Fatalf("rewrite differs from the reference\n got %s\nwant %s", got, want)
	}
	// Strings stay as the JLH wrote them, so the bytes outside the quoted
	// values are the JLH's: the top-level keys come in the same order.
	if gk, ik := topKeys(t, got), topKeys(t, in); !reflect.DeepEqual(gk, ik) {
		t.Fatalf("key order changed: %v, was %v", gk, ik)
	}
}

func topKeys(t testing.TB, b []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(b))
	_, _ = dec.Token()
	var ks []string
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		ks = append(ks, k.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return ks
}

// SPEC §3.7.2: the byte rewrite of chPayload equals the encoding/json one on
// the golden corpus and on hostile values.
func TestChPayloadMatchesTheReference(t *testing.T) {
	docs, _ := filepath.Glob("../convert/testdata/corpus/*.gelf")
	if len(docs) < 15 {
		t.Fatalf("only %d corpus documents", len(docs))
	}
	for _, p := range docs {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		checkChPayload(t, string(b))
	}
	for _, d := range []string{
		`{"version":"1.1","host":"h","short_message":"m","_q":"a\"b\\c","_o":{"k":"\"\\ "},"_n":-0.0e+1}`,
		`{"version":"1.1","host":"h","short_message":"m","x":{"a":[{"b":"}]\"{["},[]]},"y":[],"z":{},"w":null,"_t":true}`,
		`{"version":"1.1","host":"h","short_message":"m","_a":{"fields":1},"extra":"s"}`,
		`{"short_message":"\"fields\":{\"a\":1}","_k\u0000":7}`,
	} {
		checkChPayload(t, d)
	}
}

func FuzzChPayload(f *testing.F) {
	f.Add(`{"version":"1.1","host":"h","short_message":"m","_a":1.50,"x":{"y":[1,"\""]}}`)
	f.Add(`{"_a":"\\","b":true,"c":[{"d":"]"}]}`)
	f.Fuzz(func(t *testing.T, doc string) {
		if len(doc) > 1<<16 || strings.TrimSpace(doc) == "" {
			t.Skip()
		}
		checkChPayload(t, doc)
	})
}

func BenchmarkChPayload(b *testing.B) {
	doc := `{"version":"1.1","host":"docker-host","short_message":"GET /health 200","timestamp":1761912000.5,"level":6,` +
		`"_container_id":"0123456789ab","_container_name":"web","_image_name":"nginx:1.27","_tag":"web","_command":"nginx -g daemon off;",` +
		`"_created":"2026-10-31T12:00:00.000000000Z","_n":42,"_f":1.50}`
	bt := batchOf(b, doc)
	in, _ := jlh.Payload(bt.Envelopes[0].JLH)
	b.ReportAllocs()
	for b.Loop() {
		_ = chPayload(in)
	}
}
