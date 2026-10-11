package jlh

import (
	"encoding/json"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

func TestAppendStringEscapes(t *testing.T) {
	cases := map[string]string{
		"plain":            `"plain"`,
		"<a&b>":            `"<a&b>"`,
		"q\"b\\":           `"q\"b\\"`,
		"\n\r\t\b\f":       `"\n\r\t\b\f"`,
		"\x00\x01\x1f\x7f": `"\u0000\u0001\u001f` + "\x7f" + `"`,
		" é":               "\" é\"",
		"a\xffb":           "\"a�b\"",
	}
	for in, want := range cases {
		if got := string(AppendString(nil, []byte(in))); got != want {
			t.Errorf("%q: %s want %s", in, got, want)
		}
		if got := string(AppendStr(nil, in)); got != want {
			t.Errorf("str %q: %s want %s", in, got, want)
		}
		var back string
		if err := json.Unmarshal(AppendString(nil, []byte(in)), &back); err != nil {
			t.Errorf("%q: not JSON: %v", in, err)
		}
	}
}

// One sample, two views: the uuid carries the same instant as event, to the
// nanosecond at level 3 (SPEC §3.1).
func TestUUIDAndEventShareTheSample(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	u := NewUUID(ts)
	got, ok := u.TimestampWithLevel(uuidv7.Level3)
	if !ok || !got.Equal(ts) {
		t.Fatalf("uuid instant %v want %v", got, ts)
	}
	e := EventAt(ts)
	if e.UTS != 1767225600 || e.Sub != 123456789 || string(e.AppendISO(nil)) != "2026-01-01T00:00:00.123Z" {
		t.Fatalf("event %+v %s", e, e.AppendISO(nil))
	}
}

func TestAppendHeaderIsValidJSON(t *testing.T) {
	h := Header{UUID: NewUUID(time.Unix(1, 999999999)), From: "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
		Space: "default", Type: "syslog", Level: 0, SID: MaxSID, Step: 0, Event: Event{UTS: 1, Sub: 999999999}}
	b := Close(append(AppendHeader(nil, &h), '{', '}'))
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	if m["event"].(map[string]any)["iso"] != "1970-01-01T00:00:01.999Z" {
		t.Fatalf("iso must truncate, not round: %s", b)
	}
}

// Payload is how sink=postgres finds the payload column without parsing (SPEC
// §3.7.1): a sub-slice of the envelope, found after the header even when the
// payload itself contains the separator text.
func TestPayload(t *testing.T) {
	h := &Header{UUID: NewUUID(time.Now()), From: "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
		Space: "default", Type: "syslog", Level: 5, Event: EventAt(time.Now())}
	for _, body := range []string{`{}`, `{"message":"},\"payload\":{x}"}`, `{"a":{"b":1}}`} {
		env := Close(append(AppendHeader(nil, h), body...))
		p, ok := Payload(env)
		if !ok || string(p) != body {
			t.Errorf("Payload(%s) = %s, %v; want %s", env, p, ok, body)
		}
		if ok && &p[0] != &env[len(env)-1-len(p)] {
			t.Error("Payload copied")
		}
	}
	for _, bad := range []string{``, `{}`, `{"uuid":"x"}`, `{"event":{},"payload":}`, `{"event":{},"payload":[1]}`} {
		if p, ok := Payload([]byte(bad)); ok {
			t.Errorf("Payload(%s) = %s, accepted", bad, p)
		}
	}
}
