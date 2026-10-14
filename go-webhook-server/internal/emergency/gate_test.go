package emergency

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
)

// SPEC §9.2 (port of go-webhook-pg-server's gate, 2026-10-14): a record is
// what this server writes only if it is valid UTF-8 and carries only the
// escapes the serializer writes (SPEC §12.1), and no raw U+2028/U+2029, which
// the serializer always escapes. json.Valid checks neither UTF-8 nor escapes:
// the records below pass ParseHead and json.Valid, and PostgreSQL's jsonb
// refuses the first two (22021, 22P02: logic), which made every boot exit 3.
// What follows the damaged record is treated as truncated, as for any other
// damage (SPEC §9.2).
func TestReadStopsAtInvalidUTF8AndForeignEscapes(t *testing.T) {
	_, good1 := jlh([]byte("one"))
	_, good2 := jlh([]byte("two"))
	_, base := jlh([]byte("bad"))
	path := []byte(`"path":"/x"`)
	if !bytes.Contains(base, path) {
		t.Fatalf("test setup: no %s in %s", path, base)
	}
	with := func(s string) []byte { return bytes.Replace(base, path, []byte(`"path":"`+s+`"`), 1) }

	bad := map[string][]byte{
		"invalid UTF-8":          with("/\xff"),
		"escaped lone surrogate": with(`/\ud800`),
		"foreign escape":         with(`/\u0100`),
		"raw U+2028":             with("/\u2028"),
		"raw U+2029":             with("/\u2029"),
	}
	record := func(env []byte) []byte { return append([]byte(fmt.Sprintf("%d\n", len(env))), env...) }
	st := NewStore(t.TempDir(), testAgent)

	for name, env := range bad {
		t.Run(name, func(t *testing.T) {
			if _, _, err := envelope.ParseHead(env); err != nil || !json.Valid(env) {
				t.Fatalf("test setup: the record must pass the old gate (ParseHead: %v, json.Valid: %v)",
					err, json.Valid(env))
			}
			data := append(append(append(record(good1), record(good2)...), record(env)...), record(good1)...)
			p := filepath.Join(t.TempDir(), "emg-gate-1"+fileSuffix)
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
			res, err := st.Read(p)
			if err != nil {
				t.Fatalf("Read: %v; want a truncation, not an error", err)
			}
			if !res.Truncated || len(res.Items) != 2 {
				t.Fatalf("truncated=%v with %d items; want the 2 records before the damage, and the rest set aside",
					res.Truncated, len(res.Items))
			}
		})
	}

	// The control: what the serializer does write passes -- every escape it
	// produces, U+2028 escaped, and non-ASCII text.
	_, ours := jlh([]byte("quote \" backslash \\ nl \n tab \t cr \r ctl \x01 us \x1f del \x7f ls \u2028 ps \u2029 \u00e9 \u2713"))
	p := filepath.Join(t.TempDir(), "emg-gate-2"+fileSuffix)
	if err := os.WriteFile(p, record(ours), 0o600); err != nil {
		t.Fatal(err)
	}
	if res, err := st.Read(p); err != nil || res.Truncated || len(res.Items) != 1 {
		t.Fatalf("a record the serializer wrote was refused: %v, %+v", err, res)
	}
}

// onlyOurEscapes admits exactly the escapes the serializer writes (SPEC
// §9.2, W §19.1 item 34): \" \\ \n \r \t, \u00XX in lowercase hex for a
// control byte with no short form, and \u2028/\u2029 -- nothing else. It used
// to admit any two-character escape and any \u00XX, and this test asserted
// the opposite of §9.2.
func TestOnlyOurEscapes(t *testing.T) {
	for in, want := range map[string]bool{
		`"a\u0000b"`:     true,
		`"\u001f"`:       true,
		`"\u007f"`:       false, // DEL is written raw
		`"\u2028\u2029"`: true,
		`"\\ud800"`:      true, // an escaped backslash, then the text ud800
		`"\n\t\r\"\\"`:   true,
		`"\/"`:           false,
		`"\b"`:           false,
		`"\f"`:           false,
		`"\u000a"`:       false, // the serializer writes \n
		`"\u0009"`:       false, // and \t
		`"\u000d"`:       false, // and \r
		`"\u001F"`:       false, // uppercase hex
		`"\u0041"`:       false, // a printable character
		`"\ud800"`:       false,
		`"\u00e9"`:       false,
		`"\u0100"`:       false,
		`"\u20AC"`:       false,
		`"\u202a"`:       false,
		`"\`:             false,
		`"\u00`:          false,
	} {
		if got := onlyOurEscapes([]byte(in)); got != want {
			t.Errorf("onlyOurEscapes(%s) = %v, want %v", in, got, want)
		}
	}
}
