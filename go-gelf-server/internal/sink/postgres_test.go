package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlhcheck"
)

// Nothing in this file needs a server. The integration half lives in
// postgres_integration_test.go and skips without GELF_TEST_PG_DSN.

// The rule that matters most in the whole package: class 42 is fatal. Classified
// as logic, a dropped table would discard a chunk every few milliseconds, with
// an error log, forever, in a process every probe reports healthy. This test
// and the table of SPEC §10.5 change together.
func TestClassifySQLState(t *testing.T) {
	fatal := []string{
		"42P01", // undefined_table
		"42703", // undefined_column
		"42P10", // invalid_column_reference: no unique index for ON CONFLICT
		"42501", // insufficient_privilege
		"42804", // datatype_mismatch
		"25006", // read_only_sql_transaction
		"28000", // invalid_authorization_specification
		"28P01", // invalid_password
		"2F005", // function executed no return statement
		// 3D000 invalid_catalog_name: sink.postgres.database does not exist;
		// as network it was retried until boot_timeout with the network's
		// message, where MySQL's 1049 is fatal (pendência 8 de 2026-10-11).
		"3D000",
		// 23502 not_null_violation: the sink never sends NULL, so it is the
		// table (a required column added, a DEFAULT dropped); as logic it
		// dropped every chunk with the sink UP (pendência 7 de 2026-10-11).
		"23502",
		// 0A000 feature_not_supported: ON CONFLICT on a table with an INSERT
		// or UPDATE rule; as network it was retried forever with the sink UP
		// (go-relplog-pg-server review, 2026-10-13).
		"0A000",
	}
	for _, c := range fatal {
		if got := classifySQLState(c); got != ClassFatal {
			t.Errorf("SQLSTATE %s = %v, want fatal", c, got)
		}
	}
	network := []string{
		"08006", "08003", "08001", // connection
		"53100", "53200", "53300", // resources
		"57P01", "57P03", "57014", // operator intervention
		"40001", "40P01", // rollback
		"55P03", "XX001", "54000", "", "9", // anything else
	}
	for _, c := range network {
		if got := classifySQLState(c); got != ClassNetwork {
			t.Errorf("SQLSTATE %s = %v, want network", c, got)
		}
	}
	logic := []string{"22021", "22003", "22P02", "22P05", "23505", "23514"}
	for _, c := range logic {
		if got := classifySQLState(c); got != ClassLogic {
			t.Errorf("SQLSTATE %s = %v, want logic", c, got)
		}
	}
	if got := ClassOf(classifyPostgres("insert", errors.New("dial tcp: refused"))); got != ClassNetwork {
		t.Errorf("an error with no SQLSTATE = %v, want network", got)
	}
	// 23514 naming a constraint is this data; naming none is "no partition
	// of relation ... found for row", the table: fatal (review finding 4).
	if got := ClassOf(classifyPostgres("insert", &pgconn.PgError{Code: "23514", ConstraintName: "inbox_level_ck"})); got != ClassLogic {
		t.Errorf("a CHECK violation = %v, want logic", got)
	}
	if got := ClassOf(classifyPostgres("insert", &pgconn.PgError{Code: "23514"})); got != ClassFatal {
		t.Errorf("a partition routing failure = %v, want fatal", got)
	}
}

// SELECT * FROM unnest(...) yields columns in argument order: the column list,
// the casts, the values and the validation contract must agree position by
// position.
func TestPostgresInsertStatementMatchesTheContract(t *testing.T) {
	m := regexp.MustCompile(`\(([a-z_, ]+)\) SELECT \* FROM unnest\((.+)\) ON CONFLICT \(uuid\) DO NOTHING$`).
		FindStringSubmatch(pgInsert)
	if m == nil {
		t.Fatalf("unexpected statement shape: %s", pgInsert)
	}
	cols := strings.Split(m[1], ", ")
	casts := strings.Split(m[2], ", ")
	if len(cols) != 10 || len(casts) != 10 || len(pgColumns) != 10 {
		t.Fatalf("%d columns, %d arrays, %d in the contract; SPEC §3.7.1 writes 10", len(cols), len(casts), len(pgColumns))
	}
	pgName := map[string]string{"timestamptz": "timestamp with time zone"}
	for i, c := range pgColumns {
		if cols[i] != c.name {
			t.Errorf("column %d is %q in the statement and %q in the contract", i, cols[i], c.name)
		}
		typ := strings.TrimSuffix(casts[i][strings.Index(casts[i], "::")+2:], "[]")
		if n, ok := pgName[typ]; ok {
			typ = n
		}
		if !contains(c.types, typ) {
			t.Errorf("column %q is cast to %s, not a type the contract accepts (%v)", c.name, typ, c.types)
		}
	}
	a := &pgArgs{}
	if err := a.add(&batchN(t, 1).Envelopes[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(a.values()); n != 10 {
		t.Fatalf("%d parameters, want 10", n)
	}
}

// SPEC §3.7.1: every column comes from the JLH header the converter wrote, and
// the payload is the JLH's own payload object -- a sub-slice, not a copy.
func TestPGArgsMapsTheEnvelope(t *testing.T) {
	b := batchOf(t, docGELF)
	e := &b.Envelopes[0]
	a := &pgArgs{}
	if err := a.add(e); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		UUID, From, Space, Type string
		Level, SID, Step        int64
		Event                   struct{ UTS, Sub int64 }
		Payload                 json.RawMessage
	}
	if err := json.Unmarshal(e.JLH, &doc); err != nil {
		t.Fatal(err)
	}
	if uuidv7.UUID(a.uuid[0]).String() != doc.UUID || uuidv7.UUID(a.from[0]).String() != doc.From ||
		a.space[0] != doc.Space || a.typ[0] != doc.Type || int64(a.level[0]) != doc.Level ||
		a.sid[0] != doc.SID || int64(a.step[0]) != doc.Step {
		t.Errorf("header columns %v %v %q %q %d %d %d differ from the JLH %+v",
			a.uuid[0], a.from[0], a.space[0], a.typ[0], a.level[0], a.sid[0], a.step[0], doc)
	}
	if doc.Type != "gelf" || doc.Level != 5 || doc.From != testFrom {
		t.Errorf("unexpected JLH header %+v", doc)
	}
	at, nsec := a.eventAt[0], a.eventNsec[0]
	if at.Location() != time.UTC || at.Nanosecond()%1000 != 0 ||
		at.Unix() != doc.Event.UTS || int64(at.Nanosecond())+int64(nsec) != doc.Event.Sub {
		t.Errorf("event_at %v + event_nsec %d is not event %+v without loss", at, nsec, doc.Event)
	}
	if !bytes.Equal(a.payload[0], doc.Payload) {
		t.Errorf("payload column\n%s\nwant the JLH payload\n%s", a.payload[0], doc.Payload)
	}
	if &a.payload[0][0] != &e.JLH[len(e.JLH)-1-len(a.payload[0])] {
		t.Error("a payload without NUL was copied; it must be a sub-slice of the chunk's JLH")
	}
}

// SPEC §3.7.1: jsonb refuses \u0000 with 22P05, class 22. The column gets a
// rebuilt copy -- raw exact in base64, U+FFFD for the NUL in every other string,
// keys of fields and extra included, at any depth -- and the chunk's JLH, the
// bytes every retry resends and every other destination receives, stays intact.
func TestPGArgsRebuildsAPayloadWithNUL(t *testing.T) {
	cfg := testConvert()
	cfg.KeepRaw = convert.KeepRawAlways // raw on a valid document too
	b := batchWith(t, cfg, newItem(docNUL))
	e := &b.Envelopes[0]
	before := string(e.JLH)
	a := &pgArgs{}
	if err := a.add(e); err != nil {
		t.Fatal(err)
	}
	if string(e.JLH) != before || !strings.Contains(before, `a\u0000b`) {
		t.Fatal("the JLH was modified, or the converter no longer escapes NUL as \\u0000")
	}
	col := a.payload[0]
	if hasNULEscape(col) || !json.Valid(col) {
		t.Fatalf("payload column still has the NUL escape, or is not JSON: %s", col)
	}
	var p map[string]any
	dec := json.NewDecoder(bytes.NewReader(col))
	dec.UseNumber()
	if err := dec.Decode(&p); err != nil {
		t.Fatal(err)
	}
	// The document's NULs are \u0000 escapes, text in raw: raw has no NUL
	// byte and stays as it is, exact.
	if p["raw"] != docNUL || p["raw-encoding"] != nil || p["size"] != json.Number(strconv.Itoa(len(docNUL))) {
		t.Errorf("raw %q (%v) is not the exact document of %d octets (size %v)", p["raw"], p["raw-encoding"], len(docNUL), p["size"])
	}
	const r = "\uFFFD"
	host, _ := p["host"].(map[string]any)
	for name, got := range map[string]any{
		"short-message": p["short-message"], "full-message": p["full-message"], "facility": p["facility"],
		"file": p["file"], "hostname": host["hostname"],
	} {
		if s, _ := got.(string); strings.Contains(s, "\x00") || !strings.Contains(s, r) {
			t.Errorf("%s = %q: want U+FFFD for the NUL", name, got)
		}
	}
	fields, _ := p["fields"].(map[string]any)
	if fields["x"] != "a"+r+"b" {
		t.Errorf("fields.x = %q, want U+FFFD for the NUL (fields %v)", fields["x"], fields)
	}
	// Numbers are the text that arrived, never a float64 (SPEC §3.3).
	if fields["n"] != json.Number("12345678901234567890") || fields["big"] != json.Number("1e400") {
		t.Errorf("fields n %v big %v: numbers must survive as their literal text", fields["n"], fields["big"])
	}
	if !bytes.Contains(col, []byte(`12345678901234567890`)) || !bytes.Contains(col, []byte(`1e400`)) {
		t.Errorf("the column lost the literal numbers: %s", col)
	}
	// Keys with a NUL, at every depth of extra (and of fields, wherever the
	// converter put the invalid field name), carry U+FFFD.
	extra, _ := p["extra"].(map[string]any)
	other, ok := extra["other"+r+"key"].(map[string]any)
	if !ok {
		t.Fatalf("extra has no key other\uFFFDkey: %v", extra)
	}
	inner, ok := other["in"+r+"ner"].([]any)
	if !ok || len(inner) != 3 || inner[0] != r || inner[1] != json.Number("1e400") {
		t.Fatalf("extra.other.inner = %#v", other)
	}
	if deep, _ := inner[2].(map[string]any); deep["k"+r] != "v"+r {
		t.Errorf("the object inside the array = %v, want its key and value with U+FFFD", inner[2])
	}
	if strings.Contains(fmt.Sprint(p), "\x00") {
		t.Errorf("a NUL survived somewhere: %v", p)
	}
	head := before[:strings.Index(before, `"payload":`)+len(`"payload":`)]
	if err := validateQueueEnvelope(append(append([]byte(head), col...), '}')); err != nil {
		t.Errorf("the envelope with the column's payload is not a valid type=gelf envelope: %v", err)
	}
}

// A raw that holds a real NUL byte -- a malformed document, raw its only text --
// goes to the column in base64 with raw-encoding, exact, and size still counts
// its octets (SPEC §3.7.1).
func TestPGArgsRebuildsARawWithNUL(t *testing.T) {
	const doc = "not\x00gelf\x00"
	b := batchOf(t, doc)
	e := &b.Envelopes[0]
	if e.Res.Format != convert.FormatMalformed {
		t.Fatalf("format %v, want malformed", e.Res.Format)
	}
	a := &pgArgs{}
	if err := a.add(e); err != nil {
		t.Fatal(err)
	}
	col := a.payload[0]
	if hasNULEscape(col) || !json.Valid(col) {
		t.Fatalf("payload column: %s", col)
	}
	var p struct {
		Raw    string
		RawEnc string `json:"raw-encoding"`
		Size   int
	}
	if err := json.Unmarshal(col, &p); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(p.Raw)
	if p.RawEnc != "base64" || err != nil || string(raw) != doc || p.Size != len(doc) {
		t.Errorf("raw %q (%s, %v) is not the exact document (size %d)", raw, p.RawEnc, err, p.Size)
	}
	head := string(e.JLH[:strings.Index(string(e.JLH), `"payload":`)+len(`"payload":`)])
	if err := validateQueueEnvelope(append(append([]byte(head), col...), '}')); err != nil {
		t.Errorf("the envelope with the column's payload is not valid: %v", err)
	}
}

// validateQueueEnvelope validates an envelope rebuilt from a queue row: both
// schemas and the semantic rules (jlhcheck), with one allowance. The copy puts
// a raw that holds a NUL in base64 although it is valid UTF-8 (SPEC §3.7.1),
// which the oracle's "raw is UTF-8 and still went in base64" refuses; that raw
// is put back as text before the check, so everything else is still proved.
func validateQueueEnvelope(env []byte) error {
	var doc map[string]any
	dec := json.NewDecoder(bytes.NewReader(env))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return err
	}
	if p, ok := doc["payload"].(map[string]any); ok && p["raw-encoding"] == "base64" {
		if raw, err := base64.StdEncoding.DecodeString(p["raw"].(string)); err == nil &&
			utf8.Valid(raw) && bytes.IndexByte(raw, 0) >= 0 {
			p["raw"] = string(raw)
			delete(p, "raw-encoding")
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return jlhcheck.Validate(bytes.TrimSpace(buf.Bytes()))
}

// Two keys of one object that become the same key once the NUL is replaced
// resolve the same way on every call -- the one without a NUL wins -- never by
// the map's iteration order: the copy must not change between retries.
func TestInboxPayloadKeyCollisionIsDeterministic(t *testing.T) {
	in := []byte(`{"fields":{"a\u0000":"nul","a\ufffd":"plain","b\u0000\u0000":"2","b\u0000\ufffd":"1"}}`)
	first := string(inboxPayload(in))
	for i := 0; i < 200; i++ {
		if got := string(inboxPayload(in)); got != first {
			t.Fatalf("call %d: %s, first %s", i, got, first)
		}
	}
	const want = `{"fields":{"a\ufffd":"plain","b\ufffd\ufffd":"2"}}`
	var got, w any
	if err := json.Unmarshal([]byte(first), &got); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(want), &w)
	if !reflect.DeepEqual(got, w) {
		t.Errorf("collision = %s, want %s", first, want)
	}
}

// A payload that does not decode still never reaches jsonb with a NUL escape.
func TestInboxPayloadFallsBackToTheByteSwap(t *testing.T) {
	if got := string(inboxPayload([]byte(`{"m":"\u0000"`))); got != `{"m":"\ufffd"` {
		t.Errorf("fallback = %s", got)
	}
	clean := []byte(`{"short-message":"m"}`)
	if got := inboxPayload(clean); &got[0] != &clean[0] {
		t.Error("a payload without NUL must not be copied")
	}
	// raw already in base64 is left as it is; the NUL elsewhere is swapped.
	got := inboxPayload([]byte(`{"raw":"AA==","raw-encoding":"base64","short-message":"\u0000"}`))
	if !bytes.Contains(got, []byte(`"raw":"AA=="`)) || bytes.Contains(got, nulEscape) {
		t.Errorf("base64 raw: %s", got)
	}
}

func TestJSONBSafe(t *testing.T) {
	for in, want := range map[string]string{
		`{"m":"a"}`:              `{"m":"a"}`,
		`{"m":"\u0000"}`:         `{"m":"\ufffd"}`,
		`{"m":"x\u0000y\u0000"}`: `{"m":"x\ufffdy\ufffd"}`,
		`{"m":"\\u0000"}`:        `{"m":"\\u0000"}`,      // an escaped backslash, then text
		`{"m":"\\\u0000"}`:       `{"m":"\\\ufffd"}`,     // an escaped backslash, then a real escape
		`{"m":"\u00000"}`:        `{"m":"\ufffd0"}`,      // six characters, not seven
		`{"m":"\u0001\u0000"}`:   `{"m":"\u0001\ufffd"}`, // other control escapes are jsonb-safe
		`{"m":"\"\u0000\""}`:     `{"m":"\"\ufffd\""}`,   // after a quote escape
		`{"\u0000":1}`:           `{"\ufffd":1}`,         // keys too
		`{"m":"\u000`:            `{"m":"\u000`,          // truncated: left alone, no panic
		`\`:                      `\`,                    // a lone backslash at the end
	} {
		if got := string(jsonbSafe([]byte(in))); got != want {
			t.Errorf("jsonbSafe(%s) = %s, want %s", in, got, want)
		}
	}
	clean := []byte(`{"m":"no escape here"}`)
	if got := jsonbSafe(clean); &got[0] != &clean[0] {
		t.Error("a payload without the escape must be returned as is, not copied")
	}
	nul := []byte(`{"m":"\u0000"}`)
	jsonbSafe(nul)
	if string(nul) != `{"m":"\u0000"}` {
		t.Error("jsonbSafe wrote into its input")
	}
}

// FuzzJSONBSafe: for any string the JSON encoder can produce, the replacement
// decodes to the same document with U+0000 turned into U+FFFD, and nothing else.
func FuzzJSONBSafe(f *testing.F) {
	for _, s := range []string{"", "a", "\x00", "\\u0000", "\\\x00", "x\x00\x00y", "\"\x00", " \x00"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		// A payload-shaped object: a standard key and an emitter-controlled
		// key, as fields and extra have.
		in, err := json.Marshal(map[string]string{"short-message": s, "_" + s: "k"})
		if err != nil {
			t.Skip()
		}
		var want, got map[string]string
		if err := json.Unmarshal(in, &want); err != nil {
			t.Fatal(err)
		}
		out := jsonbSafe(in)
		if bytes.Contains(out, nulEscape) && !bytes.Contains(out, []byte(`\\u0000`)) {
			t.Fatalf("a NUL escape survived: %s", out)
		}
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("jsonbSafe(%s) = %s: %v", in, out, err)
		}
		repl := func(v string) string { return strings.ReplaceAll(v, "\x00", "\ufffd") }
		if len(got) != len(want) {
			t.Fatalf("%d keys, want %d", len(got), len(want))
		}
		for k, v := range want {
			if got[repl(k)] != repl(v) {
				t.Fatalf("key %q: %q, want %q", k, got[repl(k)], repl(v))
			}
		}
	})
}

// SPEC §3.7.1, with the skill's own vector: integer arithmetic, no float8.
func TestInboxEvent(t *testing.T) {
	at, nsec := inboxEvent(1777175016, 623123136)
	if at.Format(time.RFC3339Nano) != "2026-04-26T03:43:36.623123Z" || nsec != 136 {
		t.Errorf("event_at %s event_nsec %d", at.Format(time.RFC3339Nano), nsec)
	}
	for _, sub := range []int64{0, 999, 1000, 999999999} {
		at, nsec := inboxEvent(1, sub)
		if int64(at.Nanosecond())+int64(nsec) != sub || at.Unix() != 1 {
			t.Errorf("sub %d -> %v + %d", sub, at, nsec)
		}
	}
}

// A pgArgs returned to the pool still holding references would pin a chunk
// buffer that already went back to its pool, and the next chunk would carry
// another emitter's envelope (SPEC §10.5).
func TestPGArgsResetDropsReferences(t *testing.T) {
	b := batchOf(t, docGELF, `{"version":"1.1","host":"web-01","short_message":"m"}`)
	a := &pgArgs{}
	for i := range b.Envelopes {
		if err := a.add(&b.Envelopes[i]); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.payload) != 2 || a.space[1] != "default" {
		t.Fatalf("add stored %d payloads, spaces %v", len(a.payload), a.space)
	}
	a.reset()
	if len(a.payload) != 0 || len(a.uuid) != 0 || len(a.space) != 0 || len(a.eventAt) != 0 {
		t.Fatal("reset did not truncate")
	}
	for _, p := range a.payload[:cap(a.payload)] {
		if p != nil {
			t.Fatal("reset left a payload reference behind")
		}
	}
	for _, s := range a.space[:cap(a.space)] {
		if s != "" {
			t.Fatal("reset left a space behind")
		}
	}
}

// An envelope the sink cannot map is a logic refusal of this data, not a
// destination failure.
func TestPGArgsRefusesWhatItCannotMap(t *testing.T) {
	b := batchN(t, 1)
	e := b.Envelopes[0]
	e.Item.Head.From = "not-a-uuid"
	if err := (&pgArgs{}).add(&e); ClassOf(err) != ClassLogic || err == nil {
		t.Errorf("bad from: %v", err)
	}
	// A from of another UUID version: the queue's from_uuid CHECK wants 7
	// (review finding 6), so it is refused here, not as a 23514 of the chunk.
	e = b.Envelopes[0]
	e.Item.Head.From = "01962a3f-07bc-40b4-ae05-c57e7541d8a8"
	if err := (&pgArgs{}).add(&e); ClassOf(err) != ClassLogic || err == nil {
		t.Errorf("v4 from: %v", err)
	}
	e = b.Envelopes[0]
	e.JLH = []byte(`{"uuid":"x"}`)
	if err := (&pgArgs{}).add(&e); ClassOf(err) != ClassLogic || err == nil {
		t.Errorf("no payload: %v", err)
	}
}

// inboxSkillSHA256 is the hash of the DDL of skill loghub-postgres-table-inbox
// (sql/inbox.sql) as consulted on 2026-10-06: the text of docker/postgres/inbox.sql
// from its "-- ====" line to the end. The queue is shared by every LogHub
// producer; a local edit would split it. Changing the DDL means consulting the
// skill again, then this hash, SPEC §3.7.1 and the validation together.
const inboxSkillSHA256 = "6ad5deec8a8c56204a829d94acd9b9da85af4110f2f809459be900e5c00d1e28"

// SPEC §3.7.1: docker/postgres/inbox.sql is the file operators apply and the
// file the integration suites build their queues from; it is the skill's DDL
// verbatim, and every column the server writes is in it with a type the
// validation accepts.
func TestPostgresInboxFileIsTheSkillDDL(t *testing.T) {
	b, err := os.ReadFile("../../docker/postgres/inbox.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	i := strings.Index(s, "-- ====")
	if i < 0 {
		t.Fatal("no -- ==== line: the skill's DDL is not in the file")
	}
	if sum := sha256.Sum256([]byte(s[i:])); hex.EncodeToString(sum[:]) != inboxSkillSHA256 {
		t.Errorf("the DDL in docker/postgres/inbox.sql is not the skill's (sha256 %x)", sum)
	}
	start := strings.Index(s, "CREATE TABLE loghub.inbox (")
	end := strings.Index(s, "CONSTRAINT inbox_pk PRIMARY KEY (uuid),")
	if start < 0 || end < 0 {
		t.Fatal("the file lacks loghub.inbox or its key on (uuid)")
	}
	have := map[string]string{}
	for _, line := range strings.Split(s[start:end], "\n")[1:] {
		if f := strings.Fields(line); len(f) >= 2 {
			have[f[0]] = f[1]
		}
	}
	pgName := map[string]string{"timestamptz": "timestamp with time zone"}
	for _, c := range pgColumns {
		typ, ok := have[c.name]
		if n, alias := pgName[typ]; alias {
			typ = n
		}
		if !ok || !contains(c.types, typ) {
			t.Errorf("column %s is %q in the file, the contract wants %v", c.name, typ, c.types)
		}
	}
	if have["received_at"] != "timestamptz" {
		t.Error("received_at (the table's DEFAULT now()) is missing")
	}
}

func TestSplitPostgresAddr(t *testing.T) {
	for in, want := range map[string]string{
		"127.0.0.1:5432": "127.0.0.1 5432", "[::1]:6543": "::1 6543", "/var/run/postgresql": "/var/run/postgresql 5432",
	} {
		h, p, err := splitPostgresAddr(in)
		if err != nil || h+" "+strconv.Itoa(int(p)) != want {
			t.Errorf("%q -> %q %d %v", in, h, p, err)
		}
	}
	for _, bad := range []string{"", "nohost", "h:0", "h:99999"} {
		if _, _, err := splitPostgresAddr(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestNewPostgresIsLazyAndRefusesBadConfig(t *testing.T) {
	p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d",
		ConnectTimeout: time.Second}, discard(), metrics())
	if err != nil {
		t.Fatalf("NewPostgres against a dead port: %v", err)
	}
	if p.Type() != "postgres" || p.regname != "loghub.inbox" {
		t.Errorf("type %q table %q", p.Type(), p.regname)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Health(ctx); err == nil || ClassOf(err) != ClassNetwork {
		t.Errorf("Health against a dead port = %v, want network so the boot keeps waiting", err)
	}
	if err := p.Write(ctx, batchN(t, 1)); err == nil || ClassOf(err) != ClassNetwork {
		t.Errorf("Write against a dead port = %v, want network", err)
	}
	p.Close()
	if err := p.Write(ctx, batchN(t, 1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v", err)
	}

	for name, cfg := range map[string]PostgresConfig{
		"sql in table": {Addr: "127.0.0.1:5432", Table: "t; drop table x"},
		"upper table":  {Addr: "127.0.0.1:5432", Table: "Events"},
		"bad addr":     {Addr: "nohost"},
		"missing CA":   {Addr: "127.0.0.1:5432", TLS: true, TLSCA: "/nonexistent/ca.pem"},
		"three-part":   {Addr: "127.0.0.1:5432", Table: "a.b.c"},
	} {
		if p, err := NewPostgres(cfg, discard(), metrics()); err == nil {
			p.Close()
			t.Errorf("%s: accepted", name)
		}
	}
}

// Review finding 5: the libpq environment reached the pool -- PGOPTIONS as a
// runtime parameter, PGTARGETSESSIONATTRS as a ValidateConnect that refused
// every primary. Only what the registry owns survives, plus client_encoding.
func TestNewPostgresIgnoresTheLibpqEnvironment(t *testing.T) {
	t.Setenv("PGOPTIONS", "-c search_path=nowhere")
	t.Setenv("PGTARGETSESSIONATTRS", "standby")
	t.Setenv("PGAPPNAME", "from-env")
	t.Setenv("PGHOST", "10.255.255.1,10.255.255.2")

	build := func(t *testing.T) *pgxpool.Config {
		t.Helper()
		p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", AppName: "sls"}, discard(), metrics())
		if err != nil {
			t.Fatalf("the libpq environment reached the parse: %v", err)
		}
		t.Cleanup(func() { p.Close() })
		return p.pool.Config()
	}

	// Pendência 9 de 2026-10-11, first half: these used to fail the parse itself --
	// exit 1 over a variable the registry does not know.
	for name, kv := range map[string][2]string{
		"PGSSLMODE=bogus":   {"PGSSLMODE", "bogus"},
		"PGPORT not a port": {"PGPORT", "not-a-port"},
		"PGSERVICE missing": {"PGSERVICE", "no-such-service"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("PGSERVICEFILE", filepath.Join(t.TempDir(), "absent.conf"))
			t.Setenv(kv[0], kv[1])
			if _, err := pgxpool.ParseConfig(""); err == nil {
				t.Fatalf("control: pgx parsed %s=%s; the environment did not reach the parser", kv[0], kv[1])
			}
			build(t)
			if os.Getenv(kv[0]) != kv[1] {
				t.Errorf("%s was not restored after the parse", kv[0])
			}
		})
	}

	// Second half: these parse, and landed in fields nothing overwrote,
	// refusing every connection with no SQLSTATE -- network, retried until
	// boot_timeout with the log blaming the network.
	t.Run("connection settings", func(t *testing.T) {
		t.Setenv("PGSSLMODE", "require")
		t.Setenv("PGCHANNELBINDING", "require")
		t.Setenv("PGREQUIREAUTH", "md5")
		t.Setenv("PGSSLNEGOTIATION", "direct")
		t.Setenv("PGMINPROTOCOLVERSION", "3.2")
		t.Setenv("PGMAXPROTOCOLVERSION", "3.2")
		ctl, err := pgxpool.ParseConfig("")
		if err != nil {
			t.Fatal(err)
		}
		if c := ctl.ConnConfig; c.ChannelBinding != "require" || c.RequireAuth != "md5" ||
			c.SSLNegotiation != "direct" || c.MinProtocolVersion != "3.2" || len(c.Fallbacks) == 0 {
			t.Fatalf("control: the environment did not reach the parser (%q %q %q %q, %d fallbacks)",
				c.ChannelBinding, c.RequireAuth, c.SSLNegotiation, c.MinProtocolVersion, len(c.Fallbacks))
		}

		cc := build(t).ConnConfig
		if cc.ValidateConnect != nil {
			t.Error("ValidateConnect from PGTARGETSESSIONATTRS survived")
		}
		if cc.TLSConfig != nil {
			t.Error("PGSSLMODE reached the pool as a TLSConfig; TLS comes from [sink.postgres] alone")
		}
		if len(cc.Fallbacks) != 0 {
			t.Errorf("%d fallbacks survived: a refused dial would be retried elsewhere or in another mode", len(cc.Fallbacks))
		}
		if cc.Host != "127.0.0.1" || cc.Port != 1 {
			t.Errorf("host %s port %d, want the configured 127.0.0.1:1 over PGHOST", cc.Host, cc.Port)
		}
		if cc.ChannelBinding == "require" || cc.RequireAuth != "" || cc.SSLNegotiation == "direct" ||
			cc.MinProtocolVersion == "3.2" || cc.MaxProtocolVersion == "3.2" {
			t.Errorf("libpq settings survived: channel_binding=%q require_auth=%q sslnegotiation=%q "+
				"min_protocol=%q max_protocol=%q", cc.ChannelBinding, cc.RequireAuth, cc.SSLNegotiation,
				cc.MinProtocolVersion, cc.MaxProtocolVersion)
		}
		want := map[string]string{"client_encoding": "UTF8", "application_name": "sls"}
		if len(cc.RuntimeParams) != len(want) {
			t.Errorf("runtime params %v, want %v", cc.RuntimeParams, want)
		}
		for k, v := range want {
			if cc.RuntimeParams[k] != v {
				t.Errorf("runtime param %s = %q, want %q", k, cc.RuntimeParams[k], v)
			}
		}
	})
}

// Review finding 8 (minor): the text "\\u0000" is not a NUL escape, and its
// payload is passed through, not rebuilt.
func TestHasNULEscape(t *testing.T) {
	for in, want := range map[string]bool{
		`{"m":"\u0000"}`:   true,
		`{"m":"\\u0000"}`:  false,
		`{"m":"\\\u0000"}`: true,
		`{"m":"x"}`:        false,
	} {
		if got := hasNULEscape([]byte(in)); got != want {
			t.Errorf("hasNULEscape(%s) = %v", in, got)
		}
	}
	literal := []byte(`{"short-message":"\\u0000"}`)
	if got := inboxPayload(literal); &got[0] != &literal[0] {
		t.Error("a payload with the literal text \\u0000 was copied")
	}
}

// FuzzInboxPayload is the real path (review finding 8): a GELF document with
// the fuzzed strings in every place an emitter controls -- short_message,
// full_message, host, facility, file, a field name and value, the keys and
// values of a nested extra object, numbers no float64 holds -- converted as a
// worker does, with raw forced or not. Whatever they hold, the column copy is
// JSON without a NUL escape, raw comes back exact, every other string and key
// equals the original with U+0000 -> U+FFFD, numbers keep their text, and the
// envelope rebuilt with the copy still validates against both schemas and the
// semantic rules. With whole, the fuzzed bytes ARE the document (mostly
// malformed: raw alone).
func FuzzInboxPayload(f *testing.F) {
	for _, s := range []string{"", "a", "\x00", "a\x00b\x00", "\\u0000", "\u2028\x00", "\xff\x00", "\ufffd"} {
		f.Add(s, s, false, false)
		f.Add(s, "k", true, false)
	}
	f.Add(`{"version":"1.1","host":"h","short_message":"m","_a":"\u0000"}`, "", false, true)
	f.Add("\x00not json", "", true, true)
	f.Fuzz(func(t *testing.T, msg, key string, always, whole bool) {
		doc := []byte(msg + key)
		if !whole {
			q := func(s string) string { b, _ := json.Marshal(s); return string(b) }
			doc = []byte(`{"version":"1.0","host":` + q("h"+key) + `,"short_message":` + q(msg) +
				`,"full_message":` + q(msg+"x") + `,"facility":` + q(key+"f") + `,"file":` + q(msg+"f") +
				`,"_v":` + q(msg) + `,` + q("_"+key) + `:` + q(msg) + `,"_n":1e400,` +
				q("x"+key) + `:{` + q(key) + `:[` + q(msg) + `,12345678901234567890,{` + q(msg) + `:` + q(key) + `}]}}`)
		}
		if len(doc) > 1<<16 {
			t.Skip()
		}
		// A document of only JSON whitespace never reaches the converter: the
		// receiver drops it as empty (SPEC §5.6), and the chunk builder refuses
		// an empty one (§6.1), so there is no envelope to look at. Found by
		// this target on 2026-10-31 (testdata 681c1e7122fdf67c), as in
		// go-gelf-pg-server.
		if len(bytes.Trim(doc, " \t\r\n")) == 0 {
			t.Skip()
		}
		cfg := testConvert()
		if always {
			cfg.KeepRaw = convert.KeepRawAlways
		}
		b := batchWith(t, cfg, newItem(string(doc)))
		e := &b.Envelopes[0]
		in, ok := jlh.Payload(e.JLH)
		if !ok {
			t.Fatal("no payload")
		}
		out := inboxPayload(in)
		if hasNULEscape(out) || !json.Valid(out) {
			t.Fatalf("inboxPayload(%s) = %s", in, out)
		}
		decode := func(b []byte) map[string]any {
			var m map[string]any
			dec := json.NewDecoder(bytes.NewReader(b))
			dec.UseNumber()
			if err := dec.Decode(&m); err != nil {
				t.Fatal(err)
			}
			return m
		}
		orig, got := decode(in), decode(out)
		raw, _ := got["raw"].(string)
		if got["raw-encoding"] == "base64" {
			b, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		origRaw, _ := orig["raw"].(string)
		if orig["raw-encoding"] == "base64" {
			b, _ := base64.StdEncoding.DecodeString(origRaw)
			origRaw = string(b)
		}
		if raw != origRaw {
			t.Fatalf("raw %q, want %q", raw, origRaw)
		}
		for _, m := range []map[string]any{orig, got} {
			delete(m, "raw")
			delete(m, "raw-encoding")
		}
		if want, collided := replNUL(orig); !collided && !reflect.DeepEqual(want, any(got)) {
			t.Fatalf("copy %s\nfrom %s", out, in)
		}
		head := string(e.JLH[:len(e.JLH)-1-len(in)])
		if err := validateQueueEnvelope(append(append([]byte(head), out...), '}')); err != nil {
			t.Fatalf("the envelope with the column's payload is invalid: %v\n%s", err, out)
		}
	})
}

// replNUL is the expected copy of a decoded value: U+0000 -> U+FFFD in every
// string and key. collided reports two keys of one object that became one, a
// case whose winner TestInboxPayloadKeyCollisionIsDeterministic covers.
func replNUL(v any) (out any, collided bool) {
	r := func(s string) string { return strings.ReplaceAll(s, "\x00", "\uFFFD") }
	switch x := v.(type) {
	case string:
		return r(x), false
	case []any:
		o := make([]any, len(x))
		for i := range x {
			var c bool
			o[i], c = replNUL(x[i])
			collided = collided || c
		}
		return o, collided
	case map[string]any:
		o := make(map[string]any, len(x))
		for k, e := range x {
			if _, dup := o[r(k)]; dup {
				collided = true
			}
			var c bool
			o[r(k)], c = replNUL(e)
			collided = collided || c
		}
		return o, collided
	}
	return v, false
}

// SPEC §3.7.1: a number jsonb cannot hold (22003, class 22: the whole chunk
// lost as logic) goes in the queue's copy as the string of its text; every
// other number, and the JLH itself, is untouched. The ceilings are the numeric
// type's: 131072 digits before the point, 16383 after.
func TestInboxPayloadCarriesNumbersJSONBCannotHoldAsText(t *testing.T) {
	for num, holds := range map[string]bool{
		"1": true, "-0": true, "1.50": true, "1e400": true, "12345678901234567890": true,
		"1e131071": true, "1e-16383": true, "1E+131071": true, "0.5e-16382": true, "1e0000000000131071": true,
		"1e131072": false, "1e-16384": false, "0.5e-16383": false, "1e1000000": false,
		"-1e99999999999999999999": false, "1e-99999999999999999999": false,
		// A zero mantissa is 0 whatever a positive exponent says; the scale
		// still counts (review of the cuts, 2026-10-31).
		"0e999999": true, "0.0e131072": true, "-0e99999999999999999999": true, "0E+5": true,
		"0e-20000": false, "0.0e-16383": false, "0.0e-16382": true,
	} {
		if got := jsonbHolds([]byte(num)); got != holds {
			t.Errorf("jsonbHolds(%s) = %v, want %v", num, got, holds)
		}
	}

	b := batchOf(t, `{"version":"1.1","host":"h","short_message":"m","_huge":1e1000000,"_ok":1e400,`+
		`"_s":"1e1000000","x":{"deep":[1e-16384,2]}}`)
	e := &b.Envelopes[0]
	payload, _ := jlh.Payload(e.JLH)
	if !hasJSONBOverflow(payload) {
		t.Fatal("the payload holds 1e1000000 and was not flagged")
	}
	col := inboxPayload(payload)
	if hasJSONBOverflow(col) || !json.Valid(col) {
		t.Fatalf("the column still holds a number jsonb refuses: %s", col)
	}
	var doc struct {
		Fields map[string]any            `json:"fields"`
		Extra  map[string]map[string]any `json:"extra"`
	}
	dec := json.NewDecoder(bytes.NewReader(col))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if doc.Fields["huge"] != "1e1000000" || doc.Fields["ok"] != json.Number("1e400") || doc.Fields["s"] != "1e1000000" {
		t.Errorf("fields in the copy: %v", doc.Fields)
	}
	if deep, _ := doc.Extra["x"]["deep"].([]any); len(deep) != 2 || deep[0] != "1e-16384" || deep[1] != json.Number("2") {
		t.Errorf("extra in the copy: %v", doc.Extra)
	}
	if !bytes.Contains(e.JLH, []byte(`"huge":1e1000000`)) {
		t.Errorf("the JLH itself was changed: %s", e.JLH)
	}
	// The control: a payload without either problem is the JLH's own bytes.
	plain := batchOf(t, docGELF).Envelopes[0]
	p2, _ := jlh.Payload(plain.JLH)
	if c := inboxPayload(p2); &c[0] != &p2[0] {
		t.Error("a payload jsonb takes as is was copied")
	}
}
