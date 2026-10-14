package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// Nothing in this file needs a server. The integration half lives in
// postgres_integration_test.go and skips without WHS_TEST_PG_DSN.

// SPEC §8.5.7 is the table; this is the table read back. The line that
// matters most is class 42: a dropped table classified as logic would drop a
// batch every 5 ms, loudly, forever, while the process looked healthy.
func TestClassifySQLState(t *testing.T) {
	cases := []struct {
		code string
		want Class
	}{
		// Waiting fixes it.
		{"08006", ClassNetwork}, // connection_failure
		{"08003", ClassNetwork}, // connection_does_not_exist
		{"57P01", ClassNetwork}, // admin_shutdown
		{"57P03", ClassNetwork}, // cannot_connect_now
		{"53300", ClassNetwork}, // too_many_connections
		{"40001", ClassNetwork}, // serialization_failure
		{"40P01", ClassNetwork}, // deadlock_detected
		{"55P03", ClassNetwork}, // lock_not_available
		{"57014", ClassNetwork}, // query_canceled

		// Waiting never fixes it: end the process.
		{"25006", ClassFatal}, // read_only_sql_transaction -- deliberately the hard choice
		{"53100", ClassFatal}, // disk_full
		{"53200", ClassFatal}, // out_of_memory
		{"53400", ClassFatal}, // configuration_limit_exceeded
		{"28P01", ClassFatal}, // invalid_password
		{"28000", ClassFatal}, // invalid_authorization_specification
		{"3D000", ClassFatal}, // invalid_catalog_name
		{"42501", ClassFatal}, // insufficient_privilege
		{"42P01", ClassFatal}, // undefined_table
		{"42703", ClassFatal}, // undefined_column
		{"42P10", ClassFatal}, // invalid_column_reference: no unique index for ON CONFLICT
		{"42804", ClassFatal}, // datatype_mismatch
		{"57P04", ClassFatal}, // database_dropped
		{"XX001", ClassFatal}, // data_corrupted
		// The sink never sends NULL: a not_null_violation after the boot is
		// the table (a required column added, a DEFAULT dropped, a trigger).
		{"23502", ClassFatal},
		// feature_not_supported: ON CONFLICT on a table with an INSERT or
		// UPDATE rule, refused at boot; one created later fails every batch.
		{"0A000", ClassFatal},

		// The destination is alive and refused THIS data.
		{"22021", ClassLogic}, // character_not_in_repertoire
		{"22001", ClassLogic}, // string_data_right_truncation
		{"22003", ClassLogic}, // numeric_value_out_of_range: a narrowed sid column
		{"22P05", ClassLogic}, // untranslatable_character: the NUL jsonbSafe exists for
		{"23514", ClassLogic}, // check_violation: a CHECK of the inbox
		{"23505", ClassLogic}, // unique_violation on some OTHER index
		{"54000", ClassLogic}, // program_limit_exceeded

		// Unknown is retried, not dropped.
		{"P0001", ClassNetwork},
		{"", ClassNetwork},
		{"1", ClassNetwork},
	}
	for _, tc := range cases {
		if got := classifySQLState(tc.code); got != tc.want {
			t.Errorf("sqlstate %q: class = %s, want %s", tc.code, got, tc.want)
		}
	}
}

// Anything without a SQLSTATE never reached a server that answered: it is the
// connection, and it is retried.
func TestClassifyPostgresTransportErrors(t *testing.T) {
	for _, err := range []error{
		errors.New("dial tcp 127.0.0.1:1: connect: connection refused"),
		errors.New("unexpected EOF"),
		context.DeadlineExceeded,
		context.Canceled,
		fmt.Errorf("wrapped: %w", errors.New("closed pool")),
	} {
		if got := ClassOf(classifyPostgres("insert", err)); got != ClassNetwork {
			t.Errorf("%v: class = %s, want network", err, got)
		}
	}
	if classifyPostgres("insert", nil) != nil {
		t.Error("a nil error must classify as nil")
	}
}

// The driver wraps a PgError in connect and pool errors; errors.As must find
// it through the wrapping, and the class must follow the code, not the text.
func TestClassifyPostgresUnwrapsPgError(t *testing.T) {
	inner := &pgconn.PgError{Code: "42P01", Message: `relation "loghub.inbox" does not exist`}
	err := classifyPostgres("insert", fmt.Errorf("exec: %w", inner))

	if !IsFatal(err) {
		t.Fatalf("42P01 through a wrapper classified as %s, want fatal", ClassOf(err))
	}
	if !strings.Contains(err.Error(), "sqlstate=42P01") {
		t.Errorf("error %q does not carry the SQLSTATE for the operator", err)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Error("the classified error must still unwrap to the PgError")
	}

	logic := classifyPostgres("insert", &pgconn.PgError{Code: "23514", ConstraintName: "inbox_level_check"})
	if ClassOf(logic) != ClassLogic {
		t.Errorf("23514 with a constraint name = %s, want logic", ClassOf(logic))
	}
}

// parseUUID is checked against the reference implementation over random
// inputs, in both cases of hex.
func TestParseUUID(t *testing.T) {
	for i := 0; i < 2000; i++ {
		ref := uuid.New()
		if i%2 == 0 {
			v7, err := uuid.NewV7()
			if err != nil {
				t.Fatal(err)
			}
			ref = v7
		}
		s := ref.String()
		if i%3 == 0 {
			s = strings.ToUpper(s)
		}
		got, ok := parseUUID(s)
		if !ok {
			t.Fatalf("parseUUID(%q) refused a valid uuid", s)
		}
		if got != [16]byte(ref) {
			t.Fatalf("parseUUID(%q) = %x, want %x", s, got, [16]byte(ref))
		}
	}
	// The envelope's own generator must be accepted as-is.
	if u := envelope.NewUUID(time.Now()); func() bool { got, ok := parseUUID(u.String()); return !ok || got != [16]byte(u) }() {
		t.Error("parseUUID does not round-trip a uuid from envelope.NewUUID")
	}

	for _, bad := range []string{
		"",
		"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6",   // 35 chars
		"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f0", // 37 chars
		"019823f17a2c7b3d8e4f1a2b3c4d5e6f0000",  // no dashes
		"019823f1-7a2c-7b3d-8e4f_1a2b3c4d5e6f",  // wrong separator
		"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6g",  // g is not hex
		"{19823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f}",
	} {
		if _, ok := parseUUID(bad); ok {
			t.Errorf("parseUUID(%q) accepted a malformed id", bad)
		}
	}
}

// SELECT * FROM unnest(...) yields columns in argument order: the column list,
// the casts, the values and the validation contract must agree position by
// position (SPEC §8.5.4).
func TestPostgresInsertStatementMatchesTheContract(t *testing.T) {
	m := regexp.MustCompile(`\(([a-z_, ]+)\) SELECT \* FROM unnest\((.+)\) ON CONFLICT \(uuid\) DO NOTHING$`).
		FindStringSubmatch(pgInsert)
	if m == nil {
		t.Fatalf("unexpected statement shape: %s", pgInsert)
	}
	cols := strings.Split(m[1], ", ")
	casts := strings.Split(m[2], ", ")
	if len(cols) != 10 || len(casts) != 10 || len(pgColumns) != 10 {
		t.Fatalf("%d columns, %d arrays, %d in the contract; SPEC §3.6 writes 10", len(cols), len(casts), len(pgColumns))
	}
	pgName := map[string]string{"timestamptz": "timestamp with time zone"}
	for i, c := range pgColumns {
		if cols[i] != c.name {
			t.Errorf("column %d is %q in the statement and %q in the contract", i, cols[i], c.name)
		}
		if want := fmt.Sprintf("$%d::", i+1); !strings.HasPrefix(casts[i], want) {
			t.Errorf("array %d is %q, want parameter %s", i, casts[i], want)
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
	if err := a.add(mkItem("one")); err != nil {
		t.Fatal(err)
	}
	if n := len(a.values()); n != 10 {
		t.Fatalf("%d parameters, want 10", n)
	}
}

// SPEC §3.6 and §8.5.4: every column comes from the JLH header, read back from
// the item's own bytes, and the payload is the JLH's own payload object -- a
// sub-slice, not a copy.
func TestPGArgsMapsTheEnvelope(t *testing.T) {
	it := itemIn("acme.store", testRequest("/hook", []byte(`{"a":1}`)))
	a := &pgArgs{}
	if err := a.add(it); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		UUID, From, Space, Type string
		Level, SID, Step        int64
		Event                   struct{ UTS, Sub int64 }
		Payload                 json.RawMessage
	}
	if err := json.Unmarshal(it.Env, &doc); err != nil {
		t.Fatal(err)
	}
	if uuidv7.UUID(a.uuid[0]).String() != doc.UUID || uuidv7.UUID(a.from[0]).String() != doc.From ||
		a.space[0] != doc.Space || a.typ[0] != doc.Type || int64(a.level[0]) != doc.Level ||
		a.sid[0] != doc.SID || int64(a.step[0]) != doc.Step {
		t.Errorf("header columns %v %v %q %q %d %d %d differ from the JLH %+v",
			a.uuid[0], a.from[0], a.space[0], a.typ[0], a.level[0], a.sid[0], a.step[0], doc)
	}
	if doc.UUID != it.ID || doc.Type != envelope.Type || doc.Level != envelope.Level || doc.Space != "acme.store" ||
		doc.From != testFrom {
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
	if &a.payload[0][0] != &it.Env[len(it.Env)-1-len(a.payload[0])] {
		t.Error("a payload without NUL was copied; it must be a sub-slice of the item's JLH")
	}
}

// SPEC §3.6: jsonb refuses \u0000 with 22P05, class 22, and a %00 in the query
// produces one. The column gets a copy with U+FFFD; the item's JLH -- the
// bytes every retry resends and every other destination receives -- stays
// intact.
func TestPGArgsReplacesNULInACopyOnly(t *testing.T) {
	r := testRequest("/q", []byte(`{"a":1}`))
	r.RawQuery = "a=x%00y&b=%5Cu0000"
	it := itemIn("default", r)
	before := string(it.Env)
	if !strings.Contains(before, `"a":"x\u0000y"`) {
		t.Fatalf("the serialiser no longer escapes NUL as \\u0000: %s", before)
	}
	a := &pgArgs{}
	if err := a.add(it); err != nil {
		t.Fatal(err)
	}
	if string(it.Env) != before {
		t.Fatal("the JLH was modified; the replacement must touch the column copy only")
	}
	col := a.payload[0]
	if bytes.Contains(col, nulEscape) && !bytes.Contains(col, []byte(`\\u0000`)) || !json.Valid(col) {
		t.Fatalf("payload column still has the NUL escape, or is not JSON: %s", col)
	}
	var p struct{ Query map[string]string }
	if err := json.Unmarshal(col, &p); err != nil {
		t.Fatal(err)
	}
	// The real escape is replaced; a literal backslash followed by u0000 is
	// text, and stays.
	if p.Query["a"] != "x\ufffdy" || p.Query["b"] != `\u0000` {
		t.Errorf("query in the column = %q, want U+FFFD for the NUL and the literal text kept", p.Query)
	}
	// The item's JLH is still a valid webhook. The copy is checked
	// against it, not through the oracle: its query says U+FFFD where the url
	// still says %00, the one divergence the column is allowed (invariant
	// 11), and the oracle rightly refuses a url that disagrees with query.
	validJLH(t, it.Env)
	orig := before[strings.Index(before, `"payload":`)+len(`"payload":`) : len(before)-1]
	want := strings.Replace(orig, `"x\u0000y"`, `"x\ufffdy"`, 1)
	if want == orig || string(col) != want {
		t.Errorf("payload column differs from the JLH's payload by more than the NUL escape:\n%s\nwant:\n%s", col, want)
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
	for _, s := range []string{"", "a", "\x00", "\\u0000", "\\\x00", "x\x00\x00y", "\"\x00", " \x00"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		in, err := json.Marshal(map[string]string{"m": s, s: "k"})
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

// SPEC §3.6, with the skill's own vector: integer arithmetic, no float8.
func TestInboxEvent(t *testing.T) {
	at, nsec := inboxEvent(1777175016, 623123136)
	if at.Format(time.RFC3339Nano) != "2026-04-26T03:43:36.623123Z" || nsec != 136 || at.Location() != time.UTC {
		t.Errorf("event_at %s event_nsec %d", at.Format(time.RFC3339Nano), nsec)
	}
	for _, sub := range []int64{0, 999, 1000, 999999999} {
		at, nsec := inboxEvent(1, sub)
		if int64(at.Nanosecond())+int64(nsec) != sub || at.Unix() != 1 {
			t.Errorf("sub %d -> %v + %d", sub, at, nsec)
		}
	}
}

// SPEC §8.5.4, cousin of §12.3: a pooled argument set that keeps references to
// the payloads after the batch pins JLH buffers that have gone back to the
// envelope pool. reset must leave nothing behind, in the backing arrays too.
func TestPgArgsResetDropsReferences(t *testing.T) {
	a := &pgArgs{}
	items := append(mkBatch(4), itemIn("acme", testRequest("/other", nil)))
	for _, it := range items {
		if err := a.add(it); err != nil {
			t.Fatalf("add refused a valid item: %v", err)
		}
	}
	if len(a.payload) != 5 || len(a.uuid) != 5 || len(a.eventNsec) != 5 || a.space[4] != "acme" {
		t.Fatalf("add did not append every column: payload=%d uuid=%d nsec=%d spaces=%v",
			len(a.payload), len(a.uuid), len(a.eventNsec), a.space)
	}

	a.reset()

	if len(a.payload) != 0 || len(a.uuid) != 0 || len(a.space) != 0 || len(a.eventAt) != 0 {
		t.Fatalf("reset left lengths payload=%d uuid=%d space=%d", len(a.payload), len(a.uuid), len(a.space))
	}
	if cap(a.payload) < 5 {
		t.Error("reset dropped the capacity; the pool would then allocate on every batch")
	}
	for i, b := range a.payload[:cap(a.payload)] {
		if b != nil {
			t.Errorf("payload[%d] still references a buffer after reset", i)
		}
	}
	for i, s := range a.space[:cap(a.space)] {
		if s != "" {
			t.Errorf("space[%d] = %q still held after reset", i, s)
		}
	}
	for i, s := range a.typ[:cap(a.typ)] {
		if s != "" {
			t.Errorf("type[%d] = %q still held after reset", i, s)
		}
	}
}

// An empty body is still a payload: the content object with length 0, never
// a nil slice pgx would encode as NULL and NOT NULL would refuse with class
// 23, dropping the batch.
func TestPgArgsEmptyBodyIsAPayloadNotNull(t *testing.T) {
	a := &pgArgs{}
	if err := a.add(itemIn("default", testRequest("/empty", nil))); err != nil {
		t.Fatal(err)
	}
	if a.payload[0] == nil {
		t.Fatal("an empty body produced a nil payload, which pgx encodes as NULL")
	}
	var p struct{ Content struct{ Length int } }
	if err := json.Unmarshal(a.payload[0], &p); err != nil || p.Content.Length != 0 {
		t.Errorf("payload %s (%v), want an object with content.length 0", a.payload[0], err)
	}
}

// An envelope the sink cannot map is a logic refusal of this data, not a
// destination failure (SPEC §8.5.4).
func TestPGArgsRefusesWhatItCannotMap(t *testing.T) {
	if err := (&pgArgs{}).add(badItem()); err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a v1 envelope: %v, want logic", err)
	}
	good := mkItem("cut")
	truncated := envelope.NewItem(good.ID, good.Env[:len(good.Env)-1], -1)
	if err := (&pgArgs{}).add(truncated); err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a truncated JLH: %v, want logic", err)
	}
	if err := (&pgArgs{}).add(good); err != nil {
		t.Errorf("control: the whole JLH was refused: %v", err)
	}
	// A from of another UUID version: the queue's from_uuid CHECK wants 7.
	v4 := envelope.NewItem(good.ID, bytes.Replace(good.Env, []byte(`"from":"01962a3f-07bc-7`), []byte(`"from":"01962a3f-07bc-4`), 1), -1)
	if bytes.Equal(v4.Env, good.Env) {
		t.Fatal("the fixture's from is not the one this test rewrites")
	}
	if err := (&pgArgs{}).add(v4); err == nil || ClassOf(err) != ClassLogic {
		t.Errorf("a v4 from: %v, want logic", err)
	}
}

// 23514 naming a constraint is this data; naming none is "no partition of
// relation ... found for row", the table: fatal.
func TestClassifyPartitionRoutingIsFatal(t *testing.T) {
	if got := ClassOf(classifyPostgres("insert", &pgconn.PgError{Code: "23514", ConstraintName: "inbox_level_ck"})); got != ClassLogic {
		t.Errorf("a CHECK violation = %v, want logic", got)
	}
	if got := ClassOf(classifyPostgres("insert", &pgconn.PgError{Code: "23514"})); got != ClassFatal {
		t.Errorf("a partition routing failure = %v, want fatal", got)
	}
}

// The libpq environment does not reach the pool: PGOPTIONS would be a
// runtime parameter, PGTARGETSESSIONATTRS a ValidateConnect refusing every
// primary.
func TestNewPostgresIgnoresTheLibpqEnvironment(t *testing.T) {
	t.Setenv("PGOPTIONS", "-c search_path=nowhere")
	t.Setenv("PGTARGETSESSIONATTRS", "standby")
	t.Setenv("PGAPPNAME", "from-env")
	p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", Table: "loghub.inbox", AppName: "whs"}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cc := p.pool.Config().ConnConfig
	if cc.ValidateConnect != nil {
		t.Error("ValidateConnect from PGTARGETSESSIONATTRS survived")
	}
	want := map[string]string{"client_encoding": "UTF8", "application_name": "whs"}
	if len(cc.RuntimeParams) != len(want) {
		t.Errorf("runtime params %v, want %v", cc.RuntimeParams, want)
	}
	for k, v := range want {
		if cc.RuntimeParams[k] != v {
			t.Errorf("runtime param %s = %q, want %q", k, cc.RuntimeParams[k], v)
		}
	}
}

// The text "\\u0000" is not a NUL escape, and its payload is not copied.
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
	literal := []byte(`{"q":"\\u0000"}`)
	if got := jsonbSafe(literal); &got[0] != &literal[0] {
		t.Error("a payload with the literal text was copied")
	}
}

// inboxSkillSHA256 is the hash of the DDL of skill loghub-postgres-table-inbox
// (sql/inbox.sql) as consulted on 2026-10-06: the text of
// docker/postgres/inbox.sql from its "-- ====" line to the end. The queue is
// shared by every LogHub producer; a local edit would split it. Changing the
// DDL means consulting the skill again, then this hash, SPEC §3.6 and the
// validation together.
const inboxSkillSHA256 = "6ad5deec8a8c56204a829d94acd9b9da85af4110f2f809459be900e5c00d1e28"

// SPEC §3.6: docker/postgres/inbox.sql is the file operators apply and the
// file the integration suite builds its queues from; it is the skill's DDL
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
	cases := []struct {
		in   string
		host string
		port uint16
		ok   bool
	}{
		{"127.0.0.1:5432", "127.0.0.1", 5432, true},
		{"db.internal:15432", "db.internal", 15432, true},
		{"[::1]:5432", "::1", 5432, true},
		{"/var/run/postgresql", "/var/run/postgresql", 5432, true},
		{"/tmp/sock:15432", "/tmp/sock", 15432, true},
		{"", "", 0, false},
		{"nohost", "", 0, false},
		{"host:notaport", "", 0, false},
		{"host:0", "", 0, false},
		{"host:70000", "", 0, false},
	}
	for _, tc := range cases {
		host, port, err := splitPostgresAddr(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("%q: err = %v, want ok=%v", tc.in, err, tc.ok)
			continue
		}
		if tc.ok && (host != tc.host || port != tc.port) {
			t.Errorf("%q = %s:%d, want %s:%d", tc.in, host, port, tc.host, tc.port)
		}
	}
}

// NewPostgres never connects (SPEC §8.5.2): a dead address is not an error,
// so WaitReady owns the retry loop and the admin port is up while it waits.
func TestNewPostgresIsLazy(t *testing.T) {
	p, err := NewPostgres(PostgresConfig{
		Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "loghub.inbox", PoolSize: 2,
	}, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewPostgres against a dead port: %v", err)
	}
	if p.Type() != "postgres" {
		t.Errorf("Type = %q", p.Type())
	}
	if !strings.HasPrefix(p.insert, `INSERT INTO "loghub"."inbox" (uuid, from_uuid,`) ||
		!strings.HasSuffix(p.insert, "ON CONFLICT (uuid) DO NOTHING") {
		t.Errorf("unexpected insert statement: %s", p.insert)
	}

	// The connection failure surfaces as a NETWORK class from Health, which
	// is what makes WaitReady keep trying.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = p.Health(ctx)
	if err == nil {
		t.Fatal("Health against a dead port succeeded")
	}
	if ClassOf(err) != ClassNetwork {
		t.Errorf("dead port classified as %s, want network", ClassOf(err))
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Write(ctx, mkBatch(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := p.Health(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v, want ErrClosed", err)
	}
}

func TestNewPostgresSchemaQualifiedTable(t *testing.T) {
	p, err := NewPostgres(PostgresConfig{
		Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "hooks.inbox",
	}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !strings.Contains(p.insert, `INSERT INTO "hooks"."inbox"`) {
		t.Errorf("schema-qualified table not quoted per part: %s", p.insert)
	}
	if p.regname != "hooks.inbox" {
		t.Errorf("regname = %q", p.regname)
	}
}

func TestNewPostgresRejectsBadConfig(t *testing.T) {
	missingCA := filepath.Join(t.TempDir(), "absent.pem")
	junkCA := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junkCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := PostgresConfig{Addr: "127.0.0.1:1", Username: "u", Database: "d", Table: "loghub.inbox"}

	cases := []struct {
		name string
		mut  func(*PostgresConfig)
		want string
	}{
		{"uppercase table", func(c *PostgresConfig) { c.Table = "Inbox" }, "identifier"},
		{"sql in table", func(c *PostgresConfig) { c.Table = "inbox; drop table x" }, "identifier"},
		{"quoted table", func(c *PostgresConfig) { c.Table = `"inbox"` }, "identifier"},
		{"three-part table", func(c *PostgresConfig) { c.Table = "a.b.c" }, "identifier"},
		{"bad addr", func(c *PostgresConfig) { c.Addr = "nohost" }, "addr"},
		{"missing CA", func(c *PostgresConfig) { c.TLS = true; c.TLSCA = missingCA }, "reading"},
		{"junk CA", func(c *PostgresConfig) { c.TLS = true; c.TLSCA = junkCA }, "no usable certificate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			p, err := NewPostgres(cfg, quietLog(), testMetrics())
			if err == nil {
				p.Close()
				t.Fatal("NewPostgres accepted the configuration")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// fatalHealthSink answers every health check with a permanent refusal, the
// shape of a missing table or a wrong password.
type fatalHealthSink struct{ calls int }

func (s *fatalHealthSink) Write(context.Context, []*envelope.Item) error { return nil }
func (s *fatalHealthSink) Health(context.Context) error {
	s.calls++
	return fatalErr("validate", errors.New("table loghub.inbox does not exist"))
}
func (s *fatalHealthSink) Close() error { return nil }
func (s *fatalHealthSink) Type() string { return "fatal" }

// SPEC §8.5.5: a fatal boot check ends the wait at once. A wrong password
// does not become right in sink.boot_timeout seconds, and the operator is
// waiting for the exit code.
func TestWaitReadyStopsOnFatal(t *testing.T) {
	fs := &fatalHealthSink{}
	h := NewHealth(fs, HealthConfig{Interval: time.Hour, Timeout: 100 * time.Millisecond},
		quietLog(), testMetrics())

	start := time.Now()
	err := h.WaitReady(context.Background(), 30*time.Second, 10*time.Millisecond, 50*time.Millisecond)
	took := time.Since(start)

	if err == nil {
		t.Fatal("WaitReady succeeded against a sink that refuses permanently")
	}
	if !IsFatal(err) {
		t.Errorf("WaitReady returned %v, want the fatal error itself", err)
	}
	if fs.calls != 1 {
		t.Errorf("health was checked %d times, want exactly 1: a fatal answer must not be retried", fs.calls)
	}
	if took > 2*time.Second {
		t.Errorf("WaitReady took %s against a 30s budget; it did not stop on the fatal answer", took)
	}
}

// SPEC §8.5.5: the schema validation runs under sink.op_timeout, not under the
// deadline of the health check that triggers it. The check's deadline passing
// must leave the validation running; a real cancellation of the boot must
// still reach it.
func TestValidationContextOutlivesTheCheckDeadline(t *testing.T) {
	check, cancelCheck := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Second)
	defer stop()

	<-check.Done()
	select {
	case <-vctx.Done():
		t.Fatalf("validation ended with the health check's deadline: %v", vctx.Err())
	case <-time.After(50 * time.Millisecond):
	}
	if dl, ok := vctx.Deadline(); !ok || time.Until(dl) > time.Second {
		t.Errorf("validation deadline = %v (set %v), want at most sink.op_timeout from now", dl, ok)
	}
}

func TestValidationContextFollowsACancelledBoot(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	check, cancelCheck := context.WithTimeout(boot, time.Hour)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled boot did not cancel the schema validation")
	}
}

func TestValidationContextHasItsOwnDeadline(t *testing.T) {
	vctx, stop := validationContext(context.Background(), 20*time.Millisecond)
	defer stop()
	select {
	case <-vctx.Done():
		if !errors.Is(vctx.Err(), context.DeadlineExceeded) {
			t.Errorf("err = %v, want DeadlineExceeded", vctx.Err())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink.op_timeout never ended the validation")
	}
}

// The case the review found: the boot is cancelled AFTER the health check's
// own deadline has passed, while the validation is still running on its own.
// The check context can no longer say so; the lifetime it carries can.
func TestValidationContextFollowsABootCancelledAfterTheCheckDeadline(t *testing.T) {
	boot, cancelBoot := context.WithCancel(context.Background())
	defer cancelBoot()
	check, cancelCheck := context.WithTimeout(withLifetime(boot), 10*time.Millisecond)
	defer cancelCheck()
	vctx, stop := validationContext(check, time.Hour)
	defer stop()

	<-check.Done()
	time.Sleep(20 * time.Millisecond)
	if vctx.Err() != nil {
		t.Fatalf("the check's deadline ended the validation: %v", vctx.Err())
	}
	cancelBoot()
	select {
	case <-vctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("a boot cancelled after the check deadline never reached the validation")
	}
}
