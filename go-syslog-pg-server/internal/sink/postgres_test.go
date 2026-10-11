package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlhcheck"
)

// Nothing in this file needs a server. The integration half lives in
// postgres_integration_test.go and skips without SYSLOG_TEST_PG_DSN.

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
		"3D000", // invalid_catalog_name: the database does not exist
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
	logic := []string{"22021", "22003", "22P02", "22P05", "23502", "23505", "23514"}
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
	b := batchOf(t, line5424)
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
	if doc.Type != "syslog" || doc.Level != 5 || doc.From != testFrom {
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
// rebuilt copy -- raw exact in base64, U+FFFD for the NUL elsewhere -- and the
// chunk's JLH, the bytes every retry resends and every other destination
// receives, stays intact.
func TestPGArgsRebuildsAPayloadWithNUL(t *testing.T) {
	line := "<13>Oct 11 22:14:15 web\x00-01 su[230]: a\x00b"
	b := batchOf(t, line)
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
	if bytes.Contains(col, nulEscape) || !json.Valid(col) {
		t.Fatalf("payload column still has the NUL escape, or is not JSON: %s", col)
	}
	var p struct {
		Message, Raw string
		RawEnc       string `json:"raw-encoding"`
		Size         int
		Host         struct{ Hostname string }
	}
	if err := json.Unmarshal(col, &p); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(p.Raw)
	if p.RawEnc != "base64" || err != nil || string(raw) != line || p.Size != len(line) {
		t.Errorf("raw %q (%s, %v) is not the exact line of %d octets (size %d)", raw, p.RawEnc, err, len(line), p.Size)
	}
	if p.Message != "a\uFFFDb" || p.Host.Hostname != "web\uFFFD-01" {
		t.Errorf("message %q hostname %q: want U+FFFD for the NUL", p.Message, p.Host.Hostname)
	}
	head := before[:strings.Index(before, `"payload":`)+len(`"payload":`)]
	if err := jlhcheck.Validate(append(append([]byte(head), col...), '}')); err != nil {
		t.Errorf("the envelope with the column's payload is not a valid type=syslog envelope: %v", err)
	}
}

// A payload that does not decode still never reaches jsonb with a NUL escape.
func TestInboxPayloadFallsBackToTheByteSwap(t *testing.T) {
	if got := string(inboxPayload([]byte(`{"m":"\u0000"`))); got != `{"m":"\ufffd"` {
		t.Errorf("fallback = %s", got)
	}
	clean := []byte(`{"message":"m"}`)
	if got := inboxPayload(clean); &got[0] != &clean[0] {
		t.Error("a payload without NUL must not be copied")
	}
	// raw already in base64 is left as it is; the NUL elsewhere is swapped.
	got := inboxPayload([]byte(`{"raw":"AA==","raw-encoding":"base64","message":"\u0000"}`))
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
	b := batchOf(t, line5424, "<13>Oct 11 22:14:15 web-01 app: m")
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
//
// And no TLS, whatever PGSSLMODE says (SPEC §1.2): this server has no TLS
// configuration at all, so a TLSConfig coming from the environment would be one
// nobody can see in the registry, the dump or --help -- and a PGHOST list or
// sslmode=prefer would add fallbacks that retry the dial elsewhere or in
// another mode. The control parses the same environment the way NewPostgres
// starts, and must see TLS and fallbacks, or the variables never reached the
// parser and the assertions below prove nothing.
// Review B #1: overwriting the parsed fields was not enough. Some libpq
// variables failed the parse itself (exit 1 over TLS settings, in a server
// without TLS) and others refused every connection with no SQLSTATE, retried
// as network until sink.boot_timeout. With every PG* hidden during the parse
// none of them reaches the pool, and the environment is back afterwards.
func TestNewPostgresParsesWithoutTheLibpqEnvironment(t *testing.T) {
	env := map[string]string{
		"PGSSLMODE":            "bogus",
		"PGSSLROOTCERT":        "/nonexistent/root.crt",
		"PGSSLCERT":            "/nonexistent/client.crt",
		"PGCHANNELBINDING":     "require",
		"PGREQUIREAUTH":        "md5",
		"PGSERVICE":            "nonexistent-service",
		"PGPORT":               "abc",
		"PGCONNECT_TIMEOUT":    "x",
		"PGMINPROTOCOLVERSION": "3.2",
		"PGHOST":               "10.255.255.1",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	// Control: the same environment does fail a plain parse.
	if _, err := pgxpool.ParseConfig(""); err == nil {
		t.Fatal("control: the hostile environment parsed cleanly; the test proves nothing")
	}
	p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", AppName: "slpg"}, discard(), metrics())
	if err != nil {
		t.Fatalf("NewPostgres under a hostile libpq environment: %v", err)
	}
	defer p.Close()
	cc := p.pool.Config().ConnConfig
	if cc.TLSConfig != nil || len(cc.Fallbacks) != 0 || cc.Host != "127.0.0.1" || cc.Port != 1 {
		t.Errorf("TLS %v, %d fallbacks, %s:%d", cc.TLSConfig, len(cc.Fallbacks), cc.Host, cc.Port)
	}
	if cc.ChannelBinding != "" && cc.ChannelBinding != "prefer" && cc.ChannelBinding != "disable" {
		t.Errorf("channel binding %q reached the pool", cc.ChannelBinding)
	}
	for k, v := range env {
		if got, ok := os.LookupEnv(k); !ok || got != v {
			t.Errorf("%s was not restored: %q %v", k, got, ok)
		}
	}
}

func TestNewPostgresIgnoresTheLibpqEnvironment(t *testing.T) {
	t.Setenv("PGOPTIONS", "-c search_path=nowhere")
	t.Setenv("PGTARGETSESSIONATTRS", "standby")
	t.Setenv("PGAPPNAME", "from-env")
	t.Setenv("PGSSLMODE", "require")
	t.Setenv("PGHOST", "10.255.255.1,10.255.255.2")

	ctl, err := pgxpool.ParseConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if ctl.ConnConfig.TLSConfig == nil || len(ctl.ConnConfig.Fallbacks) == 0 {
		t.Fatalf("control: PGSSLMODE=require and a PGHOST list gave TLSConfig %v and %d fallbacks; "+
			"the environment did not reach the parser", ctl.ConnConfig.TLSConfig, len(ctl.ConnConfig.Fallbacks))
	}

	p, err := NewPostgres(PostgresConfig{Addr: "127.0.0.1:1", AppName: "sls"}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cc := p.pool.Config().ConnConfig
	if cc.ValidateConnect != nil {
		t.Error("ValidateConnect from PGTARGETSESSIONATTRS survived")
	}
	if cc.TLSConfig != nil {
		t.Error("PGSSLMODE=require reached the pool as a TLSConfig; the sink is plaintext only")
	}
	if len(cc.Fallbacks) != 0 {
		t.Errorf("%d fallbacks survived: a refused dial would be retried elsewhere or in another mode", len(cc.Fallbacks))
	}
	if cc.Host != "127.0.0.1" || cc.Port != 1 {
		t.Errorf("host %s port %d, want the configured 127.0.0.1:1 over PGHOST", cc.Host, cc.Port)
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
	literal := []byte(`{"message":"\\u0000"}`)
	if got := inboxPayload(literal); &got[0] != &literal[0] {
		t.Error("a payload with the literal text \\u0000 was copied")
	}
}

// FuzzInboxPayload is the real path (review finding 8): whatever the payload
// strings hold, the column copy is JSON without a NUL escape, raw comes back
// exact, and every other string equals the original with U+0000 -> U+FFFD.
func FuzzInboxPayload(f *testing.F) {
	for _, s := range []string{"", "a", "\x00", "a\x00b\x00", "\\u0000", "\u2028\x00", "\xff\x00"} {
		f.Add(s, s)
	}
	f.Fuzz(func(t *testing.T, msg, host string) {
		in, err := json.Marshal(map[string]any{
			"message": msg, "raw": msg, "size": len(msg), "host": map[string]string{"hostname": host},
			"sd": map[string]map[string]string{"a@1": {host: msg}},
		})
		if err != nil {
			t.Skip()
		}
		out := inboxPayload(in)
		if hasNULEscape(out) || !json.Valid(out) {
			t.Fatalf("inboxPayload(%s) = %s", in, out)
		}
		var orig, got struct {
			Message, Raw string
			RawEnc       string `json:"raw-encoding"`
			Host         struct{ Hostname string }
			SD           map[string]map[string]string
		}
		if err := json.Unmarshal(in, &orig); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatal(err)
		}
		repl := func(s string) string { return strings.ReplaceAll(s, "\x00", "\uFFFD") }
		raw := got.Raw
		if got.RawEnc == "base64" {
			b, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		if raw != orig.Raw || got.Message != repl(orig.Message) || got.Host.Hostname != repl(orig.Host.Hostname) {
			t.Fatalf("raw %q message %q host %q from %q", raw, got.Message, got.Host.Hostname, msg)
		}
		for k, v := range orig.SD["a@1"] {
			if got.SD["a@1"][repl(k)] != repl(v) {
				t.Fatalf("sd %v from %v", got.SD, orig.SD)
			}
		}
	})
}
