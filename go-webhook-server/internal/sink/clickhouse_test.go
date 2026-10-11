package sink

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

// SPEC §8.8.7, read back from the document itself: every code a row of the
// table lists maps to that row's class. A code moved in the code and not in
// the SPEC -- or the other way round -- fails here, which is what "the table
// IS the function" means.
func TestClassifyClickHouseCodeMatchesTheSpec(t *testing.T) {
	b, err := os.ReadFile("../../docs/SPEC.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	start := strings.Index(doc, "#### 8.8.7")
	if start < 0 {
		t.Fatal("SPEC §8.8.7 not found")
	}
	section := doc[start:]
	section = section[:strings.Index(section[4:], "\n####")+4]

	code := regexp.MustCompile("`(\\d+)`")
	classes := map[string]Class{"ErrNetwork": ClassNetwork, "ErrLogic": ClassLogic, "ErrFatal": ClassFatal}
	checked := 0
	for _, line := range strings.Split(section, "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 5 {
			continue
		}
		var want Class
		found := false
		for name, c := range classes {
			if strings.Contains(cells[3], name) {
				want, found = c, true
			}
		}
		if !found {
			continue
		}
		for _, m := range code.FindAllStringSubmatch(cells[1], -1) {
			n, _ := strconv.Atoi(m[1])
			if got := classifyClickHouseCode(int32(n)); got != want {
				t.Errorf("code %d: class %s, SPEC §8.8.7 says %s", n, got, want)
			}
			checked++
		}
	}
	if checked < 40 {
		t.Fatalf("read only %d codes from SPEC §8.8.7; the table changed shape", checked)
	}
}

// The lines of SPEC §8.8.7 that decide the most, spelled out so a reader of
// this file sees them without the SPEC.
func TestClassifyClickHouseCode(t *testing.T) {
	cases := []struct {
		code int32
		want Class
	}{
		{164, ClassFatal},    // READONLY: the user's profile, never a failover
		{242, ClassFatal},    // TABLE_IS_READ_ONLY: the decision 25006 took on PostgreSQL
		{241, ClassFatal},    // MEMORY_LIMIT_EXCEEDED: like Redis's OOM; the boot checks the limit
		{497, ClassFatal},    // ACCESS_DENIED
		{516, ClassFatal},    // AUTHENTICATION_FAILED
		{60, ClassFatal},     // UNKNOWN_TABLE: logic would drop a batch every few ms forever
		{16, ClassFatal},     // NO_SUCH_COLUMN_IN_TABLE
		{115, ClassFatal},    // UNKNOWN_SETTING: the server is older than the INSERT needs
		{117, ClassLogic},    // INCORRECT_DATA: invalid JSON, only from damage
		{469, ClassLogic},    // VIOLATED_CONSTRAINT
		{252, ClassNetwork},  // TOO_MANY_PARTS: slow down, do not drop
		{319, ClassNetwork},  // UNKNOWN_STATUS_OF_INSERT: the token makes the retry safe
		{999, ClassNetwork},  // KEEPER_EXCEPTION
		{209, ClassNetwork},  // SOCKET_TIMEOUT
		{4242, ClassNetwork}, // unknown: a filling ring is visible, a dropped batch is not
	}
	for _, tc := range cases {
		if got := classifyClickHouseCode(tc.code); got != tc.want {
			t.Errorf("code %d: %s, want %s", tc.code, got, tc.want)
		}
	}
}

// What is not a server exception: the context and the connection are
// network; a value the driver refuses to convert on the client is a column
// that changed type after the boot, and fatal.
func TestClassifyClickHouseErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"deadline", fmt.Errorf("send: %w", context.DeadlineExceeded), ClassNetwork},
		{"cancelled", context.Canceled, ClassNetwork},
		{"pool exhausted", clickhouse.ErrAcquireConnTimeout, ClassNetwork},
		{"refused dial", errors.New("dial tcp 127.0.0.1:9000: connect: connection refused"), ClassNetwork},
		{"exception", fmt.Errorf("prepare: %w", &clickhouse.Exception{Code: 60, Message: "no table"}), ClassFatal},
		{"logic exception", &clickhouse.Exception{Code: 117}, ClassLogic},
		{"converter", &column.ColumnConverterError{Op: "Append", To: "Int64", From: "[]uint64"}, ClassFatal},
		{"invalid batch", clickhouse.ErrBatchInvalid, ClassFatal},
	}
	for _, tc := range cases {
		err := classifyClickHouse("insert", tc.err)
		if got := ClassOf(err); got != tc.want {
			t.Errorf("%s: %s, want %s (%v)", tc.name, got, tc.want, err)
		}
		if !errors.Is(err, tc.err) {
			t.Errorf("%s: the classified error lost its cause", tc.name)
		}
	}
	if classifyClickHouse("insert", nil) != nil {
		t.Error("nil classified as an error")
	}
	var ex *clickhouse.Exception
	err := classifyClickHouse("insert", &clickhouse.Exception{Code: 252})
	if !errors.As(err, &ex) || !strings.Contains(err.Error(), "clickhouse=252") {
		t.Errorf("the code is not in the message or not reachable: %v", err)
	}
}

// SPEC §3.9 and §8.8.5: every column rule, the accepted and the refused.
func TestClickHouseColumnRules(t *testing.T) {
	ok := []struct{ name, typ, def string }{
		{"uuid", "UUID", ""},
		{"from_uuid", "UUID", ""},
		{"event_at", "DateTime64(6, 'UTC')", ""},
		{"event_at", "DateTime64(9, 'America/Sao_Paulo')", ""}, // the zone is display only
		{"event_at", "DateTime64(6)", ""},
		{"received_at", "DateTime64(6, 'UTC')", "DEFAULT"},
		{"received_at", "DateTime64(7)", "MATERIALIZED"},
		{"sid", "UInt64", ""},
		{"step", "UInt16", ""},
		{"level", "UInt8", ""},
		{"event_nsec", "UInt16", ""},
		{"space", "LowCardinality(String)", ""},
		{"space", "String", ""},
		{"type", "LowCardinality(String)", ""},
		{"payload", "JSON(params Map(String, String), query Map(String, Dynamic))", ""},
		{"payload", "JSON(max_dynamic_paths=64, params Map(String, String), query Map(String, Dynamic))", ""},
		{"payload", "JSON(max_dynamic_types=8, max_dynamic_paths=0, params Map(String, String), query Map(String, Dynamic))", ""},
	}
	for _, c := range ok {
		if p := chColumnProblem(c.name, chColumnInfo{typ: c.typ, defaultKind: c.def}); p != "" {
			t.Errorf("%s %s refused: %s", c.name, c.typ, p)
		}
	}
	bad := []struct{ name, typ, def, says string }{
		{"uuid", "String", "", "want UUID"},
		{"uuid", "Nullable(UUID)", "", "want UUID"},
		{"from_uuid", "FixedString(16)", "", "want UUID"},
		{"event_at", "DateTime64(3, 'UTC')", "", "at least 6"},
		{"event_at", "DateTime", "", "want DateTime64"},
		{"received_at", "DateTime64(6, 'UTC')", "", "no DEFAULT"},
		{"received_at", "DateTime64(6, 'UTC')", "ALIAS", "no DEFAULT"},
		{"received_at", "DateTime64(3)", "DEFAULT", "at least 6"},
		{"sid", "Int64", "", "want UInt64"},
		{"sid", "UInt32", "", "want UInt64"},
		{"step", "UInt32", "", "want UInt16"},
		{"level", "UInt16", "", "want UInt8"},
		{"event_nsec", "Int16", "", "want UInt16"},
		{"space", "FixedString(64)", "", "want String"},
		{"type", "Nullable(String)", "", "want String"},
		{"payload", "String", "", "drops empty objects"},
		{"payload", "JSON", "", "drops empty objects"},
		{"payload", "JSON(query Map(String, Dynamic))", "", `without "params Map(String, String)"`},
		{"payload", "JSON(params Map(String, String))", "", `without "query Map(String, Dynamic)"`},
		{"payload", "JSON(params Map(String, String), query Map(String, String))", "", "is not allowed"},
		{"payload", "JSON(params Map(String, String), query Map(String, Dynamic), SKIP content)", "", "SKIP"},
		{"payload", "JSON(content.length UInt8, params Map(String, String), query Map(String, Dynamic))", "", "is not allowed"},
		{"payload", "Object('json')", "", "want JSON("},
	}
	for _, c := range bad {
		p := chColumnProblem(c.name, chColumnInfo{typ: c.typ, defaultKind: c.def})
		if p == "" {
			t.Errorf("%s %s (%q) accepted", c.name, c.typ, c.def)
		} else if !strings.Contains(p, c.says) {
			t.Errorf("%s %s: %q, want it to say %q", c.name, c.typ, p, c.says)
		}
	}
	// A column this server does not write is the operator's.
	if p := chColumnProblem("tenant", chColumnInfo{typ: "Nullable(String)"}); p != "" {
		t.Errorf("an extra column refused: %s", p)
	}
}

// SPEC §8.8.5: engine, sorting key, partition key and CHECK constraints.
func TestClickHouseTableRules(t *testing.T) {
	const create = "CREATE TABLE db.inbox (`uuid` UUID) ENGINE = ReplacingMergeTree ORDER BY (event_at, uuid)"
	type tc struct{ engine, full, key, part, create, says string }
	ok := []tc{
		{"ReplacingMergeTree", "ReplacingMergeTree PARTITION BY toYYYYMM(event_at)", "event_at, uuid", "toYYYYMM(event_at)", create, ""},
		{"ReplicatedReplacingMergeTree", "ReplicatedReplacingMergeTree('/ch/{shard}/inbox', '{replica}')", "event_at, uuid", "", create, ""},
		{"ReplicatedReplacingMergeTree", "ReplicatedReplacingMergeTree('/p', 'r', received_at)", "event_at, uuid", "", create, ""},
		{"ReplacingMergeTree", "ReplacingMergeTree(received_at)", "uuid", "toStartOfHour(event_at)", create, ""},
		{"SharedReplacingMergeTree", "SharedReplacingMergeTree", "uuid", "", create, ""},
		{"ReplacingMergeTree", "ReplacingMergeTree", "toStartOfHour(event_at), uuid", "`event_at`", create, ""},
		{"ReplacingMergeTree", "ReplacingMergeTree", "space, type, event_at, `uuid`", "", create, ""},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "", create + " CONSTRAINT a ASSUME level < 16", ""},
	}
	for _, c := range ok {
		if p := chTableProblem(c.engine, c.full, c.key, c.part, c.create); p != "" {
			t.Errorf("%s / %s / ORDER BY (%s) / PARTITION BY %s refused: %s", c.engine, c.full, c.key, c.part, p)
		}
	}
	bad := []tc{
		{"MergeTree", "MergeTree", "event_at, uuid", "", create, "redelivery"},
		{"Distributed", "Distributed(c, db, t)", "", "", create, "asynchronously"},
		{"Memory", "Memory", "", "", create, "want ReplacingMergeTree"},
		{"ReplacingMergeTree", "ReplacingMergeTree(event_at, level)", "event_at, uuid", "", create, "is_deleted"},
		{"ReplicatedReplacingMergeTree", "ReplicatedReplacingMergeTree('/p', 'r', ver, is_deleted)", "event_at, uuid", "", create, "is_deleted"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at", "", create, "without uuid"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, toString(uuid)", "", create, "without uuid"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "cityHash64(uuid, event_at)", "", create, "without uuid"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "toYYYYMMDD(received_at)", create, "received_at"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "cityHash64(uuid) % 512", create, "uuid"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "(toYYYYMM(event_at), space)", create, "space"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "", "CREATE TABLE t (x UInt8, CONSTRAINT lvl CHECK level < 16) ENGINE = ReplacingMergeTree", "CHECK"},
		{"ReplacingMergeTree", "ReplacingMergeTree", "event_at, uuid", "", "CREATE TABLE t (x UInt8, CONSTRAINT `lvl ok` CHECK level < 16) ENGINE = ReplacingMergeTree", "CHECK"},
	}
	for _, c := range bad {
		p := chTableProblem(c.engine, c.full, c.key, c.part, c.create)
		if p == "" {
			t.Errorf("%s / %s / ORDER BY (%s) / PARTITION BY %s accepted", c.engine, c.full, c.key, c.part)
		} else if !strings.Contains(p, c.says) {
			t.Errorf("%s / ORDER BY (%s) / PARTITION BY %s: %q, want it to say %q", c.full, c.key, c.part, p, c.says)
		}
	}
}

// SPEC §8.8.5: a column the server writes is a plain column. EPHEMERAL
// stores nothing -- the value vanished with Write returning nil -- and
// MATERIALIZED and ALIAS refuse the INSERT (independent review of 2026-10-07).
func TestClickHouseColumnKinds(t *testing.T) {
	for _, kind := range []string{"EPHEMERAL", "MATERIALIZED", "ALIAS"} {
		for _, name := range []string{"uuid", "sid", "space", "payload"} {
			typ := map[string]string{"uuid": "UUID", "sid": "UInt64", "space": "String", "payload": "JSON"}[name]
			if p := chColumnProblem(name, chColumnInfo{typ: typ, defaultKind: kind}); !strings.Contains(p, kind) {
				t.Errorf("%s %s accepted or refused for another reason: %q", name, kind, p)
			}
		}
	}
	if p := chColumnProblem("sid", chColumnInfo{typ: "UInt64", defaultKind: "DEFAULT"}); p != "" {
		t.Errorf("a DEFAULT on a written column refused: %s", p)
	}
}

func TestSplitTopLevel(t *testing.T) {
	got := splitTopLevel("max_dynamic_paths=1, params Map(String, String),query Map(String, Dynamic)")
	want := []string{"max_dynamic_paths=1", "params Map(String, String)", "query Map(String, Dynamic)"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestClickHouseVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"25.8.33.6": true, "25.8": true, "25.10.1.1": true, "26.1.2.3": true,
		"25.7.9.9": false, "24.12.1": false, "": false, "x.y": false, "25": false,
	} {
		if got := chVersionAtLeast(v, chMinVersion); got != want {
			t.Errorf("version %q: %v, want %v", v, got, want)
		}
	}
}

// SPEC §8.8.5: the memory limits against the worst batch. Zero is unlimited
// in ClickHouse, and a zero batch skips the check. The per-user limit holds for
// every query of the user at once: it must take one batch per pooled
// connection.
func TestClickHouseMemoryRule(t *testing.T) {
	const batch = 512 << 20
	need := chMemoryNeed(batch)
	if need != 128<<20+8*512<<20 {
		t.Fatalf("need = %d", need)
	}
	if p := chMemoryProblem(batch, 5, 0, 0); p != "" {
		t.Errorf("unlimited refused: %s", p)
	}
	if p := chMemoryProblem(batch, 5, need, 5*need); p != "" {
		t.Errorf("exactly enough refused: %s", p)
	}
	if p := chMemoryProblem(batch, 5, need-1, 0); !strings.Contains(p, "max_memory_usage=") ||
		!strings.Contains(p, strconv.FormatUint(need, 10)) {
		t.Errorf("one byte short = %q, want the numbers", p)
	}
	if p := chMemoryProblem(batch, 5, 0, 5*need-1); !strings.Contains(p, "max_memory_usage_for_user=") ||
		!strings.Contains(p, "5 concurrent") {
		t.Errorf("a per-user limit for fewer than the pool's INSERTs = %q", p)
	}
	if p := chMemoryProblem(batch, 0, 0, need); p != "" {
		t.Errorf("a pool of 0 counts as one INSERT: %s", p)
	}
	if p := chMemoryProblem(0, 5, 1, 1); p != "" {
		t.Errorf("a zero batch was checked: %s", p)
	}
}

// SPEC §8.8.4: the token is a function of the batch's uuids, in order. The
// same batch repeats it, and any other batch -- one item more, one less, the
// same items in another order -- gets another; a constant or a counter would
// make the server drop a different batch in silence.
func TestDedupToken(t *testing.T) {
	ids := func(items ...string) [][16]byte {
		out := make([][16]byte, len(items))
		for i, it := range items {
			u, ok := parseUUID(it)
			if !ok {
				t.Fatalf("not a uuid: %s", it)
			}
			out[i] = u
		}
		return out
	}
	a := "01962a3f-07bc-70b4-ae05-c57e7541d8a8"
	b := "01962a3f-07bc-70b4-ae05-c57e7541d8a9"
	c := "01962a3f-07bc-70b4-ae05-c57e7541d8aa"
	tok := dedupToken(ids(a, b))
	if !regexp.MustCompile(`^whs-[0-9a-f]{32}$`).MatchString(tok) {
		t.Fatalf("token %q", tok)
	}
	if dedupToken(ids(a, b)) != tok {
		t.Error("the same batch got another token")
	}
	for name, other := range map[string][][16]byte{
		"reordered": ids(b, a), "one more": ids(a, b, c), "one less": ids(a), "other": ids(c),
	} {
		if dedupToken(other) == tok {
			t.Errorf("%s batch shares the token", name)
		}
	}
}

// The columns go back to the pool holding nothing: payload references the
// items' buffers (SPEC §8.5.4, §8.8.3).
func TestChColumnsResetDropsReferences(t *testing.T) {
	var a chColumns
	for _, it := range mkBatch(5) {
		r, err := inboxRowOf(it)
		if err != nil {
			t.Fatal(err)
		}
		a.add(r)
	}
	payload, uuids := a.payload[:cap(a.payload)], a.uuid[:cap(a.uuid)]
	a.reset()
	if len(a.payload) != 0 || len(a.ids) != 0 || len(a.uuid) != 0 || len(a.eventAt) != 0 {
		t.Fatal("reset left rows")
	}
	for i := range payload {
		if payload[i] != nil {
			t.Fatalf("payload slot %d still references a buffer", i)
		}
	}
	for i := range uuids {
		if uuids[i] != "" {
			t.Fatalf("uuid slot %d still set", i)
		}
	}
}

// The text form the UUID column parses is the canonical one the envelope
// carries.
func TestUUIDText(t *testing.T) {
	for _, it := range mkBatch(20) {
		r, err := inboxRowOf(it)
		if err != nil {
			t.Fatal(err)
		}
		if got := uuidText(r.UUID); got != it.ID {
			t.Fatalf("uuidText = %s, want %s", got, it.ID)
		}
	}
}

// A misconfigured sink is refused before anything dials.
func TestNewClickHouseRefusesBadConfig(t *testing.T) {
	base := ClickHouseConfig{Addrs: []string{"127.0.0.1:9000"}, Database: "db", Table: "inbox"}
	for name, mod := range map[string]func(*ClickHouseConfig){
		"no address":    func(c *ClickHouseConfig) { c.Addrs = nil },
		"injection":     func(c *ClickHouseConfig) { c.Table = "inbox` DROP" },
		"leading digit": func(c *ClickHouseConfig) { c.Table = "1inbox" },
		"no database":   func(c *ClickHouseConfig) { c.Database = "" },
		"bad ca":        func(c *ClickHouseConfig) { c.TLS, c.TLSCA = true, "/nonexistent/ca.pem" },
	} {
		cfg := base
		mod(&cfg)
		if _, err := NewClickHouse(cfg, quietLog(), testMetrics()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:9000"}, Database: "db",
		Table: "other.inbox"}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.insert != "INSERT INTO `other`.`inbox` (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload)" {
		t.Errorf("insert = %s", c.insert)
	}
	if c.Type() != "clickhouse" {
		t.Errorf("Type = %s", c.Type())
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := c.Write(context.Background(), mkBatch(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v", err)
	}
	if err := c.Health(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v", err)
	}
}
