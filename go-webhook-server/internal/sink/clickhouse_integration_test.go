package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
)

// The destination is remote and has a schema, and what is worth proving --
// the columnar INSERT, the JSON column with its two hints, DateTime64 in UTC,
// the deduplication token and the merge behind it, the catalogue queries and
// the empty probe of the boot validation -- is exactly what a fake would have
// to invent. These run against a real server addressed by
// WHS_TEST_CLICKHOUSE_DSN (the native protocol) and skip without it
// (docs/TESTING.md §5.5):
//
//	WHS_TEST_CLICKHOUSE_DSN='clickhouse://whs:whs@127.0.0.1:19000/whs_test' \
//	  go test -race ./internal/sink/
//
// Every test creates its own table from docker/clickhouse/inbox.sql under a
// unique name and drops it afterwards. The privilege tests also create users,
// which needs access management (CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 in the
// official image); they skip where the test user lacks it.

const clickhouseDSNEnv = "WHS_TEST_CLICKHOUSE_DSN"

type chFixture struct {
	t     *testing.T
	admin driver.Conn
	opts  *clickhouse.Options
	table string
	cfg   ClickHouseConfig
}

func chSetup(t *testing.T) *chFixture {
	t.Helper()
	dsn := os.Getenv(clickhouseDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the clickhouse sink tests (see docs/TESTING.md §5.5)", clickhouseDSNEnv)
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %v", clickhouseDSNEnv, err)
	}
	admin, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("%s is not reachable: %v", clickhouseDSNEnv, err)
	}
	t.Cleanup(func() { admin.Close() })

	f := &chFixture{t: t, admin: admin, opts: opts, cfg: ClickHouseConfig{
		Addrs:    opts.Addr,
		Username: opts.Auth.Username,
		Password: opts.Auth.Password,
		Database: opts.Auth.Database,
		PoolSize: 4,
		AppName:  "whs-test",
	}}
	f.table = f.newTable()
	f.cfg.Table = f.table
	return f
}

// newTable creates a table from the canonical DDL, with each edit applied to
// the DDL text first (old => new), and registers its removal.
func (f *chFixture) newTable(edits ...string) string {
	f.t.Helper()
	name := fmt.Sprintf("whs_inbox_%d", time.Now().UnixNano())
	ddl := chDDLFor(f.t, name)
	for i := 0; i+1 < len(edits); i += 2 {
		if !strings.Contains(ddl, edits[i]) {
			f.t.Fatalf("the DDL has no %q to edit", edits[i])
		}
		ddl = strings.Replace(ddl, edits[i], edits[i+1], 1)
	}
	f.exec(ddl)
	f.t.Cleanup(func() {
		_ = f.admin.Exec(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
	})
	return name
}

// chDDLFor reads docker/clickhouse/inbox.sql -- the file operators apply --
// and retargets it at a test table. Using the real file is the point: the
// validation of SPEC §8.8.5 and the table the operator gets must agree.
func chDDLFor(t *testing.T, table string) string {
	t.Helper()
	b, err := os.ReadFile("../../docker/clickhouse/inbox.sql")
	if err != nil {
		t.Fatalf("reading the canonical DDL: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	s := strings.Join(lines, "\n")
	s = s[:strings.LastIndexByte(s, ';')]
	const head = "CREATE TABLE IF NOT EXISTS inbox ("
	if !strings.Contains(s, head) {
		t.Fatal("docker/clickhouse/inbox.sql changed shape; the fixture cannot retarget it")
	}
	return strings.Replace(s, head, "CREATE TABLE `"+table+"` (", 1)
}

func (f *chFixture) exec(sql string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f.admin.Exec(ctx, sql); err != nil {
		f.t.Fatalf("%v\n%s", err, sql)
	}
}

func (f *chFixture) count(table string, final bool) int {
	f.t.Helper()
	q := "SELECT count() FROM `" + table + "`"
	if final {
		q += " FINAL"
	}
	var n uint64
	if err := f.admin.QueryRow(context.Background(), q).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return int(n)
}

func (f *chFixture) sink(cfg ClickHouseConfig) *ClickHouse {
	f.t.Helper()
	c, err := NewClickHouse(cfg, quietLog(), testMetrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { c.Close() })
	return c
}

// ready runs the boot check the way WaitReady would, and fails the test if
// the table is refused.
func (f *chFixture) ready(c *ClickHouse) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

// chReadSettings is how a consumer reads the payload back: the escaped dots of
// SPEC §3.9 unescaped, and 64-bit integers as numbers, not strings.
var chReadSettings = clickhouse.Settings{
	"json_type_escape_dots_in_keys":           1,
	"output_format_json_quote_64bit_integers": 0,
}

// rowJLH rebuilds the envelope of one inbox row (SPEC §3.9) the way a
// consumer of the mirror does, and returns how long ago received_at is,
// against the server's clock.
func (f *chFixture) rowJLH(table, id string) ([]byte, time.Duration) {
	f.t.Helper()
	var (
		uuid, from, space, typ, payload string
		level                           uint8
		sid                             uint64
		step, nsec                      uint16
		eventAt                         time.Time
		lagMicros                       int64
	)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(chReadSettings))
	err := f.admin.QueryRow(ctx, "SELECT toString(uuid), toString(from_uuid), toString(space), "+
		"toString(type), level, sid, step, event_at, event_nsec, toString(payload), "+
		"dateDiff('microsecond', received_at, now64(6)) FROM `"+table+"` WHERE uuid = ?", id).
		Scan(&uuid, &from, &space, &typ, &level, &sid, &step, &eventAt, &nsec, &payload, &lagMicros)
	if err != nil {
		f.t.Fatalf("reading %s back: %v", id, err)
	}
	return jlhFromColumns(uuid, from, space, typ, int64(level), int64(sid), int64(step),
		eventAt, int64(nsec), []byte(payload)), time.Duration(lagMicros) * time.Microsecond
}

// ------------------------------------------------------------------ writes

// SPEC §3.9 and §8.8.3: every column lands from the JLH header, event_at is
// the event in UTC to the microsecond and event_nsec the rest, and the JSON
// column gives back a payload equal to the JLH's as a JSON value -- params
// and query empty or not, keys with dots, the empty key, a NUL, a binary body
// -- so the envelope rebuilt from the row passes the oracle.
func TestClickHouseWriteStoresTheEnvelope(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)

	nul := testRequest("/nul", []byte(`{"n":1}`))
	nul.RawQuery = "a=x%00y&b=2"
	noQuery := testRequest("/no-query", nil)
	noQuery.RawQuery = ""
	// The query keys are the client's: dots, a literal %2E, the empty key and
	// a repeated one -- every shape that broke the JSON type without the Map
	// hint (SPEC §3.9).
	dotted := testRequest("/dotted", []byte("text"))
	dotted.RawQuery = "a.b=1&a=2&a%252Eb=3&=e&k=x&k=y&.=d&a..b=4"
	invalidUTF8 := testRequest("/hook/%FF%FE", nil)
	invalidUTF8.RawQuery = "q=%FF\xc0\xaf"
	items := []*envelope.Item{
		mkItem("plain"),
		itemIn("default", nul),
		itemIn("default", noQuery),
		itemIn("default", dotted),
		itemIn("default", testRequest("/bin", []byte("A\x00B\nC\xffD\r\x00"))),
		itemIn("acme.store", testRequest("/other-space", nil)),
		itemIn("default", invalidUTF8),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Write(ctx, items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := f.count(f.table, false); n != len(items) {
		t.Fatalf("%d rows, want %d", n, len(items))
	}

	for i, it := range items {
		rebuilt, lag := f.rowJLH(f.table, it.ID)
		e := validJLH(t, rebuilt)
		if !sameJSON(t, rebuilt, it.Env) {
			t.Errorf("envelope %d rebuilt from the row:\n%s\nwant the JLH:\n%s", i, rebuilt, it.Env)
		}
		if e.UUID != it.ID {
			t.Errorf("envelope %d: uuid %s, want %s", i, e.UUID, it.ID)
		}
		if lag < 0 || lag > time.Minute {
			t.Errorf("envelope %d: received_at is %v away from now64(); not UTC?", i, lag)
		}
	}

	// The NUL survives (SPEC §3.9): no replacement, unlike jsonb.
	var doc struct {
		Payload struct {
			Query  map[string]any
			Params map[string]any
		}
	}
	rebuilt, _ := f.rowJLH(f.table, items[1].ID)
	if err := json.Unmarshal(rebuilt, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Payload.Query["a"] != "x\x00y" {
		t.Errorf("query.a = %q, want the NUL kept", doc.Payload.Query["a"])
	}
	// The empty objects the JSON type would drop.
	rebuilt, _ = f.rowJLH(f.table, items[2].ID)
	if !bytes.Contains(rebuilt, []byte(`"params":{}`)) || !bytes.Contains(rebuilt, []byte(`"query":{}`)) {
		t.Errorf("params and query must come back as {}:\n%s", rebuilt)
	}
	// The binary body arrives exact, in base64 inside the payload.
	rebuilt, _ = f.rowJLH(f.table, items[4].ID)
	body, err := validJLH(t, rebuilt).Payload.Body()
	if err != nil || !bytes.Equal(body, []byte("A\x00B\nC\xffD\r\x00")) {
		t.Errorf("body = %q (%v), want the bytes that arrived", body, err)
	}
	// The client's keys are Map keys: literal, and still a subcolumn query.
	var v string
	if err := f.admin.QueryRow(context.Background(), "SELECT payload.query['a.b']::String FROM `"+
		f.table+"` WHERE uuid = ?", items[3].ID).Scan(&v); err != nil || v != "1" {
		t.Errorf("payload.query['a.b'] = %q (%v), want 1", v, err)
	}
}

// SPEC §3.9 and §8.8.3: a string is never read as anything but a string. With
// the server's defaults the JSON type infers dates from strings, and once a
// path holds a date variant in a block it reads the strings of OTHER rows
// through it: the victim's empty body and content type came back as
// "1970-01-01 00:00:00.000000000", its query values as dates (independent
// review of 2026-10-07). The attacker and the victim share one INSERT.
func TestClickHouseStringsAreNeverInferred(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)

	attacker := testRequest("/attack", []byte("2020-01-01 10:00:00.000"))
	attacker.RawQuery = "t=2020-01-01%2010:00:00.000&d=2020-01-01T10:00:00&bad=2020-02-30"
	attacker.ContentType = "2020-01-01 10:00:00.000"
	attacker.Header = http.Header{"Content-Type": {attacker.ContentType}, "X-Ts": {"2026-10-07 12:00:00.000"}}
	victim := testRequest("/victim", nil)
	victim.RawQuery = "flag&id=1700000000&n=12345&neg=-1&lead=0012"
	victim.ContentType = ""
	victim.Header = http.Header{"X-Slack-Request-Timestamp": {"1531420618"}, "X-Empty": {""}}
	items := []*envelope.Item{itemIn("default", attacker), itemIn("default", victim)}
	if err := c.Write(context.Background(), items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for i, it := range items {
		rebuilt, _ := f.rowJLH(f.table, it.ID)
		validJLH(t, rebuilt)
		if !sameJSON(t, rebuilt, it.Env) {
			t.Errorf("envelope %d changed in the JSON column:\n%s\nwant:\n%s", i, rebuilt, it.Env)
		}
	}
}

// SPEC §3.2 and §8.8.3: the largest envelope this server can accept -- a body
// of max_body_bytes that is not UTF-8, so it travels in base64 -- goes in one
// INSERT and comes back byte for byte. The native protocol has no packet
// ceiling to check against; this is the proof that none bites.
func TestClickHouseLargestEnvelope(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)

	body := bytes.Repeat([]byte{0xff, 0x00, 'x'}, (5<<20)/3)
	big := itemIn("default", testRequest("/big", body))
	if len(big.Env) < 6<<20 {
		t.Fatalf("the envelope has %d bytes; the fixture is not the worst case", len(big.Env))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Write(ctx, []*envelope.Item{big, mkItem("small")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	rebuilt, _ := f.rowJLH(f.table, big.ID)
	got, err := validJLH(t, rebuilt).Payload.Body()
	if err != nil || !bytes.Equal(got, body) {
		t.Errorf("the largest body came back with %d bytes (%v), want %d exact", len(got), err, len(body))
	}
}

// SPEC §8.8.4: the pipeline is at-least-once. The SAME batch written twice is
// a worker retry: the token repeats and the server drops the block, counted
// in whs_sink_clickhouse_dedup_blocks_total. A batch grouped differently -- a
// dump reingested, a ring.db re-read -- gets another token and is written,
// and the ReplacingMergeTree collapses it by (event_at, uuid).
func TestClickHouseRedeliveryIsDeduplicated(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)
	ctx := context.Background()

	items := mkBatch(7)
	if err := c.Write(ctx, items); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := c.Write(ctx, items); err != nil {
		t.Fatalf("retried Write: %v", err)
	}
	if n := f.count(f.table, false); n != 7 {
		t.Errorf("%d rows after the retry, want 7: the token must drop the repeated block", n)
	}
	if got := c.m.SinkClickHouseDedupBlocksTotal.Load(); got != 1 {
		t.Errorf("dedup counter = %d, want 1", got)
	}

	regrouped := append(items[:3:3], mkBatch(2)...)
	if err := c.Write(ctx, regrouped); err != nil {
		t.Fatalf("regrouped Write: %v", err)
	}
	if n := f.count(f.table, true); n != 9 {
		t.Errorf("%d rows with FINAL, want 9: the merge key must collapse the redelivered uuids", n)
	}
	f.exec("OPTIMIZE TABLE `" + f.table + "` FINAL")
	if n := f.count(f.table, false); n != 9 {
		t.Errorf("%d rows after the merge, want 9", n)
	}
}

// An item whose JLH this server did not write is refused as data (logic)
// before the INSERT leaves: nothing of the batch reaches the table.
func TestClickHouseRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)
	ctx := context.Background()

	err := c.Write(ctx, []*envelope.Item{mkItem("good"), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := f.count(f.table, false); n != 0 {
		t.Errorf("%d rows; a refused batch must send nothing", n)
	}
}

// SPEC §8.8.7: a payload that is not JSON passes envelope.ParseHead and only
// the server notices: 117, logic, and the next batch is taken.
func TestClickHouseInvalidJSONIsLogic(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)
	ctx := context.Background()

	err := c.Write(ctx, []*envelope.Item{malformedPayloadItem(t)})
	if err == nil || ClassOf(err) != ClassLogic || !strings.Contains(err.Error(), "clickhouse=117") {
		t.Errorf("a payload that is not JSON = %v (class %s), want logic 117", err, ClassOf(err))
	}
	if err := c.Write(ctx, mkBatch(2)); err != nil {
		t.Errorf("control: valid items refused after the refusal: %v", err)
	}
	if n := f.count(f.table, false); n != 2 {
		t.Errorf("%d rows, want the 2 of the control", n)
	}
}

// Many workers, one table: the pool serves them all, and the gauges reflect
// the configured ceiling after a health check.
func TestClickHouseConcurrentWritersAndPoolGauges(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)

	const writers, batches, per = 4, 5, 20
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func() {
			for b := 0; b < batches; b++ {
				if err := c.Write(context.Background(), mkBatch(per)); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Fatalf("writer: %v", err)
		}
	}
	if n := f.count(f.table, false); n != writers*batches*per {
		t.Errorf("%d rows, want %d", n, writers*batches*per)
	}
	f.ready(c)
	if got := c.m.SinkClickHousePoolConns[2].Load(); got != int64(f.cfg.PoolSize) {
		t.Errorf("pool max gauge = %d, want %d", got, f.cfg.PoolSize)
	}
}

// ------------------------------------------------------------- validation

// SPEC §8.8.5: each refusal is FATAL, so the boot stops at once instead of
// waiting out sink.boot_timeout on a table that will not change. The accepted
// cases are the control: they write after validating, so a rule that refused
// everything would fail them rather than pass the refusals for the wrong
// reason.
func TestClickHouseHealthValidatesTheSchema(t *testing.T) {
	f := chSetup(t)
	const (
		payload = "payload     JSON(params Map(String, String), query Map(String, Dynamic))"
		engine  = "ENGINE = ReplacingMergeTree"
		orderBy = "ORDER BY (event_at, uuid)"
	)
	accepted := []struct {
		name  string
		edits []string
	}{
		{"canonical", nil},
		{"plain String space and type", []string{"space       LowCardinality(String)", "space String",
			"type        LowCardinality(String)", "type String"}},
		{"nanosecond event_at in another zone", []string{"event_at    DateTime64(6, 'UTC')",
			"event_at DateTime64(9, 'America/Sao_Paulo')"}},
		{"path limits", []string{payload,
			"payload JSON(max_dynamic_paths=64, params Map(String, String), query Map(String, Dynamic))"}},
		{"hour bucket in the key", []string{orderBy, "ORDER BY (toStartOfHour(event_at), uuid)"}},
		{"hour partitions and a version column", []string{engine, engine + "(received_at)",
			"PARTITION BY toYYYYMM(event_at)", "PARTITION BY toStartOfHour(event_at)"}},
		{"no received_at", []string{"received_at DateTime64(6, 'UTC') DEFAULT now64(6),", ""}},
		{"extra column with a default", []string{"  sid ", "  tenant String DEFAULT 'x',\n  sid "}},
	}
	for _, tc := range accepted {
		t.Run("accepts "+tc.name, func(t *testing.T) {
			cfg := f.cfg
			cfg.Table = f.newTable(tc.edits...)
			c := f.sink(cfg)
			f.ready(c)
			if err := c.Write(context.Background(), mkBatch(3)); err != nil {
				t.Fatalf("Write after an accepted validation: %v", err)
			}
			if n := f.count(cfg.Table, false); n != 3 {
				t.Errorf("%d rows, want 3", n)
			}
		})
	}

	refused := []struct {
		name  string
		edits []string // nil with missing set: no table at all
		says  string
	}{
		{"missing table", nil, "does not exist"},
		{"plain MergeTree", []string{engine, "ENGINE = MergeTree"}, "want ReplacingMergeTree"},
		{"key without uuid", []string{orderBy, "ORDER BY (event_at)"}, "without uuid"},
		{"uuid as String", []string{"uuid        UUID,", "uuid String,"}, `column "uuid"`},
		{"from_uuid as String", []string{"from_uuid   UUID,", "from_uuid String,"}, `column "from_uuid"`},
		{"millisecond event_at", []string{"event_at    DateTime64(6, 'UTC')", "event_at DateTime64(3, 'UTC')"}, "at least 6"},
		{"signed sid", []string{"sid         UInt64", "sid Int64"}, `column "sid"`},
		{"wide step", []string{"step        UInt16", "step UInt32"}, `column "step"`},
		{"wide level", []string{"level       UInt8", "level UInt16"}, `column "level"`},
		{"signed event_nsec", []string{"event_nsec  UInt16", "event_nsec Int16"}, `column "event_nsec"`},
		{"nullable space", []string{"space       LowCardinality(String)", "space Nullable(String)"}, `column "space"`},
		{"payload without hints", []string{payload, "payload JSON"}, "drops empty objects"},
		{"payload with SKIP", []string{payload,
			"payload JSON(params Map(String, String), query Map(String, Dynamic), SKIP content)"}, "SKIP"},
		{"payload as String", []string{payload, "payload String"}, `column "payload"`},
		{"missing column", []string{"  event_nsec  UInt16,\n", ""}, `no column "event_nsec"`},
		{"received_at without default", []string{"received_at DateTime64(6, 'UTC') DEFAULT now64(6)",
			"received_at DateTime64(6, 'UTC')"}, "no DEFAULT"},
		{"check constraint", []string{payload, payload + ",\n  CONSTRAINT lvl CHECK level < 16"}, "CHECK"},
		// Independent review of 2026-10-07: each of these was accepted and
		// then lost or refused every batch.
		{"check constraint with a quoted name", []string{payload, payload + ",\n  CONSTRAINT `lvl ok` CHECK level < 16"}, "CHECK"},
		{"ephemeral payload", []string{payload, payload + " EPHEMERAL"}, "EPHEMERAL"},
		{"materialized sid", []string{"sid         UInt64", "sid UInt64 MATERIALIZED 0"}, "MATERIALIZED"},
		{"is_deleted", []string{engine, engine + "(event_at, level)"}, "is_deleted"},
		{"partition by arrival", []string{"PARTITION BY toYYYYMM(event_at)", "PARTITION BY toYYYYMMDD(received_at)"}, "received_at"},
		{"partition by uuid", []string{"PARTITION BY toYYYYMM(event_at)", "PARTITION BY cityHash64(uuid) % 512"}, "uuid"},
	}
	for _, tc := range refused {
		t.Run("refuses "+tc.name, func(t *testing.T) {
			cfg := f.cfg
			if tc.edits == nil {
				cfg.Table = fmt.Sprintf("whs_missing_%d", time.Now().UnixNano())
			} else {
				cfg.Table = f.newTable(tc.edits...)
			}
			c := f.sink(cfg)
			err := c.Health(context.Background())
			if err == nil || ClassOf(err) != ClassFatal {
				t.Fatalf("Health = %v (class %s), want fatal", err, ClassOf(err))
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("Health = %v, want it to say %q", err, tc.says)
			}
			if tc.edits != nil {
				if n := f.count(cfg.Table, false); n != 0 {
					t.Errorf("the refused table gained %d rows", n)
				}
			}
		})
	}
}

// chUser creates a user for one test, or skips where the test user cannot.
func (f *chFixture) chUser(name, extra string, grants ...string) ClickHouseConfig {
	f.t.Helper()
	user := fmt.Sprintf("whs_%s_%d", name, time.Now().UnixNano())
	ctx := context.Background()
	if err := f.admin.Exec(ctx, "CREATE USER "+user+" IDENTIFIED BY 'pw' "+extra); err != nil {
		f.t.Skipf("cannot CREATE USER (needs access management): %v", err)
	}
	f.t.Cleanup(func() { _ = f.admin.Exec(context.Background(), "DROP USER IF EXISTS "+user) })
	for _, g := range grants {
		f.exec(strings.ReplaceAll(strings.ReplaceAll(g, "$U", user), "$T", "`"+f.cfg.Database+"`.`"+f.table+"`"))
	}
	cfg := f.cfg
	cfg.Username, cfg.Password = user, "pw"
	return cfg
}

// SPEC §8.8.5 and §8.8.8: the empty probe proves the privileges without a row
// left behind -- a user with SELECT only is refused by name (497); a readonly
// profile is refused by the health check before the probe; INSERT alone is
// enough, and writes.
func TestClickHouseHealthValidatesThePrivilege(t *testing.T) {
	f := chSetup(t)

	t.Run("select only", func(t *testing.T) {
		cfg := f.chUser("sel", "", "GRANT SELECT ON $T TO $U")
		err := f.sink(cfg).Health(context.Background())
		if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "lacks INSERT") {
			t.Fatalf("Health = %v (class %s), want fatal naming INSERT", err, ClassOf(err))
		}
	})
	t.Run("readonly profile", func(t *testing.T) {
		cfg := f.chUser("ro", "SETTINGS readonly = 1", "GRANT INSERT, SELECT ON $T TO $U")
		err := f.sink(cfg).Health(context.Background())
		if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "readonly=1") {
			t.Fatalf("Health = %v (class %s), want fatal naming readonly", err, ClassOf(err))
		}
	})
	t.Run("insert only", func(t *testing.T) {
		cfg := f.chUser("ins", "", "GRANT INSERT ON $T TO $U")
		c := f.sink(cfg)
		f.ready(c)
		if err := c.Write(context.Background(), mkBatch(2)); err != nil {
			t.Fatalf("Write with INSERT alone: %v", err)
		}
		if n := f.count(f.table, false); n != 2 {
			t.Errorf("%d rows, want 2; the probe must leave none", n)
		}
	})
	t.Run("wrong password", func(t *testing.T) {
		cfg := f.cfg
		cfg.Password = "not-the-password"
		err := f.sink(cfg).Health(context.Background())
		if err == nil || ClassOf(err) != ClassFatal {
			t.Fatalf("Health = %v (class %s), want fatal", err, ClassOf(err))
		}
	})
}

// SPEC §8.8.5: a session memory limit below what the worst batch costs the
// server is refused at boot with the numbers; MEMORY_LIMIT_EXCEEDED would be
// fatal at run time. Without a worst batch the check is skipped.
func TestClickHouseMemoryLimitIsCheckedAtBoot(t *testing.T) {
	f := chSetup(t)
	cfg := f.chUser("mem", "SETTINGS max_memory_usage = 1000000000", "GRANT INSERT ON $T TO $U")

	cfg.MaxBatchBytes = 512 << 20
	err := f.sink(cfg).Health(context.Background())
	if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "max_memory_usage=1000000000") {
		t.Fatalf("Health = %v (class %s), want fatal naming the limit", err, ClassOf(err))
	}
	cfg.MaxBatchBytes = 64 << 20
	f.ready(f.sink(cfg))
}

// SPEC §8.8.5: the per-user limit holds for every INSERT of the user at once;
// four concurrent legitimate batches broke one the single-batch comparison had
// accepted, and 241 is fatal (independent review of 2026-10-07).
func TestClickHouseUserMemoryLimitCountsThePool(t *testing.T) {
	f := chSetup(t)
	cfg := f.chUser("memu", "SETTINGS max_memory_usage_for_user = 2000000000", "GRANT INSERT ON $T TO $U")
	cfg.MaxBatchBytes, cfg.PoolSize = 64<<20, 4 // 640 MiB each, 2.5 GiB for the pool
	err := f.sink(cfg).Health(context.Background())
	if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "4 concurrent") {
		t.Fatalf("Health = %v (class %s), want fatal naming the pool", err, ClassOf(err))
	}
	cfg.PoolSize = 2
	f.ready(f.sink(cfg))
}

// SPEC §8.8.5 and §8.8.7: the probe runs with the drain's deadline, so the
// max_execution_time the driver derives from it is the drain's too. A profile
// that forbids changing it passed a probe sent under the 1 s health deadline
// and refused every batch with 452, retried forever as network.
func TestClickHouseProbeCarriesTheDrainsDeadline(t *testing.T) {
	f := chSetup(t)
	cfg := f.chUser("met", "SETTINGS max_execution_time = 30 CONST", "GRANT INSERT ON $T TO $U")
	cfg.ConnectTimeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // sink.health_timeout
	defer cancel()
	err := f.sink(cfg).Health(ctx)
	if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "452") {
		t.Fatalf("Health = %v (class %s), want fatal 452", err, ClassOf(err))
	}
}

// SPEC §8.8.7: a write against a table dropped after boot is FATAL, never
// logic. Logic would drop this batch and every following one, quietly.
func TestClickHouseDroppedTableIsFatalNotLogic(t *testing.T) {
	f := chSetup(t)
	c := f.sink(f.cfg)
	f.ready(c)
	f.exec("DROP TABLE `" + f.table + "`")
	err := c.Write(context.Background(), mkBatch(1))
	if err == nil || ClassOf(err) != ClassFatal || !strings.Contains(err.Error(), "clickhouse=60") {
		t.Errorf("Write to a dropped table = %v (class %s), want fatal 60", err, ClassOf(err))
	}
}

// SPEC §8.8.1: an unreachable server is a network failure, retried by the
// boot's WaitReady until sink.boot_timeout -- never fatal.
func TestClickHouseUnreachableIsNetwork(t *testing.T) {
	if os.Getenv(clickhouseDSNEnv) == "" {
		t.Skipf("set %s to run the clickhouse sink tests", clickhouseDSNEnv)
	}
	c, err := NewClickHouse(ClickHouseConfig{Addrs: []string{"127.0.0.1:1"}, Database: "db", Table: "inbox",
		ConnectTimeout: time.Second}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Health(ctx); err == nil || ClassOf(err) != ClassNetwork {
		t.Errorf("Health against a closed port = %v (class %s), want network", err, ClassOf(err))
	}
	if err := c.Write(ctx, mkBatch(1)); err == nil || ClassOf(err) != ClassNetwork {
		t.Errorf("Write against a closed port = %v (class %s), want network", err, ClassOf(err))
	}
}
