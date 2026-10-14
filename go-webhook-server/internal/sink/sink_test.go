package sink

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func testMetrics() *obs.Metrics {
	return obs.NewMetrics("test", "none", "go", "memory", "test")
}

// testFrom is the AgentUUID every test envelope carries (SPEC §3.5).
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// testRequest is a plausible POST as the handler resolves it before
// serialising anything (SPEC §3.2).
func testRequest(path string, body []byte) *envelope.Request {
	return &envelope.Request{Mode: envelope.ModeFast,
		Src: netip.MustParseAddr("203.0.113.9"), Dst: netip.MustParseAddr("127.0.0.1"),
		SPort: 5555, DPort: 8080, PeerAddr: netip.MustParseAddr("203.0.113.9"), PeerPort: 5555,
		SrcSource: "tcp", Proto: "HTTP/1.1", Method: "POST", Host: "example.com", Path: path,
		RawQuery: "a=1", Header: http.Header{"Content-Type": {"application/json"}},
		HostHeader: "example.com", ContentType: "application/json", Body: body}
}

// itemIn serialises r into a JLH of the given space, the way the handler
// does: one clock sample for the uuid and the event, and the item owns the
// bytes.
func itemIn(space string, r *envelope.Request) *envelope.Item {
	now := time.Now()
	h := envelope.Header{UUID: envelope.NewUUID(now), From: testFrom, Space: space,
		Type: envelope.Type, Level: envelope.Level, SID: 1, Event: envelope.EventAt(now)}
	env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(r.Body))
	return envelope.NewItem(h.UUID.String(), env, -1)
}

// mkItem is a valid envelope of the default space whose path carries tag, so
// a failing test can say which event it was looking at.
func mkItem(tag string) *envelope.Item {
	return itemIn("default", testRequest("/"+tag, []byte(`{"tag":"`+tag+`"}`)))
}

func mkBatch(n int) []*envelope.Item {
	out := make([]*envelope.Item, n)
	for i := range out {
		out[i] = mkItem(fmt.Sprintf("b%d", i))
	}
	return out
}

// sizedItem carries a text body of exactly n bytes, so its envelope grows
// with n one for one.
func sizedItem(n int) *envelope.Item {
	return itemIn("default", testRequest("/sized", bytes.Repeat([]byte{'x'}, n)))
}

// badItem is an item whose Env is not a JLH this server wrote: the envelope
// of the pre-LogHub format, as a ring.db or a dump of the previous version
// would hold it. Every sink must refuse it as data (logic) and send nothing.
func badItem() *envelope.Item {
	return envelope.NewItem("0198f1c2-0000-7000-8000-000000000001",
		[]byte(`{"id":"0198f1c2-0000-7000-8000-000000000001","v":1,"ts":1757116800,"method":"POST"}`), -1)
}

// malformedPayloadItem passes envelope.ParseHead -- the header is exact and
// the payload is one balanced object -- but its payload is not JSON. Only a
// destination that parses the payload notices, and the refusal is of this
// data alone: logic.
func malformedPayloadItem(t *testing.T) *envelope.Item {
	t.Helper()
	good := mkItem("malformed")
	i := bytes.Index(good.Env, []byte(`"payload":`)) + len(`"payload":`)
	env := append(append([]byte(nil), good.Env[:i]...), `{"mode"}}`...)
	if _, _, err := envelope.ParseHead(env); err != nil {
		t.Fatalf("the malformed-payload fixture must pass ParseHead: %v", err)
	}
	return envelope.NewItem(good.ID, env, -1)
}

// idsOf lists the uuids of items, the key every destination stores them by.
func idsOf(items ...*envelope.Item) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	return out
}

// validJLH runs the contract oracle on an envelope read back from a
// destination (SPEC §15.1).
func validJLH(t *testing.T, raw []byte) *jlhcheck.Envelope {
	t.Helper()
	e, err := jlhcheck.Validate(raw)
	if err != nil {
		t.Fatalf("envelope read back is not a valid webhook.request JLH: %v\n%s", err, raw)
	}
	return e
}

// jlhFromColumns rebuilds an envelope from the columns of an inbox row the
// way a consumer of SPEC §3.6/§3.7 does: the header from its columns, event
// from event_at plus event_nsec, and the payload column as it is. It is
// written here with fmt, independently of the serialiser under test.
func jlhFromColumns(uuid, from, space, typ string, level, sid, step int64,
	eventAt time.Time, nsec int64, payload []byte) []byte {
	at := eventAt.UTC()
	sub := int64(at.Nanosecond()) + nsec
	iso := time.Unix(at.Unix(), sub).UTC().Format("2006-01-02T15:04:05.000Z")
	return fmt.Appendf(nil, `{"uuid":%q,"from":%q,"space":%q,"type":%q,"level":%d,"sid":%d,"step":%d,`+
		`"event":{"uts":%d,"sub":%d,"iso":%q},"payload":%s}`,
		uuid, from, space, typ, level, sid, step, at.Unix(), sub, iso, payload)
}

// sameJSON reports whether a and b are the same JSON value, numbers compared
// exactly: what a destination that normalises the document (jsonb, MySQL
// JSON) is required to keep.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	decode := func(raw []byte) any {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("not JSON: %v\n%s", err, raw)
		}
		return v
	}
	ja, _ := json.Marshal(decode(a))
	jb, _ := json.Marshal(decode(b))
	return bytes.Equal(ja, jb)
}

// The fixtures themselves: an item built here is a valid webhook.request,
// and the refusal fixtures are refused for the reason they are named after.
// A helper that produced invalid envelopes would make every refusal test
// below pass for the wrong reason.
func TestFixturesAreWhatTheyClaim(t *testing.T) {
	for _, it := range append(mkBatch(3), sizedItem(10), itemIn("acme.store", testRequest("/x", nil))) {
		e := validJLH(t, it.Env)
		if e.UUID != it.ID || e.From != testFrom {
			t.Errorf("item %s carries uuid %s from %s", it.ID, e.UUID, e.From)
		}
	}
	if e := validJLH(t, mkItem("tagged").Env); e.Payload.Path != "/tagged" {
		t.Errorf("mkItem path = %q, want the tag", e.Payload.Path)
	}
	if _, _, err := envelope.ParseHead(badItem().Env); err == nil {
		t.Error("badItem passes ParseHead; the refusal tests would prove nothing")
	}
	if json.Valid(malformedPayloadItem(t).Env) {
		t.Error("malformedPayloadItem is valid JSON")
	}
	seen := map[string]bool{}
	for _, it := range mkBatch(500) {
		if seen[it.ID] {
			t.Fatalf("two items share uuid %s", it.ID)
		}
		seen[it.ID] = true
	}
}

// ------------------------------------------------------- error classification

// SPEC §8.2.4 and §8.3.4. The distinction that matters: a class chosen wrongly
// either drops data that a retry would have saved, or retries forever against a
// destination that will never accept it.
func TestClassifyRedisReply(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want Class
	}{
		// OOM means the server is ALIVE and refusing writes. Waiting cannot fix
		// it, so retrying would just fill the ring and 429 the world.
		{"OOM is fatal", "OOM command not allowed when used memory > 'maxmemory'", ClassFatal},

		// The key is the wrong type or the credentials cannot write: nothing
		// changes on its own and EVERY batch fails identically, so it is fatal
		// -- classified as logic it would drop a batch after its 200, forever,
		// in a process whose PING says it is healthy.
		{"WRONGTYPE is fatal", "WRONGTYPE Operation against a key holding the wrong kind of value", ClassFatal},
		{"NOPERM is fatal", "NOPERM this user has no permissions", ClassFatal},
		{"NOAUTH is fatal", "NOAUTH Authentication required", ClassFatal},
		{"WRONGPASS is fatal", "WRONGPASS invalid username-password pair", ClassFatal},

		// Redis has no logic class at all: XADD takes any bytes, so nothing a
		// live server answers is a refusal of one event alone.
		{"NOSCRIPT is retried, not dropped", "NOSCRIPT No matching script. Please use EVAL.", ClassNetwork},

		// A live server that is temporarily unable: retrying is exactly right.
		{"LOADING is retryable", "LOADING Redis is loading the dataset in memory", ClassNetwork},
		{"BUSY is retryable", "BUSY Redis is busy running a script", ClassNetwork},
		{"READONLY is retryable", "READONLY You can't write against a read only replica", ClassNetwork},
		{"MASTERDOWN is retryable", "MASTERDOWN Link with MASTER is down", ClassNetwork},
		{"CLUSTERDOWN is retryable", "CLUSTERDOWN Hash slot not served", ClassNetwork},
		{"TRYAGAIN is retryable", "TRYAGAIN Multiple keys request during rehashing of slot", ClassNetwork},

		// An unrecognised reply is retried rather than dropped: filling the ring
		// is visible, silently discarding data is not.
		{"unknown reply is retried", "ERR something new", ClassNetwork},

		// Matching must not depend on the server's casing.
		{"lowercase oom is fatal", "oom command not allowed", ClassFatal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRedisReply(tc.msg); got != tc.want {
				t.Errorf("class = %s, want %s", got, tc.want)
			}
		})
	}
}

// Anything that is not a server reply -- a dial failure, a broken pipe, a
// deadline -- is the connection, and is retried.
func TestClassifyRedisTransportErrors(t *testing.T) {
	for _, err := range []error{
		errors.New("dial tcp: connection refused"),
		errors.New("write: broken pipe"),
		context.DeadlineExceeded,
		context.Canceled,
	} {
		if got := ClassOf(classifyRedis("xadd", err, "id")); got != ClassNetwork {
			t.Errorf("%v: class = %s, want network", err, got)
		}
	}
	if classifyRedis("xadd", nil, "") != nil {
		t.Error("a nil error must classify as nil")
	}
}

func TestClassifySQLite(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want Class
	}{
		// These never resolve on their own; retrying would leave the process
		// alive, useless and returning 503 forever.
		{"disk full is fatal", "database or disk is full", ClassFatal},
		{"corrupt is fatal", "database disk image is malformed", ClassFatal},
		{"not a database is fatal", "file is not a database", ClassFatal},
		{"readonly is fatal", "attempt to write a readonly database", ClassFatal},

		// A CHECK of the inbox, or a payload json_type cannot read: this data,
		// not the file (SPEC §8.3.4). "malformed JSON" must not be read as the
		// "malformed" of a corrupt database, which would end the process.
		{"check is logic", "constraint failed: CHECK constraint failed: level BETWEEN 0 AND 15 (275)", ClassLogic},
		{"malformed json is logic", "malformed JSON (1)", ClassLogic},

		// Transient contention: retry.
		{"busy is retryable", "database is locked", ClassNetwork},
		{"io error is retryable", "disk i/o error", ClassNetwork},
		{"unknown is retryable", "something unexpected", ClassNetwork},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassOf(classifySQLite("insert", errors.New(tc.msg)))
			if got != tc.want {
				t.Errorf("class = %s, want %s", got, tc.want)
			}
		})
	}
	if classifySQLite("op", nil) != nil {
		t.Error("a nil error must classify as nil")
	}
}

func TestClassOfDefaultsToNetwork(t *testing.T) {
	// An unwrapped error must be retried, not dropped: guessing "permanent"
	// loses data, guessing "transient" only fills the ring, which is visible.
	if got := ClassOf(errors.New("bare")); got != ClassNetwork {
		t.Errorf("class = %s, want network", got)
	}
	if IsFatal(errors.New("bare")) {
		t.Error("a bare error must not be fatal")
	}
	if !IsFatal(fatalErr("op", errors.New("x"))) {
		t.Error("a fatal error must report as fatal")
	}
}

// --------------------------------------------------------------- topic

// SPEC §3.3 and §3.8: {space} and {type} are expanded per envelope with the
// envelope's own values; a template without placeholders is used as is.
func TestTopicExpandsPerEnvelope(t *testing.T) {
	tp := newTopic("loghub.{space}.{type}", "default")
	if tp.own != "loghub.default.webhook.request" {
		t.Errorf("own = %q", tp.own)
	}
	if got := tp.For("default", envelope.Type); got != tp.own {
		t.Errorf("the instance's own envelope goes to %q, want %q", got, tp.own)
	}
	if got := tp.For("acme.store", envelope.Type); got != "loghub.acme.store.webhook.request" {
		t.Errorf("another space goes to %q", got)
	}
	if got := tp.For("default", "other.type"); got != "loghub.default.other.type" {
		t.Errorf("another type goes to %q", got)
	}
	lit := newTopic("webhooks.in", "default")
	if lit.own != "webhooks.in" || lit.For("acme", envelope.Type) != "webhooks.in" {
		t.Errorf("a literal name was rewritten: own %q, for acme %q", lit.own, lit.For("acme", envelope.Type))
	}
}

// --------------------------------------------------------------- none sink

func TestNoneSink(t *testing.T) {
	n := NewNone()
	if err := n.Write(context.Background(), mkBatch(10)); err != nil {
		t.Errorf("Write: %v", err)
	}
	// This sink can never be the reason the server stops serving.
	if err := n.Health(context.Background()); err != nil {
		t.Errorf("Health: %v", err)
	}
	if n.Type() != "none" {
		t.Errorf("Type = %q", n.Type())
	}
}

// --------------------------------------------------------------- sqlite sink

func newSQLiteSink(t *testing.T, cfg SQLiteConfig) (*SQLite, string) {
	t.Helper()
	dir := t.TempDir()
	if cfg.Path == "" {
		cfg.Path = filepath.Join(dir, "events.db")
	}
	if cfg.Synchronous == "" {
		cfg.Synchronous = "OFF"
	}
	s, err := NewSQLite(cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func countInbox(t *testing.T, s *SQLite) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM inbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// sqliteRowJLH rebuilds the envelope of one inbox row (SPEC §3.7).
func sqliteRowJLH(t *testing.T, db *sql.DB, id string) []byte {
	t.Helper()
	var (
		uuid, from, space, typ, eventAt, payload string
		level, sid, step, nsec                   int64
	)
	err := db.QueryRow(`SELECT uuid, from_uuid, space, type, level, sid, step, event_at, event_nsec, payload
		FROM inbox WHERE uuid = ?`, id).
		Scan(&uuid, &from, &space, &typ, &level, &sid, &step, &eventAt, &nsec, &payload)
	if err != nil {
		t.Fatalf("reading %s back: %v", id, err)
	}
	at, err := time.Parse(sqliteEventLayout, eventAt)
	if err != nil {
		t.Fatalf("event_at %q is not %s: %v", eventAt, sqliteEventLayout, err)
	}
	return jlhFromColumns(uuid, from, space, typ, level, sid, step, at, nsec, []byte(payload))
}

func TestSQLiteSinkWrite(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{})
	if err := s.Write(context.Background(), mkBatch(20)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := countInbox(t, s); n != 20 {
		t.Errorf("%d rows, want 20", n)
	}
}

// SPEC §3.7: the inbox mirrors loghub.inbox column for column, the payload is
// the JLH's text byte for byte, and the envelope rebuilds from the row with
// nothing lost -- the bytes themselves, event to the nanosecond.
func TestSQLiteSinkRowRebuildsTheEnvelope(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{})
	items := []*envelope.Item{
		mkItem("evt-1"),
		itemIn("acme.store", testRequest("/hook/telegram", []byte(`{"update_id":1}`))),
		itemIn("default", testRequest("/bin", []byte{0x00, 0x0a, 0xff, 0x0d, 0x1a, 0x7f, 'A'})),
	}
	if err := s.Write(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		rebuilt := sqliteRowJLH(t, s.db, it.ID)
		if !bytes.Equal(rebuilt, it.Env) {
			t.Errorf("row %s rebuilds\n%s\nwant the JLH byte for byte\n%s", it.ID, rebuilt, it.Env)
		}
		validJLH(t, rebuilt)
	}

	// The inbox is queryable with plain SQL over its columns, the JLH with
	// json_extract: this is a final destination, not a dump file.
	var id, eventAt, receivedAt, host string
	err := s.db.QueryRow(`SELECT uuid, event_at, received_at, json_extract(payload, '$.host') FROM inbox
		WHERE space = ? AND type = ? AND event_at >= ?`,
		"acme.store", envelope.Type, "2026-01-01T00:00:00.000000Z").Scan(&id, &eventAt, &receivedAt, &host)
	if err != nil {
		t.Fatalf("a plain SQL query over the columns failed: %v", err)
	}
	if id != items[1].ID || host != "example.com" || len(eventAt) != 27 || !strings.HasSuffix(receivedAt, "Z") {
		t.Errorf("uuid %s event_at %q received_at %q host %q", id, eventAt, receivedAt, host)
	}

	// The body arrives exactly: binary in base64 inside the payload, text as
	// it came.
	e := validJLH(t, sqliteRowJLH(t, s.db, items[2].ID))
	body, err := e.Payload.Body()
	if err != nil || !bytes.Equal(body, []byte{0x00, 0x0a, 0xff, 0x0d, 0x1a, 0x7f, 'A'}) || e.Payload.Content.Encoding != "base64" {
		t.Errorf("binary body = %v (%s, %v)", body, e.Payload.Content.Encoding, err)
	}
}

// SPEC §8.3: redelivery is normal in an at-least-once pipeline. A plain INSERT
// would fail the whole batch for one duplicate, taking every event beside it
// down with it -- and so would a uuid twice inside one batch.
func TestSQLiteSinkToleratesRedelivery(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{})
	batch := mkBatch(5)

	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("redelivering the same batch must succeed, got: %v", err)
	}

	// A mixed batch -- some already stored, some new -- must store the new ones.
	fresh := mkItem("brand-new")
	mixed := append(batch[:2:2], fresh, fresh)
	if err := s.Write(context.Background(), mixed); err != nil {
		t.Fatalf("mixed batch with a uuid twice: %v", err)
	}
	if n := countInbox(t, s); n != 6 {
		t.Errorf("%d rows, want 6 (5 original plus 1 new)", n)
	}
}

// SPEC §3.7 and §8.3.4: the upsert clause is ON CONFLICT (uuid) DO NOTHING,
// never INSERT OR IGNORE. Both absorb a duplicate uuid; only the first lets a
// CHECK failure surface. OR IGNORE would skip the refused row and commit the
// batch, and the event would vanish uncounted with the sink UP.
func TestSQLiteSinkCheckFailureIsLogicNotIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	// The operator's inbox, created before the server's CREATE TABLE IF NOT
	// EXISTS, with one CHECK more than §3.7: the only way to make a CHECK
	// refuse an envelope this server wrote.
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	ddl := strings.Replace(sqliteSchema, "payload     TEXT    NOT NULL            CHECK (json_type(payload) = 'object')",
		"payload     TEXT    NOT NULL            CHECK (json_type(payload) = 'object'),\n  CHECK (space <> 'blocked')", 1)
	if ddl == sqliteSchema {
		t.Fatal("the schema changed shape; this test cannot add its CHECK")
	}
	if _, err := db.Exec(ddl); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, _ := newSQLiteSink(t, SQLiteConfig{Path: path})
	ctx := context.Background()
	stored := mkItem("stored")
	if err := s.Write(ctx, []*envelope.Item{stored}); err != nil {
		t.Fatalf("control: a valid envelope was refused: %v", err)
	}
	// The duplicate is absorbed...
	if err := s.Write(ctx, []*envelope.Item{stored, stored}); err != nil {
		t.Fatalf("a redelivered uuid was not absorbed: %v", err)
	}
	// ...the CHECK is not.
	fresh := mkItem("fresh")
	blocked := itemIn("blocked", testRequest("/blocked", nil))
	err = s.Write(ctx, []*envelope.Item{fresh, blocked})
	if err == nil {
		t.Fatal("a CHECK failure was swallowed: the batch committed")
	}
	if ClassOf(err) != ClassLogic {
		t.Errorf("CHECK failure = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := countInbox(t, s); n != 1 {
		t.Errorf("%d rows; the refused batch must roll back whole, leaving only the control", n)
	}

	// A payload json_type cannot read is logic too, never the "malformed"
	// of a corrupt file.
	if err := s.Write(ctx, []*envelope.Item{malformedPayloadItem(t)}); ClassOf(err) != ClassLogic || err == nil {
		t.Errorf("malformed payload = %v (class %s), want logic", err, ClassOf(err))
	}
}

// SPEC §8.3: an item whose JLH this server did not write is refused as data,
// before anything is committed.
func TestSQLiteSinkRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{})
	err := s.Write(context.Background(), []*envelope.Item{mkItem("good"), badItem()})
	if err == nil || ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
	}
	if n := countInbox(t, s); n != 0 {
		t.Errorf("%d rows; nothing of a refused batch may be committed", n)
	}
}

// SPEC §3.7: the v1 events table is neither written nor read. In an existing
// events.db it stays as it was, next to the new inbox.
func TestSQLiteSinkLeavesTheV1EventsTableAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	const v1 = `CREATE TABLE events (id TEXT NOT NULL PRIMARY KEY, ts INTEGER NOT NULL, method TEXT NOT NULL,
		host TEXT NOT NULL, path TEXT NOT NULL, src_ip TEXT NOT NULL, env BLOB NOT NULL, body BLOB NOT NULL)`
	if _, err := db.Exec(v1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO events VALUES ('old-1', 1757116800, 'POST', 'h', '/p', '::1', x'7b7d', x'00ff')`); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE name = 'events'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, _ := newSQLiteSink(t, SQLiteConfig{Path: path})
	if err := s.Write(context.Background(), mkBatch(3)); err != nil {
		t.Fatalf("Write next to a v1 table: %v", err)
	}
	var after, id string
	var body []byte
	var n int
	if err := s.db.QueryRow(`SELECT sql FROM sqlite_master WHERE name = 'events'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*), MIN(id), MIN(body) FROM events`).Scan(&n, &id, &body); err != nil {
		t.Fatal(err)
	}
	if after != before || n != 1 || id != "old-1" || !bytes.Equal(body, []byte{0x00, 0xff}) {
		t.Errorf("the v1 table changed: schema %q -> %q, %d rows, id %q body %x", before, after, n, id, body)
	}
	if got := countInbox(t, s); got != 3 {
		t.Errorf("%d inbox rows, want 3", got)
	}
}

// SPEC §8.3.5: the probe is a WRITE, rewritten in place, so it proves the file
// is still writable and costs the same at any table size. quick_check, which
// reads the whole file, runs at boot only.
func TestSQLiteSinkHealth(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{})
	if err := s.Health(context.Background()); err != nil {
		t.Errorf("Health on a good database: %v", err)
	}

	var first, second int64
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&first); err != nil {
		t.Fatalf("the heartbeat row was not written: %v", err)
	}
	time.Sleep(time.Millisecond)
	if err := s.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT ts FROM health WHERE id = 1`).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Errorf("the heartbeat did not advance: %d -> %d", first, second)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM health`).Scan(&rows); err != nil || rows != 1 {
		t.Errorf("health table has %d rows (%v), want exactly one rewritten in place", rows, err)
	}
	if err := s.integrityCheck(); err != nil {
		t.Errorf("integrityCheck on a good database: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err == nil {
		t.Error("Health on a closed sink should fail")
	}
}

// A file that is not a database is refused at boot, before anything is
// accepted that could not be stored.
func TestSQLiteSinkRefusesADamagedFileAtBoot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	junk := []byte(strings.Repeat("this is not a database ", 100))
	if err := os.WriteFile(path, junk, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, quietLog(), testMetrics()); err == nil {
		t.Fatal("a file that is not a database was opened as one")
	}
}

func TestSQLiteSinkRotationPaths(t *testing.T) {
	s, _ := newSQLiteSink(t, SQLiteConfig{Rotate: "daily"})

	now := time.Date(2026, 9, 6, 14, 30, 0, 0, time.UTC)
	if got := periodOf(now, "daily"); got != "2026-09-06" {
		t.Errorf("daily period = %q", got)
	}
	if got := periodOf(now, "hourly"); got != "2026-09-06-14" {
		t.Errorf("hourly period = %q", got)
	}
	if got := periodOf(now, "none"); got != "" {
		t.Errorf("rotation off should give an empty period, got %q", got)
	}
	// The period is evaluated in UTC, so a local timezone cannot shift which
	// file an event lands in.
	local := now.In(time.FixedZone("X", 12*3600))
	if periodOf(local, "daily") != periodOf(now, "daily") {
		t.Error("the rotation period must not depend on the local timezone")
	}

	if got := s.pathFor("2026-09-06"); !strings.HasSuffix(got, "events-2026-09-06.db") {
		t.Errorf("pathFor = %q", got)
	}
	if got := s.pathFor(""); !strings.HasSuffix(got, "events.db") {
		t.Errorf("pathFor(\"\") = %q", got)
	}
}

// SPEC §8.3.3: keep_files defaults to 0, which removes NOTHING. Silently
// deleting a webhook archive is not a decision this program makes unasked.
func TestSQLiteSinkKeepFilesZeroDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	s, _ := newSQLiteSink(t, SQLiteConfig{
		Path: filepath.Join(dir, "events.db"), Rotate: "daily", KeepFiles: 0,
	})
	for _, name := range []string{"events-2026-09-01.db", "events-2026-09-02.db", "events-2026-09-03.db"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.pruneRotated()

	// Only the three fabricated files are checked: the sink opened a rotated
	// file of its own for today, with -wal and -shm sidecars beside it.
	for _, name := range []string{"events-2026-09-01.db", "events-2026-09-02.db", "events-2026-09-03.db"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s was deleted; keep_files=0 must remove nothing", name)
		}
	}
}

func TestSQLiteSinkKeepFilesPrunesOldest(t *testing.T) {
	dir := t.TempDir()
	s, _ := newSQLiteSink(t, SQLiteConfig{
		Path: filepath.Join(dir, "events.db"), Rotate: "daily", KeepFiles: 2,
	})
	names := []string{
		"events-2026-09-01.db", "events-2026-09-02.db",
		"events-2026-09-03.db", "events-2026-09-04.db",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	s.pruneRotated()

	for _, n := range names[:2] {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			t.Errorf("%s should have been pruned", n)
		}
	}
	for _, n := range names[2:] {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s should have been kept", n)
		}
	}
}

// SPEC §13.3 lists sink.sqlite.keep_files among the keys SIGHUP may change.
// It was declared reloadable in the config registry from the start, but nothing
// ever pushed the new value into the sink, so a reload logged "applied" and
// changed nothing -- the failure an operator discovers when the disk fills
// anyway.
func TestSQLiteSinkKeepFilesIsReloadable(t *testing.T) {
	dir := t.TempDir()
	s, _ := newSQLiteSink(t, SQLiteConfig{
		Path: filepath.Join(dir, "events.db"), Rotate: "daily", KeepFiles: 0,
	})
	names := []string{
		"events-2026-09-01.db", "events-2026-09-02.db",
		"events-2026-09-03.db", "events-2026-09-04.db",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	s.pruneRotated()
	for _, n := range names {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Fatalf("%s was removed while keep_files was still 0", n)
		}
	}

	s.SetKeepFiles(2)
	s.pruneRotated()

	for _, n := range names[:2] {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			t.Errorf("%s survived a reload that set keep_files=2", n)
		}
	}
	for _, n := range names[2:] {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s should have been kept", n)
		}
	}

	// Back to 0 and nothing more is removed: the reload has to work in both
	// directions, or an operator cannot undo a retention they regret.
	s.SetKeepFiles(0)
	s.pruneRotated()
	for _, n := range names[2:] {
		if _, err := os.Stat(filepath.Join(dir, n)); err != nil {
			t.Errorf("%s was removed after keep_files went back to 0", n)
		}
	}
}

// SPEC §8.3.3 and §15.3: the real hourly turnover, not just the filename
// arithmetic. Rotation happens BETWEEN batches, so no event may be lost, no
// event may be duplicated, and no event may land in the wrong file.
//
// The clock is walked rather than waited on, which is the only way to exercise
// the boundary at all: an untested rotation is the feature most likely to lose
// an event the first time it fires for real.
func TestSQLiteSinkHourlyTurnover(t *testing.T) {
	dir := t.TempDir()
	s, _ := newSQLiteSink(t, SQLiteConfig{
		Path: filepath.Join(dir, "events.db"), Rotate: "hourly",
	})

	var mu sync.Mutex
	clock := time.Date(2026, 9, 6, 14, 30, 0, 0, time.UTC)
	s.setClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	})
	advance := func(d time.Duration) {
		mu.Lock()
		clock = clock.Add(d)
		mu.Unlock()
	}

	// Three batches: two inside one hour, one after the boundary.
	first := []*envelope.Item{mkItem("a1"), mkItem("a2")}
	second := []*envelope.Item{mkItem("a3")}
	third := []*envelope.Item{mkItem("b1"), mkItem("b2")}

	ctx := context.Background()
	if err := s.Write(ctx, first); err != nil {
		t.Fatal(err)
	}
	advance(20 * time.Minute) // 14:50, same period
	if err := s.Write(ctx, second); err != nil {
		t.Fatal(err)
	}

	// Counted from here, so the one rotation the sink performs on its first
	// write -- moving off the real wall-clock hour it opened at construction --
	// is not confused with the turnover under test.
	beforeTurnover := s.m.SinkSQLiteRotationsTotal.Load()

	advance(20 * time.Minute) // 15:10, next period
	if err := s.Write(ctx, third); err != nil {
		t.Fatal(err)
	}

	hour14 := filepath.Join(dir, "events-2026-09-06-14.db")
	hour15 := filepath.Join(dir, "events-2026-09-06-15.db")

	if got, want := idsIn(t, hour14), idsOf(append(first, second...)...); !sameSet(got, want) {
		t.Errorf("the 14:00 file holds %v, want %v", got, want)
	}
	if got, want := idsIn(t, hour15), idsOf(third...); !sameSet(got, want) {
		t.Errorf("the 15:00 file holds %v, want %v", got, want)
	}

	// Exactly one rotation for the boundary, and none for the second write
	// inside the same hour: rotating per batch would fragment the archive.
	if got := s.m.SinkSQLiteRotationsTotal.Load() - beforeTurnover; got != 1 {
		t.Errorf("the turnover caused %d rotations, want 1", got)
	}

	// The write after the boundary must not have gone into the old file too.
	if n := len(idsIn(t, hour14)); n != 3 {
		t.Errorf("the 14:00 file holds %d events after the turnover, want 3", n)
	}
}

// SPEC §8.3.3: keep_files never removes the file currently being written, even
// when the retention would otherwise select it.
func TestSQLiteSinkNeverPrunesTheCurrentFile(t *testing.T) {
	dir := t.TempDir()
	s, _ := newSQLiteSink(t, SQLiteConfig{
		Path: filepath.Join(dir, "events.db"), Rotate: "hourly", KeepFiles: 1,
	})

	var mu sync.Mutex
	clock := time.Date(2026, 9, 6, 14, 0, 0, 0, time.UTC)
	s.setClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	})

	ctx := context.Background()
	var last *envelope.Item
	for i := 0; i < 4; i++ {
		last = mkItem(fmt.Sprintf("h%d", i))
		if err := s.Write(ctx, []*envelope.Item{last}); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		clock = clock.Add(time.Hour)
		mu.Unlock()
	}

	// The last write opened 17:00 and is the current file; it must still exist
	// no matter how aggressive the retention is.
	current := filepath.Join(dir, "events-2026-09-06-17.db")
	if _, err := os.Stat(current); err != nil {
		t.Errorf("the file being written was pruned: %v", err)
	}
	if ids := idsIn(t, current); len(ids) != 1 || ids[0] != last.ID {
		t.Errorf("the current file holds %v, want the one event written into it (%s)", ids, last.ID)
	}
}

// idsIn reads the envelope uuids out of the inbox of a sink database file.
func idsIn(t *testing.T, path string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT uuid FROM inbox ORDER BY uuid`)
	if err != nil {
		t.Fatalf("querying %s: %v", path, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------- health

type scriptedSink struct {
	mu       sync.Mutex
	healthy  bool
	writeErr error
	writes   atomic.Int64
}

func (s *scriptedSink) Write(ctx context.Context, items []*envelope.Item) error {
	s.writes.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeErr
}

func (s *scriptedSink) Health(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healthy {
		return nil
	}
	return errors.New("sink is down")
}

func (s *scriptedSink) Close() error { return nil }
func (s *scriptedSink) Type() string { return "scripted" }

func (s *scriptedSink) setHealthy(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = v
}

func (s *scriptedSink) setWriteErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeErr = err
}

func TestHealthTransitions(t *testing.T) {
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{
		Interval: 10 * time.Millisecond, Timeout: 50 * time.Millisecond,
		FailAfter: time.Hour, MaxRetries: 1000,
	}, quietLog(), m)

	if !h.Up() {
		t.Fatal("a new Health should start UP")
	}
	if m.SinkUp.Load() != 1 {
		t.Error("whs_sink_up should be 1 while UP")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	ss.setHealthy(false)
	if !eventually(t, 2*time.Second, func() bool { return !h.Up() }) {
		t.Fatal("the sink never went DOWN")
	}
	if m.SinkUp.Load() != 0 {
		t.Error("whs_sink_up should be 0 while DOWN")
	}

	ss.setHealthy(true)
	if !eventually(t, 2*time.Second, func() bool { return h.Up() }) {
		t.Fatal("the sink never recovered")
	}
	if m.SinkUp.Load() != 1 {
		t.Error("whs_sink_up should return to 1 on recovery")
	}
}

// SPEC §10.2: DOWN persisting past either fuse becomes FATAL.
func TestHealthFatalAfterMaxRetries(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 20 * time.Millisecond,
		FailAfter:  time.Hour, // the count is what should trip
		MaxRetries: 3,
	}, quietLog(), testMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("FATAL never fired after the retry limit")
	}
	if h.State() != StateFatal {
		t.Errorf("state = %s, want fatal", h.State())
	}
	if h.FatalErr() == nil {
		t.Error("FatalErr should report the cause")
	}
}

func TestHealthFatalAfterTimeout(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 20 * time.Millisecond,
		FailAfter:  80 * time.Millisecond,
		MaxRetries: 100000, // the clock is what should trip
	}, quietLog(), testMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("FATAL never fired after fail_timeout")
	}
}

// MarkFatal is called from the drain workers; a second call must not panic on a
// closed channel.
func TestMarkFatalIsIdempotent(t *testing.T) {
	h := NewHealth(&scriptedSink{healthy: true}, HealthConfig{
		Interval: time.Hour, Timeout: time.Second,
	}, quietLog(), testMetrics())

	h.MarkFatal(errors.New("first"))
	h.MarkFatal(errors.New("second"))
	h.MarkFatal(errors.New("third"))

	if h.FatalErr() == nil || !strings.Contains(h.FatalErr().Error(), "first") {
		t.Errorf("FatalErr = %v, want the first cause retained", h.FatalErr())
	}
}

func TestWaitReadySucceedsAfterRetries(t *testing.T) {
	ss := &scriptedSink{healthy: false}
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: 100 * time.Millisecond},
		quietLog(), testMetrics())

	go func() {
		time.Sleep(150 * time.Millisecond)
		ss.setHealthy(true)
	}()

	err := h.WaitReady(context.Background(), 3*time.Second, 10*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Errorf("WaitReady: %v", err)
	}
}

func TestWaitReadyGivesUp(t *testing.T) {
	h := NewHealth(&scriptedSink{healthy: false},
		HealthConfig{Interval: time.Hour, Timeout: 50 * time.Millisecond},
		quietLog(), testMetrics())

	start := time.Now()
	err := h.WaitReady(context.Background(), 200*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Error("WaitReady should fail once the boot timeout passes")
	}
	if time.Since(start) > 3*time.Second {
		t.Errorf("WaitReady took %s, far past its 200ms budget", time.Since(start))
	}
}

// ------------------------------------------------------------------ workers

func TestWorkersDeliverAndRelease(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 2, Batch: 8, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond,
	}, quietLog(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	items := mkBatch(30)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	if !eventually(t, 3*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 30 }) {
		t.Fatalf("delivered %d of 30", m.EventsWrittenTotal.Load())
	}
	if !eventually(t, time.Second, func() bool { return r.Stats().Items == 0 }) {
		t.Errorf("ring still holds %d items", r.Stats().Items)
	}

	// SPEC §12.3: buffers return to the pool exactly once, on the confirmed
	// path and nowhere else.
	if !eventually(t, time.Second, func() bool {
		for _, it := range items {
			if !it.Released() {
				return false
			}
		}
		return true
	}) {
		t.Error("delivered items were not released")
	}
}

// SPEC §10.4: a retryable failure makes the worker HOLD the batch and stop
// pulling. The ring is untouched, so it fills and 429 appears on its own.
func TestWorkersHoldBatchOnRetryableFailure(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: errors.New("connection refused")}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: 200 * time.Millisecond,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 10 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued BEFORE the worker starts: with batch_delay = 0 a worker that
	// wakes between two Enqueues takes a partial batch, holds it, and pulls
	// nothing more -- and the assertion below would then count fewer than 4.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// The worker keeps retrying rather than giving up or dropping.
	if !eventually(t, 2*time.Second, func() bool { return ss.writes.Load() > 2 }) {
		t.Fatalf("only %d write attempts; the worker is not retrying", ss.writes.Load())
	}
	if m.EventsWrittenTotal.Load() != 0 {
		t.Error("nothing was delivered, so nothing should be counted as written")
	}

	// Stopping must surrender the held batch, or those events -- already
	// answered 200 -- would vanish with no emergency dump.
	cancel()
	w.Wait()

	pending := w.Pending()
	if len(pending) != 4 {
		t.Errorf("Pending returned %d items, want the 4 the worker was holding", len(pending))
	}
	for i, it := range pending {
		if it.Released() {
			t.Errorf("held item %d was released; its buffer is back in the pool while still referenced", i)
		}
	}
}

// A permanent rejection drops the batch, loudly and counted, rather than
// retrying against a destination that will never accept it.
func TestWorkersDropOnLogicError(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: logicErr("insert", errors.New("WRONGTYPE"))}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued before the worker starts: started first, it can dequeue the
	// first item alone and the rest as a second batch -- two logic drops, two
	// writes, and a flaky "retried" failure that is not one.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	if !eventually(t, 2*time.Second, func() bool {
		return m.EventsDroppedTotal[obs.DropSinkLogic].Load() == 4
	}) {
		t.Errorf("dropped %d, want 4 counted", m.EventsDroppedTotal[obs.DropSinkLogic].Load())
	}
	if !eventually(t, time.Second, func() bool { return r.Stats().Items == 0 }) {
		t.Error("a dropped batch must still clear the ring")
	}
	// Its bytes too: Commit subtracts len(Env), which Release empties, so a
	// Release first left the ring counting bytes nobody held -- forever, until
	// it refused every request as full (the review of go-webhook-nats-server,
	// 2026-10-10).
	if b := r.Stats().Bytes; b != 0 {
		t.Errorf("the empty ring still counts %d bytes", b)
	}
	if ss.writes.Load() != 1 {
		t.Errorf("%d attempts; a permanent rejection must not be retried", ss.writes.Load())
	}
}

// SPEC §8.1, §8.7.2: a logic refusal that names the items the destination
// stored drops only the others. The stored ones count as written, the batch
// still leaves the ring whole, every item is released once, and the write is
// not retried. Counting the whole batch as lost was the defect of this
// module's NATS sink until 2026-10-11; the refused ones are the only loss.
func TestWorkersPartialDeliveryDropsOnlyTheRefused(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	partial := &Error{Class: ClassLogic, Op: "ack", Err: errors.New("message size exceeds maximum allowed"),
		Delivered: []int{0, 2}}
	ss := &scriptedSink{healthy: true, writeErr: partial}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	items := mkBatch(4)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	if !eventually(t, 2*time.Second, func() bool { return r.Stats().Items == 0 }) {
		t.Fatal("a partially delivered batch must still clear the ring")
	}
	// Its bytes too: Commit subtracts len(Env), which Release empties, so a
	// Release first left the ring counting bytes nobody held -- forever, until
	// it refused every request as full (review 2026-10-10).
	if b := r.Stats().Bytes; b != 0 {
		t.Errorf("the empty ring still counts %d bytes", b)
	}
	if got := m.EventsWrittenTotal.Load(); got != 2 {
		t.Errorf("written = %d, want the 2 the destination stored", got)
	}
	if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 2 {
		t.Errorf("dropped = %d, want only the 2 refused", got)
	}
	if ss.writes.Load() != 1 {
		t.Errorf("%d attempts; a permanent refusal must not be retried", ss.writes.Load())
	}
	for i, it := range items {
		if !it.Released() {
			t.Errorf("item %d was not released", i)
		}
	}

	// Control: the same refusal without Delivered loses the whole batch.
	if got := refusedOf(items, logicErr("x", errors.New("x"))); len(got) != 4 {
		t.Errorf("no Delivered: %d refused, want all 4", len(got))
	}
	// An index out of range or repeated is ignored, never trusted into a panic.
	odd := &Error{Class: ClassLogic, Err: errors.New("x"), Delivered: []int{-1, 1, 1, 9, 3}}
	if got := refusedOf(items, odd); len(got) != 2 || got[0] != items[0] || got[1] != items[2] {
		t.Errorf("odd Delivered: refused %v, want items 0 and 2", got)
	}
	// Delivered on a class that is not logic is not a partial delivery.
	if _, ok := DeliveredOf(&Error{Class: ClassNetwork, Err: errors.New("x"), Delivered: []int{0}}); ok {
		t.Error("DeliveredOf read a network error as a partial delivery")
	}
}

// A fatal write result ends the process immediately rather than waiting for the
// next health check.
func TestWorkersMarkFatal(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &scriptedSink{healthy: true, writeErr: fatalErr("xadd", errors.New("OOM"))}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued before the worker starts, so its first batch is all four:
	// started first, it can take the first item alone and stop on the fatal
	// answer, holding one.
	for _, it := range mkBatch(4) {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("a fatal write result did not mark the sink fatal")
	}

	// The items stay held so the emergency dump can collect them. Pending
	// hands them over and forgets them: it is read once.
	cancel()
	w.Wait()
	if pending := w.Pending(); len(pending) != 4 {
		t.Errorf("Pending returned %d, want the 4 held for the dump", len(pending))
	}
}

// SPEC §12.3: an attempt that ends by the clock taints the batch. Even when the
// retry succeeds, the buffers go to the GC rather than the pool, because the
// timed-out command may still be on its way through the pipeline.
func TestWorkersTaintABatchAfterATimedOutAttempt(t *testing.T) {
	r := ring.NewMemory(100, 1<<20, 0)
	ss := &stallOnceSink{}
	m := testMetrics()
	h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)
	w := NewWorkers(r, ss, h, WorkerConfig{
		Workers: 1, Batch: 4, OpTimeout: 20 * time.Millisecond,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, quietLog(), m)

	// Enqueued before the worker starts, so the stalled first write carries
	// all four. Started first, the worker could take item 0 alone, stall on
	// it, and then deliver items 1-3 in an attempt that never timed out:
	// those are pooled, rightly, and the test read their Env while Release
	// was clearing it -- a DATA RACE about 1 run in 70 under -race, or an
	// "item 3 was pooled" failure (integration of the syslog/RELP port,
	// 2026-10-13). The ownership rule held; the test raced its own setup.
	items := mkBatch(4)
	for _, it := range items {
		if err := r.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	if !eventually(t, 3*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 4 }) {
		t.Fatalf("delivered %d of 4 after the stall", m.EventsWrittenTotal.Load())
	}
	if n := ss.stalled.Load(); n != 4 {
		t.Fatalf("the stalled write carried %d of the 4 items; the rest were never at risk", n)
	}
	if !eventually(t, time.Second, func() bool {
		for _, it := range items {
			if !it.Released() {
				return false
			}
		}
		return true
	}) {
		t.Fatal("delivered items were not released")
	}
	for i, it := range items {
		if it.Env == nil {
			t.Errorf("item %d was pooled after a timed-out attempt; the first command may still be in flight", i)
		}
	}
	if ss.writes.Load() < 2 {
		t.Fatalf("only %d attempts; the stall never happened", ss.writes.Load())
	}
	// SPEC §11.1: the timed-out attempt is counted apart from network errors.
	if m.SinkErrorsTotal[obs.SinkErrTimeout].Load() == 0 {
		t.Error("the timed-out attempt was not counted in kind=\"timeout\"")
	}
}

// stallOnceSink blocks the first write until its context expires -- the shape
// of a destination that stopped answering -- and accepts everything after.
type stallOnceSink struct {
	writes  atomic.Int64
	stalled atomic.Int64 // items in the write that stalled
}

func (s *stallOnceSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.writes.Add(1) == 1 {
		s.stalled.Store(int64(len(items)))
		<-ctx.Done()
		return networkErr("xadd", ctx.Err())
	}
	return nil
}
func (s *stallOnceSink) Health(context.Context) error { return nil }
func (s *stallOnceSink) Close() error                 { return nil }
func (s *stallOnceSink) Type() string                 { return "stall" }

func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// ------------------------------------------------- worker hold, the overwrite

// A worker that ABANDONS a batch on shutdown keeps it registered in held[id],
// which is what lets Pending() hand it to the emergency dump. But the worker's
// loop then calls Dequeue again, and Dequeue's first select races a cancelled
// context against a ring that still has a backlog: Go picks a ready case at
// random, so the channel can win. The worker comes back with a second batch,
// deliver calls hold(id, ...) -- and the map assignment OVERWRITES the batch it
// was still holding.
//
// Those events were answered 200 and are then in neither the ring nor held.
// They are gone, with nothing logged, and the count in "events still pending
// after the drain" looks perfectly reasonable.
//
// This is the same family as the bug that made a retrying worker's batch vanish
// (docs/TESTING.md §9); the registration fixed that one and left this hole.
func TestWorkersNeverLoseAnAbandonedBatch(t *testing.T) {
	const (
		rounds   = 30
		backlog  = 40
		perRound = backlog + 1
	)
	for round := 0; round < rounds; round++ {
		func() {
			m := testMetrics()
			// The sink refuses every write, so nothing is ever committed and
			// every accepted item must still be accounted for at the end.
			ss := &scriptedSink{healthy: true, writeErr: networkErr("write", errors.New("destination is down"))}
			r := ring.NewMemory(perRound*2, 0, 0)
			h := NewHealth(ss, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m)

			// One worker and a batch of one: the worker takes a single item,
			// fails, and holds it while the rest of the traffic piles up in the
			// ring behind it. That is exactly the shape the incident had.
			w := NewWorkers(r, ss, h, WorkerConfig{
				Workers: 1, Batch: 256, OpTimeout: time.Second,
				BackoffMin: 50 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
			}, quietLog(), m)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Seed one item and let the worker pick it up and start retrying.
			first := mkItem("first")
			if err := r.Enqueue(ctx, first); err != nil {
				t.Fatalf("round %d: seeding: %v", round, err)
			}
			w.Start(ctx)
			if !eventually(t, 2*time.Second, func() bool { return ss.writes.Load() > 0 }) {
				t.Fatalf("round %d: the worker never attempted a write", round)
			}

			// Now fill the ring behind the stuck worker.
			for i := 0; i < backlog; i++ {
				if err := r.Enqueue(ctx, mkItem(fmt.Sprintf("b%d", i))); err != nil {
					t.Fatalf("round %d: filling: %v", round, err)
				}
			}

			cancel()
			w.Wait()

			// Everything answered 200 must be recoverable: what the workers
			// were holding, plus whatever is still in the ring.
			got := len(w.Pending()) + len(r.Drain())
			if got != perRound {
				t.Fatalf("round %d: %d of %d accepted events survived the shutdown; "+
					"%d were answered 200 and then silently lost",
					round, got, perRound, perRound-got)
			}
		}()
	}
}
