package verify

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
)

// The readers against the real stores. SQLite always runs; the others are
// opt-in (TESTING §1): WI_TEST_NATS_URL, WI_TEST_PG_URL, WI_TEST_REDIS_URL,
// WI_TEST_VALKEY_URL.

// v7 is a UUIDv7-shaped uuid of the millisecond of t.
func v7(t time.Time, n int) string {
	ms := uint64(t.UnixMilli())
	return fmt.Sprintf("%08x-%04x-7000-8000-%012x", ms>>16, ms&0xffff, n)
}

func envJLH(uuid string, seq int) string {
	return fmt.Sprintf(`{"uuid":%q,"from":"f","space":"default","type":"webhook.request","payload":{"mode":"fast","content":{"length":1,"type":"application/json","encoding":"utf-8","body":%q}}}`,
		uuid, body(seq, "run1"))
}

func payloadOf(seq int) string {
	return fmt.Sprintf(`{"mode":"fast","content":{"length":1,"type":"application/json","encoding":"utf-8","body":%q}}`, body(seq, "run1"))
}

// collect reads once and returns "uuid seq" of every record, sorted.
func collect(t *testing.T, r reader) []string {
	t.Helper()
	var out []string
	err := r.read(context.Background(), func(rec record) {
		if rec.jlh != nil {
			rec = fromJLH(rec.key, rec.jlh)
		}
		s, _ := marker(rec.encoding, rec.body, "run1")
		out = append(out, rec.uuid+" "+strconv.FormatUint(s, 10))
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

const sqliteDDL = `CREATE TABLE inbox (
  uuid TEXT NOT NULL PRIMARY KEY, from_uuid TEXT NOT NULL, event_at TEXT NOT NULL,
  received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  sid INTEGER NOT NULL DEFAULT 0, step INTEGER NOT NULL DEFAULT 0, level INTEGER NOT NULL,
  event_nsec INTEGER NOT NULL DEFAULT 0, space TEXT NOT NULL DEFAULT 'default', type TEXT NOT NULL,
  payload TEXT NOT NULL CHECK (json_type(payload) = 'object')) STRICT`

// SQLite: the window by uuid time, then only the rows after the last
// rowid; the payload column is the JLH payload (W SPEC §3.7).
func TestSQLiteReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(sqliteDDL); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ins := func(u string, seq int) {
		if _, err := db.Exec(`INSERT INTO inbox (uuid, from_uuid, event_at, level, type, payload) VALUES (?, 'f', 'x', 9, 'webhook.request', ?)`, u, payloadOf(seq)); err != nil {
			t.Fatal(err)
		}
	}
	ins(v7(now.Add(-time.Hour), 1), 1) // before the window
	ins(v7(now, 2), 2)
	ins(v7(now, 3), 3)
	r, err := openSQLite(context.Background(), path, "inbox", now.Add(-Margin))
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if got := collect(t, r); len(got) != 2 || !strings.HasSuffix(got[0], " 2") || !strings.HasSuffix(got[1], " 3") {
		t.Fatalf("%q", got)
	}
	ins(v7(now, 4), 4)
	if got := collect(t, r); len(got) != 1 || !strings.HasSuffix(got[0], " 4") {
		t.Fatalf("second read: %q", got)
	}
	if _, err := openSQLite(context.Background(), path, "nope", now); err == nil {
		t.Fatal("missing table accepted")
	}
}

func TestPGReader(t *testing.T) {
	url := os.Getenv("WI_TEST_PG_URL")
	if url == "" {
		t.Skip("WI_TEST_PG_URL not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	tbl := fmt.Sprintf("wi_test_inbox_%d", time.Now().UnixNano())
	if _, err := c.Exec(ctx, "CREATE TABLE "+tbl+" (uuid uuid PRIMARY KEY, payload jsonb NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Exec(context.Background(), "DROP TABLE "+tbl) })
	now := time.Now()
	for i, u := range []string{v7(now.Add(-time.Hour), 1), v7(now, 2), v7(now, 3)} {
		if _, err := c.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1, $2)", u, payloadOf(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := openPG(ctx, url, tbl, now.Add(-Margin))
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if got := collect(t, r); len(got) != 2 || !strings.HasSuffix(got[0], " 2") {
		t.Fatalf("%q", got)
	}
	if _, err := openPG(ctx, url, "loghub.nope", now); err == nil {
		t.Fatal("missing table accepted")
	}
}

func testRedis(t *testing.T, env string) {
	url := os.Getenv(env)
	if url == "" {
		t.Skip(env + " not set")
	}
	ctx := context.Background()
	r, err := openRedis(ctx, url, "", time.Now().Add(-Margin))
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	key := fmt.Sprintf("loghub.witest%d.webhook.request", time.Now().UnixNano())
	other := key + "x" // matches the pattern too
	t.Cleanup(func() { r.c.Do(context.Background(), r.c.B().Del().Key(key, other).Build()) })
	add := func(k, u string, seq int) {
		if err := r.c.Do(ctx, r.c.B().Xadd().Key(k).Id("*").FieldValue().FieldValue("e", envJLH(u, seq)).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	add(key, v7(now, 1), 1)
	add(other, v7(now, 2), 2)
	got := collect(t, r)
	if !contains(got, v7(now, 1)+" 1") || !contains(got, v7(now, 2)+" 2") {
		t.Fatalf("%q", got)
	}
	// The same envelope again: a second entry, read after the cursor.
	add(key, v7(now, 1), 1)
	if got := collect(t, r); len(got) != 1 || got[0] != v7(now, 1)+" 1" {
		t.Fatalf("second read: %q", got)
	}
	// A key that is not a stream is refused with --verify-stream.
	r.c.Do(ctx, r.c.B().Set().Key(key+"str").Value("x").Build())
	t.Cleanup(func() { r.c.Do(context.Background(), r.c.B().Del().Key(key+"str").Build()) })
	if _, err := openRedis(ctx, url, key+"str", now); err == nil {
		t.Fatal("string key accepted")
	}
	_ = rueidis.Nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestRedisReader(t *testing.T)  { testRedis(t, "WI_TEST_REDIS_URL") }
func TestValkeyReader(t *testing.T) { testRedis(t, "WI_TEST_VALKEY_URL") }

func TestNATSReader(t *testing.T) {
	url := os.Getenv("WI_TEST_NATS_URL")
	if url == "" {
		t.Skip("WI_TEST_NATS_URL not set")
	}
	ctx := context.Background()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	name := fmt.Sprintf("WI_TEST_%d", time.Now().UnixNano())
	subj := "wi.test." + name
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
	now := time.Now()
	for i := 1; i <= 2500; i++ {
		if _, err := js.Publish(ctx, subj, []byte(envJLH(v7(now, i), i))); err != nil {
			t.Fatal(err)
		}
	}
	r, err := openNATS(ctx, url, name, now.Add(-Margin), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if got := collect(t, r); len(got) != 2500 {
		t.Fatalf("%d records", len(got))
	}
	js.Publish(ctx, subj, []byte(envJLH(v7(now, 1), 1)))
	if got := collect(t, r); len(got) != 1 || got[0] != v7(now, 1)+" 1" {
		t.Fatalf("second read: %q", got)
	}
	if _, err := openNATS(ctx, url, name+"_NOPE", now, 2*time.Second); err == nil {
		t.Fatal("missing stream accepted")
	}
}
