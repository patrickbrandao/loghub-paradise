package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
)

func TestParse(t *testing.T) {
	for raw, want := range map[string]string{
		"nats://u:secret@h:4222":                   "nats",
		"tls://h:4222":                             "nats",
		"postgres://u:secret@h/db?sslmode=disable": "postgres",
		"postgresql://h/db":                        "postgres",
		"redis://:secret@h:6379/1?protocol=2":      "redis",
		"rediss://h:6379":                          "redis",
		"valkey://h:6379":                          "redis",
		"valkeys://h:6379":                         "redis",
		"postgres:///db?host=/run/postgresql":      "postgres",
	} {
		s, err := Parse(raw, "default", "")
		if err != nil || s.Kind != want {
			t.Fatalf("%s: %v %+v", raw, err, s)
		}
		if strings.Contains(s.Display, "secret") {
			t.Fatalf("%s: password shown: %s", raw, s.Display)
		}
	}
	for _, raw := range []string{"amqp://h", "nats://h?x=1", "redis://", "http://h", "::"} {
		if _, err := Parse(raw+"#pw-secret", "default", ""); err == nil {
			t.Fatalf("%s accepted", raw)
		} else if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error echoes the URL: %v", err)
		}
	}
	s, _ := Parse("valkeys://u:pw@h:1/2?dial_timeout=1s", "a.b", "k")
	if s.raw != "rediss://u:pw@h:1/2?dial_timeout=1s" || s.stream != "k" || s.Display != "valkeys://h:1 (stream k)" {
		t.Fatalf("valkeys: %s %s %s", s.raw, s.stream, s.Display)
	}
	s, _ = Parse("redis://h:1", "a.b", "")
	if s.stream != "loghub.a.b.gelf" {
		t.Fatalf("redis key %s", s.stream)
	}
	s, _ = Parse("nats://tok@h:1", "x", "S")
	if s.raw != "nats://tok@h:1" || s.stream != "S" || s.subject != "loghub.x.gelf" || strings.Contains(s.Display, "tok") {
		t.Fatalf("nats: %+v", s)
	}
	for _, c := range [][3]string{{"nats://h", "bad space", ""}, {"postgres://h/db", "default", "k"}, {"nats://h?stream=x", "default", ""}} {
		if _, err := Parse(c[0], c[1], c[2]); err == nil {
			t.Fatalf("%v accepted", c)
		}
	}
	s, _ = Parse("postgres://u:p%40ss@h/db", "default", "")
	if s.Display != "postgres://h (loghub.inbox, space default)" {
		t.Fatalf("display %s", s.Display)
	}
	if got := s.Err(errors.New("auth failed for p@ss and p%40ss")).Error(); strings.Contains(got, "ss ") || strings.Contains(got, "p@ss") {
		t.Fatalf("scrub: %s", got)
	}
}

func TestAddJLH(t *testing.T) {
	c := NewCounter(2)
	for _, j := range []string{
		`{"type":"gelf","payload":{"fields":{"seq":1,"run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"seq":"2","run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"seq":2,"run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"seq":3,"run":"other"}}}`,
		`{"type":"syslog","payload":{"fields":{"seq":4,"run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"seq":1.5,"run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"seq":0,"run":"r1"}}}`,
		`{"type":"gelf","payload":{"fields":{"run":"r1"}}}`,
		`not json`,
	} {
		c.AddJLH([]byte(j), "r1")
	}
	if c.Count(1) != 1 || c.Count(2) != 2 || c.Count(3) != 0 || c.extra != 1 || c.Unparsable != 3 {
		t.Fatalf("%+v", c)
	}
}

// Every outcome class of SPEC §10.1, and what makes a run bad.
func TestClassify(t *testing.T) {
	var tr Tracker
	for seq := uint64(1); seq <= 6; seq++ {
		tr.Done(seq, seq != 5 && seq != 6) // 5 and 6 failed
	}
	c := NewCounter(6)
	// 4 lost, 2 three times, 5 failed but found, 6 failed and missing, 9
	// never produced (twice).
	for _, s := range []string{"1", "2", "2", "2", "3", "5", "9", "9"} {
		c.AddSeq(s)
	}
	r := Classify(tr.snapshot(6), c)
	// The names and meanings of go-syslog-injector (review of 2026-11-07):
	// duplicated is the copies beyond the first (2 of seq 2, 1 of seq 9),
	// unexpected the seqs found that are no success (5 and 9).
	want := Result{Expected: 4, Found: 3, Lost: 1, Duplicated: 3, FailedFound: 1, FailedMissing: 1,
		Unexpected: 2, LostSeqs: []string{"4"}, DuplicatedSample: []uint64{2, 9}, UnexpectedSample: []uint64{5, 9}}
	if !reflect.DeepEqual(r, want) || !r.Bad() {
		t.Fatalf("got  %+v\nwant %+v", r, want)
	}
	c = NewCounter(6)
	for _, s := range []string{"1", "2", "3", "4"} {
		c.AddSeq(s)
	}
	if r := Classify(tr.snapshot(6), c); r.Bad() || r.Lost != 0 || r.FailedMissing != 2 {
		t.Fatalf("clean run is bad: %+v", r)
	}
	// unexpected and unparsable are informative: no exit 1 for them alone.
	c = NewCounter(6)
	for _, s := range []string{"1", "2", "3", "4", "5", "77", "x"} {
		c.AddSeq(s)
	}
	if r := Classify(tr.snapshot(6), c); r.Bad() || r.Unexpected != 2 || r.Unparsable != 1 {
		t.Fatalf("informative counters made the run bad: %+v", r)
	}
}

// The floor sorts below every UUIDv7 of its millisecond and later, as text
// and as bytes (PostgreSQL compares uuid bytewise).
func TestUUIDFloor(t *testing.T) {
	t0 := time.UnixMilli(1791028800123)
	f := uuidFloor(t0)
	// 1791028800123 ms = 0x01a101a2aa7b: the first 48 bits of the UUIDv7
	// of go-gelf-server SPEC §3.1's example.
	if f != "01a101a2-aa7b-0000-0000-000000000000" {
		t.Fatalf("floor %s", f)
	}
	later := uuidFloor(t0.Add(time.Millisecond))
	if !(f < later) || len(f) != 36 || f[14] != '0' {
		t.Fatalf("%s %s", f, later)
	}
	v7 := strings.Replace(f, "-0000-0000-", "-7abc-8def-", 1)
	if !(v7 > f) {
		t.Fatalf("a v7 of the same ms sorts below the floor: %s", v7)
	}
}

// fakeDest returns its counts after n reads.
type fakeDest struct {
	reads, ready int
	seqs         []string
	err          error
}

func (f *fakeDest) Check(context.Context) error { return nil }
func (f *fakeDest) Incremental() bool           { return false }
func (f *fakeDest) Close()                      {}
func (f *fakeDest) Read(_ context.Context, c *Counter) error {
	f.reads++
	if f.err != nil {
		return f.err
	}
	if f.reads >= f.ready {
		for _, s := range f.seqs {
			c.AddSeq(s)
		}
	}
	return nil
}

// The wait polls until every success is found, then reads once more; past
// the timeout what is missing is lost; no good read at all is a failure.
func TestRunWaits(t *testing.T) {
	var tr Tracker
	tr.Done(1, true)
	tr.Done(2, true)
	s, _ := SQLite("x", "default")
	d := &fakeDest{ready: 3, seqs: []string{"1", "2"}}
	r := Run(context.Background(), s, d, "r", &tr, 2, 5*time.Second)
	if !r.Complete || r.Found != 2 || d.reads != 4 || r.Bad() || r.Waited < Settle.Seconds() {
		t.Fatalf("%+v reads %d", r, d.reads)
	}
	d = &fakeDest{ready: 1, seqs: []string{"1"}}
	r = Run(context.Background(), s, d, "r", &tr, 2, 1500*time.Millisecond)
	if r.Complete || r.Lost != 1 || !r.Bad() || r.Failed {
		t.Fatalf("%+v", r)
	}
	d = &fakeDest{err: errors.New("down")}
	r = Run(context.Background(), s, d, "r", &tr, 2, 300*time.Millisecond)
	if !r.Failed || r.Error != "down" || r.Found != 0 {
		t.Fatalf("%+v", r)
	}
}

// ---- the destinations, against what go-gelf-server writes ----

func jlhOf(run string, seq int, uuid string) string {
	return fmt.Sprintf(`{"uuid":"%s","from":"0196f9bb-ad7b-7000-8000-000000000001","space":"default","type":"gelf","level":6,"sid":1,"step":0,`+
		`"event":{"uts":1,"sub":0,"iso":"x"},"payload":{"host":{"src-ip":"127.0.0.1","src-port":1,"dst-port":12201},"transport":"tcp",`+
		`"format":"gelf","level":{"code":6,"name":"info"},"short-message":"seq=%d","fields":{"seq":%d,"run":"%s"},"size":10}}`, uuid, seq, seq, run)
}

func uuidAt(t time.Time, n int) string {
	f := uuidFloor(t)
	return f[:14] + fmt.Sprintf("7%03x-8%03x-%012x", n%4096, n%4096, n)
}

// checkDest writes run "vt" seqs 1..5 with 3 twice and 4 missing, plus a
// record of another run, and expects the check to see exactly that.
func checkDest(t *testing.T, s *Spec, write func(run string, seq int)) {
	t.Helper()
	run := fmt.Sprintf("vt%d", time.Now().UnixNano())
	since := time.Now().Add(-Window)
	d := s.Open(run, since)
	defer d.Close()
	if err := d.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []int{1, 2, 3, 3, 5} {
		write(run, seq)
	}
	write("other"+run, 4)
	var tr Tracker
	for seq := uint64(1); seq <= 5; seq++ {
		tr.Done(seq, true)
	}
	r := Run(context.Background(), s, d, run, &tr, 5, 2*time.Second)
	if r.Failed || r.Found != 4 || r.Lost != 1 || r.Duplicated != 1 || r.Unexpected != 0 ||
		!reflect.DeepEqual(r.LostSeqs, []string{"4"}) {
		t.Fatalf("%+v", r)
	}
}

// A stream is read from after its last entry at the check: a record written
// before it (here, of the same run) is not the run's (SPEC §10.3).
func checkStartsAfterLast(t *testing.T, s *Spec, write func(run string, seq int)) {
	t.Helper()
	run := fmt.Sprintf("va%d", time.Now().UnixNano())
	write(run, 1)
	d := s.Open(run, time.Now().Add(-Window))
	defer d.Close()
	if err := d.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	write(run, 2)
	var tr Tracker
	tr.Done(1, true)
	tr.Done(2, true)
	r := Run(context.Background(), s, d, run, &tr, 2, 1500*time.Millisecond)
	if r.Found != 1 || !reflect.DeepEqual(r.LostSeqs, []string{"1"}) {
		t.Fatalf("read before the check: %+v", r)
	}
}

func TestSQLiteDest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// go-gelf-server SPEC §3.7.
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS gelf_events (
  uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
  level INTEGER NOT NULL, sid INTEGER NOT NULL,
  ev_uts INTEGER NOT NULL, ev_sub INTEGER NOT NULL,
  format TEXT NOT NULL, transport TEXT NOT NULL,
  src_ip TEXT, hostname TEXT,
  env BLOB NOT NULL, nbytes INTEGER NOT NULL,
  PRIMARY KEY (uuid, step)) STRICT`); err != nil {
		t.Fatal(err)
	}
	n := 0
	old := uuidAt(time.Now().Add(-2*Window), 9)
	if _, err := db.Exec(`INSERT INTO gelf_events VALUES (?,0,'default',6,1,1,0,'gelf','tcp',NULL,NULL,?,1)`, old, []byte(jlhOf("x", 1, old))); err != nil {
		t.Fatal(err)
	}
	sq, _ := SQLite(path, "default")
	checkDest(t, sq, func(run string, seq int) {
		n++
		u := uuidAt(time.Now(), n)
		env := jlhOf(run, seq, u)
		if _, err := db.Exec(`INSERT INTO gelf_events VALUES (?,0,'default',6,1,1,0,'gelf','tcp','127.0.0.1',NULL,?,?)`, u, []byte(env), len(env)); err != nil {
			t.Fatal(err)
		}
	})
	// Read-only: a missing file is an error, never an empty new database.
	nsq, _ := SQLite(filepath.Join(t.TempDir(), "none.db"), "default")
	d := nsq.Open("r", time.Now())
	if err := d.Check(context.Background()); err == nil {
		t.Fatal("missing file opened")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "none.db")); err == nil {
		t.Fatal("created")
	}
}

// GELF_TEST_VERIFY_PG=postgres://postgres:pw@127.0.0.1:38822/postgres?sslmode=disable
// with docker/postgres/inbox.sql of go-gelf-server loaded.
func TestPostgresDest(t *testing.T) {
	url := os.Getenv("GELF_TEST_VERIFY_PG")
	if url == "" {
		t.Skip("GELF_TEST_VERIFY_PG not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	s, err := Parse(url, "default", "")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	checkDest(t, s, func(run string, seq int) {
		n++
		u := uuidAt(time.Now(), n)
		var payload string
		env := jlhOf(run, seq, u)
		payload = env[strings.Index(env, `"payload":`)+10 : len(env)-1]
		if _, err := conn.Exec(ctx, `INSERT INTO loghub.inbox (uuid, from_uuid, event_at, level, type, payload)
			VALUES ($1, '0196f9bb-ad7b-7000-8000-000000000001', now(), 6, 'gelf', $2::jsonb)`, u, payload); err != nil {
			t.Fatal(err)
		}
	})
}

// GELF_TEST_VERIFY_REDIS=redis://127.0.0.1:38823,valkey://127.0.0.1:38824
func TestRedisDest(t *testing.T) {
	urls := os.Getenv("GELF_TEST_VERIFY_REDIS")
	if urls == "" {
		t.Skip("GELF_TEST_VERIFY_REDIS not set")
	}
	for _, u := range strings.Split(urls, ",") {
		t.Run(u, func(t *testing.T) {
			key := fmt.Sprintf("vt.%d.gelf", time.Now().UnixNano())
			s, err := Parse(u, "default", key)
			if err != nil {
				t.Fatal(err)
			}
			opt, _ := rueidis.ParseURL(s.raw)
			opt.DisableCache = true
			cl, err := rueidis.NewClient(opt)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			ctx := context.Background()
			n := 0
			write := func(run string, seq int) {
				n++
				if err := cl.Do(ctx, cl.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("e", jlhOf(run, seq, uuidAt(time.Now(), n))).Build()).Error(); err != nil {
					t.Fatal(err)
				}
			}
			checkDest(t, s, write)
			checkStartsAfterLast(t, s, write)
			// A key of another type is refused at the check.
			_ = cl.Do(ctx, cl.B().Set().Key(key+"s").Value("x").Build()).Error()
			s2, _ := Parse(u, "default", key+"s")
			if err := s2.Open("r", time.Now()).Check(ctx); err == nil {
				t.Fatal("string key accepted")
			}
		})
	}
}

// GELF_TEST_VERIFY_NATS=nats://127.0.0.1:38821 (JetStream on). The stream of
// docker/nats/stream.json is created when missing.
func TestNATSDest(t *testing.T) {
	url := os.Getenv("GELF_TEST_VERIFY_NATS")
	if url == "" {
		t.Skip("GELF_TEST_VERIFY_NATS not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "LOGHUB_GELF", Subjects: []string{"loghub.*.gelf"},
		Storage: jetstream.FileStorage, Duplicates: 10 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	s, _ := Parse(url, "default", "")
	n := 0
	write := func(run string, seq int) {
		n++
		u := uuidAt(time.Now(), n)
		m := nats.NewMsg("loghub.default.gelf")
		m.Header.Set("Nats-Msg-Id", u+".0")
		m.Data = []byte(jlhOf(run, seq, u))
		if _, err := js.PublishMsg(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	checkDest(t, s, write)
	checkStartsAfterLast(t, s, write)
	s2, _ := Parse(url, "default", "NOPE")
	if err := s2.Open("r", time.Now()).Check(ctx); err == nil {
		t.Fatal("missing stream accepted")
	}
}

// The lost seqs come as ranges, at most 20.
func TestLostSeqs(t *testing.T) {
	var tr Tracker
	for seq := uint64(1); seq <= 200; seq++ {
		tr.Done(seq, true)
	}
	c := NewCounter(12)
	for _, s := range []string{"1", "5", "6", "9"} {
		c.AddSeq(s)
	}
	r := Classify(tr.snapshot(12), c)
	if strings.Join(r.LostSeqs, ",") != "2-4,7-8,10-12" {
		t.Fatalf("%v", r.LostSeqs)
	}
	c = NewCounter(200)
	for s := 2; s <= 200; s += 2 {
		c.AddSeq(fmt.Sprint(s))
	}
	if r = Classify(tr.snapshot(200), c); len(r.LostSeqs) != 20 || r.LostSeqs[19] != "39" || r.Lost != 100 {
		t.Fatalf("%d %v", len(r.LostSeqs), r.LostSeqs)
	}
}
