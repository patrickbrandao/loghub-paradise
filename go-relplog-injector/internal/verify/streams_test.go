package verify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
)

// The streams start after their last entry at the Probe, before the first
// send (SPEC §8.5; ported from go-gelf-injector, 2026-11-07): an envelope
// already there at the Probe is not read, however recent. Opt-in, against
// real servers:
//
//	RI_TEST_VERIFY_NATS=nats://127.0.0.1:38931 (JetStream on)
//	RI_TEST_VERIFY_REDIS=redis://127.0.0.1:38932/0,valkey://127.0.0.1:38933/0
//	RI_TEST_VERIFY_PG=postgres://postgres:pw@127.0.0.1:39502/postgres

// seqMatcher finds seq=N, the marker of envelope.
func seqMatcher(t *testing.T) *Matcher {
	t.Helper()
	m, err := NewMatcher(`seq=([0-9]+)`)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func envelope(space string, seq int) string {
	return fmt.Sprintf(`{"uuid":"u%d","space":%q,"type":"syslog","payload":{"message":"seq=%d"}}`, seq, space, seq)
}

// readAfterProbe runs the Probe, then write, then one reader as Check opens
// it, and returns the messages read.
func readAfterProbe(t *testing.T, tg *Target, before, after func()) []string {
	t.Helper()
	before()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := Probe(pctx, tg, "r", seqMatcher(t)); err != nil {
		t.Fatal(err)
	}
	after()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := open(ctx, tg, time.Now(), "r", false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []string
	if err := r.Read(ctx, func(e Entry) { got = append(got, e.Message) }); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestNATSStartsAfterLast(t *testing.T) {
	url := os.Getenv("RI_TEST_VERIFY_NATS")
	if url == "" {
		t.Skip("RI_TEST_VERIFY_NATS not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	name := fmt.Sprintf("RI_TEST_%d", time.Now().UnixNano())
	space := fmt.Sprintf("ri%d", time.Now().UnixNano())
	subj := "loghub." + space + ".syslog"
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj + ".>", subj}, Storage: jetstream.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
	tg, err := ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	tg.Override(space, name)
	pub := func(from, to int) {
		for seq := from; seq <= to; seq++ {
			if _, err := js.Publish(ctx, subj, []byte(envelope(space, seq))); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := readAfterProbe(t, tg, func() { pub(1, 3) }, func() { pub(4, 1503) })
	if len(got) != 1500 || got[0] != "seq=4" {
		t.Fatalf("read %d (want 1500, from seq=4)", len(got))
	}
}

func TestRedisStartsAfterLast(t *testing.T) {
	urls := os.Getenv("RI_TEST_VERIFY_REDIS")
	if urls == "" {
		t.Skip("RI_TEST_VERIFY_REDIS not set")
	}
	for _, u := range strings.Split(urls, ",") {
		t.Run(u, func(t *testing.T) {
			tg, err := ParseURL(u)
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("ri.test.%d", time.Now().UnixNano())
			tg.Override("", key)
			opt, _ := rueidis.ParseURL(tg.url)
			opt.DisableCache = true
			cl, err := rueidis.NewClient(opt)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			ctx := context.Background()
			t.Cleanup(func() { cl.Do(context.Background(), cl.B().Del().Key(key).Build()) })
			add := func(seq int) {
				if err := cl.Do(ctx, cl.B().Xadd().Key(key).Id("*").FieldValue().FieldValue("e", envelope("default", seq)).Build()).Error(); err != nil {
					t.Fatal(err)
				}
			}
			// Not born at the Probe: read whole.
			if got := readAfterProbe(t, tg, func() {}, func() { add(1) }); len(got) != 1 || got[0] != "seq=1" {
				t.Fatalf("new stream: %q", got)
			}
			// Existing: read after its last entry.
			if got := readAfterProbe(t, tg, func() { add(2) }, func() { add(3); add(4) }); len(got) != 2 || got[0] != "seq=3" {
				t.Fatalf("existing stream: %q (want seq=3, seq=4)", got)
			}
		})
	}
}

// natsSpace creates a stream on loghub.<space>.syslog and .syslog.> (the
// server's LOGHUB_SYSLOG shape) for one test.
func natsSpace(t *testing.T) (*Target, jetstream.JetStream, string) {
	t.Helper()
	url := os.Getenv("RI_TEST_VERIFY_NATS")
	if url == "" {
		t.Skip("RI_TEST_VERIFY_NATS not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	name := fmt.Sprintf("RI_TEST_%d", time.Now().UnixNano())
	space := fmt.Sprintf("ri%d", time.Now().UnixNano())
	subj := "loghub." + space + ".syslog"
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: []string{subj, subj + ".>"}, Storage: jetstream.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
	tg, err := ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	tg.Override(space, name)
	return tg, js, subj
}

// TestNATSFacilitySeverity (finding 11): subjects=facility-severity publishes
// below loghub.<space>.syslog.>; the filter covers both.
func TestNATSFacilitySeverity(t *testing.T) {
	tg, js, subj := natsSpace(t)
	ctx := context.Background()
	got := readAfterProbe(t, tg, func() {}, func() {
		for seq := 1; seq <= 10; seq++ {
			s := subj + ".user.notice"
			if seq == 10 {
				s = subj
			}
			if _, err := js.Publish(ctx, s, []byte(envelope("x", seq))); err != nil {
				t.Fatal(err)
			}
		}
	})
	if len(got) != 10 {
		t.Fatalf("read %d of 10", len(got))
	}
}

// TestNATSForeignTraffic (finding 2): with constant traffic of another load on
// the subject the batch is never empty; a read still ends, and the check
// completes.
func TestNATSForeignTraffic(t *testing.T) {
	tg, js, subj := natsSpace(t)
	m := seqMatcher(t)
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	if err := Probe(pctx, tg, "r", m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // another load, as fast as it goes, with no seq= marker
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			js.PublishAsync(subj, []byte(`{"uuid":"f","payload":{"message":"foreign"}}`))
			if i%500 == 0 {
				<-js.PublishAsyncComplete()
			}
		}
	}()
	defer wg.Wait()
	defer cancel()
	time.Sleep(200 * time.Millisecond)
	for seq := 1; seq <= 20; seq++ {
		if _, err := js.Publish(context.Background(), subj, []byte(envelope("x", seq))); err != nil {
			t.Fatal(err)
		}
	}
	v := Check(context.Background(), tg, m, 20, ackedAll(20), Options{Timeout: 20 * time.Second})
	if !v.Complete || v.Found != 20 || v.Unreadable || v.WaitedSec > 15 {
		t.Fatalf("%+v", v)
	}
}

// TestPostgresRunPresent (finding 4): rows of --run already in the window
// refuse the run at the Probe.
func TestPostgresRunPresent(t *testing.T) {
	u := os.Getenv("RI_TEST_VERIFY_PG")
	if u == "" {
		t.Skip("RI_TEST_VERIFY_PG not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	table := fmt.Sprintf("ri_inbox_%d", time.Now().UnixNano())
	if _, err := c.Exec(ctx, `CREATE TABLE `+table+` (uuid uuid DEFAULT gen_random_uuid(), type text, space text, received_at timestamptz, payload jsonb)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Exec(context.Background(), `DROP TABLE `+table) })
	if _, err := c.Exec(ctx, `INSERT INTO `+table+` (type, space, received_at, payload) VALUES ('syslog', 'default', now(), '{"message": "seq=1 run=used "}')`); err != nil {
		t.Fatal(err)
	}
	probe := func(run string) error {
		tg, err := ParseURL(u)
		if err != nil {
			t.Fatal(err)
		}
		tg.Override("", "public."+table)
		m, err := NewMatcher(`seq=([0-9]+) run=` + run + ` `)
		if err != nil {
			t.Fatal(err)
		}
		return Probe(ctx, tg, run, m)
	}
	if err := probe("used"); !errors.Is(err, ErrRunPresent) {
		t.Fatalf("reused run: %v", err)
	}
	if err := probe("use"); err != nil {
		t.Fatalf("a new run: %v", err)
	}
}
