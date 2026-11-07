package verify

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/rueidis"
)

// The streams start after their last entry at Open, the check before the
// first send (SPEC §8.5; ported from go-gelf-injector, 2026-11-07): an
// envelope of the run already there at Open is not read, however recent.
// Opt-in, against real servers:
//
//	SI_TEST_VERIFY_NATS=nats://127.0.0.1:38931 (JetStream on)
//	SI_TEST_VERIFY_REDIS=redis://127.0.0.1:38932/0,valkey://127.0.0.1:38933/0

func envelope(space, run string, seq int) string {
	return fmt.Sprintf(`{"space":%q,"type":"syslog","payload":{"message":"seq=%d run=%s"}}`, space, seq, run)
}

func pollAll(t *testing.T, r reader, max uint64) *Counter {
	t.Helper()
	c := NewCounter(max)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.poll(ctx, c); err != nil {
		t.Fatal(err)
	}
	return c
}

// natsStream creates a stream on subjects for one test; it returns the
// JetStream context, the stream name and a space no other stream holds.
func natsStream(t *testing.T, subjects func(space string) []string) (string, jetstream.JetStream, string, string) {
	t.Helper()
	url := os.Getenv("SI_TEST_VERIFY_NATS")
	if url == "" {
		t.Skip("SI_TEST_VERIFY_NATS not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	name := fmt.Sprintf("SI_TEST_%d", time.Now().UnixNano())
	space := fmt.Sprintf("si%d", time.Now().UnixNano())
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: subjects(space)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
	return url, js, name, space
}

func exact(space string) []string { return []string{"loghub." + space + ".syslog"} }

func TestNATSStartsAfterLast(t *testing.T) {
	url, js, name, space := natsStream(t, exact)
	ctx := context.Background()
	subj := "loghub." + space + ".syslog"
	run := fmt.Sprintf("r%d", time.Now().UnixNano())
	for seq := 1; seq <= 3; seq++ {
		if _, err := js.Publish(ctx, subj, []byte(envelope(space, run, seq))); err != nil {
			t.Fatal(err)
		}
	}
	r, err := openNATS(ctx, &Target{Kind: "nats", URL: url, Space: space, Key: name, Run: run, Since: time.Now().Add(-ClockSlack)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	for seq := 4; seq <= 1503; seq++ { // more than one Fetch batch
		if _, err := js.Publish(ctx, subj, []byte(envelope(space, run, seq))); err != nil {
			t.Fatal(err)
		}
	}
	c := pollAll(t, r, 1503)
	if c.records != 1500 || c.n(1) != 0 || c.n(4) != 1 || c.n(1503) != 1 {
		t.Fatalf("read %d records (want 4..1503, none from before Open; seq 1 seen %d)", c.records, c.n(1))
	}
}

// TestNATSFacilitySeverity (SPEC §8.5, review finding 1): the server's stream
// covers loghub.<space>.syslog and loghub.<space>.syslog.>; with
// subjects=facility-severity it publishes below the second. The lookup finds
// the stream by the first subject; the filter must still be both.
func TestNATSFacilitySeverity(t *testing.T) {
	url, js, _, space := natsStream(t, func(space string) []string {
		return []string{"loghub." + space + ".syslog", "loghub." + space + ".syslog.>"}
	})
	ctx := context.Background()
	run := fmt.Sprintf("r%d", time.Now().UnixNano())
	r, err := openNATS(ctx, &Target{Kind: "nats", URL: url, Space: space, Run: run})
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	for seq := 1; seq <= 10; seq++ {
		subj := "loghub." + space + ".syslog.user.notice"
		if seq == 10 {
			subj = "loghub." + space + ".syslog" // and one of subjects=exact
		}
		if _, err := js.Publish(ctx, subj, []byte(envelope(space, run, seq))); err != nil {
			t.Fatal(err)
		}
	}
	if c := pollAll(t, r, 10); c.records != 10 {
		t.Fatalf("read %d of 10", c.records)
	}
}

// TestNATSForeignTraffic (SPEC §8.5, review finding 2): with constant traffic
// of another run on the subject the batch is never empty; a read still ends
// (NumPending 0, or readSlice), and the wait completes.
func TestNATSForeignTraffic(t *testing.T) {
	url, js, name, space := natsStream(t, exact)
	ctx, cancel := context.WithCancel(context.Background())
	subj := "loghub." + space + ".syslog"
	run := fmt.Sprintf("r%d", time.Now().UnixNano())
	tg := &Target{Kind: "nats", URL: url, Space: space, Key: name, Run: run}
	chk, err := Open(ctx, tg)
	if err != nil {
		t.Fatal(err)
	}
	defer chk.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // another load, as fast as it goes
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			js.PublishAsync(subj, []byte(envelope(space, "foreign", i+1)))
			if i%500 == 0 {
				<-js.PublishAsyncComplete()
			}
		}
	}()
	defer wg.Wait()
	defer cancel()
	time.Sleep(200 * time.Millisecond)
	for seq := 1; seq <= 20; seq++ {
		if _, err := js.Publish(context.Background(), subj, []byte(envelope(space, run, seq))); err != nil {
			t.Fatal(err)
		}
	}
	v := chk.Wait(context.Background(), seqs(20), 20*time.Second)
	if !v.Complete || v.Found != 20 || v.Error != "" || v.WaitedSec > 15 {
		t.Fatalf("%+v", v)
	}
}

func TestRedisStartsAfterLast(t *testing.T) {
	urls := os.Getenv("SI_TEST_VERIFY_REDIS")
	if urls == "" {
		t.Skip("SI_TEST_VERIFY_REDIS not set")
	}
	for _, u := range strings.Split(urls, ",") {
		t.Run(u, func(t *testing.T) {
			tg, err := Parse(u, "", "default", fmt.Sprintf("si.test.%d", time.Now().UnixNano()), fmt.Sprintf("r%d", time.Now().UnixNano()), "")
			if err != nil {
				t.Fatal(err)
			}
			tg.Since = time.Now().Add(-ClockSlack)
			opt, _ := rueidis.ParseURL(tg.URL)
			opt.DisableCache = true
			cl, err := rueidis.NewClient(opt)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			ctx := context.Background()
			t.Cleanup(func() { cl.Do(context.Background(), cl.B().Del().Key(tg.Key).Build()) })
			add := func(seq int) {
				if err := cl.Do(ctx, cl.B().Xadd().Key(tg.Key).Id("*").FieldValue().FieldValue("e", envelope("default", tg.Run, seq)).Build()).Error(); err != nil {
					t.Fatal(err)
				}
			}
			// A stream that never appears is everything lost (exit 1), not
			// "cannot check" (SPEC §8.5, alignment of 2026-11-07).
			chk, err := Open(ctx, tg)
			if err != nil {
				t.Fatal(err)
			}
			if v := chk.Wait(ctx, seqs(2), 600*time.Millisecond); v.Error != "" || v.Lost != 2 {
				t.Fatalf("never appeared: %+v", v)
			}
			chk.Close()
			// A stream not born at Open is read whole.
			r0, err := openRedis(ctx, tg)
			if err != nil {
				t.Fatal(err)
			}
			add(1)
			if c := pollAll(t, r0, 3); c.records != 1 || c.n(1) != 1 {
				t.Fatalf("new stream: %+v", c)
			}
			r0.close()
			// One that exists is read after its last entry.
			r, err := openRedis(ctx, tg)
			if err != nil {
				t.Fatal(err)
			}
			defer r.close()
			add(2)
			add(3)
			if c := pollAll(t, r, 3); c.records != 2 || c.n(1) != 0 {
				t.Fatalf("existing stream: %+v (want 2 and 3 only)", c)
			}
		})
	}
}
