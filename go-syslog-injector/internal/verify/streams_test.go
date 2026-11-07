package verify

import (
	"context"
	"fmt"
	"os"
	"strings"
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

func pollAll(t *testing.T, r reader) *Counter {
	t.Helper()
	c := NewCounter()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.poll(ctx, c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNATSStartsAfterLast(t *testing.T) {
	url := os.Getenv("SI_TEST_VERIFY_NATS")
	if url == "" {
		t.Skip("SI_TEST_VERIFY_NATS not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()
	name := fmt.Sprintf("SI_TEST_%d", time.Now().UnixNano())
	space := fmt.Sprintf("si%d", time.Now().UnixNano()) // a subject no other stream holds
	subj := "loghub." + space + ".syslog"
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subj}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
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
	c := pollAll(t, r)
	if len(c.seen) != 1500 || c.seen[1] != 0 || c.seen[4] != 1 || c.seen[1503] != 1 {
		t.Fatalf("read %d seqs (want 4..1503, none from before Open; seq 1 seen %d)", len(c.seen), c.seen[1])
	}
}

func TestRedisStartsAfterLast(t *testing.T) {
	urls := os.Getenv("SI_TEST_VERIFY_REDIS")
	if urls == "" {
		t.Skip("SI_TEST_VERIFY_REDIS not set")
	}
	for _, u := range strings.Split(urls, ",") {
		t.Run(u, func(t *testing.T) {
			tg, err := Parse(u, "", "default", fmt.Sprintf("si.test.%d", time.Now().UnixNano()), fmt.Sprintf("r%d", time.Now().UnixNano()))
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
			// A stream not born at Open is read whole.
			r0, err := openRedis(ctx, tg)
			if err != nil {
				t.Fatal(err)
			}
			add(1)
			if c := pollAll(t, r0); len(c.seen) != 1 || c.seen[1] != 1 {
				t.Fatalf("new stream: %v", c.seen)
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
			if c := pollAll(t, r); len(c.seen) != 2 || c.seen[1] != 0 {
				t.Fatalf("existing stream: %v (want 2 and 3 only)", c.seen)
			}
			if err := r.final(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
