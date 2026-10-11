package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ring"
)

// What is worth proving here -- the async publish with futures, the server's
// deduplication by Nats-Msg-Id <uuid>.<step>, the expected-stream refusal, the
// stream validation by local subject matching, the cover check of a foreign
// subject, core mode, partial delivery, the connect hooks of the beacon, and
// the largest message the boot check admits actually fitting the server -- is
// exactly what a fake would have to invent. These run against a real server
// with JetStream, addressed by WHS_TEST_NATS_URL, and skip without it:
//
//	go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
//	WHS_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race ./internal/sink/
//
// Tests that need a server WITHOUT JetStream read WHS_TEST_NATS_NOJS_URL:
//
//	go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &
//
// Every test works in its own space, with its own stream created from the
// canonical stream of nats_test.go and deleted afterwards.

const natsURLEnv = "WHS_TEST_NATS_URL"

type natsFixture struct {
	t      *testing.T
	url    string
	nc     *nats.Conn
	js     jetstream.JetStream
	space  string
	stream string
	ns     string
	cfg    NATSConfig
}

func natsSetup(t *testing.T) *natsFixture {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the nats sink tests", natsURLEnv)
	}
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("%s is not reachable: %v", natsURLEnv, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	f := &natsFixture{t: t, url: url, nc: nc, js: js}
	f.space, f.stream = f.newStream(nil)
	f.ns = fmt.Sprintf("whs_test_ctl_%d", time.Now().UnixNano())
	f.cfg = NATSConfig{
		URLs: []string{url}, Stream: f.stream, Workspace: f.space, CtlNamespace: f.ns,
		ClientName: "whs-test", ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second, MaxPending: 4096,
	}
	return f
}

// newStream creates a stream from the canonical config (natsCanonicalStream)
// for a fresh space, and registers its removal. mut adjusts the config first.
func (f *natsFixture) newStream(mut func(*jetstream.StreamConfig)) (space, name string) {
	f.t.Helper()
	n := time.Now().UnixNano()
	space, name = fmt.Sprintf("t%d", n), fmt.Sprintf("WHS_TEST_%d", n)
	cfg := natsCanonicalStream(name, space)
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.CreateStream(ctx, cfg); err != nil {
		f.t.Fatalf("creating stream %s: %v", name, err)
	}
	f.t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
	return space, name
}

func (f *natsFixture) sink(cfg NATSConfig) *NATS {
	f.t.Helper()
	p, err := NewNATS(cfg, quietLog(), testMetrics())
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { p.Close() })
	return p
}

func (f *natsFixture) ready(p *NATS) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Health(ctx); err != nil {
		f.t.Fatalf("Health: %v", err)
	}
}

func (f *natsFixture) items(space string, n int) []*envelope.Item {
	out := make([]*envelope.Item, n)
	for i := range out {
		out[i] = itemIn(space, testRequest(fmt.Sprintf("/nats/%d", i), []byte(fmt.Sprintf(`{"n":%d}`, i))))
	}
	return out
}

// messages reads every message of a stream back, in sequence order.
func (f *natsFixture) messages(stream string) []*jetstream.RawStreamMsg {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := f.js.Stream(ctx, stream)
	if err != nil {
		f.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	var out []*jetstream.RawStreamMsg
	if info.State.Msgs == 0 {
		return out
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out
}

func (f *natsFixture) count(stream string) uint64 {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := f.js.Stream(ctx, stream)
	if err != nil {
		f.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return info.State.Msgs
}

// sizedItemIn is an item of space whose envelope is exactly n bytes: the body
// is text, one byte per byte, so the envelope grows one for one with it --
// plus the digits of content.length, which the second pass corrects. One clock
// sample for every pass: a fresh one changed the envelope's length by a byte
// or two now and then, and the passes did not converge.
func sizedItemIn(t *testing.T, space string, n int) *envelope.Item {
	t.Helper()
	now := time.Now()
	body := n - len(itemInAt(space, testRequest("/sized", nil), now).Env)
	for range 3 {
		if body < 0 {
			t.Fatalf("an envelope of %d bytes is below the empty one", n)
		}
		it := itemInAt(space, testRequest("/sized", bytes.Repeat([]byte{'x'}, body)), now)
		if len(it.Env) == n {
			return it
		}
		body += n - len(it.Env)
	}
	t.Fatalf("no body gives an envelope of exactly %d bytes", n)
	return nil
}

// ------------------------------------------------------------------ writes

// SPEC §3.8: Data is the JLH and nothing else; the five headers; the exact
// subject of the envelope's space; one message per envelope; the items come
// back released cleanly by the worker afterwards.
func TestNATSPublishStoresTheJLH(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	hostile := testRequest("/hostile", []byte("A\x00B\xff\n"))
	hostile.RawQuery = "q=a%00b"
	items := append(f.items(f.space, 4), itemIn(f.space, hostile))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	if len(got) != len(items) {
		t.Fatalf("%d messages, want %d", len(got), len(items))
	}
	for i, m := range got {
		it := items[i]
		if m.Subject != "loghub."+f.space+".webhook" {
			t.Errorf("subject = %q", m.Subject)
		}
		for k, v := range map[string]string{
			"Nats-Msg-Id": it.ID + ".0", "Content-Type": "application/json",
			"X-LogHub-Type": "webhook", "X-LogHub-UUID": it.ID, "X-LogHub-Space": f.space,
		} {
			if got := m.Header.Get(k); got != v {
				t.Errorf("message %d: %s = %q, want %q", i, k, got, v)
			}
		}
		if !bytes.Equal(m.Data, it.Env) {
			t.Errorf("message %d: Data is not the JLH byte for byte", i)
		}
		if _, err := jlhcheck.Validate(m.Data); err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
	for i, it := range items {
		if it.Released() {
			t.Errorf("item %d released by the sink: ownership moves only in the worker", i)
		}
	}
}

// SPEC §8.7.3: the pipeline is at-least-once, and a redelivery inside the
// duplicate window is acknowledged as a duplicate, stored once and counted.
func TestNATSRedeliveryIsDeduplicatedAndCounted(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	items := f.items(f.space, 7)
	ctx := context.Background()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	mixed := append(append([]*envelope.Item{}, items[:3]...), f.items(f.space, 2)...)
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	if n := f.count(f.stream); n != 9 {
		t.Errorf("%d messages, want 9: redelivery must not duplicate", n)
	}
	if got := p.m.SinkNATSDuplicatesTotal.Load(); got != 10 {
		t.Errorf("duplicates counter = %d, want 10 (7 from the full redelivery, 3 from the overlap)", got)
	}
}

// Many workers, one connection: every publish is acknowledged.
func TestNATSConcurrentWriters(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	const writers, per = 8, 20
	batches := make([][]*envelope.Item, writers)
	for w := range batches {
		batches[w] = f.items(f.space, per)
	}
	start := make(chan struct{})
	errs := make(chan error, writers)
	for _, b := range batches {
		go func() {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}()
	}
	close(start)
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Errorf("writer: %v", err)
		}
	}
	if n := f.count(f.stream); n != writers*per {
		t.Errorf("%d messages, want %d", n, writers*per)
	}
}

// SPEC §8.7.5 against a real server: an envelope as large as the boot check
// admits is accepted by the server's actual max_payload; one byte more of the
// largest envelope is refused at boot.
func TestNATSLargestAcceptedMessageFitsTheServer(t *testing.T) {
	f := natsSetup(t)
	payload := f.nc.MaxPayload()
	header := int64(natsHeaderMax + natsExpectedStreamHeader + len(f.stream))

	cfg := f.cfg
	cfg.MaxEventBytes = payload - header
	p := f.sink(cfg)
	f.ready(p)

	it := sizedItemIn(t, f.space, int(cfg.MaxEventBytes))
	if err := p.Write(context.Background(), []*envelope.Item{it}); err != nil {
		t.Fatalf("an envelope the boot check admits was refused by the server (max_payload=%d, envelope=%d): %v",
			payload, len(it.Env), err)
	}

	cfg.MaxEventBytes++
	over := f.sink(cfg)
	if err := over.Health(context.Background()); err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "max_payload") {
		t.Errorf("one byte past the ceiling passed the boot check: %v", err)
	}
}

// SPEC §8.7.2: a message larger than the server's max_payload is refused by
// the client before it leaves (ErrMaxPayload) -- a reconnection to a node with
// a smaller max_payload. That refusal used to end the batch: the later
// envelopes were never published yet confirmed, and the earlier ones, stored,
// were counted lost. Only the oversized one is refused, in both modes, and
// Delivered says exactly which were published.
func TestNATSOversizedMessageIsRefusedAlone(t *testing.T) {
	f := natsSetup(t)
	for _, mode := range []string{NATSModeJetStream, NATSModeCore} {
		t.Run(mode, func(t *testing.T) {
			space, stream := f.newStream(nil)
			cfg := f.cfg
			cfg.Stream, cfg.Workspace, cfg.Mode = stream, space, mode
			p := f.sink(cfg)
			var sub *nats.Subscription
			if mode == NATSModeCore {
				var err error
				if sub, err = f.nc.SubscribeSync("loghub." + space + ".webhook"); err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				f.nc.Flush()
			}
			items := []*envelope.Item{itemIn(space, testRequest("/zero", nil)),
				sizedItemIn(t, space, int(f.nc.MaxPayload())+1024), itemIn(space, testRequest("/two", nil))}
			err := p.Write(context.Background(), items)
			if ClassOf(err) != ClassLogic {
				t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
			}
			if d, ok := DeliveredOf(err); !ok || fmt.Sprint(d) != "[0 2]" {
				t.Fatalf("Delivered = %v, want [0 2]", d)
			}
			if got := refusedOf(items, err); len(got) != 1 || got[0] != items[1] {
				t.Fatalf("refused %v, want only the oversized item", got)
			}
			if mode == NATSModeJetStream {
				if n := f.count(stream); n != 2 {
					t.Fatalf("%d messages stored, want 2", n)
				}
				return
			}
			for i := 0; i < 2; i++ {
				if _, err := sub.NextMsg(5 * time.Second); err != nil {
					t.Fatalf("core subscriber got %d of 2 messages: %v", i, err)
				}
			}
		})
	}
}

// SPEC §8.7.2: one message the stream refuses for good (10054, above its
// max_msg_size) does not make the batch "lost" as a whole: the other futures
// say which messages were stored, and they come back in Error.Delivered.
func TestNATSLogicRefusalReportsTheAckedMessages(t *testing.T) {
	f := natsSetup(t)
	space, stream := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 4000 })
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, space
	p := f.sink(cfg) // not validated: the point is the write path

	items := []*envelope.Item{itemIn(space, testRequest("/zero", nil)), sizedItemIn(t, space, 5000),
		itemIn(space, testRequest("/two", nil))}
	err := p.Write(context.Background(), items)
	if ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic (10054)", err, ClassOf(err))
	}
	var e *Error
	if !errors.As(err, &e) || fmt.Sprint(e.Delivered) != "[0 2]" {
		t.Fatalf("Delivered = %v, want [0 2]", e.Delivered)
	}
	if n := f.count(stream); n != 2 {
		t.Fatalf("%d messages stored, want 2", n)
	}
}

// SPEC §8.7.2, §9.2: an envelope this server did not write maps to nothing and
// is refused alone; the good ones of its batch are published and reported
// delivered. The fast's sink published the items before the damaged one and
// then counted the whole batch dropped.
func TestNATSRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := p.Write(ctx, []*envelope.Item{badItem()})
	if d, ok := DeliveredOf(err); !ok || len(d) != 0 {
		t.Fatalf("Write(v1 envelope) = %v, want logic with nothing delivered", err)
	}
	err = p.Write(ctx, []*envelope.Item{itemIn(f.space, testRequest("/good", nil)), badItem(),
		itemIn(f.space, testRequest("/good2", nil))})
	if d, ok := DeliveredOf(err); !ok || fmt.Sprint(d) != "[0 2]" {
		t.Fatalf("Write(good, v1, good) = %v, want logic with [0 2] delivered", err)
	}
	if n := f.count(f.stream); n != 2 {
		t.Errorf("%d messages, want the 2 good ones", n)
	}
}

// ------------------------------------------------------------- validation

// SPEC §8.7.5: each refusal is FATAL, so the boot stops at once. The accepted
// cases write after validating: a rule that refused everything would fail
// them instead of passing the refusals for the wrong reason.
func TestNATSHealthValidatesTheStream(t *testing.T) {
	f := natsSetup(t)
	_, other := f.newStream(nil)
	smallSpace, small := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 4096 })
	wideSpace, wide := f.newStream(func(c *jetstream.StreamConfig) {
		c.Subjects = append(c.Subjects, c.Subjects[0]+".>")
	})
	tailSpace, tailOnly := f.newStream(func(c *jetstream.StreamConfig) { c.Subjects = []string{c.Subjects[0] + ".>"} })
	ctlSpace, ctl := f.newStream(func(c *jetstream.StreamConfig) { c.Subjects = append(c.Subjects, f.ns+".>") })
	noAckSpace, noAck := f.newStream(func(c *jetstream.StreamConfig) { c.NoAck = true })

	sealedSpace, sealed := f.newStream(nil)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s, err := f.js.Stream(ctx, sealed)
		if err != nil {
			t.Fatal(err)
		}
		cfg := s.CachedInfo().Config
		cfg.Sealed = true
		if _, err := f.js.UpdateStream(ctx, cfg); err != nil {
			t.Fatalf("sealing %s: %v", sealed, err)
		}
		cancel()
	}

	cases := []struct {
		name string
		mut  func(*NATSConfig)
		want string // substring of the fatal error; "" means accepted
	}{
		{"canonical stream", func(*NATSConfig) {}, ""},
		{"missing stream", func(c *NATSConfig) { c.Stream = "WHS_MISSING_STREAM" }, "stream"},
		{"stream of another space", func(c *NATSConfig) { c.Stream = other }, "does not cover"},
		// webhook.> holds only LONGER subjects: it does not cover the exact one.
		{"stream of webhook.> alone", func(c *NATSConfig) { c.Stream, c.Workspace = tailOnly, tailSpace }, "does not cover"},
		{"stream of the exact subject and webhook.>", func(c *NATSConfig) { c.Stream, c.Workspace = wide, wideSpace }, ""},
		{"stream capturing the control plane", func(c *NATSConfig) { c.Stream, c.Workspace = ctl, ctlSpace }, "control plane"},
		{"sealed stream", func(c *NATSConfig) { c.Stream, c.Workspace = sealed, sealedSpace }, "sealed"},
		{"no_ack stream", func(c *NATSConfig) { c.Stream, c.Workspace = noAck, noAckSpace }, "no_ack"},
		{"payload ceiling", func(c *NATSConfig) { c.MaxEventBytes = f.nc.MaxPayload() }, "max_payload"},
		{"stream max_msg_size", func(c *NATSConfig) {
			// 3900 plus the header block passes the stream's 4096.
			c.Stream, c.Workspace, c.MaxEventBytes = small, smallSpace, 3900
		}, "max_msg_size"},
		{"stream max_msg_size, fitting", func(c *NATSConfig) {
			c.Stream, c.Workspace, c.MaxEventBytes = small, smallSpace, 3000
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			tc.mut(&cfg)
			p := f.sink(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := p.Health(ctx)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Health refused a valid stream: %v", err)
				}
				if err := p.Write(ctx, f.items(cfg.Workspace, 2)); err != nil {
					t.Errorf("Write after validation: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Health accepted a stream that violates the contract")
			}
			if !IsFatal(err) {
				t.Errorf("class = %s, want fatal so the boot stops at once", ClassOf(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// SPEC §8.7.2: the expected-stream header makes the server refuse a publish
// whose subject belongs to another stream (10060), and the refusal is fatal, so
// a misconfiguration that slipped past the boot check cannot write anywhere.
// The refusal comes from a future: the batch is tainted.
func TestNATSExpectedStreamMismatchIsFatalOnWrite(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	// The subject leaves the configured stream for another one after the boot.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	moved := natsCanonicalStream(f.stream, f.space+".unused")
	if _, err := f.js.UpdateStream(ctx, moved); err != nil {
		t.Fatal(err)
	}
	thief := natsCanonicalStream(f.stream+"_THIEF", f.space)
	if _, err := f.js.CreateStream(ctx, thief); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), thief.Name) })

	items := f.items(f.space, 1)
	err := p.Write(ctx, items)
	if err == nil {
		t.Fatal("a publish into another stream was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s (%v), want fatal", ClassOf(err), err)
	}
	items[0].Release()
	if items[0].Env == nil {
		t.Error("an ack failure must taint the batch: other futures may still be pending, the buffer goes to the GC")
	}
	if n, m := f.count(f.stream), f.count(thief.Name); n != 0 || m != 0 {
		t.Errorf("%d and %d messages landed", n, m)
	}
}

// SPEC §8.7.5: the boot validates the instance's own subject only. An envelope
// of another space -- an emergency dump from a life with another Workspace --
// lands on a subject the stream may not cover. Published blind it got no
// responders, network, and the boot waited out sink.boot_timeout in a restart
// loop with an error that never named the cause. Now it is fatal before
// anything of the batch is published, and the error names the subject; once
// the stream covers it, the same envelope goes through, and the subject is
// remembered.
func TestNATSUncoveredForeignSubjectIsFatal(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stray := fmt.Sprintf("gone%d", time.Now().UnixNano())
	foreign := itemIn(stray, testRequest("/foreign", nil))
	err := p.Write(ctx, []*envelope.Item{itemIn(f.space, testRequest("/own", nil)), foreign})
	if err == nil {
		t.Fatal("an envelope whose subject no stream covers was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("err = %v (class %s), want fatal", err, ClassOf(err))
	}
	for _, want := range []string{"loghub." + stray + ".webhook", "until the dump drains"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if got := f.count(f.stream); got != 0 {
		t.Errorf("%d messages published from a batch refused as fatal; want none", got)
	}

	// Another stream owns the subject: fatal, naming it.
	otherSpace, otherStream := f.newStream(nil)
	err = p.Write(ctx, []*envelope.Item{itemIn(otherSpace, testRequest("/other", nil))})
	if !IsFatal(err) || !strings.Contains(err.Error(), otherStream) {
		t.Errorf("a subject owned by another stream: %v, want fatal naming %s", err, otherStream)
	}

	// The operator widens the stream: the same envelope is published, and the
	// subject is remembered as covered.
	wide := natsCanonicalStream(f.stream, f.space)
	wide.Subjects = append(wide.Subjects, "loghub."+stray+".webhook")
	if _, err := f.js.UpdateStream(ctx, wide); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(ctx, []*envelope.Item{foreign}); err != nil {
		t.Fatalf("Write after the stream covers the subject: %v", err)
	}
	if got := f.count(f.stream); got != 1 {
		t.Errorf("%d messages, want 1", got)
	}
	p.coverMu.Lock()
	_, cached := p.covered["loghub."+stray+".webhook"]
	p.coverMu.Unlock()
	if !cached {
		t.Error("a covered subject must be remembered, or every batch of the dump asks the server again")
	}
}

// SPEC §8.7.7: a stream deleted after boot is caught by the health check as
// fatal, never as a permanent stream of logic drops.
func TestNATSDroppedStreamIsFatal(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.js.DeleteStream(ctx, f.stream); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(ctx, f.items(f.space, 1)); err == nil {
		t.Error("a publish into a deleted stream succeeded")
	} else if ClassOf(err) == ClassLogic {
		t.Errorf("a deleted stream classified as logic on write: %v", err)
	}
	err := p.Health(ctx)
	if err == nil {
		t.Fatal("Health passed with the stream gone")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s, want fatal", ClassOf(err))
	}
}

// SPEC §8.7.2: core mode publishes by core NATS and flushes once per batch;
// the server received the messages, and a subscriber got them -- the JLH in
// Data, the same headers.
func TestNATSCoreMode(t *testing.T) {
	f := natsSetup(t)
	space := fmt.Sprintf("core%d", time.Now().UnixNano())
	sub, err := f.nc.SubscribeSync("loghub." + space + ".webhook")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	cfg := f.cfg
	cfg.Mode, cfg.Stream, cfg.Workspace = NATSModeCore, "", space
	cfg.MaxEventBytes = 64 << 10
	p := f.sink(cfg)
	f.ready(p)

	items := f.items(space, 3)
	if err := p.Write(context.Background(), items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for i, it := range items {
		m, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !bytes.Equal(m.Data, it.Env) || m.Header.Get("Nats-Msg-Id") != it.ID+".0" {
			t.Errorf("message %d: %q %v", i, m.Data, m.Header)
		}
	}

	over := cfg
	over.MaxEventBytes = f.nc.MaxPayload()
	if err := f.sink(over).Health(context.Background()); !IsFatal(err) {
		t.Errorf("core mode accepted a largest event above max_payload: %v", err)
	}
}

// SPEC §8.7.10: what the beacon needs of the sink -- the connect hook on the
// first connection and on every reconnection, a core publish on the same
// connection, and the server address.
func TestNATSHooksPublishCoreAndServerAddr(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	var calls atomic.Int64
	p.OnConnect(func() { calls.Add(1) })
	f.ready(p)
	if calls.Load() != 1 {
		t.Fatalf("hook ran %d times on the first connection, want 1", calls.Load())
	}
	if p.ServerAddr() == "" {
		t.Error("no server address while connected")
	}

	// A hook registered late runs at once.
	var late atomic.Int64
	p.OnConnect(func() { late.Add(1) })
	if late.Load() != 1 {
		t.Error("a hook registered on a live connection did not run")
	}

	sub, err := f.nc.SubscribeSync(f.ns + ".beacon")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishCore(f.ns+".beacon", []byte(`{"hello":1}`)); err != nil {
		t.Fatal(err)
	}
	if m, err := sub.NextMsg(5 * time.Second); err != nil || string(m.Data) != `{"hello":1}` {
		t.Fatalf("core publish not received: %v", err)
	}

	p.mu.Lock()
	nc := p.nc
	p.mu.Unlock()
	if err := nc.ForceReconnect(); err != nil {
		t.Fatal(err)
	}
	if !eventually(t, 10*time.Second, func() bool {
		return calls.Load() == 2 && p.m.SinkNATSReconnectsTotal.Load() == 1
	}) {
		t.Fatalf("after a reconnection: hook ran %d times, reconnects %d", calls.Load(),
			p.m.SinkNATSReconnectsTotal.Load())
	}
}

// The whole drain path against the real server: ring, workers, async publish,
// acks, and every item released cleanly afterwards.
func TestNATSWorkersEndToEnd(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	rg := ring.NewMemory(1000, 1<<30, time.Millisecond)
	m := testMetrics()
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m),
		WorkerConfig{Workers: 4, Batch: 32, OpTimeout: 5 * time.Second,
			BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond}, quietLog(), m)
	items := f.items(f.space, 300)
	envs := make([][]byte, len(items))
	for i, it := range items {
		envs[i] = append([]byte(nil), it.Env...)
		if err := rg.Enqueue(context.Background(), it); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	if !eventually(t, 30*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 300 }) {
		t.Fatalf("written %d of 300", m.EventsWrittenTotal.Load())
	}
	cancel()
	w.Wait()

	if n := f.count(f.stream); n != 300 {
		t.Fatalf("%d messages, want 300", n)
	}
	want := map[string]bool{}
	for _, e := range envs {
		want[string(e)] = true
	}
	for _, msg := range f.messages(f.stream) {
		if !want[string(msg.Data)] {
			t.Fatal("a stored message is not one of the envelopes enqueued, byte for byte")
		}
	}
	for _, it := range items {
		if !it.Released() {
			t.Fatal("an item was not released after delivery")
		}
	}
}

// SPEC §8.7.5 lists "JetStream desligado" as fatal. A server without
// JetStream answers nobody on the stream API (no responders, which on its own
// is network); the first validation tells it apart and ends the boot at once
// instead of after sink.boot_timeout. Needs a nats-server WITHOUT -js.
func TestNATSJetStreamDisabledIsFatal(t *testing.T) {
	url := os.Getenv("WHS_TEST_NATS_NOJS_URL")
	if url == "" {
		t.Skip("set WHS_TEST_NATS_NOJS_URL to a nats-server started without -js")
	}
	p, err := NewNATS(NATSConfig{URLs: []string{url}, Stream: "LOGHUB_WEBHOOK", Workspace: "default",
		ConnectTimeout: 5 * time.Second}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	h := NewHealth(p, HealthConfig{Timeout: 5 * time.Second}, quietLog(), testMetrics())
	start := time.Now()
	err = h.WaitReady(context.Background(), 30*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	if !IsFatal(err) || !strings.Contains(err.Error(), "JetStream") {
		t.Fatalf("WaitReady on a server without JetStream = %v (class %s), want fatal", err, ClassOf(err))
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the verdict took %v; it must not wait out the boot timeout", d)
	}

	// core mode needs no JetStream at all.
	core, err := NewNATS(NATSConfig{URLs: []string{url}, Mode: NATSModeCore, Workspace: "default"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	if err := core.Health(context.Background()); err != nil {
		t.Fatalf("core mode on a server without JetStream: %v", err)
	}
}

// ------------------------------------------------- review 2026-10-10

// A logic refusal frees the ring's bytes in both modes. jetstream taints the
// batch, which kept Env alive for Commit; core does not, and the worker's
// Release before Commit left the ring counting the whole batch forever.
func TestNATSLogicRefusalFreesTheRingBytes(t *testing.T) {
	for _, mode := range []string{NATSModeCore, NATSModeJetStream} {
		t.Run(mode, func(t *testing.T) {
			f := natsSetup(t)
			cfg := f.cfg
			cfg.Mode = mode
			p := f.sink(cfg)
			f.ready(p)

			rg := ring.NewMemory(1000, 256<<20, time.Millisecond)
			m := testMetrics()
			w := NewWorkers(rg, p, NewHealth(p, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m),
				WorkerConfig{Workers: 1, Batch: 32, OpTimeout: 5 * time.Second,
					BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond}, quietLog(), m)
			// One envelope above max_payload: ErrMaxPayload, refused alone in
			// the client -- the reconnection to a smaller server of §8.7.2.
			big := itemIn(f.space, testRequest("/big", bytes.Repeat([]byte{'x'}, int(f.nc.MaxPayload()))))
			for _, it := range append(f.items(f.space, 3), big) {
				if err := rg.Enqueue(context.Background(), it); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			w.Start(ctx)
			ok := eventually(t, 20*time.Second, func() bool {
				return rg.Stats().Items == 0 && m.EventsWrittenTotal.Load() == 3
			})
			cancel()
			w.Wait()
			if !ok {
				t.Fatalf("not drained: %+v written=%d", rg.Stats(), m.EventsWrittenTotal.Load())
			}
			if b := rg.Stats().Bytes; b != 0 {
				t.Errorf("the empty ring still counts %d bytes", b)
			}
		})
	}
}

// The stream contract holds on every health check, not only at the boot. A
// stream edited underneath answered every publish with a timeout -- network,
// retried forever -- while Health, which saw the stream still there, kept the
// sink UP and the ring filled.
func TestNATSStreamEditedAtRuntimeIsFatal(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(*jetstream.StreamConfig)
		want string
	}{
		"subjects narrowed": {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.someoneelse.webhook"} },
			"does not cover"},
		"no_ack turned on": {func(c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
	} {
		t.Run(name, func(t *testing.T) {
			f := natsSetup(t)
			p := f.sink(f.cfg)
			f.ready(p)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sc := natsCanonicalStream(f.stream, f.space)
			tc.mut(&sc)
			if _, err := f.js.UpdateStream(ctx, sc); err != nil {
				t.Fatalf("update: %v", err)
			}
			if err := p.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Health after the edit = %v, want fatal naming %q", err, tc.want)
			}
		})
	}
}

// SPEC §8.7.5: a subject of another space, covered when the dump was
// reingested and dropped from the stream since -- the operator narrowing it
// once the dump drained, which is what cover's own error tells them they may
// do -- is forgotten, not fatal. Until 2026-10-11 the next health check took
// the healthy process down: FATAL, the ring dumped, exit 3. A later publish
// there still asks again and is refused then, naming the subject.
func TestNATSForeignSubjectDroppedAfterTheDrainIsForgotten(t *testing.T) {
	f := natsSetup(t)
	foreign := f.space + "x"
	subject := "loghub." + foreign + ".webhook"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sc := natsCanonicalStream(f.stream, f.space)
	sc.Subjects = append(sc.Subjects, subject)
	if _, err := f.js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	p := f.sink(f.cfg)
	f.ready(p)
	if err := p.Write(ctx, f.items(foreign, 1)); err != nil {
		t.Fatalf("Write to the covered foreign space: %v", err)
	}
	sc.Subjects = sc.Subjects[:1]
	if _, err := f.js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("Health after the foreign subject was dropped = %v, want the sink UP", err)
	}
	p.coverMu.Lock()
	_, cached := p.covered[subject]
	p.coverMu.Unlock()
	if cached {
		t.Error("the dropped subject is still cached as covered: a publish there would go out blind")
	}
	if err := p.Write(ctx, f.items(f.space, 1)); err != nil {
		t.Errorf("Write to the own subject after the narrowing: %v", err)
	}
	err := p.Write(ctx, f.items(foreign, 1))
	if !IsFatal(err) || !strings.Contains(err.Error(), subject) {
		t.Fatalf("a publish to the dropped subject = %v (class %s), want fatal naming it", err, ClassOf(err))
	}
}
