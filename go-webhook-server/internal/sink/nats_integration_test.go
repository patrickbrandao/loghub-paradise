package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// What is worth proving here -- the async publish with futures, the subject
// expanded per envelope, the server's deduplication by Nats-Msg-Id, the
// expected-stream refusal and the stream validation -- is exactly what a fake
// would have to invent. These run against a real server addressed by
// WHS_TEST_NATS_URL, with JetStream enabled, and skip without it
// (docs/TESTING.md §5.4):
//
//	WHS_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race ./internal/sink/
//
// Every test creates its own streams from docker/nats/stream.json under a
// unique name, each listening on the subjects of a Workspace of its own: two
// streams may not share a subject, and the server is shared with other
// suites that use the default one.

const natsURLEnv = "WHS_TEST_NATS_URL"

type natsFixture struct {
	t         *testing.T
	url       string
	nc        *nats.Conn
	js        jetstream.JetStream
	stream    string
	workspace string
	cfg       NATSConfig
}

func natsSetup(t *testing.T) *natsFixture {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the nats sink tests (see docs/TESTING.md §5.4)", natsURLEnv)
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
	f.stream, f.workspace = f.newStream(nil)
	f.cfg = NATSConfig{
		URLs: []string{url}, Stream: f.stream, Subject: "loghub.{space}.{type}", Workspace: f.workspace,
		ClientName: "whs/test@test-host", ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second,
		MaxPending: 4096,
	}
	return f
}

// natsSubject is the LogHub subject of a space (SPEC §3.8).
func natsSubject(space string) string { return "loghub." + space + "." + envelope.Type }

// otherSpace is the second Workspace every test stream also listens on: the
// envelope of another space, reingested from a dump an earlier life left.
func otherSpace(space string) string { return space + ".other" }

// newStream creates a stream from the canonical config under a unique name,
// listening on the subjects of a fresh Workspace, and registers its removal.
// mut adjusts the config first.
func (f *natsFixture) newStream(mut func(*jetstream.StreamConfig)) (name, workspace string) {
	f.t.Helper()
	n := time.Now().UnixNano()
	name, workspace = fmt.Sprintf("WHS_TEST_%d", n), fmt.Sprintf("whs-test-%d", n)
	cfg := natsStreamConfigFor(f.t, name, natsSubject(workspace), natsSubject(otherSpace(workspace)))
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.CreateStream(ctx, cfg); err != nil {
		f.t.Fatalf("creating stream %s: %v", name, err)
	}
	f.t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
	return name, workspace
}

// natsStreamConfigFor reads docker/nats/stream.json -- the file operators
// hand to `nats stream add` -- and retargets it at a test stream. Using the
// real file is the point: the validation of SPEC §8.7.5 and the stream the
// operator gets must agree.
func natsStreamConfigFor(t *testing.T, name string, subjects ...string) jetstream.StreamConfig {
	t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatalf("reading the canonical stream config: %v", err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("parsing the canonical stream config: %v", err)
	}
	if cfg.Name != "LOGHUB_WEBHOOK_REQUEST" || len(cfg.Subjects) != 1 || cfg.Subjects[0] != natsSubject("default") {
		t.Fatalf("docker/nats/stream.json is %s on %v; SPEC §3.8 wants LOGHUB_WEBHOOK_REQUEST on %s",
			cfg.Name, cfg.Subjects, natsSubject("default"))
	}
	cfg.Name = name
	cfg.Subjects = subjects
	return cfg
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
		raw, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		out = append(out, raw)
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

// ------------------------------------------------------------------ writes

// SPEC §3.8: one message per event, on the subject of the envelope's own
// space, with Nats-Msg-Id = <uuid>.0 and the X-LogHub-* headers, and Data the
// JLH byte for byte -- body included, binary in base64.
func TestNATSPublishStoresTheEnvelope(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	raw := []byte("A\x00B\nC\xffD\r\x00")
	items := []*envelope.Item{
		itemIn(f.workspace, testRequest("/own", []byte(`{"a":1}`))),
		itemIn(f.workspace, testRequest("/b1", nil)),
		itemIn(f.workspace, testRequest("/b2", nil)),
		itemIn(f.workspace, testRequest("/bin", raw)),
		itemIn(otherSpace(f.workspace), testRequest("/other", nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	if len(got) != len(items) {
		t.Fatalf("%d messages, want %d", len(got), len(items))
	}
	byID := map[string]*envelope.Item{}
	for _, it := range items {
		byID[it.ID] = it
	}
	subjects := map[string]int{}
	for _, m := range got {
		subjects[m.Subject]++
		id := m.Header.Get(natsHeaderUUID)
		it, ok := byID[id]
		if !ok {
			t.Errorf("message with unknown X-LogHub-UUID %q", id)
			continue
		}
		e := validJLH(t, m.Data)
		if !bytes.Equal(m.Data, it.Env) {
			t.Errorf("%s: Data is not the JLH byte for byte", id)
		}
		if want := natsSubject(e.Space); m.Subject != want {
			t.Errorf("%s: subject %q, want %q", id, m.Subject, want)
		}
		for k, v := range map[string]string{
			jetstream.MsgIDHeader: id + ".0",
			"Content-Type":        "application/json",
			natsHeaderType:        envelope.Type,
			natsHeaderSpace:       e.Space,
		} {
			if got := m.Header.Get(k); got != v {
				t.Errorf("%s: %s = %q, want %q", id, k, got, v)
			}
		}
		if it == items[3] {
			body, err := e.Payload.Body()
			if err != nil || !bytes.Equal(body, raw) || e.Payload.Content.Encoding != "base64" {
				t.Errorf("binary body = %q (%s, %v), want the bytes that arrived", body, e.Payload.Content.Encoding, err)
			}
		}
	}
	if subjects[natsSubject(f.workspace)] != 4 || subjects[natsSubject(otherSpace(f.workspace))] != 1 {
		t.Errorf("messages per subject %v: want 4 on the Workspace's, 1 on the other space's", subjects)
	}
}

// SPEC §8.7.3: the pipeline is at-least-once, and a redelivery inside the
// duplicate window is acknowledged as a duplicate, stored once and counted --
// a uuid twice inside one batch as well.
func TestNATSRedeliveryIsDeduplicatedAndCounted(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := func(n int) []*envelope.Item {
		out := make([]*envelope.Item, n)
		for i := range out {
			out[i] = itemIn(f.workspace, testRequest(fmt.Sprintf("/r%d", i), nil))
		}
		return out
	}
	items := batch(7)
	ctx := context.Background()
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, items); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	mixed := append(items[:3:3], batch(2)...)
	if err := p.Write(ctx, mixed); err != nil {
		t.Fatalf("mixed Write: %v", err)
	}
	twice := itemIn(f.workspace, testRequest("/twice", nil))
	if err := p.Write(ctx, []*envelope.Item{twice, twice}); err != nil {
		t.Fatalf("a uuid twice in one batch: %v", err)
	}
	if n := f.count(f.stream); n != 10 {
		t.Errorf("%d messages, want 10: redelivery must not duplicate", n)
	}
	if got := p.m.SinkNATSDuplicatesTotal.Load(); got != 11 {
		t.Errorf("duplicates counter = %d, want 11 (7 from the full redelivery, 3 from the overlap, 1 in-batch)", got)
	}
}

// Many workers, one connection: every publish is acknowledged.
func TestNATSConcurrentWriters(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	const writers, per = 8, 20
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		b := make([]*envelope.Item, per)
		for i := range b {
			b[i] = itemIn(f.workspace, testRequest("/c", nil))
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}()
	}
	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			t.Errorf("writer: %v", err)
		}
	}
	if n := f.count(f.stream); n != writers*per {
		t.Errorf("%d messages, want %d", n, writers*per)
	}
}

// SPEC §8.7.2: an envelope this server did not write maps to nothing and is
// refused alone; the good ones of its batch are published and reported
// delivered. Until 2026-10-11 the items before the damaged one were published
// and the whole batch counted dropped (the BUG this test used to skip with).
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
	err = p.Write(ctx, []*envelope.Item{itemIn(f.workspace, testRequest("/good", nil)), badItem(),
		itemIn(f.workspace, testRequest("/good2", nil))})
	if d, ok := DeliveredOf(err); !ok || fmt.Sprint(d) != "[0 2]" {
		t.Fatalf("Write(good, v1, good) = %v, want logic with [0 2] delivered", err)
	}
	if n := f.count(f.stream); n != 2 {
		t.Errorf("%d messages, want the 2 good ones", n)
	}
}

// SPEC §8.7.2: a message larger than the server's max_payload is refused by
// the client before it leaves (ErrMaxPayload) -- a reconnection to a node with
// a smaller max_payload. That refusal used to end the batch: the later
// envelopes were never published yet confirmed, and the earlier ones, stored,
// were counted lost. Only the oversized one is refused, and Delivered says
// exactly which were published.
func TestNATSOversizedMessageIsRefusedAlone(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	items := []*envelope.Item{itemIn(f.workspace, testRequest("/zero", nil)),
		itemIn(f.workspace, testRequest("/big", bytes.Repeat([]byte{'x'}, int(f.nc.MaxPayload())))),
		itemIn(f.workspace, testRequest("/two", nil))}
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
	if n := f.count(f.stream); n != 2 {
		t.Fatalf("%d messages stored, want 2", n)
	}
}

// SPEC §8.7.2: one message the stream refuses for good (10054, above its
// max_msg_size) does not make the batch "lost" as a whole: the other futures
// say which messages were stored, and they come back in Error.Delivered.
func TestNATSLogicRefusalReportsTheAckedMessages(t *testing.T) {
	f := natsSetup(t)
	stream, workspace := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 4000 })
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, workspace
	p := f.sink(cfg) // not validated: the point is the write path

	items := []*envelope.Item{itemIn(workspace, testRequest("/zero", nil)),
		itemIn(workspace, testRequest("/big", bytes.Repeat([]byte{'x'}, 5000))),
		itemIn(workspace, testRequest("/two", nil))}
	err := p.Write(context.Background(), items)
	if ClassOf(err) != ClassLogic {
		t.Fatalf("Write = %v (class %s), want logic (10054)", err, ClassOf(err))
	}
	var e *Error
	if !errors.As(err, &e) || fmt.Sprint(e.Delivered) != "[0 2]" {
		t.Fatalf("Delivered = %v, want [0 2]", err)
	}
	if n := f.count(stream); n != 2 {
		t.Fatalf("%d messages stored, want 2", n)
	}
}

// ------------------------------------------------------------- validation

// SPEC §8.7.5: each refusal is FATAL, so the boot stops at once. The accepted
// cases write after validating: a rule that refused everything would fail
// them instead of passing the refusals for the wrong reason.
func TestNATSHealthValidatesTheStream(t *testing.T) {
	f := natsSetup(t)
	_, otherWorkspace := f.newStream(nil)
	small, smallWorkspace := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 1024 })

	sealed, sealedWorkspace := f.newStream(nil)
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
	noAck, noAckWorkspace := f.newStream(func(c *jetstream.StreamConfig) { c.NoAck = true })
	// A stream that also listens on the control plane. On a server shared
	// with another suite that holds such a stream the creation fails on the
	// overlap (10065); the case is skipped then, and TestNATSValidateRefusals
	// still covers the rule.
	var ctl, ctlWorkspace string
	{
		n := time.Now().UnixNano()
		name, ws := fmt.Sprintf("WHS_TEST_CTL_%d", n), fmt.Sprintf("whs-test-ctl-%d", n)
		cfg := natsStreamConfigFor(t, name, natsSubject(ws), natsCtlBeacon)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := f.js.CreateStream(ctx, cfg)
		cancel()
		if err == nil {
			ctl, ctlWorkspace = name, ws
			t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
		} else {
			t.Logf("no control-plane stream on this shared server (%v): that case is skipped", err)
		}
	}
	maxPayload := f.nc.MaxPayload()
	fits := envelope.MaxEventBytes(64<<10, 16<<10, 8<<10)
	tooBig := envelope.MaxEventBytes(maxPayload, 64<<10, 8<<10)
	if fits+natsHeaderOverhead > maxPayload {
		t.Skipf("max_payload=%d is too small for the control case", maxPayload)
	}

	cases := []struct {
		name string
		mut  func(*NATSConfig)
		want string // substring of the fatal error; "" means accepted
	}{
		{"canonical stream", func(*NATSConfig) {}, ""},
		{"an envelope that fits max_payload", func(c *NATSConfig) { c.MaxEventBytes = fits }, ""},
		{"missing stream", func(c *NATSConfig) { c.Stream = fmt.Sprintf("WHS_MISSING_%d", time.Now().UnixNano()) }, "stream"},
		{"workspace subject of another stream", func(c *NATSConfig) { c.Workspace = otherWorkspace }, "belongs to stream"},
		{"workspace subject with no stream", func(c *NATSConfig) { c.Workspace = "nobody-home" + f.workspace }, "no stream listens"},
		{"sealed stream", func(c *NATSConfig) { c.Stream, c.Workspace = sealed, sealedWorkspace }, "sealed"},
		{"no_ack stream", func(c *NATSConfig) { c.Stream, c.Workspace = noAck, noAckWorkspace }, "no_ack"},
		{"stream capturing the control plane", func(c *NATSConfig) { c.Stream, c.Workspace = ctl, ctlWorkspace }, "control plane"},
		{"max_payload below the largest envelope", func(c *NATSConfig) { c.MaxEventBytes = tooBig }, "max_payload"},
		{"payload ceiling counts the headers", func(c *NATSConfig) { c.MaxEventBytes = maxPayload }, "max_payload"},
		{"stream max_msg_size", func(c *NATSConfig) { c.Stream, c.Workspace, c.MaxEventBytes = small, smallWorkspace, 4096 }, "max_msg_size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.cfg
			tc.mut(&cfg)
			if cfg.Stream == "" {
				t.Skip("the stream for this case could not be created")
			}
			p := f.sink(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := p.Health(ctx)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Health refused a valid stream: %v", err)
				}
				items := []*envelope.Item{itemIn(cfg.Workspace, testRequest("/v", nil)),
					itemIn(cfg.Workspace, testRequest("/v", nil))}
				if err := p.Write(ctx, items); err != nil {
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

// SPEC §8.7.5: an envelope of a space whose subject another stream owns --
// reingested from a dump of another Workspace, say -- is refused as fatal
// before anything is published, and the error names the stream that owns the
// subject. It cannot land anywhere but the configured stream.
func TestNATSExpectedStreamMismatchIsFatalOnWrite(t *testing.T) {
	f := natsSetup(t)
	other, otherWorkspace := f.newStream(nil)
	p := f.sink(f.cfg)
	f.ready(p)

	err := p.Write(context.Background(), []*envelope.Item{itemIn(otherWorkspace, testRequest("/elsewhere", nil))})
	if err == nil {
		t.Fatal("a publish into another stream was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("err = %v (class %s), want fatal", err, ClassOf(err))
	}
	if !strings.Contains(err.Error(), natsSubject(otherWorkspace)) || !strings.Contains(err.Error(), other) {
		t.Errorf("error %q does not name the subject and the stream that owns it", err)
	}
	if n, m := f.count(f.stream), f.count(other); n != 0 || m != 0 {
		t.Errorf("%d messages in the configured stream, %d in the other; want none", n, m)
	}
}

// SPEC §8.7.2: the expected-stream header still guards the instance's own
// subject between two health checks, which is where the contract is
// validated: moved to another stream after the last check, the publish is
// refused by the server (10060), fatal, and lands nowhere.
func TestNATSExpectedStreamHeaderGuardsTheOwnSubject(t *testing.T) {
	f := natsSetup(t)
	n := time.Now().UnixNano()
	name, workspace := fmt.Sprintf("WHS_TEST_MOVED_%d", n), fmt.Sprintf("whs-test-moved-%d", n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.CreateStream(ctx, natsStreamConfigFor(t, name, natsSubject(workspace))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = name, workspace
	p := f.sink(cfg)
	f.ready(p)

	// The subject leaves the configured stream for another one.
	moved := natsStreamConfigFor(t, name, natsSubject(workspace+".unused"))
	if _, err := f.js.UpdateStream(ctx, moved); err != nil {
		t.Fatal(err)
	}
	thief := name + "_THIEF"
	if _, err := f.js.CreateStream(ctx, natsStreamConfigFor(t, thief, natsSubject(workspace))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), thief) })

	err := p.Write(ctx, []*envelope.Item{itemIn(workspace, testRequest("/moved", nil))})
	if err == nil {
		t.Fatal("a publish into another stream was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("err = %v (class %s), want fatal", err, ClassOf(err))
	}
	if a, b := f.count(name), f.count(thief); a != 0 || b != 0 {
		t.Errorf("%d messages in the configured stream, %d in the other; want none", a, b)
	}
}

// SPEC §8.7.5: the boot validates the instance's own subject only. An
// envelope of another space -- a backlog that ring.db or an emergency dump
// kept from a life with another Workspace -- expands to a subject the stream
// may not cover. Published blind it got "no response from stream", network,
// and the boot drain exited 3 in a restart loop with an error that never named
// the cause (review of 2026-10-06). Now it is fatal before anything of the
// batch is published, and the error names the subject; once the stream covers
// it, the same envelope goes through, and the subject is remembered.
func TestNATSUncoveredBacklogSubjectIsFatal(t *testing.T) {
	f := natsSetup(t)
	n := time.Now().UnixNano()
	name, workspace := fmt.Sprintf("WHS_TEST_OWN_%d", n), fmt.Sprintf("whs-test-own-%d", n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The stream covers the instance's own subject and nothing else.
	if _, err := f.js.CreateStream(ctx, natsStreamConfigFor(t, name, natsSubject(workspace))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = name, workspace
	p := f.sink(cfg)
	f.ready(p)

	stray := fmt.Sprintf("whs-test-gone-%d", n)
	backlog := itemIn(stray, testRequest("/backlog", nil))
	err := p.Write(ctx, []*envelope.Item{itemIn(workspace, testRequest("/own", nil)), backlog})
	if err == nil {
		t.Fatal("an envelope whose subject no stream covers was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("err = %v (class %s), want fatal: network retries it forever and the boot drain loops", err, ClassOf(err))
	}
	for _, want := range []string{natsSubject(stray), "until the backlog drains"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if got := f.count(name); got != 0 {
		t.Errorf("%d messages published from a batch refused as fatal; want none", got)
	}

	// The operator widens the stream: the same envelope is published, and the
	// subject is remembered as covered.
	if _, err := f.js.UpdateStream(ctx, natsStreamConfigFor(t, name, natsSubject(workspace), natsSubject(stray))); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(ctx, []*envelope.Item{backlog}); err != nil {
		t.Fatalf("Write after the stream covers the subject: %v", err)
	}
	if got := f.count(name); got != 1 {
		t.Errorf("%d messages, want 1", got)
	}
	p.coverMu.Lock()
	_, cached := p.covered[natsSubject(stray)]
	p.coverMu.Unlock()
	if !cached {
		t.Error("a covered subject must be remembered, or every backlog batch asks the server again")
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
	if err := p.Write(ctx, []*envelope.Item{itemIn(f.workspace, testRequest("/gone", nil))}); err == nil {
		t.Error("a publish into a deleted stream succeeded")
	}
	err := p.Health(ctx)
	if err == nil {
		t.Fatal("Health passed with the stream gone")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s, want fatal", ClassOf(err))
	}
}

// SPEC §8.7.5 lists a server without JetStream as fatal. Such a server
// answers nobody on the stream API (no responders, which on its own is
// network); the first validation tells it apart and ends the boot at once
// instead of after sink.boot_timeout. Needs a nats-server WITHOUT -js.
func TestNATSJetStreamDisabledIsFatal(t *testing.T) {
	url := os.Getenv("WHS_TEST_NATS_NOJS_URL")
	if url == "" {
		t.Skip("set WHS_TEST_NATS_NOJS_URL to a nats-server started without -js")
	}
	p, err := NewNATS(NATSConfig{URLs: []string{url}, Stream: "LOGHUB_WEBHOOK", Subject: "loghub.{space}.{type}",
		Workspace: "default", ConnectTimeout: 5 * time.Second}, quietLog(), testMetrics())
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
}

// A logic refusal frees the ring's bytes (SPEC §12.3), end to end with the
// workers, and the stored items are counted written. It cannot catch a
// Release before the Commit: the NATS sink taints a refused batch, so Env
// survives the Release; TestWorkersDropOnLogicError, with a scripted sink,
// does.
func TestNATSLogicRefusalFreesTheRingBytes(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	rg := ring.NewMemory(1000, 256<<20, time.Millisecond)
	m := testMetrics()
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), m),
		WorkerConfig{Workers: 1, Batch: 32, OpTimeout: 5 * time.Second,
			BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond}, quietLog(), m)
	// One envelope above max_payload: ErrMaxPayload, refused alone in the
	// client -- the reconnection to a smaller server of §8.7.2.
	big := itemIn(f.workspace, testRequest("/big", bytes.Repeat([]byte{'x'}, int(f.nc.MaxPayload()))))
	items := []*envelope.Item{itemIn(f.workspace, testRequest("/a", nil)),
		itemIn(f.workspace, testRequest("/b", nil)), itemIn(f.workspace, testRequest("/c", nil)), big}
	for _, it := range items {
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
	if got := m.EventsDroppedTotal[obs.DropSinkLogic].Load(); got != 1 {
		t.Errorf("dropped = %d, want only the oversized one", got)
	}
	if n := f.count(f.stream); n != 3 {
		t.Errorf("%d messages stored, want 3", n)
	}
}

// SPEC §8.7.5: the stream contract holds on every health check, not only at
// the boot. A stream edited underneath answered every publish with a timeout
// -- network, retried forever -- while Health, which saw the stream still
// there, kept the sink UP and the ring filled.
func TestNATSStreamEditedAtRuntimeIsFatal(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(*jetstream.StreamConfig)
		want string
	}{
		"subjects narrowed": {func(c *jetstream.StreamConfig) { c.Subjects = []string{natsSubject("someoneelse" + c.Name)} },
			"does not cover"},
		"no_ack turned on": {func(c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
	} {
		t.Run(name, func(t *testing.T) {
			f := natsSetup(t)
			p := f.sink(f.cfg)
			f.ready(p)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sc := natsStreamConfigFor(t, f.stream, natsSubject(f.workspace), natsSubject(otherSpace(f.workspace)))
			tc.mut(&sc)
			if _, err := f.js.UpdateStream(ctx, sc); err != nil {
				t.Fatalf("update: %v", err)
			}
			if err := p.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Health after the edit = %v, want fatal naming %q", err, tc.want)
			}
		})
	}

	// A subject of another space, covered when the backlog was published and
	// dropped from the stream once it drained: the operator did what cover's
	// error told them, and the process stays up. A later publish there is
	// asked again, and refused naming the subject.
	f := natsSetup(t)
	foreign := otherSpace(f.workspace)
	p := f.sink(f.cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, []*envelope.Item{itemIn(foreign, testRequest("/backlog", nil))}); err != nil {
		t.Fatalf("Write to the covered foreign space: %v", err)
	}
	sc := natsStreamConfigFor(t, f.stream, natsSubject(f.workspace))
	if _, err := f.js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("Health after the drained foreign subject was dropped = %v, want healthy", err)
	}
	err := p.Write(ctx, []*envelope.Item{itemIn(foreign, testRequest("/late", nil))})
	if !IsFatal(err) || !strings.Contains(err.Error(), natsSubject(foreign)) {
		t.Fatalf("a later publish to the dropped subject = %v, want fatal naming it", err)
	}
}
