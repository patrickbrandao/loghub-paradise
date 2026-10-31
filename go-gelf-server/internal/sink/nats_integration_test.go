package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ring"
)

// What is worth proving here -- the async publish with futures, the server's
// deduplication by Nats-Msg-Id <uuid>.<step>, the expected-stream refusal, the
// stream validation by local subject matching, core mode, the connect hooks of
// the beacon, and the largest message the boot check admits actually fitting
// the server -- is exactly what a fake would have to invent. These run against
// a real server with JetStream, addressed by GELF_TEST_NATS_URL, and skip
// without it:
//
//	go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
//	GELF_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race ./internal/sink/
//
// Every test works in its own space, with its own stream created from
// docker/nats/stream.json and deleted afterwards.

const natsURLEnv = "GELF_TEST_NATS_URL"

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
	f.ns = fmt.Sprintf("sls_test_ctl_%d", time.Now().UnixNano())
	f.cfg = NATSConfig{
		URLs: []string{url}, Stream: f.stream, Workspace: f.space, CtlNamespace: f.ns,
		ClientName: "sls-test", ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second, MaxPending: 4096,
	}
	return f
}

// newStream creates a stream from the canonical config, retargeted at a fresh
// space, and registers its removal. mut adjusts the config first.
func (f *natsFixture) newStream(mut func(*jetstream.StreamConfig)) (space, name string) {
	f.t.Helper()
	n := time.Now().UnixNano()
	space, name = fmt.Sprintf("t%d", n), fmt.Sprintf("SLS_TEST_%d", n)
	cfg := natsStreamConfigFor(f.t, name, space)
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

// natsStreamConfigFor reads docker/nats/stream.json -- the file operators hand
// to `nats stream add` -- and retargets it at a test stream and space. Using
// the real file is the point: the validation of SPEC §10.7 and the stream the
// operator gets must agree.
func natsStreamConfigFor(t *testing.T, name, space string) jetstream.StreamConfig {
	t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatalf("reading the canonical stream config: %v", err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("parsing the canonical stream config: %v", err)
	}
	cfg.Name = name
	for i, s := range cfg.Subjects {
		// The file covers every space (loghub.*.gelf, SPEC §3.8); two test
		// streams on it would overlap, which JetStream refuses, so each one
		// is narrowed to its own space.
		s = strings.Replace(s, "loghub.*.", "loghub."+space+".", 1)
		cfg.Subjects[i] = strings.Replace(s, "loghub.default.", "loghub."+space+".", 1)
	}
	return cfg
}

func (f *natsFixture) sink(cfg NATSConfig) *NATS {
	f.t.Helper()
	p, err := NewNATS(cfg, discard(), metrics())
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

func (f *natsFixture) items(space string, n int) []*item.Item {
	out := make([]*item.Item, n)
	for i := range out {
		out[i] = newItemIn(space, fmt.Sprintf(`{"version":"1.1","host":"web-01","short_message":"nats %d","level":5,"_a":1}`, i))
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

// ------------------------------------------------------------------ writes

// SPEC §3.8: Data is the JLH and nothing else; the five headers; the exact
// subject of the envelope's space; one message per envelope.
func TestNATSPublishStoresTheJLH(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchFrom(t, f.items(f.space, 5)...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	if len(got) != len(b.Envelopes) {
		t.Fatalf("%d messages, want %d", len(got), len(b.Envelopes))
	}
	for i, m := range got {
		e := &b.Envelopes[i]
		if m.Subject != "loghub."+f.space+".gelf" {
			t.Errorf("subject = %q", m.Subject)
		}
		for k, v := range map[string]string{
			"Nats-Msg-Id": e.UUID() + ".0", "Content-Type": "application/json",
			"X-LogHub-Type": "gelf", "X-LogHub-UUID": e.UUID(), "X-LogHub-Space": f.space,
		} {
			if got := m.Header.Get(k); got != v {
				t.Errorf("message %d: %s = %q, want %q", i, k, got, v)
			}
		}
		if string(m.Data) != string(e.JLH) {
			t.Errorf("message %d: Data is not the JLH byte for byte", i)
		}
		if err := jlhcheck.Validate(m.Data); err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
}

// SPEC §3.8: an envelope built from hostile emitter bytes, and one far above the
// 64 KiB JetStream allows a header block, both come back exactly as written. In
// a header, the second one was a 10097.
func TestNATSEnvelopeSurvivesHostileBytes(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchFrom(t,
		newItemIn(f.space, `{"version":"1.1","host":"web\u0001\u007f\ud800é-01","short_message":"body"}`),
		newItemIn(f.space, `{"version":"1.1","host":"h","short_message":"body","_k":"quote \" backslash \\ ctl \u001b\u007f\u0000","x\u0000":{"\"":[1e400]}}`),
		newItemIn(f.space, `{"version":"1.1","host":"h","short_message":"body","_k":"`+strings.Repeat(`\u0001`, 12<<10)+`"}`),
	)
	if len(b.Envelopes[2].JLH) <= 64<<10 {
		t.Fatalf("the large envelope is only %d bytes", len(b.Envelopes[2].JLH))
	}
	if err := p.Write(context.Background(), b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	if len(got) != 3 {
		t.Fatalf("%d messages, want 3", len(got))
	}
	for i, m := range got {
		if string(m.Data) != string(b.Envelopes[i].JLH) {
			t.Errorf("message %d came back different (%d bytes, want %d)", i, len(m.Data), len(b.Envelopes[i].JLH))
		}
		if err := jlhcheck.Validate(m.Data); err != nil {
			t.Errorf("message %d: %v", i, err)
		}
	}
	var e struct {
		Payload struct {
			Host struct{ Hostname string } `json:"host"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(got[0].Data, &e); err != nil || e.Payload.Host.Hostname != "web\x01\x7f\uFFFDé-01" {
		t.Errorf("hostname decoded as %q (%v)", e.Payload.Host.Hostname, err)
	}
}

// SPEC §10.2: the pipeline is at-least-once, and a redelivery inside the
// duplicate window is acknowledged as a duplicate, stored once and counted.
func TestNATSRedeliveryIsDeduplicatedAndCounted(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchFrom(t, f.items(f.space, 7)...)
	ctx := context.Background()
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, b); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	fresh := batchFrom(t, f.items(f.space, 2)...)
	mixed := &Batch{Envelopes: append(append([]Envelope{}, b.Envelopes[:3]...), fresh.Envelopes...)}
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
	batches := make([]*Batch, writers)
	for w := range batches {
		batches[w] = batchFrom(t, f.items(f.space, per)...)
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

// SPEC §3.8: the subject is loghub.<space>.gelf whatever the level -- there is
// no facility-severity form in this server (decision 16) -- and the canonical
// stream covers it.
func TestNATSSubjectIsExactForEveryLevel(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	b := batchFrom(t, newItemIn(f.space, `{"version":"1.1","host":"h","short_message":"accepted","level":6}`),
		newItemIn(f.space, `{"version":"1.1","host":"h","short_message":"panic","level":0}`))
	if b.Envelopes[0].Res.Level != 6 || b.Envelopes[1].Res.Level != 0 {
		t.Fatalf("levels %d %d, want 6 0", b.Envelopes[0].Res.Level, b.Envelopes[1].Res.Level)
	}
	if err := p.Write(context.Background(), b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	want := "loghub." + f.space + ".gelf"
	if len(got) != 2 || got[0].Subject != want || got[1].Subject != want {
		t.Fatalf("subjects = %v, want %s twice", subjectsOf(got), want)
	}
}

func subjectsOf(ms []*jetstream.RawStreamMsg) []string {
	var out []string
	for _, m := range ms {
		out = append(out, m.Subject)
	}
	return out
}

// SPEC §3.9 against a real server: the largest raw line the boot check lets
// through, shaped for the largest JLH, is accepted by the server's actual
// max_payload; one byte more of convert.max_message_bytes is refused at boot.
func TestNATSLargestAcceptedMessageFitsTheServer(t *testing.T) {
	f := natsSetup(t)
	payload := f.nc.MaxPayload()
	header := int64(natsHeaderMax + natsExpectedStreamHeader + len(f.stream))
	L := (payload - header - 8<<10) / 6

	cfg := f.cfg
	cfg.MaxEventBytes = MaxEventBytes(L)
	p := f.sink(cfg)
	f.ready(p)

	cc := convert.DefaultConfig()
	cc.MaxMessageBytes = int(L)
	// A document of control bytes is malformed and travels whole as raw,
	// every byte escaped as \u00XX: the 6 × L of SPEC §3.9.
	line := strings.Repeat("\x01", int(L))
	b := batchWith(t, cc, newItemIn(f.space, line))
	if int64(len(b.Envelopes[0].JLH)) < 6*L-L/16 {
		t.Fatalf("the envelope is only %d bytes for a %d-byte line; not a worst case", len(b.Envelopes[0].JLH), L)
	}
	if err := p.Write(context.Background(), b); err != nil {
		t.Fatalf("a message the boot check admits was refused by the server (max_payload=%d, line=%d, JLH=%d): %v",
			payload, L, len(b.Envelopes[0].JLH), err)
	}

	cfg.MaxEventBytes = MaxEventBytes(L + 1)
	over := f.sink(cfg)
	if err := over.Health(context.Background()); err == nil || !IsFatal(err) || !strings.Contains(err.Error(), "max_payload") {
		t.Errorf("one byte past the ceiling passed the boot check: %v", err)
	}
}

// ------------------------------------------------------------- validation

// SPEC §10.7: each refusal is FATAL, so the boot stops at once.
func TestNATSHealthValidatesTheStream(t *testing.T) {
	f := natsSetup(t)
	_, other := f.newStream(nil)
	smallSpace, small := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 1024 })
	exactSpace, exactOnly := f.newStream(func(c *jetstream.StreamConfig) { c.Subjects = c.Subjects[:1] })
	// A stream of the syslog sibling on this space: same space, other type.
	siblingSpace, sibling := f.newStream(func(c *jetstream.StreamConfig) {
		for i := range c.Subjects {
			c.Subjects[i] = strings.TrimSuffix(c.Subjects[i], ".gelf") + ".syslog"
		}
	})
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
		{"missing stream", func(c *NATSConfig) { c.Stream = "SLS_MISSING_STREAM" }, "stream"},
		{"stream of another space", func(c *NATSConfig) { c.Stream = other }, "does not cover"},
		{"exact-subject stream", func(c *NATSConfig) { c.Stream, c.Workspace = exactOnly, exactSpace }, ""},
		{"stream of a sibling type", func(c *NATSConfig) { c.Stream, c.Workspace = sibling, siblingSpace }, "does not cover"},
		{"stream capturing the control plane", func(c *NATSConfig) { c.Stream, c.Workspace = ctl, ctlSpace }, "control plane"},
		{"sealed stream", func(c *NATSConfig) { c.Stream, c.Workspace = sealed, sealedSpace }, "sealed"},
		// Review finding 4: it would never acknowledge a publish.
		{"no_ack stream", func(c *NATSConfig) { c.Stream, c.Workspace = noAck, noAckSpace }, "no_ack"},
		{"payload ceiling", func(c *NATSConfig) { c.MaxEventBytes = f.nc.MaxPayload() }, "max_payload"},
		{"stream max_msg_size", func(c *NATSConfig) {
			// 900 plus the header block passes the stream's 1024.
			c.Stream, c.Workspace, c.MaxEventBytes = small, smallSpace, 900
		}, "max_msg_size"},
		{"stream max_msg_size, fitting", func(c *NATSConfig) {
			c.Stream, c.Workspace, c.MaxEventBytes = small, smallSpace, 600
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
				if err := p.Write(ctx, batchFrom(t, f.items(cfg.Workspace, 2)...)); err != nil {
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

// SPEC §10.7: the expected-stream header makes the server refuse a publish
// whose subject belongs to another stream (10060), and the refusal is fatal, so
// a misconfiguration that slipped past the boot check cannot write anywhere.
func TestNATSExpectedStreamMismatchIsFatalOnWrite(t *testing.T) {
	f := natsSetup(t)
	otherSpace, other := f.newStream(nil)
	p := f.sink(f.cfg) // deliberately not validated: this is the write path

	err := p.Write(context.Background(), batchFrom(t, f.items(otherSpace, 1)...))
	if err == nil {
		t.Fatal("a publish into another stream was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s (%v), want fatal", ClassOf(err), err)
	}
	if !taints(err) {
		t.Error("an ack failure must taint the chunk: other futures may still be pending")
	}
	if n, m := f.count(f.stream), f.count(other); n != 0 || m != 0 {
		t.Errorf("%d and %d messages landed", n, m)
	}
}

// SPEC §10.7: a stream deleted after boot is caught by the health check as
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
	if err := p.Write(ctx, batchFrom(t, f.items(f.space, 1)...)); err == nil {
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

// SPEC §10.7: core mode publishes by core NATS and flushes once per chunk; the
// server received the messages, and a subscriber got them -- the JLH in Data,
// the same headers.
func TestNATSCoreMode(t *testing.T) {
	f := natsSetup(t)
	space := fmt.Sprintf("core%d", time.Now().UnixNano())
	sub, err := f.nc.SubscribeSync("loghub." + space + ".gelf")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	cfg := f.cfg
	cfg.Mode, cfg.Stream, cfg.Workspace = NATSModeCore, "", space
	cfg.MaxEventBytes = MaxEventBytes(64 << 10)
	p := f.sink(cfg)
	f.ready(p)

	b := batchFrom(t, f.items(space, 3)...)
	if err := p.Write(context.Background(), b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for i := range b.Envelopes {
		m, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if string(m.Data) != string(b.Envelopes[i].JLH) || m.Header.Get("Nats-Msg-Id") != b.Envelopes[i].UUID()+".0" {
			t.Errorf("message %d: %q %v", i, m.Data, m.Header)
		}
	}

	over := cfg
	over.MaxEventBytes = f.nc.MaxPayload()
	if err := f.sink(over).Health(context.Background()); !IsFatal(err) {
		t.Errorf("core mode accepted a largest event above max_payload: %v", err)
	}
}

// SPEC §11: what the beacon needs of the sink -- the connect hook on the first
// connection and on every reconnection, a core publish on the same connection,
// and the server address.
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
	waitFor(t, 10*time.Second, func() bool { return calls.Load() == 2 && p.m.SinkNATSReconnectsTotal.Load() == 1 })
}

// The whole drain path against the real server: ring, workers, conversion,
// async publish, acks.
func TestNATSWorkersEndToEnd(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	rg := ring.NewMemory(1000, 0, time.Millisecond)
	m := metrics()
	w := NewWorkers(rg, p, NewHealth(p, HealthConfig{}, discard(), m), workerCfg(32), discard(), m)
	items := f.items(f.space, 300)
	enqueueAll(t, rg, items)
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	waitFor(t, 30*time.Second, func() bool { return m.EventsWrittenTotal.Load() == 300 })
	cancel()
	w.Wait()

	if n := f.count(f.stream); n != 300 {
		t.Fatalf("%d messages, want 300", n)
	}
	for _, msg := range f.messages(f.stream) {
		if err := jlhcheck.Validate(msg.Data); err != nil {
			t.Fatal(err)
		}
	}
	for _, it := range items {
		if !it.Released() || it.Tainted() {
			t.Fatal("an item was not released cleanly after delivery")
		}
	}
}

// Review finding 5: SPEC §10.7 lists "JetStream desligado" as fatal. A server
// without JetStream answers nobody on the stream API (no responders, which on
// its own is network); the first validation tells it apart and ends the boot at
// once instead of after sink.boot_timeout. Needs a nats-server WITHOUT -js:
//
//	go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &
//	GELF_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 go test -race ./internal/sink/
func TestNATSJetStreamDisabledIsFatal(t *testing.T) {
	url := os.Getenv("GELF_TEST_NATS_NOJS_URL")
	if url == "" {
		t.Skip("set GELF_TEST_NATS_NOJS_URL to a nats-server started without -js")
	}
	p, err := NewNATS(NATSConfig{URLs: []string{url}, Stream: "LOGHUB_GELF", Workspace: "default",
		ConnectTimeout: 5 * time.Second}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	h := NewHealth(p, HealthConfig{Timeout: 5 * time.Second}, discard(), metrics())
	start := time.Now()
	err = h.WaitReady(context.Background(), 30*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	if !IsFatal(err) || !strings.Contains(err.Error(), "JetStream") {
		t.Fatalf("WaitReady on a server without JetStream = %v (class %s), want fatal", err, ClassOf(err))
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the verdict took %v; it must not wait out the boot timeout", d)
	}

	// core mode needs no JetStream at all.
	core, err := NewNATS(NATSConfig{URLs: []string{url}, Mode: NATSModeCore, Workspace: "default"}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	if err := core.Health(context.Background()); err != nil {
		t.Fatalf("core mode on a server without JetStream: %v", err)
	}
}

// Review finding 8: one message the stream refuses for good (10054, above its
// max_msg_size) does not make the chunk "lost" as a whole: the other futures
// say which messages were stored, and they come back in Error.Delivered.
func TestNATSLogicRefusalReportsTheAckedMessages(t *testing.T) {
	f := natsSetup(t)
	space, stream := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 2000 })
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, space
	p := f.sink(cfg) // not validated: the point is the write path

	b := batchFrom(t, newItemIn(space, gdoc("small zero")), newItemIn(space, gdoc(strings.Repeat("x", 3000))),
		newItemIn(space, gdoc("small two")))
	err := p.Write(context.Background(), b)
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
	if stored, lost := settleLogic(err, b.Envelopes); stored != 2 || len(lost) != 1 || lost[0] != b.Envelopes[1].UUID() {
		t.Fatalf("settled as %d stored, lost %v", stored, lost)
	}
}
