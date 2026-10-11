package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

// What is worth proving here -- the async publish with futures, the server's
// deduplication by Nats-Msg-Id, the expected-stream refusal, the stream
// validation, and the largest message the boot check admits actually fitting
// the server -- is exactly what a fake would have to invent. These run against
// a real server with JetStream, addressed by SYSLOG_TEST_NATS_URL, and skip
// without it (docs/TESTING.md §6.4):
//
//	nats-server -js -sd "$(mktemp -d)" -p 14222 &
//	SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race ./internal/sink/
//
// Every test creates its own stream from docker/nats/stream.json and deletes it
// afterwards, so the suite never depends on an empty server.

const natsURLEnv = "SYSLOG_TEST_NATS_URL"

type natsFixture struct {
	t       *testing.T
	url     string
	nc      *nats.Conn
	js      jetstream.JetStream
	stream  string
	subject string
	cfg     NATSConfig
}

func natsSetup(t *testing.T) *natsFixture {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the nats sink tests (see docs/TESTING.md §6.4)", natsURLEnv)
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
	f.stream, f.subject = f.newStream(nil)
	f.cfg = NATSConfig{
		URLs: []string{url}, Stream: f.stream, Subject: f.subject, ClientName: "sls-test",
		ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second, MaxPending: 4096,
	}
	return f
}

// newStream creates a stream from the canonical config under a unique name and
// registers its removal. mut adjusts the config first.
func (f *natsFixture) newStream(mut func(*jetstream.StreamConfig)) (name, subject string) {
	f.t.Helper()
	name = fmt.Sprintf("SLS_TEST_%d", time.Now().UnixNano())
	prefix := strings.ToLower(name)
	cfg := natsStreamConfigFor(f.t, name, prefix+".>")
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.CreateStream(ctx, cfg); err != nil {
		f.t.Fatalf("creating stream %s: %v", name, err)
	}
	f.t.Cleanup(func() { _ = f.js.DeleteStream(context.Background(), name) })
	return name, prefix + ".in"
}

// natsStreamConfigFor reads docker/nats/stream.json -- the file operators hand
// to `nats stream add` -- and retargets it at a test stream. Using the real file
// is the point: the validation of SPEC §10.8.5 and the stream the operator gets
// must agree.
func natsStreamConfigFor(t *testing.T, name, subject string) jetstream.StreamConfig {
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
	cfg.Subjects = []string{subject}
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

type storedMsg struct {
	id, version string
	env, raw    []byte
}

// messages reads every message of a stream back, in sequence order.
func (f *natsFixture) messages(stream string) []storedMsg {
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
	var out []storedMsg
	if info.State.Msgs == 0 {
		return out
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		raw, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		// SPEC §3.8, read the way a consumer reads it.
		size, err := strconv.Atoi(raw.Header.Get(natsHeaderEnvelopeSize))
		if err != nil || size < 0 || size > len(raw.Data) {
			f.t.Fatalf("message %d: Sls-Envelope-Size = %q for %d bytes of Data",
				seq, raw.Header.Get(natsHeaderEnvelopeSize), len(raw.Data))
		}
		out = append(out, storedMsg{
			id:      raw.Header.Get(jetstream.MsgIDHeader),
			version: raw.Header.Get(natsHeaderVersion),
			env:     raw.Data[:size],
			raw:     raw.Data[size:],
		})
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

// SPEC §3.8: id and version in headers, the envelope then the raw line in
// Data, one message per syslog message.
func TestNATSPublishStoresEnvelopeAndRaw(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := batchOf(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := f.messages(f.stream)
	if len(got) != len(batch) {
		t.Fatalf("%d messages, want %d", len(got), len(batch))
	}
	byID := map[string]*Prepared{}
	for i := range batch {
		byID[batch[i].Item.Head.ID] = &batch[i]
	}
	for _, m := range got {
		want, ok := byID[m.id]
		if !ok {
			t.Errorf("message with unknown Nats-Msg-Id %q", m.id)
			continue
		}
		if m.version != envelope.Version {
			t.Errorf("%s: Sls-Version = %q, want %q", m.id, m.version, envelope.Version)
		}
		if string(m.env) != string(want.Env) {
			t.Errorf("%s: envelope = %q, want %q", m.id, m.env, want.Env)
		}
		if string(m.raw) != string(want.Raw()) {
			t.Errorf("%s: raw line = %q, want %q", m.id, m.raw, want.Raw())
		}
		var e map[string]any
		if err := json.Unmarshal(m.env, &e); err != nil {
			t.Fatalf("%s: the stored envelope is not JSON: %v", m.id, err)
		}
		if e["id"] != m.id {
			t.Errorf("%s: the envelope says id %v", m.id, e["id"])
		}
		if int(e["raw-size"].(float64)) != len(m.raw) {
			t.Errorf("%s: raw-size = %v, the raw line has %d bytes", m.id, e["raw-size"], len(m.raw))
		}
	}
}

// The raw line must carry NUL, newline, CR and a byte that is not UTF-8,
// unchanged; an empty line leaves Data holding the envelope alone.
func TestNATSPreservesBinaryAndEmptyRaw(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	line := "<13>1 - h a - - - A\x00B\nC\xffD\r\x00"
	batch := prepared(t, line, "")
	if err := p.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for _, m := range f.messages(f.stream) {
		switch m.id {
		case batch[0].Item.Head.ID:
			if string(m.raw) != line {
				t.Errorf("binary line = %q, want %q byte for byte", m.raw, line)
			}
		case batch[1].Item.Head.ID:
			if len(m.raw) != 0 || string(m.env) != string(batch[1].Env) {
				t.Errorf("empty line published as envelope %q and raw %q", m.env, m.raw)
			}
		}
	}
	if n := f.count(f.stream); n != 2 {
		t.Errorf("%d messages, want 2", n)
	}
}

// SPEC §3.8: an envelope built from hostile emitter bytes -- control bytes, DEL,
// invalid UTF-8, quotes -- and one far above the 64 KiB JetStream allows a
// header block both come back exactly as written, and decode to what the
// emitter sent. In a header, the second one was a 10097.
func TestNATSEnvelopeSurvivesHostileBytes(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	host := "web\x01\x7f\xffé -01"
	line := "<13>Oct 11 22:14:15 " + host + " app[42]: body"
	sd := `<13>1 - h a p m [x@1 k="quote \" backslash \\ bracket \] ctl ` + "\x1b\x7f" + `"] body`
	huge := `<13>1 - h a p m [x@1 k="` + strings.Repeat("\x01", 100<<10) + `"] body`
	batch := prepared(t, line, sd, huge)
	if len(batch[2].Env) <= 64<<10 {
		t.Fatalf("the large envelope is only %d bytes", len(batch[2].Env))
	}
	if err := p.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	stored := map[string]storedMsg{}
	for _, m := range f.messages(f.stream) {
		stored[m.id] = m
	}
	for i := range batch {
		m, ok := stored[batch[i].Item.Head.ID]
		if !ok {
			t.Fatalf("message %d is not in the stream", i)
		}
		if string(m.env) != string(batch[i].Env) {
			t.Errorf("message %d: the envelope came back different (%d bytes, want %d)", i, len(m.env), len(batch[i].Env))
		}
		if string(m.raw) != string(batch[i].Raw()) {
			t.Errorf("message %d: the raw line came back different", i)
		}
		var e map[string]any
		if err := json.Unmarshal(m.env, &e); err != nil {
			t.Fatalf("message %d: the envelope is not JSON: %v", i, err)
		}
		if i == 0 && e["hostname"] != "web\x01\x7f�é -01" {
			t.Errorf("hostname decoded as %q", e["hostname"])
		}
		if i == 1 {
			v := e["structured-data"].(map[string]any)["x@1"].(map[string]any)["k"]
			if v != "quote \" backslash \\ bracket ] ctl \x1b\x7f" {
				t.Errorf("structured-data value decoded as %q", v)
			}
		}
	}
}

// SPEC §10.8.3: the pipeline is at-least-once, and a redelivery inside the
// duplicate window is acknowledged as a duplicate, stored once and counted.
func TestNATSRedeliveryIsDeduplicatedAndCounted(t *testing.T) {
	f := natsSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)

	batch := batchOf(t, 7)
	ctx := context.Background()
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if err := p.Write(ctx, batch); err != nil {
		t.Fatalf("redelivered Write: %v", err)
	}
	fresh := prepared(t, "<13>fresh one", "<13>fresh two")
	for i := range fresh {
		fresh[i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-%012d", 900+i)
	}
	mixed := append(batch[:3:3], fresh...)
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
	batches := make([][]Prepared, writers)
	for w := range batches {
		lines := make([]string, per)
		for i := range lines {
			lines[i] = fmt.Sprintf("<13>writer %d message %d", w, i)
		}
		batches[w] = prepared(t, lines...)
		for i := range batches[w] {
			batches[w][i].Item.Head.ID = fmt.Sprintf("019823f1-7a2c-7b3d-%04d-%012d", w, i)
		}
	}
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(b []Prepared) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			errs <- p.Write(ctx, b)
		}(batches[w])
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

// SPEC §3.9 against a real server: the largest raw line the boot check lets
// through, shaped for the largest envelope, is accepted by the server's actual
// max_payload. The bound errs on the safe side of the limit, never over it.
func TestNATSLargestAcceptedMessageFitsTheServer(t *testing.T) {
	f := natsSetup(t)
	payload := f.nc.MaxPayload()

	// The longest line whose worst case still passes the boot check.
	listener, node := int64(len(":1514")), int64(len("sls-test"))
	fits := func(raw int64) bool {
		cfg := f.cfg
		cfg.MaxRawBytes, cfg.MaxEnvelopeBytes = raw, envelope.MaxBytes(raw, listener, node)
		return (&NATS{cfg: cfg}).maxMessageBytes() <= payload
	}
	lo, hi := int64(1), payload
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if fits(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	cfg := f.cfg
	cfg.MaxRawBytes, cfg.MaxEnvelopeBytes = lo, envelope.MaxBytes(lo, listener, node)
	p := f.sink(cfg)
	f.ready(p)

	prefix := "<13>Oct 11 22:14:15 "
	suffix := " app: m"
	line := prefix + strings.Repeat("\x01", int(lo)-len(prefix)-len(suffix)) + suffix
	batch := prepared(t, line)
	if int64(len(line)) != lo {
		t.Fatalf("built a %d-byte line, want %d", len(line), lo)
	}
	if err := p.Write(context.Background(), batch); err != nil {
		t.Fatalf("a message the boot check admits was refused by the server (max_payload=%d, line=%d, envelope=%d): %v",
			payload, len(line), len(batch[0].Env), err)
	}

	cfg.MaxRawBytes++
	cfg.MaxEnvelopeBytes = envelope.MaxBytes(cfg.MaxRawBytes, listener, node)
	over := f.sink(cfg)
	if err := over.Health(context.Background()); err == nil || !IsFatal(err) {
		t.Errorf("one byte past the ceiling passed the boot check: %v", err)
	}
}

// ------------------------------------------------------------- validation

// SPEC §10.8.5: each refusal is FATAL, so the boot stops at once.
func TestNATSHealthValidatesTheStream(t *testing.T) {
	f := natsSetup(t)
	_, otherSubject := f.newStream(nil)
	small, smallSubject := f.newStream(func(c *jetstream.StreamConfig) { c.MaxMsgSize = 1024 })

	sealed, sealedSubject := f.newStream(nil)
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
		{"subject of another stream", func(c *NATSConfig) { c.Subject = otherSubject }, "belongs to stream"},
		{"subject with no stream", func(c *NATSConfig) { c.Subject = "nobody.home" }, "no stream listens"},
		{"sealed stream", func(c *NATSConfig) { c.Stream, c.Subject = sealed, sealedSubject }, "sealed"},
		{"payload ceiling", func(c *NATSConfig) { c.MaxRawBytes = f.nc.MaxPayload() }, "max_payload"},
		{"stream max_msg_size", func(c *NATSConfig) {
			c.Stream, c.Subject, c.MaxRawBytes, c.MaxEnvelopeBytes = small, smallSubject, 512, 512
		}, "max_msg_size"},
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
				if err := p.Write(ctx, batchOf(t, 2)); err != nil {
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

// SPEC §10.8.2: the expected-stream header makes the server refuse a publish
// whose subject belongs to another stream, and the refusal is fatal, so a
// misconfiguration that slipped past the boot check cannot write anywhere.
func TestNATSExpectedStreamMismatchIsFatalOnWrite(t *testing.T) {
	f := natsSetup(t)
	_, otherSubject := f.newStream(nil)
	cfg := f.cfg
	cfg.Subject = otherSubject
	p := f.sink(cfg) // deliberately not validated: this is the write path

	err := p.Write(context.Background(), batchOf(t, 1))
	if err == nil {
		t.Fatal("a publish into another stream was accepted")
	}
	if !IsFatal(err) {
		t.Errorf("class = %s, want fatal", ClassOf(err))
	}
	if n := f.count(f.stream); n != 0 {
		t.Errorf("%d messages landed in the configured stream", n)
	}
}

// SPEC §10.8.7: a stream deleted after boot is caught by the health check as
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
	if err := p.Write(ctx, batchOf(t, 1)); err == nil {
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
