package app

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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
)

// sink=nats, the default destination (SPEC §10.7), against a real nats-server
// with JetStream addressed by GELF_TEST_NATS_URL; skipped without it:
//
//	source tasks/infra/env.sh   # GELF_TEST_NATS_URL, from the shared test infrastructure
//	go test -race -run NATS ./internal/app/
//
// Every test creates its stream from docker/nats/stream.json -- the file the
// operator applies; the server never creates one (invariant §20.17) -- under a
// unique name, and deletes it afterwards.

const natsURLEnv = "GELF_TEST_NATS_URL"

type natsEnv struct {
	url string
	nc  *nats.Conn
	js  jetstream.JetStream
}

func natsTest(t *testing.T) *natsEnv {
	t.Helper()
	url := os.Getenv(natsURLEnv)
	if url == "" {
		t.Skipf("set %s to run the nats suite", natsURLEnv)
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
	return &natsEnv{url: url, nc: nc, js: js}
}

// stream creates the canonical stream under a unique name, its subject
// narrowed to loghub.default.gelf.
func (e *natsEnv) stream(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatalf("reading the canonical stream config: %v", err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("parsing the canonical stream config: %v", err)
	}
	cfg.Name = fmt.Sprintf("SLS_APP_%d", time.Now().UnixNano())
	// Narrowed to the instances' own space: loghub.*.gelf overlaps the
	// loghub.t<n>.gelf streams internal/sink creates when the two packages
	// run in parallel on the same server, and JetStream refuses the second
	// one (10065, review 2026-10-31). testID and otherID share the space.
	for i, sub := range cfg.Subjects {
		cfg.Subjects[i] = strings.Replace(sub, "loghub.*.", "loghub."+testID.Space+".", 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.CreateStream(ctx, cfg); err != nil {
		t.Fatalf("creating stream %s: %v", cfg.Name, err)
	}
	// Registered on the CALLER's test: a subtest's stream must be gone before
	// the next subtest creates one over the same subjects.
	t.Cleanup(func() { _ = e.js.DeleteStream(context.Background(), cfg.Name) })
	return cfg.Name
}

func (e *natsEnv) args(dir, stream string, extra ...string) []string {
	return append(append(baseArgs(dir),
		"--sink.type=nats",
		"--sink.nats.urls="+e.url,
		"--sink.nats.stream="+stream,
	), extra...)
}

// natsMsg is one stored message.
type natsMsg struct {
	subject string
	header  nats.Header
	env     jlhEnv
}

// count is the number of messages the stream holds.
func (e *natsEnv) count(stream string) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, stream)
	if err != nil {
		return 0
	}
	info, err := s.Info(ctx)
	if err != nil {
		return 0
	}
	return info.State.Msgs
}

// consume reads n messages back through an ordered consumer filtered on
// filter, the way a LogHub consumer would, and validates every envelope.
func (e *natsEnv) consume(t *testing.T, stream, filter string, n int) []natsMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{filter}})
	if err != nil {
		t.Fatal(err)
	}
	var out []natsMsg
	for len(out) < n {
		batch, err := cons.Fetch(n-len(out), jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		got := 0
		for m := range batch.Messages() {
			got++
			out = append(out, natsMsg{subject: m.Subject(), header: m.Headers(), env: mustJLH(t, m.Data())})
		}
		if got == 0 {
			t.Fatalf("consumed %d of %d messages on %s", len(out), n, filter)
		}
	}
	return out
}

// checkHeaders is SPEC §3.8.
func checkHeaders(t *testing.T, m natsMsg) {
	t.Helper()
	for k, want := range map[string]string{
		"Nats-Msg-Id":    m.env.UUID + ".0",
		"Content-Type":   "application/json",
		"X-LogHub-Type":  "gelf",
		"X-LogHub-UUID":  m.env.UUID,
		"X-LogHub-Space": m.env.Space,
	} {
		if got := m.header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}
}

func natsLines(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"version":"1.1","host":"web-01","short_message":"%s-%04d","level":5,"_n":%d}`, prefix, i, i)
	}
	return out
}

// SPEC §3.8, §10.7: ring=memory -> nats end to end. Each envelope lands on the
// exact subject loghub.default.gelf, in Data, with the headers of §3.8 --
// Nats-Msg-Id <uuid>.<step>, never the bare uuid -- and passes both schemas.
func TestNATSMemoryEndToEnd(t *testing.T) {
	e := natsTest(t)
	stream := e.stream(t)
	in := launch(t, testID, e.args(t.TempDir(), stream), nil)
	in.waitReady()

	lines := natsLines("nats-memory", 30)
	sendNUL(t, in.app.TransportAddr("tcp"), lines...)
	sendUDP(t, in.app.TransportAddr("udp"), doc("via-udp \x00 nul"))
	want := len(lines) + 1
	waitFor(t, 20*time.Second, "every message in the stream", func() bool { return e.count(stream) == uint64(want) })

	msgs := e.consume(t, stream, "loghub.default.gelf", want)
	seen := map[string]bool{}
	for _, m := range msgs {
		if m.subject != "loghub.default.gelf" {
			t.Errorf("subject %q, want the exact loghub.default.gelf", m.subject)
		}
		checkHeaders(t, m)
		if m.env.From != testID.From || m.env.Space != "default" {
			t.Errorf("envelope identity %s/%s", m.env.From, m.env.Space)
		}
		seen[m.env.Payload.message()] = true
	}
	for i := range lines {
		if !seen[fmt.Sprintf("nats-memory-%04d", i)] {
			t.Errorf("nats-memory-%04d is missing", i)
		}
	}
	if !seen["via-udp \x00 nul"] {
		t.Error("the UDP message with a NUL is missing")
	}
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="nats",transports="udp,tcp"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

// SPEC §3.8 (decision 16): there is only the exact subject. Documents of every
// level land on loghub.default.gelf; nothing goes below it.
func TestNATSOnlyTheExactSubject(t *testing.T) {
	e := natsTest(t)
	stream := e.stream(t)
	in := launch(t, testID, e.args(t.TempDir(), stream), nil)
	in.waitReady()

	var lines []string
	for lvl := 0; lvl <= 7; lvl++ {
		lines = append(lines, gdoc(fmt.Sprintf("level-%d", lvl), lvl))
	}
	sendNUL(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 20*time.Second, "eight messages in the stream", func() bool { return e.count(stream) == 8 })
	seen := map[int]bool{}
	for _, m := range e.consume(t, stream, "loghub.>", 8) {
		checkHeaders(t, m)
		if m.subject != "loghub.default.gelf" {
			t.Errorf("%q went to %q, want the exact loghub.default.gelf", m.env.Payload.message(), m.subject)
		}
		if m.env.Payload.message() != fmt.Sprintf("level-%d", m.env.Level) {
			t.Errorf("level %d carries %q", m.env.Level, m.env.Payload.message())
		}
		seen[m.env.Level] = true
	}
	if len(seen) != 8 {
		t.Errorf("levels seen: %v", seen)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

// SPEC §11: the heartbeat beacon goes to loghub._ctl.beacon by core NATS, on
// the first connection, as a JLH of type beacon, level 9, from this instance.
func TestNATSBeacon(t *testing.T) {
	e := natsTest(t)
	stream := e.stream(t)
	sub, err := e.nc.SubscribeSync("loghub._ctl.beacon")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := e.nc.Flush(); err != nil {
		t.Fatal(err)
	}

	in := launch(t, testID, e.args(t.TempDir(), stream, "--loghub.beacon_interval=1"), nil)
	in.waitReady()

	var beacon struct {
		UUID    string `json:"uuid"`
		From    string `json:"from"`
		Space   string `json:"space"`
		Type    string `json:"type"`
		Level   int    `json:"level"`
		SID     int    `json:"sid"`
		Step    int    `json:"step"`
		Payload struct {
			Name     string `json:"name"`
			Hostname string `json:"hostname"`
			Machine  string `json:"machine"`
			Address  string `json:"address"`
			Uptime   int64  `json:"uptime"`
			Meta     []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"meta"`
		} `json:"payload"`
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		m, err := sub.NextMsg(time.Until(deadline))
		if err != nil {
			t.Fatalf("no beacon from this instance on loghub._ctl.beacon: %v", err)
		}
		if err := jlhcheck.ValidateEnvelopeSchema(m.Data); err != nil {
			t.Errorf("the beacon is not a valid JLH envelope: %v\n%s", err, m.Data)
		}
		if err := json.Unmarshal(m.Data, &beacon); err != nil {
			t.Fatalf("beacon: %v\n%s", err, m.Data)
		}
		if beacon.From == testID.From {
			break
		}
	}
	if beacon.Type != "beacon" || beacon.Level != 9 || beacon.Space != "default" || beacon.Step != 0 || beacon.SID != os.Getpid() {
		t.Errorf("beacon envelope = type %s level %d space %s step %d sid %d", beacon.Type, beacon.Level, beacon.Space, beacon.Step, beacon.SID)
	}
	p := beacon.Payload
	if p.Name != testID.AgentName || p.Hostname != testID.Hostname || p.Machine != testID.MachineID || p.Address == "" {
		t.Errorf("beacon payload = %+v", p)
	}
	meta := map[string]any{}
	for _, kv := range p.Meta {
		meta[kv.Key] = kv.Value
	}
	if meta["sink"] != "nats" || meta["ring"] != "memory" || fmt.Sprint(meta["transports"]) != "[udp tcp]" || meta["version"] == nil {
		t.Errorf("beacon meta = %v", meta)
	}
	// The periodic beacon (interval 1 s) follows.
	waitFor(t, 10*time.Second, "a periodic beacon", func() bool {
		return in.app.metrics.BeaconsOK.Load() >= 2
	})
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	if n := e.count(stream); n != 0 {
		t.Errorf("the data stream captured %d control-plane messages", n)
	}
}

// SPEC §9.4, §9.6, §19 with sink=nats: ring=sqlite and ring=none accept, exit
// 0 and leave every accepted message in the stream. With ring=none the
// acceptance IS the PubAck: the message is in the stream at that instant.
func TestNATSRingModes(t *testing.T) {
	e := natsTest(t)
	for _, mode := range []string{"sqlite", "none"} {
		t.Run(mode, func(t *testing.T) {
			stream := e.stream(t)
			in := launch(t, testID, e.args(t.TempDir(), stream, "--ring.mode="+mode), nil)
			in.waitReady()
			lines := natsLines("nats-"+mode, 20)
			sendNUL(t, in.app.TransportAddr("tcp"), lines...)
			waitFor(t, 20*time.Second, "every message accepted", func() bool {
				return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == uint64(len(lines))
			})
			if mode == "none" {
				if got := e.count(stream); got != uint64(len(lines)) {
					t.Errorf("ring=none: %d messages in the stream at the instant of acceptance, want %d", got, len(lines))
				}
				// The write-through is instrumented like the workers (SPEC §9.6).
				n := strconv.Itoa(len(lines))
				for series, want := range map[string]string{
					"gelf_events_written_total":        n,
					"gelf_sink_duration_seconds_count": n,
					"gelf_sink_batch_size_count":       n,
					"gelf_sink_nats_duplicates_total":  "0",
					"gelf_sink_nats_reconnects_total":  "0",
				} {
					if got := in.metric(series); got != want {
						t.Errorf("%s = %s, want %s", series, got, want)
					}
				}
			}
			if got := in.metric(fmt.Sprintf(`gelf_config_info{ring_mode=%q,sink_type="nats",transports="udp,tcp"}`, mode)); got != "1" {
				t.Errorf("gelf_config_info = %s", got)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit %d", code)
			}
			msgs := e.consume(t, stream, "loghub.default.gelf", len(lines))
			seen := map[string]bool{}
			for _, m := range msgs {
				checkHeaders(t, m)
				seen[m.env.Payload.message()] = true
			}
			for _, l := range lines {
				if !seen[shortOf(l)] {
					t.Errorf("%q never reached the stream", l)
				}
			}
		})
	}
}
