package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func metrics() *obs.Metrics {
	return obs.NewMetrics(obs.Info{RingMode: "memory", SinkType: "nats", Transports: "udp,tcp", Version: "test"})
}

var testIdentity = ident.Identity{
	From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
	Space:     "acme.prod",
	AgentName: "go-syslog-server",
	Hostname:  "syslog-01",
	MachineID: "79fb72c3199f4b85978420223a389b94",
}

// fakePub records what the beacon publishes.
type fakePub struct {
	mu      sync.Mutex
	addr    string
	fail    error
	subject []string
	data    [][]byte
}

func (p *fakePub) PublishCore(subject string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	p.subject = append(p.subject, subject)
	p.data = append(p.data, append([]byte(nil), data...))
	return nil
}

func (p *fakePub) ServerAddr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}

func (p *fakePub) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.data)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func testConfig() Config {
	return Config{
		Identity: testIdentity, Version: "1.2.3", RingMode: "memory",
		Transports: []string{"udp", "tcp", "tls"},
		Started:    time.Now().Add(-90 * time.Second),
	}
}

// keysInOrder returns the keys of the object at the start of raw, in document
// order.
func keysInOrder(t *testing.T, raw []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %v", err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// SPEC §11: the envelope -- type beacon, level 9, sid = PID, step 0, uuid and
// event from one sample, from/space from the identity -- and the payload with
// its keys in order, meta in order, valid against the JLH envelope schema.
func TestBeaconEnvelope(t *testing.T) {
	cfg := testConfig()
	cfg.Started = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	b := New(&fakePub{}, cfg, discard(), metrics())
	now := time.Date(2026, 10, 5, 12, 34, 56, 789123456, time.UTC)
	raw := b.Envelope(now, "10.20.0.14")

	if err := jlhcheck.ValidateEnvelopeSchema(raw); err != nil {
		t.Fatalf("the beacon fails the JLH envelope schema: %v\n%s", err, raw)
	}
	var e struct {
		UUID  string `json:"uuid"`
		From  string `json:"from"`
		Space string `json:"space"`
		Type  string `json:"type"`
		Level int    `json:"level"`
		SID   int64  `json:"sid"`
		Step  int    `json:"step"`
		Event struct {
			UTS int64  `json:"uts"`
			Sub int64  `json:"sub"`
			ISO string `json:"iso"`
		} `json:"event"`
		Payload struct {
			Name     string `json:"name"`
			Hostname string `json:"hostname"`
			Machine  string `json:"machine"`
			Address  string `json:"address"`
			Uptime   int64  `json:"uptime"`
			Meta     []struct {
				Key   string          `json:"key"`
				Value json.RawMessage `json:"value"`
			} `json:"meta"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	if e.Type != "beacon" || e.Level != 9 || e.Step != 0 || e.SID != int64(os.Getpid()) ||
		e.From != testIdentity.From || e.Space != testIdentity.Space {
		t.Errorf("envelope header: %+v", e)
	}
	if e.Event.UTS != now.Unix() || e.Event.Sub != int64(now.Nanosecond()) || e.Event.ISO != "2026-10-05T12:34:56.789Z" {
		t.Errorf("event = %+v", e.Event)
	}
	// One sample: the uuid's instant is the event's, to the nanosecond.
	u, err := uuidv7.FromString(e.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if ts, ok := u.TimestampWithLevel(uuidv7.Level3); !ok || !ts.Equal(now) {
		t.Errorf("uuid instant %v, want %v", ts, now)
	}

	p := e.Payload
	if p.Name != "go-syslog-server" || p.Hostname != "syslog-01" || p.Machine != testIdentity.MachineID ||
		p.Address != "10.20.0.14" {
		t.Errorf("payload = %+v", p)
	}
	if want := int64(34*60*1000 + 56*1000 + 789); p.Uptime != want {
		t.Errorf("uptime = %d ms, want %d", p.Uptime, want)
	}

	if got := keysInOrder(t, raw); len(got) != 9 || got[0] != "uuid" || got[8] != "payload" {
		t.Errorf("envelope keys = %v", got)
	}
	start := bytes.Index(raw, []byte(`"payload":`)) + len(`"payload":`)
	want := []string{"name", "hostname", "machine", "address", "uptime", "meta"}
	if got := keysInOrder(t, raw[start:len(raw)-1]); len(got) != len(want) {
		t.Errorf("payload keys = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("payload keys = %v, want %v", got, want)
				break
			}
		}
	}
	metaWant := []struct{ key, value string }{
		{"version", `"1.2.3"`}, {"ring", `"memory"`}, {"sink", `"nats"`}, {"transports", `["udp","tcp","tls"]`},
	}
	if len(p.Meta) != len(metaWant) {
		t.Fatalf("meta = %+v", p.Meta)
	}
	for i, m := range metaWant {
		if p.Meta[i].Key != m.key || string(p.Meta[i].Value) != m.value {
			t.Errorf("meta[%d] = %s %s, want %s %s", i, p.Meta[i].Key, p.Meta[i].Value, m.key, m.value)
		}
	}
	if b.Subject() != "loghub._ctl.beacon" {
		t.Errorf("subject = %q", b.Subject())
	}
}

// An identity with characters that need escaping still produces valid JSON:
// AgentName and Hostname come from the environment.
func TestBeaconEscapesItsStrings(t *testing.T) {
	cfg := testConfig()
	cfg.Identity.AgentName = `we"ird\name`
	cfg.Transports = nil
	b := New(&fakePub{}, cfg, discard(), metrics())
	raw := b.Envelope(time.Now(), "127.0.0.1")
	if err := jlhcheck.ValidateEnvelopeSchema(raw); err != nil {
		t.Fatalf("%v\n%s", err, raw)
	}
	if !bytes.Contains(raw, []byte(`{"key":"transports","value":[]}`)) {
		t.Errorf("no transports must still be an array: %s", raw)
	}
}

// SPEC §11: the address is the local IP of the route to the server, found by a
// UDP "dial" that sends nothing; without a server it is 127.0.0.1.
func TestBeaconAddress(t *testing.T) {
	pub := &fakePub{}
	b := New(pub, testConfig(), discard(), metrics())
	if got := b.address(); got != "127.0.0.1" {
		t.Errorf("no server: %q", got)
	}
	pub.addr = "127.0.0.1:14222"
	if got := b.address(); got != "127.0.0.1" {
		t.Errorf("loopback server: %q", got)
	}
	pub.addr = "not an address"
	if got := b.address(); got != "127.0.0.1" {
		t.Errorf("garbage address: %q", got)
	}
	// A routable destination yields some local, non-loopback-or-loopback IP:
	// whatever the kernel picks, it must parse.
	pub.addr = "192.0.2.1:4222"
	if got := b.address(); net.ParseIP(got) == nil {
		t.Errorf("routable server: %q is not an IP", got)
	}
}

// SPEC §11 and §13.1: a kick that arrives before Start -- the first connection
// happens during the boot's WaitReady -- goes out as soon as the beacon starts;
// further kicks are reconnections.
func TestBeaconKickBeforeStartIsNotLost(t *testing.T) {
	pub := &fakePub{}
	m := metrics()
	b := New(pub, testConfig(), discard(), m) // Interval 0: no periodic beacon
	b.Kick()
	b.Kick() // coalesces with the first
	b.Start(t.Context())
	defer b.Stop()
	waitFor(t, 2*time.Second, func() bool { return pub.count() == 1 })

	b.Kick()
	waitFor(t, 2*time.Second, func() bool { return pub.count() == 2 })
	time.Sleep(20 * time.Millisecond)
	if pub.count() != 2 {
		t.Fatalf("%d beacons, want 2: interval 0 disables only the periodic one", pub.count())
	}
	if m.BeaconsOK.Load() != 2 {
		t.Errorf(`beacons_total{result="ok"} = %d`, m.BeaconsOK.Load())
	}
	for _, s := range pub.subject {
		if s != "loghub._ctl.beacon" {
			t.Errorf("published on %q", s)
		}
	}
}

func TestBeaconPeriodic(t *testing.T) {
	pub := &fakePub{}
	cfg := testConfig()
	cfg.Namespace = "custom.ctl"
	b := New(pub, cfg, discard(), metrics())
	// Below MinInterval, which New would floor: set after it, so the test
	// sees several beacons in milliseconds.
	b.cfg.Interval = 10 * time.Millisecond
	b.Start(t.Context())
	waitFor(t, 2*time.Second, func() bool { return pub.count() >= 3 })
	b.Stop()
	n := pub.count()
	time.Sleep(30 * time.Millisecond)
	if pub.count() != n {
		t.Fatal("the beacon kept publishing after Stop")
	}
	if pub.subject[0] != "custom.ctl.beacon" {
		t.Errorf("subject = %q", pub.subject[0])
	}
	// Each beacon has its own uuid.
	seen := map[string]bool{}
	for _, d := range pub.data {
		seen[string(d[9:45])] = true
	}
	if len(seen) != len(pub.data) {
		t.Error("two beacons share a uuid")
	}
}

// SPEC §11: a failure is counted and logged with sampling; it never stops the
// beacon (nor touches the sink's health, which it cannot even see).
func TestBeaconFailureIsCounted(t *testing.T) {
	pub := &fakePub{fail: errors.New("not connected")}
	m := metrics()
	b := New(pub, testConfig(), discard(), m)
	for i := 0; i < 3; i++ {
		if b.Publish() {
			t.Fatal("Publish reported success on a failing publisher")
		}
	}
	if m.BeaconsError.Load() != 3 || m.BeaconsOK.Load() != 0 {
		t.Errorf("error=%d ok=%d", m.BeaconsError.Load(), m.BeaconsOK.Load())
	}
	pub.mu.Lock()
	pub.fail = nil
	pub.mu.Unlock()
	if !b.Publish() || m.BeaconsOK.Load() != 1 {
		t.Error("the beacon did not recover")
	}
}

func TestBeaconStartStopIdempotent(t *testing.T) {
	b := New(&fakePub{}, testConfig(), discard(), metrics())
	b.Stop() // before Start: no-op
	b.Start(t.Context())
	b.Start(t.Context())
	b.Stop()
	b.Stop()
}

// SPEC §22 pendência 21: an interval of 1ns or 15ms (for 15s) turned the beacon
// into a loop that flooded <ctl_namespace>.beacon and competed for the data
// plane's connection. The configuration refuses it; the beacon floors it on
// its own too, so with no kick not one beacon goes out in a tenth of a second.
// Zero, MinInterval itself and the default are the controls.
func TestBeaconTinyIntervalIsFloored(t *testing.T) {
	for _, iv := range []time.Duration{time.Nanosecond, 15 * time.Millisecond} {
		pub := &fakePub{}
		cfg := testConfig()
		cfg.Interval = iv
		b := New(pub, cfg, discard(), metrics())
		if b.cfg.Interval != MinInterval {
			t.Errorf("interval %s kept as %s, want the floor %s", iv, b.cfg.Interval, MinInterval)
		}
		b.Start(t.Context())
		time.Sleep(100 * time.Millisecond)
		b.Stop()
		if n := pub.count(); n > 0 {
			t.Errorf("interval %s: %d beacons in 100ms, want none before the floor elapses", iv, n)
		}
	}
	for _, iv := range []time.Duration{0, MinInterval, 15 * time.Second} {
		cfg := testConfig()
		cfg.Interval = iv
		if b := New(&fakePub{}, cfg, discard(), metrics()); b.cfg.Interval != iv {
			t.Errorf("interval %s changed to %s", iv, b.cfg.Interval)
		}
	}
}
