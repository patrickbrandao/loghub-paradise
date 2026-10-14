package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
)

// Nothing in this file needs a server. The integration half lives in
// nats_integration_test.go and skips without WHS_TEST_NATS_URL. Ported from
// go-syslog-nats-server and go-webhook-fast-server (SPEC §8.7).

// SPEC §8.7.6 is the table; this is the table read back.
func TestClassifyNATS(t *testing.T) {
	api := func(code int, ec jetstream.ErrorCode) error {
		return &jetstream.APIError{Code: code, ErrorCode: ec, Description: "test"}
	}
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"deadline", context.DeadlineExceeded, ClassNetwork},
		{"canceled", context.Canceled, ClassNetwork},
		{"timeout", nats.ErrTimeout, ClassNetwork},
		{"no responders", nats.ErrNoResponders, ClassNetwork},
		{"no servers", nats.ErrNoServers, ClassNetwork},
		{"disconnected", nats.ErrDisconnected, ClassNetwork},
		{"reconnecting", nats.ErrConnectionReconnecting, ClassNetwork},
		{"reconnect buffer", nats.ErrReconnectBufExceeded, ClassNetwork},
		{"no stream response", jetstream.ErrNoStreamResponse, ClassNetwork},
		{"async timeout", jetstream.ErrAsyncPublishTimeout, ClassNetwork},
		{"stalled", jetstream.ErrTooManyStalledMsgs, ClassNetwork},

		{"authorization", nats.ErrAuthorization, ClassFatal},
		{"auth expired", nats.ErrAuthExpired, ClassFatal},
		{"auth revoked", nats.ErrAuthRevoked, ClassFatal},
		{"account auth expired", nats.ErrAccountAuthExpired, ClassFatal},
		// What nats.Connect returns: the server's own words, which its
		// errors.Is never matched to the sentinels above.
		{"connect: user authentication expired", errors.New("nats: User Authentication Expired"), ClassFatal},
		{"connect: user authentication revoked", errors.New("nats: User Authentication Revoked"), ClassFatal},
		{"connect: account authentication expired", errors.New("nats: Account Authentication Expired"), ClassFatal},
		{"closed", nats.ErrConnectionClosed, ClassFatal},
		{"stream not found", jetstream.ErrStreamNotFound, ClassFatal},
		{"jetstream not enabled", jetstream.ErrJetStreamNotEnabled, ClassFatal},
		{"jetstream not enabled for account", jetstream.ErrJetStreamNotEnabledForAccount, ClassFatal},
		{"api stream not found 10059", api(404, jsErrStreamNotFound), ClassFatal},
		{"api jetstream off 10076", api(503, jsErrJetStreamNotEnabled), ClassFatal},
		{"api jetstream off for account 10039", api(503, jsErrJetStreamNotEnabledFA), ClassFatal},
		{"api stream not match 10060", api(400, jsErrStreamNotMatch), ClassFatal},
		{"api sealed 10109", api(400, jsErrStreamSealed), ClassFatal},
		{"api insufficient resources 10023", api(503, jsErrInsufficientResources), ClassFatal},
		{"api store failed 10077", api(503, jsErrStreamStoreFailed), ClassFatal},
		// A full discard=new stream, or one closing, is 10077 too, with the
		// store's own words: transient (porte da família syslog/RELP,
		// 2026-10-13). "store is closed" is nats-server 2.14.6's text
		// (server/store.go, ErrStoreClosed); "store closed" an older one.
		{"10077 max msgs", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages exceeded"}, ClassNetwork},
		{"10077 max bytes", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum bytes exceeded"}, ClassNetwork},
		{"10077 max per subject", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages per subject exceeded"}, ClassNetwork},
		{"10077 store is closed", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "store is closed"}, ClassNetwork},
		{"10077 store closed", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "store closed"}, ClassNetwork},
		{"10077 record too large", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "message too large"}, ClassFatal},
		{"api unknown 4xx", api(400, 10999), ClassFatal},
		// SPEC §18.1 item 8 (closed): transient though 4xx. 10158 (409) is a
		// Nats-Msg-Id still in a Raft proposal, 10167 (429) a full ingest
		// queue, and any 429 is backpressure; another 409 stays fatal.
		{"api duplicate in process 10158", api(409, jsErrDuplicateInProcess), ClassNetwork},
		{"api too many requests 10167", api(429, jsErrStreamTooManyRequests), ClassNetwork},
		{"api unknown 429", api(429, 10210), ClassNetwork},
		{"api unknown 409", api(409, 10999), ClassFatal},
		{"wrapped api 10158", fmt.Errorf("ack: %w", api(409, jsErrDuplicateInProcess)), ClassNetwork},

		{"max payload", nats.ErrMaxPayload, ClassLogic},
		{"max bytes", jetstream.ErrMaxBytesExceeded, ClassLogic},
		{"api message too big 10054", api(400, jsErrMessageExceedsMaximum), ClassLogic},
		{"api header too big 10097", api(400, jsErrHeaderExceedsMaximum), ClassLogic},

		{"api unknown 5xx", api(503, 10998), ClassNetwork},
		{"plain error", errors.New("boom"), ClassNetwork},
		{"wrapped fatal", fmt.Errorf("publish: %w", nats.ErrAuthorization), ClassFatal},
		{"wrapped api", fmt.Errorf("ack: %w", api(400, jsErrStreamSealed)), ClassFatal},

		// SPEC §8.7.1: a server that demands TLS will never take this server's
		// plaintext; waiting cannot change it.
		{"server requires tls", errNATSServerRequiresTLS, ClassFatal},
		{"wrapped server requires tls", fmt.Errorf("connect: %w", errNATSServerRequiresTLS), ClassFatal},
		// Control: any other TLS failure of the client is not that verdict.
		{"other tls error", nats.ErrTLS, ClassNetwork},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassOf(classifyNATS("op", tc.err)); got != tc.want {
				t.Errorf("class = %s, want %s", got, tc.want)
			}
		})
	}
	if classifyNATS("op", nil) != nil {
		t.Error("a nil error must classify as nil")
	}
}

func newTestNATS(t *testing.T, mut func(*NATSConfig)) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_WEBHOOK", Workspace: "default"}
	if mut != nil {
		mut(&cfg)
	}
	n, err := NewNATS(cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// SPEC §3.8: Data is the JLH and nothing else, byte for byte; the headers are
// exactly the five of the contract, read from the envelope's own header;
// Nats-Msg-Id is <uuid>.<step>, never the bare uuid.
func TestNATSMessageShape(t *testing.T) {
	n := newTestNATS(t, nil)
	it := itemIn("default", testRequest("/hook", []byte("A\x00B\xff")))
	msg, err := n.message(it)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Subject != "loghub.default.webhook" {
		t.Errorf("subject = %q", msg.Subject)
	}
	want := map[string]string{
		"Nats-Msg-Id":    it.ID + ".0",
		"Content-Type":   "application/json",
		"X-LogHub-Type":  "webhook",
		"X-LogHub-UUID":  it.ID,
		"X-LogHub-Space": "default",
	}
	if len(msg.Header) != len(want) {
		t.Errorf("headers = %v: exactly %d, and the JLH in none of them", msg.Header, len(want))
	}
	for k, v := range want {
		// The literal names, not Get's canonicalisation: nats.Header keeps
		// what was written, and a consumer reads exactly these.
		if got := msg.Header[k]; len(got) != 1 || got[0] != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if !bytes.Equal(msg.Data, it.Env) || unsafe.SliceData(msg.Data) != unsafe.SliceData(it.Env) {
		t.Error("Data is not the item's JLH slice itself")
	}
	validJLH(t, msg.Data)
}

// SPEC §3.8, §9.2: a message is built only from a JLH this server writes. An
// envelope of another format, or of another type, maps to nothing and is the
// logic refusal of that item alone.
func TestNATSMessageRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	n := newTestNATS(t, nil)
	if _, err := n.message(badItem()); err == nil {
		t.Error("a pre-LogHub envelope was mapped to a message")
	}
	good := mkItem("syslog")
	env := bytes.Replace(good.Env, []byte(`"type":"webhook"`), []byte(`"type":"syslog"`), 1)
	if _, err := n.message(envelope.NewItem(good.ID, env, -1)); err == nil ||
		!strings.Contains(err.Error(), "syslog") {
		t.Errorf("an envelope of type syslog: err = %v, want a refusal naming the type", err)
	}
	// Control: the fixture itself maps.
	if _, err := n.message(good); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// SPEC §3.8: the header block is small and fixed -- at most 257 bytes with a
// 64-byte space and a two-digit step, plus Nats-Expected-Stream -- far below
// the 64 KiB JetStream allows, because the JLH is in Data.
func TestNATSHeaderBlockIsBounded(t *testing.T) {
	space := strings.Repeat("s", 64)
	n := newTestNATS(t, func(c *NATSConfig) { c.Workspace = space })
	msg, err := n.message(itemIn(space, testRequest("/h", nil)))
	if err != nil {
		t.Fatal(err)
	}
	msg.Header[jetstream.MsgIDHeader] = []string{msg.Header[jetstream.MsgIDHeader][0][:36] + ".32"} // the largest step
	size := len("NATS/1.0\r\n") + len("\r\n")
	for k, vs := range msg.Header {
		for _, v := range vs {
			size += len(k) + len(": ") + len(v) + len("\r\n")
		}
	}
	if size != natsHeaderMax {
		t.Fatalf("header block of %d bytes, want exactly the %d of SPEC §3.8", size, natsHeaderMax)
	}
}

// SPEC §3.8: the subject is loghub.<space>.webhook, always, with the space of
// the envelope's OWN header -- not the instance's -- and the string is cached
// per space; the instance's own is precomputed.
func TestNATSSubjects(t *testing.T) {
	n := newTestNATS(t, nil)
	items := []*envelope.Item{
		itemIn("acme.prod", testRequest("/a", nil)), itemIn("default", testRequest("/b", nil)),
		itemIn("acme.prod", testRequest("/c", nil)),
	}
	want := []string{"loghub.acme.prod.webhook", "loghub.default.webhook", "loghub.acme.prod.webhook"}
	subjects := make([]string, len(items))
	for i, it := range items {
		msg, err := n.message(it)
		if err != nil {
			t.Fatal(err)
		}
		if subjects[i] = msg.Subject; msg.Subject != want[i] {
			t.Errorf("item %d: subject = %q, want %q", i, msg.Subject, want[i])
		}
	}
	if unsafe.StringData(subjects[0]) != unsafe.StringData(subjects[2]) {
		t.Error("the subject of a space is not cached: two envelopes of one space built it twice")
	}
	if unsafe.StringData(subjects[1]) != unsafe.StringData(n.own) {
		t.Error("the instance's own subject is not the precomputed one")
	}
	spaces := 0
	n.subjects.Range(func(k, v any) bool {
		spaces++
		if v.(string) != "loghub."+k.(string)+".webhook" {
			t.Errorf("cache maps %q to %q", k, v)
		}
		return true
	})
	if spaces != 1 {
		t.Errorf("%d foreign spaces cached, want 1 (the own one is precomputed)", spaces)
	}
}

// The local pattern matching of the boot validation (SPEC §8.7.5).
func TestSubjectCovers(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"loghub.default.webhook", "loghub.default.webhook", true},
		{"loghub.default.webhook.>", "loghub.default.webhook", false},
		{"a.b.>", "a.b.*.*", true},
		{"a.b.*.*", "a.b.*.*", true},
		{"a.b.*", "a.b.*.*", false},
		{"loghub.*.webhook", "loghub.default.webhook", true},
		{"loghub.>", "loghub.default.webhook", true},
		{"loghub.>", "loghub._ctl.beacon", true},
		{"loghub.*.*", "loghub._ctl.beacon", true},
		{"loghub.default.>", "loghub._ctl.beacon", false},
		{"a.b.c.*", "a.b.*.*", false},
		{"loghub.*.webhook", "loghub.acme.prod.webhook", false},
		{"loghub.acme.*.webhook", "loghub.acme.prod.webhook", true},
		{">", "anything.at.all", true},
		{"*", "a.b", false},
		{"loghub.default.webhook", "loghub.default.webhook.>", false},
		{"loghub.default", "loghub.default.webhook", false},
	}
	for _, c := range cases {
		if got := subjectCovers(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectCovers(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

// natsCanonicalStream is the stream an operator creates for one space (SPEC
// §8.7.5): the exact subject of the space and nothing that could capture the
// control plane, file storage, a ten-minute duplicate window for the
// redelivery of SPEC §8.7.3. The integration suite builds every stream from it.
func natsCanonicalStream(name, space string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              name,
		Description:       "go-webhook-nats-server: JLH type=webhook of space " + space,
		Subjects:          []string{"loghub." + space + ".webhook"},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		Replicas:          1,
		MaxConsumers:      -1,
		MaxMsgs:           -1,
		MaxMsgsPerSubject: -1,
		MaxBytes:          -1,
		MaxMsgSize:        -1,
		Duplicates:        10 * time.Minute,
		Discard:           jetstream.DiscardOld,
	}
}

// natsStreamContract is what SPEC §8.7.5 asks of the stream of space default.
func natsStreamContract(t *testing.T, cfg jetstream.StreamConfig) {
	t.Helper()
	if cfg.Name != "LOGHUB_WEBHOOK" || cfg.Storage != jetstream.FileStorage || cfg.Duplicates != 10*time.Minute ||
		cfg.Sealed || cfg.NoAck {
		t.Errorf("stream = %s storage=%s dup=%s sealed=%v no_ack=%v", cfg.Name, cfg.Storage, cfg.Duplicates,
			cfg.Sealed, cfg.NoAck)
	}
	covers := func(s string) bool {
		for _, p := range cfg.Subjects {
			if subjectCovers(p, s) {
				return true
			}
		}
		return false
	}
	if !covers("loghub.default.webhook") {
		t.Errorf("subjects %v do not cover loghub.default.webhook", cfg.Subjects)
	}
	if covers(DefaultCtlNamespace + ".beacon") {
		t.Errorf("subjects %v capture the control plane", cfg.Subjects)
	}
	for _, p := range cfg.Subjects {
		if p == "loghub.>" {
			t.Error("loghub.> would capture loghub._ctl")
		}
	}
	// The largest message of the defaults (SPEC §8.7.5) must fit the stream.
	if cfg.MaxMsgSize > 0 && int64(cfg.MaxMsgSize) < 8022700+natsHeaderMax+int64(natsExpectedStreamHeader)+14 {
		t.Errorf("max_msg_size %d is below the largest message of the defaults", cfg.MaxMsgSize)
	}
}

// SPEC §8.7.5: the stream the integration suite builds from, and the
// docker/nats/stream.json an operator hands to `nats stream add`, meet the
// contract the boot validates.
func TestNATSCanonicalStreamMatchesTheContract(t *testing.T) {
	natsStreamContract(t, natsCanonicalStream("LOGHUB_WEBHOOK", "default"))

	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	natsStreamContract(t, cfg)
}

func TestNewNATSIsLazy(t *testing.T) {
	p, err := NewNATS(NATSConfig{
		URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default",
		ConnectTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	}, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewNATS against a dead port: %v", err)
	}
	if p.Type() != "nats" {
		t.Errorf("Type = %q", p.Type())
	}
	if p.ServerAddr() != "" {
		t.Error("ServerAddr before any connection")
	}
	if err := p.PublishCore("loghub._ctl.beacon", []byte("{}")); err == nil || IsFatal(err) {
		t.Errorf("PublishCore before a connection = %v, want a network error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = p.Health(ctx)
	if err == nil {
		t.Fatal("Health against a dead port succeeded")
	}
	if ClassOf(err) != ClassNetwork {
		t.Errorf("dead port classified as %s, want network so the boot keeps waiting", ClassOf(err))
	}
	// A write against a dead server is a network failure: retried, the items
	// still the worker's and untouched.
	items := mkBatch(2)
	if err := p.Write(ctx, items); err == nil || ClassOf(err) != ClassNetwork {
		t.Errorf("Write against a dead port = %v, want network", err)
	}
	for _, it := range items {
		if it.Released() {
			t.Error("a failed write released an item")
		}
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Write(ctx, mkBatch(1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := p.Health(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v, want ErrClosed", err)
	}
}

func TestNewNATSRejectsBadConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	creds := filepath.Join(t.TempDir(), "x.creds")
	if err := os.WriteFile(creds, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default"}

	// Control: the base itself is valid, or every case below could fail for
	// a reason of its own.
	for _, urls := range [][]string{base.URLs, {"nats://u:p@10.0.0.5:4222", "NATS://10.0.0.6:4222"}} {
		cfg := base
		cfg.URLs = urls
		p, err := NewNATS(cfg, quietLog(), testMetrics())
		if err != nil {
			t.Fatalf("NewNATS(%v) refused a valid configuration: %v", urls, err)
		}
		p.Close()
	}

	cases := []struct {
		name string
		mut  func(*NATSConfig)
		want string
	}{
		{"no urls", func(c *NATSConfig) { c.URLs = nil }, "URL"},
		{"jetstream without stream", func(c *NATSConfig) { c.Stream = "" }, "stream"},
		{"bad mode", func(c *NATSConfig) { c.Mode = "push" }, "mode"},
		{"no workspace", func(c *NATSConfig) { c.Workspace = "" }, "workspace"},
		{"password without username", func(c *NATSConfig) { c.Password = "p" }, "username"},
		{"two methods", func(c *NATSConfig) { c.Username, c.Password, c.Token = "u", "p", "t" }, "exactly one"},
		{"three methods", func(c *NATSConfig) { c.Token, c.Credentials, c.Username = "t", creds, "u" }, "exactly one"},
		{"missing credentials", func(c *NATSConfig) { c.Credentials = missing }, "credentials"},
		// SPEC §8.7.1: no TLS. nats.go would dial these with TLS (tls://,
		// wss://) or as a websocket; the configuration refuses them, and so
		// does the sink, so no path reaches a TLS dial.
		{"tls url", func(c *NATSConfig) { c.URLs = []string{"tls://127.0.0.1:4222"} }, "nats://"},
		{"tls url in a cluster", func(c *NATSConfig) { c.URLs = []string{"nats://a:4222", "tls://b:4222"} }, "nats://"},
		{"tls url in a comma list", func(c *NATSConfig) { c.URLs = []string{"nats://a:4222,tls://b:4222"} }, "nats://"},
		{"wss url", func(c *NATSConfig) { c.URLs = []string{"wss://127.0.0.1:443"} }, "nats://"},
		{"ws url", func(c *NATSConfig) { c.URLs = []string{"ws://127.0.0.1:80"} }, "nats://"},
		{"bare host", func(c *NATSConfig) { c.URLs = []string{"127.0.0.1:4222"} }, "nats://"},
		{"empty url", func(c *NATSConfig) { c.URLs = []string{""} }, "nats://"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			p, err := NewNATS(cfg, quietLog(), testMetrics())
			if err == nil {
				p.Close()
				t.Fatal("NewNATS accepted the configuration")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	// core needs no stream.
	cfg := base
	cfg.Mode, cfg.Stream = NATSModeCore, ""
	p, err := NewNATS(cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("core without a stream refused: %v", err)
	}
	p.Close()
}

// The largest message counts the header block, and in jetstream mode the
// expected-stream header with the stream name (SPEC §8.7.5).
func TestNATSMaxMessageBytes(t *testing.T) {
	js := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 1000 })
	core := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 1000; c.Mode = NATSModeCore })
	if got := core.maxMessageBytes(); got != 1000+natsHeaderMax {
		t.Errorf("core: %d", got)
	}
	if got := js.maxMessageBytes(); got != int64(1000+natsHeaderMax+natsExpectedStreamHeader+len("LOGHUB_WEBHOOK")) {
		t.Errorf("jetstream: %d", got)
	}
}

// OnConnect registered before any connection runs only when one happens; the
// hook list survives and fireHooks runs every hook.
func TestNATSOnConnectHooks(t *testing.T) {
	n := newTestNATS(t, nil)
	calls := 0
	n.OnConnect(func() { calls++ })
	if calls != 0 {
		t.Fatal("a hook ran without a connection")
	}
	n.fireHooks()
	n.fireHooks()
	if calls != 2 {
		t.Fatalf("hook ran %d times, want 2", calls)
	}
}

// The validation refusals that need no server (SPEC §8.7.5): no_ack, sealed,
// a stream that does not cover the own subject, one that captures the control
// plane, and the file store's record ceiling. Each with its control.
func TestNATSValidateRefusals(t *testing.T) {
	n := newTestNATS(t, nil)
	ok := jetstream.StreamConfig{Name: "LOGHUB_WEBHOOK", Subjects: []string{"loghub.default.webhook"},
		Storage: jetstream.FileStorage}
	if err := n.validate(1<<20, &jetstream.StreamInfo{Config: ok}); err != nil {
		t.Fatalf("control: %v", err)
	}
	for name, tc := range map[string]struct {
		mut  func(*jetstream.StreamConfig)
		want string
	}{
		"no_ack":        {func(c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
		"sealed":        {func(c *jetstream.StreamConfig) { c.Sealed = true }, "sealed"},
		"not covered":   {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.other.webhook"} }, "does not cover"},
		"wrong type":    {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.default.syslog"} }, "does not cover"},
		"loghub.>":      {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.>"} }, "control plane"},
		"two wildcards": {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.*.*"} }, "control plane"},
	} {
		cfg := ok
		cfg.Subjects = append([]string(nil), ok.Subjects...)
		tc.mut(&cfg)
		err := n.validate(1<<20, &jetstream.StreamInfo{Config: cfg})
		if !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: validate = %v, want a fatal error naming %q", name, err, tc.want)
		}
	}

	// The sizes (SPEC §8.7.5): the largest message against max_payload, the
	// stream's max_msg_size and the file store's 32 MiB record.
	sized := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 8022700 })
	need := sized.maxMessageBytes()
	if err := sized.validate(need, &jetstream.StreamInfo{Config: ok}); err != nil {
		t.Fatalf("size control: max_payload equal to the largest message: %v", err)
	}
	if err := sized.validate(need-1, &jetstream.StreamInfo{Config: ok}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "max_payload") {
		t.Errorf("max_payload one byte short: %v", err)
	}
	small := ok
	small.MaxMsgSize = int32(need - 1)
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: small}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "max_msg_size") {
		t.Errorf("max_msg_size one byte short: %v", err)
	}
	huge := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 33 << 20 })
	if err := huge.validate(64<<20, &jetstream.StreamInfo{Config: ok}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "file storage") {
		t.Errorf("a record above the file store's 32 MiB: %v", err)
	}
	mem := ok
	mem.Storage = jetstream.MemoryStorage
	if err := huge.validate(64<<20, &jetstream.StreamInfo{Config: mem}); err != nil {
		t.Errorf("memory storage has no record ceiling: %v", err)
	}

	// A stream that refuses new messages when full and is smaller than one
	// message: 10077 on the first large event, fatal, and again on every
	// reingestion. Discarding old messages makes room instead (the control).
	full := ok
	full.MaxBytes, full.Discard = need, jetstream.DiscardNew
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: full}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "max_bytes") {
		t.Errorf("max_bytes below a record with discard=new: %v", err)
	}
	full.Discard = jetstream.DiscardOld
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: full}); err != nil {
		t.Errorf("max_bytes with discard=old makes room: %v", err)
	}

	// A subject of another space found covered (cover) that the stream no
	// longer covers -- the operator narrowed it once the dump drained, as
	// cover's error says they may -- is forgotten, not fatal: the next
	// publish there asks again. Still covered, it is kept.
	n.coverMu.Lock()
	n.covered = map[string]struct{}{"loghub.acme.webhook": {}, "loghub.kept.webhook": {}}
	n.coverMu.Unlock()
	still := ok
	still.Subjects = []string{"loghub.default.webhook", "loghub.kept.webhook"}
	if err := n.validate(1<<20, &jetstream.StreamInfo{Config: still}); err != nil {
		t.Errorf("a foreign subject the stream dropped after the drain: %v, want no error", err)
	}
	n.coverMu.Lock()
	_, dropped := n.covered["loghub.acme.webhook"]
	_, kept := n.covered["loghub.kept.webhook"]
	n.coverMu.Unlock()
	if dropped || !kept {
		t.Errorf("covered after the check: acme=%v kept=%v, want only the still-covered one", dropped, kept)
	}

	// No responders at the first check is JetStream unavailable: fatal, at
	// once. After a validated check it is a blip: network, retried.
	if err := n.streamErr(nats.ErrNoResponders); !IsFatal(err) || !strings.Contains(err.Error(), "JetStream") {
		t.Errorf("no responders at the first check = %v, want fatal naming JetStream", err)
	}
	n.validated.Store(true)
	if err := n.streamErr(nats.ErrNoResponders); ClassOf(err) != ClassNetwork {
		t.Errorf("no responders after a validated check = %v (class %s), want network", err, ClassOf(err))
	}
}

// SPEC §8.7.5: nobody answering the stream API at the first check is fatal
// naming JetStream, at once, and nothing else is asked. Until 2026-10-11 the
// sink then asked AccountInfo, whose $JS.API.INFO a minimal role may not be
// granted: the denial became a permanent permission verdict that named the
// wrong cause, after a wait of the whole health timeout. The fake server
// answers every request with "no responders" and denies $JS.API.INFO, as a
// leaf whose hub is down would to such a role.
func TestNATSNoRespondersNeverProbesTheAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	srv.noResponders = true
	srv.denied = "$JS.API.INFO"
	n := newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.ConnectTimeout, c.OpTimeout = []string{"nats://" + srv.addr}, 5*time.Second, 5*time.Second
	})
	hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
	err := n.Health(hctx)
	hcancel()
	if !IsFatal(err) || !strings.Contains(err.Error(), "JetStream API") {
		t.Fatalf("first check against a server where nobody answers the stream API = %v (class %s), "+
			"want fatal naming the JetStream API", err, ClassOf(err))
	}
	if !srv.published("$JS.API.STREAM.INFO.LOGHUB_WEBHOOK") {
		t.Fatal("the stream info was never asked: the fake server is not what the test thinks")
	}
	time.Sleep(100 * time.Millisecond) // an -ERR, had one been earned, arrives asynchronously
	if srv.published("$JS.API.INFO") {
		t.Error("the sink asked $JS.API.INFO (AccountInfo): a minimal role is denied it")
	}
	if v := n.verdict.Load(); v != nil {
		t.Errorf("a permission verdict was kept: %v", *v)
	}
}

// countingHandler counts the records it receives.
type countingHandler struct{ n atomic.Int64 }

func (h *countingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error { h.n.Add(1); return nil }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countingHandler) WithGroup(string) slog.Handler             { return h }

// The client's async errors arrive once per message when, say, publish
// permission is denied; they are logged sampled, never one line per message
// (SPEC §11.3).
func TestNATSAsyncErrorsAreSampled(t *testing.T) {
	h := &countingHandler{}
	n, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default"},
		slog.New(h), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for i := 0; i < 100; i++ {
		n.asyncError(errors.New(`nats: permissions violation for publish to "loghub.default.webhook"`))
	}
	if got := h.n.Load(); got != 1 {
		t.Fatalf("%d log lines for 100 async errors, want 1", got)
	}
}

// ------------------------------------------------------------------- no TLS

// SPEC §8.7.1: this server has no TLS. None of the options NewNATS hands the
// client asks for it -- no Secure, no tls.Config, no handshake first -- and
// the one TLS hook it does set is the refusal that stops nats.go from
// upgrading on its own when a server says tls_required.
func TestNATSOptionsNeverAskForTLS(t *testing.T) {
	for _, mut := range []func(*NATSConfig){
		nil,
		func(c *NATSConfig) { c.Username, c.Password = "u", "p" },
		func(c *NATSConfig) { c.Token = "t" },
		func(c *NATSConfig) { c.Mode, c.Stream = NATSModeCore, "" },
	} {
		n := newTestNATS(t, mut)
		o := nats.GetDefaultOptions()
		for _, opt := range n.opts {
			if err := opt(&o); err != nil {
				t.Fatal(err)
			}
		}
		if o.Secure || o.TLSConfig != nil || o.TLSHandshakeFirst || o.RootCAsCB != nil {
			t.Errorf("options ask for TLS: secure=%v config=%v first=%v", o.Secure, o.TLSConfig != nil,
				o.TLSHandshakeFirst)
		}
		if o.TLSCertCB == nil {
			t.Fatal("no TLS refusal hook: nats.go would upgrade to TLS for a server with tls_required")
		}
		if _, err := o.TLSCertCB(); !errors.Is(err, errNATSServerRequiresTLS) {
			t.Errorf("the TLS hook returned %v, want the refusal", err)
		}
		// The three options that are not the library's defaults (SPEC §8.7.1,
		// §8.7.2): if someone "simplifies" one, nothing else fails.
		if o.ReconnectBufSize != -1 || o.MaxReconnect != -1 {
			t.Errorf("ReconnectBufSize %d MaxReconnect %d, want -1 and -1", o.ReconnectBufSize, o.MaxReconnect)
		}
	}
}

// fakeNATS is just enough of a NATS server to see what a client sends after
// the INFO: it greets every connection with info, answers PING with PONG, and
// hands each connection's first bytes (empty when the client hung up without
// a word) to first.
type fakeNATS struct {
	addr  string
	first chan []byte

	// pubReply, when set, is written after every PUB/HPUB, before any PONG
	// that follows it -- the order of a real server's -ERR.
	pubReply string
	// pubs counts the PUB/HPUB lines received.
	pubs atomic.Int64
	// noResponders, when set, answers every request -- a PUB/HPUB with a
	// reply subject -- with the 503 status a server sends when nobody
	// listens on the subject: the client's ErrNoResponders. It is what a
	// server without JetStream answers the stream API.
	noResponders bool
	// denied, when set, is a subject whose publishes a real server would
	// refuse to a role without the grant: answered with a permissions
	// violation and nothing else, never routed.
	denied string

	mu       sync.Mutex
	conns    []net.Conn
	subjects []string // of every PUB/HPUB received, in order
}

// published reports whether a PUB/HPUB to subject was received.
func (f *fakeNATS) published(subject string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.subjects {
		if s == subject {
			return true
		}
	}
	return false
}

// drop hangs up every connection, so a client reconnects.
func (f *fakeNATS) drop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.conns = nil
}

func startFakeNATS(t *testing.T, info string) *fakeNATS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &fakeNATS{addr: ln.Addr().String(), first: make(chan []byte, 16)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c, info)
		}
	}()
	return f
}

func (f *fakeNATS) serve(c net.Conn, info string) {
	defer c.Close()
	f.mu.Lock()
	f.conns = append(f.conns, c)
	f.mu.Unlock()
	if _, err := c.Write([]byte("INFO " + info + "\r\n")); err != nil {
		f.first <- nil
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 4096)
	n, _ := c.Read(buf)
	f.first <- append([]byte(nil), buf[:n]...)
	// The plaintext path goes on: answer every PING so a flush completes.
	rd := bufio.NewReader(io.MultiReader(bytes.NewReader(buf[:n]), c))
	subs := map[string]string{} // subject of each SUB -> its sid
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		if fields := strings.Fields(line); len(fields) >= 3 && fields[0] == "SUB" {
			subs[fields[1]] = fields[len(fields)-1]
		}
		if strings.HasPrefix(line, "PUB ") || strings.HasPrefix(line, "HPUB ") {
			f.pubs.Add(1)
			if f.pubReply != "" {
				if _, err := c.Write([]byte(f.pubReply)); err != nil {
					return
				}
			}
			if reply := f.request(line, subs); reply != "" {
				if _, err := c.Write([]byte(reply)); err != nil {
					return
				}
			}
		}
		if strings.HasPrefix(line, "PING") {
			if _, err := c.Write([]byte("PONG\r\n")); err != nil {
				return
			}
		}
	}
}

// request records the subject of a PUB/HPUB line and returns what a server
// would answer it with under denied and noResponders: a permissions
// violation for the denied subject, the 503 status to the reply subject's
// subscription for a request, or nothing.
func (f *fakeNATS) request(line string, subs map[string]string) string {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return ""
	}
	subject := fields[1]
	f.mu.Lock()
	f.subjects = append(f.subjects, subject)
	f.mu.Unlock()
	if f.denied != "" && subject == f.denied {
		return "-ERR 'Permissions Violation for Publish to \"" + subject + "\"'\r\n"
	}
	// PUB <subject> <reply> <size>, HPUB <subject> <reply> <hdr> <total>.
	withReply := (fields[0] == "PUB" && len(fields) == 4) || (fields[0] == "HPUB" && len(fields) == 5)
	if !f.noResponders || !withReply {
		return ""
	}
	reply := fields[2]
	sid := subs[reply]
	if sid == "" {
		// The client's request mux listens on _INBOX.<nuid>.*.
		if i := strings.LastIndexByte(reply, '.'); i > 0 {
			sid = subs[reply[:i]+".*"]
		}
	}
	if sid == "" {
		return ""
	}
	const status = "NATS/1.0 503\r\n\r\n"
	return fmt.Sprintf("HMSG %s %s %d %d\r\n%s\r\n", reply, sid, len(status), len(status), status)
}

func (f *fakeNATS) next(t *testing.T) []byte {
	t.Helper()
	select {
	case b := <-f.first:
		return b
	case <-time.After(10 * time.Second):
		t.Fatal("the client never connected to the fake server")
		return nil
	}
}

func fakeINFO(extra string) string {
	return `{"server_id":"FAKE","server_name":"fake","version":"2.14.6","proto":1,"host":"127.0.0.1",` +
		`"port":4222,"headers":true,"max_payload":1048576` + extra + `}`
}

// SPEC §8.7.1 against the wire: a server whose INFO says tls_required is never
// sent a TLS ClientHello -- nor anything else -- and the refusal is fatal with
// an error that names the cause, so the boot stops at once instead of retrying
// until sink.boot_timeout. A server that merely OFFERS TLS (tls_available)
// gets plaintext, and so does one that says nothing: the controls, which also
// prove the fake speaks enough NATS for a client to get through.
func TestNATSServerRequiringTLSIsNeverDialledWithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, mode := range []string{NATSModeJetStream, NATSModeCore} {
		t.Run("tls_required/"+mode, func(t *testing.T) {
			srv := startFakeNATS(t, fakeINFO(`,"tls_required":true`))
			n := newTestNATS(t, func(c *NATSConfig) {
				c.URLs, c.Mode, c.ConnectTimeout = []string{"nats://" + srv.addr}, mode, 5*time.Second
				if mode == NATSModeCore {
					c.Stream = ""
				}
			})
			err := n.Health(ctx)
			sent := srv.next(t)
			if len(sent) > 0 && sent[0] == 0x16 {
				t.Fatalf("the client sent a TLS handshake record (% x...)", sent[:min(len(sent), 8)])
			}
			if len(sent) > 0 {
				t.Errorf("the client sent %q to a server it cannot talk to", sent)
			}
			if err == nil {
				t.Fatal("Health succeeded against a server that requires TLS")
			}
			if !IsFatal(err) || !errors.Is(err, errNATSServerRequiresTLS) || !strings.Contains(err.Error(), "TLS") {
				t.Errorf("Health = %v (class %s), want a fatal error naming TLS", err, ClassOf(err))
			}
			if n.ServerAddr() != "" {
				t.Error("a connection was kept")
			}
		})
	}

	for name, extra := range map[string]string{"tls_available": `,"tls_available":true`, "plaintext": ""} {
		t.Run(name, func(t *testing.T) {
			srv := startFakeNATS(t, fakeINFO(extra))
			n := newTestNATS(t, func(c *NATSConfig) {
				c.URLs, c.Mode, c.Stream, c.ConnectTimeout = []string{"nats://" + srv.addr}, NATSModeCore, "", 5*time.Second
			})
			if err := n.Health(ctx); err != nil {
				t.Fatalf("Health against a plaintext server: %v", err)
			}
			if sent := srv.next(t); !bytes.HasPrefix(sent, []byte("CONNECT {")) {
				t.Errorf("the client opened with %q, want a plaintext CONNECT", sent)
			}
		})
	}
}

// A permissions violation reaches this sink only asynchronously: a denied
// publish times out (jetstream) or vanishes (core), a denied stream info times
// out, so nothing ever turned FATAL and the ring filled behind a sink that
// looked UP. A violation on the data path is kept as a verdict Health returns
// as fatal; one on the beacon's subject is not (the control plane must not
// take the data down), and one on another type's subject is not this
// server's data path.
func TestNATSPermissionViolationOnTheDataPathIsFatal(t *testing.T) {
	viol := func(s string) error {
		return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, s)
	}
	for _, tc := range []struct {
		err   error
		fatal bool
	}{
		{viol(`Permissions Violation for Publish to "loghub.acme.webhook"`), true},
		{viol(`Permissions Violation for Publish to "loghub.a.b.webhook"`), true},
		{viol(`Permissions Violation for Publish to "$JS.API.STREAM.INFO.LOGHUB_WEBHOOK"`), true},
		// The acks and the API's answers arrive on _INBOX: denied there, every
		// batch times out (SPEC §18.1 item 9, closed in the porte da família
		// syslog/RELP, 2026-10-13).
		{viol(`Permissions Violation for Subscription to "_INBOX.x"`), true},
		{viol(`Permissions Violation for Subscription to "_INBOX.x" using queue "q"`), true},
		{viol(`Permissions Violation for Publish to "loghub._ctl.beacon"`), false},
		{viol(`Permissions Violation for Publish to "loghub.ops.webhook.beacon"`), false},
		{viol(`Permissions Violation for Publish to "loghub.webhook"`), false},
		{viol(`Permissions Violation for Subscription to "loghub.acme.webhook"`), false},
		{viol(`Permissions Violation for Publish to "loghub.acme.syslog"`), false},
		{nil, false},
		{errors.New(`Permissions Violation for Publish to "loghub.acme.webhook"`), false}, // not the sentinel
		{nats.ErrTimeout, false},
	} {
		if got := deniesTheDataPath(tc.err); got != tc.fatal {
			t.Errorf("deniesTheDataPath(%v) = %v, want %v", tc.err, got, tc.fatal)
		}
	}

	p, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "acme"},
		quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.asyncError(viol(`Permissions Violation for Publish to "loghub._ctl.beacon"`))
	if p.verdict.Load() != nil {
		t.Fatal("a beacon denial became a verdict on the data path")
	}
	p.asyncError(viol(`Permissions Violation for Publish to "loghub.acme.webhook"`))
	err = p.Health(context.Background())
	if !IsFatal(err) || !strings.Contains(err.Error(), "permissions violation") {
		t.Fatalf("Health after a data-path denial = %v, want fatal naming it", err)
	}
}

// A token is the whole user part of a URL (SPEC §8.7.9), and nats.go's
// ConnectedUrlRedacted hides only a password: the token was printed in the
// "nats connected" INFO of every boot and the WARN of every reconnect.
func TestNATSConnectLogsNoCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, u := range []string{"nats://s3cr3tTok@", "nats://bob:s3cr3tPw@"} {
		srv := startFakeNATS(t, fakeINFO(""))
		var buf syncBuffer
		log := slog.New(slog.NewTextHandler(&buf, nil))
		n, err := NewNATS(NATSConfig{URLs: []string{u + srv.addr}, Mode: NATSModeCore, Workspace: "default",
			ConnectTimeout: 5 * time.Second}, log, testMetrics())
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Health(ctx); err != nil {
			t.Fatalf("Health against the fake: %v", err)
		}
		srv.drop()
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(buf.String(), "nats reconnected") && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		n.Close()
		out := buf.String()
		for _, want := range []string{"nats connected", "nats reconnected", "url=nats://" + srv.addr} {
			if !strings.Contains(out, want) {
				t.Errorf("%s: the log lacks %q:\n%s", u, want, out)
			}
		}
		if strings.Contains(out, "s3cr3t") {
			t.Errorf("%s: a credential reached the log:\n%s", u, out)
		}
	}
}

// syncBuffer is a bytes.Buffer a logger and the test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Core mode: a denied publish never fails PublishMsg -- the server answers
// with an -ERR, and the client reports it asynchronously. Write returned nil,
// so the batch was confirmed, counted as written and released: lost, and so
// was every batch until the next health check. The -ERR comes before the
// flush's PONG, so the batch that met it already comes back fatal and stays
// held for the dump.
func TestNATSCoreDeniedPublishIsFatalAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	srv.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.webhook\"'\r\n"
	n := newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.Mode, c.Stream, c.ConnectTimeout, c.OpTimeout =
			[]string{"nats://" + srv.addr}, NATSModeCore, "", 5*time.Second, 5*time.Second
	})
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health before any publish: %v", err)
	}
	items := mkBatch(3)
	if err := n.Write(ctx, items); !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Write of a denied batch = %v (class %s), want fatal naming the denial", err, ClassOf(err))
	}
	sent := srv.pubs.Load()
	if err := n.Write(ctx, mkBatch(1)); !IsFatal(err) {
		t.Errorf("Write after the verdict = %v, want fatal without publishing", err)
	}
	if got := srv.pubs.Load(); got != sent {
		t.Errorf("Write after the verdict published %d messages, want none", got-sent)
	}
	if err := n.Health(ctx); !IsFatal(err) {
		t.Errorf("Health after the verdict = %v, want fatal", err)
	}
}

// Core mode publishes a dump of another space with no stream to ask (cover is
// jetstream's): lost unless someone subscribes there. It is said once per
// subject, never per message, and never for this instance's own subject.
func TestNATSCoreWarnsOnceAboutAForeignSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	var buf syncBuffer
	n, err := NewNATS(NATSConfig{URLs: []string{"nats://" + srv.addr}, Mode: NATSModeCore, Workspace: "default",
		ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second},
		slog.New(slog.NewTextHandler(&buf, nil)), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.Write(ctx, mkBatch(2)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "another space") {
		t.Fatalf("a warning for the own subject:\n%s", buf.String())
	}
	for range 3 {
		items := []*envelope.Item{itemIn("acme", testRequest("/a", nil)), itemIn("acme", testRequest("/b", nil))}
		if err := n.Write(ctx, items); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(buf.String(), "subject=loghub.acme.webhook"); got != 1 {
		t.Errorf("%d warnings for the foreign subject, want 1:\n%s", got, buf.String())
	}
}
