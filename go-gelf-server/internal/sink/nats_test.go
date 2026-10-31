package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Nothing in this file needs a server. The integration half lives in
// nats_integration_test.go and skips without GELF_TEST_NATS_URL.

// SPEC §10.7 is the table; this is the table read back.
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
		// SPEC §22 pendência 19: the sentinels the table lacked, and what
		// nats.Connect returns at the first CONNECT -- the server's own words,
		// which its errors.Is never matched to the sentinels.
		{"auth revoked", nats.ErrAuthRevoked, ClassFatal},
		{"account auth expired", nats.ErrAccountAuthExpired, ClassFatal},
		{"connect: user authentication expired", errors.New("nats: User Authentication Expired"), ClassFatal},
		{"connect: user authentication revoked", errors.New("nats: User Authentication Revoked"), ClassFatal},
		{"connect: account authentication expired", errors.New("nats: Account Authentication Expired"), ClassFatal},
		{"connect: authorization violation", errors.New("nats: Authorization Violation"), ClassFatal},
		// Control: a permissions violation is about one subject, the verdict's
		// to judge (deniesTheDataPath), never a refusal of the connection.
		{"permissions violation", fmt.Errorf("%w: Permissions Violation for Publish to \"x\"",
			nats.ErrPermissionViolation), ClassNetwork},
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
		// A full discard=new stream, or one closing, is 10077 too: transient
		// (independent review, 2026-10-11, against nats-server 2.14.6).
		{"10077 max msgs", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages exceeded"}, ClassNetwork},
		{"10077 max bytes", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum bytes exceeded"}, ClassNetwork},
		{"10077 max per subject", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages per subject exceeded"}, ClassNetwork},
		// The text nats-server 2.14.6 sends: ErrStoreClosed, server/store.go:43,
		// as the {err} of JSStreamStoreFailedF. The invented "store closed"
		// this case used to carry matched a rule no server ever triggered
		// (independent review of the port, 2026-10-13).
		{"10077 store is closed", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "store is closed"}, ClassNetwork},
		{"10077 record too large", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "message too large"}, ClassFatal},
		{"api unknown 4xx", api(400, 10999), ClassFatal},
		// SPEC §22 pendência 18: transient though 4xx. 10158 (409) is a
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
	cfg := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_GELF", Workspace: "default"}
	if mut != nil {
		mut(&cfg)
	}
	n, err := NewNATS(cfg, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// SPEC §3.8: Data is the JLH and nothing else; the headers are exactly the
// five of the contract; Nats-Msg-Id is <uuid>.<step>, never the bare uuid.
func TestNATSMessageShape(t *testing.T) {
	n := newTestNATS(t, nil)
	b := batchOf(t, `{"version":"1.1","host":"web-01","short_message":"A\u0000B","_x":"\u00ff"}`)
	e := &b.Envelopes[0]
	msg := n.message(e)

	if msg.Subject != "loghub.default.gelf" {
		t.Errorf("subject = %q", msg.Subject)
	}
	uuid := e.Item.Head.UUID.String()
	want := map[string]string{
		"Nats-Msg-Id":    uuid + ".0",
		"Content-Type":   "application/json",
		"X-LogHub-Type":  "gelf",
		"X-LogHub-UUID":  uuid,
		"X-LogHub-Space": "default",
	}
	if len(msg.Header) != len(want) {
		t.Errorf("headers = %v: exactly %d, and the JLH in none of them", msg.Header, len(want))
	}
	for k, v := range want {
		if got := msg.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if string(msg.Data) != string(e.JLH) {
		t.Errorf("Data = %q, want the JLH alone", msg.Data)
	}
	var env map[string]any
	if err := json.Unmarshal(msg.Data, &env); err != nil || env["uuid"] != uuid {
		t.Errorf("Data does not decode to the envelope: %v", err)
	}
}

// SPEC §3.8: the header block is small and fixed -- at most 256 bytes with a
// 64-byte space, plus Nats-Expected-Stream -- far below the 64 KiB JetStream
// allows, because the JLH is in Data.
func TestNATSHeaderBlockIsBounded(t *testing.T) {
	space := strings.Repeat("s", 64)
	n := newTestNATS(t, func(c *NATSConfig) { c.Workspace = space })
	b := batchFrom(t, newItemIn(space, docGELF))
	msg := n.message(&b.Envelopes[0])
	msg.Header.Set(jetstream.MsgIDHeader, msg.Header.Get(jetstream.MsgIDHeader)[:36]+".32") // the largest step
	size := len("NATS/1.0\r\n") + len("\r\n")
	for k, vs := range msg.Header {
		for _, v := range vs {
			size += len(k) + len(": ") + len(v) + len("\r\n")
		}
	}
	if size > natsHeaderMax {
		t.Fatalf("header block of %d bytes, above the %d of SPEC §3.8", size, natsHeaderMax)
	}
}

// SPEC §3.8: the subject is loghub.<space>.gelf with the envelope's own space,
// whatever the level, and it is what the boot proves the stream covers.
func TestNATSSubjects(t *testing.T) {
	n := newTestNATS(t, nil)
	b := batchFrom(t, newItemIn("acme.prod", `{"version":"1.1","host":"h","short_message":"x","level":6}`),
		newItemIn("default", `{"version":"1.1","host":"h","short_message":"x","level":0}`))
	cases := []struct {
		e    *Envelope
		want string
	}{
		{&b.Envelopes[0], "loghub.acme.prod.gelf"},
		{&b.Envelopes[1], "loghub.default.gelf"},
	}
	for _, c := range cases {
		if got := n.message(c.e).Subject; got != c.want {
			t.Errorf("subject = %q, want %q", got, c.want)
		}
	}
	if got := n.coverageSubject(); got != "loghub.default.gelf" {
		t.Errorf("coverage subject = %q", got)
	}
}

// Decision 16: sink.nats.subjects (go-syslog-server's facility-severity form)
// does not exist here. A field left behind in NATSConfig would be a value the
// config could set and this package ignore, or worse, honour.
func TestNATSHasNoSubjectsOption(t *testing.T) {
	if _, ok := reflect.TypeOf(NATSConfig{}).FieldByName("Subjects"); ok {
		t.Error("NATSConfig has a Subjects field; the subject is always loghub.<space>.gelf (SPEC §3.8)")
	}
	// A data subject is the exact one only: the granular syslog shape is
	// not recognised as this server's.
	for subject, want := range map[string]bool{
		"loghub.default.gelf":              true,
		"loghub.a.b.gelf":                  true,
		"loghub.gelf":                      false,
		"loghub.default.gelf.kern.emerg":   false,
		"loghub.default.syslog":            false,
		"loghub.default.syslog.kern.emerg": false,
		"other.default.gelf":               false,
	} {
		if got := isDataSubject(subject); got != want {
			t.Errorf("isDataSubject(%q) = %v, want %v", subject, got, want)
		}
	}
}

// The local pattern matching of the boot validation (SPEC §10.7).
func TestSubjectCovers(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"loghub.default.gelf", "loghub.default.gelf", true},
		{"loghub.default.gelf.>", "loghub.default.gelf", false},
		{"loghub.*.gelf", "loghub.acme.gelf", true},
		{"loghub.*.gelf", "loghub.acme.prod.gelf", false},
		{"loghub.*.gelf", "loghub.default.gelf", true},
		{"loghub.>", "loghub.default.gelf", true},
		{"loghub.>", "loghub._ctl.beacon", true},
		{"loghub.*.*", "loghub._ctl.beacon", true},
		{"loghub.default.>", "loghub._ctl.beacon", false},
		{"loghub.*.syslog", "loghub.default.gelf", false},
		{">", "anything.at.all", true},
		{"*", "a.b", false},
		{"loghub.default.gelf", "loghub.default.gelf.>", false},
		{"loghub.default", "loghub.default.gelf", false},
	}
	for _, c := range cases {
		if got := subjectCovers(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectCovers(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

// SPEC §10.7 and §3.8: docker/nats/stream.json is the stream an operator
// creates, and the one the integration suite builds its streams from. It covers
// the subject of every single-token space and NEVER the control plane.
func TestNATSStreamFileMatchesTheContract(t *testing.T) {
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "LOGHUB_GELF" || cfg.Storage != jetstream.FileStorage || cfg.Duplicates != 10*time.Minute || cfg.Sealed {
		t.Errorf("stream = %s storage=%s dup=%s sealed=%v", cfg.Name, cfg.Storage, cfg.Duplicates, cfg.Sealed)
	}
	covers := func(s string) bool {
		for _, p := range cfg.Subjects {
			if subjectCovers(p, s) {
				return true
			}
		}
		return false
	}
	if !covers("loghub.default.gelf") || !covers("loghub.acme.gelf") {
		t.Errorf("subjects %v do not cover loghub.<space>.gelf", cfg.Subjects)
	}
	if covers("loghub.default.syslog") {
		t.Errorf("subjects %v capture the syslog sibling's subject", cfg.Subjects)
	}
	if covers(DefaultCtlNamespace + ".beacon") {
		t.Errorf("subjects %v capture the control plane", cfg.Subjects)
	}
	for _, p := range cfg.Subjects {
		if p == "loghub.>" {
			t.Error("loghub.> would capture loghub._ctl")
		}
	}
}

func TestNewNATSIsLazy(t *testing.T) {
	p, err := NewNATS(NATSConfig{
		URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default",
		ConnectTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	}, discard(), metrics())
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

	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := p.Write(ctx, batchN(t, 1)); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	if err := p.Health(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Health after Close = %v, want ErrClosed", err)
	}
}

func TestNewNATSRejectsBadConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	junkCA := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junkCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds := filepath.Join(t.TempDir(), "x.creds")
	if err := os.WriteFile(creds, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default"}

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
		{"missing CA", func(c *NATSConfig) { c.TLS = true; c.TLSCA = missing }, "reading"},
		{"junk CA", func(c *NATSConfig) { c.TLS = true; c.TLSCA = junkCA }, "no usable certificate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mut(&cfg)
			p, err := NewNATS(cfg, discard(), metrics())
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
	p, err := NewNATS(cfg, discard(), metrics())
	if err != nil {
		t.Fatalf("core without a stream refused: %v", err)
	}
	p.Close()
}

// The largest message counts the header block, and in jetstream mode the
// expected-stream header with the stream name (SPEC §3.9, §10.7).
func TestNATSMaxMessageBytes(t *testing.T) {
	js := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 1000 })
	core := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 1000; c.Mode = NATSModeCore })
	if got := core.maxMessageBytes(); got != 1000+natsHeaderMax {
		t.Errorf("core: %d", got)
	}
	if got := js.maxMessageBytes(); got != int64(1000+natsHeaderMax+natsExpectedStreamHeader+len("LOGHUB_GELF")) {
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

// Review finding 4: a stream with no_ack never acknowledges, so every write
// would time out as network forever with the sink UP. Refused at boot.
func TestNATSValidateRefusesNoAck(t *testing.T) {
	n := newTestNATS(t, nil)
	err := n.validate(1<<20, &jetstream.StreamInfo{Config: jetstream.StreamConfig{
		Name: "LOGHUB_GELF", Subjects: []string{"loghub.*.gelf"}, NoAck: true,
	}})
	if !IsFatal(err) || !strings.Contains(err.Error(), "no_ack") {
		t.Fatalf("validate = %v, want a fatal error naming no_ack", err)
	}
}

// countingHandler counts the records it receives.
type countingHandler struct{ n atomic.Int64 }

func (h *countingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error { h.n.Add(1); return nil }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countingHandler) WithGroup(string) slog.Handler             { return h }

// Review finding 7: the client's async errors arrive once per message when,
// say, publish permission is denied; they are logged sampled, never one line
// per message (SPEC §14.2).
func TestNATSAsyncErrorsAreSampled(t *testing.T) {
	h := &countingHandler{}
	n, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "default"},
		slog.New(h), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for i := 0; i < 100; i++ {
		n.asyncError(errors.New(`nats: permissions violation for publish to "loghub.default.gelf"`))
	}
	if got := h.n.Load(); got != 1 {
		t.Fatalf("%d log lines for 100 async errors, want 1", got)
	}
}
