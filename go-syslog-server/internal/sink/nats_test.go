package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Nothing in this file needs a server. The integration half lives in
// nats_integration_test.go and skips without SYSLOG_TEST_NATS_URL.

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
		{"api unknown 4xx", api(400, 10999), ClassFatal},

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
	cfg := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_SYSLOG", Workspace: "default"}
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
	b := batchOf(t, "<165>1 - web-01 nginx - - - A\x00B\xff")
	e := &b.Envelopes[0]
	msg := n.message(e)

	if msg.Subject != "loghub.default.syslog" {
		t.Errorf("subject = %q", msg.Subject)
	}
	uuid := e.Item.Head.UUID.String()
	want := map[string]string{
		"Nats-Msg-Id":    uuid + ".0",
		"Content-Type":   "application/json",
		"X-LogHub-Type":  "syslog",
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
	b := batchFrom(t, newItemIn(space, line5424))
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

// SPEC §3.8: with facility-severity the subject carries the RFC 5427 names;
// the space is the envelope's own.
func TestNATSSubjects(t *testing.T) {
	exact := newTestNATS(t, nil)
	gran := newTestNATS(t, func(c *NATSConfig) { c.Subjects = NATSSubjectsFacilitySeverity })

	b := batchFrom(t, newItemIn("acme.prod", "<86>1 - h a - - - x"), newItemIn("default", "<0>1 - h a - - - x"))
	cases := []struct {
		n    *NATS
		e    *Envelope
		want string
	}{
		{exact, &b.Envelopes[0], "loghub.acme.prod.syslog"},
		{gran, &b.Envelopes[0], "loghub.acme.prod.syslog.authpriv.info"},
		{gran, &b.Envelopes[1], "loghub.default.syslog.kern.emerg"},
	}
	for _, c := range cases {
		if got := c.n.message(c.e).Subject; got != c.want {
			t.Errorf("subject = %q, want %q", got, c.want)
		}
	}
	if got := gran.coverageSubject(); got != "loghub.default.syslog.*.*" {
		t.Errorf("coverage subject = %q", got)
	}
}

// The local pattern matching of the boot validation (SPEC §10.7).
func TestSubjectCovers(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"loghub.default.syslog", "loghub.default.syslog", true},
		{"loghub.default.syslog.>", "loghub.default.syslog", false},
		{"loghub.default.syslog.>", "loghub.default.syslog.*.*", true},
		{"loghub.default.syslog.*.*", "loghub.default.syslog.*.*", true},
		{"loghub.default.syslog.*", "loghub.default.syslog.*.*", false},
		{"loghub.*.syslog", "loghub.default.syslog", true},
		{"loghub.>", "loghub.default.syslog", true},
		{"loghub.>", "loghub._ctl.beacon", true},
		{"loghub.*.*", "loghub._ctl.beacon", true},
		{"loghub.default.>", "loghub._ctl.beacon", false},
		{"loghub.default.syslog.authpriv.*", "loghub.default.syslog.*.*", false},
		{">", "anything.at.all", true},
		{"*", "a.b", false},
		{"loghub.default.syslog", "loghub.default.syslog.>", false},
		{"loghub.default", "loghub.default.syslog", false},
	}
	for _, c := range cases {
		if got := subjectCovers(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectCovers(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

// SPEC §10.7 and §3.8: docker/nats/stream.json is the stream an operator
// creates, and the one the integration suite builds its streams from. It covers
// both subject shapes of space default and NEVER the control plane.
func TestNATSStreamFileMatchesTheContract(t *testing.T) {
	b, err := os.ReadFile("../../docker/nats/stream.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg jetstream.StreamConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Name != "LOGHUB_SYSLOG" || cfg.Storage != jetstream.FileStorage || cfg.Duplicates != 10*time.Minute || cfg.Sealed {
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
	if !covers("loghub.default.syslog") || !covers("loghub.default.syslog.*.*") {
		t.Errorf("subjects %v do not cover both subject shapes", cfg.Subjects)
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
		{"bad subjects", func(c *NATSConfig) { c.Subjects = "per-host" }, "subjects"},
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
	if got := js.maxMessageBytes(); got != int64(1000+natsHeaderMax+natsExpectedStreamHeader+len("LOGHUB_SYSLOG")) {
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
	err := n.validate(nil, &jetstream.StreamInfo{Config: jetstream.StreamConfig{
		Name: "LOGHUB_SYSLOG", Subjects: []string{"loghub.default.syslog", "loghub.default.syslog.>"}, NoAck: true,
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
		n.asyncError(errors.New(`nats: permissions violation for publish to "loghub.default.syslog"`))
	}
	if got := h.n.Load(); got != 1 {
		t.Fatalf("%d log lines for 100 async errors, want 1", got)
	}
}
