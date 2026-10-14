package sink

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// Nothing in this file needs a server. The integration half lives in
// nats_integration_test.go and skips without WHS_TEST_NATS_URL.

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
		// nats.go returns the server's words at CONNECT, and its errors.Is
		// compares the whole text: these never matched the sentinels above.
		{"connect: authorization violation", errors.New("nats: Authorization Violation"), ClassFatal},
		{"connect: user authentication expired", errors.New("nats: User Authentication Expired"), ClassFatal},
		{"connect: user authentication revoked", errors.New("nats: User Authentication Revoked"), ClassFatal},
		{"connect: account authentication expired", errors.New("nats: Account Authentication Expired"), ClassFatal},
		// A TLS that can never succeed: sink.nats.tls against a server
		// without TLS, a certificate that does not verify. A handshake cut
		// short is network (SPEC §8.7.9).
		{"tls wanted", nats.ErrSecureConnWanted, ClassFatal},
		{"tls verification", fmt.Errorf("%w: %w", nats.ErrTLS, &tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}), ClassFatal},
		{"tls handshake reset", fmt.Errorf("%w: connection reset by peer", nats.ErrTLS), ClassNetwork},
		{"closed", nats.ErrConnectionClosed, ClassFatal},
		{"stream not found", jetstream.ErrStreamNotFound, ClassFatal},
		{"jetstream not enabled", jetstream.ErrJetStreamNotEnabled, ClassFatal},
		{"jetstream not enabled for account", jetstream.ErrJetStreamNotEnabledForAccount, ClassFatal},
		{"api stream not match", api(400, jsErrStreamNotMatch), ClassFatal},
		{"api sealed", api(400, jsErrStreamSealed), ClassFatal},
		{"api insufficient resources", api(503, jsErrInsufficientResources), ClassFatal},
		{"api store failed", api(503, jsErrStreamStoreFailed), ClassFatal},
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
		{"api stream not found by code", api(404, jetstream.JSErrCodeStreamNotFound), ClassFatal},
		{"api stream not found 10059", api(404, jsErrStreamNotFound), ClassFatal},
		{"api jetstream off 10076", api(503, jsErrJetStreamNotEnabled), ClassFatal},
		{"api jetstream off for account 10039", api(503, jsErrJetStreamNotEnabledFA), ClassFatal},
		{"api unknown 4xx", api(400, 10999), ClassFatal},
		// Transient though 4xx: 10158 (409) is a Nats-Msg-Id still in a Raft
		// proposal, 10167 (429) a full ingest queue, and any 429 is
		// backpressure; another 409 stays fatal.
		{"api duplicate in process 10158", api(409, jsErrDuplicateInProcess), ClassNetwork},
		{"api too many requests 10167", api(429, jsErrStreamTooManyRequests), ClassNetwork},
		{"api unknown 429", api(429, 10210), ClassNetwork},
		{"api unknown 409", api(409, 10999), ClassFatal},
		{"wrapped api 10158", fmt.Errorf("ack: %w", api(409, jsErrDuplicateInProcess)), ClassNetwork},

		{"max payload", nats.ErrMaxPayload, ClassLogic},
		{"max bytes", jetstream.ErrMaxBytesExceeded, ClassLogic},
		{"api message too big", api(400, jsErrMessageExceedsMaximum), ClassLogic},
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

// SPEC §3.8: the subject expanded from the envelope's own space and type,
// Nats-Msg-Id = <uuid>.<step>, the X-LogHub-* headers, and Data the JLH
// itself -- the item's slice, not a copy, and no part of the envelope in a
// header (the JetStream ceiling on headers is 64 KiB).
func TestNATSMessageShape(t *testing.T) {
	n := &NATS{subject: newTopic("loghub.{space}.{type}", "default")}
	for _, tc := range []struct {
		space, subject string
	}{
		{"default", "loghub.default.webhook"},
		{"acme.store", "loghub.acme.store.webhook"},
	} {
		it := itemIn(tc.space, testRequest("/x", []byte("A\x00B")))
		msg, err := n.message(it)
		if err != nil {
			t.Fatalf("%s: %v", tc.space, err)
		}
		if msg.Subject != tc.subject {
			t.Errorf("subject = %q, want %q", msg.Subject, tc.subject)
		}
		want := map[string]string{
			jetstream.MsgIDHeader: it.ID + ".0",
			"Content-Type":        "application/json",
			natsHeaderType:        envelope.Type,
			natsHeaderUUID:        it.ID,
			natsHeaderSpace:       tc.space,
		}
		for k, v := range want {
			if got := msg.Header.Get(k); got != v {
				t.Errorf("%s = %q, want %q", k, got, v)
			}
		}
		if len(msg.Header) != len(want) {
			t.Errorf("headers %v, want exactly %v", msg.Header, want)
		}
		if !bytes.Equal(msg.Data, it.Env) || &msg.Data[0] != &it.Env[0] {
			t.Error("Data is not the item's JLH slice")
		}
		validJLH(t, msg.Data)
	}

	// A literal subject, without placeholders, is used as is.
	lit := &NATS{subject: newTopic("webhooks.in", "default")}
	msg, err := lit.message(itemIn("acme", testRequest("/x", nil)))
	if err != nil || msg.Subject != "webhooks.in" {
		t.Errorf("literal subject = %q (%v)", msg.Subject, err)
	}
}

// An item whose JLH this server did not write maps to no message; Write
// refuses it alone, as logic (SPEC §8.7.2).
func TestNATSMessageRefusesAnEnvelopeItDidNotWrite(t *testing.T) {
	n := &NATS{subject: newTopic("loghub.{space}.{type}", "default")}
	if msg, err := n.message(badItem()); err == nil || msg != nil {
		t.Errorf("message(v1 envelope) = %v, %v, want nil and an error", msg, err)
	}
	// Control: the fixture itself maps.
	if _, err := n.message(mkItem("good")); err != nil {
		t.Fatalf("control: %v", err)
	}
}

func TestNewNATSIsLazy(t *testing.T) {
	p, err := NewNATS(NATSConfig{
		URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_WEBHOOK", Subject: "loghub.{space}.{type}", Workspace: "default",
		ConnectTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	}, quietLog(), testMetrics())
	if err != nil {
		t.Fatalf("NewNATS against a dead port: %v", err)
	}
	if p.Type() != "nats" {
		t.Errorf("Type = %q", p.Type())
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
	if err := p.Write(ctx, mkBatch(1)); !errors.Is(err, ErrClosed) {
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
	base := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_WEBHOOK", Subject: "loghub.{space}.{type}", Workspace: "default"}

	cases := []struct {
		name string
		mut  func(*NATSConfig)
		want string
	}{
		{"no urls", func(c *NATSConfig) { c.URLs = nil }, "URL"},
		{"empty stream", func(c *NATSConfig) { c.Stream = "" }, "stream"},
		{"empty subject", func(c *NATSConfig) { c.Subject = "" }, "subject"},
		{"missing credentials", func(c *NATSConfig) { c.Credentials = missing }, "credentials"},
		{"missing CA", func(c *NATSConfig) { c.TLS = true; c.TLSCA = missing }, "reading"},
		{"junk CA", func(c *NATSConfig) { c.TLS = true; c.TLSCA = junkCA }, "no usable certificate"},
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

// newTestNATS is a sink against a dead port, for the tests that never
// connect.
func newTestNATS(t *testing.T, mut func(*NATSConfig)) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_WEBHOOK",
		Subject: "loghub.{space}.{type}", Workspace: "default"}
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

// SPEC §8.7.5 refusal by refusal, without a server: each is fatal, and each
// has a control that passes. Every Health runs this against the stream info
// it fetched, so a stream edited after the boot is refused the same way.
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
	// A subject template without the LogHub prefix is still held off the
	// control plane: what matters is what the stream captures.
	lit := newTestNATS(t, func(c *NATSConfig) { c.Subject = "webhooks.in" })
	both := ok
	both.Subjects = []string{"webhooks.in", "loghub._ctl.>"}
	if err := lit.validate(1<<20, &jetstream.StreamInfo{Config: both}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "control plane") {
		t.Errorf("a stream that also captures loghub._ctl.>: %v", err)
	}

	// The sizes (SPEC §8.7.5): the largest message against max_payload, the
	// stream's max_msg_size, the file store's 32 MiB record and a stream that
	// refuses new messages when full.
	sized := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = 8022700 })
	need := int64(8022700+natsHeaderOverhead) + int64(len(sized.cfg.Stream))
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
	// longer covers -- the operator narrowed it once the backlog drained, as
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

// countingHandler counts the records it receives.
type countingHandler struct{ n atomic.Int64 }

func (h *countingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error { h.n.Add(1); return nil }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *countingHandler) WithGroup(string) slog.Handler             { return h }

// The client's async errors arrive once per message when, say, publish
// permission is denied; they are logged sampled, never one line per message
// (SPEC §8.7.2, §11.3).
func TestNATSAsyncErrorsAreSampled(t *testing.T) {
	h := &countingHandler{}
	n, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S",
		Subject: "loghub.{space}.{type}", Workspace: "default"}, slog.New(h), testMetrics())
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

// SPEC §8.7.2: a denied publish or JetStream API call is reported only by the
// asynchronous error callback; the publish itself just timed out, network,
// so nothing ever turned FATAL and the ring filled behind a sink that looked
// UP (the review of go-syslog-nats-server). Every violation is a verdict --
// this server publishes nothing but data on its connection -- that Health and
// every Write return as fatal, Write without publishing anything.
func TestNATSPermissionViolationIsFatal(t *testing.T) {
	viol := func(s string) error {
		return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, s)
	}
	for _, tc := range []struct {
		err   error
		fatal bool
	}{
		{viol(`Permissions Violation for Publish to "loghub.acme.webhook"`), true},
		{viol(`Permissions Violation for Publish to "$JS.API.STREAM.INFO.LOGHUB_WEBHOOK"`), true},
		{viol(`Permissions Violation for Subscription to "_INBOX.x"`), true},
		{errors.New(`Permissions Violation for Publish to "loghub.acme.webhook"`), false}, // not the sentinel
		{nats.ErrTimeout, false},
	} {
		if got := deniesTheDataPath(tc.err); got != tc.fatal {
			t.Errorf("deniesTheDataPath(%v) = %v, want %v", tc.err, got, tc.fatal)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	p := newTestNATS(t, func(c *NATSConfig) { c.URLs = []string{"nats://" + srv.addr} })
	p.asyncError(errors.New("nats: slow consumer"))
	if p.verdict.Load() != nil {
		t.Fatal("an error that is no permission violation became a verdict")
	}
	p.asyncError(viol(`Permissions Violation for Publish to "loghub.default.webhook"`))
	err := p.Health(ctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Health after a denial = %v, want fatal naming it", err)
	}
	if err := p.Write(ctx, mkBatch(2)); !IsFatal(err) {
		t.Errorf("Write after the verdict = %v, want fatal", err)
	}
	if got := srv.pubs.Load(); got != 0 {
		t.Errorf("the verdict let %d messages out, want none", got)
	}
}

// SPEC §8.7.1: the connect and reconnect logs show the server's scheme and
// host only. nats.go's ConnectedUrl is the URL with the password, and its
// ConnectedUrlRedacted hides a password but not a token user part: one or the
// other was printed in the INFO of every boot and the WARN of every
// reconnect.
func TestNATSConnectLogsNoCredentials(t *testing.T) {
	for _, u := range []string{"nats://s3cr3tTok@", "nats://bob:s3cr3tPw@"} {
		srv := startFakeNATS(t, fakeINFO(""))
		var buf syncBuffer
		log := slog.New(slog.NewTextHandler(&buf, nil))
		n, err := NewNATS(NATSConfig{URLs: []string{u + srv.addr}, Stream: "S",
			Subject: "loghub.{space}.{type}", Workspace: "default", ConnectTimeout: 5 * time.Second},
			log, testMetrics())
		if err != nil {
			t.Fatal(err)
		}
		// The fake answers no JetStream API: Health connects, then waits
		// for a stream info that never comes. Only the logs matter here.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_ = n.Health(ctx)
		cancel()
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

	mu    sync.Mutex
	conns []net.Conn
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
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "PUB ") || strings.HasPrefix(line, "HPUB ") {
			f.pubs.Add(1)
			if f.pubReply != "" {
				if _, err := c.Write([]byte(f.pubReply)); err != nil {
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
