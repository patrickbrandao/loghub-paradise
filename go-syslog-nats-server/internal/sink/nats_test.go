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
		{"auth revoked", nats.ErrAuthRevoked, ClassFatal},
		{"account auth expired", nats.ErrAccountAuthExpired, ClassFatal},
		// What nats.Connect returns: the server's own words, which its
		// errors.Is never matched to the sentinels above (SPEC §10.7).
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
		// (ported from go-relplog-nats-server, 2026-10-13; R's independent
		// review, 2026-10-11, against nats-server 2.14.6).
		{"10077 max msgs", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages exceeded"}, ClassNetwork},
		{"10077 max bytes", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum bytes exceeded"}, ClassNetwork},
		{"10077 max per subject", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "maximum messages per subject exceeded"}, ClassNetwork},
		{"10077 store closed", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "store closed"}, ClassNetwork},
		{"10077 record too large", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: "message too large"}, ClassFatal},
		{"api unknown 4xx", api(400, 10999), ClassFatal},
		// SPEC §21.1 item 6 (closed): transient though 4xx. 10158 (409) is a
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

		// SPEC §1.2: a server that demands TLS will never take this server's
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

// SPEC §3.8: the subject is loghub.<space>.syslog, always, with the space of
// the envelope's OWN head -- not the instance's -- and the string is cached per
// space, so a chunk does not rebuild it per message.
func TestNATSSubjects(t *testing.T) {
	n := newTestNATS(t, nil)

	b := batchFrom(t, newItemIn("acme.prod", "<86>1 - h a - - - x"), newItemIn("default", "<0>1 - h a - - - x"),
		newItemIn("acme.prod", "<13>1 - h a - - - y"))
	want := []string{"loghub.acme.prod.syslog", "loghub.default.syslog", "loghub.acme.prod.syslog"}
	for i := range b.Envelopes {
		if got := n.message(&b.Envelopes[i]).Subject; got != want[i] {
			t.Errorf("envelope %d: subject = %q, want %q", i, got, want[i])
		}
	}
	// Control: severity and facility leave no trace in the subject, so the two
	// envelopes of acme.prod (authpriv.info and user.notice) share one.
	first, again := n.subject(&b.Envelopes[0]), n.subject(&b.Envelopes[2])
	if first != again || unsafe.StringData(first) != unsafe.StringData(again) {
		t.Error("the subject of a space is not cached: two envelopes of one space built it twice")
	}
	spaces := 0
	n.subjects.Range(func(k, v any) bool {
		spaces++
		if v.(string) != "loghub."+k.(string)+".syslog" {
			t.Errorf("cache maps %q to %q", k, v)
		}
		return true
	})
	// The own space never reaches the map: it is the hot path, one string
	// comparison (SPEC §21.1 item 8, closed); the foreign one is cached.
	if spaces != 1 {
		t.Errorf("%d spaces cached, want 1 (only the foreign one)", spaces)
	}
	if got := n.coverageSubject(); got != "loghub.default.syslog" {
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
		{"a.b.>", "a.b.*.*", true},
		{"a.b.*.*", "a.b.*.*", true},
		{"a.b.*", "a.b.*.*", false},
		{"loghub.*.syslog", "loghub.default.syslog", true},
		{"loghub.>", "loghub.default.syslog", true},
		{"loghub.>", "loghub._ctl.beacon", true},
		{"loghub.*.*", "loghub._ctl.beacon", true},
		{"loghub.default.>", "loghub._ctl.beacon", false},
		{"a.b.c.*", "a.b.*.*", false},
		{"loghub.*.syslog", "loghub.acme.prod.syslog", false},
		{"loghub.acme.*.syslog", "loghub.acme.prod.syslog", true},
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

// natsCanonicalStream is the stream an operator creates for one space (SPEC
// §10.7): the exact subject of the space and nothing that could capture the
// control plane, file storage, a ten-minute duplicate window for the
// redelivery of SPEC §10.2. The integration suite builds every stream from it.
func natsCanonicalStream(name, space string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              name,
		Description:       "go-syslog-nats-server: JLH type=syslog of space " + space,
		Subjects:          []string{"loghub." + space + ".syslog"},
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

// natsStreamContract is what SPEC §10.7 asks of the stream of space default.
func natsStreamContract(t *testing.T, cfg jetstream.StreamConfig) {
	t.Helper()
	if cfg.Name != "LOGHUB_SYSLOG" || cfg.Storage != jetstream.FileStorage || cfg.Duplicates != 10*time.Minute ||
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
	if !covers("loghub.default.syslog") {
		t.Errorf("subjects %v do not cover loghub.default.syslog", cfg.Subjects)
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

// SPEC §10.7: the stream the integration suite builds from, and the
// docker/nats/stream.json an operator hands to `nats stream add` once this
// module ships one, meet the contract the boot validates.
func TestNATSCanonicalStreamMatchesTheContract(t *testing.T) {
	natsStreamContract(t, natsCanonicalStream("LOGHUB_SYSLOG", "default"))

	b, err := os.ReadFile("../../docker/nats/stream.json")
	if errors.Is(err, os.ErrNotExist) {
		t.Log("docker/nats/stream.json does not exist yet; only the suite's canonical stream was checked")
		return
	}
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
		p, err := NewNATS(cfg, discard(), metrics())
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
		// SPEC §1.2: no TLS. nats.go would dial these with TLS (tls://, wss://)
		// or as a websocket; the configuration refuses them, and so does the
		// sink, so no path reaches a TLS dial.
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

// The validation refusals that need no server (SPEC §10.7), each with its
// control: no_ack (review finding 4: every write would time out as network
// forever with the sink UP), sealed, a stream that does not cover the own
// subject, one that captures the control plane, and the sizes -- max_payload,
// max_msg_size, the file store's record, and the max_bytes of a stream that
// discards new messages (ported from go-webhook-nats-server).
func TestNATSValidateRefusals(t *testing.T) {
	n := newTestNATS(t, nil)
	ok := jetstream.StreamConfig{Name: "LOGHUB_SYSLOG", Subjects: []string{"loghub.default.syslog"},
		Storage: jetstream.FileStorage}
	if err := n.validate(1<<20, &jetstream.StreamInfo{Config: ok}); err != nil {
		t.Fatalf("control: %v", err)
	}
	if err := n.validate(1<<20, nil); err == nil || IsFatal(err) {
		t.Errorf("no stream info = %v, want a network error", err)
	}
	for name, tc := range map[string]struct {
		mut  func(*jetstream.StreamConfig)
		want string
	}{
		"no_ack":        {func(c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
		"sealed":        {func(c *jetstream.StreamConfig) { c.Sealed = true }, "sealed"},
		"not covered":   {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.other.syslog"} }, "does not cover"},
		"wrong type":    {func(c *jetstream.StreamConfig) { c.Subjects = []string{"loghub.default.webhook"} }, "does not cover"},
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

	// The sizes (SPEC §3.9): the largest message against max_payload, the
	// stream's max_msg_size and the file store's 32 MiB record.
	sized := newTestNATS(t, func(c *NATSConfig) { c.MaxEventBytes = MaxEventBytes(64 << 10) })
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
	// record: 10077 on the first large message, fatal, and again on every
	// reingestion. One record exactly fits; discarding old messages makes
	// room instead (the controls).
	record := need + int64(len("loghub.default.syslog")) + 64
	full := ok
	full.MaxBytes, full.Discard = record-1, jetstream.DiscardNew
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: full}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "max_bytes") {
		t.Errorf("max_bytes one byte below a record with discard=new: %v", err)
	}
	full.MaxBytes = record
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: full}); err != nil {
		t.Errorf("max_bytes of exactly one record with discard=new: %v", err)
	}
	full.MaxBytes, full.Discard = record-1, jetstream.DiscardOld
	if err := sized.validate(64<<20, &jetstream.StreamInfo{Config: full}); err != nil {
		t.Errorf("max_bytes with discard=old makes room: %v", err)
	}

	// No responders at the first check is JetStream unavailable: fatal, at
	// once. After a validated check it is a blip: network, retried.
	js := newTestNATS(t, nil)
	if err := js.streamErr(nats.ErrNoResponders); !IsFatal(err) || !strings.Contains(err.Error(), "JetStream") {
		t.Errorf("no responders at the first check = %v, want fatal naming JetStream", err)
	}
	js.validated.Store(true)
	if err := js.streamErr(nats.ErrNoResponders); ClassOf(err) != ClassNetwork {
		t.Errorf("no responders after a validated check = %v (class %s), want network", err, ClassOf(err))
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

// ------------------------------------------------------------------- no TLS

// SPEC §1.2: this server has no TLS. None of the options NewNATS hands the
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
	}
}

// fakeNATS is just enough of a NATS server to see what a client sends after
// the INFO: it greets every connection with info, answers PING with PONG, and
// hands each connection's first bytes (empty when the client hung up without
// a word) to first.
type fakeNATS struct {
	addr  string
	first chan []byte

	// pubs counts the PUB/HPUB lines received.
	pubs atomic.Int64

	mu sync.Mutex
	// info is what the next connection is greeted with: setInfo changes it
	// for the connections after a drop, as a server restarted with another
	// configuration would.
	info string
	// pubReply, when set, is written after every PUB/HPUB, before any PONG
	// that follows it -- the order of a real server's -ERR.
	pubReply string
	conns    []net.Conn
}

func startFakeNATS(t *testing.T, info string) *fakeNATS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &fakeNATS{addr: ln.Addr().String(), first: make(chan []byte, 16), info: info}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

// setInfo changes the INFO of the connections accepted from now on.
func (f *fakeNATS) setInfo(info string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.info = info
}

// setPubReply makes the fake answer every publish with reply.
func (f *fakeNATS) setPubReply(reply string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubReply = reply
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

func (f *fakeNATS) serve(c net.Conn) {
	defer c.Close()
	f.mu.Lock()
	f.conns = append(f.conns, c)
	info := f.info
	f.mu.Unlock()
	if _, err := c.Write([]byte("INFO " + info + "\r\n")); err != nil {
		f.first <- nil
		return
	}
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 4096)
	n, _ := c.Read(buf)
	select {
	case f.first <- append([]byte(nil), buf[:n]...):
	default:
		// Nobody reads the first bytes of a reconnection; never block on it.
	}
	// The plaintext path goes on: answer every PING so a flush completes.
	rd := bufio.NewReader(io.MultiReader(bytes.NewReader(buf[:n]), c))
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		if strings.HasPrefix(line, "PUB ") || strings.HasPrefix(line, "HPUB ") {
			f.pubs.Add(1)
			f.mu.Lock()
			reply := f.pubReply
			f.mu.Unlock()
			if reply != "" {
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

// SPEC §1.2 against the wire: a server whose INFO says tls_required is never
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

// Review D #3: a permissions violation reaches this sink only asynchronously:
// a denied publish times out (jetstream) or vanishes (core), a denied stream
// info times out, so nothing ever turned FATAL and the ring filled behind a
// sink that looked UP. A violation on the data path is now kept as a verdict
// Health returns as fatal; one on the beacon's subject is not (the control
// plane must not take the data down).
func TestNATSPermissionViolationOnTheDataPathIsFatal(t *testing.T) {
	viol := func(s string) error {
		return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, s)
	}
	for _, tc := range []struct {
		err   error
		fatal bool
	}{
		{viol(`Permissions Violation for Publish to "loghub.acme.syslog"`), true},
		{viol(`Permissions Violation for Publish to "loghub.a.b.syslog"`), true},
		{viol(`Permissions Violation for Publish to "$JS.API.STREAM.INFO.LOGHUB_SYSLOG"`), true},
		// The acks and the API's answers arrive on _INBOX: denied there, every
		// chunk times out (SPEC §21.1 item 7, closed).
		{viol(`Permissions Violation for Subscription to "_INBOX.x"`), true},
		{viol(`Permissions Violation for Publish to "loghub._ctl.beacon"`), false},
		{errors.New(`Permissions Violation for Publish to "loghub.acme.syslog"`), false}, // not the sentinel
		{nats.ErrTimeout, false},
	} {
		if got := deniesTheDataPath(tc.err); got != tc.fatal {
			t.Errorf("deniesTheDataPath(%v) = %v, want %v", tc.err, got, tc.fatal)
		}
	}

	p, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Workspace: "acme"},
		discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.asyncError(viol(`Permissions Violation for Publish to "loghub._ctl.beacon"`))
	if p.verdict.Load() != nil {
		t.Fatal("a beacon denial became a verdict on the data path")
	}
	p.asyncError(viol(`Permissions Violation for Publish to "loghub.acme.syslog"`))
	err = p.Health(context.Background())
	if !IsFatal(err) || !strings.Contains(err.Error(), "permissions violation") {
		t.Fatalf("Health after a data-path denial = %v, want fatal naming it", err)
	}
}

// ------------------------------------------------ ported from go-webhook-nats-server

// A token is the whole user part of a URL (SPEC §10.9), and nats.go's
// ConnectedUrlRedacted hides only a password: the token was printed in the
// "nats connected" INFO of every boot and the WARN of every reconnect
// (SPEC §10.7). The password case is the control that the log line is there.
func TestNATSConnectLogsNoCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, u := range []string{"nats://s3cr3tTok@", "nats://bob:s3cr3tPw@"} {
		srv := startFakeNATS(t, fakeINFO(""))
		var buf syncBuffer
		log := slog.New(slog.NewTextHandler(&buf, nil))
		n, err := NewNATS(NATSConfig{URLs: []string{u + srv.addr}, Mode: NATSModeCore, Workspace: "default",
			ConnectTimeout: 5 * time.Second}, log, metrics())
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

// Core mode: a denied publish never fails PublishMsg -- the server answers
// with an -ERR, and the client reports it asynchronously. Write returned nil,
// so the chunk was confirmed, counted as written and released: lost, and so
// was every chunk until the next health check. The -ERR comes before the
// flush's PONG, so the chunk that met it already comes back fatal and stays
// held for the dump; the next Write publishes nothing (SPEC §10.7).
func TestNATSCoreDeniedPublishIsFatalAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	srv.setPubReply("-ERR 'Permissions Violation for Publish to \"loghub.default.syslog\"'\r\n")
	n := newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.Mode, c.Stream, c.ConnectTimeout, c.OpTimeout =
			[]string{"nats://" + srv.addr}, NATSModeCore, "", 5*time.Second, 5*time.Second
	})
	// The asynchronous ErrorHandler is held back, so only the Write's own
	// read of LastError can produce the verdict in time: with the handler
	// racing the flush, a Write that skipped the read passed now and then.
	n.opts = append(n.opts, func(o *nats.Options) error {
		async := o.AsyncErrorCB
		o.AsyncErrorCB = func(c *nats.Conn, s *nats.Subscription, err error) {
			time.Sleep(time.Second)
			async(c, s, err)
		}
		return nil
	})
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health before any publish: %v", err)
	}
	if err := n.Write(ctx, batchN(t, 3)); !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Write of a denied chunk = %v (class %s), want fatal naming the denial", err, ClassOf(err))
	}
	sent := srv.pubs.Load()
	if sent != 3 {
		t.Fatalf("the fake saw %d publishes, want the 3 of the chunk", sent)
	}
	if err := n.Write(ctx, batchN(t, 1)); !IsFatal(err) {
		t.Errorf("Write after the verdict = %v, want fatal without publishing", err)
	}
	if got := srv.pubs.Load(); got != sent {
		t.Errorf("Write after the verdict published %d messages, want none", got-sent)
	}
	if err := n.Health(ctx); !IsFatal(err) {
		t.Errorf("Health after the verdict = %v, want fatal", err)
	}
}

// Every Write returns the verdict, in jetstream mode too, before touching the
// connection: a chunk published after the server refused this one would only
// time out, network, and be retried until the next health check (SPEC §10.7).
func TestNATSWriteReturnsTheVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n := newTestNATS(t, func(c *NATSConfig) { c.ConnectTimeout = time.Second })
	n.asyncError(fmt.Errorf("%w: %s", nats.ErrPermissionViolation,
		`Permissions Violation for Publish to "$JS.API.STREAM.INFO.LOGHUB_SYSLOG"`))
	err := n.Write(ctx, batchN(t, 1))
	if !IsFatal(err) || !strings.Contains(err.Error(), "permissions violation") {
		t.Fatalf("Write after a data-path denial = %v (class %s), want fatal naming it", err, ClassOf(err))
	}
}

// Core mode checks max_payload on every health check, not once: a reconnection
// may land on a server with a smaller one, and every larger message would then
// be refused by the client, logic, chunk after chunk, with the sink UP
// (SPEC §10.7).
func TestNATSCoreMaxPayloadIsCheckedOnEveryCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startFakeNATS(t, fakeINFO(""))
	n := newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.Mode, c.Stream, c.ConnectTimeout = []string{"nats://" + srv.addr}, NATSModeCore, "", 5*time.Second
		c.MaxEventBytes = MaxEventBytes(64 << 10) // fits the fake's 1 MiB
	})
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health with a max_payload that fits: %v", err)
	}
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: a second check against the same server: %v", err)
	}
	// The connect hook (the beacon's) says when the client is back; a check
	// that raced the reconnection would only see it half done.
	back := make(chan struct{}, 4)
	n.OnConnect(func() { back <- struct{}{} })
	<-back // registered while connected: it runs once at once
	// The server comes back with 64 KiB.
	srv.setInfo(strings.Replace(fakeINFO(""), `"max_payload":1048576`, `"max_payload":65536`, 1))
	srv.drop()
	select {
	case <-back:
	case <-ctx.Done():
		t.Fatal("the client never reconnected")
	}
	if err := n.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "max_payload=65536") {
		t.Fatalf("Health after reconnecting to a smaller max_payload = %v, want fatal naming it", err)
	}
}
