package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

// Nothing in this file needs a server. The integration half lives in
// nats_integration_test.go and skips without SYSLOG_TEST_NATS_URL.

// SPEC §10.8.6 is the table; this is the table read back.
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
		{"api stream not match", api(400, jsErrStreamNotMatch), ClassFatal},
		{"api sealed", api(400, jsErrStreamSealed), ClassFatal},
		{"api insufficient resources", api(503, jsErrInsufficientResources), ClassFatal},
		{"api store failed", api(503, jsErrStreamStoreFailed), ClassFatal},
		{"api stream not found by code", api(404, jetstream.JSErrCodeStreamNotFound), ClassFatal},
		{"api unknown 4xx", api(400, 10999), ClassFatal},

		{"max payload", nats.ErrMaxPayload, ClassLogic},
		{"max bytes", jetstream.ErrMaxBytesExceeded, ClassLogic},
		{"api message too big", api(400, jsErrMessageExceedsMaximum), ClassLogic},
		{"api header too big", api(400, jsErrHeaderExceedsMaximum), ClassLogic},

		// SPEC §10.8.6: 4xx that waiting resolves are network, not the fatal
		// of an unknown 4xx.
		{"api duplicate in process (409)", api(409, jsErrDuplicateInProcess), ClassNetwork},
		{"api stream too many requests (429)", api(429, jsErrStreamTooManyRequests), ClassNetwork},
		{"api unknown 429", api(429, 10997), ClassNetwork},
		{"api unknown 409", api(409, 10996), ClassFatal},

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

// SPEC §3.8: id, version and envelope size in headers, and Data the envelope
// followed by the raw line, byte for byte. Nothing of the envelope in a header:
// JetStream refuses a header block above 64 KiB.
func TestNATSMessageShape(t *testing.T) {
	n := &NATS{cfg: NATSConfig{Subject: "syslog.in"}}
	line := "<165>1 - web-01 nginx - - - A\x00B\xff\n"
	b := prepared(t, line)
	data := append(append([]byte{}, b[0].Env...), line...)

	msg := n.message(&b[0], data)
	if msg.Subject != "syslog.in" {
		t.Errorf("subject = %q", msg.Subject)
	}
	if got := msg.Header.Get(jetstream.MsgIDHeader); got != b[0].Item.Head.ID {
		t.Errorf("Nats-Msg-Id = %q, want the message id", got)
	}
	if got := msg.Header.Get(natsHeaderVersion); got != envelope.Version {
		t.Errorf("Sls-Version = %q, want %q", got, envelope.Version)
	}
	size, err := strconv.Atoi(msg.Header.Get(natsHeaderEnvelopeSize))
	if err != nil || size != len(b[0].Env) {
		t.Fatalf("Sls-Envelope-Size = %q, want %d", msg.Header.Get(natsHeaderEnvelopeSize), len(b[0].Env))
	}
	if !bytes.Equal(msg.Data[:size], b[0].Env) || !bytes.Equal(msg.Data[size:], []byte(line)) {
		t.Errorf("Data = %q, want the envelope then the raw line", msg.Data)
	}
	if len(msg.Header) != 3 {
		t.Errorf("headers = %v: the envelope must not travel in one", msg.Header)
	}
}

func TestNewNATSIsLazy(t *testing.T) {
	p, err := NewNATS(NATSConfig{
		URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Subject: "s.in",
		ConnectTimeout: 500 * time.Millisecond, OpTimeout: time.Second,
	}, discard(), metrics())
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
	if err := p.Write(ctx, batchOf(t, 1)); !errors.Is(err, ErrClosed) {
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
	base := NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S", Subject: "s.in"}

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
}

// SPEC §17.5: the nats-init of the compose files creates the stream with the
// nats-box CLI, so its version is part of the recipe. With latest the same
// recipe could install different CLIs on different days (§21.1 item 19).
func TestComposeNATSBoxIsPinned(t *testing.T) {
	files, err := filepath.Glob("../../docker/*nats*.yml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no nats compose files found: %v", err)
	}
	files = append(files, "../../docker/.env.example")
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if !strings.Contains(line, "natsio/nats-box") {
				continue
			}
			if !strings.Contains(line, "natsio/nats-box:0.20.0") {
				t.Errorf("%s:%d: nats-box is not pinned to 0.20.0: %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}
