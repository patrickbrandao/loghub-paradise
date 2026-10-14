package sink

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// Regression tests from the port of the go-relplog-pg-server and
// go-relplog-nats-server known-issues round (2026-10-14, SPEC §22.1).

// A failed dial of a reconnection is not logged (SPEC §10.7): every one of a
// long outage as an ERROR, one every reconnect_wait, repeated what the
// disconnection WARN, /readyz and relplog_sink_up already say. Only the
// verdict goes to ERROR. The code already did this; the test pins the rule.
func TestNATSReconnectDialFailureIsNotLogged(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	var buf natsLogBuf
	n := fakeSink(t, srv, NATSModeCore, &buf, nil)
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	n.onReconnectErr(nil, dial)
	n.onReconnectErr(nil, dial)
	if strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("a failed reconnection dial was logged as an ERROR:\n%s", buf.String())
	}
	if v := n.verdict.Load(); v != nil {
		t.Fatalf("a failed dial set a verdict: %v", *v)
	}
	// Control: the TLS refusal of the sole server (sink.nats.tls against a
	// server without TLS) is the verdict, and it is logged.
	n2 := fakeSink(t, srv, NATSModeCore, &buf, nil)
	n2.onReconnectErr(nil, nats.ErrSecureConnWanted)
	if v := n2.verdict.Load(); v == nil || !errors.Is(*v, nats.ErrSecureConnWanted) {
		t.Fatal("the TLS refusal of the sole server set no verdict")
	}
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatal("the TLS verdict was not logged")
	}
}

// The ReconnectErrCB path of soleServer: with a pool of two servers a TLS
// refusal reported there is no verdict either (SPEC §10.7). The Health path is
// TestNATSOneUntrustedServerOfAPoolIsNoVerdict, with a real nats-server.
func TestNATSReconnectErrOfAPoolIsNoVerdict(t *testing.T) {
	a := startNATSFake(t, natsFakeINFO(""))
	b := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, a, NATSModeCore, nil, func(c *NATSConfig) {
		c.URLs = []string{"nats://" + a.addr, "nats://" + b.addr}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	if got := len(n.nc.Servers()); got != 2 {
		t.Fatalf("the client's pool has %d servers, want 2", got)
	}
	n.onReconnectErr(n.nc, nats.ErrSecureConnWanted)
	if v := n.verdict.Load(); v != nil {
		t.Fatalf("a TLS refusal reported with a pool of 2 set a verdict: %v", *v)
	}
}

// A network error of a write the caller's own cancellation cut short -- a
// SIGTERM during the boot's reingestion or backlog drain -- is not a sink
// failure and is not counted (SPEC §14.1), as in the workers. writeChunk
// counted it before Deliver looked at the context. A fatal refusal during the
// same cancellation is still counted: it is a verdict, not a consequence.
func TestDeliverCancellationIsNotANetworkError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &hookSink{write: func(context.Context, *Batch) error {
		cancel() // the signal lands during the write
		return networkErr("insert", context.Canceled)
	}}
	m := metrics()
	d := NewInstrumented(fs, nil, workerCfg(4), discard(), m)
	if err := d.Deliver(ctx, itemsOf(lines(2)...), time.Second, func(int, error) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver = %v, want the cancellation", err)
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 0 {
		t.Fatalf("kind=network counted %d for a write the cancellation cut short", got)
	}
	// Control: the same error without a cancellation is counted, once per attempt.
	m = metrics()
	stop := errors.New("give up")
	fs = &hookSink{write: func(context.Context, *Batch) error { return networkErr("insert", errors.New("reset")) }}
	d = NewInstrumented(fs, nil, workerCfg(4), discard(), m)
	if err := d.Deliver(context.Background(), itemsOf(lines(2)...), time.Second, func(int, error) error { return stop }); err != stop {
		t.Fatalf("Deliver = %v, want the wait's error", err)
	}
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 1 {
		t.Fatalf("kind=network counted %d, want 1", got)
	}
	// Control: a fatal refusal during a cancellation is counted.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	m = metrics()
	fs = &hookSink{write: func(context.Context, *Batch) error {
		cancel()
		return fatalErr("insert", errors.New("permission denied"))
	}}
	d = NewInstrumented(fs, nil, workerCfg(4), discard(), m)
	_ = d.Deliver(ctx, itemsOf(lines(2)...), time.Second, func(int, error) error { return nil })
	if got := m.SinkErrorsTotal[obs.SinkErrFatal].Load(); got != 1 {
		t.Fatalf("kind=fatal counted %d during a cancellation, want 1", got)
	}
}
