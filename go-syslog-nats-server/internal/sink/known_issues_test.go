package sink

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Regression tests from the port of the RP/RN known-issues round (2026-10-14,
// SPEC §21.1).

// A failed dial of a reconnection is not logged (SPEC §10.7): every one of a
// long outage as an ERROR, one every 30 s, repeated what the disconnection
// WARN, /readyz and syslog_sink_up already say. Only the verdict goes to
// ERROR. The form of go-relplog-server and go-syslog-server.
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
	// Control: the refusal of a server that demands TLS, with no other server
	// to go to, is the verdict, and it is logged.
	n2 := fakeSink(t, srv, NATSModeCore, &buf, nil)
	n2.onReconnectErr(nil, errNATSServerRequiresTLS)
	if v := n2.verdict.Load(); v == nil || !errors.Is(*v, errNATSServerRequiresTLS) {
		t.Fatal("the TLS refusal of the sole server set no verdict")
	}
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatal("the TLS verdict was not logged")
	}
}

// A server that demands TLS is a verdict only with no other server to go to
// (soleServer, SPEC §10.7), as in go-relplog-server and go-syslog-server. With
// a pool -- several sink.nats.urls, or servers a cluster announced -- the
// client cycles through them while the one that serves is down, LastError
// holds the TLS refusal of the other, and Health read it as the permanent
// verdict: FATAL, a dump and exit 3, though the good server came back a moment
// later. With a pool it stays network, and Health names the refusal.
func TestNATSOneTLSServerOfAPoolIsNoVerdict(t *testing.T) {
	good := startNATSFake(t, natsFakeINFO(""))
	tlsOnly := startNATSFake(t, natsFakeINFO(`,"tls_required":true`))
	n := fakeSink(t, good, NATSModeCore, nil, func(c *NATSConfig) {
		c.URLs = []string{"nats://" + good.addr, "nats://" + tlsOnly.addr}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health with the good server up = %v", err)
	}
	if got := len(n.nc.Servers()); got != 2 {
		t.Fatalf("the client's pool has %d servers, want 2", got)
	}
	// The ReconnectErrCB path: a pool is never a verdict there either.
	n.onReconnectErr(n.nc, errNATSServerRequiresTLS)
	if v := n.verdict.Load(); v != nil {
		t.Fatalf("a TLS refusal reported with a pool of 2 set a verdict: %v", *v)
	}

	good.stop()
	sawTLS := false
	for start := time.Now(); time.Since(start) < 4*time.Second; time.Sleep(50 * time.Millisecond) {
		hctx, hcancel := context.WithTimeout(ctx, time.Second)
		err := n.Health(hctx)
		hcancel()
		if IsFatal(err) {
			t.Fatalf("Health while the good server is down = %v: one TLS-only server of a pool made a verdict", err)
		}
		if err != nil && errors.Is(err, errNATSServerRequiresTLS) {
			sawTLS = true
		}
	}
	if !sawTLS {
		t.Fatal("Health never named the TLS refusal as the reason the sink is DOWN")
	}
	good.restart(t)
	waitConnected(t, n)
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health after the good server came back = %v, want UP", err)
	}
	if v := n.verdict.Load(); v != nil {
		t.Fatalf("a verdict after the good server came back: %v", *v)
	}
}

// At the boot, nats.Connect tries every URL and returns the last one's error:
// a pool whose serving server is down and whose other server demands TLS waits
// out sink.boot_timeout (network, naming the TLS) instead of exiting 3 at once.
// The single-server refusal stays fatal.
func TestNATSBootTLSRefusalOfAPoolIsNetwork(t *testing.T) {
	tlsOnly := startNATSFake(t, natsFakeINFO(`,"tls_required":true`))
	down := startNATSFake(t, natsFakeINFO(""))
	down.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sawTLS := false
	for i := 0; i < 20 && !sawTLS; i++ { // the pool order is random: the last error may be the down one's
		n := fakeSink(t, tlsOnly, NATSModeCore, nil, func(c *NATSConfig) {
			c.URLs = []string{"nats://" + down.addr, "nats://" + tlsOnly.addr}
			c.ConnectTimeout = time.Second
		})
		err := n.Health(ctx)
		if IsFatal(err) {
			t.Fatalf("boot Health of a pool with one TLS-only server = %v, want network", err)
		}
		sawTLS = errors.Is(err, errNATSServerRequiresTLS)
	}
	if !sawTLS {
		t.Fatal("the boot error never named the TLS refusal")
	}
	// Control: alone, the same server is the verdict at once.
	n := fakeSink(t, tlsOnly, NATSModeCore, nil, nil)
	if err := n.Health(ctx); !IsFatal(err) || !errors.Is(err, errNATSServerRequiresTLS) {
		t.Fatalf("boot Health of the sole TLS-only server = %v, want the fatal TLS verdict", err)
	}
}
