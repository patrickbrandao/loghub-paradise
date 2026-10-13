package sink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Findings of go-syslog-server's independent review of 2026-10-13 that hold
// for this server's NATS sink (the same code): the TLS verdict of a pool
// (C1), the store-closing text of nats-server (C2).

// A pool where ONE server's certificate does not verify: connected to the
// trusted one, the trusted one goes down; while the client cycles between the
// two, LastError holds the untrusted one's x509 error, and Health read it as
// the permanent verdict -- FATAL, a dump and exit 3 -- although the trusted
// server came back a moment later. The refusal is only a verdict when no
// other server can serve (soleServer): with a pool it stays network, naming
// the TLS refusal as the reason. The pool may come from sink.nats.urls or
// from the servers the cluster announces (connect_urls).
func TestNATSOneUntrustedServerOfAPoolIsNoVerdict(t *testing.T) {
	trusted, caFile := fakeTLSServer(t)
	untrusted, _ := fakeTLSServer(t) // another self-signed CA: never verifies
	info := natsFakeINFO(`,"tls_required":true`)
	for _, how := range []string{"urls", "announced"} {
		t.Run(how, func(t *testing.T) {
			b := startNATSFake(t, info)
			b.set(func(f *natsFake) { f.tlsConf = untrusted })
			aInfo := info
			if how == "announced" {
				aInfo = natsFakeINFO(fmt.Sprintf(`,"tls_required":true,"connect_urls":[%q]`, b.addr))
			}
			a := startNATSFake(t, aInfo)
			a.set(func(f *natsFake) { f.tlsConf = trusted })
			n := fakeSink(t, a, NATSModeCore, nil, func(c *NATSConfig) {
				c.TLS, c.TLSCA = true, caFile
				if how == "urls" {
					c.URLs = append(c.URLs, "nats://"+b.addr)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := n.Health(ctx); err != nil {
				t.Fatalf("control: Health with the trusted server up = %v", err)
			}
			if got := len(n.nc.Servers()); got != 2 {
				t.Fatalf("the client's pool has %d servers, want 2", got)
			}
			a.set(func(f *natsFake) { f.refuse = true })
			a.drop()
			var cv *tls.CertificateVerificationError
			sawTLS := false
			for start := time.Now(); time.Since(start) < 4*time.Second; time.Sleep(50 * time.Millisecond) {
				err := n.Health(ctx)
				if IsFatal(err) {
					t.Fatalf("Health while the trusted server is down = %v: one untrusted server of a pool made a verdict", err)
				}
				if err != nil && errors.As(err, &cv) {
					sawTLS = true
				}
			}
			if !sawTLS {
				t.Fatal("Health never named the TLS refusal as the reason the sink is DOWN")
			}
			a.set(func(f *natsFake) { f.refuse = false })
			waitConnected(t, n)
			if err := n.Health(ctx); err != nil {
				t.Fatalf("Health after the trusted server came back = %v, want UP", err)
			}
		})
	}
}

// At the boot, nats.Connect tries every URL and returns the last one's error:
// a pool whose trusted server is down and whose other server does not verify
// waits out sink.boot_timeout (network, naming the TLS) instead of exiting at
// once. The single-server refusal stays fatal (TestNATSBootTLSRefusalIsFatal).
func TestNATSBootTLSRefusalOfAPoolIsNetwork(t *testing.T) {
	untrusted, _ := fakeTLSServer(t)
	_, otherCA := fakeTLSServer(t) // the CA the client trusts: not the one that signed bad's
	info := natsFakeINFO(`,"tls_required":true`)
	down := startNATSFake(t, info)
	down.set(func(f *natsFake) { f.refuse = true })
	bad := startNATSFake(t, info)
	bad.set(func(f *natsFake) { f.tlsConf = untrusted })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n := fakeSink(t, down, NATSModeCore, nil, func(c *NATSConfig) {
		c.TLS, c.TLSCA = true, otherCA
		c.URLs = append(c.URLs, "nats://"+bad.addr)
		c.ConnectTimeout = 2 * time.Second
	})
	var cv *tls.CertificateVerificationError
	// nats.go shuffles the pool on every Connect, and the error is the LAST
	// server's: the down one's half of the time. Every attempt must be
	// network; one of them, at least, names the TLS.
	for i := 0; i < 100; i++ {
		err := n.Health(ctx)
		if IsFatal(err) {
			t.Fatalf("boot Health of a pool with one untrusted server = %v, want network", err)
		}
		if errors.As(err, &cv) {
			return
		}
	}
	t.Fatal("the boot error never named the TLS refusal")
}

// nats-server 2.14.6 answers a publish to a stream whose store is closing --
// a stream being deleted or a server shutting down -- with 10077 and its
// ErrStoreClosed text, "store is closed" (server/store.go). The sink matched
// "store closed", a text the server never sends, so a rolling restart of NATS
// was FATAL, a dump and exit 3 (go-syslog-server review, 2026-10-13).
func TestNATSStoreIsClosedIsNetwork(t *testing.T) {
	for _, desc := range []string{"store is closed", "Store Is Closed", "store closed"} {
		err := classifyNATS("publish", &jetstream.APIError{Code: 503, ErrorCode: jsErrStreamStoreFailed, Description: desc})
		if ClassOf(err) != ClassNetwork {
			t.Errorf("10077 %q = %s, want network", desc, ClassOf(err))
		}
	}
	// Control: a store that cannot take the record at all stays fatal.
	err := classifyNATS("publish", &jetstream.APIError{Code: 500, ErrorCode: jsErrStreamStoreFailed, Description: "record too large"})
	if !IsFatal(err) {
		t.Errorf("10077 record too large = %s, want fatal", ClassOf(err))
	}
}
