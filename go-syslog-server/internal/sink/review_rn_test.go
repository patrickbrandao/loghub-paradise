package sink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Findings of go-relplog-nats-server's independent review of 2026-10-13,
// ported. Each test failed before its fix, or -- the last five -- kills a
// mutation that suite used to survive. go-relplog-nats-server has no TLS and
// refuses every server that demands it; this server has TLS (sink.nats.tls,
// tls_ca, tls_insecure), so the TLS finding is adapted: the permanent refusal
// here is a certificate that does not verify, or a server that stopped
// offering the TLS this client demands (isTLSRefusal).

// fakeTLSServer is a self-signed certificate for 127.0.0.1, as a server
// configuration for the fake and as a PEM file a client can trust.
func fakeTLSServer(t *testing.T) (conf *tls.Config, caFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fake-nats"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caFile = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion: tls.VersionTLS12}, caFile
}

// A server that, after the boot, starts demanding TLS with a certificate this
// client does not trust: nats.go upgrades on its own (system roots, with
// sink.nats.tls off), the handshake fails, and it never reports that through
// ReconnectErrCB (that is for a failed TCP dial), so Health answered
// "connection is RECONNECTING", network, until the fuses -- or forever. The
// control is the same switch with the certificate's CA configured: the client
// reconnects over TLS and stays healthy, so the verdict is about verification,
// not about TLS.
func TestNATSReconnectToAnUntrustedTLSServerIsAVerdict(t *testing.T) {
	tlsConf, caFile := fakeTLSServer(t)
	for _, mode := range []string{NATSModeCore, NATSModeJetStream} {
		for _, trusted := range []bool{false, true} {
			name := mode + "/untrusted"
			if trusted {
				name = mode + "/trusted (control)"
			}
			t.Run(name, func(t *testing.T) {
				info := `,"tls_available":true`
				if mode == NATSModeJetStream {
					info += `,"jetstream":true`
				}
				srv := startNATSFake(t, natsFakeINFO(info))
				n := fakeSink(t, srv, mode, nil, func(c *NATSConfig) {
					if trusted {
						c.TLSCA = caFile
						c.TLS = true
					}
				})
				if trusted {
					// TLS from the first connection: the fake must speak it.
					srv.set(func(f *natsFake) { f.info = natsFakeINFO(info + `,"tls_required":true`); f.tlsConf = tlsConf })
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if _, _, err := n.connect(); err != nil {
					t.Fatal(err)
				}
				srv.set(func(f *natsFake) { f.info = natsFakeINFO(info + `,"tls_required":true`); f.tlsConf = tlsConf })
				srv.drop()
				if trusted {
					waitConnected(t, n)
					deadline := time.Now().Add(8 * time.Second)
					for n.nc.Stats().Reconnects == 0 || !n.nc.IsConnected() {
						if time.Now().After(deadline) {
							t.Fatal("control: the client did not reconnect to the trusted TLS server")
						}
						time.Sleep(10 * time.Millisecond)
					}
					// The fake has no JetStream API: in jetstream mode the
					// stream info times out, network; never fatal.
					hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
					defer hcancel()
					err := n.Health(hctx)
					if IsFatal(err) || (mode == NATSModeCore && err != nil) {
						t.Fatalf("control: Health after reconnecting to a trusted TLS server = %v", err)
					}
					return
				}
				deadline := time.Now().Add(8 * time.Second)
				var err error
				for time.Now().Before(deadline) {
					if err = n.Health(ctx); IsFatal(err) {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				var cv *tls.CertificateVerificationError
				if !IsFatal(err) || !errors.As(err, &cv) {
					t.Fatalf("Health after reconnecting to an untrusted TLS server = %v (class %s), want the fatal TLS verdict",
						err, ClassOf(err))
				}
				if werr := n.Write(ctx, batchOf(t, line5424)); !IsFatal(werr) {
					t.Fatalf("Write after the verdict = %v, want fatal", werr)
				}
			})
		}
	}
}

// The same refusals at the first CONNECT end the boot at once, fatal, instead
// of waiting out sink.boot_timeout with the network's message: a certificate
// that does not verify, and sink.nats.tls on against a server that offers no
// TLS (ErrSecureConnWanted). tls_insecure is the control: the same server is
// accepted.
func TestNATSBootTLSRefusalIsFatal(t *testing.T) {
	tlsConf, _ := fakeTLSServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := startNATSFake(t, natsFakeINFO(`,"tls_required":true`))
	srv.set(func(f *natsFake) { f.tlsConf = tlsConf })
	n := fakeSink(t, srv, NATSModeCore, nil, func(c *NATSConfig) { c.TLS = true })
	var cv *tls.CertificateVerificationError
	if err := n.Health(ctx); !IsFatal(err) || !errors.As(err, &cv) {
		t.Fatalf("Health against an untrusted certificate = %v (class %s), want fatal", err, ClassOf(err))
	}

	ok := fakeSink(t, srv, NATSModeCore, nil, func(c *NATSConfig) { c.TLS, c.TLSInsecure = true, true })
	if err := ok.Health(ctx); err != nil {
		t.Fatalf("control: tls_insecure against the same server = %v", err)
	}

	plain := startNATSFake(t, natsFakeINFO(""))
	n = fakeSink(t, plain, NATSModeCore, nil, func(c *NATSConfig) { c.TLS = true })
	if err := n.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health with sink.nats.tls against a server without TLS = %v (class %s), want fatal",
			err, ClassOf(err))
	}
}

// A core flush that STARTS while the client reconnects is resolved by the new
// server's PONG, but the chunk's PUBs went down the old socket: it was
// confirmed although nothing says the server received it.
func TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	beforeCoreFlush = func() {
		srv.drop()
		deadline := time.Now().Add(5 * time.Second)
		for n.nc.IsConnected() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	t.Cleanup(func() { beforeCoreFlush = nil })
	err := n.Write(ctx, batchOf(t, line5424))
	if err == nil || ClassOf(err) != ClassNetwork {
		t.Fatalf("a chunk whose flush crossed a reconnection = %v (class %s), want network", err, ClassOf(err))
	}
	// Control: with the connection steady, the same write is confirmed.
	beforeCoreFlush = nil
	waitConnected(t, n)
	if err := n.Write(ctx, batchOf(t, line5424)); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// Core: a denial of the beacon subject that lands between the chunk's PUBs
// and the flush hides an earlier denial of the data path (nats.go keeps only
// the last -ERR). The ErrorHandler is held back, so only writeCore's own
// maskedDenial check can refuse to confirm the chunk.
func TestNATSCoreMaskedDenialIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.replyFor = func(subject string) string {
			return "-ERR 'Permissions Violation for Publish to \"" + subject + "\"'\r\n"
		}
	})
	beforeCoreFlush = func() { _ = n.nc.Publish("loghub._ctl.beacon", []byte("{}")) }
	t.Cleanup(func() { beforeCoreFlush = nil })
	if err := n.Write(ctx, batchOf(t, line5424)); err == nil {
		t.Fatal("a chunk the server denied was confirmed: the beacon's -ERR masked the data path's")
	}
}

// Core Health reads LastError after its flush: a denial of the data path
// becomes the verdict there even with the ErrorHandler late.
func TestNATSCoreHealthReadsTheDenialItsFlushFollows(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.syslog\"'\r\n"
	})
	if err := n.nc.Publish("loghub.default.syslog", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := n.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health after a denied publish = %v, want the fatal verdict", err)
	}
}

// JetStream: a publish that fails after another of the chunk is already in
// the air must taint the chunk (that future still points at its buffer, SPEC
// §15.2). A window of one makes the second publish stall and fail.
func TestNATSPublishErrorAfterTheFirstTaints(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.MaxPending = 1 })
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := n.Write(ctx, batchOf(t, line5424, line5424))
	if err == nil || !taints(err) {
		t.Fatalf("Write = %v, taint=%v; want a tainted error", err, taints(err))
	}
}

// JetStream: a logic refusal in an ack ends the chunk with Delivered, and the
// error taints (SPEC §10.7 leaves the buffer of any failed write to the GC).
func TestNATSLogicRefusalInAnAckTaints(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	srv.set(func(f *natsFake) {
		f.acks = true
		f.ackErr = func(n int) string {
			if n == 2 {
				return `{"error":{"code":400,"err_code":10054,"description":"message size exceeds maximum allowed"}}`
			}
			return ""
		}
	})
	n := fakeSink(t, srv, NATSModeJetStream, nil, nil)
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := n.Write(ctx, batchOf(t, line5424, line5424, line5424))
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassLogic || !taints(err) || len(e.Delivered) != 2 {
		t.Fatalf("Write = %v (%+v), want a tainted logic refusal with 2 delivered", err, e)
	}
}

// WithRetryAttempts(0): a "no responders" answer must not make the library
// publish the message again on its own -- the retry would re-read msg.Data
// after Write returned (SPEC §15.2). Exactly one PUB reaches the server.
func TestNATSLibraryNeverRepublishes(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	srv.set(func(f *natsFake) { f.noResponders = true })
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.OpTimeout = 2 * time.Second })
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before := srv.pubs.Load()
	if err := n.Write(ctx, batchOf(t, line5424)); err == nil {
		t.Fatal("a publish nobody acknowledged was confirmed")
	}
	time.Sleep(700 * time.Millisecond) // the library's retry wait is 250 ms
	if got := srv.pubs.Load() - before; got != 1 {
		t.Fatalf("%d PUBs for one message: the library republished it", got)
	}
}
