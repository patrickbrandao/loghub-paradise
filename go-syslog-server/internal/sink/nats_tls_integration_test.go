package sink

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// sink.nats.tls against a REAL nats-server with tls {} (SPEC §10.7), which the
// natsFake of review_rn_test.go cannot be: a pool of servers that the client
// cycles through, each with its own certificate. The suite starts its own
// servers from the binary named by SYSLOG_TEST_NATS_SERVER_BIN (2.14.6), on
// ports 14700-14799, and skips without it:
//
//	go build -o /tmp/nats-server github.com/nats-io/nats-server/v2@v2.14.6
//	SYSLOG_TEST_NATS_SERVER_BIN=/tmp/nats-server go test -race -run TLSPool ./internal/sink/

const natsServerBinEnv = "SYSLOG_TEST_NATS_SERVER_BIN"

// natsTLSPKI is two certificate authorities, each with a server certificate
// for 127.0.0.1: "good" is the one the client trusts (caFile), "other" one it
// does not -- a node of the pool with a certificate of another CA.
type natsTLSPKI struct {
	caFile              string
	goodCert, goodKey   string
	otherCert, otherKey string
}

func newNATSTLSPKI(t *testing.T) *natsTLSPKI {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	issue := func(prefix string) (caPath, certPath, keyPath string) {
		caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		caTmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: prefix + "-ca"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true, IsCA: true,
		}
		caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		ca, _ := x509.ParseCertificate(caDER)
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: prefix + "-nats"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return write(prefix+"-ca.pem", "CERTIFICATE", caDER), write(prefix+".pem", "CERTIFICATE", der),
			write(prefix+".key", "EC PRIVATE KEY", kb)
	}
	p := &natsTLSPKI{}
	p.caFile, p.goodCert, p.goodKey = issue("good")
	_, p.otherCert, p.otherKey = issue("other")
	return p
}

// natsTLSPort is a free port in 14700-14799, the range this suite owns.
func natsTLSPort(t *testing.T) int {
	t.Helper()
	for p := 14700 + int(time.Now().UnixNano()%100); ; {
		for i := 0; i < 100; i++ {
			port := 14700 + (p-14700+i)%100
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			if err == nil {
				ln.Close()
				return port
			}
		}
		t.Fatal("no free port in 14700-14799")
	}
}

// natsTLSServer starts a nats-server with tls {} on port (JetStream in its own
// store when js) and returns its stop function, also registered as cleanup.
func natsTLSServer(t *testing.T, port int, cert, key string, js bool) func() {
	t.Helper()
	bin := os.Getenv(natsServerBinEnv)
	if bin == "" {
		t.Skipf("set %s to a nats-server 2.14.6 binary to run the TLS pool tests", natsServerBinEnv)
	}
	dir := t.TempDir()
	conf := fmt.Sprintf("listen: 127.0.0.1:%d\ntls { cert_file: %q, key_file: %q }\n", port, cert, key)
	if js {
		conf += fmt.Sprintf("jetstream { store_dir: %q }\n", filepath.Join(dir, "js"))
	}
	cf := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(cf, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", cf)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			return stop
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("nats-server did not start")
	return stop
}

func natsTLSSink(t *testing.T, urls []string, ca string, mutate func(*NATSConfig)) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: urls, TLS: true, TLSCA: ca, Mode: NATSModeCore, Workspace: "default",
		ConnectTimeout: 2 * time.Second, OpTimeout: 3 * time.Second, MaxEventBytes: 64 << 10}
	if mutate != nil {
		mutate(&cfg)
	}
	n, err := NewNATS(cfg, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// SPEC §10.7: a TLS refusal is a verdict only when no other server can serve.
// Pool A (trusted) + B (a certificate of another CA): connected to A, A
// restarts; while the client cycles between B (the TLS refusal) and A
// (refused), LastError holds B's x509 error, and Health set it as the
// permanent verdict -- FATAL, a dump and exit 3 -- although A came back
// within seconds (independent review of the port, 2026-10-13: 5 of 5 before
// the fix). Network now; A back, the sink is healthy.
func TestNATSTLSPoolOneUntrustedNodeIsNotAVerdict(t *testing.T) {
	pki := newNATSTLSPKI(t)
	pa, pb := natsTLSPort(t), 0
	stopA := natsTLSServer(t, pa, pki.goodCert, pki.goodKey, false)
	pb = natsTLSPort(t)
	natsTLSServer(t, pb, pki.otherCert, pki.otherKey, false)
	urlA, urlB := fmt.Sprintf("nats://127.0.0.1:%d", pa), fmt.Sprintf("nats://127.0.0.1:%d", pb)
	n := natsTLSSink(t, []string{urlA, urlB}, pki.caFile, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("boot Health with one trusted node: %v", err)
	}
	if got := n.ServerAddr(); got != fmt.Sprintf("127.0.0.1:%d", pa) {
		t.Fatalf("connected to %s, want A", got)
	}

	stopA()
	sawTLS := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		hctx, hcancel := context.WithTimeout(ctx, time.Second)
		err := n.Health(hctx)
		hcancel()
		if IsFatal(err) {
			t.Fatalf("Health while A restarts, B untrusted = %v: a permanent verdict for a pool that still has A", err)
		}
		if err != nil && strings.Contains(err.Error(), "refused TLS") {
			sawTLS = true
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawTLS {
		t.Log("no health check saw B's TLS refusal in LastError (the client was between servers each time)")
	}
	natsTLSServer(t, pa, pki.goodCert, pki.goodKey, false)
	waitConnected(t, n)
	var err error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if err = n.Health(ctx); err == nil || IsFatal(err) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Health with A back = %v (class %s)", err, ClassOf(err))
	}
	if err := n.Write(ctx, batchOf(t, line5424)); err != nil {
		t.Fatalf("Write with A back: %v", err)
	}
}

// The same rule at the first connect: A down for a moment, B untrusted. The
// connect tries both and returns the last error -- B's x509 in about half of
// the orders the client shuffles -- and the boot ended FATAL at once instead
// of waiting sink.boot_timeout for A. Ten connects make the shuffled order
// irrelevant. The control: B alone is still the immediate verdict.
func TestNATSTLSPoolBootWaitsForATrustedNode(t *testing.T) {
	pki := newNATSTLSPKI(t)
	pa, pb := natsTLSPort(t), 0
	stopA := natsTLSServer(t, pa, pki.goodCert, pki.goodKey, false)
	pb = natsTLSPort(t) // while A holds its port
	natsTLSServer(t, pb, pki.otherCert, pki.otherKey, false)
	stopA()
	urlA, urlB := fmt.Sprintf("nats://127.0.0.1:%d", pa), fmt.Sprintf("nats://127.0.0.1:%d", pb)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	n := natsTLSSink(t, []string{urlA, urlB}, pki.caFile, nil)
	for i := 0; i < 10; i++ {
		err := n.Health(ctx)
		if err == nil || IsFatal(err) {
			t.Fatalf("connect %d with A down and B untrusted = %v (class %s), want network", i, err, ClassOf(err))
		}
	}
	natsTLSServer(t, pa, pki.goodCert, pki.goodKey, false)
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health with A up = %v", err)
	}

	// Control: B is the only server, and the refusal is the verdict.
	alone := natsTLSSink(t, []string{urlB}, pki.caFile, nil)
	if err := alone.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health against B alone = %v (class %s), want the fatal TLS verdict", err, ClassOf(err))
	}
}

// Control on the real server: one trusted TLS server, in jetstream mode,
// restarts. The outage is network, never a verdict, and the sink is healthy
// and writes when it is back.
func TestNATSTLSPoolTrustedServerSurvivesARestart(t *testing.T) {
	pki := newNATSTLSPKI(t)
	port := natsTLSPort(t)
	stop := natsTLSServer(t, port, pki.goodCert, pki.goodKey, true)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)

	admin := natsTLSSink(t, []string{url}, pki.caFile, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	nc, _, err := admin.connect()
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	space, stream := fmt.Sprintf("tls%d", time.Now().UnixNano()), fmt.Sprintf("SLS_TLS_%d", time.Now().UnixNano())
	if _, err := js.CreateStream(ctx, natsStreamConfigFor(t, stream, space)); err != nil {
		t.Fatal(err)
	}

	n := natsTLSSink(t, []string{url}, pki.caFile, func(c *NATSConfig) {
		c.Mode, c.Stream, c.Workspace = NATSModeJetStream, stream, space
	})
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if err := n.Write(ctx, batchFrom(t, newItemIn(space, line5424))); err != nil {
		t.Fatalf("Write: %v", err)
	}
	stop()
	for i := 0; i < 10; i++ {
		hctx, hcancel := context.WithTimeout(ctx, time.Second)
		err := n.Health(hctx)
		hcancel()
		if IsFatal(err) {
			t.Fatalf("Health during the outage of a trusted server = %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The store is a new temporary directory: the stream comes back empty and
	// must be created again before the sink can write.
	natsTLSServer(t, port, pki.goodCert, pki.goodKey, true)
	waitConnected(t, n)
	nc2, _, err := natsTLSSink(t, []string{url}, pki.caFile, nil).connect()
	if err != nil {
		t.Fatal(err)
	}
	js2, err := jetstream.New(nc2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js2.CreateStream(ctx, natsStreamConfigFor(t, stream, space)); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(15 * time.Second); ; {
		hctx, hcancel := context.WithTimeout(ctx, 2*time.Second)
		err = n.Health(hctx)
		hcancel()
		if err == nil || IsFatal(err) || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Health after the restart = %v (class %s)", err, ClassOf(err))
	}
	if err := n.Write(ctx, batchFrom(t, newItemIn(space, line5424))); err != nil {
		t.Fatalf("Write after the restart: %v", err)
	}
}
