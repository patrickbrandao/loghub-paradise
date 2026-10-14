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
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// Porte das correções da família syslog/RELP (2026-10-13), the NATS sink:
// the fixes of go-relplog-server, go-relplog-nats-server and go-syslog-server
// (SPEC §8.7) that hold for this server, each test failing before its fix.
// Every test here runs against a real nats-server, and skips without it:
//
//	WHS_TEST_NATS_URL       JetStream (the suite's own, nats_integration_test.go)
//	WHS_TEST_NATS_PERM_URL  JetStream with the users of natsPermSetup
//	WHS_TEST_NATS_TLS_URL   a listener with tls {} whose certificate is
//	WHS_TEST_NATS_TLS_CA    this PEM (self-signed, CN/SAN 127.0.0.1)
//	WHS_TEST_NATS_SERVER    the nats-server binary, for the servers a test
//	                        starts, stops and restarts on its own port
//
// (docs/TESTING.md §5.4).

// ------------------------------------------------- 10077 of a full stream

// A stream with discard=new that is full answers 10077 with the store's own
// words, "maximum messages exceeded": backpressure that clears when a
// consumer acknowledges or the limits move, not a store that can never take
// the message. It was fatal -- a dump and exit 3, and the stored part of the
// batch duplicated at reingestion. Against the real server, so the text the
// classification matches is the one nats-server 2.14.6 sends.
func TestNATSFullDiscardNewStreamIsNetwork(t *testing.T) {
	f := natsSetup(t)
	stream, workspace := f.newStream(func(c *jetstream.StreamConfig) {
		c.MaxMsgs, c.Discard = 1, jetstream.DiscardNew
	})
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, workspace
	p := f.sink(cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, []*envelope.Item{itemIn(workspace, testRequest("/a", []byte("a")))}); err != nil {
		t.Fatalf("control: the first message into an empty stream: %v", err)
	}
	err := p.Write(ctx, []*envelope.Item{itemIn(workspace, testRequest("/b", []byte("b")))})
	if err == nil {
		t.Fatal("a message past max_msgs with discard=new was confirmed")
	}
	if ClassOf(err) != ClassNetwork || !strings.Contains(err.Error(), "maximum messages exceeded") {
		t.Fatalf("Write into a full discard=new stream = %v (class %s), want network naming the store's refusal",
			err, ClassOf(err))
	}
}

// ------------------------------------------- retention and per-subject cap

// retention=interest acknowledges and keeps nothing while no consumer is
// interested, so a confirmed event could be lost: refused, fatal (decided in
// go-syslog-server, docs/DECISOES.md). workqueue and limits are the controls.
// max_msgs_per_subject is only warned about, once per value, though validate
// runs on every check.
func TestNATSValidateRetentionAndPerSubjectCap(t *testing.T) {
	ok := jetstream.StreamConfig{Name: "S", Subjects: []string{natsSubject("default")}}
	n := newTestNATS(t, nil)
	for _, r := range []jetstream.RetentionPolicy{jetstream.LimitsPolicy, jetstream.WorkQueuePolicy} {
		cfg := ok
		cfg.Retention = r
		if err := n.validate(1<<20, &jetstream.StreamInfo{Config: cfg}); err != nil {
			t.Errorf("retention=%s refused: %v", r, err)
		}
	}
	interest := ok
	interest.Retention = jetstream.InterestPolicy
	if err := n.validate(1<<20, &jetstream.StreamInfo{Config: interest}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "retention=interest") {
		t.Errorf("retention=interest = %v, want fatal naming it", err)
	}

	var buf syncBuffer
	w, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "S",
		Subject: "loghub.{space}.{type}", Workspace: "default"}, slog.New(slog.NewTextHandler(&buf, nil)), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	check := func(perSubject int64) {
		t.Helper()
		cfg := ok
		cfg.MaxMsgsPerSubject = perSubject
		if err := w.validate(1<<20, &jetstream.StreamInfo{Config: cfg}); err != nil {
			t.Fatalf("max_msgs_per_subject=%d refused: %v", perSubject, err)
		}
	}
	warnings := func() int { return strings.Count(buf.String(), "max_msgs_per_subject=") }
	check(-1)
	check(0)
	if warnings() != 0 {
		t.Fatalf("a warning without a per-subject cap:\n%s", buf.String())
	}
	check(10)
	check(10)
	check(10)
	if warnings() != 1 {
		t.Fatalf("%d warnings for one value over three checks, want 1:\n%s", warnings(), buf.String())
	}
	check(20)
	check(-1)
	check(20)
	if warnings() != 3 {
		t.Fatalf("%d warnings, want 3 (10, 20, and 20 again after it was lifted):\n%s", warnings(), buf.String())
	}
}

// Against a real server: a stream with retention=interest and no consumer
// acknowledges a publish and keeps nothing -- shown here, so the refusal is
// about a real loss -- and Health refuses it, fatal.
func TestNATSInterestStreamIsRefused(t *testing.T) {
	f := natsSetup(t)
	stream, workspace := f.newStream(func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.Publish(ctx, natsSubject(workspace), []byte("{}")); err != nil {
		t.Fatalf("control: the interest stream refused a publish: %v", err)
	}
	if n := f.count(stream); n != 0 {
		t.Fatalf("control: the interest stream kept %d messages with no consumer", n)
	}
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, workspace
	if err := f.sink(cfg).Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "retention=interest") {
		t.Fatalf("Health of an interest stream = %v, want fatal naming it", err)
	}
}

// ------------------------------------------------------------ permissions

// natsPermSetup addresses a JetStream server whose users have limited
// permissions, by WHS_TEST_NATS_PERM_URL (no user part), and creates a stream
// for a fresh space as its admin, on loghub.<space>.webhook. The server's
// configuration:
//
//	jetstream { store_dir: "/tmp/nats-perm" }
//	authorization { users = [
//	  { user: admin, password: admin }
//	  { user: nopub, password: nopub, permissions: {
//	      publish: { allow: [">"], deny: ["loghub.*.webhook"] },
//	      subscribe: { allow: [">"] } } }
//	  { user: noinbox, password: noinbox, permissions: {
//	      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
//	] }
func natsPermSetup(t *testing.T) (url, space, stream string) {
	t.Helper()
	url = strings.TrimSpace(os.Getenv("WHS_TEST_NATS_PERM_URL"))
	if url == "" {
		t.Skip("set WHS_TEST_NATS_PERM_URL to a JetStream server with the users of natsPermSetup")
	}
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin"), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("WHS_TEST_NATS_PERM_URL is not reachable as admin: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	n := time.Now().UnixNano()
	space, stream = fmt.Sprintf("p%d", n), fmt.Sprintf("WHS_PERM_%d", n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, natsStreamConfigFor(t, stream, "loghub."+space+".webhook")); err != nil {
		t.Fatalf("creating stream %s: %v", stream, err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), stream) })
	return url, space, stream
}

// permSink publishes on loghub.<space>.webhook whatever the envelope's type,
// so the deny rule of nopub matches in every module of the family.
func permSink(t *testing.T, url, user, space, stream string, opTimeout time.Duration) *NATS {
	t.Helper()
	p, err := NewNATS(NATSConfig{URLs: []string{url}, Username: user, Password: user, Stream: stream,
		Subject: "loghub.{space}.webhook", Workspace: space, ConnectTimeout: 5 * time.Second,
		OpTimeout: opTimeout}, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// A role denied the data subject, or the _INBOX the acks and the API's
// answers come back on, is a permanent verdict (SPEC §8.7.2). The denial
// reaches the client only as an asynchronous -ERR; the publish itself never
// gets an ack and times out, and the stream info times out the same way:
// network. Read back from the connection's last error as soon as that round
// trip fails (escalate), it is fatal at once, from the very Write or Health
// that met it -- it was network there, retried, with the sink UP until a later
// check.
func TestNATSPermissionDeniedIsFatal(t *testing.T) {
	url, space, stream := natsPermSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The ack's own timeout (sink.op_timeout) ends the wait, or the worker's
	// deadline does first: the verdict either way.
	for name, op := range map[string]time.Duration{"publish denied": time.Second, "publish denied, the deadline first": time.Minute} {
		t.Run(name, func(t *testing.T) {
			p := permSink(t, url, "nopub", space, stream, op)
			if err := p.Health(ctx); err != nil {
				t.Fatalf("control: Health before any publish: %v", err)
			}
			wctx, wcancel := context.WithTimeout(ctx, 3*time.Second)
			defer wcancel()
			if err := p.Write(wctx, []*envelope.Item{itemIn(space, testRequest("/p", []byte("p")))}); !IsFatal(err) ||
				!strings.Contains(err.Error(), "ermissions") {
				t.Fatalf("Write of a denied publish = %v (class %s), want fatal naming the denial", err, ClassOf(err))
			}
			if err := p.Health(ctx); !IsFatal(err) {
				t.Fatalf("Health after the denial = %v, want fatal", err)
			}
		})
	}
	t.Run("inbox subscription denied", func(t *testing.T) {
		p := permSink(t, url, "noinbox", space, stream, time.Second)
		hctx, hcancel := context.WithTimeout(ctx, 3*time.Second)
		defer hcancel()
		if err := p.Health(hctx); !IsFatal(err) || !strings.Contains(err.Error(), "_INBOX") {
			t.Fatalf("Health with the ack inbox denied = %v (class %s), want fatal naming _INBOX", err, ClassOf(err))
		}
	})
	t.Run("admin (control)", func(t *testing.T) {
		p := permSink(t, url, "admin", space, stream, time.Second)
		if err := p.Health(ctx); err != nil {
			t.Fatalf("Health as admin: %v", err)
		}
		if err := p.Write(ctx, []*envelope.Item{itemIn(space, testRequest("/c", []byte("c")))}); err != nil {
			t.Fatalf("Write as admin: %v", err)
		}
	})
}

// -------------------------------------------------------------------- TLS

// natsTLSCert writes a fresh self-signed certificate for 127.0.0.1 and its
// key, PEM: the certificate is its own CA, so caFile == certFile. Two calls
// give two authorities that do not trust each other.
func natsTLSCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "whs-test-nats"},
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
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// natsProc is a nats-server this test owns, by WHS_TEST_NATS_SERVER, on a port
// of its own that survives a stop and a start: a client reconnects to the
// same address, and finds there whatever configuration the test gave the
// next life.
type natsProc struct {
	t    *testing.T
	bin  string
	port int
	cmd  *exec.Cmd
}

func natsServerBin(t *testing.T) string {
	t.Helper()
	bin := strings.TrimSpace(os.Getenv("WHS_TEST_NATS_SERVER"))
	if bin == "" {
		t.Skip("set WHS_TEST_NATS_SERVER to a nats-server binary to run the tests that start their own servers")
	}
	return bin
}

// freePort is a port nobody listens on right now.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newNATSProc(t *testing.T, port int) *natsProc {
	t.Helper()
	p := &natsProc{t: t, bin: natsServerBin(t), port: port}
	t.Cleanup(p.stop)
	return p
}

func (p *natsProc) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", p.port) }

// start runs a life of the server with conf after its listen line, and waits
// until the port accepts.
func (p *natsProc) start(conf string) {
	p.t.Helper()
	p.stop()
	file := filepath.Join(p.t.TempDir(), "nats.conf")
	body := fmt.Sprintf("listen: 127.0.0.1:%d\n%s\n", p.port, conf)
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		p.t.Fatal(err)
	}
	p.cmd = exec.Command(p.bin, "-c", file)
	if err := p.cmd.Start(); err != nil {
		p.t.Fatalf("starting nats-server: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p.port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("nats-server on %d never accepted: %v", p.port, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *natsProc) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	p.cmd = nil
}

func natsTLSConf(certFile, keyFile string) string {
	return fmt.Sprintf("tls { cert_file: %q, key_file: %q }", certFile, keyFile)
}

// natsWaitConnected waits for the sink's connection to be up again.
func natsWaitConnected(t *testing.T, n *NATS) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		nc := n.nc
		n.mu.Unlock()
		if nc != nil && nc.IsConnected() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the client did not reconnect")
}

// natsTLSSink is a sink with sink.nats.tls on, trusting caFile.
func natsTLSSink(t *testing.T, caFile string, urls ...string) *NATS {
	t.Helper()
	return newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.TLS, c.TLSCA = urls, true, caFile
		c.ConnectTimeout, c.OpTimeout = 2*time.Second, 2*time.Second
	})
}

// At the first CONNECT a TLS that can never succeed ends the boot at once,
// fatal, instead of waiting out sink.boot_timeout with the network's message:
// a certificate that does not verify (against the shared listener of
// WHS_TEST_NATS_TLS_URL, its CA not configured) and sink.nats.tls on against a
// server that offers no TLS (ErrSecureConnWanted, against WHS_TEST_NATS_URL).
// The controls connect to the same TLS listener with its CA, and with
// tls_insecure.
func TestNATSBootTLSRefusalIsFatal(t *testing.T) {
	tlsURL, ca := os.Getenv("WHS_TEST_NATS_TLS_URL"), os.Getenv("WHS_TEST_NATS_TLS_CA")
	if tlsURL == "" || ca == "" {
		t.Skip("set WHS_TEST_NATS_TLS_URL and WHS_TEST_NATS_TLS_CA to a nats-server listener with tls {} and its CA")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n := natsTLSSink(t, "", tlsURL)
	began := time.Now()
	var cv *tls.CertificateVerificationError
	if err := n.Health(ctx); !IsFatal(err) || !errors.As(err, &cv) {
		t.Fatalf("Health against an untrusted certificate = %v (class %s), want fatal", err, ClassOf(err))
	}
	if took := time.Since(began); took > 3*time.Second {
		t.Errorf("the refusal took %s: it must not wait for a timeout", took)
	}
	for name, c := range map[string]*NATS{
		"tls_ca":       natsTLSSink(t, ca, tlsURL),
		"tls_insecure": newTestNATS(t, func(c *NATSConfig) { c.URLs, c.TLS, c.TLSInsecure = []string{tlsURL}, true, true }),
	} {
		if _, _, err := c.connect(); err != nil {
			t.Errorf("control %s: connect to the same listener = %v", name, err)
		}
	}

	if plain := os.Getenv(natsURLEnv); plain != "" {
		n := natsTLSSink(t, ca, plain)
		if err := n.Health(ctx); !IsFatal(err) || !errors.Is(err, nats.ErrSecureConnWanted) {
			t.Fatalf("Health with sink.nats.tls against a server without TLS = %v (class %s), want fatal",
				err, ClassOf(err))
		}
	}
}

// A server that, after the boot, starts demanding TLS with a certificate this
// client does not trust: nats.go upgrades on its own (system roots, with
// sink.nats.tls off), the handshake fails, and it never reports that through
// ReconnectErrCB (that is for a failed TCP dial), so Health answered
// "connection is RECONNECTING", network, until the fuses -- or forever. Read
// from LastError, with no other server to go to, it is the verdict. The
// control reconnects to a server whose CA the client trusts: the verdict is
// about verification, not about TLS.
func TestNATSReconnectToAnUntrustedTLSServerIsAVerdict(t *testing.T) {
	cert, key := natsTLSCert(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("untrusted", func(t *testing.T) {
		srv := newNATSProc(t, freePort(t))
		srv.start("")
		n := newTestNATS(t, func(c *NATSConfig) { c.URLs, c.ConnectTimeout = []string{srv.url()}, 2*time.Second })
		if _, _, err := n.connect(); err != nil {
			t.Fatal(err)
		}
		srv.start(natsTLSConf(cert, key))
		deadline := time.Now().Add(10 * time.Second)
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
		if werr := n.Write(ctx, mkBatch(1)); !IsFatal(werr) {
			t.Fatalf("Write after the verdict = %v, want fatal", werr)
		}
	})
	t.Run("trusted (control)", func(t *testing.T) {
		srv := newNATSProc(t, freePort(t))
		srv.start(natsTLSConf(cert, key))
		n := natsTLSSink(t, cert, srv.url())
		if _, _, err := n.connect(); err != nil {
			t.Fatal(err)
		}
		srv.start(natsTLSConf(cert, key))
		natsWaitConnected(t, n)
		if v := n.verdict.Load(); v != nil {
			t.Fatalf("a verdict after reconnecting to a trusted TLS server: %v", *v)
		}
	})
}

// A pool where ONE server's certificate does not verify: connected to the
// trusted one, the trusted one goes down; while the client cycles between the
// two, LastError holds the untrusted one's x509 error, and Health read it as
// the permanent verdict -- FATAL, a dump and exit 3 -- although the trusted
// server came back a moment later (go-syslog-server review, 2026-10-13, C1).
// The refusal is only a verdict when no other server can serve (soleServer):
// with a pool it stays network, naming the TLS refusal as the reason. The pool
// comes from sink.nats.urls, or from the servers a cluster announces.
func TestNATSOneUntrustedServerOfAPoolIsNoVerdict(t *testing.T) {
	cert, key := natsTLSCert(t)
	badCert, badKey := natsTLSCert(t) // another authority: never verifies
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run := func(t *testing.T, a *natsProc, aConf string, n *NATS) {
		t.Helper()
		natsWaitConnected(t, n)
		deadline := time.Now().Add(10 * time.Second)
		for len(n.nc.Servers()) != 2 {
			if time.Now().After(deadline) {
				t.Fatalf("the client's pool has %d servers, want 2", len(n.nc.Servers()))
			}
			time.Sleep(20 * time.Millisecond)
		}
		a.stop()
		for n.nc.IsConnected() {
			time.Sleep(time.Millisecond)
		}
		var cv *tls.CertificateVerificationError
		sawTLS := false
		for start := time.Now(); time.Since(start) < 4*time.Second; time.Sleep(50 * time.Millisecond) {
			hctx, hcancel := context.WithTimeout(ctx, time.Second)
			err := n.Health(hctx)
			hcancel()
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
		a.start(aConf)
		natsWaitConnected(t, n)
		if v := n.verdict.Load(); v != nil {
			t.Fatalf("a verdict after the trusted server came back: %v", *v)
		}
	}

	t.Run("urls", func(t *testing.T) {
		b := newNATSProc(t, freePort(t))
		b.start(natsTLSConf(badCert, badKey))
		a := newNATSProc(t, freePort(t))
		aConf := natsTLSConf(cert, key) + fmt.Sprintf("\njetstream { store_dir: %q }", t.TempDir())
		a.start(aConf)
		natsCreateStream(t, a.url(), cert)
		n := natsTLSSink(t, cert, a.url(), b.url())
		if err := n.Health(ctx); err != nil {
			t.Fatalf("control: Health with the trusted server up = %v", err)
		}
		run(t, a, aConf, n)
		if err := n.Health(ctx); err != nil {
			t.Fatalf("Health after the trusted server came back = %v, want UP", err)
		}
	})
	t.Run("announced", func(t *testing.T) {
		ra, rb := freePort(t), freePort(t)
		cluster := func(own, peer int) string {
			return fmt.Sprintf("cluster { name: whs, listen: \"127.0.0.1:%d\", routes: [\"nats-route://127.0.0.1:%d\"] }", own, peer)
		}
		b := newNATSProc(t, freePort(t))
		b.start(natsTLSConf(badCert, badKey) + "\n" + cluster(rb, ra))
		a := newNATSProc(t, freePort(t))
		aConf := natsTLSConf(cert, key) + "\n" + cluster(ra, rb)
		a.start(aConf)
		n := natsTLSSink(t, cert, a.url())
		if _, _, err := n.connect(); err != nil {
			t.Fatal(err)
		}
		run(t, a, aConf, n)
	})
}

// natsCreateStream creates, on the server at url (TLS, trusting caFile), the
// stream the sinks of newTestNATS validate: their stream on the subject of
// their space.
func natsCreateStream(t *testing.T, url, caFile string) {
	t.Helper()
	probe := newTestNATS(t, nil)
	nc, err := nats.Connect(url, nats.RootCAs(caFile), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: probe.cfg.Stream,
		Subjects: []string{natsSubject(probe.cfg.Workspace)}, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
}

// At the boot, nats.Connect tries every URL and returns the last one's error:
// a pool whose trusted server is down and whose other server does not verify
// waits out sink.boot_timeout (network, naming the TLS) instead of exiting at
// once. The single-server refusal stays fatal (TestNATSBootTLSRefusalIsFatal).
func TestNATSBootTLSRefusalOfAPoolIsNetwork(t *testing.T) {
	tlsURL := os.Getenv("WHS_TEST_NATS_TLS_URL")
	if tlsURL == "" {
		t.Skip("set WHS_TEST_NATS_TLS_URL to a nats-server listener with tls {}")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	down := fmt.Sprintf("nats://127.0.0.1:%d", freePort(t))
	other, _ := natsTLSCert(t) // a CA that did not sign the listener's certificate
	var cv *tls.CertificateVerificationError
	for i := 0; i < 20; i++ { // the pool order is random: the last error may be the down one's
		n := natsTLSSink(t, other, down, tlsURL)
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
