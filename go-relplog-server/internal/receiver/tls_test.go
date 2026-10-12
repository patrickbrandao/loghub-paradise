package receiver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// pki is an in-memory CA with a server and a client certificate, and a second,
// unrelated CA with its own client: everything a TLS test needs, written as PEM
// to a temporary directory because the receiver reads files (SPEC §4.5).
type pki struct {
	dir                   string
	caFile                string
	serverCert, serverKey string
	client                tls.Certificate // signed by the CA
	clientLeaf            *x509.Certificate
	stranger              tls.Certificate // signed by another CA
	pool                  *x509.CertPool  // trusts the CA, for clients
}

type issued struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCert(t *testing.T, tmpl *x509.Certificate, parent *issued) *issued {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, parentCert := key, tmpl
	if parent != nil {
		signer, parentCert = parent.key, parent.cert
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &issued{cert: c, key: key}
}

var serial int64

func template(cn string, ca bool, usage ...x509.ExtKeyUsage) *x509.Certificate {
	serial++
	t := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn, Organization: []string{"LogHub Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
	}
	if ca {
		t.IsCA, t.BasicConstraintsValid = true, true
		t.KeyUsage |= x509.KeyUsageCertSign
	}
	return t
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func tlsCert(t *testing.T, is *issued) tls.Certificate {
	return tls.Certificate{Certificate: [][]byte{is.cert.Raw}, PrivateKey: is.key, Leaf: is.cert}
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	ca := newCert(t, template("Test CA", true), nil)
	srvTmpl := template("syslog.test", false, x509.ExtKeyUsageServerAuth)
	srvTmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	srv := newCert(t, srvTmpl, ca)
	cli := newCert(t, template("rsyslog-01.intranet", false, x509.ExtKeyUsageClientAuth), ca)
	other := newCert(t, template("Other CA", true), nil)
	stranger := newCert(t, template("stranger", false, x509.ExtKeyUsageClientAuth), other)

	p := &pki{dir: t.TempDir()}
	p.caFile = filepath.Join(p.dir, "ca.pem")
	p.serverCert = filepath.Join(p.dir, "server.pem")
	p.serverKey = filepath.Join(p.dir, "server.key")
	writePEM(t, p.caFile, "CERTIFICATE", ca.cert.Raw)
	writePEM(t, p.serverCert, "CERTIFICATE", srv.cert.Raw)
	keyDER, err := x509.MarshalECPrivateKey(srv.key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, p.serverKey, "EC PRIVATE KEY", keyDER)

	p.client, p.clientLeaf = tlsCert(t, cli), cli.cert
	p.stranger = tlsCert(t, stranger)
	p.pool = x509.NewCertPool()
	p.pool.AddCert(ca.cert)
	return p
}

func startTLS(t *testing.T, h *harness, p *pki, cfg TLSConfig) *Stream {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.CertFile == "" {
		cfg.CertFile, cfg.KeyFile = p.serverCert, p.serverKey
	}
	tr, err := NewTLS(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); tr.Close() })
	tr.Start(ctx)
	return tr
}

func dialTLS(t *testing.T, addr string, p *pki, cert *tls.Certificate, maxVer uint16) (*tls.Conn, error) {
	t.Helper()
	cc := &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1", MaxVersion: maxVer}
	if cert != nil {
		// Forced: Go's client silently withholds a certificate whose issuer
		// is not among the CAs the server advertised, which would turn the
		// "stranger" tests into tests of no certificate at all.
		cc.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return cert, nil
		}
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	c, err := tls.DialWithDialer(d, "tcp", addr, cc)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { c.Close() })
	// TLS 1.3 reports a refused client certificate only on the first read,
	// after the client believed the handshake done.
	_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := c.Read(make([]byte, 1)); err != nil && !isTimeout(err) {
		return nil, err
	}
	_ = c.SetReadDeadline(time.Time{})
	return c, nil
}

func TestTLSWithoutClientCert(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional, ClientCAFile: p.caFile})
	c, err := dialTLS(t, tr.Addr(), p, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	relpSendAll(t, c, "<13>over tls", "<14>two")
	for _, it := range h.drainRing() {
		hd := it.Head
		if hd.Transport != item.TransportTLS || hd.Framing != item.FramingOctetCounting {
			t.Fatalf("wire = %v/%v", hd.Transport, hd.Framing)
		}
		if hd.TLS == nil || hd.TLS.Version != "TLS1.3" || !item.ValidTLSCipher(hd.TLS.Cipher) {
			t.Fatalf("session = %+v", hd.TLS)
		}
		if hd.TLS.PeerSubject != "" || hd.TLS.PeerFingerprint != "" {
			t.Fatalf("peer-* without a client certificate: %+v", hd.TLS)
		}
		if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
			t.Fatal(err)
		}
	}
	// Separate label and limiter (SPEC §4.5).
	if h.m.ConnsTotal[obs.TransportTLS].Load() != 1 || h.m.ConnsTotal[obs.TransportTCP].Load() != 0 {
		t.Fatal("the connection was not counted under tls")
	}
}

func TestTLSWithClientCert(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional, ClientCAFile: p.caFile})
	c, err := dialTLS(t, tr.Addr(), p, &p.client, 0)
	if err != nil {
		t.Fatal(err)
	}
	relpSendAll(t, c, "<13>with a certificate")
	s := h.drainRing()[0].Head.TLS
	if s.PeerSubject != p.clientLeaf.Subject.String() || !strings.Contains(s.PeerSubject, "CN=rsyslog-01.intranet") {
		t.Fatalf("peer-subject = %q", s.PeerSubject)
	}
	if s.PeerFingerprint != Fingerprint(p.clientLeaf.Raw) || !item.ValidFingerprint(s.PeerFingerprint) {
		t.Fatalf("peer-fingerprint = %q", s.PeerFingerprint)
	}
}

// optional WITHOUT a CA accepts a certificate nobody can verify, and records it
// just the same: the payload says what was presented, not what was trusted.
func TestTLSOptionalWithoutCARecordsUnverified(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional})
	c, err := dialTLS(t, tr.Addr(), p, &p.stranger, 0)
	if err != nil {
		t.Fatal(err)
	}
	relpSendAll(t, c, "<13>unverified")
	s := h.drainRing()[0].Head.TLS
	if !strings.Contains(s.PeerSubject, "CN=stranger") || s.PeerFingerprint != Fingerprint(p.stranger.Leaf.Raw) {
		t.Fatalf("session = %+v", s)
	}
}

// optional WITH a CA refuses a certificate it cannot verify.
func TestTLSOptionalWithCARefusesAStranger(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional, ClientCAFile: p.caFile})
	if _, err := dialTLS(t, tr.Addr(), p, &p.stranger, 0); err == nil {
		t.Fatal("a certificate from an unknown CA was accepted")
	}
	waitFor(t, "the handshake error", func() bool { return h.m.TLSHandshakeErrorsTotal.Load() == 1 })
}

func TestTLSRequire(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthRequire, ClientCAFile: p.caFile})

	if _, err := dialTLS(t, tr.Addr(), p, nil, 0); err == nil {
		t.Fatal("require accepted a client without a certificate")
	}
	waitFor(t, "the handshake error", func() bool { return h.m.TLSHandshakeErrorsTotal.Load() == 1 })

	c, err := dialTLS(t, tr.Addr(), p, &p.client, 0)
	if err != nil {
		t.Fatalf("require refused a valid client: %v", err)
	}
	relpSendAll(t, c, "<13>mutual")
	if h.drainRing()[0].Head.TLS.PeerFingerprint == "" {
		t.Fatal("no fingerprint under require")
	}
}

// SPEC §13.2: require without a CA is exit 1, and so are missing files.
func TestBuildTLSConfigRefusals(t *testing.T) {
	p := newPKI(t)
	for name, cfg := range map[string]TLSConfig{
		"require without CA": {CertFile: p.serverCert, KeyFile: p.serverKey, ClientAuth: ClientAuthRequire},
		"no certificate":     {ClientAuth: ClientAuthNone},
		"unreadable key":     {CertFile: p.serverCert, KeyFile: filepath.Join(p.dir, "nope")},
		"CA not PEM":         {CertFile: p.serverCert, KeyFile: p.serverKey, ClientCAFile: p.serverKey},
		"min version 1.1":    {CertFile: p.serverCert, KeyFile: p.serverKey, MinVersion: tls.VersionTLS11},
	} {
		if _, err := BuildTLSConfig(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for in, want := range map[string]tls.ClientAuthType{"optional": tls.RequestClientCert, "none": tls.NoClientCert} {
		ca, _ := ParseClientAuth(in)
		tc, err := BuildTLSConfig(TLSConfig{CertFile: p.serverCert, KeyFile: p.serverKey, ClientAuth: ca})
		if err != nil || tc.ClientAuth != want {
			t.Errorf("%s: %v %v", in, tc.ClientAuth, err)
		}
	}
	tc, err := BuildTLSConfig(TLSConfig{CertFile: p.serverCert, KeyFile: p.serverKey, ClientCAFile: p.caFile})
	if err != nil || tc.ClientAuth != tls.VerifyClientCertIfGiven || tc.MinVersion != tls.VersionTLS12 {
		t.Fatalf("optional+CA: %v min %x %v", tc.ClientAuth, tc.MinVersion, err)
	}
	if _, err := ParseClientAuth("mandatory"); err == nil {
		t.Error("an unknown client_auth was accepted")
	}
	if v, err := ParseTLSMinVersion("1.3"); err != nil || v != tls.VersionTLS13 {
		t.Error("1.3")
	}
	if _, err := ParseTLSMinVersion("1.1"); err == nil {
		t.Error("1.1 was accepted")
	}
}

func TestTLSMinVersion(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthNone, MinVersion: tls.VersionTLS13})
	if _, err := dialTLS(t, tr.Addr(), p, nil, tls.VersionTLS12); err == nil {
		t.Fatal("a TLS 1.2 client got past min_version=1.3")
	}

	// And a 1.2 session is recorded as TLS1.2 with an IANA cipher name.
	h2 := newHarness(t, 10, "debug", nil)
	tr2 := startTLS(t, h2, p, TLSConfig{ClientAuth: ClientAuthNone})
	c, err := dialTLS(t, tr2.Addr(), p, nil, tls.VersionTLS12)
	if err != nil {
		t.Fatal(err)
	}
	relpSendAll(t, c, "<13>old")
	s := h2.drainRing()[0].Head.TLS
	if s.Version != "TLS1.2" || !strings.HasPrefix(s.Cipher, "TLS_ECDHE_") {
		t.Fatalf("session = %+v", s)
	}
}

// Inside the tunnel only RELP is spoken: a plain syslog line over TLS is a
// malformed frame and closes the connection.
func TestTLSSpeaksOnlyRELP(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthNone})
	c, err := dialTLS(t, tr.Addr(), p, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(c, "<13>lf over tls\n")
	expectClosed(t, c)
	waitFor(t, "the malformed frame", func() bool {
		return h.m.RELPFrameErrorsTotal[obs.FrameMalformed].Load() == 1
	})
	if h.ring.Stats().Items != 0 {
		t.Fatal("a plain syslog line got in over TLS")
	}
}

// relpSendAll opens a RELP session on c, sends msgs and requires a 200 for
// each.
func relpSendAll(t *testing.T, c net.Conn, msgs ...string) {
	t.Helper()
	rc := newRELPClient(t, c)
	rc.open()
	ids := rc.syslog(msgs...)
	got := rc.responses(len(ids))
	for _, id := range ids {
		if got[id] != "200 OK" {
			t.Fatalf("txnr %d answered %q", id, got[id])
		}
	}
}

// tls.handshake_timeout closes a client that connects and never speaks.
func TestTLSHandshakeTimeout(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthNone, HandshakeTimeout: 150 * time.Millisecond})
	c := dial(t, tr.Addr())
	expectClosed(t, c)
	waitFor(t, "the handshake error", func() bool { return h.m.TLSHandshakeErrorsTotal.Load() == 1 })
}

// SPEC §4.6: the PROXY header comes in clear BEFORE the handshake.
func TestTLSBehindAProxy(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 10, "debug", nil)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthOptional, ClientCAFile: p.caFile,
		StreamConfig: StreamConfig{Proxy: ProxyConfig{Mode: ProxyRequired, Trusted: cidrs}}})

	raw := dial(t, tr.Addr())
	if _, err := raw.Write(proxyV2(0x21, 0x11, v2Inet("192.0.2.77", "198.51.100.1", 5000, 20515, nil))); err != nil {
		t.Fatal(err)
	}
	c := tls.Client(raw, &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1", Certificates: []tls.Certificate{p.client}})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	relpSendAll(t, c, "<13>proxied tls")
	hd := h.drainRing()[0].Head
	if hd.SrcIP.String() != "192.0.2.77" || hd.SrcPort != 5000 || !hd.Proxied() {
		t.Fatalf("head %+v", hd)
	}
	if hd.TLS == nil || hd.TLS.PeerFingerprint != Fingerprint(p.clientLeaf.Raw) {
		t.Fatalf("session %+v", hd.TLS)
	}
	if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
		t.Fatal(err)
	}
}

// RFC 5425 §4.2.2: "sha-256:" and the 32 octets, uppercase, colon-separated.
func TestFingerprintFormat(t *testing.T) {
	fp := Fingerprint([]byte("abc"))
	sum := sha256.Sum256([]byte("abc"))
	if !strings.HasPrefix(fp, "sha-256:BA:78:16:BF:") || len(fp) != len("sha-256:")+32*3-1 {
		t.Fatalf("fingerprint = %q", fp)
	}
	if !strings.HasSuffix(fp, fmt.Sprintf("%02X", sum[31])) || !item.ValidFingerprint(fp) {
		t.Fatalf("fingerprint = %q", fp)
	}
}

// sessionInfo omits what the payload schema would refuse instead of producing
// an envelope that fails later; a subject above 1 KiB is omitted (SPEC §4.5).
func TestSessionInfoOmitsWhatTheSchemaRefuses(t *testing.T) {
	long := &x509.Certificate{Raw: []byte("der"), Subject: pkix.Name{CommonName: strings.Repeat("c", 1100)}}
	s := sessionInfo(tls.ConnectionState{Version: tls.VersionTLS13, CipherSuite: 0xffff,
		PeerCertificates: []*x509.Certificate{long}})
	if s.Version != "TLS1.3" || s.Cipher != "" || s.PeerSubject != "" || s.PeerFingerprint == "" {
		t.Fatalf("session = %+v", s)
	}
	if sessionInfo(tls.ConnectionState{Version: 0x0999, CipherSuite: 0xffff}) != nil {
		t.Fatal("an empty session must be nil, so the payload omits tls")
	}
}
