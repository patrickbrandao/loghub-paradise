package receiver

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// defaultHandshakeTimeout is tls.handshake_timeout (SPEC §4.5, §16.5).
const defaultHandshakeTimeout = 10 * time.Second

// ClientAuth is tls.client_auth (SPEC §4.5).
type ClientAuth uint8

const (
	// ClientAuthOptional (default) asks for a client certificate. With
	// tls.client_ca it verifies one when it comes; without a CA it accepts it
	// unverified and still records peer-* for pinning downstream.
	ClientAuthOptional ClientAuth = iota
	// ClientAuthRequire demands and verifies one; tls.client_ca is mandatory.
	ClientAuthRequire
	// ClientAuthNone does not ask.
	ClientAuthNone
)

func (a ClientAuth) String() string {
	switch a {
	case ClientAuthOptional:
		return "optional"
	case ClientAuthRequire:
		return "require"
	case ClientAuthNone:
		return "none"
	}
	return "unknown"
}

// ParseClientAuth reads tls.client_auth: optional, require or none.
func ParseClientAuth(s string) (ClientAuth, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "optional", "":
		return ClientAuthOptional, nil
	case "require":
		return ClientAuthRequire, nil
	case "none":
		return ClientAuthNone, nil
	}
	return 0, fmt.Errorf("receiver: tls.client_auth %q: want optional, require or none", s)
}

// ParseTLSMinVersion reads tls.min_version: 1.2 (default) or 1.3.
func ParseTLSMinVersion(s string) (uint16, error) {
	switch strings.TrimSpace(s) {
	case "1.2", "":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	}
	return 0, fmt.Errorf("receiver: tls.min_version %q: want 1.2 or 1.3", s)
}

// TLSConfig is [listen] tls + tls_listeners and the [tls] section (SPEC §4.5,
// §16.5), plus the shared [proxy] section in StreamConfig.Proxy. The limits in
// StreamConfig are tls's OWN, with the [tcp] defaults: distinct transports,
// distinct limiters and a distinct "tls" metric label.
type TLSConfig struct {
	StreamConfig

	// CertFile and KeyFile are the server certificate chain and key, PEM.
	// Read once, at construction: changing them needs a restart in v1.
	CertFile string
	KeyFile  string
	// ClientAuth is tls.client_auth.
	ClientAuth ClientAuth
	// ClientCAFile is tls.client_ca, a PEM bundle of the CAs that sign client
	// certificates. Mandatory with ClientAuthRequire.
	ClientCAFile string
	// MinVersion is tls.VersionTLS12 (default when zero) or
	// tls.VersionTLS13; see ParseTLSMinVersion.
	MinVersion uint16
	// HandshakeTimeout is tls.handshake_timeout (10 s); zero or negative
	// selects 10 s.
	HandshakeTimeout time.Duration
}

// BuildTLSConfig loads the files and builds the crypto/tls configuration of
// SPEC §4.5. It is what NewTLS calls, exported so the boot can refuse a bad
// certificate with exit 1 before anything opens (SPEC §13.2).
//
// The client_auth table of SPEC §4.5:
//
//	optional + CA     VerifyClientCertIfGiven
//	optional, no CA   RequestClientCert (presented, recorded, not verified)
//	require           RequireAndVerifyClientCert, CA mandatory
//	none              NoClientCert
//
// The cipher suites are Go's defaults, which cover RFC 9662's.
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, errors.New("receiver tls: tls.cert_file and tls.key_file are required with listen.tls")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("receiver tls: loading the certificate: %w", err)
	}
	minVer := cfg.MinVersion
	switch minVer {
	case 0:
		minVer = tls.VersionTLS12
	case tls.VersionTLS12, tls.VersionTLS13:
	default:
		return nil, fmt.Errorf("receiver tls: tls.min_version 0x%04x: want TLS 1.2 or 1.3", minVer)
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVer,
	}

	var pool *x509.CertPool
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("receiver tls: reading tls.client_ca: %w", err)
		}
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("receiver tls: tls.client_ca %s holds no PEM certificate", cfg.ClientCAFile)
		}
	}
	switch cfg.ClientAuth {
	case ClientAuthOptional:
		if pool != nil {
			tc.ClientAuth = tls.VerifyClientCertIfGiven
			tc.ClientCAs = pool
		} else {
			// The payload says what was PRESENTED, not what was trusted
			// (SPEC §4.5): the fingerprint lets a consumer pin it.
			tc.ClientAuth = tls.RequestClientCert
		}
	case ClientAuthRequire:
		if pool == nil {
			return nil, errors.New("receiver tls: tls.client_auth=require needs tls.client_ca")
		}
		tc.ClientAuth = tls.RequireAndVerifyClientCert
		tc.ClientCAs = pool
	case ClientAuthNone:
		tc.ClientAuth = tls.NoClientCert
	default:
		return nil, fmt.Errorf("receiver tls: unknown tls.client_auth %d", cfg.ClientAuth)
	}
	return tc, nil
}

// NewTLS opens the listeners of RELP over TLS (SPEC §4.5): crypto/tls, then the
// same RELP session as TCP. It is what rsyslog's omrelp speaks with tls="on":
// the handshake right after connect, then RELP frames inside the tunnel.
//
// The PROXY header, when configured, is read BEFORE the handshake: the
// balancer speaks it in clear in front of the ClientHello (SPEC §4.6).
func NewTLS(ctx context.Context, cfg TLSConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	tc, err := BuildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	hs := cfg.HandshakeTimeout
	if hs <= 0 {
		hs = defaultHandshakeTimeout
	}
	return newStream(ctx, item.TransportTLS, cfg.StreamConfig, tc, hs, in, m, log)
}

// sessionInfo builds payload.tls ONCE per connection, after the handshake
// (SPEC §4.5). Every value the payload schema would reject is omitted at the
// source -- with the item package's own validators -- instead of producing an
// envelope that fails later:
//
//	version           tls.VersionName without the space: "TLS1.3"
//	cipher            tls.CipherSuiteName, only when it is an IANA TLS_ name
//	peer-subject      the client certificate's DN (RFC 4514), omitted above
//	                  1 KiB: the fingerprint identifies the certificate, and
//	                  the ceiling keeps the worst envelope bounded (SPEC §3.9)
//	peer-fingerprint  "sha-256:" + the 32 octets of SHA-256(DER) in UPPERCASE
//	                  hex joined by ':' (RFC 5425 §4.2.2)
//
// Without a client certificate peer-* are omitted. A certificate presented and
// not verified (optional without a CA) is recorded just the same.
func sessionInfo(st tls.ConnectionState) *item.TLSInfo {
	info := &item.TLSInfo{}
	if v := strings.ReplaceAll(tls.VersionName(st.Version), " ", ""); item.ValidTLSVersion(v) {
		info.Version = v
	}
	if c := tls.CipherSuiteName(st.CipherSuite); item.ValidTLSCipher(c) {
		info.Cipher = c
	}
	if len(st.PeerCertificates) > 0 {
		leaf := st.PeerCertificates[0]
		if subj := leaf.Subject.String(); item.ValidPeerSubject(subj) {
			info.PeerSubject = subj
		}
		if fp := Fingerprint(leaf.Raw); item.ValidFingerprint(fp) {
			info.PeerFingerprint = fp
		}
	}
	if info.Empty() {
		return nil
	}
	return info
}

// Fingerprint is the RFC 5425 §4.2.2 form of a certificate's SHA-256:
// "sha-256:" followed by the 32 octets in uppercase hex separated by ':'.
func Fingerprint(der []byte) string {
	const hexdigits = "0123456789ABCDEF"
	sum := sha256.Sum256(der)
	b := make([]byte, 0, len("sha-256:")+len(sum)*3-1)
	b = append(b, "sha-256:"...)
	for i, c := range sum {
		if i > 0 {
			b = append(b, ':')
		}
		b = append(b, hexdigits[c>>4], hexdigits[c&0x0f])
	}
	return string(b)
}
