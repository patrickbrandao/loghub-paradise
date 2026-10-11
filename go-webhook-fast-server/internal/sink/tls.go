package sink

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// loadTLS builds the client TLS configuration the newer remote destinations
// share (SPEC §8.6.10, §8.7.9): verification on by default, against the
// system pool or the configured CA; insecure only when asked. serverName may
// be empty when the client library fills it from the address it dials, as
// nats.go does.
func loadTLS(what, caPath string, insecure bool, serverName string) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if insecure {
		tc.InsecureSkipVerify = true
	}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("%s: reading %s: %w", what, caPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: %s contains no usable certificate", what, caPath)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}
