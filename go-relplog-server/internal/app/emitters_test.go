package app

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// Helpers of the live-emitter suite of SPEC §18.7: real emitters on the wire,
// in Docker, which is where incompatibilities live. The RELP scenarios
// (rsyslog's omrelp, in clear and over TLS) live in emitters_relp_test.go; the
// syslog sibling's scenarios (UDP, LF and octet-counted TCP, syslog-ng's
// network drivers) were removed with the transports they exercised. It needs
// Docker and pulls packages, so it runs only when asked:
//
//	RELPLOG_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/

func requireEmitters(t *testing.T) {
	t.Helper()
	if os.Getenv("RELPLOG_TEST_EMITTERS") == "" {
		t.Skip("set RELPLOG_TEST_EMITTERS=1 to run against real emitters in Docker")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
}

// publicPort binds 0.0.0.0 on a free TCP port and releases it: a container
// reaches the host through host.docker.internal, which does not arrive on
// loopback everywhere.
func publicPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	p, _ := strconv.Atoi(portOf(t, addr))
	return p
}

// runContainer runs a shell script in a throwaway Alpine container that can
// reach the host, and returns its combined output.
func runContainer(t *testing.T, name, script string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--name", name,
		"--add-host", "host.docker.internal:host-gateway",
		"alpine:3.22", "sh", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("container %s output:\n%s", name, out)
		t.Fatalf("container %s failed: %v", name, err)
	}
	return string(out)
}

// clientPEM is the CA-signed client certificate and key, for an emitter
// presenting one over TLS.
func (p *pki) clientPEM(t *testing.T) (cert, key string) {
	t.Helper()
	kd, err := x509.MarshalECPrivateKey(p.clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.clientDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}))
}
