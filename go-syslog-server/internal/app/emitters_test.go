package app

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The live-emitter suite of SPEC §18.7.
//
// The corpus proves the converter against real LINES; this proves framing and
// TLS against real EMITTERS on the wire, which is where incompatibilities live:
// rsyslog's octet-counted and traditional TCP, its gtls driver, syslog-ng's two
// network drivers. Nothing short of the real programs exercises them. It needs
// Docker and pulls packages, so it runs only when asked:
//
//	SYSLOG_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/

func requireEmitters(t *testing.T) {
	t.Helper()
	if os.Getenv("SYSLOG_TEST_EMITTERS") == "" {
		t.Skip("set SYSLOG_TEST_EMITTERS=1 to run against real rsyslog and syslog-ng in Docker")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
}

// publicPort binds 0.0.0.0 on a free port and releases it: a container reaches
// the host through host.docker.internal, which does not arrive on loopback
// everywhere.
func publicPort(t *testing.T, network string) int {
	t.Helper()
	var addr string
	if network == "udp" {
		c, err := net.ListenPacket("udp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		addr = c.LocalAddr().String()
		c.Close()
	} else {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		addr = l.Addr().String()
		l.Close()
	}
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

type emitterStack struct {
	*instance
	udp, tcp, tls int
	pki           *pki
}

func startForEmitters(t *testing.T) *emitterStack {
	t.Helper()
	s := &emitterStack{
		udp: publicPort(t, "udp"), tcp: publicPort(t, "tcp"), tls: publicPort(t, "tcp"),
		pki: newPKI(t, "host.docker.internal"),
	}
	s.instance = start(t, t.TempDir(),
		"--listen.udp=0.0.0.0:"+strconv.Itoa(s.udp),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(s.tcp),
		"--listen.tls=0.0.0.0:"+strconv.Itoa(s.tls),
		"--tls.cert_file="+s.pki.serverCert, "--tls.key_file="+s.pki.serverKey,
		"--tls.client_ca="+s.pki.caFile,
		// Every container arrives from the same gateway address.
		"--ratelimit.enabled=false",
	)
	return s
}

// byWire waits until want is reached and returns the envelopes carrying
// marker, grouped by transport/framing -- the two things the wire decides.
// Every envelope is validated with the oracle on the way.
func (s *emitterStack) byWire(marker string, want map[string]int) map[string][]jlhEnv {
	s.t.Helper()
	var got map[string][]jlhEnv
	eventually(60*time.Second, func() bool {
		got = map[string][]jlhEnv{}
		if sqliteCount(s.t, s.eventsDB(), "syslog_events") <= 0 {
			return false
		}
		for _, r := range sqliteRows(s.t, s.eventsDB()) {
			if !strings.Contains(string(r.Env), marker) {
				continue
			}
			e := checkRow(s.t, r)
			k := e.Payload.Transport + "/" + e.Payload.Framing
			got[k] = append(got[k], e)
		}
		for k, n := range want {
			if len(got[k]) < n {
				return false
			}
		}
		return true
	})
	return got
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

// rsyslog forwarding every message over four actions at once: UDP, TCP with
// traditional LF framing, TCP octet-counted in RFC 5424, and TLS through the
// gtls driver (octet-counted, RFC 5425), verifying the server by name and
// presenting a client certificate.
func TestEmittersRsyslog(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)

	const n = 25
	marker := fmt.Sprintf("rsyslog-%d", time.Now().UnixNano())
	cert, key := s.pki.clientPEM(t)
	conf := fmt.Sprintf(`
global(
  DefaultNetstreamDriver="gtls"
  DefaultNetstreamDriverCAFile="/etc/sls/ca.pem"
  DefaultNetstreamDriverCertFile="/etc/sls/client.pem"
  DefaultNetstreamDriverKeyFile="/etc/sls/client.key"
)
module(load="imuxsock")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="udp")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="tcp"
           TCP_Framing="traditional" template="RSYSLOG_ForwardFormat")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="tcp"
           TCP_Framing="octet-counted" template="RSYSLOG_SyslogProtocol23Format")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="tcp"
           StreamDriver="gtls" StreamDriverMode="1" StreamDriverAuthMode="x509/name"
           StreamDriverPermittedPeers="host.docker.internal"
           TCP_Framing="octet-counted" template="RSYSLOG_SyslogProtocol23Format")
`, s.udp, s.tcp, s.tcp, s.tls)

	script := fmt.Sprintf(`set -e
apk add --no-cache rsyslog rsyslog-tls >/dev/null
mkdir -p /etc/sls
cat > /etc/sls/ca.pem <<'PEM'
%s
PEM
cat > /etc/sls/client.pem <<'PEM'
%s
PEM
cat > /etc/sls/client.key <<'PEM'
%s
PEM
cat > /etc/rsyslog.conf <<'CONF'
%s
CONF
rsyslogd -n > /tmp/rsyslog.log 2>&1 &
for i in $(seq 1 100); do [ -S /dev/log ] && break; sleep 0.1; done
sleep 1
for i in $(seq 1 %d); do logger -t emittertest -p local0.notice "%s msg-$i"; done
sleep 8
echo "--- rsyslog log ---"
cat /tmp/rsyslog.log
`, s.pki.caPEM(t), cert, key, conf, n, marker)
	out := runContainer(t, "sls-emit-rsyslog-"+strconv.Itoa(os.Getpid()), script)

	want := map[string]int{
		"udp/datagram":        n,
		"tcp/non-transparent": n,
		"tcp/octet-counting":  n,
		"tls/octet-counting":  n,
	}
	got := s.byWire(marker, want)
	for k, w := range want {
		if len(got[k]) != w {
			t.Errorf("%s: %d envelopes, want %d", k, len(got[k]), w)
		}
	}
	for k, envs := range got {
		for _, e := range envs {
			if show(e.Payload.AppName) != "emittertest" || e.Payload.Facility.Name != "local0" || e.Payload.Severity.Name != "notice" {
				t.Errorf("%s: parsed as appname %s, %s.%s: %s", k, show(e.Payload.AppName),
					e.Payload.Facility.Name, e.Payload.Severity.Name, e.bytes)
				break
			}
		}
	}
	for _, e := range got["tls/octet-counting"] {
		if e.Payload.TLS == nil || show(e.Payload.TLS.PeerFingerprint) != fingerprint(s.pki.clientDER) {
			t.Errorf("rsyslog's client certificate is not in payload.tls: %s", e.bytes)
			break
		}
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", out)
	}
	if code := s.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

// syslog-ng with both of its network drivers: network() speaks BSD syslog with
// LF framing over TCP and UDP, syslog() speaks RFC 5424 octet-counted.
func TestEmittersSyslogNG(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)

	const n = 25
	marker := fmt.Sprintf("syslogng-%d", time.Now().UnixNano())
	// Both TCP destinations aim at the same host:port, which is the point --
	// the server decides the framing per connection (SPEC §5.2). syslog-ng
	// derives a persist name from host and port and refuses the whole
	// configuration when two destinations collide, so each gets its own.
	conf := fmt.Sprintf(`@version: current
source s_local { unix-dgram("/dev/log"); };
destination d_bsd  { network("host.docker.internal" port(%d) transport("tcp") persist-name("d_bsd")); };
destination d_5424 { syslog("host.docker.internal" port(%d) transport("tcp") persist-name("d_5424")); };
destination d_udp  { network("host.docker.internal" port(%d) transport("udp") persist-name("d_udp")); };
log { source(s_local); destination(d_bsd); destination(d_5424); destination(d_udp); };
`, s.tcp, s.tcp, s.udp)

	script := fmt.Sprintf(`set -e
apk add --no-cache syslog-ng util-linux-misc >/dev/null 2>&1 || apk add --no-cache syslog-ng >/dev/null
cat > /etc/syslog-ng/syslog-ng.conf <<'CONF'
%s
CONF
syslog-ng -F -e > /tmp/syslog-ng.log 2>&1 &
for i in $(seq 1 100); do [ -S /dev/log ] && break; sleep 0.1; done
sleep 1
for i in $(seq 1 %d); do logger -t emittertest -p local0.warning "%s msg-$i"; done
sleep 8
echo "--- syslog-ng log ---"
cat /tmp/syslog-ng.log
`, conf, n, marker)
	out := runContainer(t, "sls-emit-syslogng-"+strconv.Itoa(os.Getpid()), script)

	want := map[string]int{"tcp/non-transparent": n, "tcp/octet-counting": n, "udp/datagram": n}
	got := s.byWire(marker, want)
	for k, w := range want {
		if len(got[k]) != w {
			t.Errorf("%s: %d envelopes, want %d", k, len(got[k]), w)
		}
	}
	for k, envs := range got {
		for _, e := range envs {
			if show(e.Payload.AppName) != "emittertest" || e.Payload.Facility.Name != "local0" || e.Payload.Severity.Name != "warning" {
				t.Errorf("%s: parsed as appname %s, %s.%s: %s", k, show(e.Payload.AppName),
					e.Payload.Facility.Name, e.Payload.Severity.Name, e.bytes)
				break
			}
		}
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", out)
	}
	if code := s.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}
