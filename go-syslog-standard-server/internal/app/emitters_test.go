package app

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The live-emitter suite of SPEC §17.4.
//
// The corpus proves the parser against real LINES. This proves the framing
// against real EMITTERS on the wire, which is where incompatibilities actually
// live: rsyslog's octet-counted TCP, its traditional LF framing, librelp's
// handshake and window, syslog-ng's two network drivers. Nothing short of the
// real programs exercises them.
//
// It needs Docker and pulls packages, so it runs only when asked:
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

// publicAddr binds 0.0.0.0 on a free port: a container reaches the host through
// host.docker.internal, which does not arrive on loopback everywhere.
func publicAddr(t *testing.T, network string) (addr string, port int) {
	t.Helper()
	var p string
	switch network {
	case "udp":
		c, err := net.ListenPacket("udp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		_, p, _ = net.SplitHostPort(c.LocalAddr().String())
		c.Close()
	default:
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		_, p, _ = net.SplitHostPort(l.Addr().String())
		l.Close()
	}
	port, _ = strconv.Atoi(p)
	return "0.0.0.0:" + p, port
}

// runContainer runs a shell script in a throwaway Alpine container that can reach
// the host, and returns its combined output.
func runContainer(t *testing.T, ctx context.Context, name, script string) string {
	t.Helper()
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
	udpPort, tcpPort, relpPort int
}

func startForEmitters(t *testing.T) *emitterStack {
	t.Helper()
	udp, udpPort := publicAddr(t, "udp")
	tcp, tcpPort := publicAddr(t, "tcp")
	relp, relpPort := publicAddr(t, "tcp")

	in := start(t, t.TempDir(),
		"--listen.udp="+udp, "--listen.tcp="+tcp, "--listen.relp="+relp,
		// The limiter keys on the peer, and every container arrives from the
		// same gateway address.
		"--ratelimit.enabled=false",
	)
	return &emitterStack{instance: in, udpPort: udpPort, tcpPort: tcpPort, relpPort: relpPort}
}

// rowsWith counts destination rows whose raw line carries marker, grouped by
// transport and framing -- the two things the wire decides.
func (s *emitterStack) rowsWith(marker string) map[string]int {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(s.dir, "events.db"))
	if err != nil {
		s.t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT transport, json_extract(env, '$.framing'), COUNT(*)
	                       FROM events WHERE CAST(raw AS TEXT) LIKE ?
	                       GROUP BY 1, 2`, "%"+marker+"%")
	if err != nil {
		s.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var tr, fr string
		var n int
		rows.Scan(&tr, &fr, &n)
		out[tr+"/"+fr] = n
	}
	return out
}

func (s *emitterStack) waitFor(marker string, want map[string]int) map[string]int {
	s.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var got map[string]int
	for time.Now().Before(deadline) {
		got = s.rowsWith(marker)
		done := true
		for k, n := range want {
			if got[k] < n {
				done = false
			}
		}
		if done {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
	return got
}

// rsyslog forwarding every message over four actions at once: UDP, TCP with
// traditional LF framing, TCP octet-counted in RFC 5424, and RELP.
func TestEmittersRsyslog(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)
	defer s.stop()

	const n = 25
	marker := fmt.Sprintf("rsyslog-%d", time.Now().UnixNano())
	conf := fmt.Sprintf(`
module(load="imuxsock")
module(load="omrelp")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="udp")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="tcp"
           TCP_Framing="traditional" template="RSYSLOG_ForwardFormat")
*.* action(type="omfwd" target="host.docker.internal" port="%d" protocol="tcp"
           TCP_Framing="octet-counted" template="RSYSLOG_SyslogProtocol23Format")
*.* action(type="omrelp" target="host.docker.internal" port="%d")
`, s.udpPort, s.tcpPort, s.tcpPort, s.relpPort)

	script := fmt.Sprintf(`set -e
apk add --no-cache rsyslog rsyslog-relp >/dev/null
cat > /etc/rsyslog.conf <<'CONF'
%s
CONF
rsyslogd -n &
for i in $(seq 1 100); do [ -S /dev/log ] && break; sleep 0.1; done
for i in $(seq 1 %d); do logger -t emittertest -p local0.notice "%s msg-$i"; done
sleep 8
`, conf, n, marker)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runContainer(t, ctx, "sls-emit-rsyslog-"+strconv.Itoa(os.Getpid()), script)

	want := map[string]int{
		"udp/datagram": n,
		"tcp/lf":       n,
		"tcp/octet":    n,
		"relp/relp":    n,
	}
	got := s.waitFor(marker, want)
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %d rows, want %d", k, got[k], w)
		}
	}

	// rsyslog's forward format puts an RFC 3339 timestamp into an RFC 3164
	// line. The corpus caught the parser misreading it; this proves the fix on
	// the real emitter's real output.
	db, _ := sql.Open("sqlite", "file:"+filepath.Join(s.dir, "events.db"))
	defer db.Close()
	var bad int
	db.QueryRow(`SELECT COUNT(*) FROM events WHERE CAST(raw AS TEXT) LIKE ?
	             AND (app_name != 'emittertest' OR severity != 5 OR facility != 16)`,
		"%"+marker+"%").Scan(&bad)
	if bad != 0 {
		t.Errorf("%d rows from rsyslog were parsed with the wrong app-name, facility or severity", bad)
	}
	if acks := s.app.metrics.RELPAcksTotal.Load(); acks < n {
		t.Errorf("librelp got %d acknowledgements for %d messages", acks, n)
	}
}

// syslog-ng with both of its network drivers: network() speaks BSD syslog with
// LF framing, syslog() speaks RFC 5424 octet-counted.
func TestEmittersSyslogNG(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)
	defer s.stop()

	const n = 25
	marker := fmt.Sprintf("syslogng-%d", time.Now().UnixNano())
	// Both TCP destinations aim at the same host:port, which is the point -- the
	// server auto-detects the framing per connection. syslog-ng derives a
	// persist name from host and port and REFUSES the whole configuration when
	// two destinations collide, so each gets an explicit one. Without it the
	// daemon opens its connections, aborts, and delivers nothing.
	conf := fmt.Sprintf(`@version: current
source s_local { unix-dgram("/dev/log"); };
destination d_bsd  { network("host.docker.internal" port(%d) transport("tcp") persist-name("d_bsd")); };
destination d_5424 { syslog("host.docker.internal" port(%d) transport("tcp") persist-name("d_5424")); };
destination d_udp  { network("host.docker.internal" port(%d) transport("udp") persist-name("d_udp")); };
log { source(s_local); destination(d_bsd); destination(d_5424); destination(d_udp); };
`, s.tcpPort, s.tcpPort, s.udpPort)

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out := runContainer(t, ctx, "sls-emit-syslogng-"+strconv.Itoa(os.Getpid()), script)

	want := map[string]int{"tcp/lf": n, "tcp/octet": n, "udp/datagram": n}
	got := s.waitFor(marker, want)
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %d rows, want %d", k, got[k], w)
		}
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", out)
		t.Logf("rows by transport/framing: %v", got)
		t.Logf("sample raw lines: %s", strings.Join(sampleRaw(t, s.dir, marker), " | "))
	}
}

func sampleRaw(t *testing.T, dir, marker string) []string {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "events.db"))
	if err != nil {
		return nil
	}
	defer db.Close()
	rows, err := db.Query(`SELECT CAST(raw AS TEXT) FROM events WHERE CAST(raw AS TEXT) LIKE ? LIMIT 4`,
		"%"+marker+"%")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	return out
}
