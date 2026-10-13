package app

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The live-emitter suite of SPEC §18.7: the real rsyslog, with its omrelp
// output (librelp), against the server in this process, publishing to the real
// NATS of the fixture. It is the only proof that the reading of librelp behind
// SPEC §5.4 holds on the wire: a 200 is delivered, a session closed without an
// answer is reconnected and retransmitted, and a stop with the ring holding
// confirmed messages loses nothing through the dump.
//
//	RELPLOG_TEST_EMITTERS=1 RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14292 \
//	  go test -race -run EmittersRELP -timeout 900s ./internal/app/

// requireEmitters skips the suite without RELPLOG_TEST_EMITTERS or Docker.
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

// rsyslogImage has its own tag (rlnats-): the build of go-relplog-server and of
// its siblings may run at the same time on the same host.
const rsyslogImage = "rlnats-test-rsyslog:alpine-3.22"

var rsyslogBuild struct {
	once sync.Once
	err  error
	out  []byte
}

// ensureRsyslogImage builds, once per test binary, an Alpine image with rsyslog
// and its RELP output, so each scenario does not pull packages again.
func ensureRsyslogImage(t *testing.T) {
	t.Helper()
	rsyslogBuild.once.Do(func() {
		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			cmd := exec.Command("docker", "build", "-t", rsyslogImage, "-")
			cmd.Stdin = strings.NewReader("FROM alpine:3.22\n" +
				"RUN apk add --no-cache rsyslog rsyslog-relp\n")
			var out []byte
			out, err = cmd.CombinedOutput()
			rsyslogBuild.out = out
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt) * 5 * time.Second)
		}
		rsyslogBuild.err = err
	})
	if rsyslogBuild.err != nil {
		t.Fatalf("building %s: %v\n%s", rsyslogImage, rsyslogBuild.err, rsyslogBuild.out)
	}
}

// rsyslogScript writes the files and the configuration, starts rsyslogd in
// the background and logs n lines tagged with marker through /dev/log. hold
// keeps the container alive afterwards, for the scenarios that act on the
// server while rsyslog retries; otherwise it waits settle seconds and prints
// rsyslog's own log.
func rsyslogScript(files map[string]string, conf, marker string, n, settle int, hold bool) string {
	var b strings.Builder
	b.WriteString("set -e\nmkdir -p /etc/rlnats /var/lib/rsyslog\n")
	for path, content := range files {
		fmt.Fprintf(&b, "cat > %s <<'PEM'\n%s\nPEM\n", path, content)
	}
	fmt.Fprintf(&b, "cat > /etc/rsyslog.conf <<'CONF'\n%s\nCONF\n", conf)
	b.WriteString("rsyslogd -n -f /etc/rsyslog.conf > /tmp/rsyslog.log 2>&1 &\n")
	b.WriteString("for i in $(seq 1 100); do [ -S /dev/log ] && break; sleep 0.1; done\nsleep 1\n")
	fmt.Fprintf(&b, "for i in $(seq 1 %d); do logger -t emittertest -p local0.notice \"%s msg-$i\"; done\n", n, marker)
	if hold {
		// librelp notices a closed session only when it next SENDS: with
		// nothing new to send, the unconfirmed messages would wait in rsyslog
		// for the next line (observed, SPEC §5.8). A live host always has a
		// next line; this one sends a tick a second.
		fmt.Fprintf(&b, "echo logged\ni=0; while true; do i=$((i+1)); logger -t emittertest -p local0.notice \"%s tick-$i\"; sleep 1; done\n", marker)
	} else {
		// Ticks during the settle time too: librelp reads the answers only
		// when it sends again, so without a next line a 500 could stay unread
		// and its error unlogged until rsyslog stopped.
		fmt.Fprintf(&b, "for i in $(seq 1 %d); do logger -t emittertest -p local0.notice \"%s tick-$i\"; sleep 1; done\n", settle, marker)
		b.WriteString("echo '--- rsyslog log ---'\ncat /tmp/rsyslog.log /tmp/rsyslog-internal.log 2>/dev/null || true\n")
	}
	return b.String()
}

// runRsyslog runs a script to its end and returns its output.
func runRsyslog(t *testing.T, name, script string) string {
	t.Helper()
	ensureRsyslogImage(t)
	cmd := exec.Command("docker", "run", "--rm", "--name", name,
		"--add-host", "host.docker.internal:host-gateway", rsyslogImage, "sh", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("container %s failed: %v\n%s", name, err, out)
	}
	return string(out)
}

// startRsyslog runs a script detached and returns a function that prints its
// log so far. The container is removed when the test ends.
func startRsyslog(t *testing.T, name, script string) func() string {
	t.Helper()
	ensureRsyslogImage(t)
	_ = exec.Command("docker", "rm", "-f", name).Run()
	out, err := exec.Command("docker", "run", "-d", "--name", name,
		"--add-host", "host.docker.internal:host-gateway", rsyslogImage, "sh", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("starting %s: %v\n%s", name, err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	return func() string {
		out, _ := exec.Command("docker", "logs", name).CombinedOutput()
		return string(out)
	}
}

// omrelpAction is one omrelp action with a queue that does not lose messages
// while the server is away: rsyslog retries forever, once a second. Only
// local0, the facility of the test lines: rsyslogd's own start message would
// otherwise be one more envelope in the stream.
func omrelpAction(port int, extra string) string {
	return fmt.Sprintf(`local0.* action(type="omrelp" target="host.docker.internal" port="%d"
      template="RSYSLOG_SyslogProtocol23Format" windowSize="128" timeout="10" conn.timeout="5"
      queue.type="LinkedList" queue.size="10000"
      action.resumeRetryCount="-1" action.resumeInterval="1" %s)`, port, extra)
}

// rsyslogHead loads the input and the RELP output, and writes rsyslogd's own
// messages (syslog.*: its errors, "peer sent error response" among them) to a
// file the script prints, instead of forwarding them.
const rsyslogHead = `global(workDirectory="/var/lib/rsyslog")
module(load="imuxsock")
module(load="omrelp")
syslog.* action(type="omfile" file="/tmp/rsyslog-internal.log")
`

// markerMsgs returns, for every message of the fixture's stream that carries
// marker, the validated envelope, keyed by the msg-N it carries; dup counts
// repeats.
func markerMsgs(t *testing.T, f *fixture, marker string) (byMsg map[string]jlhEnv, dup int) {
	t.Helper()
	byMsg = map[string]jlhEnv{}
	if f.count() <= 0 {
		return byMsg, 0
	}
	for _, m := range f.msgs() {
		if !strings.Contains(string(m.data), marker) {
			continue
		}
		e := checkMsg(t, m)
		msg := e.Payload.message()
		key := msg[strings.LastIndex(msg, " ")+1:]
		if !strings.HasPrefix(key, "msg-") {
			continue // a tick of a held scenario
		}
		if _, ok := byMsg[key]; ok {
			dup++
		}
		byMsg[key] = e
	}
	return byMsg, dup
}

func waitMarkerMsgs(t *testing.T, f *fixture, marker string, n int, d time.Duration) (map[string]jlhEnv, int) {
	t.Helper()
	var got map[string]jlhEnv
	var dup int
	eventually(d, func() bool {
		got, dup = markerMsgs(t, f, marker)
		return len(got) >= n
	})
	return got, dup
}

func metricInt(t *testing.T, in *instance, series string) int {
	t.Helper()
	v, err := strconv.ParseFloat(in.metric(series), 64)
	if err != nil {
		t.Fatalf("%s: %v", series, err)
	}
	return int(v)
}

func containerName(scenario string) string {
	return "rlnats-emit-" + scenario + "-" + strconv.Itoa(os.Getpid())
}

// RELP in clear from rsyslog: every message delivered to the stream once, as
// transport tcp, framing octet-counting, x-transport relp. There is no RELP
// over TLS here (SPEC §1.2).
func TestEmittersRELPInClear(t *testing.T) {
	requireEmitters(t)
	f := newFixture(t)
	tcpPort := publicPort(t)
	in := f.start(t.TempDir(), "--listen.tcp=0.0.0.0:"+strconv.Itoa(tcpPort))

	const n = 25
	marker := fmt.Sprintf("relp-%d", time.Now().UnixNano())
	out := runRsyslog(t, containerName("clear"), rsyslogScript(nil,
		rsyslogHead+omrelpAction(tcpPort, ""), marker, n, 10, false))

	got, dup := waitMarkerMsgs(t, f, marker, n, 60*time.Second)
	if len(got) != n || dup != 0 {
		t.Errorf("%d distinct of %d messages, %d duplicates", len(got), n, dup)
	}
	for _, e := range got {
		pl := e.Payload
		if show(pl.XTransport) != "relp" || pl.Framing != "octet-counting" || pl.Transport != "tcp" ||
			show(pl.AppName) != "emittertest" || pl.Facility.Name != "local0" || pl.Severity.Name != "notice" {
			t.Errorf("envelope %s", e.bytes)
			break
		}
	}
	if got := metricInt(t, in, `relplog_relp_responses_total{code="500"}`); got != 0 {
		t.Errorf("%d 500 responses", got)
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", out)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

// go-relplog-server's TestEmittersRELPFiveHundredIsFinal needs ring=none: the
// destination's final refusal is its one 500 in the normal flow. Here the ring
// is memory, and a refusal of the destination is the drain worker's, after the
// 200 (SPEC §5.4, §10.2): there is no 500 to send rsyslog.

// SPEC §5.7, decision 5: a full ring is held and, past
// relp.backpressure_timeout, the session closes WITHOUT an answer. rsyslog
// reconnects and retransmits until the ring drains: every message arrives, no
// 500 is ever sent, nothing is counted as dropped.
func TestEmittersRELPBackpressureRetransmits(t *testing.T) {
	requireEmitters(t)
	port := publicPort(t)
	fx := newFixture(t)
	f := newFaultySink(true) // the destination holds every write until opened
	in := launch(t, fx.id, fx.args(t.TempDir(),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(port),
		"--ring.capacity_items=5", "--relp.backpressure_timeout=2s", "--sink.op_timeout=60s",
	), f.wrap)
	in.waitReady()

	const n = 20
	marker := fmt.Sprintf("relpbp-%d", time.Now().UnixNano())
	logs := startRsyslog(t, containerName("bp"),
		rsyslogScript(nil, rsyslogHead+omrelpAction(port, ""), marker, n, 0, true))
	defer func() {
		if t.Failed() {
			t.Logf("emitter output:\n%s", logs())
		}
	}()

	waitFor(t, 90*time.Second, "a session closed by backpressure", func() bool {
		return metricInt(t, in, `relplog_relp_unanswered_total{reason="backpressure"}`) >= 1
	})
	f.open()
	got, dup := waitMarkerMsgs(t, fx, marker, n, 90*time.Second)
	reconnects := metricInt(t, in, `relplog_conns_total{transport="tcp"}`)
	if len(got) != n {
		t.Errorf("%d of %d messages arrived after the ring drained", len(got), n)
	}
	if reconnects < 2 {
		t.Errorf("%d connections: rsyslog never reconnected", reconnects)
	}
	if v := metricInt(t, in, `relplog_relp_responses_total{code="500"}`); v != 0 {
		t.Errorf("%d 500 responses for a full ring", v)
	}
	for _, r := range []string{"empty", "unaddressable"} {
		if v := metricInt(t, in, `relplog_messages_dropped_total{reason="`+r+`"}`); v != 0 {
			t.Errorf("messages_dropped{%s} = %d", r, v)
		}
	}
	// A message whose 200 was decided just after the session closed comes back
	// once more under a new uuid (SPEC §19): allowed, and reported.
	t.Logf("%d duplicates, %d connections", dup, reconnects)
	in.stop()
}

// SPEC §5.8, §12, §18.7: the server stops with transactions in flight and the
// destination stuck, and a new one takes the same port and data directory.
// The ring is memory: what was confirmed goes to the emergency dump (exit 3)
// and comes back at the next boot; what was not, rsyslog retransmits. Every
// message arrives exactly once: nothing confirmed is lost, nothing unconfirmed
// was enqueued.
func TestEmittersRELPSurvivesARestart(t *testing.T) {
	requireEmitters(t)
	port := publicPort(t)
	dir := t.TempDir()
	fx := newFixture(t)
	args := func() []string {
		return fx.args(dir,
			"--listen.tcp=0.0.0.0:"+strconv.Itoa(port),
			"--ring.capacity_items=5", "--relp.backpressure_timeout=30s",
			"--shutdown.drain_timeout=1s", "--sink.op_timeout=60s")
	}
	f := newFaultySink(true)
	a := launch(t, fx.id, args(), f.wrap)
	a.waitReady()

	const n = 20
	marker := fmt.Sprintf("relprs-%d", time.Now().UnixNano())
	logs := startRsyslog(t, containerName("restart"),
		rsyslogScript(nil, rsyslogHead+omrelpAction(port, ""), marker, n, 0, true))

	waitFor(t, 90*time.Second, "the ring full and transactions held", func() bool {
		return metricInt(t, a, `relplog_messages_accepted_total{transport="tcp"}`) >= 5 &&
			metricInt(t, a, "relplog_relp_inflight_transactions") >= 1
	})
	accepted := metricInt(t, a, `relplog_messages_accepted_total{transport="tcp"}`)
	// The destination never answered: the drain times out and the confirmed
	// messages go to the dump, exit 3 (SPEC §13.4).
	if code := a.stop(); code != ExitSink {
		t.Errorf("first life exited %d, want %d (the dump)", code, ExitSink)
	}
	if d := dumpFiles(t, a.emgDir(), fx.id.From); len(d) != 1 {
		t.Fatalf("dump files = %v, want 1", d)
	}

	b := launch(t, fx.id, args(), nil)
	b.waitReady()
	got, dup := waitMarkerMsgs(t, fx, marker, n, 120*time.Second)
	if len(got) != n || dup != 0 {
		t.Errorf("%d distinct of %d messages, %d duplicates, after a restart with %d confirmed",
			len(got), n, dup, accepted)
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", logs())
	}
	b.stop()
}
