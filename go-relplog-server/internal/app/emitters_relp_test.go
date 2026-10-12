package app

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// The live-emitter suite of SPEC §18.7: the real rsyslog, with its omrelp
// output (librelp), against the server in this process. It is the only proof
// that the reading of librelp behind SPEC §5.4 holds on the wire: a 200 is
// delivered, a 500 is dropped by the emitter and never retransmitted, a
// session closed without an answer is reconnected and retransmitted.
//
//	RELPLOG_TEST_EMITTERS=1 go test -race -run EmittersRELP -timeout 900s ./internal/app/

const rsyslogImage = "rls-test-rsyslog:alpine-3.22"

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
	b.WriteString("set -e\nmkdir -p /etc/rls /var/lib/rsyslog\n")
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
// otherwise take a rate-limit token and a row.
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

// markerRows returns, for every row whose message carries marker, the
// validated envelope, keyed by the msg-N it carries; dup counts repeats.
func markerRows(t *testing.T, in *instance, marker string) (byMsg map[string]jlhEnv, dup int) {
	t.Helper()
	byMsg = map[string]jlhEnv{}
	if sqliteCount(t, in.eventsDB(), "syslog_events") <= 0 {
		return byMsg, 0
	}
	for _, r := range sqliteRows(t, in.eventsDB()) {
		if !strings.Contains(string(r.Env), marker) {
			continue
		}
		e := checkRow(t, r)
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

func waitMarkerRows(t *testing.T, in *instance, marker string, n int, d time.Duration) (map[string]jlhEnv, int) {
	t.Helper()
	var got map[string]jlhEnv
	var dup int
	eventually(d, func() bool {
		got, dup = markerRows(t, in, marker)
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
	return "rls-emit-" + scenario + "-" + strconv.Itoa(os.Getpid())
}

// RELP in clear and RELP over TLS, at once from one rsyslog: every message
// delivered once, as transport tcp/tls, framing octet-counting, x-transport
// relp, with rsyslog's client certificate in payload.tls.
func TestEmittersRELPInClearAndOverTLS(t *testing.T) {
	requireEmitters(t)
	tcpPort, tlsPort := publicPort(t), publicPort(t)
	p := newPKI(t, "host.docker.internal")
	in := start(t, t.TempDir(),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(tcpPort),
		"--listen.tls=0.0.0.0:"+strconv.Itoa(tlsPort),
		"--tls.cert_file="+p.serverCert, "--tls.key_file="+p.serverKey,
		"--tls.client_ca="+p.caFile,
		"--ratelimit.enabled=false", // every container arrives from the gateway
	)

	const n = 25
	marker := fmt.Sprintf("relp-%d", time.Now().UnixNano())
	cert, key := p.clientPEM(t)
	conf := rsyslogHead + omrelpAction(tcpPort, "") + "\n" + omrelpAction(tlsPort,
		`tls="on" tls.caCert="/etc/rls/ca.pem" tls.myCert="/etc/rls/client.pem"
      tls.myPrivKey="/etc/rls/client.key" tls.authMode="name" tls.permittedPeer=["host.docker.internal"]`)
	out := runRsyslog(t, containerName("both"), rsyslogScript(map[string]string{
		"/etc/rls/ca.pem": p.caPEM(t), "/etc/rls/client.pem": cert, "/etc/rls/client.key": key,
	}, conf, marker, n, 10, false))

	var byWire map[string]int
	eventually(60*time.Second, func() bool {
		byWire = map[string]int{}
		for _, r := range sqliteRows(t, in.eventsDB()) {
			if strings.Contains(string(r.Env), marker+" msg-") {
				byWire[r.Transport]++
			}
		}
		return byWire["tcp"] >= n && byWire["tls"] >= n
	})
	if byWire["tcp"] != n || byWire["tls"] != n {
		t.Errorf("envelopes by transport %v, want %d each", byWire, n)
	}
	for _, r := range sqliteRows(t, in.eventsDB()) {
		if !strings.Contains(string(r.Env), marker+" msg-") {
			continue
		}
		e := checkRow(t, r)
		pl := e.Payload
		if show(pl.XTransport) != "relp" || pl.Framing != "octet-counting" || show(pl.AppName) != "emittertest" ||
			pl.Facility.Name != "local0" || pl.Severity.Name != "notice" {
			t.Errorf("envelope %s", e.bytes)
			break
		}
		if pl.Transport == "tls" && (pl.TLS == nil || show(pl.TLS.PeerFingerprint) != fingerprint(p.clientDER)) {
			t.Errorf("rsyslog's client certificate is not in payload.tls: %s", e.bytes)
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

// SPEC §5.4, decision 4: a 500 is FINAL for librelp. ring=none with a
// destination that refuses msg-3 for good (a logic error): msg-3 gets
// "500 refused by the destination", rsyslog logs the error response and never
// sends msg-3 again, and every other message arrives.
//
// Observed on the way (2026-10-11, rsyslog 8.2410): the 500 also SUSPENDS the
// action and breaks the session (librelp error 10020), so the transactions of
// the window already sent and not yet answered are retransmitted on the next
// session -- the ones this server had accepted come back as duplicates under
// new uuids. A 500 is final for its message and costs a reconnect for its
// neighbours (SPEC §5.4).
func TestEmittersRELPFiveHundredIsFinal(t *testing.T) {
	requireEmitters(t)
	port := publicPort(t)
	const n = 10
	marker := fmt.Sprintf("relp500-%d", time.Now().UnixNano())
	refusedLine := marker + " msg-3"
	var seen sync.Map // refusedLine -> attempts
	f := newFaultySink(false)
	f.decide = func(b *sink.Batch) error {
		for _, e := range b.Envelopes {
			if strings.HasSuffix(strings.TrimRight(string(e.Item.Raw), "\r\n"), refusedLine) {
				v, _ := seen.LoadOrStore(refusedLine, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
				return &sink.Error{Class: sink.ClassLogic, Op: "insert", Err: errors.New("refused for good (injected)")}
			}
		}
		return nil
	}
	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(port), "--ratelimit.enabled=false", "--ring.mode=none",
	), f.wrap)
	in.waitReady()

	out := runRsyslog(t, containerName("500"),
		rsyslogScript(nil, rsyslogHead+omrelpAction(port, ""), marker, n, 15, false))

	got, dup := waitMarkerRows(t, in, marker, n-1, 30*time.Second)
	if len(got) != n-1 {
		t.Errorf("%d distinct messages arrived, want every one but msg-3 (%d)", len(got), n-1)
	}
	if _, ok := got["msg-3"]; ok {
		t.Error("msg-3 arrived although the destination refused it")
	}
	attempts := int64(0)
	if v, ok := seen.Load(refusedLine); ok {
		attempts = v.(*atomic.Int64).Load()
	}
	// The container ran 15 s past the last line with resumeInterval=1 and
	// resumeRetryCount=-1: a refused message rsyslog kept would have come
	// back at the next session.
	if attempts != 1 {
		t.Errorf("the refused message reached the destination %d times, want 1: librelp retransmitted it after the 500", attempts)
	}
	if v := metricInt(t, in, `relplog_relp_responses_total{code="500"}`); v != 1 {
		t.Errorf("%d 500 responses, want 1", v)
	}
	if !strings.Contains(out, "peer sent error response") {
		t.Error("rsyslog did not log the error response")
	}
	t.Logf("%d duplicates of neighbours retransmitted after the 500", dup)
	if t.Failed() {
		t.Logf("emitter output:\n%s", out)
	}
	in.stop()
}

// SPEC §5.7, decision 5: a full ring is held and, past
// relp.backpressure_timeout, the session closes WITHOUT an answer. rsyslog
// reconnects and retransmits until the ring drains: every message arrives, no
// 500 is ever sent, nothing is counted as dropped.
func TestEmittersRELPBackpressureRetransmits(t *testing.T) {
	requireEmitters(t)
	port := publicPort(t)
	f := newFaultySink(true) // the destination holds every write until opened
	in := launch(t, testID, append(baseArgs(t.TempDir()),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(port), "--ratelimit.enabled=false",
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
	got, dup := waitMarkerRows(t, in, marker, n, 90*time.Second)
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
	for _, r := range []string{"filter", "empty", "unaddressable"} {
		if v := metricInt(t, in, `relplog_messages_dropped_total{reason="`+r+`"}`); v != 0 {
			t.Errorf("messages_dropped{%s} = %d", r, v)
		}
	}
	// A message whose 200 was decided just after the session closed comes back
	// once more under a new uuid (SPEC §19): allowed, and reported.
	t.Logf("%d duplicates, %d connections", dup, reconnects)
	in.stop()
}

// SPEC §5.8, §18.7: the server stops with transactions in flight and a new one
// takes the same port and data directory. ring=sqlite keeps what was
// confirmed; rsyslog retransmits what was not. Every message arrives exactly
// once: nothing confirmed is lost, nothing unconfirmed was enqueued.
func TestEmittersRELPSurvivesARestart(t *testing.T) {
	requireEmitters(t)
	port := publicPort(t)
	dir := t.TempDir()
	args := func() []string {
		return append(baseArgs(dir),
			"--listen.tcp=0.0.0.0:"+strconv.Itoa(port), "--ratelimit.enabled=false",
			"--ring.mode=sqlite", "--ring.capacity_items=5", "--relp.backpressure_timeout=30s",
			"--shutdown.drain_timeout=1s", "--sink.op_timeout=60s")
	}
	f := newFaultySink(true)
	a := launch(t, testID, args(), f.wrap)
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
	if code := a.stop(); code != ExitOK {
		t.Errorf("first life exited %d", code)
	}

	b := start(t, dir, args()[len(baseArgs(dir)):]...)
	got, dup := waitMarkerRows(t, b, marker, n, 120*time.Second)
	if len(got) != n || dup != 0 {
		t.Errorf("%d distinct of %d messages, %d duplicates, after a restart with %d confirmed",
			len(got), n, dup, accepted)
	}
	if t.Failed() {
		t.Logf("emitter output:\n%s", logs())
	}
	b.stop()
}
