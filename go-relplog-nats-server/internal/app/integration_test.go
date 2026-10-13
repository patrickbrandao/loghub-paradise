package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/sink"
)

// The in-process integration suite of SPEC §18.5: the real stack against the
// real destination, a JetStream stream on NATS. What it proves end to end are
// the promises of §2.3, §3, §4, §5, §9, §10, §12, §13, §14 and §16.3.

// ------------------------------------------------------------ transports

// SPEC §3.8, §4, §18.5: RELP in clear -- the one transport (SPEC §1.2) --
// reaches the stream, each message on loghub.<space>.syslog with the headers of
// its envelope, and each envelope with this instance's from and space,
// type=syslog, step 0, the level of its severity and the reception clock in
// event -- with a payload that records transport tcp, the octet-counting
// framing every RELP frame has, the x-transport extension that says RELP
// (decision 2 of go-relplog-server, SPEC §3.2) and the network addresses the
// socket knew.
func TestAllTransportsReachTheStream(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	tcpAddr := in.app.TransportAddr("tcp")
	tcpPort, _ := strconv.Atoi(portOf(t, tcpAddr))

	before := time.Now()
	rc := dialRELP(t, tcpAddr)
	src := rc.c.LocalAddr().(*net.TCPAddr).Port
	rc.sendOK(`<14>1 2026-09-12T14:33:02Z host-b app 9 - - via-relp-5424`,
		"<34>Oct 11 22:14:15 mymachine su[230]: via-relp-3164")
	rc.close()

	// Every message was answered 200, which means it is in the ring: the
	// stream follows once the workers drain.
	msgs := f.waitMsgs(2)
	after := time.Now()
	envs := byMessage(t, msgs)
	for marker, level := range map[string]int{"via-relp-5424": 6, "via-relp-3164": 2} {
		e := messageWith(t, envs, marker)
		if e.Payload.Transport != "tcp" || e.Payload.Framing != "octet-counting" || show(e.Payload.XTransport) != XTransport {
			t.Errorf("%s arrived as %s/%s x-transport=%s, want tcp/octet-counting/%s", marker,
				e.Payload.Transport, e.Payload.Framing, show(e.Payload.XTransport), XTransport)
		}
		if e.From != f.id.From || e.Space != f.space || e.Type != "syslog" || e.Step != 0 || e.Level != level {
			t.Errorf("%s: header from=%s space=%s type=%s step=%d level=%d, want %s/%s/syslog/0/%d",
				marker, e.From, e.Space, e.Type, e.Step, e.Level, f.id.From, f.space, level)
		}
		// event is the reception clock (SPEC §3.1), never the message's own
		// timestamp, which is in 2026-09 or a year-less 3164 date.
		if at := time.Unix(e.Event.UTS, e.Event.Sub); at.Before(before.Add(-time.Second)) || at.After(after.Add(time.Second)) {
			t.Errorf("%s: event %s is not the reception instant (%s .. %s)", marker, at, before, after)
		}
		h := e.Payload.Host
		if h == nil || show(h.SrcIP) != "127.0.0.1" || show(h.SrcPort) != strconv.Itoa(src) ||
			show(h.DstIP) != "127.0.0.1" || show(h.DstPort) != strconv.Itoa(tcpPort) {
			t.Errorf("%s: host = %s, want 127.0.0.1:%d -> 127.0.0.1:%d", marker, e.bytes, src, tcpPort)
		}
		if e.SID == 0 {
			t.Errorf("%s: sid = 0: a network message has the hash of its host", marker)
		}
		if bytes.Contains(e.bytes, []byte(`"tls"`)) {
			t.Errorf("%s: the envelope carries tls: %s", marker, e.bytes)
		}
	}

	// One session: an open, two syslog and a close (SPEC §14.1).
	for series, want := range map[string]string{
		`relplog_relp_frames_total{command="open"}`:                                 "1",
		`relplog_relp_frames_total{command="syslog"}`:                               "2",
		`relplog_relp_frames_total{command="close"}`:                                "1",
		`relplog_messages_accepted_total{transport="tcp"}`:                          "2",
		`relplog_relp_responses_total{code="200"}`:                                  "3", // open and two syslog; close has an empty rsp
		`relplog_config_info{ring_mode="memory",sink_type="nats",transports="tcp"}`: "1",
	} {
		if got := in.metric(series); got != want {
			t.Errorf("%s = %s, want %s", series, got, want)
		}
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0", code)
	}
}

// proxyV2 builds a binary PROXY v2 header, PROXY command, TCP over IPv4.
func proxyV2(src, dst netip.AddrPort) []byte {
	b := []byte("\r\n\r\n\x00\r\nQUIT\n")
	b = append(b, 0x21, 0x11, 0, 12)
	s, d := src.Addr().As4(), dst.Addr().As4()
	b = append(b, s[:]...)
	b = append(b, d[:]...)
	b = binary.BigEndian.AppendUint16(b, src.Port())
	return binary.BigEndian.AppendUint16(b, dst.Port())
}

// SPEC §4.6, §20.8: behind a trusted balancer, the payload's addresses are
// the client and the address it dialed, taken from the PROXY header (v1 and
// v2), the sid is computed from them (jlhcheck recomputes it), and the real TCP
// peer is kept in x-proxy. With proxy.protocol=optional a trusted peer that
// sends no header keeps its own address and gets no x-proxy.
func TestProxyProtocolFromATrustedPeer(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--proxy.protocol=optional")
	addr := in.app.TransportAddr("tcp")
	tcpPort, _ := strconv.Atoi(portOf(t, addr))

	// The header comes first on the connection, and the RELP session follows
	// on the same bytes (SPEC §4.6).
	session := func(header []byte, msg string) int {
		t.Helper()
		c := dialTCP(t, addr)
		peer := c.LocalAddr().(*net.TCPAddr).Port
		if _, err := c.Write(header); err != nil {
			t.Fatal(err)
		}
		rc := newRELPConn(t, c)
		rc.open()
		rc.sendOK(msg)
		rc.close()
		return peer
	}
	v1Peer := session([]byte("PROXY TCP4 203.0.113.7 198.51.100.9 51000 1514\r\n"),
		"<13>1 2026-09-12T14:33:01Z web-01 app - - - via-proxy-v1")
	v2Peer := session(proxyV2(netip.MustParseAddrPort("192.0.2.10:40000"), netip.MustParseAddrPort("192.0.2.20:6514")),
		"<13>1 2026-09-12T14:33:02Z web-02 app - - - via-proxy-v2")
	plainPeer := session(nil, "<13>1 2026-09-12T14:33:03Z web-03 app - - - via-no-header")

	envs := byMessage(t, f.waitMsgs(3))
	for _, tc := range []struct {
		marker, hostname, src string
		srcPort               int
		dst                   string
		dstPort, peerPort     int
		proxied               bool
	}{
		{"via-proxy-v1", "web-01", "203.0.113.7", 51000, "198.51.100.9", 1514, v1Peer, true},
		{"via-proxy-v2", "web-02", "192.0.2.10", 40000, "192.0.2.20", 6514, v2Peer, true},
		{"via-no-header", "web-03", "127.0.0.1", plainPeer, "127.0.0.1", tcpPort, 0, false},
	} {
		e := messageWith(t, envs, tc.marker)
		h := e.Payload.Host
		if h == nil || h.Hostname == nil || *h.Hostname != tc.hostname ||
			h.SrcIP == nil || *h.SrcIP != tc.src || h.SrcPort == nil || *h.SrcPort != tc.srcPort ||
			h.DstIP == nil || *h.DstIP != tc.dst || h.DstPort == nil || *h.DstPort != tc.dstPort {
			t.Errorf("%s: host = %s", tc.marker, e.bytes)
		}
		switch {
		case tc.proxied && (e.Payload.XProxy == nil || e.Payload.XProxy.PeerIP != "127.0.0.1" ||
			e.Payload.XProxy.PeerPort != tc.peerPort):
			t.Errorf("%s: x-proxy = %+v, want the real peer 127.0.0.1:%d", tc.marker, e.Payload.XProxy, tc.peerPort)
		case !tc.proxied && e.Payload.XProxy != nil:
			t.Errorf("%s: x-proxy present without a PROXY header", tc.marker)
		}
	}
	for series, want := range map[string]string{
		`relplog_proxy_headers_total{result="v1"}`:     "1",
		`relplog_proxy_headers_total{result="v2"}`:     "1",
		`relplog_proxy_headers_total{result="absent"}`: "1",
	} {
		if got := in.metric(series); got != want {
			t.Errorf("%s = %s, want %s", series, got, want)
		}
	}
}

// ------------------------------------------------------------ fidelity

// SPEC §3.2, §3.8, §6.7, invariants §20.6 and §20.7: the raw line comes back
// in payload.raw byte for byte, NUL included and framing removed; every field
// that did not come is absent, never ""; and the stream holds the JLH exactly
// as the worker wrote it -- a NUL stays a NUL (escaped \u0000) in every
// string, raw stays text unless the line is not UTF-8.
func TestTheStreamMessageIsFaithful(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())

	full := "<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] a mensagem \x00 com nul"
	nil5424 := "<13>1 2026-09-12T14:33:02Z - - - - - so a mensagem"
	nopid := "<34>Oct 11 22:14:15 mymachine su: sem pid"
	nulHdr := "<13>Oct 11 22:14:15 ho\x00st ap\x00p[1\x002]: nul no cabecalho"
	plain := "<13>1 2026-09-12T14:33:03Z web-02 app - - - texto simples"
	sendRELP(t, in.app.TransportAddr("tcp"), full, nil5424, nopid, nulHdr, plain)

	envs := byMessage(t, f.waitMsgs(5))

	e := messageWith(t, envs, "nul no cabecalho")
	if show(e.Payload.hostname()) != "ho\x00st" || show(e.Payload.AppName) != "ap\x00p" || show(e.Payload.ProcID) != "1\x002" {
		t.Errorf("the NULs of the header fields did not survive: %s", e.bytes)
	}
	if got := e.Payload.rawBytes(); e.Payload.RawEnc != "" || !bytes.Equal(got, []byte(nulHdr)) {
		t.Errorf("raw of a line with NUL = %q (%s), want the line exact, as text", got, e.Payload.RawEnc)
	}

	e = messageWith(t, envs, "a mensagem \x00 com nul")
	if got := e.Payload.rawBytes(); !bytes.Equal(got, []byte(full)) {
		t.Fatalf("payload.raw changed\n got %q\nwant %q", got, full)
	}
	if e.Payload.Size != len(full) || e.Payload.Format != "rfc5424" {
		t.Errorf("size = %d format = %s", e.Payload.Size, e.Payload.Format)
	}
	if e.Payload.Facility.Code != 20 || e.Payload.Severity.Code != 5 || e.Level != 5 {
		t.Errorf("facility/severity/level = %d/%d/%d", e.Payload.Facility.Code, e.Payload.Severity.Code, e.Level)
	}
	if v, _ := e.Payload.SD["a@1"]["k"].(string); v != `v"q` {
		t.Errorf("structured-data a@1 k = %q, want the unescaped value", v)
	}
	if show(e.Payload.hostname()) != "web-01" || show(e.Payload.AppName) != "nginx" ||
		show(e.Payload.ProcID) != "42" || show(e.Payload.MsgID) != "ID7" {
		t.Errorf("header fields: %s", e.bytes)
	}

	e = messageWith(t, envs, "texto simples")
	if got := e.Payload.rawBytes(); e.Payload.RawEnc == "base64" || !bytes.Equal(got, []byte(plain)) {
		t.Errorf("raw of a plain line = %q (%s), want the text itself", got, e.Payload.RawEnc)
	}

	e = messageWith(t, envs, "so a mensagem")
	if e.Payload.hostname() != nil || e.Payload.AppName != nil || e.Payload.ProcID != nil || e.Payload.MsgID != nil {
		t.Errorf("NILVALUE fields must be omitted: %s", e.bytes)
	}
	e = messageWith(t, envs, "sem pid")
	if e.Payload.ProcID != nil || e.Payload.MsgID != nil || e.Payload.AppName == nil || *e.Payload.AppName != "su" {
		t.Errorf("RFC 3164 without PID: %s", e.bytes)
	}
}

// ------------------------------------------------------------ SIGHUP

// SPEC §16.3: log.level, log.access and log.access_sample are reloaded at
// runtime -- changed, and then reverted to the boot value, which must be
// APPLIED (the running reference); a restart-only key is reported as ignored
// on EVERY reload while it differs from the boot (the boot reference) and
// really is not applied. a.reload() is what the SIGHUP handler calls; the real
// signal is TestBinarySIGHUPAppliesTheLogLevel's.
func TestSIGHUPReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	write := func(level, maxBytes string) {
		t.Helper()
		body := fmt.Sprintf("[log]\nlevel = %s\n\n[syslog]\nmax_message_bytes = %s\n", level, maxBytes)
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("error", "64KiB")
	f := newFixture(t)
	// --log.level would shadow the file (SPEC §16.1): the flag goes.
	in := launch(t, f.id, append(withoutFlag(f.args(dir), "--log.level="), "--config="+ini), nil)
	in.waitReady()
	enabled := func(l slog.Level) bool { return in.app.log.Enabled(context.Background(), l) }
	if enabled(slog.LevelWarn) {
		t.Fatal("the boot did not apply log.level=error from the file")
	}

	// Raise the level and change a restart-only key.
	write("debug", "32KiB")
	r := in.app.reload()
	if strings.Join(r.applied, ",") != "log.level" || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if !enabled(slog.LevelDebug) {
		t.Error("log.level=debug is not in force after the reload")
	}

	// The same file again: nothing to apply, and the restart-only key is
	// reported again -- every SIGHUP warns until the restart.
	if r := in.app.reload(); len(r.applied) != 0 || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// Revert the level to its BOOT value. Compared with the boot snapshot this
	// would look unchanged and be skipped; compared with what runs, it is a
	// change and is applied.
	write("error", "32KiB")
	r = in.app.reload()
	if strings.Join(r.applied, ",") != "log.level" || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if enabled(slog.LevelWarn) {
		t.Error("the reverted log.level=error is not in force")
	}

	// Everything back to the boot values: nothing applied, nothing ignored.
	write("error", "64KiB")
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot-values reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	// And the restart-only key really was ignored: the receiver still cuts at
	// the boot limit, not at the 32 KiB of the reloaded file.
	if got := in.app.ingest.MaxMessageBytes(); got != 64<<10 {
		t.Errorf("max_message_bytes in force = %d, want the boot 65536", got)
	}

	// Access logging is reloadable too.
	if err := os.WriteFile(ini, []byte("[log]\nlevel = error\naccess = true\naccess_sample = 1\n\n[syslog]\nmax_message_bytes = 64KiB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if r := in.app.reload(); strings.Join(r.applied, ",") != "log.access,log.access_sample" || len(r.ignored) != 0 {
		t.Fatalf("access reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if !in.app.log.AccessEnabled() {
		t.Error("log.access=true with access_sample=1 is not in force")
	}
}

// ------------------------------------------------------------ emergency

// SPEC §10.2, §12, §13.4, invariant §20.13: a destination that refuses every
// write while its health check passes makes the workers HOLD batches outside
// the ring, and the drain never empties. At shutdown.drain_timeout the shutdown
// collects them with the rest of the ring into an emergency dump -- one valid
// JLH per line, emg-<agent_uuid>-<unixnano>.jsonl -- and exits 3; the next life
// reingests the dump BEFORE any transport opens, the messages carry the SAME
// uuid and the SAME bytes the dump holds -- including the first life's from
// and space, though an instance of ANOTHER space published them, on the first
// life's subject loghub.<space>.syslog -- and the dump is removed.
func TestEmergencyDumpAndReingestionAreExact(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	args := f.args(dir,
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms",
	)

	// --- first life ---------------------------------------------------------
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, f.id, args, fs.wrap)
	first.waitReady()

	lines := []string{
		`<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 [a@1 k="v"] para-o-dump`,
		`<34>Oct 11 22:14:15 mymachine su[230]: segunda linha`,
		"sem pri, com \x00 no meio",
		`<13>1 2026-09-12T14:33:05Z - - - - - quarta`,
		"<190>Oct  1 00:00:00 host app: quinta \xff octeto",
	}
	sendRELP(t, first.app.TransportAddr("tcp"), lines...)
	m := first.app.metrics
	waitFor(t, 10*time.Second, "every message accepted and a failed write", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == uint64(len(lines)) &&
			m.SinkErrorsTotal[obs.SinkErrNetwork].Load() > 0
	})
	if s := first.app.health.State(); s != sink.StateUp {
		t.Fatalf("health state %v: the incident needs a sink that looks healthy", s)
	}

	if code := first.stop(); code != ExitSink {
		t.Fatalf("a shutdown that had to dump exited %d, want %d", code, ExitSink)
	}
	if d := m.EventsDroppedTotal[obs.EventDropAbort].Load() + m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); d != 0 {
		t.Errorf("%d messages counted as lost although they were dumped", d)
	}
	dumps := dumpFiles(t, first.emgDir(), f.id.From)
	if len(dumps) != 1 {
		all, _ := os.ReadDir(first.emgDir())
		t.Fatalf("dump files = %v (dir: %v), want one emg-%s-*.jsonl", dumps, all, f.id.From)
	}
	if n := f.count(); n != 0 {
		t.Fatalf("%d messages written by a sink that refused every write", n)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != len(lines) {
		t.Fatalf("the dump holds %d envelopes, want %d", len(dumped), len(lines))
	}
	// The dump holds the envelope the worker would have written, raw included:
	// the line with a NUL keeps it, escaped, as the converter wrote it.
	raws := map[string]bool{}
	for _, e := range dumped {
		raws[string(e.Payload.rawBytes())] = true
		if e.From != f.id.From || e.Space != f.space {
			t.Errorf("dumped from/space = %s/%s, want the receiving instance", e.From, e.Space)
		}
	}
	for _, l := range lines {
		if !raws[l] {
			t.Errorf("the dump has no envelope with raw %q", l)
		}
	}
	// Read from the registry: the admin port closed with the process.
	if got := m.EmergencyEventsPending.Load(); got != int64(len(lines)) {
		t.Errorf("relplog_emergency_events_pending = %d after the dump, want %d", got, len(lines))
	}

	// --- second life: a working sink, ANOTHER identity in ANOTHER space ------
	second := launch(t, f.other, append(args, "--shutdown.drain_timeout=5s"), nil)
	second.waitReady()
	// Reingestion runs before the transports open (SPEC §13.1 step 9): at
	// Ready, every message is already there.
	msgs := f.msgs()
	checkReingested(t, dumped, msgs)
	for _, m := range msgs {
		if m.env.From != f.id.From || m.env.Space != f.space || m.subject != subject(f.space) {
			t.Errorf("message %s from %s space %s on %s: the envelope is who RECEIVED it (the first "+
				"life), not who reingested it", m.env.UUID, m.env.From, m.env.Space, m.subject)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(second.emgDir(), "emg-*")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if got := second.metric("relplog_emergency_files"); got != "0" {
		t.Errorf("relplog_emergency_files = %s after the reingestion", got)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// SPEC §13.4 phase 4: a destination that never answers -- no error, no
// confirmation -- leaves the ring full at shutdown.drain_timeout. The workers
// surrender what they hold, the ring is drained into the dump, the exit is 3
// right after the deadline, and the next boot delivers everything.
func TestDrainTimeoutDumpsAndExitsThree(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	args := f.args(dir)
	fs := newFaultySink(true) // the gate never opens: every write hangs
	first := launch(t, f.id, append(args, "--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs.wrap)
	first.waitReady()
	const n = 10
	for i := 0; i < n; i++ {
		sendRELP(t, first.app.TransportAddr("tcp"), fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - slow-%02d", i))
	}
	waitFor(t, 10*time.Second, "every message accepted and a write hanging", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n && fs.attempts.Load() > 0
	})
	began := time.Now()
	if code := first.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	// The hanging write has 60 s of op_timeout: an exit within a few seconds
	// proves the shutdown did not wait for it.
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the shutdown took %s with a 300ms drain_timeout", took)
	}
	dumps := dumpFiles(t, first.emgDir(), f.id.From)
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}
	if c := f.count(); c != 0 {
		t.Fatalf("%d messages went past a write that never returned", c)
	}

	second := launch(t, f.id, args, nil)
	second.waitReady()
	checkReingested(t, dumped, f.msgs())
	if left := dumpFiles(t, second.emgDir(), f.id.From); len(left) != 0 {
		t.Errorf("the dump is still pending: %v", left)
	}
}

// SPEC §6.6, §12.1: with syslog.accept_malformed=false the dump does not
// resurrect what the worker would have refused. A malformed line still in the
// ring at the shutdown is left out of the dump and counted as
// relplog_events_dropped_total{reason="malformed"}; the well-formed lines are
// dumped. One worker, held on the first message, keeps the others in the ring
// so the dump -- not a worker -- is what meets the malformed one.
func TestMalformedIsLeftOutOfTheDump(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	fs := newFaultySink(true)
	in := launch(t, f.id, f.args(dir, "--syslog.accept_malformed=false", "--sink.workers=1",
		"--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs.wrap)
	in.waitReady()
	tcp := in.app.TransportAddr("tcp")
	sendRELP(t, tcp, "<13>1 2026-09-12T14:33:01Z host app - - - held-by-the-worker")
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	sendRELP(t, tcp, "GET / HTTP/1.1", "<13>1 2026-09-12T14:33:02Z host app - - - still-in-the-ring")
	m := in.app.metrics
	waitFor(t, 10*time.Second, "three accepted", func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 3 })
	if got := m.ConvertTotal[obs.FormatMalformed].Load(); got != 0 {
		t.Fatalf("a worker converted the malformed line (%d): the test needs it in the ring", got)
	}
	if code := in.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	dumps := dumpFiles(t, in.emgDir(), f.id.From)
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	got := map[string]bool{}
	for _, e := range dumpLines(t, dumps[0]) {
		got[e.Payload.message()] = true
		if e.Payload.Format == "malformed" {
			t.Errorf("a malformed envelope was dumped with accept_malformed=false: %s", e.bytes)
		}
	}
	if len(got) != 2 || !got["held-by-the-worker"] || !got["still-in-the-ring"] {
		t.Errorf("dumped messages = %v, want the two well-formed ones", got)
	}
	if d := m.EventsDroppedTotal[obs.EventDropMalformed].Load(); d != 1 {
		t.Errorf(`relplog_events_dropped_total{reason="malformed"} = %d, want 1`, d)
	}

	// The control: the same incident with accept_malformed=true dumps all three.
	dir2 := t.TempDir()
	fs2 := newFaultySink(true)
	in2 := launch(t, f.id, f.args(dir2, "--sink.workers=1",
		"--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs2.wrap)
	in2.waitReady()
	tcp2 := in2.app.TransportAddr("tcp")
	sendRELP(t, tcp2, "<13>1 2026-09-12T14:33:01Z host app - - - held-by-the-worker")
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs2.attempts.Load() > 0 })
	sendRELP(t, tcp2, "GET / HTTP/1.1", "<13>1 2026-09-12T14:33:02Z host app - - - still-in-the-ring")
	waitFor(t, 10*time.Second, "three accepted", func() bool {
		return in2.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == 3
	})
	if code := in2.stop(); code != ExitSink {
		t.Fatalf("control exit %d, want %d", code, ExitSink)
	}
	dumps = dumpFiles(t, in2.emgDir(), f.id.From)
	if len(dumps) != 1 || len(dumpLines(t, dumps[0])) != 3 {
		t.Errorf("control: dumps %v, want one with the three messages", dumps)
	}
}

// ------------------------------------------------------------ shutdown

// SPEC §13.4, §18.8: SIGTERM loses nothing. Every accepted message is in the
// destination after the exit, with exit 0.
func TestGracefulShutdownDeliversEverything(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	const n = 200
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - msg-%04d", i)
	}
	sendRELP(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "200 accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0", code)
	}
	msgs := f.msgs()
	if len(msgs) != n {
		t.Fatalf("%d of %d accepted messages reached the destination after SIGTERM", len(msgs), n)
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		seen[checkMsg(t, m).Payload.message()] = true
	}
	for _, l := range lines {
		if !seen[l[strings.LastIndexByte(l, ' ')+1:]] {
			t.Errorf("%q is missing", l)
		}
	}
}

// SPEC §13.4: a second signal during the shutdown exits at once with 130.
func TestSecondSignalAborts(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--shutdown.pre_stop_delay=1h")
	in.cancel() // the first signal: phase 1, then phase 2 sits in pre_stop_delay
	waitFor(t, 5*time.Second, "/readyz to report the shutdown", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "shutting down")
	})
	in.app.abort <- os.Interrupt // the second signal
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
}

// ------------------------------------------------------------ boot refusals

// SPEC §13.2, §13.5: configuration and identity errors exit 1, before anything
// opens.
func TestBootRefusalsExitOne(t *testing.T) {
	bad := testID
	bad.Space = "bad_space"
	empty := testID
	empty.Space = "a..b"
	for _, tc := range []struct {
		name string
		id   ident.Identity
		args []string
	}{
		{"no transport", testID, []string{"--listen.tcp="}},
		{"relp.window out of range", testID, []string{"--relp.window=0"}},
		{"a tls:// URL", testID, []string{"--sink.nats.urls=tls://127.0.0.1:4222"}},
		{"a removed key", testID, []string{"--listen.tls=:20515"}},
		{"workspace with an underscore", bad, nil},
		{"workspace with an empty segment", empty, nil},
		{"stream name with a wildcard", testID, []string{"--sink.nats.stream=LOGHUB_*"}},
		{"unknown sink.nats.mode", testID, []string{"--sink.nats.mode=kafka"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			code := RunContext(ctx, append(localArgs(t.TempDir()), tc.args...), tc.id)
			if code != ExitConfig {
				t.Fatalf("exit %d, want %d", code, ExitConfig)
			}
		})
	}
	// The control: a valid identity, a transport and a plain stream boot.
	t.Run("control", func(t *testing.T) {
		f := newFixture(t)
		in := launch(t, f.id, f.args(t.TempDir()), nil)
		in.waitReady()
		if code := in.stop(); code != ExitOK {
			t.Fatalf("control exited %d", code)
		}
	})
}

// ------------------------------------------------------------ admin

// labelSets is the closed set of every label SPEC §14.1 allows, by metric.
// Nothing an emitter controls may appear as a label (invariant §20.9).
var labelSets = map[string]map[string][]string{
	"relplog_messages_total":           {"transport": {"tcp"}},
	"relplog_messages_accepted_total":  {"transport": {"tcp"}},
	"relplog_messages_truncated_total": {"transport": {"tcp"}},
	// What RELP hands back to the emitter -- a held transaction left
	// unanswered -- is not a drop (SPEC §5.7), and there is no filter.
	"relplog_messages_dropped_total": {"reason": {"empty", "unaddressable"}},
	"relplog_convert_total":          {"format": {"rfc5424", "rfc3164", "malformed"}},
	"relplog_convert_warnings_total": {"code": {"pri-missing", "pri-invalid", "version-unsupported",
		"header-invalid", "timestamp-missing", "timestamp-invalid", "timestamp-year-inferred",
		"timestamp-zone-inferred", "hostname-missing", "sd-invalid", "sd-duplicate-id",
		"encoding-invalid", "encoding-legacy", "truncated"}},
	"relplog_conns_active":               {"transport": {"tcp"}},
	"relplog_conns_total":                {"transport": {"tcp"}},
	"relplog_conns_refused_total":        {"transport": {"tcp"}, "reason": {"global", "per_ip", "proxy"}},
	"relplog_proxy_headers_total":        {"result": {"v1", "v2", "local", "absent", "invalid"}},
	"relplog_backpressure_seconds_total": {"transport": {"tcp"}},
	"relplog_relp_frames_total":          {"command": {"open", "syslog", "close", "unknown"}},
	"relplog_relp_responses_total":       {"code": {"200", "500"}},
	"relplog_relp_unanswered_total":      {"reason": {"backpressure", "shutdown", "conn_lost", "server_closed"}},
	"relplog_relp_frame_errors_total":    {"reason": {"malformed", "too_large"}},
	"relplog_sink_errors_total":          {"kind": {"network", "logic", "fatal", "timeout"}},
	"relplog_events_dropped_total":       {"reason": {"sink_logic", "malformed", "emergency_failed", "abort"}},
	"relplog_beacons_total":              {"result": {"ok", "error"}},
}

// unlabelled is every series SPEC §14.1 exposes without labels, histograms
// included (as their _count).
var unlabelled = []string{
	"relplog_message_bytes_count", "relplog_convert_duration_seconds_count",
	"relplog_confirm_duration_seconds_count",
	"relplog_relp_inflight_bytes", "relplog_relp_inflight_transactions", "relplog_relp_inflight_waits_total",
	"relplog_ring_items", "relplog_ring_bytes", "relplog_ring_capacity_items", "relplog_ring_capacity_bytes",
	"relplog_sink_batch_size_count", "relplog_sink_batch_bytes_count", "relplog_sink_duration_seconds_count",
	"relplog_sink_health_duration_seconds_count", "relplog_sink_up",
	"relplog_sink_nats_duplicates_total", "relplog_sink_nats_reconnects_total",
	"relplog_events_written_total", "relplog_emergency_files", "relplog_emergency_events_pending",
}

// parseSeries splits name{a="x",b="y"} into the name and its labels.
func parseSeries(t *testing.T, s string) (string, map[string]string) {
	t.Helper()
	i := strings.IndexByte(s, '{')
	if i < 0 {
		return s, nil
	}
	name, body := s[:i], strings.TrimSuffix(s[i+1:], "}")
	labels := map[string]string{}
	for body != "" {
		k, rest, ok := strings.Cut(body, `="`)
		if !ok {
			t.Fatalf("unparseable series %q", s)
		}
		v, after, ok := strings.Cut(rest, `"`)
		if !ok {
			t.Fatalf("unparseable series %q", s)
		}
		labels[k] = v
		body = strings.TrimPrefix(after, ",")
	}
	return name, labels
}

// SPEC §14: /healthz is liveness, /readyz fails with the reason while the sink
// is DOWN and recovers with it, and /metrics exposes the identity, the
// configuration, every series of the table and nothing but the closed label
// sets -- with histogram bounds that are the same strings on every scrape.
func TestAdminSurface(t *testing.T) {
	f := newFixture(t)
	fs := newFaultySink(false)
	in := launch(t, f.id, f.args(t.TempDir(),
		// DOWN must not escalate to FATAL during the test.
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000"), fs.wrap)
	in.waitReady()

	if code, body := in.get("/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("/healthz = %d %q", code, body)
	}
	if code, body := in.get("/readyz"); code != 200 {
		t.Fatalf("/readyz = %d %q while ready", code, body)
	}

	sendRELP(t, in.app.TransportAddr("tcp"), "<13>Oct 11 22:14:15 evil-host evilapp[1]: for the metrics")
	f.waitMsgs(1)

	first, err := in.scrapeRaw()
	if err != nil {
		t.Fatal(err)
	}
	series := parseExposition(first)
	for want, v := range map[string]string{
		fmt.Sprintf(`relplog_identity_info{from=%q,space=%q,agent_name=%q}`, f.id.From, f.space, f.id.AgentName): "1",
		`relplog_config_info{ring_mode="memory",sink_type="nats",transports="tcp"}`:                              "1",
		`relplog_messages_total{transport="tcp"}`:                                                                "1",
		`relplog_messages_accepted_total{transport="tcp"}`:                                                       "1",
		`relplog_convert_total{format="rfc3164"}`:                                                                "1",
		`relplog_events_written_total`:                                                                           "1",
		`relplog_sink_up`:                                                                                        "1",
	} {
		if series[want] != v {
			t.Errorf("/metrics: %s = %q, want %s", want, series[want], v)
		}
	}
	for _, name := range unlabelled {
		if _, ok := series[name]; !ok {
			t.Errorf("/metrics lacks %s", name)
		}
	}
	exposed := map[string]map[string]bool{} // metric -> "label=value" seen
	les := map[string]bool{}
	for s := range series {
		name, labels := parseSeries(t, s)
		if !strings.HasPrefix(name, "relplog_") {
			t.Errorf("series without the relplog_ prefix: %s", s)
		}
		if len(labels) == 0 {
			continue
		}
		switch name {
		case "relplog_identity_info", "relplog_config_info", "relplog_build_info":
			continue
		}
		if le, ok := labels["le"]; ok && strings.HasSuffix(name, "_bucket") && len(labels) == 1 {
			les[name+"|"+le] = true
			continue
		}
		set, ok := labelSets[name]
		if !ok {
			t.Errorf("labelled series outside SPEC §14.1: %s", s)
			continue
		}
		for k, v := range labels {
			allowed := set[k]
			found := false
			for _, a := range allowed {
				found = found || a == v
			}
			if !found {
				t.Errorf("%s: label %s=%q is outside the closed set %v", name, k, v, allowed)
			}
			if exposed[name] == nil {
				exposed[name] = map[string]bool{}
			}
			exposed[name][k+"="+v] = true
		}
	}
	// Every value of every closed set is exposed (SPEC §14.1: "cada label tem
	// teste de exposição"), so a dashboard never meets a missing series.
	for name, set := range labelSets {
		for k, values := range set {
			for _, v := range values {
				if !exposed[name][k+"="+v] {
					t.Errorf("%s{%s=%q} is not exposed", name, k, v)
				}
			}
		}
	}
	if b := series[fmt.Sprintf(`relplog_build_info{version="dev",commit="none",go=%q}`, runtime.Version())]; b != "1" {
		t.Errorf("relplog_build_info is missing or not 1")
	}
	for _, leak := range []string{"evil-host", "evilapp", "for the metrics"} {
		if strings.Contains(first, leak) {
			t.Errorf("/metrics carries emitter-controlled content %q", leak)
		}
	}
	// Histogram bounds are pre-formatted strings (SPEC §14.1): the same set of
	// le values on every scrape.
	second, err := in.scrapeRaw()
	if err != nil {
		t.Fatal(err)
	}
	for s := range parseExposition(second) {
		name, labels := parseSeries(t, s)
		if le, ok := labels["le"]; ok && strings.HasSuffix(name, "_bucket") && !les[name+"|"+le] {
			t.Errorf("histogram bound %s le=%q appeared only on the second scrape", name, le)
		}
	}

	// The sink goes DOWN: /readyz says so, with the reason, and /healthz
	// does not care (a restart would not fix a destination).
	fs.failHealth.Store(true)
	waitFor(t, 10*time.Second, "/readyz 503 with the sink reason", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "sink is DOWN")
	})
	if code, _ := in.get("/healthz"); code != 200 {
		t.Errorf("/healthz = %d while the sink is DOWN", code)
	}
	if got := in.metric("relplog_sink_up"); got != "0" {
		t.Errorf("relplog_sink_up = %s while DOWN", got)
	}
	fs.failHealth.Store(false)
	waitFor(t, 10*time.Second, "/readyz 200 after the sink recovered", func() bool {
		code, _ := in.get("/readyz")
		return code == 200
	})
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

func (in *instance) scrapeRaw() (string, error) {
	code, body := in.get("/metrics")
	if code != 200 {
		return "", fmt.Errorf("/metrics answered %d", code)
	}
	return body, nil
}

// ------------------------------------------------------------ malformed

// SPEC §6.6: with syslog.accept_malformed=false a malformed line is converted,
// counted as relplog_events_dropped_total{reason="malformed"} and never written;
// the well-formed line beside it is.
func TestMalformedIsRefusedWhenNotAccepted(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--syslog.accept_malformed=false")
	sendRELP(t, in.app.TransportAddr("tcp"), "GET / HTTP/1.1", "<13>1 2026-09-12T14:33:01Z host app - - - bem-formada")
	waitFor(t, 10*time.Second, "the malformed drop and the good message", func() bool {
		return in.app.metrics.EventsDroppedTotal[obs.EventDropMalformed].Load() == 1 && f.count() == 1
	})
	if got := in.metric(`relplog_events_dropped_total{reason="malformed"}`); got != "1" {
		t.Errorf(`relplog_events_dropped_total{reason="malformed"} = %s`, got)
	}
	if got := in.metric(`relplog_convert_total{format="malformed"}`); got != "1" {
		t.Errorf(`relplog_convert_total{format="malformed"} = %s: the line must be converted before it is refused`, got)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	msgs := f.msgs()
	if len(msgs) != 1 {
		t.Fatalf("%d messages, want only the well-formed one", len(msgs))
	}
	if e := checkMsg(t, msgs[0]); e.Payload.message() != "bem-formada" || e.Payload.Format != "rfc5424" {
		t.Errorf("the surviving message is %s", e.bytes)
	}
}

// ------------------------------------------------------------ truncation

// SPEC §5.3 (decision 13 of go-syslog-server): a message above
// syslog.max_message_bytes is cut, the envelope says truncated with the
// ORIGINAL size, the excess is consumed without closing the session, and the
// next frame on the same session arrives intact.
func TestTruncationKeepsTheSessionInSync(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir(), "--syslog.max_message_bytes=480")
	big := "<13>1 2026-09-12T14:33:01Z host app - - - big:" + strings.Repeat("x", 2000)
	big = big[:2000]
	next := "<13>1 2026-09-12T14:33:02Z host app - - - depois-do-grande"
	sendRELP(t, in.app.TransportAddr("tcp"), big, next)

	msgs := f.waitMsgs(2)
	var gotBig, gotNext bool
	for _, m := range msgs {
		e := checkMsg(t, m)
		switch {
		case strings.Contains(e.Payload.message(), "big:"):
			gotBig = true
			if !e.Payload.hasWarning("truncated") || e.Payload.Size != 2000 {
				t.Errorf("truncated message: warnings=%v size=%d", e.Payload.Parse, e.Payload.Size)
			}
			if raw := e.Payload.rawBytes(); !bytes.Equal(raw, []byte(big[:480])) {
				t.Errorf("raw is %d bytes, want the first 480 of the message", len(raw))
			}
		case e.Payload.message() == "depois-do-grande":
			gotNext = true
			if e.Payload.Parse != nil || e.Payload.Size != len(next) || string(e.Payload.rawBytes()) != next {
				t.Errorf("the frame after the truncated one is not intact: %s", e.bytes)
			}
		default:
			t.Errorf("unexpected message %s", e.bytes)
		}
	}
	if !gotBig || !gotNext {
		t.Fatalf("big=%v next=%v", gotBig, gotNext)
	}
	if got := in.metric(`relplog_messages_truncated_total{transport="tcp"}`); got != "1" {
		t.Errorf(`relplog_messages_truncated_total{transport="tcp"} = %s`, got)
	}
	for _, series := range []string{`relplog_relp_frame_errors_total{reason="malformed"}`,
		`relplog_relp_frame_errors_total{reason="too_large"}`} {
		if got := in.metric(series); got != "0" {
			t.Errorf("the session was treated as broken: %s = %s", series, got)
		}
	}
}
