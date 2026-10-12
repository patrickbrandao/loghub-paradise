package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// The in-process integration suite of SPEC §18.5: the real stack, sink=sqlite
// and sink=none, no external server. What it proves end to end are the
// promises of §2.3, §3, §4, §5, §9, §10, §12, §13, §14 and §16.3.

// ------------------------------------------------------------ transports

// SPEC §4, §18.5: both transports reach the destination -- RELP in clear and
// RELP over TLS -- and each envelope records the transport it arrived with,
// the octet-counting framing every RELP frame has, the x-transport extension
// that says RELP (decision 2, SPEC §3.2), the network addresses the socket
// knew and the TLS session (§4.5).
func TestAllTransportsReachSQLite(t *testing.T) {
	p := newPKI(t)
	in := start(t, t.TempDir(),
		"--listen.tls=127.0.0.1:0",
		"--tls.cert_file="+p.serverCert, "--tls.key_file="+p.serverKey,
		// client_auth=optional (the default) WITH a CA: a certificate that
		// comes is verified, and its absence is accepted (SPEC §4.5).
		"--tls.client_ca="+p.caFile,
	)

	tcpAddr := in.app.TransportAddr("tcp")
	tcpPort, _ := strconv.Atoi(portOf(t, tcpAddr))
	tlsAddr := in.app.TransportAddr("tls")
	tlsPort, _ := strconv.Atoi(portOf(t, tlsAddr))

	// RELP in clear.
	rc := dialRELP(t, tcpAddr)
	tcpSrc := rc.c.LocalAddr().(*net.TCPAddr).Port
	rc.sendOK(`<14>1 2026-09-12T14:33:02Z host-b app 9 - - via-relp-tcp`)
	rc.close()

	// RELP over TLS with and without a client certificate.
	sendTLS := func(cert *tls.Certificate, msg string) int {
		cfg := &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1"}
		if cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
		rc := dialRELPTLS(t, tlsAddr, cfg)
		src := rc.c.LocalAddr().(*net.TCPAddr).Port
		rc.sendOK(msg)
		rc.close()
		return src
	}
	clientCert := tls.Certificate{Certificate: [][]byte{p.clientDER}, PrivateKey: p.clientKey}
	certSrc := sendTLS(&clientCert, `<13>1 2026-09-12T14:33:03Z tls-host app - - - via-tls-cert`)
	anonSrc := sendTLS(nil, `<13>1 2026-09-12T14:33:04Z tls-host app - - - via-tls-anon`)

	// Every message was answered 200, which means it is in the ring: the rows
	// follow once the workers drain.
	envs := byMessage(t, in.waitRows(3))
	eqHost := func(name string, e jlhEnv, srcPort, dstPort int) {
		t.Helper()
		h := e.Payload.Host
		if h == nil {
			t.Errorf("%s: no host object: %s", name, e.bytes)
			return
		}
		if show(h.SrcIP) != "127.0.0.1" || show(h.SrcPort) != strconv.Itoa(srcPort) ||
			show(h.DstIP) != "127.0.0.1" || show(h.DstPort) != strconv.Itoa(dstPort) {
			t.Errorf("%s: host = %s:%s -> %s:%s, want 127.0.0.1:%d -> 127.0.0.1:%d", name,
				show(h.SrcIP), show(h.SrcPort), show(h.DstIP), show(h.DstPort), srcPort, dstPort)
		}
	}
	check := func(marker, transport string) jlhEnv {
		t.Helper()
		e := messageWith(t, envs, marker)
		if e.Payload.Transport != transport || e.Payload.Framing != "octet-counting" {
			t.Errorf("%s arrived as %s/%s, want %s/octet-counting", marker, e.Payload.Transport, e.Payload.Framing, transport)
		}
		if show(e.Payload.XTransport) != XTransport {
			t.Errorf("%s: x-transport = %s, want %q", marker, show(e.Payload.XTransport), XTransport)
		}
		if e.From != testID.From || e.Space != testID.Space || e.Type != "syslog" || e.Step != 0 {
			t.Errorf("%s: envelope identity %s/%s/%s/%d", marker, e.From, e.Space, e.Type, e.Step)
		}
		return e
	}

	e := check("via-relp-tcp", "tcp")
	eqHost("tcp", e, tcpSrc, tcpPort)
	if e.Payload.TLS != nil {
		t.Errorf("RELP in clear carries payload.tls: %s", e.bytes)
	}

	e = check("via-tls-cert", "tls")
	eqHost("tls with a certificate", e, certSrc, tlsPort)
	if tl := e.Payload.TLS; tl == nil {
		t.Error("tls with a client certificate: no payload.tls")
	} else {
		if tl.Version != "TLS1.2" && tl.Version != "TLS1.3" {
			t.Errorf("tls.version = %q", tl.Version)
		}
		if tl.Cipher == "" {
			t.Error("tls.cipher is empty")
		}
		if tl.PeerFingerprint == nil || *tl.PeerFingerprint != fingerprint(p.clientDER) {
			t.Errorf("tls.peer-fingerprint = %v, want %s", tl.PeerFingerprint, fingerprint(p.clientDER))
		}
		if tl.PeerSubject == nil || !strings.Contains(*tl.PeerSubject, "CN=rsyslog-01.intranet") {
			t.Errorf("tls.peer-subject = %v, want the client DN %s", tl.PeerSubject, p.clientSubject)
		}
	}

	e = check("via-tls-anon", "tls")
	eqHost("tls anonymous", e, anonSrc, tlsPort)
	if tl := e.Payload.TLS; tl == nil {
		t.Error("tls without a client certificate: no payload.tls")
	} else if tl.PeerFingerprint != nil || tl.PeerSubject != nil || tl.Version == "" || tl.Cipher == "" {
		t.Errorf("tls without a client certificate = %+v: version and cipher only", *tl)
	}

	// Three sessions, each an open, one syslog and a close (SPEC §14.1).
	for series, want := range map[string]string{
		`relplog_relp_frames_total{command="open"}`:                                       "3",
		`relplog_relp_frames_total{command="syslog"}`:                                     "3",
		`relplog_relp_frames_total{command="close"}`:                                      "3",
		`relplog_messages_accepted_total{transport="tcp"}`:                                "1",
		`relplog_messages_accepted_total{transport="tls"}`:                                "2",
		`relplog_config_info{ring_mode="memory",sink_type="sqlite",transports="tcp,tls"}`: "1",
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
	in := start(t, t.TempDir(), "--proxy.protocol=optional")
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

	envs := byMessage(t, in.waitRows(3))
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

// SPEC §3.2, §3.7, §6.7, invariants §20.6 and §20.7: the raw line comes back in
// payload.raw byte for byte, NUL included and framing removed; message keeps
// the NUL (escaped as \u0000 in the JSON); every promoted column is the copy of
// its payload field, and NULL -- not ” -- when the field did not come.
func TestTheDestinationRowIsFaithful(t *testing.T) {
	in := start(t, t.TempDir())

	full := "<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] a mensagem \x00 com nul"
	nil5424 := "<13>1 2026-09-12T14:33:02Z - - - - - so a mensagem"
	nopid := "<34>Oct 11 22:14:15 mymachine su: sem pid"
	nulCols := "<13>Oct 11 22:14:15 ho\x00st ap\x00p[1\x002]: nul nas colunas"
	sendRELP(t, in.app.TransportAddr("tcp"), full, nil5424, nopid, nulCols)

	rows := in.waitRows(4)
	envs := byMessage(t, rows)

	// A NUL inside a header field stays in the envelope and becomes U+FFFD in
	// the promoted column only (SPEC §3.7).
	e := messageWith(t, envs, "nul nas colunas")
	if show(e.Payload.hostname()) != "ho\x00st" || show(e.Payload.AppName) != "ap\x00p" || show(e.Payload.ProcID) != "1\x002" {
		t.Errorf("the NULs of the header fields did not reach the payload: %s", e.bytes)
	}
	for _, r := range rows {
		if r.UUID == e.UUID && (r.Hostname.String != "ho\uFFFDst" || r.AppName.String != "ap\uFFFDp" || r.ProcID.String != "1\uFFFD2") {
			t.Errorf("columns with NUL = %q %q %q, want U+FFFD in place of each NUL", r.Hostname.String, r.AppName.String, r.ProcID.String)
		}
	}

	e = messageWith(t, envs, "a mensagem \x00 com nul")
	if got := e.Payload.rawBytes(); !bytes.Equal(got, []byte(full)) {
		t.Fatalf("payload.raw changed\n got %q\nwant %q", got, full)
	}
	if e.Payload.message() != "a mensagem \x00 com nul" {
		t.Errorf("message = %q", e.Payload.message())
	}
	if !bytes.Contains(e.bytes, []byte(`a mensagem \u0000 com nul`)) {
		t.Errorf("the NUL is not escaped as \\u0000 in the envelope: %s", e.bytes)
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
	// The promoted columns themselves, not just their agreement with the
	// payload, for the one row that has them all.
	for _, r := range rows {
		if r.UUID != e.UUID {
			continue
		}
		if r.Hostname.String != "web-01" || r.AppName.String != "nginx" || r.ProcID.String != "42" ||
			r.MsgID.String != "ID7" || r.SrcIP.String != "127.0.0.1" || r.Transport != "tcp" || r.Format != "rfc5424" {
			t.Errorf("columns = %+v", r)
		}
	}

	e = messageWith(t, envs, "so a mensagem")
	if e.Payload.hostname() != nil || e.Payload.AppName != nil || e.Payload.ProcID != nil || e.Payload.MsgID != nil {
		t.Errorf("NILVALUE fields must be omitted: %s", e.bytes)
	}
	e = messageWith(t, envs, "sem pid")
	if e.Payload.ProcID != nil || e.Payload.MsgID != nil || e.Payload.AppName == nil || *e.Payload.AppName != "su" {
		t.Errorf("RFC 3164 without PID: %s", e.bytes)
	}
	// checkRow (inside byMessage) already proved that each absent field is a
	// NULL column; say it explicitly for the record.
	nulls := 0
	for _, r := range rows {
		for _, c := range []bool{r.Hostname.Valid, r.AppName.Valid, r.ProcID.Valid, r.MsgID.Valid} {
			if !c {
				nulls++
			}
		}
	}
	if nulls != 4+2+1 {
		t.Errorf("%d NULL text columns, want 7 (four for the NILVALUE line, procid and msgid for the 3164 one, msgid for the NUL one)", nulls)
	}
}

// ------------------------------------------------------------ filter + SIGHUP

// SPEC §7 and §16.3: the filter drops before the ring, and is reloaded at
// runtime -- loosened, and then reverted to its boot value, which must be
// APPLIED (the running reference); a restart-only key ([syslog]) is reported as
// ignored on EVERY reload while it differs from the boot (the boot reference).
// a.reload() is what the SIGHUP handler calls; no real signal is sent to the
// test process.
func TestFilterAndSIGHUPReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	write := func(minSev, maxBytes string) {
		t.Helper()
		body := fmt.Sprintf("[filter]\nmin_severity = %s\n\n[syslog]\nmax_message_bytes = %s\n", minSev, maxBytes)
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("err", "64KiB")
	in := start(t, dir, "--config="+ini)
	tcp := in.app.TransportAddr("tcp")
	dropped := func() uint64 { return in.app.metrics.MessagesDroppedTotal[obs.DropFilter].Load() }
	rows := func() int { return sqliteCount(t, in.eventsDB(), "syslog_events") }

	sendRELP(t, tcp, "<167>debug noise", "<163>real error")
	waitFor(t, 10*time.Second, "the debug drop and the error row", func() bool { return dropped() == 1 && rows() == 1 })
	if got := in.metric(`relplog_messages_dropped_total{reason="filter"}`); got != "1" {
		t.Fatalf(`relplog_messages_dropped_total{reason="filter"} = %s, want 1`, got)
	}

	// Loosen the filter and change a restart-only key.
	write("debug", "32KiB")
	r := in.app.reload()
	if strings.Join(r.applied, ",") != "filter.min_severity" || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	sendRELP(t, tcp, "<167>debug now wanted")
	waitFor(t, 10*time.Second, "the debug line after the reload", func() bool { return rows() == 2 })

	// The same file again: nothing to apply, and the restart-only key is
	// reported again -- every SIGHUP warns until the restart.
	if r := in.app.reload(); len(r.applied) != 0 || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// Revert the filter to its BOOT value. Compared with the boot snapshot
	// this would look unchanged and be skipped; compared with what runs, it
	// is a change and is applied.
	write("err", "32KiB")
	r = in.app.reload()
	if strings.Join(r.applied, ",") != "filter.min_severity" || strings.Join(r.ignored, ",") != "syslog.max_message_bytes" {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	sendRELP(t, tcp, "<167>debug noise again", "<163>another error")
	waitFor(t, 10*time.Second, "the reverted filter", func() bool { return dropped() == 2 && rows() == 3 })

	// Everything back to the boot values: nothing applied, nothing ignored.
	write("err", "64KiB")
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot-values reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	// And the restart-only key really was ignored: the receiver still cuts at
	// the boot limit, not at the 32 KiB of the reloaded file.
	if got := in.app.ingest.MaxMessageBytes(); got != 64<<10 {
		t.Errorf("max_message_bytes in force = %d, want the boot 65536", got)
	}
	for _, r := range sqliteRows(t, in.eventsDB()) {
		checkRow(t, r)
	}
}

// ------------------------------------------------------------ emergency

// SPEC §10.2, §12, §13.4, invariant §20.13: a destination that refuses every
// write while its health check passes makes the workers HOLD batches outside
// the ring. The shutdown collects them with the rest of the ring into an
// emergency dump and exits 3; the next life reingests the dump BEFORE any
// transport opens, the rows carry the SAME uuid and the SAME envelope bytes the
// converter produces from the dumped head + raw line -- including the first
// life's from, though another instance wrote them -- and the dump is removed.
func TestEmergencyDumpAndReingestionAreExact(t *testing.T) {
	dir := t.TempDir()
	args := append(baseArgs(dir),
		"--ring.mode=memory",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms",
	)

	// --- first life ---------------------------------------------------------
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, testID, args, fs.wrap)
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
	dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-"+testID.From+"-*.bin"))
	if len(dumps) != 1 {
		all, _ := os.ReadDir(filepath.Join(dir, "emergency"))
		t.Fatalf("dump files = %v (dir: %v), want one emg-%s-*.bin", dumps, all, testID.From)
	}
	if n := sqliteCount(t, filepath.Join(dir, "events.db"), "syslog_events"); n != 0 {
		t.Fatalf("%d rows written by a sink that refused every write", n)
	}

	// The envelopes the converter makes from the dumped records, under the
	// frozen conversion config (SPEC §3.5).
	res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(dumps[0])
	if err != nil || res.Damaged() {
		t.Fatalf("reading the dump: %v damaged=%v", err, res != nil && res.Damaged())
	}
	if len(res.Items) != len(lines) {
		t.Fatalf("the dump holds %d records, want %d", len(res.Items), len(lines))
	}
	conv := convert.New(first.app.convertConfig())
	want := map[string][]byte{}
	for _, it := range res.Items {
		env, _ := conv.Append(nil, &it.Head, it.Raw)
		want[it.Head.UUID.String()] = env
	}

	// --- second life: a working sink, ANOTHER identity -----------------------
	second := launch(t, otherID, append(args, "--shutdown.drain_timeout=5s"), nil)
	second.waitReady()
	// Reingestion runs before the transports open (SPEC §13.1 step 9): at
	// Ready, every row is already there.
	rows := sqliteRows(t, filepath.Join(dir, "events.db"))
	if len(rows) != len(lines) {
		t.Fatalf("reingested %d of %d messages", len(rows), len(lines))
	}
	for _, r := range rows {
		e := checkRow(t, r)
		w, ok := want[r.UUID]
		if !ok {
			t.Errorf("row %s has a uuid the dump did not hold", r.UUID)
			continue
		}
		if !bytes.Equal(r.Env, w) {
			t.Errorf("row %s is not the envelope of the dumped record\n got %s\nwant %s", r.UUID, r.Env, w)
		}
		if e.From != testID.From {
			t.Errorf("row %s from = %s: from is who RECEIVED (the first life), not who reingested", r.UUID, e.From)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if got := second.metric("relplog_emergency_files"); got != "0" {
		t.Errorf("relplog_emergency_files = %s after the reingestion", got)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// ------------------------------------------------------------ shutdown

// SPEC §13.4, §18.8: SIGTERM loses nothing. Every accepted message is in the
// destination after the exit, with exit 0.
func TestGracefulShutdownDeliversEverything(t *testing.T) {
	in := start(t, t.TempDir())
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
	rows := sqliteRows(t, in.eventsDB())
	if len(rows) != n {
		t.Fatalf("%d of %d accepted messages reached the destination after SIGTERM", len(rows), n)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[checkRow(t, r).Payload.message()] = true
	}
	for _, l := range lines {
		if !seen[l[strings.LastIndexByte(l, ' ')+1:]] {
			t.Errorf("%q is missing", l)
		}
	}
}

// SPEC §13.4: a second signal during the shutdown exits at once with 130.
func TestSecondSignalAborts(t *testing.T) {
	in := start(t, t.TempDir(), "--shutdown.pre_stop_delay=1h")
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

// SPEC §13.2, §13.5: configuration errors exit 1, before anything opens.
func TestBootRefusalsExitOne(t *testing.T) {
	dir := t.TempDir()
	same := filepath.Join(dir, "same.db")
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
		{"workspace with an underscore", bad, nil},
		{"workspace with an empty segment", empty, nil},
		{"ring.sqlite.path == sink.sqlite.path", testID,
			[]string{"--ring.mode=sqlite", "--ring.sqlite.path=" + same, "--sink.sqlite.path=" + same}},
		// The control: a valid identity and distinct paths boot.
		{"control", testID, []string{"--ring.mode=sqlite"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "control" {
				in := launch(t, tc.id, append(baseArgs(t.TempDir()), tc.args...), nil)
				in.waitReady()
				if code := in.stop(); code != ExitOK {
					t.Fatalf("control exited %d", code)
				}
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			code := RunContext(ctx, append(baseArgs(t.TempDir()), tc.args...), tc.id)
			if code != ExitConfig {
				t.Fatalf("exit %d, want %d", code, ExitConfig)
			}
		})
	}
	if _, err := os.Stat(same); err == nil {
		t.Error("the refused boot created the shared database file")
	}
}

// ------------------------------------------------------------ ring=sqlite

// SPEC §2.3, §5.4, §9.4, §19: with ring=sqlite the confirmation is the COMMIT.
// At the instant the "200 OK" reaches the emitter the row is already in
// ring.db, while the destination has not confirmed anything; when the
// destination confirms, the row moves to events.db and leaves ring.db.
func TestRingSQLiteConfirmsAfterTheCommit(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(true) // writes block until the gate opens
	in := launch(t, testID, append(baseArgs(dir), "--ring.mode=sqlite"), fs.wrap)
	in.waitReady()
	ringDB := in.app.cfg.RingSQL.Path

	sendRELP(t, in.app.TransportAddr("tcp"), "<13>1 2026-09-12T14:33:01Z host app - - - confirmado")
	// No polling from here: the 200 is answered after the commit, so the row
	// must be there NOW.
	if n := sqliteCount(t, ringDB, "ring"); n != 1 {
		t.Fatalf("ring.db holds %d rows at the instant of the 200, want 1", n)
	}
	waitFor(t, 10*time.Second, "a worker blocked on the destination", func() bool { return fs.attempts.Load() > 0 })
	if n := sqliteCount(t, in.eventsDB(), "syslog_events"); n != 0 {
		t.Fatalf("events.db holds %d rows before the destination confirmed", n)
	}

	fs.open()
	rows := in.waitRows(1)
	if e := checkRow(t, rows[0]); e.Payload.message() != "confirmado" {
		t.Errorf("message = %q", e.Payload.message())
	}
	waitFor(t, 10*time.Second, "the delivered row to leave ring.db", func() bool {
		return sqliteCount(t, ringDB, "ring") == 0
	})
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
}

// SPEC §9.4, §12, §13.1 step 10, §13.4: rows left in ring.db by a shutdown are
// not a loss -- exit 0, no dump, nothing counted as dropped -- and the next
// boot delivers the whole backlog BEFORE any transport opens.
func TestRingSQLiteBacklogIsDrainedBeforeTransportsOpen(t *testing.T) {
	dir := t.TempDir()
	tcpAddr := freePort(t)
	args := append(baseArgs(dir), "--ring.mode=sqlite", "--listen.tcp="+tcpAddr)

	// --- first life: the destination never confirms --------------------------
	fs := newFaultySink(true)
	first := launch(t, testID, append(args, "--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	const n = 7
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - backlog-%d", i)
	}
	sendRELP(t, tcpAddr, lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	ringDB := first.app.cfg.RingSQL.Path
	if got := sqliteCount(t, ringDB, "ring"); got != n {
		t.Fatalf("ring.db holds %d rows, want %d", got, n)
	}
	if code := first.stop(); code != ExitOK {
		t.Fatalf("exit %d, want 0: rows left in ring.db are not a failure", code)
	}
	if d := first.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != 0 {
		t.Errorf("%d rows counted as dropped although they are on disk", d)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "*.bin")); len(files) != 0 {
		t.Errorf("an emergency dump was written for rows that are on disk: %v", files)
	}
	if got := sqliteCount(t, ringDB, "ring"); got != n {
		t.Fatalf("%d rows left in ring.db, want %d", got, n)
	}

	// --- second life: every write records whether a transport was open -------
	var ready <-chan struct{}
	type observed struct {
		items      int
		ready      bool
		tcpRefused bool
	}
	seen := make(chan observed, 64)
	ws := newFaultySink(false)
	ws.onWrite = func(b *sink.Batch) {
		o := observed{items: len(b.Envelopes)}
		select {
		case <-ready:
			o.ready = true
		default:
		}
		if c, err := net.DialTimeout("tcp", tcpAddr, 200*time.Millisecond); err != nil {
			o.tcpRefused = true
		} else {
			c.Close()
		}
		seen <- o
	}
	second := launch(t, testID, args, ws.wrap, func(a *App) { ready = a.ready })
	second.waitReady()
	if got := sqliteCount(t, second.eventsDB(), "syslog_events"); got != n {
		t.Fatalf("at Ready, events.db holds %d rows, want the whole backlog of %d", got, n)
	}
	// The ring's writer runs the DELETEs of a commit asynchronously.
	waitFor(t, 10*time.Second, "the drained rows to leave ring.db", func() bool {
		return sqliteCount(t, ringDB, "ring") == 0
	})
	delivered := 0
	for len(seen) > 0 {
		o := <-seen
		if o.ready || !o.tcpRefused {
			t.Errorf("a backlog write ran with the transports open (ready=%v, tcp refused=%v)", o.ready, o.tcpRefused)
		}
		delivered += o.items
	}
	if delivered != n {
		t.Errorf("the boot drain wrote %d items, want %d", delivered, n)
	}
	for _, r := range sqliteRows(t, second.eventsDB()) {
		checkRow(t, r)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}

// ------------------------------------------------------------ ring=none

// SPEC §5.4, §9.6, §19: ring=none writes through. With sink=sqlite the "200
// OK" IS the commit in events.db, so the row is there the instant the emitter
// gets its answer; with sink=none (the transport benchmark) every message is
// converted and "written" to nowhere; and a destination's logic refusal is a
// final "500 refused by the destination" for that message alone, counted once
// as sink_logic.
func TestRingNone(t *testing.T) {
	t.Run("sink=sqlite", func(t *testing.T) {
		in := start(t, t.TempDir(), "--ring.mode=none")
		const n = 25
		lines := make([]string, n)
		for i := range lines {
			lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - none-%d", i)
		}
		sendRELP(t, in.app.TransportAddr("tcp"), lines...)
		// No waiting: answered means committed.
		rows := sqliteRows(t, in.eventsDB())
		if len(rows) != n {
			t.Fatalf("%d rows at the instant of the last 200, want %d", len(rows), n)
		}
		for _, r := range rows {
			checkRow(t, r)
		}
		if got := in.metric("relplog_events_written_total"); got != strconv.Itoa(n) {
			t.Errorf("relplog_events_written_total = %s: the write-through must be instrumented", got)
		}
		if got := in.metric(`relplog_config_info{ring_mode="none",sink_type="sqlite",transports="tcp"}`); got != "1" {
			t.Errorf("relplog_config_info = %s", got)
		}
		if code := in.stop(); code != ExitOK {
			t.Errorf("exit %d", code)
		}
	})
	t.Run("sink=none", func(t *testing.T) {
		in := start(t, t.TempDir(), "--ring.mode=none", "--sink.type=none")
		const n = 50
		lines := make([]string, n)
		for i := range lines {
			lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - bench-%d", i)
		}
		sendRELP(t, in.app.TransportAddr("tcp"), lines...)
		if got := in.app.metrics.EventsWrittenTotal.Load(); got != n {
			t.Errorf("relplog_events_written_total = %d at the last 200, want %d", got, n)
		}
		if got := in.metric(`relplog_convert_total{format="rfc5424"}`); got != strconv.Itoa(n) {
			t.Errorf(`relplog_convert_total{format="rfc5424"} = %s: sink=none must still convert`, got)
		}
		if _, err := os.Stat(filepath.Join(in.dir, "events.db")); err == nil {
			t.Error("sink=none created events.db")
		}
		if code := in.stop(); code != ExitOK {
			t.Errorf("exit %d", code)
		}
	})
	t.Run("logic refusal is a final 500", func(t *testing.T) {
		fs := newFaultySink(false)
		fs.decide = func(b *sink.Batch) error {
			for i := range b.Envelopes {
				if bytes.Contains(b.Envelopes[i].JLH, []byte("refuse-me")) {
					return &sink.Error{Class: sink.ClassLogic, Op: "write", Err: errors.New("value too long (injected)")}
				}
			}
			return nil
		}
		in := launch(t, testID, append(baseArgs(t.TempDir()), "--ring.mode=none"), fs.wrap)
		in.waitReady()
		rc := dialRELP(t, in.app.TransportAddr("tcp"))
		got := rc.transact(
			"<13>1 2026-09-12T14:33:01Z host app - - - kept-before",
			"<13>1 2026-09-12T14:33:02Z host app - - - refuse-me",
			"<13>1 2026-09-12T14:33:03Z host app - - - kept-after",
		)
		// librelp drops what it gets a 500 for (scrsp.c): the refusal is final,
		// as it must be -- a retransmission would be refused the same way.
		if want := []string{relpRSPOK, "500 refused by the destination", relpRSPOK}; strings.Join(got, "|") != strings.Join(want, "|") {
			t.Fatalf("answers %q, want %q", got, want)
		}
		rc.close()
		envs := byMessage(t, sqliteRows(t, in.eventsDB()))
		if _, ok := envs["kept-before"]; !ok || len(envs) != 2 {
			t.Errorf("rows %v, want kept-before and kept-after", keysOfEnv(envs))
		}
		if _, ok := envs["kept-after"]; !ok {
			t.Errorf("rows %v, want kept-after too", keysOfEnv(envs))
		}
		m := in.app.metrics
		if d := m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); d != 1 {
			t.Errorf(`relplog_events_dropped_total{reason="sink_logic"} = %d, want 1`, d)
		}
		if got := in.metric(`relplog_relp_responses_total{code="500"}`); got != "1" {
			t.Errorf(`relplog_relp_responses_total{code="500"} = %s, want 1`, got)
		}
		if code := in.stop(); code != ExitOK {
			t.Errorf("exit %d", code)
		}
	})
}

// ------------------------------------------------------------ admin

// labelSets is the closed set of every label SPEC §14.1 allows, by metric.
// Nothing an emitter controls may appear as a label (invariant §20.9).
var labelSets = map[string]map[string][]string{
	"relplog_messages_total":           {"transport": {"tcp", "tls"}},
	"relplog_messages_accepted_total":  {"transport": {"tcp", "tls"}},
	"relplog_messages_truncated_total": {"transport": {"tcp", "tls"}},
	// What RELP hands back to the emitter -- a held transaction left
	// unanswered -- is not a drop (SPEC §5.7): ring_full, sink_down and the
	// rest of the syslog sibling's reasons do not exist here.
	"relplog_messages_dropped_total": {"reason": {"filter", "empty", "unaddressable"}},
	"relplog_convert_total":          {"format": {"rfc5424", "rfc3164", "malformed"}},
	"relplog_convert_warnings_total": {"code": {"pri-missing", "pri-invalid", "version-unsupported",
		"header-invalid", "timestamp-missing", "timestamp-invalid", "timestamp-year-inferred",
		"timestamp-zone-inferred", "hostname-missing", "sd-invalid", "sd-duplicate-id",
		"encoding-invalid", "encoding-legacy", "truncated"}},
	"relplog_conns_active":               {"transport": {"tcp", "tls"}},
	"relplog_conns_total":                {"transport": {"tcp", "tls"}},
	"relplog_conns_refused_total":        {"transport": {"tcp", "tls"}, "reason": {"global", "per_ip", "proxy"}},
	"relplog_proxy_headers_total":        {"result": {"v1", "v2", "local", "absent", "invalid"}},
	"relplog_backpressure_seconds_total": {"transport": {"tcp", "tls"}},
	"relplog_relp_frames_total":          {"command": {"open", "syslog", "close", "unknown"}},
	"relplog_relp_responses_total":       {"code": {"200", "500"}},
	"relplog_relp_unanswered_total":      {"reason": {"backpressure", "shutdown", "conn_lost", "server_closed"}},
	"relplog_relp_frame_errors_total":    {"reason": {"malformed", "too_large"}},
	"relplog_sink_errors_total":          {"kind": {"network", "logic", "fatal", "timeout"}},
	"relplog_events_dropped_total":       {"reason": {"sink_logic", "malformed", "emergency_failed", "abort"}},
	"relplog_beacons_total":              {"result": {"ok", "error"}},
	"relplog_sink_postgres_pool_conns":   {"state": {"acquired", "idle", "max"}},
	"relplog_sink_mysql_pool_conns":      {"state": {"acquired", "idle", "max"}},
	"relplog_sink_clickhouse_pool_conns": {"state": {"acquired", "idle", "max"}},
}

// unlabelled is every series SPEC §14.1 exposes without labels, histograms
// included (as their _count).
var unlabelled = []string{
	"relplog_message_bytes_count", "relplog_convert_duration_seconds_count",
	"relplog_tls_handshake_errors_total", "relplog_confirm_duration_seconds_count",
	"relplog_relp_inflight_bytes", "relplog_relp_inflight_transactions", "relplog_relp_inflight_waits_total",
	"relplog_ring_items", "relplog_ring_bytes", "relplog_ring_capacity_items", "relplog_ring_capacity_bytes",
	"relplog_ring_quarantined_total", "relplog_ring_redis_claimed_total",
	"relplog_sink_batch_size_count", "relplog_sink_batch_bytes_count", "relplog_sink_duration_seconds_count",
	"relplog_sink_health_duration_seconds_count", "relplog_sink_up",
	"relplog_sink_sqlite_file_bytes", "relplog_sink_sqlite_rotations_total", "relplog_sink_sqlite_files_removed_total",
	"relplog_sink_postgres_duplicates_total", "relplog_sink_postgres_pool_waits_total",
	"relplog_sink_mysql_duplicates_total", "relplog_sink_mysql_pool_waits_total",
	"relplog_sink_nats_duplicates_total", "relplog_sink_nats_reconnects_total",
	"relplog_sink_clickhouse_dedup_blocks_total",
	"relplog_events_written_total", "relplog_emergency_files", "relplog_emergency_events_pending",
	"relplog_ratelimit_buckets",
	"relplog_ratelimit_held_total",
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
	fs := newFaultySink(false)
	in := launch(t, testID, append(baseArgs(t.TempDir()),
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
	in.waitRows(1)

	first, err := in.scrapeRaw()
	if err != nil {
		t.Fatal(err)
	}
	series := parseExposition(first)
	for want, v := range map[string]string{
		fmt.Sprintf(`relplog_identity_info{from=%q,space="default",agent_name=%q}`, testID.From, testID.AgentName): "1",
		`relplog_config_info{ring_mode="memory",sink_type="sqlite",transports="tcp"}`:                              "1",
		`relplog_messages_total{transport="tcp"}`:                                                                  "1",
		`relplog_messages_accepted_total{transport="tcp"}`:                                                         "1",
		`relplog_convert_total{format="rfc3164"}`:                                                                  "1",
		`relplog_events_written_total`:                                                                             "1",
		`relplog_sink_up`:                                                                                          "1",
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
	in := start(t, t.TempDir(), "--syslog.accept_malformed=false")
	sendRELP(t, in.app.TransportAddr("tcp"), "GET / HTTP/1.1", "<13>1 2026-09-12T14:33:01Z host app - - - bem-formada")
	waitFor(t, 10*time.Second, "the malformed drop and the good row", func() bool {
		return in.app.metrics.EventsDroppedTotal[obs.EventDropMalformed].Load() == 1 &&
			sqliteCount(t, in.eventsDB(), "syslog_events") == 1
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
	rows := sqliteRows(t, in.eventsDB())
	if len(rows) != 1 {
		t.Fatalf("%d rows, want only the well-formed one", len(rows))
	}
	if e := checkRow(t, rows[0]); e.Payload.message() != "bem-formada" || e.Payload.Format != "rfc5424" {
		t.Errorf("the surviving row is %s", e.bytes)
	}
}

// ------------------------------------------------------------ truncation

// SPEC §5.3 (decision 13): a message above syslog.max_message_bytes is cut,
// the envelope says truncated with the ORIGINAL size, the rest of its DATA is
// consumed without closing the session -- the frame is still answered 200 --
// and the next frame on the same session arrives intact.
func TestTruncationKeepsTheSessionInSync(t *testing.T) {
	in := start(t, t.TempDir(), "--syslog.max_message_bytes=480")
	big := "<13>1 2026-09-12T14:33:01Z host app - - - big:" + strings.Repeat("x", 2000)
	big = big[:2000]
	next := "<13>1 2026-09-12T14:33:02Z host app - - - depois-do-grande"
	sendRELP(t, in.app.TransportAddr("tcp"), big, next)

	rows := in.waitRows(2)
	var gotBig, gotNext bool
	for _, r := range rows {
		e := checkRow(t, r)
		switch {
		case strings.Contains(e.Payload.message(), "big:"):
			gotBig = true
			if !e.Payload.hasWarning("truncated") || e.Payload.Size != 2000 {
				t.Errorf("truncated row: warnings=%v size=%d", e.Payload.Parse, e.Payload.Size)
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
			t.Errorf("unexpected row %s", e.bytes)
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
