package app

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/sink"
)

// The in-process integration suite of SPEC §18.5: the real stack, sink=sqlite
// and sink=none, no external server. What it proves end to end are the
// promises of §2.3, §3, §4, §5, §9, §10, §12, §13, §14 and §16.3.

// ------------------------------------------------------------ transports

// SPEC §4, §18.5: every transport reaches the destination, and each envelope
// records the transport it arrived with, its compression and chunk count, the
// network addresses the socket knew (dst-ip on a wildcard UDP socket through
// PKTINFO, §4.2) and the TLS session (§4.7).
func TestAllTransportsReachSQLite(t *testing.T) {
	p := newPKI(t)
	in := start(t, t.TempDir(),
		"--listen.udp=0.0.0.0:0",
		"--listen.tls=127.0.0.1:0",
		"--listen.http=127.0.0.1:0",
		"--listen.https=127.0.0.1:0",
		"--tls.cert_file="+p.serverCert, "--tls.key_file="+p.serverKey,
		// client_auth=optional (the default) WITH a CA: a certificate that
		// comes is verified, and its absence is accepted (SPEC §4.7).
		"--tls.client_ca="+p.caFile,
	)

	pktinfo := false
	for _, nt := range in.app.transports {
		if d, ok := nt.t.(interface{ PktinfoEnabled() bool }); ok && nt.name == "udp" {
			pktinfo = d.PktinfoEnabled()
		}
	}

	udpPort, _ := strconv.Atoi(portOf(t, in.app.TransportAddr("udp")))
	udpAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(udpPort))
	tcpAddr := in.app.TransportAddr("tcp")
	tcpPort, _ := strconv.Atoi(portOf(t, tcpAddr))
	tlsAddr := in.app.TransportAddr("tls")

	// UDP to the loopback address of a 0.0.0.0 socket: one plain datagram,
	// and one gzip document in three chunks.
	udpSrc := sendUDP(t, udpAddr, doc("via-udp"))
	chunked := doc("chunked-over-udp " + strings.Repeat("padding ", 64))
	chunkSrc := sendDatagrams(t, udpAddr, gelfChunks(0xabcdef, gzipOf(t, []byte(chunked)), 3)...)

	// TCP, \0 framing.
	c := dialTCP(t, tcpAddr)
	tcpSrc := c.LocalAddr().(*net.TCPAddr).Port
	writeNUL(t, c, doc("via-tcp"))
	c.Close()

	// TLS with and without a client certificate.
	sendTLS := func(cert *tls.Certificate, msg string) {
		cfg := &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1"}
		if cert != nil {
			cfg.Certificates = []tls.Certificate{*cert}
		}
		tc, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", tlsAddr, cfg)
		if err != nil {
			t.Fatalf("TLS dial: %v", err)
		}
		defer tc.Close()
		writeNUL(t, tc, doc(msg))
	}
	clientCert := p.clientCert()
	sendTLS(&clientCert, "via-tls-cert")
	sendTLS(nil, "via-tls-anon")

	// HTTP and HTTPS.
	if code, _ := postGELF(t, nil, "http://"+in.app.TransportAddr("http")+"/gelf", "", []byte(doc("via-http"))); code != 202 {
		t.Fatalf("POST /gelf over http = %d, want 202", code)
	}
	if code, _ := postGELF(t, p.httpsClient(&clientCert), "https://"+in.app.TransportAddr("https")+"/gelf",
		"gzip", gzipOf(t, []byte(doc("over-https")))); code != 202 {
		t.Fatalf("POST /gelf over https = %d, want 202", code)
	}

	envs := byMessage(t, in.waitRows(7))
	intp := func(n int) *int { return &n }
	eqHost := func(name string, e jlhEnv, srcIP string, srcPort *int, dstIP string, dstPort *int) {
		t.Helper()
		h := e.Payload.Host
		if h == nil {
			t.Errorf("%s: no host object: %s", name, e.bytes)
			return
		}
		if show(h.SrcIP) != srcIP || (srcPort != nil && show(h.SrcPort) != strconv.Itoa(*srcPort)) ||
			(dstIP != "" && show(h.DstIP) != dstIP) || (dstPort != nil && show(h.DstPort) != strconv.Itoa(*dstPort)) {
			want := fmt.Sprintf("%s:%v -> %s:%v", srcIP, show(srcPort), dstIP, show(dstPort))
			t.Errorf("%s: host = %s:%s -> %s:%s, want %s", name,
				show(h.SrcIP), show(h.SrcPort), show(h.DstIP), show(h.DstPort), want)
		}
	}
	check := func(marker, transport, compression string, chunks int) jlhEnv {
		t.Helper()
		e := messageWith(t, envs, marker)
		if e.Payload.Transport != transport || show(e.Payload.Compression) != compression ||
			(chunks == 0) != (e.Payload.Chunks == nil) || (chunks != 0 && *e.Payload.Chunks != chunks) {
			t.Errorf("%s arrived as %s compression=%s chunks=%s, want %s/%s/%d", marker, e.Payload.Transport,
				show(e.Payload.Compression), show(e.Payload.Chunks), transport, compression, chunks)
		}
		if e.From != testID.From || e.Space != testID.Space || e.Type != "gelf" || e.Step != 0 {
			t.Errorf("%s: envelope identity %s/%s/%s/%d", marker, e.From, e.Space, e.Type, e.Step)
		}
		if e.Payload.Format != "gelf" || show(e.Payload.hostname()) != "h" || show(e.Payload.GELFVersion) != "1.1" {
			t.Errorf("%s: payload %s", marker, e.bytes)
		}
		return e
	}

	e := check("via-udp", "udp", "<absent>", 0)
	if pktinfo {
		eqHost("udp", e, "127.0.0.1", intp(udpSrc), "127.0.0.1", intp(udpPort))
	} else {
		t.Log("PKTINFO is not available on this platform: the dst-ip assertion is skipped")
		eqHost("udp", e, "127.0.0.1", intp(udpSrc), "", intp(udpPort))
	}
	e = check("chunked-over-udp", "udp", "gzip", 3)
	eqHost("udp chunked", e, "127.0.0.1", intp(chunkSrc), "", intp(udpPort))
	if e.Payload.Size != len(chunked) {
		t.Errorf("chunked size = %d, want the decompressed %d", e.Payload.Size, len(chunked))
	}

	e = check("via-tcp", "tcp", "<absent>", 0)
	eqHost("tcp", e, "127.0.0.1", intp(tcpSrc), "127.0.0.1", intp(tcpPort))

	e = check("via-tls-cert", "tls", "<absent>", 0)
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
		if tl.PeerSubject == nil || !strings.Contains(*tl.PeerSubject, "CN=gelf-client-01.intranet") {
			t.Errorf("tls.peer-subject = %v, want the client DN %s", tl.PeerSubject, p.clientSubject)
		}
	}

	e = check("via-tls-anon", "tls", "<absent>", 0)
	if tl := e.Payload.TLS; tl == nil {
		t.Error("tls without a client certificate: no payload.tls")
	} else if tl.PeerFingerprint != nil || tl.PeerSubject != nil || tl.Version == "" || tl.Cipher == "" {
		t.Errorf("tls without a client certificate = %+v: version and cipher only", *tl)
	}

	e = check("via-http", "http", "<absent>", 0)
	if e.Payload.TLS != nil {
		t.Errorf("plain http carries payload.tls: %s", e.bytes)
	}
	httpPort, _ := strconv.Atoi(portOf(t, in.app.TransportAddr("http")))
	eqHost("http", e, "127.0.0.1", nil, "127.0.0.1", intp(httpPort))

	e = check("over-https", "https", "gzip", 0)
	if tl := e.Payload.TLS; tl == nil || tl.PeerFingerprint == nil || *tl.PeerFingerprint != fingerprint(p.clientDER) {
		t.Errorf("https with a client certificate: payload.tls = %+v", tl)
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
	hdoc := func(host, msg string) string {
		return fmt.Sprintf(`{"version":"1.1","host":%q,"short_message":%q}`, host, msg)
	}

	c := dialTCP(t, addr)
	v1Peer := c.LocalAddr().(*net.TCPAddr).Port
	fmt.Fprint(c, "PROXY TCP4 203.0.113.7 198.51.100.9 51000 12201\r\n")
	writeNUL(t, c, hdoc("web-01", "via-proxy-v1"))
	c.Close()

	c = dialTCP(t, addr)
	v2Peer := c.LocalAddr().(*net.TCPAddr).Port
	c.Write(proxyV2(netip.MustParseAddrPort("192.0.2.10:40000"), netip.MustParseAddrPort("192.0.2.20:12201")))
	writeNUL(t, c, hdoc("web-02", "via-proxy-v2"))
	c.Close()

	c = dialTCP(t, addr)
	plainPeer := c.LocalAddr().(*net.TCPAddr).Port
	writeNUL(t, c, hdoc("web-03", "via-no-header"))
	c.Close()

	envs := byMessage(t, in.waitRows(3))
	for _, tc := range []struct {
		marker, hostname, src string
		srcPort               int
		dst                   string
		dstPort, peerPort     int
		proxied               bool
	}{
		{"via-proxy-v1", "web-01", "203.0.113.7", 51000, "198.51.100.9", 12201, v1Peer, true},
		{"via-proxy-v2", "web-02", "192.0.2.10", 40000, "192.0.2.20", 12201, v2Peer, true},
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
		`gelf_proxy_headers_total{result="v1"}`:     "1",
		`gelf_proxy_headers_total{result="v2"}`:     "1",
		`gelf_proxy_headers_total{result="absent"}`: "1",
	} {
		if got := in.metric(series); got != want {
			t.Errorf("%s = %s, want %s", series, got, want)
		}
	}
}

// ------------------------------------------------------------ fidelity

// SPEC §3.2, §3.3, §3.7, §6.5–§6.9: every standard key lands in its payload
// field, additional fields in fields (numbers as the text that arrived),
// unknown keys in extra; a NUL inside a string stays in the envelope (escaped
// as \u0000) and becomes U+FFFD in the promoted column only; a malformed
// document keeps its bytes in raw (base64 when not UTF-8); every promoted
// column is the copy of its payload field, and NULL when the field did not come.
func TestTheDestinationRowIsFaithful(t *testing.T) {
	in := start(t, t.TempDir())

	full := `{"version":"1.1","host":"web-01","short_message":"a mensagem \u0000 com nul",` +
		`"full_message":"detalhe\nlinha 2","level":5,"timestamp":1791028800.5,"facility":"kern",` +
		`"file":"main.go","line":42,"_app":"nginx","_n":12345678901234567890,"custom":"x"}`
	noHost := `{"version":"1.1","short_message":"sem host"}`
	nulHost := `{"version":"1.1","host":"ho\u0000st","short_message":"nul na coluna","level":2}`
	malformed := "nao e json \xff"
	sendNUL(t, in.app.TransportAddr("tcp"), full, noHost, nulHost, malformed)

	rows := in.waitRows(4)
	envs := byMessage(t, rows)

	// A NUL inside the host stays in the envelope and becomes U+FFFD in the
	// promoted column only (SPEC §3.7).
	e := messageWith(t, envs, "nul na coluna")
	if show(e.Payload.hostname()) != "ho\x00st" || e.Level != 2 || e.Payload.Level.Name != "crit" {
		t.Errorf("the NUL host or the level did not reach the payload: %s", e.bytes)
	}
	for _, r := range rows {
		if r.UUID == e.UUID && r.Hostname.String != "ho\uFFFDst" {
			t.Errorf("hostname column with NUL = %q, want U+FFFD in place of the NUL", r.Hostname.String)
		}
	}

	e = messageWith(t, envs, "a mensagem \x00 com nul")
	if !bytes.Contains(e.bytes, []byte(`a mensagem \u0000 com nul`)) {
		t.Errorf("the NUL is not escaped as \\u0000 in the envelope: %s", e.bytes)
	}
	pl := e.Payload
	if pl.Size != len(full) || pl.Format != "gelf" || pl.Raw != nil || pl.Transport != "tcp" {
		t.Errorf("size = %d format = %s raw = %s transport = %s", pl.Size, pl.Format, show(pl.Raw), pl.Transport)
	}
	if e.Level != 5 || pl.Level.Code != 5 || pl.Level.Name != "notice" {
		t.Errorf("level = %d %+v", e.Level, pl.Level)
	}
	if show(pl.FullMessage) != "detalhe\nlinha 2" || show(pl.Facility) != "kern" || show(pl.File) != "main.go" ||
		show(pl.Line) != "42" || show(pl.Timestamp) != "2026-10-03T12:00:00.500Z" || show(pl.GELFVersion) != "1.1" {
		t.Errorf("standard fields: %s", e.bytes)
	}
	if pl.Fields["app"] != "nginx" || !bytes.Contains(e.bytes, []byte(`"n":12345678901234567890`)) {
		t.Errorf("fields = %v: _app and the literal number text must survive", pl.Fields)
	}
	if pl.Extra["custom"] != "x" || !pl.hasWarning("key-unknown") {
		t.Errorf("extra = %v warnings = %+v", pl.Extra, pl.Parse)
	}
	for _, r := range rows {
		if r.UUID != e.UUID {
			continue
		}
		if r.Hostname.String != "web-01" || r.SrcIP.String != "127.0.0.1" || r.Transport != "tcp" ||
			r.Format != "gelf" || r.Level != 5 {
			t.Errorf("columns = %+v", r)
		}
	}

	e = messageWith(t, envs, "sem host")
	if e.Payload.hostname() != nil || !e.Payload.hasWarning("host-missing") || !e.Payload.hasWarning("level-default") || e.Level != 6 {
		t.Errorf("a document without host: %s", e.bytes)
	}

	// The malformed document: short-message "", level 6, raw in base64.
	var bad jlhEnv
	for _, x := range envs {
		if x.Payload.Format == "malformed" {
			bad = x
		}
	}
	if bad.UUID == "" {
		t.Fatalf("no malformed row among %d", len(rows))
	}
	if got := bad.Payload.rawBytes(); !bytes.Equal(got, []byte(malformed)) || bad.Payload.RawEnc != "base64" {
		t.Errorf("malformed raw = %q (%s), want %q in base64", got, bad.Payload.RawEnc, malformed)
	}
	if bad.Level != 6 || bad.Payload.ShortMessage == nil || *bad.Payload.ShortMessage != "" ||
		bad.Payload.Parse == nil || bad.Payload.Parse.Error == "" {
		t.Errorf("malformed envelope: %s", bad.bytes)
	}

	// checkRow (inside byMessage) already proved that each absent field is a
	// NULL column; say it explicitly for the record.
	nulls := 0
	for _, r := range rows {
		if !r.Hostname.Valid {
			nulls++
		}
	}
	if nulls != 2 {
		t.Errorf("%d NULL hostname columns, want 2 (the document without host and the malformed one)", nulls)
	}
}

// ------------------------------------------------------------ filter + SIGHUP

// SPEC §7 and §16.3: the level filter drops before the ring, and is reloaded at
// runtime -- loosened, and then reverted to its boot value, which must be
// APPLIED (the running reference); a restart-only key ([convert]) is reported
// as ignored on EVERY reload while it differs from the boot (the boot
// reference). a.reload() is what the SIGHUP handler calls; no real signal is
// sent to the test process.
func TestFilterAndSIGHUPReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "gelf.ini")
	write := func(minLevel, maxBytes string) {
		t.Helper()
		body := fmt.Sprintf("[filter]\nmin_level = %s\n\n[convert]\nmax_message_bytes = %s\n", minLevel, maxBytes)
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("err", "64KiB")
	in := start(t, dir, "--config="+ini)
	tcp := in.app.TransportAddr("tcp")
	dropped := func() uint64 { return in.app.metrics.MessagesDroppedTotal[obs.DropFilter].Load() }
	rows := func() int { return sqliteCount(t, in.eventsDB(), "gelf_events") }

	sendNUL(t, tcp, gdoc("debug noise", 7), gdoc("real error", 3))
	waitFor(t, 10*time.Second, "the debug drop and the error row", func() bool { return dropped() == 1 && rows() == 1 })
	if got := in.metric(`gelf_messages_dropped_total{reason="filter"}`); got != "1" {
		t.Fatalf(`gelf_messages_dropped_total{reason="filter"} = %s, want 1`, got)
	}

	// Loosen the filter and change a restart-only key.
	write("debug", "32KiB")
	r := in.app.reload()
	if strings.Join(r.applied, ",") != "filter.min_level" || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	sendNUL(t, tcp, gdoc("debug now wanted", 7))
	waitFor(t, 10*time.Second, "the debug document after the reload", func() bool { return rows() == 2 })

	// The same file again: nothing to apply, and the restart-only key is
	// reported again -- every SIGHUP warns until the restart.
	if r := in.app.reload(); len(r.applied) != 0 || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// Revert the filter to its BOOT value. Compared with the boot snapshot
	// this would look unchanged and be skipped; compared with what runs, it
	// is a change and is applied.
	write("err", "32KiB")
	r = in.app.reload()
	if strings.Join(r.applied, ",") != "filter.min_level" || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	sendNUL(t, tcp, gdoc("debug noise again", 7), gdoc("another error", 3))
	waitFor(t, 10*time.Second, "the reverted filter", func() bool { return dropped() == 2 && rows() == 3 })

	// Everything back to the boot values: nothing applied, nothing ignored.
	write("err", "64KiB")
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot-values reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	// And the restart-only key really was ignored: the receiver still refuses
	// at the boot limit, not at the 32 KiB of the reloaded file.
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
		"--listen.udp=",
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
		`{"version":"1.1","host":"web-01","short_message":"para-o-dump","level":5,"timestamp":1791028800.123,"_k":"v"}`,
		gdoc("segunda", 2),
		"nao e json, com \x01 no meio",
		`{"short_message":"quarta","custom":[1,2]}`,
		"{\"version\":\"1.1\",\"host\":\"h\",\"short_message\":\"quinta \xff octeto\"}",
	}
	sendNUL(t, first.app.TransportAddr("tcp"), lines...)
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
	if n := sqliteCount(t, filepath.Join(dir, "events.db"), "gelf_events"); n != 0 {
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
	if got := second.metric("gelf_emergency_files"); got != "0" {
		t.Errorf("gelf_emergency_files = %s after the reingestion", got)
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
	lines := docs(n, "msg-%04d")
	sendNUL(t, in.app.TransportAddr("tcp"), lines...)
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
	for i := 0; i < n; i++ {
		if m := fmt.Sprintf("msg-%04d", i); !seen[m] {
			t.Errorf("%q is missing", m)
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
	collide := freePort(t)
	for _, tc := range []struct {
		name string
		id   ident.Identity
		args []string
	}{
		{"no transport", testID, []string{"--listen.udp=", "--listen.tcp=", "--listen.http=", "--listen.https="}},
		// SPEC §4.1: two TCP transports on one address is a configuration
		// error, refused before anything binds.
		{"http on the tcp address", testID, []string{"--listen.tcp=" + collide, "--listen.http=" + collide}},
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

// SPEC §2.3, §9.4, §19: with ring=sqlite the confirmation is the COMMIT. At the
// instant a TCP message is accepted its row is already in ring.db, while the
// destination has not confirmed anything; when the destination confirms, the
// row moves to events.db and leaves ring.db.
func TestRingSQLiteConfirmsAfterTheCommit(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(true) // writes block until the gate opens
	in := launch(t, testID, append(baseArgs(dir), "--ring.mode=sqlite"), fs.wrap)
	in.waitReady()
	ringDB := in.app.cfg.RingSQL.Path

	sendNUL(t, in.app.TransportAddr("tcp"), doc("confirmado"))
	waitFor(t, 10*time.Second, "the message accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == 1
	})
	// No polling from here: acceptance happens after the commit, so the row
	// must be there NOW.
	if n := sqliteCount(t, ringDB, "ring"); n != 1 {
		t.Fatalf("ring.db holds %d rows at the instant of acceptance, want 1", n)
	}
	waitFor(t, 10*time.Second, "a worker blocked on the destination", func() bool { return fs.attempts.Load() > 0 })
	if n := sqliteCount(t, in.eventsDB(), "gelf_events"); n != 0 {
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
	args := append(baseArgs(dir), "--ring.mode=sqlite", "--listen.udp=", "--listen.tcp="+tcpAddr)

	// --- first life: the destination never confirms --------------------------
	fs := newFaultySink(true)
	first := launch(t, testID, append(args, "--shutdown.drain_timeout=200ms"), fs.wrap)
	first.waitReady()
	const n = 7
	sendNUL(t, tcpAddr, docs(n, "backlog-%d")...)
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
	if got := sqliteCount(t, second.eventsDB(), "gelf_events"); got != n {
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

// SPEC §9.6, §19: ring=none writes through. With sink=sqlite the TCP
// acceptance IS the commit in events.db, so the row is there the instant the
// message is counted as accepted; with sink=none (the transport benchmark)
// every message is converted and "written" to nowhere.
func TestRingNone(t *testing.T) {
	t.Run("sink=sqlite", func(t *testing.T) {
		in := start(t, t.TempDir(), "--ring.mode=none")
		const n = 25
		sendNUL(t, in.app.TransportAddr("tcp"), docs(n, "none-%d")...)
		sendUDP(t, in.app.TransportAddr("udp"), doc("none-udp"))
		waitFor(t, 10*time.Second, "every message accepted", func() bool {
			return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n &&
				in.app.metrics.MessagesAcceptedTotal[obs.TransportUDP].Load() == 1
		})
		// No waiting: accepted means committed.
		rows := sqliteRows(t, in.eventsDB())
		if len(rows) != n+1 {
			t.Fatalf("%d rows at the instant of acceptance, want %d", len(rows), n+1)
		}
		for _, r := range rows {
			checkRow(t, r)
		}
		if got := in.metric("gelf_events_written_total"); got != strconv.Itoa(n+1) {
			t.Errorf("gelf_events_written_total = %s: the write-through must be instrumented", got)
		}
		if got := in.metric(`gelf_config_info{ring_mode="none",sink_type="sqlite",transports="udp,tcp"}`); got != "1" {
			t.Errorf("gelf_config_info = %s", got)
		}
		if code := in.stop(); code != ExitOK {
			t.Errorf("exit %d", code)
		}
	})
	t.Run("sink=none", func(t *testing.T) {
		in := start(t, t.TempDir(), "--ring.mode=none", "--sink.type=none")
		const n = 50
		sendNUL(t, in.app.TransportAddr("tcp"), docs(n, "bench-%d")...)
		waitFor(t, 10*time.Second, "every message accepted and converted", func() bool {
			return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n &&
				in.app.metrics.EventsWrittenTotal.Load() == n
		})
		if got := in.metric(`gelf_convert_total{format="gelf"}`); got != strconv.Itoa(n) {
			t.Errorf(`gelf_convert_total{format="gelf"} = %s: sink=none must still convert`, got)
		}
		if _, err := os.Stat(filepath.Join(in.dir, "events.db")); err == nil {
			t.Error("sink=none created events.db")
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
	"gelf_messages_total":          {"transport": {"udp", "tcp", "tls", "http", "https"}},
	"gelf_messages_accepted_total": {"transport": {"udp", "tcp", "tls", "http", "https"}},
	"gelf_messages_dropped_total": {"reason": {"ring_full", "ring_unavailable", "sink_down", "filter",
		"ratelimit", "malformed_frame", "proxy", "empty", "shutdown", "too_large", "decompress",
		"chunk_invalid", "chunk_timeout", "chunk_overflow"}},
	"gelf_convert_total":              {"format": {"gelf", "malformed"}},
	"gelf_convert_warnings_total":     {"code": convert.WarningNames()},
	"gelf_conns_active":               {"transport": {"tcp", "tls", "http", "https"}},
	"gelf_conns_total":                {"transport": {"tcp", "tls", "http", "https"}},
	"gelf_conns_refused_total":        {"transport": {"tcp", "tls", "http", "https"}, "limit": {"global", "per_ip"}},
	"gelf_proxy_headers_total":        {"result": {"v1", "v2", "local", "absent", "invalid"}},
	"gelf_backpressure_seconds_total": {"transport": {"tcp", "tls"}},
	"gelf_sink_errors_total":          {"kind": {"network", "logic", "fatal", "timeout"}},
	"gelf_events_dropped_total":       {"reason": {"sink_logic", "malformed", "emergency_failed", "abort"}},
	"gelf_beacons_total":              {"result": {"ok", "error"}},
	"gelf_sink_postgres_pool_conns":   {"state": {"acquired", "idle", "max"}},
	"gelf_sink_mysql_pool_conns":      {"state": {"acquired", "idle", "max"}},
	"gelf_sink_clickhouse_pool_conns": {"state": {"acquired", "idle", "max"}},
}

// unlabelled is every series SPEC §14.1 exposes without labels, histograms
// included (as their _count).
var unlabelled = []string{
	"gelf_message_bytes_count", "gelf_convert_duration_seconds_count",
	"gelf_tls_handshake_errors_total", "gelf_confirm_duration_seconds_count",
	"gelf_udp_receive_errors_total", "gelf_udp_read_buffer_bytes",
	"gelf_udp_reassembly_bytes", "gelf_udp_chunk_duplicates_total",
	"gelf_ring_items", "gelf_ring_bytes", "gelf_ring_capacity_items", "gelf_ring_capacity_bytes",
	"gelf_ring_quarantined_total", "gelf_ring_redis_claimed_total",
	"gelf_sink_batch_size_count", "gelf_sink_batch_bytes_count", "gelf_sink_duration_seconds_count",
	"gelf_sink_health_duration_seconds_count", "gelf_sink_up",
	"gelf_sink_sqlite_file_bytes", "gelf_sink_sqlite_rotations_total", "gelf_sink_sqlite_files_removed_total",
	"gelf_sink_postgres_duplicates_total", "gelf_sink_postgres_pool_waits_total",
	"gelf_sink_mysql_duplicates_total", "gelf_sink_mysql_pool_waits_total",
	"gelf_sink_nats_duplicates_total", "gelf_sink_nats_reconnects_total",
	"gelf_sink_clickhouse_dedup_blocks_total",
	"gelf_events_written_total", "gelf_emergency_files", "gelf_emergency_events_pending",
	"gelf_ratelimit_buckets",
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

	sendNUL(t, in.app.TransportAddr("tcp"), `{"version":"1.1","host":"evil-host","short_message":"for the metrics","_evilapp":"x"}`)
	in.waitRows(1)

	first, err := in.scrapeRaw()
	if err != nil {
		t.Fatal(err)
	}
	series := parseExposition(first)
	for want, v := range map[string]string{
		fmt.Sprintf(`gelf_identity_info{from=%q,space="default",agent_name=%q}`, testID.From, testID.AgentName): "1",
		`gelf_config_info{ring_mode="memory",sink_type="sqlite",transports="udp,tcp"}`:                          "1",
		`gelf_messages_total{transport="tcp"}`:                                                                  "1",
		`gelf_messages_accepted_total{transport="tcp"}`:                                                         "1",
		`gelf_convert_total{format="gelf"}`:                                                                     "1",
		`gelf_events_written_total`:                                                                             "1",
		`gelf_sink_up`:                                                                                          "1",
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
		if !strings.HasPrefix(name, "gelf_") {
			t.Errorf("series without the gelf_ prefix: %s", s)
		}
		if len(labels) == 0 {
			continue
		}
		switch name {
		case "gelf_identity_info", "gelf_config_info", "gelf_build_info":
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
	if b := series[fmt.Sprintf(`gelf_build_info{version="dev",commit="none",go=%q}`, runtime.Version())]; b != "1" {
		t.Errorf("gelf_build_info is missing or not 1")
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
	if got := in.metric("gelf_sink_up"); got != "0" {
		t.Errorf("gelf_sink_up = %s while DOWN", got)
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

// SPEC §6.1, §16.5: with convert.accept_malformed=false a malformed document is
// converted, counted as gelf_events_dropped_total{reason="malformed"} and never
// written; the well-formed document beside it is.
func TestMalformedIsRefusedWhenNotAccepted(t *testing.T) {
	in := start(t, t.TempDir(), "--convert.accept_malformed=false")
	sendNUL(t, in.app.TransportAddr("tcp"), "GET / HTTP/1.1", doc("bem-formada"))
	waitFor(t, 10*time.Second, "the malformed drop and the good row", func() bool {
		return in.app.metrics.EventsDroppedTotal[obs.EventDropMalformed].Load() == 1 &&
			sqliteCount(t, in.eventsDB(), "gelf_events") == 1
	})
	if got := in.metric(`gelf_events_dropped_total{reason="malformed"}`); got != "1" {
		t.Errorf(`gelf_events_dropped_total{reason="malformed"} = %s`, got)
	}
	if got := in.metric(`gelf_convert_total{format="malformed"}`); got != "1" {
		t.Errorf(`gelf_convert_total{format="malformed"} = %s: the document must be converted before it is refused`, got)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	rows := sqliteRows(t, in.eventsDB())
	if len(rows) != 1 {
		t.Fatalf("%d rows, want only the well-formed one", len(rows))
	}
	if e := checkRow(t, rows[0]); e.Payload.message() != "bem-formada" || e.Payload.Format != "gelf" {
		t.Errorf("the surviving row is %s", e.bytes)
	}
}

// ------------------------------------------------------------ size

// SPEC §5.3, §5.5 (decision 10): a document above convert.max_message_bytes is
// never truncated -- cutting JSON makes it invalid -- but refused and counted
// as too_large; on TCP the excess is consumed up to the next \0 without
// closing the connection, and the next document on the same connection arrives
// intact. The value of max_message_bytes reaches the receiver through the app.
func TestTooLargeKeepsTheFramingInSync(t *testing.T) {
	in := start(t, t.TempDir(), "--convert.max_message_bytes=1KiB")
	if got := in.app.ingest.MaxMessageBytes(); got != 1024 {
		t.Fatalf("the receiver cuts at %d, want convert.max_message_bytes = 1024", got)
	}
	big := doc("big:" + strings.Repeat("x", 2000))
	fits := doc("fits:" + strings.Repeat("y", 900))
	next := doc("depois-do-grande")
	sendNUL(t, in.app.TransportAddr("tcp"), big, fits, next)

	rows := in.waitRows(2)
	var gotFits, gotNext bool
	for _, r := range rows {
		e := checkRow(t, r)
		switch {
		case strings.HasPrefix(e.Payload.message(), "fits:"):
			gotFits = true
			if e.Payload.Size != len(fits) {
				t.Errorf("the document under the limit has size %d, want %d", e.Payload.Size, len(fits))
			}
		case e.Payload.message() == "depois-do-grande":
			gotNext = true
			if e.Payload.Parse != nil || e.Payload.Size != len(next) {
				t.Errorf("the frame after the refused one is not intact: %s", e.bytes)
			}
		default:
			t.Errorf("unexpected row %s", e.bytes)
		}
	}
	if !gotFits || !gotNext || len(rows) != 2 {
		t.Fatalf("fits=%v next=%v rows=%d", gotFits, gotNext, len(rows))
	}
	if got := in.metric(`gelf_messages_dropped_total{reason="too_large"}`); got != "1" {
		t.Errorf(`gelf_messages_dropped_total{reason="too_large"} = %s`, got)
	}
	if got := in.metric(`gelf_messages_dropped_total{reason="malformed_frame"}`); got != "0" {
		t.Errorf("the connection was treated as broken: malformed_frame = %s", got)
	}
	if got := in.metric(`gelf_conns_total{transport="tcp"}`); got != "1" {
		t.Errorf(`gelf_conns_total{transport="tcp"} = %s: the oversized document must not close the connection`, got)
	}
}
