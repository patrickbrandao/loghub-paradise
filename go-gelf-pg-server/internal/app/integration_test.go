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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/sink"
)

// The in-process integration suite of SPEC §18.5: the real stack against the
// real queue, loghub.inbox on PostgreSQL. What it proves end to end are the
// promises of §2.3, §3, §4, §5, §9, §10, §12, §13, §14 and §16.3.

// ------------------------------------------------------------ transports

// SPEC §3.7.1, §4, §18.5: every transport reaches the queue, and each row
// holds the columns of its envelope -- uuid, from_uuid, space, type, level,
// sid, step, event_at + event_nsec -- with a payload that records the transport
// the document arrived with, its compression and chunk count, and the network
// addresses the socket knew (dst-ip on a wildcard UDP socket through PKTINFO,
// §4.2). There is no TLS, so no payload carries one.
func TestAllTransportsReachTheQueue(t *testing.T) {
	in := start(t, t.TempDir(), "--listen.udp=0.0.0.0:0")

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

	before := time.Now()
	// UDP to the loopback address of a 0.0.0.0 socket: one plain datagram,
	// and one gzip document in three chunks.
	udpSrc := sendUDP(t, udpAddr, gdoc("via-udp", 5))
	chunked := gdoc("chunked-over-udp "+strings.Repeat("padding ", 64), 3)
	chunkSrc := sendDatagrams(t, udpAddr, gelfChunks(0xabcdef, gzipOf(t, []byte(chunked)), 3)...)

	// TCP, \0 framing.
	c := dialTCP(t, tcpAddr)
	tcpSrc := c.LocalAddr().(*net.TCPAddr).Port
	writeNUL(t, c, gdoc("via-tcp", 2))
	c.Close()

	// HTTP, identity and gzip.
	httpURL := "http://" + in.app.TransportAddr("http") + "/gelf"
	if code, _ := postGELF(t, nil, httpURL, "", []byte(gdoc("via-http", 6))); code != 202 {
		t.Fatalf("POST /gelf = %d, want 202", code)
	}
	if code, _ := postGELF(t, nil, httpURL, "gzip", gzipOf(t, []byte(gdoc("gzip-over-http", 4)))); code != 202 {
		t.Fatalf("POST /gelf gzip = %d, want 202", code)
	}

	rows := in.waitRows(5)
	after := time.Now()
	envs := byMessage(t, rows)
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
	byUUID := map[string]pgRow{}
	for _, r := range rows {
		byUUID[r.UUID] = r
	}
	check := func(marker, transport, compression string, chunks, level int) jlhEnv {
		t.Helper()
		e := messageWith(t, envs, marker)
		if e.Payload.Transport != transport || show(e.Payload.Compression) != compression ||
			(chunks == 0) != (e.Payload.Chunks == nil) || (chunks != 0 && *e.Payload.Chunks != chunks) {
			t.Errorf("%s arrived as %s compression=%s chunks=%s, want %s/%s/%d", marker, e.Payload.Transport,
				show(e.Payload.Compression), show(e.Payload.Chunks), transport, compression, chunks)
		}
		if e.Payload.Format != "gelf" || show(e.Payload.hostname()) != "h" || show(e.Payload.GELFVersion) != "1.1" {
			t.Errorf("%s: payload %s", marker, e.bytes)
		}
		// The columns themselves, not only their agreement with the payload.
		r := byUUID[e.UUID]
		if r.From != testID.From || r.Space != testID.Space || r.Type != "gelf" || r.Step != 0 || r.Level != level {
			t.Errorf("%s: columns from=%s space=%s type=%s step=%d level=%d, want %s/%s/gelf/0/%d",
				marker, r.From, r.Space, r.Type, r.Step, r.Level, testID.From, testID.Space, level)
		}
		// event_at is the reception clock (SPEC §3.1), never the document's
		// own timestamp.
		if at := r.EventAt; at.Before(before.Add(-time.Second)) || at.After(after.Add(time.Second)) {
			t.Errorf("%s: event_at %s is not the reception instant (%s .. %s)", marker, at, before, after)
		}
		if e.SID == 0 {
			t.Errorf("%s: sid = 0: a network document has the hash of its host", marker)
		}
		return e
	}

	e := check("via-udp", "udp", "<absent>", 0, 5)
	if pktinfo {
		eqHost("udp", e, "127.0.0.1", intp(udpSrc), "127.0.0.1", intp(udpPort))
	} else {
		t.Log("PKTINFO is not available on this platform: the dst-ip assertion is skipped")
		eqHost("udp", e, "127.0.0.1", intp(udpSrc), "", intp(udpPort))
	}
	e = check("chunked-over-udp", "udp", "gzip", 3, 3)
	eqHost("udp chunked", e, "127.0.0.1", intp(chunkSrc), "", intp(udpPort))
	if e.Payload.Size != len(chunked) {
		t.Errorf("chunked size = %d, want the decompressed %d", e.Payload.Size, len(chunked))
	}

	e = check("via-tcp", "tcp", "<absent>", 0, 2)
	eqHost("tcp", e, "127.0.0.1", intp(tcpSrc), "127.0.0.1", intp(tcpPort))

	httpPort, _ := strconv.Atoi(portOf(t, in.app.TransportAddr("http")))
	e = check("via-http", "http", "<absent>", 0, 6)
	eqHost("http", e, "127.0.0.1", nil, "127.0.0.1", intp(httpPort))
	check("gzip-over-http", "http", "gzip", 0, 4)

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

// SPEC §3.2, §3.3, §3.7.1, invariants §20.6 and §20.7: a malformed document
// comes back in payload.raw byte for byte, NUL included; every field that did
// not come is absent, never ""; numbers keep their value (jsonb keeps numeric
// exactly, not its spelling); and the queue's jsonb holds what the envelope
// said except where it cannot -- U+0000, which becomes U+FFFD in the strings
// and keys while a raw with a NUL goes in base64 and stays exact.
func TestTheQueueRowIsFaithful(t *testing.T) {
	in := start(t, t.TempDir())

	full := `{"version":"1.1","host":"web-01","short_message":"a mensagem \u0000 com nul","full_message":"linha 1\nlinha 2",` +
		`"timestamp":1757687581.5,"level":5,"facility":"app","_k":"v\"q","_n\u0000x":"nul na chave","_big":1e400,"_dec":1.50,"message":{"a":[1,true]}}`
	minimal := `{"version":"1.1","host":"h","short_message":"so a mensagem"}`
	nulRaw := "nao e gelf \x00 com nul cru"
	plain := "nao e gelf, texto simples"
	sendNUL(t, in.app.TransportAddr("tcp"), full, minimal)
	sendUDP(t, in.app.TransportAddr("udp"), nulRaw)
	sendUDP(t, in.app.TransportAddr("udp"), plain)

	rows := in.waitRows(4)
	envs := byMessage(t, rows)

	e := messageWith(t, envs, "a mensagem \uFFFD com nul")
	p := e.Payload
	if p.Format != "gelf" || e.Level != 5 || p.Level.Code != 5 || p.Level.Name != "notice" || p.Size != len(full) ||
		show(p.hostname()) != "web-01" || show(p.FullMessage) != "linha 1\nlinha 2" || show(p.Facility) != "app" ||
		show(p.Timestamp) != "2025-09-12T14:33:01.500Z" {
		t.Errorf("standard fields: %s", e.bytes)
	}
	if p.Fields["k"] != `v"q` || p.Fields["n\uFFFDx"] != "nul na chave" || p.Raw != nil {
		t.Errorf("fields (a NUL in a key is U+FFFD in the queue, and a valid document has no raw): %s", e.bytes)
	}
	// jsonb keeps the value of a number exactly, not always its spelling
	// (SPEC §3.7.1): 1.50 keeps its scale, 1e400 comes back in full.
	if fmt.Sprint(p.Fields["dec"]) != "1.50" || fmt.Sprint(p.Fields["big"]) != "1"+strings.Repeat("0", 400) {
		t.Errorf("numbers: dec=%v big=%v", p.Fields["dec"], p.Fields["big"])
	}
	if _, ok := p.Extra["message"].(map[string]any); !ok || !p.hasWarning("key-unknown") {
		t.Errorf("a non-standard key without _ goes to extra as JSON: %s", e.bytes)
	}

	e = messageWith(t, envs, "so a mensagem")
	if e.Payload.FullMessage != nil || e.Payload.Timestamp != nil || e.Payload.Fields != nil ||
		e.Payload.Extra != nil || e.Payload.Raw != nil || e.Payload.Facility != nil || e.Level != 6 ||
		!e.Payload.hasWarning("level-default") {
		t.Errorf("absent fields must be omitted, level 6 by default: %s", e.bytes)
	}

	var gotNul, gotPlain bool
	for _, r := range rows {
		env := checkRow(t, r)
		if env.Payload.Format != "malformed" {
			continue
		}
		raw := env.Payload.rawBytes()
		switch {
		case bytes.Equal(raw, []byte(nulRaw)):
			gotNul = true
			if env.Payload.RawEnc != "base64" || env.Payload.Size != len(nulRaw) {
				t.Errorf("raw of a document with a NUL byte must go exact in base64: %s", env.bytes)
			}
		case bytes.Equal(raw, []byte(plain)):
			gotPlain = true
			if env.Payload.RawEnc == "base64" || show(env.Payload.ShortMessage) != "" {
				t.Errorf("raw of a plain malformed document must be the text itself: %s", env.bytes)
			}
		default:
			t.Errorf("unexpected malformed row %s", env.bytes)
		}
	}
	if !gotNul || !gotPlain {
		t.Errorf("malformed rows: nul=%v plain=%v", gotNul, gotPlain)
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
	ini := filepath.Join(dir, "gelf.ini")
	write := func(level, maxBytes string) {
		t.Helper()
		body := fmt.Sprintf("[log]\nlevel = %s\n\n[convert]\nmax_message_bytes = %s\n", level, maxBytes)
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("error", "128KiB")
	// --log.level would shadow the file (SPEC §16.1): the flag goes.
	in := launch(t, testID, append(withoutFlag(baseArgs(t, dir), "--log.level="), "--config="+ini), nil)
	in.waitReady()
	enabled := func(l slog.Level) bool { return in.app.log.Enabled(context.Background(), l) }
	if enabled(slog.LevelWarn) {
		t.Fatal("the boot did not apply log.level=error from the file")
	}

	// Raise the level and change a restart-only key.
	write("debug", "32KiB")
	r := in.app.reload()
	if strings.Join(r.applied, ",") != "log.level" || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if !enabled(slog.LevelDebug) {
		t.Error("log.level=debug is not in force after the reload")
	}

	// The same file again: nothing to apply, and the restart-only key is
	// reported again -- every SIGHUP warns until the restart.
	if r := in.app.reload(); len(r.applied) != 0 || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// Revert the level to its BOOT value. Compared with the boot snapshot this
	// would look unchanged and be skipped; compared with what runs, it is a
	// change and is applied.
	write("error", "32KiB")
	r = in.app.reload()
	if strings.Join(r.applied, ",") != "log.level" || strings.Join(r.ignored, ",") != "convert.max_message_bytes" {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if enabled(slog.LevelWarn) {
		t.Error("the reverted log.level=error is not in force")
	}

	// Everything back to the boot values: nothing applied, nothing ignored.
	write("error", "128KiB")
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot-values reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	// And the restart-only key really was ignored: the receiver still refuses
	// at the boot limit, not at the 32 KiB of the reloaded file.
	if got := in.app.ingest.MaxMessageBytes(); got != 128<<10 {
		t.Errorf("max_message_bytes in force = %d, want the boot 131072", got)
	}

	// Access logging is reloadable too.
	if err := os.WriteFile(ini, []byte("[log]\nlevel = error\naccess = true\naccess_sample = 1\n\n[convert]\nmax_message_bytes = 128KiB\n"), 0o600); err != nil {
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
// reingests the dump BEFORE any transport opens, the rows carry the SAME uuid
// and the SAME envelope the dump holds -- including the first life's from,
// though another instance wrote them -- and the dump is removed.
func TestEmergencyDumpAndReingestionAreExact(t *testing.T) {
	dir := t.TempDir()
	args := append(baseArgs(t, dir),
		"--listen.udp=",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--shutdown.drain_timeout=200ms",
	)

	// --- first life ---------------------------------------------------------
	fs := newFaultySink(false)
	fs.failWrites.Store(true)
	first := launch(t, testID, args, fs.wrap)
	first.waitReady()

	lines := []string{
		`{"version":"1.1","host":"web-01","short_message":"para-o-dump","level":3,"_k":"v","_n":1e400}`,
		`{"version":"1.0","host":"mymachine","short_message":"segunda","level":2,"facility":"auth","file":"su.c","line":230}`,
		"nao e gelf, com \x00 no meio",
		`{"version":"1.1","host":"h","short_message":"quarta \u0000 nul","x":{"deep":[1,{"a":null}]}}`,
		"{\"version\":\"1.1\",\"host\":\"h\",\"short_message\":\"quinta \xff octeto\"}",
	}
	// The document with a NUL byte cannot travel on the \0 framing (SPEC
	// §5.3): it goes as the body of a POST.
	var tcp []string
	for _, l := range lines {
		if strings.IndexByte(l, 0) >= 0 {
			if code, _ := postGELF(t, nil, "http://"+first.app.TransportAddr("http")+"/gelf", "", []byte(l)); code != 202 {
				t.Fatalf("POST = %d", code)
			}
			continue
		}
		tcp = append(tcp, l)
	}
	sendNUL(t, first.app.TransportAddr("tcp"), tcp...)
	m := first.app.metrics
	waitFor(t, 10*time.Second, "every message accepted and a failed write", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load()+m.MessagesAcceptedTotal[obs.TransportHTTP].Load() == uint64(len(lines)) &&
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
	dumps := dumpFiles(t, first.emgDir(), testID.From)
	if len(dumps) != 1 {
		all, _ := os.ReadDir(first.emgDir())
		t.Fatalf("dump files = %v (dir: %v), want one emg-%s-*.jsonl", dumps, all, testID.From)
	}
	if n := first.count(); n != 0 {
		t.Fatalf("%d rows written by a sink that refused every write", n)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != len(lines) {
		t.Fatalf("the dump holds %d envelopes, want %d", len(dumped), len(lines))
	}
	// The dump holds the envelope the worker would have written, byte for
	// byte: the same converter, the same frozen [convert].
	sizes := map[int]bool{}
	for _, e := range dumped {
		sizes[e.Payload.Size] = true
		if e.From != testID.From {
			t.Errorf("dumped from = %s, want the receiving instance", e.From)
		}
	}
	for _, l := range lines {
		if !sizes[len(l)] {
			t.Errorf("the dump has no envelope of size %d (%q)", len(l), l)
		}
	}
	// Read from the registry: the admin port closed with the process.
	if got := m.EmergencyEventsPending.Load(); got != int64(len(lines)) {
		t.Errorf("gelf_emergency_events_pending = %d after the dump, want %d", got, len(lines))
	}

	// --- second life: a working sink, ANOTHER identity -----------------------
	second := launch(t, otherID, append(args, "--shutdown.drain_timeout=5s"), nil)
	second.waitReady()
	// Reingestion runs before the transports open (SPEC §13.1 step 9): at
	// Ready, every row is already there.
	rows := second.rows()
	checkReingested(t, dumped, rows)
	for _, r := range rows {
		if r.From != testID.From {
			t.Errorf("row %s from = %s: from is who RECEIVED (the first life), not who reingested", r.UUID, r.From)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(second.emgDir(), "emg-*")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if got := second.metric("gelf_emergency_files"); got != "0" {
		t.Errorf("gelf_emergency_files = %s after the reingestion", got)
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
	dir := t.TempDir()
	args := append(baseArgs(t, dir), "--listen.udp=")
	fs := newFaultySink(true) // the gate never opens: every write hangs
	first := launch(t, testID, append(args, "--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs.wrap)
	first.waitReady()
	const n = 10
	for i := 0; i < n; i++ {
		sendNUL(t, first.app.TransportAddr("tcp"), doc(fmt.Sprintf("slow-%02d", i)))
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
	dumps := dumpFiles(t, first.emgDir(), testID.From)
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}

	second := launch(t, testID, args, nil)
	second.waitReady()
	checkReingested(t, dumped, second.rows())
	if left := dumpFiles(t, second.emgDir(), testID.From); len(left) != 0 {
		t.Errorf("the dump is still pending: %v", left)
	}
}

// SPEC §6.1, §12.1: with convert.accept_malformed=false the dump does not
// resurrect what the worker would have refused. A malformed document still in
// the ring at the shutdown is left out of the dump and counted as
// gelf_events_dropped_total{reason="malformed"}; the well-formed lines are
// dumped. One worker, held on the first message, keeps the others in the ring
// so the dump -- not a worker -- is what meets the malformed one.
func TestMalformedIsLeftOutOfTheDump(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultySink(true)
	in := launch(t, testID, append(baseArgs(t, dir), "--listen.udp=",
		"--convert.accept_malformed=false", "--sink.workers=1",
		"--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs.wrap)
	in.waitReady()
	tcp := in.app.TransportAddr("tcp")
	sendNUL(t, tcp, doc("held-by-the-worker"))
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	sendNUL(t, tcp, "GET / HTTP/1.1", doc("still-in-the-ring"))
	m := in.app.metrics
	waitFor(t, 10*time.Second, "three accepted", func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 3 })
	if got := m.ConvertTotal[obs.FormatMalformed].Load(); got != 0 {
		t.Fatalf("a worker converted the malformed line (%d): the test needs it in the ring", got)
	}
	if code := in.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	dumps := dumpFiles(t, in.emgDir(), testID.From)
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
		t.Errorf(`gelf_events_dropped_total{reason="malformed"} = %d, want 1`, d)
	}

	// The control: the same incident with accept_malformed=true dumps all three.
	dir2 := t.TempDir()
	fs2 := newFaultySink(true)
	in2 := launch(t, testID, append(baseArgs(t, dir2), "--listen.udp=", "--sink.workers=1",
		"--shutdown.drain_timeout=300ms", "--sink.op_timeout=60s"), fs2.wrap)
	in2.waitReady()
	tcp2 := in2.app.TransportAddr("tcp")
	sendNUL(t, tcp2, doc("held-by-the-worker"))
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs2.attempts.Load() > 0 })
	sendNUL(t, tcp2, "GET / HTTP/1.1", doc("still-in-the-ring"))
	waitFor(t, 10*time.Second, "three accepted", func() bool {
		return in2.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == 3
	})
	if code := in2.stop(); code != ExitSink {
		t.Fatalf("control exit %d, want %d", code, ExitSink)
	}
	dumps = dumpFiles(t, in2.emgDir(), testID.From)
	if len(dumps) != 1 || len(dumpLines(t, dumps[0])) != 3 {
		t.Errorf("control: dumps %v, want one with the three messages", dumps)
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
		lines[i] = doc(fmt.Sprintf("msg-%04d", i))
	}
	sendNUL(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "200 accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0", code)
	}
	rows := in.rows()
	if len(rows) != n {
		t.Fatalf("%d of %d accepted messages reached the destination after SIGTERM", len(rows), n)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[checkRow(t, r).Payload.message()] = true
	}
	for _, l := range lines {
		if !seen[shortOf(l)] {
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
		{"no transport", testID, []string{"--listen.udp=", "--listen.tcp=", "--listen.http="}},
		{"workspace with an underscore", bad, nil},
		{"workspace with an empty segment", empty, nil},
		{"table that is not an identifier", testID, []string{"--sink.postgres.table=loghub.\"inbox\""}},
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
	// The control: a valid identity, a transport and a plain table boot.
	t.Run("control", func(t *testing.T) {
		in := launch(t, testID, baseArgs(t, t.TempDir()), nil)
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
	"gelf_messages_total":          {"transport": {"udp", "tcp", "http"}},
	"gelf_messages_accepted_total": {"transport": {"udp", "tcp", "http"}},
	"gelf_messages_dropped_total": {"reason": {"ring_full", "sink_down", "malformed_frame", "proxy",
		"empty", "shutdown", "too_large", "decompress", "chunk_invalid", "chunk_timeout", "chunk_overflow"}},
	"gelf_convert_total":              {"format": {"gelf", "malformed"}},
	"gelf_convert_warnings_total":     {"code": convert.WarningNames()},
	"gelf_conns_active":               {"transport": {"tcp", "http"}},
	"gelf_conns_total":                {"transport": {"tcp", "http"}},
	"gelf_conns_refused_total":        {"transport": {"tcp", "http"}, "limit": {"global", "per_ip"}},
	"gelf_proxy_headers_total":        {"result": {"v1", "v2", "local", "absent", "invalid"}},
	"gelf_backpressure_seconds_total": {"transport": {"tcp"}},
	"gelf_sink_errors_total":          {"kind": {"network", "logic", "fatal", "timeout"}},
	"gelf_events_dropped_total":       {"reason": {"sink_logic", "malformed", "emergency_failed", "abort"}},
	"gelf_sink_postgres_pool_conns":   {"state": {"acquired", "idle", "max"}},
}

// unlabelled is every series SPEC §14.1 exposes without labels, histograms
// included (as their _count).
var unlabelled = []string{
	"gelf_message_bytes_count", "gelf_convert_duration_seconds_count",
	"gelf_confirm_duration_seconds_count",
	"gelf_udp_receive_errors_total", "gelf_udp_read_buffer_bytes",
	"gelf_udp_reassembly_bytes", "gelf_udp_chunk_duplicates_total",
	"gelf_ring_items", "gelf_ring_bytes", "gelf_ring_capacity_items", "gelf_ring_capacity_bytes",
	"gelf_sink_batch_size_count", "gelf_sink_batch_bytes_count", "gelf_sink_duration_seconds_count",
	"gelf_sink_health_duration_seconds_count", "gelf_sink_up",
	"gelf_sink_postgres_duplicates_total", "gelf_sink_postgres_pool_waits_total",
	"gelf_events_written_total", "gelf_emergency_files", "gelf_emergency_events_pending",
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
	in := launch(t, testID, append(baseArgs(t, t.TempDir()),
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
		`gelf_config_info{ring_mode="memory",sink_type="postgres",transports="udp,tcp,http"}`:                   "1",
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
		return in.app.metrics.EventsDroppedTotal[obs.EventDropMalformed].Load() == 1 && in.count() == 1
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
	rows := in.rows()
	if len(rows) != 1 {
		t.Fatalf("%d rows, want only the well-formed one", len(rows))
	}
	if e := checkRow(t, rows[0]); e.Payload.message() != "bem-formada" || e.Payload.Format != "gelf" {
		t.Errorf("the surviving row is %s", e.bytes)
	}
}

// ------------------------------------------------------------ truncation

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
