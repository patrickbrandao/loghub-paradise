package receiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
)

func startTCP(t *testing.T, h *harness, cfg TCPConfig) *Stream {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.BackpressureTimeout == 0 {
		cfg.BackpressureTimeout = 2 * time.Second
	}
	tr, err := NewTCP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); tr.Close() })
	tr.Start(ctx)
	return tr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// expectClosed waits for the server to close the connection.
func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	for {
		_, err := c.Read(buf)
		if err == nil {
			continue
		}
		if isTimeout(err) {
			t.Fatal("the server did not close the connection")
		}
		return
	}
}

func TestTCPBothFramings(t *testing.T) {
	for name, tc := range map[string]struct {
		payload string
		framing item.Framing
	}{
		"lf":    {"<13>um\n<14>dois\n", item.FramingNonTransparent},
		"octet": {octet("<13>um", "<14>dois"), item.FramingOctetCounting},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, 100)
			tr := startTCP(t, h, TCPConfig{})

			c := dial(t, tr.Addr())
			if _, err := c.Write([]byte(tc.payload)); err != nil {
				t.Fatal(err)
			}
			h.waitItems(t, 2)

			items := h.drainRing()
			want := map[string]bool{"<13>um": true, "<14>dois": true}
			local := c.LocalAddr().(*net.TCPAddr).AddrPort()
			remote := c.RemoteAddr().(*net.TCPAddr).AddrPort()
			for _, it := range items {
				hd := it.Head
				if hd.Transport != item.TransportTCP || hd.Framing != tc.framing {
					t.Errorf("wire = %v/%v", hd.Transport, hd.Framing)
				}
				if !want[string(it.Raw)] {
					t.Errorf("unexpected line %q", it.Raw)
				}
				// The payload's two ends are the socket's (SPEC §4.4).
				if netip.AddrPortFrom(hd.SrcIP, hd.SrcPort) != local ||
					netip.AddrPortFrom(hd.DstIP, hd.DstPort) != remote {
					t.Errorf("addresses %v:%d -> %v:%d, want %v -> %v",
						hd.SrcIP, hd.SrcPort, hd.DstIP, hd.DstPort, local, remote)
				}
				if hd.Proxied() {
					t.Errorf("plain TCP with a proxy peer: %+v", hd)
				}
			}
			for tr := obs.Transport(0); tr < obs.NumTransports; tr++ {
				want := int64(0)
				if tr == obs.TransportTCP {
					want = 1
				}
				if got := h.m.ConnsTotal[tr].Load(); int64(got) != want {
					t.Fatalf("conns_total{%s} = %d, want %d", tr, got, want)
				}
			}
		})
	}
}

// SPEC §5.2: the log4j line as the first frame keeps the connection on LF, and
// the connection survives to carry the next lines.
func TestTCPLog4jFirstFrameKeepsTheConnection(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "2026-09-13 10:00:00,123 ERROR boom\n")
	h.waitItems(t, 1)
	fmt.Fprint(c, "2026-09-13 10:00:01,456 WARN again\n")
	h.waitItems(t, 2)
	for _, it := range h.drainRing() {
		if it.Head.Framing != item.FramingNonTransparent || !strings.HasPrefix(string(it.Raw), "2026-09-13") {
			t.Fatalf("item %q framing %v", it.Raw, it.Head.Framing)
		}
	}
	if h.m.MessagesDroppedTotal[obs.DropMalformedFrame].Load() != 0 {
		t.Fatal("a log4j line was treated as a broken frame")
	}
}

// SPEC §5.5: an oversized message is truncated and the connection carries on.
func TestTCPTruncationKeepsTheConnection(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 100, cfg)
	tr := startTCP(t, h, TCPConfig{})
	c := dial(t, tr.Addr())
	long := "<13>" + strings.Repeat("L", 996)
	fmt.Fprint(c, octet(long, "<14>after"))
	h.waitItems(t, 2)
	items := h.drainRing()
	if string(items[0].Raw) != long[:480] || items[0].Head.Size != 1000 {
		t.Fatalf("first: %d bytes, size %d", len(items[0].Raw), items[0].Head.Size)
	}
	if string(items[1].Raw) != "<14>after" {
		t.Fatalf("second = %q: the stream lost sync", items[1].Raw)
	}
	if h.m.MessagesTruncatedTotal[obs.TransportTCP].Load() != 1 {
		t.Fatal("truncation not counted")
	}
}

// SPEC §5.4: a connection closed mid-frame delivers what arrived with the
// declared size.
func TestTCPMidFrameCloseDeliversWhatArrived(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{Framing: FramingOctet})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "100 <13>curto")
	c.(*net.TCPConn).CloseWrite()
	h.waitItems(t, 1)
	it := h.drainRing()[0]
	if string(it.Raw) != "<13>curto" || it.Head.Size != 100 {
		t.Fatalf("raw %q size %d", it.Raw, it.Head.Size)
	}
}

// SPEC §5.3, §5.6: empty frames are counted as reason="empty".
func TestTCPEmptyFramesAreCounted(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>a\n\n\r\n<13>b\n")
	h.waitItems(t, 2)
	waitFor(t, "two empty drops", func() bool { return h.m.MessagesDroppedTotal[obs.DropEmpty].Load() == 2 })
}

// This is the 429 of this protocol, and it is better than one: nothing is lost.
func TestTCPBackpressureLosesNothing(t *testing.T) {
	h := newHarness(t, 2) // a tiny ring, so it fills at once
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{BackpressureTimeout: 10 * time.Second}})
	c := dial(t, tr.Addr())
	const n = 20
	go func() {
		for i := 0; i < n; i++ {
			fmt.Fprintf(c, "<13>msg-%02d\n", i)
		}
	}()
	// A consumer that drains slowly, so the reader has to stall repeatedly.
	got := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		for _, it := range h.ring.Drain() {
			got[string(it.Raw)] = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(got) != n {
		t.Fatalf("received %d of %d messages through backpressure", len(got), n)
	}
	// Nothing was dropped: the emitter was held, not refused.
	if d := h.m.MessagesDroppedTotal[obs.DropRingFull].Load(); d != 0 {
		t.Fatalf("%d messages dropped while backpressure was available", d)
	}
	if got := h.m.MessagesTotal[obs.TransportTCP].Load(); got != n {
		t.Fatalf("arrivals = %d, want %d: a retry was counted as an arrival", got, n)
	}
	if h.m.BackpressureSecondsHund[obs.TransportTCP].Load() == 0 {
		t.Log("note: the ring drained fast enough that no stall was recorded")
	}
}

// SPEC §5.7: past backpressure_timeout the connection is closed and the
// message in hand is counted with its reason.
func TestTCPBackpressureTimeoutCloses(t *testing.T) {
	h := newHarness(t, 1)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{BackpressureTimeout: 200 * time.Millisecond}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>fills\n<13>held\n")
	expectClosed(t, c)
	waitFor(t, "the ring_full drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropRingFull].Load() == 1 })
	if h.m.BackpressureSecondsHund[obs.TransportTCP].Load() == 0 {
		t.Fatal("the stall was not recorded")
	}
}

// A sink DOWN holds the emitter like a full ring, and is reported as
// sink_down when the timeout gives up.
func TestTCPSinkDownHoldsThenCloses(t *testing.T) {
	h := newHarness(t, 10)
	h.sink.up.Store(false)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{BackpressureTimeout: 200 * time.Millisecond}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>x\n")
	expectClosed(t, c)
	waitFor(t, "the sink_down drop", func() bool { return h.m.MessagesDroppedTotal[obs.DropSinkDown].Load() == 1 })
}

// A desynchronised stream ends the connection: there is no way to find the next
// message boundary, and guessing would write fragments as messages.
func TestTCPMalformedFrameClosesTheConnection(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	c := dial(t, tr.Addr())
	// The first frame confirms octet-counting; then the count is not a number.
	fmt.Fprint(c, octet("<13>ok")+"12x garbage")
	expectClosed(t, c)
	waitFor(t, "the malformed_frame drop", func() bool {
		return h.m.MessagesDroppedTotal[obs.DropMalformedFrame].Load() == 1
	})
	if items := h.drainRing(); len(items) != 1 || string(items[0].Raw) != "<13>ok" {
		t.Fatalf("items = %v", items)
	}
}

// The idle timeout closes a silent connection, and is not an error.
func TestTCPIdleTimeout(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 100 * time.Millisecond}})
	c := dial(t, tr.Addr())
	expectClosed(t, c)
	waitFor(t, "the connection to end", func() bool { return h.m.ConnsActive[obs.TransportTCP].Load() == 0 })
}

// Close must not wait for an idle connection's two-minute timeout.
func TestTCPCloseUnblocksIdleReaders(t *testing.T) {
	h := newHarness(t, 100)
	tr, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{
		Addr: "127.0.0.1:0", IdleTimeout: time.Hour}}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>x\n")
	h.waitItems(t, 1)
	cancel()
	done := make(chan struct{})
	go func() { tr.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited on an idle reader")
	}
}

// SPEC §14.1: syslog_confirm_duration_seconds is observed on tcp for accepted
// messages, from the frame being whole to the ring's guarantee.
func TestConfirmDurationIsObservedOnTCP(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "<13>one\n<13>two\n")
	h.waitItems(t, 2)
	waitFor(t, "two observations", func() bool { return h.m.ConfirmDuration.Count() == 2 })
}

func TestTCPPerIPLimit(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{MaxConnsPerIP: 2}})
	for i := 0; i < 2; i++ {
		c := dial(t, tr.Addr())
		// Send something so the server has certainly registered the connection.
		fmt.Fprintf(c, "<13>conn-%d\n", i)
	}
	h.waitItems(t, 2)
	// The third is accepted by the kernel and closed immediately: blocking would
	// stall every other address behind one abusive peer.
	c := dial(t, tr.Addr())
	expectClosed(t, c)
	waitFor(t, "the per_ip refusal", func() bool {
		return h.m.ConnsRefusedTotal[obs.TransportTCP][obs.ConnLimitPerIP].Load() == 1
	})
}

// The global ceiling parks the accept loop, which is counted; the waiting
// connection is served as soon as a slot frees.
func TestTCPGlobalLimitParksAndCounts(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{MaxConns: 1}})
	first := dial(t, tr.Addr())
	fmt.Fprint(first, "<13>first\n")
	h.waitItems(t, 1)

	second := dial(t, tr.Addr()) // completes in the kernel backlog
	fmt.Fprint(second, "<13>second\n")
	waitFor(t, "the global park", func() bool {
		return h.m.ConnsRefusedTotal[obs.TransportTCP][obs.ConnLimitGlobal].Load() >= 1
	})
	time.Sleep(100 * time.Millisecond)
	if h.ring.Stats().Items != 1 {
		t.Fatal("the second connection was served past max_conns")
	}
	first.Close()
	h.waitItems(t, 2)
}

// ------------------------------------------------------------------ limiter

// stubListener hands out pre-made connections and then reports closed. Every
// connection shares the zero peer key, so a per-IP cap of 1 refuses all but
// the first -- which is exactly what a reconnecting abuser looks like.
type stubListener struct {
	conns []net.Conn
	next  int
}

func (s *stubListener) Accept() (net.Conn, error) {
	if s.next >= len(s.conns) {
		return nil, net.ErrClosed
	}
	c := s.conns[s.next]
	s.next++
	return c, nil
}
func (s *stubListener) Close() error   { return nil }
func (s *stubListener) Addr() net.Addr { return &net.TCPAddr{} }

func pipeConn() net.Conn {
	a, b := net.Pipe()
	go func() { _ = b.Close() }()
	return a
}

// The per-IP refusal used to call Accept recursively, one stack frame per
// refused connection. A peer reconnecting past its cap in a tight loop -- the
// case the cap exists for -- would grow the stack until the runtime threw. With
// a tight stack limit, twenty thousand refusals prove the loop.
func TestLimitListenerRefusesWithoutRecursing(t *testing.T) {
	old := debug.SetMaxStack(256 << 10)
	defer debug.SetMaxStack(old)

	const refusals = 20000
	st := &stubListener{}
	st.conns = append(st.conns, pipeConn()) // the one that gets the slot
	for i := 0; i < refusals; i++ {
		st.conns = append(st.conns, pipeConn())
	}
	var counted int
	ln := newConnLimiter(0, 1, func(l obs.ConnLimit) {
		if l == obs.ConnLimitPerIP {
			counted++
		}
	}).wrap(st)

	first, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after %d refusals", err, refusals)
	}
	if st.next != refusals+1 {
		t.Fatalf("listener served %d connections, want %d", st.next, refusals+1)
	}
	if counted != refusals {
		t.Fatalf("%d refusals counted, want %d", counted, refusals)
	}
}

// SPEC §4.4: the ceilings are per transport. Two listeners of one transport
// share one limiter, or max_conns is silently multiplied by the listener count.
func TestConnLimitsAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(1, 0, nil)
	st1 := &stubListener{conns: []net.Conn{pipeConn()}}
	st2 := &stubListener{conns: []net.Conn{pipeConn()}}
	ln1, ln2 := lim.wrap(st1), lim.wrap(st2)

	a1, err := ln1.Accept()
	if err != nil {
		t.Fatal(err)
	}
	second := make(chan struct{})
	go func() {
		_, _ = ln2.Accept()
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("listener 2 accepted past the global cap; the limiter is not shared")
	case <-time.After(150 * time.Millisecond):
	}
	_ = a1.Close()
	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("listener 2 did not resume after listener 1 freed the slot")
	}
}

// A shutdown at max_conns must not wait for a client to free a slot: closing
// the listener unblocks the parked Accept.
func TestLimitListenerCloseUnblocksAccept(t *testing.T) {
	lim := newConnLimiter(1, 0, nil)
	st := &stubListener{conns: []net.Conn{pipeConn()}}
	ln := lim.wrap(st)
	if _, err := ln.Accept(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept stayed parked on the global cap after Close")
	}
}

// SPEC §4.4: the per-IP ceiling is per transport too. net.Pipe peers share the
// zero address, so to the limiter every stub connection is one IP: the second
// listener must refuse its connection and come back with its own exhaustion
// instead of a second accept.
func TestConnLimitsPerIPAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(0, 1, nil)
	st1 := &stubListener{conns: []net.Conn{pipeConn()}}
	st2 := &stubListener{conns: []net.Conn{pipeConn()}}
	ln1, ln2 := lim.wrap(st1), lim.wrap(st2)

	a1, err := ln1.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ln2.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener 2: err = %v, want the per-IP refusal followed by ErrClosed", err)
	}
	if st2.next != 1 {
		t.Fatalf("listener 2 served %d connections, want exactly the one refused", st2.next)
	}
	lim.mu.Lock()
	n := lim.byIP[netip.Addr{}]
	lim.mu.Unlock()
	if n != 1 {
		t.Fatalf("per-IP count = %d after one accept and one refusal, want 1", n)
	}

	_ = a1.Close()
	lim.mu.Lock()
	_, still := lim.byIP[netip.Addr{}]
	lim.mu.Unlock()
	if still {
		t.Fatal("the per-IP entry survived the last connection's Close")
	}
}

// With both ceilings on, a connection refused by the per-IP limit has ALREADY
// taken a global slot on its way in; that slot must come back, or a burst from
// one address would consume the whole transport.
func TestBothCeilingsAreReleasedExactlyOnce(t *testing.T) {
	lim := newConnLimiter(2, 1, nil)
	st := &stubListener{conns: []net.Conn{pipeConn(), pipeConn(), pipeConn()}}
	ln := lim.wrap(st)

	a1, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after the refusals", err)
	}
	if held := len(lim.sem); held != 1 {
		t.Fatalf("%d global slots held after the refusals, want 1 (the live connection)", held)
	}
	_ = a1.Close()
	_ = a1.Close() // a second Close must not release a second time
	if held := len(lim.sem); held != 0 {
		t.Fatalf("%d global slots held after the last Close, want 0", held)
	}
	lim.mu.Lock()
	left := len(lim.byIP)
	lim.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d per-IP entries left after the last Close, want 0", left)
	}
}

// The wiring, not the mechanism: every listener a transport opens is wrapped by
// the SAME limiter. A limiter created inside the listener loop would pass the
// unit tests above and still multiply the ceilings by listen.tcp_listeners.
func TestTCPListenersShareOneLimiter(t *testing.T) {
	// SO_REUSEPORT on port 0 would give each socket its own port; take one.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	h := newHarness(t, 64)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{Addr: addr, Listeners: 2, MaxConns: 1}})
	if len(tr.lns) != 2 {
		t.Fatalf("%d listeners, want 2", len(tr.lns))
	}
	var shared *connLimiter
	for i, l := range tr.lns {
		ll, ok := l.(*limitListener)
		if !ok {
			t.Fatalf("listener %d is not under the limiter", i)
		}
		if shared == nil {
			shared = ll.cl
		} else if ll.cl != shared {
			t.Fatal("the listeners hold different limiters; max_conns would be per listener again")
		}
	}
}

// The keep-alive settings of serve() are applied through the limiter's
// wrapper: a *limitConn is not a *net.TCPConn, and a plain type assertion on
// the wrapper silently skipped them whenever a ceiling was configured.
func TestKeepAliveReachesTheSocketUnderTheLimiter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, ok := tcpConnOf(&limitConn{Conn: c, cl: newConnLimiter(1, 0, nil)}); !ok {
		t.Fatal("tcpConnOf did not reach the TCP socket behind the limiter's wrapper")
	}
	if _, ok := tcpConnOf(pipeConn()); ok {
		t.Fatal("tcpConnOf reported a TCP socket for a pipe")
	}
}

func TestNewTCPRefusesAProxyWithoutTrustedNetworks(t *testing.T) {
	h := newHarness(t, 1)
	_, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{
		Addr: "127.0.0.1:0", Proxy: ProxyConfig{Mode: ProxyOptional}}}, h.in, h.m, logger())
	if err == nil {
		t.Fatal("proxy.protocol=optional without proxy.trusted was accepted (SPEC §13.2: exit 1)")
	}
}
