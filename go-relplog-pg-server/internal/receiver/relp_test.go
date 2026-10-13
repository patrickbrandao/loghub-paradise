package receiver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// gateRing holds every Enqueue until the test opens its gate for it, so a
// test can watch what the session does while a transaction is undecided.
type gateRing struct {
	ring.Ring
	mu      sync.Mutex
	entered atomic.Int64
	gates   []chan error
	raws    []string
	arrived chan struct{}
}

func newGateRing(inner ring.Ring) *gateRing {
	return &gateRing{Ring: inner, arrived: make(chan struct{}, 1024)}
}

func (g *gateRing) Enqueue(ctx context.Context, it *item.Item) error {
	gate := make(chan error, 1)
	g.mu.Lock()
	g.gates = append(g.gates, gate)
	g.raws = append(g.raws, string(it.Raw))
	g.mu.Unlock()
	g.entered.Add(1)
	g.arrived <- struct{}{}
	err := <-gate
	if err != nil {
		return err
	}
	return g.Ring.Enqueue(ctx, it)
}

// release lets the i-th Enqueue finish with err.
func (g *gateRing) release(i int, err error) {
	g.mu.Lock()
	gate := g.gates[i]
	g.mu.Unlock()
	gate <- err
}

// releaseRaw lets the Enqueue of the message raw finish with err. The
// transactions of a window race into the ring, so arrival order is not the
// order they were sent in.
func (g *gateRing) releaseRaw(t *testing.T, raw string, err error) {
	t.Helper()
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, r := range g.raws {
		if r == raw {
			g.gates[i] <- err
			return
		}
	}
	t.Fatalf("%q never reached the ring", raw)
}

func (g *gateRing) wait(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-g.arrived:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d transactions reached the ring", i, n)
		}
	}
}

func gatedHarness(t *testing.T) (*harness, *gateRing) {
	t.Helper()
	h := newHarness(t, 1000)
	g := newGateRing(h.ring)
	h.in = NewIngest(g, h.sink, h.m, logger(), h.cfg)
	return h, g
}

func TestRELPSessionHappyPath(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>one", "<13>two")
	got := rc.responses(2)
	for _, id := range ids {
		if got[id] != "200 OK" {
			t.Fatalf("txnr %d answered %q", id, got[id])
		}
	}
	h.waitItems(t, 2)
	n := rc.send("close", "")
	rc.expect(n, "")
	// librelp's server ends the session after the close answer, with no
	// serverclose (cclose.c).
	rc.expectEOF()

	m := h.m
	if m.RELPFramesTotal[obs.RELPOpen].Load() != 1 || m.RELPFramesTotal[obs.RELPSyslog].Load() != 2 ||
		m.RELPFramesTotal[obs.RELPClose].Load() != 1 {
		t.Fatal("frames not counted by command")
	}
	waitFor(t, "two 200 counted", func() bool { return m.RELPResponsesTotal[obs.RELPCode200].Load() == 3 })
	if m.ConfirmDuration.Count() != 2 {
		t.Fatalf("confirm duration observed %d times, want 2", m.ConfirmDuration.Count())
	}
	for _, it := range h.drainRing() {
		if it.Head.Framing != item.FramingOctetCounting || it.Head.Transport != item.TransportTCP {
			t.Fatalf("head %+v: RELP is tcp/octet-counting", it.Head)
		}
	}
}

// The open answer is librelp's own shape, with this build's relp_software.
func TestRELPOpenAdvertisesTheSoftware(t *testing.T) {
	h := newHarness(t, 10)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Software: "go-relplog-server,v9"}}})
	rc := newRELPClient(t, dial(t, tr.Addr()))
	n := rc.send("open", relpTestOffers)
	rc.expect(n, "200 OK\nrelp_version=0\nrelp_software=go-relplog-server,v9\ncommands=syslog")
}

// SPEC §5.4, decision 13: syslog before open gets librelp's "command
// disabled", and the session goes on; a second open ends it.
func TestRELPCommandOrder(t *testing.T) {
	h := newHarness(t, 10)
	tr := startTCP(t, h, TCPConfig{})
	rc := newRELPClient(t, dial(t, tr.Addr()))
	n := rc.send("syslog", "<13>too early")
	rc.expect(n, "500 command disabled")
	rc.open()
	n = rc.send("syslog", "<13>now")
	rc.expect(n, "200 OK")
	n = rc.send("open", relpTestOffers)
	rc.expect(n, "500 protocol error: connection already open")
	rc.expectServerClose()
	if h.ring.Stats().Items != 1 {
		t.Fatalf("%d items, want only the one after open", h.ring.Stats().Items)
	}
}

// An unknown command is a client extension, not a broken stream.
func TestRELPUnknownCommandKeepsTheConnection(t *testing.T) {
	h := newHarness(t, 10)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	n := rc.send("starttls", "")
	rc.expect(n, "500 command not supported")
	n = rc.send("syslog", "<13>still here")
	rc.expect(n, "200 OK")
	if h.m.RELPFramesTotal[obs.RELPUnknown].Load() != 1 {
		t.Fatal("unknown command not counted")
	}
}

// The heart of the server: no 200 before the ring's guarantee.
func TestRELPAcknowledgesOnlyAfterTheRing(t *testing.T) {
	h, g := gatedHarness(t)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>held")
	g.wait(t, 1)
	rc.expectNothing(200 * time.Millisecond)
	g.release(0, nil)
	rc.expect(ids[0], "200 OK")
}

// Out-of-order answers inside the window: three transactions in the ring at
// once, released in reverse, answered in that order.
func TestRELPWindowAllowsConcurrentTransactions(t *testing.T) {
	h, g := gatedHarness(t)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	msgs := []string{"<13>a", "<13>b", "<13>c"}
	ids := rc.syslog(msgs...)
	g.wait(t, 3) // all three in flight together
	for i := 2; i >= 0; i-- {
		g.releaseRaw(t, msgs[i], nil)
		f := rc.read()
		if f.cmd != "rsp" || f.data != "200 OK" {
			t.Fatalf("got %v", f)
		}
		if f.txnr != ids[i] {
			// Release order is answer order only when each answer is out
			// before the next release; read() waited for it.
			t.Fatalf("answer for txnr %d, want %d", f.txnr, ids[i])
		}
	}
}

// relp.window bounds what is read: with a window of 2 the third frame stays
// in the socket until a transaction is decided.
func TestRELPWindowStopsTheReader(t *testing.T) {
	h, g := gatedHarness(t)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Window: 2}}})
	rc := dialRELP(t, tr.Addr())
	rc.syslog("<13>a", "<13>b", "<13>c")
	g.wait(t, 2)
	time.Sleep(100 * time.Millisecond)
	if n := g.entered.Load(); n != 2 {
		t.Fatalf("%d transactions in flight with a window of 2", n)
	}
	if v := h.m.RELPInflightTransactions.Load(); v != 2 {
		t.Fatalf("in-flight gauge = %d, want 2", v)
	}
	g.mu.Lock()
	first := g.raws[0]
	g.mu.Unlock()
	g.releaseRaw(t, first, nil)
	g.wait(t, 1)
	for _, m := range []string{"<13>a", "<13>b", "<13>c"} {
		if m != first {
			g.releaseRaw(t, m, nil)
		}
	}
	rc.responses(3)
	waitFor(t, "the gauge back to zero", func() bool { return h.m.RELPInflightTransactions.Load() == 0 })
}

// SPEC §5.6: the copies of in-flight transactions are bounded by ONE budget for
// the process. With room for a single 2 KiB copy, a second connection's
// transaction waits -- the reader stops -- until the first is decided.
func TestRELPInflightBudgetIsGlobal(t *testing.T) {
	h, g := gatedHarness(t)
	cfg := h.cfg
	cfg.MaxMessageBytes = 2 << 10
	h.in = NewIngest(g, h.sink, h.m, logger(), cfg)
	budget := NewBudget(txnCharge(len("<13>first")), h.m) // room for exactly one
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Budget: budget}}})
	a := dialRELP(t, tr.Addr())
	b := dialRELP(t, tr.Addr())
	ida := a.syslog("<13>first")
	g.wait(t, 1)
	if budget.Used() != txnCharge(len("<13>first")) {
		t.Fatalf("budget used %d, want the charge of one small transaction", budget.Used())
	}
	idb := b.syslog("<13>second")
	waitFor(t, "the second reader waiting", func() bool { return h.m.RELPInflightWaitsTotal.Load() == 1 })
	if g.entered.Load() != 1 {
		t.Fatal("the second transaction was copied past relp.inflight_max_bytes")
	}
	g.release(0, nil)
	a.expect(ida[0], "200 OK")
	g.wait(t, 1)
	g.release(1, nil)
	b.expect(idb[0], "200 OK")
	waitFor(t, "the budget back to zero", func() bool { return budget.Used() == 0 })
	if h.m.RELPInflightBytes.Load() != 0 {
		t.Fatal("in-flight bytes gauge not back to zero")
	}
}

// SPEC §5.4, §5.7, decision 5: a full ring is HELD, never answered with 500 --
// librelp drops what it gets a 500 for. When the ring drains in time, the
// held transaction gets its 200 and nothing was lost.
func TestRELPFullRingIsHeldNotRefused(t *testing.T) {
	h := newHarness(t, 1)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	first := rc.syslog("<13>fills the ring")
	rc.expect(first[0], "200 OK")
	ids := rc.syslog("<13>waits")
	rc.expectNothing(300 * time.Millisecond)
	h.drainRing()
	rc.expect(ids[0], "200 OK")
	if h.m.RELPResponsesTotal[obs.RELPCode500].Load() != 0 {
		t.Fatal("a 500 was sent for a full ring")
	}
	if h.m.BackpressureSecondsHund[obs.TransportTCP].Load() == 0 {
		t.Fatal("backpressure time not counted")
	}
}

// Past relp.backpressure_timeout the session is closed WITHOUT an answer:
// librelp reconnects and retransmits what it has no 200 for. Not a drop.
func TestRELPBackpressureTimeoutClosesWithoutAnswering(t *testing.T) {
	h := newHarness(t, 1)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: 200 * time.Millisecond}}})
	rc := dialRELP(t, tr.Addr())
	first := rc.syslog("<13>fills the ring")
	rc.expect(first[0], "200 OK")
	rc.syslog("<13>held", "<13>behind it")
	rc.expectServerClose()
	m := h.m
	waitFor(t, "the unanswered count", func() bool {
		return m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load() == 2
	})
	if m.RELPResponsesTotal[obs.RELPCode500].Load() != 0 {
		t.Fatal("a 500 was sent")
	}
	for i := range m.MessagesDroppedTotal {
		if m.MessagesDroppedTotal[i].Load() != 0 {
			t.Fatalf("%s counted as dropped: what RELP hands back is not a loss", obs.DropReason(i))
		}
	}
	if m.MessagesTotal[obs.TransportTCP].Load() != m.MessagesAcceptedTotal[obs.TransportTCP].Load()+2 {
		t.Fatal("messages_total != accepted + unanswered")
	}
}

// A DOWN sink is held like a full ring (SPEC §13.3).
func TestRELPSinkDownIsHeld(t *testing.T) {
	h := newHarness(t, 100)
	h.sink.up.Store(false)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>waits for the sink")
	rc.expectNothing(200 * time.Millisecond)
	h.sink.up.Store(true)
	rc.expect(ids[0], "200 OK")
}

// An empty message gets 200: there is nothing to keep, a 500 would be a lie,
// and rsyslog would log an error per message (SPEC §5.4).
func TestRELPEmptyIsAcknowledged(t *testing.T) {
	h := newHarness(t, 100)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	n := rc.send("syslog", "")
	ids := rc.syslog("<15>a debug line is no longer filtered")
	got := rc.responses(2)
	if got[ids[0]] != "200 OK" || got[n] != "200 OK" {
		t.Fatalf("answers %v", got)
	}
	if h.m.MessagesDroppedTotal[obs.DropEmpty].Load() != 1 {
		t.Fatal("empty not counted")
	}
	// There is no filter here (SPEC §1.2): the debug line is in the ring.
	if h.ring.Stats().Items != 1 {
		t.Fatalf("%d items in the ring, want the debug line", h.ring.Stats().Items)
	}
}

// The parent answered a final refusal of the destination under ring=none with
// "500 refused by the destination". This server has no ring=none: whatever
// the memory ring answers that is not ErrFull or ErrClosed -- an error it is
// not supposed to have -- is HELD like a full ring and retried, never a 500,
// which librelp would drop for good (SPEC §5.4).
func TestRELPUnexpectedRingErrorIsHeldNotRefused(t *testing.T) {
	h := newHarness(t, 100)
	var attempts atomic.Int64
	h.in = NewIngest(&failOnceRing{Ring: h.ring, attempts: &attempts}, h.sink, h.m, logger(), h.cfg)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>held, then accepted")
	rc.expect(ids[0], "200 OK")
	if n := attempts.Load(); n != 2 {
		t.Fatalf("%d attempts, want the refused one and the retry", n)
	}
	if h.ring.Stats().Items != 1 {
		t.Fatal("the retried message is not in the ring")
	}
	if h.m.RELPResponsesTotal[obs.RELPCode500].Load() != 0 {
		t.Fatal("a 500 for an error that clears on its own")
	}
}

// failOnceRing answers its first Enqueue with an error the memory ring never
// returns, and passes the rest on.
type failOnceRing struct {
	ring.Ring
	attempts *atomic.Int64
}

func (r *failOnceRing) Enqueue(ctx context.Context, it *item.Item) error {
	if r.attempts.Add(1) == 1 {
		return errors.New("unexpected")
	}
	return r.Ring.Enqueue(ctx, it)
}

// SPEC §5.8, decision 6: at shutdown the decided answers go out, the
// undecided transactions get none, and serverclose comes after every answer.
func TestRELPShutdownWithTransactionsInFlight(t *testing.T) {
	h, g := gatedHarness(t)
	tr, cancel := startTCPCtx(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>decided", "<13>undecided")
	g.wait(t, 2)

	done := make(chan struct{})
	go func() {
		cancel() // the app cancels the receive context first (SPEC §13.4)
		// The first transaction is inside Enqueue and comes back accepted
		// after the shutdown began; the second is told the ring is closed.
		g.releaseRaw(t, "<13>decided", nil)
		g.releaseRaw(t, "<13>undecided", ring.ErrClosed)
		tr.Close()
		close(done)
	}()
	rc.expect(ids[0], "200 OK")
	rc.expectServerClose()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	waitFor(t, "the unanswered shutdown", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredShutdown].Load() == 1
	})
}

// A transaction held in backpressure at shutdown is released by the cancelled
// receive context and left unanswered; Close does not wait out the timeout.
func TestRELPShutdownReleasesAHeldTransaction(t *testing.T) {
	h := newHarness(t, 1)
	tr, cancel := startTCPCtx(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	rc := dialRELP(t, tr.Addr())
	first := rc.syslog("<13>fills")
	rc.expect(first[0], "200 OK")
	rc.syslog("<13>held")
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()
	tr.Close()
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Close took %s with a held transaction", time.Since(start))
	}
	rc.expectServerClose()
	if h.m.RELPUnansweredTotal[obs.UnansweredShutdown].Load() != 1 {
		t.Fatal("the held transaction was not counted as unanswered at shutdown")
	}
}

// The close command waits for every transaction in flight, so the client sees
// every answer of the session before the close answer (decision 7).
func TestRELPCloseWaitsForTransactionsInFlight(t *testing.T) {
	h, g := gatedHarness(t)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>in flight")
	g.wait(t, 1)
	n := rc.send("close", "")
	rc.expectNothing(150 * time.Millisecond)
	g.release(0, nil)
	rc.expect(ids[0], "200 OK")
	rc.expect(n, "")
	rc.expectEOF()
}

// The idle timeout ends the session with the serverclose hint.
func TestRELPIdleTimeout(t *testing.T) {
	h := newHarness(t, 10)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 150 * time.Millisecond}})
	rc := dialRELP(t, tr.Addr())
	rc.expectServerClose()
}

// A malformed frame closes the connection; what was decided before it still
// gets its answer. "Decided" is in the ring: a transaction still being built
// when the session ends is left unanswered, for librelp to retransmit (SPEC
// §5.7, the end of the session wins), so the test waits for the ring.
func TestRELPMalformedFrameClosesAfterTheAnswers(t *testing.T) {
	h := newHarness(t, 10)
	tr := startTCP(t, h, TCPConfig{})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>fine")
	waitFor(t, "the message in the ring", func() bool { return h.ring.Stats().Items == 1 })
	rc.write("2 syslog x garbage\n")
	rc.expect(ids[0], "200 OK")
	rc.expectServerClose()
	if h.m.RELPFrameErrorsTotal[obs.FrameMalformed].Load() != 1 {
		t.Fatal("malformed frame not counted")
	}
}

// A DATALEN above relp.max_frame_bytes closes before reading the body;
// between the message limit and the fuse the message is truncated and the
// session goes on (SPEC §5.2, §5.3).
func TestRELPFrameSizes(t *testing.T) {
	h := newHarnessCfg(t, 10, IngestConfig{Identity: testIdentity, MaxMessageBytes: 480})
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{MaxFrameBytes: 4096}}})
	rc := dialRELP(t, tr.Addr())
	ids := rc.syslog("<13>" + strings.Repeat("x", 1000))
	rc.expect(ids[0], "200 OK")
	items := h.drainRing()
	if len(items) != 1 || len(items[0].Raw) != 480 || items[0].Head.Size != 1004 {
		t.Fatalf("truncated item: %d bytes, size %d", len(items[0].Raw), items[0].Head.Size)
	}
	if h.m.MessagesTruncatedTotal[obs.TransportTCP].Load() != 1 {
		t.Fatal("truncation not counted")
	}
	rc.write("9 syslog 5000 ")
	rc.expectServerClose()
	if h.m.RELPFrameErrorsTotal[obs.FrameTooLarge].Load() != 1 {
		t.Fatal("too-large frame not counted")
	}
}

// newStream refuses a fuse below the message limit and a budget too small for
// one message: both would turn normal traffic into closed sessions or a
// reader waiting forever.
func TestNewStreamRefusesInconsistentRELPLimits(t *testing.T) {
	h := newHarness(t, 1) // MaxMessageBytes 64 KiB
	if _, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{Addr: "127.0.0.1:0",
		RELP: RELPConfig{MaxFrameBytes: 1024}}}, h.in, h.m, logger()); err == nil {
		t.Fatal("relp.max_frame_bytes below syslog.max_message_bytes was accepted")
	}
	if _, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{Addr: "127.0.0.1:0",
		RELP: RELPConfig{Budget: NewBudget(1024, h.m)}}}, h.in, h.m, logger()); err == nil {
		t.Fatal("relp.inflight_max_bytes below syslog.max_message_bytes was accepted")
	}
}

// A peer that vanishes with a transaction held: the transaction is abandoned
// (conn_lost), not enqueued later behind the back of a retransmission.
func TestRELPVanishedPeerAbandonsHeldTransactions(t *testing.T) {
	h := newHarness(t, 1)
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{BackpressureTimeout: time.Minute}}})
	rc := dialRELP(t, tr.Addr())
	first := rc.syslog("<13>fills")
	rc.expect(first[0], "200 OK")
	rc.syslog("<13>held")
	time.Sleep(100 * time.Millisecond)
	rc.c.Close()
	waitFor(t, "the conn_lost count", func() bool {
		return h.m.RELPUnansweredTotal[obs.UnansweredConnLost].Load() == 1
	})
	h.drainRing()
	time.Sleep(150 * time.Millisecond)
	if h.ring.Stats().Items != 0 {
		t.Fatal("the abandoned transaction reached the ring after its peer left")
	}
}

func TestBudgetAcquireRelease(t *testing.T) {
	m := newMetrics()
	b := NewBudget(100, m)
	if n, ok := b.Acquire(context.Background(), 60); !ok || n != 60 {
		t.Fatal(n, ok)
	}
	// Clamped to the ceiling, so a single copy can never wait forever.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := b.Acquire(ctx, 1000); ok {
		t.Fatal("acquired past the ceiling while 60 were held")
	}
	b.Release(60)
	if n, ok := b.Acquire(context.Background(), 1000); !ok || n != 100 {
		t.Fatalf("clamped acquire = %d, %v", n, ok)
	}
	b.Release(100)
	if b.Used() != 0 || m.RELPInflightBytes.Load() != 0 {
		t.Fatal("budget not back to zero")
	}
}
