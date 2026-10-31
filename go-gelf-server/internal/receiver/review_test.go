package receiver

// Regression tests for the independent review of 2026-10-05. Each names its
// finding (F1..F7, and the sink reviewer's ring=none logic refusal).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/sink"
)

// ------------------------------------------------------------------------ F1

// F1: a message held by backpressure was re-charged to the rate limiter on
// every 50 ms retry and ended dropped as "ratelimit", although it passed the
// limiter when it arrived and the ring drained in time.
func TestTCPHeldMessageIsChargedOnce(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	h.withLimiter(ratelimit.Params{Enabled: true, Rate: 1, Burst: 3, V4Mask: 24, V6Mask: 48, MaxBuckets: 100})
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{BackpressureTimeout: 10 * time.Second}})

	// Fill the one-slot ring from elsewhere, so the TCP message is held.
	if out := h.in.Accept(context.Background(), []byte(doc("filler")), udpWire("192.0.2.1")); out != Accepted {
		t.Fatalf("filler: %v", out)
	}
	c := dial(t, tr.Addr())
	fmt.Fprint(c, nul(doc("held")))

	// ~12 retries, far more than burst 3 plus 0.6 s of refill; then drain.
	time.Sleep(600 * time.Millisecond)
	h.ring.Drain()
	h.waitItems(t, 1)
	if got := h.ring.Drain(); len(got) != 1 || string(got[0].Raw) != doc("held") {
		t.Fatalf("held message lost: %v", got)
	}
	if rl := h.m.MessagesDroppedTotal[obs.DropRateLimit].Load(); rl != 0 {
		t.Fatalf("%d ratelimit drops for a message admitted on arrival", rl)
	}
}

// F1, at the pipeline: filter and rate limiter decide ONCE per message. Retry
// neither charges a token nor asks a filter reloaded in the meantime.
func TestIngestRetryDecidesFilterAndRateLimitOnce(t *testing.T) {
	stub := &stubRing{errs: []error{ring.ErrFull, ring.ErrFull, ring.ErrFull, nil}}
	rule, _ := filter.Compile("debug", nil)
	f := filter.New(rule)
	l := ratelimit.New(ratelimit.Params{Enabled: true, Rate: 1e-9, Burst: 1, V4Mask: 32, V6Mask: 128, MaxBuckets: 10}, 4)
	m := newMetrics()
	in := NewIngest(stub, newUp(), f, l, m, logger(), defaultIngestConfig())

	w := tcpWire("10.0.0.1")
	if out := in.Accept(context.Background(), []byte(doc("x")), w); out != RingFull {
		t.Fatal(out)
	}
	// SIGHUP in the middle of the hold: the message was admitted already.
	strict, _ := filter.Compile("emerg", nil)
	f.Replace(strict)
	for i := 0; i < 2; i++ {
		if out := in.Retry(context.Background(), []byte(doc("x")), w); out != RingFull {
			t.Fatalf("retry %d: %v, want RingFull (no token charged, no re-filtering)", i, out)
		}
	}
	if out := in.Retry(context.Background(), []byte(doc("x")), w); out != Accepted {
		t.Fatalf("final retry: %v", out)
	}
	// Exactly the one token of the arrival was spent: a NEW message from the
	// same source finds the bucket empty.
	f.Replace(rule)
	if out := in.Accept(context.Background(), []byte(doc("y")), tcpWire("10.0.0.1")); out != RateLimited {
		t.Fatalf("a new message: %v, want RateLimited", out)
	}
}

// ------------------------------------------------------------------------ F2

// recConn records the deadlines set on it.
type recConn struct {
	net.Conn
	mu            sync.Mutex
	read, written time.Time
	readSet       int
	writeSet      int
}

func (c *recConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read, c.written = t, t
	c.readSet++
	c.writeSet++
	return nil
}
func (c *recConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = t
	c.readSet++
	return nil
}
func (c *recConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = t
	c.writeSet++
	return nil
}

// F2: the frame loop arms the WRITE deadline with the read one -- crypto/tls
// writes from inside Read -- and a shutdown expires both.
func TestArmDeadlineArmsBothDirections(t *testing.T) {
	var closing atomic.Bool
	c := &recConn{}
	armDeadline(c, time.Minute, &closing)
	if c.writeSet == 0 || c.written.IsZero() || !c.written.Equal(c.read) {
		t.Fatalf("write deadline %v, read %v: the write direction is not armed", c.written, c.read)
	}
	armDeadline(c, 0, &closing)
	if !c.written.IsZero() || !c.read.IsZero() {
		t.Fatal("idle_timeout=0 must clear both deadlines")
	}
	closing.Store(true)
	armDeadline(c, time.Hour, &closing)
	if time.Until(c.written) > time.Second || time.Until(c.read) > time.Second {
		t.Fatal("a reader re-arming during shutdown undid the shutdown's deadline")
	}
}

// blockConn blocks its reader until Close, ignoring every deadline: a reader no
// deadline can reach, the worst case Close's grace exists for.
type blockConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (b *blockConn) Close() error                     { b.once.Do(func() { close(b.closed) }); return nil }
func (b *blockConn) SetDeadline(time.Time) error      { return nil }
func (b *blockConn) SetReadDeadline(time.Time) error  { return nil }
func (b *blockConn) SetWriteDeadline(time.Time) error { return nil }

// F2: if a reader still does not return after closeGrace, Close closes its
// socket instead of waiting on the peer forever.
func TestStreamCloseForceClosesAfterGrace(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m, log: logger(),
		conns: map[net.Conn]struct{}{}}
	c := &blockConn{closed: make(chan struct{})}
	if !s.track(c) {
		t.Fatal("track")
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.untrack(c)
		<-c.closed // a reader parked where no deadline reaches
	}()
	start := time.Now()
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeGrace + 5*time.Second):
		t.Fatal("Close waited on a reader its deadlines could not reach")
	}
	if el := time.Since(start); el < closeGrace/2 {
		t.Fatalf("Close returned after %v: it did not give the readers their grace", el)
	}
}

// ------------------------------------------------------------------------ F3

// F3 over a socket: the idle timeout in the middle of a frame delivers what
// arrived (SPEC §5.3) -- a whole document without its NUL, or half of one,
// which the converter marks malformed.
func TestTCPIdleTimeoutMidFrameDeliversIt(t *testing.T) {
	for _, payload := range []string{doc("no NUL yet"), `{"short_message":"cut`, "123"} {
		h := newHarness(t, 10, "debug", nil)
		tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{IdleTimeout: 200 * time.Millisecond}})
		c := dial(t, tr.Addr())
		fmt.Fprint(c, payload)
		expectClosed(t, c)
		h.waitItems(t, 1)
		if got := h.drainRing(); len(got) != 1 || string(got[0].Raw) != payload {
			t.Fatalf("payload %q: %v", payload, got)
		}
	}
}

// ------------------------------------------------------------------------ F7

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type emfileListener struct {
	mu     sync.Mutex
	closed bool
	calls  int
}

func (l *emfileListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, net.ErrClosed
	}
	l.calls++
	return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
}
func (l *emfileListener) Close() error   { l.mu.Lock(); l.closed = true; l.mu.Unlock(); return nil }
func (l *emfileListener) Addr() net.Addr { return &net.TCPAddr{} }

// F7: a persistent accept error (EMFILE under a flood) logged an unsampled
// WARN every 10 ms. It is sampled now, and the loop still pauses and goes on.
func TestAcceptErrorLogIsSampled(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	var buf syncBuf
	l := &emfileListener{}
	s := &Stream{transport: item.TransportTCP, obsT: obs.TransportTCP, in: h.in, m: h.m,
		log: obs.NewLogger(&buf, "text", "warn"), conns: map[net.Conn]struct{}{},
		lns: []net.Listener{l}, acceptLog: obs.NewSampler(30 * time.Second)}
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	l.Close()
	s.wg.Wait()
	if n := strings.Count(buf.String(), "accept failed"); n != 1 {
		t.Fatalf("%d accept-failed lines in 0.5 s, want 1 (sampled)", n)
	}
	l.mu.Lock()
	calls := l.calls
	l.mu.Unlock()
	if calls < 5 || calls > 100 {
		t.Fatalf("%d accept attempts in 0.5 s: the loop must keep trying, with its 10 ms pause", calls)
	}
}

// --------------------------------------------- ring=none logic refusal (sink)

// fakeSink refuses, with a ClassLogic error, every write that carries a line
// containing "refused" -- permanently, as a real constraint violation would --
// and records the rest.
type fakeSink struct {
	mu      sync.Mutex
	refused int
	written []string
}

func (s *fakeSink) Write(ctx context.Context, b *sink.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range b.Envelopes {
		if bytes.Contains(e.Item.Raw, []byte("refused")) {
			s.refused++
			return &sink.Error{Class: sink.ClassLogic, Op: "insert", Err: errors.New("value too long")}
		}
	}
	for _, e := range b.Envelopes {
		s.written = append(s.written, e.UUID())
	}
	return nil
}
func (s *fakeSink) Health(ctx context.Context) error { return nil }
func (s *fakeSink) Close() error                     { return nil }
func (s *fakeSink) Type() string                     { return "fake" }

func (s *fakeSink) count() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refused, len(s.written)
}

// Sink reviewer: with ring=none, a ClassLogic refusal is FINAL for that
// message (SPEC §9.6, §10.2). It used to come back as SinkDown -- retryable --
// so TCP retried the same line until the backpressure timeout and the sink
// counted sink_logic again on every attempt. Now: counted once, by the sink;
// nothing counted by the receiver; the connection keeps reading and the next
// message is delivered.
func TestRingNoneLogicRefusalIsFinal(t *testing.T) {
	m := newMetrics()
	fs := &fakeSink{}
	direct := sink.NewInstrumented(fs, nil, sink.WorkerConfig{Convert: convert.DefaultConfig(), AcceptMalformed: true},
		logger().Logger, m)
	r := ring.NewNone(direct, 16, 2*time.Second)
	cfg := defaultIngestConfig()
	cfg.Final = func(err error) bool { return sink.ClassOf(err) == sink.ClassLogic }
	h := &harness{m: m, sink: newUp(), cfg: cfg}
	h.in = NewIngest(r, h.sink, nil, nil, m, logger(), cfg)

	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{BackpressureTimeout: 5 * time.Second}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, nul(doc("refused for good"), doc("next one")))
	waitFor(t, "the second message", func() bool { _, n := fs.count(); return n == 1 })

	refused, _ := fs.count()
	if refused != 1 {
		t.Fatalf("the sink saw the refused message %d times: it was retried", refused)
	}
	if got := m.EventsDroppedTotal[obs.EventDropSinkLogic].Load(); got != 1 {
		t.Fatalf("sink_logic = %d, want exactly 1", got)
	}
	for r := obs.DropReason(0); r <= obs.DropChunkOverflow; r++ {
		if got := m.MessagesDroppedTotal[r].Load(); got != 0 {
			t.Fatalf("messages_dropped{%s} = %d: the refusal was counted again by the receiver", r, got)
		}
	}
	// The connection is alive and still reading.
	fmt.Fprint(c, nul(doc("third")))
	waitFor(t, "the third message", func() bool { _, n := fs.count(); return n == 2 })
	if got := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); got != 2 {
		t.Fatalf("accepted = %d, want 2", got)
	}
}

// ... and at the pipeline: Final makes the outcome Consumed, which nobody
// retries, RecordDrop does not count, and whose buffer is discarded, never
// pooled. Without the hook the old, retryable behaviour stands.
func TestIngestFinalRefusalIsConsumed(t *testing.T) {
	logic := &sink.Error{Class: sink.ClassLogic, Op: "xadd", Err: errors.New("WRONGTYPE? no: too long")}
	cfg := defaultIngestConfig()
	cfg.Final = func(err error) bool { return sink.ClassOf(err) == sink.ClassLogic }
	stub := &stubRing{errs: []error{logic}}
	m := newMetrics()
	in := NewIngest(stub, newUp(), nil, nil, m, logger(), cfg)
	out := in.Accept(context.Background(), []byte(doc("x")), tcpWire("10.0.0.1"))
	if out != Consumed || out.Retryable() {
		t.Fatalf("outcome %v", out)
	}
	in.RecordDrop(out)
	for r := obs.DropReason(0); r <= obs.DropChunkOverflow; r++ {
		if m.MessagesDroppedTotal[r].Load() != 0 {
			t.Fatalf("Consumed counted as %s", r)
		}
	}
	if it := stub.items[0]; !it.Released() || !it.Tainted() {
		t.Fatal("the refused item must be discarded, not pooled")
	}

	// A network error is still retryable.
	stub = &stubRing{errs: []error{&sink.Error{Class: sink.ClassNetwork, Op: "xadd", Err: errors.New("reset")}}}
	in = NewIngest(stub, newUp(), nil, nil, newMetrics(), logger(), cfg)
	if out := in.Accept(context.Background(), []byte(doc("x")), tcpWire("10.0.0.1")); out != SinkDown {
		t.Fatalf("network error: %v, want SinkDown", out)
	}
}

// --------------------------------------------------- PROXY: a stalled header

// A trusted peer whose v1 header starts and then stalls past header_timeout is
// an INVALID header (the signature matched), closed as reason="proxy" -- never
// "absent", and never fed to the framer.
func TestProxyStalledHeaderIsInvalid(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	cidrs, _ := ParseCIDRList("127.0.0.1/32")
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{
		Proxy: ProxyConfig{Mode: ProxyOptional, Trusted: cidrs, HeaderTimeout: 150 * time.Millisecond}}})
	c := dial(t, tr.Addr())
	fmt.Fprint(c, "PROXY TCP4 1.2.3.4") // and nothing more
	expectClosed(t, c)
	waitFor(t, "the invalid header", func() bool {
		return h.m.ProxyHeadersTotal[obs.ProxyInvalid].Load() == 1 &&
			h.m.MessagesDroppedTotal[obs.DropProxy].Load() == 1
	})
	if h.m.ProxyHeadersTotal[obs.ProxyAbsent].Load() != 0 || h.m.MessagesTotal[obs.TransportTCP].Load() != 0 {
		t.Fatal("a stalled header was treated as absent and framed")
	}
}
