package receiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
)

func logger() *obs.Logger { return obs.NewLogger(io.Discard, "text", "error") }

func newMetrics() *obs.Metrics {
	return obs.NewMetrics("memory", "none", "all", obs.BuildInfo{Version: "test"})
}

type upState struct{ up atomic.Bool }

func (u *upState) Up() bool { return u.up.Load() }

func newUp() *upState {
	u := &upState{}
	u.up.Store(true)
	return u
}

type harness struct {
	ring  *ring.Memory
	in    *Ingest
	m     *obs.Metrics
	sink  *upState
	filt  *filter.Filter
	limit *ratelimit.Limiter
}

func newHarness(t *testing.T, capItems int, minSev string, facilities []string) *harness {
	t.Helper()
	rule, err := filter.Compile(minSev, facilities)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{
		ring: ring.NewMemory(capItems, 0, time.Millisecond),
		m:    newMetrics(),
		sink: newUp(),
		filt: filter.New(rule),
	}
	h.in = NewIngest(h.ring, h.sink, h.filt, nil, h.m, logger(),
		IngestConfig{DefaultFacility: 1, DefaultSeverity: 5})
	return h
}

// drainRing takes everything the ring holds, so a test can assert on the items
// instead of on a counter.
func (h *harness) drainRing() []*envelope.Item { return h.ring.Drain() }

func (h *harness) waitItems(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h.ring.Stats().Items >= int64(n) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d of %d items reached the ring", h.ring.Stats().Items, n)
}

// ---------------------------------------------------------------- the pipeline

func TestIngestAcceptFillsTheHead(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	w := Wire{
		Transport: envelope.TransportUDP,
		Framing:   envelope.FramingDatagram,
		Listener:  ":1514",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  4242,
	}
	line := `<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - oi`
	if out := h.in.Accept(context.Background(), []byte(line), w); out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
	items := h.drainRing()
	if len(items) != 1 {
		t.Fatalf("items = %d", len(items))
	}
	it := items[0]
	if it.Head.ID == "" || len(it.Head.ID) != 36 {
		t.Fatalf("id = %q", it.Head.ID)
	}
	if it.Head.PRI != 165 || !it.Head.FromWire {
		t.Fatalf("pri = %d wire=%v", it.Head.PRI, it.Head.FromWire)
	}
	if it.Head.Transport != envelope.TransportUDP || it.Head.PeerPort != 4242 {
		t.Fatalf("wire = %+v", it.Head)
	}
	if it.Head.TS == 0 || it.Head.NTS == 0 {
		t.Fatalf("time not marked: %+v", it.Head)
	}
	// The item owns its own copy: the transport's buffer is reused.
	if string(it.Raw) != line {
		t.Fatalf("raw = %q", it.Raw)
	}
}

// A message with no PRI gets the RFC 3164 §4.3.3 defaults and still gets in.
func TestIngestAppliesDefaultPRI(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	if out := h.in.Accept(context.Background(), []byte("linha solta"), Wire{}); out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
	it := h.drainRing()[0]
	if it.Head.PRI != 13 || it.Head.FromWire {
		t.Fatalf("pri = %d wire=%v, want 13 false", it.Head.PRI, it.Head.FromWire)
	}
}

// The filter runs BEFORE the copy: a message that is not getting in must not cost
// a buffer, which is the normal case on UDP under a flood of debug.
func TestIngestFilterDropsBeforeTheRing(t *testing.T) {
	h := newHarness(t, 100, "warning", nil)
	// severity 7 (debug) on facility 20
	if out := h.in.Accept(context.Background(), []byte("<167>debug noise"), Wire{}); out != Filtered {
		t.Fatalf("outcome = %v, want Filtered", out)
	}
	h.in.RecordDrop(Filtered)
	if h.ring.Stats().Items != 0 {
		t.Fatal("a filtered message entered the ring")
	}
	if got := h.m.MessagesDroppedTotal[obs.DropFilter].Load(); got != 1 {
		t.Fatalf("filter drops = %d", got)
	}
	// severity 3 (err) passes
	if out := h.in.Accept(context.Background(), []byte("<163>real problem"), Wire{}); out != Accepted {
		t.Fatalf("err severity was dropped: %v", out)
	}
}

func TestIngestSinkDownFailsFast(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	h.sink.up.Store(false)
	out := h.in.Accept(context.Background(), []byte("<13>oi"), Wire{})
	if out != SinkDown {
		t.Fatalf("outcome = %v", out)
	}
	// Fail fast even with an empty ring: accepting into a ring that will not
	// drain only postpones the same loss.
	if h.ring.Stats().Items != 0 {
		t.Fatal("a message entered the ring with the sink DOWN")
	}
	if !out.Retryable() {
		t.Fatal("a down sink must be retryable: it may well come back")
	}
}

func TestIngestRingFullIsRetryable(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	if out := h.in.Accept(context.Background(), []byte("<13>um"), Wire{}); out != Accepted {
		t.Fatal(out)
	}
	out := h.in.Accept(context.Background(), []byte("<13>dois"), Wire{})
	if out != RingFull || !out.Retryable() {
		t.Fatalf("outcome = %v retryable=%v", out, out.Retryable())
	}
}

// A filtered or rate-limited message is NOT retryable: it will never drain on its
// own, so holding the emitter's window would move the queue without resolving it.
// SPEC §14.2, error paths. Which Enqueue errors let the receive path pool the
// buffer and which must leave it to the GC: the ring refusing outright means
// nobody below saw the item; anything else came from ring=none, where rueidis
// may still be serialising it.
func TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		want   Outcome
		pooled bool
	}{
		{"ring full releases", ring.ErrFull, RingFull, true},
		{"ring closed releases", ring.ErrClosed, Shutdown, true},
		{"op_timeout discards", context.DeadlineExceeded, RingFull, false},
		{"cancellation discards", context.Canceled, Shutdown, false},
		{"destination failure discards", errors.New("xadd: broken pipe"), SinkDown, false},
	}
	rule, err := filter.Compile("debug", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{err: tc.err}
			in := NewIngest(stub, newUp(), filter.New(rule), nil, newMetrics(), logger(),
				IngestConfig{DefaultFacility: 1, DefaultSeverity: 5})

			out := in.Accept(context.Background(), []byte("<13>hello"), Wire{
				Transport: envelope.TransportTCP,
				Framing:   envelope.FramingLF,
				PeerAddr:  netip.MustParseAddr("10.0.0.7"),
			})
			if out != tc.want {
				t.Fatalf("outcome = %s, want %s", out, tc.want)
			}
			it := stub.last
			if it == nil {
				t.Fatal("the pipeline never reached Enqueue")
			}
			if !it.Released() {
				t.Fatal("the item was neither released nor discarded: a leak of the ownership rule")
			}
			if tc.pooled && it.Raw != nil {
				t.Error("Release must clear the buffer it pooled")
			}
			if !tc.pooled && it.Raw == nil {
				t.Error("Discard must leave the buffer alone; something below may still read it")
			}
		})
	}
}

// stubRing answers Enqueue with whatever the test needs and remembers the item,
// so a test can check what the pipeline did with its buffer afterwards.
type stubRing struct {
	err  error
	last *envelope.Item
}

func (r *stubRing) Enqueue(ctx context.Context, it *envelope.Item) error {
	r.last = it
	return r.err
}
func (r *stubRing) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	return nil, ring.ErrNoBacklog
}
func (r *stubRing) Commit(items []*envelope.Item) error { return nil }
func (r *stubRing) Drain() []*envelope.Item             { return nil }
func (r *stubRing) Stats() ring.Stats                   { return ring.Stats{} }
func (r *stubRing) Mode() string                        { return "stub" }
func (r *stubRing) Close() error                        { return nil }

func TestOutcomeRetryability(t *testing.T) {
	retryable := map[Outcome]bool{
		Accepted: false, Filtered: false, RateLimited: false,
		SinkDown: true, RingFull: true, Shutdown: false, Failed: false,
	}
	for o, want := range retryable {
		if o.Retryable() != want {
			t.Errorf("%v.Retryable() = %v, want %v", o, o.Retryable(), want)
		}
	}
}

// The rate-limit key is the transport peer, always. The hostname inside the
// message is the emitter's claim and would poison every other bucket.
func TestIngestRateLimitKeysOnThePeer(t *testing.T) {
	h := newHarness(t, 1000, "debug", nil)
	h.limit = ratelimit.New(ratelimit.Params{
		Enabled: true, Rate: 1, Burst: 2, V4Mask: 32, V6Mask: 128, MaxBuckets: 1000,
	}, 4)
	h.in = NewIngest(h.ring, h.sink, h.filt, h.limit, h.m, logger(), IngestConfig{})

	w := Wire{PeerAddr: netip.MustParseAddr("10.0.0.1")}
	var limited int
	for i := 0; i < 10; i++ {
		// Every message claims a different hostname; only the peer matters.
		line := fmt.Sprintf("<13>Sep 12 10:00:00 host-%d app: x", i)
		if h.in.Accept(context.Background(), []byte(line), w) == RateLimited {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("the limiter never fired despite one peer sending ten messages at rate 1")
	}
	// A different peer has its own bucket.
	other := Wire{PeerAddr: netip.MustParseAddr("10.0.0.2")}
	if out := h.in.Accept(context.Background(), []byte("<13>x"), other); out != Accepted {
		t.Fatalf("a different peer was limited: %v", out)
	}
}

// The unix transport has no network peer, so it is exempt by construction:
// rate-limiting /dev/log would silence the host itself.
func TestIngestUnixIsExemptFromRateLimit(t *testing.T) {
	h := newHarness(t, 1000, "debug", nil)
	h.limit = ratelimit.New(ratelimit.Params{
		Enabled: true, Rate: 1, Burst: 1, V4Mask: 32, V6Mask: 128, MaxBuckets: 10,
	}, 4)
	h.in = NewIngest(h.ring, h.sink, h.filt, h.limit, h.m, logger(), IngestConfig{})

	w := Wire{Transport: envelope.TransportUnix} // no PeerAddr
	for i := 0; i < 20; i++ {
		if out := h.in.Accept(context.Background(), []byte("<13>x"), w); out != Accepted {
			t.Fatalf("message %d on unix was refused: %v", i, out)
		}
	}
}

// Counting is split across Accept, Retry and RecordDrop so a held message is one
// arrival and one drop, not six hundred of each.
func TestRetryDoesNotRecount(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	w := Wire{Transport: envelope.TransportTCP}
	h.in.Accept(context.Background(), []byte("<13>um"), w) // fills the ring

	h.in.Accept(context.Background(), []byte("<13>dois"), w)
	for i := 0; i < 5; i++ {
		h.in.Retry(context.Background(), []byte("<13>dois"), w)
	}
	h.in.RecordDrop(RingFull)

	if got := h.m.MessagesTotal[obs.TransportTCP].Load(); got != 2 {
		t.Fatalf("arrivals = %d, want 2", got)
	}
	if got := h.m.MessagesDroppedTotal[obs.DropRingFull].Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
}

// ------------------------------------------------------------------------- UDP

func TestUDPEndToEnd(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	d, err := NewDatagram(context.Background(), DatagramConfig{
		Addr: "127.0.0.1:0", Listeners: 1, ReadBuffer: 1 << 20,
	}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	c, err := net.Dial("udp", d.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	lines := []string{
		`<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - um`,
		`<34>Oct 11 22:14:15 mymachine su[230]: dois`,
		`sem pri`,
	}
	for _, l := range lines {
		if _, err := c.Write([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}
	h.waitItems(t, len(lines))

	items := h.drainRing()
	seen := map[string]bool{}
	for _, it := range items {
		if it.Head.Transport != envelope.TransportUDP ||
			it.Head.Framing != envelope.FramingDatagram {
			t.Errorf("wire = %v/%v", it.Head.Transport, it.Head.Framing)
		}
		if !it.Head.PeerAddr.IsValid() || it.Head.PeerPort == 0 {
			t.Errorf("peer not recorded: %v:%d", it.Head.PeerAddr, it.Head.PeerPort)
		}
		seen[string(it.Raw)] = true
	}
	for _, l := range lines {
		if !seen[l] {
			t.Errorf("line not received: %q", l)
		}
	}
}

// A full ring on UDP is a counted drop and nothing else: there is nobody to tell.
func TestUDPFullRingCountsTheDrop(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	d, err := NewDatagram(context.Background(), DatagramConfig{
		Addr: "127.0.0.1:0", Listeners: 1,
	}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	c, _ := net.Dial("udp", d.Addr())
	defer c.Close()
	for i := 0; i < 20; i++ {
		c.Write([]byte("<13>x"))
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.m.MessagesDroppedTotal[obs.DropRingFull].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.m.MessagesDroppedTotal[obs.DropRingFull].Load() == 0 {
		t.Fatal("a full ring on UDP produced no counted drop")
	}
}

// ------------------------------------------------------------------ unix dgram

func TestUnixEndToEnd(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	path := filepath.Join(t.TempDir(), "log")
	d, err := NewDatagram(context.Background(), DatagramConfig{
		Addr: path, Unix: true, Mode: 0o666,
	}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 0666 is what /dev/log has everywhere, and it has to be: any process on the
	// host needs to be able to log.
	if fi.Mode().Perm() != 0o666 {
		t.Fatalf("socket mode = %o, want 666", fi.Mode().Perm())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	c, err := net.Dial("unixgram", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("<13>Sep 12 10:00:00 host app: local")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	h.waitItems(t, 1)

	it := h.drainRing()[0]
	if it.Head.Transport != envelope.TransportUnix {
		t.Fatalf("transport = %v", it.Head.Transport)
	}
	// No network peer exists on this transport.
	if it.Head.PeerAddr.IsValid() || it.Head.PeerPort != 0 || it.Head.PeerIPVersion() != 0 {
		t.Fatalf("unix message carries a peer: %v:%d", it.Head.PeerAddr, it.Head.PeerPort)
	}

	// Close removes the socket file, so the next boot has nothing to decide about.
	d.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("the socket file survived Close")
	}
}

// Stealing /dev/log from a running syslogd is not a mistake to make quietly.
func TestUnixRefusesALiveSocket(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	path := filepath.Join(t.TempDir(), "log")

	first, err := NewDatagram(context.Background(), DatagramConfig{Addr: path, Unix: true},
		h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	_, err = NewDatagram(context.Background(), DatagramConfig{Addr: path, Unix: true},
		h.in, h.m, logger())
	if err == nil {
		t.Fatal("the second receiver stole a served socket")
	}
	if !strings.Contains(err.Error(), "already served") {
		t.Fatalf("err = %v", err)
	}
}

// A socket left by a previous run must be cleared, or bind fails forever.
func TestUnixRemovesAStaleSocket(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	path := filepath.Join(t.TempDir(), "log")

	// A socket file nobody is listening on: exactly what a SIGKILL leaves.
	c, err := net.ListenPacket("unixgram", path)
	if err != nil {
		t.Fatal(err)
	}
	c.Close() // the file stays, unserved

	d, err := NewDatagram(context.Background(), DatagramConfig{Addr: path, Unix: true},
		h.in, h.m, logger())
	if err != nil {
		t.Fatalf("a stale socket was not cleared: %v", err)
	}
	d.Close()
}

// ------------------------------------------------------------------------- TCP

func startTCP(t *testing.T, h *harness, cfg TCPConfig) *TCP {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.MaxFrameBytes == 0 {
		cfg.MaxFrameBytes = 1 << 20
	}
	if cfg.BackpressureTimeout == 0 {
		cfg.BackpressureTimeout = 2 * time.Second
	}
	cfg.FramingAuto = true
	tr, err := NewTCP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); tr.Close() })
	tr.Start(ctx)
	return tr
}

func TestTCPBothFramings(t *testing.T) {
	for name, payload := range map[string]string{
		"lf":    "<13>um\n<14>dois\n",
		"octet": octet("<13>um", "<14>dois"),
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, 100, "debug", nil)
			tr := startTCP(t, h, TCPConfig{})

			c, err := net.Dial("tcp", tr.Addr())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Write([]byte(payload)); err != nil {
				t.Fatal(err)
			}
			h.waitItems(t, 2)
			c.Close()

			items := h.drainRing()
			if len(items) != 2 {
				t.Fatalf("items = %d", len(items))
			}
			for _, it := range items {
				if it.Head.Transport != envelope.TransportTCP {
					t.Errorf("transport = %v", it.Head.Transport)
				}
				// No framing byte may reach the raw line.
				if strings.ContainsAny(string(it.Raw), "\n") {
					t.Errorf("raw line carries a terminator: %q", it.Raw)
				}
			}
			want := map[string]bool{"<13>um": true, "<14>dois": true}
			for _, it := range items {
				if !want[string(it.Raw)] {
					t.Errorf("unexpected line %q", it.Raw)
				}
			}
		})
	}
}

// This is the 429 of this protocol, and it is better than one: nothing is lost.
func TestTCPBackpressureLosesNothing(t *testing.T) {
	h := newHarness(t, 2, "debug", nil) // a tiny ring, so it fills at once
	tr := startTCP(t, h, TCPConfig{BackpressureTimeout: 10 * time.Second})

	c, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

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
		time.Sleep(2 * time.Millisecond)
	}

	if len(got) != n {
		t.Fatalf("received %d of %d messages through backpressure", len(got), n)
	}
	// Nothing was dropped: the emitter was held, not refused.
	if d := h.m.MessagesDroppedTotal[obs.DropRingFull].Load(); d != 0 {
		t.Fatalf("%d messages dropped while backpressure was available", d)
	}
	if h.m.BackpressureSecondsHund[obs.TransportTCP].Load() == 0 &&
		h.m.MessagesTotal[obs.TransportTCP].Load() == n {
		t.Log("note: the ring drained fast enough that no stall was recorded")
	}
}

// A desynchronised stream ends the connection: there is no way to find the next
// message boundary, and guessing would write fragments as messages.
func TestTCPMalformedFrameClosesTheConnection(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	tr := startTCP(t, h, TCPConfig{})

	c, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Starts with a digit, so octet framing; then the count is not a number.
	if _, err := c.Write([]byte("12x garbage")); err != nil {
		t.Fatal(err)
	}
	c.(*net.TCPConn).CloseWrite()

	// The server closes, so the read returns EOF.
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("the connection stayed open after a malformed frame")
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.m.MessagesDroppedTotal[obs.DropMalformedFrame].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.m.MessagesDroppedTotal[obs.DropMalformedFrame].Load() == 0 {
		t.Fatal("a malformed frame was not counted")
	}
}

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
	ln := newConnLimiter(0, 1).wrap(st)

	first, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	// Every remaining connection is over the per-IP cap; Accept must refuse
	// them all and come back with the listener's own error, not a stack
	// overflow.
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed after %d refusals", err, refusals)
	}
	if st.next != refusals+1 {
		t.Fatalf("listener served %d connections, want %d", st.next, refusals+1)
	}
}

// SPEC §4.4: the ceilings are per transport. Two listeners of one transport
// share one limiter, or max_conns is silently multiplied by the listener count.
func TestConnLimitsAreSharedAcrossListeners(t *testing.T) {
	lim := newConnLimiter(1, 0)
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
	lim := newConnLimiter(1, 0)
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

// SPEC §13.1: syslog_confirm_duration_seconds is observed on tcp and relp,
// from the frame being whole to the ring's guarantee being met -- and nowhere
// else, because udp and unix have nothing to confirm.
func TestConfirmDurationIsObservedOnTCPAndRELP(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	tr := startTCP(t, h, TCPConfig{})

	c, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(c, "<13>one\n<13>two\n")
	c.Close()
	h.waitItems(t, 2)

	rl := startRELP(t, h, RELPConfig{Window: 4})
	rc, err := net.Dial("tcp", rl.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	br := bufio.NewReader(rc)
	relpSend(t, rc, 1, "syslog", "<13>three")
	if _, body := readRSP(t, rc, br); !strings.Contains(body, "200 OK") {
		t.Fatalf("RELP answered %q", body)
	}

	deadline := time.Now().Add(2 * time.Second)
	for h.m.ConfirmDuration.Count() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := h.m.ConfirmDuration.Count(); got != 3 {
		t.Fatalf("confirm_duration observed %d times, want 3 (two tcp, one relp)", got)
	}
}

func TestTCPPerIPLimit(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	tr := startTCP(t, h, TCPConfig{MaxConnsPerIP: 2})

	var open []net.Conn
	defer func() {
		for _, c := range open {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", tr.Addr())
		if err != nil {
			t.Fatal(err)
		}
		open = append(open, c)
		// Send something so the server has certainly registered the connection.
		fmt.Fprintf(c, "<13>conn-%d\n", i)
	}
	h.waitItems(t, 2)

	// The third is accepted by the kernel and closed immediately: blocking would
	// stall every other address behind one abusive peer.
	c, err := net.Dial("tcp", tr.Addr())
	if err == nil {
		open = append(open, c)
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatal("the third connection from one IP stayed open")
		}
	}
}

// ------------------------------------------------------------------------ RELP

func startRELP(t *testing.T, h *harness, cfg RELPConfig) *RELP {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.MaxFrameBytes == 0 {
		cfg.MaxFrameBytes = 1 << 20
	}
	if cfg.Window == 0 {
		cfg.Window = 8
	}
	if cfg.BackpressureTimeout == 0 {
		cfg.BackpressureTimeout = 2 * time.Second
	}
	cfg.Version = "test"
	r, err := NewRELP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); r.Close() })
	r.Start(ctx)
	return r
}

// relpSend writes one frame with the length COMPUTED. Writing it by hand is how
// a framing test drifts out of agreement with itself, and the failure mode is a
// closed connection and a reader that waits forever.
func relpSend(t *testing.T, c net.Conn, txnr int64, cmd, data string) {
	t.Helper()
	if _, err := fmt.Fprintf(c, "%d %s %d %s\n", txnr, cmd, len(data), data); err != nil {
		t.Fatalf("writing a %s frame: %v", cmd, err)
	}
}

// relpSendEmpty writes a frame with no data, in the spelling librelp uses.
func relpSendEmpty(t *testing.T, c net.Conn, txnr int64, cmd string) {
	t.Helper()
	if _, err := fmt.Fprintf(c, "%d %s 0 \n", txnr, cmd); err != nil {
		t.Fatalf("writing a %s frame: %v", cmd, err)
	}
}

// readRSP reads one response frame and returns its txnr and body.
func readRSP(t *testing.T, c net.Conn, br *bufio.Reader) (int64, string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading a response: %v", err)
	}
	var txnr, n int64
	var cmd string
	// "<txnr> rsp <len> <body>\n" or "<txnr> rsp 0\n"
	parts := strings.SplitN(strings.TrimSuffix(line, "\n"), " ", 4)
	if len(parts) < 3 {
		t.Fatalf("malformed response %q", line)
	}
	fmt.Sscanf(parts[0], "%d", &txnr)
	cmd = parts[1]
	fmt.Sscanf(parts[2], "%d", &n)
	if cmd != "rsp" && cmd != "serverclose" {
		t.Fatalf("unexpected command %q in %q", cmd, line)
	}
	body := ""
	if len(parts) == 4 {
		body = parts[3]
	}
	// The body may itself contain newlines (the open handshake does), so read the
	// remainder by declared length.
	for int64(len(body)) < n {
		more, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading a multi-line response body: %v", err)
		}
		body += "\n" + strings.TrimSuffix(more, "\n")
	}
	return txnr, body
}

// The whole promise of this transport: 200 OK goes out only after the ring's
// guarantee is met.
func TestRELPAcknowledgesOnlyAfterTheRing(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	r := startRELP(t, h, RELPConfig{})

	c, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)

	relpSend(t, c, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	txnr, body := readRSP(t, c, br)
	if txnr != 1 || !strings.HasPrefix(body, "200 OK") {
		t.Fatalf("open answered %d %q", txnr, body)
	}
	if !strings.Contains(body, "commands=syslog") {
		t.Fatalf("the handshake did not advertise the syslog command: %q", body)
	}

	msg := `<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - relp`
	relpSend(t, c, 2, "syslog", msg)
	txnr, body = readRSP(t, c, br)
	if txnr != 2 || body != "200 OK" {
		t.Fatalf("syslog answered %d %q", txnr, body)
	}

	// The acknowledgement means the item is in the ring, now, not eventually.
	items := h.drainRing()
	if len(items) != 1 || string(items[0].Raw) != msg {
		t.Fatalf("ring holds %d items: %v", len(items), items)
	}
	if items[0].Head.Transport != envelope.TransportRELP ||
		items[0].Head.Framing != envelope.FramingRELP {
		t.Fatalf("wire = %v/%v", items[0].Head.Transport, items[0].Head.Framing)
	}
	if h.m.RELPAcksTotal.Load() != 1 {
		t.Fatalf("acks = %d", h.m.RELPAcksTotal.Load())
	}

	relpSendEmpty(t, c, 3, "close")
	if txnr, _ = readRSP(t, c, br); txnr != 3 {
		t.Fatalf("close answered %d", txnr)
	}
	if txnr, _ = readRSP(t, c, br); txnr != 0 {
		t.Fatalf("serverclose txnr = %d, want 0", txnr)
	}
}

// A full ring answers 500 with the reason, and the connection lives on. rsyslog
// retains the message and retries, which is exactly what is wanted.
func TestRELPRefusesWithFiveHundredOnAFullRing(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	r := startRELP(t, h, RELPConfig{BackpressureTimeout: 100 * time.Millisecond, Window: 1})

	c, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)

	// Fill the ring.
	relpSend(t, c, 1, "syslog", "<13>um")
	if _, body := readRSP(t, c, br); body != "200 OK" {
		t.Fatalf("first message answered %q", body)
	}
	// The next one cannot get in and the ring is not being drained.
	relpSend(t, c, 2, "syslog", "<13>dois")
	txnr, body := readRSP(t, c, br)
	if txnr != 2 || !strings.HasPrefix(body, "500") {
		t.Fatalf("answered %d %q, want a 500", txnr, body)
	}
	if !strings.Contains(body, "ring_full") {
		t.Fatalf("the refusal did not say why: %q", body)
	}
	// The connection survives: the emitter retries on it.
	relpSendEmpty(t, c, 3, "close")
	if txnr, _ := readRSP(t, c, br); txnr != 3 {
		t.Fatal("the connection did not survive a refusal")
	}
}

// A filtered message is acknowledged, not refused: the operator asked for it to
// be thrown away, so telling the emitter to retry would be a lie.
func TestRELPAcknowledgesAFilteredMessage(t *testing.T) {
	h := newHarness(t, 100, "err", nil)
	r := startRELP(t, h, RELPConfig{})

	c, _ := net.Dial("tcp", r.Addr())
	defer c.Close()
	br := bufio.NewReader(c)

	relpSend(t, c, 1, "syslog", "<167>debug junk")
	txnr, body := readRSP(t, c, br)
	if txnr != 1 || body != "200 OK" {
		t.Fatalf("answered %d %q, want 200 OK", txnr, body)
	}
	if h.ring.Stats().Items != 0 {
		t.Fatal("a filtered message entered the ring")
	}
	if h.m.MessagesDroppedTotal[obs.DropFilter].Load() != 1 {
		t.Fatal("the filtered message was not counted")
	}
}

func TestRELPUnknownCommandKeepsTheConnection(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	r := startRELP(t, h, RELPConfig{})

	c, _ := net.Dial("tcp", r.Addr())
	defer c.Close()
	br := bufio.NewReader(c)

	relpSendEmpty(t, c, 1, "whatever")
	txnr, body := readRSP(t, c, br)
	if txnr != 1 || !strings.HasPrefix(body, "500") {
		t.Fatalf("answered %d %q", txnr, body)
	}
	// Still usable: an unknown command is a client extension, not a broken stream.
	relpSend(t, c, 2, "syslog", "<13>ok")
	if txnr, body := readRSP(t, c, br); txnr != 2 || body != "200 OK" {
		t.Fatalf("the connection did not survive: %d %q", txnr, body)
	}
}

// The window is what lets a single connection stay fast when a ring=sqlite
// Enqueue waits for a group commit. Acknowledgements may come back out of order.
func TestRELPWindowAllowsConcurrentTransactions(t *testing.T) {
	h := newHarness(t, 1000, "debug", nil)
	r := startRELP(t, h, RELPConfig{Window: 16})

	c, _ := net.Dial("tcp", r.Addr())
	defer c.Close()
	br := bufio.NewReader(c)

	const n = 32
	for i := 1; i <= n; i++ {
		relpSend(t, c, int64(i), "syslog", fmt.Sprintf("<13>msg-%03d", i))
	}
	seen := map[int64]bool{}
	for i := 0; i < n; i++ {
		txnr, body := readRSP(t, c, br)
		if body != "200 OK" {
			t.Fatalf("txnr %d answered %q", txnr, body)
		}
		if seen[txnr] {
			t.Fatalf("txnr %d acknowledged twice", txnr)
		}
		seen[txnr] = true
	}
	if len(seen) != n {
		t.Fatalf("%d distinct acknowledgements of %d", len(seen), n)
	}
}
