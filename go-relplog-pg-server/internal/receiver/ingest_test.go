package receiver

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// tcpWire is the Wire a RELP session fills: TCP, octet-counting (SPEC §5.1).
func tcpWire(src string) *Wire {
	return &Wire{
		Transport: item.TransportTCP,
		Framing:   item.FramingOctetCounting,
		SrcIP:     netip.MustParseAddr(src),
		SrcPort:   4242,
		DstIP:     netip.MustParseAddr("10.0.0.10"),
		DstPort:   20514,
		Time:      time.Now(),
	}
}

// obs mirrors item.Transport on purpose, so the label of a counter is chosen by
// casting one enum into the other. That only works while the orders agree.
func TestTransportEnumsAgree(t *testing.T) {
	if int(obs.NumTransports) != item.NumTransports {
		t.Fatalf("obs has %d transports, item %d", obs.NumTransports, item.NumTransports)
	}
	for i := 0; i < item.NumTransports; i++ {
		if a, b := item.Transport(i).String(), obsTransport(item.Transport(i)).String(); a != b {
			t.Fatalf("transport %d: item %q, obs %q", i, a, b)
		}
	}
}

// ---------------------------------------------------------------- the pipeline

func TestIngestAcceptFillsTheHead(t *testing.T) {
	h := newHarness(t, 100)
	w := tcpWire("10.0.0.7")
	line := `<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - oi`
	if out := h.in.Accept(context.Background(), []byte(line), w); out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
	items := h.drainRing()
	if len(items) != 1 {
		t.Fatalf("items = %d", len(items))
	}
	hd := items[0].Head
	if !hd.UUID.IsValid() {
		t.Fatalf("uuid %v is not a UUIDv7", hd.UUID)
	}
	if hd.From != testIdentity.From || hd.Space != testIdentity.Space {
		t.Fatalf("identity = %q %q", hd.From, hd.Space)
	}
	if hd.Transport != item.TransportTCP || hd.Framing != item.FramingOctetCounting {
		t.Fatalf("wire = %v/%v", hd.Transport, hd.Framing)
	}
	if hd.SrcIP != netip.MustParseAddr("10.0.0.7") || hd.SrcPort != 4242 ||
		hd.DstIP != netip.MustParseAddr("10.0.0.10") || hd.DstPort != 20514 {
		t.Fatalf("addresses = %+v", hd)
	}
	if hd.Proxied() {
		t.Fatalf("a head with a proxy peer it never had: %+v", hd)
	}
	if hd.Size != len(line) {
		t.Fatalf("size = %d", hd.Size)
	}
	// The item owns its own copy: the transport's buffer is reused.
	if string(items[0].Raw) != line {
		t.Fatalf("raw = %q", items[0].Raw)
	}
	// What the pipeline builds must be what the drain worker can turn into a
	// valid envelope, or the sink would refuse every row it writes.
	checkConverts(t, items[0])
}

// SPEC §3.1, invariant §20.14: ONE clock sample per message, and both the uuid
// and the event come from it. The uuid's level-3 instant must equal the head's
// clock sample to the nanosecond.
func TestIngestUUIDAndEventShareOneSample(t *testing.T) {
	h := newHarness(t, 100)
	sample := time.Date(2026, 10, 5, 12, 0, 0, 123456789, time.UTC)
	w := tcpWire("10.0.0.7")
	w.Time = sample
	if out := h.in.Accept(context.Background(), []byte("<13>x"), w); out != Accepted {
		t.Fatal(out)
	}
	hd := h.drainRing()[0].Head
	if !hd.Time().Equal(sample) {
		t.Fatalf("head sample %v, want %v", hd.Time(), sample)
	}
	got, ok := hd.UUID.TimestampWithLevel(uuidv7.Level3)
	if !ok || !got.Equal(sample) {
		t.Fatalf("uuid instant %v, want the sample %v", got, sample)
	}
}

// There is no filter in this server (SPEC §1.2): every severity gets in, and a
// line without a PRI -- or with one that is not valid -- gets in too. Accept
// must not even READ the PRI; deciding what a message is belongs to the drain
// worker, and a receive path that parsed would be paying for it on every
// frame. The raw line reaches the ring byte for byte.
func TestIngestNeverFilters(t *testing.T) {
	h := newHarness(t, 100)
	lines := []string{
		"<167>debug noise",           // local4.debug: the severity a filter drops first
		"<15>Sep 12 10:00:00 h a: x", // user.debug
		"linha solta sem PRI",        // no PRI at all
		"<192>pri invalido",          // a PRI out of range
		"<013>zero a esquerda",       // a PRI with a leading zero
	}
	for _, line := range lines {
		if out := h.in.Accept(context.Background(), []byte(line), tcpWire("10.0.0.1")); out != Accepted {
			t.Errorf("%q: outcome = %v, want Accepted", line, out)
		}
	}
	items := h.drainRing()
	if len(items) != len(lines) {
		t.Fatalf("%d of %d messages reached the ring", len(items), len(lines))
	}
	for i, it := range items {
		if string(it.Raw) != lines[i] {
			t.Errorf("raw %q, want %q untouched", it.Raw, lines[i])
		}
	}
	for r := range h.m.MessagesDroppedTotal {
		if got := h.m.MessagesDroppedTotal[r].Load(); got != 0 {
			t.Errorf("messages_dropped{%s} = %d for messages that all got in", obs.DropReason(r), got)
		}
	}
}

func TestIngestSinkDownFailsFast(t *testing.T) {
	h := newHarness(t, 100)
	h.sink.up.Store(false)
	out := h.in.Accept(context.Background(), []byte("<13>oi"), tcpWire("10.0.0.1"))
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
	// On this server a held message is handed back to the emitter, never
	// dropped: RecordDrop counts nothing for it (SPEC §5.7).
	h.in.RecordDrop(out)
	for r := range h.m.MessagesDroppedTotal {
		if h.m.MessagesDroppedTotal[r].Load() != 0 {
			t.Fatalf("sink DOWN counted as %s", obs.DropReason(r))
		}
	}
}

// SPEC §2.1: sink DOWN is decided BEFORE the uuid and the copy. A message
// that is not getting in must cost neither a random draw nor a pooled buffer:
// that is the whole time a down sink lasts, on every retry of every held
// transaction (SPEC §5.7), which would otherwise churn the pool for nothing.
// The stub ring
// proves Enqueue was never reached, the zero uuid proves step 4 never started,
// and the control case proves both happen once the sink is back.
func TestIngestSinkDownFailsBeforeTheCopy(t *testing.T) {
	stub := &stubRing{}
	sink := newUp()
	sink.up.Store(false)
	in := NewIngest(stub, sink, newMetrics(), logger(), defaultIngestConfig())

	w := tcpWire("10.0.0.7")
	if out := in.Accept(context.Background(), []byte("<13>x"), w); out != SinkDown {
		t.Fatalf("outcome = %v, want SinkDown", out)
	}
	for i := 0; i < 3; i++ {
		if out := in.Retry(context.Background(), []byte("<13>x"), w); out != SinkDown {
			t.Fatalf("retry %d: %v, want SinkDown", i, out)
		}
	}
	if len(stub.items) != 0 {
		t.Fatalf("%d items reached Enqueue with the sink DOWN", len(stub.items))
	}
	if !w.UUID.IsZero() {
		t.Fatalf("a uuid %v was minted for a message that never got in", w.UUID)
	}

	// The control: the same held message, once the sink is UP, is built and
	// enqueued -- so the check above is not passing because nothing ever is.
	sink.up.Store(true)
	if out := in.Retry(context.Background(), []byte("<13>x"), w); out != Accepted {
		t.Fatalf("sink back UP: %v, want Accepted", out)
	}
	if len(stub.items) != 1 || w.UUID.IsZero() || stub.items[0].Head.UUID != w.UUID {
		t.Fatalf("items %d, uuid %v: the message was not built once the sink came back", len(stub.items), w.UUID)
	}
}

func TestIngestRingFullIsRetryable(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte("<13>um"), tcpWire("10.0.0.1")); out != Accepted {
		t.Fatal(out)
	}
	out := h.in.Accept(context.Background(), []byte("<13>dois"), tcpWire("10.0.0.1"))
	if out != RingFull || !out.Retryable() {
		t.Fatalf("outcome = %v retryable=%v", out, out.Retryable())
	}
}

// SPEC §15.2, error paths. Which Enqueue errors let the receive path pool the
// buffer and which must leave it to the GC: the ring refusing outright -- full
// or closed, however wrapped -- means nobody below saw the item. The memory
// ring has no other error; anything else is unexplained, so the buffer is
// discarded rather than pooled (nothing here can prove nobody kept a view of
// it) and the message is held like a full ring, which clears on its own.
func TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		want   Outcome
		pooled bool
	}{
		{"ring full releases", ring.ErrFull, RingFull, true},
		{"wrapped full releases", fmt.Errorf("enqueue: %w", ring.ErrFull), RingFull, true},
		{"ring closed releases", ring.ErrClosed, Shutdown, true},
		{"wrapped closed releases", fmt.Errorf("enqueue: %w", ring.ErrClosed), Shutdown, true},
		{"deadline discards", context.DeadlineExceeded, RingFull, false},
		{"cancellation discards, no answer", context.Canceled, Shutdown, false},
		{"anything else discards", errors.New("enqueue: something odd"), RingFull, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{errs: []error{tc.err}}
			in := NewIngest(stub, newUp(), newMetrics(), logger(), defaultIngestConfig())

			out := in.Accept(context.Background(), []byte("<13>hello"), tcpWire("10.0.0.7"))
			if out != tc.want {
				t.Fatalf("outcome = %s, want %s", out, tc.want)
			}
			if len(stub.items) != 1 {
				t.Fatal("the pipeline never reached Enqueue")
			}
			it := stub.items[0]
			if !it.Released() {
				t.Fatal("the item was neither released nor discarded: a leak of the ownership rule")
			}
			if tc.pooled && it.Raw != nil {
				t.Error("Release must clear the buffer it pooled")
			}
			if !tc.pooled && (it.Raw == nil || !it.Tainted()) {
				t.Error("Discard must leave the buffer alone; something below may still read it")
			}
		})
	}
}

// An Enqueue error the ring is not supposed to have is a bug somewhere, and the
// one place it can be seen is the log: RingFull alone would make it look like
// ordinary saturation. It is logged at ERROR -- sampled, because a ring that
// keeps failing would otherwise write one line per message (SPEC §14.2) -- and
// the ordinary refusals, full and closed, are not logged at all.
func TestIngestUnexpectedEnqueueErrorIsLogged(t *testing.T) {
	var buf syncBuf
	odd := errors.New("enqueue: something odd")
	stub := &stubRing{errs: []error{ring.ErrFull, ring.ErrClosed, odd, odd, odd}}
	in := NewIngest(stub, newUp(), newMetrics(), obs.NewLogger(&buf, "text", "warn"), defaultIngestConfig())

	for _, want := range []Outcome{RingFull, Shutdown} {
		if out := in.Accept(context.Background(), []byte("<13>x"), tcpWire("10.0.0.1")); out != want {
			t.Fatalf("outcome = %v, want %v", out, want)
		}
	}
	if buf.String() != "" {
		t.Fatalf("an ordinary refusal was logged: %q", buf.String())
	}

	for i := 0; i < 3; i++ {
		out := in.Accept(context.Background(), []byte("<13>x"), tcpWire("10.0.0.1"))
		if out != RingFull || !out.Retryable() {
			t.Fatalf("attempt %d: outcome = %v, want a retryable RingFull", i, out)
		}
	}
	for _, it := range stub.items[2:] {
		if !it.Released() || !it.Tainted() || it.Raw == nil {
			t.Fatal("an item refused with an unexplained error was pooled; it must be discarded")
		}
	}
	logged := buf.String()
	if n := strings.Count(logged, "could not accept a message"); n != 1 {
		t.Fatalf("%d lines for 3 failures in a burst, want 1 (sampled):\n%s", n, logged)
	}
	if !strings.Contains(logged, "something odd") || !strings.Contains(logged, "level=ERROR") {
		t.Fatalf("the line does not carry the error at ERROR: %q", logged)
	}
	// Invariant §20.10: no payload in a log line at INFO or above.
	if strings.Contains(logged, "<13>x") {
		t.Fatalf("the line carries the payload: %q", logged)
	}
}

// A message held through backpressure is ONE fact, with one uuid (SPEC §3.1:
// the uuid is never regenerated on retry). The uuid minted on the FIRST try is
// written back into the Wire and every later attempt -- after a full ring, after
// an unexplained error whose buffer was discarded -- builds its item with it,
// so the row that finally lands has the identity the message had on arrival.
func TestIngestRetryReusesTheUUID(t *testing.T) {
	stub := &stubRing{errs: []error{ring.ErrFull, errors.New("enqueue: something odd"), ring.ErrFull, nil}}
	in := NewIngest(stub, newUp(), newMetrics(), logger(), defaultIngestConfig())
	w := tcpWire("10.0.0.7")
	if !w.UUID.IsZero() {
		t.Fatal("a transport-filled Wire must start without a uuid")
	}
	if out := in.Accept(context.Background(), []byte("<13>x"), w); out != RingFull {
		t.Fatal(out)
	}
	first := w.UUID
	if first.IsZero() || stub.items[0].Head.UUID != first {
		t.Fatalf("the first try minted %v into the Wire, %v into the item", first, stub.items[0].Head.UUID)
	}
	for _, want := range []Outcome{RingFull, RingFull, Accepted} {
		if out := in.Retry(context.Background(), []byte("<13>x"), w); out != want {
			t.Fatalf("retry: %v, want %v", out, want)
		}
	}
	if len(stub.items) != 4 {
		t.Fatalf("%d attempts", len(stub.items))
	}
	for i, it := range stub.items {
		if it.Head.UUID != first {
			t.Fatalf("attempt %d has uuid %v, the first %v", i, it.Head.UUID, first)
		}
		// The uuid is not the only thing that must not drift: the clock
		// sample it came from is the event of every attempt.
		if !it.Head.Time().Equal(stub.items[0].Head.Time()) {
			t.Fatalf("attempt %d has event %v, the first %v", i, it.Head.Time(), stub.items[0].Head.Time())
		}
	}
}

func TestOutcomeRetryability(t *testing.T) {
	retryable := map[Outcome]bool{
		Accepted: false, Empty: false, Unaddressable: false, Shutdown: false,
		SinkDown: true, RingFull: true,
	}
	for o, want := range retryable {
		if o.Retryable() != want {
			t.Errorf("%v.Retryable() = %v, want %v", o, o.Retryable(), want)
		}
	}
}

// The final refusals map to the reasons of SPEC §14.1, with the outcome's own
// name -- what a log line prints -- the same word. What RELP hands back to the
// emitter (held, shutdown) is no drop: librelp retransmits it (SPEC §5.7), and
// the session counts it in relp_unanswered_total.
func TestOutcomeDropReasons(t *testing.T) {
	for o, want := range map[Outcome]string{Empty: "empty", Unaddressable: "unaddressable"} {
		r, ok := o.dropReason()
		if !ok || r.String() != want {
			t.Errorf("%v -> %q %v, want %q", o, r, ok, want)
		}
		if got := o.String(); got != want {
			t.Errorf("outcome named %q, its reason %q", got, want)
		}
	}
	for o, name := range map[Outcome]string{Accepted: "accepted", SinkDown: "sink_down",
		RingFull: "ring_full", Shutdown: "shutdown"} {
		if _, ok := o.dropReason(); ok {
			t.Errorf("%v is counted as a drop", o)
		}
		if o.String() != name {
			t.Errorf("outcome %d named %q, want %q", o, o.String(), name)
		}
	}
	// RecordDrop of a held outcome counts nothing: it is called with every
	// final outcome, and a held one is handed back, not lost.
	m := newMetrics()
	in := NewIngest(&stubRing{}, newUp(), m, logger(), defaultIngestConfig())
	for _, o := range []Outcome{Accepted, SinkDown, RingFull, Shutdown} {
		in.RecordDrop(o)
	}
	for r := range m.MessagesDroppedTotal {
		if m.MessagesDroppedTotal[r].Load() != 0 {
			t.Fatalf("a message that was not dropped was counted as %s", obs.DropReason(r))
		}
	}
}

// x-proxy only when a PROXY header replaced the addresses (SPEC §4.6): the
// peer is carried, canonical, exactly when the transport set it.
func TestIngestPeerOnlyWhereItBelongs(t *testing.T) {
	h := newHarness(t, 10)

	w := tcpWire("10.0.0.1")
	w.PeerIP, w.PeerPort = netip.MustParseAddr("::ffff:192.0.2.1"), 9
	h.in.Accept(context.Background(), []byte("<13>proxied"), w)

	h.in.Accept(context.Background(), []byte("<13>direct"), tcpWire("10.0.0.1"))

	items := h.drainRing()
	if len(items) != 2 {
		t.Fatalf("%d items", len(items))
	}
	for _, it := range items {
		hd := it.Head
		switch string(it.Raw) {
		case "<13>direct":
			if hd.Proxied() {
				t.Errorf("%s: %+v", it.Raw, hd)
			}
		case "<13>proxied":
			if !hd.Proxied() || hd.PeerIP != netip.MustParseAddr("192.0.2.1") || hd.PeerPort != 9 {
				t.Errorf("tcp: peer %v:%d (must be canonical)", hd.PeerIP, hd.PeerPort)
			}
		}
		checkConverts(t, it)
	}
}

// Counting is split across Accept, Retry and RecordDrop so a held message is one
// arrival, not six hundred (SPEC §5.7), and one RecordDrop of a final refusal
// is one drop.
func TestRetryDoesNotRecount(t *testing.T) {
	h := newHarness(t, 1)
	h.in.Accept(context.Background(), []byte("<13>um"), tcpWire("10.0.0.1")) // fills the ring

	w := tcpWire("10.0.0.1")
	h.in.Accept(context.Background(), []byte("<13>dois"), w)
	for i := 0; i < 5; i++ {
		h.in.Retry(context.Background(), []byte("<13>dois"), w)
	}
	h.in.RecordDrop(RingFull) // handed back: no drop
	h.in.RecordDrop(Empty)

	if got := h.m.MessagesTotal[obs.TransportTCP].Load(); got != 2 {
		t.Fatalf("arrivals = %d, want 2", got)
	}
	if got := h.m.MessageBytes.Count(); got != 2 {
		t.Fatalf("message_bytes observed %d times, want 2", got)
	}
	var drops uint64
	for r := range h.m.MessagesDroppedTotal {
		drops += h.m.MessagesDroppedTotal[r].Load()
	}
	if got := h.m.MessagesDroppedTotal[obs.DropEmpty].Load(); got != 1 || drops != 1 {
		t.Fatalf("drops = %d (empty %d), want the one empty", drops, got)
	}
}

// SPEC §5.3: truncation happens BEFORE the copy, size stays original, and the
// truncation is counted once, per transport.
func TestIngestTruncatesBeforeTheCopy(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 10, cfg)
	line := "<13>" + strings.Repeat("z", 996)
	w := tcpWire("10.0.0.1")
	w.Size = len(line)
	if out := h.in.Accept(context.Background(), []byte(line), w); out != Accepted {
		t.Fatal(out)
	}
	it := h.drainRing()[0]
	if len(it.Raw) != 480 || string(it.Raw) != line[:480] {
		t.Fatalf("raw has %d bytes", len(it.Raw))
	}
	if it.Head.Size != 1000 {
		t.Fatalf("size = %d, want the original 1000", it.Head.Size)
	}
	if cap(it.Raw) > 2048 {
		t.Fatalf("the pooled buffer has %d bytes: it was sized before the cut", cap(it.Raw))
	}
	if h.m.MessagesTruncatedTotal[obs.TransportTCP].Load() != 1 {
		t.Fatal("truncation not counted")
	}
	// A frame the framer already cut arrives with its original size in Wire.
	w = tcpWire("10.0.0.1")
	w.Size = 5000
	h.in.Accept(context.Background(), []byte(line[:480]), w)
	if it := h.drainRing()[0]; it.Head.Size != 5000 || len(it.Raw) != 480 {
		t.Fatalf("size %d raw %d", it.Head.Size, len(it.Raw))
	}
	if h.m.MessagesTruncatedTotal[obs.TransportTCP].Load() != 2 {
		t.Fatal("the truncation by the framer not counted")
	}
}

// SPEC §5.4: zero octets is no envelope, counted as reason="empty", never
// copied into the ring.
func TestIngestEmptyIsCounted(t *testing.T) {
	h := newHarness(t, 10)
	out := h.in.Accept(context.Background(), nil, tcpWire("10.0.0.1"))
	if out != Empty || out.Retryable() {
		t.Fatalf("outcome = %v", out)
	}
	h.in.RecordDrop(out)
	if h.m.MessagesDroppedTotal[obs.DropEmpty].Load() != 1 || h.ring.Stats().Items != 0 {
		t.Fatal("an empty message was not dropped as empty")
	}
}

// A source of 0.0.0.0 has no address the payload could record (SPEC §4.2),
// and an envelope without host.src-ip fails the schema. A TCP peer always has
// one: on this server it is a guard, answered with a final 500 (SPEC §5.4).
func TestIngestUnaddressableNetworkMessage(t *testing.T) {
	h := newHarness(t, 10)
	for _, src := range []string{"0.0.0.0", "::"} {
		out := h.in.Accept(context.Background(), []byte("<13>x"), tcpWire(src))
		if out != Unaddressable {
			t.Fatalf("%s: outcome = %v", src, out)
		}
		h.in.RecordDrop(out)
	}
	if h.m.MessagesDroppedTotal[obs.DropUnaddressable].Load() != 2 || h.ring.Stats().Items != 0 {
		t.Fatal("an unaddressable message got in, or was not counted")
	}
}

// The access log is metadata only and off by default; with it on, Accept still
// works and does not panic on any transport.
func TestIngestAccessLog(t *testing.T) {
	h := newHarness(t, 10)
	var buf strings.Builder
	l := obs.NewLogger(&buf, "json", "info")
	l.SetAccess(true, 1)
	h.in = NewIngest(h.ring, h.sink, h.m, l, h.cfg)
	if out := h.in.Accept(context.Background(), []byte("<13>secret"), tcpWire("10.0.0.1")); out != Accepted {
		t.Fatal(out)
	}
	if !strings.Contains(buf.String(), `"accepted"`) {
		t.Fatalf("no access line: %q", buf.String())
	}
	// Invariant §20.10: no payload in a log line at INFO or above.
	if strings.Contains(buf.String(), "secret") {
		t.Fatalf("the access line carries the payload: %q", buf.String())
	}
}

// releaseRing accepts every item and pools its buffer at once, so a benchmark
// measures the pipeline and not a growing ring.
type releaseRing struct{ stubRing }

func (r *releaseRing) Enqueue(ctx context.Context, it *item.Item) error {
	it.Release()
	return nil
}

// BenchmarkIngestAccept is the per-message cost of the receive path for a
// typical 200-byte line: uuid, head, pooled copy, enqueue. Nothing reads the
// line itself.
func BenchmarkIngestAccept(b *testing.B) {
	in := NewIngest(&releaseRing{}, newUp(), newMetrics(), logger(), defaultIngestConfig())
	line := []byte("<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 - " + strings.Repeat("m", 140))
	w := tcpWire("10.0.0.7")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.UUID = uuidv7.UUID{}
		in.Accept(context.Background(), line, w)
	}
}

// Review C of go-syslog-pg-server, win 2: a full ring is refused before the
// uuid, the copy and the item. A held RELP transaction retries every 50 ms
// (SPEC §5.7), and the refusal used to cost as much as an acceptance. The
// control is the accepted message that fills the ring.
func TestIngestFullRingIsRefusedBeforeTheCopy(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte("<13>1 - - a - - - first"), tcpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
	w := tcpWire("10.0.0.3")
	allocs := testing.AllocsPerRun(200, func() {
		w.UUID = uuidv7.UUID{}
		if out := h.in.Accept(context.Background(), []byte("<13>1 - - a - - - second"), w); out != RingFull {
			t.Fatalf("full ring: %v", out)
		}
	})
	if allocs != 0 {
		t.Fatalf("refusing on a full ring allocated %.1f times per message, want 0", allocs)
	}
	if !w.UUID.IsZero() {
		t.Fatal("a uuid was drawn for a message the full ring refused")
	}
}
