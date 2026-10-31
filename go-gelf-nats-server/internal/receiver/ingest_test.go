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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ring"
)

func udpWire(src string) *Wire {
	return &Wire{
		Transport: item.TransportUDP,
		SrcIP:     netip.MustParseAddr(src),
		SrcPort:   4242,
		DstIP:     netip.MustParseAddr("10.0.0.10"),
		DstPort:   1514,
		Time:      time.Now(),
	}
}

func tcpWire(src string) *Wire {
	w := udpWire(src)
	w.Transport = item.TransportTCP
	return w
}

// obs has only the transports of this server; obsTransport maps the item's
// enumeration (go-gelf-server's, copied as is) onto it by name, and the two
// it does not have onto their plaintext twins, which are never built here.
func TestTransportEnumsAgree(t *testing.T) {
	if obs.NumTransports != 3 {
		t.Fatalf("obs has %d transports, want udp, tcp and http", obs.NumTransports)
	}
	for tr, want := range map[item.Transport]string{
		item.TransportUDP: "udp", item.TransportTCP: "tcp", item.TransportHTTP: "http",
		item.TransportTLS: "tcp", item.TransportHTTPS: "http",
	} {
		if got := obsTransport(tr).String(); got != want {
			t.Errorf("transport %s: obs %q, want %q", tr, got, want)
		}
	}
}

// ---------------------------------------------------------------- the pipeline

func TestIngestAcceptFillsTheHead(t *testing.T) {
	h := newHarness(t, 100)
	w := udpWire("10.0.0.7")
	line := doc("oi")
	w.Compression, w.Chunks = item.CompressionGzip, 3
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
	if hd.Transport != item.TransportUDP || hd.Compression != item.CompressionGzip || hd.Chunks != 3 {
		t.Fatalf("wire = %v/%v/%d", hd.Transport, hd.Compression, hd.Chunks)
	}
	if hd.SrcIP != netip.MustParseAddr("10.0.0.7") || hd.SrcPort != 4242 ||
		hd.DstIP != netip.MustParseAddr("10.0.0.10") || hd.DstPort != 1514 {
		t.Fatalf("addresses = %+v", hd)
	}
	if hd.Proxied() {
		t.Fatalf("a UDP head with a proxy peer: %+v", hd)
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
	w := udpWire("10.0.0.7")
	w.Time = sample
	if out := h.in.Accept(context.Background(), []byte(doc("x")), w); out != Accepted {
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

// There is no filter in this server (SPEC §1.2): every level gets in, and a
// document without a level -- or with one that is not valid, or one that is not
// JSON at all -- gets in too. Accept must not even READ the level; deciding
// what a document is belongs to the drain worker, and a receive path that
// parsed would be paying for it on every datagram of a flood. The document
// reaches the ring byte for byte.
func TestIngestNeverFilters(t *testing.T) {
	h := newHarness(t, 100)
	lines := []string{
		`{"version":"1.1","host":"h","short_message":"debug noise","level":7}`, // the level a filter drops first
		`{"version":"1.1","host":"h","short_message":"no level"}`,              // absent: 6 at conversion
		`{"short_message":"bad","level":"x"}`,                                  // not a level
		`{"short_message":"out of range","level":9}`,                           // out of range
		`not json at all`, // malformed
	}
	for _, line := range lines {
		if out := h.in.Accept(context.Background(), []byte(line), udpWire("10.0.0.1")); out != Accepted {
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
	for r := obs.DropReason(0); r <= obs.DropShutdown; r++ {
		if got := h.m.MessagesDroppedTotal[r].Load(); got != 0 {
			t.Errorf("messages_dropped{%s} = %d for messages that all got in", r, got)
		}
	}
}

func TestIngestSinkDownFailsFast(t *testing.T) {
	h := newHarness(t, 100)
	h.sink.up.Store(false)
	out := h.in.Accept(context.Background(), []byte(doc("oi")), udpWire("10.0.0.1"))
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
	h.in.RecordDrop(out)
	if h.m.MessagesDroppedTotal[obs.DropSinkDown].Load() != 1 {
		t.Fatal("sink_down not counted")
	}
}

// SPEC §2.1: sink DOWN is decided BEFORE the uuid and the copy. A message
// that is not getting in must cost neither a random draw nor a pooled buffer:
// that is the whole time a down sink lasts, on every datagram, and a flood
// arriving then would otherwise churn the pool for nothing. The stub ring
// proves Enqueue was never reached, the zero uuid proves step 4 never started,
// and the control case proves both happen once the sink is back.
func TestIngestSinkDownFailsBeforeTheCopy(t *testing.T) {
	stub := &stubRing{}
	sink := newUp()
	sink.up.Store(false)
	in := NewIngest(stub, sink, newMetrics(), logger(), defaultIngestConfig())

	w := tcpWire("10.0.0.7")
	if out := in.Accept(context.Background(), []byte(doc("x")), w); out != SinkDown {
		t.Fatalf("outcome = %v, want SinkDown", out)
	}
	for i := 0; i < 3; i++ {
		if out := in.Retry(context.Background(), []byte(doc("x")), w); out != SinkDown {
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
	if out := in.Retry(context.Background(), []byte(doc("x")), w); out != Accepted {
		t.Fatalf("sink back UP: %v, want Accepted", out)
	}
	if len(stub.items) != 1 || w.UUID.IsZero() || stub.items[0].Head.UUID != w.UUID {
		t.Fatalf("items %d, uuid %v: the message was not built once the sink came back", len(stub.items), w.UUID)
	}
}

func TestIngestRingFullIsRetryable(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte(doc("um")), tcpWire("10.0.0.1")); out != Accepted {
		t.Fatal(out)
	}
	out := h.in.Accept(context.Background(), []byte(doc("dois")), tcpWire("10.0.0.1"))
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
		{"cancellation discards", context.Canceled, Shutdown, false},
		{"anything else discards", errors.New("enqueue: something odd"), RingFull, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{errs: []error{tc.err}}
			in := NewIngest(stub, newUp(), newMetrics(), logger(), defaultIngestConfig())

			out := in.Accept(context.Background(), []byte(doc("hello")), tcpWire("10.0.0.7"))
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
		if out := in.Accept(context.Background(), []byte(doc("x")), tcpWire("10.0.0.1")); out != want {
			t.Fatalf("outcome = %v, want %v", out, want)
		}
	}
	if buf.String() != "" {
		t.Fatalf("an ordinary refusal was logged: %q", buf.String())
	}

	for i := 0; i < 3; i++ {
		out := in.Accept(context.Background(), []byte(doc("x")), tcpWire("10.0.0.1"))
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
	if strings.Contains(logged, doc("x")) {
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
	if out := in.Accept(context.Background(), []byte(doc("x")), w); out != RingFull {
		t.Fatal(out)
	}
	first := w.UUID
	if first.IsZero() || stub.items[0].Head.UUID != first {
		t.Fatalf("the first try minted %v into the Wire, %v into the item", first, stub.items[0].Head.UUID)
	}
	for _, want := range []Outcome{RingFull, RingFull, Accepted} {
		if out := in.Retry(context.Background(), []byte(doc("x")), w); out != want {
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
		TooLarge: false, DecompressFailed: false, ChunkInvalid: false, ChunkExpired: false, ChunkOverflow: false,
	}
	for o, want := range retryable {
		if o.Retryable() != want {
			t.Errorf("%v.Retryable() = %v, want %v", o, o.Retryable(), want)
		}
	}
}

// Every outcome but Accepted maps to the reason of SPEC §14.1 with its name,
// and the outcome's own name -- what the backpressure WARN prints -- is the
// same word, so a log line and a counter can be matched by eye.
func TestOutcomeDropReasons(t *testing.T) {
	for o, want := range map[Outcome]string{
		SinkDown: "sink_down", RingFull: "ring_full", Shutdown: "shutdown", Empty: "empty",
		Unaddressable: "malformed_frame", TooLarge: "too_large", DecompressFailed: "decompress",
		ChunkInvalid: "chunk_invalid", ChunkExpired: "chunk_timeout", ChunkOverflow: "chunk_overflow",
	} {
		if got := o.dropReason().String(); got != want {
			t.Errorf("%v -> %q, want %q", o, got, want)
		}
		if got := o.String(); got != want {
			t.Errorf("outcome named %q, its reason %q", got, want)
		}
	}
	// RecordDrop of Accepted counts nothing: it is called with every outcome.
	m := newMetrics()
	NewIngest(&stubRing{}, newUp(), m, logger(), defaultIngestConfig()).RecordDrop(Accepted)
	for r := obs.DropReason(0); r <= obs.DropShutdown; r++ {
		if m.MessagesDroppedTotal[r].Load() != 0 {
			t.Fatalf("an accepted message was counted as dropped (%s)", r)
		}
	}
}

// x-proxy only when a PROXY header replaced the addresses, and never on UDP:
// PROXY is read on TCP alone (SPEC §4.6), so a peer on a datagram Wire can only
// be a transport's mistake and must not reach the payload.
func TestIngestPeerOnlyWhereItBelongs(t *testing.T) {
	h := newHarness(t, 10)

	w := udpWire("10.0.0.1")
	w.PeerIP, w.PeerPort = netip.MustParseAddr("192.0.2.1"), 9
	h.in.Accept(context.Background(), []byte(doc("udp")), w)

	w = tcpWire("10.0.0.1")
	w.PeerIP, w.PeerPort = netip.MustParseAddr("::ffff:192.0.2.1"), 9
	h.in.Accept(context.Background(), []byte(doc("proxied")), w)

	h.in.Accept(context.Background(), []byte(doc("direct")), tcpWire("10.0.0.1"))

	items := h.drainRing()
	if len(items) != 3 {
		t.Fatalf("%d items", len(items))
	}
	for _, it := range items {
		hd := it.Head
		switch string(it.Raw) {
		case doc("udp"), doc("direct"):
			if hd.Proxied() {
				t.Errorf("%s: %+v", it.Raw, hd)
			}
		case doc("proxied"):
			if !hd.Proxied() || hd.PeerIP != netip.MustParseAddr("192.0.2.1") || hd.PeerPort != 9 {
				t.Errorf("tcp: peer %v:%d (must be canonical)", hd.PeerIP, hd.PeerPort)
			}
		}
		checkConverts(t, it)
	}
}

// Counting is split across Accept, Retry and RecordDrop so a held message is one
// arrival and one drop, not six hundred of each (SPEC §5.7).
func TestRetryDoesNotRecount(t *testing.T) {
	h := newHarness(t, 1)
	h.in.Accept(context.Background(), []byte(doc("um")), tcpWire("10.0.0.1")) // fills the ring

	w := tcpWire("10.0.0.1")
	h.in.Accept(context.Background(), []byte(doc("dois")), w)
	for i := 0; i < 5; i++ {
		h.in.Retry(context.Background(), []byte(doc("dois")), w)
	}
	h.in.RecordDrop(RingFull)

	if got := h.m.MessagesTotal[obs.TransportTCP].Load(); got != 2 {
		t.Fatalf("arrivals = %d, want 2", got)
	}
	if got := h.m.MessageBytes.Count(); got != 2 {
		t.Fatalf("message_bytes observed %d times, want 2", got)
	}
	if got := h.m.MessagesDroppedTotal[obs.DropRingFull].Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
}

// SPEC §5.5: a document above max_message_bytes is refused whole, never cut,
// and before the copy; exactly max is accepted.
func TestIngestTooLargeIsRefusedWhole(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 1024
	h := newHarnessCfg(t, 10, cfg)
	exact := []byte(doc(strings.Repeat("z", 1024-len(doc("")))))
	if len(exact) != 1024 {
		t.Fatalf("test document has %d bytes", len(exact))
	}
	if out := h.in.Accept(context.Background(), exact, udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("exactly max: %v", out)
	}
	over := append(exact[:len(exact):len(exact)], ' ')
	out := h.in.Accept(context.Background(), over, udpWire("10.0.0.1"))
	if out != TooLarge || out.Retryable() {
		t.Fatalf("max+1: %v", out)
	}
	h.in.RecordDrop(out)
	if items := h.drainRing(); len(items) != 1 || len(items[0].Raw) != 1024 {
		t.Fatalf("ring holds %d items", len(items))
	}
	if h.m.MessagesDroppedTotal[obs.DropTooLarge].Load() != 1 {
		t.Fatal("too_large not counted")
	}
	if h.m.MessagesTotal[obs.TransportUDP].Load() != 2 {
		t.Fatal("arrivals not counted")
	}
}

// Reject is the transport's refusal before Accept (too large on the wire, a
// container that does not decode, a failed reassembly): one arrival, one drop.
func TestIngestRejectCountsArrivalAndDrop(t *testing.T) {
	h := newHarness(t, 10)
	for _, o := range []Outcome{TooLarge, DecompressFailed, ChunkInvalid, ChunkExpired, ChunkOverflow} {
		h.in.Reject(item.TransportUDP, o)
	}
	h.in.Reject(item.TransportHTTP, DecompressFailed)
	if got := h.m.MessagesTotal[obs.TransportUDP].Load(); got != 5 {
		t.Fatalf("udp arrivals = %d", got)
	}
	if got := h.m.MessagesTotal[obs.TransportHTTP].Load(); got != 1 {
		t.Fatalf("http arrivals = %d", got)
	}
	for r, want := range map[obs.DropReason]uint64{obs.DropTooLarge: 1, obs.DropDecompress: 2,
		obs.DropChunkInvalid: 1, obs.DropChunkTimeout: 1, obs.DropChunkOverflow: 1} {
		if got := h.m.MessagesDroppedTotal[r].Load(); got != want {
			t.Errorf("%s = %d, want %d", r, got, want)
		}
	}
	if h.ring.Stats().Items != 0 {
		t.Fatal("a rejected document entered the ring")
	}
}

// SPEC §5.6: zero octets, or only JSON whitespace, is no envelope, counted
// as reason="empty", never copied into the ring.
func TestIngestEmptyIsCounted(t *testing.T) {
	h := newHarness(t, 10)
	for _, b := range [][]byte{nil, {}, []byte(" \t\r\n ")} {
		out := h.in.Accept(context.Background(), b, udpWire("10.0.0.1"))
		if out != Empty || out.Retryable() {
			t.Fatalf("%q: outcome = %v", b, out)
		}
		h.in.RecordDrop(out)
	}
	if h.m.MessagesDroppedTotal[obs.DropEmpty].Load() != 3 || h.ring.Stats().Items != 0 {
		t.Fatal("an empty document was not dropped as empty")
	}
	// Anything else, even a single non-JSON byte, is a document.
	if out := h.in.Accept(context.Background(), []byte("x"), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("a one-byte document: %v", out)
	}
}

// A datagram from 0.0.0.0 has no source the payload could record (SPEC §4.2),
// and an envelope without host.src-ip on udp fails the schema.
func TestIngestUnaddressableNetworkMessage(t *testing.T) {
	h := newHarness(t, 10)
	for _, src := range []string{"0.0.0.0", "::"} {
		out := h.in.Accept(context.Background(), []byte(doc("x")), udpWire(src))
		if out != Unaddressable {
			t.Fatalf("%s: outcome = %v", src, out)
		}
		h.in.RecordDrop(out)
	}
	if h.m.MessagesDroppedTotal[obs.DropMalformedFrame].Load() != 2 || h.ring.Stats().Items != 0 {
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
	if out := h.in.Accept(context.Background(), []byte(doc("secret")), udpWire("10.0.0.1")); out != Accepted {
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
// typical 200-byte document: uuid, head, pooled copy, enqueue. Nothing reads
// the document itself.
func BenchmarkIngestAccept(b *testing.B) {
	in := NewIngest(&releaseRing{}, newUp(), newMetrics(), logger(), defaultIngestConfig())
	line := []byte(doc(strings.Repeat("m", 140)))
	w := udpWire("10.0.0.7")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.UUID = uuidv7.UUID{}
		in.Accept(context.Background(), line, w)
	}
}

// Review C, win 2: a full ring is refused before the uuid, the copy and the
// item. On a UDP flood, or a TCP reader retrying every 50 ms, the refusal used
// to cost as much as an acceptance. The control is the accepted message that
// fills the ring.
func TestIngestFullRingIsRefusedBeforeTheCopy(t *testing.T) {
	h := newHarness(t, 1)
	if out := h.in.Accept(context.Background(), []byte("<13>1 - - a - - - first"), udpWire("10.0.0.3")); out != Accepted {
		t.Fatalf("control: %v", out)
	}
	w := udpWire("10.0.0.3")
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
