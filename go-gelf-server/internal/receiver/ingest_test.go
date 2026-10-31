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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ring"
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
	h := newHarness(t, 100, "debug", nil)
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
	if hd.Proxied() || hd.TLS != nil {
		t.Fatalf("a UDP head with proxy or TLS: %+v", hd)
	}
	// The item owns its own copy: the transport's buffer is reused.
	if string(items[0].Raw) != line {
		t.Fatalf("raw = %q", items[0].Raw)
	}
	// What the pipeline builds must be what the ring codec accepts, or
	// ring=sqlite would quarantine every row (SPEC §3.4).
	if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
		t.Fatalf("the head codec refuses the pipeline's own head: %v", err)
	}
}

// SPEC §3.1, invariant §20.14: ONE clock sample per message, and both the uuid
// and the event come from it. The uuid's level-3 instant must equal the head's
// clock sample to the nanosecond.
func TestIngestUUIDAndEventShareOneSample(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
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

// SPEC §7: the filter runs on the EFFECTIVE level, read by the converter's
// own rules: absent, invalid or a malformed document is 6 (info).
func TestIngestFilterUsesTheEffectiveLevel(t *testing.T) {
	// min_level=notice (5): the effective 6 is dropped.
	h := newHarness(t, 100, "notice", nil)
	for _, d := range []string{
		`{"version":"1.1","host":"h","short_message":"no level"}`,
		`{"version":"1.1","host":"h","short_message":"bad","level":"x"}`,
		`{"version":"1.1","host":"h","short_message":"out of range","level":9}`,
		`not json at all`,
	} {
		if out := h.in.Accept(context.Background(), []byte(d), udpWire("10.0.0.1")); out != Filtered {
			t.Errorf("%s: outcome = %v, want Filtered under the effective level 6", d, out)
		}
	}
	// An explicit level is its own: 3 (err) passes notice.
	if out := h.in.Accept(context.Background(), []byte(gdoc("x", 3)), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
	// min_level=info keeps the effective 6.
	h = newHarness(t, 100, "info", nil)
	if out := h.in.Accept(context.Background(), []byte(`{"short_message":"no level"}`), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("outcome = %v, want Accepted under min_level=info", out)
	}
}

// The filter runs BEFORE the copy: a document that is not getting in must not
// cost a buffer, which is the normal case on UDP under a flood of debug.
func TestIngestFilterDropsBeforeTheRing(t *testing.T) {
	h := newHarness(t, 100, "warning", nil)
	out := h.in.Accept(context.Background(), []byte(gdoc("debug noise", 7)), udpWire("10.0.0.1"))
	if out != Filtered {
		t.Fatalf("outcome = %v, want Filtered", out)
	}
	h.in.RecordDrop(out)
	if h.ring.Stats().Items != 0 {
		t.Fatal("a filtered document entered the ring")
	}
	if got := h.m.MessagesDroppedTotal[obs.DropFilter].Load(); got != 1 {
		t.Fatalf("filter drops = %d", got)
	}
	if out := h.in.Accept(context.Background(), []byte(gdoc("real problem", 3)), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("err level was dropped: %v", out)
	}
	// filter.levels is an allowlist on top of min_level.
	h = newHarness(t, 100, "debug", []string{"crit", "7"})
	if out := h.in.Accept(context.Background(), []byte(gdoc("x", 2)), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("crit (2) was dropped: %v", out)
	}
	if out := h.in.Accept(context.Background(), []byte(gdoc("x", 7)), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("debug (7) was dropped: %v", out)
	}
	if out := h.in.Accept(context.Background(), []byte(gdoc("x", 3)), udpWire("10.0.0.1")); out != Filtered {
		t.Fatalf("err (3) was kept: %v", out)
	}
}

func TestIngestSinkDownFailsFast(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
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

func TestIngestRingFullIsRetryable(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	if out := h.in.Accept(context.Background(), []byte(doc("um")), tcpWire("10.0.0.1")); out != Accepted {
		t.Fatal(out)
	}
	out := h.in.Accept(context.Background(), []byte(doc("dois")), tcpWire("10.0.0.1"))
	if out != RingFull || !out.Retryable() {
		t.Fatalf("outcome = %v retryable=%v", out, out.Retryable())
	}
}

// SPEC §15.2, error paths. Which Enqueue errors let the receive path pool the
// buffer and which must leave it to the GC: the ring refusing outright means
// nobody below saw the item; ErrUnavailable (ring=redis) and anything else
// (ring=none) may still be read by a client pipeline.
func TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		want   Outcome
		pooled bool
	}{
		{"ring full releases", ring.ErrFull, RingFull, true},
		{"ring closed releases", ring.ErrClosed, Shutdown, true},
		{"ring unavailable discards", ring.ErrUnavailable, RingUnavailable, false},
		{"wrapped unavailable discards", fmt.Errorf("xadd: %w", ring.ErrUnavailable), RingUnavailable, false},
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
			stub := &stubRing{errs: []error{tc.err}}
			in := NewIngest(stub, newUp(), filter.New(rule), nil, newMetrics(), logger(), defaultIngestConfig())

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

// A message held through backpressure is ONE fact. When an attempt's fate is
// unknown (ring=redis unreachable) and the retry lands, both copies must carry
// the same uuid, or the destination cannot deduplicate them (SPEC §3.1: the
// uuid is never regenerated on retry).
func TestIngestRetryReusesTheUUID(t *testing.T) {
	stub := &stubRing{errs: []error{ring.ErrUnavailable, ring.ErrFull, nil}}
	in := NewIngest(stub, newUp(), nil, nil, newMetrics(), logger(), defaultIngestConfig())
	w := tcpWire("10.0.0.7")
	if out := in.Accept(context.Background(), []byte(doc("x")), w); out != RingUnavailable {
		t.Fatal(out)
	}
	if out := in.Retry(context.Background(), []byte(doc("x")), w); out != RingFull {
		t.Fatal(out)
	}
	if out := in.Retry(context.Background(), []byte(doc("x")), w); out != Accepted {
		t.Fatal(out)
	}
	if len(stub.items) != 3 {
		t.Fatalf("%d attempts", len(stub.items))
	}
	for i, it := range stub.items {
		if it.Head.UUID != stub.items[0].Head.UUID || it.Head.UUID.IsZero() {
			t.Fatalf("attempt %d has uuid %v, the first %v", i, it.Head.UUID, stub.items[0].Head.UUID)
		}
	}
}

func TestOutcomeRetryability(t *testing.T) {
	retryable := map[Outcome]bool{
		Accepted: false, Filtered: false, RateLimited: false, Empty: false, Unaddressable: false,
		SinkDown: true, RingFull: true, RingUnavailable: true, Shutdown: false, Consumed: false,
		TooLarge: false, DecompressFailed: false, ChunkInvalid: false, ChunkExpired: false, ChunkOverflow: false,
	}
	for o, want := range retryable {
		if o.Retryable() != want {
			t.Errorf("%v.Retryable() = %v, want %v", o, o.Retryable(), want)
		}
	}
}

// Every outcome but Accepted maps to the reason of SPEC §14.1 with its name.
func TestOutcomeDropReasons(t *testing.T) {
	for o, want := range map[Outcome]string{
		Filtered: "filter", RateLimited: "ratelimit", SinkDown: "sink_down", RingFull: "ring_full",
		RingUnavailable: "ring_unavailable", Shutdown: "shutdown", Empty: "empty",
		Unaddressable: "malformed_frame", TooLarge: "too_large", DecompressFailed: "decompress",
		ChunkInvalid: "chunk_invalid", ChunkExpired: "chunk_timeout", ChunkOverflow: "chunk_overflow",
	} {
		if got := o.dropReason().String(); got != want {
			t.Errorf("%v -> %q, want %q", o, got, want)
		}
	}
}

// SPEC §8: the rate-limit key is the RESOLVED src-ip. Behind one trusted
// balancer, two clients have two buckets; the hostname in the message never
// matters.
func TestIngestRateLimitKeysOnTheResolvedSource(t *testing.T) {
	h := newHarness(t, 1000, "debug", nil)
	h.withLimiter(ratelimit.Params{Enabled: true, Rate: 0.001, Burst: 2, V4Mask: 32, V6Mask: 128, MaxBuckets: 1000})

	behind := func(client string) *Wire {
		w := tcpWire(client)
		w.PeerIP, w.PeerPort = netip.MustParseAddr("192.0.2.1"), 40000 // the balancer
		return w
	}
	var limited int
	for i := 0; i < 10; i++ {
		// Every message claims a different hostname; only the source matters.
		line := fmt.Sprintf(`{"version":"1.1","host":"host-%d","short_message":"x"}`, i)
		if h.in.Accept(context.Background(), []byte(line), behind("10.0.0.1")) == RateLimited {
			limited++
		}
	}
	if limited != 8 {
		t.Fatalf("%d of 10 limited, want 8 past a burst of 2", limited)
	}
	// Another client behind the SAME balancer has its own bucket.
	if out := h.in.Accept(context.Background(), []byte(doc("x")), behind("10.0.0.2")); out != Accepted {
		t.Fatalf("a different client behind the balancer was limited: %v", out)
	}
	h.in.RecordDrop(RateLimited)
	if h.m.MessagesDroppedTotal[obs.DropRateLimit].Load() != 1 {
		t.Fatal("ratelimit not counted")
	}
}

// x-proxy only when a PROXY header replaced the addresses, never on UDP; the
// TLS session only on tls.
func TestIngestPeerAndTLSOnlyWhereTheyBelong(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	sess := &item.TLSInfo{Version: "TLS1.3"}

	w := udpWire("10.0.0.1")
	w.PeerIP, w.PeerPort, w.TLS = netip.MustParseAddr("192.0.2.1"), 9, sess
	h.in.Accept(context.Background(), []byte(doc("udp")), w)

	w = tcpWire("10.0.0.1")
	w.PeerIP, w.PeerPort, w.TLS = netip.MustParseAddr("::ffff:192.0.2.1"), 9, sess
	h.in.Accept(context.Background(), []byte(doc("tcp")), w)

	w = tcpWire("10.0.0.1")
	w.Transport, w.TLS = item.TransportTLS, sess
	h.in.Accept(context.Background(), []byte(doc("tls")), w)

	for _, it := range h.drainRing() {
		hd := it.Head
		switch string(it.Raw) {
		case doc("udp"):
			if hd.Proxied() || hd.TLS != nil {
				t.Errorf("udp: %+v", hd)
			}
		case doc("tcp"):
			if !hd.Proxied() || hd.PeerIP != netip.MustParseAddr("192.0.2.1") || hd.TLS != nil {
				t.Errorf("tcp: peer %v (must be canonical) tls %v", hd.PeerIP, hd.TLS)
			}
		case doc("tls"):
			if hd.Proxied() || hd.TLS != sess {
				t.Errorf("tls: %+v", hd)
			}
		}
		if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
			t.Errorf("%s: %v", it.Raw, err)
		}
	}
}

// Counting is split across Accept, Retry and RecordDrop so a held message is one
// arrival and one drop, not six hundred of each (SPEC §5.7).
func TestRetryDoesNotRecount(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
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
	h := newHarnessCfg(t, 10, "debug", nil, cfg)
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
	h := newHarness(t, 10, "debug", nil)
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
	h := newHarness(t, 10, "debug", nil)
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
	h := newHarness(t, 10, "debug", nil)
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
	h := newHarness(t, 10, "debug", nil)
	var buf strings.Builder
	l := obs.NewLogger(&buf, "json", "info")
	l.SetAccess(true, 1)
	h.in = NewIngest(h.ring, h.sink, h.filt, nil, h.m, l, h.cfg)
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
// typical 200-byte document: filter, uuid, head, pooled copy, enqueue.
func BenchmarkIngestAccept(b *testing.B) {
	rule, _ := filter.Compile("debug", nil)
	in := NewIngest(&releaseRing{}, newUp(), filter.New(rule), nil, newMetrics(), logger(), defaultIngestConfig())
	line := []byte(doc(strings.Repeat("m", 140)))
	w := udpWire("10.0.0.7")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.UUID = uuidv7.UUID{}
		in.Accept(context.Background(), line, w)
	}
}
