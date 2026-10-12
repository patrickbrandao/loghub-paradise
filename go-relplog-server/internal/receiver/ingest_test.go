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

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

func udpWire(src string) *Wire {
	return &Wire{
		Transport: item.TransportUDP,
		Framing:   item.FramingDatagram,
		SrcIP:     netip.MustParseAddr(src),
		SrcPort:   4242,
		DstIP:     netip.MustParseAddr("10.0.0.10"),
		DstPort:   1514,
		Time:      time.Now(),
	}
}

func tcpWire(src string) *Wire {
	w := udpWire(src)
	w.Transport, w.Framing = item.TransportTCP, item.FramingNonTransparent
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
	if hd.Transport != item.TransportUDP || hd.Framing != item.FramingDatagram {
		t.Fatalf("wire = %v/%v", hd.Transport, hd.Framing)
	}
	if hd.SrcIP != netip.MustParseAddr("10.0.0.7") || hd.SrcPort != 4242 ||
		hd.DstIP != netip.MustParseAddr("10.0.0.10") || hd.DstPort != 1514 {
		t.Fatalf("addresses = %+v", hd)
	}
	if hd.Proxied() || hd.TLS != nil {
		t.Fatalf("a UDP head with proxy or TLS: %+v", hd)
	}
	if hd.Size != len(line) {
		t.Fatalf("size = %d", hd.Size)
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

// SPEC §7: the filter runs on the EFFECTIVE priority. A message with no PRI is
// filtered as syslog.default_priority, the value its payload will carry.
func TestIngestFilterUsesTheEffectivePRI(t *testing.T) {
	// default 13 = user.notice (5): dropped by min_severity=warning.
	h := newHarness(t, 100, "warning", nil)
	for _, line := range []string{"linha solta", "<192>pri invalido", "<013>zero a esquerda"} {
		if out := h.in.Accept(context.Background(), []byte(line), udpWire("10.0.0.1")); out != Filtered {
			t.Errorf("%q: outcome = %v, want Filtered under the default PRI 13", line, out)
		}
	}
	// default 11 = user.err (3): kept.
	cfg := defaultIngestConfig()
	cfg.DefaultPriority = 11
	h = newHarnessCfg(t, 100, "warning", nil, cfg)
	if out := h.in.Accept(context.Background(), []byte("linha solta"), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("outcome = %v, want Accepted under the default PRI 11", out)
	}
	// And a valid PRI is its own: <11> is user.err.
	if out := h.in.Accept(context.Background(), []byte("<11>x"), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("outcome = %v", out)
	}
}

// The filter runs BEFORE the copy: a message that is not getting in must not
// cost a buffer, which is the normal case on UDP under a flood of debug.
func TestIngestFilterDropsBeforeTheRing(t *testing.T) {
	h := newHarness(t, 100, "warning", nil)
	// severity 7 (debug) on facility 20
	out := h.in.Accept(context.Background(), []byte("<167>debug noise"), udpWire("10.0.0.1"))
	if out != Filtered {
		t.Fatalf("outcome = %v, want Filtered", out)
	}
	h.in.RecordDrop(out)
	if h.ring.Stats().Items != 0 {
		t.Fatal("a filtered message entered the ring")
	}
	if got := h.m.MessagesDroppedTotal[obs.DropFilter].Load(); got != 1 {
		t.Fatalf("filter drops = %d", got)
	}
	// severity 3 (err) passes
	if out := h.in.Accept(context.Background(), []byte("<163>real problem"), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("err severity was dropped: %v", out)
	}
	// and a facility allowlist uses the RFC 5427 names: 14 is console.
	h = newHarness(t, 100, "debug", []string{"console"})
	if out := h.in.Accept(context.Background(), []byte("<112>x"), udpWire("10.0.0.1")); out != Accepted {
		t.Fatalf("console (14) was dropped: %v", out)
	}
	if out := h.in.Accept(context.Background(), []byte("<120>x"), udpWire("10.0.0.1")); out != Filtered {
		t.Fatalf("cron2 (15) was kept: %v", out)
	}
}

func TestIngestSinkDownFailsFast(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	h.sink.up.Store(false)
	out := h.in.Accept(context.Background(), []byte("<13>oi"), udpWire("10.0.0.1"))
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

func TestIngestRingFullIsRetryable(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	if out := h.in.Accept(context.Background(), []byte("<13>um"), tcpWire("10.0.0.1")); out != Accepted {
		t.Fatal(out)
	}
	out := h.in.Accept(context.Background(), []byte("<13>dois"), tcpWire("10.0.0.1"))
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

// A message held through backpressure is ONE fact. When an attempt's fate is
// unknown (ring=redis unreachable) and the retry lands, both copies must carry
// the same uuid, or the destination cannot deduplicate them (SPEC §3.1: the
// uuid is never regenerated on retry).
func TestIngestRetryReusesTheUUID(t *testing.T) {
	stub := &stubRing{errs: []error{ring.ErrUnavailable, ring.ErrFull, nil}}
	in := NewIngest(stub, newUp(), nil, nil, newMetrics(), logger(), defaultIngestConfig())
	w := tcpWire("10.0.0.7")
	if out := in.Accept(context.Background(), []byte("<13>x"), w); out != RingUnavailable {
		t.Fatal(out)
	}
	if out := in.Retry(context.Background(), []byte("<13>x"), w); out != RingFull {
		t.Fatal(out)
	}
	if out := in.Retry(context.Background(), []byte("<13>x"), w); out != Accepted {
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
		Accepted: false, Filtered: false, RateLimited: true, Empty: false, Unaddressable: false,
		SinkDown: true, RingFull: true, RingUnavailable: true, Shutdown: false,
	}
	for o, want := range retryable {
		if o.Retryable() != want {
			t.Errorf("%v.Retryable() = %v, want %v", o, o.Retryable(), want)
		}
	}
}

// The final refusals map to the reasons of SPEC §14.1; what RELP hands back to
// the emitter (held, shutdown) and what the sink counted (consumed) is no drop.
func TestOutcomeDropReasons(t *testing.T) {
	for o, want := range map[Outcome]string{
		Filtered: "filter", Empty: "empty", Unaddressable: "unaddressable",
	} {
		r, ok := o.dropReason()
		if !ok || r.String() != want {
			t.Errorf("%v -> %q %v, want %q", o, r, ok, want)
		}
	}
	for _, o := range []Outcome{Accepted, Consumed, SinkDown, RingFull, RingUnavailable, Shutdown, RateLimited} {
		if _, ok := o.dropReason(); ok {
			t.Errorf("%v is counted as a drop", o)
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
		line := fmt.Sprintf("<13>Sep 12 10:00:00 host-%d app: x", i)
		if h.in.Accept(context.Background(), []byte(line), behind("10.0.0.1")) == RateLimited {
			limited++
		}
	}
	if limited != 8 {
		t.Fatalf("%d of 10 limited, want 8 past a burst of 2", limited)
	}
	// Another client behind the SAME balancer has its own bucket.
	if out := h.in.Accept(context.Background(), []byte("<13>x"), behind("10.0.0.2")); out != Accepted {
		t.Fatalf("a different client behind the balancer was limited: %v", out)
	}
	// Held, not dropped, on this server (SPEC §8): RecordDrop counts nothing.
	h.in.RecordDrop(RateLimited)
	for r := range h.m.MessagesDroppedTotal {
		if h.m.MessagesDroppedTotal[r].Load() != 0 {
			t.Fatalf("a rate-limited message counted as %s", obs.DropReason(r))
		}
	}
}

// The unix transport has no network peer, so it is exempt by construction:
// rate-limiting /dev/log would silence the host itself (SPEC §4.3).
func TestIngestUnixIsExemptFromRateLimit(t *testing.T) {
	h := newHarness(t, 1000, "debug", nil)
	h.withLimiter(ratelimit.Params{Enabled: true, Rate: 0.001, Burst: 1, V4Mask: 32, V6Mask: 128, MaxBuckets: 10})
	for i := 0; i < 20; i++ {
		w := &Wire{Transport: item.TransportUnix, Framing: item.FramingDatagram, Time: time.Now()}
		if out := h.in.Accept(context.Background(), []byte("<13>x"), w); out != Accepted {
			t.Fatalf("message %d on unix was refused: %v", i, out)
		}
	}
}

// The unix head carries no address at all, whatever the transport filled in:
// the payload schema forbids every network field there.
func TestIngestUnixHasNoAddresses(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	w := &Wire{Transport: item.TransportUnix, Framing: item.FramingDatagram, Time: time.Now(),
		SrcIP: netip.MustParseAddr("10.0.0.1"), SrcPort: 1, DstPort: 2,
		PeerIP: netip.MustParseAddr("10.0.0.2"), PeerPort: 3}
	if out := h.in.Accept(context.Background(), []byte("<13>x"), w); out != Accepted {
		t.Fatal(out)
	}
	hd := h.drainRing()[0].Head
	if hd.SrcIP.IsValid() || hd.SrcPort != 0 || hd.DstPort != 0 || hd.Proxied() {
		t.Fatalf("unix head carries addresses: %+v", hd)
	}
	if _, err := item.DecodeHead(item.EncodeHead(nil, &hd)); err != nil {
		t.Fatal(err)
	}
}

// x-proxy only when a PROXY header replaced the addresses, never on UDP; the
// TLS session only on tls.
func TestIngestPeerAndTLSOnlyWhereTheyBelong(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	sess := &item.TLSInfo{Version: "TLS1.3"}

	w := udpWire("10.0.0.1")
	w.PeerIP, w.PeerPort, w.TLS = netip.MustParseAddr("192.0.2.1"), 9, sess
	h.in.Accept(context.Background(), []byte("<13>udp"), w)

	w = tcpWire("10.0.0.1")
	w.PeerIP, w.PeerPort, w.TLS = netip.MustParseAddr("::ffff:192.0.2.1"), 9, sess
	h.in.Accept(context.Background(), []byte("<13>tcp"), w)

	w = tcpWire("10.0.0.1")
	w.Transport, w.Framing, w.TLS = item.TransportTLS, item.FramingOctetCounting, sess
	h.in.Accept(context.Background(), []byte("<13>tls"), w)

	for _, it := range h.drainRing() {
		hd := it.Head
		switch string(it.Raw) {
		case "<13>udp":
			if hd.Proxied() || hd.TLS != nil {
				t.Errorf("udp: %+v", hd)
			}
		case "<13>tcp":
			if !hd.Proxied() || hd.PeerIP != netip.MustParseAddr("192.0.2.1") || hd.TLS != nil {
				t.Errorf("tcp: peer %v (must be canonical) tls %v", hd.PeerIP, hd.TLS)
			}
		case "<13>tls":
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
// arrival, not six hundred (SPEC §5.7), and one RecordDrop is one drop.
func TestRetryDoesNotRecount(t *testing.T) {
	h := newHarness(t, 1, "debug", nil)
	h.in.Accept(context.Background(), []byte("<13>um"), tcpWire("10.0.0.1")) // fills the ring

	w := tcpWire("10.0.0.1")
	h.in.Accept(context.Background(), []byte("<13>dois"), w)
	for i := 0; i < 5; i++ {
		h.in.Retry(context.Background(), []byte("<13>dois"), w)
	}
	h.in.RecordDrop(Filtered)

	if got := h.m.MessagesTotal[obs.TransportTCP].Load(); got != 2 {
		t.Fatalf("arrivals = %d, want 2", got)
	}
	if got := h.m.MessageBytes.Count(); got != 2 {
		t.Fatalf("message_bytes observed %d times, want 2", got)
	}
	if got := h.m.MessagesDroppedTotal[obs.DropFilter].Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
}

// SPEC §5.3: truncation happens BEFORE the copy, size stays original, and the
// truncation is counted once, per transport.
func TestIngestTruncatesBeforeTheCopy(t *testing.T) {
	cfg := defaultIngestConfig()
	cfg.MaxMessageBytes = 480
	h := newHarnessCfg(t, 10, "debug", nil, cfg)
	line := "<13>" + strings.Repeat("z", 996)
	w := udpWire("10.0.0.1")
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
	if h.m.MessagesTruncatedTotal[obs.TransportUDP].Load() != 1 {
		t.Fatal("truncation not counted")
	}
	// A frame the framer already cut arrives with its original size in Wire.
	w = tcpWire("10.0.0.1")
	w.Size = 5000
	h.in.Accept(context.Background(), []byte(line[:480]), w)
	if it := h.drainRing()[0]; it.Head.Size != 5000 || len(it.Raw) != 480 {
		t.Fatalf("size %d raw %d", it.Head.Size, len(it.Raw))
	}
	if h.m.MessagesTruncatedTotal[obs.TransportTCP].Load() != 1 {
		t.Fatal("tcp truncation not counted")
	}
	if h.m.MessagesTruncatedTotal[obs.TransportUDP].Load() != 1 {
		t.Fatal("a whole message was counted as truncated")
	}
}

// SPEC §5.4: zero octets is no envelope, counted as reason="empty", never
// copied into the ring.
func TestIngestEmptyIsCounted(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	out := h.in.Accept(context.Background(), nil, udpWire("10.0.0.1"))
	if out != Empty || out.Retryable() {
		t.Fatalf("outcome = %v", out)
	}
	h.in.RecordDrop(out)
	if h.m.MessagesDroppedTotal[obs.DropEmpty].Load() != 1 || h.ring.Stats().Items != 0 {
		t.Fatal("an empty message was not dropped as empty")
	}
}

// A datagram from 0.0.0.0 has no source the payload could record (SPEC §4.2),
// and an envelope without host.src-ip on udp fails the schema.
func TestIngestUnaddressableNetworkMessage(t *testing.T) {
	h := newHarness(t, 10, "debug", nil)
	for _, src := range []string{"0.0.0.0", "::"} {
		out := h.in.Accept(context.Background(), []byte("<13>x"), udpWire(src))
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
	h := newHarness(t, 10, "debug", nil)
	var buf strings.Builder
	l := obs.NewLogger(&buf, "json", "info")
	l.SetAccess(true, 1)
	h.in = NewIngest(h.ring, h.sink, h.filt, nil, h.m, l, h.cfg)
	if out := h.in.Accept(context.Background(), []byte("<13>secret"), udpWire("10.0.0.1")); out != Accepted {
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
// typical 200-byte line: PRI, filter, uuid, head, pooled copy, enqueue.
func BenchmarkIngestAccept(b *testing.B) {
	rule, _ := filter.Compile("debug", nil)
	in := NewIngest(&releaseRing{}, newUp(), filter.New(rule), nil, newMetrics(), logger(), defaultIngestConfig())
	line := []byte("<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 - " + strings.Repeat("m", 140))
	w := udpWire("10.0.0.7")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.UUID = uuidv7.UUID{}
		in.Accept(context.Background(), line, w)
	}
}
