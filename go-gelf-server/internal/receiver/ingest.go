package receiver

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ring"
)

// defaultMaxMessageBytes is convert.max_message_bytes when nothing configured
// it (SPEC §5.5, §16.5).
const defaultMaxMessageBytes = convert.DefaultMaxMessageBytes

// SinkState is the slice of the sink's health machine that a receiver needs.
//
// Declared here rather than imported so this package never sees internal/sink.
// One bit is all a transport needs; what it DOES with that bit is the
// transport's business, and that is exactly the point of SPEC §2.3.
type SinkState interface{ Up() bool }

// Outcome is what the receive pipeline did with a message.
//
// The transport turns it into its own form of backpressure, which is the whole
// asymmetry of SPEC §2.3:
//
//	tcp, tls     keep reading, or stop reading until the ring drains
//	http, https  202, or 503 with Retry-After (4xx only for what never passes)
//	udp          nothing at all; the drop is counted and that is the only record
type Outcome uint8

const (
	Accepted Outcome = iota
	Filtered
	RateLimited
	SinkDown
	RingFull
	RingUnavailable
	Shutdown
	// Empty is a message of zero octets: no envelope, reason="empty" (SPEC
	// §5.6).
	Empty
	// Consumed is a message the destination refused FINALLY (sink ClassLogic
	// under ring=none, SPEC §10.2): the sink already counted it as
	// events_dropped{reason="sink_logic"}, so it is neither retried nor
	// counted again here, and a stream transport moves on to the next frame.
	Consumed
	// TooLarge is a document above convert.max_message_bytes, decompressed or
	// not (SPEC §5.5): refused whole, never cut.
	TooLarge
	// Decompress is a gzip/zlib/deflate container that does not decode
	// (SPEC §5.4).
	DecompressFailed
	// ChunkInvalid, ChunkExpired and ChunkOverflow are the UDP reassembly
	// drops of SPEC §5.2.
	ChunkInvalid
	ChunkExpired
	ChunkOverflow
	// Unaddressable is a network message without a usable source address --
	// a datagram from 0.0.0.0. The payload schema requires host.src-ip for
	// network transports and SPEC §4.2 forbids recording an unspecified one,
	// so there is no envelope it could become (invariant §20.5). Counted as
	// reason="malformed_frame": the frame that carried it cannot exist.
	Unaddressable
)

// Retryable reports whether the refusal is one the emitter should be made to
// wait on rather than told about.
//
// A full ring, an unreachable ring and a down sink WILL clear on their own, so
// holding the emitter is right. A rate-limited or filtered message will not:
// holding the window would only move the queue to the other side without
// resolving anything, and a filtered message is one the operator asked to be
// thrown away.
func (o Outcome) Retryable() bool { return o == RingFull || o == RingUnavailable || o == SinkDown }

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case Filtered:
		return "filter"
	case RateLimited:
		return "ratelimit"
	case SinkDown:
		return "sink_down"
	case RingFull:
		return "ring_full"
	case RingUnavailable:
		return "ring_unavailable"
	case Shutdown:
		return "shutdown"
	case Empty:
		return "empty"
	case Unaddressable:
		return "malformed_frame"
	case Consumed:
		return "consumed"
	case TooLarge:
		return "too_large"
	case DecompressFailed:
		return "decompress"
	case ChunkInvalid:
		return "chunk_invalid"
	case ChunkExpired:
		return "chunk_timeout"
	case ChunkOverflow:
		return "chunk_overflow"
	}
	return "unknown"
}

// dropReason maps an outcome to its metric label (SPEC §14.1).
func (o Outcome) dropReason() obs.DropReason {
	switch o {
	case Filtered:
		return obs.DropFilter
	case RateLimited:
		return obs.DropRateLimit
	case SinkDown:
		return obs.DropSinkDown
	case RingUnavailable:
		return obs.DropRingUnavailable
	case Shutdown:
		return obs.DropShutdown
	case Empty:
		return obs.DropEmpty
	case Unaddressable:
		return obs.DropMalformedFrame
	case TooLarge:
		return obs.DropTooLarge
	case DecompressFailed:
		return obs.DropDecompress
	case ChunkInvalid:
		return obs.DropChunkInvalid
	case ChunkExpired:
		return obs.DropChunkTimeout
	case ChunkOverflow:
		return obs.DropChunkOverflow
	}
	return obs.DropRingFull
}

// Wire is everything about how ONE message arrived: what the converter cannot
// recover from the bytes (SPEC §3.4). The transport fills it once per frame or
// datagram and passes the same pointer to Accept and to every Retry.
type Wire struct {
	Transport item.Transport

	// Compression and Chunks describe how the document arrived (SPEC §3.4);
	// the bytes handed to Accept are always the decompressed document.
	Compression item.Compression
	Chunks      int

	// SrcIP/SrcPort and DstIP/DstPort are the payload's host.src-* and
	// host.dst-*: the transport's two ends, or what a TRUSTED PROXY header
	// declared (SPEC §4.6). Ingest canonicalises them (item.Canonical); a
	// zero DstIP is "unknown" and is omitted. Ignored on unix.
	SrcIP   netip.Addr
	SrcPort uint16
	DstIP   netip.Addr
	DstPort uint16

	// PeerIP/PeerPort are the real TCP peer, set ONLY when a PROXY header
	// replaced the addresses above; they become payload x-proxy.
	PeerIP   netip.Addr
	PeerPort uint16

	// TLS is the session of a tls or https connection, built once after the
	// handshake and shared, read-only, by every message of the connection.
	TLS *item.TLSInfo

	// Time is the ONE clock sample of the message (SPEC §3.1), taken by the
	// transport when the last octet of the frame was read -- or when recvmsg
	// returned the datagram -- and before any parse. The uuid and the
	// envelope's event both come from it.
	Time time.Time

	// UUID is minted by the pipeline from Time the first time the message
	// gets as far as becoming an item, and REUSED by every Retry. A message
	// held through backpressure is one fact; if one attempt's fate is unknown
	// (ring=redis unreachable, a ring=none write that timed out) and the next
	// succeeds, the two copies carry the same uuid and the destination
	// deduplicates them by (uuid, step). A uuid per attempt would turn that
	// into two facts. Transports leave it zero.
	UUID uuidv7.UUID

	// admitted records that the filter and the rate limiter already let this
	// message through, so a Retry does not ask them again (see try). A fresh
	// Wire per frame or datagram resets it.
	admitted bool
}

// IngestConfig is what the pipeline needs from the configuration.
type IngestConfig struct {
	// Identity supplies the head's From (AgentUUID) and Space (Workspace):
	// who RECEIVED the message (SPEC §3.1). It must be valid; the app
	// validated it at boot (SPEC §13.2).
	Identity ident.Identity

	// MaxMessageBytes is convert.max_message_bytes (128 KiB): the longest
	// decompressed document accepted (SPEC §5.5). Zero or negative selects
	// the default. The framer, the reassembler, the decompressor and the HTTP
	// handler all read it from here, so the limit has one source.
	MaxMessageBytes int

	// Final reports whether an Enqueue error is a FINAL refusal of the
	// message by the destination -- with ring=none, where the receiver is the
	// writer, a sink ClassLogic error (SPEC §9.6, §10.2). Such a message is
	// consumed: the sink already logged its uuid and counted it in
	// events_dropped{reason="sink_logic"}, retrying cannot help, and every
	// retry would count it again. The pipeline returns Consumed, which no
	// transport retries and RecordDrop does not count.
	//
	// It is a hook because this package never sees internal/sink. The app
	// wires:
	//
	//	Final: func(err error) bool { return sink.ClassOf(err) == sink.ClassLogic }
	//
	// nil treats every such error as retryable, as before.
	Final func(error) bool
}

// Ingest is the shared receive pipeline (SPEC §2.1, §2.2, §2.3).
//
// Order is strict, and the first condition that fails ends the message:
//
//  1. empty                   only JSON whitespace is not a document (SPEC §5.6)
//  2. size                    above max_message_bytes is refused, never cut
//  3. level                   only when the filter can refuse (SPEC §7)
//  4. filter                  the EFFECTIVE level, the converter's own
//  5. rate-limit              per network prefix of the resolved src-ip
//  6. sink DOWN               fail fast, even with an empty ring
//  7. uuid, head, copy into a pooled buffer
//  8. ring.Enqueue            blocks for the mode's guarantee
//
// Steps 3 to 5 run once per message, never again on a Retry. Steps 1 to 6
// happen BEFORE the copy and before the uuid, so a message that is
// not getting in never costs a buffer or a random draw. That matters most on
// UDP, where a flood of filtered debug is the normal case.
type Ingest struct {
	ring    ring.Ring
	sink    SinkState
	filter  *filter.Filter
	limiter *ratelimit.Limiter
	m       *obs.Metrics
	log     *obs.Logger
	cfg     IngestConfig
	max     int

	// levels holds converters for the level scan of step 3. A Converter is
	// not safe for concurrent use and every transport goroutine may scan.
	levels sync.Pool

	// failLog rate-limits the "could not accept" line. A refusing destination
	// produces one per message, and a collector must not be able to fill its
	// own log with notices about not collecting (SPEC §14.2).
	failLog *obs.Sampler
}

// NewIngest wires the pipeline. f and l may be nil: no filter accepts
// everything, no limiter limits nothing. The app normally passes both and
// reloads them in place on SIGHUP (filter.Replace, ratelimit.SetParams).
func NewIngest(r ring.Ring, s SinkState, f *filter.Filter, l *ratelimit.Limiter,
	m *obs.Metrics, log *obs.Logger, cfg IngestConfig) *Ingest {
	limit := cfg.MaxMessageBytes
	if limit <= 0 {
		limit = defaultMaxMessageBytes
	}
	return &Ingest{
		ring: r, sink: s, filter: f, limiter: l, m: m, log: log, cfg: cfg, max: limit,
		failLog: obs.NewSampler(30 * time.Second),
	}
}

// MaxMessageBytes is the effective convert.max_message_bytes.
func (in *Ingest) MaxMessageBytes() int { return in.max }

// Accept runs the pipeline on one document and counts its ARRIVAL.
//
// raw is the decompressed document and does NOT have to outlive the call: step
// 7 copies it into a buffer the item owns. Every transport reads into a reused
// buffer, and making each of them own the copy would put the same subtle
// lifetime rule in five places.
//
// It blocks for as long as the ring's durability guarantee takes, and returns
// only once the transport is free to confirm.
//
// It does NOT count a drop. Counting is split across three calls -- Accept,
// Retry and RecordDrop -- because the stream transports hold a refused message
// and try it again: a message held through a thirty-second backpressure window
// would otherwise appear as six hundred arrivals and six hundred drops (SPEC
// §5.7). The caller passes every outcome to RecordDrop exactly once.
func (in *Ingest) Accept(ctx context.Context, raw []byte, w *Wire) Outcome {
	in.m.MessagesTotal[obsTransport(w.Transport)].Inc()
	if blank(raw) {
		return Empty
	}
	in.m.MessageBytes.Observe(int64(len(raw)))
	if len(raw) > in.max {
		return TooLarge
	}
	if w.Time.IsZero() {
		// Every transport takes the sample; this only keeps a forgotten one
		// from producing a head the codec refuses (uts < 0).
		w.Time = time.Now()
	}
	if !item.Canonical(w.SrcIP).IsValid() {
		return Unaddressable
	}
	return in.try(ctx, raw, w)
}

// Reject counts the arrival and the drop of a document the transport refused
// before it could reach Accept: too large on the wire, a container that does
// not decompress, a UDP reassembly that failed (SPEC §5.2, §5.4, §5.5).
func (in *Ingest) Reject(t item.Transport, o Outcome) {
	in.m.MessagesTotal[obsTransport(t)].Inc()
	in.RecordDrop(o)
}

// blank reports whether b holds only JSON whitespace (SPEC §5.6).
func blank(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return false
		}
	}
	return true
}

// Retry re-runs the pipeline for a message Accept already counted. It is what
// the TCP and TLS backpressure loop calls.
func (in *Ingest) Retry(ctx context.Context, raw []byte, w *Wire) Outcome {
	return in.try(ctx, raw, w)
}

// RecordDrop counts a message that did not get in, exactly once. The transport
// calls it when it finally gives up, not on every attempt.
func (in *Ingest) RecordDrop(o Outcome) {
	// Accepted is not a drop, and Consumed was counted by the sink as an
	// event it lost (sink_logic): counting it here as well, under another
	// reason, would report one message lost twice.
	if o == Accepted || o == Consumed {
		return
	}
	in.m.MessagesDroppedTotal[o.dropReason()].Inc()
}

func (in *Ingest) try(ctx context.Context, raw []byte, w *Wire) Outcome {
	// Steps 3 to 5 decide ONCE per message. A message held by backpressure
	// comes back here every 50 ms (SPEC §5.7); charging it a rate-limit token
	// on every retry would drain its prefix's bucket while it waits, and the
	// message -- admitted when it arrived -- would end up dropped as
	// "ratelimit" the moment the ring had room again. Re-filtering it would
	// likewise let a SIGHUP in the middle of a hold judge one message twice.
	if !w.admitted {
		// --- 3 and 4. level and filter ---------------------------------------
		//
		// The level is read with the converter's own function (SPEC §7), so
		// a document is never filtered under one level and converted under
		// another. Only when the filter can refuse something: without that,
		// validating every document here would be CPU on the hot path that
		// changes nothing.
		if in.filter != nil && in.filter.Active() {
			c, _ := in.levels.Get().(*convert.Converter)
			if c == nil {
				c = convert.New(convert.DefaultConfig())
			}
			level := c.LevelOf(raw)
			in.levels.Put(c)
			if !in.filter.AllowLevel(level) {
				return Filtered
			}
		}

		// --- 5. rate-limit ---------------------------------------------------
		//
		// The key is the RESOLVED src-ip: the transport peer, or the client a
		// trusted PROXY header declared (SPEC §8). The hostname inside the
		// document is an emitter's claim and would poison every other bucket.
		// X-Forwarded-For is never read (SPEC §4.5).
		if in.limiter != nil {
			if ok, _ := in.limiter.Allow(item.Canonical(w.SrcIP)); !ok {
				return RateLimited
			}
		}
		w.admitted = true
	}

	// --- 6. sink DOWN ------------------------------------------------------
	//
	// Fail fast even with an empty ring (SPEC §13.3). Accepting into a ring
	// that is not going to drain only postpones the same loss and spends the
	// ring's capacity on messages that will end up in a dump.
	if in.sink != nil && !in.sink.Up() {
		return SinkDown
	}

	// --- 7. the item -------------------------------------------------------
	if w.UUID.IsZero() {
		// The SAME sample as event (SPEC §3.1, invariant §20.14): one reading
		// of the clock, two views of it.
		w.UUID = jlh.NewUUID(w.Time)
	}
	h := item.Head{
		UUID:        w.UUID,
		From:        in.cfg.Identity.From,
		Space:       in.cfg.Identity.Space,
		Transport:   w.Transport,
		Compression: w.Compression,
		Chunks:      w.Chunks,
	}
	h.SetTime(w.Time)
	h.SrcIP, h.SrcPort = item.Canonical(w.SrcIP), w.SrcPort
	h.DstIP, h.DstPort = item.Canonical(w.DstIP), w.DstPort
	if w.Transport != item.TransportUDP {
		if p := item.Canonical(w.PeerIP); p.IsValid() {
			h.PeerIP, h.PeerPort = p, w.PeerPort
		}
	}
	if w.Transport.Secure() && !w.TLS.Empty() {
		h.TLS = w.TLS
	}

	buf, class := item.CopyRaw(raw)
	it := item.New(h, buf, class)

	// --- 8. ring -----------------------------------------------------------
	err := in.ring.Enqueue(ctx, it)
	if err == nil {
		in.m.MessagesAcceptedTotal[obsTransport(w.Transport)].Inc()
		if in.log != nil && in.log.AccessEnabled() {
			// Metadata only: no payload in a log line at INFO or above
			// (invariant §20.10).
			in.log.Info("accepted", "uuid", h.UUID.String(), "transport", h.Transport.String(),
				"src_ip", h.SrcIP.String(), "size", len(raw))
		}
		return Accepted
	}

	switch {
	case errors.Is(err, ring.ErrFull):
		// Refused before the hand-off: this goroutine still owns the buffer
		// and nothing below has seen it (SPEC §15.2).
		it.Release()
		return RingFull
	case errors.Is(err, ring.ErrClosed):
		it.Release()
		return Shutdown
	}

	// Everything else may still be read below. ring=redis unreachable leaves
	// the command in rueidis's pipeline, which may yet send it -- a view of
	// this very buffer -- and ring=none hands the item straight to a sink
	// whose write may have timed out with its command still queued. This
	// package cannot see the sink's error classes, by design, so it cannot
	// tell a consumed command from a queued one, and the safe answer for all
	// of them is the same: finish the item WITHOUT pooling it (SPEC §15.2).
	it.Discard()

	if in.cfg.Final != nil && in.cfg.Final(err) {
		// Refused for good, and already counted by the sink. The buffer is
		// still discarded rather than pooled: a refusal can carry a taint
		// this package cannot see (an async publish with other futures
		// pending), and one GC'd buffer on a rare path is the cheap side.
		return Consumed
	}

	switch {
	case errors.Is(err, ring.ErrUnavailable):
		// Retryable like a full ring: UDP drops with ring_unavailable, TCP
		// stops reading. The uuid in w is kept, so if the first attempt did
		// land, the retry is the same fact.
		return RingUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		// The destination did not answer in time. Retryable, like a full
		// ring: the transport holds the emitter and tries again.
		return RingFull
	case errors.Is(err, context.Canceled):
		return Shutdown
	}

	// The destination itself refused or failed. It is already counted and
	// classified by the sink's own wrapper; here it is one more message that
	// did not get in, and it is retryable for the same reason a down sink is:
	// the destination may well come back.
	if in.log != nil {
		if ok, suppressed := in.failLog.Allow(); ok {
			in.log.Error("could not accept a message", "error", err, "suppressed", suppressed)
		}
	}
	return SinkDown
}
