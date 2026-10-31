package receiver

import (
	"context"
	"errors"
	"net/netip"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ring"
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
//	tcp   keep reading, or stop reading until the ring drains
//	http  202, or 503 with Retry-After (4xx only for what never passes)
//	udp   nothing at all; the drop is counted and that is the only record
type Outcome uint8

const (
	Accepted Outcome = iota
	SinkDown
	RingFull
	Shutdown
	// Empty is a message of zero octets: no envelope, reason="empty" (SPEC
	// §5.6).
	Empty
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
// wait on rather than told about. A full ring and a down sink WILL clear on
// their own, so holding the emitter is right.
func (o Outcome) Retryable() bool { return o == RingFull || o == SinkDown }

func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "accepted"
	case SinkDown:
		return "sink_down"
	case RingFull:
		return "ring_full"
	case Shutdown:
		return "shutdown"
	case Empty:
		return "empty"
	case Unaddressable:
		return "malformed_frame"
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
	case SinkDown:
		return obs.DropSinkDown
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

	// Time is the ONE clock sample of the message (SPEC §3.1), taken by the
	// transport when the last octet of the frame was read -- or when recvmsg
	// returned the datagram -- and before any parse. The uuid and the
	// envelope's event both come from it.
	Time time.Time

	// UUID is minted by the pipeline from Time the first time the message
	// gets as far as becoming an item, and REUSED by every Retry: a message
	// held through backpressure is one fact, with one uuid. Transports leave
	// it zero.
	UUID uuidv7.UUID
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
}

// Ingest is the shared receive pipeline (SPEC §2.1, §2.2, §2.3).
//
// Order is strict, and the first condition that fails ends the message:
//
//  1. empty                   only JSON whitespace is not a document (SPEC §5.6)
//  2. size                    above max_message_bytes is refused, never cut
//  3. sink DOWN, ring full     fail fast, even with an empty ring; full on
//     either ceiling, for the document's length
//  4. uuid, head, copy into a pooled buffer
//  5. ring.Enqueue            never blocks
//
// Steps 1 to 3 happen BEFORE the copy and before the uuid, so a message that
// is not getting in never costs a buffer or a random draw. Nothing here reads
// the document itself: there is no filter and no rate limiter in this server
// (SPEC §1.2), so the level is the drain worker's business, like the rest of
// the parse.
type Ingest struct {
	ring ring.Ring
	full func(n int) bool // the ring's cheap pre-check for an n-byte document, when it has one
	sink SinkState
	m    *obs.Metrics
	log  *obs.Logger
	cfg  IngestConfig
	max  int

	// failLog rate-limits the "could not accept" line. A refusing destination
	// produces one per message, and a collector must not be able to fill its
	// own log with notices about not collecting (SPEC §14.2).
	failLog *obs.Sampler
}

// NewIngest wires the pipeline.
func NewIngest(r ring.Ring, s SinkState, m *obs.Metrics, log *obs.Logger, cfg IngestConfig) *Ingest {
	limit := cfg.MaxMessageBytes
	if limit <= 0 {
		limit = defaultMaxMessageBytes
	}
	in := &Ingest{ring: r, sink: s, m: m, log: log, cfg: cfg, max: limit,
		failLog: obs.NewSampler(30 * time.Second)}
	if f, ok := r.(interface{ FullFor(n int) bool }); ok {
		in.full = f.FullFor
	}
	return in
}

// MaxMessageBytes is the effective convert.max_message_bytes.
func (in *Ingest) MaxMessageBytes() int { return in.max }

// Accept runs the pipeline on one document and counts its ARRIVAL.
//
// raw is the decompressed document and does NOT have to outlive the call: step
// 4 copies it into a buffer the item owns. Every transport reads into a reused
// buffer, and making each of them own the copy would put the same subtle
// lifetime rule in three places.
//
// It returns only once the transport is free to confirm.
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
// the TCP backpressure loop calls.
func (in *Ingest) Retry(ctx context.Context, raw []byte, w *Wire) Outcome {
	return in.try(ctx, raw, w)
}

// RecordDrop counts a message that did not get in, exactly once. The transport
// calls it when it finally gives up, not on every attempt.
func (in *Ingest) RecordDrop(o Outcome) {
	if o == Accepted {
		return
	}
	in.m.MessagesDroppedTotal[o.dropReason()].Inc()
}

func (in *Ingest) try(ctx context.Context, raw []byte, w *Wire) Outcome {
	// --- 3. sink DOWN ------------------------------------------------------
	//
	// Fail fast even with an empty ring (SPEC §13.3). Accepting into a ring
	// that is not going to drain only postpones the same loss and spends the
	// ring's capacity on messages that will end up in a dump.
	if in.sink != nil && !in.sink.Up() {
		return SinkDown
	}
	// A full ring is refused before the uuid, the copy and the item: on a UDP
	// flood, or a TCP reader retrying every 50 ms, the refusal would otherwise
	// cost as much as an acceptance. Full on EITHER ceiling, for the document
	// about to be copied: ring.max_bytes binds as well as ring.capacity_items.
	// The check is racy on purpose -- Enqueue below is still the authority, and
	// a slot freed in between only costs one retry.
	if in.full != nil && in.full(len(raw)) {
		return RingFull
	}

	// --- 4. the item -------------------------------------------------------
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

	buf, class := item.CopyRaw(raw)
	it := item.New(h, buf, class)

	// --- 5. ring -----------------------------------------------------------
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

	// The memory ring has no other error. Should one appear anyway, the item
	// is finished WITHOUT pooling its buffer -- nothing here can prove that
	// nobody below kept a view of it (SPEC §15.2) -- and the message is held
	// like a full ring, which clears on its own.
	it.Discard()
	if errors.Is(err, context.Canceled) {
		// The receive context ended while a ring waited (SPEC §13.4): the
		// shutdown, never a full ring -- an HTTP request in flight gets its
		// 503 at once instead of a held retry (SPEC §4.5).
		return Shutdown
	}
	if in.log != nil {
		if ok, suppressed := in.failLog.Allow(); ok {
			in.log.Error("could not accept a message", "error", err, "suppressed", suppressed)
		}
	}
	return RingFull
}
