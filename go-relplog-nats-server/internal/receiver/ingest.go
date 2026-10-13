package receiver

import (
	"context"
	"errors"
	"net/netip"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ring"
)

// defaultMaxMessageBytes is syslog.max_message_bytes when nothing configured
// it (SPEC §5.3, §16.5).
const defaultMaxMessageBytes = 64 << 10

// SinkState is the slice of the sink's health machine that a receiver needs.
//
// Declared here rather than imported so this package never sees internal/sink.
// One bit is all a transport needs; what it DOES with that bit is the
// transport's business, and that is exactly the point of SPEC §2.3.
type SinkState interface{ Up() bool }

// Outcome is what the receive pipeline did with a message. The RELP session
// turns it into an answer (SPEC §5.4):
//
//	Accepted, Empty                  200 OK
//	Unaddressable                    500 <why>: final, librelp drops it
//	RingFull, SinkDown               held; past the timeout, no answer and
//	                                 the session closes (retransmitted)
//	Shutdown                         no answer (retransmitted)
//
// The parent go-relplog-server has four more: Filtered and RateLimited (no
// filter and no rate limiter here), RingUnavailable (only ring=redis could
// say it) and Consumed, the FINAL refusal by the destination that only
// ring=none -- where the receiver is the writer -- could see. With the one
// memory ring of this server a destination's refusal reaches the drain worker,
// never a transaction (SPEC §5.4).
type Outcome uint8

const (
	Accepted Outcome = iota
	SinkDown
	RingFull
	Shutdown
	// Empty is a message of zero octets: no envelope, reason="empty" (SPEC
	// §5.4).
	Empty
	// Unaddressable is a message without a usable source address. A TCP peer
	// always has one, so on this server it is a guard: the payload schema
	// requires host.src-ip and SPEC §4.2 forbids recording an unspecified
	// one, so there is no envelope it could become (invariant §20.5).
	Unaddressable
)

// Retryable reports whether the refusal is one the emitter should be made to
// wait on -- the transaction held -- rather than told about. A full ring and
// a down sink WILL clear on their own, so holding the emitter is right.
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
		return "unaddressable"
	}
	return "unknown"
}

// dropReason maps a FINAL refusal to its metric label (SPEC §14.1). ok is
// false for the outcomes that are not drops on this server: accepted, and
// everything RELP hands back to the emitter -- a full ring, a DOWN sink, the
// shutdown -- which librelp retransmits (SPEC §5.7).
func (o Outcome) dropReason() (obs.DropReason, bool) {
	switch o {
	case Empty:
		return obs.DropEmpty, true
	case Unaddressable:
		return obs.DropUnaddressable, true
	}
	return 0, false
}

// Wire is everything about how ONE message arrived: what the converter cannot
// recover from the bytes (SPEC §3.4). The transport fills it once per frame
// and passes the same pointer to Accept and to every Retry.
type Wire struct {
	Transport item.Transport
	Framing   item.Framing

	// SrcIP/SrcPort and DstIP/DstPort are the payload's host.src-* and
	// host.dst-*: the transport's two ends, or what a TRUSTED PROXY header
	// declared (SPEC §4.6). Ingest canonicalises them (item.Canonical); a
	// zero DstIP is "unknown" and is omitted.
	SrcIP   netip.Addr
	SrcPort uint16
	DstIP   netip.Addr
	DstPort uint16

	// PeerIP/PeerPort are the real TCP peer, set ONLY when a PROXY header
	// replaced the addresses above; they become payload x-proxy.
	PeerIP   netip.Addr
	PeerPort uint16

	// Size is the ORIGINAL size of the SYSLOG-MSG, before truncation (SPEC
	// §5.3). Zero means len(raw).
	Size int

	// Time is the ONE clock sample of the message (SPEC §3.1), taken by the
	// transport when the last octet of the frame was read, and before any
	// parse. The uuid and the envelope's event both come from it.
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

	// MaxMessageBytes is syslog.max_message_bytes (64 KiB): the longest raw
	// line an item keeps (SPEC §5.3). Zero or negative selects 64 KiB. The
	// RELP framer reads it from here, so the limit has one source.
	MaxMessageBytes int
}

// Ingest is the shared receive pipeline (SPEC §2.1, §2.2, §2.3).
//
// Order is strict, and the first condition that fails ends the message:
//
//  1. empty                   zero octets is not a message (SPEC §5.4)
//  2. truncation              keep max_message_bytes, size stays original
//     source address          a frame without a usable one is Unaddressable
//     (the guard of SPEC §5.4; Accept only, a Retry already passed it)
//  3. sink DOWN, ring full    fail fast, even with an empty ring; full on
//     either ceiling, for the truncated length
//  4. uuid, head, copy into a pooled buffer
//  5. the session's end, then ring.Enqueue, which never blocks
//
// Steps 1 to 3 happen BEFORE the copy and before the uuid, so a message that
// is not getting in never costs a buffer or a random draw -- which matters
// here because a held transaction comes back every 50 ms (SPEC §5.7). Nothing
// here reads the message itself: there is no filter and no rate limiter in
// this server (SPEC §1.2), so the PRI is the drain worker's business, like
// the rest of the parse.
type Ingest struct {
	ring ring.Ring
	full func(n int) bool // the ring's cheap pre-check for an n-byte line, when it has one
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

// MaxMessageBytes is the effective syslog.max_message_bytes.
func (in *Ingest) MaxMessageBytes() int { return in.max }

// Accept runs the pipeline on one message and counts its ARRIVAL.
//
// raw does NOT have to outlive the call: step 4 copies it into a buffer the
// item owns. The RELP transaction holds its own copy of the frame (the frame
// buffer is reused by the next read), and the item gets one more, from the
// pool, sized for the ring. raw may be longer than max_message_bytes; it is
// cut here, before the copy (SPEC §5.3).
//
// It returns only once the transport is free to confirm.
//
// It does NOT count a drop. Counting is split across three calls -- Accept,
// Retry and RecordDrop -- because the RELP session holds a refused message
// and tries it again: a message held through a thirty-second backpressure
// window would otherwise appear as six hundred arrivals, and
// relplog_messages_total would stop meaning anything (SPEC §5.7). The caller
// passes every final outcome to RecordDrop exactly once.
func (in *Ingest) Accept(ctx context.Context, raw []byte, w *Wire) Outcome {
	in.m.MessagesTotal[obsTransport(w.Transport)].Inc()
	if len(raw) == 0 {
		return Empty
	}
	if w.Size < len(raw) {
		w.Size = len(raw)
	}
	in.m.MessageBytes.Observe(int64(w.Size))
	kept := len(raw)
	if kept > in.max {
		kept = in.max
	}
	if w.Size > kept {
		in.m.MessagesTruncatedTotal[obsTransport(w.Transport)].Inc()
	}
	if w.Time.IsZero() {
		// Every transport takes the sample; this only keeps a forgotten one
		// from producing a head with no time (uts < 0).
		w.Time = time.Now()
	}
	if w.Transport.Network() && !item.Canonical(w.SrcIP).IsValid() {
		return Unaddressable
	}
	return in.try(ctx, raw, w)
}

// Retry re-runs the pipeline for a message Accept already counted. It is what
// the RELP backpressure loop calls.
func (in *Ingest) Retry(ctx context.Context, raw []byte, w *Wire) Outcome {
	return in.try(ctx, raw, w)
}

// RecordDrop counts a message that did not get in, exactly once. The transport
// calls it when the outcome is final, not on every attempt.
func (in *Ingest) RecordDrop(o Outcome) {
	// Accepted is not a drop, and what is handed back to the emitter is
	// counted as unanswered by the session: counting it here would report a
	// message lost that was not.
	if r, ok := o.dropReason(); ok {
		in.m.MessagesDroppedTotal[r].Inc()
	}
}

func (in *Ingest) try(ctx context.Context, raw []byte, w *Wire) Outcome {
	// --- 2. truncation -----------------------------------------------------
	if len(raw) > in.max {
		raw = raw[:in.max]
	}

	// --- 3. sink DOWN, ring full --------------------------------------------
	//
	// Fail fast even with an empty ring (SPEC §13.3). Accepting into a ring
	// that is not going to drain only postpones the same loss and spends the
	// ring's capacity on messages that will end up in a dump.
	if in.sink != nil && !in.sink.Up() {
		return SinkDown
	}
	// A full ring is refused before the uuid, the copy and the item: a held
	// transaction comes back here every 50 ms (SPEC §5.7), and the refusal
	// would otherwise cost as much as an acceptance. Full on EITHER ceiling,
	// for the line about to be copied: ring.max_bytes binds as well as
	// ring.capacity_items. The check is racy on purpose -- Enqueue below is
	// still the authority, and a slot freed in between only costs one retry.
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
		UUID:      w.UUID,
		From:      in.cfg.Identity.From,
		Space:     in.cfg.Identity.Space,
		Transport: w.Transport,
		Framing:   w.Framing,
		Size:      w.Size,
	}
	h.SetTime(w.Time)
	if h.Size < len(raw) {
		h.Size = len(raw)
	}
	if w.Transport.Network() {
		h.SrcIP, h.SrcPort = item.Canonical(w.SrcIP), w.SrcPort
		h.DstIP, h.DstPort = item.Canonical(w.DstIP), w.DstPort
		if p := item.Canonical(w.PeerIP); p.IsValid() {
			h.PeerIP, h.PeerPort = p, w.PeerPort
		}
	}

	buf, class, box := item.CopyRawBox(raw)
	it := item.NewBoxed(h, buf, class, box)

	// --- 5. ring -----------------------------------------------------------
	//
	// The end of the session wins (SPEC §5.7): a transaction whose session
	// ended while it was being built -- its peer gone, the shutdown -- is not
	// enqueued, because librelp retransmits what it never got an answer for,
	// and the copy in the ring would be a duplicate.
	if ctx.Err() != nil {
		it.Release()
		return Shutdown
	}
	err := in.ring.Enqueue(ctx, it)
	if err == nil {
		in.m.MessagesAcceptedTotal[obsTransport(w.Transport)].Inc()
		if in.log != nil && in.log.AccessEnabled() {
			// Metadata only: no payload in a log line at INFO or above
			// (invariant §20.10).
			in.log.Info("accepted", "uuid", h.UUID.String(), "transport", h.Transport.String(),
				"src_ip", h.SrcIP.String(), "size", h.Size)
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
	// nobody below kept a view of it (SPEC §15.2). A cancelled context is the
	// session or the process ending: no answer, as go-relplog-server maps it,
	// and nothing worth an ERROR at every shutdown. A deadline, or anything
	// unexplained, is held like a full ring, which clears on its own: never a
	// 500, which librelp would drop for good (SPEC §5.4).
	it.Discard()
	switch {
	case errors.Is(err, context.Canceled):
		return Shutdown
	case errors.Is(err, context.DeadlineExceeded):
		return RingFull
	}
	if in.log != nil {
		if ok, suppressed := in.failLog.Allow(); ok {
			in.log.Error("could not accept a message", "error", err, "suppressed", suppressed)
		}
	}
	return RingFull
}
