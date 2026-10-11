package receiver

import (
	"context"
	"errors"
	"net/netip"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// SinkState is the slice of the sink's health machine that a receiver needs.
//
// Declared here rather than imported so this package never sees internal/sink.
// One bit is all a transport needs; what it DOES with that bit is the transport's
// business, and that is exactly the point of SPEC §2.3.
type SinkState interface{ Up() bool }

// Outcome is what the receive pipeline did with a message.
//
// The transport turns it into its own form of backpressure, which is the whole
// asymmetry of this server against its HTTP sibling (SPEC §2.3):
//
//	relp  200 OK, or 500 with the reason
//	tcp   keep reading, or stop reading until the ring drains
//	unix  nothing; the kernel already blocked the sender
//	udp   nothing at all; the drop is counted and that is the only record
type Outcome uint8

const (
	Accepted Outcome = iota
	Filtered
	RateLimited
	SinkDown
	RingFull
	Shutdown
	Failed
)

// Retryable reports whether the refusal is one the emitter should be made to
// wait on rather than told about.
//
// A full ring and a down sink WILL clear on their own, so holding the emitter is
// right. A rate-limited or filtered message will not: holding the window would
// only move the queue to the other side without resolving anything, and a
// filtered message is one the operator asked to be thrown away.
func (o Outcome) Retryable() bool { return o == RingFull || o == SinkDown }

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
	case Shutdown:
		return "shutdown"
	}
	return "failed"
}

// dropReason maps an outcome to its metric label.
func (o Outcome) dropReason() obs.DropReason {
	switch o {
	case Filtered:
		return obs.DropFilter
	case RateLimited:
		return obs.DropRateLimit
	case SinkDown:
		return obs.DropSinkDown
	case Shutdown:
		return obs.DropShutdown
	}
	return obs.DropRingFull
}

// Wire is everything about how a message arrived. It is what the parser cannot
// recover from the bytes (SPEC §3.2).
type Wire struct {
	Transport envelope.Transport
	Framing   envelope.Framing
	Listener  string
	PeerAddr  netip.Addr
	PeerPort  uint16
	Truncated bool
}

// IngestConfig holds the defaults the pipeline applies when no PRI is present.
type IngestConfig struct {
	DefaultFacility int
	DefaultSeverity int
}

// Ingest is the shared receive pipeline, the counterpart of the sibling
// service's request pipeline (SPEC §4.3 there, §2.2 and §2.3 here).
//
// Order is strict, and the first condition that fails ends the message:
//
//  1. PRI                     five bytes; the filter needs it
//  2. filter                  facility and severity
//  3. rate-limit              per network prefix of the PEER
//  4. sink DOWN               fail fast, even with an empty ring
//  5. copy into a pooled buffer, mint the id, mark ts/uts/nts
//  6. ring.Enqueue            blocks for the mode's guarantee
//
// Steps 1 to 4 happen BEFORE the copy, so a message that is not getting in never
// costs a buffer. That matters most on UDP, where a flood of filtered debug is
// the normal case.
type Ingest struct {
	ring    ring.Ring
	sink    SinkState
	filter  *filter.Filter
	limiter *ratelimit.Limiter
	m       *obs.Metrics
	log     *obs.Logger
	cfg     IngestConfig

	// defaultPRI is precomputed: it is the same value for every message that
	// arrives without one.
	defaultPRI int

	// failLog rate-limits the "could not accept" line. A refusing destination
	// produces one per message, and a collector must not be able to fill its own
	// log with notices about not collecting.
	failLog *obs.Sampler
}

// NewIngest wires the pipeline. limiter may be nil when rate limiting is off.
func NewIngest(r ring.Ring, s SinkState, f *filter.Filter, l *ratelimit.Limiter,
	m *obs.Metrics, log *obs.Logger, cfg IngestConfig) *Ingest {
	return &Ingest{
		ring: r, sink: s, filter: f, limiter: l, m: m, log: log, cfg: cfg,
		defaultPRI: cfg.DefaultFacility*8 + cfg.DefaultSeverity,
		failLog:    obs.NewSampler(30 * time.Second),
	}
}

// Accept runs the pipeline on one framed message and counts its ARRIVAL.
//
// raw does NOT have to outlive the call: step 5 copies it into a buffer the item
// owns. That is deliberate -- every transport reads into a reused buffer, and
// making them each own the copy would put the same subtle lifetime rule in four
// places.
//
// It blocks for as long as the ring's durability guarantee takes, and returns
// only once the transport is free to confirm.
//
// It does NOT count a drop. Counting is split across three calls -- Accept,
// Retry and RecordDrop -- because the stream transports hold a refused message
// and try it again: a message held through a thirty-second backpressure window
// would otherwise appear as six hundred arrivals and six hundred drops, and
// syslog_messages_total would stop meaning anything.
func (in *Ingest) Accept(ctx context.Context, raw []byte, w Wire) Outcome {
	in.m.MessagesTotal[w.Transport].Inc()
	in.m.MessageBytes.Observe(int64(len(raw)))
	return in.try(ctx, raw, w)
}

// Retry re-runs the pipeline for a message Accept already counted. It is what
// the TCP and RELP backpressure loops call.
func (in *Ingest) Retry(ctx context.Context, raw []byte, w Wire) Outcome {
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

func (in *Ingest) try(ctx context.Context, raw []byte, w Wire) Outcome {
	// --- 1. PRI -------------------------------------------------------------
	pri, fromWire := in.defaultPRI, false
	if p, _, ok := syslog.PRI(raw); ok {
		pri, fromWire = p, true
	}
	facility, severity := pri/8, pri%8

	// --- 2. filter ----------------------------------------------------------
	if !in.filter.Allow(facility, severity) {
		return Filtered
	}

	// --- 3. rate-limit ------------------------------------------------------
	//
	// The key is the transport peer, always. Syslog has no forwarding header,
	// and the hostname inside the message is an emitter's claim: keyed on that,
	// one forged line would poison every other emitter's bucket (§19.8).
	//
	// The unix transport is exempt by construction: it has no network peer, and
	// rate-limiting /dev/log would silence the host itself.
	if in.limiter != nil && w.PeerAddr.IsValid() {
		if ok, _ := in.limiter.Allow(w.PeerAddr); !ok {
			return RateLimited
		}
	}

	// --- 4. sink DOWN -------------------------------------------------------
	//
	// Fail fast even with an empty ring. Accepting into a ring that is not going
	// to drain only postpones the same loss and spends the ring's capacity on
	// messages that will end up in a dump.
	if in.sink != nil && !in.sink.Up() {
		return SinkDown
	}

	// --- 5. the item --------------------------------------------------------
	buf, class := envelope.GetRaw(len(raw))
	buf = append(buf, raw...)

	h := envelope.Head{
		ID:        envelope.NewID(),
		Transport: w.Transport,
		Framing:   w.Framing,
		Listener:  w.Listener,
		PeerAddr:  w.PeerAddr,
		PeerPort:  w.PeerPort,
		PRI:       pri,
		FromWire:  fromWire,
		Truncated: w.Truncated,
	}
	// One clock reading, decomposed. Three readings could straddle a second
	// boundary and produce a timestamp that never existed. The instant marked is
	// the end of reading the FRAME, before any parse.
	h.SetTime(time.Now())

	it := envelope.NewItem(h, buf, class)

	// --- 6. ring ------------------------------------------------------------
	err := in.ring.Enqueue(ctx, it)
	if err == nil {
		in.m.MessagesAcceptedTotal[w.Transport].Inc()
		return Accepted
	}

	switch {
	case errors.Is(err, ring.ErrFull):
		// Refused before the hand-off: this goroutine still owns the buffer
		// and nothing below has seen it (SPEC §14.2).
		it.Release()
		return RingFull
	case errors.Is(err, ring.ErrClosed):
		it.Release()
		return Shutdown
	}

	// Everything else came back from ring=none, and there the buffer may still
	// be in use below: a write that hit sink.op_timeout leaves its command
	// queued and rueidis still sends it, a BinaryString view of this very line.
	// This package cannot see the sink's error classes -- by design -- so it
	// cannot tell a consumed command from a queued one, and the safe answer for
	// all of them is the same: finish the item without pooling it (SPEC §14.2).
	it.Discard()

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// The destination did not answer in time. Retryable, like a full ring:
		// the transport holds the emitter and tries again.
		return RingFull
	case errors.Is(err, context.Canceled):
		return Shutdown
	}

	// The destination itself refused or failed. It is already counted and
	// classified by the instrumented wrapper; here it is one more message that
	// did not get in, and it is retryable for the same reason a down sink is:
	// the destination may well come back.
	if ok, suppressed := in.failLog.Allow(); ok {
		in.log.Error("could not accept a message", "error", err, "suppressed", suppressed)
	}
	return SinkDown
}
