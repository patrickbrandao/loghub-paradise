// Package envelope holds an accepted message on its way from a receiver,
// through the ring, to a sink -- and builds the event envelope of SPEC §3.1.
//
// The central structural difference from go-webhook-server lives here. There,
// the handler serialises the whole envelope before the item enters the ring.
// Here it does not: only the PRI is read on the receive path, and the full
// parse plus the envelope happen in the drain worker (SPEC §2.2).
//
// So an Item carries two things and no envelope:
//
//	Head  what the parser CANNOT recover from the line: identity, reception
//	      instant, transport, peer, effective PRI
//	Raw   the wire bytes of the syslog message, without any transport framing
//
// Everything else in the envelope is DERIVED from those two, deterministically
// (invariant §19.13). That is what makes reingestion exact: an item restored
// from an emergency file is indistinguishable from the original, and there is
// no metadata to reconstruct by parsing escaped JSON -- the trap the sibling
// service has in MetaFromJSON simply does not exist here.
package envelope

import (
	"net/netip"
	"sync/atomic"
	"time"
)

// Transport is which of the four listeners received the message. The value set
// is closed and known at compile time, which is what allows it to be a metric
// label (SPEC §13.1).
type Transport uint8

const (
	TransportUDP Transport = iota
	TransportTCP
	TransportUnix
	TransportRELP
	numTransports
)

func (t Transport) String() string {
	switch t {
	case TransportUDP:
		return "udp"
	case TransportTCP:
		return "tcp"
	case TransportUnix:
		return "unix"
	case TransportRELP:
		return "relp"
	}
	return "unknown"
}

// ParseTransport is the inverse, for decoding an item head.
func ParseTransport(s string) (Transport, bool) {
	for t := Transport(0); t < numTransports; t++ {
		if t.String() == s {
			return t, true
		}
	}
	return 0, false
}

// NumTransports is the label cardinality of the transport dimension.
const NumTransports = int(numTransports)

// Framing is how the message was delimited on the wire (SPEC §5).
type Framing uint8

const (
	FramingDatagram Framing = iota // UDP and unix: the datagram is the message
	FramingOctet                   // RFC 6587 §3.4.1, length-prefixed
	FramingLF                      // RFC 6587 §3.4.2, newline-delimited
	FramingRELP
	numFramings
)

func (f Framing) String() string {
	switch f {
	case FramingDatagram:
		return "datagram"
	case FramingOctet:
		return "octet"
	case FramingLF:
		return "lf"
	case FramingRELP:
		return "relp"
	}
	return "unknown"
}

// ParseFraming is the inverse, for decoding an item head.
func ParseFraming(s string) (Framing, bool) {
	for f := Framing(0); f < numFramings; f++ {
		if f.String() == s {
			return f, true
		}
	}
	return 0, false
}

// Head is everything about a message that reading its bytes cannot tell you.
//
// It is what travels beside the raw line through a persistent ring and an
// emergency file, and it is the whole reason reingestion needs no
// reconstruction step (SPEC §3.2).
type Head struct {
	ID string

	// One time.Now() decomposed, never two clock reads. Seconds plus a
	// within-the-second fraction keeps every number far below 2^53, so no
	// consumer -- JavaScript included -- loses digits.
	TS  int64 // unix seconds
	UTS int64 // microseconds within the second
	NTS int64 // nanoseconds within the second

	Transport Transport
	Framing   Framing
	Listener  string // the listen.* address that received it

	// PeerAddr is the real end of the transport, always. On unix it is the
	// zero Addr: there is no network peer. The hostname INSIDE the message is
	// the emitter's claim and is never used for any server decision
	// (invariant §19.8).
	PeerAddr netip.Addr
	PeerPort uint16

	// PRI is the EFFECTIVE priority. FromWire is false when the message had no
	// usable PRI and parse.default_facility/default_severity supplied it, which
	// is exactly what RFC 3164 §4.3.3 tells a relay to do.
	PRI      int
	FromWire bool

	// Truncated records that the transport or the frame ceiling cut the line.
	Truncated bool
}

// Facility and Severity decompose the effective priority.
func (h *Head) Facility() int { return h.PRI / 8 }
func (h *Head) Severity() int { return h.PRI % 8 }

// PRISource is the envelope's pri-source field.
func (h *Head) PRISource() string {
	if h.FromWire {
		return "wire"
	}
	return "default"
}

// SetTime decomposes one instant into the three envelope fields.
//
// Callers must pass a single time.Now() result. Reading the clock three times
// could straddle a second boundary and produce a timestamp that never existed.
func (h *Head) SetTime(t time.Time) {
	h.TS = t.Unix()
	n := int64(t.Nanosecond())
	h.UTS = n / 1000
	h.NTS = n
}

// PeerIPVersion is 4, 6, or 0 for the unix transport.
func (h *Head) PeerIPVersion() int {
	if !h.PeerAddr.IsValid() {
		return 0
	}
	if h.PeerAddr.Unmap().Is4() {
		return 4
	}
	return 6
}

// Item is one message travelling from a receiver through the ring to a sink.
//
// Raw is owned by the Item. Release must be called exactly once, by whoever
// confirms the item (SPEC §14.2).
type Item struct {
	Head Head
	Raw  []byte

	// rawClass is the pool class Raw came from; -1 when it was not pooled.
	rawClass int

	// released guards against a double return, which is the ownership bug the
	// whole pooling scheme exists to prevent.
	//
	// It is atomic, not a plain bool, because a non-atomic flag is not a guard:
	// if two goroutines ever both believe they own the item, both would read
	// false and both would file the same buffer back into the pool, and one
	// emitter's line would end up inside another emitter's event.
	released atomic.Bool

	// tainted records that a lower layer may still be reading Raw even though
	// the call that used it has returned. It is set on the error paths of
	// SPEC §14.2 -- a sink write that timed out or was cancelled -- and it
	// turns the eventual Release into a hand-over to the GC instead of to the
	// pool. rueidis is why it exists: a DoMulti that returns early on a
	// cancelled context leaves its commands queued, and the pipeline still
	// serialises them -- BinaryString views of this very buffer -- after the
	// caller has moved on.
	tainted atomic.Bool

	// done, when non-nil, is closed by the ring once the item's durability
	// guarantee is met. It is what releases a RELP connection to answer
	// "200 OK", and a TCP reader to resume reading.
	done chan error

	// RowID is the ring=sqlite row this item came from, or 0. It lets Commit
	// delete exactly the delivered rows without a side table keyed by item.
	RowID int64
}

// Size is the item's contribution to the ring's byte accounting.
//
// It counts the raw line and nothing else, unlike the sibling service which
// also counts a serialised envelope. Here the envelope does not exist while the
// item is in the ring, so adding it would be accounting for memory nobody has
// allocated. The worker's envelope buffers are bounded separately, by
// sink.workers x sink.batch (SPEC §9.2, §14.3).
func (it *Item) Size() int64 { return int64(len(it.Raw)) }

// Release returns the item's buffer to its pool. It is idempotent, so a double
// release is a no-op rather than corruption -- but a double release is still a
// bug: the second caller believed it owned the item.
//
// A tainted item (see Taint) is finished but NOT pooled: its buffer is left to
// the garbage collector, because something below may still be reading it.
func (it *Item) Release() {
	if it == nil || it.released.Swap(true) {
		return
	}
	if it.tainted.Load() {
		return
	}
	PutRaw(it.Raw, it.rawClass)
	it.Raw = nil
}

// Taint marks the item as one whose buffer may still be read by a lower layer
// after the call that used it returned: a sink write that timed out or was
// cancelled. From then on Release hands the buffer to the GC instead of the
// pool, even if a later attempt delivers the item -- the earlier attempt's
// command may still be on its way through another pipeline connection, and
// pooling the buffer is how one emitter's line ends up inside another's event.
// The cost is one allocation on a path that only runs when something already
// went wrong.
func (it *Item) Taint() {
	if it != nil {
		it.tainted.Store(true)
	}
}

// Discard gives the item up WITHOUT returning its buffer to the pool.
//
// It is Taint followed by Release, for the error paths where the receiver no
// longer wants the item but cannot know whether a lower layer still reads it:
// a ring=none write that timed out, or any failure that is not the ring
// refusing the item outright (SPEC §14.2). It deliberately does not nil Raw --
// that write would race with the reader. The buffer goes to the GC instead.
func (it *Item) Discard() {
	if it == nil {
		return
	}
	it.tainted.Store(true)
	it.released.Store(true)
}

// Released reports whether Release has already run. Tests use it to assert the
// ownership rule.
func (it *Item) Released() bool { return it.released.Load() }

// NewItem builds an item that owns raw. rawClass must be the value GetRaw
// returned, or -1 for a buffer that must never be pooled.
func NewItem(head Head, raw []byte, rawClass int) *Item {
	return &Item{Head: head, Raw: raw, rawClass: rawClass}
}

// Done returns the channel a receiver waits on for the ring's durability
// acknowledgement, creating it on first use.
func (it *Item) Done() chan error {
	if it.done == nil {
		it.done = make(chan error, 1)
	}
	return it.done
}

// Ack delivers the durability result to a waiting receiver. It is safe to call
// when nobody is waiting, which is the normal case for UDP: that transport has
// no way to acknowledge anything (SPEC §2.3).
func (it *Item) Ack(err error) {
	if it.done == nil {
		return
	}
	select {
	case it.done <- err:
	default:
	}
}
