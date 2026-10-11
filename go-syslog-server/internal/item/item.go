// Package item holds an accepted message on its way from a receiver, through
// the ring, to a sink (SPEC §2.2, §3.4).
//
// An Item carries two things and NO envelope:
//
//	Head  what the converter cannot recover from the line: uuid, the clock
//	      sample, transport, addresses, the TLS session, the original size,
//	      and the identity of the instance that received it
//	Raw   the syslog message bytes, already without any framing and already
//	      truncated to syslog.max_message_bytes
//
// The JLH envelope is DERIVED from those two plus the frozen conversion config,
// deterministically (SPEC §3.5, invariant §20.13). That is what makes a retry,
// a reingested emergency dump and an XAUTOCLAIMed ring entry produce the same
// bytes for the same uuid, as the JLH outbox rule requires.
package item

import (
	"net/netip"
	"sync/atomic"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

// Transport is the listener that received the message. The set is closed and
// known at compile time, which is what allows it to be a metric label.
type Transport uint8

const (
	TransportUDP Transport = iota
	TransportTCP
	TransportTLS
	TransportUnix
	numTransports
)

// NumTransports is the label cardinality of the transport dimension.
const NumTransports = int(numTransports)

var transportNames = [numTransports]string{"udp", "tcp", "tls", "unix"}

func (t Transport) String() string {
	if t < numTransports {
		return transportNames[t]
	}
	return "unknown"
}

// Network reports whether the transport has network addresses. The payload
// schema requires host.src-ip and host.src-port for these and forbids every
// network field for unix.
func (t Transport) Network() bool { return t == TransportUDP || t == TransportTCP || t == TransportTLS }

// ParseTransport is the inverse of String, for the head codec.
func ParseTransport(s string) (Transport, bool) {
	for t := Transport(0); t < numTransports; t++ {
		if transportNames[t] == s {
			return t, true
		}
	}
	return 0, false
}

// Framing is how the message was delimited on the wire (SPEC §5). The names are
// the payload's.
type Framing uint8

const (
	FramingDatagram Framing = iota
	FramingOctetCounting
	FramingNonTransparent
	numFramings
)

var framingNames = [numFramings]string{"datagram", "octet-counting", "non-transparent"}

func (f Framing) String() string {
	if f < numFramings {
		return framingNames[f]
	}
	return "unknown"
}

// ParseFraming is the inverse of String.
func ParseFraming(s string) (Framing, bool) {
	for f := Framing(0); f < numFramings; f++ {
		if framingNames[f] == s {
			return f, true
		}
	}
	return 0, false
}

// TLSInfo is the payload.tls object (SPEC §4.5). Empty fields are omitted from
// the payload. One value is shared, read-only, by every item of a connection.
type TLSInfo struct {
	Version         string // TLS1.2, TLS1.3
	Cipher          string // IANA name
	PeerSubject     string // RFC 4514 DN, omitted above 1 KiB
	PeerFingerprint string // sha-256:XX:XX:...
}

// Empty reports whether nothing would be written for it.
func (t *TLSInfo) Empty() bool {
	return t == nil || t.Version == "" && t.Cipher == "" && t.PeerSubject == "" && t.PeerFingerprint == ""
}

// Head is everything about a message that reading its bytes cannot tell you.
type Head struct {
	UUID uuidv7.UUID

	// From and Space are the identity of the instance that RECEIVED the
	// message. With ring=redis another replica may drain it, and the envelope
	// still says who brought it in (SPEC §3.1).
	From  string
	Space string

	// UTS and Sub are the one clock sample of the message (SPEC §3.1), taken
	// when the last octet of the frame was read. The uuid was generated from
	// the same sample, and event is copied from it.
	UTS int64
	Sub int64

	Transport Transport
	Framing   Framing

	// SrcIP/SrcPort and DstIP/DstPort are canonical (Canonical). For network
	// transports SrcIP is always valid; DstIP is the zero Addr when the local
	// address is unknown and is then omitted from the payload. With a trusted
	// PROXY header these are the addresses the header declared.
	SrcIP   netip.Addr
	SrcPort uint16
	DstIP   netip.Addr
	DstPort uint16

	// PeerIP/PeerPort are the real TCP peer, set ONLY when a PROXY header
	// replaced the addresses above. They become payload x-proxy (SPEC §4.6).
	PeerIP   netip.Addr
	PeerPort uint16

	TLS *TLSInfo

	// Size is the message length before truncation. Size > len(Raw) is how
	// the converter knows the message was truncated (SPEC §5.5).
	Size int
}

// Time is the clock sample as a time.Time, in UTC.
func (h *Head) Time() time.Time { return time.Unix(h.UTS, h.Sub).UTC() }

// SetTime records the one clock sample.
func (h *Head) SetTime(t time.Time) {
	h.UTS = t.Unix()
	h.Sub = int64(t.Nanosecond())
}

// Proxied reports whether a PROXY header supplied the addresses.
func (h *Head) Proxied() bool { return h.PeerIP.IsValid() }

// Canonical applies the payload's address form (SPEC §4.2): an IPv4-mapped
// IPv6 address becomes IPv4, the zone is dropped, and an unspecified address
// (0.0.0.0, ::) becomes the zero Addr -- unknown, never recorded.
func Canonical(a netip.Addr) netip.Addr {
	if !a.IsValid() {
		return netip.Addr{}
	}
	a = a.Unmap().WithZone("")
	if a.IsUnspecified() {
		return netip.Addr{}
	}
	return a
}

// Item is one message travelling from a receiver through the ring to a sink.
//
// Raw is owned by the Item. Release must be called exactly once, by whoever
// confirms the item (SPEC §15.2).
type Item struct {
	Head Head
	Raw  []byte

	// rawClass is the pool class Raw came from; -1 when it was not pooled.
	rawClass int

	// released guards against a double return, which is the ownership bug the
	// whole pooling scheme exists to prevent. It is atomic because a plain
	// bool is not a guard: two goroutines that both believed they owned the
	// item would both read false and both file the buffer back into the pool,
	// and one emitter's line would end up inside another emitter's event.
	released atomic.Bool

	// tainted records that a lower layer may still read Raw even though the
	// call that used it returned: a sink write that ended by the clock, or a
	// ring=redis enqueue whose fate is unknown. It turns Release into a
	// hand-over to the GC instead of to the pool (SPEC §15.2).
	tainted atomic.Bool

	// done, when non-nil, receives the ring's durability result. It is what
	// releases a TCP reader to resume reading.
	done chan error

	// RowID is the ring=sqlite row this item came from, or 0.
	RowID int64

	// StreamID is the ring=redis entry id this item came from, or "".
	StreamID string
}

// New builds an item that owns raw. rawClass must be the value GetRaw returned,
// or -1 for a buffer that must never be pooled.
func New(head Head, raw []byte, rawClass int) *Item {
	return &Item{Head: head, Raw: raw, rawClass: rawClass}
}

// Size is the item's contribution to the ring's byte accounting: the raw line
// and nothing else. The JLH does not exist while the item is in the ring
// (SPEC §9.2).
func (it *Item) Size() int64 { return int64(len(it.Raw)) }

// Release returns the buffer to its pool. It is idempotent, so a double release
// is a no-op rather than corruption -- but it is still a bug: the second caller
// believed it owned the item. A tainted item is finished but NOT pooled.
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

// Taint marks the buffer as possibly still read by a lower layer. From then on
// Release hands it to the GC, even if a later attempt delivers the item: the
// earlier attempt may still be on its way through another pipeline connection.
func (it *Item) Taint() {
	if it != nil {
		it.tainted.Store(true)
	}
}

// Discard gives the item up WITHOUT returning its buffer to the pool, for the
// error paths where a lower layer may still read it. It deliberately does not
// nil Raw: that write would race with the reader.
func (it *Item) Discard() {
	if it == nil {
		return
	}
	it.tainted.Store(true)
	it.released.Store(true)
}

// Released reports whether Release (or Discard) already ran.
func (it *Item) Released() bool { return it.released.Load() }

// Tainted reports whether the buffer will be left to the GC.
func (it *Item) Tainted() bool { return it.tainted.Load() }

// Done returns the channel a receiver waits on for the ring's durability
// acknowledgement, creating it on first use.
func (it *Item) Done() chan error {
	if it.done == nil {
		it.done = make(chan error, 1)
	}
	return it.done
}

// Ack delivers the durability result to a waiting receiver. It is safe to call
// when nobody waits, which is the normal case for UDP.
func (it *Item) Ack(err error) {
	if it.done == nil {
		return
	}
	select {
	case it.done <- err:
	default:
	}
}
