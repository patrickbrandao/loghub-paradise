package envelope

import (
	"encoding/json"
	"fmt"
	"net/netip"
)

// The item head codec (SPEC §3.2).
//
// One encoder, two consumers: the ring=sqlite head column and the emergency
// file. Both store head + raw line, because everything else in the envelope is
// derived from those two deterministically (invariant §19.13).
//
// That is why reingestion here is exact and why it is a documented trap in
// go-webhook-server. There the dump stored a finished envelope, and the
// queryable columns came from an ItemMeta that had to be REBUILT by parsing
// escaped JSON; forgetting it wrote rows with an empty path and ts = 0 --
// invisible to the very query those columns exist for, with an intact envelope
// beside them hiding the loss. Here the head IS the metadata.
//
// The format is JSON. It runs once per event at boot, far off the hot path, and
// a hand-rolled binary format would be a new parser of input that a corrupted
// disk can make arbitrary -- which is exactly the thing this project fuzzes
// instead of hand-writing.

// EncodeHead appends the head as a JSON object. It writes by hand for the same
// reason AppendJSON does: this runs once per accepted message when the ring is
// persistent.
func EncodeHead(dst []byte, h *Head) []byte {
	b := append(dst, '{')

	b = appendKey(b, "id")
	b = appendString(b, h.ID)

	b = append(b, ',')
	b = appendKey(b, "ts")
	b = appendInt(b, h.TS)

	b = append(b, ',')
	b = appendKey(b, "uts")
	b = appendInt(b, h.UTS)

	b = append(b, ',')
	b = appendKey(b, "nts")
	b = appendInt(b, h.NTS)

	b = append(b, ',')
	b = appendKey(b, "transport")
	b = appendString(b, h.Transport.String())

	b = append(b, ',')
	b = appendKey(b, "framing")
	b = appendString(b, h.Framing.String())

	b = append(b, ',')
	b = appendKey(b, "listener")
	b = appendString(b, h.Listener)

	b = append(b, ',')
	b = appendKey(b, "peer-addr")
	if h.PeerAddr.IsValid() {
		b = append(b, '"')
		b = h.PeerAddr.Unmap().AppendTo(b)
		b = append(b, '"')
	} else {
		b = append(b, '"', '"')
	}

	b = append(b, ',')
	b = appendKey(b, "peer-port")
	b = appendInt(b, int64(h.PeerPort))

	b = append(b, ',')
	b = appendKey(b, "pri")
	b = appendInt(b, int64(h.PRI))

	b = append(b, ',')
	b = appendKey(b, "pri-wire")
	b = appendBool(b, h.FromWire)

	b = append(b, ',')
	b = appendKey(b, "truncated")
	b = appendBool(b, h.Truncated)

	return append(b, '}')
}

// headJSON mirrors what EncodeHead writes.
type headJSON struct {
	ID        string `json:"id"`
	TS        int64  `json:"ts"`
	UTS       int64  `json:"uts"`
	NTS       int64  `json:"nts"`
	Transport string `json:"transport"`
	Framing   string `json:"framing"`
	Listener  string `json:"listener"`
	PeerAddr  string `json:"peer-addr"`
	PeerPort  int    `json:"peer-port"`
	PRI       int    `json:"pri"`
	FromWire  bool   `json:"pri-wire"`
	Truncated bool   `json:"truncated"`
}

// DecodeHead rebuilds a Head. It is deliberately forgiving about individual
// fields and strict about the object itself.
//
// A head is read back after a crash, from a file a disk may have half written.
// Refusing a whole record because one field is wrong would throw away a message
// the emitter was already told we had; a field that is missing or nonsense
// leaves its zero value and the message still reaches the destination with its
// identity and its bytes intact.
//
// The one thing that IS rejected is a head that is not a JSON object at all,
// because then nothing about it can be trusted -- including the id, which is
// the primary key at the destination.
func DecodeHead(b []byte) (Head, error) {
	var v headJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return Head{}, fmt.Errorf("envelope: decoding item head: %w", err)
	}
	if v.ID == "" {
		return Head{}, fmt.Errorf("envelope: item head has no id")
	}

	h := Head{
		ID:        v.ID,
		TS:        v.TS,
		UTS:       v.UTS,
		NTS:       v.NTS,
		Listener:  v.Listener,
		PRI:       v.PRI,
		FromWire:  v.FromWire,
		Truncated: v.Truncated,
	}
	// An unknown transport or framing falls back to the first value rather than
	// failing: they are labels, and a label nobody recognises is a worse reason
	// to lose an event than it is a problem.
	if t, ok := ParseTransport(v.Transport); ok {
		h.Transport = t
	}
	if f, ok := ParseFraming(v.Framing); ok {
		h.Framing = f
	}
	if v.PeerAddr != "" {
		if a, err := netip.ParseAddr(v.PeerAddr); err == nil {
			h.PeerAddr = a.Unmap()
		}
	}
	if v.PeerPort >= 0 && v.PeerPort <= 65535 {
		h.PeerPort = uint16(v.PeerPort)
	}
	// A priority outside 0..191 could only come from a corrupted record, and
	// every consumer downstream indexes on facility and severity derived from
	// it. Clamping to the RFC 3164 §4.3.3 default keeps those indexes sane.
	if h.PRI < 0 || h.PRI > 191 {
		h.PRI = 13 // user.notice
		h.FromWire = false
	}
	return h, nil
}
