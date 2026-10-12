package item

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strconv"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
)

// The item head codec (SPEC §3.4).
//
// One encoder, three consumers: the head column of ring.db, the h field of the
// ring=redis stream and the emergency dump. With ring=memory nothing is
// serialised at all.
//
// The format is JSON. It runs once per message, off the hot path, and a
// hand-rolled binary format would be a new parser of input that a corrupted
// disk -- or another replica on a shared Redis -- can make arbitrary. The
// encoder is an append writer; the decoder is encoding/json followed by a
// validation that refuses anything that could make the converter emit an
// envelope the schema rejects. FuzzHead holds it to that.

// HeadVersion is the codec version. Any other value is an undecodable row
// (SPEC §9.4: quarantine).
const HeadVersion = 1

// maxPeerSubject is the ceiling of payload.tls.peer-subject (SPEC §4.5).
const maxPeerSubject = 1024

// MaxTimestamp is the last second event.iso can represent (JLH schema).
const MaxTimestamp = 253402300799

// ErrHead is wrapped by every decoding failure.
var ErrHead = errors.New("item: undecodable head")

// EncodeHead appends the head as a JSON object.
func EncodeHead(dst []byte, h *Head) []byte {
	dst = append(dst, `{"v":`...)
	dst = strconv.AppendInt(dst, HeadVersion, 10)
	dst = append(dst, `,"uuid":"`...)
	dst = h.UUID.AppendTo(dst)
	dst = append(dst, `","from":`...)
	dst = jlh.AppendStr(dst, h.From)
	dst = append(dst, `,"space":`...)
	dst = jlh.AppendStr(dst, h.Space)
	dst = append(dst, `,"uts":`...)
	dst = strconv.AppendInt(dst, h.UTS, 10)
	dst = append(dst, `,"sub":`...)
	dst = strconv.AppendInt(dst, h.Sub, 10)
	dst = append(dst, `,"transport":"`...)
	dst = append(dst, h.Transport.String()...)
	dst = append(dst, `","framing":"`...)
	dst = append(dst, h.Framing.String()...)
	dst = append(dst, '"')
	if h.Transport.Network() {
		dst = append(dst, `,"src-ip":"`...)
		dst = h.SrcIP.AppendTo(dst)
		dst = append(dst, `","src-port":`...)
		dst = strconv.AppendUint(dst, uint64(h.SrcPort), 10)
		if h.DstIP.IsValid() {
			dst = append(dst, `,"dst-ip":"`...)
			dst = h.DstIP.AppendTo(dst)
			dst = append(dst, '"')
		}
		dst = append(dst, `,"dst-port":`...)
		dst = strconv.AppendUint(dst, uint64(h.DstPort), 10)
		if h.PeerIP.IsValid() {
			dst = append(dst, `,"peer-ip":"`...)
			dst = h.PeerIP.AppendTo(dst)
			dst = append(dst, `","peer-port":`...)
			dst = strconv.AppendUint(dst, uint64(h.PeerPort), 10)
		}
	}
	if !h.TLS.Empty() {
		dst = append(dst, `,"tls":{`...)
		first := true
		field := func(k, v string) {
			if v == "" {
				return
			}
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = jlh.AppendKey(dst, k)
			dst = jlh.AppendStr(dst, v)
		}
		field("version", h.TLS.Version)
		field("cipher", h.TLS.Cipher)
		field("peer-subject", h.TLS.PeerSubject)
		field("peer-fingerprint", h.TLS.PeerFingerprint)
		dst = append(dst, '}')
	}
	dst = append(dst, `,"size":`...)
	dst = strconv.AppendInt(dst, int64(h.Size), 10)
	return append(dst, '}')
}

type wireTLS struct {
	Version         *string `json:"version"`
	Cipher          *string `json:"cipher"`
	PeerSubject     *string `json:"peer-subject"`
	PeerFingerprint *string `json:"peer-fingerprint"`
}

type wireHead struct {
	V         *int64   `json:"v"`
	UUID      *string  `json:"uuid"`
	From      *string  `json:"from"`
	Space     *string  `json:"space"`
	UTS       *int64   `json:"uts"`
	Sub       *int64   `json:"sub"`
	Transport *string  `json:"transport"`
	Framing   *string  `json:"framing"`
	SrcIP     *string  `json:"src-ip"`
	SrcPort   *int64   `json:"src-port"`
	DstIP     *string  `json:"dst-ip"`
	DstPort   *int64   `json:"dst-port"`
	PeerIP    *string  `json:"peer-ip"`
	PeerPort  *int64   `json:"peer-port"`
	TLS       *wireTLS `json:"tls"`
	Size      *int64   `json:"size"`
}

var (
	tlsVersionRE  = regexp.MustCompile(`^D?TLS1\.[0-3]$`)
	tlsCipherRE   = regexp.MustCompile(`^TLS_[A-Z0-9_]+$`)
	fingerprintRE = regexp.MustCompile(`^[a-z0-9-]+(:[0-9A-F]{2})+$`)
)

// ValidTLSVersion, ValidTLSCipher and ValidFingerprint are the payload schema's
// patterns. The receiver uses them too, so a value the schema would reject is
// omitted at the source instead of being refused here later.
func ValidTLSVersion(s string) bool  { return tlsVersionRE.MatchString(s) }
func ValidTLSCipher(s string) bool   { return tlsCipherRE.MatchString(s) }
func ValidFingerprint(s string) bool { return fingerprintRE.MatchString(s) }

// ValidPeerSubject is the receiver's rule for payload.tls.peer-subject.
func ValidPeerSubject(s string) bool { return s != "" && len(s) <= maxPeerSubject }

// DecodeHead parses a head written by EncodeHead and validates it.
//
// Every refusal wraps ErrHead. Nothing that passes can make the converter emit
// an envelope that fails the JLH or the syslog payload schema.
func DecodeHead(b []byte) (Head, error) {
	var w wireHead
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return Head{}, fmt.Errorf("%w: %v", ErrHead, err)
	}
	if dec.More() {
		return Head{}, fmt.Errorf("%w: trailing data", ErrHead)
	}
	h, err := w.head()
	if err != nil {
		return Head{}, fmt.Errorf("%w: %v", ErrHead, err)
	}
	return h, nil
}

func (w *wireHead) head() (Head, error) {
	var h Head
	if w.V == nil || *w.V != HeadVersion {
		return h, errors.New("unknown codec version")
	}
	if w.UUID == nil || w.From == nil || w.Space == nil || w.UTS == nil || w.Sub == nil ||
		w.Transport == nil || w.Framing == nil || w.Size == nil {
		return h, errors.New("missing required key")
	}
	u, err := uuidv7.FromString(*w.UUID)
	if err != nil || !u.IsValid() {
		return h, errors.New("uuid is not a UUIDv7")
	}
	h.UUID = u
	if !ident.ValidFrom(*w.From) {
		return h, errors.New("invalid from")
	}
	if err := ident.ValidateSpace(*w.Space); err != nil {
		return h, err
	}
	h.From, h.Space = *w.From, *w.Space
	if *w.UTS < 0 || *w.UTS > MaxTimestamp || *w.Sub < 0 || *w.Sub > 999999999 {
		return h, errors.New("clock sample out of range")
	}
	h.UTS, h.Sub = *w.UTS, *w.Sub
	// The uuid and the clock sample are one sample (SPEC §3.1). A head where
	// they disagree was not written by this program, and the envelope it would
	// produce breaks the JLH rule that, in real time, the uuid instant is the
	// event.
	if t, ok := u.TimestampWithLevel(uuidv7.Level1); !ok || t.UnixMilli() != h.Time().UnixMilli() {
		return h, errors.New("uuid instant does not match the clock sample")
	}

	var ok bool
	if h.Transport, ok = ParseTransport(*w.Transport); !ok {
		return h, errors.New("unknown transport")
	}
	if h.Framing, ok = ParseFraming(*w.Framing); !ok {
		return h, errors.New("unknown framing")
	}
	if !FramingAllowed(h.Transport, h.Framing) {
		return h, errors.New("framing does not match the transport")
	}

	if h.Transport.Network() {
		if w.SrcIP == nil || w.SrcPort == nil || w.DstPort == nil {
			return h, errors.New("network transport without addresses")
		}
		if h.SrcIP, err = canonicalText(*w.SrcIP); err != nil || !h.SrcIP.IsValid() {
			return h, errors.New("invalid src-ip")
		}
		if h.SrcPort, err = port(*w.SrcPort); err != nil {
			return h, err
		}
		if w.DstIP != nil {
			if h.DstIP, err = canonicalText(*w.DstIP); err != nil || !h.DstIP.IsValid() {
				return h, errors.New("invalid dst-ip")
			}
		}
		if h.DstPort, err = port(*w.DstPort); err != nil {
			return h, err
		}
		if (w.PeerIP == nil) != (w.PeerPort == nil) {
			return h, errors.New("peer-ip and peer-port go together")
		}
		if w.PeerIP != nil {
			if h.Transport == TransportUDP {
				return h, errors.New("udp has no proxy peer")
			}
			if h.PeerIP, err = canonicalText(*w.PeerIP); err != nil || !h.PeerIP.IsValid() {
				return h, errors.New("invalid peer-ip")
			}
			if h.PeerPort, err = port(*w.PeerPort); err != nil {
				return h, err
			}
		}
	} else if w.SrcIP != nil || w.SrcPort != nil || w.DstIP != nil || w.DstPort != nil ||
		w.PeerIP != nil || w.PeerPort != nil {
		return h, errors.New("unix transport with network addresses")
	}

	if w.TLS != nil {
		if h.Transport != TransportTLS {
			return h, errors.New("tls session on a non-tls transport")
		}
		t := &TLSInfo{}
		if w.TLS.Version != nil {
			if !ValidTLSVersion(*w.TLS.Version) {
				return h, errors.New("invalid tls version")
			}
			t.Version = *w.TLS.Version
		}
		if w.TLS.Cipher != nil {
			if !ValidTLSCipher(*w.TLS.Cipher) {
				return h, errors.New("invalid tls cipher")
			}
			t.Cipher = *w.TLS.Cipher
		}
		if w.TLS.PeerSubject != nil {
			if !ValidPeerSubject(*w.TLS.PeerSubject) {
				return h, errors.New("invalid tls peer-subject")
			}
			t.PeerSubject = *w.TLS.PeerSubject
		}
		if w.TLS.PeerFingerprint != nil {
			if !ValidFingerprint(*w.TLS.PeerFingerprint) {
				return h, errors.New("invalid tls peer-fingerprint")
			}
			t.PeerFingerprint = *w.TLS.PeerFingerprint
		}
		if !t.Empty() {
			h.TLS = t
		}
	}

	if *w.Size < 0 {
		return h, errors.New("negative size")
	}
	h.Size = int(*w.Size)
	return h, nil
}

// FramingAllowed is the payload schema's transport x framing table (SPEC §3.2).
func FramingAllowed(t Transport, f Framing) bool {
	switch t {
	case TransportUDP:
		return f == FramingDatagram
	case TransportTCP:
		return f == FramingOctetCounting || f == FramingNonTransparent
	case TransportTLS:
		return f == FramingOctetCounting
	case TransportUnix:
		return f == FramingDatagram || f == FramingNonTransparent
	}
	return false
}

// canonicalText parses an address and accepts it only in its canonical text
// form, so the payload carries exactly what the head stored.
func canonicalText(s string) (netip.Addr, error) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, err
	}
	c := Canonical(a)
	if !c.IsValid() || c.String() != s {
		return netip.Addr{}, errors.New("address is not canonical")
	}
	return c, nil
}

func port(n int64) (uint16, error) {
	if n < 0 || n > 65535 {
		return 0, errors.New("port out of range")
	}
	return uint16(n), nil
}
