package envelope

import (
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Version is the value of the stream entry's "v" field. It lives OUTSIDE the
// envelope so a future version can change the envelope's ENCODING without a
// consumer having to parse the envelope in order to discover how to parse it.
const Version = "1"

// AppendJSON writes the envelope of SPEC §3.1 into dst and returns the extended
// slice. dst is expected to come from GetEnv.
//
// It runs in the drain worker, after the full parse, never on the receive path
// (SPEC §2.2). p must be the result of parsing it.Raw.
//
// The message text is deliberately NOT here. The envelope carries msg-offset
// and msg-size, and the consumer slices the raw line:
//
//	msg := raw[env["msg-offset"] : env["msg-offset"]+env["msg-size"]]
//
// A "msg" field would duplicate the bulk of every event -- in a 200-byte
// message the text is most of it -- and would cost escaping on write and
// UNESCAPING on read, where slicing costs neither. The raw line has to exist
// regardless: the parse is best-effort, and when it reports rfc "unknown" the
// original bytes are the only way to find out what the emitter actually sent.
func AppendJSON(dst []byte, it *Item, p *syslog.Parsed, nodeID string) []byte {
	h := &it.Head
	b := dst
	b = append(b, '{')

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

	// The real end of the transport, always. The hostname field below is what
	// the emitter claims; these two are what the kernel saw (invariant §19.8).
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
	b = appendKey(b, "peer-ip-version")
	b = appendInt(b, int64(h.PeerIPVersion()))

	b = append(b, ',')
	b = appendKey(b, "rfc")
	b = appendString(b, p.RFC.String())

	b = append(b, ',')
	b = appendKey(b, "pri")
	b = appendInt(b, int64(h.PRI))

	b = append(b, ',')
	b = appendKey(b, "pri-source")
	b = appendString(b, h.PRISource())

	// facility-name and severity-name are redundant with the numbers. It is
	// the same trade uts makes against nts: about twenty bytes per event so
	// that no consumer has to carry a translation table.
	b = append(b, ',')
	b = appendKey(b, "facility")
	b = appendInt(b, int64(h.Facility()))

	b = append(b, ',')
	b = appendKey(b, "facility-name")
	b = appendString(b, syslog.FacilityName(h.Facility()))

	b = append(b, ',')
	b = appendKey(b, "severity")
	b = appendInt(b, int64(h.Severity()))

	b = append(b, ',')
	b = appendKey(b, "severity-name")
	b = appendString(b, syslog.SeverityName(h.Severity()))

	b = append(b, ',')
	b = appendKey(b, "version")
	b = appendInt(b, int64(p.Version))

	// msg-ts is exported and used for nothing (SPEC §6.4). RFC 3164 carries no
	// year and no zone, both of which the parser had to invent; RFC 5424
	// carries a clock nobody audited. Correlation uses ts.
	b = append(b, ',')
	b = appendKey(b, "msg-ts")
	if p.MsgTSSrc == syslog.TSNone {
		b = append(b, '"', '"')
	} else {
		b = append(b, '"')
		b = p.MsgTS.AppendFormat(b, time.RFC3339Nano)
		b = append(b, '"')
	}

	b = append(b, ',')
	b = appendKey(b, "msg-ts-source")
	b = appendString(b, p.MsgTSSrc.String())

	b = append(b, ',')
	b = appendKey(b, "hostname")
	b = appendBytes(b, p.Hostname)

	b = append(b, ',')
	b = appendKey(b, "app-name")
	b = appendBytes(b, p.AppName)

	b = append(b, ',')
	b = appendKey(b, "procid")
	b = appendBytes(b, p.ProcID)

	b = append(b, ',')
	b = appendKey(b, "msgid")
	b = appendBytes(b, p.MsgID)

	b = append(b, ',')
	b = appendKey(b, "structured-data")
	b = appendSD(b, p.SD)

	b = append(b, ',')
	b = appendKey(b, "msg-offset")
	b = appendInt(b, int64(p.MsgOffset))

	b = append(b, ',')
	b = appendKey(b, "msg-size")
	b = appendInt(b, int64(p.MsgSize))

	b = append(b, ',')
	b = appendKey(b, "raw-size")
	b = appendInt(b, int64(len(it.Raw)))

	b = append(b, ',')
	b = appendKey(b, "truncated")
	b = appendBool(b, h.Truncated)

	b = append(b, ',')
	b = appendKey(b, "node-id")
	b = appendString(b, nodeID)

	return append(b, '}')
}

// appendSD renders structured data as an object of objects, the same shape the
// sibling service gives HTTP headers.
//
// A duplicate SD-ID cannot appear here: the parser resolves it to last-wins,
// because two entries with the same key in a JSON object are ambiguous to every
// consumer that reads one.
func appendSD(b []byte, sd []syslog.SDElement) []byte {
	b = append(b, '{')
	for i := range sd {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendBytes(b, sd[i].ID)
		b = append(b, ':', '{')
		for j := range sd[i].Params {
			if j > 0 {
				b = append(b, ',')
			}
			b = appendBytes(b, sd[i].Params[j].Name)
			b = append(b, ':')
			b = appendBytes(b, sd[i].Params[j].Value)
		}
		b = append(b, '}')
	}
	return append(b, '}')
}

const (
	// maxEscape is the most bytes one input byte can become in the envelope: a
	// control byte is written as \u00XX.
	maxEscape = 6

	// envelopeFixedBytes covers what does not scale with the input: the thirty
	// keys and their punctuation, and every value of bounded size -- the id,
	// the three clocks, the closed-set names, the peer address with a zone, the
	// port, msg-ts and the three offsets. Measured at its widest it is about
	// 600 bytes; TestMaxBytesCoversTheFixedPart holds it to the constant.
	envelopeFixedBytes = 1 << 10
)

// MaxBytes bounds the envelope AppendJSON can produce for a raw line of at most
// rawLen bytes, received on a listener whose address is at most listenerLen
// bytes, by a node whose id is nodeIDLen bytes (SPEC §3.9).
//
// It exists for the destinations whose per-message ceiling sits far below the
// frame ceiling -- max_allowed_packet on MySQL, max_payload on NATS. They
// compare it with their own limit at boot, so a message the transport already
// confirmed is never refused later for its size. Refused would be ClassLogic,
// and logic drops the whole batch, not just the message that did not fit.
//
// The bound rests on two properties of the parse and of the serialiser. Every
// text field of the envelope -- hostname, app-name, procid, msgid, and every
// SD-ID, name and value -- is a disjoint span of the raw line, and an unescaped
// SD value is never longer than the span it came from. Escaping turns one byte
// into at most six. The JSON syntax around structured data is paid for by the
// raw bytes that delimit it: `[x]` is three bytes and renders as `"x":{},` in
// seven, and ` n=""` is five bytes that render as `"n":"",` in seven -- both far
// under six to one. What is left is envelopeFixedBytes.
//
// It is pessimistic on purpose. The MSG, which is most of a real line, never
// enters the envelope, so a real one is a fraction of this. But a realistic
// factor would let one broken emitter, sending a hostname of control bytes,
// take down batches of other emitters' confirmed messages.
func MaxBytes(rawLen, listenerLen, nodeIDLen int64) int64 {
	return envelopeFixedBytes + maxEscape*(rawLen+listenerLen+nodeIDLen)
}

// Meta is the set of envelope fields that a sink with a schema promotes to
// queryable columns (SPEC §3.5, §3.6).
//
// It is built from the Item and the parse result together, in the worker, right
// before the write. The strings alias the raw line, so a Meta is valid only as
// long as the item that produced it.
type Meta struct {
	TS        int64
	NTS       int64
	Facility  int
	Severity  int
	Hostname  string
	AppName   string
	ProcID    string
	MsgID     string
	PeerIP    string
	Transport string
	RFC       string
	MsgOffset int
	MsgSize   int
}

// BuildMeta extracts the promoted columns.
//
// The strings here are copies made by the []byte conversion, which is the one
// place this package allocates per message on purpose: a column value must
// outlive nothing, but it does travel into a driver that may retain it past the
// point where the raw buffer returns to the pool.
func BuildMeta(it *Item, p *syslog.Parsed) Meta {
	h := &it.Head
	m := Meta{
		TS:        h.TS,
		NTS:       h.NTS,
		Facility:  h.Facility(),
		Severity:  h.Severity(),
		Hostname:  string(p.Hostname),
		AppName:   string(p.AppName),
		ProcID:    string(p.ProcID),
		MsgID:     string(p.MsgID),
		Transport: h.Transport.String(),
		RFC:       p.RFC.String(),
		MsgOffset: p.MsgOffset,
		MsgSize:   p.MsgSize,
	}
	if h.PeerAddr.IsValid() {
		m.PeerIP = h.PeerAddr.Unmap().String()
	}
	return m
}
