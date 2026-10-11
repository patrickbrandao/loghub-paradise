package syslog

import "time"

// RFC classifies which grammar the message turned out to match.
type RFC uint8

const (
	RFCUnknown RFC = iota
	RFC3164
	RFC5424
)

func (r RFC) String() string {
	switch r {
	case RFC3164:
		return "3164"
	case RFC5424:
		return "5424"
	}
	return "unknown"
}

// TSSource says where MsgTS came from, which is the only way a consumer can
// know how much to trust it: an RFC 3164 timestamp has no year and no zone,
// both of which this package had to invent (SPEC §6.4).
type TSSource uint8

const (
	TSNone TSSource = iota
	TS3164
	TS5424
)

func (t TSSource) String() string {
	switch t {
	case TS3164:
		return "rfc3164"
	case TS5424:
		return "rfc5424"
	}
	return "none"
}

// Anomaly is a parse oddity worth a counter but never worth a rejection. They
// are exported as syslog_parse_anomalies_total with a fixed label set.
type Anomaly uint8

const (
	AnomDupSDID Anomaly = iota
	AnomSDTruncated
	AnomSDUnterminated
	AnomBadTimestamp
	numAnomalies
)

func (a Anomaly) String() string {
	switch a {
	case AnomDupSDID:
		return "dup_sd_id"
	case AnomSDTruncated:
		return "sd_truncated"
	case AnomSDUnterminated:
		return "sd_unterminated"
	case AnomBadTimestamp:
		return "bad_timestamp"
	}
	return "unknown"
}

// SDParam is one structured-data parameter.
//
// Value aliases the raw line when it carries no escape, and points into the
// Parsed scratch buffer when it does. Either way it is valid only while the
// caller still owns both.
type SDParam struct {
	Name  []byte
	Value []byte
}

// SDElement is one bracketed structured-data element.
type SDElement struct {
	ID     []byte
	Params []SDParam
}

// Options are the knobs of SPEC §15.5 [parse], plus the reception instant that
// the RFC 3164 year rule needs.
type Options struct {
	Prefer          RFC // RFCUnknown means auto
	SDMaxElements   int
	SDMaxParams     int
	DefaultFacility int
	DefaultSeverity int
}

// Parsed is the full result. Every slice field aliases the raw line, so it is
// valid exactly as long as the caller keeps that line alive.
//
// It is designed to be REUSED: a worker keeps one Parsed and calls Parse on
// each item of its batch, which keeps the SD slices and the unescape scratch
// allocated across a whole drain loop instead of once per message.
type Parsed struct {
	RFC      RFC
	Version  int
	MsgTS    time.Time
	MsgTSSrc TSSource

	Hostname []byte
	AppName  []byte
	ProcID   []byte
	MsgID    []byte

	SD []SDElement

	// MsgOffset and MsgSize locate MSG inside the raw line. The message text
	// is deliberately NOT copied anywhere: the envelope carries these two
	// numbers and the consumer slices (SPEC §3.1).
	MsgOffset int
	MsgSize   int

	// Anomalies counts oddities by kind, for the metric. It is reset by Parse.
	Anomalies [numAnomalies]int

	// scratch holds unescaped structured-data values. It is cleared, not
	// freed, between messages; the []byte handed out in SDParam.Value points
	// into it, which is why it must never be reallocated while a Parsed is
	// still being read.
	scratch []byte
}

// Reset clears the result without releasing the capacity that makes reuse
// worthwhile.
func (p *Parsed) Reset() {
	p.RFC = RFCUnknown
	p.Version = 0
	p.MsgTS = time.Time{}
	p.MsgTSSrc = TSNone
	p.Hostname = nil
	p.AppName = nil
	p.ProcID = nil
	p.MsgID = nil
	for i := range p.SD {
		p.SD[i].ID = nil
		p.SD[i].Params = p.SD[i].Params[:0]
	}
	p.SD = p.SD[:0]
	p.MsgOffset = 0
	p.MsgSize = 0
	p.Anomalies = [numAnomalies]int{}
	p.scratch = p.scratch[:0]
}

// Parse fills p from raw. hdr is the offset just past the PRI, as returned by
// PRI; pass 0 when there was none. now is the reception instant, used only to
// supply the year an RFC 3164 timestamp does not carry.
//
// It never fails and never panics. The guarantee the fuzz target asserts is:
//
//	0 <= p.MsgOffset && p.MsgOffset+p.MsgSize == len(raw)
//
// MSG always runs to the end of the line, because syslog has no trailer inside
// the message: whatever is left after the header is the message, including
// bytes that look like more header.
func Parse(p *Parsed, raw []byte, hdr int, now time.Time, opt Options) {
	p.Reset()

	if hdr < 0 || hdr > len(raw) {
		hdr = 0
	}
	// Default position: everything after the PRI is the message. Every parse
	// step that succeeds moves this forward; every step that fails leaves it
	// where the previous one put it. That is what makes a half-understood
	// message degrade instead of failing.
	p.setMsg(raw, hdr)

	rest := raw[hdr:]
	if len(rest) == 0 {
		return
	}

	switch opt.Prefer {
	case RFC5424:
		if parse5424(p, raw, hdr, opt) {
			p.RFC = RFC5424
		}
		return
	case RFC3164:
		if parse3164(p, raw, hdr, now) {
			p.RFC = RFC3164
		}
		return
	}

	// Auto (SPEC §6.2). A digit followed by a space is the RFC 5424 VERSION;
	// RFC 3164 never has a digit there, it has a month name. Checking the
	// space too matters: "1" alone, or "12abc", is not a version, and trying
	// 5424 on it would consume the line and produce six empty fields where
	// 3164 might still have found a timestamp.
	if len(rest) >= 2 && rest[0] >= '1' && rest[0] <= '9' && rest[1] == ' ' {
		if parse5424(p, raw, hdr, opt) {
			p.RFC = RFC5424
			return
		}
	}
	if parse3164(p, raw, hdr, now) {
		p.RFC = RFC3164
		return
	}
	// RFCUnknown, with MsgOffset still at hdr: the whole line after the PRI is
	// the message. It gets stored, counted and delivered like any other (SPEC
	// §6.6) -- refusing it would lose the log of the misconfigured emitter at
	// precisely the moment someone is investigating why it is misconfigured.
}

// setMsg points MSG at everything from off to the end of the line.
func (p *Parsed) setMsg(raw []byte, off int) {
	if off < 0 {
		off = 0
	}
	if off > len(raw) {
		off = len(raw)
	}
	p.MsgOffset = off
	p.MsgSize = len(raw) - off
}

// nilIfDash converts the RFC 5424 NILVALUE into an empty field. A literal "-"
// means absent, and a consumer should never have to know that.
func nilIfDash(b []byte) []byte {
	if len(b) == 1 && b[0] == '-' {
		return nil
	}
	return b
}

// truncate applies an RFC field limit as truncation, never as rejection. The
// limits exist to bound what a receiver must store; a sender that exceeds one
// is out of spec, but losing the whole message over a 49-byte APP-NAME would
// be a worse outcome than keeping 48 of it.
func truncate(b []byte, max int) []byte {
	if len(b) > max {
		return b[:max]
	}
	return b
}
