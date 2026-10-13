// Package convert turns one syslog message into one JLH envelope of
// type=syslog (SPEC §6).
//
// It implements the conversion algorithm of the skill
// loghub-type-syslog-schema, with the two deviations of SPEC §1.3 (D2 lives
// here: the strict TAG without a timestamp, §6.5.5). Its conformance criterion
// is the skill's golden vectors, compared field by field (SPEC §18.2), and its
// fuzz property is that EVERY output passes both JSON schemas (SPEC §18.3).
//
// The absolute rule: the conversion never fails. The worst outcome is
// format "malformed", with the whole message in message, raw present and the
// reason in parse.error. An unreadable message is exactly the one an operator
// most needs to see.
//
// A Converter is a pure function of (head, raw line, Config): it never reads
// the clock, the network or the identity library. That is what makes the
// envelope reproducible for a retry, a reingested dump or a claimed ring entry
// (SPEC §3.5).
package convert

import (
	"bytes"
	"encoding/base64"
	"hash/fnv"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlh"
)

// Config is the conversion configuration: the [syslog] section of SPEC §16.5.
// It is frozen at boot (SPEC §16.3); a change between the item being written
// and being drained would give the same uuid different bytes.
type Config struct {
	// DefaultPriority is the PRI of pri-missing and pri-invalid (13,
	// user.notice, RFC 3164 §4.3.3).
	DefaultPriority int
	// Location is the zone of RFC 3164 timestamps that carry none. nil is UTC.
	Location *time.Location
	// IncludeRaw writes raw when it is not mandatory.
	IncludeRaw bool
	// Charset decodes a message that is not UTF-8 (SPEC §6.7).
	Charset Charset
	// MaxMessageBytes truncates. The receivers already cut there; applying it
	// again keeps a reingested item from an older run inside the limit.
	MaxMessageBytes int
	// SDMaxElements and SDMaxParams bound the STRUCTURED-DATA (SPEC §6.11).
	SDMaxElements int
	SDMaxParams   int
	// XTransport, when set, is written as the payload extension
	// "x-transport". This server always sets "relp" (decision 2, SPEC §3.2):
	// payload.transport says tcp, as the skill's enum allows, and the
	// extension says the bytes came in RELP frames, acknowledged one by one.
	// The skill's golden vectors run without it. It is constant for the life
	// of the process, so it keeps the JLH a pure function of head + raw +
	// config (SPEC §3.5).
	XTransport string
}

// DefaultConfig is the skill's default behaviour.
func DefaultConfig() Config {
	return Config{
		DefaultPriority: 13,
		Location:        time.UTC,
		IncludeRaw:      true,
		MaxMessageBytes: 64 << 10,
		SDMaxElements:   32,
		SDMaxParams:     64,
	}
}

// Result summarises one conversion for the sinks, the metrics and the
// accept_malformed policy.
type Result struct {
	Format   Format
	Priority int
	SID      uint64
	Warnings Warnings
}

// Facility and Severity decompose the priority.
func (r *Result) Facility() int { return r.Priority / 8 }
func (r *Result) Severity() int { return r.Priority % 8 }

type field struct {
	b       []byte
	present bool
}

// parsed is one message after parsing and before emission.
type parsed struct {
	format     Format
	pri        int
	version    int
	hasVersion bool
	ts         tsValue
	hasTS      bool
	hostname   field
	appname    field
	procid     field
	msgid      field
	hasSD      bool
	body       []byte
	bomAllowed bool
	warn       Warnings
	errText    string
	forceRaw   bool
}

// Converter holds the scratch of one drain worker. It is NOT safe for
// concurrent use: each worker, and the emergency dump, has its own.
type Converter struct {
	cfg Config
	ar  arena
	sd  sdState
	p   parsed
}

// New builds a converter.
func New(cfg Config) *Converter {
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	return &Converter{cfg: cfg}
}

// Config returns the converter's configuration.
func (c *Converter) Config() Config { return c.cfg }

var appendJSONBytes = jlh.AppendString

// Append converts one message and appends its whole JLH envelope to dst.
func (c *Converter) Append(dst []byte, h *item.Head, raw []byte) ([]byte, Result) {
	c.ar.reset()
	c.p = parsed{}
	p := &c.p

	size := h.Size
	if size < len(raw) {
		size = len(raw)
	}
	if c.cfg.MaxMessageBytes > 0 && len(raw) > c.cfg.MaxMessageBytes {
		raw = raw[:c.cfg.MaxMessageBytes]
	}
	truncated := size > len(raw)
	if truncated {
		p.warn.Add(WarnTruncated)
	}

	pri, st, n := ScanPRI(raw)
	switch st {
	case PRIInvalid:
		// RFC 3164 §4.3.3: the whole message, <...> included, is CONTENT.
		p.warn.Add(WarnPRIInvalid)
		p.errText = errPRIInvalid
		p.format = FormatMalformed
		p.body = raw
	case PRIMissing:
		p.warn.Add(WarnPRIMissing)
		if !c.parse3164(raw, false, h) {
			p.errText = errNoHeader
			p.format = FormatMalformed
			p.body = raw
		}
	default:
		rest := raw[n:]
		if !c.parse5424(rest) {
			c.parse3164(rest, true, h)
		}
	}
	if st == PRIOk {
		p.pri = pri
	} else {
		p.pri = c.cfg.DefaultPriority
	}

	body := trimEOL(p.body)
	if p.bomAllowed && bytes.HasPrefix(body, bom) {
		body = body[len(bom):]
	}
	if truncated {
		body = body[:len(body)-incompleteTail(body)]
	}
	message := body
	if !utf8.Valid(body) {
		if c.cfg.Charset != CharsetNone {
			p.warn.Add(WarnEncodingLegacy)
			message = c.ar.decodeLegacy(body, c.cfg.Charset)
		} else {
			p.warn.Add(WarnEncodingInvalid)
			message, _ = c.ar.decode(body)
		}
	}

	res := Result{Format: p.format, Priority: p.pri, Warnings: p.warn}
	res.SID = SID(h)

	dst = jlh.AppendHeader(dst, &jlh.Header{
		UUID:  h.UUID,
		From:  h.From,
		Space: h.Space,
		Type:  "syslog",
		Level: uint8(p.pri % 8),
		SID:   res.SID,
		Step:  0,
		Event: jlh.Event{UTS: h.UTS, Sub: h.Sub},
	})
	dst = c.appendPayload(dst, h, raw, message, size)
	dst = jlh.Close(dst)
	return dst, res
}

// appendPayload writes the payload object in the key order of SPEC §3.2.
func (c *Converter) appendPayload(dst []byte, h *item.Head, raw, message []byte, size int) []byte {
	p := &c.p
	dst = append(dst, '{')

	network := h.Transport.Network()
	if p.hostname.present || network {
		dst = append(dst, `"host":{`...)
		comma := false
		if p.hostname.present {
			dst = append(dst, `"hostname":`...)
			dst = appendJSONBytes(dst, p.hostname.b)
			comma = true
		}
		if network {
			if comma {
				dst = append(dst, ',')
			}
			dst = append(dst, `"src-ip":"`...)
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
		}
		dst = append(dst, `},`...)
	}

	dst = append(dst, `"transport":"`...)
	dst = append(dst, h.Transport.String()...)
	dst = append(dst, `","framing":"`...)
	dst = append(dst, h.Framing.String()...)
	dst = append(dst, '"')

	dst = append(dst, `,"format":"`...)
	dst = append(dst, p.format.String()...)
	dst = append(dst, '"')
	if p.hasVersion {
		dst = append(dst, `,"version":`...)
		dst = strconv.AppendInt(dst, int64(p.version), 10)
	}
	fac, sev := p.pri/8, p.pri%8
	dst = append(dst, `,"priority":`...)
	dst = strconv.AppendInt(dst, int64(p.pri), 10)
	dst = append(dst, `,"facility":{"code":`...)
	dst = strconv.AppendInt(dst, int64(fac), 10)
	dst = append(dst, `,"name":"`...)
	dst = append(dst, FacilityNames[fac]...)
	dst = append(dst, `"},"severity":{"code":`...)
	dst = strconv.AppendInt(dst, int64(sev), 10)
	dst = append(dst, `,"name":"`...)
	dst = append(dst, SeverityNames[sev]...)
	dst = append(dst, `"}`...)

	if p.hasTS {
		dst = append(dst, `,"timestamp":"`...)
		dst = appendTimestamp(dst, p.ts)
		dst = append(dst, '"')
	}
	if p.appname.present {
		dst = append(dst, `,"appname":`...)
		dst = appendJSONBytes(dst, p.appname.b)
	}
	if p.procid.present {
		dst = append(dst, `,"procid":`...)
		dst = appendJSONBytes(dst, p.procid.b)
	}
	if p.msgid.present {
		dst = append(dst, `,"msgid":`...)
		dst = appendJSONBytes(dst, p.msgid.b)
	}
	if p.hasSD {
		dst = append(dst, `,"structured-data":`...)
		dst = c.appendSD(dst)
	}
	dst = append(dst, `,"message":`...)
	dst = appendJSONBytes(dst, message)
	dst = append(dst, `,"size":`...)
	dst = strconv.AppendInt(dst, int64(size), 10)

	needRaw := p.format == FormatMalformed || p.forceRaw ||
		p.warn.Has(WarnEncodingInvalid) || p.warn.Has(WarnEncodingLegacy)
	if c.cfg.IncludeRaw || needRaw {
		dst = append(dst, `,"raw":`...)
		if utf8.Valid(raw) {
			dst = appendJSONBytes(dst, raw)
		} else {
			dst = append(dst, '"')
			dst = base64.StdEncoding.AppendEncode(dst, raw)
			dst = append(dst, `","raw-encoding":"base64"`...)
		}
	}

	if !p.warn.Empty() || p.errText != "" {
		dst = append(dst, `,"parse":{`...)
		if !p.warn.Empty() {
			dst = append(dst, `"warnings":[`...)
			first := true
			for w := Warning(0); w < NumWarnings; w++ {
				if !p.warn.Has(w) {
					continue
				}
				if !first {
					dst = append(dst, ',')
				}
				first = false
				dst = append(dst, '"')
				dst = append(dst, warningNames[w]...)
				dst = append(dst, '"')
			}
			dst = append(dst, ']')
		}
		if p.errText != "" {
			if !p.warn.Empty() {
				dst = append(dst, ',')
			}
			dst = append(dst, `"error":`...)
			dst = jlh.AppendStr(dst, p.errText)
		}
		dst = append(dst, '}')
	}

	if h.Proxied() && network {
		dst = append(dst, `,"x-proxy":{"peer-ip":"`...)
		dst = h.PeerIP.AppendTo(dst)
		dst = append(dst, `","peer-port":`...)
		dst = strconv.AppendUint(dst, uint64(h.PeerPort), 10)
		dst = append(dst, '}')
	}
	if c.cfg.XTransport != "" {
		dst = append(dst, `,"x-transport":`...)
		dst = jlh.AppendStr(dst, c.cfg.XTransport)
	}
	return append(dst, '}')
}

// SID is the envelope sid (SPEC §6.10): FNV-1a 64 of
// "transport|src-ip|src-port|dst-ip|dst-port" reduced to 53 bits, 0 on unix.
// Any consumer can recompute it from payload.host and payload.transport.
func SID(h *item.Head) uint64 {
	if !h.Transport.Network() {
		return 0
	}
	var buf [128]byte
	b := append(buf[:0], h.Transport.String()...)
	b = append(b, '|')
	b = h.SrcIP.AppendTo(b)
	b = append(b, '|')
	b = strconv.AppendUint(b, uint64(h.SrcPort), 10)
	b = append(b, '|')
	if h.DstIP.IsValid() {
		b = h.DstIP.AppendTo(b)
	}
	b = append(b, '|')
	b = strconv.AppendUint(b, uint64(h.DstPort), 10)
	f := fnv.New64a()
	f.Write(b)
	return f.Sum64() & jlh.MaxSID
}

// ------------------------------------------------------------------ RFC 5424

// parse5424 tries the RFC 5424 header on rest (the bytes after a valid PRI).
// It reports false when rest does not start with VERSION SP followed by "-" or
// a date, which is the reference's _V5424_RE: that requirement is what keeps
// "<13>1 coisa" from being read as an IETF header (SPEC §6.3).
func (c *Converter) parse5424(rest []byte) bool {
	p := &c.p
	i := 0
	for i < len(rest) && i < 3 && isDigit(rest[i]) {
		i++
	}
	if i == 0 || rest[0] == '0' || i >= len(rest) || rest[i] != ' ' {
		return false
	}
	after := rest[i+1:]
	nil1 := len(after) >= 1 && after[0] == '-' && (len(after) == 1 || after[1] == ' ')
	date := len(after) >= 11 && isDigit(after[0]) && isDigit(after[1]) && isDigit(after[2]) &&
		isDigit(after[3]) && after[4] == '-' && isDigit(after[5]) && isDigit(after[6]) &&
		after[7] == '-' && isDigit(after[8]) && isDigit(after[9]) && (after[10] == 'T' || after[10] == 't')
	if !nil1 && !date {
		return false
	}
	v, _ := strconv.Atoi(string(rest[:i]))
	p.format = FormatRFC5424
	p.version = v
	p.hasVersion = true
	if v != 1 {
		p.warn.Add(WarnVersionUnsupported)
	}

	pos := i + 1
	var tokens [5][]byte
	nt := 0
	for nt < 5 {
		if pos >= len(rest) {
			break
		}
		sp := bytes.IndexByte(rest[pos:], ' ')
		if sp < 0 {
			tokens[nt] = rest[pos:]
			pos = len(rest)
		} else {
			tokens[nt] = rest[pos : pos+sp]
			pos += sp + 1
		}
		nt++
	}
	if nt < 5 || pos >= len(rest) {
		p.warn.Add(WarnHeaderInvalid)
	}
	for k := nt; k < 5; k++ {
		tokens[k] = dash
	}

	if ts := tokens[0]; !bytes.Equal(ts, dash) {
		if t, lax, ok := parseTS5424(ts); ok {
			p.ts, p.hasTS = t, true
			if lax {
				p.warn.Add(WarnHeaderInvalid)
			}
		} else {
			p.warn.Add(WarnTimestampInvalid)
		}
	}
	p.hostname = c.headerToken(tokens[1], 255)
	p.appname = c.headerToken(tokens[2], 48)
	p.procid = c.headerToken(tokens[3], 128)
	p.msgid = c.headerToken(tokens[4], 32)

	p.bomAllowed = true
	if pos >= len(rest) {
		return true
	}
	switch rest[pos] {
	case '-':
		if pos+1 == len(rest) || rest[pos+1] == ' ' {
			if pos+2 <= len(rest) {
				p.body = rest[min(pos+2, len(rest)):]
			}
			return true
		}
	case '[':
		var local Warnings
		end, ok := c.parseSD(rest, pos, &p.warn, &local)
		if ok {
			switch {
			case end == len(rest):
				p.body = nil
			case rest[end] == ' ':
				p.body = rest[end+1:]
			default:
				ok = false // text glued to the last ']'
			}
		}
		if ok {
			p.hasSD = true
			p.warn.Merge(local)
			return true
		}
	}
	// SPEC §6.4.3: anything that is not "-" or a well-formed STRUCTURED-DATA
	// is sd-invalid, and the whole text from where the SD should start goes
	// to message. Nothing is lost.
	c.sd.reset()
	p.warn.Add(WarnSDInvalid)
	p.body = rest[pos:]
	return true
}

var dash = []byte{'-'}

// headerToken applies the RFC 5424 header field rules (SPEC §6.4.1).
func (c *Converter) headerToken(tok []byte, max int) field {
	p := &c.p
	if bytes.Equal(tok, dash) {
		return field{}
	}
	if len(tok) == 0 {
		p.warn.Add(WarnHeaderInvalid)
		return field{}
	}
	if len(tok) > max || !printableASCII(tok) {
		p.warn.Add(WarnHeaderInvalid)
	}
	out, valid := c.ar.decode(tok)
	if !valid {
		// Flagged even when the value is dropped below: the octets are not
		// UTF-8 whatever happens to the field (skill §10).
		p.warn.Add(WarnEncodingInvalid)
	}
	if hasJSWhitespace(out) {
		// The payload schema declares these fields ^\S+$. The value cannot be
		// emitted, so it is omitted, flagged, and raw is forced so that it is
		// still in the envelope (SPEC §6.4.1).
		p.warn.Add(WarnHeaderInvalid)
		p.forceRaw = true
		return field{}
	}
	return field{b: out, present: true}
}

func printableASCII(b []byte) bool {
	for _, c := range b {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------ RFC 3164

// parse3164 reads the tolerant RFC 3164 form (SPEC §6.5). It reports false only
// when the message has no PRI and no recognisable timestamp: that is malformed.
func (c *Converter) parse3164(rest []byte, priOK bool, h *item.Head) bool {
	p := &c.p
	var tsw Warnings
	t, kind, pos := parseTS3164(rest, c.cfg.Location, h.UTS, h.Sub, &tsw)
	if kind == tsNone {
		if !priOK {
			return false
		}
		// RFC 3164 §4.3.2: no timestamp, no HOSTNAME. The skill stops here and
		// sends everything to message; this server still takes a STRICT tag,
		// deviation D2 (SPEC §6.5.5).
		p.format = FormatRFC3164
		p.warn.Add(WarnTimestampMissing)
		r := rest
		if end, ok := c.tag(r, true); ok {
			r = r[end:]
		}
		p.body = r
		return true
	}
	p.format = FormatRFC3164
	p.warn.Merge(tsw)
	if kind == tsFound {
		p.ts, p.hasTS = t, true
	}
	r := rest[pos:]

	// SPEC §6.5.3: the token after the timestamp is the HOSTNAME if and only if
	// it is followed by a space, does not end with ':' and has no '['. A token
	// the schema could not carry (Unicode whitespace inside) is not a hostname
	// either, and stays in the TAG/CONTENT.
	sp := bytes.IndexByte(r, ' ')
	hostOK := false
	if sp > 0 {
		tok := r[:sp]
		if tok[len(tok)-1] != ':' && bytes.IndexByte(tok, '[') < 0 {
			out, valid := c.ar.decode(tok)
			if !hasJSWhitespace(out) {
				if !valid {
					p.warn.Add(WarnEncodingInvalid)
				}
				p.hostname = field{b: out, present: true}
				r = r[sp+1:]
				hostOK = true
			}
		}
	}
	if !hostOK {
		p.warn.Add(WarnHostnameMissing)
	}
	if end, ok := c.tag(r, false); ok {
		r = r[end:]
	}
	p.body = r
	return true
}

// asciiSpace is Python's bytes \s: space, \t, \n, \v, \f, \r.
func asciiSpace(c byte) bool { return c == ' ' || c >= '\t' && c <= '\r' }

// tag reads `TAG[pid]: ` at the start of r (SPEC §6.5.4) and fills appname and
// procid. strict adds the first-character rule of SPEC §6.5.5. It returns where
// message starts.
//
//	TAG = 1*48 octets except whitespace, "[", "]" and ":"
//	      [ "[" 1*128 octets except whitespace and "]" "]" ]
//	      ":" followed by SP or the end
func (c *Converter) tag(r []byte, strict bool) (int, bool) {
	p := &c.p
	i := 0
	for i < len(r) && i < 48 {
		ch := r[i]
		if asciiSpace(ch) || ch == '[' || ch == ']' || ch == ':' {
			break
		}
		i++
	}
	if i == 0 || i >= len(r) {
		return 0, false
	}
	if strict {
		ch := r[0]
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_' || ch == '/') {
			return 0, false
		}
	}
	name := r[:i]
	var pid []byte
	j := i
	if r[j] == '[' {
		k := j + 1
		for k < len(r) && k-(j+1) < 128 && r[k] != ']' && !asciiSpace(r[k]) {
			k++
		}
		if k == j+1 || k >= len(r) || r[k] != ']' {
			return 0, false
		}
		pid = r[j+1 : k]
		j = k + 1
	}
	if j >= len(r) || r[j] != ':' {
		return 0, false
	}
	j++
	if j < len(r) {
		if r[j] != ' ' {
			return 0, false
		}
		j++
	}

	appname, okA := c.ar.decode(name)
	if hasJSWhitespace(appname) {
		return 0, false
	}
	var procid []byte
	okP := true
	if pid != nil {
		procid, okP = c.ar.decode(pid)
		if hasJSWhitespace(procid) {
			return 0, false
		}
	}
	if !okA || !okP {
		p.warn.Add(WarnEncodingInvalid)
	}
	p.appname = field{b: appname, present: true}
	if pid != nil {
		p.procid = field{b: procid, present: true}
	}
	return j, true
}
