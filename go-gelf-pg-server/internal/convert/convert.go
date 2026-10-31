// Package convert turns one GELF document into one JLH envelope of type=gelf
// (SPEC §6).
//
// The absolute rule: the conversion never fails. The worst outcome is format
// "malformed", with short-message "", raw present and the reason in
// parse.error. An unreadable document is exactly the one an operator most needs
// to see.
//
// A Converter is a pure function of (head, document, Config): it never reads
// the clock, the network or the identity library. That is what makes the
// envelope reproducible for a retry, a reingested dump or a claimed ring entry
// (SPEC §3.5).
//
// This package is copied verbatim to go-gelf-pg-server and go-gelf-nats-server
// (DESIGN §1): it imports only item and jlh, which they carry too.
package convert

import (
	"bytes"
	"encoding/base64"
	"hash/fnv"
	"strconv"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/jlh"
)

// KeepRaw is convert.keep_raw (decision 4, SPEC §6.9).
type KeepRaw uint8

const (
	// KeepRawMalformed writes raw only when it is needed: a malformed
	// document, or a lossy warning.
	KeepRawMalformed KeepRaw = iota
	// KeepRawAlways writes raw on every envelope.
	KeepRawAlways
)

// ParseKeepRaw reads the convert.keep_raw value.
func ParseKeepRaw(s string) (KeepRaw, bool) {
	switch s {
	case "malformed":
		return KeepRawMalformed, true
	case "always":
		return KeepRawAlways, true
	}
	return 0, false
}

// Config is the conversion configuration: the [convert] section of SPEC §16.5.
// It is frozen at boot (SPEC §16.3).
type Config struct {
	KeepRaw KeepRaw
	// MaxMessageBytes is convert.max_message_bytes. The receivers refuse
	// anything larger; the converter does not cut (cutting JSON makes it
	// invalid), so it is here only for the record and the worst-case maths.
	MaxMessageBytes int
	// Columns fills Result's column fields, which only the SQL sinks need.
	Columns bool
}

// DefaultMaxMessageBytes is the default of convert.max_message_bytes (SPEC
// §3.9: the largest round value whose worst envelope fits the NATS default
// max_payload of 1 MiB with room to spare).
const DefaultMaxMessageBytes = 128 << 10

// DefaultConfig is the default behaviour.
func DefaultConfig() Config {
	return Config{KeepRaw: KeepRawMalformed, MaxMessageBytes: DefaultMaxMessageBytes}
}

// WorstJLH is the largest envelope a document of at most maxMessage bytes can
// produce (SPEC §3.9): a malformed document of control bytes, every one
// escaped to \u00XX, plus the fixed part.
func WorstJLH(maxMessage int) int { return 6*maxMessage + 8<<10 }

// NullString is a column value that may be absent (SPEC §3.7: NULL is "did not
// come", which is different from "").
type NullString struct {
	S     string
	Valid bool
}

// Result summarises one conversion for the sinks, the metrics and the
// accept_malformed policy.
type Result struct {
	Format   Format
	Level    int
	SID      uint64
	Warnings Warnings

	// Columns, filled only with Config.Columns. Decoded text, never
	// JSON-escaped.
	Hostname, SrcIP NullString
}

// Standard keys of GELF 1.1 (and the two of 1.0), by their index in std.
const (
	stdVersion = iota
	stdHost
	stdShortMessage
	stdFullMessage
	stdTimestamp
	stdLevel
	stdFacility
	stdFile
	stdLine
	numStd
)

var stdKeys = [numStd]string{"version", "host", "short_message", "full_message",
	"timestamp", "level", "facility", "file", "line"}

func stdIndex(key []byte) int {
	for i, k := range stdKeys {
		if string(key) == k {
			return i
		}
	}
	return -1
}

// entry is one member of fields or extra after its key was decoded: the
// output name is keys[start:end] (decoded, without the leading _ for fields).
// Offsets rather than slices, because keys may move while it grows.
type entry struct {
	start, end int
	m          *member
}

// Converter holds the scratch of one drain worker. It is NOT safe for
// concurrent use; ring=none reaches it through a pool.
type Converter struct {
	cfg Config
	sc  scanner

	keys    []byte // decoded keys of the document, entries point into it
	fields  []entry
	extra   []entry
	std     [numStd]*member
	seen    map[string]int
	scratch []byte
	warn    Warnings
}

// New builds a converter.
func New(cfg Config) *Converter { return &Converter{cfg: cfg, seen: map[string]int{}} }

// Config returns the converter's configuration.
func (c *Converter) Config() Config { return c.cfg }

// LevelOf is the level the receiver filters on (SPEC §7): exactly the level
// the converter will write, by the same validation and the same rules.
func (c *Converter) LevelOf(doc []byte) int {
	if c.sc.scan(doc) != docOK {
		return DefaultLevel
	}
	var lv *member
	for i := range c.sc.members {
		m := &c.sc.members[i]
		if keyIs(m.key, "level") {
			lv = m
		}
	}
	level, _ := levelOf(lv)
	return level
}

// keyIs compares a raw key token with a constant, decoding only when the token
// has an escape (a key written as "level" is level, SPEC §6.3).
func keyIs(tok []byte, k string) bool {
	in := tok[1 : len(tok)-1]
	if bytes.IndexByte(in, '\\') < 0 {
		return string(in) == k
	}
	var buf [64]byte
	d, _ := appendDecoded(buf[:0], tok)
	return string(d) == k
}

// Append converts one document and appends its whole JLH envelope to dst.
//
// raw must not be empty (SPEC §6.1): "the conversion never fails" holds for
// every document of at least one byte, and no envelope can carry zero bytes
// (the schema's size >= 1, and a malformed one needs raw). The receivers never
// queue an empty document (§5.6) and the chunk builder refuses one before
// calling this.
func (c *Converter) Append(dst []byte, h *item.Head, raw []byte) ([]byte, Result) {
	c.warn = 0
	c.keys = c.keys[:0]
	c.fields = c.fields[:0]
	c.extra = c.extra[:0]
	c.std = [numStd]*member{}

	derr := c.sc.scan(raw)
	format := FormatGELF
	if derr != docOK {
		format = FormatMalformed
	} else {
		c.classify()
	}

	level := DefaultLevel
	if format == FormatGELF {
		var w Warnings
		level, w = levelOf(c.std[stdLevel])
		c.warn |= w
	}

	res := Result{Format: format, Level: level, SID: SID(h)}
	dst = jlh.AppendHeader(dst, &jlh.Header{
		UUID:  h.UUID,
		From:  h.From,
		Space: h.Space,
		Type:  "gelf",
		Level: uint8(level),
		SID:   res.SID,
		Step:  0,
		Event: jlh.Event{UTS: h.UTS, Sub: h.Sub},
	})
	var hostname []byte
	dst, hostname = c.appendPayload(dst, h, raw, format, derr, level)
	dst = jlh.Close(dst)
	res.Warnings = c.warn

	if c.cfg.Columns {
		if hostname != nil {
			res.Hostname = NullString{S: string(hostname), Valid: true}
		}
		if h.SrcIP.IsValid() {
			res.SrcIP = NullString{S: h.SrcIP.String(), Valid: true}
		}
	}
	return dst, res
}

// classify sorts the top-level members into the standard keys, fields and
// extra, with the last-one-wins rule of SPEC §6.4.
func (c *Converter) classify() {
	members := c.sc.members
	for i := range members {
		m := &members[i]
		mark := len(c.keys)
		var rep bool
		c.keys, rep = appendDecoded(c.keys, m.key)
		if rep {
			c.warn.Add(WarnEncodingReplaced)
		}
		key := c.keys[mark:]
		if k := stdIndex(key); k >= 0 {
			if c.std[k] != nil {
				c.warn.Add(WarnDuplicateKey)
			}
			c.std[k] = m
			c.keys = c.keys[:mark]
			continue
		}
		if len(key) > 0 && key[0] == '_' {
			c.fields = append(c.fields, entry{start: mark + 1, end: len(c.keys), m: m})
			continue
		}
		c.extra = append(c.extra, entry{start: mark, end: len(c.keys), m: m})
	}
	c.fields = c.dedupe(c.fields)
	c.extra = c.dedupe(c.extra)
}

func (c *Converter) name(e entry) []byte { return c.keys[e.start:e.end] }

// dedupe keeps the last occurrence of every name, in the order of those last
// occurrences, and flags duplicate-key.
func (c *Converter) dedupe(es []entry) []entry {
	if len(es) < 2 {
		return es
	}
	clear(c.seen)
	for i, e := range es {
		c.seen[string(c.name(e))] = i
	}
	if len(c.seen) == len(es) {
		return es
	}
	c.warn.Add(WarnDuplicateKey)
	out := es[:0]
	for i, e := range es {
		if c.seen[string(c.name(e))] == i {
			out = append(out, e)
		}
	}
	return out
}

// levelOf applies the level rules of SPEC §6.5 to the member (nil = absent).
func levelOf(m *member) (int, Warnings) {
	var w Warnings
	if m == nil {
		w.Add(WarnLevelDefault)
		return DefaultLevel, w
	}
	switch m.kind {
	case kindNumber:
		if d, ok := parseDecimal(m.val); ok {
			if n, ok := d.integer(7); ok {
				if !isPlainInt(m.val) || m.val[0] == '-' {
					w.Add(WarnLevelCoerced)
				}
				return int(n), w
			}
		}
	case kindString:
		var buf [16]byte
		if len(m.val) <= 14 {
			s, _ := appendDecoded(buf[:0], m.val)
			if len(s) == 1 && s[0] >= '0' && s[0] <= '7' {
				w.Add(WarnLevelCoerced)
				return int(s[0] - '0'), w
			}
		}
	}
	w.Add(WarnLevelInvalid)
	return DefaultLevel, w
}

// appendPayload writes the payload object in the key order of SPEC §3.2. It
// returns the decoded hostname (or nil) for the columns.
func (c *Converter) appendPayload(dst []byte, h *item.Head, raw []byte, format Format,
	derr docError, level int) ([]byte, []byte) {
	ok := format == FormatGELF

	// host: hostname first, decided before the object is opened.
	var hostname []byte
	if ok {
		hostname = c.hostname()
	}
	dst = append(dst, `{"host":{`...)
	if hostname != nil {
		dst = append(dst, `"hostname":`...)
		dst = jlh.AppendString(dst, hostname)
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
	dst = append(dst, `},"transport":"`...)
	dst = append(dst, h.Transport.String()...)
	dst = append(dst, '"')
	if h.Compression != item.CompressionNone {
		dst = append(dst, `,"compression":"`...)
		dst = append(dst, h.Compression.String()...)
		dst = append(dst, '"')
	}
	if h.Chunks > 0 {
		dst = append(dst, `,"chunks":`...)
		dst = strconv.AppendInt(dst, int64(h.Chunks), 10)
	}
	if h.Transport.Secure() && !h.TLS.Empty() {
		dst = append(dst, `,"tls":{`...)
		comma := false
		f := func(k, v string) {
			if v == "" {
				return
			}
			if comma {
				dst = append(dst, ',')
			}
			comma = true
			dst = jlh.AppendKey(dst, k)
			dst = jlh.AppendStr(dst, v)
		}
		f("version", h.TLS.Version)
		f("cipher", h.TLS.Cipher)
		f("peer-subject", h.TLS.PeerSubject)
		f("peer-fingerprint", h.TLS.PeerFingerprint)
		dst = append(dst, '}')
	}
	dst = append(dst, `,"format":"`...)
	dst = append(dst, format.String()...)
	dst = append(dst, '"')

	if ok {
		dst = c.appendVersion(dst)
	}
	dst = append(dst, `,"level":{"code":`...)
	dst = strconv.AppendInt(dst, int64(level), 10)
	dst = append(dst, `,"name":"`...)
	dst = append(dst, LevelNames[level]...)
	dst = append(dst, `"}`...)

	if ok {
		dst = c.appendFacility(dst)
		dst = c.appendTimestamp(dst)
		dst = c.appendShortMessage(dst)
		dst = c.appendFullMessage(dst)
		dst = c.appendFile(dst)
		dst = c.appendLine(dst)
		dst = c.appendFields(dst)
		dst = c.appendExtra(dst)
	} else {
		dst = append(dst, `,"short-message":""`...)
	}

	dst = append(dst, `,"size":`...)
	dst = strconv.AppendInt(dst, int64(len(raw)), 10)

	if !ok || c.warn.Lossy() || c.cfg.KeepRaw == KeepRawAlways {
		dst = append(dst, `,"raw":`...)
		if utf8.Valid(raw) {
			dst = jlh.AppendString(dst, raw)
		} else {
			dst = append(dst, '"')
			dst = base64.StdEncoding.AppendEncode(dst, raw)
			dst = append(dst, `","raw-encoding":"base64"`...)
		}
	}

	if !c.warn.Empty() || !ok {
		dst = append(dst, `,"parse":{`...)
		if !c.warn.Empty() {
			dst = append(dst, `"warnings":[`...)
			first := true
			for w := Warning(0); w < NumWarnings; w++ {
				if !c.warn.Has(w) {
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
		if !ok {
			if !c.warn.Empty() {
				dst = append(dst, ',')
			}
			dst = append(dst, `"error":`...)
			dst = jlh.AppendStr(dst, docErrorText[derr])
		}
		dst = append(dst, '}')
	}

	if h.Proxied() {
		dst = append(dst, `,"x-proxy":{"peer-ip":"`...)
		dst = h.PeerIP.AppendTo(dst)
		dst = append(dst, `","peer-port":`...)
		dst = strconv.AppendUint(dst, uint64(h.PeerPort), 10)
		dst = append(dst, '}')
	}
	return append(dst, '}'), hostname
}

// str decodes a string member into the scratch buffer.
func (c *Converter) str(m *member) []byte {
	var rep bool
	c.scratch, rep = appendDecoded(c.scratch[:0], m.val)
	if rep {
		c.warn.Add(WarnEncodingReplaced)
	}
	return c.scratch
}

// hostname applies the host rules (SPEC §6.5, D5). The result is a copy: the
// scratch is reused by every other field.
func (c *Converter) hostname() []byte {
	m := c.std[stdHost]
	if m == nil {
		c.warn.Add(WarnHostMissing)
		return nil
	}
	if m.kind == kindString {
		s := c.str(m)
		if len(s) > 0 && !hasJSWhitespace(s) {
			return append([]byte(nil), s...)
		}
	}
	c.warn.Add(WarnHostInvalid)
	return nil
}

func (c *Converter) appendVersion(dst []byte) []byte {
	m := c.std[stdVersion]
	if m == nil {
		c.warn.Add(WarnVersionMissing)
		return dst
	}
	if m.kind == kindString {
		if s := c.str(m); len(s) > 0 {
			if string(s) != "1.1" && string(s) != "1.0" {
				c.warn.Add(WarnVersionUnknown)
			}
			dst = append(dst, `,"gelf-version":`...)
			return jlh.AppendString(dst, s)
		}
	}
	c.warn.Add(WarnVersionInvalid)
	return dst
}

func (c *Converter) appendFacility(dst []byte) []byte {
	m := c.std[stdFacility]
	if m == nil {
		return dst
	}
	switch m.kind {
	case kindString:
		if s := c.str(m); len(s) > 0 {
			dst = append(dst, `,"facility":`...)
			return jlh.AppendString(dst, s)
		}
	case kindNumber:
		c.warn.Add(WarnFacilityCoerced)
		dst = append(dst, `,"facility":`...)
		return jlh.AppendString(dst, m.val)
	}
	c.warn.Add(WarnFacilityInvalid)
	return dst
}

func (c *Converter) appendTimestamp(dst []byte) []byte {
	m := c.std[stdTimestamp]
	if m == nil {
		return dst
	}
	var num []byte
	coerced := false
	switch m.kind {
	case kindNumber:
		num = m.val
	case kindString:
		if s := c.str(m); isNumber(s) {
			num, coerced = s, true
		}
	}
	if num != nil {
		mark := len(dst)
		dst = append(dst, `,"timestamp":"`...)
		if out, ok := appendGELFTimestamp(dst, num); ok {
			if coerced {
				c.warn.Add(WarnTimestampCoerced)
			}
			return append(out, '"')
		}
		dst = dst[:mark]
	}
	c.warn.Add(WarnTimestampInvalid)
	return dst
}

func (c *Converter) appendShortMessage(dst []byte) []byte {
	m := c.std[stdShortMessage]
	dst = append(dst, `,"short-message":`...)
	switch {
	case m == nil || m.kind == kindNull:
		c.warn.Add(WarnShortMessageMissing)
		return append(dst, `""`...)
	case m.kind == kindString:
		s := c.str(m)
		if len(s) == 0 {
			c.warn.Add(WarnShortMessageMissing)
		}
		return jlh.AppendString(dst, s)
	}
	c.warn.Add(WarnShortMessageCoerced)
	return c.appendAsString(dst, m)
}

func (c *Converter) appendFullMessage(dst []byte) []byte {
	m := c.std[stdFullMessage]
	if m == nil {
		return dst
	}
	if m.kind == kindString {
		s := c.str(m)
		if len(s) == 0 {
			return dst
		}
		dst = append(dst, `,"full-message":`...)
		return jlh.AppendString(dst, s)
	}
	c.warn.Add(WarnFullMessageInvalid)
	return dst
}

func (c *Converter) appendFile(dst []byte) []byte {
	m := c.std[stdFile]
	if m == nil {
		return dst
	}
	if m.kind == kindString {
		if s := c.str(m); len(s) > 0 {
			dst = append(dst, `,"file":`...)
			return jlh.AppendString(dst, s)
		}
	}
	c.warn.Add(WarnFileInvalid)
	return dst
}

// maxLine is the largest line accepted (SPEC §6.5).
const maxLine = 2147483647

func (c *Converter) appendLine(dst []byte) []byte {
	m := c.std[stdLine]
	if m == nil {
		return dst
	}
	var n int64
	ok := false
	switch m.kind {
	case kindNumber:
		if d, okd := parseDecimal(m.val); okd {
			if n, ok = d.integer(maxLine); ok && (!isPlainInt(m.val) || m.val[0] == '-') {
				c.warn.Add(WarnLineCoerced)
			}
		}
	case kindString:
		s := c.str(m)
		if len(s) > 0 && isPlainInt(s) && s[0] != '-' {
			// A string of digits, leading zeros accepted ("0042" is 42,
			// SPEC §6.5): the JSON number grammar parseDecimal checks
			// refuses them, so they go first, down to one digit.
			for len(s) > 1 && s[0] == '0' {
				s = s[1:]
			}
		}
		if len(s) > 0 && len(s) <= 10 && isPlainInt(s) && s[0] != '-' {
			if d, okd := parseDecimal(s); okd {
				if n, ok = d.integer(maxLine); ok {
					c.warn.Add(WarnLineCoerced)
				}
			}
		}
	}
	if !ok {
		c.warn.Add(WarnLineInvalid)
		return dst
	}
	dst = append(dst, `,"line":`...)
	return strconv.AppendInt(dst, n, 10)
}

// validFieldName is ^[\w\.\-]+$ in ASCII (SPEC §6.6).
func validFieldName(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, ch := range b {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || isDigit(ch) || ch == '_' || ch == '.' || ch == '-') {
			return false
		}
	}
	return true
}

func (c *Converter) appendFields(dst []byte) []byte {
	opened := false
	for _, e := range c.fields {
		m := e.m
		if m.kind == kindNull {
			c.warn.Add(WarnFieldNull)
			continue
		}
		name := c.name(e)
		if string(name) == "id" {
			c.warn.Add(WarnFieldIDReserved)
		}
		if !validFieldName(name) {
			c.warn.Add(WarnFieldNameInvalid)
		}
		if opened {
			dst = append(dst, ',')
		} else {
			dst = append(dst, `,"fields":{`...)
			opened = true
		}
		dst = jlh.AppendString(dst, name)
		dst = append(dst, ':')
		switch m.kind {
		case kindString:
			dst = jlh.AppendString(dst, c.str(m))
		case kindNumber:
			dst = append(dst, m.val...)
		default:
			c.warn.Add(WarnFieldCoerced)
			dst = c.appendAsString(dst, m)
		}
	}
	if opened {
		dst = append(dst, '}')
	}
	return dst
}

func (c *Converter) appendExtra(dst []byte) []byte {
	opened := false
	for _, e := range c.extra {
		if e.m.kind == kindNull {
			c.warn.Add(WarnFieldNull)
			continue
		}
		c.warn.Add(WarnKeyUnknown)
		if opened {
			dst = append(dst, ',')
		} else {
			dst = append(dst, `,"extra":{`...)
			opened = true
		}
		dst = jlh.AppendString(dst, c.name(e))
		dst = append(dst, ':')
		if (e.m.kind == kindObject || e.m.kind == kindArray) && hasDupKeys(e.m.val) {
			// A repeated key inside the value: written as the string of its
			// compact JSON, which keeps both, and flagged (SPEC §6.4).
			c.warn.Add(WarnDuplicateKey)
			dst = c.appendAsString(dst, e.m)
			continue
		}
		dst = c.appendCompact(dst, e.m.val)
	}
	if opened {
		dst = append(dst, '}')
	}
	return dst
}

// appendAsString writes a non-string value as a JSON string: a number's text,
// or the compact JSON of a literal, object or array (SPEC §6.6).
func (c *Converter) appendAsString(dst []byte, m *member) []byte {
	if m.kind == kindNumber {
		return jlh.AppendString(dst, m.val)
	}
	c.scratch = c.appendCompact(c.scratch[:0], m.val)
	return jlh.AppendString(dst, c.scratch)
}

// appendCompact rewrites a validated JSON value without whitespace, with every
// string decoded and re-escaped by the jlh writer (SPEC §3.3: an escaped input
// is never copied unvalidated). Numbers and literals are copied as they came.
func (c *Converter) appendCompact(dst, v []byte) []byte {
	for i := 0; i < len(v); {
		ch := v[i]
		switch {
		case isWS(ch):
			i++
		case ch == '"':
			j := i + 1
			for v[j] != '"' {
				if v[j] == '\\' {
					j++
				}
				j++
			}
			var rep bool
			mark := len(dst)
			dst, rep = appendDecoded(dst, v[i:j+1])
			if rep {
				c.warn.Add(WarnEncodingReplaced)
			}
			// Re-escape the decoded text in place: decode into the tail,
			// then rewrite it as a JSON string.
			decoded := append([]byte(nil), dst[mark:]...)
			dst = jlh.AppendString(dst[:mark], decoded)
			i = j + 1
		default:
			dst = append(dst, ch)
			i++
		}
	}
	return dst
}

// SID is the envelope sid (SPEC §6.10): FNV-1a 64 of
// "transport|src-ip|src-port|dst-ip|dst-port" reduced to 53 bits.
func SID(h *item.Head) uint64 {
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

// hasJSWhitespace reports whether b holds a character of the payload schema's
// \s (ECMAScript) or of Python's, which the skill validator uses: the union is
// what makes ^\S+$ hold for every consumer (S deviation D6, here D5).
func hasJSWhitespace(b []byte) bool {
	for len(b) > 0 {
		c := b[0]
		if c < utf8.RuneSelf {
			if c == ' ' || c >= '\t' && c <= '\r' || c >= 0x1C && c <= 0x1F {
				return true
			}
			b = b[1:]
			continue
		}
		r, n := utf8.DecodeRune(b)
		switch {
		case r == 0x0085, r == 0x00A0, r == 0x1680, r >= 0x2000 && r <= 0x200A,
			r == 0x2028, r == 0x2029, r == 0x202F, r == 0x205F, r == 0x3000, r == 0xFEFF:
			return true
		}
		b = b[n:]
	}
	return false
}
