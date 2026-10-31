package convert

import (
	"unicode/utf16"
	"unicode/utf8"
)

// The JSON reader of SPEC §6.2: a validating scan, written here rather than
// taken from encoding/json for three reasons that all bite with GELF:
//
//   - numbers must survive as the TEXT that arrived. encoding/json would hand
//     back float64 (1e400 fails, 0.1 is not 0.1, 2^53+1 becomes 2^53), or a
//     json.Number that still has to be re-validated;
//   - the reception (for the level filter) and the drain worker (for the
//     envelope) must read the SAME level. One function used in both places
//     makes that true by construction;
//   - the document is validated once, and nothing is decoded until it is
//     emitted: the top-level members are recorded as slices of the document.
//
// The scan accepts exactly RFC 8259: whitespace is space, \t, \n and \r; a BOM
// is a syntax error; strings must be valid UTF-8 with no raw control byte;
// escapes are the nine of §7 of the RFC; numbers follow §6. Nesting is capped
// at maxDepth so the recursion has a ceiling a hostile document cannot raise.

// maxDepth is the deepest nesting accepted (SPEC §6.1). The top-level object is
// depth 1.
const maxDepth = 64

// kind is the JSON type of a value.
type kind uint8

const (
	kindString kind = iota
	kindNumber
	kindTrue
	kindFalse
	kindNull
	kindObject
	kindArray
)

// docError is why a document is malformed (SPEC §6.1). The order is the order
// of the checks.
type docError uint8

const (
	docOK docError = iota
	docUTF8
	docSyntax
	docTrailing
	docNotObject
	docDepth
)

// The five normative parse.error texts (SPEC §6.1). They are data -- the
// schema closes them in an enum -- which is why they are Portuguese in an
// English code base.
var docErrorText = [...]string{
	docUTF8:      "UTF-8 inválido no documento",
	docSyntax:    "documento não é JSON válido",
	docTrailing:  "conteúdo depois do fim do documento JSON",
	docNotObject: "o documento JSON não é um objeto",
	docDepth:     "aninhamento JSON acima de 64 níveis",
}

// ErrorTexts lists the parse.error values, for the schema contract test.
func ErrorTexts() []string { return docErrorText[1:] }

// member is one top-level key/value pair, as slices of the document.
type member struct {
	key  []byte // the string token, quotes included
	val  []byte // the value token
	kind kind
}

// scanner validates one document and records its top-level members. It is
// reused: members keeps its capacity between documents.
type scanner struct {
	b       []byte
	pos     int
	err     docError
	members []member
}

func (s *scanner) reset(doc []byte) {
	s.b, s.pos, s.err = doc, 0, docOK
	s.members = s.members[:0]
}

// scan validates doc and fills s.members. It returns docOK or why the document
// is malformed.
func (s *scanner) scan(doc []byte) docError {
	s.reset(doc)
	if !utf8.Valid(doc) {
		return docUTF8
	}
	s.ws()
	if s.pos >= len(s.b) {
		return docSyntax
	}
	if s.b[s.pos] != '{' {
		// Validate the value anyway: a valid array is "not an object", a
		// broken one is "not JSON".
		// Depth 0: the value itself is level 1, as the top object is
		// (value opens a container at depth+1).
		if _, ok := s.value(0); !ok {
			return s.failure()
		}
		s.ws()
		if s.pos != len(s.b) {
			return docTrailing
		}
		return docNotObject
	}
	if !s.object(1, true) {
		return s.failure()
	}
	s.ws()
	if s.pos != len(s.b) {
		return docTrailing
	}
	return docOK
}

func (s *scanner) failure() docError {
	if s.err != docOK {
		return s.err
	}
	return docSyntax
}

func isWS(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func (s *scanner) ws() {
	for s.pos < len(s.b) && isWS(s.b[s.pos]) {
		s.pos++
	}
}

// value scans one value at s.pos and reports its kind.
func (s *scanner) value(depth int) (kind, bool) {
	if s.pos >= len(s.b) {
		return 0, false
	}
	switch c := s.b[s.pos]; {
	case c == '"':
		return kindString, s.str()
	case c == '{':
		return kindObject, s.object(depth+1, false)
	case c == '[':
		return kindArray, s.array(depth + 1)
	case c == '-' || c >= '0' && c <= '9':
		n := numberLen(s.b[s.pos:])
		if n == 0 {
			return 0, false
		}
		s.pos += n
		return kindNumber, true
	case c == 't':
		return kindTrue, s.lit("true")
	case c == 'f':
		return kindFalse, s.lit("false")
	case c == 'n':
		return kindNull, s.lit("null")
	}
	return 0, false
}

func (s *scanner) lit(w string) bool {
	if len(s.b)-s.pos < len(w) || string(s.b[s.pos:s.pos+len(w)]) != w {
		return false
	}
	s.pos += len(w)
	return true
}

// object scans `{...}`. With top, the members are recorded.
func (s *scanner) object(depth int, top bool) bool {
	if depth > maxDepth {
		s.err = docDepth
		return false
	}
	s.pos++ // '{'
	s.ws()
	if s.pos < len(s.b) && s.b[s.pos] == '}' {
		s.pos++
		return true
	}
	for {
		if s.pos >= len(s.b) || s.b[s.pos] != '"' {
			return false
		}
		ks := s.pos
		if !s.str() {
			return false
		}
		key := s.b[ks:s.pos]
		s.ws()
		if s.pos >= len(s.b) || s.b[s.pos] != ':' {
			return false
		}
		s.pos++
		s.ws()
		vs := s.pos
		k, ok := s.value(depth)
		if !ok {
			return false
		}
		if top {
			s.members = append(s.members, member{key: key, val: s.b[vs:s.pos], kind: k})
		}
		s.ws()
		if s.pos >= len(s.b) {
			return false
		}
		switch s.b[s.pos] {
		case ',':
			s.pos++
			s.ws()
		case '}':
			s.pos++
			return true
		default:
			return false
		}
	}
}

func (s *scanner) array(depth int) bool {
	if depth > maxDepth {
		s.err = docDepth
		return false
	}
	s.pos++ // '['
	s.ws()
	if s.pos < len(s.b) && s.b[s.pos] == ']' {
		s.pos++
		return true
	}
	for {
		if _, ok := s.value(depth); !ok {
			return false
		}
		s.ws()
		if s.pos >= len(s.b) {
			return false
		}
		switch s.b[s.pos] {
		case ',':
			s.pos++
			s.ws()
		case ']':
			s.pos++
			return true
		default:
			return false
		}
	}
}

// str scans a string token. The document is already known to be valid UTF-8;
// what is checked here is the JSON string grammar: no raw byte below 0x20 and
// only the nine escapes.
func (s *scanner) str() bool {
	i := s.pos + 1
	for i < len(s.b) {
		c := s.b[i]
		switch {
		case c == '"':
			s.pos = i + 1
			return true
		case c < 0x20:
			return false
		case c == '\\':
			if i+1 >= len(s.b) {
				return false
			}
			switch s.b[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(s.b) || !isHex4(s.b[i+2:i+6]) {
					return false
				}
				i += 6
			default:
				return false
			}
		default:
			i++
		}
	}
	return false
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func isHex4(b []byte) bool { return isHex(b[0]) && isHex(b[1]) && isHex(b[2]) && isHex(b[3]) }

func hexVal(b []byte) rune {
	var r rune
	for _, c := range b[:4] {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c-'a') + 10
		default:
			r |= rune(c-'A') + 10
		}
	}
	return r
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// numberLen returns the length of the RFC 8259 number at the start of b, or 0
// when b does not start with one.
//
//	number = [ "-" ] int [ frac ] [ exp ]
//	int    = "0" / ( digit1-9 *DIGIT )
//	frac   = "." 1*DIGIT
//	exp    = ( "e" / "E" ) [ "-" / "+" ] 1*DIGIT
func numberLen(b []byte) int {
	i := 0
	if i < len(b) && b[i] == '-' {
		i++
	}
	if i >= len(b) || !isDigit(b[i]) {
		return 0
	}
	if b[i] == '0' {
		i++
	} else {
		for i < len(b) && isDigit(b[i]) {
			i++
		}
	}
	if i < len(b) && b[i] == '.' {
		i++
		if i >= len(b) || !isDigit(b[i]) {
			return 0
		}
		for i < len(b) && isDigit(b[i]) {
			i++
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= len(b) || !isDigit(b[i]) {
			return 0
		}
		for i < len(b) && isDigit(b[i]) {
			i++
		}
	}
	return i
}

// isNumber reports whether b is exactly one JSON number.
func isNumber(b []byte) bool { n := numberLen(b); return n > 0 && n == len(b) }

// appendDecoded appends the content of the string token tok (quotes included,
// already validated) to dst, with the escapes resolved. A lone surrogate
// becomes U+FFFD and sets replaced (SPEC §6.3).
func appendDecoded(dst, tok []byte) (out []byte, replaced bool) {
	b := tok[1 : len(tok)-1]
	start := 0
	for i := 0; i < len(b); {
		if b[i] != '\\' {
			i++
			continue
		}
		dst = append(dst, b[start:i]...)
		e := b[i+1]
		switch e {
		case '"', '\\', '/':
			dst = append(dst, e)
			i += 2
		case 'b':
			dst = append(dst, '\b')
			i += 2
		case 'f':
			dst = append(dst, '\f')
			i += 2
		case 'n':
			dst = append(dst, '\n')
			i += 2
		case 'r':
			dst = append(dst, '\r')
			i += 2
		case 't':
			dst = append(dst, '\t')
			i += 2
		case 'u':
			r := hexVal(b[i+2:])
			i += 6
			if utf16.IsSurrogate(r) {
				// A high surrogate followed by an escaped low one is one
				// character; anything else is a lone surrogate.
				if r < 0xDC00 && i+6 <= len(b) && b[i] == '\\' && b[i+1] == 'u' {
					if r2 := hexVal(b[i+2:]); r2 >= 0xDC00 && r2 <= 0xDFFF {
						dst = utf8.AppendRune(dst, utf16.DecodeRune(r, r2))
						i += 6
						break
					}
				}
				dst = utf8.AppendRune(dst, utf8.RuneError)
				replaced = true
				break
			}
			dst = utf8.AppendRune(dst, r)
		}
		start = i
	}
	return append(dst, b[start:]...), replaced
}

// hasDupKeys reports whether any object inside the validated value v repeats a
// key (after decoding). Such a value cannot be written as JSON into extra: a
// consumer's parser would keep one of the two in silence (SPEC §6.4).
func hasDupKeys(v []byte) bool {
	d := dupScan{b: v}
	d.value()
	return d.dup
}

type dupScan struct {
	b   []byte
	pos int
	dup bool
}

func (d *dupScan) ws() {
	for d.pos < len(d.b) && isWS(d.b[d.pos]) {
		d.pos++
	}
}

// strEnd returns the index after the string token at d.pos.
func (d *dupScan) strEnd() int {
	j := d.pos + 1
	for d.b[j] != '"' {
		if d.b[j] == '\\' {
			j++
		}
		j++
	}
	return j + 1
}

func (d *dupScan) value() {
	d.ws()
	switch d.b[d.pos] {
	case '{':
		d.pos++
		var seen map[string]struct{}
		for !d.dup {
			d.ws()
			if d.b[d.pos] == '}' {
				d.pos++
				return
			}
			if d.b[d.pos] == ',' {
				d.pos++
				d.ws()
			}
			end := d.strEnd()
			k, _ := appendDecoded(nil, d.b[d.pos:end])
			if seen == nil {
				seen = map[string]struct{}{}
			}
			if _, ok := seen[string(k)]; ok {
				d.dup = true
				return
			}
			seen[string(k)] = struct{}{}
			d.pos = end
			d.ws()
			d.pos++ // ':'
			d.value()
		}
	case '[':
		d.pos++
		for !d.dup {
			d.ws()
			if d.b[d.pos] == ']' {
				d.pos++
				return
			}
			if d.b[d.pos] == ',' {
				d.pos++
			}
			d.value()
		}
	case '"':
		d.pos = d.strEnd()
	default:
		for d.pos < len(d.b) && !isWS(d.b[d.pos]) && d.b[d.pos] != ',' && d.b[d.pos] != '}' && d.b[d.pos] != ']' {
			d.pos++
		}
	}
}
