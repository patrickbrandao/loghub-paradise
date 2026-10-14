package emergency

import "bytes"

// payloadKeys is the top-level key sequence of every payload the writer
// produces (envelope.AppendWebhook, SPEC §3.2), in its order.
var payloadKeys = []string{
	"mode", "network", "method", "schema", "host", "path", "url",
	"headers", "params", "query", "content",
}

// Limits of what the writer can produce. Its deepest value is three levels
// below the payload (network.meta[i]; query.k[i] is two), and its numbers are
// the IP version, two ports, one more port in meta and content.length.
const (
	shapeMaxDepth  = 6
	shapeMaxDigits = 20
)

// ourShape reports whether payload -- the object ParseHead returned, already
// known to be valid JSON -- has the shape only this server's writer gives it
// (SPEC §9.2). json.Valid accepts a great deal the writer never writes, and
// NATS never reads Data: a duplicate "mode" that last-wins readers see as
// "bucket", a number like 1e999999 that a jsonb or float decoder refuses as
// poison, nesting a Python reader dies on. Each would be published and
// confirmed, and the file removed. So, beyond the header:
//
//   - the payload's keys are exactly payloadKeys, in that order;
//   - no object anywhere holds the same key twice;
//   - nesting stops at shapeMaxDepth;
//   - a number is a non-negative integer of at most shapeMaxDigits digits;
//   - true, false and null never appear;
//   - a string holds only the escapes the serializer writes: \" \\ \n \r
//     \t, \u00XX for another control byte, \u2028 and \u2029. That also
//     makes the escaping canonical, so two keys are the same key exactly when
//     their bytes are; a raw U+2028/U+2029, a second spelling of the escaped
//     one, is refused (SPEC §9.2; until 2026-10-14 str let it through).
func ourShape(payload []byte) bool {
	s := shapeScanner{b: payload}
	keys, ok := s.object(1)
	if !ok || s.i != len(s.b) || len(keys) != len(payloadKeys) {
		return false
	}
	for i, k := range keys {
		if string(k) != payloadKeys[i] {
			return false
		}
	}
	return true
}

type shapeScanner struct {
	b []byte
	i int
}

func (s *shapeScanner) peek() byte {
	if s.i >= len(s.b) {
		return 0
	}
	return s.b[s.i]
}

func (s *shapeScanner) value(depth int) bool {
	switch c := s.peek(); {
	case c == '{':
		_, ok := s.object(depth + 1)
		return ok
	case c == '[':
		return s.array(depth + 1)
	case c == '"':
		_, ok := s.str()
		return ok
	case c >= '0' && c <= '9':
		return s.number()
	}
	return false // true, false, null, a sign, or garbage
}

// object scans {...} at depth and returns its keys, raw, in order.
func (s *shapeScanner) object(depth int) ([][]byte, bool) {
	if depth > shapeMaxDepth || s.peek() != '{' {
		return nil, false
	}
	s.i++
	var keys [][]byte
	var seen map[string]struct{}
	if s.peek() == '}' {
		s.i++
		return keys, true
	}
	for {
		k, ok := s.str()
		if !ok {
			return nil, false
		}
		// Linear for the small objects the writer mostly produces; a map
		// once a query brings thousands of keys.
		if seen == nil && len(keys) >= 16 {
			seen = make(map[string]struct{}, 2*len(keys))
			for _, old := range keys {
				seen[string(old)] = struct{}{}
			}
		}
		if seen != nil {
			if _, dup := seen[string(k)]; dup {
				return nil, false
			}
			seen[string(k)] = struct{}{}
		} else {
			for _, old := range keys {
				if bytes.Equal(old, k) {
					return nil, false
				}
			}
		}
		keys = append(keys, k)
		if s.peek() != ':' {
			return nil, false
		}
		s.i++
		if !s.value(depth) {
			return nil, false
		}
		switch s.peek() {
		case ',':
			s.i++
		case '}':
			s.i++
			return keys, true
		default:
			return nil, false
		}
	}
}

func (s *shapeScanner) array(depth int) bool {
	if depth > shapeMaxDepth || s.peek() != '[' {
		return false
	}
	s.i++
	if s.peek() == ']' {
		s.i++
		return true
	}
	for {
		if !s.value(depth) {
			return false
		}
		switch s.peek() {
		case ',':
			s.i++
		case ']':
			s.i++
			return true
		default:
			return false
		}
	}
}

// str scans a string and returns its content between the quotes, raw.
func (s *shapeScanner) str() ([]byte, bool) {
	if s.peek() != '"' {
		return nil, false
	}
	s.i++
	start := s.i
	for s.i < len(s.b) {
		c := s.b[s.i]
		switch {
		case c == '"':
			out := s.b[start:s.i]
			s.i++
			return out, true
		case c < 0x20:
			return nil, false
		case c != '\\':
			// A raw U+2028/U+2029 (E2 80 A8/A9): the writer always escapes
			// them (SPEC §9.2, §12.1). Accepted, it would be a second
			// spelling of a key the duplicate check compares by bytes.
			if c == 0xE2 && s.i+2 < len(s.b) && s.b[s.i+1] == 0x80 &&
				(s.b[s.i+2] == 0xA8 || s.b[s.i+2] == 0xA9) {
				return nil, false
			}
			s.i++
			continue
		}
		if s.i+1 >= len(s.b) {
			return nil, false
		}
		switch s.b[s.i+1] {
		case '"', '\\', 'n', 'r', 't':
			s.i += 2
			continue
		case 'u':
		default:
			return nil, false // \/ \b \f: valid JSON, never written
		}
		if s.i+6 > len(s.b) || !ourUnicodeEscape(s.b[s.i+2:s.i+6]) {
			return nil, false
		}
		s.i += 6
	}
	return nil, false
}

// ourUnicodeEscape is the four hex digits of a \u escape the serializer writes
// (envelope.appendEscape and the U+2028/U+2029 case): lowercase hex, a
// control byte without a short form, or a JavaScript line terminator.
func ourUnicodeEscape(q []byte) bool {
	if string(q) == "2028" || string(q) == "2029" {
		return true
	}
	if q[0] != '0' || q[1] != '0' || (q[2] != '0' && q[2] != '1') {
		return false
	}
	v := hexVal(q[2])<<4 | hexVal(q[3])
	if hexVal(q[3]) < 0 || v == '\n' || v == '\r' || v == '\t' {
		return false
	}
	return true
}

func hexVal(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	}
	return -1
}

func (s *shapeScanner) number() bool {
	start := s.i
	for s.i < len(s.b) && s.b[s.i] >= '0' && s.b[s.i] <= '9' {
		s.i++
	}
	// A fraction or an exponent needs no check of its own: the '.' or 'e'
	// left behind is not a ',' or a closing bracket, and the caller refuses
	// it.
	n := s.i - start
	return n <= shapeMaxDigits && (n == 1 || s.b[start] != '0')
}
