package envelope

import "strings"

// AppendQuery writes the query object of SPEC §3.2 from the raw query string,
// decoded the way Python's urllib.parse.parse_qsl(keep_blank_values=True)
// decodes it -- the reference the LogHub webhook skills validate against:
//
//   - pairs are separated by '&' only (not ';', as in Python 3.10+);
//   - an empty piece is skipped; a piece without '=' is a key with value "";
//   - '+' becomes a space BEFORE percent-decoding, so %2B stays a '+';
//   - %XX with two hex digits becomes that byte; any other '%' stays literal;
//   - the decoded bytes are written as a JSON string, so invalid UTF-8 becomes
//     one U+FFFD per maximal invalid subpart, as Python's errors='replace'.
//
// One occurrence of a key is a string; a repeated key is an array in arrival
// order. Keys are written in the order of their first occurrence. No query is
// {}. A key is grouped by the text that is written (validText), not by its
// bytes: "%FF" and "%FE" are both written as U+FFFD, and grouping them apart
// wrote the same JSON name twice -- every parser, jsonb and MySQL JSON included,
// keeps the last one and drops the other value without a word.
func AppendQuery(dst []byte, raw string) []byte {
	if raw == "" {
		return append(dst, '{', '}')
	}
	type pair struct{ k, v string }
	var pairs []pair
	for piece := range strings.SplitSeq(raw, "&") {
		if piece == "" {
			continue
		}
		k, v, _ := strings.Cut(piece, "=")
		pairs = append(pairs, pair{validText(unquotePlus(k)), unquotePlus(v)})
	}
	if len(pairs) == 0 {
		return append(dst, '{', '}')
	}

	// Group the values of each key, keys in order of first occurrence. A
	// query is at most server.max_uri_bytes long -- thousands of pairs -- so
	// the grouping is linear: a nested scan would be a cheap CPU amplifier
	// for whoever sends "a&b&c&...".
	order := make([]string, 0, len(pairs))
	vals := make(map[string][]string, len(pairs))
	for _, p := range pairs {
		if _, seen := vals[p.k]; !seen {
			order = append(order, p.k)
		}
		vals[p.k] = append(vals[p.k], p.v)
	}

	dst = append(dst, '{')
	for i, k := range order {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = AppendString(dst, k)
		dst = append(dst, ':')
		v := vals[k]
		if len(v) == 1 {
			dst = AppendString(dst, v[0])
			continue
		}
		dst = append(dst, '[')
		for j, x := range v {
			if j > 0 {
				dst = append(dst, ',')
			}
			dst = AppendString(dst, x)
		}
		dst = append(dst, ']')
	}
	return append(dst, '}')
}

// unquotePlus is Python's unquote_plus on bytes: '+' to space, then %XX.
func unquotePlus(s string) string {
	if strings.IndexByte(s, '+') < 0 && strings.IndexByte(s, '%') < 0 {
		return s
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '+':
			b = append(b, ' ')
		case c == '%' && i+2 < len(s) && isHex(s[i+1]) && isHex(s[i+2]):
			b = append(b, unhex(s[i+1])<<4|unhex(s[i+2]))
			i += 2
		default:
			b = append(b, c)
		}
	}
	return string(b)
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func unhex(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}
