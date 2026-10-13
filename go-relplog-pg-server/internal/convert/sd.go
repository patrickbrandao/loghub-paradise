package convert

import "bytes"

// STRUCTURED-DATA (SPEC §6.4.3).
//
//	STRUCTURED-DATA = NILVALUE / 1*SD-ELEMENT
//	SD-ELEMENT      = "[" SD-ID *(SP PARAM-NAME "=" DQUOTE PARAM-VALUE DQUOTE) "]"
//	SD-NAME         = 1*32 PRINTUSASCII except "=", SP, "]" and DQUOTE
//
// The parse fills flat slices owned by the Converter, reused across messages:
// elements in arrival order, and parameters in arrival order tagged with the
// element they belong to. Grouping a repeated PARAM-NAME into an array happens
// at emission time, so a duplicate SD-ID can simply point its parameters at the
// first element with that id (the merge of SPEC §6.4.3).

type sdParam struct {
	elem  int
	name  []byte
	value []byte // unescaped and decoded
}

type sdState struct {
	ids     [][]byte
	params  []sdParam
	emitted []bool
	perElem []int   // parameter count per element, for sd_max_params
	byElem  [][]int // parameter indices per element, rebuilt by appendSD
}

func (s *sdState) reset() {
	for i := range s.ids {
		s.ids[i] = nil
	}
	s.ids = s.ids[:0]
	for i := range s.params {
		s.params[i] = sdParam{}
	}
	s.params = s.params[:0]
	s.perElem = s.perElem[:0]
}

// sdNameByte is SD-NAME: printable US-ASCII (33..126) except '=', ']' and '"'.
func sdNameByte(c byte) bool {
	return c >= 33 && c <= 126 && c != '=' && c != ']' && c != '"'
}

func sdName(b []byte, pos int) ([]byte, int, bool) {
	start := pos
	for pos < len(b) && sdNameByte(b[pos]) {
		pos++
	}
	n := pos - start
	if n < 1 || n > 32 {
		return nil, pos, false
	}
	return b[start:pos], pos, true
}

// parseSD reads STRUCTURED-DATA starting at b[pos] == '['. It returns the
// position after the last ']' and whether the grammar held. On failure the
// caller omits structured-data and sends the whole text from pos to message, so
// nothing is lost (sd-invalid). The value decoding warnings go to local and
// are merged by the caller only on success: a failed SD is re-decoded as part
// of message, which produces its own.
func (c *Converter) parseSD(b []byte, pos int, warn, local *Warnings) (int, bool) {
	s := &c.sd
	s.reset()
	maxElems, maxParams := c.cfg.SDMaxElements, c.cfg.SDMaxParams
	nElems := 0
	for pos < len(b) && b[pos] == '[' {
		nElems++
		if maxElems > 0 && nElems > maxElems {
			return pos, false
		}
		id, p, ok := sdName(b, pos+1)
		if !ok {
			return p, false
		}
		pos = p
		elem := -1
		for i, x := range s.ids {
			if bytes.Equal(x, id) {
				elem = i
				break
			}
		}
		dup := elem >= 0
		if !dup {
			elem = len(s.ids)
			s.ids = append(s.ids, id)
			s.perElem = append(s.perElem, 0)
		}
		for pos < len(b) && b[pos] == ' ' {
			name, p, ok := sdName(b, pos+1)
			if !ok {
				return p, false
			}
			pos = p
			if pos+1 >= len(b) || b[pos] != '=' || b[pos+1] != '"' {
				return pos, false
			}
			pos += 2
			m := c.ar.mark()
			closed := false
			for pos < len(b) {
				ch := b[pos]
				if ch == '\\' && pos+1 < len(b) && (b[pos+1] == '"' || b[pos+1] == '\\' || b[pos+1] == ']') {
					c.ar.b = append(c.ar.b, b[pos+1])
					pos += 2
					continue
				}
				if ch == '"' {
					pos++
					closed = true
					break
				}
				c.ar.b = append(c.ar.b, ch)
				pos++
			}
			if !closed {
				return pos, false
			}
			value, valid := c.ar.decode(c.ar.since(m))
			if !valid {
				local.Add(WarnEncodingInvalid)
			}
			s.perElem[elem]++
			if maxParams > 0 && s.perElem[elem] > maxParams {
				return pos, false
			}
			s.params = append(s.params, sdParam{elem: elem, name: name, value: value})
		}
		if pos >= len(b) || b[pos] != ']' {
			return pos, false
		}
		pos++
		// Like the reference, the duplicate is recorded once the repeated
		// element has closed: that is when its parameters are merged.
		if dup {
			warn.Add(WarnSDDuplicateID)
		}
	}
	return pos, true
}

// appendSD writes the structured-data object: SD-IDs and PARAM-NAMEs in order of
// first arrival, a single occurrence as a string and a repeated one as an array
// in arrival order -- never an array of one item.
//
// The parameters are first bucketed by element, so the name grouping only ever
// scans one element's parameters: bounded by sd_max_params, not by the whole
// message. Scanning every parameter of the message for every element made an SD
// at the caps (32 x 64) cost milliseconds per message.
func (c *Converter) appendSD(dst []byte) []byte {
	s := &c.sd
	if cap(s.emitted) < len(s.params) {
		s.emitted = make([]bool, len(s.params))
	}
	s.emitted = s.emitted[:len(s.params)]
	clear(s.emitted)
	for len(s.byElem) < len(s.ids) {
		s.byElem = append(s.byElem, nil)
	}
	for e := range s.ids {
		s.byElem[e] = s.byElem[e][:0]
	}
	for i := range s.params {
		e := s.params[i].elem
		s.byElem[e] = append(s.byElem[e], i)
	}

	dst = append(dst, '{')
	for e, id := range s.ids {
		if e > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONBytes(dst, id)
		dst = append(dst, ':', '{')
		idx := s.byElem[e]
		first := true
		for a, i := range idx {
			if s.emitted[i] {
				continue
			}
			p := &s.params[i]
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONBytes(dst, p.name)
			dst = append(dst, ':')
			count := 0
			for _, j := range idx[a:] {
				if !s.emitted[j] && bytes.Equal(s.params[j].name, p.name) {
					count++
				}
			}
			if count == 1 {
				s.emitted[i] = true
				dst = appendJSONBytes(dst, p.value)
				continue
			}
			dst = append(dst, '[')
			k := 0
			for _, j := range idx[a:] {
				q := &s.params[j]
				if !s.emitted[j] && bytes.Equal(q.name, p.name) {
					if k > 0 {
						dst = append(dst, ',')
					}
					k++
					s.emitted[j] = true
					dst = appendJSONBytes(dst, q.value)
				}
			}
			dst = append(dst, ']')
		}
		dst = append(dst, '}')
	}
	return append(dst, '}')
}
