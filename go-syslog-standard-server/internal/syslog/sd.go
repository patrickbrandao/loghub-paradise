package syslog

// Structured data, RFC 5424 §6.3 and SPEC §6.5.
//
//	SD          = "-" / 1*SD-ELEMENT
//	SD-ELEMENT  = "[" SD-ID *(SP SD-PARAM) "]"
//	SD-PARAM    = PARAM-NAME "=" %d34 PARAM-VALUE %d34
//	SD-NAME     = 1*32 PRINTUSASCII, except '=', SP, ']' and '"'
//
// Inside a value, and ONLY there, three escapes exist: \" \\ and \]. There is
// no \n, no \t and no \uXXXX. A backslash before anything else is a literal
// backslash followed by that byte, and that is how it comes out in the
// envelope -- inventing the C escapes here would silently rewrite a Windows
// path an emitter sent on purpose.
//
// parseSD reports how many bytes it consumed. It never fails: a malformed
// element ends the structured-data section and the rest of the line becomes
// MSG, with an anomaly counted.
func parseSD(p *Parsed, b []byte, opt Options) int {
	if len(b) == 0 {
		return 0
	}
	if b[0] == '-' {
		return 1
	}
	if b[0] != '[' {
		// Not NILVALUE and not an element: the sender skipped the field. Treat
		// it as absent rather than consuming anything, so MSG starts here.
		return 0
	}

	// The unescape scratch can never exceed the line it came from, so reserving
	// that much once makes every later append allocation-free -- and, more
	// importantly, makes it impossible for a reallocation to move bytes that
	// an already-returned SDParam.Value points at.
	if cap(p.scratch) < len(b) {
		p.scratch = make([]byte, 0, len(b))
	}

	maxEl := opt.SDMaxElements
	if maxEl <= 0 {
		maxEl = 32
	}
	maxParam := opt.SDMaxParams
	if maxParam <= 0 {
		maxParam = 64
	}

	i := 0
	for i < len(b) && b[i] == '[' {
		n, ok := p.parseSDElement(b[i:], maxEl, maxParam)
		if !ok {
			// An element that does not close leaves the rest of the line as
			// MSG. Guessing where it should have ended would silently move
			// message text into a parameter value.
			p.Anomalies[AnomSDUnterminated]++
			return i
		}
		i += n
	}
	return i
}

// parseSDElement reads one "[...]" and returns the bytes consumed.
func (p *Parsed) parseSDElement(b []byte, maxEl, maxParam int) (int, bool) {
	i := 1 // past '['
	start := i
	for i < len(b) && !isSDDelim(b[i]) {
		i++
	}
	if i == start || i >= len(b) {
		return 0, false
	}
	id := b[start:i]

	el := p.element(id, maxEl)

	for i < len(b) && b[i] == ' ' {
		i++
		ns, name, value, ok := p.parseSDParam(b[i:])
		if !ok {
			return 0, false
		}
		i += ns
		if el != nil && len(el.Params) < maxParam {
			el.Params = append(el.Params, SDParam{Name: name, Value: value})
		} else if el != nil {
			p.Anomalies[AnomSDTruncated]++
		}
	}
	if i >= len(b) || b[i] != ']' {
		return 0, false
	}
	return i + 1, true
}

// element returns the SDElement to fill, creating it or reusing the existing
// one with the same id.
//
// A duplicate SD-ID is forbidden by the RFC and appears in the field anyway.
// The last one wins, because the envelope renders structured data as a JSON
// object and two entries with the same key would be ambiguous to every
// consumer. Returning nil means the element ceiling was hit and this element
// is ignored.
func (p *Parsed) element(id []byte, maxEl int) *SDElement {
	for i := range p.SD {
		if string(p.SD[i].ID) == string(id) {
			p.Anomalies[AnomDupSDID]++
			p.SD[i].Params = p.SD[i].Params[:0]
			return &p.SD[i]
		}
	}
	if len(p.SD) >= maxEl {
		p.Anomalies[AnomSDTruncated]++
		return nil
	}
	// Grow by reusing the capacity Reset kept, so a steady stream of messages
	// with structured data stops allocating after the first few.
	if len(p.SD) < cap(p.SD) {
		p.SD = p.SD[:len(p.SD)+1]
		e := &p.SD[len(p.SD)-1]
		e.ID = id
		e.Params = e.Params[:0]
		return e
	}
	p.SD = append(p.SD, SDElement{ID: id})
	return &p.SD[len(p.SD)-1]
}

// parseSDParam reads PARAM-NAME="PARAM-VALUE".
func (p *Parsed) parseSDParam(b []byte) (n int, name, value []byte, ok bool) {
	i := 0
	for i < len(b) && !isSDDelim(b[i]) {
		i++
	}
	if i == 0 || i >= len(b) || b[i] != '=' {
		return 0, nil, nil, false
	}
	name = b[:i]
	i++
	if i >= len(b) || b[i] != '"' {
		return 0, nil, nil, false
	}
	i++

	// First pass: find the closing quote and note whether any escape is
	// present. Values without one -- the overwhelming majority -- are handed
	// out as a subslice of the raw line, with no copy at all.
	vs := i
	escaped := false
	for i < len(b) {
		if b[i] == '\\' {
			escaped = true
			i += 2
			continue
		}
		if b[i] == '"' {
			break
		}
		i++
	}
	// i can overshoot by one when the line ends with a trailing backslash.
	if i >= len(b) || b[i] != '"' {
		return 0, nil, nil, false
	}
	value = b[vs:i]
	if escaped {
		value = p.unescape(value)
	}
	return i + 1, name, value, true
}

// unescape resolves the three escapes into the scratch buffer and returns a
// slice of it.
func (p *Parsed) unescape(v []byte) []byte {
	start := len(p.scratch)
	for i := 0; i < len(v); i++ {
		if v[i] == '\\' && i+1 < len(v) {
			switch v[i+1] {
			case '"', '\\', ']':
				p.scratch = append(p.scratch, v[i+1])
				i++
				continue
			}
		}
		p.scratch = append(p.scratch, v[i])
	}
	return p.scratch[start:]
}

// isSDDelim reports the four bytes an SD-NAME may not contain.
func isSDDelim(c byte) bool {
	return c == '=' || c == ' ' || c == ']' || c == '"'
}
