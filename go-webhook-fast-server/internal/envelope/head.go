package envelope

import (
	"errors"
	"fmt"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

// Head is the header of a JLH this server wrote, read back from the bytes
// (SPEC §12.1).
//
// The destinations that need header fields -- space and type for the Redis
// stream and the NATS subject, every column of the inbox tables -- read them
// here, from the envelope itself, and never from a copy kept beside it: an item
// that comes back from ring.db or from an emergency dump is then
// indistinguishable from the one that went in.
type Head struct {
	UUID  uuidv7.UUID
	From  string
	Space string
	Type  string
	Level int
	SID   uint64
	Step  int
	Event Event
}

// ErrHead is wrapped by every refusal of ParseHead.
var ErrHead = errors.New("envelope: not a JLH header written by this server")

// ParseHead reads the header of env and returns it with the payload object, a
// sub-slice of env (no copy).
//
// It is strict on purpose: it accepts exactly the shape AppendHeader writes --
// the nine keys in canonical order, no whitespace -- and checks every value
// against the JLH rules (uuid and from lowercase UUIDv7s, space
// and type in their grammars, level 0-15, sid <= 2^53-1, step <= 32, sub below
// one second, iso equal to uts+sub truncated). Anything else -- an envelope of
// the pre-LogHub format left in a ring.db or a dump, a truncated record -- is
// refused, never half-read. Only from, space and type allocate.
func ParseHead(env []byte) (Head, []byte, error) {
	var h Head
	p := headParser{b: env}
	p.lit(`{"uuid":"`)
	u := p.take(36)
	p.lit(`","from":"`)
	from := p.take(36)
	p.lit(`","space":"`)
	space := p.until('"')
	p.lit(`","type":"`)
	typ := p.until('"')
	p.lit(`","level":`)
	level := p.uint()
	p.lit(`,"sid":`)
	sid := p.uint()
	p.lit(`,"step":`)
	step := p.uint()
	p.lit(`,"event":{"uts":`)
	uts := p.uint()
	p.lit(`,"sub":`)
	sub := p.uint()
	p.lit(`,"iso":"`)
	iso := p.take(len(isoLayout))
	p.lit(`"},"payload":`)
	if p.err != nil {
		return h, nil, p.err
	}
	rest := env[p.i:]
	end := objectEnd(rest)
	if end < 0 || end != len(rest)-1 || rest[end] != '}' {
		return h, nil, fmt.Errorf("%w: the payload is not one object closing the envelope", ErrHead)
	}
	payload := rest[:end]

	if !validV7(u) {
		return h, nil, fmt.Errorf("%w: uuid %q is not a lowercase UUIDv7", ErrHead, u)
	}
	id, err := uuidv7.FromString(string(u))
	if err != nil || !id.IsValid() {
		return h, nil, fmt.Errorf("%w: uuid %q is not a UUIDv7", ErrHead, u)
	}
	if !validV7(from) {
		return h, nil, fmt.Errorf("%w: from %q is not a lowercase UUIDv7", ErrHead, from)
	}
	if !validName(space, false) || !validName(typ, true) {
		return h, nil, fmt.Errorf("%w: space %q or type %q outside the grammar", ErrHead, space, typ)
	}
	if level > 15 || sid > MaxSID || step > MaxStep || sub > 999999999 || uts > 253402300799 {
		return h, nil, fmt.Errorf("%w: a number is out of range", ErrHead)
	}
	h = Head{UUID: id, From: string(from), Space: string(space), Type: string(typ),
		Level: int(level), SID: sid, Step: int(step), Event: Event{UTS: int64(uts), Sub: int64(sub)}}
	var want [len(isoLayout)]byte
	if string(h.Event.AppendISO(want[:0])) != string(iso) {
		return Head{}, nil, fmt.Errorf("%w: event.iso %q does not match uts+sub", ErrHead, iso)
	}
	return h, payload, nil
}

// objectEnd returns the index just past the object that starts at b[0], or -1:
// braces are counted outside strings, and a string ends at a quote that no
// backslash escapes. It is a scan, not a parse -- the payload's content is
// the destinations' business -- but it is what tells a whole envelope from a
// truncated one whose last bytes happen to be braces.
func objectEnd(b []byte) int {
	if len(b) == 0 || b[0] != '{' {
		return -1
	}
	depth := 0
	for i := 0; i < len(b); i++ {
		switch b[i] {
		case '"':
			for i++; i < len(b) && b[i] != '"'; i++ {
				if b[i] == '\\' {
					i++
				}
			}
			if i >= len(b) {
				return -1
			}
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

type headParser struct {
	b   []byte
	i   int
	err error
}

func (p *headParser) fail(what string) {
	if p.err == nil {
		p.err = fmt.Errorf("%w: %s at offset %d", ErrHead, what, p.i)
	}
}

func (p *headParser) lit(s string) {
	if p.err != nil {
		return
	}
	if len(p.b)-p.i < len(s) || string(p.b[p.i:p.i+len(s)]) != s {
		p.fail("expected " + s)
		return
	}
	p.i += len(s)
}

func (p *headParser) take(n int) []byte {
	if p.err != nil {
		return nil
	}
	if len(p.b)-p.i < n {
		p.fail("truncated")
		return nil
	}
	v := p.b[p.i : p.i+n]
	p.i += n
	return v
}

// until takes up to 64 bytes before c, which the grammars of space and type
// cap anyway.
func (p *headParser) until(c byte) []byte {
	if p.err != nil {
		return nil
	}
	for j := p.i; j < len(p.b) && j-p.i <= 64; j++ {
		if p.b[j] == c {
			v := p.b[p.i:j]
			p.i = j
			return v
		}
	}
	p.fail("unterminated string")
	return nil
}

// uint reads a JSON integer without sign, exponent or leading zero, of at most
// 19 digits.
func (p *headParser) uint() uint64 {
	if p.err != nil {
		return 0
	}
	start := p.i
	var n uint64
	for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' && p.i-start < 19 {
		n = n*10 + uint64(p.b[p.i]-'0')
		p.i++
	}
	if p.i == start || (p.b[start] == '0' && p.i-start > 1) {
		p.fail("bad integer")
		return 0
	}
	return n
}

// validV7 is the uuidv7 rule of the webhook schema, the shape AppendHeader
// writes for both uuid and from (SPEC §3.0):
// ^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$.
// uuidv7.FromString alone also takes uppercase, and the generic JLH from rule
// any version 1-8: neither is a record this server wrote.
func validV7(s []byte) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
			continue
		case 14:
			if c != '7' {
				return false
			}
			continue
		case 19:
			if c != '8' && c != '9' && c != 'a' && c != 'b' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// validName is the space grammar of SPEC §3.0 ([a-z0-9-] segments joined by
// dots, none empty, at most 64 characters), with '_' also allowed for a type
// (the AgentName grammar of the JLH).
func validName(s []byte, underscore bool) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	seg := 0
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_' && underscore:
			seg++
		case c == '.':
			if seg == 0 {
				return false
			}
			seg = 0
		default:
			return false
		}
	}
	return seg > 0
}
