package syslog

import (
	"testing"
	"time"
)

// FuzzParse is mandatory (SPEC §17.2). The parser is the only code in this
// project that reads hostile structured input, and it runs on the goroutine
// that receives from the network: an index out of range here takes down a
// whole receiver.
//
// Two properties are asserted on every input, valid or not:
//
//  1. It does not panic.
//  2. MsgOffset and MsgSize always describe a real slice of raw. Everything
//     downstream -- the envelope, the SQL columns, the consumer doing
//     raw[off:off+size] -- trusts these two numbers without checking them.
func FuzzParse(f *testing.F) {
	seeds := []string{
		`<165>1 2026-09-12T14:33:01.123456-03:00 web-01 nginx 1234 ID47 [a@1 k="v"] msg`,
		`<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`,
		`<14>myapp[123]: algo`,
		`<34>1 - - - - - -`,
		`<0>`,
		`<`,
		``,
		`linha solta`,
		`<34>1 - - - - - [a k="\"\\\]"] m`,
		`<34>1 - - - - - [a`,
		`<191>1 2026-13-45T99:99:99Z h a p i - m`,
		"<13>Sep  1 08:00:00 host app: \x00\xff\xfe",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	now := time.Date(2026, time.September, 12, 14, 33, 1, 0, time.UTC)
	var p Parsed // reused, exactly as a drain worker reuses it

	f.Fuzz(func(t *testing.T, raw []byte) {
		_, n, ok := PRI(raw)
		if !ok {
			n = 0
		}
		if n < 0 || n > len(raw) {
			t.Fatalf("PRI consumed %d of %d bytes", n, len(raw))
		}
		Parse(&p, raw, n, now, Options{})

		if p.MsgOffset < 0 || p.MsgSize < 0 {
			t.Fatalf("negative msg bounds: off=%d size=%d", p.MsgOffset, p.MsgSize)
		}
		if p.MsgOffset+p.MsgSize != len(raw) {
			t.Fatalf("msg bounds do not reach the end: off=%d size=%d len=%d",
				p.MsgOffset, p.MsgSize, len(raw))
		}
		_ = raw[p.MsgOffset : p.MsgOffset+p.MsgSize] // must not panic
	})
}

// FuzzSD targets structured data on its own. It has its own escaping and its
// own nesting, and the unescape path writes into a shared scratch buffer that
// every returned value points into -- a reallocation there would move bytes
// under a slice already handed out.
func FuzzSD(f *testing.F) {
	for _, s := range []string{
		`[a k="v"]`,
		`[a@1 k="v" j="w"][b x="y"]`,
		`[a k="\"" j="\\" l="\]"]`,
		`[a k="` + "\x00\xff" + `"]`,
		`[][`,
		`-`,
		`[a k=]`,
		`[a k="unterminated`,
		`[a k="v"` + "\\",
	} {
		f.Add([]byte(s))
	}

	var p Parsed
	f.Fuzz(func(t *testing.T, b []byte) {
		p.Reset()
		n := parseSD(&p, b, Options{SDMaxElements: 4, SDMaxParams: 4})
		if n < 0 || n > len(b) {
			t.Fatalf("parseSD consumed %d of %d", n, len(b))
		}
		if len(p.SD) > 4 {
			t.Fatalf("element ceiling exceeded: %d", len(p.SD))
		}
		for _, el := range p.SD {
			if len(el.Params) > 4 {
				t.Fatalf("param ceiling exceeded: %d", len(el.Params))
			}
			for _, prm := range el.Params {
				_ = string(prm.Name)
				_ = string(prm.Value) // must not panic
			}
		}
	})
}
