package envelope

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// SPEC §3.9 rests on MaxBytes being a true upper bound: the MySQL and NATS
// sinks refuse to boot when it does not fit their per-message ceiling, and that
// refusal is the only thing standing between a confirmed message and a batch
// dropped as ClassLogic. These tests hold the bound against the lines built to
// break it.

// The structured-data ceilings far above the defaults: the bound must not
// depend on parse.sd_max_*, so it is tested where they impose nothing.
var loose = syslog.Options{SDMaxElements: 1 << 16, SDMaxParams: 1 << 16}

// envelopeOf serialises line the way Prep.Build does, from a head whose every
// value is at its widest.
func envelopeOf(line, listener, nodeID string, opt syslog.Options) []byte {
	raw := []byte(line)
	head := Head{
		ID:        "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
		TS:        math.MinInt64,
		UTS:       math.MinInt64,
		NTS:       math.MinInt64,
		Transport: TransportRELP,
		Framing:   FramingDatagram,
		Listener:  listener,
		PeerAddr:  netip.MustParseAddr("fe80:ffff:ffff:ffff:ffff:ffff:ffff:ffff%interface-name-01"),
		PeerPort:  65535,
		PRI:       191,
		Truncated: true,
	}
	hdr := 0
	if pri, n, ok := syslog.PRI(raw); ok {
		head.PRI, head.FromWire, hdr = pri, true, n
	}
	var p syslog.Parsed
	syslog.Parse(&p, raw, hdr, time.Unix(1757116800, 0).UTC(), opt)
	return AppendJSON(nil, NewItem(head, raw, -1), &p, nodeID)
}

// adversarialLines is every shape that inflates the envelope: control bytes
// (six to one), invalid UTF-8 (three to one), U+2028 (two to one), escapes that
// shrink on unescape and grow again on escape, and structured data made of
// nothing but syntax.
func adversarialLines() map[string]string {
	ctl := func(n int) string { return strings.Repeat("\x01", n) }
	var elements, params strings.Builder
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&elements, "[%d]", i)
		fmt.Fprintf(&params, ` %d=""`, i)
	}
	return map[string]string{
		"3164 hostname of control bytes": "<13>Oct 11 22:14:15 " + ctl(8192) + " app[1]: msg",
		"3164 hostname of DEL":           "<13>Oct 11 22:14:15 " + strings.Repeat("\x7f", 8192) + " app: msg",
		"3164 hostname of invalid UTF-8": "<13>Oct 11 22:14:15 " + strings.Repeat("\xff", 8192) + " app: msg",
		"3164 hostname of U+2028":        "<13>Oct 11 22:14:15 " + strings.Repeat("\u2028", 2048) + " app: msg",
		"3164 tag and pid":               "<13>Oct 11 22:14:15 h " + strings.Repeat("a", 64) + "[" + strings.Repeat("9", 4096) + "]: m",
		"5424 header fields of control bytes": "<13>1 - " + ctl(300) + " " + ctl(60) + " " + ctl(200) +
			" " + ctl(40) + " - msg",
		"5424 sd values of control bytes":  `<13>1 - h a p m [x a="` + ctl(8192) + `" b="` + ctl(8192) + `"] msg`,
		"5424 sd escapes":                  `<13>1 - h a p m [x a="` + strings.Repeat(`\"\\\]`, 2048) + `"]`,
		"5424 thousands of empty elements": "<13>1 - h a p m " + elements.String(),
		"5424 thousands of empty params":   "<13>1 - h a p m [x" + params.String() + "]",
		"5424 duplicate ids":               "<13>1 - h a p m " + strings.Repeat(`[x a="`+ctl(64)+`"]`, 256),
		"5424 unterminated element":        `<13>1 - h a p m [x a="` + ctl(4096) + `" b="` + ctl(64),
		"empty":                            "",
		"no pri":                           ctl(4096),
	}
}

// randomLines mixes structured noise with pure noise, over an alphabet biased
// towards every byte the parser or the serialiser treats specially.
func randomLines(n int) []string {
	r := rand.New(rand.NewPCG(19, 13))
	const alphabet = "<>[]\"\\= -:.0123456789azAZ\x00\x01\x1f\x7f\xff\xc3\xe2\x80\xa8\n\r\t"
	noise := func(max int) string {
		b := make([]byte, r.IntN(max+1))
		for i := range b {
			b[i] = alphabet[r.IntN(len(alphabet))]
		}
		return string(b)
	}
	prefixes := []string{"", "<13>", "<13>1 ", "<13>1 - ", "<191>Oct 11 22:14:15 ", "<34>1 2026-09-12T14:33:01Z "}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var b strings.Builder
		b.WriteString(prefixes[r.IntN(len(prefixes))])
		switch r.IntN(3) {
		case 0:
			b.WriteString(noise(512))
		case 1:
			// A header, then structured data that may or may not close.
			fmt.Fprintf(&b, "%s %s %s %s ", noise(8), noise(8), noise(8), noise(8))
			for e := r.IntN(8); e > 0; e-- {
				b.WriteString("[" + noise(6))
				for p := r.IntN(8); p > 0; p-- {
					b.WriteString(" " + noise(4) + `="` + noise(32) + `"`)
				}
				if r.IntN(8) > 0 {
					b.WriteString("]")
				}
			}
			b.WriteString(noise(16))
		default:
			fmt.Fprintf(&b, "%s %s[%s]: %s", noise(64), noise(16), noise(8), noise(64))
		}
		out = append(out, b.String())
	}
	return out
}

func TestMaxBytesCoversTheFixedPart(t *testing.T) {
	// Every value at its widest and every text field empty: a three-digit
	// version, a nanosecond timestamp with an offset, NILVALUE everywhere, and
	// each of the 192 priorities for the longest names.
	line := `<%d>999 2026-09-12T14:33:01.123456789+14:00 - - - - -`
	widest := 0
	for pri := 0; pri < 192; pri++ {
		env := envelopeOf(fmt.Sprintf(line, pri), "", "", loose)
		widest = max(widest, len(env))
	}
	if widest > envelopeFixedBytes {
		t.Fatalf("the fixed part of the envelope is %d bytes, above envelopeFixedBytes (%d)",
			widest, envelopeFixedBytes)
	}
	t.Logf("fixed part: %d of %d bytes", widest, envelopeFixedBytes)
}

func TestMaxBytesBoundsTheEnvelope(t *testing.T) {
	listener := "/run/syslog/\x01log"
	nodeID := "node-\x7f-01"
	check := func(name, line string, opt syslog.Options) {
		t.Helper()
		env := envelopeOf(line, listener, nodeID, opt)
		bound := MaxBytes(int64(len(line)), int64(len(listener)), int64(len(nodeID)))
		if int64(len(env)) > bound {
			t.Errorf("%s: envelope of %d bytes for a %d-byte line, above MaxBytes = %d",
				name, len(env), len(line), bound)
		}
	}

	for name, line := range adversarialLines() {
		check(name, line, loose)
		check(name+" (default ceilings)", line, syslog.Options{})
	}
	for i, line := range randomLines(20000) {
		check(fmt.Sprintf("random line %d %q", i, line), line, loose)
	}

	// The factor is not slack: a hostname of control bytes really does come
	// close to six to one. A bound nobody can approach would hide a change
	// that made the serialiser cheaper and the ceiling needlessly strict.
	line := "<13>Oct 11 22:14:15 " + strings.Repeat("\x01", 1<<16) + " app: msg"
	env := envelopeOf(line, listener, nodeID, loose)
	if ratio := float64(len(env)) / float64(len(line)); ratio < 5.9 {
		t.Errorf("worst case renders at %.2f to one; maxEscape = %d is looser than the serialiser", ratio, maxEscape)
	}
}
