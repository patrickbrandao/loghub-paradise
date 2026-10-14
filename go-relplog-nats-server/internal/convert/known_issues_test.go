package convert

import (
	"strings"
	"testing"
)

// Lock test ported from go-relplog-server on 2026-10-14 (its decision 16,
// inherited; SPEC §6.5.1, §6.9): an ISO offset out of range in RFC 3164 is applied as it came,
// without a warning, as the skill's reference converter does; the same offset
// in RFC 5424 is timestamp-invalid (§6.4.2). This test pins both halves, so a
// change to either is a deliberate one, made together with the skill and the
// siblings that publish the same type.
func TestRFC3164ISOOffsetIsNotRangeChecked(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"<14>2026-10-03T08:00:00+25:00 host01 a: x", "2026-10-02T07:00:00.000Z"},
		{"<14>2026-10-03T08:00:00+05:75 host01 a: x", "2026-10-03T01:45:00.000Z"},
		{"<14>2026-10-03T08:00:00-25:00 host01 a: x", "2026-10-04T09:00:00.000Z"},
	} {
		_, p, _ := convertUDP(t, DefaultConfig(), []byte(c.in), recv)
		if p["timestamp"] != c.want {
			t.Errorf("%q: timestamp %v, want %s (the offset applied as it came)", c.in, p["timestamp"], c.want)
		}
		for _, w := range warningsOf(p) {
			if strings.HasPrefix(w, "timestamp-") {
				t.Errorf("%q: warning %s; RFC 3164 does not range-check the offset", c.in, w)
			}
		}
		if hn, _ := lookup(p, "host.hostname"); hn != "host01" || p["appname"] != "a" {
			t.Errorf("%q: the rest of the header was not read: %v", c.in, p)
		}
	}
	// The same offset in RFC 5424 is refused.
	_, p, _ := convertUDP(t, DefaultConfig(), []byte("<14>1 2026-10-03T08:00:00+25:00 h a - - - x"), recv)
	found := false
	for _, w := range warningsOf(p) {
		found = found || w == "timestamp-invalid"
	}
	if !found {
		t.Errorf("RFC 5424 with +25:00: warnings %v, want timestamp-invalid", warningsOf(p))
	}
}
