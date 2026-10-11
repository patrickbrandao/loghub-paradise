package envelope

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

func build(t *testing.T, line string) (map[string]any, *Item, []byte) {
	t.Helper()
	raw := []byte(line)
	head := Head{
		ID:        "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
		Transport: TransportTCP,
		Framing:   FramingOctet,
		Listener:  ":1514",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  51234,
	}
	head.SetTime(time.Unix(1757116800, 123456789).UTC())
	if pri, _, ok := syslog.PRI(raw); ok {
		head.PRI, head.FromWire = pri, true
	} else {
		head.PRI = 13
	}
	it := NewItem(head, raw, -1)

	var p syslog.Parsed
	_, n, ok := syslog.PRI(raw)
	if !ok {
		n = 0
	}
	syslog.Parse(&p, raw, n, time.Unix(1757116800, 0).UTC(), syslog.Options{})

	out := AppendJSON(GetEnv(), it, &p, "sls-prod-a1")
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("envelope is not valid JSON: %v\n%s", err, out)
	}
	return m, it, out
}

func TestEnvelopeFields(t *testing.T) {
	line := `<165>1 2026-09-12T14:33:01.123456Z web-01 nginx 1234 ID47 [a@1 k="v"] hello there`
	m, _, _ := build(t, line)

	want := map[string]any{
		"id":              "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
		"ts":              float64(1757116800),
		"uts":             float64(123456),
		"nts":             float64(123456789),
		"transport":       "tcp",
		"framing":         "octet",
		"listener":        ":1514",
		"peer-addr":       "10.0.0.7",
		"peer-port":       float64(51234),
		"peer-ip-version": float64(4),
		"rfc":             "5424",
		"pri":             float64(165),
		"pri-source":      "wire",
		"facility":        float64(20),
		"facility-name":   "local4",
		"severity":        float64(5),
		"severity-name":   "notice",
		"version":         float64(1),
		"msg-ts-source":   "rfc5424",
		"hostname":        "web-01",
		"app-name":        "nginx",
		"procid":          "1234",
		"msgid":           "ID47",
		"truncated":       false,
		"node-id":         "sls-prod-a1",
		"raw-size":        float64(len(line)),
	}
	for k, w := range want {
		if got := m[k]; got != w {
			t.Errorf("%s = %#v, want %#v", k, got, w)
		}
	}
	sd, ok := m["structured-data"].(map[string]any)
	if !ok || len(sd) != 1 {
		t.Fatalf("structured-data = %#v", m["structured-data"])
	}
	if sd["a@1"].(map[string]any)["k"] != "v" {
		t.Errorf("sd = %#v", sd)
	}
}

// The one invariant every consumer relies on: slicing the raw line by the two
// offsets in the envelope yields exactly the message.
func TestMsgOffsetSlicesTheRawLine(t *testing.T) {
	for _, line := range []string{
		`<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - hello there`,
		`<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`,
		`<14>myapp[123]: algo`,
		`linha solta`,
		`<34>1 - - - - - -`,
	} {
		m, it, _ := build(t, line)
		off := int(m["msg-offset"].(float64))
		size := int(m["msg-size"].(float64))
		if off+size != len(it.Raw) {
			t.Errorf("%q: off+size=%d, raw=%d", line, off+size, len(it.Raw))
			continue
		}
		if int(m["raw-size"].(float64)) != len(it.Raw) {
			t.Errorf("%q: raw-size disagrees with the line", line)
		}
		_ = string(it.Raw[off : off+size]) // must not panic
	}
}

// A hostname or app-name from an appliance speaking latin-1 must not be able to
// produce an envelope nobody can parse.
func TestInvalidUTF8BecomesReplacement(t *testing.T) {
	line := "<13>Sep  1 08:00:00 h\xffost app: msg\xfe"
	m, it, out := build(t, line)
	if !json.Valid(out) {
		t.Fatal("envelope is not valid JSON")
	}
	if h := m["hostname"].(string); h != "h�ost" {
		t.Errorf("hostname = %q", h)
	}
	// The raw line is untouched: the replacement happens only in the envelope
	// copy (invariant §19.5).
	if string(it.Raw) != line {
		t.Error("raw line was modified")
	}
}

func TestNoPRIUsesDefaults(t *testing.T) {
	m, _, _ := build(t, "linha sem pri")
	if m["pri-source"] != "default" || m["pri"] != float64(13) {
		t.Errorf("pri = %v %v", m["pri"], m["pri-source"])
	}
	if m["facility-name"] != "user" || m["severity-name"] != "notice" {
		t.Errorf("names = %v %v", m["facility-name"], m["severity-name"])
	}
	if m["rfc"] != "unknown" {
		t.Errorf("rfc = %v", m["rfc"])
	}
}

func TestHeadRoundTrip(t *testing.T) {
	orig := Head{
		ID:        "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
		TS:        1757116800,
		UTS:       123456,
		NTS:       123456789,
		Transport: TransportRELP,
		Framing:   FramingRELP,
		Listener:  "[::]:20514",
		PeerAddr:  netip.MustParseAddr("2001:db8::1"),
		PeerPort:  40000,
		PRI:       190,
		FromWire:  true,
		Truncated: true,
	}
	back, err := DecodeHead(EncodeHead(nil, &orig))
	if err != nil {
		t.Fatal(err)
	}
	if back != orig {
		t.Fatalf("round-trip changed the head\n got %+v\nwant %+v", back, orig)
	}
}

func TestHeadRoundTripUnixPeer(t *testing.T) {
	orig := Head{ID: "x", Transport: TransportUnix, Framing: FramingDatagram, PRI: 13}
	back, err := DecodeHead(EncodeHead(nil, &orig))
	if err != nil {
		t.Fatal(err)
	}
	if back.PeerAddr.IsValid() || back.PeerIPVersion() != 0 {
		t.Fatalf("unix peer came back as %v", back.PeerAddr)
	}
	if back != orig {
		t.Fatalf("got %+v want %+v", back, orig)
	}
}

func TestDecodeHeadRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, []byte(""), []byte("not json"), []byte("[]"), []byte(`{"ts":1}`)} {
		if _, err := DecodeHead(b); err == nil {
			t.Errorf("DecodeHead(%q) accepted", b)
		}
	}
}

// A corrupted record must not be able to write a row whose facility and
// severity are outside every index the destination has.
func TestDecodeHeadClampsPRI(t *testing.T) {
	h, err := DecodeHead([]byte(`{"id":"x","pri":9999,"pri-wire":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if h.PRI != 13 || h.FromWire {
		t.Fatalf("pri = %d wire=%v, want 13 false", h.PRI, h.FromWire)
	}
	if h.Facility() != 1 || h.Severity() != 5 {
		t.Fatalf("facility/severity = %d/%d", h.Facility(), h.Severity())
	}
}

// The ownership rule of SPEC §14.2. A second release means a second caller
// believed it owned the item, and filing the same buffer back twice is what
// puts one emitter's line inside another's event.
func TestReleaseIsIdempotent(t *testing.T) {
	buf, class := GetRaw(100)
	buf = append(buf, "hello"...)
	it := NewItem(Head{ID: "x"}, buf, class)

	if it.Released() {
		t.Fatal("fresh item reports released")
	}
	it.Release()
	if !it.Released() || it.Raw != nil {
		t.Fatal("release did not take effect")
	}
	it.Release() // must be a no-op, not a double pool return
}

// SPEC §14.2, error paths: Discard finishes the item without pooling anything.
// The buffer stays valid for whoever is still reading it and goes to the GC.
func TestDiscardLeavesTheBufferToTheGC(t *testing.T) {
	buf, class := GetRaw(100)
	buf = append(buf, "hello"...)
	it := NewItem(Head{ID: "x"}, buf, class)

	it.Discard()
	if !it.Released() {
		t.Fatal("Discard must mark the item finished so nobody pools it later")
	}
	if it.Raw == nil || string(it.Raw) != "hello" {
		t.Fatal("Discard must not touch Raw: a lower layer may still be reading it")
	}

	// Nothing went back to the pool: a fresh buffer of the same class is a
	// different one. (A pooled buffer would come straight back from the per-P
	// slot on this goroutine.)
	again, aclass := GetRaw(100)
	if aclass == class && &again[:1][0] == &it.Raw[:1][0] {
		t.Fatal("the discarded buffer was handed out again by the pool")
	}
	PutRaw(again, aclass)

	it.Release() // a later Release is a no-op, not a second chance to pool
	if it.Raw == nil {
		t.Fatal("Release after Discard must not pool or clear the buffer")
	}

	var nilItem *Item
	nilItem.Discard() // must not panic
}

// A tainted item is one whose buffer a lower layer may still read after the
// call returned (a timed-out sink write). Its Release hands it to the GC.
func TestTaintedReleaseDoesNotPool(t *testing.T) {
	buf, class := GetRaw(100)
	buf = append(buf, "hello"...)
	it := NewItem(Head{ID: "x"}, buf, class)

	it.Taint()
	it.Release()
	if !it.Released() {
		t.Fatal("Release did not mark the item")
	}
	if it.Raw == nil {
		t.Fatal("a tainted Release must leave the buffer alone; pooling it is the ownership bug")
	}

	again, aclass := GetRaw(100)
	if aclass == class && &again[:1][0] == &it.Raw[:1][0] {
		t.Fatal("the tainted buffer was handed out again by the pool")
	}
	PutRaw(again, aclass)

	var nilItem *Item
	nilItem.Taint() // must not panic
}

func TestSizeCountsOnlyTheRawLine(t *testing.T) {
	it := NewItem(Head{ID: "x"}, make([]byte, 317), -1)
	if it.Size() != 317 {
		t.Fatalf("Size() = %d, want 317: the envelope does not exist while the "+
			"item is in the ring", it.Size())
	}
}

func TestPoolClasses(t *testing.T) {
	for _, n := range []int{0, 1, 2 << 10, 2<<10 + 1, 8 << 10, 64 << 10, 256 << 10} {
		buf, class := GetRaw(n)
		if class < 0 {
			t.Errorf("size %d not pooled", n)
		}
		if cap(buf) < n {
			t.Errorf("size %d got capacity %d", n, cap(buf))
		}
		PutRaw(buf, class)
	}
	// Above the largest class nothing is pooled, so one pathological message
	// cannot make the pool hold its worst case forever.
	buf, class := GetRaw(PoolMaxBytes + 1)
	if class != -1 || cap(buf) < PoolMaxBytes+1 {
		t.Fatalf("oversized: class=%d cap=%d", class, cap(buf))
	}
	PutRaw(buf, class) // must be a no-op
}

func TestSanitiseUTF8(t *testing.T) {
	if got := SanitiseUTF8("ok"); got != "ok" {
		t.Errorf("valid input changed: %q", got)
	}
	if got := SanitiseUTF8("a\xffb"); got != "a�b" {
		t.Errorf("got %q", got)
	}
}

func TestSetTimeIsOneReading(t *testing.T) {
	var h Head
	h.SetTime(time.Unix(1757116800, 999999999))
	if h.TS != 1757116800 || h.NTS != 999999999 || h.UTS != 999999 {
		t.Fatalf("ts=%d uts=%d nts=%d", h.TS, h.UTS, h.NTS)
	}
}
