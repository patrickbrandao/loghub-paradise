package item

import (
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"
)

func sampleHead() Head {
	t := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
	h := Head{
		UUID:        uuidv7.GenerateAt(uuidv7.Level3, t),
		From:        "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
		Space:       "com.acme",
		Transport:   TransportHTTPS,
		Compression: CompressionDeflate,
		SrcIP:       netip.MustParseAddr("2001:db8::1"),
		SrcPort:     51514,
		DstIP:       netip.MustParseAddr("10.0.0.10"),
		DstPort:     12204,
		PeerIP:      netip.MustParseAddr("127.0.0.1"),
		PeerPort:    40000,
		TLS: &TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256",
			PeerSubject: "CN=fw01,O=\"Acme\\, Inc\"", PeerFingerprint: "sha-256:3B:8E"},
	}
	h.SetTime(t)
	return h
}

func TestHeadRoundTrip(t *testing.T) {
	for _, h := range []Head{sampleHead(), func() Head {
		h := sampleHead()
		h.Transport, h.Compression, h.TLS = TransportTCP, CompressionNone, nil
		return h
	}(), func() Head {
		h := sampleHead()
		h.Transport, h.Compression, h.Chunks, h.TLS = TransportUDP, CompressionGzip, 128, nil
		h.DstIP, h.PeerIP, h.PeerPort = netip.Addr{}, netip.Addr{}, 0
		return h
	}(), func() Head {
		h := sampleHead()
		h.Transport, h.Compression, h.Chunks = TransportTLS, CompressionNone, 0
		return h
	}(), func() Head {
		h := sampleHead()
		h.Transport, h.Compression, h.TLS = TransportHTTP, CompressionZlib, nil
		return h
	}()} {
		b := EncodeHead(nil, &h)
		got, err := DecodeHead(b)
		if err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		if !reflect.DeepEqual(got, h) {
			t.Fatalf("round trip:\n got %+v\nwant %+v\n%s", got, h, b)
		}
	}
}

// hd builds a head in the codec's text form: the fixed prefix plus rest.
func hd(rest string) string {
	return `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8",` +
		`"space":"default","uts":1767225600,"sub":123000000,` + rest + `}`
}

const netKeys = `"src-ip":"10.0.0.1","src-port":1,"dst-port":2`

func TestHeadRefusals(t *testing.T) {
	base := string(EncodeHead(nil, func() *Head { h := sampleHead(); return &h }()))
	tail := func(uuid, space, uts, sub string) string {
		return `{"v":1,"uuid":"` + uuid + `","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"` + space +
			`","uts":` + uts + `,"sub":` + sub + `,"transport":"tcp",` + netKeys + `}`
	}
	const u7 = "019b76da-a87b-71c8-b150-000000000000"
	cases := map[string]string{
		"version":                `{"v":2}`,
		"unknown key":            base[:len(base)-1] + `,"x":1}`,
		"framing is gone":        base[:len(base)-1] + `,"framing":"datagram"}`,
		"size is gone":           base[:len(base)-1] + `,"size":1}`,
		"trailing":               base + ` {}`,
		"not json":               `{`,
		"udp with tls":           hd(`"transport":"udp",` + netKeys + `,"tls":{"version":"TLS1.3"}`),
		"mapped ip":              hd(`"transport":"udp","src-ip":"::ffff:10.0.0.1","src-port":1,"dst-port":2`),
		"no addresses":           hd(`"transport":"tcp"`),
		"unix is gone":           hd(`"transport":"unix"`),
		"uuid v4":                tail("019b76da-a87b-41c8-b150-000000000000", "default", "1767225600", "123000000"),
		"space empty seg":        tail(u7, "a..b", "1767225600", "123000000"),
		"port":                   hd(`"transport":"udp","src-ip":"10.0.0.1","src-port":70000,"dst-port":2`),
		"sub":                    tail(u7, "default", "1767225600", "1000000000"),
		"tcp compressed":         hd(`"transport":"tcp","compression":"gzip",` + netKeys),
		"udp deflate":            hd(`"transport":"udp","compression":"deflate",` + netKeys),
		"unknown compression":    hd(`"transport":"http","compression":"br",` + netKeys),
		"empty compression":      hd(`"transport":"http","compression":"",` + netKeys),
		"tcp chunks":             hd(`"transport":"tcp","chunks":2,` + netKeys),
		"http chunks":            hd(`"transport":"http","chunks":2,` + netKeys),
		"chunks 0":               hd(`"transport":"udp","chunks":0,` + netKeys),
		"chunks 129":             hd(`"transport":"udp","chunks":129,` + netKeys),
		"udp peer":               hd(`"transport":"udp",` + netKeys + `,"peer-ip":"10.0.0.9","peer-port":3`),
		"peer without port":      hd(`"transport":"tcp",` + netKeys + `,"peer-ip":"10.0.0.9"`),
		"tls on http":            hd(`"transport":"http",` + netKeys + `,"tls":{"version":"TLS1.3"}`),
		"uuid instant":           tail(u7, "default", "1", "0"),
		"uuid instant, 1 ms off": tail(u7, "default", "1767225600", "124000000"),
	}
	// Valid controls in the same shapes as the refusals: without them, a rule
	// that refused everything would pass this test (AGENTS.md §9).
	for _, control := range []string{
		tail(u7, "default", "1767225600", "123999999"),
		hd(`"transport":"udp","compression":"zlib","chunks":128,` + netKeys),
		hd(`"transport":"https","compression":"deflate",` + netKeys + `,"peer-ip":"10.0.0.9","peer-port":3,"tls":{"version":"TLS1.2"}`),
		hd(`"transport":"http","compression":"gzip",` + netKeys),
		hd(`"transport":"tls",` + netKeys + `,"tls":{"version":"TLS1.2"}`),
	} {
		if _, err := DecodeHead([]byte(control)); err != nil {
			t.Fatalf("control refused: %v\n%s", err, control)
		}
	}
	for name, s := range cases {
		if _, err := DecodeHead([]byte(s)); !errors.Is(err, ErrHead) {
			t.Errorf("%s: accepted (%v)", name, err)
		}
	}
}

// FuzzHead (SPEC §18.3): the decoder never panics, and whatever it accepts
// encodes back to bytes that decode to the same head.
func FuzzHead(f *testing.F) {
	h := sampleHead()
	f.Add(EncodeHead(nil, &h))
	f.Add([]byte(`{"v":1}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := DecodeHead(b)
		if err != nil {
			return
		}
		again, err := DecodeHead(EncodeHead(nil, &h))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !reflect.DeepEqual(again, h) {
			t.Fatalf("not stable: %+v vs %+v", again, h)
		}
	})
}

func TestCanonical(t *testing.T) {
	cases := map[string]string{
		"::ffff:10.0.0.3":      "10.0.0.3",
		"2001:DB8:0:0:0:0:0:1": "2001:db8::1",
		"fe80::1%eth0":         "fe80::1",
		"0.0.0.0":              "invalid IP",
		"::":                   "invalid IP",
	}
	for in, want := range cases {
		if got := Canonical(netip.MustParseAddr(in)).String(); got != want {
			t.Errorf("%s: %s want %s", in, got, want)
		}
	}
}

func TestReleaseOnceAndTaint(t *testing.T) {
	buf, class := CopyRaw([]byte("hello"))
	it := New(Head{}, buf, class)
	it.Release()
	it.Release()
	if !it.Released() || it.Raw != nil {
		t.Fatal("release")
	}
	buf, class = CopyRaw([]byte("x"))
	it = New(Head{}, buf, class)
	it.Taint()
	it.Release()
	if it.Raw == nil {
		t.Fatal("a tainted buffer was handed back to the pool")
	}
	it = New(Head{}, []byte("y"), -1)
	it.Discard()
	if !it.Released() || !it.Tainted() || it.Raw == nil {
		t.Fatal("discard")
	}
}
