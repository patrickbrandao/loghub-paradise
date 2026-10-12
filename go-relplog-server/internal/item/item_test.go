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
		UUID:      uuidv7.GenerateAt(uuidv7.Level3, t),
		From:      "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
		Space:     "com.acme",
		Transport: TransportTLS,
		Framing:   FramingOctetCounting,
		SrcIP:     netip.MustParseAddr("2001:db8::1"),
		SrcPort:   51514,
		DstIP:     netip.MustParseAddr("10.0.0.10"),
		DstPort:   6514,
		PeerIP:    netip.MustParseAddr("127.0.0.1"),
		PeerPort:  40000,
		TLS: &TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256",
			PeerSubject: "CN=fw01,O=\"Acme\\, Inc\"", PeerFingerprint: "sha-256:3B:8E"},
		Size: 70000,
	}
	h.SetTime(t)
	return h
}

func TestHeadRoundTrip(t *testing.T) {
	for _, h := range []Head{sampleHead(), func() Head {
		h := sampleHead()
		h.Transport, h.Framing, h.TLS = TransportUnix, FramingDatagram, nil
		h.SrcIP, h.SrcPort, h.DstIP, h.DstPort, h.PeerIP, h.PeerPort = netip.Addr{}, 0, netip.Addr{}, 0, netip.Addr{}, 0
		return h
	}(), func() Head {
		h := sampleHead()
		h.Transport, h.Framing, h.TLS = TransportUDP, FramingDatagram, nil
		h.DstIP, h.PeerIP, h.PeerPort = netip.Addr{}, netip.Addr{}, 0
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

func TestHeadRefusals(t *testing.T) {
	base := string(EncodeHead(nil, func() *Head { h := sampleHead(); return &h }()))
	cases := map[string]string{
		"version":         `{"v":2}`,
		"unknown key":     base[:len(base)-1] + `,"x":1}`,
		"trailing":        base + ` {}`,
		"not json":        `{`,
		"empty":           ``,
		"udp with tls":    `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"udp","framing":"datagram","src-ip":"10.0.0.1","src-port":1,"dst-port":2,"tls":{"version":"TLS1.3"},"size":1}`,
		"mapped ip":       `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"udp","framing":"datagram","src-ip":"::ffff:10.0.0.1","src-port":1,"dst-port":2,"size":1}`,
		"unix with ip":    `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"unix","framing":"datagram","src-ip":"10.0.0.1","size":1}`,
		"tcp datagram":    `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"tcp","framing":"datagram","src-ip":"10.0.0.1","src-port":1,"dst-port":2,"size":1}`,
		"uuid v4":         `{"v":1,"uuid":"019b76da-a87b-41c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"unix","framing":"datagram","size":1}`,
		"space empty seg": `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"a..b","uts":1767225600,"sub":123000000,"transport":"unix","framing":"datagram","size":1}`,
		"port":            `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123000000,"transport":"udp","framing":"datagram","src-ip":"10.0.0.1","src-port":70000,"dst-port":2,"size":1}`,
		"sub":             `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":1000000000,"transport":"unix","framing":"datagram","size":1}`,
	}
	// Review finding: a head whose uuid instant is not its clock sample. The
	// uuid is 2026-01-01T00:00:00.123Z; the envelope would carry an event in
	// 1970 under it.
	cases["uuid instant"] = `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1,"sub":0,"transport":"unix","framing":"datagram","size":1}`
	cases["uuid instant, 1 ms off"] = `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":124000000,"transport":"unix","framing":"datagram","size":1}`

	// The control: the same head, valid, decodes. Without it every case above
	// could be refused for the instant alone and prove nothing about its name.
	control := `{"v":1,"uuid":"019b76da-a87b-71c8-b150-000000000000","from":"01962a3f-07bc-70b4-ae05-c57e7541d8a8","space":"default","uts":1767225600,"sub":123999999,"transport":"unix","framing":"datagram","size":1}`
	if _, err := DecodeHead([]byte(control)); err != nil {
		t.Fatalf("control refused: %v", err)
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
