package ring

import (
	"bytes"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
)

// Helpers shared by the tests of every mode.

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

var testBase = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// mkItem builds item i with a raw line of size bytes. The index is recoverable
// from the clock sample (idx), so tests can check order and identity without
// keeping the random part of the uuid around. The head is valid for the codec
// of SPEC §3.4: the persistent modes really encode and decode it.
func mkItem(i int, size int) *item.Item {
	t := testBase.Add(time.Duration(i) * time.Microsecond)
	h := item.Head{
		UUID:      uuidv7.GenerateAt(uuidv7.Level3, t),
		From:      testFrom,
		Space:     "default",
		Transport: item.TransportTCP,
		Framing:   item.FramingNonTransparent,
		SrcIP:     netip.MustParseAddr("10.0.0.7"),
		SrcPort:   uint16(40000 + i%1000),
		DstIP:     netip.MustParseAddr("10.0.0.10"),
		DstPort:   1514,
		Size:      size,
	}
	h.SetTime(t)
	return item.New(h, bytes.Repeat([]byte{'a'}, size), -1)
}

// mkTLSItem is an item whose head uses every optional key, for round trips.
func mkTLSItem(i int, raw string) *item.Item {
	it := mkItem(i, 0)
	it.Head.Transport = item.TransportTLS
	it.Head.Framing = item.FramingOctetCounting
	it.Head.SrcIP = netip.MustParseAddr("2001:db8::1")
	it.Head.PeerIP = netip.MustParseAddr("127.0.0.1")
	it.Head.PeerPort = 41000
	it.Head.TLS = &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256",
		PeerSubject: "CN=fw01", PeerFingerprint: "sha-256:3B:8E"}
	it.Head.Size = len(raw) + 100 // truncated on the way in
	it.Raw = []byte(raw)
	return it
}

func idx(it *item.Item) int { return int(it.Head.Time().Sub(testBase) / time.Microsecond) }

// Every mode must satisfy the same interface, so app/ never branches on mode.
func TestAllModesImplementRing(t *testing.T) {
	var _ Ring = NewMemory(1, 0, 0)
	var _ Ring = NewNone(&fakeWriter{}, 1, time.Second)
	var _ Ring = (*RedisRing)(nil)
	r, _ := newSQLiteRing(t, 1, 0)
	var _ Ring = r
}

func TestSaturated(t *testing.T) {
	s := Stats{Items: 80, CapItems: 100}
	if !s.Saturated(0.80) {
		t.Error("80/100 not saturated at 0.80")
	}
	if s.Saturated(0.81) {
		t.Error("80/100 saturated at 0.81")
	}
	b := Stats{Bytes: 90, CapBytes: 100}
	if !b.Saturated(0.80) {
		t.Error("byte saturation not detected")
	}
	// ring=none reports no byte ceiling; that must not read as saturated.
	if (Stats{Items: 0, CapItems: 2048}).Saturated(0.80) {
		t.Error("empty ring reported saturated")
	}
}
