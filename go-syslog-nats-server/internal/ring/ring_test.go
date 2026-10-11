package ring

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/item"
)

// Helpers shared by the tests of the ring.

const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

var testBase = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// mkItem builds item i with a raw line of size bytes. The index is recoverable
// from the clock sample (idx), so tests can check order and identity without
// keeping the random part of the uuid around.
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

func idx(it *item.Item) int { return int(it.Head.Time().Sub(testBase) / time.Microsecond) }

// The memory ring is the Ring the app and the receiver are wired to.
func TestMemoryImplementsRing(t *testing.T) {
	var _ Ring = NewMemory(1, 0, 0)
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
	// No byte ceiling must not read as saturated.
	if (Stats{Items: 0, CapItems: 2048}).Saturated(0.80) {
		t.Error("empty ring reported saturated")
	}
}
