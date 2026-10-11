package convert

import (
	"net/netip"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlhcheck"
)

var benchLines = map[string]string{
	"rfc3164": "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
	"rfc5424": `<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry...`,
}

func benchHead() item.Head {
	t := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
	h := item.Head{UUID: jlh.NewUUID(t), From: vectorFrom, Space: vectorSpace,
		Transport: item.TransportUDP, Framing: item.FramingDatagram,
		SrcIP: netip.MustParseAddr("10.0.0.3"), SrcPort: 59922,
		DstIP: netip.MustParseAddr("10.0.0.10"), DstPort: 514}
	h.SetTime(t)
	return h
}

// BenchmarkConvert is the cost the drain worker pays per message (SPEC §2.2).
func BenchmarkConvert(b *testing.B) {
	for name, line := range benchLines {
		b.Run(name, func(b *testing.B) {
			c := New(DefaultConfig())
			h := benchHead()
			raw := []byte(line)
			h.Size = len(raw)
			buf := make([]byte, 0, 4096)
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				buf, _ = c.Append(buf[:0], &h, raw)
			}
		})
	}
}

// BenchmarkSchemaValidate is the number behind deviation D3 (SPEC §18.2): what
// validating every envelope against both schemas would add per message, next
// to BenchmarkConvert.
func BenchmarkSchemaValidate(b *testing.B) {
	for name, line := range benchLines {
		b.Run(name, func(b *testing.B) {
			h := benchHead()
			raw := []byte(line)
			h.Size = len(raw)
			env, _ := New(DefaultConfig()).Append(nil, &h, raw)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := jlhcheck.Validate(env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
