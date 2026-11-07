package source

import (
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/extract"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
)

// anyCapture is a tcpdump -i any (cooked v2) of one GELF-over-TCP
// connection crossing a NAT: each packet seen at interface 3 with the
// original destination, then at interface 7 with the rewritten one.
func anyCapture(t *testing.T) string {
	t.Helper()
	t0 := time.Unix(1700000000, 0)
	body := []byte("{\"short_message\":\"a\"}\x00{\"short_message\":\"b\"}\x00")
	orig := pcaptest.Conn{Link: pcaptest.SLL2, Src: netip.MustParseAddrPort("172.17.0.2:40000"), Dst: netip.MustParseAddrPort("10.0.0.9:12201"), ISN: 100, T0: t0}
	nat := orig
	nat.Dst = netip.MustParseAddrPort("172.17.0.3:12201")
	a, b := orig.Frames(orig.Split(body, 20), false, false), nat.Frames(nat.Split(body, 20), false, false)
	var frames []pcaptest.Frame
	for i := range a {
		binary.BigEndian.PutUint32(a[i].Data[4:8], 3)
		binary.BigEndian.PutUint32(b[i].Data[4:8], 7)
		b[i].TS = a[i].TS.Add(20 * time.Microsecond)
		frames = append(frames, a[i], b[i])
	}
	p := filepath.Join(t.TempDir(), "any.pcap")
	if err := os.WriteFile(p, pcaptest.Classic(pcaptest.SLL2, frames), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func docs(t *testing.T, path string, opt PcapOptions) ([]string, map[string]uint64) {
	t.Helper()
	opt.TCP, opt.UDP = true, true
	pc, err := NewPcap(path, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	var out []string
	for {
		m, err := pc.Next(1, time.Now())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(m.Data))
	}
	sort.Strings(out)
	return out, pc.Stats()
}

// The TCP copies of -i any are deduplicated per packet before the assembler
// (review of 2026-11-07, item 7): with NAT the copy is another 4-tuple and
// was replayed as a second connection. --pcap-dedup on is the same rule
// (alignment 8(b)); off keeps both.
func TestTCPCopiesOfAnyDeduplicated(t *testing.T) {
	p := anyCapture(t)
	for _, mode := range []extract.DedupMode{extract.DedupAuto, extract.DedupOn} {
		got, st := docs(t, p, PcapOptions{Dedup: mode})
		if len(got) != 2 || st["tcp_segments_duplicate"] == 0 || st["tcp_directions"] != 1 {
			t.Fatalf("mode %d: %q, stats %v", mode, got, st)
		}
	}
	got, st := docs(t, p, PcapOptions{Dedup: extract.DedupOff})
	if len(got) != 4 || st["tcp_segments_duplicate"] != 0 {
		t.Fatalf("off: %q, stats %v", got, st)
	}
}
