package source

import (
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/pcap/pcaptest"
)

// TestPcapFragmentCounters (SPEC §6.3, ported from go-syslog-injector on
// 2026-11-07): a TCP segment of a request fragmented by IP, one fragment cut
// by the snaplen, is one condemned datagram (pcap_ip_datagrams_snaplen) and
// a TCP gap -- the request it carried is not replayed, the next one is; the
// fragments of another datagram still waiting when the pass ends count in
// pcap_ip_fragments_lost.
func TestPcapFragmentCounters(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli, srv := netip.MustParseAddrPort("10.0.0.1:40000"), netip.MustParseAddrPort("10.0.0.2:8080")
	req := func(body string) []byte {
		return []byte("POST /hook HTTP/1.1\r\nHost: x\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body)
	}
	r1, r2, r3 := req("one"), req(strings.Repeat("z", 4000)), req("three")
	const isn = 1000
	seq1 := uint32(isn + 1)
	seq2 := seq1 + uint32(len(r1))
	seq3 := seq2 + uint32(len(r2))
	tcp := func(seq uint32, flags uint8, p []byte) []byte {
		return pcaptest.TCP(cli.Port(), srv.Port(), seq, flags, p)
	}
	var frames []pcaptest.Frame
	at := t0
	add := func(ip []byte, snap int) {
		frames = append(frames, pcaptest.Frame{TS: at, Data: pcaptest.Link(pcaptest.Ethernet, ip), Snap: snap})
		at = at.Add(time.Millisecond)
	}
	add(pcaptest.IP(cli.Addr(), srv.Addr(), 6, tcp(isn, pcaptest.SYN, nil), 1), 0)
	add(pcaptest.IP(cli.Addr(), srv.Addr(), 6, tcp(seq1, pcaptest.ACK|pcaptest.PSH, r1), 2), 0)
	frags := pcaptest.Fragments4(cli.Addr(), srv.Addr(), 6, tcp(seq2, pcaptest.ACK|pcaptest.PSH, r2), 3, 1480)
	if len(frags) != 3 {
		t.Fatalf("%d fragments", len(frags))
	}
	add(frags[0], 0)
	add(frags[1], 14+20+100) // the snaplen cut it
	add(frags[2], 0)
	add(pcaptest.IP(cli.Addr(), srv.Addr(), 6, tcp(seq3, pcaptest.ACK|pcaptest.PSH, r3), 4), 0)
	add(pcaptest.IP(cli.Addr(), srv.Addr(), 6, tcp(seq3+uint32(len(r3)), pcaptest.FIN|pcaptest.ACK, nil), 5), 0)
	// Another datagram whose last fragment never comes.
	other := pcaptest.Fragments4(cli.Addr(), srv.Addr(), 17, pcaptest.UDP(1, 2, []byte(strings.Repeat("u", 4000))), 6, 1480)
	add(other[0], 0)
	add(other[1], 0)

	file := filepath.Join(t.TempDir(), "frag.pcap")
	if err := os.WriteFile(file, pcaptest.Classic(pcaptest.Ethernet, frames), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := NewPcap(file, PcapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var got []string
	for {
		m, err := p.Next(1, time.Now())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(m.Body))
	}
	st := p.Stats()
	if len(got) != 2 || got[0] != "one" || got[1] != "three" {
		t.Fatalf("replayed %q, stats %v", got, st)
	}
	// 3 of the condemned datagram, 2 waiting at the end. The segment held
	// a whole request, from boundary to boundary: the gap swallows it with
	// nothing cut to count in http_requests_lost (SPEC §6.3) -- tcp_gaps and
	// tcp_gap_bytes (the whole segment) say so.
	if st["pcap_ip_datagrams_snaplen"] != 1 || st["pcap_ip_fragments_lost"] != 5 ||
		st["tcp_gaps"] != 1 || st["tcp_gap_bytes"] != uint64(len(r2)) || st["http_requests_lost"] != 0 {
		t.Fatalf("stats %v", st)
	}
}
