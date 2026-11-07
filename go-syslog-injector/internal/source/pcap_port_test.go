package source

import (
	"errors"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap/pcaptest"
)

// TestPcapClosedTail: the octet-counting frame a FIN cut is replayed with
// what arrived, as the server delivered it; the same bytes in a direction
// the capture cut are lost (SPEC §6.4, §6.6) -- with and without
// --pcap-flows, whose wrapper must pass the end on.
func TestPcapClosedTail(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli, srv := netip.MustParseAddrPort("10.0.0.1:40000"), netip.MustParseAddrPort("10.0.0.2:514")
	body := []byte("5 <13>a40 <13>cut by the fin")
	for _, flows := range []bool{false, true} {
		for _, fin := range []bool{true, false} {
			c := pcaptest.Conn{Link: pcaptest.Ethernet, Src: cli, Dst: srv, ISN: 9, T0: t0}
			file := filepath.Join(t.TempDir(), "tail.pcap")
			if err := os.WriteFile(file, pcaptest.Classic(pcaptest.Ethernet, c.Frames(c.Split(body, 10), false, !fin)), 0o600); err != nil {
				t.Fatal(err)
			}
			p, err := NewPcap(file, PcapOptions{UDP: true, TCP: true, Flows: flows})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for {
				m, err := p.Next(1, time.Now())
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if !m.FlowEnd {
					got = append(got, string(m.Data))
				}
			}
			p.Close()
			want := "<13>a"
			if fin {
				want = "<13>a|<13>cut by the fin"
			}
			if strings.Join(got, "|") != want {
				t.Fatalf("flows=%v fin=%v: %q, want %q", flows, fin, got, want)
			}
		}
	}
}

// TestPcapFragmentsWaitingAtEndAreLost: the fragments of a datagram still
// incomplete when the pass ends count in pcap_ip_fragments_lost (SPEC §6.3;
// ported from go-relplog-injector on 2026-11-07: they vanished uncounted).
func TestPcapFragmentsWaitingAtEndAreLost(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli, srv := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2")
	l4 := pcaptest.UDP(40000, 514, []byte("<13>"+strings.Repeat("y", 4000)))
	frags := pcaptest.Fragments4(cli, srv, 17, l4, 5, 1480)
	whole := pcaptest.IP(cli, srv, 17, pcaptest.UDP(40000, 514, []byte("<13>whole")), 6)
	var frames []pcaptest.Frame
	for i, ip := range [][]byte{frags[0], whole, frags[1]} { // the last fragment never comes
		frames = append(frames, pcaptest.Frame{TS: t0.Add(time.Duration(i) * time.Millisecond), Data: pcaptest.Link(pcaptest.Ethernet, ip)})
	}
	file := filepath.Join(t.TempDir(), "frag.pcap")
	if err := os.WriteFile(file, pcaptest.Classic(pcaptest.Ethernet, frames), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := NewPcap(file, PcapOptions{UDP: true, TCP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	n := 0
	for {
		_, err := p.Next(1, time.Now())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 1 || p.Stats()["pcap_ip_fragments_lost"] != 2 {
		t.Fatalf("%d messages, stats %v", n, p.Stats())
	}
}
