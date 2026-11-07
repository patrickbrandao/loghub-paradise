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

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/pcap/pcaptest"
)

// TestPcapClosedTail: the octet-counting frame a FIN cut is replayed with
// what arrived, as the syslog server delivered it; the same bytes in a
// direction the capture cut are lost, and a RELP frame without its trailer
// is lost either way (SPEC §6.4, §6.6; from go-gelf-injector, 2026-11-07).
func TestPcapClosedTail(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	cli, srv := netip.MustParseAddrPort("10.0.0.1:40000"), netip.MustParseAddrPort("10.0.0.2:514")
	for _, c := range []struct {
		body      string
		fin, want string
	}{
		{"5 <13>a40 <13>cut by the fin", "<13>a|<13>cut by the fin", "<13>a"},
		{"1 syslog 5 <13>a\n2 syslog 9 <13>b", "<13>a", "<13>a"},
	} {
		for _, fin := range []bool{true, false} {
			conn := pcaptest.Conn{Link: pcaptest.Ethernet, Src: cli, Dst: srv, ISN: 9, T0: t0}
			file := filepath.Join(t.TempDir(), "tail.pcap")
			if err := os.WriteFile(file, pcaptest.Classic(pcaptest.Ethernet, conn.Frames(conn.Split([]byte(c.body), 10), false, !fin)), 0o600); err != nil {
				t.Fatal(err)
			}
			p, err := NewPcap(file, PcapOptions{UDP: true, TCP: true})
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
				got = append(got, string(m.Data))
			}
			p.Close()
			want := c.want
			if fin {
				want = c.fin
			}
			if strings.Join(got, "|") != want {
				t.Fatalf("%q fin=%v: %q, want %q", c.body, fin, got, want)
			}
		}
	}
}
