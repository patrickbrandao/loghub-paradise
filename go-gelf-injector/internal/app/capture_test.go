package app

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelftest"
)

// TestRealCaptures replays captures taken with tcpdump in a Linux container
// (TESTING §3.2) and checks the replay carries exactly the documents the
// live strict receiver accepted while the capture ran (the .jsonl next to
// each capture): the injector's own UDP chunked+gzip, UDP zlib, TCP and HTTP
// gzip traffic; Docker's gelf log driver (UDP gzip, chunked, and TCP); and
// fluent-bit's gelf output (UDP gzip, chunked, and TCP); real IP fragments
// (2026-11-07: tcpdump -i eth0 on a Docker bridge, MTU 1500, the injector in
// another container: chunks of 8192 and unchunked datagrams of 5000 bytes
// fragmented by the kernel, SPEC §6.2); and tcpdump -i any on loopback,
// where every datagram is captured twice (outgoing and incoming) and a
// sender repeats one document five times (SPEC §6.3.1).
func TestRealCaptures(t *testing.T) {
	check := map[string]func(t *testing.T, src map[string]uint64){
		"ip-fragments": func(t *testing.T, src map[string]uint64) {
			if src["pcap_ip_fragments"] < 100 || src["pcap_ip_reassembled"] != 50 || src["pcap_ip_fragments_lost"] != 0 {
				t.Fatalf("fragments: %v", src)
			}
		},
		"any-bridge": func(t *testing.T, src map[string]uint64) {
			if src["udp_datagrams_duplicate"] != 55 || src["udp_datagrams"] != 55 {
				t.Fatalf("copies: %v", src)
			}
		},
	}
	for _, name := range []string{"injector", "docker-gelf-driver", "fluent-bit", "ip-fragments", "any-bridge"} {
		t.Run(name, func(t *testing.T) {
			base := filepath.Join("testdata", "captures", name)
			b, err := os.ReadFile(base + ".jsonl")
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]int{}
			total := 0
			for _, l := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
				want[string(l)]++
				total++
			}
			for _, tr := range []string{"tcp", "udp", "http"} {
				var col *gelftest.Collector
				var addr string
				switch tr {
				case "tcp":
					r, _ := gelftest.ListenTCP(false, "", false)
					defer r.Close()
					col, addr = r.C, r.Addr
				case "udp":
					r, _ := gelftest.ListenUDP("")
					defer r.Close()
					col, addr = r.C, r.Addr
				case "http":
					r, _ := gelftest.ListenHTTP(false, "", false)
					defer r.Close()
					col, addr = r.C, r.Addr
				}
				code, s, se := run(t, "--transport", tr, "--target", addr, "--pcap", base+".pcap", "--rate", "20000")
				mustOK(t, code, s, se)
				col.Wait(total, 5*time.Second)
				col.Settle(100*time.Millisecond, time.Second)
				got := map[string]int{}
				for _, d := range col.Docs() {
					got[string(d)]++
				}
				for d, n := range want {
					if got[d] != n {
						t.Fatalf("%s: seen %d times, want %d: %.150q", tr, got[d], n, d)
					}
				}
				if col.N() != total || s.Source["frames_lost"] != 0 || s.Source["udp_chunked_incomplete"] != 0 {
					t.Fatalf("%s: %d documents, want %d; source %v", tr, col.N(), total, s.Source)
				}
				if c := check[name]; c != nil {
					c(t, s.Source)
				}
			}
		})
	}
}
