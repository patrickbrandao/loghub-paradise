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
// fluent-bit's gelf output (UDP gzip, chunked, and TCP).
func TestRealCaptures(t *testing.T) {
	for _, name := range []string{"injector", "docker-gelf-driver", "fluent-bit"} {
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
			}
		})
	}
}
