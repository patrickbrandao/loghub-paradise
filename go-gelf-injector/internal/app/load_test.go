package app

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelftest"
)

// TestLoad sends 100 000 messages over each transport into the strict
// receivers and checks every seq arrived exactly once (TESTING §3.1). Off
// by default: GELF_TEST_LOAD=1 (GELF_TEST_LOAD_N changes the count).
func TestLoad(t *testing.T) {
	if os.Getenv("GELF_TEST_LOAD") == "" {
		t.Skip("GELF_TEST_LOAD=1 runs the 100k load test")
	}
	n := 100000
	if v, err := strconv.Atoi(os.Getenv("GELF_TEST_LOAD_N")); err == nil && v > 0 {
		n = v
	}
	ns := strconv.Itoa(n)
	dir := t.TempDir()
	cases := []struct {
		name string
		args func() ([]string, *gelftest.Collector, func())
	}{
		{"udp-chunked-gzip", func() ([]string, *gelftest.Collector, func()) {
			r, err := gelftest.ListenUDP("")
			if err != nil {
				t.Fatal(err)
			}
			// 20 000 random bytes: ~15 KB of gzip, 2 chunks of 8192. Paced:
			// loopback UDP drops at the kernel past what the receiver reads.
			return []string{"--transport", "udp", "--target", r.Addr, "--payload-size", "20000", "--concurrency", "2",
				"--rate", envOr("GELF_TEST_LOAD_UDP_RATE", "20000")}, r.C, r.Close
		}},
		{"tcp", func() ([]string, *gelftest.Collector, func()) {
			r, err := gelftest.ListenTCP(false, "", false)
			if err != nil {
				t.Fatal(err)
			}
			return []string{"--transport", "tcp", "--target", r.Addr, "--concurrency", "4"}, r.C, r.Close
		}},
		{"tls", func() ([]string, *gelftest.Collector, func()) {
			r, err := gelftest.ListenTCP(true, dir, false)
			if err != nil {
				t.Fatal(err)
			}
			return []string{"--transport", "tls", "--target", r.Addr, "--tls-ca", r.CA, "--concurrency", "4"}, r.C, r.Close
		}},
		{"http-pipeline", func() ([]string, *gelftest.Collector, func()) {
			r, err := gelftest.ListenHTTP(false, "", false)
			if err != nil {
				t.Fatal(err)
			}
			return []string{"--transport", "http", "--target", r.Addr, "--concurrency", "4", "--pipeline", "32"}, r.C, r.Close
		}},
		{"http-gzip", func() ([]string, *gelftest.Collector, func()) {
			r, err := gelftest.ListenHTTP(false, "", false)
			if err != nil {
				t.Fatal(err)
			}
			return []string{"--transport", "http", "--target", r.Addr, "--concurrency", "4", "--pipeline", "32", "--compress", "gzip"}, r.C, r.Close
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args, col, stop := c.args()
			defer stop()
			runID := strings.ReplaceAll(c.name, "-", "")
			code, s, se := run(t, append(args, "--count", ns, "--run", runID)...)
			mustOK(t, code, s, se)
			col.Wait(n, 30*time.Second)
			if err := gelftest.CheckSeqs(col.Docs(), runID, n); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: %d messages in %.2fs = %.0f msg/s, %.1f MB/s; latency p50 %.0fus p99 %.0fus; received %d",
				c.name, s.Messages, s.ElapsedSec, s.RateMsgs, s.RateBytes/1e6, s.Latency.P50, s.Latency.P99, col.N())
		})
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// TestReplayCapture replays a real capture (GELF_TEST_PCAP) over TCP, UDP
// and HTTP into the strict receivers, and checks the generator runs it
// holds: GELF_TEST_PCAP_RUNS="run:count,run:count" -- every seq of each run
// exactly once (TESTING §3.2). A count of 0 is a run the capture caught in
// the middle: its seqs must be distinct and contiguous, ending at the last.
func TestReplayCapture(t *testing.T) {
	file := os.Getenv("GELF_TEST_PCAP")
	if file == "" {
		t.Skip("GELF_TEST_PCAP=<capture> GELF_TEST_PCAP_RUNS=run:n,... replays a real capture")
	}
	runs := map[string]int{}
	total, hasTail := 0, false
	var expect map[string]int // GELF_TEST_PCAP_EXPECT: what the live receiver got
	if f := os.Getenv("GELF_TEST_PCAP_EXPECT"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		expect = map[string]int{}
		for _, l := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
			expect[string(l)]++
			total++
		}
	}
	for _, kv := range strings.Split(os.Getenv("GELF_TEST_PCAP_RUNS"), ",") {
		if expect != nil {
			break
		}
		k, v, _ := strings.Cut(kv, ":")
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("GELF_TEST_PCAP_RUNS: %q", kv)
		}
		runs[k] = n
		total += n
		hasTail = hasTail || n == 0
	}
	check := func(t *testing.T, col *gelftest.Collector) {
		col.Wait(total, 20*time.Second)
		col.Settle(300*time.Millisecond, 3*time.Second)
		if expect != nil {
			// The replay must carry exactly the documents the live receiver
			// accepted when the capture was taken.
			got := map[string]int{}
			for _, d := range col.Docs() {
				got[string(d)]++
			}
			for d, n := range expect {
				if got[d] != n {
					t.Errorf("document seen %d times, want %d: %.150q", got[d], n, d)
				}
			}
			if col.N() != total {
				t.Errorf("received %d documents, want %d", col.N(), total)
			}
			return
		}
		by := map[string][][]byte{}
		for _, d := range col.Docs() {
			for r := range runs {
				if bytes.Contains(d, []byte(`"_run":"`+r+`"`)) {
					by[r] = append(by[r], d)
				}
			}
		}
		for r, n := range runs {
			if n == 0 {
				if err := gelftest.CheckTail(by[r], r); err != nil {
					t.Errorf("run %s: %v", r, err)
				} else {
					t.Logf("run %s: %d documents, a contiguous tail", r, len(by[r]))
				}
				continue
			}
			if err := gelftest.CheckSeqs(by[r], r, n); err != nil {
				t.Errorf("run %s: %v", r, err)
			}
		}
		if col.N() < total || !hasTail && col.N() != total {
			t.Errorf("received %d documents, want %d", col.N(), total)
		}
	}
	t.Run("tcp", func(t *testing.T) {
		r, _ := gelftest.ListenTCP(false, "", false)
		defer r.Close()
		code, s, se := run(t, "--target", r.Addr, "--pcap", file)
		mustOK(t, code, s, se)
		t.Logf("source %v", s.Source)
		check(t, r.C)
	})
	t.Run("udp", func(t *testing.T) {
		r, _ := gelftest.ListenUDP("")
		defer r.Close()
		code, s, se := run(t, "--transport", "udp", "--target", r.Addr, "--pcap", file, "--rate", "20000")
		mustOK(t, code, s, se)
		check(t, r.C)
	})
	t.Run("http", func(t *testing.T) {
		r, _ := gelftest.ListenHTTP(false, "", false)
		defer r.Close()
		code, s, se := run(t, "--transport", "http", "--target", r.Addr, "--pcap", file, "--pipeline", "8", "--compress", "gzip")
		mustOK(t, code, s, se)
		check(t, r.C)
	})
}
