package gelftest

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestSink serves the strict receivers for an outside capture (TESTING
// §3.2): GELF_SINK="udp=127.0.0.1:12201,tcp=127.0.0.1:12201,http=127.0.0.1:12202",
// until the file GELF_SINK_STOP exists, then writes every document to
// GELF_SINK_OUT (one per line) and prints the counters. Built for Linux
// with `go test -c` and run in the tcpdump container.
func TestSink(t *testing.T) {
	spec := os.Getenv("GELF_SINK")
	if spec == "" {
		t.Skip("GELF_SINK=udp=addr,tcp=addr,http=addr serves the receivers")
	}
	var cols []*Collector
	var names []string
	for _, kv := range strings.Split(spec, ",") {
		k, addr, _ := strings.Cut(kv, "=")
		switch k {
		case "udp":
			u, err := ListenUDP(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer u.Close()
			cols, names = append(cols, u.C), append(names, k)
		case "tcp":
			s, err := listenTCPAt(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			cols, names = append(cols, s.C), append(names, k)
		case "http":
			h, err := listenHTTPAt(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			cols, names = append(cols, h.C), append(names, k)
		}
	}
	fmt.Println("sink ready")
	stop := os.Getenv("GELF_SINK_STOP")
	for {
		if _, err := os.Stat(stop); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	f, err := os.Create(os.Getenv("GELF_SINK_OUT"))
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for i, c := range cols {
		fmt.Printf("%s: %d documents, chunked %d, gzip %d, zlib %d, incomplete %d, invalid %d\n", names[i], c.N(),
			c.Counter("chunked"), c.Counter("gzip"), c.Counter("zlib"), c.Counter("incomplete"), c.Counter("invalid"))
		for _, d := range c.Docs() {
			w.Write(d)
			w.WriteByte('\n')
		}
	}
	w.Flush()
	f.Close()
}
