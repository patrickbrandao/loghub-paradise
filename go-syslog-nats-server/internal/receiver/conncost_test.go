package receiver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ring"
)

// The fixed cost of a connection (a known pendência, fixed on 2026-10-14 as in
// go-syslog-server; SPEC §5.4, §15.1). Every TCP connection holds a
// bufio.Reader from the accept for as long as it lives, idle or not, outside
// ring.max_bytes; with max_conns = 0 (the default) a few thousand silent
// connections held 64 KiB each. The reader is 4 KiB now, and the frame buffer
// is not kept above 16 KiB between frames.

// heapPerConn opens n idle connections with open, waits until the transport
// counts them active, and returns the growth of the live heap per connection.
// The client ends live in the same process and are counted too, which only
// makes the bound stricter.
func heapPerConn(t *testing.T, h *harness, tr obs.Transport, n int, open func() net.Conn) int64 {
	t.Helper()
	var ms runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	before := ms.HeapAlloc

	conns := make([]net.Conn, 0, n)
	for i := 0; i < n; i++ {
		conns = append(conns, open())
	}
	waitFor(t, "every connection active", func() bool { return h.m.ConnsActive[tr].Load() == int64(n) })
	// The reader is allocated right after the accept; give
	// the last ones the time to get there.
	time.Sleep(100 * time.Millisecond)

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	after := ms.HeapAlloc
	runtime.KeepAlive(conns)
	if after < before {
		return 0
	}
	return int64(after-before) / int64(n)
}

func TestIdleTCPConnectionFixedCost(t *testing.T) {
	const n = 200
	for _, c := range []struct {
		name  string
		proxy ProxyMode
	}{{"plain", ProxyOff}, {"proxy optional", ProxyOptional}} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, 10)
			cfg := TCPConfig{}
			if c.proxy != ProxyOff {
				cidrs, _ := ParseCIDRList("127.0.0.1/32")
				// A header arrives at once, so the reader gets past the PROXY
				// wait and parks in the framer, the state an idle connection
				// lives in.
				cfg.Proxy = ProxyConfig{Mode: c.proxy, Trusted: cidrs, HeaderTimeout: time.Hour}
			}
			tr := startTCP(t, h, cfg)
			per := heapPerConn(t, h, obs.TransportTCP, n, func() net.Conn {
				conn := dial(t, tr.Addr())
				if c.proxy != ProxyOff {
					fmt.Fprint(conn, "PROXY TCP4 192.0.2.10 192.0.2.20 5000 1514\r\n")
				}
				return conn
			})
			t.Logf("live heap per idle %s TCP connection: %d bytes", c.name, per)
			if per > 16<<10 {
				t.Fatalf("an idle TCP connection costs %d bytes of heap; the reader must be 4 KiB (SPEC §5.4)", per)
			}
		})
	}
}

// After a large frame the framer does not keep the large buffer: an idle
// connection must not hold 64 KiB for the rest of its life (SPEC §15.1).
func TestFramerReleasesALargeBuffer(t *testing.T) {
	big := strings.Repeat("a", 60000)
	for _, c := range []struct {
		name  string
		mode  FramingMode
		input string
	}{
		{"octet", FramingOctet, octet(big, "b")},
		{"lf", FramingLF, big + "\nb\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := framer(c.input, c.mode, 64<<10)
			if msg, _, _, err := f.Next(); err != nil || len(msg) != len(big) {
				t.Fatalf("first frame: %d bytes, %v", len(msg), err)
			}
			if msg, _, _, err := f.Next(); err != nil || string(msg) != "b" {
				t.Fatalf("second frame: %q, %v", msg, err)
			}
			// The reader is empty now: the next Next would park, and lets the
			// buffer go before it does. While bytes are buffered the buffer is
			// kept for the frame they are (BenchmarkFramer12K).
			if _, _, _, err := f.Next(); err != io.EOF {
				t.Fatalf("third Next: %v, want EOF", err)
			}
			if cap(f.buf) > frameKeepCap {
				t.Fatalf("buffer of %d kept before a wait", cap(f.buf))
			}
		})
	}
}

// addrConn is a pipe that reports a chosen peer address.
type addrConn struct {
	net.Conn
	remote net.Addr
}

func (c addrConn) RemoteAddr() net.Addr { return c.remote }

func connFrom(ip string) net.Conn {
	return addrConn{Conn: pipeConn(), remote: &net.TCPAddr{IP: net.ParseIP(ip), Port: 40000}}
}

// Decided on 2026-10-14 as in go-syslog-server (SPEC §4.4, DECISOES §9):
// max_conns_per_ip counts the EXACT peer address, a /128 in IPv6. Two
// addresses of one /64 are two quotas; counting by prefix was declined,
// because a /64 can be a whole LAN of emitters. An IPv4-mapped peer is the same key as its IPv4 form
// (SPEC §4.2), so the mapping is no way around the cap. This test pins the
// rule, so a change to it is a deliberate one.
func TestPerIPLimitCountsTheExactAddress(t *testing.T) {
	var refused []obs.ConnLimit
	lim := newConnLimiter(0, 1, func(l obs.ConnLimit) { refused = append(refused, l) })
	st := &stubListener{conns: []net.Conn{
		connFrom("2001:db8::1"),
		connFrom("2001:db8::2"),      // same /64, another address: its own quota
		connFrom("2001:db8::1"),      // the first again: refused
		connFrom("192.0.2.7"),        // IPv4
		connFrom("::ffff:192.0.2.7"), // the same peer, IPv4-mapped: refused
	}}
	ln := lim.wrap(st)
	var got []string
	for {
		c, err := ln.Accept()
		if err != nil {
			break
		}
		got = append(got, peerAddrOf(c).String())
	}
	want := []string{"2001:db8::1", "2001:db8::2", "192.0.2.7"}
	if !equal(got, want) {
		t.Fatalf("accepted %v, want %v", got, want)
	}
	if len(refused) != 2 || refused[0] != obs.ConnLimitPerIP || refused[1] != obs.ConnLimitPerIP {
		t.Fatalf("refusals %v, want two per_ip", refused)
	}
}

// BenchmarkTCPReceive is the reception benchmark of one connection: frames of
// ~200 bytes through the real socket, the framer and the pipeline into a
// memory ring that a consumer drains. It is the measure behind the size of the
// reader (SPEC §5.4): a smaller buffer only costs refills, and this shows how
// many.
func BenchmarkTCPReceive(b *testing.B) {
	msg := "<13>1 2026-10-11T12:00:00.000Z host-01 app 123 - - " +
		"a message of about two hundred bytes, which is what a syslog line usually is, padded with text " +
		"until it gets there."
	for _, fr := range []struct {
		name string
		mode FramingMode
		line string
	}{
		{"octet", FramingOctet, octet(msg)},
		{"lf", FramingLF, msg + "\n"},
	} {
		b.Run(fr.name, func(b *testing.B) {
			r := ring.NewMemory(1<<16, 0, time.Millisecond)
			m := newMetrics()
			in := NewIngest(r, newUp(), m, logger(), defaultIngestConfig())
			tr, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{Addr: "127.0.0.1:0"},
				Framing: fr.mode}, in, m, logger())
			if err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			tr.Start(ctx)
			stop := make(chan struct{})
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for {
					for _, it := range r.Drain() {
						it.Release()
					}
					select {
					case <-stop:
						return
					case <-time.After(time.Millisecond):
					}
				}
			}()
			c, err := net.Dial("tcp", tr.Addr())
			if err != nil {
				b.Fatal(err)
			}
			bw := bufio.NewWriterSize(c, 64<<10)
			b.SetBytes(int64(len(msg)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bw.WriteString(fr.line)
			}
			bw.Flush()
			for m.MessagesTotal[obs.TransportTCP].Load() < uint64(b.N) {
				time.Sleep(100 * time.Microsecond)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
			c.Close()
			cancel()
			tr.Close()
			close(stop)
			<-drained
		})
	}
}

// The buffer a large frame grew is kept while the next frame is already in
// the reader: released after every frame above frameKeepCap, a stream of
// 12 KiB frames reallocated the 64 KiB class for each one (independent review
// of the port, 2026-10-13). Back-to-back frames reuse it.
func TestFramerKeepsTheBufferBetweenBufferedFrames(t *testing.T) {
	line := strings.Repeat("a", 12<<10) + "\n"
	f := NewFramer(bufio.NewReaderSize(strings.NewReader(strings.Repeat(line, 8)), 4<<10), FramingLF, 64<<10)
	if _, _, _, err := f.Next(); err != nil {
		t.Fatal(err)
	}
	first := func() *byte { return &f.buf[:cap(f.buf)][0] }
	grown := first()
	for i := 1; i < 8; i++ {
		buffered := f.r.Buffered() > 0
		if _, _, _, err := f.Next(); err != nil {
			t.Fatal(err)
		}
		if buffered && first() != grown {
			t.Fatalf("frame %d: buffer reallocated with the frame already buffered", i)
		}
		grown = first()
	}
}

// BenchmarkFramer12K is 64 frames of 12 KiB through the 4 KiB reader of SPEC
// §5.4: between the kept-buffer threshold (16 KiB) and the 64 KiB class.
func BenchmarkFramer12K(b *testing.B) {
	line := strings.Repeat("a", 12<<10) + "\n"
	in := strings.Repeat(line, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	for i := 0; i < b.N; i++ {
		f := NewFramer(bufio.NewReaderSize(strings.NewReader(in), 4<<10), FramingLF, 64<<10)
		for {
			if _, _, _, err := f.Next(); err != nil {
				break
			}
		}
	}
}
