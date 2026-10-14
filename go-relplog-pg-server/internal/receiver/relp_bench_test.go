package receiver

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/ring"
)

// BenchmarkRELPSession is one RELP session end to end, as rsyslog drives it:
// one connection, the window kept full, every frame answered 200 only after
// the memory ring took it, the ring drained by a consumer. It is the number
// for the acceptance criterion of SPEC §18.8 (one connection, 200-byte lines).
func BenchmarkRELPSession(b *testing.B) {
	r := ring.NewMemory(1<<16, 0, time.Millisecond)
	in := NewIngest(r, newUp(), newMetrics(), logger(), defaultIngestConfig())
	tr, err := NewTCP(context.Background(), TCPConfig{StreamConfig: StreamConfig{Addr: "127.0.0.1:0"}}, in, newMetrics(), logger())
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); tr.Close() }()
	tr.Start(ctx)
	go func() {
		for ctx.Err() == nil {
			for _, it := range r.Drain() {
				it.Release()
			}
			time.Sleep(time.Millisecond)
		}
	}()

	c, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	bw := bufio.NewWriterSize(c, 64<<10)
	bw.WriteString(frameOf(1, "open", relpTestOffers))
	bw.Flush()
	if _, err := readServerFrame(br); err != nil {
		b.Fatal(err)
	}

	msg := "<13>1 2026-10-11T12:00:00.000Z host-01 app 123 - - " +
		"a message of about two hundred bytes, which is what a syslog line usually is, padded with text " +
		"until it gets there."
	const window = 128
	sem := make(chan struct{}, window)
	done := make(chan struct{})
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			f, err := readServerFrame(br)
			if err != nil || f.data != "200 OK" {
				b.Errorf("answer %v %v", f, err)
				return
			}
			<-sem
		}
	}()
	for i := 0; i < b.N; i++ {
		sem <- struct{}{}
		bw.WriteString(strconv.Itoa(i+2) + " syslog " + strconv.Itoa(len(msg)) + " " + msg + "\n")
		if len(sem) == window || i == b.N-1 {
			bw.Flush()
		}
	}
	<-done
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

// cycleReader replays one stream forever, as a sender that never pauses.
type cycleReader struct {
	b   []byte
	off int
}

func (c *cycleReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) { // no short read at the wrap: that would be a pause
		k := copy(p[n:], c.b[c.off:])
		c.off = (c.off + k) % len(c.b)
		n += k
	}
	return n, nil
}

// BenchmarkRELPReaderFrames reads back-to-back syslog frames of one size
// through the session's 4 KiB bufio.Reader. Above relpKeepBlock (16 KiB) the
// reader used to let the buffer go before EVERY frame and grow it again by
// doubling: a sender of 24 KiB lines paid the whole growth per frame
// (independent review of go-syslog-server, 2026-10-13, E2; TESTING).
func BenchmarkRELPReaderFrames(b *testing.B) {
	for _, size := range []int{200, 12 << 10, 24 << 10, 60 << 10} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			msg := bytes.Repeat([]byte("a"), size)
			var stream []byte
			for i := 1; i <= 64; i++ {
				stream = append(stream, strconv.Itoa(i)+" syslog "+strconv.Itoa(size)+" "...)
				stream = append(stream, msg...)
				stream = append(stream, '\n')
			}
			rd := NewRELPReader(bufio.NewReaderSize(&cycleReader{b: stream}, relpReadBuffer), 1<<20, 64<<10)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rd.ReadFrame(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
