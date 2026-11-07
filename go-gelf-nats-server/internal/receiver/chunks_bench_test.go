package receiver

import (
	"encoding/binary"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks of the shared UDP reassembler (SPEC §5.2, §22.1 item 1 closed on
// 2026-11-07): is the one mutex the bottleneck of several udp_listeners under
// a chunk flood? The numbers and the decision are in SPEC §5.2 and TESTING §4.
//
//	go test -run XXX -bench 'Reassembl|ChunkFlood|Sweep' -benchmem -cpu 1,2,4,8 ./internal/receiver

// Chunks of a typical emitter: graylog libraries cut at 1420 bytes (the
// largest that fits an Ethernet MTU with IPv4/UDP and the 12-byte header).
const (
	benchChunkData  = 1420 - chunkHeader
	benchChunkCount = 8
)

// benchMessage is the chunks of one message; setID rewrites the id in place,
// so a goroutine reuses its datagrams across messages.
type benchMessage [][]byte

// benchLayouts are the two layouts measured: the single map under one mutex
// that the server had until 2026-11-07, and the shards that replaced it.
var benchLayouts = []struct {
	name   string
	shards int
}{{"onelock", 1}, {"sharded", reassemblyShards}}

func newBenchMessage() benchMessage {
	m := make(benchMessage, benchChunkCount)
	data := make([]byte, benchChunkData)
	for i := range data {
		data[i] = 'a' + byte(i%26)
	}
	for s := range m {
		m[s] = chunk(0, byte(s), benchChunkCount, data)
	}
	return m
}

func (m benchMessage) setID(id uint64) {
	for _, c := range m {
		binary.BigEndian.PutUint64(c[2:10], id)
	}
}

// BenchmarkReassemblerParallel is the critical section alone: every goroutine
// is an emitter (own source port) reassembling 8-chunk messages, with nothing
// outside the lock. One op is one chunk. With -cpu N it is N listeners at
// their fastest; the ns/op at -cpu 8 against -cpu 1 is the contention.
func BenchmarkReassemblerParallel(b *testing.B) {
	for _, l := range benchLayouts {
		b.Run(l.name, func(b *testing.B) { benchReassemblerParallel(b, newReassembler(ReassemblerConfig{}, l.shards)) })
	}
}

func benchReassemblerParallel(b *testing.B, r *Reassembler) {
	var port atomic.Uint32
	now := time.Now()
	b.SetBytes(benchChunkData)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		src := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(1000+port.Add(1)))
		m := newBenchMessage()
		var dst []byte
		var id uint64
		seq := 0
		for pb.Next() {
			if seq == 0 {
				id++
				m.setID(id)
			}
			var res ChunkResult
			dst, _, res = r.Add(dst[:0], src, m[seq], now)
			if res != ChunkPending && res != ChunkComplete {
				b.Errorf("result %d", res)
				return
			}
			seq = (seq + 1) % benchChunkCount
		}
	})
}

// BenchmarkReassemblerParallelInterleaved is the flood that keeps messages
// open: each goroutine interleaves 64 messages, so the map holds hundreds of
// open entries and every chunk pays a real lookup, as with many emitters.
func BenchmarkReassemblerParallelInterleaved(b *testing.B) {
	for _, l := range benchLayouts {
		b.Run(l.name, func(b *testing.B) {
			benchReassemblerInterleaved(b, newReassembler(ReassemblerConfig{}, l.shards))
		})
	}
}

func benchReassemblerInterleaved(b *testing.B, r *Reassembler) {
	var port atomic.Uint32
	now := time.Now()
	b.SetBytes(benchChunkData)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		src := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(1000+port.Add(1)))
		const inFlight = 64
		msgs := make([]benchMessage, inFlight)
		for i := range msgs {
			msgs[i] = newBenchMessage()
		}
		var dst []byte
		var base uint64
		step := 0 // step/inFlight is the seq, step%inFlight the message
		for pb.Next() {
			if step == 0 {
				for i := range msgs {
					msgs[i].setID(base + uint64(i))
				}
				base += inFlight
			}
			var res ChunkResult
			dst, _, res = r.Add(dst[:0], src, msgs[step%inFlight][step/inFlight], now)
			if res != ChunkPending && res != ChunkComplete {
				b.Errorf("result %d", res)
				return
			}
			step = (step + 1) % (inFlight * benchChunkCount)
		}
	})
}

// BenchmarkReassemblerSweep is the stall the sweeper imposes once a second:
// a Sweep that expires nothing over a full map (16 384 open messages, the
// default ceiling) holds the lock for the whole walk with one lock, and for a
// 64th of it with the shards (the total stays the same).
func BenchmarkReassemblerSweep(b *testing.B) {
	for _, l := range benchLayouts {
		b.Run(l.name, func(b *testing.B) { benchReassemblerSweep(b, newReassembler(ReassemblerConfig{}, l.shards)) })
	}
}

func benchReassemblerSweep(b *testing.B, r *Reassembler) {
	now := time.Now()
	src := netip.MustParseAddrPort("192.0.2.1:1000")
	m := newBenchMessage()
	for id := uint64(1); r.Open() < DefaultReassemblyMaxMessages; id++ {
		m.setID(id)
		r.Add(nil, src, m[0], now)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := r.Sweep(now); n != 0 {
			b.Fatalf("swept %d", n)
		}
	}
}

// BenchmarkUDPChunkFlood is the real thing on loopback: -cpu N listeners, each
// its own socket flooded by its own sender, each reader doing what udpLoop
// does before the decompression (ReadMsgUDPAddrPort, IsChunk, Add). One op is
// one chunk read and filed. "onelock" is one reassembler for every listener in
// the layout before 2026-11-07 (one map, one mutex), "sharded" is the server
// now, and "separate" gives each listener its own reassembler, the ceiling of
// a perfect sharding. All get ceilings far above the flood, so none refuses: a
// separate reassembler per listener would otherwise get N times the room.
// Equal chunks/s means the lock costs nothing the socket does not already
// cost.
//
// The sockets are separate ports, not SO_REUSEPORT: macOS does not spread UDP
// across reuseport sockets, and on Linux the kernel hashes by 4-tuple, which
// is what one socket per sender reproduces. The senders run unpaced; what the
// kernel drops is not read and not measured.
func BenchmarkUDPChunkFlood(b *testing.B) {
	for _, l := range benchLayouts {
		b.Run(l.name, func(b *testing.B) { benchUDPChunkFlood(b, l.shards) })
	}
	b.Run("separate", func(b *testing.B) { benchUDPChunkFlood(b, 0) })
}

// benchUDPChunkFlood shares one reassembler of the given shards among the
// listeners, or gives each its own (one shard) with shards = 0.
func benchUDPChunkFlood(b *testing.B, shards int) {
	n := testingCPU()
	roomy := ReassemblerConfig{MaxBytes: 4 << 30, MaxMessages: 1 << 24}
	var one *Reassembler
	if shards > 0 {
		one = newReassembler(roomy, shards)
	}
	var chunks, msgs atomic.Int64
	target := int64(b.N)
	var wg sync.WaitGroup
	conns := make([]*net.UDPConn, n)
	for i := range conns {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			b.Fatal(err)
		}
		_ = c.SetReadBuffer(8 << 20)
		conns[i] = c
	}
	stop := make(chan struct{})
	b.SetBytes(benchChunkData)
	b.ResetTimer()
	start := time.Now()
	for i, c := range conns {
		r := one
		if r == nil {
			r = newReassembler(roomy, 1)
		}
		wg.Add(2)
		go func(c *net.UDPConn) { // the listener
			defer wg.Done()
			buf := make([]byte, udpReadBuffer)
			oob := make([]byte, oobBuffer)
			var whole []byte
			for chunks.Load() < target {
				_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				k, _, _, src, err := c.ReadMsgUDPAddrPort(buf, oob)
				if err != nil || !IsChunk(buf[:k]) {
					continue
				}
				var res ChunkResult
				whole, _, res = r.Add(whole[:0], src, buf[:k], time.Now())
				chunks.Add(1)
				if res == ChunkComplete {
					msgs.Add(1)
				}
			}
		}(c)
		go func(addr *net.UDPAddr, i int) { // the emitter
			defer wg.Done()
			s, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				b.Error(err)
				return
			}
			defer s.Close()
			m := newBenchMessage()
			for id := uint64(i) << 40; ; id++ {
				select {
				case <-stop:
					return
				default:
				}
				m.setID(id)
				for _, ch := range m {
					_, _ = s.Write(ch)
				}
			}
		}(c.LocalAddr().(*net.UDPAddr), i)
	}
	for chunks.Load() < target {
		time.Sleep(time.Millisecond)
	}
	el := time.Since(start)
	b.StopTimer()
	close(stop)
	wg.Wait()
	for _, c := range conns {
		c.Close()
	}
	b.ReportMetric(float64(chunks.Load())/el.Seconds(), "chunks/s")
	b.ReportMetric(float64(msgs.Load())/el.Seconds(), "msgs/s")
}

// testingCPU is the -cpu value of this run (GOMAXPROCS during the benchmark).
func testingCPU() int { return runtime.GOMAXPROCS(0) }
