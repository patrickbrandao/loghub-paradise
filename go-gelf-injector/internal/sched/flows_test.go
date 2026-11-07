package sched

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// The --pcap-flows rules of SPEC §6.8 (review of 2026-11-07, item 6): never
// more connections than the cap, never a flow's documents out of order, and
// at the cap the flow closed is the least recently used with nothing queued.

func pair(f int) (netip.AddrPort, netip.AddrPort) {
	return netip.AddrPortFrom(netip.MustParseAddr("10.0.0.1"), uint16(40000+f)), netip.MustParseAddrPort("10.0.0.2:12201")
}

// list yields documents of the given flows in order, then the end of each.
type list struct {
	flows []int
	at    int
	n     map[int]int
}

func (l *list) Next(uint64, time.Time) (source.Message, error) {
	if l.at < len(l.flows) {
		f := l.flows[l.at]
		l.at++
		if f < 0 { // the end of flow -f
			src, dst := pair(-f)
			return source.Message{Src: src, Dst: dst, FlowEnd: true}, nil
		}
		k := l.n[f]
		l.n[f]++
		src, dst := pair(f)
		return source.Message{Src: src, Dst: dst, Data: fmt.Appendf(nil, "%d %d", f, k)}, nil
	}
	return source.Message{}, io.EOF
}
func (l *list) Stats() map[string]uint64 { return nil }
func (l *list) Close() error             { return nil }

type watch struct {
	mu        sync.Mutex
	open, max int
	next      map[int]int
	reordered int
	sent      int
	conns     map[int]int // connections per flow
}

type watchConn struct {
	w    *watch
	slow time.Duration
}

func (c *watchConn) Send(_ uint64, m *source.Message, done DoneFunc) error {
	var f, k int
	fmt.Sscanf(string(m.Data), "%d %d", &f, &k)
	time.Sleep(c.slow)
	c.w.mu.Lock()
	if k != c.w.next[f] {
		c.w.reordered++
	}
	c.w.next[f] = k + 1
	c.w.sent++
	c.w.mu.Unlock()
	done("sent", true)
	return nil
}
func (c *watchConn) Flush() error { return nil }
func (c *watchConn) Close(context.Context) error {
	time.Sleep(time.Millisecond) // the drain of what was in flight
	c.w.mu.Lock()
	c.w.open--
	c.w.mu.Unlock()
	return nil
}

func (w *watch) dialer(slow time.Duration) func(src, dst netip.AddrPort) Dialer {
	return func(src, _ netip.AddrPort) Dialer {
		return func(context.Context) (Conn, error) {
			w.mu.Lock()
			w.open++
			w.max = max(w.max, w.open)
			w.conns[int(src.Port())-40000]++
			w.mu.Unlock()
			return &watchConn{w: w, slow: slow}, nil
		}
	}
}

func newWatch() *watch { return &watch{next: map[int]int{}, conns: map[int]int{}} }

func TestFlowsCapAndOrder(t *testing.T) {
	w := newWatch()
	var fl []int
	for k := 0; k < 40; k++ {
		for f := 1; f <= 12; f++ {
			fl = append(fl, f)
		}
	}
	p := Plan{Workers: 3, FlowsPerWorker: 1, Drain: 5 * time.Second, FlowDial: w.dialer(200 * time.Microsecond)}
	st := Run(context.Background(), p, &list{flows: fl, n: map[int]int{}}, nil, report.New())
	if st.Produced != 480 || w.sent != 480 {
		t.Fatalf("produced %d, sent %d", st.Produced, w.sent)
	}
	if w.max > 3 || w.reordered != 0 || w.open != 0 {
		t.Fatalf("max %d connections open, %d reordered, %d left open", w.max, w.reordered, w.open)
	}
}

func TestFlowsEvictOnlyEmptyQueue(t *testing.T) {
	w := newWatch()
	fl := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 1}
	p := Plan{Workers: 2, FlowsPerWorker: 1, Drain: 5 * time.Second, FlowDial: w.dialer(3 * time.Millisecond)}
	Run(context.Background(), p, &list{flows: fl, n: map[int]int{}}, nil, report.New())
	if w.conns[1] != 1 || w.conns[2] != 1 || w.conns[3] != 1 || w.reordered != 0 || w.max > 2 {
		t.Fatalf("connections per flow %v, %d reordered, max open %d", w.conns, w.reordered, w.max)
	}
}

// The same pair again after its end (a reused port, the next pass of
// --loop) waits for its old worker: its documents never pass the old ones.
func TestFlowsReopenWaitsForTheOld(t *testing.T) {
	w := newWatch()
	fl := []int{1, 1, 1, 1, 1, -1, 1, 1}
	p := Plan{Workers: 2, FlowsPerWorker: 1, Drain: 5 * time.Second, FlowDial: w.dialer(3 * time.Millisecond)}
	Run(context.Background(), p, &list{flows: fl, n: map[int]int{}}, nil, report.New())
	if w.conns[1] != 2 || w.reordered != 0 || w.sent != 7 {
		t.Fatalf("connections %v, %d reordered, %d sent", w.conns, w.reordered, w.sent)
	}
}
