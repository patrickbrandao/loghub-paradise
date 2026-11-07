package sched

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/source"
)

// script yields msgs messages from each of flows flows, interleaved, then
// the end of every flow.
type script struct {
	flows, msgs, at int
}

func (s *script) Next(uint64, time.Time) (source.Message, error) {
	n := s.flows * s.msgs
	if s.at >= n+s.flows {
		return source.Message{}, io.EOF
	}
	i := s.at
	s.at++
	if i >= n {
		return source.Message{Flow: uint64(i - n + 1), FlowEnd: true}, nil
	}
	f, k := i%s.flows+1, i/s.flows
	return source.Message{Flow: uint64(f), Body: fmt.Appendf(nil, "%d %d", f, k)}, nil
}
func (s *script) Stats() map[string]uint64 { return nil }
func (s *script) Close() error             { return nil }

// watch is a fake server: it counts the connections open at once and the
// order of every flow's messages.
type watch struct {
	mu        sync.Mutex
	open, max int
	next      map[int]int // flow -> next index expected
	reordered int
	sent      int
}

type watchConn struct{ w *watch }

func (c *watchConn) Send(_ uint64, m *source.Message, done DoneFunc) error {
	var f, k int
	fmt.Sscanf(string(m.Body), "%d %d", &f, &k)
	time.Sleep(200 * time.Microsecond) // a slow server: queues fill up
	c.w.mu.Lock()
	if k != c.w.next[f] {
		c.w.reordered++
	}
	c.w.next[f] = k + 1
	c.w.sent++
	c.w.mu.Unlock()
	done("200", true)
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

// list yields the messages of the given flows in order (no flow ends).
type list struct {
	flows []int
	at    int
	n     map[int]int
}

func (l *list) Next(uint64, time.Time) (source.Message, error) {
	if l.at >= len(l.flows) {
		return source.Message{}, io.EOF
	}
	f := l.flows[l.at]
	l.at++
	k := l.n[f]
	l.n[f]++
	return source.Message{Flow: uint64(f), Body: fmt.Appendf(nil, "%d %d", f, k)}, nil
}
func (l *list) Stats() map[string]uint64 { return nil }
func (l *list) Close() error             { return nil }

// flowConn records which flow each connection carried.
type flowConn struct {
	watchConn
	flows *map[int]int
	mine  int
}

func (c *flowConn) Send(seq uint64, m *source.Message, done DoneFunc) error {
	var f, k int
	fmt.Sscanf(string(m.Body), "%d %d", &f, &k)
	c.w.mu.Lock()
	if c.mine == 0 {
		c.mine = f
		(*c.flows)[f]++
	}
	c.w.mu.Unlock()
	time.Sleep(3 * time.Millisecond)
	return c.watchConn.Send(seq, m, done)
}

// At the cap, the flow closed is the one idle the longest among those with
// nothing queued (SPEC §6.9): flow 1, the least recently used but with a
// backlog, keeps its connection, and flow 2 (its queue empty) is closed for
// flow 3.
func TestFlowsEvictOnlyEmptyQueue(t *testing.T) {
	w := &watch{next: map[int]int{}}
	conns := map[int]int{}
	dial := func(context.Context) (Conn, error) {
		w.mu.Lock()
		w.open++
		w.max = max(w.max, w.open)
		w.mu.Unlock()
		return &flowConn{watchConn: watchConn{w}, flows: &conns}, nil
	}
	seq := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2}
	src := &list{flows: append(seq, 3, 1), n: map[int]int{}}
	Run(context.Background(), Plan{Workers: 2, Flows: true, Drain: 5 * time.Second}, src, dial, report.New())
	if conns[1] != 1 || conns[2] != 1 || conns[3] != 1 || w.reordered != 0 || w.max > 2 {
		t.Fatalf("connections per flow %v, %d reordered, max open %d", conns, w.reordered, w.max)
	}
}

// --pcap-flows (SPEC §6.9, review of 2026-11-07): never more connections
// than --concurrency, even while a closed flow drains, and never a flow's
// requests out of order when the cap splits it.
func TestFlowsCapAndOrder(t *testing.T) {
	w := &watch{next: map[int]int{}}
	dial := func(context.Context) (Conn, error) {
		w.mu.Lock()
		w.open++
		w.max = max(w.max, w.open)
		w.mu.Unlock()
		return &watchConn{w}, nil
	}
	src := &script{flows: 12, msgs: 40}
	st := Run(context.Background(), Plan{Workers: 3, Flows: true, Drain: 5 * time.Second}, src, dial, report.New())
	if st.Produced != 12*40 || w.sent != 12*40 {
		t.Fatalf("produced %d, sent %d", st.Produced, w.sent)
	}
	if w.max > 3 || w.reordered != 0 || w.open != 0 {
		t.Fatalf("max %d connections open, %d reordered, %d left open", w.max, w.reordered, w.open)
	}
}
