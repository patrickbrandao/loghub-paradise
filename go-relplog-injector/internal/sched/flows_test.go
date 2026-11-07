package sched

import (
	"context"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/source"
)

// flowSource yields one message of flow 1, then, after a pause (flow 1's
// lane goes idle with its session open), many messages of flows 2 and 3.
// No flow ends: their sessions stay open until the end of the run.
type flowSource struct{ n uint64 }

func (s *flowSource) Next(seq uint64, _ time.Time) (source.Message, error) {
	s.n++
	switch {
	case s.n == 1:
		return source.Message{Data: []byte("a"), Flow: 1, At: -1}, nil
	case s.n == 2:
		time.Sleep(100 * time.Millisecond)
	case s.n > 41:
		return source.Message{}, io.EOF
	}
	return source.Message{Data: []byte("b"), Flow: 2 + s.n%2, At: -1}, nil
}
func (s *flowSource) Stats() map[string]uint64 { return nil }
func (s *flowSource) Close() error             { return nil }

// ackConn answers every message at once.
type ackConn struct{}

func (ackConn) Send(m *Msg) error           { m.Done("acked", true); return nil }
func (ackConn) Flush() error                { return nil }
func (ackConn) Close(context.Context) error { return nil }
func (ackConn) Unacked() []*Msg             { return nil }

// TestFlowsIdleLaneGivesSlot (SPEC §6.9, review finding 1, reproduced): with
// one slot, the idle lane of flow 1 must give it to flow 2 when flow 2 starts
// waiting; it blocked on its empty queue holding the slot, the producer
// filled flow 2's queue, and the run hung.
func TestFlowsIdleLaneGivesSlot(t *testing.T) {
	rep := report.New()
	rep.OKOutcomes = []string{"acked"}
	var mu sync.Mutex
	open := 0
	dial := func(context.Context, netip.AddrPort, netip.AddrPort) (Conn, error) {
		mu.Lock()
		open++
		mu.Unlock()
		return ackConn{}, nil
	}
	done := make(chan Stats, 1)
	go func() {
		done <- Run(context.Background(), Plan{Workers: 1, Drain: time.Second, PerFlow: true, FlowDial: dial}, &flowSource{}, nil, rep)
	}()
	select {
	case st := <-done:
		if ok, failed := rep.Totals(); ok != 41 || failed != 0 || st.Flows != 3 {
			t.Fatalf("ok %d failed %d flows %d", ok, failed, st.Flows)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("hung: the idle lane kept its slot")
	}
}
