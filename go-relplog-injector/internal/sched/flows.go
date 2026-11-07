package sched

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/source"
)

// One session per original flow (--pcap-per-flow, SPEC §6.9): every flow of
// the source gets its own lane -- a worker with its own queue and its own
// sessions, dialed with the flow's captured addresses -- so its messages go
// in capture order on its session, and flows run side by side. At most
// p.Workers sessions are open at a time; a lane that waits for a slot makes
// an idle lane (nothing queued) close its session and give the slot away, so
// no flow waits forever for one that has nothing to send.

// slots bound the open sessions.
type slots struct {
	ch      chan struct{}
	waiting atomic.Int32
}

func (s *slots) acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	default:
	}
	s.waiting.Add(1)
	defer s.waiting.Add(-1)
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *slots) release() { <-s.ch }

// contended reports whether a lane is waiting for a slot.
func (s *slots) contended() bool { return s.waiting.Load() > 0 }

// slotConn gives its slot back when the session is closed.
type slotConn struct {
	Conn
	once    sync.Once
	release func()
}

func (c *slotConn) Close(ctx context.Context) error {
	err := c.Conn.Close(ctx)
	c.once.Do(c.release)
	return err
}

// laneQueue is the queue of one flow: small, so the producer feels the
// backpressure of a flow whose session is slow.
const laneQueue = 4

// maxShards bounds the report shards the lanes share (a shard has a 16 KiB
// histogram; a capture can hold thousands of flows).
const maxShards = 64

func runFlows(ctx context.Context, p Plan, src source.Source, dial FlowDialer, rep *report.Report) Stats {
	stopDial, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	go func() {
		<-ctx.Done()
		cancelDial()
	}()
	sl := &slots{ch: make(chan struct{}, p.Workers)}
	shards := make([]*report.Shard, min(p.Workers, maxShards))
	for i := range shards {
		shards[i] = rep.NewShard()
	}
	lanes := map[uint64]chan job{}
	var wg sync.WaitGroup
	var flows uint64
	route := func(m *source.Message) chan<- job {
		// A flow that ended in the capture ends its session after its last
		// message: closing the queue starts the worker's end phase.
		for _, id := range m.EndFlows {
			if ch, ok := lanes[id]; ok {
				close(ch)
				delete(lanes, id)
			}
		}
		ch, ok := lanes[m.Flow]
		if ok {
			return ch
		}
		ch = make(chan job, laneQueue)
		lanes[m.Flow] = ch
		fsrc, fdst := m.Src, m.Dst
		sh := shards[flows%uint64(len(shards))]
		flows++
		laneDial := func(ctx context.Context) (Conn, error) {
			if err := sl.acquire(ctx); err != nil {
				return nil, err
			}
			c, err := dial(ctx, fsrc, fdst)
			if err != nil {
				sl.release()
				return nil, err
			}
			return &slotConn{Conn: c, release: sl.release}, nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(stopDial, ch, laneDial, sh, rep, p, sl.contended)
		}()
		return ch
	}
	st := produce(ctx, p, src, route)
	for _, ch := range lanes {
		close(ch)
	}
	giveUp := time.AfterFunc(p.Drain, cancelDial)
	wg.Wait()
	giveUp.Stop()
	st.Flows = flows
	return st
}
