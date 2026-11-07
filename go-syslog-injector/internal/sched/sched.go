// Package sched runs a load (SPEC §7): one producer that takes messages from
// the source at the planned pace, and N workers, each owning one connection
// (or, with --pcap-flows, one per captured flow, SPEC §6.8), that send them
// and record the outcomes.
package sched

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/source"
)

// DoneFunc receives the outcome of one message. ok marks a success.
type DoneFunc func(outcome string, ok bool)

// Conn is one connection (or socket) to the target.
//
// The contract every implementation keeps: done is called exactly once for
// every message given to Send, whatever happens -- at once, at the next
// Flush, when the server answers, or at Close. A Send or Flush error means
// the connection is unusable: the worker closes it and dials again.
type Conn interface {
	Send(msg []byte, done DoneFunc) error
	// Flush pushes buffered messages. Called when the worker has no next
	// message ready, and before Close.
	Flush() error
	// Close ends the connection, waiting at most until ctx ends for what is
	// in flight (RELP acknowledgements).
	Close(ctx context.Context) error
}

// Dialer opens a connection.
type Dialer func(ctx context.Context) (Conn, error)

// FlowDialer opens the connection of one captured flow, announcing src and
// dst where the transport can (the PROXY header, SPEC §6.8).
type FlowDialer func(ctx context.Context, src, dst netip.AddrPort) (Conn, error)

// Plan is what to send and how fast.
type Plan struct {
	Count    uint64        // 0 = no limit
	Duration time.Duration // 0 = no limit
	Rate     float64       // messages per second, 0 = as fast as possible
	// Original paces by Message.At / Speed (--pcap-timing original).
	Original bool
	Speed    float64
	Workers  int
	// Drain bounds the wait for in-flight messages at the end.
	Drain time.Duration
	// FlowDial, when set, gives every captured flow its own connection
	// (--pcap-flows, SPEC §6.8); the Dialer of Run is then unused.
	FlowDial FlowDialer
}

// MaxFlowConns bounds the open flow connections of one worker; past it the
// least recently used one is closed (SPEC §6.8).
const MaxFlowConns = 256

// Stats of the producer, for the report.
type Stats struct {
	SourceErr error
	Produced  uint64
	// Behind counts messages that left the producer late by more than 100 ms
	// (the target or the workers could not keep the pace).
	Behind uint64
}

type job struct {
	data     []byte
	seq      uint64
	flow     uint64
	src, dst netip.AddrPort
	end      bool // the flow ended: close its connection (no message)
}

// Backoff of a worker whose dial fails: 100 ms doubling up to 2 s.
const (
	backoffMin = 100 * time.Millisecond
	backoffMax = 2 * time.Second
)

// Run sends the plan and returns when every message produced has an outcome.
// Cancelling ctx stops the producer; what was produced is still sent and
// drained (a second interrupt is the caller's business).
func Run(ctx context.Context, p Plan, src source.Source, dial Dialer, rep *report.Report) Stats {
	if p.Workers < 1 {
		p.Workers = 1
	}
	var wg sync.WaitGroup
	// Workers outlive ctx: they must drain what was produced. They stop on
	// their own context only when dialing is hopeless (stopDial).
	stopDial, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	go func() {
		<-ctx.Done()
		cancelDial() // a worker still dialing gives up; its jobs fail
	}()
	var queues []chan job
	var route func(job) chan<- job
	if p.FlowDial != nil {
		// One queue per worker, a flow always on the same one: the order of
		// a flow is kept, flows run in parallel.
		for range p.Workers {
			queues = append(queues, make(chan job, 4))
		}
		route = func(j job) chan<- job { return queues[j.flow%uint64(len(queues))] }
		for _, q := range queues {
			wg.Add(1)
			go func() {
				defer wg.Done()
				flowWorker(stopDial, q, p.FlowDial, rep.NewShard(), rep, p.Drain)
			}()
		}
	} else {
		q := make(chan job, p.Workers*4)
		queues = append(queues, q)
		route = func(job) chan<- job { return q }
		for range p.Workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker(stopDial, q, dial, rep.NewShard(), rep, p.Drain)
			}()
		}
	}
	st := produce(ctx, p, src, route)
	for _, q := range queues {
		close(q)
	}
	// What is still queued gets Drain to find a connection; past it, a
	// worker that cannot dial gives up and its messages are not_sent.
	giveUp := time.AfterFunc(p.Drain, cancelDial)
	wg.Wait()
	giveUp.Stop()
	return st
}

func produce(ctx context.Context, p Plan, src source.Source, route func(job) chan<- job) Stats {
	var st Stats
	start := time.Now()
	var deadline time.Time
	var deadlineC <-chan time.Time // nil (blocks forever) without --duration
	if p.Duration > 0 {
		deadline = start.Add(p.Duration)
		dt := time.NewTimer(p.Duration)
		defer dt.Stop()
		deadlineC = dt.C
	}
	var interval time.Duration
	if p.Rate > 0 {
		interval = time.Duration(float64(time.Second) / p.Rate)
	}
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for seq := uint64(1); p.Count == 0 || seq <= p.Count; {
		if ctx.Err() != nil {
			break
		}
		now := time.Now()
		if !deadline.IsZero() && !now.Before(deadline) {
			break
		}
		m, err := src.Next(seq, now)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				st.SourceErr = err
			}
			break
		}
		if m.FlowEnd {
			// A control message: not a message of the run, takes no seq
			// and no pace.
			select {
			case route(job{flow: m.Flow, end: true}) <- job{flow: m.Flow, end: true}:
			case <-ctx.Done():
				return st
			case <-deadlineC:
				return st
			}
			continue
		}
		var due time.Time
		switch {
		case p.Original && m.At >= 0:
			due = start.Add(time.Duration(float64(m.At) / p.Speed))
		case interval > 0:
			due = start.Add(time.Duration(seq-1) * interval)
		}
		if !due.IsZero() {
			if !deadline.IsZero() && due.After(deadline) {
				break
			}
			if wait := time.Until(due); wait > 0 {
				timer.Reset(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return st
				case <-deadlineC:
					timer.Stop()
					return st
				case <-timer.C:
				}
			} else if -wait > 100*time.Millisecond {
				st.Behind++
			}
		}
		j := job{data: m.Data, seq: seq, flow: m.Flow, src: m.Src, dst: m.Dst}
		select {
		case route(j) <- j:
			st.Produced++
		case <-ctx.Done():
			return st
		case <-deadlineC:
			// The workers could not take it before the end: it was never
			// sent, so it is not a message of the run.
			return st
		}
		seq++
	}
	return st
}

// dialLoop dials until it gets a connection or ctx ends (nil then), with the
// worker's backoff.
func dialLoop(ctx context.Context, dial func(context.Context) (Conn, error), rep *report.Report, backoff *time.Duration) Conn {
	for {
		nc, err := dial(ctx)
		rep.Dial(err)
		if err == nil {
			*backoff = backoffMin
			return nc
		}
		if ctx.Err() != nil {
			return nil
		}
		t := time.NewTimer(*backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
		*backoff = min(*backoff*2, backoffMax)
	}
}

func closeWith(c Conn, broken error, rep *report.Report, drain time.Duration) {
	if broken != nil {
		rep.Broken(broken)
	}
	dctx, cancel := context.WithTimeout(context.Background(), drain)
	_ = c.Close(dctx)
	cancel()
}

func send(c Conn, j job, sh *report.Shard) error {
	start := time.Now()
	n, seq := len(j.data), j.seq
	return c.Send(j.data, func(outcome string, ok bool) {
		sh.Done(seq, outcome, ok, time.Since(start), n)
	})
}

func worker(ctx context.Context, jobs <-chan job, dial Dialer, sh *report.Shard, rep *report.Report, drain time.Duration) {
	var c Conn
	backoff := backoffMin
	closeConn := func(broken error) {
		if c == nil {
			return
		}
		closeWith(c, broken, rep, drain)
		c = nil
	}
	defer closeConn(nil)
	for {
		var j job
		var ok bool
		select {
		case j, ok = <-jobs:
		default:
			// Nothing ready: push what is buffered, then wait.
			if c != nil {
				if err := c.Flush(); err != nil {
					closeConn(err)
				}
			}
			j, ok = <-jobs
		}
		if !ok {
			if c != nil {
				if err := c.Flush(); err != nil {
					closeConn(err)
				}
			}
			return
		}
		if j.end {
			continue // flow ends mean nothing without --pcap-flows
		}
		if c == nil {
			c = dialLoop(ctx, dial, rep, &backoff)
		}
		if c == nil {
			// Interrupted while the target was unreachable.
			sh.Done(j.seq, "not_sent", false, 0, 0)
			continue
		}
		if err := send(c, j, sh); err != nil {
			closeConn(err)
		}
	}
}

type flowConn struct {
	c    Conn
	used uint64 // last use, in the worker's job count (LRU)
}

// flowWorker owns one connection per captured flow routed to it (SPEC §6.8):
// opened at the flow's first message, closed when the flow ends, the least
// recently used one closed past MaxFlowConns.
func flowWorker(ctx context.Context, jobs <-chan job, dial FlowDialer, sh *report.Shard, rep *report.Report, drain time.Duration) {
	conns := map[uint64]*flowConn{}
	backoff := backoffMin
	var tick uint64
	closeFlow := func(id uint64, broken error) {
		if fc := conns[id]; fc != nil {
			closeWith(fc.c, broken, rep, drain)
			delete(conns, id)
		}
	}
	flushAll := func() {
		for id, fc := range conns {
			if err := fc.c.Flush(); err != nil {
				closeFlow(id, err)
			}
		}
	}
	defer func() {
		for id := range conns {
			closeFlow(id, nil)
		}
	}()
	for {
		var j job
		var ok bool
		select {
		case j, ok = <-jobs:
		default:
			flushAll()
			j, ok = <-jobs
		}
		if !ok {
			flushAll()
			return
		}
		if j.end {
			if fc := conns[j.flow]; fc != nil {
				err := fc.c.Flush()
				closeFlow(j.flow, err)
			}
			continue
		}
		tick++
		fc := conns[j.flow]
		if fc == nil {
			if len(conns) >= MaxFlowConns {
				var lru uint64
				var lruUsed uint64 = ^uint64(0)
				for id, o := range conns {
					if o.used < lruUsed {
						lru, lruUsed = id, o.used
					}
				}
				rep.Event("flow_conn_evicted")
				err := conns[lru].c.Flush()
				closeFlow(lru, err)
			}
			src, dst := j.src, j.dst
			c := dialLoop(ctx, func(ctx context.Context) (Conn, error) { return dial(ctx, src, dst) }, rep, &backoff)
			if c == nil {
				sh.Done(j.seq, "not_sent", false, 0, 0)
				continue
			}
			fc = &flowConn{c: c}
			conns[j.flow] = fc
		}
		fc.used = tick
		if err := send(fc.c, j, sh); err != nil {
			closeFlow(j.flow, err)
		}
	}
}
