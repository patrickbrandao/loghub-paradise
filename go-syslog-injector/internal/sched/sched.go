// Package sched runs a load (SPEC §7): one producer that takes messages from
// the source at the planned pace, and N workers, each owning one connection,
// that send them and record the outcomes.
package sched

import (
	"context"
	"errors"
	"io"
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
}

// Stats of the producer, for the report.
type Stats struct {
	SourceErr error
	Produced  uint64
	// Behind counts messages that left the producer late by more than 100 ms
	// (the target or the workers could not keep the pace).
	Behind uint64
}

type job struct {
	data []byte
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
	jobs := make(chan job, p.Workers*4)
	var wg sync.WaitGroup
	// Workers outlive ctx: they must drain what was produced. They stop on
	// their own context only when dialing is hopeless (stopDial).
	stopDial, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	go func() {
		<-ctx.Done()
		cancelDial() // a worker still dialing gives up; its jobs fail
	}()
	for range p.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(stopDial, jobs, dial, rep.NewShard(), rep, p.Drain)
		}()
	}
	st := produce(ctx, p, src, jobs)
	close(jobs)
	// What is still queued gets Drain to find a connection; past it, a
	// worker that cannot dial gives up and its messages are not_sent.
	giveUp := time.AfterFunc(p.Drain, cancelDial)
	wg.Wait()
	giveUp.Stop()
	return st
}

func produce(ctx context.Context, p Plan, src source.Source, jobs chan<- job) Stats {
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
	for seq := uint64(1); p.Count == 0 || seq <= p.Count; seq++ {
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
		select {
		case jobs <- job{data: m.Data}:
			st.Produced++
		case <-ctx.Done():
			return st
		case <-deadlineC:
			// The workers could not take it before the end: it was never
			// sent, so it is not a message of the run.
			return st
		}
	}
	return st
}

func worker(ctx context.Context, jobs <-chan job, dial Dialer, sh *report.Shard, rep *report.Report, drain time.Duration) {
	var c Conn
	backoff := backoffMin
	closeConn := func(broken error) {
		if c == nil {
			return
		}
		if broken != nil {
			rep.Broken(broken)
		}
		dctx, cancel := context.WithTimeout(context.Background(), drain)
		_ = c.Close(dctx)
		cancel()
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
		for c == nil {
			nc, err := dial(ctx)
			rep.Dial(err)
			if err == nil {
				c, backoff = nc, backoffMin
				break
			}
			if ctx.Err() != nil {
				break
			}
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
			}
			backoff = min(backoff*2, backoffMax)
		}
		if c == nil {
			// Interrupted while the target was unreachable.
			sh.Done("not_sent", false, 0, 0)
			continue
		}
		start := time.Now()
		n := len(j.data)
		err := c.Send(j.data, func(outcome string, ok bool) {
			sh.Done(outcome, ok, time.Since(start), n)
		})
		if err != nil {
			closeConn(err)
		}
	}
}
