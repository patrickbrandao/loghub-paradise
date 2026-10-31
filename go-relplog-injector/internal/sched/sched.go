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

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/source"
)

// DoneFunc receives the outcome of one message. ok marks a success.
type DoneFunc func(outcome string, ok bool)

// Msg is one message in flight. Tries counts the sessions it was sent on.
type Msg struct {
	Data  []byte
	Done  DoneFunc
	Tries int
}

// Conn is one RELP session.
//
// The contract every implementation keeps: for every message given to Send,
// either Done is called exactly once (an answer arrived), or the message is
// returned by Unacked after Close -- never both, never neither. A Send or
// Flush error means the session is unusable: the worker closes it, takes the
// unanswered messages back and dials again.
type Conn interface {
	Send(m *Msg) error
	// Flush pushes buffered messages. Called when the worker has no next
	// message ready, and before Close.
	Flush() error
	// Close ends the connection, waiting at most until ctx ends for what is
	// in flight (RELP acknowledgements).
	Close(ctx context.Context) error
	// Unacked returns, after Close, the messages that never got an answer,
	// in sending order. Their Done has not been called.
	Unacked() []*Msg
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
	// Retries is how many more sessions an unanswered message is sent on
	// (librelp retransmits without limit; 0 = never retransmit).
	Retries int
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
			worker(stopDial, jobs, dial, rep.NewShard(), rep, p)
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

// Unanswered is the outcome of a message whose sessions all ended without an
// answer (SPEC §8.2).
const Unanswered = "unanswered"

func worker(ctx context.Context, jobs <-chan job, dial Dialer, sh *report.Shard, rep *report.Report, p Plan) {
	var c Conn
	var retry []*Msg      // unanswered messages, resent first and in order
	var closeBy time.Time // zero: a Close gets the whole --drain-timeout
	backoff := backoffMin
	closeConn := func(broken error, final bool) {
		if c == nil {
			return
		}
		if broken != nil {
			rep.Broken(broken)
		}
		by := closeBy
		if by.IsZero() {
			by = time.Now().Add(p.Drain)
		}
		dctx, cancel := context.WithDeadline(context.Background(), by)
		// A session that dies while closing (ack timeout, serverclose, the
		// drain running out) is a broken session too.
		if err := c.Close(dctx); err != nil && broken == nil {
			rep.Broken(err)
		}
		cancel()
		for _, m := range c.Unacked() {
			if final || m.Tries > p.Retries {
				m.Done(Unanswered, false)
				continue
			}
			if m.Tries > 0 {
				rep.Event("retransmitted")
			}
			retry = append(retry, m)
		}
		c = nil
	}
	connect := func() bool {
		for c == nil {
			nc, err := dial(ctx)
			rep.Dial(err)
			if err == nil {
				c, backoff = nc, backoffMin
				return true
			}
			if ctx.Err() != nil {
				return false
			}
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
			}
			backoff = min(backoff*2, backoffMax)
		}
		return true
	}
	send := func(m *Msg) {
		m.Tries++
		if err := c.Send(m); err != nil {
			closeConn(err, false)
		}
	}
	// flushRetry resends what a broken session left, before anything new.
	flushRetry := func() bool {
		for len(retry) > 0 {
			if !connect() {
				return false
			}
			m := retry[0]
			retry = retry[1:]
			send(m)
		}
		return true
	}
	giveUp := func(outcome string) {
		for _, m := range retry {
			m.Done(outcome, false)
		}
		retry = nil
	}
	for {
		var j job
		var ok bool
		if !flushRetry() {
			giveUp(Unanswered)
		}
		select {
		case j, ok = <-jobs:
		default:
			if c != nil {
				if err := c.Flush(); err != nil {
					closeConn(err, false)
					continue
				}
			}
			j, ok = <-jobs
		}
		if !ok {
			break
		}
		start := time.Now()
		n := len(j.data)
		m := &Msg{Data: j.data, Done: func(outcome string, ok bool) {
			sh.Done(outcome, ok, time.Since(start), n)
		}}
		if !connect() {
			m.Done("not_sent", false)
			continue
		}
		send(m)
	}
	// The end: wait for the answers; what a session that breaks now leaves
	// is retransmitted while the drain lasts.
	deadline := time.Now().Add(p.Drain)
	closeBy = deadline
	for {
		if c != nil {
			if err := c.Flush(); err != nil {
				closeConn(err, false)
			} else {
				closeConn(nil, time.Now().After(deadline))
			}
		}
		if len(retry) == 0 || time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		if !flushRetry() {
			break
		}
	}
	giveUp(Unanswered)
}
