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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// DoneFunc receives the outcome of one message. ok marks a success.
type DoneFunc func(outcome string, ok bool)

// Conn is the connection (or socket) of one worker: UDP, TCP, TLS or one
// HTTP/1.1 keep-alive connection. The dialer reports its own attempts to the
// report (transport.Options.OnDial).
//
// The contract every implementation keeps: done is called exactly once for
// every message given to Send, whatever happens -- at once, at the next
// Flush, when the server answers, or at Close. A Send or Flush error means
// the connection is unusable: the worker closes it and dials again.
type Conn interface {
	Send(seq uint64, m *source.Message, done DoneFunc) error
	// Flush pushes buffered messages. Called when the worker has no next
	// message ready, and before Close.
	Flush() error
	// Close ends the connection, waiting at most until ctx ends for what is
	// in flight (HTTP responses, and retries of 429/503).
	Close(ctx context.Context) error
}

// ErrSpent is returned by Send when the connection took the message (its
// outcome will come) but takes no more: it reached --requests-per-conn. The
// worker closes it and dials again, without counting it as broken.
var ErrSpent = errors.New("connection spent")

// Pending is a message a connection gave back without an outcome: the server
// said it would not process it (it closed the connection with
// "Connection: close" before reaching it), so it is safe to send again.
type Pending struct {
	Seq  uint64
	Msg  *source.Message
	Done DoneFunc
	// Stuck says the connection that gave it back answered nothing at all:
	// no progress was made (SPEC §7.3 bounds those resends).
	Stuck bool
	// Unwritten says it never reached the wire (the connection was already
	// closing or dead): the report tells it apart from a request the server
	// received and announced it would not process.
	Unwritten bool
}

// Resender is implemented by a connection that can give messages back. After
// Close, Unsent returns them, in order; the worker sends them first on its
// next connection.
type Resender interface {
	Unsent() []Pending
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
	seq uint64
	msg source.Message
}

// maxResend bounds how many times one message is given back by a connection
// that answered nothing.
const maxResend = 5

// brokenErr is what a Send or Flush error means for the report: ErrSpent is
// a planned close, not a broken connection.
func brokenErr(err error) error {
	if errors.Is(err, ErrSpent) {
		return nil
	}
	return err
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
		case jobs <- job{seq: seq, msg: m}:
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
	var redo []Pending // given back by a closed connection, sent first
	resent := map[uint64]int{}
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
		if rs, ok := c.(Resender); ok {
			for _, u := range rs.Unsent() {
				// A message given back again and again by connections that
				// answer nothing ends as a failure, not a loop. A server that
				// closes after some responses always makes progress.
				if u.Stuck {
					resent[u.Seq]++
				}
				if resent[u.Seq] > maxResend {
					delete(resent, u.Seq)
					u.Done("conn_error", false)
					continue
				}
				if u.Unwritten {
					rep.Event("requeued unwritten (connection closing)")
				} else {
					rep.Event("resent on a new connection")
				}
				redo = append(redo, u)
			}
		}
		c = nil
	}
	defer func() {
		closeConn(nil)
		// Given back by the last close, with no connection left to try.
		for _, p := range redo {
			p.Done("not_sent", false)
		}
	}()
	jobsOpen := true
	for {
		var j job
		var p *Pending
		ok := true
		switch {
		case len(redo) > 0:
			p, redo = &redo[0], redo[1:]
		case !jobsOpen:
			ok = false
		default:
			select {
			case j, ok = <-jobs:
			default:
				// Nothing ready: push what is buffered, then wait.
				if c != nil {
					if err := c.Flush(); err != nil {
						closeConn(brokenErr(err))
					}
				}
				if len(redo) > 0 {
					continue
				}
				j, ok = <-jobs
			}
			jobsOpen = ok
		}
		if !ok {
			if c != nil {
				if err := c.Flush(); err != nil {
					closeConn(brokenErr(err))
				}
			}
			// Closing may give messages back; they still deserve a try,
			// within the drain (stopDial ends the dialing).
			closeConn(nil)
			if len(redo) > 0 && ctx.Err() == nil {
				continue
			}
			return
		}
		for c == nil {
			nc, err := dial(ctx)
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
			if p != nil {
				p.Done("not_sent", false)
			} else {
				sh.Done("not_sent", false, 0, 0)
			}
			continue
		}
		var err error
		if p != nil {
			err = c.Send(p.Seq, p.Msg, p.Done)
		} else {
			start := time.Now()
			n := len(j.msg.Data)
			err = c.Send(j.seq, &j.msg, func(outcome string, ok bool) {
				sh.Done(outcome, ok, time.Since(start), n)
			})
		}
		if err != nil {
			closeConn(brokenErr(err))
		}
	}
}
