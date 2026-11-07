package sched

import (
	"context"
	"net/netip"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// One connection per original flow (--pcap-flows, SPEC §6.8). Each flow of
// the capture gets its own queue and a worker -- the same worker as the
// shared-queue mode, so the outcome contract, the HTTP give-backs and the
// drain are those of every run -- with a dialer that announces the flow's
// own addresses in the PROXY header. The flow's end in the capture closes the
// queue, and the worker closes its connection after the last message.

type flowKey struct{ src, dst netip.AddrPort }

// FlowsPerWorker: --pcap-flows keeps at most --concurrency × this many flow
// connections open (go-syslog-injector's 256 connections per worker).
const FlowsPerWorker = 256

type flowWorker struct {
	key  flowKey
	jobs chan job
	used uint64 // last use, for the eviction at the cap
}

// A slot of the cap is free only when its worker has returned (review of
// 2026-11-07, item 6): freed when the flow was closed, it let a new worker
// dial while the old one still sent what it had queued -- more connections
// than the cap, and a flow closed at the cap had its next documents on a new
// connection ahead of its old ones (reordered). At the cap, the flow closed
// is the least recently used among those with nothing queued, and the new
// flow waits for a worker to return.
type flowSet struct {
	p        Plan
	rep      *report.Report
	stopDial context.Context
	ctx      context.Context
	wg       sync.WaitGroup

	open     map[flowKey]*flowWorker
	closing  map[flowKey]*flowWorker // closed, worker not returned yet
	running  int                     // workers not returned (open + closing)
	returned chan *flowWorker
	tick     uint64

	mu     sync.Mutex
	shards []*report.Shard // free shards: a flow's counters outlive it in the report, the shard is reused
}

func runFlows(ctx context.Context, p Plan, src source.Source, rep *report.Report) Stats {
	stopDial, cancelDial := context.WithCancel(context.Background())
	defer cancelDial()
	go func() {
		<-ctx.Done()
		cancelDial()
	}()
	limit := p.Workers * max(p.FlowsPerWorker, 1)
	fs := &flowSet{p: p, rep: rep, stopDial: stopDial, ctx: ctx, open: map[flowKey]*flowWorker{},
		closing: map[flowKey]*flowWorker{}, returned: make(chan *flowWorker, limit)}
	st := produce(ctx, p, src, fs.route, fs.end)
	for k := range fs.open {
		fs.close(k)
	}
	giveUp := time.AfterFunc(p.Drain, cancelDial)
	fs.wg.Wait()
	giveUp.Stop()
	return st
}

func (fs *flowSet) limit() int { return fs.p.Workers * max(fs.p.FlowsPerWorker, 1) }

func (fs *flowSet) reap(w *flowWorker) {
	fs.running--
	if fs.closing[w.key] == w {
		delete(fs.closing, w.key)
	}
}

// collect takes the workers that returned, without waiting.
func (fs *flowSet) collect() {
	for {
		select {
		case w := <-fs.returned:
			fs.reap(w)
		default:
			return
		}
	}
}

// await waits for a worker to return, or for tick; false when ctx ends.
func (fs *flowSet) await(tick <-chan time.Time) bool {
	select {
	case w := <-fs.returned:
		fs.reap(w)
	case <-tick:
	case <-fs.ctx.Done():
		return false
	}
	return true
}

// route returns the queue of m's flow, opening it (and its worker) on the
// flow's first document. At the cap, the least recently used flow with an
// empty queue is closed as if it had ended, and the new one waits for a
// slot.
func (fs *flowSet) route(m source.Message) chan<- job {
	k := flowKey{m.Src, m.Dst}
	fs.tick++
	if w := fs.open[k]; w != nil {
		w.used = fs.tick
		return w.jobs
	}
	// The same pair again while its old worker still sends: after it, or
	// the flow reorders.
	for fs.closing[k] != nil {
		if !fs.await(nil) {
			return nil // a nil channel: produce sees ctx end
		}
	}
	fs.collect()
	evicted := false
	for fs.running >= fs.limit() {
		// One eviction at a time: a closed flow returning its slot is
		// enough; one with queued documents would hold it until they drain.
		if len(fs.closing) == 0 {
			var lru *flowWorker
			for _, w := range fs.open {
				if len(w.jobs) == 0 && (lru == nil || w.used < lru.used) {
					lru = w
				}
			}
			if lru != nil {
				fs.close(lru.key)
				evicted = true
			}
		}
		t := time.NewTimer(5 * time.Millisecond)
		ok := fs.await(t.C)
		t.Stop()
		if !ok {
			return nil
		}
	}
	if evicted {
		fs.rep.Event("flow closed at --concurrency")
	}
	w := &flowWorker{key: k, jobs: make(chan job, 4), used: fs.tick}
	fs.open[k] = w
	fs.running++
	sh := fs.shard()
	dial := fs.p.FlowDial(m.Src, m.Dst)
	fs.wg.Add(1)
	go func() {
		defer fs.wg.Done()
		worker(fs.stopDial, w.jobs, dial, sh, fs.rep, fs.p.Drain, fs.p.Track)
		fs.mu.Lock()
		fs.shards = append(fs.shards, sh)
		fs.mu.Unlock()
		fs.returned <- w
	}()
	return w.jobs
}

// end is the flow-end marker of the source.
func (fs *flowSet) end(m source.Message) { fs.close(flowKey{m.Src, m.Dst}) }

func (fs *flowSet) close(k flowKey) {
	if w := fs.open[k]; w != nil {
		close(w.jobs)
		delete(fs.open, k)
		fs.closing[k] = w
	}
}

func (fs *flowSet) shard() *report.Shard {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if n := len(fs.shards); n > 0 {
		sh := fs.shards[n-1]
		fs.shards = fs.shards[:n-1]
		return sh
	}
	return fs.rep.NewShard()
}
