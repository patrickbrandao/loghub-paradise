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
	jobs chan job
	used uint64 // last use, for the eviction at Plan.Workers
}

type flowSet struct {
	p        Plan
	rep      *report.Report
	stopDial context.Context
	wg       sync.WaitGroup

	open map[flowKey]*flowWorker
	tick uint64

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
	fs := &flowSet{p: p, rep: rep, stopDial: stopDial, open: map[flowKey]*flowWorker{}}
	st := produce(ctx, p, src, fs.route, fs.end)
	for k := range fs.open {
		fs.close(k)
	}
	giveUp := time.AfterFunc(p.Drain, cancelDial)
	fs.wg.Wait()
	giveUp.Stop()
	return st
}

// route returns the queue of m's flow, opening it (and its worker) on the
// flow's first message. Past Plan.Workers open flows, the least recently used
// is closed as if it had ended.
func (fs *flowSet) route(m source.Message) chan<- job {
	k := flowKey{m.Src, m.Dst}
	fs.tick++
	if w := fs.open[k]; w != nil {
		w.used = fs.tick
		return w.jobs
	}
	if len(fs.open) >= fs.p.Workers*max(fs.p.FlowsPerWorker, 1) {
		var lru flowKey
		var min uint64
		for fk, w := range fs.open {
			if min == 0 || w.used < min {
				lru, min = fk, w.used
			}
		}
		fs.close(lru)
		fs.rep.Event("flow closed at --concurrency")
	}
	w := &flowWorker{jobs: make(chan job, 4), used: fs.tick}
	fs.open[k] = w
	sh := fs.shard()
	dial := fs.p.FlowDial(m.Src, m.Dst)
	fs.wg.Add(1)
	go func() {
		defer fs.wg.Done()
		worker(fs.stopDial, w.jobs, dial, sh, fs.rep, fs.p.Drain, fs.p.Track)
		fs.mu.Lock()
		fs.shards = append(fs.shards, sh)
		fs.mu.Unlock()
	}()
	return w.jobs
}

// end is the flow-end marker of the source.
func (fs *flowSet) end(m source.Message) { fs.close(flowKey{m.Src, m.Dst}) }

func (fs *flowSet) close(k flowKey) {
	if w := fs.open[k]; w != nil {
		close(w.jobs)
		delete(fs.open, k)
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
