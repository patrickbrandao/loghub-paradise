package transport

import (
	"strconv"
	"sync"
)

// adaptive is the run's view of whether the HTTP server keeps connections
// (SPEC §4.3). A server that closes every connection after its first answer
// (keep-alive off, a cap of 1, or the GELF family before keep-alive) only
// has one useful request per connection: with --pipeline > 1 everything
// written behind the first is discarded and given back, so after
// adaptAfter connections in a row closed at their first answer the new ones
// use a pipeline of 1. A connection that gets a second answer restores
// --pipeline for the ones opened after it.
type adaptive struct {
	pipe  int // --pipeline
	event func(string)

	mu     sync.Mutex
	closed int  // connections in a row closed at their first answer
	one    bool // adapted: new connections use a pipeline of 1
}

// adaptAfter: two, not one -- a single close at the first answer is a
// per-connection cap or a shutdown, and costs at most --pipeline - 1 requests
// given back.
const adaptAfter = 2

func (a *adaptive) pipeline() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.one {
		return 1
	}
	return a.pipe
}

func (a *adaptive) closedAtFirst() {
	if a.pipe <= 1 {
		return // nothing to adapt: the slot is freed only after the close is seen
	}
	a.mu.Lock()
	a.closed++
	adapt := !a.one && a.closed >= adaptAfter
	if adapt {
		a.one = true
	}
	a.mu.Unlock()
	if adapt {
		a.event("adapted: server closes every connection, pipeline 1")
	}
}

func (a *adaptive) keptAlive() {
	if a.pipe <= 1 {
		return
	}
	a.mu.Lock()
	a.closed = 0
	restore := a.one
	a.one = false
	a.mu.Unlock()
	if restore {
		a.event("adapted: server keeps connections, pipeline " + strconv.Itoa(a.pipe))
	}
}
