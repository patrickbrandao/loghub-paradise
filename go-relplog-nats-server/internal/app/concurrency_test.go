package app

import (
	"fmt"
	"sync"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

// Invariant §20.5 under concurrency and -race: many RELP sessions at once;
// after SIGTERM every message answered "200 OK" is exactly one message of the
// stream -- no loss, no duplicate -- and every envelope is valid.
func TestConcurrentTrafficEveryAcceptedMessageIsOneMessage(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	tcp := in.app.TransportAddr("tcp")
	const conns, perConn = 16, 60
	var wg sync.WaitGroup
	errs := make(chan error, conns)
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			msgs := make([]string, perConn)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app %d - - tcp-%d-%d", c, c, i)
			}
			// sendRELPErr requires a 200 for every message, so a session
			// that returns nil confirmed all of them.
			if err := sendRELPErr(tcp, msgs...); err != nil {
				errs <- fmt.Errorf("session %d: %w", c, err)
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	m := in.app.metrics
	if got := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); got != conns*perConn {
		t.Fatalf("%d accepted after %d answers of 200", got, conns*perConn)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	msgs := f.msgs()
	if len(msgs) != conns*perConn {
		t.Fatalf("%d messages for %d answered 200", len(msgs), conns*perConn)
	}
	seen := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		e := checkMsg(t, m)
		if seen[e.Payload.message()] {
			t.Errorf("%q stored twice", e.Payload.message())
		}
		seen[e.Payload.message()] = true
	}
	for c := 0; c < conns; c++ {
		for i := 0; i < perConn; i++ {
			if !seen[fmt.Sprintf("tcp-%d-%d", c, i)] {
				t.Errorf("tcp-%d-%d is missing", c, i)
			}
		}
	}
	for _, r := range []obs.RELPUnanswered{obs.UnansweredBackpressure, obs.UnansweredShutdown, obs.UnansweredConnLost} {
		if n := m.RELPUnansweredTotal[r].Load(); n != 0 {
			t.Errorf("relplog_relp_unanswered_total[%d] = %d with every session closed by the client", r, n)
		}
	}
}
