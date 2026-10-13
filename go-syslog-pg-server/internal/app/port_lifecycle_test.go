package app

// The FATAL-shutdown signal tests of go-relplog-pg-server (lifecycle_test.go
// there), ported with the ownership of the abort's count (SPEC §13.4).

import (
	"fmt"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// fatalShutdownInDump boots with a destination that answers every
// write with ClassFatal, accepts n messages and lets the FATAL shutdown run
// until it is about to write the emergency dump, where it is held until the
// returned release is called.
func fatalShutdownInDump(t *testing.T, n int) (*instance, string, func()) {
	t.Helper()
	dir := t.TempDir()
	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	in := launch(t, testID, baseArgs(t, dir), fs.wrap, func(a *App) {
		a.beforeDump = func() {
			close(entered)
			<-release
		}
	})
	in.waitReady()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - in-dump-%02d", i)
	}
	// The worker takes the first message alone and blocks on the gate; the
	// rest wait in the ring. That is the shape that makes the abort's count
	// wrong without App.dumping: Drain commits what it takes out of the ring,
	// so during the dump the ring's own count is only the worker's batch.
	tcp := in.app.TransportAddr("tcp")
	sendLF(t, tcp, lines[0])
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	sendLFAccepted(t, in, tcp, uint64(n), lines[1:]...)
	fs.open()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the FATAL shutdown never reached the emergency dump")
	}
	return in, dir, releaseFn
}

// settle lets the shutdown an abort left behind finish, so it does not write
// into a directory the test is removing, and checks that its dump was
// abandoned: the messages were counted lost by the abort, so no file may reach
// the disk to be reingested at the next boot (emergency.Store.SetAbandon,
// go-relplog-pg-server's independent review of 2026-10-13). In the binary the abort's os.Exit
// usually ends it first.
func settle(t *testing.T, dir string, release func()) {
	t.Helper()
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if d, _ := filepath.Glob(filepath.Join(dir, "emergency", "*emg-*")); len(d) > 0 {
			// A temporary may exist for an instant before the dump gives up;
			// the final name never may.
			if f, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.jsonl")); len(f) > 0 {
				t.Fatalf("an aborted dump reached the disk: %v", f)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SPEC §13.3, §13.4, S §22.1 items 28 and 29. A FATAL shutdown starts with no
// signal, so the first SIGTERM/SIGINT during it (a docker stop while the dump
// is written) is tolerated: the dump completes and the exit is 3. A second
// one aborts; SIGQUIT aborts at once. An abort that lands while the dump is
// being written loses the WHOLE dump -- the ring's own count was already
// committed by Drain -- and counts all of it as reason="abort".
func TestFatalShutdownSignals(t *testing.T) {
	const n = 9
	dumps := func(dir string) []string {
		d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.jsonl"))
		return d
	}
	t.Run("first SIGTERM is tolerated", func(t *testing.T) {
		in, dir, release := fatalShutdownInDump(t, n)
		in.app.abort <- syscall.SIGTERM
		if code := in.waitExit(500 * time.Millisecond); code != -1 {
			t.Fatalf("the first signal of a FATAL shutdown ended it with %d", code)
		}
		release()
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d: the FATAL shutdown must reach its dump", code, ExitSink)
		}
		if d := dumps(dir); len(d) != 1 {
			t.Fatalf("dump files = %v, want 1", d)
		}
		if got := dumpLines(t, dumps(dir)[0]); len(got) != n {
			t.Fatalf("the dump holds %d of %d messages", len(got), n)
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != 0 {
			t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d after a dump that completed`, d)
		}
	})
	t.Run("second SIGTERM aborts and counts the dump", func(t *testing.T) {
		in, dir, release := fatalShutdownInDump(t, n)
		defer settle(t, dir, release)
		in.app.abort <- syscall.SIGTERM
		in.app.abort <- syscall.SIGINT
		if code := in.waitExit(5 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
		if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
			t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d, want the %d being dumped`, d, n)
		}
	})
	t.Run("SIGQUIT aborts at once", func(t *testing.T) {
		in, dir, release := fatalShutdownInDump(t, n)
		defer settle(t, dir, release)
		in.app.abort <- syscall.SIGQUIT
		if code := in.waitExit(5 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d: SIGQUIT is never tolerated", code, ExitAborted)
		}
		m := in.app.metrics
		if d := m.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
			t.Errorf(`syslog_events_dropped_total{reason="abort"} = %d, want the %d being dumped`, d, n)
		}
		if d := m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load(); d != 0 {
			t.Errorf(`syslog_events_dropped_total{reason="emergency_failed"} = %d`, d)
		}
	})
}

// sendLFAccepted is sendLF waiting until the server accepted total messages
// over TCP. Unlike RELP, whose "200 OK" the parent's tests wait for, syslog
// gives the emitter no answer: sendLF returns before the lines are in the
// ring, and a test that lands an abort on a known population needs them all
// there.
func sendLFAccepted(t *testing.T, in *instance, addr string, total uint64, lines ...string) {
	t.Helper()
	sendLF(t, addr, lines...)
	waitFor(t, 10*time.Second, "the messages accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() >= total
	})
}
