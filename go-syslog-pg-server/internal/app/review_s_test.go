package app

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/sink"
)

// Regression tests of the independent review of go-syslog-server on
// 2026-10-13, for the code this server shares with it (X1 and X2 of that
// review's plan). Each failed here before its fix.

// X1 (A1 there): a signal that lands after the reingestion and before the bind
// cancels the bind context, but a literal IP or a unix path looks nothing up,
// so no bind fails: the server opened, /readyz said 200 and the shutdown that
// followed exited 0. TestSignalBeforeTheBindExits130 is the host-name case.
func TestSignalBeforeALiteralBindExits130(t *testing.T) {
	base := func(t *testing.T, dir string) []string { return baseArgs(t, dir) }
	id := testID
	dump := func(t *testing.T, dir string) { dumpOf(t, dir, crashItems(3, "pre-bind")) }
	for _, tc := range []struct {
		name string
		args func([]string) []string
	}{
		{"literal IPs", func(args []string) []string { return args }},
		{"unix socket", func(args []string) []string {
			args = withoutFlag(withoutFlag(args, "--listen.udp="), "--listen.tcp=")
			return append(args, "--listen.unix="+shortSocketPath(t))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dump(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			code := runContext(ctx, tc.args(base(t, dir)), id, func(a *App) {
				a.abort = make(chan os.Signal, 1)
				a.wrapSink = func(s sink.Sink) sink.Sink { return cancelAfterWrite{Sink: s, cancel: cancel} }
			})
			if code != ExitAborted {
				t.Errorf("a signal before the bind exited %d, want %d", code, ExitAborted)
			}
		})
	}
}

// X2 (A2 there): abortLoss publishes aborted before it takes the dump's count.
// A dump that saw the abort in between -- it gives up (Store.SetAbandon), or,
// disabled, has nothing to write -- took the count first: emergency_failed=n
// and abort=0, the loss counted once but blamed on the disk. inAbort lands the
// dump right there; it waits for it, or, once the dump waits on lossMu (the
// fix), gives up after a moment.
func TestAbortInsideTheDumpKeepsItsReason(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "abandoned dump"
		if disabled {
			name = "disabled dump"
		}
		t.Run(name, func(t *testing.T) {
			a, held := abortApp(t)
			if !disabled {
				a.emg = emergency.NewStore(t.TempDir(), testID.From)
				a.emg.SetAbandon(a.aborted.Load)
			}
			if !a.enterDumpPhase() {
				t.Fatal("no abort yet, the dump phase must start")
			}
			items := append(held, a.ring.Drain()...)
			if !a.oweDump(len(items)) {
				t.Fatal("no abort yet, the dump is owed")
			}
			done := make(chan struct{})
			a.inAbort = func() {
				go func() {
					defer close(done)
					a.dumpEmergency(items)
				}()
				select {
				case <-done:
				case <-time.After(300 * time.Millisecond):
				}
			}
			a.abortLoss()
			<-done
			failed := a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
			if got := aborted(a); got != uint64(len(items)) || failed != 0 {
				t.Fatalf("abort %d, emergency_failed %d for %d lost to the abort", got, failed, len(items))
			}
		})
	}
}
