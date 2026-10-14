package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/sink"
)

// The shutdown, the boot gate and the transports of SPEC §13, run for real
// (SPEC §22 pendências 27-29 and the "A portar" of go-relplog-server and
// go-relplog-nats-server): ported from their lifecycle_test.go,
// review_cut_test.go and review_rn_test.go, with syslog over TCP where they
// speak RELP.

// ---------------------------------------------------------------- logs

// logBuf collects an instance's log lines, safe for the App's goroutines.
type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLog is a launch hook that sends the instance's log, at info, to buf.
// baseArgs keeps the level at error; the WARN lines are what these tests read.
func captureLog(buf *logBuf) func(*App) {
	return func(a *App) { a.log = obs.NewLogger(buf, "json", "info") }
}

// fatalHealthSink answers every health probe with ClassFatal: a refusal
// waiting will not change (a wrong password, a missing table), which
// WaitReady does not retry (SPEC §13.1 step 7).
type fatalHealthSink struct{ sink.Sink }

func (f fatalHealthSink) Health(context.Context) error {
	return &sink.Error{Class: sink.ClassFatal, Op: "health", Err: errors.New("permission denied (injected)")}
}

// SPEC §13.1 step 7, §22 pendência 27: a refusal WaitReady does not retry ends
// the boot at once, and its last line says so instead of blaming
// sink.boot_timeout and the network. The control -- a sink that is merely
// unreachable -- still gets the boot_timeout line, so the split is proven and
// not just one message replaced by another.
func TestBootRefusalLogNamesItsCause(t *testing.T) {
	t.Run("fatal refusal", func(t *testing.T) {
		var buf logBuf
		wrap := func(s sink.Sink) sink.Sink { return fatalHealthSink{s} }
		in := launch(t, testID, append(baseArgs(t.TempDir()), "--sink.boot_timeout=120s"), wrap, captureLog(&buf))
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		log := buf.String()
		if !strings.Contains(log, "the sink refused this server permanently") {
			t.Errorf("no line naming the permanent refusal:\n%s", log)
		}
		if strings.Contains(log, "within sink.boot_timeout") {
			t.Errorf("a refusal that was never retried is reported as a boot_timeout:\n%s", log)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		var buf logBuf
		fs := newFaultySink(false)
		fs.failHealth.Store(true)
		in := launch(t, testID, append(baseArgs(t.TempDir()), "--sink.boot_timeout=1s"), fs.wrap, captureLog(&buf))
		if code := in.waitExit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d", code, ExitSink)
		}
		log := buf.String()
		if !strings.Contains(log, "sink did not become usable within sink.boot_timeout") ||
			strings.Contains(log, "refused this server permanently") {
			t.Errorf("an unreachable sink must be reported as the boot_timeout:\n%s", log)
		}
	})
}

// ---------------------------------------------------------------- shutdown

// fatalShutdown boots ring=memory with a destination that answers every
// write with ClassFatal, accepts n messages -- the first one held by a
// worker, the rest queued in the ring -- and opens the gate, so the FATAL
// shutdown runs. hooks run on the App before it starts.
func fatalShutdown(t *testing.T, n int, extra []string, hooks ...func(*App)) (*instance, string) {
	t.Helper()
	dir := t.TempDir()
	fs := newFaultySink(true) // the first write waits until everything is accepted
	fs.failFatal.Store(true)
	args := append(append(baseArgs(dir), "--listen.udp=", "--ring.mode=memory"), extra...)
	in := launch(t, testID, args, fs.wrap, hooks...)
	in.waitReady()
	tcp := in.app.TransportAddr("tcp")
	sendLF(t, tcp, "<13>1 2026-09-12T14:33:01Z host app - - - in-dump-first")
	waitFor(t, 10*time.Second, "the worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	lines := make([]string, n-1)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - in-dump-%02d", i)
	}
	sendLF(t, tcp, lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == uint64(n)
	})
	fs.open()
	return in, dir
}

// fatalShutdownInDump is fatalShutdown held right before the emergency dump
// is written, until the returned release is called.
func fatalShutdownInDump(t *testing.T, n int, extra ...string) (*instance, string, func()) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	in, dir := fatalShutdown(t, n, extra, func(a *App) {
		a.beforeDump = func() {
			close(entered)
			<-release
		}
	})
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the FATAL shutdown never reached the emergency dump")
	}
	return in, dir, releaseFn
}

func dumpsIn(dir string) []string {
	d, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
	return d
}

// settle lets the shutdown an abort left behind run its dump, which must give
// up (emergency.Store.SetAbandon): the abort counted its messages lost, and a
// file published now would bring them back at the next boot. In the binary
// the abort's os.Exit ends it instead.
func settle(t *testing.T, dir string, release func()) {
	t.Helper()
	release()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if d := dumpsIn(dir); len(d) > 0 {
			t.Fatalf("an aborted dump reached the disk: %v", d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SPEC §13.3, §13.4, §22 pendências 28 and 29. A FATAL shutdown starts with no
// signal, so the first SIGTERM/SIGINT during it (a docker stop while the dump
// is written) is tolerated: the dump completes and the exit is 3. A second
// one aborts; SIGQUIT aborts at once. An abort that lands while the dump is
// being written loses the WHOLE dump -- the ring's own count was already
// committed by Drain -- counts all of it as reason="abort", and the dump
// gives up instead of publishing what was counted lost.
func TestFatalShutdownSignals(t *testing.T) {
	const n = 9
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
		d := dumpsIn(dir)
		if len(d) != 1 {
			t.Fatalf("dump files = %v, want 1", d)
		}
		res, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Read(d[0])
		if err != nil || len(res.Items) != n {
			t.Fatalf("the dump holds %d of %d messages (%v)", len(res.Items), n, err)
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

// With ONE worker the rest of the messages wait in the ring and Drain takes
// them out of its count, so during the dump the ring says 1 of n: only the
// dump phase makes the abort count all of them. With several workers every
// message can be held by some worker, Drain returns nothing, and the test
// would pass without the dump phase (mutant M07 of the review of
// go-relplog-pg-server).
func TestAbortDuringTheDumpWithItemsDrainedFromTheRing(t *testing.T) {
	const n = 9
	in, dir, release := fatalShutdownInDump(t, n, "--sink.workers=1")
	if st := in.app.ring.Stats(); st.Items == n {
		t.Fatalf("the ring still counts %d during the dump: nothing was drained, the test proves nothing", st.Items)
	}
	defer settle(t, dir, release)
	in.app.abort <- syscall.SIGTERM
	in.app.abort <- syscall.SIGINT
	if code := in.waitExit(5 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	if d := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != n {
		t.Errorf(`abort = %d, want %d`, d, n)
	}
}

// The real gracefulShutdown with an abort landing while it collects the
// leftovers (between the dump phase and the drain) and a dump that would fail:
// the messages are counted once, as abort, and no dump is attempted after it.
// A shutdown that re-armed the counter after the collection counted the
// failed dump again (2N).
func TestAbortDuringTheCollectionCountsOnceAndDumpsNothing(t *testing.T) {
	const n = 9
	in, _ := fatalShutdown(t, n, []string{"--sink.workers=1"}, func(a *App) {
		emgDir := a.cfg.Emergency.Dir
		a.beforeCollect = func() {
			// A dump into this would fail and count emergency_failed.
			if err := os.RemoveAll(emgDir); err == nil {
				_ = os.WriteFile(emgDir, nil, 0o600)
			}
			a.abortLoss() // the second signal lands here
		}
	})
	if code := in.waitExit(30 * time.Second); code != ExitAborted && code != ExitSink {
		t.Fatalf("exit %d", code)
	}
	m := in.app.metrics
	ab, failed := m.EventsDroppedTotal[obs.EventDropAbort].Load(), m.EventsDroppedTotal[obs.EventDropEmergencyFailed].Load()
	if ab != n || failed != 0 {
		t.Fatalf("abort %d, emergency_failed %d for %d messages; want %d and 0", ab, failed, n, n)
	}
}

// The same abort with a working emergency directory: no dump is published
// after it. A file written there would be reingested at the next boot although
// every message in it was counted lost.
func TestAbortDuringTheCollectionPublishesNoDump(t *testing.T) {
	const n = 5
	in, dir := fatalShutdown(t, n, []string{"--sink.workers=1"}, func(a *App) {
		a.beforeCollect = func() { a.abortLoss() }
	})
	in.waitExit(30 * time.Second)
	if d := dumpsIn(dir); len(d) != 0 {
		t.Fatalf("dumps published after the abort counted them lost: %v", d)
	}
	if ab := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); ab != n {
		t.Fatalf("abort = %d, want %d", ab, n)
	}
}

// SIGQUIT while serving aborts at once and counts what the ring holds
// (SPEC §13.4). In-process runs have no SIGQUIT channel unless a test gives
// one; without it the abortLoss of that branch was never run by a test.
func TestSIGQUITWhileServingCountsTheRing(t *testing.T) {
	const n = 4
	quit := make(chan os.Signal, 1)
	fs := newFaultySink(true) // writes wait: the messages stay in the ring
	in := launch(t, testID, append(baseArgs(t.TempDir()), "--listen.udp=", "--ring.mode=memory"), fs.wrap,
		func(a *App) { a.quit = quit })
	in.waitReady()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - quit-%d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	quit <- syscall.SIGQUIT
	if code := in.waitExit(10 * time.Second); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
	fs.open()
	if ab := in.app.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); ab != n {
		t.Fatalf("abort = %d, want the %d in the ring", ab, n)
	}
}

// ---------------------------------------------------------------- bind

// cancelAfterWrite cancels the boot's context once a write of the reingestion
// returns: the signal lands between the reingestion and the bind.
type cancelAfterWrite struct {
	sink.Sink
	cancel context.CancelFunc
}

func (c cancelAfterWrite) Write(ctx context.Context, b *sink.Batch) error {
	err := c.Sink.Write(ctx, b)
	c.cancel()
	return err
}

// SPEC §13.5: a signal that lands after the reingestion and before the bind
// cancels the bind context; with a host name in a listen key the lookup fails
// with "operation was canceled". That is an interrupted boot, 130, not the
// exit 1 of a bad address (independent review of go-relplog-nats-server,
// 2026-10-13). Every transport of this server binds with that context.
func TestSignalBeforeTheBindExits130(t *testing.T) {
	p := newPKI(t)
	signalBeforeTheBind(t, []bindCase{
		{"tcp", func(*testing.T) []string { return []string{"--listen.tcp=localhost:0"} }},
		{"udp", func(*testing.T) []string { return []string{"--listen.udp=localhost:0"} }},
		{"tls", func(*testing.T) []string { return p.tlsListen("localhost:0") }},
	})
}

// A literal IP and a unix path never look at the context: the bind succeeds
// after the signal, and the boot used to open, answer /readyz 200 and leave
// with 0 through the graceful path (independent review of the port,
// 2026-10-13, X1). The check after the last bind gives them the same 130.
func TestSignalBeforeALiteralBindExits130(t *testing.T) {
	p := newPKI(t)
	signalBeforeTheBind(t, []bindCase{
		{"tcp", func(*testing.T) []string { return []string{"--listen.tcp=127.0.0.1:0"} }},
		{"udp", func(*testing.T) []string { return []string{"--listen.udp=127.0.0.1:0"} }},
		{"tls", func(*testing.T) []string { return p.tlsListen("127.0.0.1:0") }},
		{"unix", func(t *testing.T) []string { return []string{"--listen.unix=" + shortSocketPath(t)} }},
	})
}

type bindCase struct {
	name  string
	flags func(t *testing.T) []string
}

func (p *pki) tlsListen(addr string) []string {
	return []string{"--listen.tls=" + addr, "--tls.cert_file=" + p.serverCert, "--tls.key_file=" + p.serverKey}
}

// signalBeforeTheBind lands a signal between the reingestion and the bind,
// with only the case's transport configured, and wants 130.
func signalBeforeTheBind(t *testing.T, cases []bindCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := emergency.NewStore(filepath.Join(dir, "emergency"), testID.From).Write(crashItems(3, "pre-bind")); err != nil {
				t.Fatal(err)
			}
			args := append(append(baseArgs(dir), "--listen.tcp=", "--listen.udp="), tc.flags(t)...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			code := runContext(ctx, args, testID, func(a *App) {
				a.abort = make(chan os.Signal, 1)
				a.wrapSink = func(s sink.Sink) sink.Sink { return cancelAfterWrite{Sink: s, cancel: cancel} }
			})
			if code != ExitAborted {
				t.Errorf("a signal before the bind exited %d, want %d", code, ExitAborted)
			}
		})
	}
}

// The same on admin.bind, before the sink: the lookup of "localhost" fails
// with "operation was canceled".
func TestCancelledBindIsAnAbortNotAConfigError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	args := append(baseArgs(t.TempDir()), "--admin.bind=localhost:0")
	if code := RunContext(ctx, args, testID); code != ExitAborted {
		t.Fatalf("exit %d, want %d", code, ExitAborted)
	}
}

// ---------------------------------------------------------------- close

// slowTransport is a transport whose Close takes d, as one whose sessions
// still finish their reads within closeGrace.
type slowTransport struct{ d time.Duration }

func (s slowTransport) Start(context.Context) {}
func (s slowTransport) Close() error          { time.Sleep(s.d); return nil }
func (s slowTransport) Addr() string          { return "" }

// SPEC §13.4 phase 3: the transports close AT ONCE. One after the other, the
// tcp sessions kept reading -- and accepting -- for as long as the tls ones
// took, and phase 3 could last one closeGrace per transport instead of the one
// that stop_grace_period is sized for (independent review of
// go-relplog-server, 2026-10-11).
func TestTransportsCloseTogether(t *testing.T) {
	const d = 300 * time.Millisecond
	a, _ := abortApp(t)
	for _, name := range []string{"unix", "udp", "tcp", "tls"} {
		a.transports = append(a.transports, namedTransport{name, slowTransport{d}})
	}
	began := time.Now()
	a.closeTransports()
	took := time.Since(began)
	if took < d {
		t.Fatalf("closeTransports returned after %v, before the transports closed", took)
	}
	if took >= 2*d {
		t.Fatalf("closeTransports took %v for four transports of %v each: they closed one after the other", took, d)
	}
}
