package app

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ring"
)

// The lifecycle scenarios SPEC §15.3 calls mandatory: what happens when the
// destination goes away, when it comes back, when the process is asked to stop
// twice, and what is on disk afterwards.
//
// The ones that need a destination that can be taken down on cue go through a
// TCP proxy in front of the test Redis, rather than stopping the server itself.
// That keeps the failure under the test's control and keeps a shared Redis
// usable by everything else running against it.

// ------------------------------------------------------------- tcp proxy

// tcpProxy forwards to a real address and can be cut and restored at will.
type tcpProxy struct {
	t        *testing.T
	ln       net.Listener
	upstream string

	mu      sync.Mutex
	paused  bool
	conns   map[net.Conn]struct{}
	closing bool

	// hold, set through Hold, stalls a client connection at
	// the first chunk it matches: nothing more is forwarded either way, and
	// the client waits as on a destination that stopped answering, until
	// the proxy closes.
	hold     func([]byte) bool
	released chan struct{}
}

func newTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{t: t, ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{},
		released: make(chan struct{})}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		// The client is tracked under the same lock as the check, so a Pause
		// that comes while the upstream is being dialed cuts it too.
		p.mu.Lock()
		paused, closing := p.paused, p.closing
		if !paused && !closing {
			p.conns[c] = struct{}{}
		}
		p.mu.Unlock()
		if paused || closing {
			// Refusing rather than hanging is what a dead Redis looks like to
			// the client: a connection error, not a stall.
			_ = c.Close()
			continue
		}
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(client net.Conn) {
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		p.untrack(client)
		_ = client.Close()
		return
	}
	// A Pause during the dial closed the client and dropped it from conns
	// (even if a Resume followed): the upstream must not outlive it, or the
	// test sees a destination that never went away.
	p.mu.Lock()
	_, live := p.conns[client]
	if live {
		p.conns[up] = struct{}{}
	}
	p.mu.Unlock()
	if !live {
		_ = client.Close()
		_ = up.Close()
		return
	}

	done := make(chan struct{}, 2)
	p.mu.Lock()
	hold := p.hold
	p.mu.Unlock()
	go func() {
		if hold == nil {
			_, _ = io.Copy(up, client)
			done <- struct{}{}
			return
		}
		buf := make([]byte, 64<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 && hold(buf[:n]) {
				<-p.released
				break
			}
			if n > 0 {
				if _, werr := up.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done

	p.untrack(client)
	p.untrack(up)
	_ = client.Close()
	_ = up.Close()
}

func (p *tcpProxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, c)
}

// Pause cuts every live connection and refuses new ones.
func (p *tcpProxy) Pause() {
	p.mu.Lock()
	p.paused = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
}

// Hold stalls, from the next connection on, every client connection at the
// first chunk match accepts.
func (p *tcpProxy) Hold(match func([]byte) bool) {
	p.mu.Lock()
	p.hold = match
	p.mu.Unlock()
}

// Resume starts forwarding again.
func (p *tcpProxy) Resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
	if !p.closing {
		close(p.released)
	}
	p.closing = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	_ = p.ln.Close()
}

// ---------------------------------------------------------------- shutdown

// SPEC §15.5: SIGTERM with a full ring drains completely, exits 0 and loses
// nothing. The ring is loaded first by holding the workers back with a long
// batch delay, so the drain has real work to do rather than an empty queue.
func TestGracefulShutdownDrainsAFullRing(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF",
		// Long enough that the events sit in the ring until the shutdown drain
		// comes for them.
		"--sink.batch_max_delay=3s", "--sink.batch=512",
		"--shutdown.drain_timeout=30s")

	const n = 200
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want %d -- a clean drain must exit 0", code, ExitOK)
	}

	got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 never reached the sink", len(lost), len(ids))
	}
}

// SPEC §10.3: a second signal skips the rest of the drain, accepts the loss and
// exits 130. The events it abandons must be counted, not quietly forgotten.
func TestSecondSignalExits130(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF",
		"--sink.batch_max_delay=60s", "--sink.batch=4096",
		// A drain that would take far longer than the test is willing to wait,
		// so the exit can only come from the second signal.
		"--shutdown.drain_timeout=10m",
		"--shutdown.pre_stop_delay=0s")

	if len(in.postN(50)) != 50 {
		t.Fatal("not every request was accepted")
	}

	done := make(chan int, 1)
	go func() { done <- in.stop() }()

	// Give phase 1 a moment to begin, then interrupt it.
	time.Sleep(300 * time.Millisecond)
	in.interrupt()

	select {
	case code := <-done:
		if code != ExitAborted {
			t.Errorf("exit code %d, want %d", code, ExitAborted)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the second signal did not cut the drain short")
	}
	if got := in.app.metrics.EventsDroppedTotal[obs.DropAbort].Load(); got != 50 {
		t.Errorf("dropped{abort} = %d, want the 50 events the abort left behind", got)
	}
}

// SPEC §10.3: once the abort has accepted the loss, the graceful path stops
// where it is. In the binary os.Exit makes that moot; left running, it would
// wake up at the end of drain_timeout, collect the events the abort already
// counted as lost and dump them -- delivered at the next boot after being
// reported lost, and written into a directory the process no longer owns.
func TestAbortStopsTheGracefulPath(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF",
		"--sink.batch_max_delay=60s", "--sink.batch=4096",
		"--shutdown.drain_timeout=1s",
		"--shutdown.pre_stop_delay=0s")

	if len(in.postN(20)) != 20 {
		t.Fatal("not every request was accepted")
	}
	done := make(chan int, 1)
	go func() { done <- in.stop() }()
	time.Sleep(200 * time.Millisecond)
	in.interrupt()
	if code := <-done; code != ExitAborted {
		t.Fatalf("exit code %d, want %d", code, ExitAborted)
	}

	// Well past the drain deadline the graceful path would have woken at.
	time.Sleep(2 * time.Second)
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the graceful path dumped after the abort: %v", left)
	}
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 20 || f != 0 {
		t.Errorf("dropped{abort}=%d dropped{emergency_failed}=%d, want 20 and 0: each lost event counted once", a, f)
	}
}

// SPEC §10.3: in a shutdown started by FATAL no signal has arrived yet, so
// the first one -- the orchestrator's ordinary SIGTERM, sent while the dump is
// being written -- is a first signal, not a second: it is logged and the dump
// goes on. Treating it as the abort exited 130 and threw away every event the
// dump was there to save (found while building go-webhook-pg-server).
func TestFirstSignalDuringAFatalShutdownKeepsTheDump(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	dir := t.TempDir()

	in := start(t, dir, redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory",
		"--sink.workers=1",
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s")...)
	ids := in.postN(6)
	if len(ids) != 6 {
		t.Fatal("not every request was accepted")
	}

	// Waits in the channel until the FATAL shutdown starts watching it.
	in.interrupt()
	proxy.Pause()

	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d: one signal during a FATAL shutdown is not an abort", code, ExitSink)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)
	if got := in.app.metrics.EventsDroppedTotal[obs.DropAbort].Load(); got != 0 {
		t.Errorf("dropped{abort} = %d, want 0", got)
	}
}

// fatalRedis starts an instance whose sink can be killed for good with
// proxy.Pause, holding the accepted events in the ring so the shutdown has
// something to dump (as TestFatalSinkDumpsAndTheNextBootReingests).
func fatalRedis(t *testing.T, dir string, extra ...string) (*instance, *tcpProxy) {
	t.Helper()
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	in := start(t, dir, redisArgs(proxy.Addr(), stream, append([]string{
		"--ring.mode=memory",
		"--sink.workers=1",
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s"}, extra...)...)...)
	return in, proxy
}

// SPEC §11.1: a dump that fails loses its events once, as emergency_failed,
// and never again as abort -- the counting moved from dumpEmergency to the
// shutdown with the abort accounting, and this keeps it honest.
func TestFailedDumpIsCountedOnceAsEmergencyFailed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through directory permissions")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	in, proxy := fatalRedis(t, dir)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	if err := os.Chmod(emgDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })
	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 0 || f != 5 {
		t.Errorf("dropped{abort}=%d dropped{emergency_failed}=%d, want 0 and 5", a, f)
	}
}

// SPEC §9.3: with emergency.enabled=false the events a FATAL shutdown cannot
// deliver are the abort's loss, counted once, and nothing is written.
func TestFatalSinkWithoutEmergencyCountsTheLossAsAbort(t *testing.T) {
	dir := t.TempDir()
	in, proxy := fatalRedis(t, dir, "--emergency.enabled=false")
	if len(in.postN(4)) != 4 {
		t.Fatal("not every request was accepted")
	}
	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("emergency.enabled=false still wrote a dump: %v", left)
	}
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 4 || f != 0 {
		t.Errorf("dropped{abort}=%d dropped{emergency_failed}=%d, want 4 and 0", a, f)
	}
}

// holdWrites stalls the XADDs to stream and lets the health probe of
// SPEC §8.2.4 (an XADD to stream:healthprobe) through, so the boot passes
// WaitReady and then waits on a destination that stopped answering.
func holdWrites(stream string) func([]byte) bool {
	return func(b []byte) bool {
		return bytes.Contains(b, []byte(stream)) && !bytes.Contains(b, []byte("healthprobe"))
	}
}

// waitBootState waits until /readyz reports the given boot state.
func (in *instance) waitBootState(state string, timeout time.Duration) {
	in.t.Helper()
	if !waitFor(timeout, func() bool {
		_, body := in.readyz()
		return strings.Contains(body, state)
	}) {
		_, body := in.readyz()
		in.t.Fatalf("/readyz never reported %q (last: %q)", state, body)
	}
}

// SPEC §9.2: a SIGTERM while the boot reingests a dump is an interrupted boot
// -- exit 130, the file left for the next one -- not a destination that
// refused it (exit 3), which is what an operator would go and debug.
func TestSignalDuringReingestionExits130(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		_, env := testJLH(t, testID, fmt.Sprintf("/t/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		fmt.Fprintf(&buf, "%d\n", len(env))
		buf.Write(env)
	}
	path := filepath.Join(emgDir, "emg-"+testID.From+"-1757116800000000000.bin")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	proxy.Hold(holdWrites(stream))
	in := startExpectingExit(t, dir, redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory", "--sink.op_timeout=60s", "--sink.boot_timeout=30s")...)
	in.waitBootState("reingesting", 20*time.Second)
	time.Sleep(300 * time.Millisecond) // the write is in flight, held
	in.cancel()
	// Then let go of the held connection: closing a client whose connection
	// is stalled waits out the client's own timeouts, and the exit code is
	// already decided by then.
	time.Sleep(200 * time.Millisecond)
	proxy.Close()

	select {
	case code := <-in.done:
		if code != ExitAborted {
			t.Errorf("exit code %d, want %d: the boot was interrupted, the destination refused nothing",
				code, ExitAborted)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the interrupted reingestion did not end")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the dump is gone after an interrupted reingestion: %v", err)
	}
}

// The same rule for the ring.db backlog of SPEC §10.1 step 5: a SIGTERM while
// it drains exits 130 and the rows stay for the next boot.
func TestSignalDuringTheBacklogDrainExits130(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	dir := t.TempDir()
	args := func(proxy *tcpProxy) []string {
		return redisArgs(proxy.Addr(), stream,
			"--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF",
			"--sink.op_timeout=60s", "--sink.boot_timeout=30s",
			// The closed proxy must not trip the fuses before the drain
			// deadline: a sink FATAL at the end of phase 4 exits 3 whatever
			// the ring (SPEC §10.3), and this test is about ring.db.
			"--sink.max_retries=1000", "--sink.fail_timeout=60s",
			"--shutdown.drain_timeout=1s")
	}

	// First run: every write is held, so the accepted events stay in ring.db.
	first := newTCPProxy(t, os.Getenv(redisAddrEnv))
	first.Hold(holdWrites(stream))
	in := start(t, dir, args(first)...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	first.Close()
	if code := in.stop(); code != ExitOK {
		t.Fatalf("first run exited %d, want 0 (ring=sqlite keeps the rows)", code)
	}

	second := newTCPProxy(t, os.Getenv(redisAddrEnv))
	second.Hold(holdWrites(stream))
	in2 := startExpectingExit(t, dir, args(second)...)
	in2.waitBootState("draining backlog", 20*time.Second)
	time.Sleep(300 * time.Millisecond)
	in2.cancel()
	time.Sleep(200 * time.Millisecond)
	second.Close()
	select {
	case code := <-in2.done:
		if code != ExitAborted {
			t.Errorf("exit code %d, want %d", code, ExitAborted)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the interrupted backlog drain did not end")
	}
	if n := countRows(t, filepath.Join(dir, "ring.db"), "jlh_ring"); n != 5 {
		t.Errorf("ring.db holds %d rows after the interrupted drain, want the 5 still owed", n)
	}
}

// plantDump writes n envelopes of testID as a dump in the binary format of
// SPEC §9.1 and returns its path.
func plantDump(t *testing.T, dir string, n int) string {
	t.Helper()
	emg := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emg, 0o750); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	for i := range n {
		_, env := testJLH(t, testID, fmt.Sprintf("/d/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		fmt.Fprintf(&b, "%d\n", len(env))
		b.Write(env)
	}
	p := filepath.Join(emg, fmt.Sprintf("emg-%s-%d.bin", testID.From, time.Now().UnixNano()))
	if err := os.WriteFile(p, b.Bytes(), 0o640); err != nil {
		t.Fatal(err)
	}
	return p
}

// A crash between the link and the removal of the temporary leaves a twin --
// the same file under the hidden name -- of a dump the next boot reingests
// and removes. The boot removes the twin too; an interrupted dump's
// temporary, which is no twin, it leaves for the operator (SPEC §9.1).
func TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne(t *testing.T) {
	dir := t.TempDir()
	dump := plantDump(t, dir, 3)
	twin := filepath.Join(filepath.Dir(dump), "."+filepath.Base(dump)+".tmp")
	if err := os.Link(dump, twin); err != nil {
		t.Fatal(err)
	}
	cut := filepath.Join(filepath.Dir(dump), fmt.Sprintf(".emg-%s-1.bin.tmp", testID.From))
	if err := os.WriteFile(cut, []byte("half a dump"), 0o640); err != nil {
		t.Fatal(err)
	}
	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast); len(got) != 3 {
		t.Errorf("delivered %d, want the dump's 3", len(got))
	}
	for _, p := range []string{dump, twin} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is still there: %v", filepath.Base(p), err)
		}
	}
	if _, err := os.Stat(cut); err != nil {
		t.Errorf("the interrupted dump was touched: %v", err)
	}
	// The WARN is the operator's only clue that an interrupted dump exists.
	if out := log.String(); !strings.Contains(out, "interrupted emergency dumps") || !strings.Contains(out, cut) ||
		strings.Contains(out, filepath.Base(twin)) {
		t.Errorf("the boot did not report the interrupted dump, and only it:\n%s", out)
	}
}

// ------------------------------------------------------ sink down and back

// SPEC §15.3: the sink falls, requests are refused within one health interval,
// and when it returns inside the window everything resumes with nothing lost.
//
// The 503 must arrive even with an empty ring: accepting into a ring that has
// nowhere to drain would be a 200 for an event nobody can promise (§10.2).
func TestSinkDownThenBackLosesNothing(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))

	in := start(t, t.TempDir(), redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000")...)

	before := in.postN(10)

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool {
		code, _ := in.post("/hook", `{"during":true}`)
		return code == http.StatusServiceUnavailable
	}) {
		t.Fatal("the server never started refusing while the sink was down")
	}
	if code, _ := in.readyz(); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while the sink was down, want 503", code)
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool {
		code, _ := in.readyz()
		return code == http.StatusOK
	}) {
		t.Fatal("the server never recovered after the sink came back")
	}

	after := in.postN(10)
	if len(after) != 10 {
		t.Errorf("%d of 10 requests accepted after recovery", len(after))
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}

	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast))
	if lost := missing(append(before, after...), got); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the stream", len(lost))
	}
}

// SPEC §9 and §15.3, the whole emergency cycle in one test: the sink dies for
// good, the ring still holds events that were answered 200, the process writes
// them to an emergency dump and exits 3 -- and the NEXT boot reingests them
// and removes the file, each under the uuid its client was given.
//
// This is the path that decides whether "we never answer 200 for an event we
// then lose" is true or merely intended.
func TestFatalSinkDumpsAndTheNextBootReingests(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	dir := t.TempDir()

	args := redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory",
		"--sink.workers=1",
		// Hold the events in the ring so the dump has something to write.
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s")

	in := start(t, dir, args...)
	const n = 8
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	proxy.Pause()

	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d -- a sink that never comes back must end the process",
			code, ExitSink)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)

	// Second boot, with the destination reachable again.
	proxy.Resume()
	in2 := start(t, dir, args...)
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}

	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}

	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast))
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}
}

// SPEC §9.2: a dump cut short by the crash that produced it contributes its
// intact prefix and is then QUARANTINED, not deleted. Destroying the evidence
// of a crash is not this program's call.
func TestTruncatedEmergencyDumpIsQuarantined(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Two whole records followed by a header promising more than is there.
	var buf bytes.Buffer
	var want []string
	for i := 0; i < 2; i++ {
		id, env := testJLH(t, testID, fmt.Sprintf("/t/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		fmt.Fprintf(&buf, "%d\n", len(env))
		buf.Write(env)
		want = append(want, id)
	}
	buf.WriteString("400\ntruncated")

	path := filepath.Join(emgDir, "emg-"+testID.From+"-1757116800000000000.bin")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir, redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory")...)
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}

	if _, err := os.Stat(path); err == nil {
		t.Error("the truncated dump is still in place under its original name")
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		t.Errorf("the truncated dump was not quarantined as .corrupt: %v", err)
	}

	// The intact prefix must have been delivered, under the uuids it was
	// written with: a partially readable file is still worth every record it
	// does hold.
	got := uuidsOf(verify(t, redisEnvelopes(t, c, stream), testID, envelope.ModeFast))
	if len(got) != 2 || len(missing(want, got)) > 0 {
		t.Errorf("the stream holds %v, want exactly the 2 intact records %v", got, want)
	}
}

// SPEC §9.2: a dump of the pre-LogHub format -- two lengths per record, the
// v1 envelope and the body apart -- is renamed .legacy and NOT reingested: no
// destination understands that envelope any more, and the previous version can
// still reingest the file. A current dump in the same directory is the
// control: it is reingested and removed, so the boot did read the directory,
// and the legacy file did not stop it from reaching the next one.
func TestLegacyEmergencyDumpIsSetAside(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var legacy bytes.Buffer
	for i := 0; i < 2; i++ {
		env := fmt.Sprintf(`{"v":1,"id":"0198000%d-0000-7000-8000-000000000000","body-size":3}`, i)
		fmt.Fprintf(&legacy, "%d %d\n%s%s", len(env), 3, env, "abc")
	}
	// Older than the current dump, so it is met first.
	legacyPath := filepath.Join(emgDir, "emg-whs-1-1757116800000000000.bin")
	if err := os.WriteFile(legacyPath, legacy.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	var current bytes.Buffer
	var want []string
	for i := 0; i < 2; i++ {
		id, env := testJLH(t, testID, fmt.Sprintf("/c/%d", i), fmt.Sprintf(`{"c":%d}`, i))
		fmt.Fprintf(&current, "%d\n", len(env))
		current.Write(env)
		want = append(want, id)
	}
	currentPath := filepath.Join(emgDir, "emg-"+testID.From+"-1757116900000000000.bin")
	if err := os.WriteFile(currentPath, current.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF")
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0: a legacy dump is set aside, not a failure", code)
	}

	if _, err := os.Stat(legacyPath); err == nil {
		t.Error("the legacy dump is still in place under its original name")
	}
	kept, err := os.ReadFile(legacyPath + ".legacy")
	if err != nil {
		t.Fatalf("the legacy dump was not set aside as .legacy: %v", err)
	}
	if !bytes.Equal(kept, legacy.Bytes()) {
		t.Error("the legacy dump was altered; it must stay readable by the previous version")
	}
	if _, err := os.Stat(currentPath); err == nil {
		t.Error("control: the current dump was not reingested and removed")
	}

	got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
	if len(got) != len(want) || len(missing(want, got)) > 0 {
		t.Errorf("the inbox holds %v, want exactly the current dump's %v and nothing of the legacy one",
			got, want)
	}
}

// SPEC §7.4: a ring.db from before LogHub holds the v1 table `ring`, whose
// rows carry the old envelope and the body apart. With rows it boots, delivers
// nothing from it -- no destination understands that envelope -- and leaves
// it exactly as it was, for the previous version to drain; the new ring lives
// in jlh_ring beside it. Empty, it is dropped: the control that the boot
// looked at the table at all.
func TestLegacyRingTableIsLeftUntouched(t *testing.T) {
	// plant writes a v1 ring table with n rows. The columns are the shape of
	// the v1 ring; only the name `ring` and the row count are what the boot
	// looks at.
	plant := func(t *testing.T, path string, n int) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec(`CREATE TABLE ring (
			rowid_ INTEGER PRIMARY KEY AUTOINCREMENT,
			id     TEXT    NOT NULL,
			env    BLOB    NOT NULL,
			body   BLOB    NOT NULL,
			nbytes INTEGER NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("0198000%d-0000-7000-8000-000000000000", i)
			env := fmt.Sprintf(`{"v":1,"id":"%s","body-size":3}`, id)
			if _, err := db.Exec(`INSERT INTO ring (id, env, body, nbytes) VALUES (?,?,?,?)`,
				id, []byte(env), []byte("abc"), len(env)+3); err != nil {
				t.Fatal(err)
			}
		}
	}
	// snapshot is the whole v1 table as text, to compare before and after.
	snapshot := func(t *testing.T, path string) (string, bool) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ring'`).
			Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return "", false
		}
		var s sql.NullString
		if err := db.QueryRow(`SELECT group_concat(r, ';') FROM (
			SELECT rowid_ || ':' || id || ':' || hex(env) || ':' || hex(body) || ':' || nbytes AS r
			FROM ring ORDER BY rowid_)`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s.String, true
	}
	args := []string{"--ring.mode=sqlite", "--sink.type=sqlite", "--sink.workers=1",
		"--ring.sqlite.synchronous=OFF", "--sink.sqlite.synchronous=OFF"}

	t.Run("with rows", func(t *testing.T) {
		dir := t.TempDir()
		ringDB := filepath.Join(dir, "ring.db")
		plant(t, ringDB, 3)
		before, _ := snapshot(t, ringDB)
		if strings.Count(before, ";") != 2 {
			t.Fatalf("the planted v1 table does not hold 3 rows: %q", before)
		}

		in := start(t, dir, args...)
		ids := in.postN(5)
		if len(ids) != 5 {
			t.Fatalf("%d of 5 accepted beside a legacy table", len(ids))
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit code %d, want 0", code)
		}

		got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
		if lost := missing(ids, got); len(lost) > 0 {
			t.Errorf("%d of %d events answered 200 never reached the sink", len(lost), len(ids))
		}
		if extra := missing(got, ids); len(extra) > 0 {
			t.Errorf("%d events in the inbox were never answered to a client: %v", len(extra), extra)
		}
		after, exists := snapshot(t, ringDB)
		if !exists {
			t.Fatal("the v1 ring table with rows was dropped; its events are lost")
		}
		if after != before {
			t.Errorf("the v1 ring table was altered:\nbefore %s\nafter  %s", before, after)
		}
		if n := countRows(t, ringDB, "jlh_ring"); n != 0 {
			t.Errorf("%d rows left in jlh_ring after a clean stop", n)
		}
	})

	t.Run("empty is dropped", func(t *testing.T) {
		dir := t.TempDir()
		ringDB := filepath.Join(dir, "ring.db")
		plant(t, ringDB, 0)

		in := start(t, dir, args...)
		if len(in.postN(2)) != 2 {
			t.Fatal("not every request was accepted")
		}
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit code %d, want 0", code)
		}
		if _, exists := snapshot(t, ringDB); exists {
			t.Error("the empty v1 ring table is still there; the boot never looked at it")
		}
	})
}

// ----------------------------------------------------------- more listeners

// SPEC §12.4: server.listeners > 1 opens several listeners on one address with
// SO_REUSEPORT. The BSD semantics differ from Linux's, so what is asserted here
// is only what is portable -- that the bind succeeds and every request is
// served -- not how the kernel distributes accepts.
func TestMultipleListenersServeTheSameAddress(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir,
		"--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF",
		"--server.listeners=4")

	const n = 40
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted across 4 listeners", len(ids), n)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	if lost := missing(ids, eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)); len(lost) > 0 {
		t.Errorf("%d events were lost with 4 listeners", len(lost))
	}
}

// SPEC §10.3: an upload still reading its body when phase 3 runs out of time
// goes on after it. If the body arrives after the shutdown's last Drain, the
// ring must refuse it: answered 200 there, the event was in neither the
// destination nor the dump, counted nowhere, and the process exited 0 (found
// by the independent review of 2026-10-11; one drain_timeout for phases 3 and
// 4 had left no slack after phase 3 to catch it). The ring completes the
// upload inside that Drain, once the ring is empty: a Seal after the Drain,
// or none, lets it in there (the review of the port back into
// go-webhook-nats-server: a hook run after the Drain let a Seal moved between
// the two survive).
func TestUploadFinishingAfterTheDrainIsRefused(t *testing.T) {
	dir := t.TempDir()
	hr := &drainHookRing{}
	startRing = hr.wrap
	in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--shutdown.drain_timeout=1s", "--server.read_timeout=1m")
	startRing = nil
	conn, err := net.Dial("tcp", in.pub)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /late HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\nContent-Length: 100\r\n\r\nhalf")
	time.Sleep(200 * time.Millisecond) // the handler is reading the body

	status := make(chan int, 1)
	hr.onDrain(func() {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(bytes.Repeat([]byte{'x'}, 96)); err != nil {
			status <- -1
			return
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			status <- -1
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	})
	in.stop()
	select {
	case code := <-status:
		if code == http.StatusOK {
			t.Fatal("an upload that finished after the last Drain was answered 200: that event is lost")
		}
		if code != http.StatusServiceUnavailable {
			t.Errorf("the late upload got %d, want 503", code)
		}
	default:
		t.Fatal("the shutdown never reached its last Drain")
	}
}

// ----------------------------------------------------------------- helpers

// drainHookRing is the memory ring with a function run inside its first
// Drain, after the ring is emptied and before the shutdown goes on. Seal and
// everything else are the memory ring's.
type drainHookRing struct {
	*ring.Memory
	mu    sync.Mutex
	after func()
}

func (r *drainHookRing) wrap(m *ring.Memory) ring.Ring {
	r.Memory = m
	return r
}

func (r *drainHookRing) onDrain(f func()) {
	r.mu.Lock()
	r.after = f
	r.mu.Unlock()
}

func (r *drainHookRing) Drain() []*envelope.Item {
	out := r.Memory.Drain()
	r.mu.Lock()
	f := r.after
	r.after = nil
	r.mu.Unlock()
	if f != nil {
		f()
	}
	return out
}

func emergencyFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "emg-") && strings.HasSuffix(e.Name(), ".bin") {
			out = append(out, e.Name())
		}
	}
	return out
}

// checkDump reads the one emergency dump an instance left in dir and holds it
// to SPEC §9.1: named emg-<agent_uuid>-<unixnano>.bin, ONE length per record,
// every record a JLH that passes the oracle, and exactly the uuids that were
// answered 200 -- reingestion resends these bytes, so the uuid a client holds
// is the one the destination receives.
//
// The format is parsed here by hand rather than through emergency.Read, so a
// mistake made the same way on both sides of the file cannot pass.
func checkDump(t *testing.T, dir string, id ident.Identity, mode string, ids []string) {
	t.Helper()
	emgDir := filepath.Join(dir, "emergency")
	dumps := emergencyFiles(t, emgDir)
	if len(dumps) != 1 {
		t.Fatalf("%d emergency dumps on disk, want 1: %v", len(dumps), dumps)
	}
	name := dumps[0]
	prefix := "emg-" + id.From + "-"
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".bin")
	if ns, err := strconv.ParseInt(stamp, 10, 64); !strings.HasPrefix(name, prefix) || err != nil ||
		time.Since(time.Unix(0, ns)) > time.Hour {
		t.Errorf("dump %q is not named emg-<agent_uuid>-<unixnano>.bin for %s", name, id.From)
	}

	raw, err := os.ReadFile(filepath.Join(emgDir, name))
	if err != nil {
		t.Fatal(err)
	}
	var envs [][]byte
	for i := 0; len(raw) > 0; i++ {
		line, rest, ok := bytes.Cut(raw, []byte("\n"))
		n, err := strconv.Atoi(string(line))
		if !ok || err != nil || n < 0 || n > len(rest) {
			t.Fatalf("record %d: header %q is not one length followed by that many bytes", i, line)
		}
		envs = append(envs, rest[:n])
		raw = rest[n:]
	}
	got := uuidsOf(verify(t, envs, id, mode))
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 are not in the dump", len(lost), len(ids))
	}
	if extra := missing(got, ids); len(extra) > 0 {
		t.Errorf("the dump holds %d uuids no client was given", len(extra))
	}
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// SPEC §12.4: only a memory ring holds ring.max_bytes in RAM, so only there is
// there a number to compare GOMEMLIMIT against. The old rule suggested 16 GiB
// for a sqlite ring whose memory use had nothing to do with its byte ceiling.
func TestMemoryAdviceFollowsTheRingMode(t *testing.T) {
	c := &config.Config{}
	c.Ring.MaxBytes = 512 << 20

	c.Ring.Mode = config.RingMemory
	if want, _ := memoryAdvice(c); want != 1<<30 {
		t.Errorf("memory: want = %d, want twice ring.max_bytes", want)
	}
	for _, mode := range []string{config.RingSQLiteMode, config.RingNone} {
		c.Ring.Mode = mode
		want, resident := memoryAdvice(c)
		if want != 0 || resident == "" {
			t.Errorf("%s: want = %d resident = %q; there is no ring in RAM to size from", mode, want, resident)
		}
	}
}
