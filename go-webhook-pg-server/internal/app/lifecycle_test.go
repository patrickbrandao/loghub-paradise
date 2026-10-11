package app

import (
	"bufio"
	"bytes"
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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

// The lifecycle scenarios SPEC §15.3 calls mandatory: what happens when the
// destination goes away, when it comes back, when the process is asked to stop
// twice, and what is on disk afterwards.
//
// They run against the fake destination of fakesink_test.go, taken DOWN or made
// FATAL on cue, so they need no server. The same cycle against a real
// PostgreSQL, cut by the TCP proxy below, is in postgres_test.go.

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
}

func newTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{t: t, ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{}}
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
		p.mu.Lock()
		paused, closing := p.paused, p.closing
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
		_ = client.Close()
		return
	}
	p.track(client)
	p.track(up)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done

	p.untrack(client)
	p.untrack(up)
	_ = client.Close()
	_ = up.Close()
}

func (p *tcpProxy) track(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[c] = struct{}{}
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

// Resume starts forwarding again.
func (p *tcpProxy) Resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
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
	in := start(t, t.TempDir(),
		"--sink.workers=1",
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
	if lost := missing(ids, eventsIn(t, in)); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 never reached the sink", len(lost), len(ids))
	}
	if files := emergencyFiles(t, filepath.Join(in.dir, "emergency")); len(files) != 0 {
		t.Errorf("a clean drain left emergency dumps: %v", files)
	}
}

// SPEC §10.3: a second signal skips the rest of the drain, accepts the loss and
// exits 130. The events it abandons must be counted, not quietly forgotten.
func TestSecondSignalExits130(t *testing.T) {
	in := start(t, t.TempDir(),
		"--sink.workers=1",
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
	// The abandoned events are counted, not quietly forgotten (SPEC §10.3).
	if got := in.app.metrics.EventsDroppedTotal[obs.DropAbort].Load(); got != 50 {
		t.Errorf("dropped{abort} = %d, want the 50 events the abort left behind", got)
	}
}

// ------------------------------------------------------ sink down and back

// SPEC §15.3: the sink falls, requests are refused within one health interval,
// and when it returns inside the window everything resumes with nothing lost.
//
// The 503 must arrive even with an empty ring: accepting into a ring that has
// nowhere to drain would be a 200 for an event nobody can promise (§10.2).
func TestSinkDownThenBackLosesNothing(t *testing.T) {
	in := start(t, t.TempDir(), "--sink.fail_timeout=60s", "--sink.max_retries=1000")

	before := in.postN(10)

	in.mem.down.Store(true)
	if !waitFor(20*time.Second, func() bool {
		code, _ := in.post("/hook", `{"during":true}`)
		return code == http.StatusServiceUnavailable
	}) {
		t.Fatal("the server never started refusing while the sink was down")
	}
	if code, _ := in.readyz(); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while the sink was down, want 503", code)
	}

	in.mem.down.Store(false)
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
	if lost := missing(append(before, after...), eventsIn(t, in)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not at the destination", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// fatalArgs hold the accepted events in the ring, so that when the sink dies
// the dump has something to write, and make the fuses trip fast.
var fatalArgs = []string{
	"--sink.workers=1",
	"--sink.batch_max_delay=10s", "--sink.batch=512",
	"--sink.fail_timeout=1s", "--sink.max_retries=3",
	"--shutdown.drain_timeout=3s",
}

// SPEC §9 and §15.3, the whole emergency cycle in one test: the sink dies for
// good, the ring still holds events that were answered 200, the process writes
// them to a JSON Lines dump and exits 3 -- and the NEXT boot reingests them
// and removes the file, each under the uuid its client was given.
//
// This is the path that decides whether "we never answer 200 for an event we
// then lose" is true or merely intended.
func TestFatalSinkDumpsAndTheNextBootReingests(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir, fatalArgs...)
	const n = 8
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	in.mem.fatal.Store(true)
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d -- a sink that answers \"never\" must end the process",
			code, ExitSink)
	}
	if got := in.mem.count(); got != 0 {
		t.Fatalf("the destination confirmed %d events; the test needs them all in the ring", got)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)

	// Second boot, with a destination that works.
	in2 := start(t, dir, fatalArgs...)
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	got := eventsIn(t, in2)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Errorf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}
	if extra := missing(got, ids); len(extra) > 0 {
		t.Errorf("the reingestion delivered %d uuids no client was given", len(extra))
	}
}

// SPEC §9.3: with emergency.enabled=false a fatal sink loses what the ring
// holds -- the operator chose so, and was warned at boot -- and the exit code
// still says the sink failed. No file is written anywhere.
func TestFatalSinkWithoutEmergencyExitsThreeAndWritesNothing(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir, append([]string{"--emergency.enabled=false"}, fatalArgs...)...)
	if len(in.postN(4)) != 4 {
		t.Fatal("not every request was accepted")
	}
	in.mem.fatal.Store(true)
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if _, err := os.Stat(filepath.Join(dir, "emergency")); !os.IsNotExist(err) {
		t.Errorf("emergency.enabled=false still created the dump directory (%v)", err)
	}
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 4 || f != 0 {
		t.Errorf("dropped{abort}=%d dropped{emergency_failed}=%d, want 4 and 0", a, f)
	}
}

// SPEC §9.3: a dump that cannot be written loses the ring, counted ONCE, as
// emergency_failed and not also as abort. The dump directory loses its write
// permission after the boot, which is how a read-only remount looks.
func TestFailedDumpIsCountedOnceAsEmergencyFailed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through directory permissions")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	in := start(t, dir, fatalArgs...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	if err := os.Chmod(emgDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })
	in.mem.fatal.Store(true)
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	m := in.app.metrics
	if a, f := m.EventsDroppedTotal[obs.DropAbort].Load(), m.EventsDroppedTotal[obs.DropEmergencyFailed].Load(); a != 0 || f != 5 {
		t.Errorf("dropped{abort}=%d dropped{emergency_failed}=%d, want 0 and 5", a, f)
	}
}

// SPEC §10.3: on the FATAL path no signal started the shutdown, so the first
// one that arrives -- the orchestrator's ordinary SIGTERM -- is a FIRST signal
// and must not throw the dump away; only a second one aborts. The signal is
// queued before the sink fails, so it is read the moment the shutdown starts.
func TestFirstSignalDuringAFatalShutdownKeepsTheDump(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir, fatalArgs...)
	ids := in.postN(6)
	if len(ids) != 6 {
		t.Fatal("not every request was accepted")
	}
	in.interrupt()
	in.mem.fatal.Store(true)
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d: one signal during a FATAL shutdown is not an abort", code, ExitSink)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)
	if got := in.app.metrics.EventsDroppedTotal[obs.DropAbort].Load(); got != 0 {
		t.Errorf("dropped{abort} = %d, want 0", got)
	}
}

// SPEC §9.2: a line the reader accepts and the destination still refuses as
// logic costs that line only. The batch is retried one item at a time, the
// rest is delivered, the boot goes on, and the file is quarantined with the
// refused line in it. Returning the error instead stopped every boot with
// exit 3 (independent review, 2026-10-07).
func TestALineTheDestinationRefusesIsQuarantinedNotFatal(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var want []string
	for i := 0; i < 5; i++ {
		id, env := testJLH(t, testID, fmt.Sprintf("/r/%d", i), fmt.Sprintf(`{"r":%d}`, i))
		buf.Write(env)
		buf.WriteByte('\n')
		if i != 2 {
			want = append(want, id)
		}
	}
	path := filepath.Join(emgDir, "emg-"+testID.From+"-1757116800000000000.jsonl")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	ms := &memSink{refuse: func(env []byte) bool { return bytes.Contains(env, []byte(`"path":"/r/2"`)) }}
	in := startWith(t, dir, testID, ms)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0", code)
	}
	got := eventsIn(t, in)
	if len(got) != len(want) || len(missing(want, got)) > 0 {
		t.Errorf("delivered %v, want the 4 lines the destination accepts %v", got, want)
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		t.Errorf("the file with a refused line was not quarantined: %v", err)
	}
}

// SPEC §9.2: a dump with damaged lines contributes every intact line -- before
// AND after the damage, which is what JSON Lines buys over the length-prefixed
// format -- and is then QUARANTINED, not deleted.
func TestDamagedEmergencyDumpIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	var want []string
	for i := 0; i < 4; i++ {
		id, env := testJLH(t, testID, fmt.Sprintf("/t/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		buf.Write(env)
		buf.WriteByte('\n')
		want = append(want, id)
		if i == 1 {
			buf.WriteString("{\"not\":\"an envelope\"}\n") // damage in the middle
		}
	}
	buf.WriteString(`{"uuid":"cut short by the crash`) // and a last line with no newline

	path := filepath.Join(emgDir, "emg-"+testID.From+"-1757116800000000000.jsonl")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir)
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("the damaged dump is still in place under its original name")
	}
	kept, err := os.ReadFile(path + ".corrupt")
	if err != nil {
		t.Fatalf("the damaged dump was not quarantined as .corrupt: %v", err)
	}
	if !bytes.Equal(kept, buf.Bytes()) {
		t.Error("the quarantined dump was altered; it is evidence, kept as it was")
	}
	got := eventsIn(t, in)
	if len(got) != len(want) || len(missing(want, got)) > 0 {
		t.Errorf("the destination holds %v, want exactly the 4 intact lines %v", got, want)
	}
}

// SPEC §9.2: a binary dump of go-webhook-fast-server is NOT read -- this server
// does not speak that format -- and is left exactly as it was, for the fast to
// reingest. A JSON Lines dump in the same directory is the control: it is
// reingested and removed, so the boot did read the directory.
func TestFastBinaryDumpIsLeftAlone(t *testing.T) {
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emgDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var bin bytes.Buffer
	_, env := testJLH(t, testID, "/fast", `{"fast":1}`)
	fmt.Fprintf(&bin, "%d\n", len(env))
	bin.Write(env)
	binPath := filepath.Join(emgDir, "emg-"+testID.From+"-1757116800000000000.bin")
	if err := os.WriteFile(binPath, bin.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	id, cur := testJLH(t, testID, "/current", `{"c":1}`)
	curPath := filepath.Join(emgDir, "emg-"+testID.From+"-1757116900000000000.jsonl")
	if err := os.WriteFile(curPath, append(cur, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	in := start(t, dir)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0: a foreign dump is reported, not a failure", code)
	}
	kept, err := os.ReadFile(binPath)
	if err != nil || !bytes.Equal(kept, bin.Bytes()) {
		t.Errorf("the fast's binary dump was moved or altered (%v)", err)
	}
	if _, err := os.Stat(curPath); err == nil {
		t.Error("control: the JSON Lines dump was not reingested and removed")
	}
	if got := eventsIn(t, in); len(got) != 1 || got[0] != id {
		t.Errorf("the destination holds %v, want only the JSON Lines dump's %s", got, id)
	}
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
	cut := filepath.Join(filepath.Dir(dump), fmt.Sprintf(".emg-%s-1.jsonl.tmp", testID.From))
	if err := os.WriteFile(cut, []byte("half a dump"), 0o640); err != nil {
		t.Fatal(err)
	}
	var log syncBuffer
	startLog = &log
	defer func() { startLog = nil }()
	in := start(t, dir)
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	if n := in.mem.count(); n != 3 {
		t.Errorf("delivered %d, want the dump's 3", n)
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

// The whs_emergency_* gauges after a dump are the boot's count plus the dump
// (SPEC §9): added, not re-read. Re-reading decoded every file in full -- the
// process about to exit, the leftover still live -- for a number it already
// had. The directory is made writable but not listable, so a re-read would
// find nothing and leave the gauges at zero.
func TestEmergencyGaugesCountTheDumpWithoutReReading(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root lists any directory")
	}
	dir := t.TempDir()
	in := start(t, dir, fatalArgs...)
	const n = 5
	if ids := in.postN(n); len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}
	emg := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emg, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(emg, 0o300); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(emg, 0o750)
	in.mem.fatal.Store(true)
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if f, e := in.app.metrics.EmergencyFiles.Load(), in.app.metrics.EmergencyEventsPending.Load(); f != 1 || e != n {
		t.Errorf("whs_emergency_files=%d whs_emergency_events_pending=%d, want 1 and %d", f, e, n)
	}
}

// drain_timeout bounds phases 3 and 4 together (SPEC §10.3). Each had its
// own: a slow upload held phase 3 for the whole timeout, and a DOWN sink then
// held phase 4 for another, so the dump started at twice drain_timeout -- past
// a stop_grace_period sized for once, where the SIGKILL takes the ring.
func TestShutdownDrainTimeoutBoundsConnectionsAndRingTogether(t *testing.T) {
	dir := t.TempDir()
	in := start(t, dir, "--shutdown.drain_timeout=2s", "--sink.workers=1", "--sink.batch=1",
		"--sink.max_retries=100000", "--sink.fail_timeout=10m", "--server.read_timeout=1m")
	in.mem.hang.Store(true)
	if ids := in.postN(3); len(ids) != 3 { // one held by the worker, two in the ring
		t.Fatalf("%d of 3 accepted", len(ids))
	}
	// A slow upload: the head and part of the body, never the rest.
	c, err := net.Dial("tcp", in.pub)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "POST /slow HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\nContent-Length: 100\r\n\r\nhalf")
	time.Sleep(200 * time.Millisecond)

	t0 := time.Now()
	code := in.stop()
	elapsed := time.Since(t0)
	if code != ExitSink {
		t.Fatalf("exit %d, want %d (the ring dumped)", code, ExitSink)
	}
	if elapsed > 3500*time.Millisecond {
		t.Errorf("the shutdown took %s with drain_timeout=2s: the two phases each waited it out", elapsed)
	}
	files, err := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("dumps: %v %v", files, err)
	}
}

// ----------------------------------------------------------- more listeners

// SPEC §12.4: server.listeners > 1 opens several listeners on one address with
// SO_REUSEPORT. The BSD semantics differ from Linux's, so what is asserted here
// is only what is portable -- that the bind succeeds and every request is
// served -- not how the kernel distributes accepts.
func TestMultipleListenersServeTheSameAddress(t *testing.T) {
	in := start(t, t.TempDir(), "--server.listeners=4")

	const n = 40
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted across 4 listeners", len(ids), n)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	if lost := missing(ids, eventsIn(t, in)); len(lost) > 0 {
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
	in := start(t, dir, "--shutdown.drain_timeout=1s", "--server.read_timeout=1m")
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

// plantDump writes n envelopes of this instance as a dump and returns its path.
func plantDump(t *testing.T, dir string, n int) string {
	t.Helper()
	emg := filepath.Join(dir, "emergency")
	if err := os.MkdirAll(emg, 0o750); err != nil {
		t.Fatal(err)
	}
	var b []byte
	for i := range n {
		_, env := testJLH(t, testID, fmt.Sprintf("/d/%d", i), fmt.Sprintf(`{"i":%d}`, i))
		b = append(append(b, env...), '\n')
	}
	p := filepath.Join(emg, fmt.Sprintf("emg-%s-%d.jsonl", testID.From, time.Now().UnixNano()))
	if err := os.WriteFile(p, b, 0o640); err != nil {
		t.Fatal(err)
	}
	return p
}

func emergencyFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "emg-") && strings.HasSuffix(e.Name(), ".jsonl") {
			out = append(out, e.Name())
		}
	}
	return out
}

// checkDump reads the one emergency dump an instance left in dir and holds it
// to SPEC §9.1: named emg-<agent_uuid>-<unixnano>.jsonl, one JLH per line and
// nothing else, every line ended by a newline, every envelope passing the
// oracle, and exactly the uuids that were answered 200 -- reingestion resends
// these bytes, so the uuid a client holds is the one the destination receives.
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
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".jsonl")
	if ns, err := strconv.ParseInt(stamp, 10, 64); !strings.HasPrefix(name, prefix) || err != nil ||
		time.Since(time.Unix(0, ns)) > time.Hour {
		t.Errorf("dump %q is not named emg-<agent_uuid>-<unixnano>.jsonl for %s", name, id.From)
	}

	raw, err := os.ReadFile(filepath.Join(emgDir, name))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatal("the dump does not end with a newline: its last line would read as cut short")
	}
	envs := bytes.Split(raw[:len(raw)-1], []byte("\n"))
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
