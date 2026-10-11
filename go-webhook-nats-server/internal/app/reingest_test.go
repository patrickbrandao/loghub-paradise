package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/sink"
)

// The reingestion paths the independent review of 2026-10-10 exercised from
// a scratch copy, kept here.

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

// partialSink stores every item refuse does not match and reports the rest
// as a partial delivery, as the NATS sink does for a message it cannot
// publish (SPEC §8.1).
type partialSink struct {
	mu     sync.Mutex
	envs   [][]byte
	refuse func([]byte) bool
}

func (s *partialSink) Write(_ context.Context, items []*envelope.Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	refused := false
	delivered := []int{}
	for i, it := range items {
		if s.refuse(it.Env) {
			refused = true
			continue
		}
		s.envs = append(s.envs, bytes.Clone(it.Env))
		delivered = append(delivered, i)
	}
	if refused {
		return &sink.Error{Class: sink.ClassLogic, Op: "partial", Err: errors.New("too big"), Delivered: delivered}
	}
	return nil
}
func (s *partialSink) Health(context.Context) error { return nil }
func (s *partialSink) Close() error                 { return nil }
func (s *partialSink) Type() string                 { return "partial" }

func (s *partialSink) stored() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.envs)
}

// A batch of the reingestion delivered in part: the stored items count as
// written, the refused ones stay in the file, which is quarantined, and the
// boot goes on (SPEC §9.2).
func TestReingestPartialDelivery(t *testing.T) {
	dir := t.TempDir()
	path := plantDump(t, dir, 7)
	ps := &partialSink{refuse: func(e []byte) bool {
		return bytes.Contains(e, []byte(`"path":"/d/2"`)) || bytes.Contains(e, []byte(`"path":"/d/5"`))
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	apps := make(chan *App, 1)
	args := []string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + dir, "--log.level=error", "--sink.batch=3"}
	go func() {
		done <- runContext(ctx, args, testID, func(a *App) { a.sinkOverride = ps; apps <- a })
	}()
	a := <-apps
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path + ".corrupt"); err == nil {
			break
		}
		select {
		case code := <-done:
			t.Fatalf("the boot ended with %d before quarantining the dump", code)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the dump was never quarantined")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if code := <-done; code != ExitOK && code != ExitAborted {
		t.Errorf("exit %d", code)
	}
	if n := ps.stored(); n != 5 {
		t.Errorf("stored %d, want the 5 the destination took", n)
	}
	if w := a.metrics.EventsWrittenTotal.Load(); w != 5 {
		t.Errorf("whs_events_written_total = %d, want 5", w)
	}
}

// blockSink answers health checks and holds every Write until its context
// ends.
type blockSink struct {
	writes atomic.Int32
	inside chan struct{}
}

func (s *blockSink) Write(ctx context.Context, _ []*envelope.Item) error {
	if s.writes.Add(1) == 1 {
		close(s.inside)
	}
	<-ctx.Done()
	return &sink.Error{Class: sink.ClassNetwork, Op: "block", Err: ctx.Err()}
}
func (s *blockSink) Health(context.Context) error { return nil }
func (s *blockSink) Close() error                 { return nil }
func (s *blockSink) Type() string                 { return "block" }

// SIGTERM while the boot reingests: exit 130, and the dump stays for the
// next boot (SPEC §9.2, §10.1).
func TestSIGTERMDuringReingestKeepsTheDump(t *testing.T) {
	dir := t.TempDir()
	dump := plantDump(t, dir, 5)
	bs := &blockSink{inside: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	args := []string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + dir, "--log.level=error", "--sink.op_timeout=30s"}
	go func() { done <- runContext(ctx, args, testID, func(a *App) { a.sinkOverride = bs }) }()
	select {
	case <-bs.inside:
	case <-time.After(10 * time.Second):
		t.Fatal("the reingestion never wrote")
	}
	cancel()
	select {
	case code := <-done:
		if code != ExitAborted {
			t.Errorf("exit %d, want %d", code, ExitAborted)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SIGTERM during the reingestion did not end the boot")
	}
	if _, err := os.Stat(dump); err != nil {
		t.Errorf("the dump was not kept: %v", err)
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
	// The WARN is the operator's only clue that an interrupted dump exists,
	// and it must not claim its events were counted lost: only the abort by a
	// second signal counts them (SPEC §9.1).
	out := log.String()
	if !strings.Contains(out, "interrupted emergency dumps") || !strings.Contains(out, cut) ||
		strings.Contains(out, filepath.Base(twin)) {
		t.Errorf("the boot did not report the interrupted dump, and only it:\n%s", out)
	}
	if !strings.Contains(out, "only if a second signal aborted the dump") || !strings.Contains(out, "final name") {
		t.Errorf("the WARN must say who counted the loss and how to reingest the file:\n%s", out)
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
