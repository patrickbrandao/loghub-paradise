package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
)

// Pre-existing defects the independent review of the 2026-10-11 port found in
// the emergency path and the signal handling (TESTING.md §8 item 40), each
// with the test that failed before its fix.

// ------------------------------------------------------------------- SIGHUP

// sighupChildEnv names the step a re-executed test binary runs.
const sighupChildEnv = "WHS_TEST_SIGHUP_CHILD"

// SPEC §13.3: Go's default action for SIGHUP ends the process, so nothing
// from the start of Main to its return may leave the signal to it. Until
// 2026-10-11 the watcher lived only while serving and ended with signal.Stop:
// a SIGHUP during the emergency dump killed the process with the ring,
// answered 200, lost and counted nowhere; one during the boot killed the
// instance before it served.
//
// A real signal is the only proof, and the instance runs inside the test
// binary, so each step runs in a child process of its own: a regression kills
// that child -- reported here with the step it reached -- instead of the whole
// suite.
//
// Every child must start with SIGHUP at Go's default action, or a regression
// passes. An ignored signal stays ignored across exec, and every App this
// binary ran before -- any earlier test -- left SIGHUP ignored on its way out
// (SPEC §13.3), so the parent catches the signal while it starts the
// children: exec resets a caught signal to the default action. Each child
// checks that first (the independent review of this round found the test
// passing with Main's Ignore removed, once a ClickHouse test had run before
// it).
func TestSIGHUPNeverEndsTheProcess(t *testing.T) {
	if step := os.Getenv(sighupChildEnv); step != "" {
		if signal.Ignored(syscall.SIGHUP) {
			t.Fatal("precondition: the child started with SIGHUP ignored; this step would prove nothing")
		}
		sighupChild(t, step)
		return
	}
	caught := make(chan os.Signal, 1)
	signal.Notify(caught, syscall.SIGHUP)
	defer signal.Stop(caught)
	for _, step := range []string{"main", "boot", "dump"} {
		t.Run(step, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSIGHUPNeverEndsTheProcess$",
				"-test.count=1", "-test.v")
			cmd.Env = append(os.Environ(), sighupChildEnv+"="+step)
			out, err := cmd.CombinedOutput()
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
					t.Fatalf("the process was killed by %v at %q\n%s", ws.Signal(), lastSIGHUP(out), out)
				}
			}
			if err != nil || !bytes.Contains(out, []byte("sighup child ok")) {
				t.Fatalf("child %q: %v\n%s", step, err, out)
			}
		})
	}
}

// lastSIGHUP is the last step a child announced before sending itself a SIGHUP.
func lastSIGHUP(out []byte) string {
	last := "(none)"
	for _, line := range strings.Split(string(out), "\n") {
		if s, ok := strings.CutPrefix(line, "sending SIGHUP: "); ok {
			last = s
		}
	}
	return last
}

// hup sends this process a SIGHUP and gives Go's default action, were it in
// place, the time to end it.
func hup(t *testing.T, where string) {
	t.Helper()
	fmt.Println("sending SIGHUP: " + where)
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
}

func sighupChild(t *testing.T, step string) {
	switch step {
	case "main":
		// Between Main's first line and run: the configuration and the
		// identity, before any watcher exists.
		dir := t.TempDir()
		called := false
		code := Main([]string{"--ring.data_dir=" + dir, "--log.level=error"},
			func(string) (ident.Identity, error) {
				called = true
				hup(t, "while Main resolves the identity")
				return ident.Identity{}, errors.New("stop here")
			})
		if !called {
			// An exit 1 from the configuration, before any SIGHUP was sent,
			// would pass the check below and prove nothing.
			t.Fatalf("Main returned %d before resolving the identity", code)
		}
		if code != ExitConfig {
			t.Fatalf("Main returned %d, want %d", code, ExitConfig)
		}

	case "boot":
		// The boot waits for a Redis nobody answers for; the public port is
		// not open, so this is no time to reload.
		var log syncBuffer
		adm := freePort(t)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan int, 1)
		go func() {
			done <- runContext(ctx, []string{"--server.bind=" + freePort(t), "--admin.bind=" + adm,
				"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.type=redis",
				"--sink.redis.addrs=" + freePort(t), "--sink.boot_timeout=60s"},
				testID, func(a *App) { a.log = obs.NewLogger(&log, "text", "info") })
		}()
		in := &instance{t: t, adm: adm}
		in.waitBootState("connecting to the sink", 10*time.Second)
		hup(t, "while the boot waits for the sink")
		cancel()
		if code := <-done; code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
		if !strings.Contains(log.String(), "SIGHUP ignored outside of serving") {
			t.Fatalf("no WARN for the SIGHUP during the boot:\n%s", log.String())
		}

	case "dump":
		var log syncBuffer
		startLog = &log
		defer func() { startLog = nil }()
		dir := t.TempDir()
		// The workers hold back, so drain_timeout runs out with the events
		// still in the ring and the shutdown dumps them (SPEC §10.3).
		hr := &drainHookRing{}
		startRing = hr.wrap
		in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
			"--sink.sqlite.synchronous=OFF", "--sink.batch_max_delay=60s", "--sink.batch=4096",
			"--shutdown.drain_timeout=1s")
		startRing = nil
		ids := in.postN(10)
		if len(ids) != 10 {
			t.Fatalf("%d of 10 accepted", len(ids))
		}

		// While serving, the signal still reloads.
		hup(t, "while serving")
		if !waitFor(5*time.Second, func() bool {
			return strings.Contains(log.String(), "SIGHUP: configuration unchanged")
		}) {
			t.Fatalf("a SIGHUP while serving did not reload:\n%s", log.String())
		}

		// Inside the shutdown's last Drain: the workers have stopped and the
		// ring is being collected for the dump. The pause lets anything tied
		// to the workers' context finish stopping first, as it would before
		// a dump of any size.
		hr.onDrain(func() {
			time.Sleep(200 * time.Millisecond)
			hup(t, "during the emergency dump")
		})
		if code := in.stop(); code != ExitSink {
			t.Fatalf("exit %d, want %d: the drain ran out and the ring was dumped", code, ExitSink)
		}
		checkDump(t, dir, testID, envelope.ModeFast, ids)
		if !strings.Contains(log.String(), "SIGHUP ignored outside of serving") {
			t.Fatalf("no WARN for the SIGHUP during the dump:\n%s", log.String())
		}

		// run has returned: the signal stays ignored until os.Exit.
		hup(t, "after run returned")

	default:
		t.Fatalf("unknown step %q", step)
	}
	fmt.Println("sighup child ok")
}

// ------------------------------------------------- reading in every mode

// plantIDs writes n envelopes of testID as one dump of the binary format of
// SPEC §9.1 and returns its path and their uuids.
func plantIDs(t *testing.T, emgDir string, n int) (string, []string) {
	t.Helper()
	if err := os.MkdirAll(emgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	var ids []string
	for i := range n {
		id, env := testJLH(t, testID, fmt.Sprintf("/p/%d", i), fmt.Sprintf(`{"p":%d}`, i))
		fmt.Fprintf(&b, "%d\n", len(env))
		b.Write(env)
		ids = append(ids, id)
	}
	p := filepath.Join(emgDir, fmt.Sprintf("emg-%s-%d.bin", testID.From, time.Now().UnixNano()))
	if err := os.WriteFile(p, b.Bytes(), 0o640); err != nil {
		t.Fatal(err)
	}
	return p, ids
}

// SPEC §9: writing a dump belongs to the memory ring, reading one to every
// boot. A dump is a previous life's; the next may run with another ring mode
// or with the emergency off. Until 2026-10-11 the reader existed only beside
// the writer, and in these three lives the dump stayed on disk with no line of
// log -- events answered 200 and never delivered.
func TestDumpsAreReingestedWhateverTheRingMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"ring=sqlite", []string{"--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF"}},
		{"ring=none", []string{"--ring.mode=none"}},
		{"emergency.enabled=false", []string{"--ring.mode=memory", "--emergency.enabled=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			emgDir := filepath.Join(dir, "emergency")
			dump, ids := plantIDs(t, emgDir, 3)
			cut := filepath.Join(emgDir, fmt.Sprintf(".emg-%s-1.bin.tmp", testID.From))
			if err := os.WriteFile(cut, []byte("half a dump"), 0o640); err != nil {
				t.Fatal(err)
			}

			var log syncBuffer
			startLog = &log
			defer func() { startLog = nil }()
			in := start(t, dir, append([]string{"--sink.type=sqlite", "--sink.workers=1",
				"--sink.sqlite.synchronous=OFF"}, tc.args...)...)
			if code := in.stop(); code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}

			got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast)
			if lost := missing(ids, got); len(lost) > 0 || len(got) != len(ids) {
				t.Errorf("the inbox holds %v, want exactly the dump's %v", got, ids)
			}
			if _, err := os.Stat(dump); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the dump is still on disk after the boot: %v", err)
			}
			if out := log.String(); !strings.Contains(out, "interrupted emergency dumps") ||
				!strings.Contains(out, cut) {
				t.Errorf("the boot did not report the interrupted dump:\n%s", out)
			}
		})
	}
}

// SPEC §9.2: sink=none discards by configuration, so a previous life's dump is
// not "delivered" to it and removed: it stays, counted and reported, for a
// life with a destination. Reading in every mode had extended that removal
// from ring=memory to the other two rings (independent review of this round).
func TestSinkNoneLeavesDumpsInPlace(t *testing.T) {
	for _, mode := range []string{"memory", "sqlite", "none"} {
		t.Run("ring="+mode, func(t *testing.T) {
			dir := t.TempDir()
			dump, _ := plantIDs(t, filepath.Join(dir, "emergency"), 3)
			before, err := os.ReadFile(dump)
			if err != nil {
				t.Fatal(err)
			}
			var log syncBuffer
			startLog = &log
			defer func() { startLog = nil }()
			in := start(t, dir, "--ring.mode="+mode, "--sink.type=none")
			m := in.app.metrics
			if f, e := m.EmergencyFiles.Load(), m.EmergencyEventsPending.Load(); f != 1 || e != 3 {
				t.Errorf("whs_emergency_files=%d whs_emergency_events_pending=%d, want 1 and 3", f, e)
			}
			if code := in.stop(); code != ExitOK {
				t.Fatalf("exit %d, want 0", code)
			}
			if after, err := os.ReadFile(dump); err != nil || !bytes.Equal(after, before) {
				t.Errorf("the dump was removed or altered by a boot with sink=none: %v", err)
			}
			if !strings.Contains(log.String(), "sink.type=none discards what it is given") {
				t.Errorf("the boot did not say why the dump stays:\n%s", log.String())
			}
		})
	}
}

// SPEC §9.2: an emergency.dir that cannot be listed stops only a life that
// writes dumps -- the dump will need it. Any other life logs it and serves: it
// never writes there, and until this round never even looked (independent
// review of this round).
func TestUnlistableEmergencyDirStopsOnlyALifeThatDumps(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root lists any directory")
	}
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.MkdirAll(locked, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o750) })
	emg := "--emergency.dir=" + filepath.Join(locked, "emergency")

	var log syncBuffer
	startLog = &log
	in := start(t, t.TempDir(), "--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF",
		"--sink.type=sqlite", "--sink.workers=1", emg)
	startLog = nil
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	if !strings.Contains(log.String(), "cannot list the emergency directory; any dump in it is NOT reingested") {
		t.Errorf("the boot did not name the directory it could not list:\n%s", log.String())
	}

	// Control: a life that dumps needs the directory, and stops.
	ctl := startExpectingExit(t, t.TempDir(), "--ring.mode=memory", "--sink.type=sqlite",
		"--sink.workers=1", emg)
	select {
	case code := <-ctl.done:
		if code != ExitConfig {
			t.Errorf("control: ring=memory with the emergency on exited %d, want %d", code, ExitConfig)
		}
	case <-time.After(30 * time.Second):
		ctl.cancel()
		t.Fatal("control: the boot that needs the directory did not stop")
	}
}

// ----------------------------------------------------------------- gauges

// SPEC §11.1: whs_emergency_* say what is on disk awaiting reingestion. After
// the boot they are counted from disk -- a file it could not read, or could
// not remove, is still pending -- and a dump adds itself to them. Until
// 2026-10-11 the boot set both to zero whatever it left, and a dump set them
// to 1 and its own size.
//
// The directory is made unwritable for the boot, so the dump it reingests
// cannot be removed and stays pending with its events; then writable again,
// so the shutdown's dump can be written beside the two files left.
func TestEmergencyGaugesCountWhatIsLeftAndAddTheDump(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads and removes through permissions")
	}
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	_, kept := plantIDs(t, emgDir, 3)
	locked, _ := plantIDs(t, emgDir, 2)
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(emgDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(emgDir, 0o750) })

	in := start(t, dir, "--ring.mode=memory", "--sink.type=sqlite", "--sink.workers=1",
		"--sink.sqlite.synchronous=OFF", "--sink.batch_max_delay=60s", "--sink.batch=4096",
		"--shutdown.drain_timeout=1s")
	m := in.app.metrics
	if f, e := m.EmergencyFiles.Load(), m.EmergencyEventsPending.Load(); f != 2 || e != 3 {
		t.Errorf("after the boot: whs_emergency_files=%d whs_emergency_events_pending=%d, "+
			"want 2 (the unreadable one and the one it could not remove) and 3", f, e)
	}
	if got := eventsIn(t, filepath.Join(dir, "events.db"), envelope.ModeFast); len(missing(kept, got)) > 0 {
		t.Fatalf("control: the readable dump was not reingested: %v", got)
	}

	if err := os.Chmod(emgDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if ids := in.postN(4); len(ids) != 4 {
		t.Fatalf("%d of 4 accepted", len(ids))
	}
	if code := in.stop(); code != ExitSink {
		t.Fatalf("exit %d, want %d: the drain ran out and the ring was dumped", code, ExitSink)
	}
	if f, e := m.EmergencyFiles.Load(), m.EmergencyEventsPending.Load(); f != 3 || e != 7 {
		t.Errorf("after the dump: whs_emergency_files=%d whs_emergency_events_pending=%d, want 3 and 7", f, e)
	}
	if n := len(emergencyFiles(t, emgDir)); n != 3 {
		t.Errorf("control: %d dumps on disk, want 3", n)
	}
}

// SPEC §11.1: while the boot reingests, whs_emergency_files is the number of
// dumps it listed -- every one of them awaits reingestion -- so an alert on it
// stays up through a long reingestion, read on the admin port that is open the
// whole boot.
func TestEmergencyFilesGaugeDuringReingestion(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	dir := t.TempDir()
	emgDir := filepath.Join(dir, "emergency")
	plantIDs(t, emgDir, 2)
	plantIDs(t, emgDir, 1)

	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	proxy.Hold(holdWrites(stream))
	in := startExpectingExit(t, dir, redisArgs(proxy.Addr(), stream,
		"--ring.mode=memory", "--sink.op_timeout=60s", "--sink.boot_timeout=30s")...)
	in.waitBootState("reingesting", 20*time.Second)
	if m := in.metrics(); !strings.Contains(m, "\nwhs_emergency_files 2\n") {
		t.Errorf("whs_emergency_files during the reingestion of 2 dumps:\n%s", grepLines(m, "whs_emergency_"))
	}
	in.cancel()
	time.Sleep(200 * time.Millisecond)
	proxy.Close()
	select {
	case <-in.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the interrupted reingestion did not end")
	}
}

// grepLines keeps the lines of s that contain sub.
func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
